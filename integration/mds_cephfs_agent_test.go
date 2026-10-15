package integration_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Test-only consumer capability agent for live CephFS acceptance. The product
// delivers MDS pushes and sends client messages only when asked; this agent
// plays the native client's part so native LibCephFS oracles never wait on an
// unanswered revoke. It is an independent restatement of the pinned
// client/Client.cc (add_update_cap, handle_caps, handle_cap_grant, check_caps,
// send_cap, remove_cap and handle_lease at v20.2.4; LGPL-2.1, no code
// copied), reduced to a client that holds no file data: nothing is ever used,
// dirty or flushing, and directories are never marked complete.

const (
	cephCapLinkExcl     = 2 << 4
	cephCapXattrExcl    = 2 << 6
	cephCapFileWRExtend = 64 << 8
	cephCapFileLazyIO   = 128 << 8
	cephCapAnyShared    = cephCapAuthShared | cephCapLinkShared | cephCapXattrShared | cephCapFileShared
	cephCapAnyRD        = cephCapAnyShared | cephCapFileRD | cephCapFileCache
	cephCapAnyExcl      = cephCapAuthExcl | cephCapLinkExcl | cephCapXattrExcl | cephCapFileExcl
	cephCapAnyFileRD    = cephCapFileRD | cephCapFileCache | cephCapFileShared
	cephCapAnyFileWR    = cephCapFileWR | cephCapFileBuffer | cephCapFileExcl
	cephCapAny          = cephCapAnyRD | cephCapAnyExcl | cephCapAnyFileWR | cephCapFileLazyIO | cephCapPin

	cephFileModeRD   = 1
	cephFileModeWR   = 2
	cephFileModeRDWR = 3
)

// cephCapsForMode restates ceph_caps_for_mode.
func cephCapsForMode(mode int) uint32 {
	caps := uint32(cephCapPin)
	if mode&cephFileModeRD != 0 {
		caps |= cephCapFileShared | cephCapFileRD | cephCapFileCache
	}
	if mode&cephFileModeWR != 0 {
		caps |= cephCapFileExcl | cephCapFileWR | cephCapFileBuffer | cephCapAuthShared | cephCapAuthExcl | cephCapXattrShared | cephCapXattrExcl
	}
	return caps
}

// cephCapString renders a capability mask like the native ccap_string.
func cephCapString(c uint32) string {
	generic := func(g uint32) string {
		var b strings.Builder
		for i, letter := range "sxcrwbal" {
			if g&(1<<i) != 0 {
				b.WriteRune(letter)
			}
		}
		return b.String()
	}
	var b strings.Builder
	if c&cephCapPin != 0 {
		b.WriteString("p")
	}
	for _, part := range []struct {
		name  string
		shift uint
		mask  uint32
	}{{"A", 2, 3}, {"L", 4, 3}, {"X", 6, 3}, {"F", 8, 0xff}} {
		if g := (c >> part.shift) & part.mask; g != 0 {
			b.WriteString(part.name + generic(g))
		}
	}
	if b.Len() == 0 {
		return "-"
	}
	return b.String()
}

func cephSeqAfter(a, b uint32) bool { return int32(a-b) > 0 }

// cephfsCap is the agent's view of one inode and its single (rank 0) cap.
type cephfsCap struct {
	Ino, CapID                  uint64
	Issued, Implemented, Wanted uint32
	Seq, IssueSeq, MSeq         uint32
	Realm                       uint64
	Flags                       uint8
	Dir                         bool
	UID, GID, Mode, NLink       uint32
	Size, MaxSize, TruncateSize uint64
	ReportedSize                uint64
	TruncateSeq, TimeWarpSeq    uint32
	MTime, ATime, CTime, BTime  cephmsgr.MDSTime
	ChangeAttr, XattrVersion    uint64
	FscryptAuth, FscryptFile    []byte
	Open                        map[int]int
	Released                    bool
}

func (c *cephfsCap) fileWanted() uint32 {
	var want uint32
	for mode, refs := range c.Open {
		if refs > 0 {
			want |= cephCapsForMode(mode)
		}
	}
	if want&cephCapFileBuffer != 0 {
		want |= cephCapFileExcl
	}
	return want
}

// cephfsCapRecord is one agent observation or action, in local order.
type cephfsCapRecord struct {
	Order        int
	At, Received time.Time `json:",omitempty"`
	Kind         string
	Ino          uint64 `json:",omitempty"`
	CapID        uint64 `json:",omitempty"`
	Op           uint32
	Seq          uint32 `json:",omitempty"`
	IssueSeq     uint32 `json:",omitempty"`
	MSeq         uint32 `json:",omitempty"`
	Caps         string `json:",omitempty"`
	Wanted       string `json:",omitempty"`
	Before       string `json:",omitempty"`
	After        string `json:",omitempty"`
	Revoked      string `json:",omitempty"`
	PushSequence uint64 `json:",omitempty"`
	Note         string `json:",omitempty"`
	Error        string `json:",omitempty"`
}

// cephfsHold delays the acknowledgement of the first revoke on one inode,
// simulating a native client still using the revoked caps for that long.
type cephfsHold struct {
	Ino                     uint64
	Delay                   time.Duration
	Received, Acked         time.Time
	RevokeSeq               uint32
	Before, Revoke, AckCaps uint32
	AckWanted               uint32
	Sent                    bool
	Error                   string
}

type cephfsCapAgent struct {
	s       *cephmsgr.MDSSession
	ctx     context.Context
	mu      sync.Mutex
	caps    map[uint64]*cephfsCap
	pending []cephmsgr.MDSSessionEvent
	holds   map[uint64]*cephfsHold
	held    map[uint64]bool
	barrier uint32
	records []cephfsCapRecord
	errs    []string
	done    chan struct{}
	endErr  error
}

func startCephFSCapAgent(ctx context.Context, s *cephmsgr.MDSSession) *cephfsCapAgent {
	a := &cephfsCapAgent{s: s, ctx: ctx, caps: map[uint64]*cephfsCap{}, holds: map[uint64]*cephfsHold{}, held: map[uint64]bool{}, done: make(chan struct{})}
	go a.run()
	return a
}

func (a *cephfsCapAgent) run() {
	defer close(a.done)
	for {
		event, err := a.s.Next(a.ctx)
		if err != nil {
			a.mu.Lock()
			a.endErr = err
			a.mu.Unlock()
			return
		}
		a.mu.Lock()
		a.handleLocked(event)
		a.mu.Unlock()
	}
}

func (a *cephfsCapAgent) recordLocked(r cephfsCapRecord) {
	r.Order, r.At = len(a.records), time.Now()
	r.PushSequence = a.s.Status().PushSequence
	a.records = append(a.records, r)
}

func (a *cephfsCapAgent) failLocked(format string, args ...any) {
	a.errs = append(a.errs, fmt.Sprintf(format, args...))
}

func (a *cephfsCapAgent) handleLocked(e cephmsgr.MDSSessionEvent) {
	switch {
	case e.Caps != nil:
		a.handleCapsLocked(e)
	case e.Lease != nil:
		l := e.Lease
		a.recordLocked(cephfsCapRecord{Kind: "lease", Received: e.Received, Ino: l.Ino, Op: uint32(l.Action), Seq: l.Seq, Note: fmt.Sprintf("mask=%d dentry=%q", l.Mask, l.DentryName)})
		if l.Action != cephmsgr.MDSLeaseRevoke {
			a.failLocked("unexpected MDS lease action %d", l.Action)
			return
		}
		// handle_lease: every revoke is answered with a RELEASE echoing it.
		release := cephmsgr.MDSLease{Action: cephmsgr.MDSLeaseRelease, Mask: l.Mask, Ino: l.Ino, First: l.First, Last: l.Last, Seq: l.Seq, DentryName: l.DentryName}
		err := a.send(func(ctx context.Context) error { return a.s.SendLease(ctx, release) })
		r := cephfsCapRecord{Kind: "lease-release", Ino: l.Ino, Seq: l.Seq, Note: fmt.Sprintf("dentry=%q", l.DentryName)}
		if err != nil {
			r.Error = err.Error()
			a.failLocked("lease release: %v", err)
		}
		a.recordLocked(r)
	case e.Snap != nil:
		a.recordLocked(cephfsCapRecord{Kind: "snap", Received: e.Received, Op: e.Snap.Op, Note: fmt.Sprintf("split=%d inos=%d realms=%d trace=%d", e.Snap.Split, len(e.Snap.SplitInos), len(e.Snap.SplitRealms), len(e.Snap.Trace))})
	case e.Quota != nil:
		a.recordLocked(cephfsCapRecord{Kind: "quota", Received: e.Received, Ino: e.Quota.Ino})
	default:
		r := cephfsCapRecord{Kind: "session", Received: e.Received, Op: e.Op, Seq: uint32(e.Sequence)}
		switch e.Op {
		case cephmsgr.MDSSessionOpFlushMessage:
			// Native clients flush cap releases, then acknowledge.
			if err := a.send(func(ctx context.Context) error { return a.s.AcknowledgeFlush(ctx, e.Sequence) }); err != nil {
				r.Error = err.Error()
				a.failLocked("flush acknowledgement: %v", err)
			}
		case cephmsgr.MDSSessionOpRecallState:
			r.Note = fmt.Sprintf("recall max_caps=%d max_leases=%d", e.MaxCaps, e.MaxLeases)
		default:
			a.failLocked("unexpected MDS session event op %d", e.Op)
		}
		a.recordLocked(r)
	}
}

func (a *cephfsCapAgent) send(f func(context.Context) error) error {
	ctx, stop := context.WithTimeout(a.ctx, 10*time.Second)
	defer stop()
	return f(ctx)
}

func (a *cephfsCapAgent) handleCapsLocked(e cephmsgr.MDSSessionEvent) {
	m := e.Caps
	if m.OSDEpochBarrier > a.barrier {
		a.barrier = m.OSDEpochBarrier
	}
	c := a.caps[m.Ino]
	r := cephfsCapRecord{Kind: "caps", Received: e.Received, Ino: m.Ino, CapID: m.CapID, Op: m.Op, Seq: m.Seq, IssueSeq: m.IssueSeq, MSeq: m.MigrateSeq, Caps: cephCapString(m.Caps), Wanted: cephCapString(m.Wanted)}
	switch m.Op {
	case cephmsgr.MDSCapOpGrant, cephmsgr.MDSCapOpRevoke, cephmsgr.MDSCapOpImport:
		if c != nil && c.Released {
			// The inode left the cache: release again, as handle_caps does.
			r.Note = "released inode: immediate cap release with the message sequence"
			item := cephmsgr.MDSCapItem{Ino: m.Ino, CapID: m.CapID, MigrateSeq: m.MigrateSeq, IssueSeq: m.Seq}
			if err := a.send(func(ctx context.Context) error { return a.s.ReleaseCaps(ctx, []cephmsgr.MDSCapItem{item}, a.barrier) }); err != nil {
				r.Error = err.Error()
			}
			a.recordLocked(r)
			return
		}
		if c == nil || c.CapID == 0 {
			// The reply carrying this cap was not applied yet; its order
			// relative to pushes is only known from receive timestamps.
			r.Note = "deferred until the reply trace introducing the cap is applied"
			a.pending = append(a.pending, e)
			a.recordLocked(r)
			return
		}
	}
	if c == nil {
		r.Note = "no cap for this inode; dropped as natively"
		a.recordLocked(r)
		return
	}
	switch m.Op {
	case cephmsgr.MDSCapOpGrant, cephmsgr.MDSCapOpRevoke:
		a.grantLocked(c, e, r)
	case cephmsgr.MDSCapOpTrunc:
		c.Size, c.TruncateSeq, c.TruncateSize = m.Size, m.TruncateSeq, m.TruncateSize
		a.recordLocked(r)
	case cephmsgr.MDSCapOpFlushAck, cephmsgr.MDSCapOpFlushSnapAck:
		r.Note = "flush acknowledgement without a flush from this client"
		a.recordLocked(r)
		a.failLocked("unexpected flush acknowledgement on %d", m.Ino)
	default:
		r.Note = "export/import on a single active rank"
		a.recordLocked(r)
		a.failLocked("unexpected cap operation %d on %d", m.Op, m.Ino)
	}
}

// grantLocked restates handle_cap_grant for a client that uses nothing.
func (a *cephfsCapAgent) grantLocked(c *cephfsCap, e cephmsgr.MDSSessionEvent, r cephfsCapRecord) {
	m := e.Caps
	newCaps := m.Caps
	before := c.Issued
	if m.IssueSeq != c.IssueSeq {
		r.Note = fmt.Sprintf("message issue_seq %d differs from tracked %d", m.IssueSeq, c.IssueSeq)
	}
	c.Seq = m.Seq
	implemented := c.Implemented
	if newCaps&cephCapAuthShared != 0 && implemented&cephCapAuthExcl == 0 {
		c.Mode, c.UID, c.GID, c.BTime = m.Mode, m.UID, m.GID, m.BTime
	}
	if newCaps&cephCapLinkShared != 0 && implemented&cephCapLinkExcl == 0 {
		c.NLink = m.NLink
	}
	if implemented&cephCapXattrExcl == 0 && len(m.Xattrs) != 0 && m.XattrVersion > c.XattrVersion {
		c.XattrVersion = m.XattrVersion
	}
	if newCaps&cephCapAnyRD != 0 {
		c.MTime, c.ATime, c.CTime, c.TimeWarpSeq = m.MTime, m.ATime, m.CTime, m.TimeWarpSeq
	}
	if newCaps&(cephCapAnyFileRD|cephCapAnyFileWR) != 0 {
		c.Size, c.TruncateSeq, c.TruncateSize = m.Size, m.TruncateSeq, m.TruncateSize
	}
	if newCaps&cephCapAnyFileWR != 0 && m.MaxSize != c.MaxSize {
		c.MaxSize = m.MaxSize
	}
	if m.ChangeAttr > c.ChangeAttr {
		c.ChangeAttr = m.ChangeAttr
	}
	check := false
	revoked := c.Issued &^ newCaps
	switch {
	case revoked != 0:
		c.Issued, c.Implemented = newCaps, c.Implemented|newCaps
		c.Wanted, check = 0, true
	case c.Issued == newCaps:
	default:
		c.Issued, c.Implemented = newCaps, c.Implemented|newCaps
	}
	if !check && m.Op == cephmsgr.MDSCapOpRevoke {
		c.Wanted, check = 0, true
	}
	r.Before, r.After, r.Revoked = cephCapString(before), cephCapString(c.Issued), cephCapString(revoked)
	a.recordLocked(r)
	if !check {
		return
	}
	if hold := a.holds[c.Ino]; hold != nil && hold.Received.IsZero() && m.Op == cephmsgr.MDSCapOpRevoke {
		hold.Received, hold.RevokeSeq, hold.Before, hold.Revoke = e.Received, m.Seq, before, newCaps
		a.held[c.Ino] = true
		time.AfterFunc(hold.Delay, func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			delete(a.held, c.Ino)
			msg, sent, err := a.checkCapsLocked(c, "held revoke acknowledgement")
			hold.Acked, hold.Sent = time.Now(), sent
			hold.AckCaps, hold.AckWanted = msg.Caps, msg.Wanted
			if err != nil {
				hold.Error = err.Error()
			}
		})
		return
	}
	if a.held[c.Ino] {
		// The held acknowledgement reports the latest state when it fires.
		return
	}
	a.checkCapsLocked(c, "revoke acknowledgement")
}

// checkCapsLocked restates check_caps with CHECK_CAPS_NODELAY followed by
// send_cap, for a single auth cap with nothing used, dirty or flushing.
func (a *cephfsCapAgent) checkCapsLocked(c *cephfsCap, reason string) (cephmsgr.MDSCaps, bool, error) {
	wanted := c.fileWanted()
	var used uint32
	retain := wanted | cephCapPin
	if c.NLink > 0 {
		if wanted != 0 {
			retain |= cephCapAny
		} else {
			retain |= cephCapAnyShared
			if c.MaxSize == 0 {
				retain |= cephCapAnyRD
			}
		}
	}
	revoking := c.Implemented &^ c.Issued
	approaching := c.Issued&cephCapFileWR != 0 && (c.Size >= c.MaxSize || c.MaxSize > c.ReportedSize && c.Size<<1 >= c.MaxSize+c.ReportedSize)
	switch {
	case approaching, revoking != 0 && revoking&used == 0, wanted&^(c.Wanted|c.Issued) != 0:
	case c.Issued&^retain == 0:
		return cephmsgr.MDSCaps{}, false, nil
	}
	retain &^= revoking
	before := c.Issued
	c.Issued &= retain
	c.Implemented &= c.Issued | used
	c.Wanted = wanted
	maxSize := c.MaxSize
	if wanted&cephCapAnyFileWR != 0 {
		// m->max_size = in->wanted_max_size, never raised by this client.
		maxSize = 0
	}
	c.ReportedSize = c.Size
	msg := cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpUpdate, Ino: c.Ino, CapID: c.CapID, Seq: c.Seq, IssueSeq: c.IssueSeq, Caps: c.Implemented, Wanted: wanted, MigrateSeq: c.MSeq,
		UID: c.UID, GID: c.GID, Mode: c.Mode, NLink: c.NLink, Size: c.Size, MaxSize: maxSize, TruncateSize: c.TruncateSize, TruncateSeq: c.TruncateSeq,
		MTime: c.MTime, ATime: c.ATime, CTime: c.CTime, TimeWarpSeq: c.TimeWarpSeq, OSDEpochBarrier: a.barrier, CallerUID: ^uint32(0), CallerGID: ^uint32(0),
		BTime: c.BTime, ChangeAttr: c.ChangeAttr, Files: -1, Subdirs: -1, FscryptAuth: slices.Clone(c.FscryptAuth), FscryptFile: slices.Clone(c.FscryptFile)}
	err := a.send(func(ctx context.Context) error { return a.s.SendCaps(ctx, msg, 0) })
	r := cephfsCapRecord{Kind: "update", Ino: c.Ino, CapID: c.CapID, Op: msg.Op, Seq: msg.Seq, IssueSeq: msg.IssueSeq, MSeq: msg.MigrateSeq, Caps: cephCapString(msg.Caps), Wanted: cephCapString(wanted),
		Before: cephCapString(before), After: cephCapString(c.Issued), Note: reason}
	if err != nil {
		r.Error = err.Error()
		a.failLocked("cap update on %d: %v", c.Ino, err)
	}
	a.recordLocked(r)
	return msg, err == nil, err
}

// applyStatLocked restates add_update_inode/add_update_cap for one InodeStat.
func (a *cephfsCapAgent) applyStatLocked(st *cephmsgr.MDSInodeStat, source string) {
	c := a.caps[st.Ino]
	if c == nil {
		c = &cephfsCap{Ino: st.Ino, Open: map[int]int{}}
		a.caps[st.Ino] = c
	}
	c.Dir = st.Mode&0o170000 == 0o040000
	c.UID, c.GID, c.Mode, c.NLink = st.UID, st.GID, st.Mode, st.NLink
	c.Size, c.MaxSize, c.TruncateSize, c.TruncateSeq = st.Size, st.MaxSize, st.TruncateSize, st.TruncateSeq
	c.MTime, c.ATime, c.CTime, c.BTime, c.TimeWarpSeq, c.ChangeAttr = st.MTime, st.ATime, st.CTime, st.BTime, st.TimeWarpSeq, st.ChangeAttr
	c.FscryptAuth, c.FscryptFile = slices.Clone(st.FscryptAuth), slices.Clone(st.FscryptFile)
	if st.XattrVersion > c.XattrVersion && len(st.Xattrs) != 0 {
		c.XattrVersion = st.XattrVersion
	}
	if st.Cap.Caps == 0 {
		return
	}
	issued, seq, mseq := st.Cap.Caps, st.Cap.Seq, st.Cap.MSeq
	before := c.Issued
	if c.CapID != 0 && !c.Released && !cephSeqAfter(seq, c.Seq) {
		// A push newer than this reply was already handled.
		seq, mseq, issued = c.Seq, c.MSeq, issued|c.Issued
	}
	if c.Released {
		c.Implemented, c.Wanted, c.Released = 0, 0, false
	}
	c.CapID, c.Issued, c.Implemented = st.Cap.CapID, issued, c.Implemented|issued
	if cephSeqAfter(mseq, c.MSeq) {
		c.Wanted = st.Cap.Wanted
	} else {
		c.Wanted |= st.Cap.Wanted
	}
	c.Seq, c.IssueSeq, c.MSeq, c.Realm, c.Flags = seq, seq, mseq, st.Cap.Realm, st.Cap.Flags
	a.recordLocked(cephfsCapRecord{Kind: "reply-cap", Ino: c.Ino, CapID: c.CapID, Seq: seq, IssueSeq: seq, MSeq: mseq, Caps: cephCapString(st.Cap.Caps), Wanted: cephCapString(st.Cap.Wanted),
		Before: cephCapString(before), After: cephCapString(c.Issued), Note: source})
}

// applyReply applies the first reply of a request (native insert_trace runs
// only once per request) and then any push deferred for its inodes.
func (a *cephfsCapAgent) applyReply(name string, r cephmsgr.MDSReply) (cephmsgr.MDSTrace, *cephmsgr.MDSReaddir, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var trace cephmsgr.MDSTrace
	var err error
	if r.IsDentry || r.IsTarget {
		if trace, err = r.DecodeTrace(); err != nil {
			return trace, nil, err
		}
		if trace.DirInode != nil {
			a.applyStatLocked(trace.DirInode, name+" dir")
		}
		if trace.Target != nil {
			a.applyStatLocked(trace.Target, name+" target")
		}
	}
	var listing *cephmsgr.MDSReaddir
	if r.Op == cephmsgr.MDSOpReaddir && r.Result == 0 {
		decoded, err := r.DecodeReaddir()
		if err != nil {
			return trace, nil, err
		}
		listing = &decoded
		for i := range decoded.Entries {
			a.applyStatLocked(&decoded.Entries[i].Inode, name+" entry "+decoded.Entries[i].Name)
		}
	}
	pending := a.pending
	a.pending = nil
	for _, e := range pending {
		if c := a.caps[e.Caps.Ino]; c != nil && c.CapID != 0 {
			a.handleCapsLocked(e)
		} else {
			a.pending = append(a.pending, e)
		}
	}
	return trace, listing, nil
}

// open and close restate get_open_ref/put_open_ref. Closing runs check_caps
// at once, where the native client sends the same UPDATE after its
// client_caps_release_delay.
func (a *cephfsCapAgent) open(ino uint64, mode int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.caps[ino]
	if c == nil {
		c = &cephfsCap{Ino: ino, Open: map[int]int{}}
		a.caps[ino] = c
	}
	c.Open[mode]++
}

func (a *cephfsCapAgent) close(ino uint64, mode int) (cephmsgr.MDSCaps, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.caps[ino]
	if c == nil || c.Open[mode] == 0 {
		return cephmsgr.MDSCaps{}, false, fmt.Errorf("inode %d is not open in mode %d", ino, mode)
	}
	c.Open[mode]--
	if c.CapID == 0 || c.Released {
		return cephmsgr.MDSCaps{}, false, nil
	}
	return a.checkCapsLocked(c, fmt.Sprintf("close mode %d", mode))
}

func (a *cephfsCapAgent) holdRevoke(ino uint64, delay time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.holds[ino] = &cephfsHold{Ino: ino, Delay: delay}
}

func (a *cephfsCapAgent) hold(ino uint64) cephfsHold {
	a.mu.Lock()
	defer a.mu.Unlock()
	if h := a.holds[ino]; h != nil {
		return *h
	}
	return cephfsHold{}
}

func (a *cephfsCapAgent) cap(ino uint64) (cephfsCap, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.caps[ino]
	if c == nil {
		return cephfsCap{}, false
	}
	copied := *c
	copied.Open = map[int]int{}
	for k, v := range c.Open {
		copied.Open[k] = v
	}
	return copied, true
}

// releaseAll restates remove_cap(queue_release) for every held cap.
func (a *cephfsCapAgent) releaseAll() ([]cephmsgr.MDSCapItem, uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var items []cephmsgr.MDSCapItem
	for _, c := range a.caps {
		if c.CapID == 0 || c.Released {
			continue
		}
		items = append(items, cephmsgr.MDSCapItem{Ino: c.Ino, CapID: c.CapID, MigrateSeq: c.MSeq, IssueSeq: c.IssueSeq})
		c.Released = true
		a.recordLocked(cephfsCapRecord{Kind: "release", Ino: c.Ino, CapID: c.CapID, IssueSeq: c.IssueSeq, MSeq: c.MSeq, Caps: cephCapString(c.Issued)})
	}
	slices.SortFunc(items, func(x, y cephmsgr.MDSCapItem) int { return cmp.Compare(x.Ino, y.Ino) })
	return items, a.barrier
}

type cephfsAgentSnapshot struct {
	Records  []cephfsCapRecord
	Caps     []cephfsCap
	Pending  int
	Errors   []string
	Holds    []cephfsHold
	Barrier  uint32
	EndError string `json:",omitempty"`
}

func (a *cephfsCapAgent) snapshot() cephfsAgentSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := cephfsAgentSnapshot{Records: slices.Clone(a.records), Pending: len(a.pending), Errors: slices.Clone(a.errs), Barrier: a.barrier}
	for _, c := range a.caps {
		s.Caps = append(s.Caps, *c)
	}
	slices.SortFunc(s.Caps, func(x, y cephfsCap) int { return cmp.Compare(x.Ino, y.Ino) })
	for _, h := range a.holds {
		s.Holds = append(s.Holds, *h)
	}
	if a.endErr != nil {
		s.EndError = a.endErr.Error()
	}
	return s
}

func TestCephFSCapAgentHelpers(t *testing.T) {
	for caps, want := range map[uint32]string{0: "-", 989: "pAsxLsXsxFsx", cephCapsForMode(cephFileModeRD): "pFscr", cephCapsForMode(cephFileModeRDWR): "pAsxXsxFsxcrwb",
		cephCapAnyShared: "AsLsXsFs", cephCapAny: "pAsxLsxXsxFsxcrwbl"} {
		if got := cephCapString(caps); got != want {
			t.Fatalf("ccap_string(%#x) = %q, want %q", caps, got, want)
		}
	}
	if !cephSeqAfter(1, 0) || cephSeqAfter(0, 1) || !cephSeqAfter(0, ^uint32(0)) || cephSeqAfter(5, 5) {
		t.Fatal("ceph_seq_cmp wraparound order differs")
	}
	c := &cephfsCap{Open: map[int]int{cephFileModeRD: 1, cephFileModeWR: 0}}
	if c.fileWanted() != cephCapsForMode(cephFileModeRD) {
		t.Fatal("closed modes contributed to wanted caps")
	}
	c.Open[cephFileModeWR] = 1
	if c.fileWanted()&cephCapFileExcl == 0 {
		t.Fatal("buffered writes must want FILE_EXCL")
	}
	dir := &cephmsgr.MDSInodeStat{Mode: 0o40755, NLink: 1, Subdirs: 3}
	file := &cephmsgr.MDSInodeStat{Mode: 0o100644, NLink: 2}
	unlinked := &cephmsgr.MDSInodeStat{Mode: 0o40755, NLink: 0}
	if nativeStatxNLink(dir) != 5 || nativeStatxNLink(file) != 2 || nativeStatxNLink(unlinked) != 0 {
		t.Fatal("native statx nlink presentation differs")
	}
}

package integration_test

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Test-only consumer capability agent for live CephFS acceptance. The product
// delivers MDS pushes and sends client messages only when asked; this agent
// plays the native client's part so native LibCephFS oracles never wait on an
// unanswered revoke. It is an independent restatement of the pinned
// client/Client.cc (add_update_cap, handle_caps, handle_cap_grant, check_caps,
// send_cap, remove_cap and handle_lease at v20.2.4; LGPL-2.1, no code
// copied), reduced to a client that holds no file data: nothing is ever used,
// dirty or flushing, and directories are never marked complete. It also
// restates handle_snap (realm moves on SPLIT), handle_quota, handle_session
// RECALL_STATE (trim_caps) and FLUSHMSG, counting every capability, lease,
// snapshot and quota push as got_mds_push does. It records a lease RENEW
// answer (handle_lease updates the dentry's lease sequence and expiry), a
// FORCE_RO session push (force_session_readonly; nothing is sent) and the
// FLUSHSNAP_ACK of a cap snap flush the test sent itself. Agents of one client's
// sessions with several ranks share one lock and restate handle_cap_export
// and handle_cap_import between them (cephfsRankedAgents).
//
// For the data acceptance a file the test writes through this client is
// registered as dirty (cephfsDirtyFile): the agent then restates the
// writer's part of check_caps and send_cap (dirty Fw flushed with a flush TID
// and snap follows, max_size requests from get_caps, FILE_BUFFER in use while
// buffered data is unwritten), handle_cap_grant's buffered-data flush on a
// revoke of Fb, handle_cap_flush_ack, handle_cap_trunc, and
// update_inode_file_size/update_inode_file_time, so a reply or push never
// overwrites the writer's newer size or times. A buffered writeback that does
// not complete is latched (cephfsWritebackLatch): the consumer, not the
// agent, decides whether the data is evicted or explicitly recovered.

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

	// CEPH_CAP_FLAG_RELEASE in a cap peer asks the client to release the
	// peer's cap.
	cephCapFlagRelease = 2
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
	// ParentIno and Name are the first known dentry, as Inode::make_short_path
	// uses for reconnect; zero when only the inode is known.
	ParentIno uint64 `json:",omitempty"`
	Name      string `json:",omitempty"`
	// Added orders caps as the native session cap list does (a cap is
	// appended when it is added or re-added after a release).
	Added uint64 `json:",omitempty"`
	// Quota is the latest MClientQuota for this inode (handle_quota).
	Quota *cephmsgr.MDSQuota `json:",omitempty"`
	// Exported reports that this rank's cap was removed by a cap migration
	// (EXPORT, or IMPORT on another rank naming it as the peer); the inode
	// stays known, as in the native inode cache.
	Exported bool `json:",omitempty"`
	// WantedMaxSize and RequestedMaxSize restate the inode's wanted and
	// requested max_size of get_caps and send_cap; InlineVersion is the
	// reply's inline version a dirty Fw flush reports.
	WantedMaxSize    uint64 `json:",omitempty"`
	RequestedMaxSize uint64 `json:",omitempty"`
	InlineVersion    uint64 `json:",omitempty"`
}

// cephfsTimeAfter orders two MDS times.
func cephfsTimeAfter(a, b cephmsgr.MDSTime) bool {
	return a.Seconds > b.Seconds || a.Seconds == b.Seconds && a.Nanoseconds > b.Nanoseconds
}

// updateFileSize restates update_inode_file_size: a size from the MDS is
// taken only with a newer truncate sequence or when it grows the file; the
// truncate size follows a sequence that is not older.
func (c *cephfsCap) updateFileSize(size uint64, truncateSeq uint32, truncateSize uint64) {
	if truncateSeq > c.TruncateSeq || truncateSeq == c.TruncateSeq && size > c.Size {
		c.Size, c.ReportedSize = size, size
		c.TruncateSeq = truncateSeq
	}
	if truncateSeq >= c.TruncateSeq && c.TruncateSize != truncateSize {
		c.TruncateSize = truncateSize
	}
}

// updateFileTime restates update_inode_file_time: with write, buffer or
// exclusive caps (or dirty state) times only move forward; otherwise the
// MDS's times are taken.
func (c *cephfsCap) updateFileTime(issued, timeWarpSeq uint32, ctime, mtime, atime cephmsgr.MDSTime) {
	if issued&(cephCapFileExcl|cephCapFileWR|cephCapFileBuffer|cephCapAuthExcl|cephCapXattrExcl) != 0 {
		if cephfsTimeAfter(ctime, c.CTime) {
			c.CTime = ctime
		}
		switch {
		case timeWarpSeq > c.TimeWarpSeq:
			c.MTime, c.ATime, c.TimeWarpSeq = mtime, atime, timeWarpSeq
		case timeWarpSeq == c.TimeWarpSeq:
			if cephfsTimeAfter(mtime, c.MTime) {
				c.MTime = mtime
			}
			if cephfsTimeAfter(atime, c.ATime) {
				c.ATime = atime
			}
		}
		return
	}
	if timeWarpSeq >= c.TimeWarpSeq {
		c.CTime, c.MTime, c.ATime, c.TimeWarpSeq = ctime, mtime, atime, timeWarpSeq
	}
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
	Arrival      uint64    `json:",omitempty"`
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

// cephfsLeaseEvent is one MClientLease REVOKE and the RELEASE answering it.
type cephfsLeaseEvent struct {
	Arrival      uint64
	Received     time.Time
	PushesBefore uint64
	Revoke       cephmsgr.MDSLease
	Held         time.Duration `json:",omitempty"`
	Released     time.Time
	Release      cephmsgr.MDSLease
	Sent         bool
	Error        string `json:",omitempty"`
}

// cephfsLeaseRenewal is one MClientLease RENEW answering the client's RENEW.
type cephfsLeaseRenewal struct {
	Arrival      uint64
	Received     time.Time
	PushesBefore uint64
	Lease        cephmsgr.MDSLease
}

// cephfsFlushSnapAck is one FLUSHSNAP_ACK matched to a FLUSHSNAP the test
// registered (inode and flush TID).
type cephfsFlushSnapAck struct {
	Arrival     uint64
	Received    time.Time
	Transaction uint64
	Caps        cephmsgr.MDSCaps
}

// cephfsSnapEvent is one MClientSnap with its decoded SnapRealmInfoNew trace
// and the cached inodes moved into the split realm.
type cephfsSnapEvent struct {
	Arrival     uint64
	Received    time.Time
	Op          uint32
	Split       uint64
	SplitInos   []uint64 `json:",omitempty"`
	SplitRealms []uint64 `json:",omitempty"`
	TraceBytes  int
	Realms      []cephmsgr.MDSSnapRealm
	Moved       []uint64 `json:",omitempty"`
	Error       string   `json:",omitempty"`
}

// cephfsQuotaEvent is one MClientQuota; Known reports a cached inode.
type cephfsQuotaEvent struct {
	Arrival  uint64
	Received time.Time
	Quota    cephmsgr.MDSQuota
	Known    bool
}

// cephfsRecallEvent is one RECALL_STATE and the trim that answered it.
type cephfsRecallEvent struct {
	Arrival            uint64
	Received           time.Time
	MaxCaps, MaxLeases uint32
	Before, After      int
	Released           []cephmsgr.MDSCapItem `json:",omitempty"`
	Barrier            uint32
	Sent               time.Time
	Error              string `json:",omitempty"`
}

// cephfsFlushEvent is one FLUSHMSG: the MDS's push sequence at sending
// (Sequence) against the pushes this agent counted before it, and the ack.
type cephfsFlushEvent struct {
	Arrival     uint64
	Received    time.Time
	Sequence    uint64
	AgentPushes uint64
	Acked       time.Time
	// HeldFor is set when the test held this acknowledgement (holdNextFlush).
	HeldFor time.Duration `json:",omitempty"`
	Error   string        `json:",omitempty"`
}

// cephfsFlushHold delays the acknowledgement of the next FLUSHMSG until the
// test releases it (bounded). Received closes when that FLUSHMSG arrives.
type cephfsFlushHold struct {
	received, release chan struct{}
	limit             time.Duration
}

// cephfsDirtyFile is the write state of one file the test wrote through this
// client: dirty metadata not yet flushed (Dirty, as caps_dirty), metadata
// being flushed (Flushing, as flushing_caps) and, while Buffered, data held
// as the native ObjectCacher holds it (FILE_BUFFER in use). WriteBack writes
// the buffered data to the OSDs; it runs without the agent lock after Hold.
// A writeback that does not complete sets Latch (mds_cephfs_writeback_test.go):
// the data stays buffered and only an explicit decision resolves it.
type cephfsDirtyFile struct {
	Ino             uint64
	Dirty, Flushing uint32
	Follows         uint64
	Buffered        bool
	Hold            time.Duration
	WriteBack       func() error `json:"-"`
	writing         bool
	Latch           *cephfsWritebackLatch  `json:",omitempty"`
	Resolved        []cephfsWritebackLatch `json:",omitempty"`
	Events          []cephfsDirtyEvent
}

// cephfsDirtyEvent is one step of a dirty file: a revoke received, a cap
// message sent (with the flush TID when it flushed dirty metadata), the
// buffered-data writeback and the FLUSH_ACK.
type cephfsDirtyEvent struct {
	At      time.Time
	Arrival uint64 `json:",omitempty"`
	Kind    string
	Seq     uint32 `json:",omitempty"`
	Caps    string `json:",omitempty"`
	Revoked string `json:",omitempty"`
	Dirty   string `json:",omitempty"`
	TID     uint64 `json:",omitempty"`
	Size    uint64 `json:",omitempty"`
	Note    string `json:",omitempty"`
	Error   string `json:",omitempty"`
}

// cephfsFlushAck is one FLUSH_ACK matched to a flush this agent sent.
type cephfsFlushAck struct {
	Arrival     uint64
	Received    time.Time
	Transaction uint64
	Caps        cephmsgr.MDSCaps
}

// cephfsTruncEvent is one TRUNC push and the size state around it.
type cephfsTruncEvent struct {
	Arrival           uint64
	Received          time.Time
	Caps              cephmsgr.MDSCaps
	SizeBefore        uint64
	TruncateSeqBefore uint32
	SizeAfter         uint64
	TruncateSeqAfter  uint32
}

// cephfsMaxSizeEvent is one GRANT or REVOKE that changed max_size.
type cephfsMaxSizeEvent struct {
	Arrival  uint64
	Received time.Time
	Ino      uint64
	Op       uint32
	Seq      uint32
	Before   uint64
	MaxSize  uint64
	Size     uint64
	Caps     string
}

// cephfsMigrationEvent is one EXPORT or IMPORT of a multi-rank client and
// what the agent did with it.
type cephfsMigrationEvent struct {
	Rank          int32
	Arrival       uint64
	Received      time.Time
	PushSequence  uint64
	Op            uint32
	Ino, CapID    uint64
	Seq, IssueSeq uint32
	MSeq          uint32
	Caps, Wanted  string
	Realm         uint64
	Peer          cephmsgr.MDSCapPeer
	Action        string
	Released      bool   `json:",omitempty"`
	Error         string `json:",omitempty"`
}

type cephfsCapAgent struct {
	s       *cephmsgr.MDSSession
	ctx     context.Context
	mu      *sync.Mutex
	caps    map[uint64]*cephfsCap
	pending []cephmsgr.MDSSessionEvent
	holds   map[uint64]*cephfsHold
	held    map[uint64]bool
	barrier uint32
	records []cephfsCapRecord
	errs    []string
	done    chan struct{}
	endErr  error
	// pushes counts capability, lease, snapshot and quota pushes handled.
	pushes uint64
	// added numbers caps in the order they were added (cephfsCap.Added).
	added      uint64
	realms     map[uint64]cephmsgr.MDSSnapRealm
	leaseHolds map[string]time.Duration
	leases     []cephfsLeaseEvent
	snaps      []cephfsSnapEvent
	quotas     []cephfsQuotaEvent
	recalls    []cephfsRecallEvent
	flushes    []cephfsFlushEvent
	flushHold  *cephfsFlushHold
	// rank and peers are set for one client's sessions with several ranks:
	// peers maps every rank to its current agent, all sharing mu.
	rank       int32
	peers      map[int32]*cephfsCapAgent
	migrations []cephfsMigrationEvent
	renewals   []cephfsLeaseRenewal
	readOnly   []time.Time
	flushSnaps map[cephfsFlushSnapKey]cephfsFlushSnapExpected // inode/follows -> expected TID and dirty mask
	capSnaps   map[cephfsFlushSnapKey]*cephfsCapSnap          // opt-in data-acceptance generations
	snapAcks   []cephfsFlushSnapAck
	// dirty holds the files the test wrote; flushing maps outstanding flush
	// TIDs (last_flush_tid numbering from 1) to their inode.
	dirty     map[uint64]*cephfsDirtyFile
	flushTID  uint64
	flushing  map[uint64]uint64
	flushAcks []cephfsFlushAck
	truncs    []cephfsTruncEvent
	maxSizes  []cephfsMaxSizeEvent
	// capSend replaces the session's SendCaps in cluster-free tests.
	capSend func(context.Context, cephmsgr.MDSCaps, uint64) error
}

func newCephFSCapAgent(ctx context.Context, s *cephmsgr.MDSSession) *cephfsCapAgent {
	return &cephfsCapAgent{s: s, ctx: ctx, mu: &sync.Mutex{}, caps: map[uint64]*cephfsCap{}, holds: map[uint64]*cephfsHold{}, held: map[uint64]bool{}, done: make(chan struct{}),
		realms: map[uint64]cephmsgr.MDSSnapRealm{}, leaseHolds: map[string]time.Duration{}, dirty: map[uint64]*cephfsDirtyFile{}, flushing: map[uint64]uint64{}}
}

func startCephFSCapAgent(ctx context.Context, s *cephmsgr.MDSSession) *cephfsCapAgent {
	a := newCephFSCapAgent(ctx, s)
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
	if a.s != nil {
		r.PushSequence = a.s.Status().PushSequence
	}
	a.records = append(a.records, r)
}

func (a *cephfsCapAgent) failLocked(format string, args ...any) {
	a.errs = append(a.errs, fmt.Sprintf(format, args...))
}

func (a *cephfsCapAgent) handleLocked(e cephmsgr.MDSSessionEvent) {
	if e.Caps != nil || e.Lease != nil || e.Snap != nil || e.Quota != nil {
		// got_mds_push: every counted push, whatever it carries.
		a.pushes++
	}
	switch {
	case e.Caps != nil:
		a.handleCapsLocked(e)
	case e.Lease != nil:
		a.handleLeaseLocked(e)
	case e.Snap != nil:
		a.handleSnapLocked(e)
	case e.Quota != nil:
		q := *e.Quota
		ev := cephfsQuotaEvent{Arrival: e.Arrival, Received: e.Received, Quota: q}
		// handle_quota updates a cached inode's quota and rstat; no answer.
		if c := a.caps[q.Ino]; c != nil {
			c.Quota, ev.Known = &q, true
		}
		a.quotas = append(a.quotas, ev)
		a.recordLocked(cephfsCapRecord{Kind: "quota", Received: e.Received, Arrival: e.Arrival, Ino: q.Ino,
			Note: fmt.Sprintf("max_bytes=%d max_files=%d rbytes=%d rfiles=%d rsubdirs=%d", q.MaxBytes, q.MaxFiles, q.RBytes, q.RFiles, q.RSubdirs)})
	default:
		r := cephfsCapRecord{Kind: "session", Received: e.Received, Arrival: e.Arrival, Op: e.Op, Seq: uint32(e.Sequence)}
		switch e.Op {
		case cephmsgr.MDSSessionOpFlushMessage:
			// Native clients flush pending cap releases, then acknowledge. This
			// agent sends releases at once, so nothing is pending here.
			ev := cephfsFlushEvent{Arrival: e.Arrival, Received: e.Received, Sequence: e.Sequence, AgentPushes: a.pushes}
			if h := a.flushHold; h != nil {
				// The test holds this acknowledgement: a migration's exporter
				// waits for it with the subtree frozen. The answer follows the
				// release without the shared lock.
				a.flushHold = nil
				index, seq := len(a.flushes), e.Sequence
				a.flushes = append(a.flushes, ev)
				close(h.received)
				go func() {
					select {
					case <-h.release:
					case <-time.After(h.limit):
					}
					err := a.send(func(ctx context.Context) error { return a.s.AcknowledgeFlush(ctx, seq) })
					a.mu.Lock()
					defer a.mu.Unlock()
					f := &a.flushes[index]
					f.Acked = time.Now()
					f.HeldFor = f.Acked.Sub(f.Received)
					if err != nil {
						f.Error = err.Error()
						a.failLocked("held flush acknowledgement: %v", err)
					}
				}()
				r.Note = fmt.Sprintf("flushmsg seq=%d agent_pushes=%d held", e.Sequence, a.pushes)
				break
			}
			err := a.send(func(ctx context.Context) error { return a.s.AcknowledgeFlush(ctx, e.Sequence) })
			ev.Acked = time.Now()
			if err != nil {
				r.Error, ev.Error = err.Error(), err.Error()
				a.failLocked("flush acknowledgement: %v", err)
			}
			r.Note = fmt.Sprintf("flushmsg seq=%d agent_pushes=%d", e.Sequence, a.pushes)
			a.flushes = append(a.flushes, ev)
		case cephmsgr.MDSSessionOpForceReadOnly:
			// force_session_readonly marks the session read-only and wakes
			// writers waiting for Fw; the client sends nothing.
			a.readOnly = append(a.readOnly, e.Received)
			r.Note = "force read-only"
		case cephmsgr.MDSSessionOpRecallState:
			ev := a.trimCapsLocked(e)
			r.Note = fmt.Sprintf("recall max_caps=%d max_leases=%d caps %d->%d released=%d", e.MaxCaps, e.MaxLeases, ev.Before, ev.After, len(ev.Released))
			if ev.Error != "" {
				r.Error = ev.Error
			}
			a.recalls = append(a.recalls, ev)
		default:
			a.failLocked("unexpected MDS session event op %d", e.Op)
		}
		a.recordLocked(r)
	}
}

// handleLeaseLocked restates handle_lease: every REVOKE is answered with a
// RELEASE echoing its sequence, mask, inode, snap range and dentry name. A
// hold registered for the dentry name delays that answer once.
func (a *cephfsCapAgent) handleLeaseLocked(e cephmsgr.MDSSessionEvent) {
	l := *e.Lease
	a.recordLocked(cephfsCapRecord{Kind: "lease", Received: e.Received, Arrival: e.Arrival, Ino: l.Ino, Op: uint32(l.Action), Seq: l.Seq,
		Note: fmt.Sprintf("mask=%d dentry=%q first=%d last=%#x", l.Mask, l.DentryName, l.First, l.Last)})
	if l.Action == cephmsgr.MDSLeaseRenew {
		// handle_lease: a RENEW refreshes the dentry lease (sequence and
		// duration); nothing is answered.
		a.renewals = append(a.renewals, cephfsLeaseRenewal{Arrival: e.Arrival, Received: e.Received, PushesBefore: a.pushes - 1, Lease: l})
		return
	}
	if l.Action != cephmsgr.MDSLeaseRevoke {
		a.failLocked("unexpected MDS lease action %d", l.Action)
		return
	}
	release := cephmsgr.MDSLease{Action: cephmsgr.MDSLeaseRelease, Mask: l.Mask, Ino: l.Ino, First: l.First, Last: l.Last, Seq: l.Seq, DentryName: l.DentryName}
	a.leases = append(a.leases, cephfsLeaseEvent{Arrival: e.Arrival, Received: e.Received, PushesBefore: a.pushes - 1, Revoke: l, Release: release})
	index := len(a.leases) - 1
	answer := func() {
		ev := &a.leases[index]
		err := a.send(func(ctx context.Context) error { return a.s.SendLease(ctx, release) })
		ev.Released, ev.Sent = time.Now(), err == nil
		r := cephfsCapRecord{Kind: "lease-release", Ino: l.Ino, Seq: l.Seq, Note: fmt.Sprintf("dentry=%q", l.DentryName)}
		if err != nil {
			r.Error, ev.Error = err.Error(), err.Error()
			a.failLocked("lease release: %v", err)
		}
		a.recordLocked(r)
	}
	if delay, ok := a.leaseHolds[l.DentryName]; ok {
		delete(a.leaseHolds, l.DentryName)
		a.leases[index].Held = delay
		time.AfterFunc(delay, func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			answer()
		})
		return
	}
	answer()
}

// handleSnapLocked restates handle_snap: on SPLIT the first realm of the
// trace must be the split realm, and cached inodes listed in split_inos move
// into it unless they already belong to a newer realm; every realm of the
// trace is then recorded (update_snap_trace). The data acceptance opts into
// capturing dirty cap-snap generations with their pre-update contexts; it
// drains their writeback and sends FLUSHSNAP explicitly. Nothing is sent here.
func (a *cephfsCapAgent) handleSnapLocked(e cephmsgr.MDSSessionEvent) {
	m := e.Snap
	oldContexts := a.oldDirtySnapContextsLocked()
	ev := cephfsSnapEvent{Arrival: e.Arrival, Received: e.Received, Op: m.Op, Split: m.Split, SplitInos: slices.Clone(m.SplitInos), SplitRealms: slices.Clone(m.SplitRealms), TraceBytes: len(m.Trace)}
	realms, err := cephmsgr.MDSReply{SnapTrace: m.Trace}.DecodeSnapTrace()
	ev.Realms = realms
	switch {
	case err != nil:
		ev.Error = err.Error()
		a.failLocked("snap trace of op %d: %v", m.Op, err)
	case len(realms) == 0:
		ev.Error = "empty snap trace"
		a.failLocked("snap op %d without a realm", m.Op)
	case m.Op == cephmsgr.MDSSnapOpSplit && realms[0].Ino != m.Split:
		ev.Error = "split realm is not the first realm of the trace"
		a.failLocked("snap split %d trace starts with realm %d", m.Split, realms[0].Ino)
	case m.Op == cephmsgr.MDSSnapOpSplit:
		info := realms[0]
		for _, ino := range m.SplitInos {
			c := a.caps[ino]
			if c == nil || c.CapID == 0 || c.Released || c.Realm == info.Ino {
				continue
			}
			if old, ok := a.realms[c.Realm]; ok && old.Created > info.Created {
				continue
			}
			c.Realm = info.Ino
			ev.Moved = append(ev.Moved, ino)
		}
	}
	for _, r := range realms {
		a.realms[r.Ino] = r
	}
	if ev.Error == "" && m.Op != cephmsgr.MDSSnapOpDestroy {
		a.queueChangedCapSnapsLocked(oldContexts)
	}
	a.snaps = append(a.snaps, ev)
	a.recordLocked(cephfsCapRecord{Kind: "snap", Received: e.Received, Arrival: e.Arrival, Op: m.Op, Error: ev.Error,
		Note: fmt.Sprintf("split=%d inos=%v realms=%v trace=%d moved=%v", m.Split, m.SplitInos, m.SplitRealms, len(m.Trace), ev.Moved)})
}

// trimCapsLocked restates trim_caps for a client whose only expireable
// dentries are those of unopened regular files: walking caps in the order
// they were added, it removes (with queue_release) unopened non-root file
// caps until at most max_caps remain and sends the releases at once, where
// the native client flushes them on its next tick. Directories, the root and
// open files are kept, as their dentries or handles pin them natively.
func (a *cephfsCapAgent) trimCapsLocked(e cephmsgr.MDSSessionEvent) cephfsRecallEvent {
	ev := cephfsRecallEvent{Arrival: e.Arrival, Received: e.Received, MaxCaps: e.MaxCaps, MaxLeases: e.MaxLeases, Barrier: a.barrier}
	var live []*cephfsCap
	for _, c := range a.caps {
		if c.CapID != 0 && !c.Released {
			live = append(live, c)
		}
	}
	slices.SortFunc(live, func(x, y *cephfsCap) int { return cmp.Or(cmp.Compare(x.Added, y.Added), cmp.Compare(x.Ino, y.Ino)) })
	ev.Before, ev.After = len(live), len(live)
	for _, c := range live {
		if ev.After <= int(e.MaxCaps) {
			break
		}
		open := 0
		for _, refs := range c.Open {
			open += refs
		}
		if c.Ino == 1 || c.Dir || open != 0 {
			continue
		}
		ev.Released = append(ev.Released, cephmsgr.MDSCapItem{Ino: c.Ino, CapID: c.CapID, MigrateSeq: c.MSeq, IssueSeq: c.IssueSeq})
		c.Released = true
		ev.After--
		a.recordLocked(cephfsCapRecord{Kind: "release", Ino: c.Ino, CapID: c.CapID, IssueSeq: c.IssueSeq, MSeq: c.MSeq, Caps: cephCapString(c.Issued), Note: "trim_caps for RECALL_STATE"})
	}
	if len(ev.Released) == 0 {
		return ev
	}
	items := slices.Clone(ev.Released)
	err := a.send(func(ctx context.Context) error { return a.s.ReleaseCaps(ctx, items, a.barrier) })
	ev.Sent = time.Now()
	if err != nil {
		ev.Error = err.Error()
		a.failLocked("recall cap release: %v", err)
	}
	return ev
}

// expectFlushSnap registers a FLUSHSNAP the test is about to send.
func (a *cephfsCapAgent) expectFlushSnap(ino, follows, tid uint64, dirty uint32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.expectFlushSnapLocked(ino, follows, tid, dirty)
}

// realm returns the newest SnapRealmInfoNew seen for a realm.
func (a *cephfsCapAgent) realm(ino uint64) (cephmsgr.MDSSnapRealm, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.realms[ino]
	return r, ok
}

// holdNextFlush holds the acknowledgement of the next FLUSHMSG until release
// is called or limit elapses. received closes when that FLUSHMSG arrives.
func (a *cephfsCapAgent) holdNextFlush(limit time.Duration) (received <-chan struct{}, release func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	h := &cephfsFlushHold{received: make(chan struct{}), release: make(chan struct{}), limit: limit}
	a.flushHold = h
	var once sync.Once
	return h.received, func() { once.Do(func() { close(h.release) }) }
}

// holdLease delays the RELEASE answering the next REVOKE of one dentry name.
func (a *cephfsCapAgent) holdLease(name string, delay time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.leaseHolds[name] = delay
}

// liveCaps counts caps that are held (not released).
func (a *cephfsCapAgent) liveCaps() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, c := range a.caps {
		if c.CapID != 0 && !c.Released {
			n++
		}
	}
	return n
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
	if a.peers != nil && (m.Op == cephmsgr.MDSCapOpExport || m.Op == cephmsgr.MDSCapOpImport) {
		a.migrateLocked(e)
		return
	}
	c := a.caps[m.Ino]
	r := cephfsCapRecord{Kind: "caps", Received: e.Received, Arrival: e.Arrival, Ino: m.Ino, CapID: m.CapID, Op: m.Op, Seq: m.Seq, IssueSeq: m.IssueSeq, MSeq: m.MigrateSeq, Caps: cephCapString(m.Caps), Wanted: cephCapString(m.Wanted)}
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
		if c != nil && c.CapID == 0 && c.Exported {
			// Another rank's IMPORT or this rank's EXPORT moved the cap away:
			// the inode is known but has no cap on this rank, which
			// Client::handle_caps drops ("don't have ... cap on mds").
			r.Note = "no cap on this rank after a migration; dropped as natively"
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
	case cephmsgr.MDSCapOpImport:
		// handle_cap_import -> add_update_cap with the message's cap; no
		// peer cap exists on a single active rank.
		before := c.Issued
		a.addUpdateCapLocked(c, m.CapID, m.Caps, m.Wanted, m.Seq, m.MigrateSeq, m.Realm)
		r.Before, r.After, r.Note = cephCapString(before), cephCapString(c.Issued), fmt.Sprintf("import from peer mds %d cap %d", m.Peer.MDS, m.Peer.CapID)
		a.recordLocked(r)
	case cephmsgr.MDSCapOpExport:
		// handle_cap_export: a matching cap without a peer is removed
		// without queueing a release; anything else is ignored.
		if c.CapID == m.CapID && m.Peer.CapID == 0 {
			c.Released, r.Note = true, "export without peer: cap removed"
		} else {
			r.Note = fmt.Sprintf("export for cap %d ignored (tracked cap %d, peer cap %d)", m.CapID, c.CapID, m.Peer.CapID)
		}
		a.recordLocked(r)
	case cephmsgr.MDSCapOpTrunc:
		// handle_cap_trunc -> update_inode_file_size.
		ev := cephfsTruncEvent{Arrival: e.Arrival, Received: e.Received, Caps: *m, SizeBefore: c.Size, TruncateSeqBefore: c.TruncateSeq}
		c.updateFileSize(m.Size, m.TruncateSeq, m.TruncateSize)
		ev.SizeAfter, ev.TruncateSeqAfter = c.Size, c.TruncateSeq
		a.truncs = append(a.truncs, ev)
		r.Note = fmt.Sprintf("trunc size %d truncate_size %d truncate_seq %d -> size %d seq %d", m.Size, m.TruncateSize, m.TruncateSeq, c.Size, c.TruncateSeq)
		a.recordLocked(r)
	case cephmsgr.MDSCapOpFlushSnapAck:
		if a.consumeFlushSnapLocked(m, e.Transaction, e.Received) {
			a.snapAcks = append(a.snapAcks, cephfsFlushSnapAck{Arrival: e.Arrival, Received: e.Received, Transaction: e.Transaction, Caps: *m})
			r.Note = fmt.Sprintf("flushsnap ack tid %d follows %d dirty %s", e.Transaction, m.SnapFollows, cephCapString(m.Dirty))
			a.recordLocked(r)
			return
		}
		r.Note = "flush acknowledgement without a flush from this client"
		a.recordLocked(r)
		a.failLocked("unexpected flush acknowledgement on %d", m.Ino)
	case cephmsgr.MDSCapOpFlushAck:
		if ino, ok := a.flushing[e.Transaction]; ok && ino == m.Ino {
			// handle_cap_flush_ack: every flush of this inode up to the
			// acknowledged TID is complete.
			for tid, other := range a.flushing {
				if other == m.Ino && tid <= e.Transaction {
					delete(a.flushing, tid)
				}
			}
			a.flushAcks = append(a.flushAcks, cephfsFlushAck{Arrival: e.Arrival, Received: e.Received, Transaction: e.Transaction, Caps: *m})
			if d := a.dirty[m.Ino]; d != nil {
				if !slices.Contains(slices.Collect(maps.Values(a.flushing)), m.Ino) {
					d.Flushing = 0
				}
				d.Events = append(d.Events, cephfsDirtyEvent{At: time.Now(), Arrival: e.Arrival, Kind: "flush-ack", Seq: m.Seq, Caps: cephCapString(m.Caps), Dirty: cephCapString(m.Dirty), TID: e.Transaction})
			}
			r.Note = fmt.Sprintf("flush ack tid %d dirty %s", e.Transaction, cephCapString(m.Dirty))
			a.recordLocked(r)
			return
		}
		r.Note = "flush acknowledgement without a flush from this client"
		a.recordLocked(r)
		a.failLocked("unexpected flush acknowledgement on %d", m.Ino)
	default:
		r.Note = "unexpected cap operation"
		a.recordLocked(r)
		a.failLocked("unexpected cap operation %d on %d", m.Op, m.Ino)
	}
}

// addUpdateCapLocked restates the cap fields of add_update_cap.
func (a *cephfsCapAgent) addUpdateCapLocked(c *cephfsCap, capID uint64, issued, wanted, seq, mseq uint32, realm uint64) {
	if c.CapID == 0 || c.Released {
		a.added++
		c.Added = a.added
	}
	if c.CapID != 0 && !c.Released && !cephSeqAfter(seq, c.Seq) {
		seq, mseq, issued = c.Seq, c.MSeq, issued|c.Issued
	}
	if c.Released {
		c.Implemented, c.Wanted, c.Released = 0, 0, false
	}
	c.CapID, c.Issued, c.Implemented = capID, issued, c.Implemented|issued
	if cephSeqAfter(mseq, c.MSeq) {
		c.Wanted = wanted
	} else {
		c.Wanted |= wanted
	}
	c.Seq, c.IssueSeq, c.MSeq, c.Realm = seq, seq, mseq, realm
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
	issued := implemented
	d := a.dirty[c.Ino]
	if d != nil {
		issued |= d.Dirty | d.Flushing
	}
	if newCaps&cephCapAnyRD != 0 {
		c.updateFileTime(issued, m.TimeWarpSeq, m.CTime, m.MTime, m.ATime)
	}
	if newCaps&(cephCapAnyFileRD|cephCapAnyFileWR) != 0 {
		c.updateFileSize(m.Size, m.TruncateSeq, m.TruncateSize)
	}
	if newCaps&cephCapAnyFileWR != 0 && m.MaxSize != c.MaxSize {
		a.maxSizes = append(a.maxSizes, cephfsMaxSizeEvent{Arrival: e.Arrival, Received: e.Received, Ino: c.Ino, Op: m.Op, Seq: m.Seq, Before: c.MaxSize, MaxSize: m.MaxSize, Size: m.Size,
			Caps: cephCapString(newCaps)})
		c.MaxSize = m.MaxSize
		if c.MaxSize > c.WantedMaxSize {
			c.WantedMaxSize, c.RequestedMaxSize = 0, 0
		}
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
	if d != nil && (revoked != 0 || m.Op == cephmsgr.MDSCapOpRevoke) {
		d.Events = append(d.Events, cephfsDirtyEvent{At: time.Now(), Arrival: e.Arrival, Kind: "revoke", Seq: m.Seq, Caps: cephCapString(newCaps), Revoked: cephCapString(revoked),
			Dirty: cephCapString(d.Dirty | d.Flushing), Note: fmt.Sprintf("op %d buffered %v", m.Op, d.Buffered)})
	}
	if d != nil && d.Buffered && revoked&cephCapFileBuffer != 0 && d.Latch != nil {
		// A latched writeback is never started again by a push: the data
		// stays buffered and FILE_BUFFER stays in use until the consumer's
		// explicit decision (evictBuffered or recoverBuffered).
		d.Latch.Ignored++
		d.Events = append(d.Events, cephfsDirtyEvent{At: time.Now(), Arrival: e.Arrival, Kind: "latched-revoke", Seq: m.Seq, Revoked: cephCapString(revoked),
			Note: fmt.Sprintf("writeback latched %s; not re-run", d.Latch.Outcome)})
	}
	if d != nil && d.Buffered && !d.writing && d.Latch == nil && revoked&cephCapFileBuffer != 0 {
		// handle_cap_grant: FILE_BUFFER in use is revoked, so the buffered
		// data is flushed (_flush) and the revocation completes only when
		// that flush is done (_flushed -> put_cap_ref -> check_caps). A
		// REVOKE op still runs check_caps now: it flushes the dirty metadata
		// with FILE_BUFFER still implemented.
		d.writing = true
		if m.Op == cephmsgr.MDSCapOpRevoke {
			c.Wanted = 0
			a.checkCapsLocked(c, "revoke with buffered data: dirty metadata flush")
		}
		ino := c.Ino
		writeBack := d.WriteBack
		time.AfterFunc(d.Hold, func() {
			began := time.Now()
			err := writeBack()
			a.mu.Lock()
			defer a.mu.Unlock()
			a.finishBufferedWritebackLocked(ino, d, "revoke-callback", began, err, "buffered data written back")
		})
		return
	}
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
// send_cap, for a single auth cap. Only a file registered as dirty has
// anything used (FILE_BUFFER while buffered), dirty or flushing.
func (a *cephfsCapAgent) checkCapsLocked(c *cephfsCap, reason string) (cephmsgr.MDSCaps, bool, error) {
	msg, _, sent, err := a.checkCapsFlagsLocked(c, reason, 0)
	return msg, sent, err
}

// checkCapsFlagsLocked is checkCapsLocked with MClientCaps flags (FLAG_SYNC
// for CHECK_CAPS_SYNCHRONOUS). It also returns the flush TID when the message
// flushed dirty metadata (mark_caps_flushing), zero otherwise.
func (a *cephfsCapAgent) checkCapsFlagsLocked(c *cephfsCap, reason string, flags uint32) (cephmsgr.MDSCaps, uint64, bool, error) {
	wanted := c.fileWanted()
	var used, dirty, flushing uint32
	d := a.dirty[c.Ino]
	if d != nil {
		if d.Buffered {
			used |= cephCapFileBuffer
		}
		dirty, flushing = d.Dirty, d.Flushing
	}
	if a.snapBufferUsedLocked(c.Ino) {
		used |= cephCapFileBuffer
	}
	retain := wanted | used | cephCapPin
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
	approaching := c.Issued&cephCapFileWR != 0 && flushing&cephCapFileWR == 0 && (c.Size >= c.MaxSize || c.MaxSize > c.ReportedSize && c.Size<<1 >= c.MaxSize+c.ReportedSize)
	// get_caps raised wanted_max_size beyond max_size and what was requested.
	wantMax := c.WantedMaxSize > c.MaxSize && c.WantedMaxSize > c.RequestedMaxSize
	switch {
	case wantMax, approaching, revoking != 0 && revoking&used == 0, wanted&^(c.Wanted|c.Issued) != 0:
	case c.Issued&^retain == 0 && dirty == 0:
		return cephmsgr.MDSCaps{}, 0, false, nil
	}
	retain &^= revoking
	before := c.Issued
	c.Issued &= retain
	c.Implemented &= c.Issued | used
	c.Wanted = wanted
	// send_cap: max_size is in->max_size, replaced by wanted_max_size (and
	// recorded as requested) while the client wants a file write cap.
	maxSize := c.MaxSize
	if wanted&cephCapAnyFileWR != 0 {
		maxSize = c.WantedMaxSize
		c.RequestedMaxSize = c.WantedMaxSize
	} else {
		c.RequestedMaxSize = 0
	}
	c.ReportedSize = c.Size
	msg := cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpUpdate, Ino: c.Ino, CapID: c.CapID, Seq: c.Seq, IssueSeq: c.IssueSeq, Caps: c.Implemented, Wanted: wanted, MigrateSeq: c.MSeq,
		UID: c.UID, GID: c.GID, Mode: c.Mode, NLink: c.NLink, Size: c.Size, MaxSize: maxSize, TruncateSize: c.TruncateSize, TruncateSeq: c.TruncateSeq,
		MTime: c.MTime, ATime: c.ATime, CTime: c.CTime, TimeWarpSeq: c.TimeWarpSeq, OSDEpochBarrier: a.barrier, CallerUID: ^uint32(0), CallerGID: ^uint32(0),
		BTime: c.BTime, ChangeAttr: c.ChangeAttr, Flags: flags, Files: -1, Subdirs: -1, FscryptAuth: slices.Clone(c.FscryptAuth), FscryptFile: slices.Clone(c.FscryptFile)}
	var tid uint64
	if dirty != 0 {
		// mark_caps_flushing: a new flush TID; send_cap reports the snap
		// context sequence as follows and, for Fw, the inline version.
		a.flushTID++
		tid = a.flushTID
		a.flushing[tid] = c.Ino
		msg.Dirty, msg.SnapFollows = dirty, d.Follows
		if dirty&cephCapFileWR != 0 {
			msg.InlineVersion = c.InlineVersion
		}
		d.Flushing |= dirty
		d.Dirty = 0
	}
	msg.OldestFlushTID = a.oldestFlushTIDLocked()
	sendCaps := a.capSend
	if sendCaps == nil {
		sendCaps = a.s.SendCaps
	}
	err := a.send(func(ctx context.Context) error { return sendCaps(ctx, msg, tid) })
	r := cephfsCapRecord{Kind: "update", Ino: c.Ino, CapID: c.CapID, Op: msg.Op, Seq: msg.Seq, IssueSeq: msg.IssueSeq, MSeq: msg.MigrateSeq, Caps: cephCapString(msg.Caps), Wanted: cephCapString(wanted),
		Before: cephCapString(before), After: cephCapString(c.Issued), Note: reason}
	if tid != 0 || maxSize != 0 {
		r.Note += fmt.Sprintf(" (dirty %s flush tid %d follows %d size %d max_size %d)", cephCapString(dirty), tid, msg.SnapFollows, msg.Size, maxSize)
	}
	if err != nil {
		r.Error = err.Error()
		a.failLocked("cap update on %d: %v", c.Ino, err)
	}
	a.recordLocked(r)
	if d != nil {
		ev := cephfsDirtyEvent{At: time.Now(), Kind: "cap-update", Seq: msg.Seq, Caps: cephCapString(msg.Caps), Dirty: cephCapString(dirty), TID: tid, Size: msg.Size, Note: reason}
		if err != nil {
			ev.Error = err.Error()
		}
		d.Events = append(d.Events, ev)
	}
	return msg, tid, err == nil, err
}

// markDirty restates the writer's side of Client::_write for data the test
// wrote (or buffered) through this client: the inode's size, mtime, ctime and
// change attribute are updated and FILE_WR is marked dirty. follows is the
// snap context sequence a later flush reports. With buffered set, writeBack
// writes the data when FILE_BUFFER is revoked (after hold).
func (a *cephfsCapAgent) markDirty(ino, size uint64, mtime cephmsgr.MDSTime, follows uint64, buffered bool, hold time.Duration, writeBack func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.markDirtyLocked(ino, size, mtime, follows, buffered, hold, writeBack)
}

func (a *cephfsCapAgent) markDirtyLocked(ino, size uint64, mtime cephmsgr.MDSTime, follows uint64, buffered bool, hold time.Duration, writeBack func() error) error {
	c := a.caps[ino]
	if c == nil || c.CapID == 0 || c.Released || c.Issued&cephCapFileWR == 0 || buffered && (c.Issued&cephCapFileBuffer == 0 || writeBack == nil) {
		return fmt.Errorf("inode %d does not hold the caps for this write", ino)
	}
	if size > c.Size {
		c.Size = size
	}
	c.MTime, c.CTime = mtime, mtime
	c.ChangeAttr++
	d := a.dirty[ino]
	if d == nil {
		d = &cephfsDirtyFile{Ino: ino}
		a.dirty[ino] = d
	}
	if d.Latch != nil {
		return fmt.Errorf("inode %d has latched buffered data awaiting an explicit decision", ino)
	}
	if d.Buffered || d.writing {
		return fmt.Errorf("inode %d still has buffered data", ino)
	}
	d.Dirty |= cephCapFileWR
	d.Follows, d.Buffered, d.Hold, d.WriteBack = follows, buffered, hold, writeBack
	d.Events = append(d.Events, cephfsDirtyEvent{At: time.Now(), Kind: "write", Dirty: cephCapString(d.Dirty), Size: c.Size, Note: fmt.Sprintf("buffered %v change_attr %d", buffered, c.ChangeAttr)})
	return nil
}

// flushDirty restates fsync's check_caps(CHECK_CAPS_NODELAY |
// CHECK_CAPS_SYNCHRONOUS): the dirty metadata is flushed in an UPDATE with
// FLAG_SYNC. It returns the message and its flush TID.
func (a *cephfsCapAgent) flushDirty(ino uint64) (cephmsgr.MDSCaps, uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, d := a.caps[ino], a.dirty[ino]
	if c == nil || c.CapID == 0 || c.Released || d == nil || d.Dirty == 0 || d.Buffered {
		return cephmsgr.MDSCaps{}, 0, fmt.Errorf("inode %d has no unbuffered dirty metadata to flush", ino)
	}
	const flagSync = 1 // MClientCaps::FLAG_SYNC
	msg, tid, sent, err := a.checkCapsFlagsLocked(c, "fsync", flagSync)
	if err == nil && (!sent || tid == 0) {
		err = fmt.Errorf("inode %d flush was not sent", ino)
	}
	return msg, tid, err
}

// requestMaxSize restates get_caps for a write ending at endoff: a larger
// wanted max_size is requested from the MDS with check_caps unless the
// current max_size or an outstanding request covers it.
func (a *cephfsCapAgent) requestMaxSize(ino, endoff uint64) (cephmsgr.MDSCaps, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c := a.caps[ino]
	if c == nil || c.CapID == 0 || c.Released || c.Issued&cephCapFileWR == 0 {
		return cephmsgr.MDSCaps{}, false, fmt.Errorf("inode %d does not hold Fw", ino)
	}
	if endoff <= c.MaxSize {
		return cephmsgr.MDSCaps{}, false, nil
	}
	if endoff > c.WantedMaxSize {
		c.WantedMaxSize = endoff
	}
	if c.WantedMaxSize <= c.RequestedMaxSize {
		return cephmsgr.MDSCaps{}, false, nil
	}
	return a.checkCapsLocked(c, fmt.Sprintf("get_caps wants max_size %d", endoff))
}

// dirtyState returns a copy of one dirty file's state.
func (a *cephfsCapAgent) dirtyState(ino uint64) (cephfsDirtyFile, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	d := a.dirty[ino]
	if d == nil {
		return cephfsDirtyFile{}, false
	}
	copied := *d
	copied.WriteBack, copied.Events = nil, slices.Clone(d.Events)
	copied.Latch, copied.Resolved = cloneCephFSWritebackLatch(d.Latch), cloneCephFSWritebackLatches(d.Resolved)
	return copied, true
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
	c.InlineVersion = st.InlineVersion
	if d := a.dirty[st.Ino]; d != nil {
		// A file this client wrote: add_update_inode applies the reply
		// through update_inode_file_size and update_inode_file_time.
		c.updateFileSize(st.Size, st.TruncateSeq, st.TruncateSize)
		c.updateFileTime(c.Implemented|d.Dirty|d.Flushing, st.TimeWarpSeq, st.CTime, st.MTime, st.ATime)
		c.MaxSize, c.BTime = st.MaxSize, st.BTime
		if st.ChangeAttr > c.ChangeAttr {
			c.ChangeAttr = st.ChangeAttr
		}
	} else {
		c.Size, c.MaxSize, c.TruncateSize, c.TruncateSeq = st.Size, st.MaxSize, st.TruncateSize, st.TruncateSeq
		c.MTime, c.ATime, c.CTime, c.BTime, c.TimeWarpSeq, c.ChangeAttr = st.MTime, st.ATime, st.CTime, st.BTime, st.TimeWarpSeq, st.ChangeAttr
	}
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
	if c.CapID == 0 || c.Released {
		a.added++
		c.Added = a.added
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
			if trace.DirInode != nil && trace.DentryName != "" {
				a.linkLocked(trace.Target.Ino, trace.DirInode.Ino, trace.DentryName)
			}
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
			if trace.Target != nil {
				a.linkLocked(decoded.Entries[i].Inode.Ino, trace.Target.Ino, decoded.Entries[i].Name)
			}
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

// linkLocked records the first dentry naming an inode.
func (a *cephfsCapAgent) linkLocked(ino, parent uint64, name string) {
	if c := a.caps[ino]; c != nil && c.Name == "" {
		c.ParentIno, c.Name = parent, name
	}
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

// reconnectState restates Client::send_reconnect for every cap this agent
// holds: wanted is caps_wanted() (open-file wants and the wanted mask last
// sent, FILE_BUFFER implying FILE_EXCL), issued is the implemented mask, the
// short path is (parent inode, dentry name) when a dentry is known and
// (inode, "") otherwise, the oldest pending cap-snap follows and no file
// locks. Each cap's realm is listed once with its latest decoded snap trace.
// Sequence numbers are not sent; the agent continuing on the new session
// resets them (startCephFSCapAgentFrom).
func (a *cephfsCapAgent) reconnectState(realms map[uint64]cephmsgr.MDSSnapRealm) ([]cephmsgr.MDSCapReconnect, []cephmsgr.MDSRealmReconnect, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var caps []cephmsgr.MDSCapReconnect
	var order []uint64
	for _, c := range a.caps {
		if c.CapID == 0 || c.Released {
			continue
		}
		wanted := c.fileWanted() | c.Wanted
		if wanted&cephCapFileBuffer != 0 {
			wanted |= cephCapFileExcl
		}
		base, path := c.Ino, ""
		if c.ParentIno != 0 && c.Name != "" {
			base, path = c.ParentIno, c.Name
		}
		var follows uint64
		found := false
		for key, snap := range a.capSnaps {
			if key.Ino == c.Ino && snap.Acked.IsZero() && (!found || key.Follows < follows) {
				follows, found = key.Follows, true
			}
		}
		caps = append(caps, cephmsgr.MDSCapReconnect{Ino: c.Ino, CapID: c.CapID, Wanted: wanted, Issued: c.Implemented, SnapRealm: c.Realm, PathBase: base, Path: path, SnapFollows: follows})
		if !slices.Contains(order, c.Realm) {
			order = append(order, c.Realm)
		}
	}
	slices.SortFunc(caps, func(x, y cephmsgr.MDSCapReconnect) int { return cmp.Compare(x.Ino, y.Ino) })
	slices.Sort(order)
	var out []cephmsgr.MDSRealmReconnect
	for _, ino := range order {
		r, ok := realms[ino]
		if latest, known := a.realms[ino]; known && (!ok || latest.Seq >= r.Seq) {
			r, ok = latest, true
		}
		if !ok {
			return nil, nil, fmt.Errorf("no snap trace describes realm %d", ino)
		}
		out = append(out, cephmsgr.MDSRealmReconnect{Ino: r.Ino, Seq: r.Seq, Parent: r.Parent})
	}
	return caps, out, nil
}

// startCephFSCapAgentFrom continues a Lost session's capability state on the
// session that reconnected it: as send_reconnect does, cap sequences, issue
// sequences and migrate sequences restart at zero and issued equals
// implemented. Open handles, wanted masks, dentries and pending dirty/cap-snap
// state carry over; removed caps do not. Carrying a TID does not send a flush;
// recovery must explicitly choose its messages. The previous agent must have
// stopped, with no writeback callback still running.
func startCephFSCapAgentFrom(ctx context.Context, s *cephmsgr.MDSSession, previous *cephfsCapAgent) *cephfsCapAgent {
	a := newCephFSCapAgentFrom(ctx, s, previous)
	a.mu.Lock()
	a.recordLocked(cephfsCapRecord{Kind: "reconnect", Note: fmt.Sprintf("%d caps carried over with sequences reset", len(a.caps))})
	a.mu.Unlock()
	go a.run()
	return a
}

// newCephFSCapAgentFrom installs all expected ACK identities before the new
// event consumer starts. Slices and maps belong to the continuing agent.
func newCephFSCapAgentFrom(ctx context.Context, s *cephmsgr.MDSSession, previous *cephfsCapAgent) *cephfsCapAgent {
	previous.mu.Lock()
	defer previous.mu.Unlock()
	a := newCephFSCapAgent(ctx, s)
	a.barrier, a.added = previous.barrier, previous.added
	a.flushTID, a.flushing = previous.flushTID, maps.Clone(previous.flushing)
	a.flushSnaps = maps.Clone(previous.flushSnaps)
	if previous.capSnaps != nil {
		a.capSnaps = make(map[cephfsFlushSnapKey]*cephfsCapSnap, len(previous.capSnaps))
		for key, snap := range previous.capSnaps {
			copied := cloneCephFSCapSnap(snap)
			if snap.WritebackCompleted.IsZero() {
				copied.writeBack = snap.writeBack
			}
			copied.Caps.Seq, copied.Caps.IssueSeq, copied.Caps.MigrateSeq = 0, 0, 0
			a.capSnaps[key] = &copied
		}
	}
	for ino, r := range previous.realms {
		r.Snaps, r.PriorParentSnaps = slices.Clone(r.Snaps), slices.Clone(r.PriorParentSnaps)
		a.realms[ino] = r
	}
	for ino, d := range previous.dirty {
		copied := *d
		copied.Events = slices.Clone(d.Events)
		copied.Latch, copied.Resolved = cloneCephFSWritebackLatch(d.Latch), cloneCephFSWritebackLatches(d.Resolved)
		a.dirty[ino] = &copied
	}
	for ino, c := range previous.caps {
		if c.CapID == 0 || c.Released {
			continue
		}
		copied := *c
		copied.Open = maps.Clone(c.Open)
		copied.FscryptAuth, copied.FscryptFile = slices.Clone(c.FscryptAuth), slices.Clone(c.FscryptFile)
		if c.Quota != nil {
			quota := *c.Quota
			copied.Quota = &quota
		}
		copied.Seq, copied.IssueSeq, copied.MSeq, copied.Issued = 0, 0, 0, c.Implemented
		a.caps[ino] = &copied
	}
	return a
}

// cephfsRankedAgents holds the agents of one client's sessions with several
// ranks of one filesystem. They share one lock, as the native client handles
// every MDS message under client_lock, so a cap migration can move a cap
// between two ranks' views atomically.
type cephfsRankedAgents struct {
	mu     *sync.Mutex
	agents map[int32]*cephfsCapAgent
}

func newCephFSRankedAgents() *cephfsRankedAgents {
	return &cephfsRankedAgents{mu: &sync.Mutex{}, agents: map[int32]*cephfsCapAgent{}}
}

// start begins the agent of a new session with one rank.
func (g *cephfsRankedAgents) start(ctx context.Context, rank int32, s *cephmsgr.MDSSession) *cephfsCapAgent {
	a := newCephFSCapAgent(ctx, s)
	a.mu, a.rank, a.peers = g.mu, rank, g.agents
	g.mu.Lock()
	g.agents[rank] = a
	g.mu.Unlock()
	go a.run()
	return a
}

// startFrom continues the Lost session's agent of one rank on the session
// that reconnected it, as startCephFSCapAgentFrom does: sequences restart at
// zero and issued equals implemented. It replaces that rank's agent.
func (g *cephfsRankedAgents) startFrom(ctx context.Context, rank int32, s *cephmsgr.MDSSession, previous *cephfsCapAgent) *cephfsCapAgent {
	g.mu.Lock()
	a := newCephFSCapAgent(ctx, s)
	a.mu, a.rank, a.peers = g.mu, rank, g.agents
	a.barrier, a.added = previous.barrier, previous.added
	maps.Copy(a.realms, previous.realms)
	for ino, c := range previous.caps {
		copied := *c
		copied.Open = maps.Clone(c.Open)
		if c.CapID == 0 || c.Released {
			// The inode stays known without a cap of this rank.
			copied.CapID, copied.Issued, copied.Implemented, copied.Wanted = 0, 0, 0, 0
		} else {
			copied.Seq, copied.IssueSeq, copied.MSeq, copied.Issued = 0, 0, 0, c.Implemented
		}
		a.caps[ino] = &copied
	}
	g.agents[rank] = a
	a.recordLocked(cephfsCapRecord{Kind: "reconnect", Note: fmt.Sprintf("rank %d: %d inodes carried over with sequences reset", rank, len(a.caps))})
	g.mu.Unlock()
	go a.run()
	return a
}

// inodeLocked returns this rank's entry for ino, creating it from another
// rank's entry (the client's one inode cache) when only a peer knows it.
func (a *cephfsCapAgent) inodeLocked(ino uint64) *cephfsCap {
	if c := a.caps[ino]; c != nil {
		return c
	}
	for rank, peer := range a.peers {
		if rank == a.rank {
			continue
		}
		if pc := peer.caps[ino]; pc != nil {
			copied := *pc
			copied.Open = maps.Clone(pc.Open)
			copied.CapID, copied.Issued, copied.Implemented, copied.Wanted = 0, 0, 0, 0
			copied.Seq, copied.IssueSeq, copied.MSeq, copied.Added, copied.Released, copied.Exported = 0, 0, 0, 0, false, false
			a.caps[ino] = &copied
			return &copied
		}
	}
	return nil
}

// dropMigratedLocked removes this rank's cap after a migration without
// queueing a release (remove_cap(cap, false)).
func dropMigratedLocked(c *cephfsCap) {
	c.CapID, c.Issued, c.Implemented, c.Wanted, c.Exported = 0, 0, 0, 0, true
}

// migrateLocked restates handle_cap_export and handle_cap_import for a
// client with sessions to several ranks. EXPORT carries the importer's new
// cap as the peer: the exporting rank's cap moves to the peer rank (or only
// updates a cap that IMPORT already installed there). IMPORT installs the
// importer's cap and removes the exporter's cap it names as the peer,
// releasing it when the peer flags ask for that; IMPORT is then handled as a
// GRANT, as handle_caps does.
func (a *cephfsCapAgent) migrateLocked(e cephmsgr.MDSSessionEvent) {
	m := e.Caps
	ev := cephfsMigrationEvent{Rank: a.rank, Arrival: e.Arrival, Received: e.Received, PushSequence: a.s.Status().PushSequence, Op: m.Op, Ino: m.Ino, CapID: m.CapID, Seq: m.Seq,
		IssueSeq: m.IssueSeq, MSeq: m.MigrateSeq, Caps: cephCapString(m.Caps), Wanted: cephCapString(m.Wanted), Realm: m.Realm, Peer: m.Peer}
	r := cephfsCapRecord{Kind: "caps", Received: e.Received, Arrival: e.Arrival, Ino: m.Ino, CapID: m.CapID, Op: m.Op, Seq: m.Seq, IssueSeq: m.IssueSeq, MSeq: m.MigrateSeq,
		Caps: cephCapString(m.Caps), Wanted: cephCapString(m.Wanted)}
	defer func() {
		r.Note, r.Error = fmt.Sprintf("rank %d: %s (peer mds %d cap %d seq %d mseq %d flags %d)", a.rank, ev.Action, m.Peer.MDS, m.Peer.CapID, m.Peer.IssueSeq, m.Peer.MSeq, m.Peer.Flags), ev.Error
		a.migrations = append(a.migrations, ev)
		a.recordLocked(r)
	}()
	switch m.Op {
	case cephmsgr.MDSCapOpExport:
		c := a.caps[m.Ino]
		if c == nil || c.CapID == 0 || c.Released || c.CapID != m.CapID {
			ev.Action = "export ignored: no matching cap of this rank"
			return
		}
		r.Before = cephCapString(c.Issued)
		if m.Peer.CapID == 0 {
			dropMigratedLocked(c)
			ev.Action = "export without peer: cap removed"
			return
		}
		target := a.peers[m.Peer.MDS]
		if target == nil || m.Peer.MDS == a.rank {
			ev.Action, ev.Error = "export to a rank without a session", fmt.Sprintf("no agent for rank %d", m.Peer.MDS)
			a.failLocked("cap export of %d to rank %d without a session", m.Ino, m.Peer.MDS)
			return
		}
		tc := target.inodeLocked(m.Ino)
		if tc.CapID != 0 && !tc.Released {
			if tc.CapID == m.Peer.CapID && cephSeqAfter(m.Peer.IssueSeq, tc.Seq) {
				tc.Seq, tc.IssueSeq = m.Peer.IssueSeq-1, m.Peer.IssueSeq-1
				tc.Issued, tc.Implemented = tc.Issued|c.Issued, tc.Implemented|c.Issued
				ev.Action = fmt.Sprintf("export: rank %d cap %d updated", m.Peer.MDS, tc.CapID)
			} else {
				ev.Action = fmt.Sprintf("export: rank %d cap %d already newer", m.Peer.MDS, tc.CapID)
			}
		} else {
			target.addUpdateCapLocked(tc, m.Peer.CapID, c.Issued, 0, m.Peer.IssueSeq-1, m.Peer.MSeq, c.Realm)
			tc.Exported = false
			ev.Action = fmt.Sprintf("export: cap %d added for rank %d", m.Peer.CapID, m.Peer.MDS)
		}
		dropMigratedLocked(c)
		r.After = "-"
	case cephmsgr.MDSCapOpImport:
		c := a.inodeLocked(m.Ino)
		if c == nil {
			r.Note = "deferred until the reply trace introducing the inode is applied"
			a.pending = append(a.pending, e)
			ev.Action = "import deferred: inode unknown to every rank"
			return
		}
		if len(m.SnapTrace) != 0 {
			realms, err := cephmsgr.DecodeMDSSnapTrace(m.SnapTrace)
			if err != nil {
				ev.Error = err.Error()
				a.failLocked("import snap trace of %d: %v", m.Ino, err)
				return
			}
			for _, realm := range realms {
				a.realms[realm.Ino] = realm
			}
		}
		var peerCap *cephfsCap
		peer := a.peers[m.Peer.MDS]
		if m.Peer.CapID != 0 && peer != nil && m.Peer.MDS != a.rank {
			if pc := peer.caps[m.Ino]; pc != nil && pc.CapID != 0 && !pc.Released {
				peerCap = pc
			}
		}
		before := c.Issued
		a.addUpdateCapLocked(c, m.CapID, m.Caps, m.Wanted, m.Seq, m.MigrateSeq, m.Realm)
		c.Exported = false
		ev.Action = fmt.Sprintf("import: cap %d installed", m.CapID)
		if peerCap != nil && peerCap.CapID == m.Peer.CapID {
			item := cephmsgr.MDSCapItem{Ino: m.Ino, CapID: peerCap.CapID, MigrateSeq: peerCap.MSeq, IssueSeq: peerCap.IssueSeq}
			dropMigratedLocked(peerCap)
			ev.Action += fmt.Sprintf("; rank %d cap %d removed", m.Peer.MDS, item.CapID)
			if m.Peer.Flags&cephCapFlagRelease != 0 {
				err := peer.send(func(ctx context.Context) error {
					return peer.s.ReleaseCaps(ctx, []cephmsgr.MDSCapItem{item}, peer.barrier)
				})
				ev.Released = err == nil
				if err != nil {
					ev.Error = err.Error()
					a.failLocked("release of migrated cap %d: %v", m.Ino, err)
				}
			}
		}
		r.Before, r.After = cephCapString(before), cephCapString(c.Issued)
		// handle_caps continues an IMPORT as a GRANT on the installed cap.
		a.grantLocked(c, e, cephfsCapRecord{Kind: "caps", Received: e.Received, Arrival: e.Arrival, Ino: m.Ino, CapID: m.CapID, Op: m.Op, Seq: m.Seq, IssueSeq: m.IssueSeq,
			MSeq: m.MigrateSeq, Caps: cephCapString(m.Caps), Wanted: cephCapString(m.Wanted), Note: "import handled as grant"})
	}
}

type cephfsAgentSnapshot struct {
	Records  []cephfsCapRecord
	Caps     []cephfsCap
	Pending  int
	Errors   []string
	Holds    []cephfsHold
	Barrier  uint32
	EndError string `json:",omitempty"`
	// Pushes counts the capability, lease, snapshot and quota pushes handled.
	Pushes  uint64
	Leases  []cephfsLeaseEvent  `json:",omitempty"`
	Snaps   []cephfsSnapEvent   `json:",omitempty"`
	Quotas  []cephfsQuotaEvent  `json:",omitempty"`
	Recalls []cephfsRecallEvent `json:",omitempty"`
	Flushes []cephfsFlushEvent  `json:",omitempty"`
	// Rank and Migrations are reported for agents of a multi-rank client.
	Rank       int32                  `json:",omitempty"`
	Migrations []cephfsMigrationEvent `json:",omitempty"`
	Renewals   []cephfsLeaseRenewal   `json:",omitempty"`
	ReadOnly   []time.Time            `json:",omitempty"`
	CapSnaps   []cephfsCapSnap        `json:",omitempty"`
	SnapAcks   []cephfsFlushSnapAck   `json:",omitempty"`
	// Dirty, FlushAcks, Truncs and MaxSizes are reported for a client that
	// wrote file data (the data acceptance).
	Dirty     []cephfsDirtyFile    `json:",omitempty"`
	FlushAcks []cephfsFlushAck     `json:",omitempty"`
	Truncs    []cephfsTruncEvent   `json:",omitempty"`
	MaxSizes  []cephfsMaxSizeEvent `json:",omitempty"`
}

func (a *cephfsCapAgent) snapshot() cephfsAgentSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := cephfsAgentSnapshot{Records: slices.Clone(a.records), Pending: len(a.pending), Errors: slices.Clone(a.errs), Barrier: a.barrier, Pushes: a.pushes,
		Leases: slices.Clone(a.leases), Snaps: slices.Clone(a.snaps), Quotas: slices.Clone(a.quotas), Recalls: slices.Clone(a.recalls), Flushes: slices.Clone(a.flushes),
		Rank: a.rank, Migrations: slices.Clone(a.migrations), Renewals: slices.Clone(a.renewals), ReadOnly: slices.Clone(a.readOnly), SnapAcks: slices.Clone(a.snapAcks)}
	for _, capSnap := range a.capSnaps {
		s.CapSnaps = append(s.CapSnaps, cloneCephFSCapSnap(capSnap))
	}
	slices.SortFunc(s.CapSnaps, func(x, y cephfsCapSnap) int {
		return cmp.Or(cmp.Compare(x.Ino, y.Ino), cmp.Compare(x.Context.Sequence, y.Context.Sequence))
	})
	for _, c := range a.caps {
		s.Caps = append(s.Caps, *c)
	}
	slices.SortFunc(s.Caps, func(x, y cephfsCap) int { return cmp.Compare(x.Ino, y.Ino) })
	for _, h := range a.holds {
		s.Holds = append(s.Holds, *h)
	}
	for _, d := range a.dirty {
		copied := *d
		copied.WriteBack, copied.Events = nil, slices.Clone(d.Events)
		copied.Latch, copied.Resolved = cloneCephFSWritebackLatch(d.Latch), cloneCephFSWritebackLatches(d.Resolved)
		s.Dirty = append(s.Dirty, copied)
	}
	slices.SortFunc(s.Dirty, func(x, y cephfsDirtyFile) int { return cmp.Compare(x.Ino, y.Ino) })
	s.FlushAcks, s.Truncs, s.MaxSizes = slices.Clone(a.flushAcks), slices.Clone(a.truncs), slices.Clone(a.maxSizes)
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

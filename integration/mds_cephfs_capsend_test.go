package integration_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Test-only consumer policy for dirty head UPDATEs whose cap send has an
// uncertain or refused outcome. It restates Client::mark_caps_flushing,
// send_cap and kick_flushing_caps at the pinned Tentacle source (LGPL-2.1
// references; no native code copied). The product only classifies the
// SendCaps outcome: an error is returned before writer admission (the frame
// is never transmitted), and success is admission whose remote outcome stays
// unknown until a correlated FLUSH_ACK. Which retained flush to send again,
// and when, is decided here and never by the library.
//
// Unlike native send_cap, a recovery send keeps the caller-supplied original
// metadata (size, times, change attribute) together with the original flush
// TID, dirty mask, snap follows and oldest flush TID. Only the cap envelope
// (cap ID, sequences, implemented/wanted caps, OSD barrier) is taken from the
// current session, as for the cap-snap resubmission helper.

type cephfsHeadFlushAttempt struct {
	Kind                        string
	Generation, ReconnectedFrom uint64
	Started, Returned           time.Time
	Admitted                    bool
	Error                       string `json:",omitempty"`
	CapID                       uint64
	Seq, IssueSeq, MigrateSeq   uint32
	Barrier                     uint32
}

type cephfsHeadFlush struct {
	Ino, Transaction uint64
	Caps             cephmsgr.MDSCaps
	Prepared         time.Time
	Attempts         []cephfsHeadFlushAttempt `json:",omitempty"`
}

func cloneCephFSHeadFlush(f *cephfsHeadFlush) cephfsHeadFlush {
	out := *f
	out.Caps = cloneCephFSCaps(f.Caps)
	out.Attempts = slices.Clone(f.Attempts)
	return out
}

// admittedIn reports whether a send was admitted in that session generation.
func (f *cephfsHeadFlush) admittedIn(generation uint64) bool {
	return slices.ContainsFunc(f.Attempts, func(a cephfsHeadFlushAttempt) bool { return a.Admitted && a.Generation == generation })
}

func (f *cephfsHeadFlush) admitted() bool {
	return slices.ContainsFunc(f.Attempts, func(a cephfsHeadFlushAttempt) bool { return a.Admitted })
}

// prepareHeadFlush records written data as dirty FILE_WR metadata and
// registers one flush TID for it (mark_caps_flushing) without sending. Both
// happen under one agent lock so no concurrent check_caps can flush the same
// dirty bits with another TID.
func (a *cephfsCapAgent) prepareHeadFlush(ino, size uint64, mtime cephmsgr.MDSTime, follows uint64) (*cephfsHeadFlush, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.markDirtyLocked(ino, size, mtime, follows, false, 0, nil); err != nil {
		return nil, err
	}
	c, d := a.caps[ino], a.dirty[ino]
	dirty := d.Dirty
	wanted := c.fileWanted()
	maxSize := c.MaxSize
	if wanted&cephCapAnyFileWR != 0 && c.WantedMaxSize > maxSize {
		maxSize = c.WantedMaxSize
	}
	a.flushTID++
	tid := a.flushTID
	a.flushing[tid] = ino
	d.Flushing |= dirty
	d.Dirty = 0
	const flagSync = 1 // MClientCaps::FLAG_SYNC, as fsync's check_caps
	msg := cephmsgr.MDSCaps{Op: cephmsgr.MDSCapOpUpdate, Ino: c.Ino, CapID: c.CapID, Seq: c.Seq, IssueSeq: c.IssueSeq, Caps: c.Implemented, Wanted: wanted, MigrateSeq: c.MSeq,
		UID: c.UID, GID: c.GID, Mode: c.Mode, NLink: c.NLink, Size: c.Size, MaxSize: maxSize, TruncateSize: c.TruncateSize, TruncateSeq: c.TruncateSeq,
		MTime: c.MTime, ATime: c.ATime, CTime: c.CTime, TimeWarpSeq: c.TimeWarpSeq, OSDEpochBarrier: a.barrier, CallerUID: ^uint32(0), CallerGID: ^uint32(0),
		BTime: c.BTime, ChangeAttr: c.ChangeAttr, Flags: flagSync, Files: -1, Subdirs: -1, Dirty: dirty, SnapFollows: d.Follows, InlineVersion: c.InlineVersion,
		OldestFlushTID: a.oldestFlushTIDLocked(), FscryptAuth: slices.Clone(c.FscryptAuth), FscryptFile: slices.Clone(c.FscryptFile)}
	d.Events = append(d.Events, cephfsDirtyEvent{At: time.Now(), Kind: "head-flush-prepared", Dirty: cephCapString(dirty), TID: tid, Size: msg.Size})
	return &cephfsHeadFlush{Ino: ino, Transaction: tid, Caps: msg, Prepared: time.Now()}, nil
}

// headFlushPendingLocked reports that no FLUSH_ACK covered this TID yet.
func (a *cephfsCapAgent) headFlushPendingLocked(f *cephfsHeadFlush) bool {
	ino, ok := a.flushing[f.Transaction]
	return ok && ino == f.Ino
}

// headFlushOutcome is the consumer's view: unsent (no attempt admitted),
// uncertain (admitted without a FLUSH_ACK) or acknowledged.
func (a *cephfsCapAgent) headFlushOutcome(f *cephfsHeadFlush) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case !a.headFlushPendingLocked(f):
		return "acknowledged"
	case f.admitted():
		return "uncertain"
	default:
		return "unsent"
	}
}

// sendHeadFlushLocked sends the retained identity and metadata with the
// current cap envelope and records the classified attempt.
func (a *cephfsCapAgent) sendHeadFlushLocked(f *cephfsHeadFlush, kind string, status cephmsgr.MDSSessionStatus, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	c := a.caps[f.Ino]
	if c == nil || c.CapID == 0 || c.Released {
		return fmt.Errorf("head flush %d on %d lacks its current cap", f.Transaction, f.Ino)
	}
	msg := cloneCephFSCaps(f.Caps)
	msg.CapID, msg.Seq, msg.IssueSeq, msg.MigrateSeq = c.CapID, c.Seq, c.IssueSeq, c.MSeq
	msg.Caps, msg.Wanted, msg.OSDEpochBarrier = c.Implemented, c.fileWanted(), a.barrier
	attempt := cephfsHeadFlushAttempt{Kind: kind, Generation: status.Generation, ReconnectedFrom: status.ReconnectedFrom, Started: time.Now(),
		CapID: msg.CapID, Seq: msg.Seq, IssueSeq: msg.IssueSeq, MigrateSeq: msg.MigrateSeq, Barrier: msg.OSDEpochBarrier}
	err := a.send(func(ctx context.Context) error { return send(ctx, msg, f.Transaction) })
	attempt.Returned, attempt.Admitted = time.Now(), err == nil
	if err != nil {
		// Known unsent: SendCaps refuses only before writer admission.
		attempt.Error = err.Error()
	}
	f.Attempts = append(f.Attempts, attempt)
	if d := a.dirty[f.Ino]; d != nil {
		ev := cephfsDirtyEvent{At: attempt.Returned, Kind: "head-flush-" + kind, Seq: msg.Seq, Caps: cephCapString(msg.Caps), Dirty: cephCapString(msg.Dirty), TID: f.Transaction, Size: msg.Size}
		if err != nil {
			ev.Error = err.Error()
		}
		d.Events = append(d.Events, ev)
	}
	return err
}

// firstSendHeadFlush sends a prepared flush that was never admitted. An
// admitted send is never repeated through this path; a refused one may be.
func (a *cephfsCapAgent) firstSendHeadFlush(f *cephfsHeadFlush, status cephmsgr.MDSSessionStatus, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.headFlushPendingLocked(f) || f.admitted() {
		return fmt.Errorf("head flush %d was already admitted or acknowledged", f.Transaction)
	}
	return a.sendHeadFlushLocked(f, "first-send", status, send)
}

// recoverHeadFlushesWith is the explicit caller decision after reconnect.
// The whole ordered selection is validated before the first send. A flush
// admitted by the Lost generation is resubmitted with its original TID; a
// flush whose earlier sends were all refused is first-sent. A flush already
// admitted in this generation or acknowledged is refused, and the first
// refused send stops the remaining selection.
func (a *cephfsCapAgent) recoverHeadFlushesWith(flushes []*cephfsHeadFlush, status cephmsgr.MDSSessionStatus, send func(context.Context, cephmsgr.MDSCaps, uint64) error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(flushes) == 0 || status.State != cephmsgr.MDSSessionOpen || status.ReconnectedFrom == 0 || status.Generation <= status.ReconnectedFrom {
		return errors.New("head flush recovery requires a nonempty selection and a new reconnected open session")
	}
	kinds := make([]string, len(flushes))
	for i, f := range flushes {
		if f == nil || f.Transaction == 0 || i > 0 && f.Transaction <= flushes[i-1].Transaction {
			return errors.New("head flush recovery TIDs must be distinct and in original order")
		}
		if f.Caps.Op != cephmsgr.MDSCapOpUpdate || f.Caps.Ino != f.Ino || f.Caps.Dirty == 0 || f.Caps.OldestFlushTID == 0 || f.Caps.OldestFlushTID > f.Transaction {
			return fmt.Errorf("head flush %d lacks its original UPDATE identity", f.Transaction)
		}
		if !a.headFlushPendingLocked(f) {
			return fmt.Errorf("head flush %d is already acknowledged", f.Transaction)
		}
		if f.admittedIn(status.Generation) {
			return fmt.Errorf("head flush %d was already admitted in generation %d", f.Transaction, status.Generation)
		}
		if c := a.caps[f.Ino]; c == nil || c.CapID == 0 || c.Released {
			return fmt.Errorf("head flush %d lacks its current cap", f.Transaction)
		}
		kinds[i] = "first-send"
		if f.admitted() {
			kinds[i] = "resubmit"
		}
	}
	for i, f := range flushes {
		if err := a.sendHeadFlushLocked(f, kinds[i], status, send); err != nil {
			return err
		}
	}
	return nil
}

func (a *cephfsCapAgent) recoverHeadFlushes(flushes []*cephfsHeadFlush) error {
	return a.recoverHeadFlushesWith(flushes, a.s.Status(), a.s.SendCaps)
}

// --- test-only consumer checks (no Ceph fixture) ---

func headFlushTestAgent(t *testing.T) *cephfsCapAgent {
	t.Helper()
	a := newCephFSCapAgent(context.Background(), nil)
	a.caps[17] = &cephfsCap{Ino: 17, CapID: 4, Issued: cephCapFileWR | cephCapFileBuffer, Implemented: cephCapFileWR | cephCapFileBuffer, Seq: 5, IssueSeq: 3, MSeq: 1,
		Size: 100, MaxSize: 1 << 20, Open: map[int]int{cephFileModeRDWR: 1}}
	a.barrier = 9
	return a
}

type headFlushSend struct {
	caps cephmsgr.MDSCaps
	tid  uint64
}

func recordHeadFlushSends(sent *[]headFlushSend, fail error) func(context.Context, cephmsgr.MDSCaps, uint64) error {
	return func(_ context.Context, caps cephmsgr.MDSCaps, tid uint64) error {
		if fail != nil {
			return fail
		}
		*sent = append(*sent, headFlushSend{caps, tid})
		return nil
	}
}

func TestCephFSHeadFlushPrepareRetainsIdentity(t *testing.T) {
	a := headFlushTestAgent(t)
	first, err := a.prepareHeadFlush(17, 200, cephmsgr.MDSTime{Seconds: 10}, 2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.prepareHeadFlush(17, 300, cephmsgr.MDSTime{Seconds: 11}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Transaction != 1 || second.Transaction != 2 || first.Caps.Dirty != cephCapFileWR || second.Caps.Dirty != cephCapFileWR ||
		first.Caps.OldestFlushTID != 1 || second.Caps.OldestFlushTID != 1 || first.Caps.Size != 200 || second.Caps.Size != 300 ||
		first.Caps.SnapFollows != 2 || first.Caps.Flags != 1 || len(first.Attempts) != 0 {
		t.Fatalf("prepared identities differ: %+v %+v", first, second)
	}
	if d := a.dirty[17]; d.Dirty != 0 || d.Flushing != cephCapFileWR || len(a.flushing) != 2 {
		t.Fatal("prepared dirty bits were not moved to flushing", d, a.flushing)
	}
	if a.headFlushOutcome(first) != "unsent" {
		t.Fatal("prepared flush is not unsent")
	}
}

func TestCephFSHeadFlushRefusedSendIsKnownUnsentAndAdmittedIsUncertain(t *testing.T) {
	a := headFlushTestAgent(t)
	f, err := a.prepareHeadFlush(17, 200, cephmsgr.MDSTime{Seconds: 10}, 2)
	if err != nil {
		t.Fatal(err)
	}
	old := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionLost, Generation: 1}
	var sent []headFlushSend
	refused := errors.New("lost before admission")
	if err := a.firstSendHeadFlush(f, old, recordHeadFlushSends(&sent, refused)); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	if a.headFlushOutcome(f) != "unsent" || len(f.Attempts) != 1 || f.Attempts[0].Admitted || f.Attempts[0].Error == "" {
		t.Fatal("refused send was not recorded as known unsent", f.Attempts)
	}
	open := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 1}
	if err := a.firstSendHeadFlush(f, open, recordHeadFlushSends(&sent, nil)); err != nil {
		t.Fatal("known-unsent flush could not be first-sent", err)
	}
	if a.headFlushOutcome(f) != "uncertain" || len(sent) != 1 || sent[0].tid != f.Transaction {
		t.Fatal("admitted send is not uncertain", f.Attempts, sent)
	}
	if err := a.firstSendHeadFlush(f, open, recordHeadFlushSends(&sent, nil)); err == nil || len(sent) != 1 {
		t.Fatal("admitted flush was first-sent again", err, sent)
	}
}

func TestCephFSHeadFlushRecoveryClassifiesAndKeepsIdentity(t *testing.T) {
	a := headFlushTestAgent(t)
	uncertain, _ := a.prepareHeadFlush(17, 200, cephmsgr.MDSTime{Seconds: 10}, 2)
	refused, _ := a.prepareHeadFlush(17, 300, cephmsgr.MDSTime{Seconds: 11}, 2)
	lostGeneration := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 1}
	var sent []headFlushSend
	if err := a.firstSendHeadFlush(uncertain, lostGeneration, recordHeadFlushSends(&sent, nil)); err != nil {
		t.Fatal(err)
	}
	lost := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionLost, Generation: 1}
	if err := a.firstSendHeadFlush(refused, lost, recordHeadFlushSends(&sent, errors.New("lost"))); err == nil {
		t.Fatal("refusal not reported")
	}
	// The new generation resets the cap sequences (send_reconnect).
	c := a.caps[17]
	c.Seq, c.IssueSeq, c.MSeq, c.Implemented = 0, 0, 0, cephCapFileWR
	a.barrier = 12
	reconnected := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 2, ReconnectedFrom: 1}
	for _, bad := range []struct {
		flushes []*cephfsHeadFlush
		status  cephmsgr.MDSSessionStatus
	}{
		{nil, reconnected},
		{[]*cephfsHeadFlush{uncertain, refused}, cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 1}},
		{[]*cephfsHeadFlush{uncertain, refused}, cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionLost, Generation: 2, ReconnectedFrom: 1}},
		{[]*cephfsHeadFlush{refused, uncertain}, reconnected},
		{[]*cephfsHeadFlush{uncertain, uncertain}, reconnected},
	} {
		before := len(sent)
		if err := a.recoverHeadFlushesWith(bad.flushes, bad.status, recordHeadFlushSends(&sent, nil)); err == nil || len(sent) != before {
			t.Fatal("invalid recovery selection sent a flush", bad.status, err)
		}
	}
	sent = sent[:0]
	if err := a.recoverHeadFlushesWith([]*cephfsHeadFlush{uncertain, refused}, reconnected, recordHeadFlushSends(&sent, nil)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || uncertain.Attempts[len(uncertain.Attempts)-1].Kind != "resubmit" || refused.Attempts[len(refused.Attempts)-1].Kind != "first-send" {
		t.Fatal("recovery kinds differ", uncertain.Attempts, refused.Attempts)
	}
	for i, f := range []*cephfsHeadFlush{uncertain, refused} {
		got := sent[i]
		if got.tid != f.Transaction || got.caps.Dirty != f.Caps.Dirty || got.caps.SnapFollows != f.Caps.SnapFollows || got.caps.OldestFlushTID != f.Caps.OldestFlushTID ||
			got.caps.Size != f.Caps.Size || got.caps.MTime != f.Caps.MTime || got.caps.Flags != f.Caps.Flags {
			t.Fatalf("recovery changed the original identity or metadata: %+v %+v", got, f.Caps)
		}
		if got.caps.Seq != 0 || got.caps.IssueSeq != 0 || got.caps.MigrateSeq != 0 || got.caps.Caps != cephCapFileWR || got.caps.OSDEpochBarrier != 12 || got.caps.CapID != 4 {
			t.Fatalf("recovery did not use the current cap envelope: %+v", got.caps)
		}
	}
	if err := a.recoverHeadFlushesWith([]*cephfsHeadFlush{uncertain}, reconnected, recordHeadFlushSends(&sent, nil)); err == nil || len(sent) != 2 {
		t.Fatal("a flush was resubmitted twice in one generation", err)
	}
	// The agent's FLUSH_ACK handler (handle_cap_flush_ack) clears every TID
	// of the inode up to the acknowledged one; restate that effect here.
	a.mu.Lock()
	for tid, ino := range a.flushing {
		if ino == 17 && tid <= refused.Transaction {
			delete(a.flushing, tid)
		}
	}
	a.mu.Unlock()
	if a.headFlushOutcome(uncertain) != "acknowledged" || a.headFlushOutcome(refused) != "acknowledged" {
		t.Fatal("FLUSH_ACK did not resolve the retained flushes")
	}
	next := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 3, ReconnectedFrom: 2}
	if err := a.recoverHeadFlushesWith([]*cephfsHeadFlush{uncertain}, next, recordHeadFlushSends(&sent, nil)); err == nil {
		t.Fatal("acknowledged flush was selected for recovery")
	}
}

func TestCephFSHeadFlushRecoveryFailureStopsLaterFlushes(t *testing.T) {
	a := headFlushTestAgent(t)
	first, _ := a.prepareHeadFlush(17, 200, cephmsgr.MDSTime{Seconds: 10}, 2)
	second, _ := a.prepareHeadFlush(17, 300, cephmsgr.MDSTime{Seconds: 11}, 2)
	reconnected := cephmsgr.MDSSessionStatus{State: cephmsgr.MDSSessionOpen, Generation: 2, ReconnectedFrom: 1}
	calls := 0
	failing := func(context.Context, cephmsgr.MDSCaps, uint64) error { calls++; return errors.New("refused") }
	if err := a.recoverHeadFlushesWith([]*cephfsHeadFlush{first, second}, reconnected, failing); err == nil || calls != 1 || len(second.Attempts) != 0 {
		t.Fatal("refused recovery send did not stop the selection", err, calls, second.Attempts)
	}
	if a.headFlushOutcome(first) != "unsent" || first.Attempts[0].Admitted {
		t.Fatal("refused recovery send was not known unsent", first.Attempts)
	}
}

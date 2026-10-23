package integration_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Cluster-free regressions of the buffered-writeback latch: a writeback the
// revoke callback starts and that does not complete is never treated as
// completed, keeps FILE_BUFFER in use, is not restarted by later pushes and
// ends only by an explicit eviction or recovery decision.

const cephfsLatchTestIno = 17

type cephfsLatchHarness struct {
	t     *testing.T
	a     *cephfsCapAgent
	sent  []cephmsgr.MDSCaps
	calls atomic.Int32
	seq   uint32
}

func newCephFSLatchHarness(t *testing.T) *cephfsLatchHarness {
	h := &cephfsLatchHarness{t: t, a: newCephFSCapAgent(context.Background(), nil), seq: 1}
	h.a.capSend = func(_ context.Context, m cephmsgr.MDSCaps, _ uint64) error {
		h.sent = append(h.sent, m) // called with the agent lock held
		return nil
	}
	all := cephCapsForMode(cephFileModeRDWR) | cephCapPin
	h.a.caps[cephfsLatchTestIno] = &cephfsCap{Ino: cephfsLatchTestIno, CapID: 4, Seq: 1, IssueSeq: 1, Issued: all, Implemented: all, NLink: 1, Size: 100,
		MaxSize: 1 << 22, Realm: 1, Open: map[int]int{cephFileModeRDWR: 1}}
	return h
}

// buffer registers buffered data whose writeback returns err.
func (h *cephfsLatchHarness) buffer(err error) {
	h.t.Helper()
	if e := h.a.markDirty(cephfsLatchTestIno, 200, cephmsgr.MDSTime{Seconds: 50}, 1, true, 0, func() error {
		h.calls.Add(1)
		return err
	}); e != nil {
		h.t.Fatal(e)
	}
}

// push delivers one GRANT or REVOKE with the given issued caps.
func (h *cephfsLatchHarness) push(op, caps uint32) {
	h.a.mu.Lock()
	defer h.a.mu.Unlock()
	h.seq++
	h.a.handleLocked(cephmsgr.MDSSessionEvent{Arrival: uint64(h.seq), Received: time.Now(), Caps: &cephmsgr.MDSCaps{Op: op, Ino: cephfsLatchTestIno, CapID: 4,
		Seq: h.seq, IssueSeq: h.seq, Caps: caps, Size: 200, MaxSize: 1 << 22}})
}

func (h *cephfsLatchHarness) waitLatch() cephfsDirtyFile {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		d, _ := h.a.dirtyState(cephfsLatchTestIno)
		if d.Latch != nil {
			return d
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("writeback did not latch: %+v", d)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *cephfsLatchHarness) sentCaps() []cephmsgr.MDSCaps {
	h.a.mu.Lock()
	defer h.a.mu.Unlock()
	return slices.Clone(h.sent)
}

func (h *cephfsLatchHarness) implemented() uint32 {
	c, _ := h.a.cap(cephfsLatchTestIno)
	return c.Implemented
}

func (h *cephfsLatchHarness) writebackEvents() int {
	d, _ := h.a.dirtyState(cephfsLatchTestIno)
	n := 0
	for _, ev := range d.Events {
		if ev.Kind == "data-writeback" {
			n++
		}
	}
	return n
}

func TestCephFSRevokeWritebackLatchHoldsBufferUntilEviction(t *testing.T) {
	h := newCephFSLatchHarness(t)
	h.buffer(&cephfsWritebackStopped{Phase: "b", Outcomes: []string{cephfsWriteCompleted, cephfsWriteUncertain}})
	all := cephCapsForMode(cephFileModeRDWR) | cephCapPin
	revoked := all &^ (cephCapFileWR | cephCapFileBuffer | cephCapFileExcl)
	h.push(cephmsgr.MDSCapOpRevoke, revoked)
	d := h.waitLatch()
	l := d.Latch
	if h.calls.Load() != 1 || !d.Buffered || d.writing || l.Source != "revoke-callback" || l.Outcome != cephfsWriteUncertain ||
		!slices.Equal(l.Extents, []string{cephfsWriteCompleted, cephfsWriteUncertain}) || l.Decision != "" || l.Error == "" || len(d.Resolved) != 0 {
		t.Fatalf("interrupted writeback was not latched as uncertain buffered data: %+v %+v", d, l)
	}
	sent := h.sentCaps()
	if len(sent) == 0 || sent[0].Dirty != cephCapFileWR {
		t.Fatal("the REVOKE did not flush the dirty metadata first", sent)
	}
	for _, m := range sent {
		if m.Caps&cephCapFileBuffer == 0 {
			t.Fatal("FILE_BUFFER was released while the writeback is latched", m)
		}
	}
	if h.implemented()&cephCapFileBuffer == 0 || slices.ContainsFunc(d.Events, func(ev cephfsDirtyEvent) bool { return ev.Note == "buffered data written back" }) {
		t.Fatal("the latched data was treated as written back")
	}
	if got := h.a.latchedFiles(); !slices.Equal(got, []uint64{cephfsLatchTestIno}) {
		t.Fatal("latched file not reported", got)
	}

	// Later pushes, including a fresh grant and a second revoke of Fb, never
	// restart the writeback; FILE_BUFFER stays implemented.
	h.push(cephmsgr.MDSCapOpGrant, all)
	h.push(cephmsgr.MDSCapOpRevoke, revoked)
	h.push(cephmsgr.MDSCapOpGrant, all)
	h.push(cephmsgr.MDSCapOpGrant, revoked)
	time.Sleep(20 * time.Millisecond)
	d, _ = h.a.dirtyState(cephfsLatchTestIno)
	if h.calls.Load() != 1 || h.writebackEvents() != 1 || d.Latch == nil || d.Latch.Ignored != 2 || !d.Buffered {
		t.Fatalf("a later push restarted or cleared the latched writeback: calls %d %+v", h.calls.Load(), d.Latch)
	}
	for _, m := range h.sentCaps() {
		if m.Caps&cephCapFileBuffer == 0 {
			t.Fatal("a later push released FILE_BUFFER before a decision", m)
		}
	}

	// Nothing else may consume or overwrite the latched data.
	if err := h.a.markDirty(cephfsLatchTestIno, 300, cephmsgr.MDSTime{Seconds: 60}, 1, false, 0, nil); err == nil {
		t.Fatal("a new write overwrote latched buffered data")
	}
	if _, _, err := h.a.flushDirty(cephfsLatchTestIno); err == nil {
		t.Fatal("fsync flushed metadata over latched buffered data")
	}
	h.a.mu.Lock()
	c, dirty := h.a.caps[cephfsLatchTestIno], h.a.dirty[cephfsLatchTestIno]
	dirty.Dirty = cephCapFileWR
	snapErr := h.a.queueCapSnapLocked(c, dirty, cephmsgr.OSDSnapContext{Sequence: 1})
	dirty.Dirty = 0
	h.a.mu.Unlock()
	if snapErr == nil {
		t.Fatal("a cap snap captured latched buffered data")
	}
	if err := h.a.recoverBuffered(cephfsLatchTestIno, func() error { t.Fatal("uncertain data resent"); return nil }); err == nil {
		t.Fatal("recovery of an uncertain latch admitted")
	}

	// The explicit eviction releases FILE_BUFFER and keeps the outcome.
	before := len(h.sentCaps())
	msg, sent2, err := h.a.evictBuffered(cephfsLatchTestIno)
	if err != nil || !sent2 || msg.Caps&cephCapFileBuffer != 0 || len(h.sentCaps()) != before+1 || h.implemented()&cephCapFileBuffer != 0 {
		t.Fatal("eviction did not release the revoked FILE_BUFFER", msg, sent2, err)
	}
	d, _ = h.a.dirtyState(cephfsLatchTestIno)
	if d.Buffered || d.Latch != nil || d.WriteBack != nil || len(d.Resolved) != 1 || d.Resolved[0].Decision != "evicted" || d.Resolved[0].Outcome != cephfsWriteUncertain ||
		!slices.Equal(d.Resolved[0].Extents, []string{cephfsWriteCompleted, cephfsWriteUncertain}) || d.Resolved[0].Decided.IsZero() || len(h.a.latchedFiles()) != 0 {
		t.Fatalf("eviction lost the latch outcome: %+v", d)
	}
	if _, _, err := h.a.evictBuffered(cephfsLatchTestIno); err == nil {
		t.Fatal("a second eviction was admitted")
	}
	h.push(cephmsgr.MDSCapOpGrant, all)
	h.push(cephmsgr.MDSCapOpRevoke, revoked)
	time.Sleep(20 * time.Millisecond)
	if h.calls.Load() != 1 || h.writebackEvents() != 1 || len(h.a.errs) != 0 {
		t.Fatal("evicted data was written back", h.calls.Load(), h.a.errs)
	}
}

func TestCephFSRevokeWritebackLatchRecoversOnlyKnownUnapplied(t *testing.T) {
	h := newCephFSLatchHarness(t)
	h.buffer(&cephfsWritebackStopped{Phase: "b", Outcomes: []string{cephfsWriteCompleted, cephfsWriteRefused}})
	all := cephCapsForMode(cephFileModeRDWR) | cephCapPin
	revoked := all &^ (cephCapFileWR | cephCapFileBuffer | cephCapFileExcl)
	h.push(cephmsgr.MDSCapOpRevoke, revoked)
	if l := h.waitLatch().Latch; l.Outcome != cephfsLatchKnownUnapplied || !slices.Equal(l.Extents, []string{cephfsWriteCompleted, cephfsWriteRefused}) {
		t.Fatal("known-unapplied latch differs", l)
	}
	if err := h.a.recoverBuffered(cephfsLatchTestIno, nil); err == nil {
		t.Fatal("recovery without a chosen writeback admitted")
	}
	// A failed recovery latches again with its own source and outcome.
	again := &cephfsWritebackStopped{Phase: "b-retry", Outcomes: []string{cephfsWriteFenced}}
	var retries int
	if err := h.a.recoverBuffered(cephfsLatchTestIno, func() error { retries++; return again }); !errors.Is(err, again) {
		t.Fatal(err)
	}
	d, _ := h.a.dirtyState(cephfsLatchTestIno)
	if retries != 1 || h.calls.Load() != 1 || d.Latch == nil || d.Latch.Source != "explicit-recovery" || d.Latch.Outcome != cephfsLatchKnownUnapplied ||
		len(d.Resolved) != 1 || d.Resolved[0].Decision != "recovered" || !d.Buffered || h.implemented()&cephCapFileBuffer == 0 {
		t.Fatalf("failed recovery was not latched again: %+v", d)
	}
	h.push(cephmsgr.MDSCapOpGrant, all)
	h.push(cephmsgr.MDSCapOpRevoke, revoked)
	time.Sleep(20 * time.Millisecond)
	if retries != 1 || h.calls.Load() != 1 {
		t.Fatal("a push re-ran a latched recovery")
	}
	// A successful explicit recovery completes the data and releases Fb.
	before := len(h.sentCaps())
	if err := h.a.recoverBuffered(cephfsLatchTestIno, func() error { retries++; return nil }); err != nil {
		t.Fatal(err)
	}
	d, _ = h.a.dirtyState(cephfsLatchTestIno)
	sent := h.sentCaps()
	if retries != 2 || d.Buffered || d.Latch != nil || len(d.Resolved) != 2 || len(sent) != before+1 || sent[len(sent)-1].Caps&cephCapFileBuffer != 0 ||
		h.implemented()&cephCapFileBuffer != 0 {
		t.Fatalf("explicit recovery did not complete the buffer: %+v %+v", d, sent)
	}
	if err := h.a.recoverBuffered(cephfsLatchTestIno, func() error { return nil }); err == nil {
		t.Fatal("recovery without a latch admitted")
	}
}

func TestCephFSRevokeWritebackLatchClassifiesAndCarriesOutcomes(t *testing.T) {
	for _, tc := range []struct {
		err     error
		outcome string
		extents []string
	}{
		{errors.New("osd write failed"), cephfsWriteUncertain, nil},
		{fmt.Errorf("write: %w", &cephmsgr.OutcomeUnknownError{Cause: context.DeadlineExceeded}), cephfsWriteUncertain, nil},
		{&cephfsWritebackStopped{}, cephfsWriteUncertain, nil},
		{&cephfsWritebackStopped{Outcomes: []string{cephfsWriteCompleted}}, cephfsWriteUncertain, []string{cephfsWriteCompleted}},
		{&cephfsWritebackStopped{Outcomes: []string{cephfsWriteRefused}}, cephfsLatchKnownUnapplied, []string{cephfsWriteRefused}},
		{fmt.Errorf("wrapped: %w", &cephfsWritebackStopped{Outcomes: []string{cephfsWriteCompleted, cephfsWriteFenced}}), cephfsLatchKnownUnapplied,
			[]string{cephfsWriteCompleted, cephfsWriteFenced}},
		{&cephfsWritebackStopped{Outcomes: []string{cephfsWriteRefused, cephfsWriteUncertain}}, cephfsWriteUncertain, []string{cephfsWriteRefused, cephfsWriteUncertain}},
		{&cephfsWritebackStopped{Outcomes: []string{"unexpected"}}, cephfsWriteUncertain, []string{"unexpected"}},
	} {
		outcome, extents := cephfsLatchOutcome(tc.err)
		if outcome != tc.outcome || !slices.Equal(extents, tc.extents) {
			t.Fatal(tc.err, outcome, extents)
		}
	}

	// A handoff to a reconnected agent carries the latch by value.
	h := newCephFSLatchHarness(t)
	h.buffer(&cephfsWritebackStopped{Outcomes: []string{cephfsWriteUncertain}})
	h.push(cephmsgr.MDSCapOpRevoke, (cephCapsForMode(cephFileModeRDWR)|cephCapPin)&^(cephCapFileWR|cephCapFileBuffer|cephCapFileExcl))
	h.waitLatch()
	next := newCephFSCapAgentFrom(context.Background(), nil, h.a)
	carried := next.dirty[cephfsLatchTestIno]
	if carried == nil || carried.Latch == nil || !carried.Buffered || carried.Latch.Outcome != cephfsWriteUncertain || !slices.Equal(next.latchedFiles(), []uint64{cephfsLatchTestIno}) {
		t.Fatal("reconnect handoff lost the latch", carried)
	}
	carried.Latch.Extents[0], carried.Latch.Ignored = "changed", 9
	if d, _ := h.a.dirtyState(cephfsLatchTestIno); d.Latch.Extents[0] != cephfsWriteUncertain || d.Latch.Ignored != 0 {
		t.Fatal("handoff aliases the previous agent's latch")
	}
	snap := h.a.snapshot()
	snap.Dirty[0].Latch.Extents[0] = "changed"
	if d, _ := h.a.dirtyState(cephfsLatchTestIno); d.Latch.Extents[0] != cephfsWriteUncertain {
		t.Fatal("snapshot aliases the live latch")
	}
}

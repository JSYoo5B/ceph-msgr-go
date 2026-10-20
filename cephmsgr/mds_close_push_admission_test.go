package cephmsgr

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
)

// Pause the active writer and fill its bounded queue with protocol controls.
// A following Close must snapshot its sequence and wait for queue admission;
// the independent read loop remains able to accept counted pushes.
func mdsClosePushBlockedWriter(t *testing.T) (*MDSConnection, *MDSSession, *mdsTestPeer, context.Context, func()) {
	t.Helper()
	_, m, s, peer, ctx := cephfsOpen(t)
	// cephfsOpen observes RENEWCAPS, which may precede OPEN's queued ACK.
	// Drain that actual ACK before pausing the following FlushJournal write.
	if err := peer.waitForACK(ctx, 1); err != nil {
		t.Fatal("OPEN ACK did not drain before writer pause", err)
	}
	pause := peer.pauseNextClientWrite()
	var once sync.Once
	release := func() { once.Do(func() { close(pause.release) }) }
	t.Cleanup(release)
	if err := s.FlushJournal(ctx); err != nil {
		t.Fatal(err)
	}
	mdsAdmissionAwait(t, pause.started)
	for range 64 {
		if err := s.FlushJournal(ctx); err != nil {
			t.Fatal("fill ordered writer queue", err)
		}
	}
	return m, s, peer, ctx, release
}

func mdsClosePushDuringAdmission(t *testing.T, s *MDSSession, peer *mdsTestPeer, ctx context.Context) {
	t.Helper()
	// Done may also be evaluated while acquiring the session gate. Wait for
	// Close's actual selection and sequence snapshot before sending the push.
	// The paused writer and full queue keep admission blocked at that point.
	selectedCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	for {
		s.mu.Lock()
		selected, state := s.closeSent, s.state
		s.mu.Unlock()
		if selected && state == MDSSessionOpen {
			break
		}
		select {
		case <-selectedCtx.Done():
			t.Fatal("Close did not select its pre-admission sequence", selectedCtx.Err(), selected, state)
		case <-time.After(time.Millisecond):
		}
	}
	grant := mds.EncodeCaps(mds.Caps{Op: mds.CapOpGrant, Ino: 0x10000000001, CapID: 3, Seq: 1, Caps: 0x55, Xattrs: []byte{0, 0, 0, 0}, Files: -1, Subdirs: -1}, 0)
	peer.send(t, grant)
	event, err := s.Next(ctx)
	s.mu.Lock()
	push, state, selected := s.push, s.state, s.closeSent
	s.mu.Unlock()
	if err != nil || event.Caps == nil || push != 1 || state != MDSSessionOpen || !selected {
		t.Fatal("push did not land at the selected, pre-admission Close boundary", event, err, push, state, selected)
	}
}

func mdsClosePushDrain(t *testing.T, peer *mdsTestPeer) {
	t.Helper()
	for range 65 {
		mdsSessionNext(t, peer, mds.SessionRequestFlushMDLog)
	}
}

func TestMDSClosePushDuringQueueAdmissionResendsCurrentSequence(t *testing.T) {
	_, s, peer, ctx, release := mdsClosePushBlockedWriter(t)
	observed := mdsAdmissionObserve(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Close(observed) }()
	mdsAdmissionAwait(t, observed.entered)
	mdsClosePushDuringAdmission(t, s, peer, ctx)
	release()
	mdsClosePushDrain(t, peer)
	if first := mdsSessionNext(t, peer, mds.SessionRequestClose); first.Sequence != 0 {
		t.Fatal("first CLOSE did not retain its selected sequence", first)
	}
	// The native server drops that stale sequence without an answer. No
	// further push or second public Close call may be needed to send seq 1.
	if next := mdsSessionNext(t, peer, mds.SessionRequestClose); next.Sequence != 1 {
		t.Fatal("CLOSE did not catch up with the pre-admission push", next)
	}
	peer.send(t, mdsSessionReply(mds.SessionClose, 1))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("confirmed CLOSE failed", err)
		}
	case <-ctx.Done():
		t.Fatal("Close remained stuck after current sequence was confirmed", ctx.Err())
	}
}

func TestMDSClosePushFailedAdmissionRollsBackWithoutResend(t *testing.T) {
	_, s, peer, ctx, release := mdsClosePushBlockedWriter(t)
	firstCtx, cancel := context.WithCancel(ctx)
	observed := mdsAdmissionObserve(firstCtx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- s.Close(observed) }()
	mdsAdmissionAwait(t, observed.entered)
	mdsClosePushDuringAdmission(t, s, peer, ctx)
	cancel()
	select {
	case err := <-firstDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("unadmitted Close did not preserve cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s.mu.Lock()
	selected, state := s.closeSent, s.state
	s.mu.Unlock()
	if selected || state != MDSSessionOpen {
		t.Fatal("failed Close admission changed lifecycle", selected, state)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- s.Close(ctx) }()
	release()
	mdsClosePushDrain(t, peer)
	if next := mdsSessionNext(t, peer, mds.SessionRequestClose); next.Sequence != 1 {
		t.Fatal("new explicit Close did not snapshot the already accepted push", next)
	}
	peer.send(t, mdsSessionReply(mds.SessionClose, 1))
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A failed first admission must not trigger a stale automatic CLOSE after
	// the confirmed second one. Bound this negative wire observation locally.
	select {
	case extra := <-peer.requests:
		t.Fatal("failed Close admission generated another request", extra.Type)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestMDSClosePushLossBeforeAdmissionDoesNotResend(t *testing.T) {
	_, s, peer, ctx, release := mdsClosePushBlockedWriter(t)
	closeCtx, cancel := context.WithCancel(ctx)
	observed := mdsAdmissionObserve(closeCtx)
	done := make(chan error, 1)
	go func() { done <- s.Close(observed) }()
	mdsAdmissionAwait(t, observed.entered)
	mdsClosePushDuringAdmission(t, s, peer, ctx)
	loss := errors.New("session loss after pre-admission counted push")
	s.lost(loss)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, loss) || !errors.Is(err, ErrMDSSessionActive) {
			t.Fatal("Close replaced the first session-loss cause", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if s.Status().State != MDSSessionLost {
		t.Fatal("Close revived a lost session")
	}
	release()
	mdsClosePushDrain(t, peer)
	select {
	case extra := <-peer.requests:
		t.Fatal("lost session generated an automatic CLOSE", extra.Type)
	case <-time.After(20 * time.Millisecond):
	}
}

// Force the competing schedule: a pending standalone ACK, rather than an
// application message, owns the paused writer and leaves all 64 controls in
// its queue. The Close helper must drain OPEN's ACK before installing pause.
func TestMDSClosePushWriterPauseCanSelectStandaloneACK(t *testing.T) {
	_, _, s, peer, ctx := cephfsOpen(t)
	if err := peer.waitForACK(ctx, 1); err != nil {
		t.Fatal(err)
	}
	pause := peer.pauseNextClientWrite()
	var once sync.Once
	release := func() { once.Do(func() { close(pause.release) }) }
	t.Cleanup(release)
	// OPEN's initial RENEWCAPS request is sequence 1. Its reply does not
	// trigger another application/control write, so ACK2 owns this pause.
	peer.send(t, mdsSessionReply(mds.SessionRenewCaps, 1))
	mdsAdmissionAwait(t, pause.started)
	if got := peer.ackedServerSequence.Load(); got != 1 {
		t.Fatal("paused ACK unexpectedly reached the peer", got)
	}
	for range 64 {
		if err := s.FlushJournal(ctx); err != nil {
			t.Fatal("ACK pause did not leave room for exactly 64 queued controls", err)
		}
	}
	// A cancellable 65th admission proves the writer did not consume the
	// first FlushJournal. Its cancellation must not enqueue another control.
	blockedCtx, cancel := context.WithCancel(ctx)
	observed := mdsAdmissionObserve(blockedCtx)
	blocked := make(chan error, 1)
	go func() { blocked <- s.FlushJournal(observed) }()
	mdsAdmissionAwait(t, observed.entered)
	cancel()
	select {
	case err := <-blocked:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("65th control was not blocked behind ACK", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	release()
	if err := peer.waitForACK(ctx, 2); err != nil {
		t.Fatal("released ACK was not observed", err)
	}
	for range 64 {
		mdsSessionNext(t, peer, mds.SessionRequestFlushMDLog)
	}
}

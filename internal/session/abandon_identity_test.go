package session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// abandonContextBarrier preserves the underlying Context's cancellation
// result. It only pauses the cancellation continuation at the existing Err
// evaluation outside Session.mu, after call's last empty-result peek. This
// models a scheduler pause without sleeping or adding production test hooks.
type abandonContextBarrier struct {
	context.Context
	entered, release chan struct{}
	once             sync.Once
}

func (c *abandonContextBarrier) Err() error {
	err := c.Context.Err()
	if err != nil {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return err
}
func awaitAbandonIdentity(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded abandonment synchronization timed out")
	}
}

func TestAbandonOldCallAtCannotRemoveReusedTransaction(t *testing.T) {
	s, reader, writer := testSession(t)
	s.Start()
	oldBase, cancelOld := context.WithCancel(context.Background())
	blocked := &abandonContextBarrier{Context: oldBase, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocked.release) }) }
	newCtx, cancelNew := context.WithTimeout(context.Background(), 2*time.Second)
	var calls sync.WaitGroup
	defer func() {
		cancelOld()
		cancelNew()
		release()
		s.Fail(ErrClosed)
		joined := make(chan struct{})
		go func() { calls.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Error("bounded CallAt cleanup timed out")
		}
	}()
	type outcome struct {
		message msgr.MessageData
		err     error
	}
	oldDone, newDone := make(chan outcome, 1), make(chan outcome, 1)
	const tid = uint64(41)
	calls.Add(1)
	go func() {
		defer calls.Done()
		m, err := s.CallAt(blocked, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("old")}, tid)
		oldDone <- outcome{m, err}
	}()
	oldWire := readRequest(t, reader)
	if oldWire.Transaction != tid || string(oldWire.Front) != "old" {
		t.Fatal("old caller was not sent under its explicit identity", oldWire)
	}
	cancelOld()
	awaitAbandonIdentity(t, blocked.entered)
	// The old caller has committed to its cancellation branch but has not yet
	// acquired abandon's mutex. Dispatch can remove that old pending request.
	reply(t, writer, oldWire, 1)
	idleCtx, stopIdle := context.WithTimeout(context.Background(), time.Second)
	if err := s.WaitIdle(idleCtx); err != nil {
		stopIdle()
		t.Fatal("old response was not dispatched", err)
	}
	stopIdle()
	calls.Add(1)
	go func() {
		defer calls.Done()
		m, err := s.CallAt(newCtx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("replacement")}, tid)
		newDone <- outcome{m, err}
	}()
	replacementWire := readRequest(t, reader)
	if replacementWire.Transaction != tid || string(replacementWire.Front) != "replacement" || replacementWire.Sequence != oldWire.Sequence+1 {
		t.Fatal("replacement CallAt was not independently sent", replacementWire)
	}
	s.mu.Lock()
	replacement := s.pending[tid]
	s.mu.Unlock()
	if replacement == nil || !replacement.started {
		t.Fatal("replacement was not pending and started")
	}
	release()
	var old outcome
	select {
	case old = <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old abandonment did not finish")
	}
	var unknown *OutcomeUnknownError
	if !errors.Is(old.err, context.Canceled) || !errors.As(old.err, &unknown) || old.message.Transaction != tid {
		t.Fatal("old started cancellation changed its identity or unknown classification", old)
	}
	s.mu.Lock()
	live := s.pending[tid]
	s.mu.Unlock()
	if live != replacement {
		t.Fatal("old abandonment deleted the replacement pending request with the reused transaction ID")
	}
	// WaitIdle must continue tracking the new owner of this TID until its own
	// response arrives; the old cancellation must not publish false idleness.
	probe, stopProbe := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.WaitIdle(probe)
	stopProbe()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("replacement was incorrectly reported idle", err)
	}
	reply(t, writer, replacementWire, 2)
	select {
	case got := <-newDone:
		if got.err != nil || got.message.Transaction != tid || string(got.message.Front) != "replacement" {
			t.Fatal("replacement response was lost or reassigned", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement response was not delivered")
	}
	if s.Err() != nil {
		t.Fatal("old cancellation failed the shared session", s.Err())
	}
}

func TestAbandonPreservesReplacementAndOriginalFailure(t *testing.T) {
	for _, started := range []bool{false, true} {
		s, _, _ := testSession(t)
		cause := errors.New("original writer refusal")
		old := &request{ctx: context.Background(), message: msgr.MessageData{Transaction: 73}, started: started, result: make(chan response, 1)}
		if !started {
			old.completedErr = cause
		}
		replacement := &request{ctx: context.Background(), message: msgr.MessageData{Transaction: 73, Front: []byte("new owner")}, result: make(chan response, 1)}
		s.pending[73] = replacement
		err := s.abandon(73, old, context.Canceled)
		var unknown *OutcomeUnknownError
		if started && (!errors.Is(err, context.Canceled) || !errors.As(err, &unknown)) || !started && !errors.Is(err, cause) {
			t.Fatal("old completion classification changed", started, err)
		}
		s.mu.Lock()
		live := s.pending[73]
		s.mu.Unlock()
		if live != replacement || string(live.message.Front) != "new owner" {
			t.Fatal("old abandonment touched a different request owner", started)
		}
	}
}

package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type admissionContext struct {
	context.Context
	check func() error
}

func (c admissionContext) Err() error { return c.check() }

func TestContextAdmissionAllowsSessionInspection(t *testing.T) {
	s, reader, writer := testSession(t)
	s.Start()
	var checks atomic.Uint32
	var locked atomic.Bool
	ctx := admissionContext{Context: context.Background(), check: func() error {
		checks.Add(1)
		// Detect the old deadlock without stranding the test's writer.
		if !s.mu.TryLock() {
			locked.Store(true)
			return nil
		}
		s.mu.Unlock()
		return s.Err()
	}}
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
		done <- err
	}()
	m := readRequest(t, reader)
	reply(t, writer, m, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if checks.Load() < 2 {
		t.Fatal("writer did not check the command context")
	}
	if locked.Load() {
		t.Fatal("context callback ran while the session lock was held")
	}
}

// Pause the writer's context check after it observes a live context. Cancellation
// or shutdown must be able to remove the request while that callback is running.
func pausedAdmissionContext(t *testing.T) (context.Context, context.CancelFunc, <-chan struct{}, func()) {
	t.Helper()
	base, cancel := context.WithCancel(context.Background())
	entered, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	var checks atomic.Uint32
	ctx := admissionContext{Context: base, check: func() error {
		err := base.Err()
		if checks.Add(1) == 2 {
			close(entered)
			<-released
		}
		return err
	}}
	t.Cleanup(func() { cancel(); release() })
	return ctx, cancel, entered, release
}

func awaitAdmission(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not reach the context check")
	}
}

func TestCancellationDuringContextAdmissionNeverTransmits(t *testing.T) {
	s, reader, writer := testSession(t)
	s.Start()
	ctx, cancel, entered, release := pausedAdmissionContext(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("canceled")})
		done <- err
	}()
	awaitAdmission(t, entered)
	cancel()
	select {
	case err := <-done:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("unstarted cancellation lost its known outcome", err)
		}
	case <-time.After(time.Second):
		t.Fatal("context callback prevented cancellation")
	}
	release()
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
		done <- err
	}()
	m := readRequest(t, reader)
	if string(m.Front) != "live" || m.Sequence != 1 {
		t.Fatal("canceled command was transmitted", m.Sequence, string(m.Front))
	}
	reply(t, writer, m, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDuringContextAdmissionKeepsKnownOutcome(t *testing.T) {
	s, _, _ := testSession(t)
	s.Start()
	ctx, _, entered, release := pausedAdmissionContext(t)
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage})
		done <- err
	}()
	awaitAdmission(t, entered)
	closed := make(chan struct{})
	go func() { s.Fail(ErrClosed); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("context callback prevented shutdown")
	}
	var unknown *OutcomeUnknownError
	if err := <-done; !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
		t.Fatal("unstarted shutdown lost its known outcome", err)
	}
	release()
	s.Wait()
	if s.sent.Load() != 0 {
		t.Fatal("closed request consumed a sequence number")
	}
}

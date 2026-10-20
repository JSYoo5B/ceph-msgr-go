package cephmsgr

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// Done is evaluated only after the caller reaches its admission select. A
// full slot/queue fixes that boundary without scheduler sleeps.
type mdsAdmissionObservedContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (c *mdsAdmissionObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func mdsAdmissionObserve(ctx context.Context) *mdsAdmissionObservedContext {
	return &mdsAdmissionObservedContext{Context: ctx, entered: make(chan struct{})}
}

func mdsAdmissionAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("admission boundary was not reached")
	}
}

func mdsAdmissionSession(t *testing.T) (*Client, *MDSSession) {
	t.Helper()
	local, remote := net.Pipe()
	transport := session.New(&session.Transport{Conn: local}, session.Config{}, nil, nil)
	t.Cleanup(func() { transport.Fail(session.ErrClosed); remote.Close(); transport.Wait() })
	c := &Client{options: Options{EnableCephFS: true, MaxFrameSize: 1 << 20}, calls: make(chan struct{}, 1)}
	m := &MDSConnection{client: c, session: transport}
	s := &MDSSession{conn: m, state: MDSSessionOpen, changed: make(chan struct{}), done: make(chan struct{}),
		open: MDSSessionOpenInfo{SupportedFeatures: MDSCephFSClientFeatures()}}
	m.cephfs = s
	return c, s
}

func TestMDSSubmitSlotWaitCannotEnterClosingSession(t *testing.T) {
	c, s := mdsAdmissionSession(t)
	c.calls <- struct{}{} // A holds the only shared slot.
	base, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx := mdsAdmissionObserve(base)
	type result struct {
		call *MDSRequestCall
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		call, err := s.Submit(ctx, MDSRequest{Op: MDSOpSetFileLock})
		resultCh <- result{call, err}
	}()
	mdsAdmissionAwait(t, ctx.entered)
	closeCtx, stop := context.WithTimeout(base, 20*time.Millisecond)
	err := s.Close(closeCtx) // No writer: CLOSE is queued, but never answered.
	stop()
	if !errors.Is(err, context.DeadlineExceeded) || s.Status().State != MDSSessionClosing {
		t.Fatal("Close did not establish the closing boundary", err, s.Status())
	}
	<-c.calls // A completes after CLOSE admission.
	select {
	case got := <-resultCh:
		if got.call != nil || got.err == nil {
			t.Error("slot waiter admitted a new request after REQUEST_CLOSE", got.err)
		}
		// Leave the pre-fix reproducer bounded and release any wrongly admitted call.
		s.mu.Lock()
		if got.call != nil {
			s.resolveLocked(got.call, ErrMDSSessionClosed)
		}
		tracked := len(s.requests)
		s.mu.Unlock()
		if tracked != 0 || len(c.calls) != 0 {
			t.Error("closing refusal leaked tracking or slots", tracked, len(c.calls))
		}
	case <-base.Done():
		t.Fatal("slot waiter failed to resolve", base.Err())
	}
}

func TestMDSCloseCannotOvertakeTrackedRequestAdmission(t *testing.T) {
	c, s, cancelSubmit, submitResult := mdsSubmitBlockedAtAdmission(t)
	base, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelClose()
	ctx := mdsAdmissionObserve(base)
	closeResult := make(chan error, 1)
	go func() { closeResult <- s.Close(ctx) }()
	mdsAdmissionAwait(t, ctx.entered)
	s.mu.Lock()
	selected, state := s.closeSent, s.state
	s.mu.Unlock()
	if selected || state != MDSSessionOpen {
		t.Error("Close selected REQUEST_CLOSE while an earlier tracked request still awaited queue admission", selected, state)
	}
	cancelClose()
	select {
	case err := <-closeResult:
		if !errors.Is(err, context.Canceled) {
			t.Error("Close gate wait ignored its context", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not leave its admission gate on cancellation")
	}
	cancelSubmit()
	select {
	case err := <-submitResult:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Error("known-unsent request cancellation changed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tracked request did not leave queue admission on cancellation")
	}
	s.mu.Lock()
	tracked, selected, state := len(s.requests), s.closeSent, s.state
	s.mu.Unlock()
	if tracked != 0 || len(c.calls) != 0 || selected || state != MDSSessionOpen {
		t.Fatal("canceled admission left a slot, close selection or session transition", tracked, len(c.calls), selected, state)
	}
}

func TestMDSCloseFailedQueueAdmissionAllowsNewRequest(t *testing.T) {
	c, s := mdsAdmissionSession(t)
	for range 64 {
		if err := s.conn.session.SendControl(context.Background(), msgr.MessageData{Type: mds.ClientSessionMessage}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("full queue did not refuse Close", err)
	}
	base, stop := context.WithTimeout(context.Background(), 3*time.Second)
	observed := mdsAdmissionObserve(base)
	result := make(chan error, 1)
	go func() { _, err := s.Submit(observed, MDSRequest{Op: MDSOpGetattr}); result <- err }()
	mdsAdmissionAwait(t, observed.entered)
	// The shared-slot select may be the first boundary; wait for registration
	// to prove the failed Close did not prevent a new request from entering.
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		tracked, selected, state := len(s.requests), s.closeSent, s.state
		s.mu.Unlock()
		if tracked == 1 && !selected && state == MDSSessionOpen {
			break
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatal("Close admission failure poisoned new Submit", tracked, selected, state)
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("new Submit remained stuck")
	}
	if len(c.calls) != 0 {
		t.Fatal("new Submit leaked its slot")
	}
}

func TestMDSCloseGateWaitDoesNotBlockSessionLoss(t *testing.T) {
	c, s, cancelSubmit, submitResult := mdsSubmitBlockedAtAdmission(t)
	s.mu.Lock()
	tracked := s.requests[1]
	s.mu.Unlock()
	base, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	ctx := mdsAdmissionObserve(base)
	closeResult := make(chan error, 1)
	go func() { closeResult <- s.Close(ctx) }()
	mdsAdmissionAwait(t, ctx.entered)
	loss := errors.New("session lost while request admission holds the Close gate")
	resolved := make(chan struct{})
	go func() { s.lost(loss); close(resolved) }()
	mdsAdmissionAwait(t, resolved)
	select {
	case err := <-closeResult:
		if !errors.Is(err, loss) || !errors.Is(err, ErrMDSSessionActive) {
			t.Error("Close lost the terminal cause while waiting for admission", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close gate wait did not wake on session loss")
	}
	if len(c.calls) != 0 {
		t.Error("terminal resolver did not release the tracked slot")
	}
	cancelSubmit()
	select {
	case err := <-submitResult:
		if !errors.Is(err, context.Canceled) {
			t.Error("known-unsent admission failure changed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request admission did not resolve after cancellation")
	}
	s.mu.Lock()
	cause, remaining := tracked.err, len(s.requests)
	s.mu.Unlock()
	var unknown *OutcomeUnknownError
	if !errors.As(cause, &unknown) || !errors.Is(cause, loss) || remaining != 0 || len(c.calls) != 0 {
		t.Fatal("admission cleanup changed the first unknown cause or slot ownership", cause, remaining, len(c.calls))
	}
}

func TestMDSAdmissionConfirmedClosePreservesOtherOwners(t *testing.T) {
	for _, kind := range []string{"slot", "gate"} {
		t.Run(kind, func(t *testing.T) {
			c, s := mdsAdmissionSession(t)
			s.requestAdmission = make(chan struct{}, 1)
			s.requestAdmission <- struct{}{} // A different admission owner.
			c.calls <- struct{}{}            // A different shared-slot owner.
			base, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ctx := mdsAdmissionObserve(base)
			result := make(chan error, 1)
			go func() {
				if kind == "slot" {
					result <- s.acquireSlot(ctx)
				} else {
					result <- s.lockRequestAdmission(ctx)
				}
			}()
			mdsAdmissionAwait(t, ctx.entered)
			// Exercise the actual CLOSE handler rather than inventing terminal
			// state: it stores ErrMDSSessionClosed and wakes admission waiters.
			if err := s.dispatch(mds.ClientSession{Op: mds.SessionClose}, time.Now()); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, ErrMDSSessionClosed) {
					t.Error("confirmed CLOSE became successful admission", err)
				}
			case <-base.Done():
				t.Fatal("confirmed CLOSE did not wake admission", base.Err())
			}
			if len(c.calls) != 1 || len(s.requestAdmission) != 1 || len(s.requests) != 0 {
				t.Fatal("terminal refusal changed another owner's gate/slot or tracking", len(c.calls), len(s.requestAdmission), len(s.requests))
			}
		})
	}
}

package cephmsgr

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// Keep the ordered transport queue full without starting its writer. This
// fixes the boundary after an MDS call is tracked but before SendControl
// admits it, without depending on network timing or a large allocation.
func mdsSubmitBlockedAtAdmission(t *testing.T) (*Client, *MDSSession, context.CancelFunc, <-chan error) {
	t.Helper()
	local, remote := net.Pipe()
	transport := session.New(&session.Transport{Conn: local}, session.Config{}, nil, nil)
	t.Cleanup(func() { transport.Fail(session.ErrClosed); remote.Close(); transport.Wait() })
	for range 64 { // internal/session.New's bounded ordered writer queue.
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		err := transport.SendControl(ctx, msgr.MessageData{Type: mds.ClientSessionMessage})
		stop()
		if err != nil {
			t.Fatal("could not fill the deterministic admission queue", err)
		}
	}
	c := &Client{options: Options{EnableCephFS: true, MaxFrameSize: 1 << 20}, calls: make(chan struct{}, 1)}
	m := &MDSConnection{client: c, session: transport}
	s := &MDSSession{conn: m, state: MDSSessionOpen, generation: 1, changed: make(chan struct{}), done: make(chan struct{}),
		open: MDSSessionOpenInfo{SupportedFeatures: MDSCephFSClientFeatures()}}
	m.cephfs = s
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		_, err := s.Submit(ctx, MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}})
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		tracked := len(s.requests)
		s.mu.Unlock()
		if tracked == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Submit did not reach its tracked, pre-admission boundary")
		}
		time.Sleep(time.Millisecond)
	}
	return c, s, cancel, result
}

func TestMDSSubmitLossBeforeAdmissionReleasesSlotOnce(t *testing.T) {
	c, s, cancel, result := mdsSubmitBlockedAtAdmission(t)
	s.mu.Lock()
	tracked := s.requests[1]
	s.mu.Unlock()
	// Session termination resolves the tracked call and returns its one slot
	// before the blocked SendControl returns a local cancellation error.
	loss := errors.New("synthetic session loss before request admission")
	s.lost(loss)
	if len(c.calls) != 0 {
		t.Fatal("Lost did not return the tracked request slot")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || len(c.calls) != 0 {
			t.Fatal("Submit admitted a canceled request or retained a slot", err, len(c.calls))
		}
		s.mu.Lock()
		cause := tracked.err
		s.mu.Unlock()
		var unknown *OutcomeUnknownError
		if !errors.As(cause, &unknown) || !errors.Is(cause, loss) {
			t.Fatal("admission cleanup overwrote the first session-loss outcome", cause)
		}
	case <-time.After(time.Second):
		// Bound the pre-fix failure and release the blocked second receive so
		// the regression leaves no goroutine behind. A correct implementation
		// reaches the result case without this cleanup-only token.
		c.calls <- struct{}{}
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("Submit remained stuck after the bounded regression cleanup")
		}
		t.Fatal("Submit tried to return an already-released in-flight slot and blocked after cancellation")
	}
}

func TestMDSSubmitLossBeforeAdmissionCannotStealAnotherSlot(t *testing.T) {
	c, s, cancel, result := mdsSubmitBlockedAtAdmission(t)
	s.lost(errors.New("synthetic session loss before request admission"))
	// Another session of this same Client may acquire the freed shared slot
	// before the old Submit's admission wait returns. Its token is not owned
	// by this call, even though both slots use the same client-wide channel.
	c.calls <- struct{}{}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("Submit admitted a canceled request")
		}
	case <-time.After(time.Second):
		t.Fatal("Submit did not return its admission cancellation")
	}
	if len(c.calls) != 1 {
		t.Fatal("failed Submit consumed another session's in-flight slot")
	}
	<-c.calls
}

func TestMDSSubmitCanceledBeforeAdmissionReturnsSlot(t *testing.T) {
	c, s, cancel, result := mdsSubmitBlockedAtAdmission(t)
	cancel()
	select {
	case err := <-result:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("known-unsent cancellation became an unknown execution outcome", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Submit did not return its known-unsent cancellation")
	}
	s.mu.Lock()
	tracked, state := len(s.requests), s.state
	s.mu.Unlock()
	if tracked != 0 || len(c.calls) != 0 || state != MDSSessionOpen {
		t.Fatal("ordinary admission cancellation leaked tracking/slots or ended the session", tracked, len(c.calls), state)
	}
}

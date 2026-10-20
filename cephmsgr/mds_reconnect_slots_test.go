package cephmsgr

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

func reconnectSlotFixture(t *testing.T, count, queued int) (*Client, *MDSConnection, *MDSSession, MDSReconnectState) {
	t.Helper()
	local, remote := net.Pipe()
	transport := session.New(&session.Transport{Conn: local}, session.Config{}, nil, nil)
	t.Cleanup(func() { transport.Fail(session.ErrClosed); remote.Close(); transport.Wait() })
	for range queued {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := transport.SendControl(ctx, msgr.MessageData{Type: mds.ClientSessionMessage})
		cancel()
		if err != nil {
			t.Fatal("could not stage the bounded writer queue", err)
		}
	}
	c := &Client{ctx: context.Background(), options: Options{EnableCephFS: true, MaxFrameSize: 4096},
		auth: &cephx.Client{GlobalID: 20}, calls: make(chan struct{}, 4)}
	old := &MDSConnection{client: c, globalID: 20, target: MDSTarget{FilesystemID: 7, Rank: 0}, address: msgr.Address{Nonce: 1}}
	lost := &MDSSession{conn: old, state: MDSSessionLost, generation: 1, open: MDSSessionOpenInfo{SupportedFeatures: allMDSCephFSFeatures}}
	m := &MDSConnection{client: c, globalID: 20, target: old.target, address: msgr.Address{Nonce: 2}, ctx: context.Background(), session: transport}
	var state MDSReconnectState
	for i := range count {
		state.Resubmit = append(state.Resubmit, &MDSRequestCall{session: lost, transaction: uint64(7 + i), op: MDSOpGetattr, resolved: true,
			err: &OutcomeUnknownError{Cause: errors.New("old session lost")}, request: MDSRequest{Op: MDSOpGetattr, Path: MDSFilePath{Ino: 1}}})
	}
	return c, m, lost, state
}

func TestCephFSReconnectPrePrepareRefusalReturnsSlots(t *testing.T) {
	transportFailure := errors.New("new transport already failed")
	for _, test := range []struct {
		name string
		edit func(*Client, *MDSConnection)
		want error
	}{
		{"existing-session", func(_ *Client, m *MDSConnection) { m.cephfs = &MDSSession{} }, ErrMDSSessionExists},
		{"closed-connection", func(_ *Client, m *MDSConnection) { m.closed = true }, ErrClosed},
		{"changed-identity", func(c *Client, _ *MDSConnection) { c.auth = &cephx.Client{GlobalID: 21} }, ErrMDSIdentityChanged},
		{"failed-transport", func(_ *Client, m *MDSConnection) { m.session.Fail(transportFailure) }, transportFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, m, lost, state := reconnectSlotFixture(t, 2, 0)
			test.edit(c, m)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			s, calls, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
			var unknown *OutcomeUnknownError
			if s != nil || calls != nil || !errors.Is(err, test.want) || errors.As(err, &unknown) {
				t.Fatal("pre-prepare refusal changed its known outcome", s, calls, err)
			}
			if n := len(c.calls); n != 0 {
				t.Errorf("pre-prepare refusal leaked %d reserved request slots", n)
				for range n { // Bound the pre-fix regression without leaked state.
					<-c.calls
				}
			}
		})
	}
}

func TestCephFSReconnectCanceledReservationPreservesOtherSlots(t *testing.T) {
	c, m, lost, state := reconnectSlotFixture(t, 3, 0)
	// Two slots belong to independent work. This reconnect can acquire only
	// two of its three reservations before cancellation, and owns only those.
	c.calls <- struct{}{}
	c.calls <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(c.calls) != 4 {
		if time.Now().After(deadline) {
			t.Fatal("reconnect did not wait on its final slot reservation")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || len(c.calls) != 2 || m.cephfs != nil {
			t.Fatal("partial reservation rollback consumed unrelated slots or opened a session", err, len(c.calls), m.cephfs)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect did not roll back its partial reservations")
	}
	<-c.calls
	<-c.calls
}

func TestCephFSReconnectPreparePrefixAndSuffixReleaseOnce(t *testing.T) {
	for _, loseFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancellation", true: "loss-before-cancellation"}[loseFirst], func(t *testing.T) {
			// One remaining queue slot admits the first resend. The second
			// resend is tracked but cannot enter the writer; the third remains
			// an acquired, unregistered suffix reservation.
			c, m, lost, state := reconnectSlotFixture(t, 3, 63)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, _, err := m.ReconnectSession(ctx, lost, state, MDSSessionOptions{RenewInterval: time.Hour})
				result <- err
			}()
			var opening *MDSSession
			deadline := time.Now().Add(time.Second)
			for {
				c.mu.Lock()
				opening = m.cephfs
				c.mu.Unlock()
				if opening != nil {
					opening.mu.Lock()
					tracked, admitted := len(opening.requests), opening.queued
					opening.mu.Unlock()
					if tracked == 2 && admitted {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("reconnect did not reach its admitted-prefix/tracked-request/unregistered-suffix boundary")
				}
				time.Sleep(time.Millisecond)
			}
			if loseFirst {
				opening.lost(errors.New("new session lost after its first resend was admitted"))
			}
			cancel()
			select {
			case err := <-result:
				var unknown *OutcomeUnknownError
				if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) || len(c.calls) != 0 {
					t.Fatal("partial admission lost uncertainty or released reservations incorrectly", err, len(c.calls))
				}
			case <-time.After(time.Second):
				// Release only the stuck regression's extra receives, then join
				// it so a failure cannot leave an unbounded worker behind.
				for range 4 {
					select {
					case c.calls <- struct{}{}:
					default:
					}
				}
				select {
				case <-result:
				case <-time.After(time.Second):
					t.Fatal("reconnect remained stuck after bounded regression cleanup")
				}
				t.Fatal("partial reconnect tried to return a reservation more than once")
			}
		})
	}
}

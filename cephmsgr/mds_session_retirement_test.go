package cephmsgr

import (
	"errors"
	"testing"
	"time"
)

func TestMDSSessionTerminalPublicationFollowsSlotRetirement(t *testing.T) {
	for _, state := range []MDSSessionState{MDSSessionRejected, MDSSessionClosed, MDSSessionLost} {
		t.Run(stateName(state), func(t *testing.T) {
			c := &Client{}
			m := &MDSConnection{client: c}
			s := &MDSSession{conn: m, state: MDSSessionOpening, changed: make(chan struct{}), done: make(chan struct{})}
			m.cephfs = s
			cause := errors.New("terminal session")
			unresolved := state == MDSSessionLost
			c.mu.Lock()
			started, finished := make(chan struct{}), make(chan struct{})
			go func() {
				close(started)
				s.finish(state, cause, unresolved)
				close(finished)
			}()
			<-started
			// Keep retirement blocked: publishing done here would let a caller
			// observe termination before the connection slot/address is retired.
			early := false
			select {
			case <-s.done:
				early = true
			case <-time.After(50 * time.Millisecond):
			}
			c.mu.Unlock()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("session retirement did not finish")
			}
			if early {
				t.Fatal("terminal state was published before the connection slot/address was retired")
			}
			if m.cephfs != nil || c.mdsUnresolved[m.address] != unresolved {
				t.Fatal("incorrect retired connection slot or uncertain address")
			}
			if got := s.Status(); got.State != state || !errors.Is(got.Err, cause) {
				t.Fatal("terminal state or cause changed", got)
			}
		})
	}
}

func stateName(state MDSSessionState) string {
	switch state {
	case MDSSessionRejected:
		return "rejected"
	case MDSSessionClosed:
		return "closed"
	default:
		return "lost"
	}
}

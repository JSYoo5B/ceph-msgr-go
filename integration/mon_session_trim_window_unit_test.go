package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The window in which a lost connection is attributed to the kill starts at
// an answered in-quorum status and nowhere else, and it bounds the observed
// loss, not only the kill request.
func TestMonSessionTrimWindowIsAnchoredToAnInQuorumStatus(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	peon := monElectionStatus{Name: "b", State: "peon", Quorum: []int{0, 1, 2}}
	leader := monElectionStatus{Name: "b", State: "leader", Quorum: []int{1, 2}}
	probing := monElectionStatus{Name: "b", State: "probing"}
	electing := monElectionStatus{Name: "b", State: "electing"}
	if !peon.inQuorum() || !leader.inQuorum() || probing.inQuorum() || electing.inQuorum() ||
		(monElectionStatus{State: "peon"}).inQuorum() || (monElectionStatus{}).inQuorum() {
		t.Fatal("in-quorum classification")
	}

	// The first poll after the peers stopped already shows the MON out of
	// quorum. Without any in-quorum observation nothing is admitted.
	var unanchored monTrimWindow
	unanchored.observe(start, probing)
	if err := unanchored.admits("kill requested", start); err == nil {
		t.Fatal("a window without an in-quorum observation admitted a kill")
	}

	// With the initial check as the only in-quorum observation, the origin
	// stays there: the stops and the out-of-quorum poll do not move it.
	var window monTrimWindow
	window.observe(start, peon)
	window.observe(start.Add(9*time.Second), probing)
	window.observe(start.Add(9*time.Second), electing)
	if !window.origin.Equal(start) {
		t.Fatal("an out-of-quorum status moved the origin", window.origin)
	}
	if err := window.admits("kill requested", start.Add(9*time.Second+time.Millisecond)); err == nil {
		t.Fatal("slow stops before an immediately out-of-quorum poll were not counted")
	}

	// A later in-quorum status moves the origin forward, an earlier one
	// does not move it back, and the limit is inclusive.
	window.observe(start.Add(4*time.Second), leader)
	window.observe(start.Add(2*time.Second), peon)
	if !window.origin.Equal(start.Add(4 * time.Second)) {
		t.Fatal("origin is not the latest in-quorum status request", window.origin)
	}
	origin := window.origin
	if err := window.admits("kill requested", origin.Add(monSessionTrimGrace)); err != nil {
		t.Fatal(err)
	}
	if err := window.admits("kill requested", origin.Add(monSessionTrimGrace+time.Nanosecond)); err == nil {
		t.Fatal("an event after the limit was admitted")
	}
	if err := window.admits("kill requested", origin.Add(-time.Nanosecond)); err == nil {
		t.Fatal("an event before the origin was admitted")
	}

	// Delayed control: the kill is requested in time, the controller stalls
	// and the connection ends when the MON could already have trimmed it.
	requested := origin.Add(time.Second)
	if err := window.admits("kill requested", requested); err != nil {
		t.Fatal(err)
	}
	if err := window.admits("connection lost", requested.Add(8*time.Second)); err == nil {
		t.Fatal("a connection lost after a stalled control was attributed to the kill")
	}
	if err := window.admits("connection lost", requested.Add(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
}

// The stop of the last peer and the subject's quorum loss are awaited
// independently.
func TestMonQuorumLossWaitReportsTheStopResult(t *testing.T) {
	peon := monElectionStatus{Name: "b", State: "peon", Quorum: []int{1, 2}}
	probing := monElectionStatus{Name: "b", State: "probing"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The stop fails at once and the subject stays in quorum: the control
	// error is returned promptly, not a quorum timeout.
	refused := errors.New("control refused")
	stopped := make(chan error, 1)
	stopped <- refused
	polls := 0
	began := time.Now()
	last, err := awaitMonQuorumLossDuringStop(ctx, stopped, time.Millisecond, func() monElectionStatus {
		polls++
		return peon
	})
	if !errors.Is(err, refused) || polls != 0 || last.inQuorum() || time.Since(began) > time.Second {
		t.Fatal("a stop that had already failed was not reported before polling", err, polls, time.Since(began))
	}
	// The same failure arriving while the subject is polled in quorum.
	stopped = make(chan error, 1)
	polls = 0
	last, err = awaitMonQuorumLossDuringStop(ctx, stopped, time.Millisecond, func() monElectionStatus {
		if polls++; polls == 3 {
			stopped <- refused
		}
		return peon
	})
	if !errors.Is(err, refused) || polls != 3 || !last.inQuorum() {
		t.Fatal("a failed stop did not end the wait while the subject was in quorum", err, polls, last)
	}

	// Quorum loss is seen first: polling ends, the wait for the stop does
	// not, and it returns only once the stop is acknowledged.
	stopped = make(chan error, 1)
	polls = 0
	done := make(chan error, 1)
	go func() {
		_, err := awaitMonQuorumLossDuringStop(ctx, stopped, time.Millisecond, func() monElectionStatus {
			polls++
			return probing
		})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("quorum loss alone ended the wait for the stop", err)
	case <-time.After(50 * time.Millisecond):
	}
	stopped <- nil
	if err := <-done; err != nil || polls != 1 {
		t.Fatal("acknowledged stop after the quorum loss", err, polls)
	}

	// The stop is acknowledged first and the subject leaves later.
	stopped = make(chan error, 1)
	stopped <- nil
	polls = 0
	last, err = awaitMonQuorumLossDuringStop(ctx, stopped, time.Millisecond, func() monElectionStatus {
		if polls++; polls < 4 {
			return peon
		}
		return probing
	})
	if err != nil || polls != 4 || !last.outOfQuorum() {
		t.Fatal("quorum loss after an acknowledged stop", err, polls, last)
	}

	// Neither a missing acknowledgement nor a subject that stays in quorum
	// outlives the context, and the error says which one is missing.
	expired, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	stopped = make(chan error, 1)
	stopped <- nil
	if _, err := awaitMonQuorumLossDuringStop(expired, stopped, time.Millisecond, func() monElectionStatus { return peon }); !errors.Is(err, context.DeadlineExceeded) ||
		err.Error() != "stop acknowledged=true, subject out of quorum=false: context deadline exceeded" {
		t.Fatal("a subject that stayed in quorum", err)
	}
	if _, err := awaitMonQuorumLossDuringStop(expired, make(chan error), time.Millisecond, func() monElectionStatus { return probing }); !errors.Is(err, context.DeadlineExceeded) ||
		err.Error() != "stop acknowledged=false, subject out of quorum=true: context deadline exceeded" {
		t.Fatal("a stop that was never acknowledged", err)
	}
}

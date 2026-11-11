package integration_test

import (
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

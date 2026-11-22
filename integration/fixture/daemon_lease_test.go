package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// leaseClock is a settable clock; nothing in these tests waits on time.
type leaseClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *leaseClock) Now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *leaseClock) set(t time.Time) { c.mu.Lock(); c.now = t; c.mu.Unlock() }

// fakeDaemons records the pauses and resumes of the leases' daemons.
type fakeDaemons struct {
	mu      sync.Mutex
	paused  map[string]bool
	calls   []string
	failure map[string]error
	// uncertain pauses freeze the daemon and return its hold with an error.
	uncertain map[string]error
	// block holds a call ("pause osd.2") until its channel is closed.
	block map[string]chan struct{}
}

// wait holds a call that the test blocked, without the fake's lock.
func (d *fakeDaemons) wait(call string) {
	d.mu.Lock()
	release := d.block[call]
	d.mu.Unlock()
	if release != nil {
		<-release
	}
}

// hold blocks call until the returned function releases it.
func (d *fakeDaemons) hold(call string) (release func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.block == nil {
		d.block = map[string]chan struct{}{}
	}
	ch := make(chan struct{})
	d.block[call] = ch
	return func() { close(ch) }
}

type fakeHold struct {
	d      *fakeDaemons
	daemon string
}

func (h fakeHold) Resume(context.Context) error {
	h.d.wait("resume " + h.daemon)
	h.d.mu.Lock()
	defer h.d.mu.Unlock()
	h.d.calls = append(h.d.calls, "resume "+h.daemon)
	if err := h.d.failure["resume "+h.daemon]; err != nil {
		return err
	}
	delete(h.d.paused, h.daemon)
	return nil
}

func (d *fakeDaemons) pause(_ context.Context, kind, name string) (daemonHold, error) {
	daemon := kind + "." + name
	d.wait("pause " + daemon)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, "pause "+daemon)
	if err := d.failure["pause "+daemon]; err != nil {
		return nil, err
	}
	if d.paused == nil {
		d.paused = map[string]bool{}
	}
	d.paused[daemon] = true
	return fakeHold{d, daemon}, d.uncertain["pause "+daemon]
}

func (d *fakeDaemons) record() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

func (d *fakeDaemons) isPaused(daemon string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.paused[daemon]
}

var leaseEpoch = time.Unix(1_800_000_000, 0)

func newTestLeases() (*daemonLeases, *leaseClock, *fakeDaemons) {
	clock := &leaseClock{now: leaseEpoch}
	daemons := &fakeDaemons{}
	admitted := func(kind string) bool { return kind == "mon" || kind == "mgr" || kind == "osd" }
	return newDaemonLeases(clock.Now, admitted, daemons.pause), clock, daemons
}

// The requests below are the exact text internal/testcluster DaemonLease
// publishes (its TestDaemonLeaseRequestsKeepTheFixtureWireFormat pins the
// same lines), in the envelope of their request ID.
func publishPause(t *testing.T, out string, request, lease int64, daemon string, deadline time.Time) {
	t.Helper()
	kind, name, _ := strings.Cut(daemon, ".")
	writeControlRequest(t, out, "pause-daemon", strconv.FormatInt(request, 10)+" "+strconv.FormatInt(lease, 10)+" "+kind+" "+name+" "+strconv.FormatInt(deadline.UnixNano(), 10)+"\n")
}

func publishEnd(t *testing.T, out string, request, lease int64, why string) {
	t.Helper()
	writeControlRequest(t, out, "end-daemon-lease", strconv.FormatInt(request, 10)+" "+strconv.FormatInt(lease, 10)+" "+why+"\n")
}

// leaseAck reads one acknowledgement of a lease; ok is false when it is
// absent.
func leaseAck(t *testing.T, out, state string, lease int64) (daemonLeaseRecord, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(out, "daemon-lease-"+state+"."+strconv.FormatInt(lease, 10)))
	if errors.Is(err, os.ErrNotExist) {
		return daemonLeaseRecord{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var r daemonLeaseRecord
	if err = json.Unmarshal(data, &r); err != nil {
		t.Fatal(string(data), err)
	}
	return r, true
}

// pollSettled polls once and waits until every daemon call the poll
// started has settled, and returns the failures of the poll and of those
// calls.
func pollSettled(s *daemonLeases, out string) error {
	err := s.poll(context.Background(), out)
	s.calls.Wait()
	s.mu.Lock()
	reported := s.reported
	s.reported = nil
	s.mu.Unlock()
	return errors.Join(err, errors.Join(reported...))
}

func pollLeases(t *testing.T, s *daemonLeases, out string) {
	t.Helper()
	if err := pollSettled(s, out); err != nil {
		t.Fatal(err)
	}
}

// A pause runs once claimed, every transition is acknowledged under the
// lease's ID, and its end names the lease, not the daemon.
func TestDaemonLeasePausesAndEndsByLeaseID(t *testing.T) {
	out := t.TempDir()
	s, clock, daemons := newTestLeases()
	deadline := leaseEpoch.Add(10 * time.Second)
	publishPause(t, out, 11, 7, "osd.2", deadline)
	pollLeases(t, s, out)
	requested, ok := leaseAck(t, out, leaseRequested, 7)
	if !ok || requested.Daemon != "osd.2" || requested.Deadline != deadline.UnixNano() {
		t.Fatal("requested acknowledgement", requested, ok)
	}
	paused, ok := leaseAck(t, out, leasePaused, 7)
	if !ok || paused.State != leasePaused || paused.PausedAt != leaseEpoch.UnixNano() || !daemons.isPaused("osd.2") {
		t.Fatal("paused acknowledgement", paused, ok)
	}
	if _, ok := leaseAck(t, out, leaseEnded, 7); ok || requestPending(t, out, "pause-daemon") {
		t.Fatal("the lease ended or its pause stayed pending")
	}
	clock.set(leaseEpoch.Add(2 * time.Second))
	publishEnd(t, out, 12, 7, "resume")
	pollLeases(t, s, out)
	ended, ok := leaseAck(t, out, leaseEnded, 7)
	if !ok || ended.Reason != leaseResumed || ended.End != "resume" || ended.EndedAt != clock.Now().UnixNano() || daemons.isPaused("osd.2") {
		t.Fatal("ended acknowledgement", ended, ok)
	}
	// A second end is acknowledged again and resumes nothing.
	publishEnd(t, out, 13, 7, "cleanup")
	pollLeases(t, s, out)
	if again, _ := leaseAck(t, out, leaseEnded, 7); again != ended || !slices.Equal(daemons.record(), []string{"pause osd.2", "resume osd.2"}) {
		t.Fatal("a second end changed the lease", again, daemons.record())
	}
}

// An end that arrives before its pause leaves the lease ended, and the late
// pause is refused, not executed: when the end was served in an earlier
// poll, and when both are pending in one poll in either publication order.
func TestDaemonLeaseRefusesAPauseAfterItsEnd(t *testing.T) {
	for _, order := range []string{"earlier poll", "same poll, end first", "same poll, pause first"} {
		t.Run(order, func(t *testing.T) {
			out := t.TempDir()
			s, _, daemons := newTestLeases()
			deadline := leaseEpoch.Add(10 * time.Second)
			switch order {
			case "earlier poll":
				publishEnd(t, out, 21, 9, "cancel")
				pollLeases(t, s, out)
				if r, ok := leaseAck(t, out, leaseEnded, 9); !ok || r.Reason != leaseEndedBeforePause || r.End != "cancel" {
					t.Fatal("the end before the pause", r, ok)
				}
				publishPause(t, out, 22, 9, "osd.1", deadline)
			case "same poll, end first":
				publishEnd(t, out, 21, 9, "cancel")
				publishPause(t, out, 22, 9, "osd.1", deadline)
			case "same poll, pause first":
				// The pause was published first but its publisher was
				// overtaken: the end is still served before it.
				publishPause(t, out, 21, 9, "osd.1", deadline)
				publishEnd(t, out, 22, 9, "cancel")
			}
			pollLeases(t, s, out)
			pollLeases(t, s, out)
			if calls := daemons.record(); len(calls) != 0 {
				t.Fatal("a pause after its lease's end was executed", calls)
			}
			if requestPending(t, out, "pause-daemon") || requestPending(t, out, "end-daemon-lease") {
				t.Fatal("a request stayed pending")
			}
			if r, ok := leaseAck(t, out, leaseEnded, 9); !ok || r.Reason != leaseEndedBeforePause {
				t.Fatal("the lease is not ended before its pause", r, ok)
			}
			for _, state := range []string{leaseRequested, leasePaused} {
				if _, ok := leaseAck(t, out, state, 9); ok {
					t.Fatal("the refused pause was acknowledged as", state)
				}
			}
		})
	}
}

// Two leases with one deadline are both resumed by the fixture once it
// passes, with no request from the test, and each is acknowledged under its
// own ID.
func TestDaemonLeasesWithOneDeadlineBothExpire(t *testing.T) {
	out := t.TempDir()
	s, clock, daemons := newTestLeases()
	deadline := leaseEpoch.Add(16 * time.Second)
	publishPause(t, out, 31, 101, "osd.1", deadline)
	publishPause(t, out, 32, 102, "osd.2", deadline)
	pollLeases(t, s, out)
	if !daemons.isPaused("osd.1") || !daemons.isPaused("osd.2") {
		t.Fatal("both leases were not paused", daemons.record())
	}
	clock.set(deadline.Add(-time.Nanosecond))
	pollLeases(t, s, out)
	if _, ok := leaseAck(t, out, leaseEnded, 101); ok || !daemons.isPaused("osd.1") {
		t.Fatal("a lease ended before its deadline")
	}
	clock.set(deadline)
	pollLeases(t, s, out)
	for _, lease := range []int64{101, 102} {
		r, ok := leaseAck(t, out, leaseEnded, lease)
		if !ok || r.Reason != leaseExpired || r.End != "" || r.EndedAt != deadline.UnixNano() {
			t.Fatal("lease", lease, "did not expire at its deadline", r, ok)
		}
	}
	if daemons.isPaused("osd.1") || daemons.isPaused("osd.2") {
		t.Fatal("a daemon stayed paused past its deadline", daemons.record())
	}
	// The test's later end finds the lease ended by its deadline.
	publishEnd(t, out, 33, 101, "resume")
	pollLeases(t, s, out)
	if r, _ := leaseAck(t, out, leaseEnded, 101); r.Reason != leaseExpired || len(daemons.record()) != 4 {
		t.Fatal("an end after the expiry changed the lease", r, daemons.record())
	}
}

// A pause claimed at or after its deadline, or of a daemon another lease
// holds, is not executed and its lease ends.
func TestDaemonLeaseRefusesAnExpiredOrOverlappingPause(t *testing.T) {
	out := t.TempDir()
	s, clock, daemons := newTestLeases()
	publishPause(t, out, 41, 201, "osd.3", leaseEpoch)
	publishPause(t, out, 42, 202, "mon.a", leaseEpoch.Add(time.Minute))
	publishPause(t, out, 43, 203, "mon.a", leaseEpoch.Add(time.Minute))
	pollLeases(t, s, out)
	if r, ok := leaseAck(t, out, leaseEnded, 201); !ok || r.Reason != leaseExpiredBeforePause {
		t.Fatal("a pause past its deadline", r, ok)
	}
	if r, ok := leaseAck(t, out, leaseEnded, 203); !ok || r.Reason != leaseDaemonHeld {
		t.Fatal("an overlapping pause", r, ok)
	}
	if calls := daemons.record(); !slices.Equal(calls, []string{"pause mon.a"}) {
		t.Fatal("refused pauses were executed", calls)
	}
	clock.set(leaseEpoch.Add(time.Minute))
	pollLeases(t, s, out)
	if r, _ := leaseAck(t, out, leaseEnded, 202); r.Reason != leaseExpired || daemons.isPaused("mon.a") {
		t.Fatal("the holding lease did not expire", r)
	}
}

// A malformed, unadmitted or unbounded request fails the fixture before any
// daemon is paused.
func TestDaemonLeaseRefusesInvalidRequests(t *testing.T) {
	deadline := strconv.FormatInt(leaseEpoch.Add(time.Minute).UnixNano(), 10)
	for name, request := range map[string][2]string{
		"pause fields":      {"pause-daemon", "1 5 osd 2\n"},
		"lease identity":    {"pause-daemon", "1 05 osd 2 " + deadline + "\n"},
		"zero lease":        {"pause-daemon", "1 0 osd 2 " + deadline + "\n"},
		"MDS pause":         {"pause-daemon", "1 5 mds a " + deadline + "\n"},
		"deadline":          {"pause-daemon", "1 5 osd 2 soon\n"},
		"unbounded lease":   {"pause-daemon", "1 5 osd 2 " + strconv.FormatInt(leaseEpoch.Add(maxDaemonLease+time.Second).UnixNano(), 10) + "\n"},
		"end fields":        {"end-daemon-lease", "1 5\n"},
		"end reason":        {"end-daemon-lease", "1 5 later\n"},
		"end identity":      {"end-daemon-lease", "1 x resume\n"},
		"envelope mismatch": {"end-daemon-lease", "2 5 resume\n"},
	} {
		t.Run(name, func(t *testing.T) {
			out := t.TempDir()
			s, _, daemons := newTestLeases()
			kind, content := request[0], request[1]
			if name == "envelope mismatch" {
				if err := os.WriteFile(filepath.Join(out, requestEnvelopeName(kind, "1")), []byte(content), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				writeControlRequest(t, out, kind, content)
			}
			if err := s.poll(context.Background(), out); err == nil || len(daemons.record()) != 0 {
				t.Fatal("accepted", content, err, daemons.record())
			}
			// Rejected under its request ID and taken out of the pending
			// requests: the next poll does not meet it again.
			if data, err := os.ReadFile(filepath.Join(out, "daemon-lease-rejected.1")); err != nil || !strings.Contains(string(data), `"request":"1"`) {
				t.Fatal("the rejection is not acknowledged under its request ID", string(data), err)
			}
			if requestPending(t, out, kind) {
				t.Fatal("the rejected request stayed pending")
			}
			if err := s.poll(context.Background(), out); err != nil {
				t.Fatal("the rejected request failed a second poll", err)
			}
		})
	}
}

// A request the loop rejects fails the fixture without stopping it: in the
// same polls, a held lease past its deadline is still resumed and a valid
// end still ends its lease, the malformed envelope is quarantined instead
// of failing every later poll, and a later pause is refused as after a
// fixture failure.
func TestDaemonLeaseRejectsABadEnvelopeAndKeepsEndingTheHeldLeases(t *testing.T) {
	out := t.TempDir()
	s, clock, daemons := newTestLeases()
	deadline := leaseEpoch.Add(10 * time.Second)
	publishPause(t, out, 71, 501, "osd.1", deadline)
	publishPause(t, out, 72, 502, "osd.2", deadline.Add(time.Minute))
	pollLeases(t, s, out)
	// An envelope whose content names another request ID, and one with an
	// input line, both persistent across polls if not taken out.
	if err := os.WriteFile(filepath.Join(out, requestEnvelopeName("end-daemon-lease", "73")), []byte("74 501 resume\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeControlRequest(t, out, "pause-daemon", "75 503 osd 3 "+strconv.FormatInt(deadline.UnixNano(), 10)+"\ninput x.json\n")
	publishEnd(t, out, 76, 502, "resume")
	clock.set(deadline)
	err := pollSettled(s, out)
	if err == nil || !strings.Contains(err.Error(), "request 73 rejected") || !strings.Contains(err.Error(), "request 75 rejected") {
		t.Fatal("the bad requests were not reported", err)
	}
	if r, ok := leaseAck(t, out, leaseEnded, 501); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.1") {
		t.Fatal("a bad envelope kept a lease paused past its deadline", r, ok)
	}
	if r, ok := leaseAck(t, out, leaseEnded, 502); !ok || r.Reason != leaseResumed || daemons.isPaused("osd.2") {
		t.Fatal("a bad envelope kept a valid end from ending its lease", r, ok)
	}
	for _, id := range []string{"73", "75"} {
		if !fileExists(filepath.Join(out, "daemon-lease-rejected."+id)) {
			t.Fatal("no rejection of request", id)
		}
	}
	if !fileExists(filepath.Join(out, "end-daemon-lease.73.rejected")) || requestPending(t, out, "end-daemon-lease") || requestPending(t, out, "pause-daemon") {
		t.Fatal("a bad envelope was not quarantined")
	}
	if err := pollSettled(s, out); err != nil {
		t.Fatal("a quarantined envelope failed the next poll", err)
	}
	publishPause(t, out, 77, 504, "osd.4", deadline.Add(time.Minute))
	pollLeases(t, s, out)
	if r, ok := leaseAck(t, out, leaseEnded, 504); !ok || r.Reason != leaseFixtureFailed || daemons.isPaused("osd.4") {
		t.Fatal("a pause after a rejected request", r, ok)
	}
}

// settledLeases reports each daemon call that settled, as "<call> <lease>".
func settledLeases(s *daemonLeases) <-chan string {
	settled := make(chan string, 64)
	s.settled = func(call, lease string) { settled <- call + " " + lease }
	return settled
}

// livenessBound only turns a lost settlement into a failure instead of a
// hung test; no assertion depends on it.
const livenessBound = time.Minute

// awaitSettled waits until the daemon call ("resume 601") settles.
func awaitSettled(t *testing.T, settled <-chan string, call string) {
	t.Helper()
	bound := time.After(livenessBound)
	for {
		select {
		case l := <-settled:
			if l == call {
				return
			}
		case <-bound:
			t.Fatal("the daemon call", call, "did not settle")
		}
	}
}

// pollAsync runs one poll on its own goroutine, so that a poll that waited
// for a blocked daemon call is reported instead of hanging the test.
func pollAsync(s *daemonLeases, out string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.poll(context.Background(), out) }()
	return done
}

func awaitPoll(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(livenessBound):
		t.Fatal("the poll waited for an outstanding daemon call")
	}
}

// A daemon call that stays outstanding delays no other lease: an earlier
// lease still expires while another daemon's pause is outstanding, and
// while the resume of a lease with the same deadline is outstanding. A
// pause that settles after its lease was ended, or after its deadline, is
// resumed at once.
func TestDaemonLeaseExpiresWhileAnotherDaemonCallIsOutstanding(t *testing.T) {
	deadline := leaseEpoch.Add(10 * time.Second)
	t.Run("pause outstanding", func(t *testing.T) {
		out := t.TempDir()
		s, clock, daemons := newTestLeases()
		settled := settledLeases(s)
		publishPause(t, out, 81, 601, "osd.1", deadline)
		pollLeases(t, s, out)
		release := daemons.hold("pause osd.2")
		publishPause(t, out, 82, 602, "osd.2", deadline.Add(time.Minute))
		awaitPoll(t, pollAsync(s, out))
		clock.set(deadline)
		awaitPoll(t, pollAsync(s, out))
		awaitSettled(t, settled, "resume 601")
		if r, ok := leaseAck(t, out, leaseEnded, 601); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.1") {
			t.Fatal("an outstanding pause of another daemon delayed the expiry", r, ok)
		}
		if _, ok := leaseAck(t, out, leasePaused, 602); ok {
			t.Fatal("the blocked pause settled before its release")
		}
		release()
		s.calls.Wait()
		if r, ok := leaseAck(t, out, leasePaused, 602); !ok || !daemons.isPaused("osd.2") {
			t.Fatal("the released pause did not settle", r, ok)
		}
	})
	t.Run("resume outstanding, one deadline", func(t *testing.T) {
		out := t.TempDir()
		s, clock, daemons := newTestLeases()
		settled := settledLeases(s)
		publishPause(t, out, 91, 701, "osd.1", deadline)
		publishPause(t, out, 92, 702, "osd.2", deadline)
		pollLeases(t, s, out)
		release := daemons.hold("resume osd.1")
		clock.set(deadline)
		awaitPoll(t, pollAsync(s, out))
		awaitSettled(t, settled, "resume 702")
		if r, ok := leaseAck(t, out, leaseEnded, 702); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.2") {
			t.Fatal("an outstanding resume of a lease with the same deadline delayed the expiry", r, ok)
		}
		if _, ok := leaseAck(t, out, leaseEnded, 701); ok || !daemons.isPaused("osd.1") {
			t.Fatal("the blocked resume settled before its release")
		}
		// A second poll starts no second resume of the outstanding one.
		awaitPoll(t, pollAsync(s, out))
		release()
		s.calls.Wait()
		if r, ok := leaseAck(t, out, leaseEnded, 701); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.1") {
			t.Fatal("the released resume did not end its lease", r, ok)
		}
		if resumes := slices.DeleteFunc(daemons.record(), func(c string) bool { return c != "resume osd.1" }); len(resumes) != 1 {
			t.Fatal("the outstanding resume was started again", daemons.record())
		}
	})
	for _, late := range []string{"ended", "past its deadline"} {
		t.Run("pause settles "+late, func(t *testing.T) {
			out := t.TempDir()
			s, clock, daemons := newTestLeases()
			release := daemons.hold("pause osd.3")
			publishPause(t, out, 101, 801, "osd.3", deadline)
			awaitPoll(t, pollAsync(s, out))
			want, end := leaseExpired, ""
			if late == "ended" {
				publishEnd(t, out, 102, 801, "cancel")
				want, end = leaseResumed, "cancel"
			} else {
				clock.set(deadline)
			}
			awaitPoll(t, pollAsync(s, out))
			release()
			s.calls.Wait()
			if r, ok := leaseAck(t, out, leaseEnded, 801); !ok || r.Reason != want || r.End != end || daemons.isPaused("osd.3") {
				t.Fatal("a late pause was not resumed at once", r, ok, daemons.record())
			}
		})
	}
}

// A pause call that fails but may have frozen the daemon returns a hold;
// the lease resumes it at once and ends as pause-failed, and the failure is
// the fixture's.
func TestDaemonLeaseResumesAnUncertainPause(t *testing.T) {
	out := t.TempDir()
	s, _, daemons := newTestLeases()
	daemons.uncertain = map[string]error{"pause osd.1": errors.New("transport lost")}
	publishPause(t, out, 111, 901, "osd.1", leaseEpoch.Add(time.Minute))
	if err := pollSettled(s, out); err == nil || !strings.Contains(err.Error(), "transport lost") {
		t.Fatal("an uncertain pause was not reported", err)
	}
	if r, ok := leaseAck(t, out, leaseEnded, 901); !ok || r.Reason != leasePauseFailed || r.PausedAt != 0 || daemons.isPaused("osd.1") {
		t.Fatal("an uncertain pause stayed frozen", r, ok, daemons.record())
	}
}

// A failed resume keeps the lease paused and is retried by the next poll. A
// failed pause or resume stops the fixture serving pauses: a later pause
// ends unexecuted, while ends and deadlines are still served.
func TestDaemonLeaseFailuresKeepEndingTheHeldLeases(t *testing.T) {
	t.Run("resume", func(t *testing.T) {
		out := t.TempDir()
		s, clock, daemons := newTestLeases()
		daemons.failure = map[string]error{"resume osd.1": errors.New("unpause failed")}
		publishPause(t, out, 51, 301, "osd.1", leaseEpoch.Add(time.Second))
		pollLeases(t, s, out)
		clock.set(leaseEpoch.Add(time.Second))
		if err := pollSettled(s, out); err == nil || !strings.Contains(err.Error(), "unpause failed") {
			t.Fatal("a failed resume was not reported", err)
		}
		if _, ok := leaseAck(t, out, leaseEnded, 301); ok {
			t.Fatal("a lease whose resume failed was acknowledged as ended")
		}
		daemons.mu.Lock()
		daemons.failure = nil
		daemons.mu.Unlock()
		publishPause(t, out, 52, 302, "osd.2", leaseEpoch.Add(time.Minute))
		pollLeases(t, s, out)
		if r, ok := leaseAck(t, out, leaseEnded, 301); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.1") {
			t.Fatal("the retried resume did not end the lease", r, ok)
		}
		if r, ok := leaseAck(t, out, leaseEnded, 302); !ok || r.Reason != leaseFixtureFailed || daemons.isPaused("osd.2") {
			t.Fatal("a pause after a failed resume", r, ok)
		}
	})
	t.Run("pause", func(t *testing.T) {
		out := t.TempDir()
		s, clock, daemons := newTestLeases()
		daemons.failure = map[string]error{"pause osd.2": errors.New("pause failed")}
		publishPause(t, out, 61, 311, "osd.1", leaseEpoch.Add(time.Second))
		publishPause(t, out, 62, 312, "osd.2", leaseEpoch.Add(time.Minute))
		if err := pollSettled(s, out); err == nil || !strings.Contains(err.Error(), "pause failed") {
			t.Fatal("a failed pause was not reported", err)
		}
		publishPause(t, out, 63, 313, "osd.3", leaseEpoch.Add(time.Minute))
		clock.set(leaseEpoch.Add(time.Second))
		pollLeases(t, s, out)
		if r, ok := leaseAck(t, out, leaseEnded, 311); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.1") {
			t.Fatal("a held lease did not expire after a failed pause", r, ok)
		}
		if r, ok := leaseAck(t, out, leaseEnded, 313); !ok || r.Reason != leaseFixtureFailed || daemons.isPaused("osd.3") {
			t.Fatal("a pause after a failed pause", r, ok)
		}
	})
}

// The lease loop pauses a daemon and ends its lease at the deadline while
// the control loop is still running an owned-pool oracle request, as a
// test's observation of the paused cluster that outlived its deadline. The
// oracle blocks until the test releases it, and the lease loop polls only
// when the test sends a tick, so nothing here depends on timing.
func TestDaemonLeaseExpiresBesideAStalledOwnedPoolOracle(t *testing.T) {
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "osd_pool_oracle.py"), []byte("@oracle_mode\ndef mode_observe(rados, cluster, request, out):\n    return {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	s, clock, daemons := newTestLeases()
	c := &fixtureCluster{profile: profile{osd: "1"}, leases: s, done: make(chan struct{}), leaseDone: make(chan struct{}), runOwnedPool: func(ctx context.Context, out string, args []string, budget time.Duration) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	tick := make(chan time.Time)
	go c.leaseLoop(ctx, out, s, tick)
	go c.controlLoop(ctx, out)
	defer func() {
		cancel()
		<-c.done
		<-c.leaseDone
	}()
	writeControlRequest(t, out, "verify-osd-pool", "61 observe\n")
	<-started
	// Sending a second tick returns only once the loop finished the poll of
	// the first; the daemon calls that poll started are then waited for.
	poll := func() { tick <- time.Time{}; tick <- time.Time{}; s.calls.Wait() }
	deadline := leaseEpoch.Add(16 * time.Second)
	publishPause(t, out, 62, 401, "osd.2", deadline)
	poll()
	if !daemons.isPaused("osd.2") {
		t.Fatal("the pause waited for the outstanding oracle request", daemons.record())
	}
	clock.set(deadline)
	poll()
	if r, ok := leaseAck(t, out, leaseEnded, 401); !ok || r.Reason != leaseExpired || daemons.isPaused("osd.2") {
		t.Fatal("the lease's expiry waited for the outstanding oracle request", r, ok)
	}
	if fileExists(filepath.Join(out, "osd-pool-verify.61")) {
		t.Fatal("the oracle request finished before the lease expired")
	}
	close(release)
	if err := c.checkControl(); err != nil {
		t.Fatal(err)
	}
}

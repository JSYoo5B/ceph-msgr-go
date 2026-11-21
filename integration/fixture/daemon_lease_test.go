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
}

type fakeHold struct {
	d      *fakeDaemons
	daemon string
}

func (h fakeHold) Resume(context.Context) error {
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
	d.mu.Lock()
	defer d.mu.Unlock()
	daemon := kind + "." + name
	d.calls = append(d.calls, "pause "+daemon)
	if err := d.failure["pause "+daemon]; err != nil {
		return nil, err
	}
	if d.paused == nil {
		d.paused = map[string]bool{}
	}
	d.paused[daemon] = true
	return fakeHold{d, daemon}, nil
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

func pollLeases(t *testing.T, s *daemonLeases, out string) {
	t.Helper()
	if err := s.poll(context.Background(), out); err != nil {
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
		})
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
		if err := s.poll(context.Background(), out); err == nil || !strings.Contains(err.Error(), "unpause failed") {
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
		if err := s.poll(context.Background(), out); err == nil || !strings.Contains(err.Error(), "pause failed") {
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
	// the first.
	poll := func() { tick <- time.Time{}; tick <- time.Time{} }
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

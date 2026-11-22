package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// maxDaemonLease is the longest lease the fixture accepts
// (integration/fixture/daemon_lease.go maxDaemonLease), less a margin for
// the time between this deadline and the fixture's claim of the pause.
const maxDaemonLease = 9 * time.Minute

// leaseUntil is the deadline of a lease that may last as long as ctx: ctx's
// deadline, but no later than maxDaemonLease from now.
func leaseUntil(ctx context.Context) time.Time {
	limit := time.Now().Add(maxDaemonLease)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(limit) {
		return deadline
	}
	return limit
}

// leaseDaemon returns a lease of daemon kind.name that ends by deadline
// (testcluster.DaemonLease) and registers its end as a cleanup, before
// anything is published: a pause that failed, was abandoned or is still on
// its way is ended either way, and the fixture refuses a pause it claims
// after its lease ended.
func leaseDaemon(t testing.TB, control, kind, name string, deadline time.Time) *testcluster.DaemonLease {
	t.Helper()
	lease := testcluster.NewDaemonLease(control, kind, name, deadline)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if err := endDaemonLease(cleanup, lease); err != nil {
			t.Error(err)
		}
	})
	return lease
}

// pauseDaemon pauses daemon kind.name under a lease (leaseDaemon) and fails
// the test when the daemon was not paused.
func pauseDaemon(t testing.TB, ctx context.Context, control, kind, name string, deadline time.Time) *testcluster.DaemonLease {
	t.Helper()
	lease := leaseDaemon(t, control, kind, name, deadline)
	if _, err := lease.Pause(ctx); err != nil {
		t.Fatalf("pause %s.%s: %v", kind, name, err)
	}
	return lease
}

// endDaemonLease ends a lease for a restore or a cleanup and waits for the
// fixture's ended acknowledgement, whatever ended it.
func endDaemonLease(ctx context.Context, lease *testcluster.DaemonLease) error {
	if _, err := lease.End(ctx, "cleanup"); err != nil {
		return fmt.Errorf("end the pause of %s.%s: %w", lease.Kind, lease.Name, err)
	}
	return nil
}

// resumeDaemon resumes a leased daemon under ctx, bounded by the lease's
// deadline, and returns the fixture's ended record. It fails unless this
// end resumed the daemon: a lease that the fixture ended at its deadline,
// or that never paused, did not hold the daemon as long as the test
// required.
func resumeDaemon(ctx context.Context, lease *testcluster.DaemonLease) (testcluster.DaemonLeaseRecord, error) {
	resume, stop := context.WithDeadline(ctx, lease.Deadline)
	defer stop()
	r, err := lease.End(resume, "resume")
	if err != nil {
		return r, fmt.Errorf("resume %s.%s by its lease deadline: %w", lease.Kind, lease.Name, err)
	}
	if r.Reason != testcluster.DaemonLeaseResumed {
		return r, &testcluster.DaemonLeaseEndedError{Record: r, Step: "resume"}
	}
	return r, nil
}

// heldPause is a daemon pause whose lease ends by a deadline fixed before
// the pause is published, with a held phase: every control and native
// observation while the daemon is paused runs under held, which ends a
// reserve before the deadline, so that the resume still has its own time.
// An observation still outstanding at the end of the held phase is
// abandoned, and the test fails; the fixture ends the lease by its deadline
// all the same, also while that observation is still running in the
// fixture (integration/fixture/daemon_lease.go), and an abandoned oracle
// request reads only its own input (testcluster.ControlInput).
type heldPause struct {
	lease    *testcluster.DaemonLease
	budget   time.Duration
	held     context.Context
	stopHeld context.CancelFunc
}

// startHeldPause pauses kind.name under a lease ending at deadline, the
// pause itself under the held phase.
func startHeldPause(t testing.TB, parent context.Context, control, kind, name string, deadline time.Time, reserve time.Duration) *heldPause {
	t.Helper()
	p := &heldPause{budget: time.Until(deadline).Round(time.Millisecond)}
	p.held, p.stopHeld = context.WithDeadline(parent, deadline.Add(-reserve))
	t.Cleanup(p.stopHeld)
	p.lease = pauseDaemon(t, p.held, control, kind, name, deadline)
	if err := p.check("the pause request"); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *heldPause) daemon() string { return p.lease.Kind + "." + p.lease.Name }

// check requires step to have ended within the held phase.
func (p *heldPause) check(step string) error {
	if err := p.held.Err(); err != nil {
		return fmt.Errorf("%s ended past the held phase of %s's %v pause budget: %w", step, p.daemon(), p.budget, err)
	}
	return nil
}

// resume ends the held phase and the lease, and returns how long before
// the deadline the fixture resumed the daemon. It fails unless this resume
// ended the lease before its deadline.
func (p *heldPause) resume() (time.Duration, error) {
	p.stopHeld()
	r, err := resumeDaemon(context.Background(), p.lease)
	if err != nil {
		return 0, fmt.Errorf("resume %s within its %v pause budget: %w", p.daemon(), p.budget, err)
	}
	left := time.Duration(r.Deadline - r.EndedAt)
	if left <= 0 {
		return left, fmt.Errorf("%s stayed paused %v past its %v budget", p.daemon(), -left, p.budget)
	}
	return left, nil
}

// leaseRequests returns the pending requests of action in dir, by request
// ID, and claims them as the fixture would.
func leaseRequests(t *testing.T, dir, action string) [][]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, action+".*.request"))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(paths)
	var requests [][]string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, strings.Fields(string(data)))
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	return requests
}

// writeLeaseRecord publishes the fixture's record of a lease.
func writeLeaseRecord(t *testing.T, dir string, r testcluster.DaemonLeaseRecord) {
	t.Helper()
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "daemon-lease-"+r.State+"."+r.Lease), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The test side of a held pause on a real control directory, with the test
// playing the fixture: two leases with one deadline are two requests naming
// their own lease and one deadline, fixed before publication; the held phase
// ends the reserve before it; a pause abandoned at the end of its held
// phase is still ended by the lease's cleanup; and a resume that finds its
// lease ended by the fixture's deadline is refused.
func TestHeldPauseEndsEachLeaseByItsID(t *testing.T) {
	dir := t.TempDir()
	deadline := time.Now().Add(time.Minute)
	const reserve = 20 * time.Second
	t.Run("leases", func(t *testing.T) {
		answered := make(chan struct{})
		go func() {
			defer close(answered)
			for {
				if requests := leaseRequests(t, dir, "pause-daemon"); len(requests) != 0 {
					for _, r := range requests {
						writeLeaseRecord(t, dir, testcluster.DaemonLeaseRecord{Lease: r[1], Daemon: r[2] + "." + r[3], State: testcluster.DaemonLeasePaused, PausedAt: 1, Deadline: deadline.UnixNano()})
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		first := startHeldPause(t, context.Background(), dir, "osd", "1", deadline, reserve)
		<-answered
		answered = make(chan struct{})
		go func() {
			defer close(answered)
			for {
				if requests := leaseRequests(t, dir, "pause-daemon"); len(requests) != 0 {
					if len(requests) != 1 || requests[0][4] != strconv.FormatInt(deadline.UnixNano(), 10) {
						t.Error("the second lease's request", requests)
					}
					writeLeaseRecord(t, dir, testcluster.DaemonLeaseRecord{Lease: requests[0][1], Daemon: "osd.2", State: testcluster.DaemonLeasePaused, PausedAt: 1, Deadline: deadline.UnixNano()})
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		second := startHeldPause(t, context.Background(), dir, "osd", "2", deadline, reserve)
		<-answered
		if first.lease.ID == second.lease.ID || !first.lease.Deadline.Equal(second.lease.Deadline) {
			t.Fatal("two leases of one deadline", first.lease, second.lease)
		}
		if end, ok := first.held.Deadline(); !ok || !end.Equal(deadline.Add(-reserve)) {
			t.Fatal("the held phase ends at", end, ok)
		}
		// The fixture ended the first lease at its deadline: its resume is
		// refused.
		writeLeaseRecord(t, dir, testcluster.DaemonLeaseRecord{Lease: strconv.FormatInt(first.lease.ID, 10), State: testcluster.DaemonLeaseEnded, Reason: testcluster.DaemonLeaseExpired, PausedAt: 1, EndedAt: deadline.UnixNano(), Deadline: deadline.UnixNano()})
		var ended *testcluster.DaemonLeaseEndedError
		if _, err := first.resume(); !errors.As(err, &ended) || ended.Record.Reason != testcluster.DaemonLeaseExpired {
			t.Fatal("a resume after the fixture's expiry", err)
		}
		if first.check("a step after the resume") == nil {
			t.Fatal("the held phase outlived the resume")
		}
		// The second lease is resumed in time.
		go func() {
			for {
				if requests := leaseRequests(t, dir, "end-daemon-lease"); len(requests) != 0 {
					writeLeaseRecord(t, dir, testcluster.DaemonLeaseRecord{Lease: requests[0][1], State: testcluster.DaemonLeaseEnded, Reason: testcluster.DaemonLeaseResumed, PausedAt: 1, EndedAt: deadline.UnixNano() - int64(time.Second), Deadline: deadline.UnixNano()})
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
		if left, err := second.resume(); err != nil || left != time.Second {
			t.Fatal("the second resume", left, err)
		}
		// A pause abandoned with its held phase: only its cleanup ends it.
		abandoned, stop := context.WithCancel(context.Background())
		stop()
		third := leaseDaemon(t, dir, "osd", "3", deadline)
		if _, err := third.Pause(abandoned); !errors.Is(err, context.Canceled) {
			t.Fatal("an abandoned pause", err)
		}
		writeLeaseRecord(t, dir, testcluster.DaemonLeaseRecord{Lease: strconv.FormatInt(third.ID, 10), State: testcluster.DaemonLeaseEnded, Reason: testcluster.DaemonLeaseEndedBeforePause})
	})
	// Every lease was ended by its own acknowledgement; the cleanups
	// published no end for a lease already ended.
	if ends := leaseRequests(t, dir, "end-daemon-lease"); len(ends) != 0 {
		t.Fatal("cleanup ended leases that were already ended", ends)
	}
}

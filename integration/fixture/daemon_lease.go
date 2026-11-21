package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

// A daemon pause is a lease the fixture owns. The test publishes one request
// that carries the lease ID, the daemon and an absolute deadline; the
// fixture keeps the lease's state and ends it by the deadline itself, so no
// test-side process has to publish a resume in time. The protocol, on the
// control directory (internal/testcluster DaemonLease is the test side):
//
//   - "pause-daemon.<request ID>.request" holds
//     "<request ID> <lease ID> <kind> <name> <deadline>", the deadline in
//     Unix nanoseconds. The test and the fixture run on one host and read
//     one wall clock (containers share the host's kernel clock).
//   - "end-daemon-lease.<request ID>.request" holds
//     "<request ID> <lease ID> <why>", why being resume, cancel or cleanup.
//     Ending names the lease, never the daemon.
//   - Every transition is acknowledged as "daemon-lease-<state>.<lease ID>",
//     state requested, paused or ended, holding the lease's record
//     (daemonLeaseRecord). The test treats a lease as settled only on its
//     ended acknowledgement.
//
// The states are requested, paused and ended, and ended is final:
//
//   - An end that arrives before its pause leaves the lease ended
//     (endedBeforePause), and a later pause for that lease is refused, never
//     executed. So a pause published late, after the test already ended its
//     lease, cannot freeze the daemon.
//   - A pause claimed at or after its deadline is not executed
//     (expiredBeforePause).
//   - A pause of a daemon another lease holds is not executed (daemonHeld).
//   - Once the deadline passes, a paused lease is resumed and ended
//     (expired), whether or not the test is still waiting on an observation
//     of the paused cluster.
//
// The leases are served by their own loop (leaseLoop), beside the control
// loop, so a control or owned-pool oracle request that is still running does
// not delay a lease's end or its deadline. Within one poll the pending ends
// run before the pending pauses, and expiry runs last.
//
// Not covered: a daemon that a control stops, kills or restarts while a
// lease holds it. The tests end a lease before they kill its daemon.

// The lease states and the reasons a lease ended.
const (
	leaseRequested = "requested"
	leasePaused    = "paused"
	leaseEnded     = "ended"

	// leaseResumed: an end request resumed the paused daemon.
	leaseResumed = "resumed"
	// leaseExpired: the deadline passed while the daemon was paused, and the
	// fixture resumed it.
	leaseExpired = "expired"
	// leaseEndedBeforePause: the end was claimed before any pause of the
	// lease; the daemon was never paused under it.
	leaseEndedBeforePause = "ended-before-pause"
	// leaseExpiredBeforePause: the pause was claimed at or after its
	// deadline and was not executed.
	leaseExpiredBeforePause = "expired-before-pause"
	// leaseDaemonHeld: another lease held the daemon when the pause was
	// claimed; the pause was not executed.
	leaseDaemonHeld = "daemon-held"
	// leaseFixtureFailed: the fixture had recorded a failure before the
	// pause was claimed; the pause was not executed.
	leaseFixtureFailed = "fixture-failed"
)

// maxDaemonLease bounds a lease from the claim of its pause to its deadline.
// A longer lease is refused as an invalid request.
const maxDaemonLease = 10 * time.Minute

// daemonLeaseRecord is the content of every acknowledgement of a lease.
// Times are Unix nanoseconds, zero when the event did not happen.
type daemonLeaseRecord struct {
	Lease    string `json:"lease"`
	Daemon   string `json:"daemon,omitempty"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	End      string `json:"end,omitempty"`
	Deadline int64  `json:"deadline,omitempty"`
	PausedAt int64  `json:"paused_at,omitempty"`
	EndedAt  int64  `json:"ended_at,omitempty"`
}

// daemonHold is one paused daemon: fault.PauseLease outside the tests.
type daemonHold interface {
	Resume(ctx context.Context) error
}

type daemonLease struct {
	record daemonLeaseRecord
	hold   daemonHold
}

// daemonLeases is the fixture's state of every lease. Only its loop uses it,
// and Terminate after the loop has stopped.
type daemonLeases struct {
	mu     sync.Mutex
	leases map[string]*daemonLease
	// held maps a daemon ("osd.2") to the lease that holds it paused.
	held map[string]*daemonLease
	// failed is the first failure of a pause or resume; no pause runs
	// after it, but ends and expiry still do.
	failed error

	now func() time.Time
	// admitted says whether a pause of the daemon kind is admitted.
	admitted func(kind string) bool
	// pause pauses one daemon.
	pause func(ctx context.Context, kind, name string) (daemonHold, error)
}

func newDaemonLeases(now func() time.Time, admitted func(string) bool, pause func(context.Context, string, string) (daemonHold, error)) *daemonLeases {
	return &daemonLeases{leases: map[string]*daemonLease{}, held: map[string]*daemonLease{}, now: now, admitted: admitted, pause: pause}
}

// fixtureDaemonLeases are the leases of the fixture's own daemons.
func (c *fixtureCluster) fixtureDaemonLeases() *daemonLeases {
	return newDaemonLeases(time.Now, func(kind string) bool {
		return slices.Contains(daemonControlKinds, kind) && c.profile.daemonControlAdmitted(kind, "pause")
	}, func(ctx context.Context, kind, name string) (daemonHold, error) {
		daemon, err := c.daemon(ctx, kind, name)
		if err != nil {
			return nil, err
		}
		lease, err := fault.Pause(ctx, c.cluster, daemon)
		if err != nil {
			return nil, err
		}
		return lease, nil
	})
}

// leaseLoop serves the leases until ctx ends. A failed pause or resume is
// recorded as the fixture's failure (provider.error); the loop keeps
// serving ends and deadlines, so no lease it holds outlives its deadline.
func (c *fixtureCluster) leaseLoop(ctx context.Context, out string, leases *daemonLeases, tick <-chan time.Time) {
	defer close(c.leaseDone)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			if err := leases.poll(ctx, out); err != nil && ctx.Err() == nil {
				c.recordControlFailure(out, err)
			}
		}
	}
}

// poll claims every pending end and then every pending pause, each in
// publication order, and then ends every paused lease whose deadline has
// passed.
func (s *daemonLeases) poll(ctx context.Context, out string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	ends, err := pendingRequests(out, "end-daemon-lease")
	if err != nil {
		return err
	}
	for _, r := range ends {
		fields, err := claim(out, r)
		if err != nil {
			return err
		}
		if err = s.end(ctx, out, fields); err != nil {
			errs = append(errs, err)
		}
	}
	pauses, err := pendingRequests(out, "pause-daemon")
	if err != nil {
		return err
	}
	for _, r := range pauses {
		fields, err := claim(out, r)
		if err != nil {
			return err
		}
		if err = s.start(ctx, out, fields); err != nil {
			errs = append(errs, err)
		}
	}
	errs = append(errs, s.expire(ctx, out))
	return errors.Join(errs...)
}

// leaseID is the closed shape of a lease ID: a positive decimal integer.
func leaseID(field string) (string, error) {
	if id, err := strconv.ParseInt(field, 10, 64); err != nil || id <= 0 || strconv.FormatInt(id, 10) != field {
		return "", errors.New("invalid daemon lease identity")
	}
	return field, nil
}

// start serves one pause request.
func (s *daemonLeases) start(ctx context.Context, out string, fields []string) error {
	if len(fields) != 5 {
		return errors.New("invalid daemon lease pause request")
	}
	id, err := leaseID(fields[1])
	if err != nil {
		return err
	}
	kind, name := fields[2], fields[3]
	if !s.admitted(kind) {
		return fmt.Errorf("a pause lease of a %q daemon is not admitted", kind)
	}
	deadline, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil || deadline <= 0 {
		return errors.New("invalid daemon lease deadline")
	}
	now := s.now()
	if time.Unix(0, deadline).Sub(now) > maxDaemonLease {
		return fmt.Errorf("daemon lease %s ends more than %v after its pause", id, maxDaemonLease)
	}
	if l := s.leases[id]; l != nil {
		// Ended before this pause, or a second pause of one lease: refused.
		if l.record.State != leaseEnded {
			return fmt.Errorf("daemon lease %s was paused twice", id)
		}
		return nil
	}
	l := &daemonLease{record: daemonLeaseRecord{Lease: id, Daemon: kind + "." + name, State: leaseRequested, Deadline: deadline}}
	s.leases[id] = l
	if err = s.publish(out, l); err != nil {
		return err
	}
	switch {
	case s.failed != nil:
		return s.finish(out, l, leaseFixtureFailed, "")
	case !now.Before(time.Unix(0, deadline)):
		return s.finish(out, l, leaseExpiredBeforePause, "")
	case s.held[l.record.Daemon] != nil:
		return s.finish(out, l, leaseDaemonHeld, "")
	}
	hold, err := s.pause(ctx, kind, name)
	if err != nil {
		// The daemon may or may not be paused; the fixture stops serving
		// pauses, and Terminate resumes what the cluster registered.
		s.failed = fmt.Errorf("pause %s under lease %s: %w", l.record.Daemon, id, err)
		return s.failed
	}
	l.hold = hold
	l.record.State = leasePaused
	l.record.PausedAt = s.now().UnixNano()
	s.held[l.record.Daemon] = l
	return s.publish(out, l)
}

// end serves one end request.
func (s *daemonLeases) end(ctx context.Context, out string, fields []string) error {
	if len(fields) != 3 || (fields[2] != "resume" && fields[2] != "cancel" && fields[2] != "cleanup") {
		return errors.New("invalid daemon lease end request")
	}
	id, err := leaseID(fields[1])
	if err != nil {
		return err
	}
	l := s.leases[id]
	switch {
	case l == nil:
		l = &daemonLease{record: daemonLeaseRecord{Lease: id}}
		s.leases[id] = l
		return s.finish(out, l, leaseEndedBeforePause, fields[2])
	case l.record.State == leaseEnded:
		// Its ended acknowledgement stands; publish it again in case the
		// test removed it.
		return s.publish(out, l)
	case l.record.State == leasePaused:
		return s.resume(ctx, out, l, leaseResumed, fields[2])
	}
	return fmt.Errorf("daemon lease %s is %s", id, l.record.State)
}

// expire ends every paused lease whose deadline has passed.
func (s *daemonLeases) expire(ctx context.Context, out string) error {
	now := s.now()
	var errs []error
	for _, daemon := range slices.Sorted(func(yield func(string) bool) {
		for daemon := range s.held {
			if !yield(daemon) {
				return
			}
		}
	}) {
		l := s.held[daemon]
		if !now.Before(time.Unix(0, l.record.Deadline)) {
			errs = append(errs, s.resume(ctx, out, l, leaseExpired, ""))
		}
	}
	return errors.Join(errs...)
}

// resume resumes a paused lease's daemon and ends the lease. A failed resume
// keeps the lease paused, so the next poll tries again; the first failure is
// recorded.
func (s *daemonLeases) resume(ctx context.Context, out string, l *daemonLease, reason, end string) error {
	if err := l.hold.Resume(ctx); err != nil {
		err = fmt.Errorf("resume %s under lease %s: %w", l.record.Daemon, l.record.Lease, err)
		if s.failed == nil {
			s.failed = err
		}
		return err
	}
	delete(s.held, l.record.Daemon)
	l.hold = nil
	return s.finish(out, l, reason, end)
}

// finish ends a lease that holds no daemon.
func (s *daemonLeases) finish(out string, l *daemonLease, reason, end string) error {
	l.record.State = leaseEnded
	l.record.Reason = reason
	l.record.End = end
	l.record.EndedAt = s.now().UnixNano()
	return s.publish(out, l)
}

// publish acknowledges the lease's current state.
func (s *daemonLeases) publish(out string, l *daemonLease) error {
	data, err := json.Marshal(l.record)
	if err != nil {
		return err
	}
	return publishAckData(out, "daemon-lease-"+l.record.State+"."+l.record.Lease, append(data, '\n'))
}

// resumeAll resumes every held daemon, for Terminate once the loop stopped.
func (s *daemonLeases) resumeAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for daemon, l := range s.held {
		if err := l.hold.Resume(ctx); err != nil {
			errs = append(errs, fmt.Errorf("resume %s: %w", daemon, err))
			continue
		}
		delete(s.held, daemon)
	}
	return errors.Join(errs...)
}

// publishAckData publishes an acknowledgement that carries data, atomically.
func publishAckData(out, name string, data []byte) error {
	f, err := os.CreateTemp(out, "ack-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Chmod(0o644); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(out, name))
}

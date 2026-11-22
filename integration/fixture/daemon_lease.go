package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
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
//   - A failed pause call ends the lease (pause-failed), after resuming the
//     daemon when the call may have frozen it.
//
// The leases are served by their own loop (leaseLoop), beside the control
// loop, so a control or owned-pool oracle request that is still running does
// not delay a lease's end or its deadline. Within one poll the pending ends
// run before the pending pauses, and expiry runs last.
//
// A poll never waits for a daemon. Each pause and each resume runs on its
// own goroutine, and the poll only records what each one settled, so a slow
// or stuck Docker call of one lease delays neither the end nor the deadline
// of another lease, also of a lease that shares its deadline. Each call is
// bounded: a pause by its lease's deadline, a resume by
// daemonLeaseResumeBound per attempt. A pause that settles after its lease
// was ended or its deadline passed is resumed at once. A pause takes the
// cluster's topology lock (fault.Pause), which the control loop's daemon
// lookups and topology changes also take, so a pause may wait behind a
// control; only that lease waits, and a resume takes no cluster lock. The
// pause's bound is wall-clock time, like the deadline.
//
// A request the loop cannot serve (a malformed envelope, a malformed or
// unadmitted request) is rejected: its envelope is taken out of the pending
// requests, it is acknowledged as "daemon-lease-rejected.<request ID>" with
// the reason, and the failure is the fixture's (provider.error). The poll
// still serves the other ends and the deadlines, so a bad request never
// keeps a held daemon paused past its deadline.
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
	// leasePauseFailed: the pause call failed; a daemon it may have frozen
	// was resumed. The failure is the fixture's.
	leasePauseFailed = "pause-failed"
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

// daemonLeaseRejected names the acknowledgement of a rejected request.
const daemonLeaseRejected = "rejected"

// daemonLeaseResumeBound bounds one resume call. A resume that fails or
// outlives it is retried by the next poll, and the first failure is the
// fixture's.
const daemonLeaseResumeBound = time.Minute

// daemonHold is one paused daemon: fault.PauseLease outside the tests.
type daemonHold interface {
	Resume(ctx context.Context) error
}

type daemonLease struct {
	record daemonLeaseRecord
	// hold is the paused daemon, nil when the lease holds none.
	hold daemonHold
	// call is the daemon call that is outstanding: "", "pause" or "resume".
	call string
	// endWhy is the end request that named the lease while it still held
	// its daemon: resume, cancel or cleanup.
	endWhy string
	// pauseFailed: the pause call failed but left a hold to resume.
	pauseFailed bool
}

// due says whether a held lease must be resumed now.
func (l *daemonLease) due(now time.Time) bool {
	return l.pauseFailed || l.endWhy != "" || !now.Before(time.Unix(0, l.record.Deadline))
}

// endReason is why a held lease that is due ends, and its end request.
func (l *daemonLease) endReason() (string, string) {
	switch {
	case l.pauseFailed:
		return leasePauseFailed, l.endWhy
	case l.endWhy != "":
		return leaseResumed, l.endWhy
	}
	return leaseExpired, ""
}

// daemonLeases is the fixture's state of every lease, used by its loop, by
// the daemon calls the loop starts, and by Terminate after the loop has
// stopped.
type daemonLeases struct {
	mu     sync.Mutex
	leases map[string]*daemonLease
	// held maps a daemon ("osd.2") to the lease that holds it: its pause is
	// outstanding, it is paused, or its resume is outstanding.
	held map[string]*daemonLease
	// failed is the first failure of the leases; no pause runs after it,
	// but ends and expiry still do.
	failed error
	// reported are the failures of daemon calls the next poll returns.
	reported []error
	// calls counts the outstanding daemon calls.
	calls sync.WaitGroup
	// settled, when set, is told each daemon call ("pause" or "resume") and
	// its lease once the call's outcome is recorded (the tests use it).
	settled func(call, lease string)

	now func() time.Time
	// admitted says whether a pause of the daemon kind is admitted.
	admitted func(kind string) bool
	// pause pauses one daemon. It may return a hold with an error: a hold
	// whose pause outcome is uncertain, which must be resumed.
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
		// fault.Pause returns a handle with an error when Docker may have
		// frozen the container all the same; that handle must be resumed.
		lease, err := fault.Pause(ctx, c.cluster, daemon)
		if lease == nil {
			return nil, err
		}
		return lease, err
	})
}

// leaseLoop serves the leases until ctx ends. A failure is recorded as the
// fixture's failure (provider.error); the loop keeps serving ends and
// deadlines, so no lease it holds outlives its deadline.
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

// leaseRequestError is a request the lease loop rejects.
type leaseRequestError struct{ err error }

func (e *leaseRequestError) Error() string { return e.err.Error() }
func (e *leaseRequestError) Unwrap() error { return e.err }

func invalidLeaseRequest(format string, args ...any) error {
	return &leaseRequestError{fmt.Errorf(format, args...)}
}

// poll claims every pending end and then every pending pause, each in
// publication order, starts the daemon calls they need, and then starts the
// resume of every held lease that is due. It waits for no daemon call. It
// returns the failures of this poll and of the daemon calls that settled
// since the last one; every failure is also the leases' failure.
func (s *daemonLeases) poll(ctx context.Context, out string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	errs := s.reported
	s.reported = nil
	// Each failure is the leases' failure at once, so no pause later in
	// this poll runs after it.
	add := func(err error) {
		if err != nil {
			s.fail(err)
			errs = append(errs, err)
		}
	}
	for _, control := range []string{"end-daemon-lease", "pause-daemon"} {
		pending, rejected, err := scanRequests(out, control)
		add(err)
		for _, r := range rejected {
			add(s.reject(out, r.id, r.path, r.err))
		}
		for _, r := range pending {
			if len(r.inputs) != 0 {
				add(s.reject(out, r.fields[0], r.path, errors.New("a daemon lease request carries no input")))
				continue
			}
			fields, err := claim(out, r)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err == nil {
				if control == "end-daemon-lease" {
					err = s.end(ctx, out, fields)
				} else {
					err = s.start(ctx, out, fields)
				}
			}
			if requestErr := (*leaseRequestError)(nil); errors.As(err, &requestErr) {
				err = s.reject(out, r.fields[0], "", err)
			}
			add(err)
		}
	}
	s.expire(ctx, out)
	return errors.Join(errs...)
}

// reject acknowledges request ID id as rejected, with the reason, and
// takes its envelope, when it is still pending at path, out of the pending
// requests, so that the next poll does not meet it again.
func (s *daemonLeases) reject(out, id, path string, reason error) error {
	err := fmt.Errorf("daemon lease request %s rejected: %w", id, reason)
	errs := []error{err}
	if path != "" {
		if renameErr := os.Rename(path, strings.TrimSuffix(path, ".request")+"."+daemonLeaseRejected); renameErr != nil && !errors.Is(renameErr, os.ErrNotExist) {
			// The envelope stays pending; remove it rather than meet it
			// again.
			errs = append(errs, renameErr, os.Remove(path))
		}
	}
	data, marshalErr := json.Marshal(map[string]string{"request": id, "error": reason.Error()})
	if marshalErr == nil {
		marshalErr = publishAckData(out, "daemon-lease-"+daemonLeaseRejected+"."+id, append(data, '\n'))
	}
	return errors.Join(append(errs, marshalErr)...)
}

// fail records the first failure of the leases.
func (s *daemonLeases) fail(err error) {
	if err != nil && s.failed == nil {
		s.failed = err
	}
}

// report records the failure of a daemon call for the next poll.
func (s *daemonLeases) report(err error) {
	if err != nil {
		s.fail(err)
		s.reported = append(s.reported, err)
	}
}

// leaseID is the closed shape of a lease ID: a positive decimal integer.
func leaseID(field string) (string, error) {
	if id, err := strconv.ParseInt(field, 10, 64); err != nil || id <= 0 || strconv.FormatInt(id, 10) != field {
		return "", invalidLeaseRequest("invalid daemon lease identity")
	}
	return field, nil
}

// start serves one pause request: it starts the pause call and returns
// without waiting for it.
func (s *daemonLeases) start(ctx context.Context, out string, fields []string) error {
	if len(fields) != 5 {
		return invalidLeaseRequest("invalid daemon lease pause request")
	}
	id, err := leaseID(fields[1])
	if err != nil {
		return err
	}
	kind, name := fields[2], fields[3]
	if !s.admitted(kind) {
		return invalidLeaseRequest("a pause lease of a %q daemon is not admitted", kind)
	}
	deadline, err := strconv.ParseInt(fields[4], 10, 64)
	if err != nil || deadline <= 0 {
		return invalidLeaseRequest("invalid daemon lease deadline")
	}
	now := s.now()
	if time.Unix(0, deadline).Sub(now) > maxDaemonLease {
		return invalidLeaseRequest("daemon lease %s ends more than %v after its pause", id, maxDaemonLease)
	}
	if l := s.leases[id]; l != nil {
		// Ended before this pause, or a second pause of one lease: refused.
		if l.record.State != leaseEnded {
			return invalidLeaseRequest("daemon lease %s was paused twice", id)
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
	s.held[l.record.Daemon] = l
	l.call = "pause"
	s.calls.Add(1)
	go s.runPause(ctx, out, l, kind, name)
	return nil
}

// runPause runs one pause call, bounded by the lease's deadline, and
// records its outcome.
func (s *daemonLeases) runPause(ctx context.Context, out string, l *daemonLease, kind, name string) {
	defer s.calls.Done()
	call, cancel := context.WithDeadline(ctx, time.Unix(0, l.record.Deadline))
	hold, err := s.pause(call, kind, name)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.notify("pause", l)
	l.call = ""
	l.hold = hold
	if err != nil {
		// The fixture stops serving pauses. A hold is resumed at once; with
		// none, the daemon is not paused under this lease.
		s.report(fmt.Errorf("pause %s under lease %s: %w", l.record.Daemon, l.record.Lease, err))
		if hold == nil {
			delete(s.held, l.record.Daemon)
			s.report(s.finish(out, l, leasePauseFailed, l.endWhy))
			return
		}
		l.pauseFailed = true
	} else {
		l.record.State = leasePaused
		l.record.PausedAt = s.now().UnixNano()
		s.report(s.publish(out, l))
	}
	// Ended while its pause was outstanding, or past its deadline already.
	if l.due(s.now()) {
		s.startResume(ctx, out, l)
	}
}

// end serves one end request.
func (s *daemonLeases) end(ctx context.Context, out string, fields []string) error {
	if len(fields) != 3 || (fields[2] != "resume" && fields[2] != "cancel" && fields[2] != "cleanup") {
		return invalidLeaseRequest("invalid daemon lease end request")
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
	case l.call == "resume":
		// The resume that ends the lease is outstanding already.
		return nil
	}
	l.endWhy = fields[2]
	if l.call == "" && l.hold != nil {
		s.startResume(ctx, out, l)
	}
	// Otherwise its pause is outstanding, and its settlement resumes it.
	return nil
}

// expire starts the resume of every held lease that is due and has no
// daemon call outstanding: past its deadline, or with an earlier resume or
// a pause that failed.
func (s *daemonLeases) expire(ctx context.Context, out string) {
	now := s.now()
	for _, daemon := range slices.Sorted(maps.Keys(s.held)) {
		if l := s.held[daemon]; l.call == "" && l.hold != nil && l.due(now) {
			s.startResume(ctx, out, l)
		}
	}
}

// startResume starts the resume call of a held lease.
func (s *daemonLeases) startResume(ctx context.Context, out string, l *daemonLease) {
	reason, end := l.endReason()
	l.call = "resume"
	s.calls.Add(1)
	go s.runResume(ctx, out, l, reason, end)
}

// runResume runs one resume call and ends the lease. A failed resume keeps
// the lease held, so the next poll tries again. The call is bounded by
// daemonLeaseResumeBound and outlives the loop's context, so Terminate
// finds the daemon resumed or the hold still to resume.
func (s *daemonLeases) runResume(ctx context.Context, out string, l *daemonLease, reason, end string) {
	defer s.calls.Done()
	call, cancel := context.WithTimeout(context.WithoutCancel(ctx), daemonLeaseResumeBound)
	err := l.hold.Resume(call)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.notify("resume", l)
	l.call = ""
	if err != nil {
		s.report(fmt.Errorf("resume %s under lease %s: %w", l.record.Daemon, l.record.Lease, err))
		return
	}
	delete(s.held, l.record.Daemon)
	l.hold = nil
	s.report(s.finish(out, l, reason, end))
}

func (s *daemonLeases) notify(call string, l *daemonLease) {
	if s.settled != nil {
		s.settled(call, l.record.Lease)
	}
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

// resumeAll resumes every held daemon, for Terminate once the loop
// stopped. It first waits for the outstanding daemon calls: a pause ends
// with the loop's context, and a resume within its bound.
func (s *daemonLeases) resumeAll(ctx context.Context) error {
	s.calls.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for daemon, l := range s.held {
		if l.hold == nil {
			continue
		}
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

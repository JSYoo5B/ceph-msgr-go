package testcluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// DaemonLease is the test side of one fixture-owned pause of a daemon
// (integration/fixture/daemon_lease.go is the fixture side). The lease has
// its ID before anything is published, so its end can name it whatever
// happened to the pause: the pause request carries the lease ID, the daemon
// and the absolute Deadline, and the fixture ends the lease by the deadline
// itself, so no process of the test has to publish a resume in time. An
// end that the fixture claims before the pause leaves the lease ended, and
// the fixture then refuses the pause instead of executing it, so a pause
// published late, after its lease ended, never freezes the daemon. The
// lease is settled only by the fixture's ended acknowledgement.
//
// The deadline is wall-clock time: the test and the fixture run on one host
// and read one clock.
type DaemonLease struct {
	control  string
	Kind     string
	Name     string
	ID       int64
	Deadline time.Time
}

// The states of a lease, as its acknowledgements name them.
const (
	DaemonLeaseRequested = "requested"
	DaemonLeasePaused    = "paused"
	DaemonLeaseEnded     = "ended"
)

// The reasons the fixture ended a lease.
const (
	// DaemonLeaseResumed: an end request resumed the paused daemon.
	DaemonLeaseResumed = "resumed"
	// DaemonLeaseExpired: the deadline passed while the daemon was paused,
	// and the fixture resumed it.
	DaemonLeaseExpired = "expired"
	// DaemonLeaseEndedBeforePause: the end reached the fixture before the
	// pause; the daemon was never paused under the lease.
	DaemonLeaseEndedBeforePause = "ended-before-pause"
	// DaemonLeaseExpiredBeforePause: the pause reached the fixture at or
	// after its deadline and was not executed.
	DaemonLeaseExpiredBeforePause = "expired-before-pause"
	// DaemonLeaseDaemonHeld: another lease held the daemon; the pause was
	// not executed.
	DaemonLeaseDaemonHeld = "daemon-held"
	// DaemonLeaseFixtureFailed: the fixture had failed before the pause;
	// it was not executed.
	DaemonLeaseFixtureFailed = "fixture-failed"
)

// DaemonLeaseRecord is the fixture's record of a lease at one transition.
// Times are Unix nanoseconds, zero when the event did not happen.
type DaemonLeaseRecord struct {
	Lease    string `json:"lease"`
	Daemon   string `json:"daemon,omitempty"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	End      string `json:"end,omitempty"`
	Deadline int64  `json:"deadline,omitempty"`
	PausedAt int64  `json:"paused_at,omitempty"`
	EndedAt  int64  `json:"ended_at,omitempty"`
}

// Paused says whether the daemon was paused under the lease.
func (r DaemonLeaseRecord) Paused() bool { return r.PausedAt != 0 }

// HeldFor is how long the daemon was paused under an ended lease.
func (r DaemonLeaseRecord) HeldFor() time.Duration {
	if r.PausedAt == 0 || r.EndedAt == 0 {
		return 0
	}
	return time.Duration(r.EndedAt - r.PausedAt)
}

// DaemonLeaseEndedError is the error of a pause whose lease ended before
// the daemon was paused, or of a resume that found the lease already ended
// for another reason.
type DaemonLeaseEndedError struct {
	Record DaemonLeaseRecord
	// Step is what found the lease ended: "pause" or "resume".
	Step string
}

func (e *DaemonLeaseEndedError) Error() string {
	return fmt.Sprintf("daemon lease %s of %s ended (%s) before its %s", e.Record.Lease, e.Record.Daemon, e.Record.Reason, e.Step)
}

// NewDaemonLease returns a lease of daemon kind.name that ends by deadline.
// It publishes nothing.
func NewDaemonLease(control, kind, name string, deadline time.Time) *DaemonLease {
	return &DaemonLease{control: control, Kind: kind, Name: name, ID: nextRequestID(), Deadline: deadline}
}

func (l *DaemonLease) ack(state string) string {
	return filepath.Join(l.control, fmt.Sprintf("daemon-lease-%s.%d", state, l.ID))
}

// Pause publishes the lease's pause and waits until the fixture
// acknowledges the daemon paused, or the lease ended without a pause
// (DaemonLeaseEndedError). It publishes nothing when ctx has already ended.
// A pause abandoned with ctx may still run later, until the lease ends:
// End settles it either way.
func (l *DaemonLease) Pause(ctx context.Context) (DaemonLeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return DaemonLeaseRecord{}, err
	}
	if _, err := publishRequest(l.control, "pause-daemon", nil, []string{strconv.FormatInt(l.ID, 10), l.Kind, l.Name, strconv.FormatInt(l.Deadline.UnixNano(), 10)}); err != nil {
		return DaemonLeaseRecord{}, err
	}
	path, err := waitControl(ctx, l.control, l.ack(DaemonLeasePaused), l.ack(DaemonLeaseEnded))
	if err != nil {
		return DaemonLeaseRecord{}, err
	}
	r, err := l.read(path)
	if err == nil && r.State == DaemonLeaseEnded {
		err = &DaemonLeaseEndedError{Record: r, Step: "pause"}
	}
	return r, err
}

// End ends the lease, why being "resume", "cancel" or "cleanup", and returns
// the fixture's ended record. A lease that is already ended, by an earlier
// end or by its deadline, is not ended again. Otherwise End publishes the
// end also when ctx has already ended, since only the end keeps a pause
// that is still on its way from running, and then waits for the ended
// acknowledgement under ctx. The lease is settled only when End returns
// that record.
func (l *DaemonLease) End(ctx context.Context, why string) (DaemonLeaseRecord, error) {
	if r, ok, err := l.Ended(); err != nil || ok {
		return r, err
	}
	if _, err := publishRequest(l.control, "end-daemon-lease", nil, []string{strconv.FormatInt(l.ID, 10), why}); err != nil {
		return DaemonLeaseRecord{}, err
	}
	path, err := waitControl(ctx, l.control, l.ack(DaemonLeaseEnded))
	if err != nil {
		return DaemonLeaseRecord{}, fmt.Errorf("end daemon lease %d of %s.%s: %w", l.ID, l.Kind, l.Name, err)
	}
	return l.read(path)
}

// Ended returns the lease's ended record when the fixture has acknowledged
// its end.
func (l *DaemonLease) Ended() (DaemonLeaseRecord, bool, error) {
	r, err := l.read(l.ack(DaemonLeaseEnded))
	if errors.Is(err, os.ErrNotExist) {
		return DaemonLeaseRecord{}, false, nil
	}
	return r, err == nil, err
}

// read reads one acknowledgement of the lease, which must name the lease.
func (l *DaemonLease) read(path string) (DaemonLeaseRecord, error) {
	var r DaemonLeaseRecord
	data, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(data, &r); err == nil && r.Lease != strconv.FormatInt(l.ID, 10) {
		err = fmt.Errorf("names lease %q", r.Lease)
	}
	if err != nil {
		return r, fmt.Errorf("daemon lease acknowledgement %s: %w", filepath.Base(path), err)
	}
	return r, nil
}

package testcluster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pendingEnvelope waits until request has exactly one pending envelope in
// dir and returns its request ID and content. It returns as soon as the
// envelope exists; the loop only gives the publisher's goroutine a turn.
func pendingEnvelope(t *testing.T, ctx context.Context, dir, request string) (string, string) {
	t.Helper()
	for {
		pending := envelopes(t, dir, request)
		if len(pending) > 1 {
			t.Fatal("more than one envelope", pending)
		}
		for id, data := range pending {
			return id, data
		}
		select {
		case <-ctx.Done():
			t.Fatal("no", request, "envelope:", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

// acknowledge publishes the fixture's record of lease l at state.
func acknowledge(t *testing.T, dir string, l *DaemonLease, r DaemonLeaseRecord) {
	t.Helper()
	r.Lease = strconv.FormatInt(l.ID, 10)
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "daemon-lease-"+r.State+"."+r.Lease), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// The pause and end requests are the exact lines the fixture parses
// (integration/fixture/daemon_lease_test.go publishPause and publishEnd
// write the same text), each in its own envelope.
func TestDaemonLeaseRequestsKeepTheFixtureWireFormat(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline := time.Unix(1_800_000_016, 5)
	l := NewDaemonLease(dir, "osd", "2", deadline)
	paused := make(chan error, 1)
	go func() { _, err := l.Pause(ctx); paused <- err }()
	id, data := pendingEnvelope(t, ctx, dir, "pause-daemon")
	if want := id + " " + strconv.FormatInt(l.ID, 10) + " osd 2 1800000016000000005\n"; data != want {
		t.Fatalf("pause request %q, want %q", data, want)
	}
	acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeasePaused, Daemon: "osd.2", PausedAt: 1})
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	ended := make(chan error, 1)
	go func() { _, err := l.End(ctx, "resume"); ended <- err }()
	id, data = pendingEnvelope(t, ctx, dir, "end-daemon-lease")
	if want := id + " " + strconv.FormatInt(l.ID, 10) + " resume\n"; data != want {
		t.Fatalf("end request %q, want %q", data, want)
	}
	acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeaseEnded, Reason: DaemonLeaseResumed})
	if err := <-ended; err != nil {
		t.Fatal(err)
	}
	if other := NewDaemonLease(dir, "osd", "2", deadline); other.ID == l.ID {
		t.Fatal("two leases share an ID")
	}
}

// A lease is settled only by the fixture's ended acknowledgement: its paused
// acknowledgement, a requested one or an acknowledgement of another lease
// does not end End's wait, and a pause whose lease ended first is an error
// that carries the record.
func TestDaemonLeaseSettlesOnlyOnItsEndedAcknowledgement(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l := NewDaemonLease(dir, "osd", "1", time.Now().Add(time.Minute))
	other := NewDaemonLease(dir, "osd", "1", time.Now().Add(time.Minute))
	acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeaseRequested})
	acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeasePaused, PausedAt: 1})
	acknowledge(t, dir, other, DaemonLeaseRecord{State: DaemonLeaseEnded, Reason: DaemonLeaseExpired})
	ended := make(chan DaemonLeaseRecord, 1)
	go func() {
		r, err := l.End(ctx, "cleanup")
		if err != nil {
			t.Error(err)
		}
		ended <- r
	}()
	pendingEnvelope(t, ctx, dir, "end-daemon-lease")
	select {
	case r := <-ended:
		t.Fatal("End returned before the lease's ended acknowledgement", r)
	default:
	}
	acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeaseEnded, Reason: DaemonLeaseResumed, End: "cleanup", PausedAt: 1, EndedAt: 3})
	if r := <-ended; r.Reason != DaemonLeaseResumed || r.HeldFor() != 2 {
		t.Fatal("ended record", r)
	}
	// Ended already: End returns the record and publishes nothing more.
	before := envelopes(t, dir, "end-daemon-lease")
	if r, err := l.End(ctx, "cleanup"); err != nil || r.Reason != DaemonLeaseResumed || len(envelopes(t, dir, "end-daemon-lease")) != len(before) {
		t.Fatal("a second end", r, err)
	}
	// A pause whose lease ended before it.
	late := NewDaemonLease(dir, "osd", "1", time.Now().Add(time.Minute))
	acknowledge(t, dir, late, DaemonLeaseRecord{State: DaemonLeaseEnded, Reason: DaemonLeaseEndedBeforePause})
	_, err := late.Pause(ctx)
	var endedErr *DaemonLeaseEndedError
	if !errors.As(err, &endedErr) || endedErr.Record.Reason != DaemonLeaseEndedBeforePause {
		t.Fatal("a pause of an ended lease", err)
	}
	// An acknowledgement that names another lease is refused.
	wrong := NewDaemonLease(dir, "osd", "1", time.Now().Add(time.Minute))
	if err := os.WriteFile(wrong.ack(DaemonLeaseEnded), []byte(`{"lease":"1","state":"ended"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := wrong.Ended(); err == nil || !strings.Contains(err.Error(), "names lease") {
		t.Fatal("a record of another lease was accepted", err)
	}
}

// Cancellation at each step leaves the lease endable: a pause whose context
// ended publishes nothing, a pause abandoned after its publication leaves
// its request for the fixture, and End publishes its end also under a
// context that has already ended, so the fixture can refuse that pause.
func TestDaemonLeaseCancellationAtEachStep(t *testing.T) {
	t.Run("before the pause is published", func(t *testing.T) {
		dir := t.TempDir()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		l := NewDaemonLease(dir, "mon", "b", time.Now().Add(time.Minute))
		if _, err := l.Pause(canceled); !errors.Is(err, context.Canceled) || len(envelopes(t, dir, "pause-daemon")) != 0 {
			t.Fatal("a canceled pause was published", err)
		}
		if _, err := l.End(canceled, "cancel"); !errors.Is(err, context.Canceled) {
			t.Fatal("end under a canceled context", err)
		}
		if pending := envelopes(t, dir, "end-daemon-lease"); len(pending) != 1 {
			t.Fatal("the end was not published under a canceled context", pending)
		}
	})
	t.Run("while the pause waits", func(t *testing.T) {
		dir := t.TempDir()
		parent, cancel := context.WithCancel(context.Background())
		l := NewDaemonLease(dir, "mgr", "x", time.Now().Add(time.Minute))
		paused := make(chan error, 1)
		go func() { _, err := l.Pause(parent); paused <- err }()
		wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		pendingEnvelope(t, wait, dir, "pause-daemon")
		cancel()
		if err := <-paused; !errors.Is(err, context.Canceled) {
			t.Fatal("an abandoned pause", err)
		}
		if len(envelopes(t, dir, "pause-daemon")) != 1 {
			t.Fatal("the abandoned pause was withdrawn; only the fixture claims it")
		}
		ended := make(chan error, 1)
		go func() { _, err := l.End(wait, "cleanup"); ended <- err }()
		pendingEnvelope(t, wait, dir, "end-daemon-lease")
		acknowledge(t, dir, l, DaemonLeaseRecord{State: DaemonLeaseEnded, Reason: DaemonLeaseEndedBeforePause, End: "cleanup"})
		if err := <-ended; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("after the fixture failed", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "provider.error"), []byte("synthetic failure\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		l := NewDaemonLease(dir, "osd", "0", time.Now().Add(time.Minute))
		if _, err := l.Pause(context.Background()); err == nil || len(envelopes(t, dir, "pause-daemon")) != 0 {
			t.Fatal("a pause was published to a failed fixture", err)
		}
		if _, err := l.End(context.Background(), "cleanup"); err == nil || !strings.Contains(err.Error(), "synthetic failure") {
			t.Fatal("an end on a failed fixture", err)
		}
	})
}

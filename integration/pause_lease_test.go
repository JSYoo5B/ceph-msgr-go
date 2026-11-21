package integration_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// pauseLease is one owned pause of an OSD through the fixture's daemon
// control, bounded from before the pause was requested. The fixture may
// freeze the daemon at any moment after the request is published, also while
// the acknowledgement is still on its way, so the budget starts before that
// publication. Every control and native observation of the held phase runs
// under held, which ends a reserve before the deadline so that the resume
// still has its own time.
//
// The pause ends by the deadline on one of two paths, whichever comes first,
// and only once: the test's own resume, or the lease's watchdog, which runs
// when the held phase ends without a resume (its deadline passed, or the
// run's context ended) and asks the fixture to recover the pause under a
// context that ends at the deadline. The watchdog runs in its own goroutine,
// so a native observation that is still outstanding when the held phase ends
// does not delay it; the fixture serves pause-ending requests beside a
// running owned-pool oracle request (integration/fixture/control.go
// controls), and the observation reads only its own input
// (testcluster.ControlInput), so abandoning it is safe.
type pauseLease struct {
	// request runs one fixture control request (request, response, name);
	// fixtureRequest outside the cluster-free tests.
	request  func(ctx context.Context, request, response, name string) error
	name     string
	budget   time.Duration
	deadline time.Time
	// held ends reserve before deadline.
	held      context.Context
	stopHeld  context.CancelFunc
	stopWatch func() bool

	// settled runs the first ending of the pause; the fields below are set
	// inside it, so a caller reads them after settled.Do returned.
	settled sync.Once
	// ended is the request that ended the pause first, err its outcome and
	// at when its acknowledgement (or failure) arrived.
	ended   string
	err     error
	at      time.Time
	resumed bool
}

// fixtureRequest runs one control request through the native fixture's
// control directory.
func fixtureRequest(ctx context.Context, request, response, name string) error {
	return testcluster.RequestControl(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), request, response, name)
}

// startPauseLease starts the budget, then requests the pause of osd name
// under held. It returns the lease also on an error, since a pause request
// that failed or was abandoned may still take effect: the caller's restore
// owes its recovery either way.
func startPauseLease(parent context.Context, request func(context.Context, string, string, string) error, name string, budget, reserve time.Duration) (*pauseLease, error) {
	return startPauseLeaseUntil(parent, request, name, time.Now().Add(budget), reserve)
}

// startPauseLeaseUntil is startPauseLease for a pause that must end by
// deadline, for several pauses of one window.
func startPauseLeaseUntil(parent context.Context, request func(context.Context, string, string, string) error, name string, deadline time.Time, reserve time.Duration) (*pauseLease, error) {
	p := &pauseLease{request: request, name: name, budget: time.Until(deadline).Round(time.Millisecond), deadline: deadline}
	p.held, p.stopHeld = context.WithDeadline(parent, deadline.Add(-reserve))
	p.stopWatch = context.AfterFunc(p.held, p.watch)
	if err := request(p.held, "pause-osd", "osd-pause", name); err != nil {
		return p, fmt.Errorf("pause osd.%s: %w", name, err)
	}
	return p, p.check("the pause request")
}

// watch is the lease's independent ending: when the held phase ended
// without a resume, it recovers the pause by the deadline. A plain resume
// would fail the fixture when the daemon is not paused, as after a pause
// request that was never served; the recovery settles the original pause
// and resume requests and resumes the daemon only if it is paused
// (recover-paused-osd, integration/fixture/control_daemon.go). The
// recovery is per OSD and has its own envelope (testcluster.RequestEnvelope),
// so the watchdogs of several leases that end together are all served.
func (p *pauseLease) watch() {
	ctx, stop := context.WithDeadline(context.Background(), p.deadline)
	defer stop()
	p.end(ctx, "recover-paused-osd", "osd-pause-recovered")
}

// end ends the pause with request unless it already ended; it returns the
// request that ended it.
func (p *pauseLease) end(ctx context.Context, request, response string) string {
	p.settled.Do(func() {
		p.stopHeld()
		p.ended = request
		p.err = p.request(ctx, request, response, p.name)
		p.at = time.Now()
		p.resumed = p.err == nil
	})
	return p.ended
}

// check requires step to have ended within the held phase.
func (p *pauseLease) check(step string) error {
	if err := p.held.Err(); err != nil {
		return fmt.Errorf("%s ended past the held phase of osd.%s's %v pause budget: %w", step, p.name, p.budget, err)
	}
	return nil
}

// resume ends the pause under a context that ends at the deadline, and
// requires the acknowledgement before it. It is refused when the watchdog
// ended the pause first: the held phase was over before the test resumed.
// The lease is resumed only when the fixture acknowledged the ending;
// otherwise restore recovers it.
func (p *pauseLease) resume() (left time.Duration, err error) {
	resume, stop := context.WithDeadline(context.Background(), p.deadline)
	defer stop()
	if ended := p.end(resume, "resume-osd", "osd-resume"); ended != "resume-osd" {
		return 0, fmt.Errorf("the held phase of osd.%s's %v pause budget ended before its resume; the lease's watchdog sent %s: %v", p.name, p.budget, ended, p.err)
	}
	if p.err != nil {
		return 0, fmt.Errorf("resume osd.%s within its %v pause budget: %w", p.name, p.budget, p.err)
	}
	if left = p.deadline.Sub(p.at); left <= 0 {
		return left, fmt.Errorf("osd.%s stayed paused %v past its %v budget", p.name, -left, p.budget)
	}
	return left, nil
}

// restore is the lease's recovery for cleanup: it ends the held phase,
// waits for the ending already under way, and unless that ending was
// acknowledged, asks the fixture to recover the pause under ctx.
func (p *pauseLease) restore(ctx context.Context) error {
	p.stopHeld()
	p.end(ctx, "recover-paused-osd", "osd-pause-recovered")
	if p.resumed {
		return nil
	}
	if err := p.request(ctx, "recover-paused-osd", "osd-pause-recovered", p.name); err != nil {
		return fmt.Errorf("recover the pause of osd.%s: %w", p.name, err)
	}
	p.resumed = true
	return nil
}

// fakePauseControl answers the pause lease's control requests in place of
// the fixture: each request waits until its gate (if any) is released or its
// context ends, and is recorded with its context's deadline. No daemon is
// involved and nothing waits on a clock.
type fakePauseControl struct {
	mu        sync.Mutex
	gates     map[string]chan struct{}
	fail      map[string]error
	requests  []string
	deadlines map[string]time.Time
	// arrived receives each request as it is published, when not nil.
	arrived chan string
}

func (f *fakePauseControl) request(ctx context.Context, request, response, name string) error {
	f.mu.Lock()
	if f.deadlines == nil {
		f.deadlines = map[string]time.Time{}
	}
	f.requests = append(f.requests, request+" "+name)
	f.deadlines[request], _ = ctx.Deadline()
	gate := f.gates[request]
	f.mu.Unlock()
	if f.arrived != nil {
		f.arrived <- request
	}
	if gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate:
		}
	}
	return f.fail[request]
}

func (f *fakePauseControl) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func TestPauseLease(t *testing.T) {
	const budget, reserve = time.Minute, 10 * time.Second
	restore := func(t *testing.T, p *pauseLease) {
		t.Helper()
		ctx, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := p.restore(ctx); err != nil {
			t.Fatal("restore:", err)
		}
	}
	t.Run("in time", func(t *testing.T) {
		f := &fakePauseControl{}
		p, err := startPauseLease(context.Background(), f.request, "3", budget, reserve)
		if err != nil || p.check("a step") != nil {
			t.Fatal("pause refused:", err)
		}
		if _, err := p.resume(); err != nil {
			t.Fatal("resume refused:", err)
		}
		restore(t, p)
		if !slices.Equal(f.sent(), []string{"pause-osd 3", "resume-osd 3"}) {
			t.Fatal("requests", f.sent())
		}
		if !f.deadlines["resume-osd"].Equal(p.deadline) {
			t.Fatal("the resume is not bounded by the deadline", f.deadlines["resume-osd"], p.deadline)
		}
	})
	t.Run("a held control bounded by the held phase", func(t *testing.T) {
		f := &fakePauseControl{}
		p, err := startPauseLease(context.Background(), f.request, "3", budget, reserve)
		if err != nil {
			t.Fatal(err)
		}
		if end, ok := p.held.Deadline(); !ok || !end.Equal(p.deadline.Add(-reserve)) || !f.deadlines["pause-osd"].Equal(end) {
			t.Fatalf("the held phase ends at %v (%v), the pause request at %v; want the reserve %v before the deadline %v", end, ok, f.deadlines["pause-osd"], reserve, p.deadline)
		}
		restore(t, p)
	})
	t.Run("a pause acknowledged after the held phase", func(t *testing.T) {
		// The fixture may freeze the daemon before it acknowledges, so the
		// budget is counted from before the request was published, and an
		// unacknowledged pause is recovered, not resumed.
		parent, end := context.WithCancel(context.Background())
		f := &fakePauseControl{gates: map[string]chan struct{}{"pause-osd": make(chan struct{})}, arrived: make(chan string, 4)}
		started := make(chan error, 1)
		var p *pauseLease
		go func() {
			var err error
			p, err = startPauseLease(parent, f.request, "3", budget, reserve)
			started <- err
		}()
		<-f.arrived
		end()
		if err := <-started; !errors.Is(err, context.Canceled) {
			t.Fatal("a pause without an acknowledgement in the held phase accepted:", err)
		}
		if got := <-f.arrived; got != "recover-paused-osd" {
			t.Fatal("the watchdog sent", got)
		}
		restore(t, p)
		if !slices.Equal(f.sent(), []string{"pause-osd 3", "recover-paused-osd 3"}) {
			t.Fatal("the uncertain pause was not recovered once", f.sent())
		}
	})
	t.Run("a late resume", func(t *testing.T) {
		// The acknowledgement never comes; the resume gives up at the
		// deadline, and restore recovers the pause.
		f := &fakePauseControl{gates: map[string]chan struct{}{"resume-osd": make(chan struct{})}}
		p, err := startPauseLease(context.Background(), f.request, "3", 200*time.Millisecond, 10*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.resume(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("a resume past the deadline accepted:", err)
		}
		restore(t, p)
		if !slices.Equal(f.sent(), []string{"pause-osd 3", "resume-osd 3", "recover-paused-osd 3"}) {
			t.Fatal("the uncertain resume was not recovered", f.sent())
		}
	})
	t.Run("a failed resume", func(t *testing.T) {
		f := &fakePauseControl{fail: map[string]error{"resume-osd": errors.New("refused")}}
		p, err := startPauseLease(context.Background(), f.request, "3", budget, reserve)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.resume(); err == nil {
			t.Fatal("a failed resume accepted")
		}
		restore(t, p)
		if !slices.Equal(f.sent(), []string{"pause-osd 3", "resume-osd 3", "recover-paused-osd 3"}) {
			t.Fatal("the failed resume was not recovered", f.sent())
		}
	})
	t.Run("the watchdog is independent of the parent context", func(t *testing.T) {
		f := &fakePauseControl{arrived: make(chan string, 4)}
		parent, cancel := context.WithCancel(context.Background())
		p, err := startPauseLease(parent, f.request, "3", budget, reserve)
		if err != nil {
			t.Fatal(err)
		}
		<-f.arrived
		cancel()
		if got := <-f.arrived; got != "recover-paused-osd" {
			t.Fatal("the watchdog sent", got)
		}
		if p.check("a step after the run ended") == nil {
			t.Fatal("a held phase outliving its parent accepted")
		}
		if _, err := p.resume(); err == nil {
			t.Fatal("a resume after the watchdog ended the pause accepted")
		}
		restore(t, p)
		if !slices.Equal(f.sent(), []string{"pause-osd 3", "recover-paused-osd 3"}) || !f.deadlines["recover-paused-osd"].Equal(p.deadline) {
			t.Fatal("the watchdog did not end the pause once by the deadline", f.sent(), f.deadlines["recover-paused-osd"], p.deadline)
		}
	})
}

// A native observation still outstanding when the held phase ends must not
// keep the OSD paused: the watchdog ends the pause by the deadline while the
// observation has not returned, and the observation published after it does
// not replace its input. The observations are real owned-pool requests
// (testcluster.RequestControlInput) into a control directory that no fixture
// serves, so the first stays outstanding for as long as the test wants; its
// context is not the held phase, the worst case. Nothing waits on a clock.
func TestPauseLeaseEndsWhileAnObservationIsOutstanding(t *testing.T) {
	dir := t.TempDir()
	f := &fakePauseControl{arrived: make(chan string, 4)}
	parent, deadline := context.WithCancel(context.Background())
	p, err := startPauseLease(parent, f.request, "3", time.Minute, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	<-f.arrived
	observe := func(ctx context.Context, input string) error {
		return testcluster.ControlDaemonInput(ctx, dir, "verify", "osd-pool", "opsinflight", testcluster.ControlInput{Name: "osd-pool-input.json", Data: []byte(input)})
	}
	outstanding, abandon := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- observe(outstanding, "first") }()
	// The held phase ends (here through its parent; its deadline does the
	// same) while the first observation is outstanding.
	deadline()
	if got := <-f.arrived; got != "recover-paused-osd" {
		t.Fatal("the watchdog sent", got)
	}
	select {
	case err := <-first:
		t.Fatal("the observation returned before the pause ended:", err)
	default:
	}
	if !f.deadlines["recover-paused-osd"].Equal(p.deadline) {
		t.Fatal("the pause did not end by the lease deadline", f.deadlines["recover-paused-osd"], p.deadline)
	}
	abandon()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("the outstanding observation", err)
	}
	ended, stop := context.WithCancel(context.Background())
	stop()
	if err := observe(ended, "second"); !errors.Is(err, context.Canceled) {
		t.Fatal("the next observation", err)
	}
	inputs, err := filepath.Glob(filepath.Join(dir, "osd-pool-input.json.*"))
	if err != nil || len(inputs) != 2 {
		t.Fatal("each observation keeps its own input", inputs, err)
	}
	var read []string
	for _, path := range inputs {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		read = append(read, string(data))
	}
	if slices.Sort(read); !slices.Equal(read, []string{"first", "second"}) {
		t.Fatal("an observation's input was replaced:", read)
	}
	restore, stopRestore := context.WithTimeout(context.Background(), time.Minute)
	defer stopRestore()
	if err := p.restore(restore); err != nil || !slices.Equal(f.sent(), []string{"pause-osd 3", "recover-paused-osd 3"}) {
		t.Fatal("the pause was not ended once", err, f.sent())
	}
}

// controlEnvelope is one pending publication of a control request in a
// control directory (testcluster.RequestEnvelope), as a test that stands in
// for the fixture reads it.
type controlEnvelope struct {
	path, id string
	fields   []string
}

// controlEnvelopes returns the pending envelopes of request in dir in
// request-ID order.
func controlEnvelopes(dir, request string) ([]controlEnvelope, error) {
	paths, err := filepath.Glob(filepath.Join(dir, request+".*.request"))
	if err != nil {
		return nil, err
	}
	var pending []controlEnvelope
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		first, _, _ := strings.Cut(string(data), "\n")
		e := controlEnvelope{path: path, id: strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), request+"."), ".request"), fields: strings.Fields(first)}
		if len(e.fields) == 0 || e.fields[0] != e.id {
			return nil, fmt.Errorf("envelope %s holds %q", filepath.Base(path), data)
		}
		pending = append(pending, e)
	}
	slices.SortFunc(pending, func(a, b controlEnvelope) int {
		x, _ := strconv.ParseInt(a.id, 10, 64)
		y, _ := strconv.ParseInt(b.id, 10, 64)
		return cmp.Compare(x, y)
	})
	return pending, nil
}

// Two pause leases of one window that end together, before the fixture
// polls, must both be recovered: their watchdogs publish two recoveries,
// each in its own envelope, and each lease is settled by its own
// acknowledgement. The test acknowledges the pauses and recoveries in place
// of the fixture, through a real control directory.
func TestPauseLeasesEndingTogetherAreBothRecovered(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := func(ctx context.Context, request, response, name string) error {
		return testcluster.RequestControl(ctx, dir, request, response, name)
	}
	wait := func(action string, count int) []controlEnvelope {
		t.Helper()
		for {
			pending, err := controlEnvelopes(dir, action)
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) >= count {
				return pending
			}
			select {
			case <-ctx.Done():
				t.Fatalf("%d of %d %s requests published: %v", len(pending), count, action, ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
	ack := func(e controlEnvelope, response string) {
		t.Helper()
		if err := os.Remove(e.path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, response+"."+e.id), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	parent, end := context.WithCancel(ctx)
	deadline := time.Now().Add(time.Minute)
	var leases []*pauseLease
	for _, osd := range []string{"2", "3"} {
		started := make(chan *pauseLease, 1)
		go func() {
			p, _ := startPauseLeaseUntil(parent, request, osd, deadline, 10*time.Second)
			started <- p
		}()
		ack(wait("pause-osd", 1)[0], "osd-pause")
		p := <-started
		if err := p.check("the pause"); err != nil {
			t.Fatal(err)
		}
		leases = append(leases, p)
	}
	// Both held phases end at once; no fixture polls until both watchdogs
	// have published.
	end()
	recoveries := wait("recover-paused-osd", 2)
	names := map[string]bool{}
	for _, e := range recoveries {
		names[e.fields[1]] = true
	}
	if len(recoveries) != 2 || !names["2"] || !names["3"] {
		t.Fatal("the recoveries do not name both paused OSDs", recoveries)
	}
	for _, e := range recoveries {
		ack(e, "osd-pause-recovered")
	}
	for _, p := range leases {
		restore, stop := context.WithTimeout(ctx, time.Minute)
		err := p.restore(restore)
		stop()
		if err != nil || !p.resumed || p.ended != "recover-paused-osd" {
			t.Fatal("a lease was not recovered by its own watchdog", p.name, err, p.ended)
		}
	}
}

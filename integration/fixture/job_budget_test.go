package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func budgetEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func deadlineIn(now time.Time, left time.Duration) string {
	return strconv.FormatInt(now.Add(left).Unix(), 10)
}

// Without a job deadline every suite keeps the profile's static timeout,
// verbatim, as before the deadline existed, and the start-state check keeps
// its fixed budget.
func TestSuiteTimeoutWithoutJobDeadlineIsTheStaticTimeout(t *testing.T) {
	b, err := loadJobBudget(budgetEnv(nil))
	if err != nil || !b.deadline.IsZero() || b.explicit {
		t.Fatal("an empty environment gave", b, err)
	}
	for _, static := range []string{"10m", "30m", "0", "500ms", " invalid-timeout "} {
		for _, perTest := range []bool{false, true} {
			if got, err := b.suiteTimeout(profile{testTimeout: static}, time.Now(), perTest); err != nil || got != static {
				t.Errorf("static %q perTest=%v: %q, %v", static, perTest, got, err)
			}
		}
	}
	if limit, err := b.checkBudget(profile{}, time.Now()); err != nil || limit != sequenceStartStateBudget {
		t.Error("check budget without a deadline:", limit, err)
	}
}

// With a job deadline the default selection may use all the time before the
// reserves, also past the static default; an explicit timeout, an explicit
// selection and a sequence's single test keep the shorter static bound, and
// a sequence test leaves the start-state check its budget.
func TestSuiteTimeoutIsBoundedByTheJobDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	osd := profile{osd: "1", testTimeout: "30m"}
	if reserve := teardownReserve(osd); reserve != diagnosticsReserve+osd.cleanupBudget() || reserve != 3*time.Minute || afterSuiteReserve(osd) != 0 {
		t.Fatal("OSD reserves", reserve, afterSuiteReserve(osd))
	}
	for _, test := range []struct {
		name     string
		left     time.Duration
		explicit bool
		selected bool
		perTest  bool
		want     string
	}{
		{"default selection gets the job's remainder", 40 * time.Minute, false, false, false, "37m0s"},
		{"remainder below the static default", 20 * time.Minute, false, false, false, "17m0s"},
		{"explicit timeout is shorter", 40 * time.Minute, true, false, false, "30m0s"},
		{"explicit timeout is longer than the remainder", 25 * time.Minute, true, false, false, "22m0s"},
		{"explicit selection keeps the static bound", 40 * time.Minute, false, true, false, "30m0s"},
		{"explicit selection near the deadline", 12 * time.Minute, false, true, false, "9m0s"},
		{"sequence test keeps its static bound", 100 * time.Minute, false, false, true, "30m0s"},
		{"sequence test leaves the start-state check", 10 * time.Minute, false, false, true, "6m0s"},
	} {
		env := map[string]string{jobDeadlineEnv: deadlineIn(now, test.left)}
		if test.explicit {
			env[timeoutEnv] = "30m"
		}
		if test.selected {
			env[runEnv] = "^TestCephOSDWriteSameFailoverIntegration$"
		}
		b, err := loadJobBudget(budgetEnv(env))
		if err != nil {
			t.Fatal(test.name, err)
		}
		got, err := b.suiteTimeout(osd, now, test.perTest)
		if err != nil || got != test.want {
			t.Errorf("%s: %q, %v; want %q", test.name, got, err, test.want)
		}
	}
}

// A static timeout of zero or less disables Go's alarm, so under a job
// deadline it never replaces the job's bound; a positive sub-second static
// timeout is given verbatim, never rounded to the disabling 0s.
func TestSuiteTimeoutNeverDisablesTheAlarmUnderADeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	b, err := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: deadlineIn(now, 23*time.Minute), timeoutEnv: "set"}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ static, want, wantSequence string }{
		{"0", "20m0s", "19m0s"},
		{"0s", "20m0s", "19m0s"},
		{"-1s", "20m0s", "19m0s"},
		{"500ms", "500ms", "500ms"},
		{"1500ms", "1.5s", "1.5s"},
		{"90s", "1m30s", "1m30s"},
	} {
		for perTest, want := range map[bool]string{false: test.want, true: test.wantSequence} {
			got, err := b.suiteTimeout(profile{osd: "1", testTimeout: test.static}, now, perTest)
			if err != nil || got != want {
				t.Errorf("static %q perTest=%v: %q, %v; want %q", test.static, perTest, got, err, want)
				continue
			}
			if d, err := time.ParseDuration(got); err != nil || d <= 0 {
				t.Errorf("static %q perTest=%v: %q would disable the alarm", test.static, perTest, got)
			}
		}
	}
}

// The reserves count every bounded step after the tests: the finish steps of
// the fixtures that have one and the cold-bootstrap removal proof.
func TestJobBudgetReservesTheWorkAfterTheTests(t *testing.T) {
	if got := afterSuiteReserve(profile{mdsFresh: "1"}); got != mdsFreshFinishBase+mdsFreshFinishSessions+mdsFreshFinishRanks {
		t.Error("MDS fresh finish reserve", got)
	}
	if got := afterSuiteReserve(profile{rbdBase: "1"}); got != rbdBaseFinishBudget {
		t.Error("RBD finish reserve", got)
	}
	if got := afterSuiteReserve(profile{osdCachePools: "1"}); got != cachePoolsFinishReserve {
		t.Error("cache-pool finish reserve", got)
	}
	cold := profile{mgrService: "1", mgrCount: "0", osd: "0", defaultRun: "^x$", testRun: "^x$"}
	if !cold.mgrServiceColdBootstrap() {
		t.Fatal("the cold-bootstrap profile of this test is not one")
	}
	if teardownReserve(cold) != diagnosticsReserve+cold.cleanupBudget()+coldBootstrapProofBudget {
		t.Error("cold-bootstrap teardown reserve", teardownReserve(cold))
	}
	now := time.Unix(1_800_000_000, 0)
	rbd := profile{rbdBase: "1", testTimeout: "10m"}
	b, _ := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: deadlineIn(now, 30*time.Minute)}))
	want := (30*time.Minute - teardownReserve(rbd) - rbdBaseFinishBudget).Truncate(time.Second).String()
	if got, err := b.suiteTimeout(rbd, now, false); err != nil || got != want {
		t.Errorf("RBD suite timeout %q, %v; want %q", got, err, want)
	}
	ctx, stop := b.workContext(context.Background(), rbd)
	defer stop()
	if end, ok := ctx.Deadline(); !ok || !end.Equal(b.deadline.Add(-teardownReserve(rbd))) {
		t.Error("finish steps are not cut at the teardown reserve:", end, ok)
	}
	if _, ok := (jobBudget{}).workContextDeadline(); ok {
		t.Error("a local run bounded the finish steps")
	}
}

func (b jobBudget) workContextDeadline() (time.Time, bool) {
	ctx, stop := b.workContext(context.Background(), profile{})
	defer stop()
	return ctx.Deadline()
}

// A sequence test that passes just before its timer leaves the start-state
// check its full budget; a check later than that is cut to the time before
// the reserves, and one with no time left is not started.
func TestSequenceStartStateCheckNeverUsesTheTeardownReserve(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	p := profile{osd: "1", testTimeout: "30m"}
	b, _ := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: deadlineIn(now, 20*time.Minute)}))
	timeout, err := b.suiteTimeout(p, now, true)
	if err != nil {
		t.Fatal(err)
	}
	granted, _ := time.ParseDuration(timeout)
	// The final test passes at the very end of its timer.
	passed := now.Add(granted)
	if limit, err := b.checkBudget(p, passed); err != nil || limit != sequenceStartStateBudget {
		t.Fatal("the check after a test at its cutoff got", limit, err)
	}
	if end := passed.Add(sequenceStartStateBudget); end.After(b.deadline.Add(-teardownReserve(p))) {
		t.Fatal("the check after the cutoff ends inside the teardown reserve:", end)
	}
	// A later check (the test overran its timer's start) is cut.
	late := passed.Add(45 * time.Second)
	if limit, err := b.checkBudget(p, late); err != nil || limit != 15*time.Second {
		t.Error("late check budget", limit, err)
	}
	if _, err := b.checkBudget(p, b.deadline.Add(-teardownReserve(p))); err == nil {
		t.Error("a check without time left was started")
	}
}

// The bound is enforced on a real check: a readiness check whose condition
// never settles ends when its cut budget ends, before the teardown reserve.
func TestSlowStartStateCheckEndsBeforeTheTeardownReserve(t *testing.T) {
	p := profile{osd: "1", testTimeout: "30m"}
	start := time.Now()
	b := jobBudget{deadline: start.Add(teardownReserve(p) + 2*time.Second)}
	called := false
	report, err := b.boundedCheck(context.Background(), p, start, func(ctx context.Context, limit time.Duration) (startStateReport, error) {
		called = true
		if limit > 2*time.Second {
			t.Errorf("check budget %s exceeds the time before the reserve", limit)
		}
		return waitStartState(ctx, limit, 100*time.Millisecond, &startStateSnapshot{}, nil, func(ctx context.Context) (startStateView, error) {
			<-ctx.Done()
			return startStateView{}, ctx.Err()
		}), nil
	})
	if err != nil || !called {
		t.Fatal("check not run:", err)
	}
	if report.Passed {
		t.Error("an unsettled check passed")
	}
	if ended := time.Now(); ended.After(b.deadline.Add(-teardownReserve(p)).Add(500 * time.Millisecond)) {
		t.Errorf("the check ended %s into the teardown reserve", ended.Sub(b.deadline.Add(-teardownReserve(p))))
	}
	if _, err := b.boundedCheck(context.Background(), p, b.deadline, func(context.Context, time.Duration) (startStateReport, error) {
		t.Error("a check without time left was started")
		return startStateReport{}, nil
	}); err == nil {
		t.Error("no error for a check without time left")
	}
}

// A suite is not started when the job leaves less than a minute before the
// reserves, or after the deadline; a malformed deadline is refused.
func TestSuiteTimeoutRefusesAnExhaustedOrMalformedDeadline(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	p := profile{testTimeout: "10m"}
	for _, test := range []struct {
		left    time.Duration
		perTest bool
	}{
		{teardownReserve(p) + minimumSuiteBudget - time.Second, false},
		{teardownReserve(p) + minimumSuiteBudget + sequenceStartStateBudget - time.Second, true},
		{0, false},
		{-time.Hour, true},
	} {
		b, err := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: deadlineIn(now, test.left)}))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := b.suiteTimeout(p, now, test.perTest); err == nil || !strings.Contains(err.Error(), "reserves") {
			t.Errorf("%s left perTest=%v: %q, %v", test.left, test.perTest, got, err)
		}
	}
	for _, value := range []string{"soon", "-5", "0", "1.5", "17e8"} {
		if _, err := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: value})); err == nil {
			t.Errorf("deadline %q was admitted", value)
		}
	}
	b, _ := loadJobBudget(budgetEnv(map[string]string{jobDeadlineEnv: deadlineIn(now, time.Hour), timeoutEnv: "soon"}))
	if _, err := b.suiteTimeout(profile{testTimeout: "soon"}, now, false); err == nil {
		t.Error("an unparsable explicit timeout was used")
	}
}

// The derived timeout is reported only under a job deadline, on a line the
// result gate does not read as a suite boundary.
func TestSuiteTimeoutReportNamesTheDeadline(t *testing.T) {
	var none, some strings.Builder
	jobBudget{}.report(&none, "api", "10m")
	jobBudget{deadline: time.Unix(1_800_000_000, 0)}.report(&some, "api", "36m0s")
	if none.Len() != 0 {
		t.Error("a local run reported a deadline:", none.String())
	}
	if want := "Ceph test suite api: -test.timeout 36m0s, job deadline 2027-01-15T08:00:00Z\n"; some.String() != want {
		t.Errorf("report %q, want %q", some.String(), want)
	}
}

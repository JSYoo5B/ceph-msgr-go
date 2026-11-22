package main

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// A native CI job has one time budget, its timeout-minutes. The runner of
// the job's tests must exit inside it with every selected test's verdict
// printed and the failure diagnostics saved, because the job's own kill
// leaves neither. The native workflow (.github/workflows/native-scenarios.yml)
// therefore gives the runner the job's end, less the reserve of the steps
// that run after it (the diagnostic upload and the result record), as
// jobDeadlineEnv. That reserve is the workflow's. The fixture keeps the rest
// of the time after the tests for its own work, every part of it bounded:
//
//   - the start-state check after each test of a fixture sequence
//     (sequenceStartStateBudget), reserved when a sequence test is granted
//     its timeout and cut by the deadline (jobBudget.checkBudget);
//   - the success path's finish steps before termination (afterSuiteReserve),
//     reserved for every suite run and run under jobBudget.workContext;
//   - failure diagnostics and termination with the checks after it
//     (teardownReserve).
//
// Each suite binary's -test.timeout is what remains (jobBudget.suiteTimeout).
// Setup the job spent before the fixture started (checkout, image pulls, the
// cluster start) is subtracted by the clock, not estimated; the cluster start
// itself keeps the provider's own bounds and is not cut by the deadline,
// whose first suite then refuses to start (minimumSuiteBudget).
//
// The deadline is not a fixture input: it configures no daemon and selects no
// test, it is not part of the profile, and a row cannot set it. Without it (a
// local run) every suite keeps the profile's static timeout unchanged.

// jobDeadlineEnv is the Unix time in seconds by which the runner must have
// exited.
const jobDeadlineEnv = "CEPH_MSGR_JOB_DEADLINE"

// timeoutEnv and runEnv are the pinned variables of the profile's static
// timeout and of an explicit test selection. Either keeps the static timeout
// as a bound under a job deadline.
const (
	timeoutEnv = "CEPH_MSGR_TEST_TIMEOUT"
	runEnv     = "CEPH_MSGR_TEST_RUN"
)

const (
	// diagnosticsReserve covers the failure path after a suite binary ends:
	// the Go timeout report and its stream copy, saveMetadata and the two
	// 30-second provider collections of collectDiagnostics.
	diagnosticsReserve = 2 * time.Minute
	// minimumSuiteBudget is the least time a suite run is started with; less
	// than this cannot give the suite's tests a verdict and only shortens the
	// teardown.
	minimumSuiteBudget = time.Minute
)

// jobBudget is the runner's view of the job's deadline.
type jobBudget struct {
	// deadline is zero when no job deadline was given.
	deadline time.Time
	// explicit reports a CEPH_MSGR_TEST_TIMEOUT or a CEPH_MSGR_TEST_RUN the
	// runner was given.
	explicit bool
}

// loadJobBudget reads the job deadline. An unset deadline is a local run; a
// set one must be a positive integer.
func loadJobBudget(getenv func(string) string) (jobBudget, error) {
	b := jobBudget{explicit: getenv(timeoutEnv) != "" || getenv(runEnv) != ""}
	value := strings.TrimSpace(getenv(jobDeadlineEnv))
	if value == "" {
		return b, nil
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 {
		return jobBudget{}, fmt.Errorf("%s=%q is not a Unix time in seconds", jobDeadlineEnv, value)
	}
	b.deadline = time.Unix(seconds, 0)
	return b, nil
}

// teardownReserve is the time the runner keeps after its last suite run for
// diagnostics on failure, the fixture's bounded termination and the
// read-only removal proof of the cold-bootstrap fixture after it.
func teardownReserve(p profile) time.Duration {
	reserve := diagnosticsReserve + p.cleanupBudget()
	if p.mgrServiceColdBootstrap() {
		reserve += coldBootstrapProofBudget
	}
	return reserve
}

// afterSuiteReserve is the time the success path needs after the last test
// and before termination: the bounded finish steps of the fixtures that have
// one (finishMDSFresh, finishRBDBase, finishOSDCachePools).
func afterSuiteReserve(p profile) time.Duration {
	var reserve time.Duration
	if p.mdsFresh == "1" {
		reserve += mdsFreshFinishMax
	}
	if p.rbdBase == "1" {
		reserve += rbdBaseFinishBudget
	}
	if p.osdCachePools == "1" {
		reserve += cachePoolsFinishReserve
	}
	return reserve
}

// workLeft is the time before the reserves of the work after the tests.
func (b jobBudget) workLeft(p profile, now time.Time) time.Duration {
	return b.deadline.Sub(now) - teardownReserve(p) - afterSuiteReserve(p)
}

// suiteTimeout is the -test.timeout of one suite binary started at now.
//
// Without a job deadline it is the profile's static timeout, verbatim, as
// before. With one, the run may use the time left before the reserves of the
// work after it (workLeft), less the start-state check that follows a test
// of a fixture sequence (perTest). A suite run of the fixture's default
// selection (a //scenario:suite row) gets all of it, so the size of that
// selection is bounded by the job and not by an estimate of it. An explicit
// selection or CEPH_MSGR_TEST_TIMEOUT, and one test of a fixture sequence,
// keeps the static timeout as its hang bound when that is shorter. A static
// timeout of zero or less disables Go's alarm, so it never replaces the
// job's bound; a positive one shorter than the remainder is given verbatim,
// never rounded (a sub-second value would otherwise become 0s, which also
// disables the alarm). A run that would start with less than
// minimumSuiteBudget is refused, so the result gate reports its tests
// missing while the reserves are kept.
func (b jobBudget) suiteTimeout(p profile, now time.Time, perTest bool) (string, error) {
	if b.deadline.IsZero() {
		return p.testTimeout, nil
	}
	left := b.workLeft(p, now)
	if perTest {
		left -= sequenceStartStateBudget
	}
	if left < minimumSuiteBudget {
		return "", fmt.Errorf("the job deadline leaves %s before the reserves of the work after the tests (%s), less than the %s a suite run needs",
			max(left, 0).Round(time.Second), b.deadline.Sub(now)-left, minimumSuiteBudget)
	}
	// left is at least a minute, so whole seconds are never zero.
	left = left.Truncate(time.Second)
	if b.explicit || perTest {
		static, err := time.ParseDuration(strings.TrimSpace(p.testTimeout))
		if err != nil {
			return "", fmt.Errorf("%s %q: %w", timeoutEnv, p.testTimeout, err)
		}
		if static > 0 && static < left {
			return static.String(), nil
		}
	}
	return left.String(), nil
}

// checkBudget bounds the start-state check after a test of a fixture
// sequence: sequenceStartStateBudget, cut to the time left before the
// reserves of the work after the tests. Less than a second is refused, so
// the check fails (and with it the test before it) instead of using the
// teardown's time.
func (b jobBudget) checkBudget(p profile, now time.Time) (time.Duration, error) {
	if b.deadline.IsZero() {
		return sequenceStartStateBudget, nil
	}
	left := min(sequenceStartStateBudget, b.workLeft(p, now).Truncate(time.Second))
	if left < time.Second {
		return 0, fmt.Errorf("the job deadline leaves no time for the start-state check before the reserves of the work after the tests")
	}
	return left, nil
}

// boundedCheck runs one start-state check started at now with the budget
// checkBudget gives it; a check with no time left is not started.
func (b jobBudget) boundedCheck(ctx context.Context, p profile, now time.Time,
	verify func(context.Context, time.Duration) (startStateReport, error)) (startStateReport, error) {
	limit, err := b.checkBudget(p, now)
	if err != nil {
		return startStateReport{}, err
	}
	return verify(ctx, limit)
}

// workContext bounds work after the tests that is not itself a reserve (the
// finish steps and a sequence's test listing) by the start of the teardown
// reserve, so it cannot use the teardown's time. Without a deadline it is
// ctx.
func (b jobBudget) workContext(ctx context.Context, p profile) (context.Context, context.CancelFunc) {
	if b.deadline.IsZero() {
		return context.WithCancel(ctx)
	}
	return context.WithDeadline(ctx, b.deadline.Add(-teardownReserve(p)))
}

// report prints, under a job deadline, the timeout a suite run was given, so
// a log shows how much of the job the run could use.
func (b jobBudget) report(w io.Writer, suite, timeout string) {
	if b.deadline.IsZero() {
		return
	}
	fmt.Fprintf(w, "Ceph test suite %s: -test.timeout %s, job deadline %s\n", suite, timeout, b.deadline.UTC().Format(time.RFC3339))
}

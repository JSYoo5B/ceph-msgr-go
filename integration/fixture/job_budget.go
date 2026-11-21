package main

import (
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
// jobDeadlineEnv. That reserve is the workflow's; the fixture keeps its own
// teardown reserve (teardownReserve) and derives each suite binary's
// -test.timeout from what remains (jobBudget.suiteTimeout). Setup the job
// spent before the fixture started (checkout, image pulls, the cluster
// start) is subtracted by the clock, not estimated.
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
// diagnostics on failure and the fixture's bounded termination.
func teardownReserve(p profile) time.Duration {
	return diagnosticsReserve + p.cleanupBudget()
}

// suiteTimeout is the -test.timeout of one suite binary started at now.
//
// Without a job deadline it is the profile's static timeout, as before. With
// one, the run may use the time left before the teardown reserve. A suite
// run of the fixture's default selection (a //scenario:suite row) gets all of
// it, so the size of that selection is bounded by the job and not by an
// estimate of it. An explicit selection or CEPH_MSGR_TEST_TIMEOUT, and one
// test of a fixture sequence (perTest), keeps the static timeout as its hang
// bound when that is shorter. A run that would start with
// less than minimumSuiteBudget is refused, so the result gate reports its
// tests missing while the teardown still has its reserve.
func (b jobBudget) suiteTimeout(p profile, now time.Time, perTest bool) (string, error) {
	if b.deadline.IsZero() {
		return p.testTimeout, nil
	}
	left := b.deadline.Sub(now) - teardownReserve(p)
	if left < minimumSuiteBudget {
		return "", fmt.Errorf("the job deadline leaves %s before the %s teardown reserve, less than the %s a suite run needs",
			max(left, 0).Round(time.Second), teardownReserve(p), minimumSuiteBudget)
	}
	if b.explicit || perTest {
		static, err := time.ParseDuration(strings.TrimSpace(p.testTimeout))
		if err != nil {
			return "", fmt.Errorf("%s %q: %w", timeoutEnv, p.testTimeout, err)
		}
		left = min(left, static)
	}
	return left.Truncate(time.Second).String(), nil
}

// report prints, under a job deadline, the timeout a suite run was given, so
// a log shows how much of the job the run could use.
func (b jobBudget) report(w io.Writer, suite, timeout string) {
	if b.deadline.IsZero() {
		return
	}
	fmt.Fprintf(w, "Ceph test suite %s: -test.timeout %s, job deadline %s\n", suite, timeout, b.deadline.UTC().Format(time.RFC3339))
}

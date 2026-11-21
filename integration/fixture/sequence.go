package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

// A fixture sequence runs the tests of several catalog rows that share one
// fixture configuration on one fixture. The planner (tools/scenario_plan.py
// plan) groups the selected shareable rows of one configuration that have an
// explicit test_run into one job of at most MAX_SEQUENCE_ROWS rows; an isolated or broad
// row, and a request that selects one row of a configuration, keeps a job of
// its own (tools/fixture_sharing.py holds the classification rule). It
// forwards the rows' selectors, in planning order, as CEPH_MSGR_TEST_SEQUENCE
// and their union as CEPH_MSGR_TEST_RUN, which the result gate snapshots.
//
// A sequence is admitted only when every member alone is admitted and loads
// the same profile; it declares a capability only when every member is one
// exact test, and a member that is its fixture's default selection or a soak
// runs alone. The fixture runs every selected top-level test alone
// (-test.run ^Name$), in row order, then suite order (api, client), then the
// suite's listing order, each test once, and requires the start-state check
// (start_state.go) to pass within sequenceStartStateBudget after every test
// before the next one runs and after the last one. A failed test or a failed
// check stops the sequence; the check's failure is printed as
// "Fixture start state after <suite>/<Test>: FAILED: ..." and the result gate
// (tools/native_test_results.py snapshot --sequence) reports it against the
// test that ran before it and every later test as missing. Every selected
// test still needs its own PASS line. sequenceRecordFile records each test's
// window, result and the check after it, so a daemon log line can be placed
// in the test whose window holds it.

const (
	sequenceEnv = "CEPH_MSGR_TEST_SEQUENCE"
	// sequenceStartStateBudget bounds each start-state check between tests.
	sequenceStartStateBudget = 60 * time.Second
	// sequenceRecordFile is the sequence's progress record, kept with the
	// diagnostics: each test's run window and the check after it.
	sequenceRecordFile = "fixture-sequence.json"
)

// sequenceStartStateLine is the prefix of the line that reports a check
// after a test; tools/native_test_results.py reads the failed form.
const sequenceStartStateLine = "Fixture start state after "

// listedTest is a top-level test name as -test.list prints it (any Go
// identifier after Test, as the result gate's \w reads it).
var listedTest = regexp.MustCompile(`^Test[\p{L}\p{N}_]*$`)

// loadSequenceProfile admits a sequence only when every member, as the only
// CEPH_MSGR_TEST_RUN, is admitted by loadSingleProfile and all members load
// the same profile apart from the selection: the sequence then starts the
// one fixture each of its rows would start.
func loadSequenceProfile(getenv func(string) string, sequence string) (profile, error) {
	members := strings.Fields(sequence)
	if len(members) < 2 {
		return profile{}, fmt.Errorf("%s needs at least two test selectors", sequenceEnv)
	}
	for i, member := range members {
		if strings.Contains(member, "/") {
			return profile{}, fmt.Errorf("%s member %q selects a subtest", sequenceEnv, member)
		}
		if _, err := regexp.Compile(member); err != nil {
			return profile{}, fmt.Errorf("%s member %q: %w", sequenceEnv, member, err)
		}
		if slices.Contains(members[:i], member) {
			return profile{}, fmt.Errorf("%s repeats %q", sequenceEnv, member)
		}
	}
	if union := strings.Join(members, "|"); getenv("CEPH_MSGR_TEST_RUN") != union {
		return profile{}, fmt.Errorf("%s requires CEPH_MSGR_TEST_RUN to be its members' union %q", sequenceEnv, union)
	}
	var first profile
	var alone []profile
	for i, member := range members {
		p, err := loadSingleProfile(func(name string) string {
			if name == "CEPH_MSGR_TEST_RUN" {
				return member
			}
			return getenv(name)
		})
		if err != nil {
			return profile{}, fmt.Errorf("%s member %q: %w", sequenceEnv, member, err)
		}
		if p.runsDefault() || p.osdSoak != "" {
			// Runtime paths compare the selection with the fixture's
			// default (runsDefault); a union would change them.
			return profile{}, fmt.Errorf("%s member %q is its fixture's default selection or a soak; it runs alone", sequenceEnv, member)
		}
		alone = append(alone, p)
		p.testRun = ""
		if i == 0 {
			first = p
		} else if p != first {
			return profile{}, fmt.Errorf("%s member %q loads another fixture profile than %q", sequenceEnv, member, members[0])
		}
	}
	first.testRun = strings.Join(members, "|")
	first.sequence = strings.Join(members, " ")
	// The runtime reads the selection only through the capability
	// admissions and the tests' environment; both must be what each member
	// alone gets, or the sequence would start another fixture.
	for i, p := range alone {
		for name, c := range fixtureCapabilities {
			if p.admits(c) != first.admits(c) {
				return profile{}, fmt.Errorf("%s member %q: capability %s is admitted %t alone and %t in the sequence", sequenceEnv, members[i], name, p.admits(c), first.admits(c))
			}
		}
		if !maps.Equal(p.testEnvironment("/out", ""), first.testEnvironment("/out", "")) {
			return profile{}, fmt.Errorf("%s member %q gets another test environment alone", sequenceEnv, members[i])
		}
	}
	return first, nil
}

// sequenceSteps are the fixture operations a sequence uses.
type sequenceSteps struct {
	// alive reports a failed fixture control loop.
	alive func() error
	// list prints the suite's top-level tests that selector selects.
	list func(ctx context.Context, suite, selector string) (string, error)
	// run runs one top-level test of the suite, verbosely, to stdout.
	run func(ctx context.Context, suite, test string) error
	// check is one bounded start-state check.
	check func(ctx context.Context) (startStateReport, error)
	// record keeps the progress record.
	record func(sequenceRecord) error
	stdout io.Writer
}

type sequenceRecord struct {
	Sequence []string        `json:"sequence"`
	Tests    []sequenceEntry `json:"tests"`
	// Stopped names why the sequence ended before its last test, if it did.
	Stopped string `json:"stopped,omitempty"`
}

type sequenceEntry struct {
	Suite    string    `json:"suite"`
	Test     string    `json:"test"`
	Selector string    `json:"selector"`
	Started  time.Time `json:"started"`
	Ended    time.Time `json:"ended"`
	Passed   bool      `json:"passed"`
	// StartStateAfter is the check that ran after this test.
	StartStateAfter *startStateReport `json:"start_state_after,omitempty"`
}

func runSequence(ctx context.Context, p profile, steps sequenceSteps) (result error) {
	record := sequenceRecord{Sequence: strings.Fields(p.sequence)}
	defer func() {
		if result != nil {
			record.Stopped = result.Error()
		}
		result = errors.Join(result, steps.record(record))
	}()
	// checkAfter requires the start state after the latest test.
	checkAfter := func() error {
		last := &record.Tests[len(record.Tests)-1]
		name := last.Suite + "/" + last.Test
		report, err := steps.check(ctx)
		last.StartStateAfter = &report
		if err != nil {
			// A check that could not run is a failed check of this test too.
			fmt.Fprintf(steps.stdout, "%s%s: FAILED: check error: %v\n", sequenceStartStateLine, name, err)
			return fmt.Errorf("start-state check after %s: %w", name, err)
		}
		if !report.Passed {
			failures := make([]string, 0, len(report.Failures))
			for _, f := range report.Failures {
				failures = append(failures, f.String())
			}
			fmt.Fprintf(steps.stdout, "%s%s: FAILED: %s\n", sequenceStartStateLine, name, strings.Join(failures, "; "))
			return fmt.Errorf("the fixture is not in its start state after %s: %s", name, strings.Join(failures, "; "))
		}
		fmt.Fprintf(steps.stdout, "%s%s: passed (%d polls, %d ms)\n", sequenceStartStateLine, name, report.Polls, report.ElapsedMS)
		return nil
	}
	seen := map[string]bool{}
	for _, selector := range strings.Fields(p.sequence) {
		for _, suite := range suiteNames {
			if err := steps.alive(); err != nil {
				return err
			}
			listing, err := steps.list(ctx, suite, selector)
			if err != nil {
				return fmt.Errorf("%s native suite listing of %s: %w", suite, selector, err)
			}
			for _, line := range strings.Split(listing, "\n") {
				test := strings.TrimSpace(line)
				if !listedTest.MatchString(test) || seen[suite+"/"+test] {
					continue
				}
				seen[suite+"/"+test] = true
				if len(record.Tests) != 0 {
					if err := checkAfter(); err != nil {
						return err
					}
				}
				if err := steps.alive(); err != nil {
					return err
				}
				entry := sequenceEntry{Suite: suite, Test: test, Selector: selector, Started: time.Now().UTC()}
				fmt.Fprintf(steps.stdout, "Running Ceph test suite: %s\n", suite)
				err := steps.run(ctx, suite, test)
				entry.Ended, entry.Passed = time.Now().UTC(), err == nil
				record.Tests = append(record.Tests, entry)
				if err != nil {
					return fmt.Errorf("sequence stopped after %s/%s failed: %w", suite, test, err)
				}
			}
		}
	}
	if len(record.Tests) == 0 {
		return errors.New("the sequence selected no test")
	}
	return checkAfter()
}

// sequenceRunner binds a sequence to the started fixture.
func sequenceRunner(c *fixtureCluster, out, proxy string, p profile, budget jobBudget) sequenceSteps {
	return sequenceSteps{
		alive: c.checkControl,
		list: func(ctx context.Context, suite, selector string) (string, error) {
			var listing bytes.Buffer
			err := c.runSuite(ctx, out, proxy, p, suite, []string{"-test.list", selector}, &listing)
			return listing.String(), err
		},
		run: func(ctx context.Context, suite, test string) error {
			timeout, err := budget.suiteTimeout(p, time.Now(), true)
			if err != nil {
				return fmt.Errorf("not started: %w", err)
			}
			return c.runSuite(ctx, out, proxy, p, suite, []string{"-test.run", "^" + test + "$", "-test.v", "-test.timeout", timeout}, os.Stdout)
		},
		check: func(ctx context.Context) (startStateReport, error) {
			return c.verifyStartState(ctx, out, sequenceStartStateBudget)
		},
		record: func(r sequenceRecord) error {
			data, err := json.MarshalIndent(r, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(out, sequenceRecordFile), append(data, '\n'), 0o644)
		},
		stdout: os.Stdout,
	}
}

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	sequenceRecordFile,
)

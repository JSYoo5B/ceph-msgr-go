package main

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// A suite part runs one contiguous part of a fixture's default selection on
// a fixture of its own. A broad suite row (//scenario:suite) runs every test
// its fixture configuration enables, the api suite and then the client
// suite, in one native job; when that selection outgrows the job's budget,
// the row is declared as several rows of one configuration that differ only
// in suite_part=K/N, and part K runs the K-th of N contiguous slices of the
// selection:
//
//   - The selection is what Go lists for each suite with the default
//     selector (-test.list), in suite order (api, client) and then listing
//     order, each top-level name once. Part K of N is its slice
//     [(K-1)*n/N, K*n/N) of the n names (suitePartTests): the N parts are
//     disjoint, their union is the selection, and their sizes differ by at
//     most one. Each part keeps the selection's order, so a test runs after
//     the same tests as in the whole run, apart from those of earlier parts;
//     the first test of a part runs on a fresh fixture, as the first test of
//     the whole run does. The client suite's final recovery test, which
//     leaves a MON down, stays the last test of its part.
//   - The partition needs no list of tests: a new test joins the part its
//     place in the listing gives it. TestSuitePartsCoverTheirSelectionOnce
//     requires every partitioned configuration to declare exactly the parts
//     1..N and no whole row beside them, and, on the listing of the tree,
//     each part to select at most suitePartMaxTests tests, so the growth of
//     the selection fails a cluster-free check before it exhausts a job.
//   - The profile, the fixture and the tests' environment are the whole
//     row's: only the selection of each suite run differs (-test.run of the
//     part's exact names). A part of a suite with no test of the part is not
//     run.
//
// Before its first suite runs, the default path announces the top-level
// tests each suite run selects ("Ceph test suite <suite> selects ..."), a
// part's or the whole row's, and a suite run fails when a selected test
// printed no verdict. A suite that exits early therefore leaves the tests of
// the later suites announced and unreported. The result gate (tools/native_test_results.py) requires a verdict
// of every announced test as well, so a broad run that was cut off reports
// the tests it did not reach.

// suitePartInput is the row parameter suite_part=K/N; empty runs the whole
// selection.
var suitePartInput = defineInput("suite_part", "")

// suitePartMaxParts bounds N; suitePartMaxTests bounds the tests of one
// part. The test bound is the cluster-free proxy of a part's time: on the
// hosted three-OSD rows a listed test of the broad OSD selection took about
// 11 seconds on average, passed and skipped tests together (2,131 s for 197
// tests), and a 45-minute job leaves its api suite about 36 minutes. 140
// tests are about 26 minutes, which keeps a margin for the hosted runners'
// variance and for tests that take longer than the average. A part that
// would exceed it needs another part row, not a larger bound.
const (
	suitePartMaxParts = 8
	suitePartMaxTests = 140
)

var suitePartSyntax = regexp.MustCompile(`^([1-9][0-9]*)/([1-9][0-9]*)$`)

// suitePart is the profile's part (K, N), or (0, 0) for the whole selection.
func (p profile) suitePart() (int, int, error) {
	return parseSuitePart(p.input(suitePartInput))
}

func parseSuitePart(value string) (int, int, error) {
	if value == "" {
		return 0, 0, nil
	}
	match := suitePartSyntax.FindStringSubmatch(value)
	if match == nil {
		return 0, 0, fmt.Errorf("%s=%q is not K/N", suitePartInput.variable, value)
	}
	k, errK := strconv.Atoi(match[1])
	n, errN := strconv.Atoi(match[2])
	if errK != nil || errN != nil || n < 2 || n > suitePartMaxParts || k > n {
		return 0, 0, fmt.Errorf("%s=%q needs 2 <= N <= %d and 1 <= K <= N", suitePartInput.variable, value, suitePartMaxParts)
	}
	return k, n, nil
}

// suitePartProblem admits a part only of a run of the fixture's own default
// selection: not with an explicit or capability-admitted selection, a fixture
// sequence, a soak or a routed RGW checkpoint, whose selections are already
// one test or fixed.
func suitePartProblem(p profile) error {
	k, _, err := p.suitePart()
	if err != nil || k == 0 {
		return err
	}
	if p.rgwNative == "1" || p.sequence != "" || p.osdSoak != "" || !p.runsDefault() || p.capabilities != "" {
		return fmt.Errorf("%s partitions only a fixture's default selection, without CEPH_MSGR_TEST_RUN, a capability, a sequence, a soak or %s", suitePartInput.variable, rgwNativeEnv)
	}
	return nil
}

// suitePartTests is part k of n of the selection (one listing per suite, in
// suiteNames order), per suite; k == 0 is the whole selection. A name listed
// twice in one suite counts once.
func suitePartTests(selection [][]string, k, n int) [][]string {
	type entry struct {
		suite int
		name  string
	}
	var all []entry
	for suite, names := range selection {
		seen := map[string]bool{}
		for _, name := range names {
			if !seen[name] {
				seen[name] = true
				all = append(all, entry{suite, name})
			}
		}
	}
	first, last := 0, len(all)
	if k != 0 {
		first, last = (k-1)*len(all)/n, k*len(all)/n
	}
	part := make([][]string, len(selection))
	for suite := range part {
		part[suite] = []string{}
	}
	for _, e := range all[first:last] {
		part[e.suite] = append(part[e.suite], e.name)
	}
	return part
}

// suiteSelectionLine announces the tests a suite run selects; the result
// gate reads it.
func suiteSelectionLine(suite string, tests []string) string {
	line := fmt.Sprintf("Ceph test suite %s selects %d top-level tests", suite, len(tests))
	if len(tests) != 0 {
		line += ": " + strings.Join(tests, " ")
	}
	return line
}

// exactSelector selects exactly the named top-level tests.
func exactSelector(tests []string) string {
	return "^(" + strings.Join(tests, "|") + ")$"
}

// suiteSteps are the fixture operations the default path uses.
type suiteSteps struct {
	// alive reports a failed fixture control loop.
	alive func() error
	// list prints the suite's top-level tests that selector selects.
	list func(ctx context.Context, suite, selector string) (string, error)
	// run runs the suite's tests that selector selects, verbosely, to w.
	run    func(ctx context.Context, suite, selector string, w io.Writer) error
	stdout io.Writer
}

// runSuites runs the fixture's selection (a fixture sequence runs instead
// with runSequence): each suite in order, the whole selection or the
// profile's part of it, after announcing the tests of every suite run.
func runSuites(ctx context.Context, p profile, steps suiteSteps) error {
	k, n, err := p.suitePart()
	if err != nil {
		return err
	}
	selection := make([][]string, len(suiteNames))
	for i, suite := range suiteNames {
		if err := steps.alive(); err != nil {
			return err
		}
		listing, err := steps.list(ctx, suite, p.testRun)
		if err != nil {
			return fmt.Errorf("%s native suite listing: %w", suite, err)
		}
		selection[i] = []string{}
		for _, line := range strings.Split(listing, "\n") {
			if test := strings.TrimSpace(line); listedTest.MatchString(test) {
				selection[i] = append(selection[i], test)
			}
		}
	}
	selected := suitePartTests(selection, k, n)
	if k != 0 && !slices.ContainsFunc(selected, func(tests []string) bool { return len(tests) != 0 }) {
		return fmt.Errorf("suite part %d/%d selects no test", k, n)
	}
	// Every suite's selection is announced before any suite runs, so a suite
	// that exits early still leaves the tests of the later suites required.
	for i, suite := range suiteNames {
		fmt.Fprintln(steps.stdout, suiteSelectionLine(suite, selected[i]))
	}
	for i, suite := range suiteNames {
		selector := p.testRun
		if k != 0 {
			if len(selected[i]) == 0 {
				continue
			}
			selector = exactSelector(selected[i])
		}
		if err := steps.alive(); err != nil {
			return err
		}
		verdicts := &verdictRecorder{}
		if err := steps.run(ctx, suite, selector, io.MultiWriter(steps.stdout, verdicts)); err != nil {
			return err
		}
		if missing := verdicts.missing(selected[i]); len(missing) != 0 {
			return fmt.Errorf("%s native suite printed no verdict of %s", suite, strings.Join(missing, ", "))
		}
	}
	return nil
}

// topLevelVerdict is a complete, unindented Go verbose result line, as the
// result gate reads it.
var topLevelVerdict = regexp.MustCompile(`^--- (?:PASS|FAIL|SKIP): (Test\w*) \(`)

// verdictRecorder records the top-level verdicts of a suite run's output.
type verdictRecorder struct {
	mu      sync.Mutex
	partial []byte
	seen    map[string]bool
}

func (r *verdictRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partial = append(r.partial, data...)
	for {
		end := slices.Index(r.partial, '\n')
		if end < 0 {
			break
		}
		if match := topLevelVerdict.FindSubmatch(r.partial[:end]); match != nil {
			if r.seen == nil {
				r.seen = map[string]bool{}
			}
			r.seen[string(match[1])] = true
		}
		r.partial = r.partial[end+1:]
	}
	return len(data), nil
}

// missing is the tests without a recorded verdict.
func (r *verdictRecorder) missing(tests []string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var missing []string
	for _, test := range tests {
		if !r.seen[test] {
			missing = append(missing, test)
		}
	}
	return missing
}

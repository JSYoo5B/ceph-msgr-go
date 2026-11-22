package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// suitePartCoverageProblems checks the parts 1..n of a listing: together
// they are the listing (each name once, in suite and listing order), each is
// nonempty and none has more than limit tests.
func suitePartCoverageProblems(listing [][]string, n, limit int) []string {
	var problems []string
	joined := make([][]string, len(listing))
	for k := 1; k <= n; k++ {
		part := suitePartTests(listing, k, n)
		size := len(slices.Concat(part...))
		if size == 0 {
			problems = append(problems, fmt.Sprintf("part %d/%d selects no test", k, n))
		}
		if size > limit {
			problems = append(problems, fmt.Sprintf("part %d/%d selects %d tests, more than suitePartMaxTests (%d): declare another part (suite_part=K/%d on every part)", k, n, size, limit, n+1))
		}
		for suite := range part {
			joined[suite] = append(joined[suite], part[suite]...)
		}
	}
	for suite, names := range listing {
		var unique []string
		for _, name := range names {
			if !slices.Contains(unique, name) {
				unique = append(unique, name)
			}
		}
		if !slices.Equal(joined[suite], unique) {
			problems = append(problems, fmt.Sprintf("%s suite: the parts run %v, the selection is %v", suiteNames[suite], joined[suite], unique))
		}
	}
	return problems
}

func TestSuitePartTestsPartitionEveryListing(t *testing.T) {
	for api := 0; api <= 12; api++ {
		for client := 0; client <= 4; client++ {
			listing := [][]string{{}, {}}
			for i := range api {
				listing[0] = append(listing[0], fmt.Sprintf("TestA%d", i))
			}
			for i := range client {
				listing[1] = append(listing[1], fmt.Sprintf("TestC%d", i))
			}
			if whole := suitePartTests(listing, 0, 0); !slices.Equal(whole[0], listing[0]) || !slices.Equal(whole[1], listing[1]) {
				t.Fatalf("whole selection %v of %v", whole, listing)
			}
			for n := 2; n <= suitePartMaxParts; n++ {
				problems := suitePartCoverageProblems(listing, n, api+client)
				if api+client >= n && len(problems) != 0 {
					t.Fatalf("%d+%d tests in %d parts: %v", api, client, n, problems)
				}
				sizes := []int{}
				for k := 1; k <= n; k++ {
					sizes = append(sizes, len(slices.Concat(suitePartTests(listing, k, n)...)))
				}
				if slices.Max(sizes)-slices.Min(sizes) > 1 {
					t.Fatalf("%d+%d tests in %d parts of sizes %v", api, client, n, sizes)
				}
			}
		}
	}
	// A name listed twice in one suite (an internal and an external test
	// package) is one test of one part.
	listing := [][]string{{"TestA", "TestB", "TestA", "TestC"}, {"TestD"}}
	if problems := suitePartCoverageProblems(listing, 2, 4); len(problems) != 0 {
		t.Fatal(problems)
	}
	if got := suitePartTests(listing, 1, 2); !slices.Equal(got[0], []string{"TestA", "TestB"}) || len(got[1]) != 0 {
		t.Fatalf("part 1/2 %v", got)
	}
	if got := suitePartTests(listing, 2, 2); !slices.Equal(got[0], []string{"TestC"}) || !slices.Equal(got[1], []string{"TestD"}) {
		t.Fatalf("part 2/2 %v", got)
	}
	// The coverage check reports a part over the bound and an empty part.
	if problems := suitePartCoverageProblems(listing, 2, 1); len(problems) != 2 {
		t.Fatalf("bound of one test: %v", problems)
	}
	if problems := suitePartCoverageProblems([][]string{{"TestA"}, {}}, 2, 4); len(problems) != 1 || !strings.Contains(problems[0], "selects no test") {
		t.Fatalf("one test in two parts: %v", problems)
	}
}

func TestSuitePartIsAdmittedOnlyForTheDefaultSelection(t *testing.T) {
	threeOSD := func(change map[string]string) func(string) string {
		env := map[string]string{
			"CEPH_MSGR_TEST_KEY_TYPE": "aes256k", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes256k", "CEPH_MSGR_TEST_IP_FAMILY": "4",
			"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3",
		}
		maps.Copy(env, change)
		return func(name string) string { return env[name] }
	}
	whole, err := loadProfile(threeOSD(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"1/2", "2/2", "3/8"} {
		p, err := loadProfile(threeOSD(map[string]string{suitePartInput.variable: value}))
		if err != nil {
			t.Fatalf("suite_part=%s refused: %v", value, err)
		}
		if p.testRun != whole.testRun || !p.runsDefault() {
			t.Fatalf("suite_part=%s changed the selection to %s", value, p.testRun)
		}
		if k, n, _ := p.suitePart(); fmt.Sprintf("%d/%d", k, n) != value {
			t.Fatalf("suite_part=%s read as %d/%d", value, k, n)
		}
	}
	for name, change := range map[string]map[string]string{
		"malformed":         {suitePartInput.variable: "1-2"},
		"one part":          {suitePartInput.variable: "1/1"},
		"part zero":         {suitePartInput.variable: "0/2"},
		"part after N":      {suitePartInput.variable: "3/2"},
		"too many parts":    {suitePartInput.variable: fmt.Sprintf("1/%d", suitePartMaxParts+1)},
		"leading zero":      {suitePartInput.variable: "01/2"},
		"explicit selector": {suitePartInput.variable: "1/2", "CEPH_MSGR_TEST_RUN": "^TestCephOSDIOIntegration$"},
		"soak":              {suitePartInput.variable: "1/2", "CEPH_MSGR_TEST_OSD_SOAK": "4m"},
	} {
		if p, err := loadProfile(threeOSD(change)); err == nil || p != (profile{}) {
			t.Errorf("%s: admitted %+v", name, p)
		}
	}
	routed := map[string]string{rgwNativeEnv: "1", "CEPH_MSGR_TEST_RUN": "^TestRGWNativeCheckpointIntegration$", "CEPH_MSGR_TEST_KEY_TYPE": "aes256k", "CEPH_MSGR_TEST_IP_FAMILY": "4"}
	if _, err := loadProfile(func(name string) string { return routed[name] }); err != nil {
		t.Fatal("routed checkpoint refused", err)
	}
	routed[suitePartInput.variable] = "1/2"
	if _, err := loadProfile(func(name string) string { return routed[name] }); err == nil {
		t.Error("a routed checkpoint admitted a suite part")
	}
}

// fakeSuites is a fixture whose suites list and run the named tests,
// printing a verdict of each test a run selects except those in silent.
type fakeSuites struct {
	tests  map[string][]string
	silent map[string]bool
	runs   []string
	out    bytes.Buffer
}

func (f *fakeSuites) steps() suiteSteps {
	return suiteSteps{
		alive: func() error { return nil },
		list: func(_ context.Context, suite, selector string) (string, error) {
			return strings.Join(append(slices.Clone(f.tests[suite]), "ok  \tpackage\t0.1s"), "\n") + "\n", nil
		},
		run: func(_ context.Context, suite, selector string, w io.Writer) error {
			f.runs = append(f.runs, suite+" "+selector)
			fmt.Fprintf(w, "Running Ceph test suite: %s\n", suite)
			selects := regexp.MustCompile(selector)
			for _, test := range f.tests[suite] {
				if !selects.MatchString(test) {
					continue
				}
				fmt.Fprintf(w, "=== RUN   %s\n", test)
				if !f.silent[test] {
					// Split the verdict over two writes.
					fmt.Fprintf(w, "--- PASS: %s", test)
					fmt.Fprintf(w, " (0.01s)\n")
				}
			}
			return nil
		},
		stdout: &f.out,
	}
}

func suitePartProfile(t *testing.T, value string) profile {
	t.Helper()
	p := profile{testRun: "^Test"}
	p.inputs = suitePartInput.variable + "=" + value
	return p
}

func TestRunSuitesAnnouncesAndRunsOnlyThePart(t *testing.T) {
	tests := map[string][]string{"api": {"TestA", "TestB", "TestC"}, "client": {"TestD"}}
	for _, test := range []struct {
		part, wantRuns, wantAnnounced string
	}{
		{"", "api ^Test|client ^Test",
			"Ceph test suite api selects 3 top-level tests: TestA TestB TestC\nCeph test suite client selects 1 top-level tests: TestD\n"},
		{"1/2", "api ^(TestA|TestB)$",
			"Ceph test suite api selects 2 top-level tests: TestA TestB\nCeph test suite client selects 0 top-level tests\n"},
		{"2/2", "api ^(TestC)$|client ^(TestD)$",
			"Ceph test suite api selects 1 top-level tests: TestC\nCeph test suite client selects 1 top-level tests: TestD\n"},
	} {
		f := &fakeSuites{tests: tests}
		if err := runSuites(context.Background(), suitePartProfile(t, test.part), f.steps()); err != nil {
			t.Fatalf("part %q: %v", test.part, err)
		}
		if got := strings.Join(f.runs, "|"); got != test.wantRuns {
			t.Errorf("part %q ran %s, want %s", test.part, got, test.wantRuns)
		}
		var announced strings.Builder
		for _, line := range strings.SplitAfter(f.out.String(), "\n") {
			if strings.HasPrefix(line, "Ceph test suite ") {
				announced.WriteString(line)
			}
		}
		if announced.String() != test.wantAnnounced {
			t.Errorf("part %q announced\n%s\nwant\n%s", test.part, announced.String(), test.wantAnnounced)
		}
	}
}

func TestRunSuitesFailsWithoutAVerdictOfASelectedTest(t *testing.T) {
	f := &fakeSuites{tests: map[string][]string{"api": {"TestA", "TestB"}, "client": {"TestD"}}, silent: map[string]bool{"TestB": true}}
	err := runSuites(context.Background(), suitePartProfile(t, ""), f.steps())
	if err == nil || !strings.Contains(err.Error(), "api native suite printed no verdict of TestB") {
		t.Fatalf("missing verdict: %v", err)
	}
	if len(f.runs) != 1 {
		t.Fatalf("the client suite ran after the api suite failed: %v", f.runs)
	}
	f = &fakeSuites{tests: map[string][]string{"api": {"TestA"}, "client": {}}}
	if err := runSuites(context.Background(), suitePartProfile(t, "1/2"), f.steps()); err == nil || !strings.Contains(err.Error(), "selects no test") {
		t.Fatalf("an empty part: %v", err)
	}
	stopped := errors.New("control loop failed")
	f = &fakeSuites{tests: map[string][]string{"api": {"TestA"}, "client": {"TestD"}}}
	steps := f.steps()
	steps.alive = func() error { return stopped }
	if err := runSuites(context.Background(), suitePartProfile(t, ""), steps); !errors.Is(err, stopped) || len(f.runs) != 0 {
		t.Fatalf("a failed control loop: %v, runs %v", err, f.runs)
	}
}

func TestVerdictRecorderReadsOnlyTopLevelVerdicts(t *testing.T) {
	r := &verdictRecorder{}
	for _, chunk := range []string{"=== RUN   TestA\n    --- PASS: TestA/sub (0.00s)\n--- SKI", "P: TestA (0.00s)\n", "--- FAIL: TestB (1.00s)\n", "--- PASS: TestC"} {
		if _, err := io.WriteString(r, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if missing := r.missing([]string{"TestA", "TestB", "TestC"}); !slices.Equal(missing, []string{"TestC"}) {
		t.Fatalf("missing %v", missing)
	}
}

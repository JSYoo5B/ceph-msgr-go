package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// This is selection evidence, not proof that a broad fixture executes a test
// without skipping. Only explicit selectors have the required-parent PASS gate.
type ciTestSelection struct {
	Test       string   `json:"test"`
	Source     string   `json:"source"`
	Candidates []string `json:"candidate_scenarios"`
	Required   []string `json:"required_scenarios"`
}

type ciScenario struct {
	ID     string            `json:"id"`
	Params map[string]string `json:"params"`
}

// ciForwarder is the trusted native workflow's forwarding of a job's matrix
// to the runner's environment (tools/native_env.py), started once per test
// binary: the one reading of a matrix into fixture inputs. Each request is
// one JSON matrix on a line and each answer one JSON line. The helper ends
// when this binary closes its input.
type ciForwarder struct {
	mu      sync.Mutex
	in      io.Writer
	answers *bufio.Scanner
}

var ciForwarding = sync.OnceValues(func() (*ciForwarder, error) {
	command := exec.Command("python3", filepath.Join("tools", "native_env.py"), "serve")
	command.Dir = filepath.Join("..", "..")
	command.Stderr = os.Stderr
	in, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	answers := bufio.NewScanner(out)
	answers.Buffer(make([]byte, 0, 64<<10), 16<<20)
	return &ciForwarder{in: in, answers: answers}, nil
})

// ciEnvironment is the environment the native workflow's runner receives for
// a matrix (a row's parameters, or a planned job), or the helper's refusal.
func ciEnvironment(matrix map[string]string) (map[string]string, error) {
	f, err := ciForwarding()
	if err != nil {
		return nil, fmt.Errorf("forwarding helper: %w", err)
	}
	request, err := json.Marshal(matrix)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := f.in.Write(append(request, '\n')); err != nil {
		return nil, fmt.Errorf("forwarding helper: %w", err)
	}
	if !f.answers.Scan() {
		return nil, fmt.Errorf("forwarding helper ended: %v", f.answers.Err())
	}
	var answer struct {
		Environment map[string]string `json:"environment"`
		Error       string            `json:"error"`
	}
	if err := json.Unmarshal(f.answers.Bytes(), &answer); err != nil {
		return nil, fmt.Errorf("forwarding helper: %w", err)
	}
	if answer.Error != "" {
		return nil, errors.New(answer.Error)
	}
	return answer.Environment, nil
}

// ciRowEnvironment is ciEnvironment of a matrix the helper must forward.
func ciRowEnvironment(t testing.TB, matrix map[string]string) map[string]string {
	t.Helper()
	env, err := ciEnvironment(matrix)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// ciProfile is the fixture's admission of a matrix as the native workflow
// forwards it: the helper's environment through loadEnvironmentProfile, which
// refuses a forwarded parameter that no fixture input reads.
func ciProfile(row ciScenario) (profile, error) {
	env, err := ciEnvironment(row.Params)
	if err != nil {
		return profile{}, err
	}
	environ := make([]string, 0, len(env))
	for name, value := range env {
		environ = append(environ, name+"="+value)
	}
	return loadEnvironmentProfile(environ)
}

// ciSuiteTestFiles returns the paths of the _test.go files of the suite
// package pkg under root that the build context selects (CGO disabled, as the
// listing in TestCINativeCompiledInventory): the internal and the external
// test package, without files a build constraint excludes.
func ciSuiteTestFiles(root, pkg string) ([]string, error) {
	ctx := build.Default
	ctx.CgoEnabled = false
	dir := filepath.Join(root, filepath.FromSlash(pkg))
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		// A synthetic tree may lack a suite; the repository's suites are
		// paired with go test -list (TestCINativeCompiledInventory).
		return nil, nil
	}
	info, err := ctx.ImportDir(dir, 0)
	var none *build.NoGoError
	if err != nil && !errors.As(err, &none) {
		return nil, err
	}
	var paths []string
	for _, name := range append(slices.Clone(info.TestGoFiles), info.XTestGoFiles...) {
		paths = append(paths, filepath.Join(dir, name))
	}
	return paths, nil
}

// ciNativeSuite is one package whose native tests catalog rows select, keyed
// "label/TestName". The product's API (integration) and client (cephmsgr)
// suites run on the controller's own fixture; the RGW checkpoints of this
// module (label fixture) start their own cluster, and only a row with
// rgw_native=1 routes to them (rgw_native_route.go).
type ciNativeSuite struct {
	label, dir string
	native     *regexp.Regexp
	routed     bool
}

var (
	ciProductNativeTest = regexp.MustCompile(`^TestCeph\w*Integration$`)
	ciRGWNativeTest     = rgwNativeCheckpoint
	ciNativeSuites      = []ciNativeSuite{
		{"integration", "integration", ciProductNativeTest, false},
		{"cephmsgr", "cephmsgr", ciProductNativeTest, false},
		{rgwNativeSuite, "integration/fixture", ciRGWNativeTest, true},
	}
)

// ciRoutedSuite reports whether label is the suite of rows with rgw_native=1.
func ciRoutedSuite(label string) bool {
	return slices.ContainsFunc(ciNativeSuites, func(suite ciNativeSuite) bool { return suite.label == label && suite.routed })
}

// ciNativeSources maps each native test to its declaring file. The files are
// the _test.go files the build context selects (CGO disabled, as the listing
// in TestCINativeCompiledInventory), parsed by Go's parser; that test pairs
// the names with go test -list, which decides which tests exist.
func ciNativeSources(root string) (map[string]string, error) {
	result := map[string]string{}
	for _, suite := range ciNativeSuites {
		paths, err := ciSuiteTestFiles(root, suite.dir)
		if err != nil {
			return nil, err
		}
		for _, path := range paths {
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return nil, err
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv != nil || !suite.native.MatchString(fn.Name.Name) {
					continue
				}
				key := suite.label + "/" + fn.Name.Name
				if _, exists := result[key]; exists {
					return nil, fmt.Errorf("duplicate native test %s", key)
				}
				result[key] = suite.dir + "/" + filepath.Base(path)
			}
		}
	}
	return result, nil
}

func ciInventoryProblems(declared map[string]string, listed []string) []string {
	var problems []string
	seen := map[string]bool{}
	if !slices.IsSorted(listed) {
		problems = append(problems, "native inventory is not sorted")
	}
	for _, name := range listed {
		if seen[name] {
			problems = append(problems, "duplicate inventory entry: "+name)
		}
		seen[name] = true
		if _, ok := declared[name]; !ok {
			problems = append(problems, "stale inventory entry: "+name)
		}
	}
	for name := range declared {
		if !seen[name] {
			problems = append(problems, "unregistered native test: "+name)
		}
	}
	slices.Sort(problems)
	return problems
}

// ciSelection joins every native test declared in the source with the catalog
// rows whose effective fixture selector includes it. The source declarations
// are the inventory, so there is no list to keep in step. A row that selects
// no native test and a native test that no row selects are problems.
func ciSelection(sources map[string]string, scenarios []ciScenario) ([]ciTestSelection, []string) {
	names := slices.Sorted(maps.Keys(sources))
	rows := make([]ciTestSelection, 0, len(names))
	for _, name := range names {
		rows = append(rows, ciTestSelection{Test: name, Source: sources[name], Candidates: []string{}, Required: []string{}})
	}
	var problems []string
	for _, scenario := range scenarios {
		p, err := ciProfile(scenario)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", scenario.ID, err))
			continue
		}
		selector, err := regexp.Compile(p.testRun)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", scenario.ID, err))
			continue
		}
		matches := 0
		for index := range rows {
			label, name, _ := strings.Cut(rows[index].Test, "/")
			// A routed row selects only the RGW checkpoints, every other
			// row only the suites the controller runs on its fixture.
			if ciRoutedSuite(label) != (p.rgwNative == "1") || !selector.MatchString(name) {
				continue
			}
			matches++
			rows[index].Candidates = append(rows[index].Candidates, scenario.ID)
			if scenario.Params["test_run"] != "" {
				rows[index].Required = append(rows[index].Required, scenario.ID)
			}
		}
		if matches == 0 {
			problems = append(problems, fmt.Sprintf("%s: effective fixture selector matches no native tests", scenario.ID))
		}
	}
	for _, row := range rows {
		if len(row.Candidates) == 0 {
			problems = append(problems, "no catalog selector includes "+row.Test)
		}
	}
	return rows, problems
}

func TestCICatalogInventory(t *testing.T) {
	root := filepath.Join("..", "..")
	scenarios := ciRepositoryCatalog(t)
	sources, err := ciNativeSources(root)
	if err != nil {
		t.Fatal(err)
	}
	rows, problems := ciSelection(sources, scenarios)
	for _, problem := range problems {
		t.Error(problem)
	}
	broad := 0
	for _, row := range rows {
		if len(row.Required) == 0 {
			broad++
		}
	}
	t.Logf("%d native tests; %d have only broad candidate selection (optional skips remain possible)", len(rows), broad)
	if output := os.Getenv("CEPH_MSGR_CI_SELECTION_REPORT"); output != "" {
		data, err := json.MarshalIndent(struct {
			Schema string            `json:"schema"`
			Scope  string            `json:"scope"`
			Valid  bool              `json:"valid"`
			Tests  []ciTestSelection `json:"tests"`
		}{"native-test-selection/v1", "Static selection only; broad candidates may skip and are not native PASS evidence.", !t.Failed(), rows}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(output, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCINativeCompiledInventory(t *testing.T) {
	root := filepath.Join("..", "..")
	sources, err := ciNativeSources(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// -list compiles and lists the root-module suites; it executes no test body.
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-json", "-list", "^TestCeph.*Integration$", "./integration", "./cephmsgr")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("compiled native inventory failed: %v", err)
	}
	var compiled []string
	native := regexp.MustCompile(`^TestCeph\w*Integration$`)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		var event struct{ Action, Package, Output string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		name := strings.TrimSpace(event.Output)
		if event.Action == "output" && native.MatchString(name) {
			compiled = append(compiled, filepath.Base(event.Package)+"/"+name)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	compiled = append(compiled, ciCompiledRoutedTests(t)...)
	slices.Sort(compiled)
	if problems := ciInventoryProblems(sources, compiled); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

// ciCompiledRoutedTests lists the RGW checkpoints of this module as go test
// -list compiles them, keyed like ciNativeSources.
func ciCompiledRoutedTests(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-list", "^TestRGWNative.*Integration$", ".")
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("compiled RGW checkpoint inventory failed: %v", err)
	}
	var compiled []string
	for _, line := range strings.Split(string(output), "\n") {
		if ciRGWNativeTest.MatchString(line) {
			compiled = append(compiled, rgwNativeSuite+"/"+line)
		}
	}
	return compiled
}

// TestCIRoutedTestsAreTheDeclaredRGWCheckpoints: the route admits every RGW
// checkpoint this module declares by its exact selector, by the same name
// rule that finds them, so a new checkpoint needs no route entry.
func TestCIRoutedTestsAreTheDeclaredRGWCheckpoints(t *testing.T) {
	declared := ciDeclaredRoutedTests(t)
	if len(declared) == 0 {
		t.Fatal("the module declares no RGW checkpoint")
	}
	for _, name := range declared {
		if routed, ok := rgwNativeTarget("^" + name + "$"); !ok || routed != name {
			t.Errorf("declared RGW checkpoint %s is not routed: %q %t", name, routed, ok)
		}
	}
}

// ciDeclaredRoutedTests is the sorted names of the RGW checkpoints this
// module declares.
func ciDeclaredRoutedTests(t *testing.T) []string {
	t.Helper()
	sources, err := ciNativeSources(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var declared []string
	for key := range sources {
		if label, name, _ := strings.Cut(key, "/"); ciRoutedSuite(label) {
			declared = append(declared, name)
		}
	}
	slices.Sort(declared)
	return declared
}

func TestCIInventoryRefusesMissingStaleAndDuplicateEntries(t *testing.T) {
	declared := map[string]string{"integration/TestCephAIntegration": "a.go"}
	for _, listed := range [][]string{{}, {"integration/TestCephGoneIntegration"}, {"integration/TestCephAIntegration", "integration/TestCephAIntegration"}} {
		if len(ciInventoryProblems(declared, listed)) == 0 {
			t.Fatalf("accepted %v", listed)
		}
	}
}

func TestCISelectionRefusesUnselectedTestsAndEmptyRows(t *testing.T) {
	sources := map[string]string{
		"integration/TestCephAIntegration": "integration/a_test.go",
		"cephmsgr/TestCephBIntegration":    "cephmsgr/b_test.go",
	}
	row := func(id, selector string) ciScenario {
		return ciScenario{ID: id, Params: map[string]string{"test_run": selector}}
	}
	rows, problems := ciSelection(sources, []ciScenario{row("a", "^TestCephAIntegration$"), row("b", "^TestCephBIntegration$")})
	if len(problems) != 0 || len(rows) != 2 || rows[0].Test != "cephmsgr/TestCephBIntegration" || !slices.Equal(rows[1].Required, []string{"a"}) {
		t.Fatalf("complete selection: rows %+v problems %v", rows, problems)
	}
	for name, test := range map[string]struct {
		scenarios []ciScenario
		want      string
	}{
		"native test without a row": {[]ciScenario{row("a", "^TestCephAIntegration$")}, "no catalog selector includes cephmsgr/TestCephBIntegration"},
		"row that selects nothing":  {[]ciScenario{row("a", "^TestCeph[AB]Integration$"), row("gone", "^TestCephGoneIntegration$")}, "gone: effective fixture selector matches no native tests"},
		"invalid selector":          {[]ciScenario{row("a", "^TestCeph[AB]Integration$"), row("bad", "(")}, "bad: "},
	} {
		_, problems := ciSelection(sources, test.scenarios)
		if !slices.ContainsFunc(problems, func(problem string) bool { return strings.HasPrefix(problem, test.want) }) {
			t.Errorf("%s: problems %v lack %q", name, problems, test.want)
		}
	}
}

func TestCINativeSourcesRefuseDuplicateNames(t *testing.T) {
	root := t.TempDir()
	for _, pkg := range []string{"integration", "cephmsgr"} {
		if err := os.Mkdir(filepath.Join(root, pkg), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, path), []byte("package x\n\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	native := "func TestCephAIntegration(t *testing.T) {}\n"
	write("integration/a_test.go", native+"func TestCephAHelper(t *testing.T) {}\nfunc (s suite) TestCephMethodIntegration(t *testing.T) {}\n")
	write("cephmsgr/a_test.go", native)
	sources, err := ciNativeSources(root)
	if err != nil || !maps.Equal(sources, map[string]string{"integration/TestCephAIntegration": "integration/a_test.go", "cephmsgr/TestCephAIntegration": "cephmsgr/a_test.go"}) {
		t.Fatalf("sources %v, err %v", sources, err)
	}
	// A build-ignored file and a raw string that reads like a declaration
	// add no native test; neither does a file outside the test files.
	if err := os.WriteFile(filepath.Join(root, "integration", "ignored_test.go"), []byte("//go:build ignore\n\npackage x\n\nfunc TestCephIgnoredIntegration(t *testing.T) {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	write("integration/doc_test.go", "const doc = `\nfunc TestCephTextIntegration(t *testing.T) {}\n`\n")
	write("cephmsgr/helper.go", "func TestCephSourceIntegration(t *testing.T) {}\n")
	sources, err = ciNativeSources(root)
	if err != nil || !maps.Equal(sources, map[string]string{"integration/TestCephAIntegration": "integration/a_test.go", "cephmsgr/TestCephAIntegration": "cephmsgr/a_test.go"}) {
		t.Fatalf("sources %v, err %v", sources, err)
	}
	write("integration/b_test.go", native)
	if sources, err := ciNativeSources(root); err == nil {
		t.Fatalf("accepted a duplicate native test: %v", sources)
	}
}

// The fixture refuses a forwarded parameter that no input reads, and an
// invalid topology; the helper refuses a parameter that names a variable the
// workflow sets itself or another parameter's variable. A parameter that an
// input reads is admitted without any list naming it.
func TestCIProfileRefusesUnreadParametersAndInvalidTopology(t *testing.T) {
	base := map[string]string{"key": "aes256k", "service": "aes256k", "family": "4"}
	with := func(name, value string) ciScenario {
		params := maps.Clone(base)
		params[name] = value
		return ciScenario{ID: "a.1", Params: params}
	}
	if _, err := ciProfile(ciScenario{ID: "a.1", Params: base}); err != nil {
		t.Fatal("the minimal row is refused:", err)
	}
	for _, test := range []struct{ name, value, want string }{
		{"unknown", "1", "no fixture input reads CEPH_MSGR_TEST_UNKNOWN"},
		{"mds_commands", "1", ""},
		{"diagnostics", "/tmp/x", "CEPH_MSGR_TEST_DIAGNOSTICS is set by the native workflow itself"},
		{"key_type", "aes", "CEPH_MSGR_TEST_KEY_TYPE is the variable of parameter key"},
	} {
		_, err := ciProfile(with(test.name, test.value))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s=%s: error %v, want %q", test.name, test.value, err, test.want)
		}
	}
	// An input that loadProfile reads but no legacy binding named is admitted
	// by that reading alone.
	if p, err := ciProfile(with("timeout", "20m")); err != nil || p.testTimeout != "20m" {
		t.Fatal("an input the fixture reads is refused:", p.testTimeout, err)
	}
	// The refusal is loadEnvironmentProfile's, so a hand-set variable is
	// refused at run time as well.
	if _, err := loadEnvironmentProfile([]string{"CEPH_MSGR_TEST_UNKNOWN=1", "CEPH_MSGR_TEST_KEY_TYPE=aes"}); err == nil ||
		!strings.Contains(err.Error(), "no fixture input reads CEPH_MSGR_TEST_UNKNOWN") {
		t.Fatal("an unread variable was admitted:", err)
	}
	if p, err := loadEnvironmentProfile([]string{"CEPH_MSGR_TEST_KEY_TYPE=aes", "CEPH_MSGR_TEST_KEY_TYPE=aes256k", "OTHER=1"}); err != nil || p.keyType != "aes" {
		t.Fatal("the first of a repeated variable did not win:", p.keyType, err)
	}
}

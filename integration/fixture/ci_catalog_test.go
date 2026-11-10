package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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

type ciBinding struct{ Parameter, Fallback string }

// Admit only the workflow's simple matrix references. A new expression shape
// must be taught to this validator rather than silently interpreted as empty.
// A Windows checkout may deliver the workflow with CRLF line endings, so one
// trailing carriage return per line is a line terminator, not expression text.
func ciBindings(text string) (map[string]ciBinding, error) {
	linePattern := regexp.MustCompile(`^\s+(CEPH_[A-Z0-9_]+): (.+)$`)
	valuePattern := regexp.MustCompile(`^\$\{\{ matrix\.([a-z_]+)(?: \|\| '([^']*)')? \}\}$`)
	result := map[string]ciBinding{}
	for _, line := range strings.Split(text, "\n") {
		fields := linePattern.FindStringSubmatch(strings.TrimSuffix(line, "\r"))
		if fields == nil || !strings.Contains(fields[2], "matrix.") {
			continue
		}
		value := valuePattern.FindStringSubmatch(fields[2])
		if value == nil {
			return nil, fmt.Errorf("unsupported workflow expression for %s", fields[1])
		}
		if _, ok := result[fields[1]]; ok {
			return nil, fmt.Errorf("duplicate workflow binding %s", fields[1])
		}
		result[fields[1]] = ciBinding{value[1], value[2]}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no matrix environment bindings")
	}
	return result, nil
}

func ciProfile(row ciScenario, bindings map[string]ciBinding) (profile, error) {
	known := map[string]bool{"release": true}
	for _, binding := range bindings {
		known[binding.Parameter] = true
	}
	for key := range row.Params {
		if !known[key] {
			return profile{}, fmt.Errorf("unforwarded parameter %s", key)
		}
	}
	return loadProfile(func(name string) string {
		binding := bindings[name]
		if value := row.Params[binding.Parameter]; value != "" {
			return value
		}
		return binding.Fallback
	})
}

func ciNativeSources(root string) (map[string]string, error) {
	result := map[string]string{}
	native := regexp.MustCompile(`^TestCeph\w*Integration$`)
	for _, pkg := range []string{"integration", "cephmsgr"} {
		paths, err := filepath.Glob(filepath.Join(root, pkg, "*_test.go"))
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
				if !ok || fn.Recv != nil || !native.MatchString(fn.Name.Name) {
					continue
				}
				key := pkg + "/" + fn.Name.Name
				if _, exists := result[key]; exists {
					return nil, fmt.Errorf("duplicate native test %s", key)
				}
				result[key] = pkg + "/" + filepath.Base(path)
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

func TestCICatalogInventory(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(name string) []byte {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var catalog struct {
		Scenarios []ciScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(read(".github/scenarios/catalog.json"), &catalog); err != nil {
		t.Fatal(err)
	}
	bindings, err := ciBindings(string(read(".github/workflows/native-scenarios.yml")))
	if err != nil {
		t.Fatal(err)
	}
	sources, err := ciNativeSources(root)
	if err != nil {
		t.Fatal(err)
	}
	listed := strings.Fields(string(read(".github/scenarios/native-tests.txt")))
	if problems := ciInventoryProblems(sources, listed); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
	rows := make([]ciTestSelection, 0, len(listed))
	for _, name := range listed {
		rows = append(rows, ciTestSelection{Test: name, Source: sources[name], Candidates: []string{}, Required: []string{}})
	}
	for _, scenario := range catalog.Scenarios {
		p, err := ciProfile(scenario, bindings)
		if err != nil {
			t.Errorf("%s: %v", scenario.ID, err)
			continue
		}
		selector, err := regexp.Compile(p.testRun)
		if err != nil {
			t.Errorf("%s: %v", scenario.ID, err)
			continue
		}
		matches := 0
		for index := range rows {
			name := strings.SplitN(rows[index].Test, "/", 2)[1]
			if !selector.MatchString(name) {
				continue
			}
			matches++
			rows[index].Candidates = append(rows[index].Candidates, scenario.ID)
			if scenario.Params["test_run"] != "" {
				rows[index].Required = append(rows[index].Required, scenario.ID)
			}
		}
		if matches == 0 {
			t.Errorf("%s: effective fixture selector matches no native tests", scenario.ID)
		}
	}
	broad := 0
	for _, row := range rows {
		if len(row.Candidates) == 0 {
			t.Errorf("no catalog selector includes %s", row.Test)
		}
		if len(row.Required) == 0 {
			broad++
		}
	}
	t.Logf("%d registered native tests; %d have only broad candidate selection (optional skips remain possible)", len(rows), broad)
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
	slices.Sort(compiled)
	if problems := ciInventoryProblems(sources, compiled); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

func TestCIInventoryRefusesMissingStaleAndDuplicateEntries(t *testing.T) {
	declared := map[string]string{"integration/TestCephAIntegration": "a.go"}
	for _, listed := range [][]string{{}, {"integration/TestCephGoneIntegration"}, {"integration/TestCephAIntegration", "integration/TestCephAIntegration"}} {
		if len(ciInventoryProblems(declared, listed)) == 0 {
			t.Fatalf("accepted %v", listed)
		}
	}
}

func TestCIBindingsParseCRLFLikeLF(t *testing.T) {
	root := filepath.Join("..", "..")
	workflow, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "native-scenarios.yml"))
	if err != nil {
		t.Fatal(err)
	}
	synthetic := "env:\n  CEPH_MSGR_TEST_IMAGE: ${{ matrix.image }}\n  CEPH_MSGR_TEST_MDS_COMMANDS: ${{ matrix.mds_commands || '0' }}\n  CEPH_MSGR_TEST_EMPTY: ${{ matrix.empty || '' }}\n  CEPH_MSGR_TEST_FIXED: literal\n"
	want := map[string]ciBinding{
		"CEPH_MSGR_TEST_IMAGE":        {"image", ""},
		"CEPH_MSGR_TEST_MDS_COMMANDS": {"mds_commands", "0"},
		"CEPH_MSGR_TEST_EMPTY":        {"empty", ""},
	}
	// The checked-out workflow may already be CRLF; normalize it first so both
	// encodings are exercised on every platform.
	for name, text := range map[string]string{"synthetic": synthetic, "workflow": strings.ReplaceAll(string(workflow), "\r\n", "\n")} {
		lf, err := ciBindings(text)
		if err != nil {
			t.Fatalf("%s LF: %v", name, err)
		}
		crlf, err := ciBindings(strings.ReplaceAll(text, "\n", "\r\n"))
		if err != nil {
			t.Fatalf("%s CRLF: %v", name, err)
		}
		if !maps.Equal(lf, crlf) {
			t.Fatalf("%s bindings differ: LF %v CRLF %v", name, lf, crlf)
		}
		if name == "synthetic" && !maps.Equal(lf, want) {
			t.Fatalf("synthetic bindings = %v, want %v", lf, want)
		}
		fallbacks := 0
		for _, binding := range lf {
			if binding.Fallback != "" {
				fallbacks++
			}
		}
		if fallbacks == 0 {
			t.Fatalf("%s has no fallback binding to compare", name)
		}
	}
	// Line-ending tolerance must not admit other expression shapes, duplicates
	// or carriage returns that are not a line terminator.
	for _, text := range []string{
		"  CEPH_MSGR_TEST_OSD: ${{ matrix.osd && '1' }}\r\n",
		"  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}\r\n  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}\r\n",
		"  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}\r\r\n",
		"  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}\r trailing\n",
		"  CEPH_MSGR_TEST_FIXED: literal\r\n",
	} {
		if bindings, err := ciBindings(text); err == nil {
			t.Fatalf("accepted %q as %v", text, bindings)
		}
	}
}

func TestCIProfileRejectsUnsupportedBindingsAndInvalidTopology(t *testing.T) {
	for _, text := range []string{"", "  CEPH_MSGR_TEST_OSD: ${{ matrix.osd && '1' }}", "  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}\n  CEPH_MSGR_TEST_OSD: ${{ matrix.osd }}"} {
		if _, err := ciBindings(text); err == nil {
			t.Fatalf("accepted %q", text)
		}
	}
	bindings, err := ciBindings("  CEPH_MSGR_TEST_MDS_COMMANDS: ${{ matrix.mds_commands || '0' }}")
	if err != nil {
		t.Fatal(err)
	}
	for _, params := range []map[string]string{{"unknown": "1"}, {"mds_commands": "1"}} {
		if _, err := ciProfile(ciScenario{Params: params}, bindings); err == nil {
			t.Fatalf("accepted %v", params)
		}
	}
}

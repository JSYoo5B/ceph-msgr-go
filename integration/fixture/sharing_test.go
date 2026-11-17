package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// Fixture sharing (FIXTURE_SHARING.md) says whether a native test may run on
// a fixture that another test uses after it. Each native test declares it
// beside its function, as the last line of its doc comment:
//
//	//fixture:shareable <reason>
//	//fixture:isolated <reason>
//
// The reason names what the test changes and what its cleanup proves. A
// catalog row is shareable only when every test its effective selector
// selects (the fixture's own default when the row has no test_run) is
// shareable. tools/scenario_plan.py validate runs
// TestCatalogRowsDeclareFixtureSharing and refuses every line marked
// sharingMark, and reads each row's value from the lines marked
// sharingRowMark, so the planner keeps no copy of the declarations.

const (
	sharingMark    = "fixture sharing: "
	sharingRowMark = "fixture sharing row: "
	// sharingMaxReason bounds the reason so that it stays one line.
	sharingMaxReason = 120
)

var (
	sharingNativeTest = regexp.MustCompile(`^TestCeph\w*Integration$`)
	sharingDirective  = regexp.MustCompile(`^//fixture:(shareable|isolated) (\S.*)$`)
	sharingReason     = regexp.MustCompile(`^[ -~]+$`)
)

// testSharing is one native test's declaration. Value is empty when the test
// declares none (or only invalid ones).
type testSharing struct{ Value, Reason, Source string }

// nativeTestSharing reads the declaration of every native test of the
// integration and cephmsgr suites under root, keyed by "package/TestName",
// and reports each missing, repeated, invalid or misplaced declaration. The
// files are the ones the build context selects (ciSuiteTestFiles), the same
// as ciNativeSources, so a build-excluded file neither adds a native test nor
// a declaration.
func nativeTestSharing(root string) (map[string]testSharing, []string, error) {
	tests, problems := map[string]testSharing{}, []string{}
	for _, pkg := range []string{"integration", "cephmsgr"} {
		paths, err := ciSuiteTestFiles(root, pkg)
		if err != nil {
			return nil, nil, err
		}
		for _, path := range paths {
			source := pkg + "/" + filepath.Base(path)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
			if err != nil {
				return nil, nil, err
			}
			attached := map[*ast.Comment]bool{}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv != nil || !sharingNativeTest.MatchString(fn.Name.Name) {
					continue
				}
				key := pkg + "/" + fn.Name.Name
				if _, exists := tests[key]; exists {
					return nil, nil, fmt.Errorf("duplicate native test %s", key)
				}
				declared, found := testSharing{Source: source}, 0
				if fn.Doc != nil {
					for _, comment := range fn.Doc.List {
						if !strings.HasPrefix(comment.Text, "//fixture:") {
							continue
						}
						attached[comment] = true
						found++
						match := sharingDirective.FindStringSubmatch(comment.Text)
						switch {
						case match == nil:
							problems = append(problems, fmt.Sprintf("%s (%s): invalid fixture sharing declaration %q; want //fixture:shareable <reason> or //fixture:isolated <reason>", key, source, comment.Text))
						case len(match[2]) > sharingMaxReason || !sharingReason.MatchString(match[2]) || strings.HasSuffix(match[2], "."):
							problems = append(problems, fmt.Sprintf("%s (%s): fixture sharing reason must be printable ASCII of at most %d bytes without a final period: %q", key, source, sharingMaxReason, match[2]))
						default:
							declared.Value, declared.Reason = match[1], match[2]
						}
					}
				}
				switch {
				case found == 0:
					problems = append(problems, fmt.Sprintf("%s (%s) declares no fixture sharing; add //fixture:shareable <reason> or //fixture:isolated <reason> above it", key, source))
				case found > 1:
					problems = append(problems, fmt.Sprintf("%s (%s) declares fixture sharing %d times", key, source, found))
					declared.Value = ""
				}
				tests[key] = declared
			}
			for _, group := range file.Comments {
				for _, comment := range group.List {
					if strings.HasPrefix(comment.Text, "//fixture:") && !attached[comment] {
						problems = append(problems, fmt.Sprintf("%s:%d: fixture sharing declaration %q is not on a native test's doc comment", source, fset.Position(comment.Pos()).Line, comment.Text))
					}
				}
			}
		}
	}
	slices.Sort(problems)
	return tests, problems, nil
}

// rowSharing is the derived value of one catalog row: shareable when every
// selected test is, otherwise isolated by the listed tests.
type rowSharing struct {
	ID, Value string
	Selected  int
	Isolating []string
}

// catalogSharing derives every row's value from the tests its effective
// selector selects in both suites. A row that the fixture refuses, that
// selects no native test or that selects a test without a valid declaration
// has no value and is reported instead.
func catalogSharing(rows []ciScenario, bindings map[string]ciBinding, tests map[string]testSharing) ([]rowSharing, []string) {
	names := slices.Sorted(maps.Keys(tests))
	var result []rowSharing
	var problems []string
	for _, row := range rows {
		p, err := ciProfile(row, bindings)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: no fixture sharing value: the fixture refuses the row: %v", row.ID, err))
			continue
		}
		selector, err := regexp.Compile(p.testRun)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: no fixture sharing value: %v", row.ID, err))
			continue
		}
		value := rowSharing{ID: row.ID, Value: "shareable"}
		var undeclared []string
		for _, name := range names {
			if !selector.MatchString(strings.SplitN(name, "/", 2)[1]) {
				continue
			}
			value.Selected++
			switch tests[name].Value {
			case "shareable":
			case "isolated":
				value.Isolating = append(value.Isolating, name)
			default:
				undeclared = append(undeclared, name)
			}
		}
		switch {
		case value.Selected == 0:
			problems = append(problems, fmt.Sprintf("%s: no fixture sharing value: its effective selector %q selects no native test", row.ID, p.testRun))
		case len(undeclared) != 0:
			problems = append(problems, fmt.Sprintf("%s: no fixture sharing value: it selects %s, which declares no valid fixture sharing", row.ID, strings.Join(undeclared, ", ")))
		default:
			if len(value.Isolating) != 0 {
				value.Value = "isolated"
			}
			result = append(result, value)
		}
	}
	return result, problems
}

// line is the sharingRowMark line of one row: its value and, when isolated,
// how many selected tests isolate it and the first of them.
func (r rowSharing) line() string {
	if r.Value == "shareable" {
		return fmt.Sprintf("%s %s", r.ID, r.Value)
	}
	return fmt.Sprintf("%s %s by %d of %d selected tests, first %s", r.ID, r.Value, len(r.Isolating), r.Selected, r.Isolating[0])
}

// tools/scenario_plan.py validate runs this test. It reports every problem
// on a line marked sharingMark and logs each row's value on a line marked
// sharingRowMark; a row without such a line has no value and is refused.
func TestCatalogRowsDeclareFixtureSharing(t *testing.T) {
	tests, problems, err := nativeTestSharing(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	rows, rowProblems := catalogSharing(ciRepositoryCatalog(t), ciWorkflowBindings(t), tests)
	for _, problem := range append(problems, rowProblems...) {
		t.Error(sharingMark + problem)
	}
	for _, row := range rows {
		t.Log(sharingRowMark + row.line())
	}
}

// writeSharingSuite writes a native suite of the given files under a new
// root and returns the root.
func writeSharingSuite(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, pkg := range []string{"integration", "cephmsgr"} {
		if err := os.MkdirAll(filepath.Join(root, pkg), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte("package integration\n\nimport \"testing\"\n\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestNativeTestSharingRefusesMissingRepeatedInvalidAndMisplacedDeclarations(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"missing": {"// TestCephAIntegration does things.\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"integration/TestCephAIntegration (integration/a_test.go) declares no fixture sharing"},
		"repeated": {"//fixture:shareable reads only\n//fixture:isolated stops osd.0\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"declares fixture sharing 2 times"},
		"misspelled value": {"//fixture:sharable reads only\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"invalid fixture sharing declaration"},
		"empty reason": {"//fixture:isolated\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"invalid fixture sharing declaration"},
		"reason with a final period": {"//fixture:isolated stops osd.0.\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"without a final period"},
		"long reason": {"//fixture:isolated " + strings.Repeat("x", sharingMaxReason+1) + "\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"at most 120 bytes"},
		"not on the doc comment": {"//fixture:shareable reads only\n\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"integration/a_test.go:5: fixture sharing declaration \"//fixture:shareable reads only\" is not on a native test's doc comment"},
		"on a helper": {"//fixture:shareable reads only\nfunc helper() {}\n\n//fixture:shareable reads only\nfunc TestCephAIntegration(t *testing.T) {}\n",
			"is not on a native test's doc comment"},
		"inside a test": {"//fixture:shareable reads only\nfunc TestCephAIntegration(t *testing.T) {\n\t//fixture:isolated stops osd.0\n}\n",
			"is not on a native test's doc comment"},
	} {
		t.Run(name, func(t *testing.T) {
			tests, problems, err := nativeTestSharing(writeSharingSuite(t, map[string]string{"integration/a_test.go": tc.body}))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }) {
				t.Fatalf("problems %q lack %q", problems, tc.want)
			}
			if _, ok := tests["integration/TestCephAIntegration"]; !ok {
				t.Fatal("the native test is not inventoried")
			}
		})
	}
	tests, problems, err := nativeTestSharing(writeSharingSuite(t, map[string]string{
		"integration/a_test.go": "// TestCephAIntegration reads.\n//\n//fixture:shareable reads MON maps only\nfunc TestCephAIntegration(t *testing.T) {}\n\n// TestOther is cluster-free.\nfunc TestOther(t *testing.T) {}\n",
		"cephmsgr/b_test.go":    "//fixture:isolated stops mon.a and never restarts it\nfunc TestCephAIntegration(t *testing.T) {}\n",
	}))
	if err != nil || len(problems) != 0 {
		t.Fatal(err, problems)
	}
	want := map[string]testSharing{
		"integration/TestCephAIntegration": {"shareable", "reads MON maps only", "integration/a_test.go"},
		"cephmsgr/TestCephAIntegration":    {"isolated", "stops mon.a and never restarts it", "cephmsgr/b_test.go"},
	}
	if len(tests) != len(want) {
		t.Fatalf("tests %v, want %v", tests, want)
	}
	for name, declared := range want {
		if tests[name] != declared {
			t.Fatalf("%s: %+v, want %+v", name, tests[name], declared)
		}
	}
}

// TestNativeTestSharingReadsTheBuildSelectedFiles is the negative control of
// the file selection: a build-excluded file declares no native test, so its
// test needs no declaration, and the external test package is read. On the
// repository the inventory equals ciNativeSources, which
// TestCINativeCompiledInventory pairs with go test -list.
func TestNativeTestSharingReadsTheBuildSelectedFiles(t *testing.T) {
	root := writeSharingSuite(t, map[string]string{
		"integration/a_test.go": "//fixture:shareable reads only\nfunc TestCephAIntegration(t *testing.T) {}\n",
		"cephmsgr/b_test.go":    "//fixture:shareable reads only\nfunc TestCephBIntegration(t *testing.T) {}\n",
	})
	files := map[string]string{
		"ignored_test.go":  "//go:build ignore\n\npackage integration\n\nimport \"testing\"\n\nfunc TestCephIgnoredIntegration(t *testing.T) {}\n",
		"external_test.go": "package integration_test\n\nimport \"testing\"\n\n//fixture:isolated stops osd.0\nfunc TestCephExternalIntegration(t *testing.T) {}\n",
	}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(root, "integration", name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tests, problems, err := nativeTestSharing(root)
	if err != nil || len(problems) != 0 {
		t.Fatal(err, problems)
	}
	if got, want := slices.Sorted(maps.Keys(tests)), []string{"cephmsgr/TestCephBIntegration", "integration/TestCephAIntegration", "integration/TestCephExternalIntegration"}; !slices.Equal(got, want) {
		t.Fatalf("native tests %v, want %v", got, want)
	}
	repository := filepath.Join("..", "..")
	tests, _, err = nativeTestSharing(repository)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := ciNativeSources(repository)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := slices.Sorted(maps.Keys(tests)), slices.Sorted(maps.Keys(sources)); !slices.Equal(got, want) {
		t.Fatalf("sharing inventory %v differs from the native sources %v", got, want)
	}
}

func TestCatalogSharingDerivesEveryRowAndRefusesARowWithoutAValue(t *testing.T) {
	bindings := ciWorkflowBindings(t)
	tests := map[string]testSharing{
		"integration/TestCephReadIntegration":  {Value: "shareable"},
		"cephmsgr/TestCephReadIntegration":     {Value: "shareable"},
		"integration/TestCephOtherIntegration": {Value: "shareable"},
		"integration/TestCephKillIntegration":  {Value: "isolated"},
		"integration/TestCephNewIntegration":   {},
	}
	row := func(id, run string) ciScenario {
		params := map[string]string{"key": "aes256k", "service": "aes256k", "family": "4"}
		if run != "" {
			params["test_run"] = run
		}
		return ciScenario{ID: id, Params: params}
	}
	rows, problems := catalogSharing([]ciScenario{
		row("read", "^TestCephReadIntegration$"),
		row("pair", "^TestCeph(Read|Other)Integration$"),
		row("mixed", "^TestCeph(Read|Kill)Integration$"),
		row("new", "^TestCephNewIntegration$"),
		row("none", "^TestCephMissingIntegration$"),
		row("broad", ""),
	}, bindings, tests)
	got := map[string]string{}
	for _, r := range rows {
		got[r.ID] = r.line()
	}
	want := map[string]string{
		"read":  "read shareable",
		"pair":  "pair shareable",
		"mixed": "mixed isolated by 1 of 3 selected tests, first integration/TestCephKillIntegration",
	}
	if len(got) != len(want) {
		t.Fatalf("rows %v, want %v", got, want)
	}
	for id, line := range want {
		if got[id] != line {
			t.Fatalf("%s: %q, want %q", id, got[id], line)
		}
	}
	// A row selecting an undeclared test, a row selecting nothing and a broad
	// row that includes the undeclared test have no value.
	for _, id := range []string{"new", "none", "broad"} {
		if !slices.ContainsFunc(problems, func(p string) bool { return strings.HasPrefix(p, id+": no fixture sharing value: ") }) {
			t.Fatalf("%s is not refused: %q", id, problems)
		}
	}
	if !slices.ContainsFunc(problems, func(p string) bool {
		return strings.Contains(p, "it selects integration/TestCephNewIntegration, which declares no valid fixture sharing")
	}) {
		t.Fatalf("the undeclared test is not named: %q", problems)
	}
	refused := row("refused", "^TestCephReadIntegration$")
	refused.Params["osd_replicas"] = "4"
	if _, problems := catalogSharing([]ciScenario{refused}, bindings, tests); len(problems) != 1 || !strings.Contains(problems[0], "the fixture refuses the row") {
		t.Fatalf("a refused row is not reported: %q", problems)
	}
}

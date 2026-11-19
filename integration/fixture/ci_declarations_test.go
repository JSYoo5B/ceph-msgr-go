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

// The scenario catalog is the set of rows declared beside the native tests:
// one "//scenario:row <id> [name=value ...]" or "//scenario:suite <id>
// [name=value ...]" directive per row, in the doc comment of a top-level
// native test function of a suite in ciNativeSuites. tools/scenario_declarations.py
// reads the same directives as text for the trusted planner; this reading
// uses Go's parser, and tools/scenario_plan.py validate requires both to find
// the same rows (TestCatalogRowsAreAdmittedByTheFixture,
// CEPH_MSGR_PLANNED_ROWS).
//
// A row selects its owner test: its test_run is ^<owner>$, or an explicit
// test_run that also selects the owner. A suite row has no test_run. Only the
// RGW checkpoints of this module declare rgw_native=1, and they declare it on
// every row. The group is the identifier before its first dot.
const ciDeclarationPrefix = "//scenario:"

var (
	ciDirective         = regexp.MustCompile(`^//scenario:(row|suite) (\S+)((?: \S+)*)$`)
	ciScenarioID        = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,159}$`)
	ciScenarioGroup     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	ciScenarioParameter = regexp.MustCompile(`^([a-z][a-z0-9_]*)=(\S*)$`)
)

// ciDiscoverScenarios returns the rows declared in the native suites' test
// files under root, in identifier order, and every refused declaration.
func ciDiscoverScenarios(root string) ([]ciScenario, []string) {
	var rows []ciScenario
	var problems []string
	where := map[string]string{}
	for _, suite := range ciNativeSuites {
		paths, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(suite.dir), "*_test.go"))
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		slices.Sort(paths)
		for _, path := range paths {
			found, refused := ciFileDeclarations(path, suite.dir+"/"+filepath.Base(path), suite)
			problems = append(problems, refused...)
			for _, row := range found {
				if previous, ok := where[row.ID]; ok {
					problems = append(problems, fmt.Sprintf("duplicate scenario identifier %s, declared at %s and %s", row.ID, previous, row.location))
					continue
				}
				where[row.ID] = row.location
				rows = append(rows, row.ciScenario)
			}
		}
	}
	slices.SortFunc(rows, func(a, b ciScenario) int { return strings.Compare(a.ID, b.ID) })
	return rows, problems
}

type ciDeclaredRow struct {
	ciScenario
	location string
}

func ciFileDeclarations(path, name string, suite ciNativeSuite) ([]ciDeclaredRow, []string) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return nil, []string{fmt.Sprintf("%s: %v", name, err)}
	}
	owners := map[*ast.CommentGroup]string{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Doc != nil && strings.HasPrefix(fn.Name.Name, "Test") {
			owners[fn.Doc] = fn.Name.Name
		}
	}
	var rows []ciDeclaredRow
	var problems []string
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !strings.HasPrefix(comment.Text, ciDeclarationPrefix) {
				continue
			}
			position := fset.Position(comment.Slash)
			location := fmt.Sprintf("%s:%d", name, position.Line)
			row, problem := ciDeclaration(comment.Text, position.Column, owners[group], suite)
			if problem != "" {
				problems = append(problems, location+": "+problem)
				continue
			}
			rows = append(rows, ciDeclaredRow{row, location})
		}
	}
	return rows, problems
}

func ciDeclaration(text string, column int, owner string, suite ciNativeSuite) (ciScenario, string) {
	if column != 1 {
		return ciScenario{}, "an indented scenario declaration"
	}
	match := ciDirective.FindStringSubmatch(text)
	if match == nil {
		return ciScenario{}, "a malformed scenario declaration (//scenario:row|suite <id> [name=value ...])"
	}
	kind, id := match[1], match[2]
	switch {
	case owner == "":
		return ciScenario{}, id + " is not in the doc comment of a top-level test function"
	case !suite.native.MatchString(owner):
		return ciScenario{}, fmt.Sprintf("%s is declared beside %s, which is not a native test of %s", id, owner, suite.dir)
	case !ciScenarioID.MatchString(id) || id == "all":
		return ciScenario{}, fmt.Sprintf("invalid scenario identifier %q", id)
	}
	if group, _, _ := strings.Cut(id, "."); !ciScenarioGroup.MatchString(group) {
		return ciScenario{}, id + " has no group before its first dot"
	}
	params := map[string]string{}
	for _, word := range strings.Fields(match[3]) {
		parameter := ciScenarioParameter.FindStringSubmatch(word)
		if parameter == nil {
			return ciScenario{}, fmt.Sprintf("%s: malformed parameter %q (name=value)", id, word)
		}
		if _, ok := params[parameter[1]]; ok {
			return ciScenario{}, fmt.Sprintf("%s: parameter %s is given twice", id, parameter[1])
		}
		params[parameter[1]] = parameter[2]
	}
	exact := "^" + owner + "$"
	selector, explicit := params["test_run"]
	switch {
	case kind == "suite" && explicit:
		return ciScenario{}, id + ": a suite row runs its whole fixture configuration and has no test_run"
	case kind == "suite" && suite.routed:
		return ciScenario{}, id + ": a routed RGW checkpoint is a row of its own test, not a suite row"
	case kind == "row" && explicit && selector == exact:
		return ciScenario{}, fmt.Sprintf("%s: test_run %s is the owner's own selector; leave it out", id, selector)
	case kind == "row" && explicit:
		pattern, err := regexp.Compile(selector)
		if err != nil {
			return ciScenario{}, fmt.Sprintf("%s: invalid test_run %q: %v", id, selector, err)
		}
		if !pattern.MatchString(owner) {
			return ciScenario{}, fmt.Sprintf("%s: test_run %s does not select %s, beside which it is declared", id, selector, owner)
		}
	case kind == "row":
		params["test_run"] = exact
	}
	if suite.routed != (params["rgw_native"] == "1") {
		return ciScenario{}, id + ": rgw_native=1 is declared exactly on the RGW checkpoints of integration/fixture"
	}
	return ciScenario{ID: id, Params: params}, ""
}

// ciRepositoryCatalog returns the rows declared in the repository.
func ciRepositoryCatalog(t *testing.T) []ciScenario {
	t.Helper()
	rows, problems := ciDiscoverScenarios(filepath.Join("..", ".."))
	if len(problems) != 0 {
		t.Fatal("scenario declarations:\n" + strings.Join(problems, "\n"))
	}
	if len(rows) == 0 {
		t.Fatal("no scenario is declared")
	}
	return rows
}

func ciDeclarationWrite(t *testing.T, root, name, text string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

// ciDeclaringSource is a test file whose one test carries the given doc lines.
func ciDeclaringSource(test string, doc ...string) string {
	return "package p\n\nimport \"testing\"\n\n" + strings.Join(doc, "\n") + "\n//fixture:shareable reason\nfunc " + test + "(t *testing.T) {}\n"
}

func TestScenarioDeclarationsAreReadBesideTheirTests(t *testing.T) {
	root := t.TempDir()
	ciDeclarationWrite(t, root, "integration/b_test.go", ciDeclaringSource("TestCephXIntegration",
		"// TestCephXIntegration checks x.", "//",
		"//scenario:row osd.b key=aes capabilities=c-cap stress=",
		"//scenario:suite osd.a key=aes osd=1"))
	ciDeclarationWrite(t, root, "cephmsgr/a_test.go", ciDeclaringSource("TestCephYIntegration",
		"//scenario:row mon-mgr.c test_run=^TestCeph(X|Y)Integration$ key=aes"))
	ciDeclarationWrite(t, root, "integration/fixture/r_test.go", ciDeclaringSource("TestRGWNativeRIntegration",
		"//scenario:row rgw.d key=aes256k rgw_native=1"))
	ciDeclarationWrite(t, root, "integration/fixture/helper_test.go", "package p\n\n// TestHelper has no declaration.\nfunc TestHelper(t *testing.T) {}\n")
	rows, problems := ciDiscoverScenarios(root)
	if len(problems) != 0 {
		t.Fatal(problems)
	}
	want := []ciScenario{
		{"mon-mgr.c", map[string]string{"key": "aes", "test_run": "^TestCeph(X|Y)Integration$"}},
		{"osd.a", map[string]string{"key": "aes", "osd": "1"}},
		{"osd.b", map[string]string{"key": "aes", "capabilities": "c-cap", "stress": "", "test_run": "^TestCephXIntegration$"}},
		{"rgw.d", map[string]string{"key": "aes256k", "rgw_native": "1", "test_run": "^TestRGWNativeRIntegration$"}},
	}
	if fmt.Sprint(rows) != fmt.Sprint(want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestScenarioDeclarationsRefuseMisplacedAndMalformedRows(t *testing.T) {
	row := "//scenario:row osd.a key=aes"
	for _, tc := range []struct {
		name, file, text, want string
	}{
		{"not a native test", "integration/x_test.go", ciDeclaringSource("TestHelper", row), "which is not a native test of integration"},
		{"detached", "integration/x_test.go", "package p\n\n" + row + "\n\nfunc TestCephXIntegration(t *testing.T) {}\n", "not in the doc comment"},
		{"method", "integration/x_test.go", "package p\n\n" + row + "\nfunc (s suite) TestCephXIntegration(t *testing.T) {}\n", "not in the doc comment"},
		{"indented", "integration/x_test.go", "package p\n\nfunc TestCephXIntegration(t *testing.T) {\n\t" + row + "\n}\n", "an indented scenario declaration"},
		{"unknown kind", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:test osd.a key=aes"), "malformed scenario declaration"},
		{"no identifier", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row"), "malformed scenario declaration"},
		{"malformed parameter", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row osd.a key"), "malformed parameter"},
		{"twice", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row osd.a key=aes key=aes"), "given twice"},
		{"identifier", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row Osd.A key=aes"), "invalid scenario identifier"},
		{"all", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row all key=aes"), "invalid scenario identifier"},
		{"group", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row os_d.a key=aes"), "has no group"},
		{"suite selector", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:suite osd.a test_run=^TestCephXIntegration$"), "has no test_run"},
		{"own selector", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row osd.a test_run=^TestCephXIntegration$"), "the owner's own selector"},
		{"other test", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row osd.a test_run=^TestCephYIntegration$"), "does not select TestCephXIntegration"},
		{"invalid selector", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row osd.a test_run=^(TestCeph"), "invalid test_run"},
		{"routed outside the fixture", "integration/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row rgw.a key=aes rgw_native=1"), "rgw_native=1 is declared exactly"},
		{"unrouted checkpoint", "integration/fixture/x_test.go", ciDeclaringSource("TestRGWNativeXIntegration", "//scenario:row rgw.a key=aes"), "rgw_native=1 is declared exactly"},
		{"routed suite", "integration/fixture/x_test.go", ciDeclaringSource("TestRGWNativeXIntegration", "//scenario:suite rgw.a rgw_native=1"), "not a suite row"},
		{"fixture test", "integration/fixture/x_test.go", ciDeclaringSource("TestCephXIntegration", "//scenario:row rgw.a rgw_native=1"), "not a native test of integration/fixture"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			ciDeclarationWrite(t, root, tc.file, tc.text)
			rows, problems := ciDiscoverScenarios(root)
			if len(rows) != 0 || len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
				t.Fatalf("rows = %v, problems = %q, want one containing %q", rows, problems, tc.want)
			}
		})
	}
	root := t.TempDir()
	ciDeclarationWrite(t, root, "integration/a_test.go", ciDeclaringSource("TestCephXIntegration", row))
	ciDeclarationWrite(t, root, "cephmsgr/b_test.go", ciDeclaringSource("TestCephYIntegration", row))
	if _, problems := ciDiscoverScenarios(root); len(problems) != 1 || !strings.Contains(problems[0], "duplicate scenario identifier osd.a") {
		t.Fatalf("duplicate problems = %q", problems)
	}
}

// ciReadingProblems compares the planner's rows with the fixture's reading
// of the same declarations: the same identifiers, in the same order, with
// the same parameters.
func ciReadingProblems(planned, read []ciScenario) []string {
	var problems []string
	byID := map[string]ciScenario{}
	for _, row := range read {
		byID[row.ID] = row
	}
	seen := map[string]bool{}
	for _, row := range planned {
		seen[row.ID] = true
		mine, ok := byID[row.ID]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s: the planner reads a declaration the fixture does not", row.ID))
		case !maps.Equal(mine.Params, row.Params):
			problems = append(problems, fmt.Sprintf("%s: the planner reads parameters %v, the fixture %v", row.ID, row.Params, mine.Params))
		}
	}
	for _, row := range read {
		if !seen[row.ID] {
			problems = append(problems, fmt.Sprintf("%s: the fixture reads a declaration the planner does not", row.ID))
		}
	}
	if len(problems) == 0 && !slices.EqualFunc(planned, read, func(a, b ciScenario) bool { return a.ID == b.ID }) {
		problems = append(problems, "the planner and the fixture order the declared rows differently")
	}
	return problems
}

func TestPlannerAndFixtureReadingsMustAgree(t *testing.T) {
	a := ciScenario{"osd.a", map[string]string{"key": "aes"}}
	b := ciScenario{"osd.b", map[string]string{"key": "aes"}}
	if problems := ciReadingProblems([]ciScenario{a, b}, []ciScenario{a, b}); len(problems) != 0 {
		t.Fatal(problems)
	}
	for _, tc := range []struct {
		planned, read []ciScenario
		want          string
	}{
		{[]ciScenario{a}, []ciScenario{a, b}, "osd.b: the fixture reads a declaration the planner does not"},
		{[]ciScenario{a, b}, []ciScenario{a}, "osd.b: the planner reads a declaration the fixture does not"},
		{[]ciScenario{a, {"osd.b", map[string]string{"key": "aes256k"}}}, []ciScenario{a, b}, "osd.b: the planner reads parameters"},
		{[]ciScenario{b, a}, []ciScenario{a, b}, "order the declared rows differently"},
	} {
		if problems := ciReadingProblems(tc.planned, tc.read); len(problems) != 1 || !strings.Contains(problems[0], tc.want) {
			t.Errorf("problems = %q, want %q", problems, tc.want)
		}
	}
}

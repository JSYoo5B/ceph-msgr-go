package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// catalogCapabilities returns the capabilities that the repository catalog
// declares for the rows that select exactly this test_run, so that a test
// exercises the declaration CI forwards. Rows of one selector must agree.
func catalogCapabilities(t *testing.T, selector string) string {
	t.Helper()
	found, value := false, ""
	for _, row := range ciRepositoryCatalog(t) {
		if row.Params["test_run"] != selector {
			continue
		}
		if found && row.Params["capabilities"] != value {
			t.Fatalf("rows of %s declare different capabilities", selector)
		}
		found, value = true, row.Params["capabilities"]
	}
	return value
}

// capabilityDefinitionPattern is the planner's CAPABILITY_DEFINITION
// (tools/scenario_plan.py): the name, the summary and the requirements of one
// defineCapability call, each a plain string literal.
var capabilityDefinitionPattern = regexp.MustCompile(`\bdefineCapability\(\s*"([^"\\]*)",\s*"(?:[^"\\]|\\.)*",\s*"([^"\\]*)"`)

// ciWorkflowBindings are the trusted native workflow's matrix bindings.
func ciWorkflowBindings(t *testing.T) map[string]ciBinding {
	t.Helper()
	workflow, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "native-scenarios.yml"))
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := ciBindings(string(workflow))
	if err != nil {
		t.Fatal(err)
	}
	if bindings["CEPH_MSGR_TEST_CAPABILITIES"] != (ciBinding{Parameter: "capabilities"}) {
		t.Fatal("the native workflow does not forward the capabilities parameter")
	}
	return bindings
}

// The planner reads the vocabulary and the requirements from the
// defineCapability calls of the non-test source with a regular expression.
// Every call must therefore have its name, summary and requirements as plain
// string literals, and the planner's view, the syntax tree and the registry
// the fixture admits must be equal. Every requirement names a variable the
// native workflow forwards and the profile reports, or a defined capability.
func TestFixtureCapabilityVocabularyIsTheSourceDeclarations(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var parsed, scanned []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if name, ok := call.Fun.(*ast.Ident); !ok || name.Name != "defineCapability" {
				return true
			}
			var values []string
			for _, arg := range call.Args[:3] {
				literal, ok := arg.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING || !strings.HasPrefix(literal.Value, `"`) {
					t.Fatalf("%s: defineCapability needs its name, summary and requirements as string literals", path)
				}
				value, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, value)
			}
			if strings.ContainsAny(values[0]+values[2], `"\`) {
				t.Fatalf("%s: capability %q has quotes or escapes in its name or requirements", path, values[0])
			}
			parsed = append(parsed, values[0]+" "+values[2])
			return true
		})
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range capabilityDefinitionPattern.FindAllStringSubmatch(string(source), -1) {
			scanned = append(scanned, match[1]+" "+match[2])
		}
	}
	var registered []string
	for _, name := range slices.Sorted(maps.Keys(fixtureCapabilities)) {
		registered = append(registered, name+" "+fixtureCapabilities[name].requires)
	}
	slices.Sort(parsed)
	slices.Sort(scanned)
	if !slices.Equal(parsed, scanned) || !slices.Equal(parsed, registered) || len(registered) == 0 {
		t.Fatalf("capability vocabulary differs: syntax %v, planner view %v, registry %v", parsed, scanned, registered)
	}
	bindings := ciWorkflowBindings(t)
	reported := profile{}.containerEnvironment()
	for name, c := range fixtureCapabilities {
		if !capabilityNamePattern.MatchString(name) || c.summary == "" {
			t.Fatalf("invalid capability definition %q", name)
		}
		for _, term := range strings.Fields(c.requires) {
			switch term[0] {
			case '+', '!':
				if fixtureCapabilities[term[1:]] == nil || term[1:] == name {
					t.Fatalf("%s requires undefined capability %s", name, term)
				}
			default:
				variable, _, _ := strings.Cut(term, "=")
				_, forwarded := bindings["CEPH_MSGR_TEST_"+variable]
				if _, ok := reported["CEPH_MSGR_TEST_"+variable]; !ok || !forwarded {
					t.Fatalf("%s requires %s, which the workflow does not forward or the profile does not report", name, term)
				}
			}
		}
	}
	// Every defined capability is declared by some catalog row, and every
	// declared one is defined: an unused mode would be admitted by nothing.
	declared := map[string]bool{}
	for _, row := range ciRepositoryCatalog(t) {
		if value := row.Params["capabilities"]; value != "" {
			for _, name := range strings.Split(value, ",") {
				declared[name] = true
			}
		}
	}
	if got, defined := slices.Sorted(maps.Keys(declared)), slices.Sorted(maps.Keys(fixtureCapabilities)); !slices.Equal(got, defined) {
		t.Fatalf("declared capabilities %v, defined %v", got, defined)
	}
}

func TestExactTestSelectorIsOneAnchoredTest(t *testing.T) {
	for _, selector := range []string{"^TestCephOSDUpmapIntegration$", "^TestX$", "^TestCeph_A1$"} {
		if !exactTestSelector.MatchString(selector) {
			t.Fatal("exact selector refused", selector)
		}
	}
	for _, selector := range []string{
		"", "TestCephOSDUpmapIntegration", "^TestCephOSDUpmapIntegration", "TestCephOSDUpmapIntegration$",
		"^TestCephOSD.*Integration$", "^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$",
		"^TestCephOSDErasureOptimizedK2M2(Failover)?Integration$", "^TestCephA$|^TestCephB$", "^TestCephA$/sub",
		"^Test$", "^ExampleA$", "^TestCephA$\n", " ^TestCephA$",
	} {
		if exactTestSelector.MatchString(selector) {
			t.Fatal("selector of more or other than one test accepted", selector)
		}
	}
}

// The four refusals of the acceptance, by the fixture: a capability with a
// selector of more than one test, a mode whose capability is not declared,
// a capability the fixture does not define, and a declared capability on a
// topology that does not support it. The planner's refusals are in
// tools/test_scenario_plan.py.
func TestFixtureRefusesCapabilityOutsideItsDeclaration(t *testing.T) {
	const upmap = "^TestCephOSDUpmapIntegration$"
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3",
		"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8", "CEPH_MSGR_TEST_RUN": upmap, "CEPH_MSGR_TEST_CAPABILITIES": "osd-long-tickets"}
	load := func(change map[string]string) (profile, error) {
		env := copyProfileEnv(base)
		for key, value := range change {
			env[key] = value
		}
		return loadProfile(func(name string) string { return env[name] })
	}
	if p, err := load(nil); err != nil || !p.admits(capOSDLongTickets) || p.capabilities != "osd-long-tickets" {
		t.Fatal("declared capability on its topology refused", p, err)
	}
	for name, change := range map[string]map[string]string{
		"multi-test selector":    {"CEPH_MSGR_TEST_RUN": "^TestCephOSD(Upmap|ObjectBackoffFailover)Integration$"},
		"broad selector":         {"CEPH_MSGR_TEST_RUN": "^TestCephOSD.*Integration$"},
		"default selection":      {"CEPH_MSGR_TEST_RUN": ""},
		"unanchored selector":    {"CEPH_MSGR_TEST_RUN": "TestCephOSDUpmapIntegration"},
		"unknown capability":     {"CEPH_MSGR_TEST_CAPABILITIES": "osd-upmap"},
		"unknown and known":      {"CEPH_MSGR_TEST_CAPABILITIES": "osd-long-tickets,osd-upmap"},
		"repeated capability":    {"CEPH_MSGR_TEST_CAPABILITIES": "osd-long-tickets,osd-long-tickets"},
		"empty list item":        {"CEPH_MSGR_TEST_CAPABILITIES": "osd-long-tickets,"},
		"spaced list":            {"CEPH_MSGR_TEST_CAPABILITIES": " osd-long-tickets"},
		"one-OSD topology":       {"CEPH_MSGR_TEST_OSD_REPLICAS": "1"},
		"legacy profile":         {"CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"},
		"short tickets":          {"CEPH_MSGR_TEST_SHORT_TICKETS": "1"},
		"other fixture's mode":   {"CEPH_MSGR_TEST_CAPABILITIES": "mon-forced-kill"},
		"one more unsupported":   {"CEPH_MSGR_TEST_CAPABILITIES": "mon-forced-kill,osd-long-tickets"},
		"capability without OSD": {"CEPH_MSGR_TEST_OSD": "0", "CEPH_MSGR_TEST_OSD_STORE": "memstore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1"},
	} {
		if p, err := load(change); err == nil || p != (profile{}) {
			t.Fatalf("%s: profile accepted: %+v", name, p)
		}
	}
	// A mode whose capability is not declared is not admitted, even for the
	// one test that owns it.
	p, err := load(map[string]string{"CEPH_MSGR_TEST_CAPABILITIES": ""})
	if err != nil || p.admits(capOSDLongTickets) {
		t.Fatal("undeclared capability admitted", p, err)
	}
	if auth, service := fixtureTicketTTLs(p); auth != 12e9 || service != 12e9 {
		t.Fatal("undeclared capability changed the ticket validity", auth, service)
	}
	// A profile built without loadProfile still needs the exact selector.
	for _, selector := range []string{"", "^TestCephOSD.*Integration$", upmap + "|^TestCephOSDReadIntegration$"} {
		direct := profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore", testRun: selector, capabilities: "osd-long-tickets"}
		if direct.declares(capOSDLongTickets) || direct.admits(capOSDLongTickets) {
			t.Fatal("capability admitted without an exact selector", selector)
		}
	}
}

// Every catalog row's declaration loads, and every capability it declares is
// admitted on the row's own topology.
func TestCatalogCapabilitiesAreAdmittedOnTheirRows(t *testing.T) {
	bindings := ciWorkflowBindings(t)
	rows := 0
	for _, row := range ciRepositoryCatalog(t) {
		value := row.Params["capabilities"]
		if value == "" {
			continue
		}
		rows++
		p, err := ciProfile(row, bindings)
		if err != nil {
			t.Fatalf("%s: %v", row.ID, err)
		}
		if p.capabilities != value || p.testRun != row.Params["test_run"] {
			t.Fatalf("%s: loaded capabilities %q for %q", row.ID, p.capabilities, p.testRun)
		}
		for _, name := range strings.Split(value, ",") {
			if !p.admits(fixtureCapabilities[name]) {
				t.Fatalf("%s: capability %s not admitted", row.ID, name)
			}
		}
	}
	if rows == 0 {
		t.Fatal("no catalog row declares a capability")
	}
}

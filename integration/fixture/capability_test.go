package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
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

// Every requirement names a variable the native workflow forwards and the
// profile reports, or a defined capability, and every defined capability is
// declared by some catalog row.
func TestFixtureCapabilityDefinitions(t *testing.T) {
	if len(fixtureCapabilities) == 0 {
		t.Fatal("the fixture defines no capability")
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

// catalogAdmissionProblems is the fixture's admission of catalog rows as the
// trusted native workflow forwards them: loadProfile accepts the row (every
// topology, selector and capability rule of the fixture), and a declared
// capability list is loaded unchanged for the row's own test_run, each
// capability admitted on the row's topology. A capability's requirement terms
// are only part of what loadProfile checks, so no reading of those terms can
// stand in for this.
func catalogAdmissionProblems(rows []ciScenario, bindings map[string]ciBinding) []string {
	var problems []string
	for _, row := range rows {
		p, err := ciProfile(row, bindings)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: the fixture refuses the row: %v", row.ID, err))
			continue
		}
		value := row.Params["capabilities"]
		if value == "" {
			continue
		}
		if p.capabilities != value || p.testRun != row.Params["test_run"] {
			problems = append(problems, fmt.Sprintf("%s: the fixture loaded capabilities %q for %q", row.ID, p.capabilities, p.testRun))
			continue
		}
		for _, name := range strings.Split(value, ",") {
			if !p.admits(fixtureCapabilities[name]) {
				problems = append(problems, fmt.Sprintf("%s: the fixture does not admit capability %s", row.ID, name))
			}
		}
	}
	return problems
}

// tools/scenario_plan.py validate runs this test and reports each line marked
// "catalog admission: " as a catalog problem, so a row the fixture refuses
// fails validation with the fixture's own error.
func TestCatalogRowsAreAdmittedByTheFixture(t *testing.T) {
	rows, declarations := ciDiscoverScenarios(filepath.Join("..", ".."))
	for _, problem := range declarations {
		t.Error("catalog admission: " + problem)
	}
	// validate passes the rows its planner read from the same declarations
	// (CEPH_MSGR_PLANNED_ROWS, a JSON list of {id, params}), so the
	// planner's text reading and this parser's reading cannot drift apart.
	if path := os.Getenv("CEPH_MSGR_PLANNED_ROWS"); path != "" {
		data, err := os.ReadFile(path)
		var planned []ciScenario
		if err == nil {
			err = json.Unmarshal(data, &planned)
		}
		if err != nil {
			t.Fatal("catalog admission: planned rows:", err)
		}
		for _, problem := range ciReadingProblems(planned, rows) {
			t.Error("catalog admission: " + problem)
		}
	}
	for _, problem := range catalogAdmissionProblems(rows, ciWorkflowBindings(t)) {
		t.Error("catalog admission: " + problem)
	}
	if !slices.ContainsFunc(rows, func(row ciScenario) bool { return row.Params["capabilities"] != "" }) {
		t.Error("catalog admission: no catalog row declares a capability")
	}
	// validate also passes the fixture sequences its planner makes of the
	// default coverage (CEPH_MSGR_PLANNED_JOBS, a JSON list of matrix
	// entries), so a planned job the fixture would refuse fails validation.
	if path := os.Getenv("CEPH_MSGR_PLANNED_JOBS"); path != "" {
		data, err := os.ReadFile(path)
		var jobs []map[string]string
		if err == nil {
			err = json.Unmarshal(data, &jobs)
		}
		if err != nil {
			t.Fatal("catalog admission: planned jobs:", err)
		}
		for _, problem := range plannedJobAdmissionProblems(jobs, ciWorkflowBindings(t)) {
			t.Error("catalog admission: " + problem)
		}
	}
}

// plannedJobAdmissionProblems passes each planned fixture sequence job, as the
// native workflow forwards it, through loadProfile.
func plannedJobAdmissionProblems(jobs []map[string]string, bindings map[string]ciBinding) []string {
	var problems []string
	for _, job := range jobs {
		params := maps.Clone(job)
		for _, planner := range []string{"scenario", "scenarios", "sequence_rows"} {
			delete(params, planner)
		}
		p, err := ciProfile(ciScenario{ID: job["scenario"], Params: params}, bindings)
		if err != nil {
			problems = append(problems, fmt.Sprintf("planned sequence %s: the fixture refuses it: %v", job["scenarios"], err))
		} else if job["test_sequence"] != "" && p.sequence != job["test_sequence"] {
			problems = append(problems, fmt.Sprintf("planned sequence %s: the fixture loaded no sequence", job["scenarios"]))
		}
	}
	return problems
}

// A declared capability whose own requirement terms hold can still sit on a
// topology the fixture refuses. The four-OSD erasure-capacity topology admits
// only BlueStore with the legacy-v6 or replicated-pg-v8 profile and no other
// fixture mode, which osd-erasure-width's terms (OSD=1 OSD_REPLICAS=4) do not
// repeat. The admission is the fixture's, so each such row is refused, while
// the row as the catalog declares it is admitted.
func TestCatalogAdmissionRefusesCapabilityOnRefusedTopology(t *testing.T) {
	bindings := ciWorkflowBindings(t)
	const failover = "^TestCephOSDErasureOptimizedK2M2FailoverIntegration$"
	var row ciScenario
	for _, candidate := range ciRepositoryCatalog(t) {
		if candidate.Params["test_run"] == failover {
			row = candidate
		}
	}
	if row.ID == "" || row.Params["capabilities"] != capOSDErasureWidth.name {
		t.Fatalf("no catalog row declares %s for %s", capOSDErasureWidth.name, failover)
	}
	if problems := catalogAdmissionProblems([]ciScenario{row}, bindings); len(problems) != 0 {
		t.Fatal("the catalog's own row is refused:", problems)
	}
	for name, change := range map[string]map[string]string{
		"reply-v8 profile":   {"osd_profile": "replicated-pg-reply-v8"},
		"request-v9 profile": {"osd_profile": "replicated-pg-request-v9-reply-v8"},
		"MemStore":           {"osd_store": "memstore"},
		"workflow store":     {"osd_store": ""},
		"cache pools":        {"osd_cache_pools": "1"},
		"MDS fixture":        {"mds_fresh": "1"},
		"RBD fixture":        {"rbd": "1"},
		"OSD soak":           {"osd_soak": "1m"},
		"no MGR":             {"managers": "0"},
		"three OSDs":         {"osd_replicas": "3"},
	} {
		mutated := ciScenario{ID: row.ID, Params: maps.Clone(row.Params)}
		maps.Copy(mutated.Params, change)
		problems := catalogAdmissionProblems([]ciScenario{mutated}, bindings)
		if len(problems) == 0 || !strings.HasPrefix(problems[0], row.ID+": ") {
			t.Fatalf("%s: row admitted: %v", name, problems)
		}
	}
}

// A fixture with its own default selection admits one further exact test by
// any capability defined for it with defineExclusiveCapability, and by no
// other: a capability of another fixture is refused, and so is a second
// exclusive capability of the same fixture. A capability defined beside a new
// mode is admitted without an edit to loadProfile or to any other
// capability's requirements.
func TestExclusiveCapabilityAdmitsOneTestOfItsOwnFixture(t *testing.T) {
	bindings := ciWorkflowBindings(t)
	byFixture := map[ownFixture][]string{}
	for name, c := range fixtureCapabilities {
		if c.exclusive != "" {
			byFixture[c.exclusive] = append(byFixture[c.exclusive], name)
		}
	}
	for _, f := range []ownFixture{fixtureMDSCommands, fixtureMDSCephFS, fixtureMDSCephFSCompletion, fixtureMDSCephFSData,
		fixtureRBD, fixtureOSDCachePools, fixtureOSDErasureCapacity} {
		if len(byFixture[f]) == 0 {
			t.Fatalf("no capability admits an exclusive test of the %s fixture", f)
		}
	}
	// declaresExclusive is the one exclusive capability a row declares.
	declaresExclusive := func(row ciScenario) *fixtureCapability {
		for _, name := range strings.Split(row.Params["capabilities"], ",") {
			if c := fixtureCapabilities[name]; c != nil && c.exclusive != "" {
				return c
			}
		}
		return nil
	}
	replace := func(row ciScenario, from, to string) ciScenario {
		names := strings.Split(row.Params["capabilities"], ",")
		names[slices.Index(names, from)] = to
		slices.Sort(names)
		params := maps.Clone(row.Params)
		params["capabilities"] = strings.Join(slices.Compact(names), ",")
		return ciScenario{ID: row.ID, Params: params}
	}
	checked, admittedNew, bothRefused := map[ownFixture]bool{}, map[ownFixture]bool{}, map[ownFixture]bool{}
	for _, row := range ciRepositoryCatalog(t) {
		own := declaresExclusive(row)
		if own == nil {
			continue
		}
		checked[own.exclusive] = true
		for f, names := range byFixture {
			for _, name := range names {
				if name == own.name {
					continue
				}
				problems := catalogAdmissionProblems([]ciScenario{replace(row, own.name, name)}, bindings)
				if f != own.exclusive && len(problems) == 0 {
					t.Fatalf("%s: capability %s of the %s fixture admitted on the %s fixture", row.ID, name, f, own.exclusive)
				}
				if f == own.exclusive {
					// Both declared: refused whatever their own terms say.
					both := maps.Clone(row.Params)
					both["capabilities"] = strings.Join(slices.Sorted(slices.Values(append(strings.Split(row.Params["capabilities"], ","), name))), ",")
					problems = catalogAdmissionProblems([]ciScenario{{ID: row.ID, Params: both}}, bindings)
					if len(problems) == 0 {
						t.Fatalf("%s: two exclusive capabilities %s and %s of the %s fixture admitted", row.ID, own.name, name, f)
					}
					if strings.Contains(problems[0], "each admit the one exclusive test of the "+string(f)+" fixture") {
						bothRefused[f] = true
					}
				}
			}
		}
		// An exclusive capability of another fixture whose own terms this row
		// meets is refused all the same: the fixture asks for its own.
		for f := range byFixture {
			if f == own.exclusive {
				continue
			}
			const name = "merge-proof-other-exclusive"
			defineExclusiveCapability(f, name, "a capability of another fixture defined by this test", "OSD=1", nil)
			problems := catalogAdmissionProblems([]ciScenario{replace(row, own.name, name)}, bindings)
			delete(fixtureCapabilities, name)
			if len(problems) == 0 {
				t.Fatalf("%s: an exclusive capability of the %s fixture admitted on the %s fixture", row.ID, f, own.exclusive)
			}
		}
		// A new capability for this fixture, defined beside a new mode, on a
		// row that declares nothing else. A row whose topology belongs to its
		// own capability (the three-OSD RBD primary change) refuses it.
		if row.Params["capabilities"] == own.name {
			const name = "merge-proof-exclusive"
			defineExclusiveCapability(own.exclusive, name, "a capability defined by this test", "OSD=1", nil)
			if len(catalogAdmissionProblems([]ciScenario{replace(row, own.name, name)}, bindings)) == 0 {
				admittedNew[own.exclusive] = true
			}
			delete(fixtureCapabilities, name)
		}
	}
	for f := range byFixture {
		if !admittedNew[f] {
			t.Fatalf("a new exclusive capability of the %s fixture is refused on every row of that fixture", f)
		}
		// Where a fixture has two exclusive capabilities, declaring both is
		// refused by the one-exclusive rule, not only by a topology.
		if len(byFixture[f]) > 1 && !bothRefused[f] {
			t.Fatalf("no row of the %s fixture refuses two of its exclusive capabilities %v by the one-exclusive rule", f, byFixture[f])
		}
	}
	if len(checked) != len(byFixture) {
		t.Fatalf("catalog rows declare exclusive capabilities of %d of %d fixtures", len(checked), len(byFixture))
	}
}

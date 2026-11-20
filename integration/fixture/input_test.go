package main

import (
	"encoding/json"
	"maps"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// pinnedProfileFields is the frozen field set of profile. No new input becomes
// a field: an input is declared with defineInput beside the code that reads
// it, so this literal never gains a name.
var pinnedProfileFields = []string{
	"monAddresses", "fsid", "peers", "mdsFreshIdentity", "mdsCommandsIdentity", "mdsSessionsForeignIdentity",
	"mdsCephFSIdentity", "mdsCephFSRecoveryIdentity", "mdsCephFSRanksIdentity", "mdsCephFSCompletionIdentity",
	"mdsCephFSCompletion", "mdsCephFSDataIdentity", "mdsCephFSData", "mdsFresh", "mdsCommands", "mdsSessions",
	"mdsCephFS", "mdsCephFSRanks", "mdsCephFSRanksReplay", "rbdBase", "rbdBaseIdentity", "image", "keyType",
	"serviceCipher", "ipFamily", "mgrCount", "runtime", "race", "expireTickets", "idleSessions", "authEpoch",
	"shortTickets", "authEpochCycles", "sparseReadOracle", "mapExtOracle", "u64XattrOracle", "writeSameOracle",
	"osdProfile", "nativeMgrPreparation", "monPing", "osdCachePools", "mgrService", "modeRejection",
	"connectionMode", "mappedIPv6", "osd", "osdStore", "osdReplicas", "stressDuration", "testRun", "testTimeout",
	"diagnostics", "capabilities", "defaultRun", "osdSoak", "sourceSHA", "sequence", "rgwNative", "monFreeze",
	"inputs",
}

// withInput declares a fixture input for the duration of one test.
func withInput(t *testing.T, name, fallback string) *fixtureInput {
	t.Helper()
	in := defineInput(name, fallback)
	t.Cleanup(func() { delete(fixtureInputs, in.variable) })
	return in
}

func panics(f func()) (recovered any) {
	defer func() { recovered = recover() }()
	f()
	return nil
}

// The profile's fields are frozen, and a declaration may not name the
// variable of one of them (nor the sequence variable loadProfile reads).
func TestProfileFieldsArePinned(t *testing.T) {
	ty := reflect.TypeOf(profile{})
	var fields []string
	for i := range ty.NumField() {
		fields = append(fields, ty.Field(i).Name)
	}
	if !slices.Equal(fields, pinnedProfileFields) {
		t.Fatalf("profile fields %v, pinned %v: declare a new input with defineInput", fields, pinnedProfileFields)
	}
	pinned := pinnedInputVariables()
	for _, variable := range []string{"CEPH_MSGR_TEST_KEY_TYPE", "CEPH_MSGR_TEST_RUN", "CEPH_MSGR_TEST_TIMEOUT", sequenceEnv, rgwNativeEnv} {
		if !slices.Contains(pinned, variable) {
			t.Errorf("pinned variables %v lack %s", pinned, variable)
		}
	}
	for _, name := range []string{"key_type", "run", "osd", "sequence", "rgw_native", "diagnostics"} {
		if recovered := panics(func() { defineInput(name, "") }); recovered == nil {
			delete(fixtureInputs, profileInputPrefix+strings.ToUpper(name))
			t.Errorf("an input of the pinned variable of %s was declared", name)
		}
	}
}

// A declaration is admitted once; a second declaration of one name and an
// invalid name are refused.
func TestDuplicateAndInvalidInputDeclarationsAreRefused(t *testing.T) {
	withInput(t, "declared_probe", "0")
	if recovered := panics(func() { defineInput("declared_probe", "1") }); recovered == nil ||
		!strings.Contains(recovered.(string), "duplicate fixture input declared_probe") {
		t.Fatal("a duplicate declaration was admitted:", recovered)
	}
	for _, name := range []string{"", "Upper", "1st", "with-dash"} {
		if recovered := panics(func() { defineInput(name, "") }); recovered == nil {
			t.Errorf("invalid input name %q was declared", name)
		}
	}
}

// A declared input is forwarded by the workflow's helper, read into the
// profile with its fallback, reported to the native tests and admitted by
// loadEnvironmentProfile; the same parameter without the declaration is
// refused.
func TestDeclaredInputIsReadForwardedAndAdmitted(t *testing.T) {
	row := func(params map[string]string) ciScenario {
		base := map[string]string{"key": "aes256k", "service": "aes256k", "family": "4"}
		maps.Copy(base, params)
		return ciScenario{ID: "a.1", Params: base}
	}
	if _, err := ciProfile(row(map[string]string{"declared_probe": "1"})); err == nil ||
		!strings.Contains(err.Error(), "no fixture input reads CEPH_MSGR_TEST_DECLARED_PROBE") {
		t.Fatal("an undeclared input was admitted:", err)
	}
	in := withInput(t, "declared_probe", "0")
	if env := ciRowEnvironment(t, map[string]string{"declared_probe": "1"}); env[in.variable] != "1" {
		t.Fatalf("the helper does not forward the declared input: %v", env)
	}
	set, err := ciProfile(row(map[string]string{"declared_probe": "1"}))
	if err != nil || set.input(in) != "1" || set.containerEnvironment()[in.variable] != "1" {
		t.Fatal("the declared input was not read:", set.input(in), err)
	}
	unset, err := ciProfile(row(nil))
	if err != nil || unset.input(in) != "0" || unset.containerEnvironment()[in.variable] != "0" {
		t.Fatal("the declared input lacks its fallback:", unset.input(in), err)
	}
	if set == unset {
		t.Fatal("the declared input does not distinguish profiles")
	}
	if (profile{}).input(in) != "0" {
		t.Fatal("a profile without the input lacks its fallback")
	}
	// Every declared input reaches the fixture under its own variable.
	for _, declared := range fixtureInputs {
		if env := ciRowEnvironment(t, map[string]string{declared.name: "x"}); env[declared.variable] != "x" {
			t.Errorf("declared input %s is not forwarded as %s", declared.name, declared.variable)
		}
	}
}

// For every catalog row, and every job of the planner's default coverage
// (fixture sequences included), the profile loads as it did before inputs could
// be declared: no input is declared outside tests, so the declared part is
// empty, and declaring one changes nothing but that part.
func TestDeclaredInputsLeaveEveryRowProfileUnchanged(t *testing.T) {
	var matrices []map[string]string
	for _, row := range ciRepositoryCatalog(t) {
		matrices = append(matrices, row.Params)
	}
	matrices = append(matrices, ciPlannedJobs(t)...)
	before := make([]profile, len(matrices))
	for i, matrix := range matrices {
		p, err := ciProfile(ciScenario{Params: matrix})
		if err != nil {
			t.Fatal(matrix, err)
		}
		if len(fixtureInputs) == 0 && p.inputs != "" {
			t.Fatalf("%v: inputs %q without a declaration", matrix, p.inputs)
		}
		before[i] = p
	}
	in := withInput(t, "declared_probe", "0")
	for i, matrix := range matrices {
		p, err := ciProfile(ciScenario{Params: matrix})
		if err != nil || p.input(in) != "0" {
			t.Fatal(matrix, p.input(in), err)
		}
		p.inputs = before[i].inputs
		if p != before[i] {
			t.Fatalf("%v: a declared input changed the profile\n%+v\n%+v", matrix, p, before[i])
		}
	}
}

// ciPlannedJobs are the matrix entries of the default coverage (`plan
// --select all`, its fixture sequences included) as the planner makes them.
func ciPlannedJobs(t *testing.T) []map[string]string {
	t.Helper()
	command := exec.Command("python3", filepath.Join("tools", "scenario_plan.py"), "plan", "--select", "all")
	command.Dir = filepath.Join("..", "..")
	output, err := command.Output()
	if err != nil {
		t.Fatal("planner:", err)
	}
	var plan struct {
		Include []map[string]string `json:"include"`
	}
	if err := json.Unmarshal(output, &plan); err != nil || len(plan.Include) == 0 {
		t.Fatal("planner output:", err)
	}
	return plan.Include
}

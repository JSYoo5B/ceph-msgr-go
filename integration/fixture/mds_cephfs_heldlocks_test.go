package main

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
)

// heldLockCellCapability is one capability of the held-lock cells, the exact
// tests whose rows declare it, the actions only it admits, and its fixture.
type heldLockCellCapability struct {
	capability *fixtureCapability
	selectors  []string
	actions    []string
	admit      func(string, profile) error
	fixture    func(map[string]string) bool
}

func heldLockCellCapabilities() []heldLockCellCapability {
	metadata := func(p map[string]string) bool { return p["mds_fresh"] == "1" && p["mds_cephfs"] == "1" }
	completion := func(p map[string]string) bool { return p["mds_fresh"] == "1" && p["mds_cephfs_completion"] == "1" }
	return []heldLockCellCapability{
		{capMDSHeldFcntlRanges, []string{"^TestCephMDSCephFSFcntlMergedRangeRecoveryIntegration$"}, []string{"fcntl-probe-ranges"}, admitMDSCephFSHeldLocks, metadata},
		{capMDSHeldFlockWait, []string{"^TestCephMDSCephFSWaitingFlockRecoveryIntegration$", "^TestCephMDSCephFSInterruptedFlockRecoveryIntegration$"}, []string{"lock-inflight"}, admitMDSCephFSHeldLocks, metadata},
		{capMDSHeldFcntlSplit, []string{"^TestCephMDSCephFSHeldFcntlSplitReconnectIntegration$"}, mdsCephFSHeldFcntlSplitActions, admitMDSCephFSHeldFlockSplit, completion},
		{capMDSHeldFlockSplitDense, []string{"^TestCephMDSCephFSHeldFlockSplitDenseIntegration$"}, mdsCephFSHeldFlockSplitDenseActions, admitMDSCephFSHeldFlockSplit, completion},
	}
}

// Each held-lock cell capability is declared only by its own tests' rows.
// Over every row topology combined with every row's selector, with its
// declaration and without one, its actions are admitted exactly where it is
// declared on its fixture, and then only for one of its exact tests; the
// declaration on any other topology is refused.
func TestMDSCephFSHeldLockCellCapabilitiesOverTheCatalog(t *testing.T) {
	rows := ciRepositoryCatalog(t)
	type selection struct{ selector, capabilities string }
	selections := map[selection]bool{{"", ""}: true}
	for _, row := range rows {
		s := selection{row.Params["test_run"], row.Params["capabilities"]}
		selections[s], selections[selection{s.selector, ""}] = true, true
	}
	for _, c := range heldLockCellCapabilities() {
		declares := func(capabilities string) bool {
			return slices.Contains(strings.Split(capabilities, ","), c.capability.name)
		}
		declaring := map[string]int{}
		for _, row := range rows {
			if declares(row.Params["capabilities"]) {
				if !slices.Contains(c.selectors, row.Params["test_run"]) || !c.fixture(row.Params) {
					t.Errorf("%s declares %s for %s", row.ID, c.capability.name, row.Params["test_run"])
				}
				declaring[row.Params["test_run"]]++
			}
		}
		for _, selector := range c.selectors {
			if declaring[selector] != 2 {
				t.Errorf("%s is declared by %d rows of %s, want 2", c.capability.name, declaring[selector], selector)
			}
		}
		admitted, refused := 0, 0
		for _, row := range rows {
			for s := range selections {
				params := maps.Clone(row.Params)
				params["test_run"], params["capabilities"] = s.selector, s.capabilities
				env := ciRowEnvironment(t, params)
				p, err := loadProfile(func(k string) string { return env[k] })
				probe := err == nil
				for _, action := range c.actions {
					probe = probe && c.admit(action, p) == nil
				}
				switch {
				case declares(s.capabilities) && c.fixture(row.Params) && (!probe || !slices.Contains(c.selectors, p.testRun)):
					t.Errorf("%s with %s [%s]: the %s cell was not admitted alone with its actions: %v", row.ID, s.selector, s.capabilities, c.capability.name, err)
				case declares(s.capabilities) && !c.fixture(row.Params) && err == nil:
					t.Errorf("%s with %s [%s]: %s was admitted off its fixture", row.ID, s.selector, s.capabilities, c.capability.name)
				case !declares(s.capabilities) && probe:
					t.Errorf("%s with %s [%s]: the actions of %s were admitted without it", row.ID, s.selector, s.capabilities, c.capability.name)
				}
				if probe {
					admitted++
				} else if declares(s.capabilities) {
					refused++
				}
			}
		}
		if admitted == 0 || refused == 0 {
			t.Fatal("the catalog no longer exercises both the admission and the refusal of", c.capability.name, admitted, refused)
		}
	}
}

func TestMDSCephFSHeldLockCellRequestsAreClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	id, action, err := parseMDSCephFSRequest([]string{"10", token, "fcntl-probe-ranges", "go-file"}, token, 9)
	if err != nil || id != 10 || action != (mdsCephFSAction{name: "fcntl-probe-ranges", target: "go-file"}) {
		t.Fatal("closed FCNTL range probe request refused", action, err)
	}
	id, action, err = parseMDSCephFSRequest([]string{"11", token, "lock-inflight", "4711:9"}, token, 10)
	if err != nil || id != 11 || action != (mdsCephFSAction{name: "lock-inflight", gid: 4711, tid: 9}) {
		t.Fatal("closed in-flight lock read refused", action, err)
	}
	for _, fields := range [][]string{
		{"10", token, "fcntl-probe-ranges"}, {"10", token, "fcntl-probe-ranges", "native-0"}, {"10", token, "fcntl-probe-ranges", "go-file", "w:0:1"},
		{"10", token, "lock-inflight"}, {"10", token, "lock-inflight", "4711"}, {"10", token, "lock-inflight", "0:9"}, {"10", token, "lock-inflight", "4711:0"},
		{"10", token, "lock-inflight", "04711:9"}, {"10", token, "lock-inflight", "4711:9:1"}, {"10", token, "lock-inflight", "go-file"},
	} {
		if _, _, err := parseMDSCephFSRequest(fields, token, 0); err == nil {
			t.Fatal("caller-selected held-lock cell request admitted", fields)
		}
	}
	var args []string
	for _, p := range mdsCephFSFcntlProbeSet("fcntl-probe-ranges") {
		args = append(args, p.arg())
	}
	if !slices.Equal(args, []string{"w:20:1", "w:31:1", "r:0:32", "w:32:32", "w:70:1", "w:85:1", "w:100:1", "w:128:0"}) || len(args) > 8 {
		t.Fatal("the fixed FCNTL range probe set changed", args)
	}
	if !slices.Equal(mdsCephFSFcntlProbeSet("fcntl-probe"), mdsCephFSFcntlProbes) || !slices.Equal(mdsCephFSFcntlProbeSet("split-fcntl-probe"), mdsCephFSFcntlProbes) ||
		mdsCephFSFcntlProbeSet("flock-ex-nb") != nil || mdsCephFSFcntlProbeSet("lock-inflight") != nil {
		t.Fatal("an action maps to another FCNTL probe set")
	}
	// The shared driver function is part of both workers.
	for name, script := range map[string]string{"cephfs": mdsCephFSNativePython, "completion": mdsCephFSCompletionNativePython} {
		if strings.Count(script, "def fcntl_probe(extra):") != 1 || strings.Count(script, "fcntl_probe(extra)") != 2 {
			t.Fatal("the FCNTL probe driver is not defined and called once in", name)
		}
	}
	if !strings.Contains(mdsCephFSNativePython, "elif action in ('fcntl-probe', 'fcntl-probe-ranges') and path == prefix + '/go-file' and 0 < len(extra) <= 8:") {
		t.Fatal("the metadata worker no longer bounds its FCNTL probes to go-file")
	}
}

// The range probe output is checked against its own fixed set, and the
// in-flight read reports only an observed owned session's request.
func TestMDSCephFSHeldLockCellOutputs(t *testing.T) {
	plan := mdsCephFSPlanFor(strings.Repeat("a1", 16))
	file, _ := plan.path("go-file")
	state := map[string]any{"ino": 1099511627776, "size": 0, "version": 3, "mtime": 1, "data_hex": ""}
	output := func(action string, set []mdsCephFSFcntlProbe) []byte {
		var probes []any
		for _, p := range set {
			probes = append(probes, map[string]any{"type": p.Type, "start": p.Start, "len": p.Length, "errno": 0, "acquired": true, "unlocked": true,
				"getlk": map[string]any{"type": "u", "start": p.Start, "len": p.Length, "pid": 0}})
		}
		data, _ := json.Marshal(map[string]any{"action": action, "path": file, "native_global_id": 7,
			"fcntl": map[string]any{"owner": mdsCephFSFlockOwner, "global_id": 8, "probes": probes, "elapsed_ns": 5, "before": state, "after": state}})
		return data
	}
	if _, err := decodeMDSCephFSNative("fcntl-probe-ranges", file, plan, output("fcntl-probe-ranges", mdsCephFSFcntlRangeProbes)); err != nil {
		t.Fatal("range probe output refused", err)
	}
	for name, tc := range map[string]struct {
		action string
		data   []byte
	}{
		"fixed set for the range probe":  {"fcntl-probe-ranges", output("fcntl-probe-ranges", mdsCephFSFcntlProbes)},
		"range set for the fixed probe":  {"fcntl-probe", output("fcntl-probe", mdsCephFSFcntlRangeProbes)},
		"range output for a flock probe": {"flock-ex-nb", output("flock-ex-nb", mdsCephFSFcntlRangeProbes)},
	} {
		if _, err := decodeMDSCephFSNative(tc.action, file, plan, tc.data); err == nil {
			t.Fatal("wrong FCNTL probe output accepted:", name)
		}
	}
	// Completion: the FCNTL probe of the first split file.
	cplan := mdsCephFSCompletionPlanFor(strings.Repeat("a1", 16))
	first, _ := cplan.path(cplan.splitLockFile())
	last, _ := cplan.path(cplan.splitFinalLockFile())
	split := func(path string, size int) []byte {
		var probes []any
		for _, p := range mdsCephFSFcntlProbes {
			probes = append(probes, map[string]any{"type": p.Type, "start": p.Start, "len": p.Length, "errno": 0, "acquired": true, "unlocked": true,
				"getlk": map[string]any{"type": "u", "start": p.Start, "len": p.Length, "pid": 0}})
		}
		s := map[string]any{"ino": 1099511627777, "size": size, "version": 0, "mtime": 1, "data_hex": strings.Repeat("00", size)}
		data, _ := json.Marshal(map[string]any{"action": "fcntl-probe", "path": path, "native_global_id": 7,
			"fcntl": map[string]any{"owner": mdsCephFSFlockOwner, "global_id": 8, "probes": probes, "elapsed_ns": 5, "before": s, "after": s}})
		return data
	}
	if _, err := decodeMDSCephFSCompletionNative("fcntl-probe", first, cplan, split(first, 0)); err != nil {
		t.Fatal("split FCNTL probe output refused", err)
	}
	if _, err := decodeMDSCephFSCompletionNative("fcntl-probe", last, cplan, split(last, 0)); err == nil {
		t.Fatal("a split FCNTL probe of the last split file was accepted")
	}
	if _, err := decodeMDSCephFSCompletionNative("fcntl-probe", first, cplan, split(first, 1)); err == nil {
		t.Fatal("a split FCNTL probe of a nonempty file was accepted")
	}
	// The in-flight read is refused for a session that was never observed.
	c := &fixtureCluster{mdsCephFS: &mdsCephFSScope{owned: map[string]uint64{"v1:10.0.0.1:0/7": 4711}}}
	var result mdsCephFSResult
	if err := c.mdsCephFSLockInFlightRead(t.Context(), mdsCephFSAction{name: "lock-inflight", gid: 4712, tid: 9}, &result); err == nil || result.LockInFlight != nil {
		t.Fatal("an in-flight read of an unobserved session was admitted", err)
	}
	if gid, tid, ok := parseMDSCephFSGIDTID("4711:9"); !ok || gid != 4711 || tid != 9 {
		t.Fatal("canonical GlobalID:TID refused")
	}
	data := []byte(`{"ops":[{"description":"client_request(client.4711:9 setfilelock rule 2, type 2, owner 1, pid 0, start 0, length 0, wait 1 #0x10000000002 caller_uid=0, caller_gid=0{})","initiated_at":"t","age":1,"duration":1,"type_data":{"flag_point":"failed to add lock, waiting","op_type":"client_request","events":[{"event":"initiated"},{"event":"failed to add lock, waiting"}]}}],"num_ops":1}`)
	op, _, err := mdsCephFSFindBoundaryOp(data, 4711, 9)
	if err != nil || op == nil || !slices.Contains(op.Events, "failed to add lock, waiting") {
		t.Fatal("a waiting setfilelock was not found in the in-flight dump", op, err)
	}
	if op, _, err := mdsCephFSFindBoundaryOp(data, 4711, 10); err != nil || op != nil {
		t.Fatal("another TID was found in the in-flight dump", op, err)
	}
}

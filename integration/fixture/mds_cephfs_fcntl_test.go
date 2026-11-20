package main

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
)

const mdsCephFSHeldFcntlSelector = "^TestCephMDSCephFSFcntlRangeRecoveryIntegration$"

func heldFcntlProfile(t *testing.T, selector, capabilities string) (profile, error) {
	t.Helper()
	env := mdsCephFSProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = selector
	env["CEPH_MSGR_TEST_CAPABILITIES"] = capabilities
	return loadProfile(func(k string) string { return env[k] })
}

// The FCNTL variant of the recovery acceptance is admitted only by its
// capability on the metadata fixture, and only that capability admits the
// FCNTL probe action.
func TestMDSCephFSHeldFcntlCapabilityAdmission(t *testing.T) {
	p, err := heldFcntlProfile(t, mdsCephFSHeldFcntlSelector, "mds-held-fcntl")
	if err != nil || p.testRun != mdsCephFSHeldFcntlSelector || p.mdsCephFS != "1" || !p.admits(capMDSHeldFcntl) {
		t.Fatal("the held-FCNTL acceptance was not admitted on the metadata fixture", p, err)
	}
	if err := admitMDSCephFSHeldFcntl("fcntl-probe", p); err != nil {
		t.Fatal("the FCNTL probe was refused under its capability", err)
	}
	for _, other := range []string{"flock-ex-nb", "stat", "restart-arm"} {
		if err := admitMDSCephFSHeldFcntl(other, profile{}); err != nil {
			t.Fatal("an ordinary metadata action needs the FCNTL capability", other, err)
		}
	}
	for _, tc := range []struct{ selector, capabilities string }{
		{mdsCephFSHeldFcntlSelector, ""},
		{"^TestCephMDSCephFSFcntlRangeRecoveryIntegration", "mds-held-fcntl"},
		{"^TestCephMDSCephFS(FcntlRange)?RecoveryIntegration$", "mds-held-fcntl"},
		{mdsCephFSHeldFcntlSelector, "exclusive-mds-cephfs-completion"},
		{mdsCephFSHeldFcntlSelector, "mds-held-flock-split"},
	} {
		if p, err := heldFcntlProfile(t, tc.selector, tc.capabilities); err == nil {
			t.Fatal("held-FCNTL selection admitted without its capability", tc, p)
		}
	}
	// The shared-lock and default metadata selections load, but neither
	// admits the FCNTL probe.
	for _, tc := range []struct{ selector, capabilities string }{
		{"^TestCephMDSCephFSSharedFlockRecoveryIntegration$", "exclusive-mds-cephfs"},
		{"", ""},
		{"^TestCephMDSCephFS(Push|Recovery)?Integration$", ""},
	} {
		p, err := heldFcntlProfile(t, tc.selector, tc.capabilities)
		if err != nil {
			t.Fatal("metadata selection refused", tc, err)
		}
		if admitMDSCephFSHeldFcntl("fcntl-probe", p) == nil {
			t.Fatal("the FCNTL probe was admitted without its capability", tc)
		}
	}
	// The capability alone does not open another fixture.
	env := mdsCephFSProfileEnv()
	env["CEPH_MSGR_TEST_MDS_CEPHFS"] = "0"
	env["CEPH_MSGR_TEST_RUN"], env["CEPH_MSGR_TEST_CAPABILITIES"] = mdsCephFSHeldFcntlSelector, "mds-held-fcntl"
	if p, err := loadProfile(func(k string) string { return env[k] }); err == nil {
		t.Fatal("the FCNTL capability was admitted without the metadata fixture", p)
	}
}

// The capability mds-held-fcntl came after the capability migration, so the
// legacy admission proof does not cover it; this is its own proof over the
// whole catalog. Only the FCNTL acceptance's rows declare it. Over every row
// topology combined with every row's selector, with its declaration and
// without one, the FCNTL probe is admitted exactly where mds-held-fcntl is
// declared on a fresh metadata fixture, and then only for the exact test
// selected; the declaration on any other topology is refused.
func TestMDSCephFSHeldFcntlAdmissionOverTheCatalog(t *testing.T) {
	rows := ciRepositoryCatalog(t)
	declares := func(capabilities string) bool {
		return slices.Contains(strings.Split(capabilities, ","), capMDSHeldFcntl.name)
	}
	type selection struct{ selector, capabilities string }
	selections := map[selection]bool{{"", ""}: true}
	declaring := 0
	for _, row := range rows {
		s := selection{row.Params["test_run"], row.Params["capabilities"]}
		selections[s], selections[selection{s.selector, ""}] = true, true
		if declares(s.capabilities) {
			if s.selector != mdsCephFSHeldFcntlSelector || s.capabilities != capMDSHeldFcntl.name {
				t.Errorf("%s declares %s for %s", row.ID, s.capabilities, s.selector)
			}
			declaring++
		}
	}
	if declaring == 0 {
		t.Fatal("no catalog row declares", capMDSHeldFcntl.name)
	}
	admitted, refused := 0, 0
	for _, row := range rows {
		metadata := row.Params["mds_fresh"] == "1" && row.Params["mds_cephfs"] == "1"
		for s := range selections {
			params := maps.Clone(row.Params)
			params["test_run"], params["capabilities"] = s.selector, s.capabilities
			env := ciRowEnvironment(t, params)
			p, err := loadProfile(func(k string) string { return env[k] })
			probe := err == nil && admitMDSCephFSHeldFcntl("fcntl-probe", p) == nil
			switch {
			case declares(s.capabilities) && metadata && (!probe || p.testRun != s.selector):
				t.Errorf("%s with %s [%s]: the FCNTL acceptance was not admitted alone with its probe: %v", row.ID, s.selector, s.capabilities, err)
			case declares(s.capabilities) && !metadata && err == nil:
				t.Errorf("%s with %s [%s]: the FCNTL capability was admitted off the metadata fixture", row.ID, s.selector, s.capabilities)
			case !declares(s.capabilities) && probe:
				t.Errorf("%s with %s [%s]: the FCNTL probe was admitted without its capability", row.ID, s.selector, s.capabilities)
			}
			if probe {
				admitted++
			} else if declares(s.capabilities) {
				refused++
			}
		}
	}
	if admitted == 0 || refused == 0 {
		t.Fatal("the catalog no longer exercises both the admission and the refusal", admitted, refused)
	}
	t.Logf("%d rows declare %s; %d topology and selector combinations admit its probe, %d declarations refused", declaring, capMDSHeldFcntl.name, admitted, refused)
}

func TestMDSCephFSFcntlProbeRequestAndArguments(t *testing.T) {
	token := strings.Repeat("a1", 16)
	id, action, err := parseMDSCephFSRequest([]string{"10", token, "fcntl-probe", "go-file"}, token, 9)
	if err != nil || id != 10 || action != (mdsCephFSAction{name: "fcntl-probe", target: "go-file"}) {
		t.Fatal("closed FCNTL probe request changed", id, action, err)
	}
	for _, fields := range [][]string{{"10", token, "fcntl-probe"}, {"10", token, "fcntl-probe", "native-0"}, {"10", token, "fcntl-probe", "dir"},
		{"10", token, "fcntl-probe", "go-file", "w:0:1"}, {"10", token, "fcntl-probe", "w:0:1"}} {
		if _, _, err := parseMDSCephFSRequest(fields, token, 0); err == nil {
			t.Fatal("caller-selected FCNTL probe admitted", fields)
		}
	}
	var args []string
	for _, p := range mdsCephFSFcntlProbes {
		args = append(args, p.arg())
	}
	if !slices.Equal(args, []string{"w:0:1", "w:15:1", "w:16:16", "r:32:16", "w:47:1", "w:48:0", "r:8:1"}) {
		t.Fatal("the fixed FCNTL probe set changed", args)
	}
}

func TestMDSCephFSFcntlProbeOutputShape(t *testing.T) {
	plan := mdsCephFSPlanFor(strings.Repeat("a1", 16))
	file, _ := plan.path("go-file")
	state := map[string]any{"ino": 1099511627776, "size": 0, "version": 3, "mtime": 1, "data_hex": ""}
	free := func(p mdsCephFSFcntlProbe) map[string]any {
		return map[string]any{"type": p.Type, "start": p.Start, "len": p.Length, "errno": 0, "acquired": true, "unlocked": true,
			"getlk": map[string]any{"type": "u", "start": p.Start, "len": p.Length, "pid": 0}}
	}
	conflict := func(p mdsCephFSFcntlProbe, kind string, start, length int64, pid int32) map[string]any {
		return map[string]any{"type": p.Type, "start": p.Start, "len": p.Length, "errno": 11, "acquired": false, "unlocked": false,
			"getlk": map[string]any{"type": kind, "start": start, "len": length, "pid": pid}}
	}
	// The held picture of the acceptance: exclusive [0,16) pid 4101 and
	// shared [32,48) pid 4103.
	held := func() []any {
		pr := mdsCephFSFcntlProbes
		return []any{conflict(pr[0], "w", 0, 16, 4101), conflict(pr[1], "w", 0, 16, 4101), free(pr[2]), free(pr[3]),
			conflict(pr[4], "r", 32, 16, 4103), free(pr[5]), conflict(pr[6], "w", 0, 16, 4101)}
	}
	released := func() []any {
		var out []any
		for _, p := range mdsCephFSFcntlProbes {
			out = append(out, free(p))
		}
		return out
	}
	output := func(probes []any, edit func(map[string]any)) []byte {
		o := map[string]any{"action": "fcntl-probe", "path": file, "native_global_id": 7,
			"fcntl": map[string]any{"owner": mdsCephFSFlockOwner, "global_id": 8, "probes": probes, "elapsed_ns": 5, "before": state, "after": state}}
		if edit != nil {
			edit(o)
		}
		data, _ := json.Marshal(o)
		return data
	}
	for name, data := range map[string][]byte{"held": output(held(), nil), "released": output(released(), nil)} {
		if _, err := decodeMDSCephFSNative("fcntl-probe", file, plan, data); err != nil {
			t.Fatal("admitted FCNTL probe output refused:", name, err)
		}
	}
	fc := func(o map[string]any) map[string]any { return o["fcntl"].(map[string]any) }
	probe := func(o map[string]any, i int) map[string]any { return fc(o)["probes"].([]any)[i].(map[string]any) }
	for name, edit := range map[string]func(map[string]any){
		"missing probe":      func(o map[string]any) { fc(o)["probes"] = fc(o)["probes"].([]any)[1:] },
		"reordered probes":   func(o map[string]any) { p := fc(o)["probes"].([]any); p[0], p[2] = p[2], p[0] },
		"other range":        func(o map[string]any) { probe(o, 2)["start"] = 17 },
		"conflict acquired":  func(o map[string]any) { probe(o, 0)["acquired"] = true },
		"acquire not undone": func(o map[string]any) { probe(o, 2)["unlocked"] = false },
		"free with conflict": func(o map[string]any) {
			probe(o, 2)["getlk"] = map[string]any{"type": "w", "start": 16, "len": 1, "pid": 4101}
		},
		"free other echo": func(o map[string]any) { probe(o, 2)["getlk"].(map[string]any)["pid"] = 3 },
		"refused no conflict": func(o map[string]any) {
			probe(o, 0)["getlk"] = map[string]any{"type": "u", "start": 0, "len": 1, "pid": 0}
		},
		"shared vs shared": func(o map[string]any) {
			probe(o, 3)["errno"], probe(o, 3)["acquired"], probe(o, 3)["unlocked"] = 11, false, false
			probe(o, 3)["getlk"] = map[string]any{"type": "r", "start": 32, "len": 16, "pid": 4103}
		},
		"disjoint conflict": func(o map[string]any) {
			probe(o, 0)["getlk"] = map[string]any{"type": "w", "start": 32, "len": 16, "pid": 4101}
		},
		"other errno": func(o map[string]any) { probe(o, 0)["errno"] = 5 },
		"same mount":  func(o map[string]any) { fc(o)["global_id"] = 7 },
		"no mount":    func(o map[string]any) { fc(o)["global_id"] = 0 },
		"other owner": func(o map[string]any) { fc(o)["owner"] = 1 },
		"file changed": func(o map[string]any) {
			fc(o)["after"] = map[string]any{"ino": 1099511627776, "size": 0, "version": 4, "mtime": 1, "data_hex": ""}
		},
		"flock output too": func(o map[string]any) { o["flock"] = map[string]any{"owner": 1} },
		"no probe":         func(o map[string]any) { delete(o, "fcntl") },
	} {
		if _, err := decodeMDSCephFSNative("fcntl-probe", file, plan, output(held(), edit)); err == nil {
			t.Fatal("wrong FCNTL probe output accepted:", name)
		}
	}
	if _, err := decodeMDSCephFSNative("flock-ex-nb", file, plan, output(held(), func(o map[string]any) { o["action"] = "flock-ex-nb" })); err == nil {
		t.Fatal("FCNTL output accepted for a flock probe")
	}
}

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestMDSCephFSPushPlanAndPaths(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSPushPlanFor(token)
	if plan.Directory != "ceph-msgr-cephfs-"+token+"-push" || plan.LeaseDir != "lease" || plan.Victim != "victim" || plan.Renamed != "renamed" || plan.SnapDir != "snap" ||
		plan.Snapshot != "ceph-msgr-snap-"+token[:8] || plan.QuotaDir != "quota" || len(plan.RecallFiles) != mdsCephFSPushRecallFiles || plan.RecallFiles[0] != "recall-00" ||
		plan.RecallFiles[23] != "recall-23" || plan.MinCaps != 8 || plan.MaxCaps != 12 || plan.MinCaps >= plan.MaxCaps || plan.DropTimeout != 10 {
		t.Fatal("owned CephFS push plan changed", plan)
	}
	// 0xa1a1 4 KiB steps above 16 MiB.
	if plan.QuotaMaxBytes != 1<<24+4096*0xa1a1 || plan.QuotaMaxBytes%4096 != 0 {
		t.Fatal("push quota is not the token-bound value", plan.QuotaMaxBytes)
	}
	if !reflect.DeepEqual(plan, mdsCephFSPushPlanFor(token)) || reflect.DeepEqual(plan, mdsCephFSPushPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("push plan is not deterministic and token-bound")
	}
	root := "/" + plan.Directory
	for target, want := range map[string]string{"dir": root, "lease": root + "/lease", "victim": root + "/lease/victim", "renamed": root + "/lease/renamed", "snap": root + "/snap",
		"snap-file": root + "/snap/snap-file", "snapshot": root + "/snap/.snap/" + plan.Snapshot, "quota": root + "/quota", "quota-file": root + "/quota/quota-file", "recall": root + "/recall"} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("owned push path changed", target, got, err)
		}
	}
	if len(mdsCephFSPushTargets) != 10 {
		t.Fatal("closed push target set changed", mdsCephFSPushTargets)
	}
	for _, target := range []string{"", "..", "go-file", "recall-00", "victim/x", "fenced", ".snap"} {
		if _, err := plan.path(target); err == nil {
			t.Fatal("unowned push target admitted", target)
		}
	}
	for _, directory := range []string{"ceph-msgr-cephfs-" + token, "ceph-msgr-cephfs-" + token + "-recovery", "elsewhere-push", "ceph-msgr-cephfs-x/y-push"} {
		other := plan
		other.Directory = directory
		if _, err := other.path("dir"); err == nil {
			t.Fatal("push plan outside its owned prefix and suffix admitted", directory)
		}
	}
	caps := mdsCephFSPushNativeCaps("owned")
	if caps.Mon != "allow r" || caps.MDS != "allow rwps fsname=owned" || caps.OSD != "" || caps.MGR != "" {
		t.Fatal("the push oracle must hold only MON read and a filesystem-bound MDS rwps grant", caps)
	}
}

func TestMDSCephFSPushRequestsAreClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, tc := range []struct {
		fields       []string
		name, target string
	}{
		{[]string{"10", token, "push-stat", "dir"}, "push-stat", "dir"},
		{[]string{"10", token, "push-stat", "snapshot"}, "push-stat", "snapshot"},
		{[]string{"10", token, "push-stat", "renamed"}, "push-stat", "renamed"},
		{[]string{"10", token, "push-rename"}, "push-rename", ""},
		{[]string{"10", token, "push-snapshot-create"}, "push-snapshot-create", ""},
		{[]string{"10", token, "push-snapshot-remove"}, "push-snapshot-remove", ""},
		{[]string{"10", token, "push-quota-set"}, "push-quota-set", ""},
		{[]string{"10", token, "push-quota-clear"}, "push-quota-clear", ""},
		{[]string{"10", token, "push-recall-on"}, "push-recall-on", ""},
		{[]string{"10", token, "push-recall-max-off"}, "push-recall-max-off", ""},
		{[]string{"10", token, "push-recall-off"}, "push-recall-off", ""},
		{[]string{"10", token, "push-cache-drop"}, "push-cache-drop", ""},
	} {
		id, action, err := parseMDSCephFSRequest(tc.fields, token, 9)
		if err != nil || id != 10 || action.name != tc.name || action.target != tc.target || action.gid != 0 {
			t.Fatal("closed MDS CephFS push action changed", tc.fields, id, action, err)
		}
	}
	for _, fields := range [][]string{
		{"10", token, "push-stat"}, {"10", token, "push-stat", "go-file"}, {"10", token, "push-stat", "/etc"}, {"10", token, "push-stat", "recall-00"},
		{"10", token, "push-rename", "victim"}, {"10", token, "push-snapshot-create", "x"}, {"10", token, "push-quota-set", "1"}, {"10", token, "push-quota-clear", "0"},
		{"10", token, "push-recall-on", "8"}, {"10", token, "push-cache-drop", "10"}, {"10", token, "push-force-readonly"}, {"10", token, "push-config", "mds_max_caps_per_client"},
	} {
		if _, _, err := parseMDSCephFSRequest(fields, token, 0); err == nil {
			t.Fatal("unowned or caller-selected CephFS push action admitted", fields)
		}
	}
}

func TestMDSCephFSPushNativeShapes(t *testing.T) {
	plan := mdsCephFSPushPlanFor(strings.Repeat("a1", 16))
	victim, _ := plan.path("victim")
	renamed, _ := plan.path("renamed")
	snapshot, _ := plan.path("snapshot")
	quotaDir, _ := plan.path("quota")
	quota := "16777216"
	file := map[string]any{"exists": true, "errno": 0, "ino": 1099511627790, "mode": 0o100644, "uid": 0, "gid": 0, "nlink": 1, "size": 0}
	dir := map[string]any{"exists": true, "errno": 0, "ino": 1099511627791, "mode": 0o40755, "uid": 0, "gid": 0, "nlink": 2, "size": 0}
	absent := map[string]any{"exists": false, "errno": 2, "ino": 0, "mode": 0, "uid": 0, "gid": 0, "nlink": 0, "size": 0}
	encode := func(v map[string]any) []byte { data, _ := json.Marshal(v); return data }
	base := func(action, path string, more map[string]any) map[string]any {
		out := map[string]any{"action": action, "path": path, "native_global_id": 7}
		for k, v := range more {
			out[k] = v
		}
		return out
	}
	good := []struct {
		action, path string
		extra        []string
		output       map[string]any
	}{
		{"stat", victim, nil, base("stat", victim, map[string]any{"stat": absent})},
		{"rename", victim, []string{renamed}, base("rename", victim, map[string]any{"stat": file, "source_stat": absent, "timings_ns": map[string]any{"start": 0, "renamed": 2100000000}})},
		{"mksnap", snapshot, nil, base("mksnap", snapshot, map[string]any{"stat": dir, "timings_ns": map[string]any{"start": 0, "done": 5}})},
		{"rmsnap", snapshot, nil, base("rmsnap", snapshot, map[string]any{"stat": absent, "timings_ns": map[string]any{"start": 0, "done": 5}})},
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": dir, "value_hex": hex.EncodeToString([]byte(quota))})},
		{"setquota", quotaDir, []string{"0"}, base("setquota", quotaDir, map[string]any{"stat": dir, "quota_absent": true})},
		{"setquota", quotaDir, []string{"0"}, base("setquota", quotaDir, map[string]any{"stat": dir, "value_hex": hex.EncodeToString([]byte("0"))})},
		// A readback that differs is recorded for the test to judge.
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": dir, "quota_absent": true})},
	}
	for _, tc := range good {
		if _, err := decodeMDSCephFSPushNative(tc.action, tc.path, tc.extra, encode(tc.output)); err != nil {
			t.Fatal("admitted push output rejected", tc.action, err)
		}
	}
	bad := []struct {
		action, path string
		extra        []string
		output       map[string]any
	}{
		{"rename", victim, []string{renamed}, base("rename", victim, map[string]any{"stat": file, "source_stat": file, "timings_ns": map[string]any{"start": 0, "renamed": 1}})},
		{"rename", victim, []string{renamed}, base("rename", victim, map[string]any{"stat": absent, "source_stat": absent, "timings_ns": map[string]any{"start": 0, "renamed": 1}})},
		{"rename", victim, []string{renamed}, base("rename", victim, map[string]any{"stat": file, "source_stat": absent})},
		{"mksnap", snapshot, nil, base("mksnap", snapshot, map[string]any{"stat": absent, "timings_ns": map[string]any{"start": 0, "done": 5}})},
		{"rmsnap", snapshot, nil, base("rmsnap", snapshot, map[string]any{"stat": dir, "timings_ns": map[string]any{"start": 0, "done": 5}})},
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": dir, "value_hex": hex.EncodeToString([]byte("016777216"))})},
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": dir, "value_hex": hex.EncodeToString([]byte(quota)), "quota_absent": true})},
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": dir})},
		{"setquota", quotaDir, []string{quota}, base("setquota", quotaDir, map[string]any{"stat": file, "value_hex": hex.EncodeToString([]byte(quota))})},
		{"stat", victim, nil, base("stat", victim, map[string]any{"stat": file, "timings_ns": map[string]any{"start": 0, "done": 5}})},
		{"stat", victim, nil, base("stat", renamed, map[string]any{"stat": file})},
		{"unlink", victim, nil, base("unlink", victim, map[string]any{"stat": absent})},
	}
	for _, tc := range bad {
		if _, err := decodeMDSCephFSPushNative(tc.action, tc.path, tc.extra, encode(tc.output)); err == nil {
			t.Fatal("ambiguous push output accepted", tc.action, tc.output)
		}
	}
}

func TestMDSCephFSPushNativeReadbacks(t *testing.T) {
	drop := []byte(`{"client_recall":{"return_code":0,"message":"(0) Success","recalled":22},"flush_journal":{"return_code":0,"message":""},"trim_cache":{"trimmed":3},"cache":{"pool":{"items":5,"bytes":9}},"duration":1.25}`)
	result, err := parseMDSCephFSCacheDrop(drop)
	if err != nil || result.RecallReturnCode != 0 || result.Recalled != 22 || result.FlushReturnCode != 0 || result.Trimmed != 3 || result.Duration != 1.25 || result.RecallMessage != "(0) Success" {
		t.Fatal("cache drop result differs", result, err)
	}
	if nested, err := parseMDSCephFSCacheDrop([]byte(`{"result":` + string(drop) + `}`)); err != nil || nested != result {
		t.Fatal("nested cache drop result differs", nested, err)
	}
	timeout := []byte(`{"client_recall":{"return_code":-110,"message":"(110) Connection timed out","recalled":0},"flush_journal":{"return_code":0,"message":""},"trim_cache":{"trimmed":0},"duration":10.1}`)
	if r, err := parseMDSCephFSCacheDrop(timeout); err != nil || r.RecallReturnCode != -110 {
		t.Fatal("a timed-out recall must be reported, not hidden", r, err)
	}
	for _, bad := range [][]byte{nil, []byte("x"), []byte(`{"error":"no"}`), []byte(`{"client_recall":{"return_code":0},"duration":1}`), []byte(`{"client_recall":{"return_code":0,"message":"","recalled":1},"flush_journal":{"return_code":0,"message":""},"trim_cache":{"trimmed":0}}`)} {
		if _, err := parseMDSCephFSCacheDrop(bad); err == nil {
			t.Fatal("incomplete cache drop output accepted", string(bad))
		}
	}
	if v, err := parseMDSCephFSDaemonUint([]byte(`{"mds_min_caps_per_client":"100"}`), "mds_min_caps_per_client"); err != nil || v != 100 {
		t.Fatal("daemon configuration readback differs", v, err)
	}
	for _, bad := range []string{`{"mds_min_caps_per_client":"0100"}`, `{"mds_min_caps_per_client":"-1"}`, `{"mds_max_caps_per_client":"100"}`, `{"mds_min_caps_per_client":"1","x":"2"}`, `{}`} {
		if _, err := parseMDSCephFSDaemonUint([]byte(bad), "mds_min_caps_per_client"); err == nil {
			t.Fatal("non-canonical daemon configuration accepted", bad)
		}
	}
	get := []byte(`{"mdsmap":{"fs_name":"owned","flags_state":{"allow_snaps":true}},"id":7}`)
	if allow, err := parseMDSCephFSAllowSnaps(get, 7, "owned"); err != nil || !allow {
		t.Fatal("allow_snaps readback differs", allow, err)
	}
	for _, bad := range [][]byte{get[:20], []byte(`{"mdsmap":{"fs_name":"owned","flags_state":{}},"id":7}`), []byte(`{"mdsmap":{"fs_name":"other","flags_state":{"allow_snaps":true}},"id":7}`)} {
		if _, err := parseMDSCephFSAllowSnaps(bad, 7, "owned"); err == nil {
			t.Fatal("ambiguous filesystem flags accepted", string(bad))
		}
	}
}

func TestMDSCephFSPushStepsAreOrderedAndOneShot(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"},
		mdsCephFS: &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(token), recovery: mdsCephFSRecoveryPlanFor(token), push: mdsCephFSPushPlanFor(token), owned: map[string]uint64{}}}
	x := c.mdsCephFS
	act := func(name string) error {
		var result mdsCephFSResult
		return c.mdsCephFSPushAct(context.Background(), 5, mdsCephFSAction{name: name}, &result)
	}
	// Removals, restores and the cache drop need their applied owned state;
	// the recall settings need a recorded baseline. All refuse before any
	// native command (the cluster is nil here).
	for _, name := range []string{"push-snapshot-remove", "push-quota-clear", "push-cache-drop", "push-recall-on", "push-recall-max-off", "push-recall-off", "push-unknown"} {
		if err := act(name); err == nil {
			t.Fatal("push step admitted without its owned precondition", name)
		}
	}
	if len(x.mutations) != 0 {
		t.Fatal("a refused push step was journaled", x.mutations)
	}
	// Without the push identity a native mutation is journaled first and then
	// recorded uncertain; it is never reissued.
	if err := act("push-rename"); err == nil {
		t.Fatal("push rename ran without the push identity")
	}
	if m := x.pushMutation("push-rename"); m == nil || m.State != "uncertain" || m.Error == "" {
		t.Fatal("push rename outcome was not journaled as uncertain", m)
	}
	if err := act("push-rename"); err == nil || !strings.Contains(err.Error(), "already issued") {
		t.Fatal("an uncertain push rename was reissued", err)
	}
	// An uncertain creation does not admit its removal.
	x.mutations = append(x.mutations, &mdsCephFSMutation{Kind: "push-snapshot-create", State: "uncertain"}, &mdsCephFSMutation{Kind: "push-quota-set", State: "uncertain"})
	for _, name := range []string{"push-snapshot-remove", "push-quota-clear"} {
		if err := act(name); err == nil || !strings.Contains(err.Error(), "requires an applied") {
			t.Fatal("removal admitted after an uncertain creation", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(out, "mds-cephfs-owned.json"))
	var journal struct {
		Mutations []mdsCephFSMutation `json:"mutations"`
		PushPlan  mdsCephFSPushPlan   `json:"push_plan"`
	}
	if err != nil || json.Unmarshal(data, &journal) != nil || len(journal.Mutations) != 1 || journal.Mutations[0].Kind != "push-rename" || journal.PushPlan.Directory != x.push.Directory {
		t.Fatal("push journal does not record the intent and outcome", err, journal)
	}
	if err := requireAppliedOrRestored(x, "push-config-min"); err == nil {
		t.Fatal("an unowned recall override was treated as owned")
	}
	x.mutations = append(x.mutations, &mdsCephFSMutation{Kind: "push-config-min", State: "restored"})
	if err := requireAppliedOrRestored(x, "push-config-min"); err != nil {
		t.Fatal("a restored recall override lost its ownership", err)
	}
}

func TestMDSCephFSPushCleanupWithoutBaseline(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	// A recorded push identity intent without a provider handle is an unknown
	// creation outcome: an explicit failure, never success.
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFS: &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(token),
		recovery: mdsCephFSRecoveryPlanFor(token), push: mdsCephFSPushPlanFor(token), pushIntent: true}}
	err := c.finishMDSCephFS(context.Background())
	var receipt mdsCephFSCleanup
	data, readErr := os.ReadFile(filepath.Join(out, "mds-cephfs-cleanup.json"))
	if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
		t.Fatal("cleanup receipt was not retained", readErr)
	}
	if err == nil || receipt.Completed || receipt.PushIdentityRemoved || !receipt.PushConfigRestored {
		t.Fatal("unknown push identity creation was reported as removed", err, receipt)
	}
}

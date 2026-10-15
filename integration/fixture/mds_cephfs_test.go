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
	"time"
)

func mdsCephFSProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSCephFSClosedProfileAndCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCephFSProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCephFSIntegration$" || p.mdsCephFS != "1" || p.mdsSessions != "0" || p.mdsCommands != "0" || p.osdPGV8() || !p.mdsFreshOSDAllowed() || !p.mdsPublicClient() || p.cleanupBudget() != 2*time.Minute {
			t.Fatal("MDS CephFS did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		config := mdsFreshFilesystemConfig(p, "ceph-msgr-fresh-"+strings.Repeat("ab", 16))
		if !config.MetadataPool.NoHashPSPool || !config.DataPool.NoHashPSPool {
			t.Fatal("MDS-only CephFS clients require the legacy no-HASHPSPOOL pools", config)
		}
		p.mdsFreshIdentity, p.mdsCephFSIdentity = "client.readonly-owned", "client.cephfs-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_MDS_CEPHFS_IDENTITY"] != p.mdsCephFSIdentity || x["CEPH_MSGR_MDS_CEPHFS_KEY_FILE"] != filepath.Join(dir, "mds-cephfs.key") || x["CEPH_MSGR_TEST_MDS_CEPHFS"] != "1" ||
				p.containerEnvironment()["CEPH_MSGR_TEST_MDS_CEPHFS"] != "1" || x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != "" || x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] != "" {
				t.Fatal("MDS CephFS credentials were mixed or lost", x)
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_CEPHFS": "true"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "1"}, {"CEPH_MSGR_TEST_MDS_SESSIONS": "1"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}, {"CEPH_MSGR_TEST_RUN": "^TestCephMDSSessionsIntegration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCeph(MDSFresh|MDSCephFS)Integration$"}, {"CEPH_MSGR_TEST_OSD": "0"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
	} {
		env := mdsCephFSProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS CephFS profile accepted", change, p, err)
		}
	}
}

func TestMDSCephFSIdentityCapsAndPlan(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	goCaps, nativeCaps := mdsCephFSGoCaps(name), mdsCephFSNativeCaps(name)
	if goCaps.Mon != "allow r" || goCaps.MDS != "allow rw fsname="+name || goCaps.OSD != "" || goCaps.MGR != "" {
		t.Fatal("the Go identity must hold only MON read and a filesystem-bound MDS read-write grant", goCaps)
	}
	if nativeCaps.Mon != "allow r" || nativeCaps.MDS != "allow rw fsname="+name || nativeCaps.OSD != "allow rw tag cephfs data="+name || nativeCaps.MGR != "" {
		t.Fatal("the native oracle must be bound to the owned filesystem and its data pool tag", nativeCaps)
	}
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSPlanFor(token)
	value, _ := hex.DecodeString(plan.XattrValueHex)
	payload, _ := hex.DecodeString(plan.PayloadHex)
	if plan.Directory != "ceph-msgr-cephfs-"+token || len(value) != 2+9+1+16 || value[0] != 0 || value[1] != 0xff || len(payload) != 64 || plan.XattrName != "user.ceph-msgr" {
		t.Fatal("owned CephFS plan changed", plan)
	}
	if !reflect.DeepEqual(plan, mdsCephFSPlanFor(token)) || reflect.DeepEqual(plan, mdsCephFSPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("plan is not deterministic and token-bound")
	}
	for target, want := range map[string]string{"dir": "/" + plan.Directory, "go-file": "/" + plan.Directory + "/go-file", "native-2": "/" + plan.Directory + "/native-2", "missing": "/" + plan.Directory + "/missing"} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("owned path changed", target, got, err)
		}
	}
	for _, target := range []string{"", "..", "/", "native-3", "go-file/x", "dir/.."} {
		if _, err := plan.path(target); err == nil {
			t.Fatal("unowned target admitted", target)
		}
	}
	foreign := plan
	foreign.Directory = "elsewhere"
	if _, err := foreign.path("dir"); err == nil {
		t.Fatal("plan outside the owned prefix admitted")
	}
}

func TestMDSCephFSRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, tc := range []struct {
		fields       []string
		name, target string
		gid          uint64
	}{
		{[]string{"10", token, "observe", "42"}, "observe", "", 42},
		{[]string{"10", token, "stat", "dir"}, "stat", "dir", 0},
		{[]string{"10", token, "stat", "missing"}, "stat", "missing", 0},
		{[]string{"10", token, "stat", "native-1"}, "stat", "native-1", 0},
		{[]string{"10", token, "getxattr", "go-file"}, "getxattr", "go-file", 0},
		{[]string{"10", token, "list", "dir"}, "list", "dir", 0},
		{[]string{"10", token, "create-native"}, "create-native", "dir", 0},
		{[]string{"10", token, "write", "go-file"}, "write", "go-file", 0},
		{[]string{"10", token, "read", "go-file"}, "read", "go-file", 0},
	} {
		id, action, err := parseMDSCephFSRequest(tc.fields, token, 9)
		if err != nil || id != 10 || action.name != tc.name || action.target != tc.target || action.gid != tc.gid {
			t.Fatal("closed MDS CephFS action changed", tc.fields, id, action, err)
		}
	}
	for _, fields := range [][]string{
		{"10", token}, {"10", "foreign", "stat", "dir"}, {"10", token, "observe"}, {"10", token, "observe", "0"}, {"10", token, "observe", "042"},
		{"10", token, "observe", "42", "43"}, {"10", token, "stat", "/etc"}, {"10", token, "stat", "native-3"}, {"10", token, "getxattr", "native-0"},
		{"10", token, "write", "native-0"}, {"10", token, "read", "dir"}, {"10", token, "list", "go-file"}, {"10", token, "create-native", "x"},
		{"10", token, "unlink", "go-file"}, {"10", token, "rmdir", "dir"}, {"0", token, "stat", "dir"}, {"010", token, "stat", "dir"},
	} {
		if _, _, err := parseMDSCephFSRequest(fields, token, 0); err == nil {
			t.Fatal("unowned or caller-selected CephFS action admitted", fields)
		}
	}
	if _, _, err := parseMDSCephFSRequest([]string{"10", token, "stat", "dir"}, token, 10); err == nil {
		t.Fatal("replayed MDS CephFS request admitted")
	}
}

func TestMDSCephFSNativeOutputShapes(t *testing.T) {
	plan := mdsCephFSPlanFor(strings.Repeat("a1", 16))
	dir, _ := plan.path("dir")
	file, _ := plan.path("go-file")
	stat := map[string]any{"exists": true, "errno": 0, "ino": 1099511627776, "mode": 0o100644, "uid": 0, "gid": 0, "nlink": 1, "size": 64}
	absent := map[string]any{"exists": false, "errno": 2, "ino": 0, "mode": 0, "uid": 0, "gid": 0, "nlink": 0, "size": 0}
	encode := func(v map[string]any) []byte { data, _ := json.Marshal(v); return data }
	good := []struct {
		action, path string
		output       map[string]any
	}{
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": stat}},
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": absent}},
		{"getxattr", file, map[string]any{"action": "getxattr", "path": file, "native_global_id": 7, "value_hex": plan.XattrValueHex}},
		{"create", dir, map[string]any{"action": "create", "path": dir, "native_global_id": 7, "created": []any{
			map[string]any{"name": "native-0", "stat": stat}, map[string]any{"name": "native-1", "stat": stat}, map[string]any{"name": "native-2", "stat": stat}}}},
		{"list", dir, map[string]any{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{map[string]any{"name": "go-file", "d_ino": 5, "stat": stat}}}},
		{"write", file, map[string]any{"action": "write", "path": file, "native_global_id": 7, "written": 64, "stat": stat,
			"timings_ns": map[string]any{"start": 0, "opened": 5, "written": 9, "synced": 9, "closed": 12}}},
		{"read", file, map[string]any{"action": "read", "path": file, "native_global_id": 7, "data_hex": plan.PayloadHex, "stat": stat}},
	}
	for _, tc := range good {
		if _, err := decodeMDSCephFSNative(tc.action, tc.path, plan, encode(tc.output)); err != nil {
			t.Fatal("admitted native output rejected", tc.action, err)
		}
	}
	bad := []struct {
		action, path string
		output       map[string]any
	}{
		{"stat", file, map[string]any{"action": "stat", "path": dir, "native_global_id": 7, "stat": stat}},
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 0, "stat": stat}},
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 7}},
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": map[string]any{"exists": false, "errno": 13}}},
		{"stat", file, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": stat, "extra": 1}},
		{"getxattr", file, map[string]any{"action": "getxattr", "path": file, "native_global_id": 7, "value_hex": "zz"}},
		{"create", dir, map[string]any{"action": "create", "path": dir, "native_global_id": 7, "created": []any{map[string]any{"name": "native-0", "stat": stat}}}},
		{"list", dir, map[string]any{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{
			map[string]any{"name": "a", "d_ino": 5, "stat": stat}, map[string]any{"name": "a", "d_ino": 6, "stat": stat}}}},
		{"write", file, map[string]any{"action": "write", "path": file, "native_global_id": 7, "written": 63, "stat": stat,
			"timings_ns": map[string]any{"start": 0, "opened": 5, "written": 9, "synced": 9, "closed": 12}}},
		{"write", file, map[string]any{"action": "write", "path": file, "native_global_id": 7, "written": 64, "stat": stat,
			"timings_ns": map[string]any{"start": 0, "opened": 5, "written": 4, "synced": 9, "closed": 12}}},
		{"read", file, map[string]any{"action": "read", "path": file, "native_global_id": 7, "data_hex": plan.PayloadHex}},
	}
	for _, tc := range bad {
		if _, err := decodeMDSCephFSNative(tc.action, tc.path, plan, encode(tc.output)); err == nil {
			t.Fatal("ambiguous native output accepted", tc.action, tc.output)
		}
	}
	if _, err := decodeMDSCephFSNative("stat", file, plan, []byte(strings.Repeat(" ", mdsCephFSJSONLimit+1))); err == nil {
		t.Fatal("unbounded native output accepted")
	}
}

func TestMDSCephFSControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-cephfs"), []byte("1 "+strings.Repeat("a1", 16)+" write go-file\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled, mdsCephFS: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS CephFS control scope") {
			t.Fatal("unowned CephFS mutation reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned CephFS mutation produced an acknowledgement or receipt", entries)
		}
	}
}

func TestMDSCephFSCleanupWithoutBaselineMutatesNothing(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, intent := range []bool{false, true} {
		out := t.TempDir()
		c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFS: &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(token), goIntent: intent}}
		err := c.finishMDSCephFS(context.Background())
		var receipt mdsCephFSCleanup
		data, readErr := os.ReadFile(filepath.Join(out, "mds-cephfs-cleanup.json"))
		if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
			t.Fatal("cleanup receipt was not retained", readErr)
		}
		if intent {
			// A recorded identity intent without a provider handle is an
			// unknown creation outcome: an explicit failure, never success.
			if err == nil || receipt.Completed || receipt.ClientIdentityRemoved {
				t.Fatal("unknown identity creation was reported as removed", err, receipt)
			}
			continue
		}
		if err != nil || !receipt.Completed || !receipt.NamespaceRestored || len(receipt.NewBlocklist) != 0 {
			t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
		}
	}
	if (&fixtureCluster{}).finishMDSCephFS(context.Background()) != nil {
		t.Fatal("cleanup without a CephFS scope must be a no-op")
	}
}

func TestMDSCephFSMutationsAreOneShot(t *testing.T) {
	x := &mdsCephFSScope{}
	if x.mutated("native-write") {
		t.Fatal("empty journal reports a mutation")
	}
	x.mutations = append(x.mutations, &mdsCephFSMutation{Kind: "native-write", State: "uncertain"})
	if !x.mutated("native-write") || x.mutated("native-create") {
		t.Fatal("an uncertain mutation must still block its reissue")
	}
}

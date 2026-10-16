package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func mdsCephFSRanksReplayProfileEnv() map[string]string {
	env := mdsCephFSRanksProfileEnv()
	env["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY"] = "1"
	return env
}

func TestMDSCephFSRanksReplayClosedProfile(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCephFSRanksReplayProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCephFSRanksReplayIntegration$" || p.mdsCephFSRanks != "1" || p.mdsCephFSRanksReplay != "1" || p.cleanupBudget() != 4*time.Minute ||
			!p.mdsPublicClient() || !p.mdsFreshOSDAllowed() {
			t.Fatal("standby-replay extension did not select its isolated fixture", p, err)
		}
		if active, standby := p.mdsFreshTopology(); active != 3 || standby != 1 {
			t.Fatal("standby-replay topology differs", active, standby)
		}
		config := mdsFreshFilesystemConfig(p, "ceph-msgr-fresh-"+strings.Repeat("ab", 16))
		if config.ActiveMDS != 3 || config.StandbyMDS != 1 || config.StandbyReplay {
			t.Fatal("the provider must start an ordinary standby; the fixture enables replay itself", config)
		}
		if p.testEnvironment("/out", "")["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY"] != "1" {
			t.Fatal("standby-replay opt-in did not reach the test and fixture")
		}
	}
	base, err := loadProfile(func(k string) string { return mdsCephFSRanksProfileEnv()[k] })
	if err != nil || base.mdsCephFSRanksReplay != "0" || base.testRun != "^TestCephMDSCephFSRanksIntegration$" {
		t.Fatal("the base multi-rank profile changed", base, err)
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS": "0"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": "true"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFSRanksIntegration$"}, {"CEPH_MSGR_TEST_MDS_CEPHFS": "1"},
	} {
		env := mdsCephFSRanksReplayProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe standby-replay profile accepted", change, p, err)
		}
	}
	only := map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": "1", "CEPH_MSGR_TEST_OSD": "1"}
	if _, err := loadProfile(func(k string) string { return only[k] }); err == nil {
		t.Fatal("the standby-replay extension was accepted without the multi-rank profile")
	}
}

func TestMDSCephFSRanksReplayPlanAndRequests(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSRanksPlanFor(token, true)
	base := mdsCephFSRanksPlanFor(token, false)
	if base.Chain != "" || base.listTarget("dir") || slices.Contains(base.targets(), "chain") {
		t.Fatal("the base plan gained extension targets", base)
	}
	root := "/" + plan.Directory
	for target, want := range map[string]string{"chain": root + "/chain", "chain/hop": root + "/chain/hop", "chain/hop/forwarded-dir": root + "/chain/hop/forwarded-dir",
		"inflight/held-file": root + "/inflight/held-file", "replay/inflight-dir": root + "/replay/inflight-dir"} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("extension path changed", target, got, err)
		}
	}
	if !slices.Equal(plan.pinTargets(), []string{"fwd", "mig", "chain/hop", "chain", "inflight"}) || plan.pinTarget("replay") || !plan.listTarget("chain/hop") || plan.listTarget("chain") {
		t.Fatal("extension pin or list targets differ", plan.pinTargets())
	}
	for _, tc := range []struct {
		fields       []string
		name, target string
		value        string
		gid          uint64
	}{
		{[]string{"10", token, "pin", "chain/hop", "2"}, "pin", "chain/hop", "2", 0},
		{[]string{"10", token, "placement", "chain/hop", "2"}, "placement", "chain/hop", "2", 0},
		{[]string{"10", token, "list", "inflight"}, "list", "inflight", "", 0},
		{[]string{"10", token, "ops", "2", "42"}, "ops", "", "2", 42},
		{[]string{"10", token, "replay-enable"}, "replay-enable", "", "", 0},
		{[]string{"10", token, "log-pause"}, "log-pause", "", "", 0},
		{[]string{"10", token, "log-resume"}, "log-resume", "", "", 0},
	} {
		id, action, err := parseMDSCephFSRanksRequest(tc.fields, token, plan, 3, true, 9)
		if err != nil || id != 10 || action.name != tc.name || action.target != tc.target || action.value != tc.value || action.gid != tc.gid {
			t.Fatal("extension action changed", tc.fields, action, err)
		}
		if _, _, err := parseMDSCephFSRanksRequest(tc.fields, token, base, 2, false, 9); err == nil {
			t.Fatal("extension action admitted by the base profile", tc.fields)
		}
	}
	for _, fields := range [][]string{
		{"10", token, "pin", "chain", "3"}, {"10", token, "placement", "chain", "-1"}, {"10", token, "list", "chain"}, {"10", token, "list", "/etc"},
		{"10", token, "ops", "3", "42"}, {"10", token, "ops", "0"}, {"10", token, "ops", "0", "042"}, {"10", token, "replay-enable", "x"}, {"10", token, "log-pause", "true"},
		{"10", token, "pin", "replay", "1"},
	} {
		if _, _, err := parseMDSCephFSRanksRequest(fields, token, plan, 3, true, 0); err == nil {
			t.Fatal("caller-selected extension action admitted", fields)
		}
	}
}

func TestMDSCephFSRanksReplayTopology(t *testing.T) {
	x := newRanksDumpFixture()
	name := x.oracle.FSName
	x.info["gid_54"] = ranksDaemon(name+"-3", 54, 2, 9, "up:active", "172.18.0.8:6800", 14)
	x.mds["max_mds"], x.mds["in"], x.mds["up"] = 3, []int{0, 1, 2}, map[string]int{"mds_0": 51, "mds_1": 52, "mds_2": 54}
	x.owned[name+"-3"] = true
	owned := func(n string) bool { return x.owned[n] }
	plain, err := x.parse()
	if err != nil || plain.stable(owned, mdsCephFSRanksShape{Ranks: 3}) != nil || plain.stable(owned, mdsCephFSRanksShape{Ranks: 3, Follow: true}) == nil ||
		plain.stable(owned, mdsCephFSRanksShape{Ranks: 2}) == nil || plain.spare().GID != 53 {
		t.Fatal("three ranks with a standby were not stable", plain, err)
	}
	// The standby becomes rank 0's follower: it leaves the standby list and
	// joins the MDSMap info with rank 0.
	x.dump["standbys"] = []any{}
	x.info["gid_53"] = ranksDaemon(name+"-2", 53, 0, 0, "up:standby-replay", "172.18.0.7:6800", 13)
	follow, err := x.parse()
	if err != nil || follow.stable(owned, mdsCephFSRanksShape{Ranks: 3, Follow: true}) != nil || follow.stable(owned, mdsCephFSRanksShape{Ranks: 3}) == nil ||
		len(follow.Followers) != 1 || follow.spare().GID != 53 || follow.spare().State != "up:standby-replay" || len(follow.Daemons) != 4 || follow.sameAs(plain) || !follow.sameAs(follow) {
		t.Fatal("the standby-replay follower was not parsed", follow, err)
	}
	x.info["gid_53"] = ranksDaemon(name+"-2", 53, 1, 0, "up:standby-replay", "172.18.0.7:6800", 13)
	if other, err := x.parse(); err != nil || other.stable(owned, mdsCephFSRanksShape{Ranks: 3, Follow: true}) == nil {
		t.Fatal("a follower of another rank was accepted", other, err)
	}
	views := []mdsCephFSRanksView{{Rank: 0}, {Rank: 1}, {Rank: 2, Subtrees: []mdsCephFSRanksSubtree{{Path: "/d/chain/hop", IsAuth: true, AuthFirst: 2, AuthSecond: -2}}}}
	if !mdsCephFSRanksPlaced(views, "/d/chain/hop", 2) || mdsCephFSRanksPlaced(views, "/d/chain/hop", 1) || mdsCephFSRanksPlaced(views, "/d/chain/hop", 3) {
		t.Fatal("three-rank placement differs")
	}
}

func TestMDSCephFSRanksReplayNativeList(t *testing.T) {
	plan := mdsCephFSRanksPlanFor(strings.Repeat("a1", 16), true)
	dir, _ := plan.path("inflight")
	entry := func(name string, ino int) map[string]any {
		return map[string]any{"name": name, "d_ino": ino, "stat": map[string]any{"exists": true, "errno": 0, "ino": ino, "mode": 0o100644, "uid": 0, "gid": 0, "nlink": 1, "size": 0}}
	}
	encode := func(v map[string]any) []byte { data, _ := json.Marshal(v); return data }
	if got, err := decodeMDSCephFSRanksNative("list", dir, nil, encode(map[string]any{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{entry("anchor", 9), entry("held-file", 10)}})); err != nil || len(got.Entries) != 2 {
		t.Fatal("listing rejected", err)
	}
	if got, err := decodeMDSCephFSRanksNative("list", dir, nil, encode(map[string]any{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{}})); err != nil || got.Entries == nil {
		t.Fatal("empty listing rejected", err)
	}
	for _, out := range []map[string]any{
		{"action": "list", "path": dir, "native_global_id": 7},
		{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{entry("anchor", 9), entry("anchor", 10)}},
		{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{map[string]any{"name": "x", "d_ino": 3, "stat": entry("x", 4)["stat"]}}},
		{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{entry("a/b", 9)}},
		{"action": "list", "path": dir, "native_global_id": 7, "entries": []any{}, "pin": "1"},
	} {
		if _, err := decodeMDSCephFSRanksNative("list", dir, nil, encode(out)); err == nil {
			t.Fatal("ambiguous listing accepted", out)
		}
	}
}

func TestMDSCephFSRanksReplayCleanupRuntimeAttribution(t *testing.T) {
	r := &mdsCephFSRanksRuntime{State: "applied"}
	for state, active := range map[string]bool{"intended": true, "applied": true, "uncertain": true, "restore-intended": true, "restore-uncertain": true, "restored": false, "discarded": false} {
		r.State = state
		if r.active() != active {
			t.Fatal("runtime active state differs", state)
		}
	}
	x := &mdsCephFSRanksScope{runtime: []*mdsCephFSRanksRuntime{{Kind: "log-pause", State: "restored"}, {Kind: "log-pause", State: "applied", GID: 7}}}
	if p := x.logPaused(); p == nil || p.GID != 7 {
		t.Fatal("active journal pause not found")
	}
	out := t.TempDir()
	token := strings.Repeat("a1", 16)
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSRanks: &mdsCephFSRanksScope{out: out, plan: mdsCephFSRanksPlanFor(token, true), replay: true,
		shape: mdsCephFSRanksShape{Ranks: 3}}}
	if err := c.finishMDSCephFSRanks(context.Background()); err != nil {
		t.Fatal(err)
	}
	var receipt mdsCephFSRanksCleanup
	data, err := os.ReadFile(filepath.Join(out, "mds-cephfs-ranks-cleanup.json"))
	if err != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Completed || !receipt.RuntimeRestored || receipt.Shape.Ranks != 3 || receipt.Runtime == nil {
		t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
	}
}

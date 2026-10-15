package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

func mdsCephFSRanksProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS_RANKS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSCephFSRanksClosedProfileAndCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCephFSRanksProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCephFSRanksIntegration$" || p.mdsCephFSRanks != "1" || p.mdsCephFS != "0" || p.mdsSessions != "0" || p.mdsCommands != "0" ||
			p.osdPGV8() || !p.mdsFreshOSDAllowed() || !p.mdsPublicClient() || p.cleanupBudget() != 3*time.Minute {
			t.Fatal("MDS CephFS ranks did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		if active, standby := p.mdsFreshTopology(); active != 2 || standby != 1 {
			t.Fatal("multi-rank profile topology differs", active, standby)
		}
		config := mdsFreshFilesystemConfig(p, "ceph-msgr-fresh-"+strings.Repeat("ab", 16))
		if config.ActiveMDS != 2 || config.StandbyMDS != 1 || config.StandbyReplay || !config.MetadataPool.NoHashPSPool || !config.DataPool.NoHashPSPool {
			t.Fatal("multi-rank filesystem config differs", config)
		}
		p.mdsFreshIdentity, p.mdsCephFSRanksIdentity = "client.readonly-owned", "client.ranks-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY"] != p.mdsCephFSRanksIdentity || x["CEPH_MSGR_MDS_CEPHFS_RANKS_KEY_FILE"] != filepath.Join(dir, "mds-cephfs-ranks.key") ||
				x["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS"] != "1" || x["CEPH_MSGR_MDS_CEPHFS_IDENTITY"] != "" ||
				x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != "" || x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] != "" {
				t.Fatal("MDS CephFS ranks credentials were mixed or lost", x)
			}
		}
	}
	single, _ := loadProfile(func(k string) string { return mdsCephFSProfileEnv()[k] })
	if active, standby := single.mdsFreshTopology(); active != 1 || standby != 0 {
		t.Fatal("single-rank profiles changed topology", active, standby)
	}
	if config := mdsFreshFilesystemConfig(single, "x"); config.ActiveMDS != 1 || config.StandbyMDS != 0 {
		t.Fatal("single-rank filesystem config changed", config)
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS": "true"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "1"}, {"CEPH_MSGR_TEST_MDS_SESSIONS": "1"},
		{"CEPH_MSGR_TEST_MDS_CEPHFS": "1"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}, {"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFSIntegration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Push|Recovery|Ranks)?Integration$"}, {"CEPH_MSGR_TEST_OSD": "0"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
	} {
		env := mdsCephFSRanksProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS CephFS ranks profile accepted", change, p, err)
		}
	}
	env := mdsCephFSProfileEnv()
	env["CEPH_MSGR_TEST_MDS_CEPHFS_RANKS"] = "1"
	if _, err := loadProfile(func(k string) string { return env[k] }); err == nil {
		t.Fatal("the single-rank and multi-rank CephFS profiles were combined")
	}
}

func TestMDSCephFSRanksIdentityCapsAndPlan(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	goCaps, nativeCaps := mdsCephFSRanksGoCaps(name), mdsCephFSRanksNativeCaps(name)
	if goCaps.Mon != "allow r" || goCaps.MDS != "allow rw fsname="+name || goCaps.OSD != "" || goCaps.MGR != "" {
		t.Fatal("the Go identity must hold only MON read and a filesystem-bound MDS read-write grant", goCaps)
	}
	if nativeCaps.Mon != "allow r" || nativeCaps.MDS != "allow rwp fsname="+name || nativeCaps.OSD != "" || nativeCaps.MGR != "" {
		t.Fatal("the native oracle needs only the filesystem-bound MDS grant with the vxattr flag", nativeCaps)
	}
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSRanksPlanFor(token)
	if plan.Directory != "ceph-msgr-ranks-"+token || !reflect.DeepEqual(plan, mdsCephFSRanksPlanFor(token)) || reflect.DeepEqual(plan, mdsCephFSRanksPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("plan is not deterministic and token-bound", plan)
	}
	root := "/" + plan.Directory
	for target, want := range map[string]string{"dir": root, "fwd": root + "/fwd", "fwd/target": root + "/fwd/target", "mig": root + "/mig", "mig/file-2": root + "/mig/file-2"} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("owned path changed", target, got, err)
		}
	}
	for _, target := range []string{"", "..", "/", "mig/file-3", "fwd/..", "target", "dir/fwd", "fwd/target/x"} {
		if _, err := plan.path(target); err == nil {
			t.Fatal("unowned target admitted", target)
		}
	}
	if !plan.pinTarget("fwd") || !plan.pinTarget("mig") || plan.pinTarget("dir") || plan.pinTarget("fwd/target") {
		t.Fatal("pin targets changed")
	}
	foreign := plan
	foreign.Directory = "ceph-msgr-cephfs-" + token
	if _, err := foreign.path("dir"); err == nil {
		t.Fatal("plan outside the owned prefix admitted")
	}
}

func TestMDSCephFSRanksRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSRanksPlanFor(token)
	for _, tc := range []struct {
		fields              []string
		name, target, value string
		gid                 uint64
	}{
		{[]string{"10", token, "observe", "42"}, "observe", "", "", 42},
		{[]string{"10", token, "stat", "dir"}, "stat", "dir", "", 0},
		{[]string{"10", token, "stat", "fwd/target"}, "stat", "fwd/target", "", 0},
		{[]string{"10", token, "pin", "fwd", "1"}, "pin", "fwd", "1", 0},
		{[]string{"10", token, "pin", "mig", "-1"}, "pin", "mig", "-1", 0},
		{[]string{"10", token, "pin-restore", "mig"}, "pin-restore", "mig", "", 0},
		{[]string{"10", token, "placement", "mig", "0"}, "placement", "mig", "0", 0},
		{[]string{"10", token, "takeover-arm"}, "takeover-arm", "", "", 0},
		{[]string{"10", token, "takeover-fire"}, "takeover-fire", "", "", 0},
		{[]string{"10", token, "takeover-status"}, "takeover-status", "", "", 0},
		{[]string{"10", token, "takeover-adopt"}, "takeover-adopt", "", "", 0},
	} {
		id, action, err := parseMDSCephFSRanksRequest(tc.fields, token, plan, 9)
		if err != nil || id != 10 || action.name != tc.name || action.target != tc.target || action.value != tc.value || action.gid != tc.gid {
			t.Fatal("closed MDS CephFS ranks action changed", tc.fields, id, action, err)
		}
	}
	for _, fields := range [][]string{
		{"10", token}, {"10", "foreign", "stat", "dir"}, {"10", token, "observe"}, {"10", token, "observe", "0"}, {"10", token, "observe", "042"},
		{"10", token, "observe", "42", "43"}, {"10", token, "stat", "/etc"}, {"10", token, "stat", "mig/file-3"}, {"10", token, "pin", "dir", "1"},
		{"10", token, "pin", "fwd", "2"}, {"10", token, "pin", "fwd"}, {"10", token, "pin", "fwd/target", "1"}, {"10", token, "placement", "fwd", "-1"},
		{"10", token, "placement", "fwd", "2"}, {"10", token, "pin-restore", "dir"}, {"10", token, "pin-restore", "fwd", "1"}, {"10", token, "takeover-fire", "4242"},
		{"10", token, "takeover-arm", "x"}, {"10", token, "mds-fail"}, {"10", token, "evict", "42"}, {"0", token, "stat", "dir"}, {"010", token, "stat", "dir"},
		{"10", token, "pin", "fwd", "1", "x"},
	} {
		if _, _, err := parseMDSCephFSRanksRequest(fields, token, plan, 0); err == nil {
			t.Fatal("unowned or caller-selected CephFS ranks action admitted", fields)
		}
	}
	if _, _, err := parseMDSCephFSRanksRequest([]string{"10", token, "stat", "dir"}, token, plan, 10); err == nil {
		t.Fatal("replayed MDS CephFS ranks request admitted")
	}
}

type ranksDumpFixture struct {
	oracle mdsFreshOracle
	fs     map[string]any
	mds    map[string]any
	info   map[string]any
	dump   map[string]any
	owned  map[string]bool
}

func ranksDaemon(name string, gid, rank, incarnation int, state, addr string, nonce int) map[string]any {
	return map[string]any{"name": name, "gid": gid, "rank": rank, "state": state, "incarnation": incarnation, "join_fscid": 1,
		"addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": addr, "nonce": nonce}}}}
}

func newRanksDumpFixture() ranksDumpFixture {
	name := "ceph-msgr-fresh-" + strings.Repeat("a1", 16)
	info := map[string]any{"gid_51": ranksDaemon(name+"-0", 51, 0, 6, "up:active", "172.18.0.5:6800", 11), "gid_52": ranksDaemon(name+"-1", 52, 1, 8, "up:active", "172.18.0.6:6800", 12)}
	mds := map[string]any{"epoch": 9, "fs_name": name, "max_mds": 2, "metadata_pool": 3, "data_pools": []int{4}, "in": []int{0, 1}, "up": map[string]int{"mds_0": 51, "mds_1": 52},
		"info": info, "failed": []int{}, "damaged": []int{}, "stopped": []int{}}
	fs := map[string]any{"id": 1, "mdsmap": mds}
	dump := map[string]any{"epoch": 9, "filesystems": []any{fs}, "standbys": []any{ranksDaemon(name+"-2", 53, -1, 0, "up:standby", "172.18.0.7:6800", 13)}}
	return ranksDumpFixture{oracle: mdsFreshOracle{FSID: "x", FSCID: 1, FSName: name, MetadataPool: 3, DataPools: []int64{4}}, fs: fs, mds: mds, info: info, dump: dump,
		owned: map[string]bool{name + "-0": true, name + "-1": true, name + "-2": true}}
}

func (x ranksDumpFixture) parse() (mdsCephFSRanksTopology, error) {
	data, _ := json.Marshal(x.dump)
	return parseMDSCephFSRanksTopology(data, x.oracle, "4", func(name string) bool { return x.owned[name] })
}

func TestMDSCephFSRanksTopology(t *testing.T) {
	x := newRanksDumpFixture()
	got, err := x.parse()
	if err != nil || got.stable(func(name string) bool { return x.owned[name] }) != nil || got.FSMapEpoch != 9 || got.MDSMapEpoch != 9 || got.Ranks[0].GID != 51 || got.Ranks[1].GID != 52 ||
		got.Ranks[1].Endpoint != "172.18.0.6:6800" || got.Ranks[1].Nonce != 12 || got.Standbys[0].GID != 53 || got.Standbys[0].Nonce != 13 || len(got.Daemons) != 2 || !got.sameAs(got) {
		t.Fatal("stable two-rank topology was not parsed", got, err)
	}
	owned := func(name string) bool { return x.owned[name] }
	// Transitional takeover maps parse but are not stable.
	for name, change := range map[string]func(*ranksDumpFixture){
		"rank 0 in reconnect": func(x *ranksDumpFixture) {
			x.info["gid_53"] = ranksDaemon(x.oracle.FSName+"-2", 53, 0, 12, "up:reconnect", "172.18.0.7:6800", 13)
			delete(x.info, "gid_51")
			x.mds["up"] = map[string]int{"mds_0": 53, "mds_1": 52}
			x.dump["standbys"] = []any{}
		},
		"rank 0 failed": func(x *ranksDumpFixture) {
			delete(x.info, "gid_51")
			x.mds["up"] = map[string]int{"mds_1": 52}
			x.mds["failed"] = []int{0}
		},
		"no standby":      func(x *ranksDumpFixture) { x.dump["standbys"] = []any{} },
		"foreign standby": func(x *ranksDumpFixture) { delete(x.owned, x.oracle.FSName+"-2") },
		"one rank":        func(x *ranksDumpFixture) { x.mds["max_mds"] = 1 },
	} {
		y := newRanksDumpFixture()
		change(&y)
		got, err := y.parse()
		if err != nil || got.stable(func(name string) bool { return y.owned[name] }) == nil {
			t.Fatal("transitional map was rejected or treated as stable", name, got, err)
		}
	}
	reconnect := newRanksDumpFixture()
	reconnect.info["gid_53"] = ranksDaemon(reconnect.oracle.FSName+"-2", 53, 0, 12, "up:reconnect", "172.18.0.7:6800", 13)
	delete(reconnect.info, "gid_51")
	reconnect.mds["up"] = map[string]int{"mds_0": 53, "mds_1": 52}
	if got, err := reconnect.parse(); err != nil || got.Ranks[0].GID != 53 || got.Ranks[0].State != "up:reconnect" || got.Ranks[0].Incarnation != 12 {
		t.Fatal("rank 0 in reconnect was not reported", got, err)
	}
	for name, change := range map[string]func(*ranksDumpFixture){
		"foreign FSCID":  func(x *ranksDumpFixture) { x.fs["id"] = 8 },
		"foreign name":   func(x *ranksDumpFixture) { x.mds["fs_name"] = "foreign" },
		"foreign pool":   func(x *ranksDumpFixture) { x.mds["data_pools"] = []int{9} },
		"damaged rank":   func(x *ranksDumpFixture) { x.mds["damaged"] = []int{1} },
		"bad up entry":   func(x *ranksDumpFixture) { x.mds["up"] = map[string]int{"mds_0": 99, "mds_1": 52} },
		"rank mismatch":  func(x *ranksDumpFixture) { x.mds["up"] = map[string]int{"mds_0": 52, "mds_1": 51} },
		"two FS":         func(x *ranksDumpFixture) { x.dump["filesystems"] = []any{x.fs, x.fs} },
		"huge max_mds":   func(x *ranksDumpFixture) { x.mds["max_mds"] = 1 << 30 },
		"negative ranks": func(x *ranksDumpFixture) { x.mds["max_mds"] = -1 },
	} {
		y := newRanksDumpFixture()
		change(&y)
		if _, err := y.parse(); err == nil {
			t.Fatal("foreign or malformed native map accepted", name)
		}
	}
	if _, err := parseMDSCephFSRanksTopology([]byte(strings.Repeat(" ", mdsFreshJSONLimit+1)), x.oracle, "4", owned); err == nil {
		t.Fatal("unbounded native map accepted")
	}
	// Identity comparison covers both ranks and the standby.
	base, _ := x.parse()
	for name, change := range map[string]func(*ranksDumpFixture){
		"rank 1 incarnation": func(x *ranksDumpFixture) { x.info["gid_52"].(map[string]any)["incarnation"] = 9 },
		"rank 0 nonce": func(x *ranksDumpFixture) {
			x.info["gid_51"].(map[string]any)["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.5:6800", "nonce": 99}}}
		},
		"standby GID": func(x *ranksDumpFixture) {
			x.dump["standbys"] = []any{ranksDaemon(x.oracle.FSName+"-2", 54, -1, 0, "up:standby", "172.18.0.7:6800", 13)}
		},
	} {
		y := newRanksDumpFixture()
		change(&y)
		got, err := y.parse()
		if err != nil || got.sameAs(base) {
			t.Fatal("changed daemon identity compared equal", name, err)
		}
	}
}

func TestMDSCephFSRanksSubtreesCountersAndStatus(t *testing.T) {
	prefix := "/ceph-msgr-ranks-" + strings.Repeat("a1", 16)
	entry := func(path string, auth bool, first, second, pin int) map[string]any {
		return map[string]any{"is_auth": auth, "auth_first": first, "auth_second": second, "export_pin": pin, "distributed_ephemeral_pin": false, "random_ephemeral_pin": false,
			"export_pin_target": pin, "dir": map[string]any{"path": path, "dirfrag": "0x1", "states": []string{"auth"}}}
	}
	encode := func(v any) []byte { data, _ := json.Marshal(v); return data }
	rank1 := encode([]any{entry("~mds1", true, 1, -2, -1), entry("", false, 0, -2, -1), entry(prefix+"/fwd", true, 1, -2, 1), entry("/elsewhere", true, 1, -2, -1)})
	subtrees, count, err := parseMDSCephFSRanksSubtrees(rank1, prefix)
	if err != nil || count != 4 || len(subtrees) != 2 || subtrees[0].Path != "" || subtrees[1].Path != prefix+"/fwd" || !subtrees[1].IsAuth || subtrees[1].ExportPin != 1 {
		t.Fatal("owned subtrees were not kept", subtrees, count, err)
	}
	if _, _, err := parseMDSCephFSRanksSubtrees(encode([]any{map[string]any{"is_auth": true, "dir": map[string]any{"path": prefix}}}), prefix); err == nil {
		t.Fatal("subtree without authority fields accepted")
	}
	rank0 := encode([]any{entry("", true, 0, -2, -1), entry(prefix+"/fwd", false, 1, -2, 1)})
	view := func(rank int32, data []byte) mdsCephFSRanksView {
		s, n, err := parseMDSCephFSRanksSubtrees(data, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return mdsCephFSRanksView{Rank: rank, Subtrees: s, SubtreeCount: n}
	}
	views := []mdsCephFSRanksView{view(0, rank0), view(1, rank1)}
	if !mdsCephFSRanksPlaced(views, prefix+"/fwd", 1) || mdsCephFSRanksPlaced(views, prefix+"/fwd", 0) || mdsCephFSRanksPlaced(views, prefix+"/mig", 1) {
		t.Fatal("placement convergence differs")
	}
	ambiguous := []mdsCephFSRanksView{view(0, encode([]any{entry(prefix+"/fwd", true, 0, 1, 1)})), view(1, rank1)}
	if mdsCephFSRanksPlaced(ambiguous, prefix+"/fwd", 1) {
		t.Fatal("ambiguous authority accepted as converged")
	}
	selfPinned := []mdsCephFSRanksView{view(0, encode([]any{entry(prefix+"/mig", true, 0, -2, 0)})), view(1, encode([]any{entry("", false, 0, -2, -1)}))}
	if !mdsCephFSRanksPlaced(selfPinned, prefix+"/mig", 0) {
		t.Fatal("an aux subtree on rank 0 was not accepted")
	}
	counters, err := parseMDSCephFSRanksCounters([]byte(`{"mds":{"request":9,"forward":1,"exported":2,"exported_inodes":3,"imported":4,"imported_inodes":5,"reply_latency":{"avgcount":1}}}`))
	if err != nil || counters != (mdsCephFSRanksCounters{Forward: 1, Exported: 2, ExportedInodes: 3, Imported: 4, ImportedInodes: 5}) {
		t.Fatal("counters differ", counters, err)
	}
	if _, err := parseMDSCephFSRanksCounters([]byte(`{"mds":{"forward":1}}`)); err == nil {
		t.Fatal("incomplete counters accepted")
	}
	whoami, gid, state, epoch, err := parseMDSCephFSRanksDaemonStatus([]byte(`{"whoami":1,"id":4194,"state":"up:active","mdsmap_epoch":10}`))
	if err != nil || whoami != 1 || gid != 4194 || state != "up:active" || epoch != 10 {
		t.Fatal("daemon status differs", whoami, gid, state, epoch, err)
	}
	if _, _, _, _, err := parseMDSCephFSRanksDaemonStatus([]byte(`{"whoami":1,"state":"up:active","mdsmap_epoch":10}`)); err == nil {
		t.Fatal("daemon status without GID accepted")
	}
}

func TestMDSCephFSRanksNativeOutputShapes(t *testing.T) {
	plan := mdsCephFSRanksPlanFor(strings.Repeat("a1", 16))
	dir, _ := plan.path("fwd")
	file, _ := plan.path("fwd/target")
	stat := map[string]any{"exists": true, "errno": 0, "ino": 1099511627777, "mode": 0o40755, "uid": 0, "gid": 0, "nlink": 1, "size": 0}
	fileStat := map[string]any{"exists": true, "errno": 0, "ino": 1099511627778, "mode": 0o100644, "uid": 0, "gid": 0, "nlink": 1, "size": 0}
	absent := map[string]any{"exists": false, "errno": 2, "ino": 0, "mode": 0, "uid": 0, "gid": 0, "nlink": 0, "size": 0}
	encode := func(v map[string]any) []byte { data, _ := json.Marshal(v); return data }
	for _, tc := range []struct {
		action, path string
		extra        []string
		output       map[string]any
	}{
		{"stat", file, nil, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": fileStat}},
		{"stat", file, nil, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": absent}},
		{"pin-get", dir, nil, map[string]any{"action": "pin-get", "path": dir, "native_global_id": 7, "stat": stat, "pin": "-1"}},
		{"pin-get", dir, nil, map[string]any{"action": "pin-get", "path": dir, "native_global_id": 7, "stat": absent}},
		{"pin-set", dir, []string{"1"}, map[string]any{"action": "pin-set", "path": dir, "native_global_id": 7, "pin_set": "1"}},
	} {
		if _, err := decodeMDSCephFSRanksNative(tc.action, tc.path, tc.extra, encode(tc.output)); err != nil {
			t.Fatal("admitted native output rejected", tc.action, tc.output, err)
		}
	}
	for _, tc := range []struct {
		action, path string
		extra        []string
		output       map[string]any
	}{
		{"stat", file, nil, map[string]any{"action": "stat", "path": dir, "native_global_id": 7, "stat": fileStat}},
		{"stat", file, nil, map[string]any{"action": "stat", "path": file, "native_global_id": 0, "stat": fileStat}},
		{"stat", file, nil, map[string]any{"action": "stat", "path": file, "native_global_id": 7, "stat": fileStat, "pin": "1"}},
		{"pin-get", dir, nil, map[string]any{"action": "pin-get", "path": dir, "native_global_id": 7, "stat": stat}},
		{"pin-get", dir, nil, map[string]any{"action": "pin-get", "path": dir, "native_global_id": 7, "stat": stat, "pin": "x"}},
		{"pin-get", file, nil, map[string]any{"action": "pin-get", "path": file, "native_global_id": 7, "stat": fileStat, "pin": "1"}},
		{"pin-get", dir, nil, map[string]any{"action": "pin-get", "path": dir, "native_global_id": 7, "stat": absent, "pin": "1"}},
		{"pin-set", dir, []string{"1"}, map[string]any{"action": "pin-set", "path": dir, "native_global_id": 7, "pin_set": "0"}},
		{"pin-set", dir, []string{"1"}, map[string]any{"action": "pin-set", "path": dir, "native_global_id": 7, "pin_set": "1", "extra": 1}},
	} {
		if _, err := decodeMDSCephFSRanksNative(tc.action, tc.path, tc.extra, encode(tc.output)); err == nil {
			t.Fatal("ambiguous native output accepted", tc.action, tc.output)
		}
	}
	if _, err := decodeMDSCephFSRanksNative("stat", file, nil, []byte(strings.Repeat(" ", mdsCephFSRanksJSONLimit+1))); err == nil {
		t.Fatal("unbounded native output accepted")
	}
}

func TestMDSCephFSRanksControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-cephfs-ranks"), []byte("1 "+strings.Repeat("a1", 16)+" takeover-fire\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled, mdsCephFSRanks: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS CephFS ranks control scope") {
			t.Fatal("unowned multi-rank mutation reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned multi-rank mutation produced an acknowledgement or receipt", entries)
		}
	}
}

func TestMDSCephFSRanksCleanupWithoutBaselineMutatesNothing(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, intent := range []bool{false, true} {
		out := t.TempDir()
		c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSRanks: &mdsCephFSRanksScope{out: out, plan: mdsCephFSRanksPlanFor(token), goIntent: intent}}
		err := c.finishMDSCephFSRanks(context.Background())
		var receipt mdsCephFSRanksCleanup
		data, readErr := os.ReadFile(filepath.Join(out, "mds-cephfs-ranks-cleanup.json"))
		if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
			t.Fatal("cleanup receipt was not retained", readErr)
		}
		if intent {
			if err == nil || receipt.Completed || receipt.ClientIdentityRemoved {
				t.Fatal("unknown identity creation was reported as removed", err, receipt)
			}
			continue
		}
		if err != nil || !receipt.Completed || !receipt.NamespaceRestored || !receipt.BlocklistRestored || !receipt.PinsRestored || !receipt.TopologyRestored || len(receipt.NewBlocklist) != 0 {
			t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
		}
	}
	if (&fixtureCluster{}).finishMDSCephFSRanks(context.Background()) != nil {
		t.Fatal("cleanup without a ranks scope must be a no-op")
	}
}

func TestMDSCephFSRanksTakeoverPendingRefusesOrdinaryActions(t *testing.T) {
	for state, pending := range map[string]bool{"armed": false, "intended": true, "applied": true, "uncertain": true, "adopted": false} {
		if (&mdsCephFSRanksTakeover{State: state}).pending() != pending {
			t.Fatal("takeover pending state differs", state)
		}
	}
	if (*mdsCephFSRanksTakeover)(nil).pending() {
		t.Fatal("no takeover reported pending")
	}
}

func TestMDSFreshOracleAcceptsTheMultiRankTopology(t *testing.T) {
	x := freshNativeFixture()
	x.p.mdsCephFSRanks = "1"
	second := map[string]any{"name": x.name + ".b", "gid": 52, "rank": 1, "state": "up:active", "incarnation": 2, "join_fscid": 1,
		"addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.10:6800", "nonce": 18}}}}
	x.mds["max_mds"], x.mds["in"], x.mds["up"] = 2, []int{0, 1}, map[string]int{"mds_0": 51, "mds_1": 52}
	x.mds["info"] = map[string]any{"gid_51": x.info, "gid_52": second}
	x.owned = map[string]bool{x.name + ".a": true, x.name + ".b": true, x.name + ".c": true}
	x.status.MaxMDS = 2
	x.status.Active = append(x.status.Active, cephfs.MDSStatus{Name: x.name + ".b", GID: 52, Rank: 1, State: "up:active", Owned: true})
	x.status.Standby = []cephfs.MDSStatus{{Name: x.name + ".c", GID: 53, Rank: -1, State: "up:standby", Owned: true}}
	x.dumpEdit = func(d map[string]any) { d["standbys"] = []any{map[string]any{"name": x.name + ".c", "gid": 53}} }
	got, err := x.parse()
	if err != nil || got.MDS.Rank != 0 || got.MDS.GID != 51 {
		t.Fatal("multi-rank fresh oracle rejected or names another rank", got, err)
	}
	x.status.Standby = nil
	if _, err := x.parse(); err == nil {
		t.Fatal("multi-rank fresh oracle accepted a missing standby")
	}
}

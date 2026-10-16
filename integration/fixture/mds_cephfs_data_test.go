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

func mdsCephFSDataProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSCephFSDataClosedProfileAndCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCephFSDataProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCephFSDataIntegration$" || p.mdsCephFSData != "1" || p.mdsCephFSCompletion != "0" || p.mdsCephFS != "0" || p.mdsCephFSRanks != "0" ||
			p.mdsSessions != "0" || p.mdsCommands != "0" || p.osdPGV8() || !p.mdsFreshOSDAllowed() || !p.mdsPublicClient() || p.cleanupBudget() != 2*time.Minute {
			t.Fatal("MDS CephFS data did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		if active, standby := p.mdsFreshTopology(); active != 1 || standby != 0 {
			t.Fatal("data profile topology differs", active, standby)
		}
		p.mdsFreshIdentity, p.mdsCephFSDataIdentity = "client.readonly-owned", "client.data-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_MDS_CEPHFS_DATA_IDENTITY"] != p.mdsCephFSDataIdentity || x["CEPH_MSGR_MDS_CEPHFS_DATA_KEY_FILE"] != filepath.Join(dir, "mds-cephfs-data.key") ||
				x["CEPH_MSGR_TEST_MDS_CEPHFS_DATA"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_CEPHFS_DATA"] != "1" || x["CEPH_MSGR_MDS_CEPHFS_IDENTITY"] != "" ||
				x["CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"] != "" || x["CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY"] != "" || x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != "" ||
				x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] != "" {
				t.Fatal("MDS CephFS data credentials were mixed or lost", x)
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "yes"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "1"}, {"CEPH_MSGR_TEST_MDS_SESSIONS": "1"},
		{"CEPH_MSGR_TEST_MDS_CEPHFS": "1"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS": "1"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION": "1"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Push|Recovery)?Integration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Data|Completion)Integration$"},
		{"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}, {"CEPH_MSGR_TEST_RBD": "1"},
	} {
		env := mdsCephFSDataProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS CephFS data profile accepted", change, p, err)
		}
	}
}

func TestMDSCephFSDataIdentityCapsAndPlan(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	goCaps, nativeCaps := mdsCephFSDataGoCaps(name), mdsCephFSDataNativeCaps(name)
	if goCaps.Mon != "allow r" || goCaps.MDS != "allow rw fsname="+name || goCaps.OSD != "allow rw tag cephfs data="+name || goCaps.MGR != "" {
		t.Fatal("the Go identity must hold MON read, a filesystem-bound MDS grant without layout or snapshot permission and an OSD grant on its data pool only", goCaps)
	}
	if nativeCaps.Mon != "allow r" || nativeCaps.MDS != "allow rwps fsname="+name || nativeCaps.OSD != "allow rw tag cephfs data="+name || nativeCaps.MGR != "" {
		t.Fatal("the native identity caps differ", nativeCaps)
	}
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSDataPlanFor(token)
	if plan.Directory != "ceph-msgr-data-"+token || plan.Snapshot != "data-a1a1a1a1" || !slices.Equal(plan.Files, mdsCephFSDataFiles) || plan.StripeUnit != 65536 || plan.StripeCount != 2 ||
		plan.ObjectSize != 131072 || plan.GoFileSize != 614523 || plan.NativeSize != 1620000 || plan.TruncWriteSize != 1228800 || plan.TruncSize != 280000 || plan.SharedSize != 200000 ||
		plan.SnapV1Size != 600000 || plan.SnapV2Offset+plan.SnapV2Size <= plan.SnapV1Size || len(plan.Seeds) != 9 || len(plan.ObjectCounts) != len(plan.Files) {
		t.Fatal("data plan differs", plan)
	}
	if plan.layout("owned-data") != "stripe_unit=65536 stripe_count=2 object_size=131072 pool=owned-data" || !mdsCephFSDataLayoutPattern.MatchString(plan.layout("owned-data")) {
		t.Fatal("planned directory layout differs", plan.layout("owned-data"))
	}
	other := mdsCephFSDataPlanFor(strings.Repeat("b2", 16))
	for purpose, seed := range plan.Seeds {
		if !strings.HasSuffix(seed, ":"+token) || other.Seeds[purpose] == seed {
			t.Fatal("payload seeds are not token-bound", purpose, seed)
		}
	}
	last := uint64(0)
	for _, e := range plan.NativeExtents {
		if e[0] < last || e[1] == 0 || e[0]+e[1] > plan.NativeSize {
			t.Fatal("native extents are not ordered inside the file", plan.NativeExtents)
		}
		last = e[0] + e[1]
	}
	if last != plan.NativeSize {
		t.Fatal("the native file size must end with its last extent", last)
	}
	for target, want := range map[string]string{
		"dir": "/ceph-msgr-data-" + token, "go-file": "/ceph-msgr-data-" + token + "/go-file", "snapshot": "/ceph-msgr-data-" + token + "/.snap/data-a1a1a1a1",
		"snapshot-file":  "/ceph-msgr-data-" + token + "/.snap/data-a1a1a1a1/snap-file",
		"capsnap-a":      "/ceph-msgr-data-" + token + "/.snap/data-a1a1a1a1-cap-a",
		"capsnap-b-file": "/ceph-msgr-data-" + token + "/.snap/data-a1a1a1a1-cap-b/capsnap-file",
	} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("data path differs", target, got, err)
		}
	}
	for _, target := range []string{"", "..", "go-file/..", "/etc", ".snap", "missing", "native-0"} {
		if _, err := plan.path(target); err == nil {
			t.Fatal("data path admitted", target)
		}
	}
	bad := plan
	bad.Directory = "ceph-msgr-completion-" + token
	if _, err := bad.path("dir"); err == nil {
		t.Fatal("a directory outside the data prefix was admitted")
	}
}

func TestMDSCephFSDataRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	parse := func(previous int64, fields ...string) (int64, mdsCephFSDataAction, error) {
		return parseMDSCephFSDataRequest(fields, token, previous)
	}
	for _, tc := range []struct {
		fields []string
		want   mdsCephFSDataAction
	}{
		{[]string{"1", token, "observe", "4210"}, mdsCephFSDataAction{name: "observe", gid: 4210}},
		{[]string{"2", token, "stat", "snapshot-file"}, mdsCephFSDataAction{name: "stat", target: "snapshot-file"}},
		{[]string{"3", token, "read", "native-file"}, mdsCephFSDataAction{name: "read", target: "native-file"}},
		{[]string{"4", token, "objects", "snap-file"}, mdsCephFSDataAction{name: "objects", target: "snap-file"}},
		{[]string{"5", token, "layout-set"}, mdsCephFSDataAction{name: "layout-set", target: "dir"}},
		{[]string{"6", token, "truncate"}, mdsCephFSDataAction{name: "truncate", target: "trunc-file"}},
		{[]string{"7", token, "read-shared"}, mdsCephFSDataAction{name: "read-shared", target: "shared-file"}},
		{[]string{"8", token, "snapshot-remove"}, mdsCephFSDataAction{name: "snapshot-remove", target: "snapshot"}},
		{[]string{"9", token, "capsnap-create-a"}, mdsCephFSDataAction{name: "capsnap-create-a", target: "capsnap-a"}},
		{[]string{"10", token, "capsnap-create-b"}, mdsCephFSDataAction{name: "capsnap-create-b", target: "capsnap-b"}},
		{[]string{"11", token, "capsnap-remove-a"}, mdsCephFSDataAction{name: "capsnap-remove-a", target: "capsnap-a"}},
		{[]string{"12", token, "capsnap-remove-b"}, mdsCephFSDataAction{name: "capsnap-remove-b", target: "capsnap-b"}},
		{[]string{"13", token, "read", "capsnap-a-file"}, mdsCephFSDataAction{name: "read", target: "capsnap-a-file"}},
		{[]string{"14", token, "stat", "capsnap-b-file"}, mdsCephFSDataAction{name: "stat", target: "capsnap-b-file"}},
	} {
		id, action, err := parse(0, tc.fields...)
		if err != nil || action != tc.want || id <= 0 {
			t.Fatal("closed data request differs", tc.fields, action, err)
		}
	}
	for _, fields := range [][]string{
		{"1", token, "observe"}, {"1", token, "observe", "0"}, {"1", token, "observe", "042"}, {"1", token, "stat", "/etc/passwd"}, {"1", token, "stat"},
		{"1", token, "read", "dir"}, {"1", token, "read", "snapshot"}, {"1", token, "objects", "snapshot-file"}, {"1", token, "list", "dir"}, {"1", token, "truncate", "1"},
		{"1", token, "write", "go-file"}, {"1", token, "evict", "4210"}, {"1", strings.Repeat("b2", 16), "list"}, {"0", token, "list"}, {"01", token, "list"}, {"1", token},
		{"1", token, "stat", "dir", "x"},
		{"1", token, "capsnap-create-c"}, {"1", token, "capsnap-create-a", "/etc/passwd"},
		{"1", token, "read", "capsnap-a"}, {"1", token, "objects", "capsnap-a-file"},
		{"1", token, "stat", "capsnap-c-file"},
	} {
		if _, _, err := parse(0, fields...); err == nil {
			t.Fatal("unsafe data request admitted", fields)
		}
	}
	if _, _, err := parse(5, "5", token, "list"); err == nil {
		t.Fatal("a replayed data request identity was admitted")
	}
}

func TestMDSCephFSDataNativeOutputShapes(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSDataPlanFor(token)
	pool := "owned-data"
	layout := plan.layout(pool)
	path := func(target string) string {
		p, _ := plan.path(target)
		return p
	}
	dir := `{"exists":true,"errno":0,"ino":1099511627776,"mode":16877,"uid":0,"gid":0,"nlink":2,"size":0,"mtime":1,"version":2}`
	file := `{"exists":true,"errno":0,"ino":1099511627777,"mode":33188,"uid":0,"gid":0,"nlink":1,"size":1620000,"mtime":1,"version":2}`
	absent := `{"exists":false,"errno":2,"ino":0,"mode":0,"uid":0,"gid":0,"nlink":0,"size":0,"mtime":0,"version":0}`
	hash := strings.Repeat("ab", 32)
	digest := func(length int, chunks int) string {
		parts := make([]string, chunks)
		for i := range parts {
			parts[i] = `"` + hash + `"`
		}
		return `{"length":` + itoa(length) + `,"sha256":"` + hash + `","chunks":[` + strings.Join(parts, ",") + `]}`
	}
	head := func(action, target string) string {
		return `{"action":"` + action + `","path":"` + path(target) + `","native_global_id":7`
	}
	objects := func(ino string, count int, snaps bool) string {
		var parts []string
		for i := range count {
			o := `{"objno":` + itoa(i) + `,"name":"` + ino + "." + strings.Repeat("0", 7) + itoa(i) + `","exists":true,"size":65536`
			if snaps {
				o += `,"snaps":{"seq":2,"clones":[{"head":false,"id":2,"snapshots":[2],"size":65536,"overlaps":[[0,50848]]},{"head":true,"id":0,"snapshots":[],"size":65536,"overlaps":[]}]}`
			}
			parts = append(parts, o+"}")
		}
		return `[` + strings.Join(parts, ",") + `]`
	}
	for _, tc := range []struct {
		action, target, data string
		ok                   bool
	}{
		{"stat", "dir", head("stat", "dir") + `,"stat":` + dir + `,"layout":"` + layout + `","layout_absent":false}`, true},
		{"stat", "dir", head("stat", "dir") + `,"stat":` + dir + `,"layout":"","layout_absent":true}`, true},
		{"stat", "dir", head("stat", "dir") + `,"stat":` + dir + `,"layout":"stripe_unit=4194304 stripe_count=1 object_size=4194304 pool=owned-data","layout_absent":false}`, false},
		{"stat", "go-file", head("stat", "go-file") + `,"stat":` + file + `,"layout":"` + layout + `","layout_absent":false}`, true},
		{"stat", "go-file", head("stat", "go-file") + `,"stat":` + file + `,"layout":"","layout_absent":true}`, false},
		{"stat", "go-file", head("stat", "go-file") + `,"stat":` + absent + `}`, true},
		{"stat", "snapshot", head("stat", "snapshot") + `,"stat":` + dir + `}`, true},
		{"stat", "snapshot", head("stat", "snapshot") + `,"stat":` + dir + `,"layout":"` + layout + `"}`, false},
		{"layout-set", "dir", head("layout-set", "dir") + `,"stat":` + dir + `,"layout":"` + layout + `","layout_absent":false}`, true},
		{"layout-set", "dir", head("layout-set", "dir") + `,"stat":` + file + `,"layout":"` + layout + `","layout_absent":false}`, false},
		{"layout-clear", "dir", head("layout-clear", "dir") + `,"stat":` + dir + `,"layout":"","layout_absent":true}`, true},
		{"layout-clear", "dir", head("layout-clear", "dir") + `,"stat":` + dir + `,"layout":"` + layout + `","layout_absent":false}`, false},
		{"read", "native-file", head("read", "native-file") + `,"stat":` + file + `,"read":` + digest(1620000, 99) + `}`, true},
		{"read", "native-file", head("read", "native-file") + `,"stat":` + file + `,"read":` + digest(1620000, 98) + `}`, false},
		{"read", "native-file", head("read", "native-file") + `,"stat":` + file + `,"read":` + digest(1620000, 99) + `,"timings_ns":{"start":0}}`, false},
		{"read-shared", "shared-file", head("read-shared", "shared-file") + `,"stat":` + file + `,"read":` + digest(200000, 13) + `,"timings_ns":{"start":0,"opened":5,"read":9,"closed":10}}`, true},
		{"read-shared", "shared-file", head("read-shared", "shared-file") + `,"stat":` + file + `,"read":` + digest(200000, 13) + `,"timings_ns":{"start":0,"opened":5,"read":4,"closed":10}}`, false},
		{"write-native", "native-file", head("write-native", "native-file") + `,"stat":` + file + `,"written":220000}`, true},
		{"write-native", "native-file", head("write-native", "native-file") + `,"stat":` + file + `,"written":219999}`, false},
		{"truncate", "trunc-file", head("truncate", "trunc-file") + `,"stat":{"exists":true,"errno":0,"ino":9,"mode":33188,"uid":0,"gid":0,"nlink":1,"size":280000,"mtime":1,"version":4}}`, true},
		{"truncate", "trunc-file", head("truncate", "trunc-file") + `,"stat":` + file + `}`, false},
		{"mksnap", "snapshot", head("mksnap", "snapshot") + `,"stat":` + dir + `}`, true},
		{"rmsnap", "snapshot", head("rmsnap", "snapshot") + `,"stat":` + absent + `}`, true},
		{"rmsnap", "snapshot", head("rmsnap", "snapshot") + `,"stat":` + dir + `}`, false},
		{"list", "dir", head("list", "dir") + `,"entries":[{"name":"go-file","d_ino":5,"stat":` + file + `}]}`, true},
		{"list", "dir", head("list", "dir") + `,"entries":[{"name":"other","d_ino":5,"stat":` + file + `}]}`, false},
		{"objects", "snap-file", head("objects", "snap-file") + `,"stat":` + file + `,"objects":` + objects("10000000001", 6, true) + `}`, true},
		{"objects", "snap-file", head("objects", "snap-file") + `,"stat":` + file + `,"objects":` + objects("10000000001", 5, true) + `}`, false},
		{"objects", "go-file", head("objects", "go-file") + `,"stat":` + file + `,"objects":` + objects("10000000001", 6, true) + `}`, false},
		{"objects", "go-file", head("objects", "go-file") + `,"stat":` + file + `,"objects":` + objects("10000000001", 6, false) + `}`, true},
		{"objects", "go-file", head("objects", "go-file") + `,"stat":` + absent + `,"objects":[]}`, true},
		{"stat", "dir", head("stat", "dir") + `,"stat":` + dir + `,"layout":"","layout_absent":true,"extra":1}`, false},
		{"stat", "dir", `{"action":"stat","path":"/other","native_global_id":7,"stat":` + dir + `}`, false},
	} {
		_, err := decodeMDSCephFSDataNative(tc.action, tc.target, path(tc.target), pool, plan, []byte(tc.data))
		if (err == nil) != tc.ok {
			t.Fatal("native data output shape decision differs", tc.action, tc.target, tc.data, err)
		}
	}
}

func itoa(v int) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestMDSCephFSDataMutationsAreOneShotAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	x := &mdsCephFSDataScope{out: out, plan: mdsCephFSDataPlanFor(token), owned: map[string]uint64{}, dataPool: 7, dataPoolName: "owned-data"}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSData: x}
	var result mdsCephFSDataResult
	// Ordered mutations are refused before any native work without their
	// prerequisite (the cluster is absent, so native work would panic).
	for _, name := range []string{"layout-clear", "write-native", "write-trunc", "truncate", "snapshot-create", "snapshot-remove", "capsnap-create-a", "capsnap-create-b", "capsnap-remove-a", "capsnap-remove-b"} {
		if err := c.mdsCephFSDataAct(context.Background(), 1, mdsCephFSDataAction{name: name, target: "dir"}, &result); err == nil {
			t.Fatal("mutation admitted without its prerequisite", name)
		}
	}
	x.mutations = []*mdsCephFSMutation{{Kind: "layout-set", State: "applied"}, {Kind: "write-trunc", State: "uncertain"}}
	for _, name := range []string{"layout-set", "write-trunc", "truncate"} {
		if err := c.mdsCephFSDataAct(context.Background(), 2, mdsCephFSDataAction{name: name, target: "trunc-file"}, &result); err == nil {
			t.Fatal("a repeated or unordered mutation was admitted", name)
		}
	}
	x.mutations = append(x.mutations, &mdsCephFSMutation{Kind: "capsnap-create-a", State: "uncertain"})
	for _, name := range []string{"capsnap-create-a", "capsnap-create-b", "capsnap-remove-a"} {
		if err := c.mdsCephFSDataAct(context.Background(), 3, mdsCephFSDataAction{name: name, target: "capsnap-a"}, &result); err == nil {
			t.Fatal("uncertain cap snapshot permitted replay or dependent mutation", name)
		}
	}
	for kind, requires := range mdsCephFSDataMutations {
		if requires != "" && mdsCephFSDataMutations[requires] == kind {
			t.Fatal("cyclic mutation prerequisites", kind, requires)
		}
	}
	if len(x.extra("objects", "snap-file")) != 2 || x.extra("objects", "snap-file")[1] != "snaps" || x.extra("objects", "go-file")[1] != "plain" ||
		x.extra("write-native", "native-file")[2] != "0:80000,600000:120000,1600000:20000" || x.extra("truncate", "trunc-file")[0] != "280000" ||
		!slices.Equal(x.extra("layout-set", "dir"), []string{"65536", "131072", "2", "7"}) || x.extra("stat", "dir") != nil {
		t.Fatal("native action inputs differ from the plan")
	}
}

func TestMDSCephFSDataControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-cephfs-data"), []byte("1 "+strings.Repeat("a1", 16)+" layout-set\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled, mdsCephFSData: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS CephFS data control scope") {
			t.Fatal("unowned data action reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned data action produced an acknowledgement or receipt", entries)
		}
	}
}

func TestMDSCephFSDataCleanupWithoutBaselineMutatesNothing(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, intent := range []bool{false, true} {
		out := t.TempDir()
		c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSData: &mdsCephFSDataScope{out: out, plan: mdsCephFSDataPlanFor(token), goIntent: intent}}
		err := c.finishMDSCephFSData(context.Background())
		var receipt mdsCephFSDataCleanup
		data, readErr := os.ReadFile(filepath.Join(out, "mds-cephfs-data-cleanup.json"))
		if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
			t.Fatal("cleanup receipt was not retained", readErr)
		}
		if intent {
			if err == nil || receipt.Completed || receipt.ClientIdentityRemoved {
				t.Fatal("unknown identity creation was reported as removed", err, receipt)
			}
			continue
		}
		if err != nil || !receipt.Completed || !receipt.NamespaceRestored || !receipt.LayoutRestored || !receipt.BlocklistRestored || len(receipt.NewBlocklist) != 0 {
			t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
		}
	}
	if (&fixtureCluster{}).finishMDSCephFSData(context.Background()) != nil {
		t.Fatal("cleanup without a data scope must be a no-op")
	}
}

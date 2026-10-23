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

func mdsCephFSCompletionProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSCephFSCompletionClosedProfileAndCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCephFSCompletionProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCephFSCompletionIntegration$" || p.mdsCephFSCompletion != "1" || p.mdsCephFS != "0" || p.mdsCephFSRanks != "0" ||
			p.mdsSessions != "0" || p.mdsCommands != "0" || p.osdPGV8() || !p.mdsFreshOSDAllowed() || !p.mdsPublicClient() || p.cleanupBudget() != 3*time.Minute {
			t.Fatal("MDS CephFS completion did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		if active, standby := p.mdsFreshTopology(); active != 1 || standby != 0 {
			t.Fatal("completion profile topology differs", active, standby)
		}
		p.mdsFreshIdentity, p.mdsCephFSCompletionIdentity = "client.readonly-owned", "client.completion-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"] != p.mdsCephFSCompletionIdentity || x["CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE"] != filepath.Join(dir, "mds-cephfs-completion.key") ||
				x["CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION"] != "1" || x["CEPH_MSGR_MDS_CEPHFS_IDENTITY"] != "" ||
				x["CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY"] != "" || x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != "" || x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] != "" {
				t.Fatal("MDS CephFS completion credentials were mixed or lost", x)
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION": "yes"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "1"}, {"CEPH_MSGR_TEST_MDS_SESSIONS": "1"},
		{"CEPH_MSGR_TEST_MDS_CEPHFS": "1"}, {"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS": "1"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Push|Recovery)?Integration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Completion|Recovery)Integration$"},
		{"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}, {"CEPH_MSGR_TEST_RBD": "1"},
	} {
		env := mdsCephFSCompletionProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS CephFS completion profile accepted", change, p, err)
		}
	}
}

func TestMDSCephFSCompletionIdentityCapsAndPlan(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	goCaps, nativeCaps := mdsCephFSCompletionGoCaps(name), mdsCephFSCompletionNativeCaps(name)
	if goCaps.Mon != "allow r" || goCaps.MDS != "allow rw fsname="+name || goCaps.OSD != "allow r tag cephfs data="+name || goCaps.MGR != "" {
		t.Fatal("the Go identity must hold MON read, a filesystem-bound MDS grant and an OSD read grant on its data pool only", goCaps)
	}
	if nativeCaps.Mon != "allow r" || nativeCaps.MDS != "allow rws fsname="+name || nativeCaps.OSD != "allow rw tag cephfs data="+name || nativeCaps.MGR != "" {
		t.Fatal("the native identity caps differ", nativeCaps)
	}
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	if plan.Directory != "ceph-msgr-completion-"+token || plan.Snapshot != "completion-a1a1a1a1" || len(plan.FencedPayloadHex) != 10000 ||
		plan.ReconnectTimeout != "5" || !slices.Equal(plan.Children, mdsCephFSCompletionChildren) || !slices.Equal(plan.Grandchildren, mdsCephFSCompletionGrandchildren) {
		t.Fatal("completion plan differs", plan)
	}
	if other := mdsCephFSCompletionPlanFor(strings.Repeat("b2", 16)); other.FencedPayloadHex == plan.FencedPayloadHex {
		t.Fatal("the fenced payload is not token-bound")
	}
	for target, want := range map[string]string{
		"dir": "/ceph-msgr-completion-" + token, "lost-dir": "/ceph-msgr-completion-" + token + "/lost-dir", "lease/leased": "/ceph-msgr-completion-" + token + "/lease/leased",
		"snapshot": "/ceph-msgr-completion-" + token + "/snap/.snap/completion-a1a1a1a1",
	} {
		if got, err := plan.path(target); err != nil || got != want {
			t.Fatal("completion path differs", target, got, err)
		}
	}
	for _, target := range []string{"", "..", "lease/../..", "/etc", "snap/.snap", "missing", "lease/other"} {
		if _, err := plan.path(target); err == nil {
			t.Fatal("completion path admitted", target)
		}
	}
	bad := plan
	bad.Directory = "ceph-msgr-cephfs-" + token
	if _, err := bad.path("dir"); err == nil {
		t.Fatal("a directory outside the completion prefix was admitted")
	}
	if len(plan.targets()) != 1+len(plan.Children)+len(plan.SplitFiles)+len(plan.Grandchildren)+1 {
		t.Fatal("completion targets differ", plan.targets())
	}
}

func TestMDSCephFSCompletionRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	parse := func(previous int64, fields ...string) (int64, mdsCephFSCompletionAction, error) {
		return parseMDSCephFSCompletionRequest(fields, token, plan, previous)
	}
	for _, tc := range []struct {
		fields []string
		want   mdsCephFSCompletionAction
	}{
		{[]string{"1", token, "observe", "4210"}, mdsCephFSCompletionAction{name: "observe", gid: 4210}},
		{[]string{"2", token, "mds-log", "4210", "123456"}, mdsCephFSCompletionAction{name: "mds-log", gid: 4210, nonce: 123456}},
		{[]string{"3", token, "stat", "lease/leased"}, mdsCephFSCompletionAction{name: "stat", target: "lease/leased"}},
		{[]string{"4", token, "list"}, mdsCephFSCompletionAction{name: "list", target: "dir"}},
		{[]string{"5", token, "write-fenced"}, mdsCephFSCompletionAction{name: "write-fenced", target: "fenced"}},
		{[]string{"6", token, "snapshot-create"}, mdsCephFSCompletionAction{name: "snapshot-create", target: "snapshot"}},
		{[]string{"7", token, "blackhole-on"}, mdsCephFSCompletionAction{name: "blackhole-on"}},
		{[]string{"8", token, "restart-adopt"}, mdsCephFSCompletionAction{name: "restart-adopt"}},
	} {
		id, action, err := parse(0, tc.fields...)
		if err != nil || action != tc.want || id <= 0 {
			t.Fatal("closed completion request differs", tc.fields, action, err)
		}
	}
	for _, fields := range [][]string{
		{"1", token, "observe"}, {"1", token, "observe", "0"}, {"1", token, "observe", "042"}, {"1", token, "mds-log", "42"}, {"1", token, "mds-log", "42", "0"},
		{"1", token, "stat", "/etc/passwd"}, {"1", token, "stat", "dir", "x"}, {"1", token, "list", "dir"}, {"1", token, "blackhole-on", "true"}, {"1", token, "config-set", "x"},
		{"1", token, "tell", "mds.a", "config"}, {"1", strings.Repeat("b2", 16), "list"}, {"0", token, "list"}, {"01", token, "list"}, {"1", token},
	} {
		if _, _, err := parse(0, fields...); err == nil {
			t.Fatal("unsafe completion request admitted", fields)
		}
	}
	if _, _, err := parse(5, "5", token, "list"); err == nil {
		t.Fatal("a replayed completion request identity was admitted")
	}
}

func TestMDSCephFSCompletionNativeOutputShapes(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	root, _ := plan.path("dir")
	snapshot, _ := plan.path("snapshot")
	fenced, _ := plan.path("fenced")
	stat := `{"exists":true,"errno":0,"ino":1099511627776,"mode":16877,"uid":0,"gid":0,"nlink":2,"size":0}`
	absent := `{"exists":false,"errno":2,"ino":0,"mode":0,"uid":0,"gid":0,"nlink":0,"size":0}`
	for _, tc := range []struct {
		action, path, data string
		ok                 bool
	}{
		{"stat", root, `{"action":"stat","path":"` + root + `","native_global_id":7,"stat":` + stat + `}`, true},
		{"stat", root, `{"action":"stat","path":"` + root + `","native_global_id":7,"stat":` + absent + `}`, true},
		{"list", root, `{"action":"list","path":"` + root + `","native_global_id":7,"entries":[{"name":"lost-dir","d_ino":5,"stat":` + stat + `}]}`, true},
		{"list", root, `{"action":"list","path":"` + root + `","native_global_id":7,"entries":[{"name":"other","d_ino":5,"stat":` + stat + `}]}`, false},
		{"list", root, `{"action":"list","path":"` + root + `","native_global_id":7,"entries":[{"name":"lease","d_ino":5,"stat":` + stat + `},{"name":"lease","d_ino":5,"stat":` + stat + `}]}`, false},
		{"create-write", fenced, `{"action":"create-write","path":"` + fenced + `","native_global_id":7,"written":5000,"stat":{"exists":true,"errno":0,"ino":9,"mode":33188,"uid":0,"gid":0,"nlink":1,"size":5000}}`, true},
		{"create-write", fenced, `{"action":"create-write","path":"` + fenced + `","native_global_id":7,"written":4999,"stat":{"exists":true,"errno":0,"ino":9,"mode":33188,"uid":0,"gid":0,"nlink":1,"size":4999}}`, false},
		{"mksnap", snapshot, `{"action":"mksnap","path":"` + snapshot + `","native_global_id":7,"stat":` + stat + `}`, true},
		{"mksnap", snapshot, `{"action":"mksnap","path":"` + snapshot + `","native_global_id":7,"stat":` + absent + `}`, false},
		{"rmsnap", snapshot, `{"action":"rmsnap","path":"` + snapshot + `","native_global_id":7,"stat":` + absent + `}`, true},
		{"stat", root, `{"action":"stat","path":"` + root + `","native_global_id":7,"stat":` + stat + `,"extra":1}`, false},
		{"stat", root, `{"action":"stat","path":"/other","native_global_id":7,"stat":` + stat + `}`, false},
	} {
		_, err := decodeMDSCephFSCompletionNative(tc.action, tc.path, plan, []byte(tc.data))
		if (err == nil) != tc.ok {
			t.Fatal("native completion output shape decision differs", tc.action, tc.data, err)
		}
	}
}

func TestMDSCephFSCompletionDaemonReadbacks(t *testing.T) {
	status, err := parseMDSCephFSCompletionDaemonStatus([]byte(`{"cluster_fsid":"x","whoami":0,"id":4650,"want_state":"up:reconnect","state":"up:reconnect","fs_name":"owned","rank_uptime":1.5,"mdsmap_epoch":12,"osdmap_epoch":40,"osdmap_epoch_barrier":40,"uptime":3}`))
	if err != nil || status != (mdsCephFSCompletionDaemonStatus{GID: 4650, Rank: 0, State: "up:reconnect", MDSMapEpoch: 12, FSName: "owned"}) {
		t.Fatal("daemon status differs", status, err)
	}
	for _, bad := range []string{`{}`, `{"id":0,"whoami":0,"state":"up:active","mdsmap_epoch":1}`, `{"id":5,"whoami":0,"state":"boot","mdsmap_epoch":1}`, `{"id":5,"state":"up:active","mdsmap_epoch":1}`, `x`} {
		if _, err := parseMDSCephFSCompletionDaemonStatus([]byte(bad)); err == nil {
			t.Fatal("malformed daemon status accepted", bad)
		}
	}
	if v, err := parseMDSCephFSCompletionConfig([]byte(`{"debug_mds":"1/5"}`), "debug_mds"); err != nil || v != "1/5" {
		t.Fatal("config readback differs", v, err)
	}
	for _, bad := range []string{`{"debug_ms":"1/5"}`, `{"debug_mds":""}`, `{"debug_mds":"1/5","x":"y"}`, `[]`} {
		if _, err := parseMDSCephFSCompletionConfig([]byte(bad), "debug_mds"); err == nil {
			t.Fatal("malformed config readback accepted", bad)
		}
	}
	listing := []byte(`[{"id":4210,"state":"open","num_completed_requests":2,"requests_in_flight":0,"reconnecting":false,"completed_requests":[{"tid":2,"created_ino":"0x10000000002"},{"tid":4,"created_ino":"0x10000000004"}]},
	 {"id":4211,"state":"open","num_completed_requests":0,"completed_requests":[]}]`)
	records, err := parseMDSCephFSCompletionSessions(listing, []uint64{4210})
	if err != nil || len(records) != 1 || records[0].GlobalID != 4210 || records[0].NumCompleted != 2 || records[0].Requests[1] != (mdsCephFSCompletionRequest{TID: 4, CreatedIno: "0x10000000004"}) {
		t.Fatal("completed-request record differs", records, err)
	}
	for _, bad := range []string{`[{"id":4210,"state":"open","num_completed_requests":3,"completed_requests":[{"tid":2,"created_ino":"0x1"}]}]`,
		`[{"id":4210,"state":"open","num_completed_requests":1,"completed_requests":[{"tid":0,"created_ino":"0x1"}]}]`,
		`[{"id":4210,"state":"open","completed_requests":[]}]`, `{}`} {
		if _, err := parseMDSCephFSCompletionSessions([]byte(bad), []uint64{4210}); err == nil {
			t.Fatal("malformed completed-request record accepted", bad)
		}
	}
}

func TestMDSCephFSCompletionLogFilterKeepsOneClient(t *testing.T) {
	lines := []string{
		"2026-10-15T22:10:39.397+0000 ffff8f42cc80  0 send_msgclient blackhole client_reply(???:2 = 0 (0) Success safe)",
		"2026-10-15T22:10:45.777+0000 ffff9cd8bc80  5 mds.0.server already completed client.5019:2",
		"2026-10-15T22:10:45.777+0000 ffff9cd8bc80  5 mds.0.server already completed client.50199:2",
		"2026-10-15T22:10:45.777+0000 ffff9cd8bc80 10 mds.0.server  completed request which created new inode 0x10000000004, convert it to lookup request",
		"2026-10-15T22:10:45.778+0000 ffff9cd8bc80  1 -- [v2:172.19.0.10:6800/1] --> v2:172.19.0.9:0/3469889763 -- client_reply(???:4 = 0 (0) Success safe) -- 0x1 con 0x2",
		"2026-10-15T22:10:45.778+0000 ffff9cd8bc80  1 -- [v2:172.19.0.10:6800/1] --> v2:172.19.0.9:0/346988976 -- client_reply(???:4 = 0 (0) Success safe) -- 0x1 con 0x2",
		"2026-10-15T22:10:45.779+0000 ffff9cd8bc80 10 mds.0.locker handle_client_lease client.5019 renew on [dentry #0x1/x]",
		"2026-10-15T22:10:45.779+0000 ffff9cd8bc80 10 mds.0.cache unrelated line",
		"2026-10-15T22:10:45.779+0000 ffff9cd8bc80  0 send_msgosd blackhole osd_op(...)",
	}
	kept, dropped := filterMDSCephFSCompletionLog(lines, 5019, 3469889763)
	want := []string{lines[0], lines[1], lines[3], lines[4], lines[6]}
	if !slices.Equal(kept, want) || dropped != 0 {
		t.Fatal("daemon log filter differs", kept, dropped)
	}
	many := make([]string, mdsCephFSCompletionLogLines+3)
	for i := range many {
		many[i] = "x already completed client.1:1 " + strings.Repeat("y", mdsCephFSCompletionLogLineBytes)
	}
	kept, dropped = filterMDSCephFSCompletionLog(many, 1, 1)
	if len(kept) != mdsCephFSCompletionLogLines || dropped != 3 || len(kept[0]) != mdsCephFSCompletionLogLineBytes {
		t.Fatal("daemon log filter bounds differ", len(kept), dropped)
	}
}

func TestMDSCephFSCompletionRuntimeAndRestartRules(t *testing.T) {
	for kind, spec := range mdsCephFSCompletionRuntimeKinds {
		if mdsCephFSCompletionRestoreOf(spec.restore) != kind || len(spec.settings) == 0 {
			t.Fatal("runtime restore mapping differs", kind)
		}
		for name := range spec.settings {
			if !slices.Contains(mdsCephFSCompletionConfigNames, name) {
				t.Fatal("runtime setting is not read back", name)
			}
		}
	}
	if mdsCephFSCompletionRestoreOf("blackhole-on") != "" || mdsCephFSCompletionRestoreOf("config-set") != "" {
		t.Fatal("a change action was taken for a restore")
	}
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	x := &mdsCephFSCompletionScope{out: out, plan: mdsCephFSCompletionPlanFor(token), owned: map[string]uint64{}, current: mdsFreshDaemon{Name: "owned-0", GID: 40}}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSCompletion: x}
	var result mdsCephFSCompletionResult
	for _, name := range []string{"restart-fire", "restart-status", "restart-adopt", "restart-unknown"} {
		if err := c.mdsCephFSCompletionRestartAct(context.Background(), 5, mdsCephFSCompletionAction{name: name}, &result); err == nil {
			t.Fatal("restart step admitted without an armed restart", name)
		}
	}
	x.restarts = []*mdsCephFSRestart{{State: "armed", Armed: mdsFreshDaemon{GID: 40}}}
	if x.pending() != nil {
		t.Fatal("an armed restart must not block ordinary actions")
	}
	if err := c.mdsCephFSCompletionRestartAct(context.Background(), 6, mdsCephFSCompletionAction{name: "restart-arm"}, &result); err == nil {
		t.Fatal("a second restart was armed before the first was adopted")
	}
	x.restarts[0].State = "applied"
	if x.pending() == nil {
		t.Fatal("a fired restart must be pending")
	}
	// Runtime changes other than debug levels are refused during a restart,
	// before any admin socket command.
	for _, name := range []string{"log-pause", "blackhole-on", "early-reply-off"} {
		if err := c.mdsCephFSCompletionDaemonAct(context.Background(), 7, mdsCephFSCompletionAction{name: name}, 41, &result); err == nil {
			t.Fatal("runtime change admitted during a restart", name)
		}
	}
	x.restarts[0].State = "adopted"
	x.restarts = append(x.restarts, &mdsCephFSRestart{State: "adopted"}, &mdsCephFSRestart{State: "adopted"}, &mdsCephFSRestart{State: "adopted"})
	if err := c.mdsCephFSCompletionRestartAct(context.Background(), 8, mdsCephFSCompletionAction{name: "restart-arm"}, &result); err == nil {
		t.Fatal("the restart budget was exceeded")
	}
	// Restores need an owned change in effect on that incarnation.
	for _, name := range []string{"log-resume", "blackhole-off", "early-reply-on", "debug-off"} {
		if err := c.mdsCephFSCompletionDaemonAct(context.Background(), 9, mdsCephFSCompletionAction{name: name}, 40, &result); err == nil {
			t.Fatal("restore admitted without an owned change", name)
		}
	}
	x.runtime = []*mdsCephFSCompletionRuntime{{Kind: "blackhole-on", IncarnationGID: 40, State: "applied"}, {Kind: "log-pause", IncarnationGID: 39, State: "applied"}}
	if err := c.mdsCephFSCompletionDaemonAct(context.Background(), 10, mdsCephFSCompletionAction{name: "blackhole-on"}, 40, &result); err == nil {
		t.Fatal("a runtime change was applied twice")
	}
	if err := x.quiesced(40); err == nil || x.quiesced(39) == nil || x.quiesced(41) != nil {
		t.Fatal("native-work refusal does not follow the incarnation's blackhole or journal pause")
	}
	if err := c.mdsCephFSCompletionNative(context.Background(), 11, mdsCephFSCompletionAction{name: "stat", target: "dir"}, &result); err == nil {
		t.Fatal("native work admitted while client messages are blackholed")
	}
	for _, name := range []string{"deny-off", "window-off"} {
		if err := c.mdsCephFSCompletionCentralAct(context.Background(), 12, name, &result); err == nil {
			t.Fatal("central restore admitted without an owned override", name)
		}
	}
	x.central = []*mdsCephFSCompletionCentral{{Kind: "deny", State: "applied"}}
	if err := c.mdsCephFSCompletionCentralAct(context.Background(), 13, "window-on", &result); err == nil {
		t.Fatal("a second central override was applied before the first was restored")
	}
	if err := c.mdsCephFSCompletionCentralAct(context.Background(), 14, "deny-on", &result); err == nil {
		t.Fatal("a central override was applied twice")
	}
}

func TestMDSCephFSCompletionBlocklistOwnership(t *testing.T) {
	x := &mdsCephFSCompletionScope{owned: map[string]uint64{"172.19.0.9:0/77": 4200},
		restarts:  []*mdsCephFSRestart{{Armed: mdsFreshDaemon{GID: 10}, ArmedAddresses: []string{"172.19.0.10:6800/66"}}, {Armed: mdsFreshDaemon{GID: 11}, ArmedAddresses: []string{"172.19.0.10:6800/67"}}},
		mutations: []*mdsCephFSMutation{{Kind: "eviction", Address: "172.19.0.9:0/55", Until: "2026-10-17T00:00:00Z"}}}
	gone := func(gid uint64) bool { return gid == 10 }
	for _, tc := range []struct {
		entry mdsSessionsBlock
		kind  string
		err   bool
	}{
		{mdsSessionsBlock{Address: "172.19.0.9:0/55", Until: "2026-10-17T00:00:00Z"}, "eviction", false},
		{mdsSessionsBlock{Address: "172.19.0.9:0/55", Until: "2026-10-18T00:00:00Z"}, "", true},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/66"}, "mds-restart", false},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/67"}, "", true},
		{mdsSessionsBlock{Address: "172.19.0.9:0/77"}, "owned-session", false},
		{mdsSessionsBlock{Address: "172.19.0.9:0/78"}, "", false},
		{mdsSessionsBlock{Address: "172.19.0.0/24", Range: true}, "", false},
	} {
		kind, err := x.blockOwner(tc.entry, gone)
		if kind != tc.kind || (err != nil) != tc.err {
			t.Fatal("blocklist attribution differs", tc.entry, kind, err)
		}
	}
}

func TestMDSCephFSCompletionControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-cephfs-completion"), []byte("1 "+strings.Repeat("a1", 16)+" blackhole-on\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled, mdsCephFSCompletion: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS CephFS completion control scope") {
			t.Fatal("unowned completion action reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned completion action produced an acknowledgement or receipt", entries)
		}
	}
}

func TestMDSCephFSCompletionCleanupWithoutBaselineMutatesNothing(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, intent := range []bool{false, true} {
		out := t.TempDir()
		c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSCompletion: &mdsCephFSCompletionScope{out: out, plan: mdsCephFSCompletionPlanFor(token), goIntent: intent}}
		err := c.finishMDSCephFSCompletion(context.Background())
		var receipt mdsCephFSCompletionCleanup
		data, readErr := os.ReadFile(filepath.Join(out, "mds-cephfs-completion-cleanup.json"))
		if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
			t.Fatal("cleanup receipt was not retained", readErr)
		}
		if intent {
			if err == nil || receipt.Completed || receipt.ClientIdentityRemoved {
				t.Fatal("unknown identity creation was reported as removed", err, receipt)
			}
			continue
		}
		if err != nil || !receipt.Completed || !receipt.ConfigRestored || !receipt.NamespaceRestored || !receipt.BlocklistRestored || len(receipt.NewBlocklist) != 0 {
			t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
		}
	}
	if (&fixtureCluster{}).finishMDSCephFSCompletion(context.Background()) != nil {
		t.Fatal("cleanup without a completion scope must be a no-op")
	}
}

func TestMDSCephFSMultiReconnectOwnedPaths(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := mdsCephFSCompletionPlanFor(token)
	if len(plan.SplitFiles) != 256 {
		t.Fatal("split file bound changed", len(plan.SplitFiles))
	}
	seen := map[string]bool{}
	for _, name := range plan.SplitFiles {
		path, err := plan.path(name)
		if err != nil || path != "/"+plan.Directory+"/"+name || len(name) > 255 || seen[name] {
			t.Fatal("split path is not a unique legal owned file", name, err)
		}
		seen[name] = true
		_, action, err := parseMDSCephFSCompletionRequest([]string{"1", token, "stat", name}, token, plan, 0)
		if err != nil || action.target != name {
			t.Fatal("split stat target refused", name, err)
		}
	}
	for _, name := range []string{"split-256-" + strings.Repeat("s", 215), "../split-00", "split-00/foreign"} {
		if _, err := plan.path(name); err == nil {
			t.Fatal("unplanned split target admitted", name)
		}
	}
}

func TestMDSCephFSMultiReconnectSelectorIsExclusive(t *testing.T) {
	env := mdsCephFSCompletionProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = "^TestCephMDSCephFSMultiReconnectIntegration$"
	p, err := loadProfile(func(n string) string { return env[n] })
	if err != nil || p.testRun != env["CEPH_MSGR_TEST_RUN"] {
		t.Fatal("exclusive split reconnect selector refused", p.testRun, err)
	}
	for _, selector := range []string{"^TestCephMDSCephFS(Completion|MultiReconnect)Integration$", "TestCephMDSCephFSMultiReconnectIntegration", "^TestCephMDSCephFSMultiReconnectIntegrationExtended$"} {
		env["CEPH_MSGR_TEST_RUN"] = selector
		if _, err := loadProfile(func(n string) string { return env[n] }); err == nil {
			t.Fatal("broad or foreign split selector admitted", selector)
		}
	}
}

func TestMDSCephFSReconnectHandlerLogKeepsExactClientAndOrder(t *testing.T) {
	lines := []string{"x handle_client_reconnect client.42 (more)", "x handle_client_reconnect client.420 (more)", "x client.42 client_caps(update)", "x handle_client_reconnect client.42", "x handle_client_reconnect client.42 foreign"}
	want := []string{lines[0], lines[3]}
	if got := filterMDSCephFSReconnectHandlers(lines, 42); !slices.Equal(got, want) {
		t.Fatal("reconnect handlers changed client or order", got)
	}
}

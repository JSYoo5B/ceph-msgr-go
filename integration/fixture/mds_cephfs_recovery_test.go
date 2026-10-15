package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func mdsCephFSRecoveryDump(t *testing.T, fscid int64, name string, pools []int64, info map[string]any, up map[string]uint64, failed []int, standbys []any) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"epoch": 21, "standbys": standbys, "filesystems": []any{map[string]any{"id": fscid, "mdsmap": map[string]any{
		"epoch": 20, "fs_name": name, "max_mds": 1, "metadata_pool": 3, "data_pools": pools, "up": up, "failed": failed, "info": info}}}})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mdsCephFSRecoveryDaemon(name string, gid uint64, rank int, state string, incarnation int, addr string, nonce uint32) map[string]any {
	return map[string]any{"name": name, "gid": gid, "rank": rank, "state": state, "incarnation": incarnation,
		"addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": addr, "nonce": nonce}}}}
}

func TestMDSCephFSRankStatusReadsTransitionalMaps(t *testing.T) {
	oracle := mdsFreshOracle{FSCID: 7, FSName: "owned", MetadataPool: 3, DataPools: []int64{4}}
	owned := func(name string) bool { return name == "owned-0" }
	// The armed daemon is gone and rank 0 is held by its new incarnation.
	replay := mdsCephFSRecoveryDump(t, 7, "owned", []int64{4}, map[string]any{"gid_200": mdsCephFSRecoveryDaemon("owned-0", 200, 0, "up:replay", 9, "172.19.0.10:6800", 77)},
		map[string]uint64{"mds_0": 200}, nil, []any{})
	status, err := parseMDSCephFSRankStatus(replay, oracle, "4", owned, 100)
	if err != nil || status.Rank0 == nil || status.Rank0.GID != 200 || status.Rank0.State != "up:replay" || status.Rank0.Nonce != 77 || status.Rank0.Endpoint != "172.19.0.10:6800" ||
		status.ArmedPresent || status.FSMapEpoch != 21 || status.MDSMapEpoch != 20 || len(status.Failed) != 0 {
		t.Fatal("transitional rank 0 status differs", status, err)
	}
	// A failed rank with the respawned daemon still standby, and the armed
	// GID still listed, is reported as observed.
	failed := mdsCephFSRecoveryDump(t, 7, "owned", []int64{4}, map[string]any{"gid_100": mdsCephFSRecoveryDaemon("owned-0", 100, -1, "up:standby", 6, "172.19.0.10:6800", 66)},
		map[string]uint64{}, []int{0}, []any{mdsCephFSRecoveryDaemon("owned-0", 300, -1, "up:standby", 0, "172.19.0.10:6800", 88), mdsCephFSRecoveryDaemon("foreign", 400, -1, "up:standby", 0, "172.19.0.11:6800", 99)})
	status, err = parseMDSCephFSRankStatus(failed, oracle, "4", owned, 100)
	if err != nil || status.Rank0 != nil || !status.ArmedPresent || len(status.Failed) != 1 || len(status.Standbys) != 1 || status.Standbys[0].GID != 300 {
		t.Fatal("failed-rank status differs", status, err)
	}
	// IPv6 selection ignores the other family.
	dual := mdsCephFSRecoveryDump(t, 7, "owned", []int64{4}, map[string]any{"gid_200": map[string]any{"name": "owned-0", "gid": 200, "rank": 0, "state": "up:reconnect", "incarnation": 9,
		"addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.19.0.10:6800", "nonce": 5}, map[string]any{"type": "v2", "addr": "[fd00::a]:6800", "nonce": 5}}}}},
		map[string]uint64{"mds_0": 200}, nil, []any{})
	if status, err = parseMDSCephFSRankStatus(dual, oracle, "6", owned, 100); err != nil || status.Rank0.Endpoint != "[fd00::a]:6800" {
		t.Fatal("address family selection differs", status, err)
	}
	for _, bad := range [][]byte{
		mdsCephFSRecoveryDump(t, 8, "owned", []int64{4}, map[string]any{}, map[string]uint64{}, []int{0}, []any{}),
		mdsCephFSRecoveryDump(t, 7, "other", []int64{4}, map[string]any{}, map[string]uint64{}, []int{0}, []any{}),
		mdsCephFSRecoveryDump(t, 7, "owned", []int64{4, 5}, map[string]any{}, map[string]uint64{}, []int{0}, []any{}),
		mdsCephFSRecoveryDump(t, 7, "owned", []int64{4}, map[string]any{}, map[string]uint64{"mds_0": 200}, nil, []any{}),
		[]byte("not json"), nil,
	} {
		if _, err := parseMDSCephFSRankStatus(bad, oracle, "4", owned, 100); err == nil {
			t.Fatal("changed filesystem identity or malformed map accepted", string(bad))
		}
	}
}

func TestMDSCephFSRecoveryNativeShapes(t *testing.T) {
	plan := mdsCephFSRecoveryPlanFor(strings.Repeat("a1", 16))
	file, _ := plan.path("fenced")
	dir, _ := plan.path("dir")
	stat := mdsCephFSStat{Exists: true, Ino: 1099511627790, Mode: 0o100644, NLink: 1, Size: mdsCephFSFencedBytes}
	good := []mdsCephFSNative{
		{Action: "create-write", Path: file, NativeGlobalID: 9, Written: mdsCephFSFencedBytes, Stat: &stat},
		{Action: "stat", Path: file, NativeGlobalID: 9, Stat: &stat},
		{Action: "list", Path: dir, NativeGlobalID: 9, Entries: []mdsCephFSEntry{{Name: "held", DIno: 5, Stat: stat}, {Name: "replayed", DIno: 6, Stat: stat}}},
	}
	for _, output := range good {
		if _, err := decodeMDSCephFSRecoveryNative(output.Action, output.Path, plan, output); err != nil {
			t.Fatal("admitted recovery output rejected", output.Action, err)
		}
	}
	short := stat
	short.Size = 4096
	for i, tc := range []struct {
		path   string
		output mdsCephFSNative
	}{
		{file, mdsCephFSNative{Action: "create-write", Path: file, NativeGlobalID: 9, Written: 4096, Stat: &short}},
		{file, mdsCephFSNative{Action: "create-write", Path: file, NativeGlobalID: 9, Written: mdsCephFSFencedBytes, Stat: &short}},
		{file, mdsCephFSNative{Action: "create-write", Path: file, NativeGlobalID: 0, Written: mdsCephFSFencedBytes, Stat: &stat}},
		{dir, mdsCephFSNative{Action: "list", Path: dir, NativeGlobalID: 9, Entries: []mdsCephFSEntry{{Name: "go-file", DIno: 5, Stat: stat}}}},
		{dir, mdsCephFSNative{Action: "list", Path: dir, NativeGlobalID: 9, Entries: []mdsCephFSEntry{{Name: "held", DIno: 5, Stat: stat}, {Name: "held", DIno: 6, Stat: stat}}}},
		{file, mdsCephFSNative{Action: "stat", Path: dir, NativeGlobalID: 9, Stat: &stat}},
		{file, mdsCephFSNative{Action: "write", Path: file, NativeGlobalID: 9, Written: mdsCephFSFencedBytes, Stat: &stat}},
	} {
		if _, err := decodeMDSCephFSRecoveryNative(tc.output.Action, tc.path, plan, tc.output); err == nil {
			t.Fatal("ambiguous recovery output accepted", i, tc.output)
		}
	}
}

func TestMDSCephFSRestartStepsAreOrderedAndOneShot(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"},
		mdsCephFS: &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(token), recovery: mdsCephFSRecoveryPlanFor(token), owned: map[string]uint64{}}}
	var result mdsCephFSResult
	// Without an armed restart nothing can be fired, observed or adopted;
	// these refusals happen before any native command.
	for _, name := range []string{"restart-fire", "restart-status", "restart-adopt", "restart-unknown"} {
		if err := c.mdsCephFSRestartAct(context.Background(), 5, mdsCephFSAction{name: name}, &result); err == nil {
			t.Fatal("restart step admitted without an armed restart", name)
		}
	}
	x := c.mdsCephFS
	x.restart = &mdsCephFSRestart{State: "armed"}
	if x.restart.pending() {
		t.Fatal("an armed restart must not block ordinary actions")
	}
	if err := c.mdsCephFSRestartAct(context.Background(), 6, mdsCephFSAction{name: "restart-arm"}, &result); err == nil {
		t.Fatal("a second restart was armed")
	}
	for _, state := range []string{"intended", "applied", "uncertain"} {
		x.restart.State = state
		if !x.restart.pending() {
			t.Fatal("an unadopted fired restart must block ordinary actions", state)
		}
		if err := c.mdsCephFSRestartAct(context.Background(), 7, mdsCephFSAction{name: "restart-fire"}, &result); err == nil {
			t.Fatal("a fired restart was fired again", state)
		}
	}
	x.restart.State = "adopted"
	for _, name := range []string{"restart-fire", "restart-status", "restart-adopt"} {
		if err := c.mdsCephFSRestartAct(context.Background(), 8, mdsCephFSAction{name: name}, &result); err == nil || x.restart.pending() {
			t.Fatal("an adopted restart admitted another step", name)
		}
	}
}

func TestMDSCephFSBlocklistOwnership(t *testing.T) {
	x := &mdsCephFSScope{owned: map[string]uint64{"172.19.0.9:0/77": 4200}, restart: &mdsCephFSRestart{ArmedAddresses: []string{"172.19.0.10:6800/66"}},
		mutations: []*mdsCephFSMutation{{Kind: "eviction", Address: "172.19.0.9:0/55", Until: "2026-10-17T00:00:00Z"}}}
	for _, tc := range []struct {
		entry     mdsSessionsBlock
		armedGone bool
		kind      string
		err       bool
	}{
		{mdsSessionsBlock{Address: "172.19.0.9:0/55", Until: "2026-10-17T00:00:00Z"}, true, "eviction", false},
		{mdsSessionsBlock{Address: "172.19.0.9:0/55", Until: "2026-10-18T00:00:00Z"}, true, "", true},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/66"}, true, "mds-restart", false},
		{mdsSessionsBlock{Address: "172.19.0.10:6800/66"}, false, "", true},
		{mdsSessionsBlock{Address: "172.19.0.9:0/77"}, false, "owned-session", false},
		{mdsSessionsBlock{Address: "172.19.0.9:0/78"}, true, "", false},
		{mdsSessionsBlock{Address: "172.19.0.0/24", Range: true}, true, "", false},
	} {
		kind, err := x.blockOwner(tc.entry, tc.armedGone)
		if kind != tc.kind || (err != nil) != tc.err {
			t.Fatal("blocklist attribution differs", tc.entry, kind, err)
		}
	}
	if (&mdsCephFSScope{}).restart.pending() {
		t.Fatal("no restart must not block ordinary actions")
	}
}

func TestMDSCephFSHistoricOpsKeepOneClient(t *testing.T) {
	data := []byte(`{"size":20,"duration":600,"ops":[
	 {"description":"client_request(client.4950:10 create owner_uid=0, owner_gid=0 #0x10000000000/replayed 2026-10-16T02:54:11.900000+0900 RETRY=1 REPLAY caller_uid=0, caller_gid=0{})","initiated_at":"t1","age":3.1,"duration":0.2,
	  "type_data":{"result":0,"flag_point":"done","reqid":{"entity":{"type":"client","num":4950},"tid":10},"op_type":"client_request","events":[{"time":"t1","event":"initiated"},{"time":"t2","event":"journal_committed: "}],"locks":null}},
	 {"description":"client_request(client.49501:3 lookup #0x1/x caller_uid=0, caller_gid=0{})","initiated_at":"t1","age":1,"duration":0.1,"type_data":{"result":0,"flag_point":"done","op_type":"client_request","events":[]}},
	 {"description":"internal op exportdir:mds.0:1","initiated_at":"t1","age":1,"duration":0.1,"type_data":{"result":null,"flag_point":"done","op_type":"internal_op","events":[]}}]}`)
	ops, err := parseMDSCephFSHistoricOps(data, 4950)
	if err != nil || len(ops) != 1 || !strings.Contains(ops[0].Description, " REPLAY") || ops[0].Result == nil || *ops[0].Result != 0 || len(ops[0].Events) != 2 || ops[0].Events[1] != "journal_committed: " {
		t.Fatal("historic ops of the owned client differ", ops, err)
	}
	if ops, err := parseMDSCephFSHistoricOps(data, 495); err != nil || len(ops) != 0 {
		t.Fatal("another client's ops were attributed by prefix", ops, err)
	}
	for _, bad := range [][]byte{nil, []byte("[]"), []byte(`{"ops":null}`), []byte("x")} {
		if _, err := parseMDSCephFSHistoricOps(bad, 4950); err == nil {
			t.Fatal("malformed historic ops accepted", string(bad))
		}
	}
	x := &mdsCephFSScope{owned: map[string]uint64{"172.19.0.9:0/77": 4950}}
	c := &fixtureCluster{mdsCephFS: x}
	var result mdsCephFSResult
	if err := c.mdsCephFSHistoricOps(context.Background(), 4951, &result); err == nil {
		t.Fatal("historic ops admitted for an unobserved GlobalID")
	}
}

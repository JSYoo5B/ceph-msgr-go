package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
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
	for _, name := range []string{"restart-fire", "restart-status", "restart-adopt", "restart-pause", "restart-inflight", "restart-resume"} {
		if err := c.mdsCephFSRestartAct(context.Background(), 8, mdsCephFSAction{name: name, gid: 42, tid: 7}, &result); err == nil || x.restart.pending() {
			t.Fatal("an adopted restart admitted another step", name)
		}
	}
}

// The journal fault boundary is refused before any native command unless the
// restart is armed and unfired, and the restart fires only behind an applied
// pause with a held in-flight proof.
func TestMDSCephFSJournalBoundaryIsOrderedAndOneShot(t *testing.T) {
	token := strings.Repeat("a1", 16)
	out := t.TempDir()
	armed := mdsFreshDaemon{Name: "owned-0", GID: 100}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"},
		mdsCephFS: &mdsCephFSScope{out: out, plan: mdsCephFSPlanFor(token), recovery: mdsCephFSRecoveryPlanFor(token), owned: map[string]uint64{"10.0.0.1:0/5": 42}, current: armed}}
	x := c.mdsCephFS
	var result mdsCephFSResult
	refused := func(name string, why string) {
		t.Helper()
		if err := c.mdsCephFSRestartAct(context.Background(), 9, mdsCephFSAction{name: name, gid: 42, tid: 7}, &result); err == nil {
			t.Fatal("journal boundary step admitted", name, why)
		}
	}
	for _, name := range []string{"restart-pause", "restart-inflight", "restart-resume"} {
		refused(name, "without an armed restart")
	}
	x.restart = &mdsCephFSRestart{State: "armed", Armed: armed}
	refused("restart-inflight", "without a pause")
	refused("restart-resume", "without a pause")
	refused("restart-fire", "without a pause")
	x.current.GID = 101
	refused("restart-pause", "on another incarnation")
	x.current.GID = armed.GID
	x.restart.Pause = &mdsCephFSJournalPause{State: "applied"}
	refused("restart-pause", "twice")
	refused("restart-fire", "without an in-flight proof")
	if err := c.mdsCephFSRestartAct(context.Background(), 9, mdsCephFSAction{name: "restart-inflight", gid: 43, tid: 7}, &result); err == nil {
		t.Fatal("in-flight check admitted for an unobserved session")
	}
	x.restart.Boundary = &mdsCephFSBoundaryOp{GlobalID: 42, TID: 7, InFlight: true}
	refused("restart-fire", "with a request that is not held")
	refused("restart-inflight", "twice")
	for _, state := range []string{"resumed", "resume-intended", "resume-uncertain", "discarded"} {
		x.restart.Pause.State = state
		refused("restart-resume", "after "+state)
		refused("restart-fire", "after "+state)
	}
	x.restart.Pause.State = "applied"
	x.restart.State = "applied"
	refused("restart-resume", "after the fire")
}

func TestMDSCephFSBoundaryOpIsHeldOnlyBeforeCommit(t *testing.T) {
	dump := func(ops ...map[string]any) []byte {
		data, err := json.Marshal(map[string]any{"ops": ops, "num_ops": len(ops)})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	op := func(description string, events ...string) map[string]any {
		list := []any{}
		for _, e := range events {
			list = append(list, map[string]any{"time": "t", "event": e})
		}
		return map[string]any{"description": description, "initiated_at": "t", "age": 0.5, "duration": 0.5,
			"type_data": map[string]any{"flag_point": "submit entry: journal_and_reply", "op_type": "client_request", "events": list}}
	}
	held := op("client_request(client.42:7 create #0x10000000001/replayed 2026-11-10 caller_uid=0)", "initiated", "early_replied", "submit entry: journal_and_reply")
	other := op("client_request(client.42:70 create #0x10000000001/x)", "early_replied")
	found, ok, err := mdsCephFSFindBoundaryOp(dump(other, held), 42, 7)
	if err != nil || !ok || found == nil || !strings.HasPrefix(found.Description, "client_request(client.42:7 ") {
		t.Fatal("held boundary request not found", found, ok, err)
	}
	for name, data := range map[string][]byte{
		"absent":    dump(other),
		"committed": dump(op("client_request(client.42:7 create)", "early_replied", "journal_committed: ")),
		"not early": dump(op("client_request(client.42:7 create)", "submit entry: journal_and_reply")),
		"foreign":   dump(op("client_request(client.43:7 create)", "early_replied")),
	} {
		if _, ok, err := mdsCephFSFindBoundaryOp(data, 42, 7); err != nil || ok {
			t.Fatal("boundary request reported held", name, err)
		}
	}
	if _, _, err := mdsCephFSFindBoundaryOp(dump(held, held), 42, 7); err == nil {
		t.Fatal("duplicate boundary request accepted")
	}
	if _, _, err := mdsCephFSFindBoundaryOp([]byte("{}"), 42, 7); err == nil {
		t.Fatal("op dump without ops accepted")
	}
}

func TestMDSCephFSScrubStatusShape(t *testing.T) {
	if status, err := parseMDSCephFSScrubStatus([]byte(`{"status":"no active scrubs running","scrubs":{}}`)); err != nil || status != "no active scrubs running" {
		t.Fatal("idle scrub status rejected", status, err)
	}
	for _, bad := range []string{``, `{}`, `{"status":""}`, `{"status":"x"}`, `not json`} {
		if _, err := parseMDSCephFSScrubStatus([]byte(bad)); err == nil {
			t.Fatal("malformed scrub status accepted", bad)
		}
	}
}

// Without a held pause the cleanup step needs no native command.
func TestMDSCephFSJournalPauseCleanupWithoutHeldPause(t *testing.T) {
	for _, restart := range []*mdsCephFSRestart{nil, {State: "armed"}, {State: "armed", Pause: &mdsCephFSJournalPause{State: "resumed"}},
		{State: "adopted", Pause: &mdsCephFSJournalPause{State: "discarded"}}} {
		c := &fixtureCluster{mdsCephFS: &mdsCephFSScope{restart: restart}}
		var receipt mdsCephFSCleanup
		c.finishMDSCephFSJournalPause(context.Background(), &receipt, func(err error) {
			if err != nil {
				t.Fatal(err)
			}
		})
		if !receipt.JournalPauseRestored {
			t.Fatal("a pause that is not held was not reported restored", restart)
		}
	}
	for _, state := range []string{"intended", "applied", "uncertain", "resume-intended", "resume-uncertain"} {
		if !(&mdsCephFSJournalPause{State: state}).held() {
			t.Fatal("a pause that may still hold the journal is not held", state)
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

// mdsCephFSBlocklistRun drives the shared blocklist restoration with fake
// cluster operations: remove fails for failAddress, and the final read returns
// final or readErr. It returns the receipt, every reported error in order and
// every address removal that was attempted.
func mdsCephFSBlocklistRun(entries []mdsSessionsBlock, owner func(mdsSessionsBlock) (string, error), failAddress string, final []mdsSessionsBlock, readErr error) (mdsCephFSBlocklistReceipt, []string, []string) {
	receipt := newMDSCephFSBlocklistReceipt()
	receipt.NewBlocklist = append(receipt.NewBlocklist, entries...)
	var errs, removals []string
	reads := 0
	ops := mdsCephFSBlocklistOps{
		remove: func(_ context.Context, address string) error {
			if reads != 0 {
				return errors.New("removal after the final read")
			}
			removals = append(removals, address)
			if address == failAddress {
				return errors.New("injected removal failure")
			}
			return nil
		},
		read: func(context.Context) ([]mdsSessionsBlock, error) {
			reads++
			return final, readErr
		},
	}
	receipt.restore(context.Background(), "MDS CephFS test", owner, ops, func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	})
	if reads != 1 {
		errs = append(errs, "final blocklist was not read exactly once")
	}
	return receipt, errs, removals
}

// TestMDSCephFSBlocklistRestoreKeepsScenarioOwnership runs the shared removal
// and final verification under each scenario's own ownership policy: removal
// is attempted only for attributed entries, unknown entries and the fencing
// entry of a still-mapped incarnation are kept, and a failed removal, a final
// read failure and a removed entry still present are reported in order.
func TestMDSCephFSBlocklistRestoreKeepsScenarioOwnership(t *testing.T) {
	const (
		evicted  = "172.19.0.9:0/55"
		goneMDS  = "172.19.0.10:6800/66"
		liveMDS  = "172.19.0.10:6800/67"
		session  = "172.19.0.9:0/77"
		unknown  = "172.19.0.9:0/78"
		foreign  = "172.19.0.11:0/90"
		kept     = "MDS CephFS test acceptance kept new blocklist entries it cannot attribute to an owned mutation"
		remained = "owned blocklist entry remained after removal"
		fencing  = "armed MDS incarnation is still mapped; its fencing entry is kept"
	)
	until := "2026-10-17T00:00:00Z"
	rangeEntry := mdsSessionsBlock{Address: "172.19.0.0/24", Range: true}
	completion := &mdsCephFSCompletionScope{owned: map[string]uint64{session: 4200},
		restarts:  []*mdsCephFSRestart{{Armed: mdsFreshDaemon{GID: 10}, ArmedAddresses: []string{goneMDS}}, {Armed: mdsFreshDaemon{GID: 11}, ArmedAddresses: []string{liveMDS}}},
		mutations: []*mdsCephFSMutation{{Kind: "eviction", Address: evicted, Until: until}}}
	completionOwner := func(entry mdsSessionsBlock) (string, error) {
		return completion.blockOwner(entry, func(gid uint64) bool { return gid == 10 })
	}
	ranks := &mdsCephFSRanksScope{owned: map[string]uint64{session: 4200}, takeover: &mdsCephFSRanksTakeover{ArmedAddresses: []string{goneMDS}}}
	ranksOwner := func(armedGone bool) func(mdsSessionsBlock) (string, error) {
		return func(entry mdsSessionsBlock) (string, error) { return ranks.blockOwner(entry, armedGone) }
	}
	recovery := &mdsCephFSScope{owned: map[string]uint64{session: 4200}, restart: &mdsCephFSRestart{ArmedAddresses: []string{goneMDS}},
		mutations: []*mdsCephFSMutation{{Kind: "eviction", Address: evicted, Until: until}}}
	recoveryOwner := func(entry mdsSessionsBlock) (string, error) { return recovery.blockOwner(entry, true) }
	block := func(address string) mdsSessionsBlock { return mdsSessionsBlock{Address: address} }
	removed := func(kind, address string) mdsCephFSRemovedBlock {
		return mdsCephFSRemovedBlock{Kind: kind, Entry: block(address)}
	}
	readFailure := errors.New("injected final read failure")
	for _, tc := range []struct {
		name        string
		owner       func(mdsSessionsBlock) (string, error)
		entries     []mdsSessionsBlock
		failAddress string
		final       []mdsSessionsBlock
		readErr     error
		removals    []string
		removed     []mdsCephFSRemovedBlock
		kept        []mdsSessionsBlock
		errs        []string
		restored    bool
	}{
		{name: "completion owned entries", owner: completionOwner,
			entries:  []mdsSessionsBlock{{Address: evicted, Until: until}, block(goneMDS), block(session)},
			final:    []mdsSessionsBlock{block(foreign)},
			removals: []string{evicted, goneMDS, session},
			removed:  []mdsCephFSRemovedBlock{{Kind: "eviction", Entry: mdsSessionsBlock{Address: evicted, Until: until}}, removed("mds-restart", goneMDS), removed("owned-session", session)},
			kept:     []mdsSessionsBlock{}, restored: true},
		{name: "completion keeps unknown and live fencing", owner: completionOwner,
			entries:     []mdsSessionsBlock{block(liveMDS), block(unknown), rangeEntry, block(goneMDS), block(session)},
			failAddress: session, final: []mdsSessionsBlock{block(liveMDS), block(unknown), rangeEntry, block(goneMDS), block(session)},
			removals: []string{goneMDS, session},
			removed:  []mdsCephFSRemovedBlock{removed("mds-restart", goneMDS)},
			kept:     []mdsSessionsBlock{block(liveMDS), block(unknown), rangeEntry},
			errs:     []string{"an " + fencing, "injected removal failure", kept, remained}},
		{name: "completion changed eviction expiry", owner: completionOwner,
			entries: []mdsSessionsBlock{{Address: evicted, Until: "2026-10-18T00:00:00Z"}},
			kept:    []mdsSessionsBlock{{Address: evicted, Until: "2026-10-18T00:00:00Z"}}, removed: []mdsCephFSRemovedBlock{},
			errs: []string{"owned eviction blocklist entry changed outside the fixture; refusing removal", kept}},
		{name: "ranks takeover after the incarnation left", owner: ranksOwner(true),
			entries:  []mdsSessionsBlock{block(goneMDS), block(session)},
			removals: []string{goneMDS, session},
			removed:  []mdsCephFSRemovedBlock{removed("mds-takeover", goneMDS), removed("owned-session", session)},
			kept:     []mdsSessionsBlock{}, restored: true},
		{name: "ranks keeps mapped incarnation and unknown entries", owner: ranksOwner(false),
			entries:  []mdsSessionsBlock{block(goneMDS), block(unknown), block(evicted), rangeEntry, block(session)},
			final:    []mdsSessionsBlock{block(goneMDS), block(unknown), block(evicted), rangeEntry, block(session)},
			removals: []string{session},
			removed:  []mdsCephFSRemovedBlock{removed("owned-session", session)},
			kept:     []mdsSessionsBlock{block(goneMDS), block(unknown), block(evicted), rangeEntry},
			errs:     []string{fencing, kept, remained}},
		{name: "ranks failed removal and final read failure", owner: ranksOwner(true),
			entries: []mdsSessionsBlock{block(goneMDS), block(session)}, failAddress: goneMDS, readErr: readFailure,
			removals: []string{goneMDS, session},
			removed:  []mdsCephFSRemovedBlock{removed("owned-session", session)},
			kept:     []mdsSessionsBlock{},
			errs:     []string{"injected removal failure", readFailure.Error()}},
		{name: "ranks final read failure alone", owner: ranksOwner(true),
			entries: []mdsSessionsBlock{block(session)}, readErr: readFailure,
			removals: []string{session},
			removed:  []mdsCephFSRemovedBlock{removed("owned-session", session)},
			kept:     []mdsSessionsBlock{},
			errs:     []string{readFailure.Error()}},
		{name: "recovery keeps unknown and verifies removal", owner: recoveryOwner,
			entries:  []mdsSessionsBlock{{Address: evicted, Until: until}, block(goneMDS), block(unknown)},
			final:    []mdsSessionsBlock{block(goneMDS), block(unknown)},
			removals: []string{evicted, goneMDS},
			removed:  []mdsCephFSRemovedBlock{{Kind: "eviction", Entry: mdsSessionsBlock{Address: evicted, Until: until}}, removed("mds-restart", goneMDS)},
			kept:     []mdsSessionsBlock{block(unknown)},
			errs:     []string{kept, remained}},
	} {
		receipt, errs, removals := mdsCephFSBlocklistRun(tc.entries, tc.owner, tc.failAddress, tc.final, tc.readErr)
		if !slices.Equal(removals, tc.removals) {
			t.Fatal(tc.name, "removal authority differs", removals)
		}
		if !reflect.DeepEqual(receipt.RemovedBlocklist, tc.removed) || !reflect.DeepEqual(receipt.KeptBlocklist, tc.kept) {
			t.Fatal(tc.name, "removed or kept entries differ", receipt.RemovedBlocklist, receipt.KeptBlocklist)
		}
		if !slices.Equal(errs, tc.errs) || receipt.BlocklistRestored != tc.restored {
			t.Fatal(tc.name, "reported errors or restoration differ", errs, receipt.BlocklistRestored)
		}
		if !reflect.DeepEqual(receipt.FinalBlocklist, tc.final) || !reflect.DeepEqual(receipt.NewBlocklist, append([]mdsSessionsBlock{}, tc.entries...)) {
			t.Fatal(tc.name, "final or new blocklist differs", receipt.FinalBlocklist, receipt.NewBlocklist)
		}
	}
}

// TestMDSCephFSBlocklistReceiptFieldsKeepOrder pins the shared receipt JSON
// shape every embedding cleanup receipt writes.
func TestMDSCephFSBlocklistReceiptFieldsKeepOrder(t *testing.T) {
	want := `"new_blocklist":[],"removed_blocklist":[],"kept_new_blocklist":[],"blocklist_restored":false,`
	for name, receipt := range map[string]any{
		"recovery":   mdsCephFSCleanup{mdsCephFSBlocklistReceipt: newMDSCephFSBlocklistReceipt()},
		"completion": mdsCephFSCompletionCleanup{mdsCephFSBlocklistReceipt: newMDSCephFSBlocklistReceipt()},
		"ranks":      mdsCephFSRanksCleanup{mdsCephFSBlocklistReceipt: newMDSCephFSBlocklistReceipt()},
	} {
		data, err := json.Marshal(receipt)
		if err != nil || !strings.Contains(string(data), want) {
			t.Fatal(name, "blocklist receipt fields changed", string(data), err)
		}
	}
	data, _ := json.Marshal(mdsCephFSBlocklistReceipt{FinalBlocklist: []mdsSessionsBlock{{Address: "a"}}})
	if !strings.Contains(string(data), `"kept_new_blocklist":null,"final_blocklist":[{"address":"a","range":false,"until":""}],"blocklist_restored":false}`) {
		t.Fatal("final blocklist field moved", string(data))
	}
}

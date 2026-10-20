package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func dataRuntimeBaseline() map[string]string {
	return map[string]string{"mds_log_pause": "false", "debug_mds": "0/5", "debug_ms": "0/5", "ms_blackhole_client": "false"}
}
func TestMDSCephFSDataRuntimeClosedRequests(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, name := range []string{"debug-on", "debug-off", "log-pause", "blackhole-on", "log-resume", "flush-journal", "cap-log-mark"} {
		if _, a, err := parseMDSCephFSDataRequest([]string{"1", token, name}, token, 0); err != nil || a.name != name {
			t.Fatal(a, err)
		}
		if _, _, err := parseMDSCephFSDataRequest([]string{"1", token, name, "true"}, token, 0); err == nil {
			t.Fatal("runtime config value admitted", name)
		}
	}
	if _, a, err := parseMDSCephFSDataRequest([]string{"1", token, "cap-log", "123", "77"}, token, 0); err != nil || a.gid != 123 || a.nonce != 77 {
		t.Fatal(a, err)
	}
	for _, fields := range [][]string{
		{"1", token, "cap-log"}, {"1", token, "cap-log", "123"}, {"1", token, "cap-log", "0", "77"}, {"1", token, "cap-log", "123", "0"},
		{"1", token, "cap-log", "0123", "77"}, {"1", token, "cap-log", "123", "077"}, {"1", token, "cap-log", "123", "4294967296"},
		{"1", token, "config-set", "mds_log_pause", "true"}, {"1", token, "blackhole-off"}, {"1", token, "stat", "dir", "x"},
	} {
		if _, _, err := parseMDSCephFSDataRequest(fields, token, 0); err == nil {
			t.Fatal("unsafe runtime request accepted", fields)
		}
	}
}
func TestMDSCephFSDataRuntimeOneShotAndNativeGate(t *testing.T) {
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	debug, err := x.newRuntime(1, "debug-on")
	if err != nil || debug.IncarnationGID != 100 || debug.Baseline["debug_mds"] != "0/5" || debug.Settings["debug_ms"] != "1/5" {
		t.Fatal(debug, err)
	}
	if _, err := x.newRuntime(2, "debug-on"); err == nil {
		t.Fatal("duplicate runtime mutation admitted")
	}
	if err := x.nativeReady(); err != nil {
		t.Fatal("debug-only runtime blocked native", err)
	}
	pause, err := x.newRuntime(3, "log-pause")
	if err != nil || pause.Settings["mds_log_pause"] != "true" {
		t.Fatal(pause, err)
	}
	if err := x.nativeReady(); err == nil {
		t.Fatal("native mount admitted while journal pause outcome uncertain")
	}
	c := &fixtureCluster{mdsCephFSData: x}
	var result mdsCephFSDataResult
	if err := c.mdsCephFSDataAct(context.Background(), 4, mdsCephFSDataAction{name: "layout-set", target: "dir"}, &result); err == nil || len(x.mutations) != 0 {
		t.Fatal("paused native mutation consumed an intent", err, x.mutations)
	}
	for _, state := range []string{"intended", "applied", "uncertain", "restore-intended", "restore-uncertain"} {
		pause.State = state
		if x.nativeReady() == nil {
			t.Fatal("possibly paused native mount admitted", state)
		}
	}
	for _, state := range []string{"restored", "discarded-by-restart"} {
		pause.State = state
		if x.nativeReady() != nil {
			t.Fatal("ended pause still blocked native", state)
		}
	}
	if _, err := x.newRuntime(4, "log-pause"); err == nil {
		t.Fatal("pause mutation reissued")
	}
	for _, baseline := range []string{"", "true"} {
		y := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
		y.baselineConfig["mds_log_pause"] = baseline
		if _, err := y.newRuntime(1, "log-pause"); err == nil {
			t.Fatal("unproven baseline accepted", baseline)
		}
	}
	y := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), restart: &mdsCephFSRestart{State: "uncertain"}}
	if _, err := y.newRuntime(1, "debug-on"); err == nil {
		t.Fatal("runtime mutation admitted while restart pending")
	}
}
func TestMDSCephFSDataRuntimeIntentJournalFailureConsumesAttempt(t *testing.T) {
	// Missing output directory fails the actual journal before a nil native
	// cluster could be invoked. A subsequent request cannot issue it again.
	x := &mdsCephFSDataScope{out: t.TempDir() + "/missing", current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{}, mdsCephFSData: x}
	var result mdsCephFSDataResult
	if err := c.mdsCephFSDataRuntimeAct(context.Background(), 1, mdsCephFSDataAction{name: "log-pause"}, &result); err == nil || result.Runtime == nil || result.Runtime.State != "intended" {
		t.Fatal(result, err)
	}
	if err := c.mdsCephFSDataRuntimeAct(context.Background(), 2, mdsCephFSDataAction{name: "log-pause"}, &result); err == nil || len(x.runtime) != 1 {
		t.Fatal("runtime intent reissued", err)
	}
}
func TestMDSCephFSDataRuntimeDiscardRequiresNewBaseline(t *testing.T) {
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	r, err := x.newRuntime(1, "log-pause")
	if err != nil {
		t.Fatal(err)
	}
	r.State = "uncertain"
	if err := x.discardRestartRuntime(100, dataRuntimeBaseline()); err == nil || r.State != "uncertain" {
		t.Fatal("same incarnation setting discarded", err, r)
	}
	for _, name := range mdsCephFSDataRuntimeConfigNames {
		bad := dataRuntimeBaseline()
		bad[name] = "different"
		if err := x.discardRestartRuntime(200, bad); err == nil || r.State != "uncertain" {
			t.Fatal("new daemon baseline mismatch ignored", name, err, r)
		}
	}
	if err := x.discardRestartRuntime(200, dataRuntimeBaseline()); err != nil || r.State != "discarded-by-restart" {
		t.Fatal(r, err)
	}
	if x.activeRuntime("log-pause", 100) != nil {
		t.Fatal("discarded runtime remains active")
	}
}
func TestMDSCephFSDataCapLogRequiresRegisteredIdentity(t *testing.T) {
	x := &mdsCephFSDataScope{owned: map[string]uint64{"172.19.0.9:0/77": 123}}
	for _, tc := range []struct {
		gid   uint64
		nonce uint32
		ok    bool
	}{{123, 77, true}, {1234, 77, false}, {123, 7, false}, {123, 770, false}, {0, 77, false}, {123, 0, false}} {
		if x.capLogIdentity(tc.gid, tc.nonce) != tc.ok {
			t.Fatal(tc)
		}
	}
	c := &fixtureCluster{mdsCephFSData: x}
	var result mdsCephFSDataResult
	for _, a := range []mdsCephFSDataAction{{name: "cap-log", gid: 123, nonce: 77}, {name: "debug-off"}, {name: "cap-log-mark"}, {name: "other"}} {
		if err := c.mdsCephFSDataRuntimeAct(context.Background(), 1, a, &result); err == nil {
			t.Fatal("unregistered/no-mark control reached native", a)
		}
	}
}
func capLogInput(gid uint64, nonce uint32, tid uint64, follows string) string {
	return fmt.Sprintf("2026-10-18T01:02:03.000+0000 7fffff012345 1 --  <== client.%d v2:172.19.0.9:0/%d 9 ==== client_caps(flushsnap ino 0x10000000017 0 seq 0 issue_seq 0 tid %d caps=0 dirty=Fw wanted=0 follows %s size 200000/0 mtime 0.0 ctime 0.0 change_attr 0) ==== 300+0+0 (crc 0 0 0)", gid, nonce, tid, follows)
}
func capLogHandler(tid uint64, follows string) string {
	return fmt.Sprintf("2026-10-18T01:02:03.001+0000 7fffff012345 7 mds.0.locker handle_client_caps on 0x10000000017 tid %d follows %s op flushsnap flags 0x0", tid, follows)
}
func capLogBranch(gid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.002+0000 7fffff012345 7 mds.0.locker flushsnap snap b client.%d on [inode 0x10000000017.0 auth v2 snaprealm=0x1]", gid)
}
func TestMDSCephFSDataCapLogPairsExactSourceHandlerAndJournalBranch(t *testing.T) {
	source, handler, branch := capLogInput(123, 77, 6, "a"), capLogHandler(6, "a"), capLogBranch(123)
	kept, dropped := filterMDSCephFSDataCapLog([]string{source, handler, branch}, 123, 77)
	if !slices.Equal(kept, []string{source, handler, branch}) || dropped != 0 {
		t.Fatal(kept, dropped)
	}
	tuple, ok := dataCapLogTupleFrom("flushsnap", "0x10000000017", "26", "10")
	if !ok || tuple.tid != 26 || tuple.follows != 16 {
		t.Fatal("native snapid hex/TID decimal confused", tuple, ok)
	}
	for _, lines := range [][]string{
		{handler, branch}, {capLogInput(1234, 77, 6, "a"), handler, branch}, {capLogInput(123, 770, 6, "a"), handler, branch},
		{capLogInput(123, 7, 6, "a"), handler, branch}, {source, capLogHandler(7, "a"), branch}, {source, capLogHandler(6, "b"), branch},
		{source, strings.ReplaceAll(handler, "7fffff012345", "7fffff012346"), branch},
		{source, capLogInput(999, 77, 6, "a"), handler, branch},
		{strings.ReplaceAll(source, "client_caps(flushsnap", "client_request(flushsnap"), handler, branch},
	} {
		kept, _ := filterMDSCephFSDataCapLog(lines, 123, 77)
		if len(kept) != 0 {
			t.Fatal("unbound/cross-client/near-prefix log admitted", lines, kept)
		}
	}
	for _, badBranch := range []string{capLogBranch(1234), strings.ReplaceAll(branch, "0x10000000017", "0x10000000018")} {
		kept, _ := filterMDSCephFSDataCapLog([]string{source, handler, badBranch}, 123, 77)
		if !slices.Equal(kept, []string{source, handler}) {
			t.Fatal("foreign journal branch admitted", kept)
		}
	}
	// Old history is excluded by the caller's slice at its native mark.
	history := []string{source, handler, branch, capLogInput(123, 77, 7, "b"), capLogHandler(7, "b")}
	kept, _ = filterMDSCephFSDataCapLog(history[3:], 123, 77)
	if len(kept) != 2 || strings.Contains(strings.Join(kept, "\n"), " tid 6 ") {
		t.Fatal("old history retained", kept)
	}
}
func TestMDSCephFSDataCapLogBoundsAndDiscardIsolation(t *testing.T) {
	lines := []string{}
	for i := 0; i < mdsCephFSDataCapLogGroups+1; i++ {
		lines = append(lines, capLogInput(123, 77, uint64(i+1), "a"), capLogHandler(uint64(i+1), "a"), capLogBranch(123))
	}
	kept, dropped := filterMDSCephFSDataCapLog(lines, 123, 77)
	if len(kept) != 3*mdsCephFSDataCapLogGroups || dropped != 1 {
		t.Fatal(len(kept), dropped)
	}
	// Restoring/discarding a runtime never mutates the captured baseline map.
	baseline := dataRuntimeBaseline()
	x := &mdsCephFSDataScope{baselineConfig: maps.Clone(baseline), current: dataRestartDaemon()}
	r, err := x.newRuntime(1, "debug-on")
	if err != nil {
		t.Fatal(err)
	}
	r.Baseline["debug_ms"] = "1/5"
	if x.baselineConfig["debug_ms"] != baseline["debug_ms"] {
		t.Fatal("runtime aliases baseline")
	}
	if err := x.discardRestartRuntime(200, baseline); err != nil {
		t.Fatal(err)
	}
}

func dataRuntimePausedAndBlackholed(t *testing.T) *mdsCephFSDataScope {
	t.Helper()
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	if _, err := x.newRuntime(1, "debug-on"); err != nil {
		t.Fatal(err)
	}
	pause, err := x.newRuntime(2, "log-pause")
	if err != nil {
		t.Fatal(err)
	}
	pause.State, pause.Readback = "applied", map[string]string{"mds_log_pause": "true"}
	blackhole, err := x.newRuntime(3, "blackhole-on")
	if err != nil {
		t.Fatal(err)
	}
	blackhole.State, blackhole.Readback = "applied", map[string]string{"ms_blackhole_client": "true"}
	return x
}
func TestMDSCephFSDataBlackholeResumeAndBarrierOrdering(t *testing.T) {
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	if _, err := x.newRuntime(1, "blackhole-on"); err == nil {
		t.Fatal("blackhole admitted before native arrival held by journal pause")
	}
	if x.canResumeLog() == nil || x.canFlushJournal() == nil {
		t.Fatal("barrier/resume admitted without pause or blackhole")
	}
	x = dataRuntimePausedAndBlackholed(t)
	if err := x.canResumeLog(); err != nil {
		t.Fatal(err)
	}
	if x.canFlushJournal() == nil {
		t.Fatal("barrier admitted while journal still paused")
	}
	pause := x.activeRuntime("log-pause", 100)
	pause.State, pause.RestoreRequestID, pause.RestoreReadback = "restored", 4, map[string]string{"mds_log_pause": "false"}
	if x.canResumeLog() == nil {
		t.Fatal("resume reissued")
	}
	if err := x.canFlushJournal(); err != nil {
		t.Fatal(err)
	}
	if x.nativeReady() == nil {
		t.Fatal("native mount admitted after log resume but before blackhole ended")
	}
	for _, state := range []string{"intended", "applied", "uncertain"} {
		x.flushJournal = &mdsCephFSDataFlushJournal{State: state}
		if x.canFlushJournal() == nil {
			t.Fatal("barrier repeated", state)
		}
	}
	for _, kind := range []string{"debug-on", "log-pause", "blackhole-on"} {
		if _, err := x.newRuntime(5, kind); err == nil {
			t.Fatal("same-incarnation runtime repeated", kind)
		}
	}
	x.flushJournal = nil
	if err := x.discardRestartRuntime(200, dataRuntimeBaseline()); err != nil {
		t.Fatal(err)
	}
	x.current.GID = 200
	debug, err := x.newRuntime(6, "debug-on")
	if err != nil || debug.IncarnationGID != 200 || len(x.runtime) != 4 {
		t.Fatal("new incarnation debug was refused", debug, err)
	}
	if _, err := x.newRuntime(7, "log-pause"); err == nil {
		t.Fatal("four-row runtime bound exceeded")
	}
	if x.nativeReady() != nil {
		t.Fatal("old discarded blackhole still blocks new incarnation")
	}
}
func TestMDSCephFSDataFlushBarrierOutputAndDurableIntent(t *testing.T) {
	for _, tc := range []struct {
		data string
		ok   bool
		code int64
	}{
		{`{"return_code":0,"message":"done"}`, true, 0},
		{`{"return_code":-5,"message":"I/O error"}`, false, -5},
		{`{"return_code":null}`, false, 0}, {`{"message":"done"}`, false, 0}, {`[]`, false, 0}, {`not json`, false, 0},
	} {
		r, err := decodeMDSCephFSDataFlushJournal([]byte(tc.data))
		if (err == nil) != tc.ok || r != nil && r.ReturnCode != tc.code {
			t.Fatal(tc, r, err)
		}
	}
	if _, err := decodeMDSCephFSDataFlushJournal([]byte(`{"return_code":0,"message":"` + strings.Repeat("x", 2049) + `"}`)); err == nil {
		t.Fatal("overlong barrier output accepted")
	}
	x := dataRuntimePausedAndBlackholed(t)
	pause := x.activeRuntime("log-pause", 100)
	pause.State, pause.RestoreReadback = "restored", map[string]string{"mds_log_pause": "false"}
	x.out = t.TempDir() + "/missing"
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{}, mdsCephFSData: x}
	var result mdsCephFSDataResult
	// Actual journal failure must consume the one barrier before nil native
	// transport could run. Retrying this action must not issue another barrier.
	if err := c.mdsCephFSDataFlushJournal(context.Background(), 5, &result); err == nil || x.flushJournal == nil || x.flushJournal.State != "intended" || x.flushJournal.IncarnationGID != 100 {
		t.Fatal(x.flushJournal, err)
	}
	if err := c.mdsCephFSDataFlushJournal(context.Background(), 6, &result); err == nil || x.flushJournal.RequestID != 5 {
		t.Fatal("uncertain barrier was reissued", err, x.flushJournal)
	}
	// The real owned journal retains intent, rather than only the result file.
	x.out = t.TempDir()
	if err := c.mdsCephFSDataJournal(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(x.out, "mds-cephfs-data-owned.json"))
	if err != nil {
		t.Fatal(err)
	}
	var journal struct {
		Flush *mdsCephFSDataFlushJournal `json:"flush_journal"`
	}
	if json.Unmarshal(raw, &journal) != nil || journal.Flush == nil || journal.Flush.State != "intended" || journal.Flush.RequestID != 5 {
		t.Fatal("barrier intent lost from owned journal", string(raw))
	}
}
func capLogCounted(gid, tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.010+0000 7fffff012346 10 mds.0 send_message_client_counted client.%d seq 7 client_caps(flushsnap_ack ino 0x10000000017 0 seq 0 issue_seq 0 tid %d caps=0 dirty=Fw wanted=0 follows a size 0/0)", gid, tid)
}
func capLogPeer(nonce uint32, tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.011+0000 7fffff012346 1 -- [v2:172.19.0.10:6800/66] --> [v2:172.19.0.9:0/%d] -- client_caps(flushsnap_ack ino 0x10000000017 0 seq 0 issue_seq 0 tid %d caps=0 dirty=Fw wanted=0 follows a size 0/0) -- 0x100 con 0x200", nonce, tid)
}
func capLogDrop(tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.012+0000 7fffff012346 0 -- send_msgclient blackhole client_caps(flushsnap_ack ino 0x10000000017 0 seq 0 issue_seq 0 tid %d caps=0 dirty=Fw wanted=0 follows a size 0/0)", tid)
}
func capLogDedupe(gid, tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.002+0000 7fffff012345 7 mds.0.locker handle_client_caps already flushed tid %d for client.%d", tid, gid)
}
func TestMDSCephFSDataCapLogAttributesDroppedAckTriple(t *testing.T) {
	counted, peer, drop := capLogCounted(123, 6), capLogPeer(77, 6), capLogDrop(6)
	kept, dropped := filterMDSCephFSDataCapLog([]string{counted, peer, drop}, 123, 77)
	if !slices.Equal(kept, []string{counted, peer, drop}) || dropped != 0 {
		t.Fatal(kept, dropped)
	}
	for _, lines := range [][]string{
		{peer, drop}, {counted, drop}, {drop}, {capLogCounted(1234, 6), peer, drop}, {counted, capLogPeer(770, 6), drop},
		{counted, capLogPeer(7, 6), drop}, {counted, capLogPeer(77, 7), drop}, {counted, peer, capLogDrop(7)},
		{counted, strings.ReplaceAll(peer, "0x10000000017", "0x10000000018"), drop},
		{counted, strings.ReplaceAll(peer, "follows a", "follows b"), drop},
		{counted, strings.ReplaceAll(peer, "flushsnap_ack", "flush_ack"), drop},
		{counted, strings.ReplaceAll(peer, "7fffff012346", "7fffff012347"), drop},
		{counted, capLogCounted(999, 6), peer, drop},
		// The registered nonce in the local address cannot replace a different
		// destination nonce: only the destination between --> and -- is checked.
		{counted, strings.ReplaceAll(capLogPeer(99, 6), "6800/66", "6800/77"), drop},
	} {
		kept, _ := filterMDSCephFSDataCapLog(lines, 123, 77)
		if len(kept) != 0 {
			t.Fatal("unbound/cross-client/cross-tuple ACK drop accepted", lines, kept)
		}
	}
}
func TestMDSCephFSDataCapLogDedupeRequiresOwnedIncomingHandler(t *testing.T) {
	source, handler, dedupe := capLogInput(123, 77, 6, "a"), capLogHandler(6, "a"), capLogDedupe(123, 6)
	kept, _ := filterMDSCephFSDataCapLog([]string{source, handler, dedupe}, 123, 77)
	if !slices.Equal(kept, []string{source, handler, dedupe}) {
		t.Fatal(kept)
	}
	for _, lines := range [][]string{
		{dedupe}, {source, dedupe}, {capLogInput(1234, 77, 6, "a"), handler, dedupe},
		{source, handler, capLogDedupe(1234, 6)}, {source, handler, capLogDedupe(123, 7)},
		{source, handler, strings.ReplaceAll(dedupe, "7fffff012345", "7fffff012346")},
	} {
		kept, _ := filterMDSCephFSDataCapLog(lines, 123, 77)
		if slices.Contains(kept, lines[len(lines)-1]) {
			t.Fatal("unbound dedupe branch accepted", lines, kept)
		}
	}
	// A new dispatch on the same native thread clears the previous context.
	kept, _ = filterMDSCephFSDataCapLog([]string{source, handler, capLogInput(999, 77, 6, "a"), dedupe}, 123, 77)
	if slices.Contains(kept, dedupe) {
		t.Fatal("stale handler context accepted dedupe")
	}
	// Incoming and outgoing branches may be on different native threads.
	lines := []string{source, handler, capLogBranch(123), capLogCounted(123, 6), capLogPeer(77, 6), capLogDrop(6)}
	kept, _ = filterMDSCephFSDataCapLog(lines, 123, 77)
	if !slices.Equal(kept, lines) {
		t.Fatal("independent incoming/outgoing evidence lost", kept)
	}
}

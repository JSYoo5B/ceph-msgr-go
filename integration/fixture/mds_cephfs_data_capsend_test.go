package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func dataRuntimeDebugApplied(t *testing.T) *mdsCephFSDataScope {
	t.Helper()
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	debug, err := x.newRuntime(1, "debug-on")
	if err != nil {
		t.Fatal(err)
	}
	debug.State, debug.Readback = "applied", map[string]string{"debug_mds": "10/10", "debug_ms": "1/5"}
	return x
}

func TestMDSCephFSDataClientDropAdmissionAndDiscard(t *testing.T) {
	token := strings.Repeat("a1", 16)
	if _, a, err := parseMDSCephFSDataRequest([]string{"1", token, "client-drop-on"}, token, 0); err != nil || a.name != "client-drop-on" {
		t.Fatal(a, err)
	}
	for _, fields := range [][]string{{"1", token, "client-drop-on", "true"}, {"1", token, "client-drop-off"}} {
		if _, _, err := parseMDSCephFSDataRequest(fields, token, 0); err == nil {
			t.Fatal("unsafe client drop request accepted", fields)
		}
	}
	// Without its applied debug change the drop would leave no attributable log.
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	if _, err := x.newRuntime(1, "client-drop-on"); err == nil {
		t.Fatal("client drop admitted without owned debug evidence")
	}
	if _, err := x.newRuntime(2, "debug-on"); err != nil {
		t.Fatal(err)
	}
	if _, err := x.newRuntime(3, "client-drop-on"); err == nil {
		t.Fatal("client drop admitted while the debug change is unconfirmed")
	}
	// It never combines with the journal-pause/dropped-ACK recipe.
	if _, err := dataRuntimePausedAndBlackholed(t).newRuntime(4, "client-drop-on"); err == nil {
		t.Fatal("client drop admitted beside the journal-pause blackhole")
	}
	x = dataRuntimeDebugApplied(t)
	pause, err := x.newRuntime(2, "log-pause")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.newRuntime(3, "client-drop-on"); err == nil {
		t.Fatal("client drop admitted while a journal pause is active")
	}
	pause.State = "restored"
	drop, err := x.newRuntime(4, "client-drop-on")
	if err != nil || drop.Settings["ms_blackhole_client"] != "true" || drop.Settings["debug_ms"] != "10/10" || len(drop.Settings) != 2 ||
		drop.Baseline["ms_blackhole_client"] != "false" || drop.Baseline["debug_ms"] != "0/5" {
		t.Fatal(drop, err)
	}
	if x.nativeReady() == nil {
		t.Fatal("native client admitted while client messages are dropped")
	}
	if _, err := x.newRuntime(5, "blackhole-on"); err == nil {
		t.Fatal("journal-pause blackhole admitted beside the client drop")
	}
	if _, err := x.newRuntime(5, "client-drop-on"); err == nil {
		t.Fatal("client drop repeated on one incarnation")
	}
	// The owned restart discards it only after baseline proof on a new GID.
	if err := x.discardRestartRuntime(100, dataRuntimeBaseline()); err == nil {
		t.Fatal("client drop discarded on the same incarnation")
	}
	if err := x.discardRestartRuntime(200, dataRuntimeBaseline()); err != nil || drop.State != "discarded-by-restart" {
		t.Fatal(drop.State, err)
	}
	x.current.GID = 200
	if x.nativeReady() != nil {
		t.Fatal("discarded client drop still blocks the new incarnation")
	}
}

func capLogIncomingDrop(nonce uint32, op string, tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.020+0000 7fffff012350 10 --2- [v2:172.19.0.10:6800/66,v1:172.19.0.10:6801/66] >> v2:172.19.0.9:0/%d conn(0x100 0x200 secure :-1 s=READY pgs=3 gs=4 cs=0 l=0 c_cookie=1 s_cookie=2 reconnecting=0 rev1=1 crypto rx=0x300 tx=0x400 comp rx=0 tx=0).handle_message blackhole client_caps(%s ino 0x10000000017 1 seq 4 issue_seq 1 tid %d caps=pAsxLsXsxFsxcrwb dirty=Fw wanted=pAsxXsxFxwb follows 1 size 200000/4194304 mtime 0.0 ctime 0.0 change_attr 3)", nonce, op, tid)
}
func capLogUpdateInput(gid uint64, nonce uint32, tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.000+0000 7fffff012345 1 --  <== client.%d v2:172.19.0.9:0/%d 9 ==== client_caps(update ino 0x10000000017 1 seq 0 issue_seq 0 tid %d caps=pAsxLsXsxFsxcrwb dirty=Fw wanted=pAsxXsxFxwb follows 1 size 200000/4194304 mtime 0.0 ctime 0.0 change_attr 3) ==== 300+0+0 (crc 0 0 0)", gid, nonce, tid)
}
func capLogUpdateHandler(tid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.001+0000 7fffff012345 7 mds.0.locker handle_client_caps on 0x10000000017 tid %d follows 1 op update flags 0x1", tid)
}
func capLogUpdateFlush(gid uint64) string {
	return fmt.Sprintf("2026-10-18T01:02:03.002+0000 7fffff012345 7 mds.0.locker  flush client.%d dirty Fw seq 0 on [inode 0x10000000017 [2,head] /dir/capsnap-file auth v12 s=200000 n(v0 1=1+0) caps={%d=pAsxLsXsxFsxcrwb/pAsxXsxFxwb@1} 0x500]", gid, gid)
}

func TestMDSCephFSDataCapLogAttributesIncomingDropByPeerNonce(t *testing.T) {
	drop := capLogIncomingDrop(77, "update", 7)
	kept, dropped := filterMDSCephFSDataCapLog([]string{drop}, 123, 77)
	if !slices.Equal(kept, []string{drop}) || dropped != 0 {
		t.Fatal(kept, dropped)
	}
	for _, line := range []string{
		capLogIncomingDrop(770, "update", 7), capLogIncomingDrop(7, "update", 7),
		// The registered nonce in the MDS's own address is not the peer.
		strings.ReplaceAll(capLogIncomingDrop(99, "update", 7), "6800/66", "6800/77"),
		// Server-side operations and other messages are not client sends.
		capLogIncomingDrop(77, "flush_ack", 7),
		strings.ReplaceAll(drop, "client_caps(", "client_request("),
		strings.ReplaceAll(drop, ".handle_message blackhole", ".handle_message"),
		// A bare drop without its peer segment is not attributable.
		strings.ReplaceAll(drop, " >> ", " "),
	} {
		if kept, _ := filterMDSCephFSDataCapLog([]string{line}, 123, 77); len(kept) != 0 {
			t.Fatal("unattributed incoming drop accepted", line, kept)
		}
	}
	// The outgoing ACK triple keeps its separate attribution rules.
	lines := []string{capLogCounted(123, 6), capLogPeer(77, 6), capLogDrop(6), drop}
	if kept, _ := filterMDSCephFSDataCapLog(lines, 123, 77); !slices.Equal(kept, lines) {
		t.Fatal("incoming drop disturbed dropped-ACK evidence", kept)
	}
}

func TestMDSCephFSDataCapLogUpdateBranches(t *testing.T) {
	source, handler, flush := capLogUpdateInput(123, 77, 7), capLogUpdateHandler(7), capLogUpdateFlush(123)
	kept, _ := filterMDSCephFSDataCapLog([]string{source, handler, flush}, 123, 77)
	if !slices.Equal(kept, []string{source, handler, flush}) {
		t.Fatal(kept)
	}
	dedupe := capLogDedupe(123, 7)
	kept, _ = filterMDSCephFSDataCapLog([]string{source, handler, dedupe, flush}, 123, 77)
	if !slices.Equal(kept, []string{source, handler, dedupe}) {
		t.Fatal("completed UPDATE branch was not the single recorded branch", kept)
	}
	for _, bad := range []string{
		capLogUpdateFlush(1234), strings.ReplaceAll(flush, "0x10000000017", "0x10000000018"),
		strings.ReplaceAll(flush, "7fffff012345", "7fffff012346"), capLogDedupe(123, 8),
	} {
		kept, _ := filterMDSCephFSDataCapLog([]string{source, handler, bad}, 123, 77)
		if !slices.Equal(kept, []string{source, handler}) {
			t.Fatal("foreign UPDATE branch admitted", bad, kept)
		}
	}
	if kept, _ := filterMDSCephFSDataCapLog([]string{source, flush}, 123, 77); len(kept) != 0 {
		t.Fatal("UPDATE branch admitted without its exact handler", kept)
	}
}

func TestMDSCephFSDataProfileAdmitsOnlyTheExplicitCapSendSelector(t *testing.T) {
	env := mdsCephFSDataProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = "^TestCephMDSCephFSCapSendRecoveryIntegration$"
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || p.testRun != "^TestCephMDSCephFSCapSendRecoveryIntegration$" || p.mdsCephFSData != "1" || !p.mdsFreshOSDAllowed() {
		t.Fatal("explicit cap-send selector was not admitted on the data fixture", p, err)
	}
	for _, change := range []map[string]string{
		// Without the data opt-in the selector belongs to no MDS profile.
		{"CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "0"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Data|CapSendRecovery)Integration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFSCapSendRecoveryIntegration"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
	} {
		env := mdsCephFSDataProfileEnv()
		env["CEPH_MSGR_TEST_RUN"] = "^TestCephMDSCephFSCapSendRecoveryIntegration$"
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe cap-send profile accepted", change, p, err)
		}
	}
}

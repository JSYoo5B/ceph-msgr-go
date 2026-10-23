package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestMDSCephFSDataProfileAdmitsOnlyTheExplicitWritebackSelector(t *testing.T) {
	env := mdsCephFSDataProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = mdsCephFSWritebackSelector
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || p.testRun != mdsCephFSWritebackSelector || p.mdsCephFSData != "1" || !p.mdsFreshOSDAllowed() {
		t.Fatal("explicit writeback-fence selector was not admitted on the data fixture", p, err)
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "0"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(Data|WritebackFence)Integration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFSWritebackFenceIntegration"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
	} {
		env := mdsCephFSDataProfileEnv()
		env["CEPH_MSGR_TEST_RUN"] = mdsCephFSWritebackSelector
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe writeback-fence profile accepted", change, p, err)
		}
	}
}

func TestMDSCephFSDataWritebackRequestShapes(t *testing.T) {
	token := strings.Repeat("b2", 16)
	for _, fields := range [][]string{
		{"1", token, "osd-drop-on"}, {"1", token, "osd-drop-off"}, {"1", token, "osd-log", "4100", "77"}, {"1", token, "evict", "4100"},
	} {
		_, a, err := parseMDSCephFSDataRequest(fields, token, 0)
		if err != nil || a.name != fields[2] {
			t.Fatal(fields, a, err)
		}
		if fields[2] == "osd-log" && (a.gid != 4100 || a.nonce != 77) || fields[2] == "evict" && a.gid != 4100 {
			t.Fatal("identity arguments differ", a)
		}
	}
	for _, fields := range [][]string{
		{"1", token, "osd-drop-on", "true"}, {"1", token, "osd-drop-off", "osd.0"}, {"1", token, "osd-log", "4100"}, {"1", token, "osd-log", "04100", "77"},
		{"1", token, "osd-log", "4100", "0"}, {"1", token, "evict"}, {"1", token, "evict", "0"}, {"1", token, "evict", "4100", "77"}, {"1", token, "osd-blackhole-on"},
		{"1", token, "observe", "4100", "77"},
	} {
		if _, _, err := parseMDSCephFSDataRequest(fields, token, 0); err == nil {
			t.Fatal("unsafe writeback request accepted", fields)
		}
	}
}

func TestMDSCephFSDataOSDDropAdmission(t *testing.T) {
	baseline := map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "false"}
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	if _, err := x.newOSDDrop(1, "osd.0", map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "true"}); err == nil {
		t.Fatal("OSD drop admitted from a blackholed baseline")
	}
	if _, err := x.newOSDDrop(1, "osd.0", map[string]string{"ms_blackhole_client": "false"}); err == nil {
		t.Fatal("OSD drop admitted without a debug baseline")
	}
	// Never beside an MDS client blackhole or during a restart.
	if _, err := dataRuntimePausedAndBlackholed(t).newOSDDrop(1, "osd.0", baseline); err == nil {
		t.Fatal("OSD drop admitted beside the journal-pause blackhole")
	}
	withDrop := dataRuntimeDebugApplied(t)
	if _, err := withDrop.newRuntime(2, "client-drop-on"); err != nil {
		t.Fatal(err)
	}
	if _, err := withDrop.newOSDDrop(3, "osd.0", baseline); err == nil {
		t.Fatal("OSD drop admitted beside the MDS client drop")
	}
	armed := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), restart: &mdsCephFSRestart{State: "applied"}}
	if _, err := armed.newOSDDrop(1, "osd.0", baseline); err == nil {
		t.Fatal("OSD drop admitted during a restart")
	}
	d, err := x.newOSDDrop(1, "osd.0", baseline)
	if err != nil || d.State != "intended" || d.OSD != "osd.0" || len(d.Settings) != 2 || d.Settings["ms_blackhole_client"] != "true" || d.Settings["debug_ms"] != "10/10" ||
		d.Baseline["debug_ms"] != "0/5" || d.Baseline["ms_blackhole_client"] != "false" {
		t.Fatal(d, err)
	}
	baseline["debug_ms"] = "1/1"
	if d.Baseline["debug_ms"] != "0/5" {
		t.Fatal("OSD drop baseline aliases the caller's map")
	}
	x.osdDrop = d
	for _, state := range []string{"intended", "applied", "uncertain", "restore-intended", "restore-uncertain"} {
		d.State = state
		if x.nativeReady() == nil {
			t.Fatal("native client admitted while the OSD drops client messages", state)
		}
	}
	if _, err := x.newOSDDrop(2, "osd.0", d.Baseline); err == nil {
		t.Fatal("OSD drop repeated")
	}
	d.State = "restored"
	if x.nativeReady() != nil {
		t.Fatal("restored OSD drop still blocks native clients")
	}
	// One further drop is admitted after restoration, never a third.
	second, err := x.newOSDDrop(2, "osd.0", d.Baseline)
	if err != nil || second.State != "intended" || second.RequestID != 2 {
		t.Fatal("second OSD drop refused after restoration", second, err)
	}
	x.osdDropHistory, x.osdDrop = append(x.osdDropHistory, d), second
	for _, state := range dataActiveStates {
		second.State = state
		if _, err := x.newOSDDrop(3, "osd.0", d.Baseline); err == nil {
			t.Fatal("OSD drop admitted beside an unrestored drop", state)
		}
	}
	second.State = "restored"
	if _, err := x.newOSDDrop(3, "osd.0", d.Baseline); err == nil {
		t.Fatal("third OSD drop admitted")
	}
}

func TestMDSCephFSDataBackgroundReaderAdmission(t *testing.T) {
	baseline := map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "false"}
	token := strings.Repeat("b2", 16)
	for _, name := range []string{"reader-start", "reader-status", "reader-wait"} {
		if _, a, err := parseMDSCephFSDataRequest([]string{"1", token, name}, token, 0); err != nil || a.name != name {
			t.Fatal(name, a, err)
		}
		if _, _, err := parseMDSCephFSDataRequest([]string{"1", token, name, "shared-file"}, token, 0); err == nil {
			t.Fatal("reader action admitted a caller argument", name)
		}
	}
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline()}
	if x.readerReady() == nil {
		t.Fatal("reader admitted without an OSD drop")
	}
	for _, state := range []string{"intended", "uncertain", "restore-intended", "restore-uncertain", "restored"} {
		x.osdDrop = &mdsCephFSDataOSDDrop{State: state}
		if x.readerReady() == nil {
			t.Fatal("reader admitted without an applied OSD drop", state)
		}
	}
	x.osdDrop = &mdsCephFSDataOSDDrop{State: "applied"}
	armed := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), restart: &mdsCephFSRestart{State: "applied"}, osdDrop: x.osdDrop}
	if armed.readerReady() == nil {
		t.Fatal("reader admitted during a restart")
	}
	faulted := dataRuntimeDebugApplied(t)
	if _, err := faulted.newRuntime(2, "client-drop-on"); err != nil {
		t.Fatal(err)
	}
	faulted.osdDrop = x.osdDrop
	if faulted.readerReady() == nil {
		t.Fatal("reader admitted beside an MDS client drop")
	}
	if err := x.readerReady(); err != nil {
		t.Fatal("reader refused behind an applied OSD drop", err)
	}
	// While it runs: no other native work, MDS fault, restart arm, eviction
	// or new OSD drop; status works; wait requires the restored drop.
	x.reader = newMDSCephFSDataReaderRun(5)
	if x.readerReady() == nil {
		t.Fatal("a second reader was admitted")
	}
	x.osdDrop.State = "restored"
	if x.nativeReady() == nil {
		t.Fatal("native work admitted while the reader runs")
	}
	if _, err := x.newRuntime(6, "log-pause"); err == nil {
		t.Fatal("MDS fault admitted while the reader runs")
	}
	if _, err := x.newOSDDrop(6, "osd.0", baseline); err == nil {
		t.Fatal("OSD drop admitted while the reader runs")
	}
	c := &fixtureCluster{mdsCephFSData: x}
	c.profile.testRun = mdsCephFSWritebackSelector
	x.owned = map[string]uint64{}
	if err := c.mdsCephFSDataEvict(context.Background(), 6, 4100, &mdsCephFSDataResult{}); err == nil || len(x.mutations) != 0 {
		t.Fatal("eviction admitted while the reader runs", err)
	}
	result := &mdsCephFSDataResult{}
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 6, mdsCephFSDataAction{name: "reader-status"}, result); err != nil ||
		result.Reader == nil || result.Reader.State != "running" || result.Reader.RequestID != 5 {
		t.Fatal("reader status differs", result.Reader, err)
	}
	x.osdDrop.State = "applied"
	if err := x.reader.wait(context.Background(), 7, x.osdDrop.active(), &mdsCephFSDataResult{}); err == nil {
		t.Fatal("reader wait admitted while the OSD drops client messages")
	}
	x.osdDrop.State = "restored"
	expired, stop := context.WithCancel(context.Background())
	stop()
	if err := x.reader.wait(expired, 7, false, &mdsCephFSDataResult{}); err == nil {
		t.Fatal("reader wait did not stay bounded by its request")
	}
	x.reader.finish(nil, mdsCephFSDataNative{Read: &mdsCephFSDataDigest{Length: 1}}, nil)
	if x.nativeReady() != nil {
		t.Fatal("an ended reader still blocks native work")
	}
	result = &mdsCephFSDataResult{}
	if err := x.reader.wait(context.Background(), 8, false, result); err != nil || result.Native == nil || result.Native.Read.Length != 1 ||
		result.Reader.State != "completed" || result.Reader.WaitRequestID != 8 {
		t.Fatal("reader wait result differs", result, err)
	}
	if err := x.reader.wait(context.Background(), 9, false, &mdsCephFSDataResult{}); err == nil {
		t.Fatal("reader result collected twice")
	}
	if err := x.reader.awaitCleanup(context.Background(), &mdsCephFSDataCleanup{}); err != nil {
		t.Fatal(err)
	}
	// A failed reader reports its error; nothing is retried.
	failed := newMDSCephFSDataReaderRun(10)
	failed.finish(nil, mdsCephFSDataNative{}, fmt.Errorf("native exit 1"))
	if err := failed.wait(context.Background(), 11, false, &mdsCephFSDataResult{}); err == nil || failed.snapshot().State != "failed" {
		t.Fatal("failed reader not reported", err)
	}
	var none *mdsCephFSDataReaderRun
	if none.running() || none.wait(context.Background(), 1, false, &mdsCephFSDataResult{}) == nil || none.awaitCleanup(context.Background(), &mdsCephFSDataCleanup{}) != nil {
		t.Fatal("absent reader handling differs")
	}
}

func TestMDSCephFSDataWritebackControlsRequireTheirSelector(t *testing.T) {
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), owned: map[string]uint64{}}
	c := &fixtureCluster{mdsCephFSData: x}
	for _, name := range []string{"osd-drop-on", "osd-drop-off", "osd-log", "evict", "reader-start", "reader-status", "reader-wait"} {
		result := &mdsCephFSDataResult{}
		if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: name, gid: 4100, nonce: 77}, result); err == nil ||
			!strings.Contains(err.Error(), "writeback-fence selector") || x.osdDrop != nil || len(x.mutations) != 0 {
			t.Fatal("writeback control ran without its selector", name, err)
		}
	}
	// With the selector, a restore or log without an applied override and an
	// eviction during an active drop are refused before any native work.
	c.profile.testRun = mdsCephFSWritebackSelector
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: "osd-drop-off"}, &mdsCephFSDataResult{}); err == nil {
		t.Fatal("OSD drop restore admitted without an override")
	}
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: "osd-log", gid: 4100, nonce: 77}, &mdsCephFSDataResult{}); err == nil {
		t.Fatal("OSD drop log admitted without an override")
	}
	x.osdDrop = &mdsCephFSDataOSDDrop{State: "applied"}
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: "osd-log", gid: 4100, nonce: 77}, &mdsCephFSDataResult{}); err == nil {
		t.Fatal("OSD drop log admitted for an unregistered identity")
	}
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: "evict", gid: 4100}, &mdsCephFSDataResult{}); err == nil || len(x.mutations) != 0 {
		t.Fatal("eviction admitted while the OSD drops client messages", err)
	}
	x.osdDrop.State = "restored"
	x.mutations = []*mdsCephFSMutation{{Kind: "eviction", State: "uncertain"}}
	if err := c.mdsCephFSDataWritebackAct(context.Background(), 1, mdsCephFSDataAction{name: "evict", gid: 4100}, &mdsCephFSDataResult{}); err == nil || len(x.mutations) != 1 {
		t.Fatal("a second eviction was admitted", err)
	}
}

func osdLogReceived(thread string, gid uint64, nonce uint32) string {
	return fmt.Sprintf("2026-10-23T01:02:03.010+0000 %s  5 --2- [v2:172.19.0.11:6802/55,v1:172.19.0.11:6803/55] >> v2:172.19.0.9:0/%d conn(0x100 0x200 crc :-1 s=THROTTLE_DONE pgs=3 gs=4 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_message received message m=0x55d0 seq=7 from=client.%d type=42 osd_op()", thread, nonce, gid)
}
func osdLogBlackhole(thread string, nonce uint32) string {
	return fmt.Sprintf("2026-10-23T01:02:03.011+0000 %s 10 --2- [v2:172.19.0.11:6802/55,v1:172.19.0.11:6803/55] >> v2:172.19.0.9:0/%d conn(0x100 0x200 crc :-1 s=READY pgs=3 gs=4 cs=0 l=1 c_cookie=0 s_cookie=0 reconnecting=0 rev1=1 crypto rx=0 tx=0 comp rx=0 tx=0).handle_message blackhole osd_op()", thread, nonce)
}

func TestMDSCephFSDataOSDDropLogAttribution(t *testing.T) {
	const thread, other = "7f00000a1234", "7f00000a5678"
	received, drop := osdLogReceived(thread, 4100, 77), osdLogBlackhole(thread, 77)
	kept, dropped := filterMDSCephFSDataOSDDropLog([]string{received, drop}, 4100, 77)
	if !slices.Equal(kept, []string{received, drop}) || dropped != 0 {
		t.Fatal(kept, dropped)
	}
	// Interleaved lines of another thread do not break the pair.
	noise := osdLogReceived(other, 4200, 88)
	if kept, _ := filterMDSCephFSDataOSDDropLog([]string{received, noise, drop}, 4100, 77); !slices.Equal(kept, []string{received, drop}) {
		t.Fatal("interleaved thread broke attribution", kept)
	}
	for name, lines := range map[string][]string{
		"foreign client":         {osdLogReceived(thread, 4101, 77), drop},
		"foreign nonce":          {osdLogReceived(thread, 4100, 770), osdLogBlackhole(thread, 770)},
		"nonce in the OSD addr":  {strings.ReplaceAll(osdLogReceived(thread, 4100, 99), "6802/55", "6802/77"), strings.ReplaceAll(osdLogBlackhole(thread, 99), "6802/55", "6802/77")},
		"drop on another thread": {received, osdLogBlackhole(other, 77)},
		"bare drop":              {drop},
		"other message between":  {received, strings.ReplaceAll(osdLogReceived(thread, 4100, 77), "type=42 osd_op()", "type=63 ping()"), drop},
		"not an op":              {strings.ReplaceAll(received, "type=42 osd_op()", "type=42 ping()"), drop},
		"dispatched":             {received, strings.ReplaceAll(drop, ".handle_message blackhole", ".handle_message")},
		"no peer segment":        {strings.ReplaceAll(received, " >> ", " "), strings.ReplaceAll(drop, " >> ", " ")},
	} {
		if kept, _ := filterMDSCephFSDataOSDDropLog(lines, 4100, 77); len(kept) != 0 {
			t.Fatal("unattributed OSD drop accepted:", name, kept)
		}
	}
	long := strings.Replace(received, "osd_op()", "osd_op()"+strings.Repeat(" ", mdsCephFSDataCapLogLineBytes), 1)
	if kept, dropped := filterMDSCephFSDataOSDDropLog([]string{long, drop}, 4100, 77); len(kept) != 0 || dropped != 1 {
		t.Fatal("overlong line not counted as dropped", kept, dropped)
	}
}

var dataActiveStates = []string{"intended", "applied", "uncertain", "restore-intended", "restore-uncertain"}

func TestMDSCephFSDataOSDDropAndMDSFaultsExcludeEachOther(t *testing.T) {
	baseline := map[string]string{"debug_ms": "0/5", "ms_blackhole_client": "false"}
	// OSD drop first: no MDS fault starts while it is active or uncertain.
	for _, state := range dataActiveStates {
		for _, kind := range []string{"log-pause", "client-drop-on"} {
			x := dataRuntimeDebugApplied(t)
			d, err := x.newOSDDrop(10, "osd.0", baseline)
			if err != nil {
				t.Fatal(err)
			}
			x.osdDrop, d.State = d, state
			if r, err := x.newRuntime(11, kind); err == nil || r != nil || len(x.runtime) != 1 {
				t.Fatal("MDS fault admitted beside an active OSD drop", state, kind, err)
			}
		}
		// The journal-pause recipe's later blackhole is refused as well.
		x := dataRuntimePausedAndBlackholed(t)
		x.runtime = x.runtime[:2]
		x.osdDrop = &mdsCephFSDataOSDDrop{State: state}
		if _, err := x.newRuntime(11, "blackhole-on"); err == nil || len(x.runtime) != 2 {
			t.Fatal("MDS blackhole admitted beside an active OSD drop", state, err)
		}
	}
	// The debug change and owned MDS restoration stay available beside an
	// OSD drop.
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), osdDrop: &mdsCephFSDataOSDDrop{State: "uncertain"}}
	if _, err := x.newRuntime(1, "debug-on"); err != nil {
		t.Fatal("debug change refused beside an OSD drop", err)
	}
	paused := dataRuntimePausedAndBlackholed(t)
	paused.osdDrop = &mdsCephFSDataOSDDrop{State: "restore-uncertain"}
	if err := paused.canResumeLog(); err != nil {
		t.Fatal("owned journal resume blocked by an OSD drop", err)
	}
	// MDS fault first: the OSD drop is refused while either fault kind is in
	// any active or uncertain state.
	for _, state := range dataActiveStates {
		for _, kind := range []string{"log-pause", "client-drop-on"} {
			x := dataRuntimeDebugApplied(t)
			fault, err := x.newRuntime(2, kind)
			if err != nil {
				t.Fatal(err)
			}
			fault.State = state
			if d, err := x.newOSDDrop(3, "osd.0", baseline); err == nil || d != nil {
				t.Fatal("OSD drop admitted beside an active MDS fault", state, kind)
			}
		}
	}
	// Once the OSD drop is restored an MDS fault is admitted again.
	x = dataRuntimeDebugApplied(t)
	x.osdDrop = &mdsCephFSDataOSDDrop{State: "restored"}
	if _, err := x.newRuntime(2, "client-drop-on"); err != nil {
		t.Fatal("MDS client drop refused after the OSD drop was restored", err)
	}
}

package main

import (
	"context"
	"strings"
	"testing"
)

const (
	// mdsCephFSWritebackOutcomesSelector and
	// mdsCephFSUncertainPrefixSelector are the native tests of the further
	// writeback cells (their //scenario:row declarations).
	mdsCephFSWritebackOutcomesSelector = "^TestCephMDSCephFSWritebackOutcomesIntegration$"
	mdsCephFSUncertainPrefixSelector   = "^TestCephMDSCephFSUncertainPrefixFlushIntegration$"
)

func persistProfileEnv(selector, capabilities string) map[string]string {
	env := mdsCephFSDataProfileEnv()
	env["CEPH_MSGR_TEST_RUN"], env["CEPH_MSGR_TEST_CAPABILITIES"] = selector, capabilities
	return env
}

// The rows declare their capabilities, and only the uncertain-prefix rows
// admit the OSD store and restart controls.
func TestMDSCephFSWritebackPersistCapabilityAdmission(t *testing.T) {
	if got := catalogCapabilities(t, mdsCephFSWritebackOutcomesSelector); got != "mds-writeback-fence,mds-writeback-mixed" {
		t.Fatal("the writeback outcomes rows declare", got)
	}
	if got := catalogCapabilities(t, mdsCephFSUncertainPrefixSelector); got != "mds-writeback-fence,mds-writeback-mixed,mds-writeback-persist" {
		t.Fatal("the uncertain-prefix rows declare", got)
	}
	env := persistProfileEnv(mdsCephFSUncertainPrefixSelector, "mds-writeback-fence,mds-writeback-mixed,mds-writeback-persist")
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || p.testRun != mdsCephFSUncertainPrefixSelector || !p.admits(capMDSWritebackPersist) || !p.admits(capMDSWritebackMixed) || !p.admits(capMDSWritebackFence) {
		t.Fatal("the uncertain-prefix selector was not admitted with its capabilities", p, err)
	}
	env = persistProfileEnv(mdsCephFSWritebackOutcomesSelector, "mds-writeback-fence,mds-writeback-mixed")
	if p, err := loadProfile(func(k string) string { return env[k] }); err != nil || !p.admits(capMDSWritebackMixed) || p.admits(capMDSWritebackPersist) {
		t.Fatal("the writeback outcomes profile differs", err)
	}
	for _, change := range []map[string]string{
		// The restart extends the OSD drop controls.
		{"CEPH_MSGR_TEST_CAPABILITIES": "mds-writeback-persist"},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "0"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(UncertainPrefixFlush|Data)Integration$"},
	} {
		env := persistProfileEnv(mdsCephFSUncertainPrefixSelector, "mds-writeback-fence,mds-writeback-mixed,mds-writeback-persist")
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe persistence profile accepted", change, p, err)
		}
	}
}

func TestMDSCephFSDataOSDRestartRequestIsClosed(t *testing.T) {
	token := strings.Repeat("c4", 16)
	for _, name := range []string{"osd-store", "osd-restart"} {
		_, a, err := parseMDSCephFSDataRequest([]string{"1", token, name}, token, 0)
		if err != nil || a != (mdsCephFSDataAction{name: name}) {
			t.Fatal("request differs", name, a, err)
		}
		for _, fields := range [][]string{{"1", token, name, "osd.0"}, {"1", token, name, "0", "1"}} {
			if _, _, err := parseMDSCephFSDataRequest(fields, token, 0); err == nil {
				t.Fatal("a caller argument was accepted", fields)
			}
		}
	}
}

// Both controls are refused before any native work without the capability,
// and with it wherever native work is refused; a second restart and one
// beside an unrestored OSD drop are refused too (the cluster is absent, so
// native work would panic).
func TestMDSCephFSDataOSDRestartAdmissionPrecedesNativeWork(t *testing.T) {
	token := strings.Repeat("c4", 16)
	out := t.TempDir()
	mixed := writebackProfile(profile{})
	mixed.testRun, mixed.capabilities = mdsCephFSMixedWritebackSelector, "mds-writeback-fence,mds-writeback-mixed"
	persist := writebackProfile(profile{})
	persist.testRun, persist.capabilities = mdsCephFSUncertainPrefixSelector, "mds-writeback-fence,mds-writeback-mixed,mds-writeback-persist"
	if !persist.admits(capMDSWritebackPersist) || mixed.admits(capMDSWritebackPersist) {
		t.Fatal("the test profiles differ")
	}
	for _, tc := range []struct {
		name    string
		actions []string
		p       profile
		drop    *mdsCephFSDataOSDDrop
		restart *mdsCephFSRestart
		osd     *mdsCephFSDataOSDRestart
		reader  bool
		want    string
	}{
		{"no capability", []string{"osd-store", "osd-restart"}, profile{}, nil, nil, nil, false, "capability mds-writeback-persist"},
		{"mixed rows", []string{"osd-store", "osd-restart"}, mixed, nil, nil, nil, false, "capability mds-writeback-persist"},
		{"applied OSD drop", []string{"osd-store"}, persist, &mdsCephFSDataOSDDrop{State: "applied"}, nil, nil, false, "native action refused"},
		{"applied OSD drop", []string{"osd-restart"}, persist, &mdsCephFSDataOSDDrop{State: "applied"}, nil, nil, false, "drop is not restored"},
		{"uncertain OSD drop restore", []string{"osd-restart"}, persist, &mdsCephFSDataOSDDrop{State: "restore-uncertain"}, nil, nil, false, "drop is not restored"},
		{"pending MDS restart", []string{"osd-store", "osd-restart"}, persist, nil, &mdsCephFSRestart{State: "applied"}, nil, false, "restart is not adopted"},
		{"running reader", []string{"osd-store", "osd-restart"}, persist, nil, nil, nil, true, "native action refused"},
		{"second restart", []string{"osd-restart"}, persist, nil, nil, &mdsCephFSDataOSDRestart{State: "restarted"}, false, "already issued"},
		{"unproven restart", []string{"osd-store"}, persist, nil, nil, &mdsCephFSDataOSDRestart{State: "wait-uncertain"}, false, "native action refused"},
	} {
		for _, action := range tc.actions {
			x := &mdsCephFSDataScope{out: out, plan: mdsCephFSDataPlanFor(token), owned: map[string]uint64{}, current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(),
				osdDrop: tc.drop, restart: tc.restart, osdRestart: tc.osd}
			if tc.reader {
				x.reader = newMDSCephFSDataReaderRun(1)
			}
			c := &fixtureCluster{profile: tc.p, mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSData: x}
			var result mdsCephFSDataResult
			err := c.mdsCephFSDataAct(context.Background(), 1, mdsCephFSDataAction{name: action}, &result)
			if err == nil || !strings.Contains(err.Error(), tc.want) || result.OSDStore != nil || result.OSDRestart != nil || x.osdRestart != tc.osd {
				t.Fatal("OSD store or restart reached native work", tc.name, action, err)
			}
		}
	}
}

// An unproven OSD restart holds back every native action, OSD drop, reader
// and MDS fault; a proven one holds back nothing.
func TestMDSCephFSDataUnprovenOSDRestartRefusesOtherWork(t *testing.T) {
	for _, state := range []string{"intended", "stopped", "stop-uncertain", "started", "start-uncertain", "wait-uncertain"} {
		x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), osdRestart: &mdsCephFSDataOSDRestart{State: state}}
		refused := func(err error) bool { return err != nil && strings.Contains(err.Error(), "OSD restart") }
		if !refused(x.nativeReady()) || !refused(x.readerReady()) {
			t.Fatal("native work admitted during the OSD restart", state)
		}
		if _, err := x.newOSDDrop(1, "osd.0", map[string]string{"ms_blackhole_client": "false", "debug_ms": "1/5"}); !refused(err) {
			t.Fatal("an OSD drop admitted during the OSD restart", state, err)
		}
		// log-pause needs nothing else of the scope, so only the restart
		// refuses it.
		if _, err := x.newRuntime(1, "log-pause"); !refused(err) {
			t.Fatal("an MDS fault admitted during the OSD restart", state, err)
		}
	}
	x := &mdsCephFSDataScope{current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(), osdRestart: &mdsCephFSDataOSDRestart{State: "restarted"}}
	if x.nativeReady() != nil || x.readerReady() != nil {
		t.Fatal("a proven OSD restart still refuses native work")
	}
	if _, err := x.newOSDDrop(1, "osd.0", map[string]string{"ms_blackhole_client": "false", "debug_ms": "1/5"}); err != nil {
		t.Fatal("a proven OSD restart refuses an OSD drop", err)
	}
	if _, err := x.newRuntime(1, "log-pause"); err != nil {
		t.Fatal("a proven OSD restart refuses an MDS fault", err)
	}
}

// Only a BlueStore readback admits the restart: MemStore keeps its objects
// only through MemStore::umount, which OSD::shutdown skips under
// osd_fast_shutdown.
func TestMDSCephFSDataOSDRestartStoreAdmission(t *testing.T) {
	for _, tc := range []struct {
		store mdsCephFSDataOSDStore
		ok    bool
	}{
		{mdsCephFSDataOSDStore{"osd.0", "bluestore", "true"}, true},
		{mdsCephFSDataOSDStore{"osd.0", "bluestore", "false"}, true},
		{mdsCephFSDataOSDStore{"osd.0", "memstore", "true"}, false},
		{mdsCephFSDataOSDStore{"osd.0", "memstore", "false"}, false},
		{mdsCephFSDataOSDStore{"osd.0", "bluestore", ""}, false},
		{mdsCephFSDataOSDStore{"osd.0", "", "true"}, false},
	} {
		err := tc.store.restartable()
		if (err == nil) != tc.ok || err != nil && !strings.Contains(err.Error(), "osd_objectstore="+tc.store.ObjectStore+" ") {
			t.Fatal("store admission differs", tc.store, err)
		}
	}
}

func TestMDSCephFSDataOSDRestartViews(t *testing.T) {
	dump := []byte(`{"epoch":41,"osds":[{"osd":0,"up":1,"in":1,"up_from":40,"public_addr":"v2:192.0.2.1:6802/12"},{"osd":1,"up":0,"in":0,"up_from":3,"public_addr":""}]}`)
	state, err := parseMDSCephFSDataOSDDump(dump, 0)
	if err != nil || state != (mdsCephFSDataOSDState{Epoch: 41, Up: true, In: true, UpFrom: 40, PublicAddr: "v2:192.0.2.1:6802/12"}) {
		t.Fatal("OSD map entry differs", state, err)
	}
	for _, bad := range []string{`{"epoch":0,"osds":[]}`, `{"epoch":41,"osds":[{"osd":1,"up":1}]}`, `[]`} {
		if _, err := parseMDSCephFSDataOSDDump([]byte(bad), 0); err == nil {
			t.Fatal("OSD map output accepted", bad)
		}
	}
	pgs, active, reported, err := parseMDSCephFSDataPGStates([]byte(`{"pg_ready":true,"pg_stats":[{"pgid":"1.0","state":"active+clean","reported_epoch":42},{"pgid":"2.0","state":"peering","reported_epoch":40},{"pgid":"2.1","state":"active","reported_epoch":43}]}`))
	if err != nil || pgs != 3 || active != 2 || reported != 40 {
		t.Fatal("PG states differ", pgs, active, reported, err)
	}
	for _, bad := range []string{`{"pg_stats":[]}`, `{"pg_stats":[{"pgid":"","state":"active"}]}`, `[]`} {
		if _, _, _, err := parseMDSCephFSDataPGStates([]byte(bad)); err == nil {
			t.Fatal("PG listing accepted", bad)
		}
	}
	before := mdsCephFSDataOSDState{Epoch: 30, Up: true, In: true, UpFrom: 12}
	after := mdsCephFSDataOSDState{Epoch: 34, Up: true, In: true, UpFrom: 33, PGs: 5, ActivePGs: 5, ReportedFrom: 33}
	if !restartedOSD(before, after) {
		t.Fatal("a proven restart was not recognized")
	}
	for name, change := range map[string]func(*mdsCephFSDataOSDState){
		"still down":      func(s *mdsCephFSDataOSDState) { s.Up = false },
		"out":             func(s *mdsCephFSDataOSDState) { s.In = false },
		"same up_from":    func(s *mdsCephFSDataOSDState) { s.UpFrom = before.UpFrom },
		"same epoch":      func(s *mdsCephFSDataOSDState) { s.Epoch = before.Epoch },
		"inactive PG":     func(s *mdsCephFSDataOSDState) { s.ActivePGs = 4 },
		"stale PG report": func(s *mdsCephFSDataOSDState) { s.ReportedFrom = 32 },
		"no PG":           func(s *mdsCephFSDataOSDState) { s.PGs, s.ActivePGs = 0, 0 },
	} {
		s := after
		change(&s)
		if restartedOSD(before, s) {
			t.Fatal("an unproven restart was recognized:", name)
		}
	}
}

package main

import "testing"

func TestMgrServiceColdBootstrapProfileAndOracleBoundary(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_MGR_SERVICE": "1", "CEPH_MSGR_TEST_MGR_COUNT": "0", "CEPH_MSGR_TEST_RUN": mgrServiceColdBootstrapSelector}
	p, err := loadProfile(func(k string) string { return base[k] })
	if err != nil || !p.mgrServiceColdBootstrap() || p.osd != "0" || p.runtime != "container" {
		t.Fatal(p, err)
	}
	for mode := range nativeOwnedPoolModes {
		want := mode == "mgrstate" || mode == "servicewatch"
		if nativePoolModeAllowed(p, mode) != want {
			t.Fatal("MON-only profile changed oracle authority", mode)
		}
	}
	for key, value := range map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_RUN": "^TestCeph.*Integration$", "CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_SHORT_TICKETS": "1", "CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1"} {
		t.Run(key, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			env[key] = value
			if _, err := loadProfile(func(k string) string { return env[k] }); err == nil {
				t.Fatal("mixed cold bootstrap profile accepted", key, value)
			}
		})
	}
	ordinary, err := loadProfile(func(k string) string {
		if k == "CEPH_MSGR_TEST_MGR_COUNT" {
			return "0"
		}
		return ""
	})
	if err != nil || nativePoolModeAllowed(ordinary, "mgrstate") {
		t.Fatal("ordinary no-MGR profile gained native oracle access", err)
	}
}

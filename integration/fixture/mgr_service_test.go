package main

import "testing"

func TestMgrServiceProfileIsExclusiveAndSelectsItsTest(t *testing.T) {
	defaults := map[string]string{"CEPH_MSGR_TEST_MGR_SERVICE": "1", "CEPH_MSGR_TEST_OSD": "1"}
	for _, runtime := range []string{"host", "container"} {
		config := map[string]string{"CEPH_MSGR_TEST_RUNTIME": runtime}
		for k, v := range defaults {
			config[k] = v
		}
		p, err := loadProfile(func(k string) string { return config[k] })
		if err != nil || p.testRun != "^TestCephMgrServiceRegistrationIntegration$" ||
			p.containerEnvironment()["CEPH_MSGR_TEST_MGR_SERVICE"] != "1" || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_MGR_SERVICE"] != "1" {
			t.Fatal("exclusive selection or environment", p, err)
		}
	}
	for name, value := range map[string]string{
		"CEPH_MSGR_TEST_MGR_SERVICE": "yes", "CEPH_MSGR_TEST_OSD": "0", "CEPH_MSGR_TEST_MGR_COUNT": "0",
		"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8", "CEPH_MSGR_TEST_MON_PING": "1",
		"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "1", "CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_RBD": "1",
		"CEPH_MSGR_TEST_MON_FREEZE": "1", "CEPH_MSGR_TEST_AUTH_EPOCH": "1", "CEPH_MSGR_TEST_SHORT_TICKETS": "1",
		"CEPH_MSGR_TEST_OSD_SOAK": "4m", "CEPH_MSGR_TEST_RUN": "^TestCeph.*Integration$",
	} {
		config := map[string]string{}
		for k, v := range defaults {
			config[k] = v
		}
		config[name] = value
		if _, err := loadProfile(func(k string) string { return config[k] }); err == nil {
			t.Fatal("mixed MGR service profile accepted", name, value)
		}
	}
}

func TestMgrServiceOracleModeIsAdmitted(t *testing.T) {
	if !nativeOwnedPoolModes["servicewatch"] {
		t.Fatal("the native owned-pool oracle request validator rejects the servicewatch mode")
	}
}

package main

import "testing"

func TestRequestV9FixtureKeepsClosedOSDTopologyAndConsumerContract(t *testing.T) {
	env := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-request-v9-reply-v8"}
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || !p.osdPGV8() {
		t.Fatal(p, err)
	}
	if p.containerEnvironment()["CEPH_MSGR_TEST_OSD_PROFILE"] != env["CEPH_MSGR_TEST_OSD_PROFILE"] || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_PROFILE"] != env["CEPH_MSGR_TEST_OSD_PROFILE"] {
		t.Fatal("request-v9 profile identity lost")
	}
	config := bootstrapSections(t, fixtureBootstrapConfig(p))
	if config["global"]["osd_pool_default_flag_hashpspool"] != "true" || config["global"]["osd_crush_update_on_start"] != "false" || config["osd"]["osd_op_history_size"] != "1024" {
		t.Fatal("v9 fixture changed fixed native placement/auth observation recipe", config)
	}
	for _, key := range []string{"CEPH_MSGR_TEST_MDS_FRESH", "CEPH_MSGR_TEST_RBD"} {
		env[key] = "1"
		if _, err := loadProfile(func(k string) string { return env[k] }); err == nil {
			t.Fatal("v9 OSD acceptance silently expanded workflows", key)
		}
		delete(env, key)
	}
	env["CEPH_MSGR_TEST_OSD_REPLICAS"] = "3"
	if _, err := loadProfile(func(k string) string { return env[k] }); err == nil {
		t.Fatal("v9 fixture silently expanded to unverified three-OSD recipes")
	}
	delete(env, "CEPH_MSGR_TEST_OSD_REPLICAS")
	delete(env, "CEPH_MSGR_TEST_OSD")
	if _, err := loadProfile(func(k string) string { return env[k] }); err == nil {
		t.Fatal("v9 profile accepted without OSD")
	}
}

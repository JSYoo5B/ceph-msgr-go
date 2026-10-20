package main

import (
	"strings"
	"testing"
)

func TestNativeMonPingRejectsResetAndWrongIdentity(t *testing.T) {
	valid := `{"mon_status":{"name":"a","rank":0,"monmap":{"fsid":"owned","epoch":1,"mons":[{"name":"a","rank":0},{"name":"b","rank":1},{"name":"c","rank":2}]}}}`
	if err := validateNativeMonPing([]byte(valid), "a", 0, "owned"); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"", "null", "{}", valid + "{}", strings.Replace(valid, `"rank":0`, `"rank":1`, 1), strings.Replace(valid, `"fsid":"owned"`, `"fsid":"foreign"`, 1), strings.Replace(valid, `"name":"b"`, `"name":"a"`, 1), strings.Replace(valid, `"epoch":1`, `"epoch":0`, 1)} {
		if err := validateNativeMonPing([]byte(data), "a", 0, "owned"); err == nil {
			t.Fatalf("accepted empty/reset or mismatched native observation %q", data)
		}
	}
}

func TestMonPingProfileIsExplicitAndExclusive(t *testing.T) {
	for _, config := range []map[string]string{
		{"CEPH_MSGR_TEST_MON_PING": "1"},
		{"CEPH_MSGR_TEST_MON_PING": "1", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_KEY_TYPE": "aes"},
		{"CEPH_MSGR_TEST_MON_PING": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6"},
	} {
		p, err := loadProfile(func(name string) string { return config[name] })
		if err != nil || p.testRun != "^TestCephMonPingIntegration$" || p.containerEnvironment()["CEPH_MSGR_TEST_MON_PING"] != "1" || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_MON_PING"] != "1" {
			t.Fatal("native ping selector/environment", p, err)
		}
	}
	for _, extra := range []map[string]string{
		{"CEPH_MSGR_TEST_MON_PING": "yes"}, {"CEPH_MSGR_TEST_OSD": "1"}, {"CEPH_MSGR_TEST_MGR_COUNT": "0"}, {"CEPH_MSGR_TEST_EXPIRE_TICKETS": "1"}, {"CEPH_MSGR_TEST_AUTH_EPOCH": "1"}, {"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1"}, {"CEPH_MSGR_STRESS_DURATION": "3m"}, {"CEPH_MSGR_TEST_MON_FREEZE": "1"}, {"CEPH_MSGR_TEST_RUN": "^TestCeph.*Integration$"},
	} {
		config := map[string]string{"CEPH_MSGR_TEST_MON_PING": "1"}
		for name, value := range extra {
			config[name] = value
		}
		if _, err := loadProfile(func(name string) string { return config[name] }); err == nil {
			t.Fatal("accepted mixed MON ping fixture", config)
		}
	}
}

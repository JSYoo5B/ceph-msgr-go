package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func mdsCommandsProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_COMMANDS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSCommandsClosedProfileAndSeparateCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsCommandsProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSCommandsIntegration$" || p.mdsCommands != "1" || p.osdPGV8() || !p.mdsFreshOSDAllowed() {
			t.Fatal("D2 did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		p.mdsFreshIdentity, p.mdsCommandsIdentity = "client.readonly-owned", "client.commands-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_IDENTITY"] != "client.test" || x["CEPH_MSGR_KEY_FILE"] != filepath.Join(dir, "key") || x["CEPH_MSGR_MDS_FRESH_IDENTITY"] != p.mdsFreshIdentity || x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != p.mdsCommandsIdentity || x["CEPH_MSGR_MDS_COMMANDS_KEY_FILE"] != filepath.Join(dir, "mds-commands.key") || x["CEPH_MSGR_MDS_FRESH_KEY_FILE"] != filepath.Join(dir, "mds-fresh.key") || x["CEPH_MSGR_TEST_MDS_COMMANDS"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_COMMANDS"] != "1" {
				t.Fatal("D2 full/read-only credentials were mixed or lost", x)
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "true"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSFreshIntegration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCeph(MDSFresh|MDSCommands)Integration$"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		{"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_SPARSE_READ_ORACLE": "on"},
	} {
		env := mdsCommandsProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe D2 profile accepted", change, p, err)
		}
	}
}

func TestMDSCommandsPoolAndCapabilityContracts(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	commands := mdsCommandsCaps()
	if commands.Mon != "allow r" || commands.MDS != "allow *" || commands.MGR != "" || commands.OSD != "" || strings.Contains(commands.MDS, "fsname") {
		t.Fatal("command capability is not the explicit native allow_all contract")
	}
	for _, enabled := range []string{"0", "1"} {
		p := profile{mdsCommands: enabled}
		config := mdsFreshFilesystemConfig(p, name)
		if config.MetadataPool.PGNum != 16 || config.DataPool.PGNum != 1 || config.MetadataPool.Name != name+"-meta" || config.DataPool.Name != name+"-data" || config.MetadataPool.Replicas != 1 || config.DataPool.Replicas != 1 || config.MetadataPool.NoHashPSPool != (enabled == "1") || config.DataPool.NoHashPSPool != (enabled == "1") {
			t.Fatal("D2 requires explicit no-HASHPSPOOL pools; existing M0 must retain its profile", config)
		}
	}
	sections := bootstrapSections(t, fixtureBootstrapConfig(profile{mdsFresh: "1", mdsCommands: "1", osd: "1", osdProfile: "legacy-v6"}))
	if sections["global"]["osd_pool_default_flag_hashpspool"] != "false" {
		t.Fatal("MDS-only public admission fixture inherited HASHPSPOOL", sections)
	}
}

func TestMDSCommandsNativeControlIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, action := range []string{"version", "invalid", "readonly-version"} {
		id, got, err := parseMDSNativeCommand([]string{"10", token, action}, token, 9)
		wantCommand, wantIdentity := mdsCommandVersionJSON, "client.mds-commands-"+token
		if action == "invalid" {
			wantCommand = mdsCommandInvalidJSON
		}
		if action == "readonly-version" {
			wantIdentity = "client.mds-fresh-" + token
		}
		if err != nil || id != 10 || got.command != wantCommand || got.identity != wantIdentity || !strings.HasPrefix(got.keyring, "/out/mds-") {
			t.Fatal("native action scope or exact command changed", got, err)
		}
	}
	for _, fields := range [][]string{
		{"10", token, "shutdown"}, {"10", token, "version extra"}, {"10", token, `{"prefix":"version"}`},
		{"10", "foreign", "version"}, {"10", token}, {"10", token, "version", "outside"},
		{"0", token, "version"}, {"010", token, "version"}, {"9223372036854775808", token, "version"},
	} {
		if _, _, err := parseMDSNativeCommand(fields, token, 0); err == nil {
			t.Fatal("unowned or caller-selected native command admitted", fields)
		}
	}
	if _, _, err := parseMDSNativeCommand([]string{"10", token, "version"}, token, 10); err == nil {
		t.Fatal("replayed native request admitted")
	}
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-commands-native"), []byte("10 "+token+" version\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsCommands: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned native MDS command") {
			t.Fatal("unowned native action reached an external call", err)
		}
	}
}

func TestMDSCommandsObserverPairPreservesOldFreshBinding(t *testing.T) {
	token := strings.Repeat("a1", 16)
	id, ids, err := parseMDSCommandsObservation([]string{"10", token, "42", "43"}, token, 9, 42, 43)
	if err != nil || id != 10 || !reflect.DeepEqual(ids, []uint64{42, 43}) {
		t.Fatal("full/read-only client pair lost", id, ids, err)
	}
	for _, fields := range [][]string{
		{"10", token, "42"}, {"10", token, "42", "42"}, {"10", token, "42", "0"},
		{"10", token, "42", "+43"}, {"10", token, "42", "18446744073709551616"},
		{"10", token, "42", "43", "44"}, {"10", token, "43", "42"},
	} {
		if _, _, err := parseMDSCommandsObservation(fields, token, 9, 42, 43); err == nil {
			t.Fatal("different or unbounded observer pair admitted", fields)
		}
	}
	if _, _, err := parseMDSFreshObservation([]string{"10", token, "42", "43"}, token, 9, 42); err == nil {
		t.Fatal("old fresh observer silently accepted the D2 protocol")
	}
}

func TestMDSCommandsNativeOutputPreservesSignedBinaryResults(t *testing.T) {
	for _, code := range []int32{0, -13, -22} {
		status := "permission denied"
		data, _ := json.Marshal(map[string]any{"code": code, "data_hex": "00ff806d647300", "status": status, "status_utf8_hex": hex.EncodeToString([]byte(status)), "native_global_id": 44})
		var result mdsNativeCommandResult
		if err := decodeMDSNativeCommandOutput(data, &result); err != nil || result.Code != code || result.Data != "00ff806d647300" || result.Status != status || result.NativeGlobalID != 44 {
			t.Fatal("native signed code, raw bytes or status changed", result, err)
		}
	}
	for _, data := range []string{
		`{}`, `{"code":0}`, `{"code":2147483648,"data_hex":"","status":"","status_utf8_hex":"","native_global_id":1}`,
		`{"code":0,"data_hex":"zz","status":"","status_utf8_hex":"","native_global_id":1}`,
		`{"code":0,"data_hex":"00","status":"permission denied","status_utf8_hex":"00","native_global_id":1}`,
		`{"code":0,"data_hex":"","status":"","status_utf8_hex":"","native_global_id":0}`,
		`{"code":0,"data_hex":"","status":"","native_global_id":1}`, strings.Repeat("x", (64<<10)+1),
	} {
		var result mdsNativeCommandResult
		if err := decodeMDSNativeCommandOutput([]byte(data), &result); err == nil {
			t.Fatal("invalid or unbounded native command result admitted", data[:min(80, len(data))])
		}
	}
}

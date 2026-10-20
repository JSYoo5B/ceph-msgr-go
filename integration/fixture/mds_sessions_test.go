package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func mdsSessionsProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_SESSIONS": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestMDSSessionsClosedProfileAndForeignCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := mdsSessionsProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephMDSSessionsIntegration$" || p.mdsSessions != "1" || p.mdsCommands != "0" || p.osdPGV8() || !p.mdsFreshOSDAllowed() || !p.mdsPublicClient() || p.cleanupBudget() != 2*time.Minute {
			t.Fatal("MDS sessions did not select its isolated no-HASHPSPOOL fixture", p, err)
		}
		p.mdsFreshIdentity, p.mdsSessionsForeignIdentity = "client.readonly-owned", "client.foreign-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_IDENTITY"] != "client.test" || x["CEPH_MSGR_MDS_FRESH_IDENTITY"] != p.mdsFreshIdentity || x["CEPH_MSGR_MDS_FRESH_KEY_FILE"] != filepath.Join(dir, "mds-fresh.key") ||
				x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] != p.mdsSessionsForeignIdentity || x["CEPH_MSGR_MDS_SESSIONS_FOREIGN_KEY_FILE"] != filepath.Join(dir, "mds-sessions-foreign.key") ||
				x["CEPH_MSGR_TEST_MDS_SESSIONS"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_SESSIONS"] != "1" || x["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] != "" {
				t.Fatal("MDS sessions credentials were mixed or lost", x)
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "0"}, {"CEPH_MSGR_TEST_MDS_SESSIONS": "true"}, {"CEPH_MSGR_TEST_MDS_COMMANDS": "1"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSFreshIntegration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCeph(MDSFresh|MDSSessions)Integration$"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		{"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_SPARSE_READ_ORACLE": "on"},
	} {
		env := mdsSessionsProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS sessions profile accepted", change, p, err)
		}
	}
	if (profile{}).cleanupBudget() != time.Minute {
		t.Fatal("session cleanup budget leaked into other profiles")
	}
}

func TestMDSSessionsPoolAndCapabilityContracts(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("ab", 16)
	config := mdsFreshFilesystemConfig(profile{mdsSessions: "1"}, name)
	if !config.MetadataPool.NoHashPSPool || !config.DataPool.NoHashPSPool || config.MetadataPool.PGNum != 16 || config.DataPool.PGNum != 1 {
		t.Fatal("MDS-only session clients require the legacy no-HASHPSPOOL pools", config)
	}
	foreign := mdsSessionsForeignCaps("ceph-msgr-foreign-" + strings.Repeat("ab", 16))
	if foreign.Mon != "allow r" || foreign.MDS != "allow r fsname=ceph-msgr-foreign-"+strings.Repeat("ab", 16) || foreign.MGR != "" || foreign.OSD != "" || strings.Contains(foreign.MDS, name) {
		t.Fatal("foreign identity must hold only an MDS read grant for a different filesystem", foreign)
	}
	if mdsSessionsStaleWindow.Timeout != 30*time.Second || mdsSessionsStaleWindow.Autoclose != 120*time.Second {
		t.Fatal("stale window left the native minimum or shortened autoclose", mdsSessionsStaleWindow)
	}
	key, err := mdsSessionsKeyringSecret([]byte("[client.x]\n\tkey = QUJD\n\tcaps mon = \"allow r\"\n"))
	if err != nil || key != "QUJD" {
		t.Fatal("keyring secret lost", key, err)
	}
	for _, keyring := range []string{"[client.x]\n", "key = a\nkey = b\n"} {
		if _, err := mdsSessionsKeyringSecret([]byte(keyring)); err == nil {
			t.Fatal("missing or ambiguous key accepted", keyring)
		}
	}
}

func TestMDSSessionsRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, tc := range []struct {
		fields []string
		name   string
		gids   []uint64
	}{
		{[]string{"10", token, "observe", "42"}, "observe", []uint64{42}},
		{[]string{"10", token, "observe", "42", "43"}, "observe", []uint64{42, 43}},
		{[]string{"10", token, "evict", "42"}, "evict", []uint64{42}},
		{[]string{"10", token, "refuse-on"}, "refuse-on", nil},
		{[]string{"10", token, "refuse-off"}, "refuse-off", nil},
		{[]string{"10", token, "require-on"}, "require-on", nil},
		{[]string{"10", token, "require-off"}, "require-off", nil},
		{[]string{"10", token, "stale-on"}, "stale-on", nil},
		{[]string{"10", token, "stale-off"}, "stale-off", nil},
	} {
		id, action, err := parseMDSSessionsRequest(tc.fields, token, 9)
		if err != nil || id != 10 || action.name != tc.name || !reflect.DeepEqual(action.gids, tc.gids) {
			t.Fatal("closed MDS sessions action changed", tc.fields, id, action, err)
		}
	}
	for _, fields := range [][]string{
		{"10", token}, {"10", "foreign", "observe", "42"}, {"10", token, "observe"},
		{"10", token, "observe", "1", "2", "3", "4", "5", "6", "7", "8", "9"},
		{"10", token, "observe", "42", "42"}, {"10", token, "observe", "0"}, {"10", token, "observe", "+42"},
		{"10", token, "observe", "042"}, {"10", token, "observe", "18446744073709551616"},
		{"10", token, "evict"}, {"10", token, "evict", "42", "43"}, {"10", token, "refuse-on", "42"},
		{"10", token, "fs", "set"}, {"10", token, "osd", "blocklist", "rm"}, {"10", token, "stale-on", "60"},
		{"0", token, "observe", "42"}, {"010", token, "observe", "42"}, {"-1", token, "observe", "42"},
	} {
		if _, _, err := parseMDSSessionsRequest(fields, token, 0); err == nil {
			t.Fatal("unowned, unbounded or caller-selected session action admitted", fields)
		}
	}
	if _, _, err := parseMDSSessionsRequest([]string{"10", token, "observe", "42"}, token, 10); err == nil {
		t.Fatal("replayed MDS sessions request admitted")
	}
}

func mdsSessionsNativeSample(address string, nonce uint32) map[string]any {
	return map[string]any{
		"id": 4321, "auth_name": map[string]any{"type": 8, "type_str": "client", "id": "mds-fresh-owned"},
		"entity": map[string]any{"name": map[string]any{"type": "client", "num": 4321}, "addr": map[string]any{"type": "v2", "addr": address, "nonce": nonce}},
		"state":  "open", "num_leases": 0, "num_caps": 0, "uptime": 1.5, "inst": "client.4321 v2:" + address,
		"completed_requests": []any{}, "prealloc_inos": []any{},
		"client_metadata": map[string]any{
			"client_features": map[string]any{"feature_bits": "0x00000000000001ff"},
			"metric_spec":     map[string]any{"metric_flags": map[string]any{"feature_bits": "0x0"}},
			"entity_id":       "mds-fresh-owned", "hostname": "ceph-msgr-go-a1a1a1a1", "root": "/",
		},
	}
}

func TestMDSSessionsNativeListingPreservesComparedFields(t *testing.T) {
	for _, tc := range []struct{ addr, want string }{{"172.18.0.9:0", "172.18.0.9:0/123456"}, {"[fd62:93fe::9]:0", "[fd62:93fe::9]:0/123456"}} {
		data, _ := json.Marshal([]any{mdsSessionsNativeSample(tc.addr, 123456)})
		got, err := parseMDSSessionsList(data)
		want := []mdsSessionsNative{{ID: 4321, State: "open", AuthType: "client", AuthID: "mds-fresh-owned", EntityType: "client", AddressType: "v2", Address: tc.want,
			ClientFeatures: "0x00000000000001ff", MetricFlags: "0x0", Metadata: map[string]string{"entity_id": "mds-fresh-owned", "hostname": "ceph-msgr-go-a1a1a1a1", "root": "/"}}}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("native session identity, address or metadata changed", got, err)
		}
	}
	if got, err := parseMDSSessionsList([]byte("[]")); err != nil || len(got) != 0 || got == nil {
		t.Fatal("empty native listing must remain an empty owned observation", got, err)
	}
	for name, change := range map[string]func(map[string]any){
		"zero id":         func(m map[string]any) { m["id"] = 0 },
		"entity mismatch": func(m map[string]any) { m["entity"].(map[string]any)["name"].(map[string]any)["num"] = 4322 },
		"non-client":      func(m map[string]any) { m["entity"].(map[string]any)["name"].(map[string]any)["type"] = "mds" },
		"no state":        func(m map[string]any) { m["state"] = "" },
		"no caps":         func(m map[string]any) { delete(m, "num_caps") },
		"no metadata":     func(m map[string]any) { delete(m, "client_metadata") },
		"dns address": func(m map[string]any) {
			m["entity"].(map[string]any)["addr"].(map[string]any)["addr"] = "client.example:0"
		},
		"no nonce": func(m map[string]any) { delete(m["entity"].(map[string]any)["addr"].(map[string]any), "nonce") },
		"no features": func(m map[string]any) {
			delete(m["client_metadata"].(map[string]any), "client_features")
		},
		"bad metric": func(m map[string]any) {
			m["client_metadata"].(map[string]any)["metric_spec"] = map[string]any{"metric_flags": "0x0"}
		},
		"non-string value": func(m map[string]any) { m["client_metadata"].(map[string]any)["root"] = 7 },
	} {
		sample := mdsSessionsNativeSample("172.18.0.9:0", 1)
		change(sample)
		data, _ := json.Marshal([]any{sample})
		if _, err := parseMDSSessionsList(data); err == nil {
			t.Fatal("ambiguous native session entry accepted", name)
		}
	}
	duplicate, _ := json.Marshal([]any{mdsSessionsNativeSample("172.18.0.9:0", 1), mdsSessionsNativeSample("172.18.0.9:0", 2)})
	for _, data := range [][]byte{duplicate, []byte("null"), []byte("{}"), []byte(strings.Repeat(" ", mdsSessionsJSONLimit+1))} {
		if _, err := parseMDSSessionsList(data); err == nil {
			t.Fatal("duplicate, absent or unbounded native listing accepted")
		}
	}
}

func TestMDSSessionsNativeMapAndDaemonReadbacks(t *testing.T) {
	name := "ceph-msgr-fresh-owned"
	sample := func(edit func(m map[string]any)) []byte {
		mds := map[string]any{"epoch": 9, "fs_name": name, "flags_state": map[string]any{"joinable": true, "refuse_client_session": true}, "required_client_features": map[string]any{"feature_9": "reply_encoding"}, "session_timeout": 30, "session_autoclose": 120}
		fs := map[string]any{"id": 1, "mdsmap": mds}
		if edit != nil {
			edit(fs)
		}
		data, _ := json.Marshal(fs)
		return data
	}
	got, err := parseMDSSessionsMap(sample(nil), 1, name)
	if err != nil || !reflect.DeepEqual(got, mdsSessionsMap{Epoch: 9, Refuse: true, Required: map[string]string{"feature_9": "reply_encoding"}, Timeout: 30, Autoclose: 120}) {
		t.Fatal("native session map fields changed", got, err)
	}
	for label, edit := range map[string]func(map[string]any){
		"foreign FSCID":  func(m map[string]any) { m["id"] = 2 },
		"foreign name":   func(m map[string]any) { m["mdsmap"].(map[string]any)["fs_name"] = "foreign" },
		"no flags":       func(m map[string]any) { delete(m["mdsmap"].(map[string]any), "flags_state") },
		"no refuse flag": func(m map[string]any) { m["mdsmap"].(map[string]any)["flags_state"] = map[string]any{} },
		"no features":    func(m map[string]any) { delete(m["mdsmap"].(map[string]any), "required_client_features") },
		"no timeout":     func(m map[string]any) { delete(m["mdsmap"].(map[string]any), "session_timeout") },
		"zero epoch":     func(m map[string]any) { m["mdsmap"].(map[string]any)["epoch"] = 0 },
	} {
		if _, err := parseMDSSessionsMap(sample(edit), 1, name); err == nil {
			t.Fatal("native map without owned session fields accepted", label)
		}
	}
	if epoch, err := parseMDSSessionsDaemonEpoch([]byte(`{"cluster_fsid":"x","whoami":0,"state":"up:active","mdsmap_epoch":12}`)); err != nil || epoch != 12 {
		t.Fatal("daemon map epoch lost", epoch, err)
	}
	for _, data := range []string{`{"state":"up:active"}`, `{"state":"up:replay","mdsmap_epoch":12}`, `{"state":"up:active","mdsmap_epoch":0}`, `[]`} {
		if _, err := parseMDSSessionsDaemonEpoch([]byte(data)); err == nil {
			t.Fatal("inactive or incomplete daemon status accepted", data)
		}
	}
	if value, err := parseMDSSessionsDaemonConfig([]byte(`{"mds_defer_session_stale":"false"}`), mdsSessionsDeferOption); err != nil || value != "false" {
		t.Fatal("daemon runtime option lost", value, err)
	}
	for _, data := range []string{`{}`, `{"mds_defer_session_stale":"0"}`, `{"other":"true"}`, `{"mds_defer_session_stale":"true","other":"true"}`, `"true"`} {
		if _, err := parseMDSSessionsDaemonConfig([]byte(data), mdsSessionsDeferOption); err == nil {
			t.Fatal("ambiguous daemon runtime option accepted", data)
		}
	}
	if got := mdsSessionsFindBlock([]mdsSessionsBlock{{Address: "10.0.0.0/24", Range: true}, {Address: "172.18.0.9:0/1"}}, "172.18.0.9:0/1"); got == nil || got.Range {
		t.Fatal("exact blocklist entry lookup lost")
	}
	if mdsSessionsFindBlock([]mdsSessionsBlock{{Address: "172.18.0.9:0/1", Range: true}}, "172.18.0.9:0/1") != nil {
		t.Fatal("range entry matched an exact session address")
	}
}

func TestMDSSessionsControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-sessions"), []byte("1 "+strings.Repeat("a1", 16)+" refuse-on\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled, mdsSessions: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS sessions control scope") {
			t.Fatal("unowned session mutation reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned session mutation produced an acknowledgement or receipt", entries)
		}
	}
}

func TestMDSSessionsCleanupWithoutBaselineMutatesNothing(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, intent := range []bool{false, true} {
		out := t.TempDir()
		c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsSessions: &mdsSessionsScope{out: out, identityCreateIntent: intent}}
		err := c.finishMDSSessions(context.Background())
		var receipt mdsSessionsCleanup
		data, readErr := os.ReadFile(filepath.Join(out, "mds-sessions-cleanup.json"))
		if readErr != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Attempted {
			t.Fatal("cleanup receipt was not retained", readErr)
		}
		if intent {
			// A recorded identity intent without a provider handle is an unknown
			// creation outcome. It stays an explicit failure, never success.
			if err == nil || receipt.Completed || receipt.IdentityRemoved {
				t.Fatal("unknown identity creation was reported as removed", err, receipt)
			}
			continue
		}
		if err != nil || !receipt.Completed || len(receipt.RemovedBlocklist) != 0 {
			t.Fatal("cleanup before any owned mutation did not complete trivially", err, receipt)
		}
	}
}

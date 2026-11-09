package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRBDMirrorMarkerOnlyReachesExclusiveTestEnvironment(t *testing.T) {
	// A caller-supplied marker cannot enable a broad suite or another row.
	t.Setenv(rbdMirrorMarker, "1")
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1", rbdMirrorMarker: "1"}
	for _, runtime := range []string{"host", "container"} {
		for _, selector := range []string{rbdMirrorSelector, "", "^TestCeph.*Integration$", "^TestCeph(OSD|MonOSDMaps).*Integration$", "^TestCephRBDMirrorPathRecoveryIntegration", copyFromLateErrorSelector} {
			t.Run(runtime+"/"+selector, func(t *testing.T) {
				p, err := loadProfile(func(key string) string {
					switch key {
					case "CEPH_MSGR_TEST_RUNTIME":
						return runtime
					case "CEPH_MSGR_TEST_RUN":
						return selector
					}
					return base[key]
				})
				if err != nil {
					t.Fatal(err)
				}
				want := ""
				if selector == rbdMirrorSelector {
					want = "1"
				}
				inputs := p.testEnvironment("/out", "")
				if inputs[rbdMirrorMarker] != want || p.rbdMirrorSelected() != (want == "1") {
					t.Fatal("RBD mirror marker did not match exclusive selection", selector)
				}
				env := envList(inputs)
				if runtime == "host" {
					env = environment(inputs, true)
				}
				count := 0
				for _, field := range env {
					if strings.HasPrefix(field, rbdMirrorMarker+"=") {
						count++
						if field != rbdMirrorMarker+"="+want {
							t.Fatal("inherited marker enabled an unrelated suite", field)
						}
					}
				}
				if count != 1 {
					t.Fatal("test marker was absent or duplicated", count)
				}
				if _, leaked := p.containerEnvironment()[rbdMirrorMarker]; leaked {
					t.Fatal("RBD mirror marker leaked to daemon environment")
				}
			})
		}
	}
}

func TestRBDMirrorSelectionRequiresItsFixtureShape(t *testing.T) {
	selected := profile{osd: "1", osdStore: "bluestore", osdReplicas: "1", osdProfile: "legacy-v6", ipFamily: "4", mappedIPv6: "0", rbdBase: "0", mdsFresh: "0", osdCachePools: "0", mgrService: "0", testRun: rbdMirrorSelector}
	if !selected.rbdMirrorSelected() {
		t.Fatal("the closed single-BlueStore IPv4 profile is not selected")
	}
	for name, change := range map[string]func(*profile){
		"memstore":   func(p *profile) { p.osdStore = "memstore" },
		"three OSDs": func(p *profile) { p.osdReplicas = "3" },
		"pg v8":      func(p *profile) { p.osdProfile = "replicated-pg-v8" },
		"IPv6":       func(p *profile) { p.ipFamily = "6" },
		"RBD base":   func(p *profile) { p.rbdBase = "1" },
		"MDS":        func(p *profile) { p.mdsFresh = "1" },
		"soak":       func(p *profile) { p.osdSoak = "1m" },
		"selector":   func(p *profile) { p.testRun = "^TestCeph.*Integration$" },
		"no OSD":     func(p *profile) { p.osd = "0" },
	} {
		p := selected
		change(&p)
		if p.rbdMirrorSelected() {
			t.Fatal("RBD mirror selected for", name)
		}
	}
	if selected.cleanupBudget() < 3*60e9 {
		t.Fatal("RBD mirror cleanup budget is shorter than its owned teardown")
	}
}

func TestRBDMirrorControlRequiresExclusiveSelector(t *testing.T) {
	for _, p := range []profile{{}, {osd: "1", osdStore: "bluestore", osdReplicas: "1", osdProfile: "legacy-v6", ipFamily: "4", testRun: ".*"}} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "rbd-mirror"), []byte("1 start\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: p}
		if err := c.controlRBDMirrorRequest(context.Background(), out); err == nil {
			t.Fatal("nonexclusive RBD mirror checkpoint admitted")
		}
		if c.rbdMirror != nil {
			t.Fatal("refused request created an owned scope")
		}
	}
}

func TestRBDMirrorControlEnforcesPhaseOrder(t *testing.T) {
	p := profile{osd: "1", osdStore: "bluestore", osdReplicas: "1", osdProfile: "legacy-v6", ipFamily: "4", mappedIPv6: "0", rbdBase: "0", mdsFresh: "0", osdCachePools: "0", mgrService: "0", testRun: rbdMirrorSelector}
	for _, tc := range []struct {
		scope  *rbdMirrorScope
		action string
	}{
		{nil, "cut"},
		{nil, "restore"},
		{nil, "finish"},
		{&rbdMirrorScope{receipt: rbdMirrorFixtureReceipt{Phase: "start"}}, "start"},
		{&rbdMirrorScope{receipt: rbdMirrorFixtureReceipt{Phase: "start"}}, "restore"},
		{&rbdMirrorScope{receipt: rbdMirrorFixtureReceipt{Phase: "start", Error: "cut: failed"}}, "cut"},
		{&rbdMirrorScope{receipt: rbdMirrorFixtureReceipt{Phase: "restore"}, finished: true}, "finish"},
		{&rbdMirrorScope{receipt: rbdMirrorFixtureReceipt{Phase: "start"}}, "replay"},
	} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "rbd-mirror"), []byte("7 "+tc.action+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: p, rbdMirror: tc.scope}
		if err := c.controlRBDMirrorRequest(context.Background(), out); err == nil {
			t.Fatalf("action %s admitted after %+v", tc.action, tc.scope)
		}
		if _, err := os.Stat(filepath.Join(out, "rbd-mirror-done.7")); err == nil {
			t.Fatal("refused action was acknowledged")
		}
	}
}

func TestRBDMirrorSessionProjection(t *testing.T) {
	raw := []byte(`{"name":"client","messenger":{"my_addrs":{"addrvec":[]},"connections":[
{"async_connection":{"state":"STATE_CONNECTION_ESTABLISHED","status":{"connected":true},"peer":{"type":"client","id":4410,"global_id":4410,"addr":{"addrvec":[{"type":"any","addr":"172.18.0.9:0","nonce":7}]}},"protocol":{"v2":{"state":"READY"}}}},
{"async_connection":{"state":"STATE_CONNECTION_ESTABLISHED","status":{"connected":true},"peer":{"type":"client","id":4207,"global_id":4207,"addr":{"addrvec":[{"type":"any","addr":"172.18.0.4:0","nonce":9}]}},"protocol":{"v2":{"state":"READY"}}}}],
"anon_conns":[{"state":"STATE_CLOSED","status":{"connected":false},"peer":{"type":"mon","id":-1,"global_id":0,"addr":{"addrvec":[{"type":"v2","addr":"0.0.0.0:0","nonce":0}]}}}]}}`)
	sessions, err := parseRBDMirrorSessions(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 3 || sessions[0].Address != "" || sessions[1].Address != "172.18.0.4" || sessions[1].GlobalID != 4207 || sessions[1].State != "READY" || !sessions[1].Connected || sessions[2].Address != "172.18.0.9" {
		t.Fatalf("sessions: %+v", sessions)
	}
	if !rbdMirrorPeerSession(sessions, "172.18.0.9") || rbdMirrorPeerSession(sessions, "172.18.0.5") {
		t.Fatal("peer session attribution differs")
	}
	wrapped := append(append([]byte(`{"status":`), raw...), '}')
	if again, err := parseRBDMirrorSessions(wrapped); err != nil || len(again) != 3 {
		t.Fatal("tell-wrapped dump", err, again)
	}
	if _, err := parseRBDMirrorSessions([]byte(`{"name":"client"}`)); err == nil {
		t.Fatal("dump without a messenger admitted")
	}
}

func TestRBDMirrorMonSessionAttribution(t *testing.T) {
	raw := []byte(`[{"name":"client.4410","entity_name":"client.rbd-mirror-peer","addrs":{"addrvec":[{"type":"any","addr":"172.18.0.9:0","nonce":7}]},"socket_addr":{"type":"any","addr":"172.18.0.9:43120","nonce":7},"global_id":4410},
{"name":"client.4207","entity_name":"client.admin","addrs":{"addrvec":[{"type":"any","addr":"172.18.0.4:0","nonce":9}]},"socket_addr":{"type":"any","addr":"172.18.0.4:40000","nonce":9},"global_id":4207}]`)
	found, err := parseRBDMirrorMonSessions("a", raw, netip.MustParseAddr("172.18.0.9"))
	if err != nil || len(found) != 1 || found[0].Entity != "client.rbd-mirror-peer" || found[0].GlobalID != 4410 || found[0].Monitor != "a" {
		t.Fatal("MON session attribution", err, found)
	}
	if _, err := parseRBDMirrorMonSessions("a", []byte(`{}`), netip.MustParseAddr("172.18.0.9")); err == nil {
		t.Fatal("malformed MON sessions admitted")
	}
}

func TestRBDMirrorProgressAndDiff(t *testing.T) {
	p, err := parseRBDMirrorProgress("up+replaying", `replaying, {"bytes_per_second":0.0,"last_snapshot_bytes":1048576,"local_snapshot_timestamp":1700000002,"remote_snapshot_timestamp":1700000002,"replay_state":"idle"}`)
	if err != nil || p.State != "up+replaying" || p.Local != p.Remote || p.LastSnapshotBytes != 1<<20 || p.ReplayState != "idle" {
		t.Fatal("progress", err, p)
	}
	if _, err := parseRBDMirrorProgress("up+starting_replay", "starting replay"); err == nil {
		t.Fatal("status without snapshot progress admitted")
	}
	patch := []byte{1, 2, 3}
	diff := rbdMirrorDiff(8, 4, patch)
	if !bytes.HasPrefix(diff, []byte("rbd diff v1\ns")) || binary.LittleEndian.Uint64(diff[13:]) != 8 || diff[21] != 'w' ||
		binary.LittleEndian.Uint64(diff[22:]) != 4 || binary.LittleEndian.Uint64(diff[30:]) != 3 || !bytes.Equal(diff[38:41], patch) || diff[41] != 'e' || len(diff) != 42 {
		t.Fatalf("diff stream %q", diff)
	}
	if digest, err := rbdMirrorParseDigest([]byte(rbdMirrorDigest(patch) + "  -\n")); err != nil || digest != rbdMirrorDigest(patch) {
		t.Fatal("export digest", err, digest)
	}
	if _, err := rbdMirrorParseDigest([]byte("\n")); err == nil {
		t.Fatal("empty export digest admitted")
	}
	before := rbdMirrorPayload(16, rbdMirrorSeedBefore)
	if before[0] != byte(rbdMirrorSeedBefore%251+1) || before[1] != byte((37+rbdMirrorSeedBefore)%251+1) {
		t.Fatal("payload formula differs", before)
	}
}

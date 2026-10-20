package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func TestShortTicketProfileUsesNativeSurvivableFractionalAuthTTL(t *testing.T) {
	auth, service := fixtureTicketTTLs(profile{shortTickets: "1"})
	if auth != 6500*time.Millisecond || service != 12*time.Second {
		t.Fatal("short-ticket TTLs changed", auth, service)
	}
	// Fractional, still below the MGR ticket, and long enough that a native
	// MonClient's quarter-validity renewal outlives its one-second tick.
	if auth%time.Second == 0 || auth >= service || auth/4 < 1500*time.Millisecond {
		t.Fatal("short-ticket AUTH TTL must stay fractional and native-survivable", auth)
	}
	for _, test := range []struct {
		p             profile
		auth, service time.Duration
	}{
		{profile{}, 12 * time.Second, 12 * time.Second},
		{profile{monFreeze: "0"}, 12 * time.Second, 12 * time.Second},
		{profile{monFreeze: "1"}, 120 * time.Second, 12 * time.Second},
		{profile{monFreeze: "1", shortTickets: "1"}, 6500 * time.Millisecond, 12 * time.Second},
		{profile{authEpoch: "1"}, 120 * time.Second, 120 * time.Second},
		{profile{idleSessions: "1"}, 120 * time.Second, 120 * time.Second},
	} {
		if auth, service := fixtureTicketTTLs(test.p); auth != test.auth || service != test.service {
			t.Fatal("ordinary ticket TTLs changed", test.p, auth, service)
		}
	}
}

func gateState(active string, activeGID uint64, standby string, standbyGID uint64) managerPreparationMap {
	state := managerPreparationMap{Epoch: 9}
	state.Available, state.ActiveName, state.ActiveGID = true, active, activeGID
	if standby != "" {
		state.Standbys = append(state.Standbys, struct {
			Name string `json:"name"`
			GID  uint64 `json:"gid"`
		}{standby, standbyGID})
	}
	return state
}

func TestNativeCrashClassifiesManagerAuthServerSegfault(t *testing.T) {
	report := &ceph.CrashMetadataReport{Containers: []ceph.CrashMetadataContainer{
		{Role: "client", Name: "messenger-oracles", Records: []ceph.CrashRecord{{Entity: "client.test", CrashID: "ignored"}}},
		{Role: "mgr", Name: "a"},
		{Role: "mgr", Name: "b", Records: []ceph.CrashRecord{{
			Entity: "mgr.b", Process: "ceph-mgr", CrashID: "2026-10-15T12:10:28.962525Z_abc", Timestamp: "2026-10-15T12:10:28.962525Z",
			Fields: map[string]any{"backtrace": []string{
				"/lib64/libc.so.6(+0x3fb30) [0x7efd6d347b30]",
				"(ProtocolV2::handle_auth_request(ceph::buffer::v15_2_0::list&)+0x2ca) [0x7efd6e1ddbfa]",
				"(ProtocolV2::run_continuation(Ct<ProtocolV2>&)+0x39) [0x7efd6e1cc9e9]",
			}},
		}}},
		{Role: "osd", Name: "0", Records: []ceph.CrashRecord{{CrashID: "osd-crash", Fields: map[string]any{"backtrace": []any{"(OSD::shutdown()+0x1) [0x1]"}}}}},
	}}
	crashes := nativeCrashes(report)
	if len(crashes) != 2 || crashes[0].Daemon != "mgr.b" || crashes[0].Known == "" || !strings.Contains(crashes[0].Frame, nativeMgrAuthServerFrame) {
		t.Fatal("native MGR auth_server crash not classified", crashes)
	}
	if crashes[1].Daemon != "osd.0" || crashes[1].Known != "" || crashes[1].Frame != "(OSD::shutdown()+0x1) [0x1]" {
		t.Fatal("other native crash not reported by its first symbol frame", crashes[1])
	}
	cause := errors.New("exec ceph: context deadline exceeded")
	err := classifyNativeCrash(cause, crashes)
	if !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "native daemon crash fixture error: mgr.b crash 2026-10-15T12:10:28.962525Z_abc") ||
		!strings.Contains(err.Error(), "null auth_server") {
		t.Fatal("control failure not labelled as a native crash fixture error", err)
	}
	if classifyNativeCrash(cause, nil) != cause || classifyNativeCrash(nil, crashes) != nil || nativeCrashes(nil) != nil {
		t.Fatal("classification changed a failure without crash evidence")
	}
}

func TestForcedOSDKillSendsSIGKILLOnceAndRequiresExit137(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		states []*container.State
		ok     bool
	}{
		{"killed", []*container.State{{Running: true}, {Running: false, ExitCode: 137}}, true},
		{"fast shutdown after SIGTERM", []*container.State{{Running: false, ExitCode: 0}}, false},
		{"OOM", []*container.State{{Running: false, ExitCode: 137, OOMKilled: true}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			kills, reads := 0, 0
			state, err := forceKill(ctx, func(context.Context) error { kills++; return nil }, func(context.Context) (*container.State, error) {
				current := test.states[min(reads, len(test.states)-1)]
				reads++
				return current, nil
			}, time.Second)
			if kills != 1 || (err == nil) != test.ok || test.ok && (state == nil || state.ExitCode != 137) {
				t.Fatal("forced kill receipt", kills, state, err)
			}
		})
	}
	kills := 0
	if _, err := forceKill(ctx, func(context.Context) error { kills++; return errors.New("no such container") }, nil, time.Second); err == nil || kills != 1 {
		t.Fatal("failed SIGKILL must not be retried or ignored", err)
	}
	_, err := forceKill(ctx, func(context.Context) error { return nil }, func(context.Context) (*container.State, error) {
		return &container.State{Running: true}, nil
	}, 200*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not stop") {
		t.Fatal("still-running OSD accepted after SIGKILL", err)
	}
	if _, err := forceKill(ctx, nil, nil, 0); err == nil {
		t.Fatal("unbounded forced kill accepted")
	}
}

func TestForcedOSDKillNoLongerUsesDockerStop(t *testing.T) {
	source, err := os.ReadFile("control.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, `case "kill":`)
	end := strings.Index(text, `case "stop":`)
	if start < 0 || end < start {
		t.Fatal("daemon kill control not found")
	}
	if kill := text[start:end]; strings.Contains(kill, ".Stop(") || !strings.Contains(kill, "forceKill(") || !strings.Contains(kill, "dockerKill(") {
		t.Fatal("forced OSD crash must send SIGKILL itself", kill)
	}
}

func TestRelayListensBelowEphemeralRangeAndFailsFastOnBind(t *testing.T) {
	if relayListenPort != "30400" || relayContainerPort != "30400/tcp" {
		t.Fatal("relay port changed", relayContainerPort)
	}
	relay, err := os.ReadFile("../relay/main.go")
	if err != nil || !strings.Contains(string(relay), `flag.String("listen", ":30400"`) {
		t.Fatal("relay default listener differs from the fixture port", err)
	}
	if relayLogFailure("  \n") != nil {
		t.Fatal("empty relay log treated as a failure")
	}
	bind := relayLogFailure("2026/10/15 23:11:01 listen tcp :30400: bind: address already in use\n")
	if bind == nil || !strings.HasPrefix(bind.Error(), "fixture error: relay could not bind") {
		t.Fatal("bind failure not reported as a fixture error", bind)
	}
	if other := relayLogFailure("invalid native daemon allowlist"); other == nil || strings.Contains(other.Error(), "bind") {
		t.Fatal("other fatal relay output not reported", other)
	}

	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "relay.log"), []byte("listen tcp :30400: bind: address already in use\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := waitRelay(context.Background(), out, 10*time.Second); err == nil || !strings.Contains(err.Error(), "bind") || time.Since(started) > 2*time.Second {
		t.Fatal("bind failure did not end the relay wait early", err)
	}
	ready := t.TempDir()
	if err := os.WriteFile(filepath.Join(ready, "relay.ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitRelay(context.Background(), ready, time.Second); err != nil {
		t.Fatal("ready relay rejected", err)
	}
	if err := waitRelay(context.Background(), t.TempDir(), 200*time.Millisecond); err == nil || !strings.HasPrefix(err.Error(), "fixture error: relay did not become ready") {
		t.Fatal("silent relay not bounded", err)
	}
}

func TestCIPrePullsTheTestcontainersReaperWithRetry(t *testing.T) {
	pull := "docker pull " + testcontainers.ReaperDefaultImage
	// Both jobs that start Testcontainers clusters pre-pull the exact reaper
	// image this module's Testcontainers version runs. Both jobs are reusable.
	for _, name := range []string{"platform-checks.yml", "native-scenarios.yml"} {
		data, err := os.ReadFile("../../.github/workflows/" + name)
		if err != nil {
			t.Fatal(err)
		}
		workflow := string(data)
		if strings.Count(workflow, pull) != 1 || strings.Count(workflow, "name: Pre-pull the Testcontainers reaper image") != 1 {
			t.Fatal(name, "must pre-pull the Testcontainers reaper", testcontainers.ReaperDefaultImage)
		}
	}
}

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephNativeManagerPreparationIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION") != "1" {
		t.Skip("requires the isolated opt-in native MGR preparation acceptance fixture")
	}
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	prepareNativeManagerFailover(t, ctx, control)
	c, clientCtx := fixtureClient(t, 3*time.Minute)
	initial := nativeManagerSnapshot(t, clientCtx, c, control, "initial").Manager
	if len(initial.Standbys) != 1 {
		t.Fatal("native preparation acceptance lacks a standby")
	}
	name := initial.Standbys[0].Name
	path := filepath.Join(control, "manager-preparation.jsonl")
	before, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(before) != 0 {
		t.Fatal("isolated preparation fixture already needed a restoration")
	}
	if err := testcluster.ControlDaemon(ctx, control, "pause", "mgr", name); err != nil {
		t.Fatal("public standby pause", err)
	}
	resumed := false
	t.Cleanup(func() {
		if !resumed {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := testcluster.ControlDaemon(cleanup, control, "resume", "mgr", name); err != nil {
				t.Error("resume owned standby after acceptance failure", err)
			}
		}
	})
	t.Logf("paused native standby mgr.%s for 45s with ordinary 12s AUTH/service tickets; Go clients remain unpaused", name)
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := testcluster.ControlDaemon(ctx, control, "resume", "mgr", name); err != nil {
		t.Fatal("public standby resume", err)
	}
	resumed = true
	observed := struct {
		Daemon    string `json:"daemon"`
		Missing   bool   `json:"missing"`
		Rejection string `json:"native_rejection"`
	}{}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := testcluster.ControlDaemon(ctx, control, "verify", "mgr-auth", name); err != nil {
			t.Fatal("observe native refusal before preparation", err)
		}
		data, err := os.ReadFile(filepath.Join(control, "manager-native-auth.json"))
		if err != nil || json.Unmarshal(data, &observed) != nil {
			t.Fatal("native refusal observation", err)
		}
		if observed.Daemon != "mgr."+name {
			t.Fatal("native refusal belongs to a different daemon", observed.Daemon)
		}
		if observed.Missing && strings.Contains(observed.Rejection, "handle_auth_bad_method failed to auth with my available methods: (13) Permission denied") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pause/resume did not establish exact missing native AUTH refusal; no restoration branch claimed: %+v", observed)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Logf("independent native refusal observed before preparation: %+v", observed)
	prepareNativeManagerFailover(t, ctx, control)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("missing once-only native restoration receipt", err)
	}
	// MgrMonitor retains a registration until beacon expiry, so after the
	// resume the preparation can still see the refused standby registered
	// (one receipt for that observation) before the map drops it and the
	// restoration is recorded. Either way the restoration comes last and
	// both receipts carry the same first native refusal.
	lines := strings.Split(strings.TrimSpace(string(after)), "\n")
	if len(lines) != 1 && len(lines) != 2 {
		t.Fatalf("native restoration receipt count=%d want 1, or 2 with a registered-slot observation first", len(lines))
	}
	for i, line := range lines {
		var receipt struct {
			Daemon     string `json:"daemon"`
			Rejection  string `json:"native_rejection"`
			Registered bool   `json:"registered"`
			Action     string `json:"action"`
		}
		want := "restore missing native daemon before independent failover"
		if i < len(lines)-1 {
			want = "observe native refusal in registered slot before independent failover"
		}
		if err := json.Unmarshal([]byte(line), &receipt); err != nil || receipt.Daemon != "mgr."+name || receipt.Rejection != observed.Rejection ||
			receipt.Action != want || i < len(lines)-1 && !receipt.Registered {
			t.Fatal("restoration did not retain the exact first native refusal", i, receipt, err)
		}
	}
	final := nativeManagerSnapshot(t, clientCtx, c, control, "restored").Manager
	if final.Name != initial.Name || final.GlobalID != initial.GlobalID || len(final.Standbys) != 1 || final.Standbys[0].Name != name || final.Standbys[0].GlobalID == initial.Standbys[0].GlobalID || c.Snapshot().Manager.Ready {
		t.Fatal("once-only public restart did not restore two owned slots while preserving active identity and lazy Go MGR state", final)
	}
	// A second healthy preparation must remain read-only and append no receipt.
	prepareNativeManagerFailover(t, ctx, control)
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != string(after) {
		t.Fatal("healthy preparation repeated a native daemon restart", err)
	}
	t.Log("native missing standby AUTH refusal triggered one public Stop/Start, preserved its receipt, restored both owned slots, and healthy preparation performed no further restart")
}

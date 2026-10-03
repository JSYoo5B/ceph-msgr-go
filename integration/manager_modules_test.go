package integration_test

import (
	"context"
	"net"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephManagerModulesIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	dial := options.DialContext
	var managerDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(address)
		if port == "36800" || port == "36801" {
			managerDials.Add(1)
		}
		return dial(ctx, network, address)
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	initial := nativeManagerSnapshot(t, ctx, client, control, "initial")
	if !slices.Contains(initial.Manager.EnabledModules, "iostat") || len(initial.Manager.AvailableModules) == 0 || len(initial.Manager.Standbys) == 0 {
		t.Fatal("module fixture metadata missing", initial.Manager)
	}
	var unavailable bool
	for _, module := range initial.Manager.AvailableModules {
		unavailable = unavailable || (!module.CanRun && module.ErrorString != "")
	}
	if !unavailable {
		t.Fatal("fixture has no reported module dependency failure")
	}
	initial.Manager.EnabledModules[0] = "caller-owned"
	initial.Manager.AvailableModules[0] = cephmsgr.ManagerModule{Name: "caller-owned"}
	state := client.Snapshot()
	if state.Manager.EnabledModules[0] == "caller-owned" || state.Manager.AvailableModules[0].Name == "caller-owned" {
		t.Fatal("module snapshot aliases client state")
	}
	command := func(prefix string, args map[string]any) {
		t.Helper()
		cmd, err := newCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.MonCommand(ctx, cmd); err != nil {
			t.Fatal(prefix, err)
		}
	}
	restore := false
	t.Cleanup(func() {
		if !restore {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		admin, err := cephmsgr.Dial(cleanup, integrationOptions(t))
		if err != nil {
			t.Error("restore fixture iostat", err)
			return
		}
		defer admin.Close()
		cmd, _ := newCommand("mgr module enable", map[string]any{"module": "iostat"})
		if _, err := admin.MonCommand(cleanup, cmd); err != nil {
			t.Error("restore fixture iostat", err)
		}
	})
	// Each fixture mutation is transmitted once. Map observation never replays
	// an enable/disable command whose result is uncertain.
	command("mgr module disable", map[string]any{"module": "iostat"})
	restore = true
	disabled := waitClientState(t, client, ctx, func(state cephmsgr.State) bool {
		return state.Manager.MapEpoch > initial.Manager.MapEpoch && !slices.Contains(state.Manager.EnabledModules, "iostat")
	})
	nativeManagerSnapshot(t, ctx, client, control, "disabled")
	restore = false
	command("mgr module enable", map[string]any{"module": "iostat"})
	waitClientState(t, client, ctx, func(state cephmsgr.State) bool {
		return state.Manager.MapEpoch > disabled.Manager.MapEpoch && slices.Contains(state.Manager.EnabledModules, "iostat")
	})
	restored := nativeManagerSnapshot(t, ctx, client, control, "restored")
	command("mgr fail", map[string]any{"who": restored.Manager.Name})
	waitClientState(t, client, ctx, func(state cephmsgr.State) bool {
		return state.Manager.Available && state.Manager.Name != restored.Manager.Name && len(state.Manager.AvailableModules) != 0
	})
	replacement := nativeManagerSnapshot(t, ctx, client, control, "replacement")
	if !slices.Contains(replacement.Manager.EnabledModules, "iostat") || replacement.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("module discovery opened MGR or lost explicit activation", replacement.Manager, managerDials.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closed := client.Snapshot()
	if !closed.Closed || !reflect.DeepEqual(closed.Manager.EnabledModules, replacement.Manager.EnabledModules) || !reflect.DeepEqual(closed.Manager.AvailableModules, replacement.Manager.AvailableModules) {
		t.Fatal("Close lost last reported module metadata", closed.Manager)
	}
}

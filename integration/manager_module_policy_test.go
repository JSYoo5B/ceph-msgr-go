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

func TestCephManagerModulePolicyIntegration(t *testing.T) {
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
	if len(initial.Manager.AlwaysOnModules) != 6 || len(initial.Manager.ForceDisabledModules) != 0 {
		t.Fatal("unexpected disposable fixture policy", initial.Manager)
	}
	for _, module := range []string{"progress", "status"} {
		if !slices.Contains(initial.Manager.AlwaysOnModules[20], module) || slices.Contains(initial.Manager.EnabledModules, module) {
			t.Fatal("fixture module is not solely always-on", module)
		}
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
	remaining := make(map[string]bool)
	t.Cleanup(func() {
		if len(remaining) == 0 {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		admin, err := cephmsgr.Dial(cleanup, integrationOptions(t))
		if err != nil {
			t.Error("restore always-on fixture modules", err)
			return
		}
		defer admin.Close()
		for module := range remaining {
			cmd, _ := newCommand("mgr module enable", map[string]any{"module": module})
			if _, err := admin.MonCommand(cleanup, cmd); err != nil {
				t.Error("restore always-on fixture module", module, err)
			}
		}
	})
	// Mutate only this disposable cluster, once per command. Two disabled
	// modules also exercise the native dump's repeated "module" object keys.
	for _, module := range []string{"progress", "status"} {
		command("mgr module force disable", map[string]any{"module": module, "yes_i_really_mean_it": true})
		remaining[module] = true
		waitClientState(t, client, ctx, func(state cephmsgr.State) bool {
			return slices.Contains(state.Manager.ForceDisabledModules, module)
		})
	}
	disabled := nativeManagerSnapshot(t, ctx, client, control, "disabled")
	if !reflect.DeepEqual(disabled.Manager.ForceDisabledModules, []string{"progress", "status"}) || !reflect.DeepEqual(disabled.Manager.AlwaysOnModules, initial.Manager.AlwaysOnModules) {
		t.Fatal("force-disable changed base policy or lost a module", disabled.Manager)
	}
	disabled.Manager.AlwaysOnModules[20][0] = "caller-owned"
	disabled.Manager.ForceDisabledModules[0] = "caller-owned"
	if slices.Contains(client.Snapshot().Manager.AlwaysOnModules[20], "caller-owned") || slices.Contains(client.Snapshot().Manager.ForceDisabledModules, "caller-owned") {
		t.Fatal("policy snapshot aliases client map")
	}
	active := client.Snapshot().Manager.Name
	command("mgr fail", map[string]any{"who": active})
	waitClientState(t, client, ctx, func(state cephmsgr.State) bool { return state.Manager.Available && state.Manager.Name != active })
	replacement := nativeManagerSnapshot(t, ctx, client, control, "replacement")
	if !reflect.DeepEqual(replacement.Manager.ForceDisabledModules, []string{"progress", "status"}) {
		t.Fatal("active transition lost force-disabled policy", replacement.Manager)
	}
	for _, module := range []string{"progress", "status"} {
		delete(remaining, module)
		command("mgr module enable", map[string]any{"module": module})
		waitClientState(t, client, ctx, func(state cephmsgr.State) bool { return !slices.Contains(state.Manager.ForceDisabledModules, module) })
	}
	restored := nativeManagerSnapshot(t, ctx, client, control, "restored")
	if !reflect.DeepEqual(restored.Manager.AlwaysOnModules, initial.Manager.AlwaysOnModules) || len(restored.Manager.ForceDisabledModules) != 0 || restored.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("restored policy or lazy MGR connection differs", restored.Manager, managerDials.Load())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closed := client.Snapshot()
	if !closed.Closed || !reflect.DeepEqual(closed.Manager.AlwaysOnModules, restored.Manager.AlwaysOnModules) || !reflect.DeepEqual(closed.Manager.ForceDisabledModules, restored.Manager.ForceDisabledModules) {
		t.Fatal("Close lost last reported module policy", closed.Manager)
	}
}

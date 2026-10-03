package integration_test

import (
	"context"
	"maps"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephManagerServicesIntegration(t *testing.T) {
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
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	initial := nativeManagerSnapshot(t, ctx, c, control, "initial")
	if len(initial.Manager.Services) != 0 {
		t.Fatal("ordinary fixture already advertises services", initial.Manager.Services)
	}
	var canRun bool
	for _, module := range initial.Manager.AvailableModules {
		if module.Name == "prometheus" {
			canRun = module.CanRun
		}
	}
	if !canRun {
		t.Fatal("fixture cannot run prometheus")
	}
	command := func(prefix string, args map[string]any) {
		t.Helper()
		cmd, err := cephmsgr.NewCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.MonCommand(ctx, cmd); err != nil {
			t.Fatal(prefix, err)
		}
	}
	// The two fixture daemons share a host. Localized ports let active and
	// standby module listeners coexist and distinguish the promoted URI.
	command("config set", map[string]any{"who": "mgr", "name": "mgr/prometheus/a/server_port", "value": "9280"})
	command("config set", map[string]any{"who": "mgr", "name": "mgr/prometheus/b/server_port", "value": "9281"})
	command("mgr module enable", map[string]any{"module": "prometheus"})
	restore := true
	t.Cleanup(func() {
		if !restore {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		admin, err := cephmsgr.Dial(cleanup, integrationOptions(t))
		if err != nil {
			t.Error("disable fixture prometheus", err)
			return
		}
		defer admin.Close()
		cmd, _ := cephmsgr.NewCommand("mgr module disable", map[string]any{"module": "prometheus"})
		if _, err := admin.MonCommand(cleanup, cmd); err != nil {
			t.Error("disable fixture prometheus", err)
		}
	})
	waitClientState(t, c, ctx, func(state cephmsgr.State) bool { return state.Manager.Services["prometheus"] != "" })
	enabled := nativeManagerSnapshot(t, ctx, c, control, "restored")
	expectedPort := map[string]string{"a": ":9280/", "b": ":9281/"}
	if !strings.HasSuffix(enabled.Manager.Services["prometheus"], expectedPort[enabled.Manager.Name]) {
		t.Fatal("service URI did not use active daemon's localized port", enabled.Manager)
	}
	owned := c.Snapshot()
	owned.Manager.Services["prometheus"] = "caller-owned"
	owned.Manager.Services["caller-added"] = "raw URI"
	if !maps.Equal(c.Snapshot().Manager.Services, enabled.Manager.Services) {
		t.Fatal("caller modified retained service map")
	}
	// Transmit each fixture mutation once; only map observation is repeated.
	command("mgr fail", map[string]any{"who": enabled.Manager.Name})
	waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Manager.Available && state.Manager.Name != enabled.Manager.Name && state.Manager.Services["prometheus"] != ""
	})
	replacement := nativeManagerSnapshot(t, ctx, c, control, "replacement")
	if !strings.HasSuffix(replacement.Manager.Services["prometheus"], expectedPort[replacement.Manager.Name]) || replacement.Manager.Services["prometheus"] == enabled.Manager.Services["prometheus"] {
		t.Fatal("promoted daemon's service URI missing", replacement.Manager)
	}
	restore = false // An uncertain disable is never retried by fixture cleanup.
	command("mgr module disable", map[string]any{"module": "prometheus"})
	waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		_, present := state.Manager.Services["prometheus"]
		return state.Manager.MapEpoch > replacement.Manager.MapEpoch && !present
	})
	disabled := nativeManagerSnapshot(t, ctx, c, control, "disabled")
	if len(disabled.Manager.Services) != 0 || disabled.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("removed service retained or discovery opened MGR", disabled.Manager, managerDials.Load())
	}
	command("config rm", map[string]any{"who": "mgr", "name": "mgr/prometheus/a/server_port"})
	command("config rm", map[string]any{"who": "mgr", "name": "mgr/prometheus/b/server_port"})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := c.Snapshot()
	if !closed.Closed || !maps.Equal(closed.Manager.Services, disabled.Manager.Services) {
		t.Fatal("Close changed last service metadata", closed.Manager)
	}
	if !strings.HasSuffix(enabled.Manager.Services["prometheus"], expectedPort[enabled.Manager.Name]) {
		t.Fatal("later map replaced earlier snapshot")
	}
}

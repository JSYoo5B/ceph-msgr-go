package integration_test

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephManagerModuleOptionsIntegration(t *testing.T) {
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
	schema := func(state cephmsgr.State) map[string]cephmsgr.ManagerModuleOption {
		for _, module := range state.Manager.AvailableModules {
			if module.Name == "prometheus" {
				return module.Options
			}
		}
		return nil
	}
	initial := nativeManagerSnapshot(t, ctx, c, control, "initial")
	opts := schema(initial)
	port, present := opts["server_port"]
	if !present || port.Name != "server_port" || port.DefaultValue != "9283" || len(opts) == 0 {
		t.Fatal("fixture prometheus option descriptors missing", opts)
	}
	owned := c.Snapshot()
	for i := range owned.Manager.AvailableModules {
		module := &owned.Manager.AvailableModules[i]
		if module.Name == "prometheus" {
			module.Options["server_port"] = cephmsgr.ManagerModuleOption{Name: "caller-owned"}
			delete(module.Options, "standby_behaviour")
		}
	}
	if !reflect.DeepEqual(schema(c.Snapshot()), opts) {
		t.Fatal("caller modified retained option map")
	}
	command := func(prefix string, args map[string]any) cephmsgr.Result {
		t.Helper()
		cmd, err := newCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		result, err := c.MonCommand(ctx, cmd)
		if err != nil {
			t.Fatal(prefix, err)
		}
		return result
	}
	// An option descriptor reports the schema default, not the configured
	// effective value. Each fixture mutation is transmitted once.
	command("config set", map[string]any{"who": "mgr", "name": "mgr/prometheus/server_port", "value": "9393"})
	value := command("config get", map[string]any{"who": "mgr." + initial.Manager.Name, "key": "mgr/prometheus/server_port"})
	if strings.TrimSpace(string(value.Data)) != "9393" {
		t.Fatal("fixture effective option value", string(value.Data))
	}
	configured := nativeManagerSnapshot(t, ctx, c, control, "restored")
	if !reflect.DeepEqual(schema(configured), opts) {
		t.Fatal("effective configuration replaced reported default", schema(configured))
	}
	command("mgr fail", map[string]any{"who": configured.Manager.Name})
	waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Manager.Available && state.Manager.Name != configured.Manager.Name && len(schema(state)) != 0
	})
	replacement := nativeManagerSnapshot(t, ctx, c, control, "replacement")
	if !reflect.DeepEqual(schema(replacement), opts) || replacement.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("replacement descriptors lost or discovery opened MGR", replacement.Manager, managerDials.Load())
	}
	command("config rm", map[string]any{"who": "mgr", "name": "mgr/prometheus/server_port"})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := c.Snapshot()
	if !closed.Closed || !reflect.DeepEqual(schema(closed), opts) {
		t.Fatal("Close lost option schema", schema(closed))
	}
	if !reflect.DeepEqual(schema(initial), opts) {
		t.Fatal("later map modified earlier descriptor snapshot")
	}
}

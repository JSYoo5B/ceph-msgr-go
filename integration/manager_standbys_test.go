package integration_test

import (
	"context"
	"encoding/json"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type managerModuleOptionOracle struct {
	Name            string   `json:"name"`
	Type            uint8    `json:"type"`
	Level           uint8    `json:"level"`
	Flags           uint32   `json:"flags"`
	DefaultValue    string   `json:"default_value"`
	Min             string   `json:"min"`
	Max             string   `json:"max"`
	EnumAllowed     []string `json:"enum_allowed"`
	Description     string   `json:"desc"`
	LongDescription string   `json:"long_desc"`
	Tags            []string `json:"tags"`
	SeeAlso         []string `json:"see_also"`
}

func (o managerModuleOptionOracle) value() cephmsgr.ManagerModuleOption {
	return cephmsgr.ManagerModuleOption{
		Name: o.Name, Type: o.Type, Level: o.Level, Flags: o.Flags,
		DefaultValue: o.DefaultValue, Min: o.Min, Max: o.Max,
		EnumAllowed: append([]string(nil), o.EnumAllowed...),
		Description: o.Description, LongDescription: o.LongDescription,
		Tags: append([]string(nil), o.Tags...), SeeAlso: append([]string(nil), o.SeeAlso...),
	}
}

func nativeManagerSnapshot(t *testing.T, ctx context.Context, c *cephmsgr.Client, control, label string) cephmsgr.State {
	t.Helper()
	for {
		if err := testcluster.ControlDaemon(ctx, control, "verify", "manager-map", label); err != nil {
			t.Fatal("fresh native MGR map", err)
		}
		data, err := os.ReadFile(filepath.Join(control, "manager-oracle.json"))
		var oracle struct {
			Epoch     uint32 `json:"epoch"`
			Available bool   `json:"available"`
			Name      string `json:"active_name"`
			ID        uint64 `json:"active_gid"`
			Standbys  []struct {
				Name string `json:"name"`
				ID   uint64 `json:"gid"`
			} `json:"standbys"`
			Modules              []string            `json:"modules"`
			Services             map[string]string   `json:"services"`
			AlwaysOnModules      map[uint32][]string `json:"always_on_modules"`
			ForceDisabledModules []string            `json:"force_disabled_modules"`
			AvailableModules     []struct {
				Name        string                               `json:"name"`
				CanRun      bool                                 `json:"can_run"`
				ErrorString string                               `json:"error_string"`
				Options     map[string]managerModuleOptionOracle `json:"module_options"`
			} `json:"available_modules"`
		}
		if err != nil || json.Unmarshal(data, &oracle) != nil || oracle.Epoch == 0 {
			t.Fatal("native manager metadata", err)
		}
		state := waitClientState(t, c, ctx, func(state cephmsgr.State) bool { return state.Manager.MapEpoch >= oracle.Epoch })
		if state.Manager.MapEpoch != oracle.Epoch {
			// A newer subscribed map can overtake the independent read. Repeat
			// only that read until both views describe the same committed epoch.
			continue
		}
		var expected []cephmsgr.StandbyManager
		for _, standby := range oracle.Standbys {
			expected = append(expected, cephmsgr.StandbyManager{Name: standby.Name, GlobalID: standby.ID})
		}
		var available []cephmsgr.ManagerModule
		for _, module := range oracle.AvailableModules {
			var options map[string]cephmsgr.ManagerModuleOption
			if len(module.Options) > 0 {
				options = make(map[string]cephmsgr.ManagerModuleOption, len(module.Options))
			}
			for key, option := range module.Options {
				options[key] = option.value()
			}
			available = append(available, cephmsgr.ManagerModule{Name: module.Name, CanRun: module.CanRun, ErrorString: module.ErrorString, Options: options})
		}
		enabled := append([]string(nil), oracle.Modules...)
		if state.Manager.Available != oracle.Available || state.Manager.Name != oracle.Name || state.Manager.GlobalID != oracle.ID || !reflect.DeepEqual(state.Manager.Standbys, expected) || !reflect.DeepEqual(state.Manager.EnabledModules, enabled) || !reflect.DeepEqual(state.Manager.AvailableModules, available) || !maps.Equal(state.Manager.Services, oracle.Services) || !reflect.DeepEqual(state.Manager.AlwaysOnModules, oracle.AlwaysOnModules) || !reflect.DeepEqual(state.Manager.ForceDisabledModules, append([]string(nil), oracle.ForceDisabledModules...)) {
			t.Fatal("snapshot differs from same-epoch native MgrMap", state.Manager, oracle)
		}
		t.Logf("epoch %d: active %s/%d, standbys %+v match native MgrMap", oracle.Epoch, oracle.Name, oracle.ID, expected)
		t.Logf("epoch %d: explicit modules %v and %d reported modules match native MgrMap", oracle.Epoch, enabled, len(available))
		t.Logf("epoch %d: advertised services %v match native MgrMap", oracle.Epoch, oracle.Services)
		count := 0
		for _, module := range available {
			count += len(module.Options)
		}
		t.Logf("epoch %d: %d module option descriptors match native MgrMap", oracle.Epoch, count)
		t.Logf("epoch %d: %d release policies and force-disabled modules %v match native MgrMap", oracle.Epoch, len(oracle.AlwaysOnModules), oracle.ForceDisabledModules)
		return state
	}
}

func TestCephManagerStandbysIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	defer c.Close()
	initial := nativeManagerSnapshot(t, ctx, c, control, "initial")
	if initial.Manager.Ready || len(initial.Manager.Standbys) != 1 || managerDials.Load() != 0 {
		t.Fatal("standby metadata required MGR connection or fixture has no standby", initial.Manager)
	}
	promoted := initial.Manager.Standbys[0]
	initial.Manager.Standbys[0].Name = "caller-owned"
	if c.Snapshot().Manager.Standbys[0].Name != promoted.Name {
		t.Fatal("snapshot standby slice aliases client state")
	}
	command, err := cephmsgr.NewCommand("mgr fail", map[string]any{"who": initial.Manager.Name})
	if err != nil {
		t.Fatal(err)
	}
	// One fixture mutation; observing new maps never replays this command.
	if _, err := c.MonCommand(ctx, command); err != nil {
		t.Fatal("MGR promotion", err)
	}
	waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Manager.Available && state.Manager.Name == promoted.Name && state.Manager.GlobalID == promoted.GlobalID && len(state.Manager.Standbys) > 0
	})
	replacement := nativeManagerSnapshot(t, ctx, c, control, "replacement")
	if replacement.Manager.Standbys[0].Name != initial.Manager.Name || replacement.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("standby-to-active transition or lazy MGR state", replacement.Manager, managerDials.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := c.Snapshot()
	if !closed.Closed || closed.Manager.Ready || !reflect.DeepEqual(closed.Manager.Standbys, replacement.Manager.Standbys) {
		t.Fatal("Close lost last advertised standby metadata", closed.Manager)
	}
}

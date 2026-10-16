package main

import (
	"encoding/json"
	"reflect"
	"runtime/debug"
	"testing"
)

func TestFixtureProviderIdentityUsesEffectiveBuildModule(t *testing.T) {
	for _, tc := range []struct {
		name                string
		info                *debug.BuildInfo
		ok                  bool
		status, version     string
		original, effective *fixtureProviderModuleIdentity
	}{
		{name: "ordinary dependency", info: &debug.BuildInfo{Deps: []*debug.Module{nil, {Path: "unrelated.example/module", Version: "v9.0.0"}, {Path: fixtureProviderModule, Version: "v1.2.3", Sum: "h1:original"}}}, ok: true, status: "module", version: "v1.2.3", original: &fixtureProviderModuleIdentity{fixtureProviderModule, "v1.2.3", "h1:original"}, effective: &fixtureProviderModuleIdentity{fixtureProviderModule, "v1.2.3", "h1:original"}},
		{name: "versioned replacement", info: &debug.BuildInfo{Deps: []*debug.Module{{Path: fixtureProviderModule, Version: "v1.2.3", Sum: "h1:original", Replace: &debug.Module{Path: "example.org/fork", Version: "v2.0.0", Sum: "h1:replacement"}}}}, ok: true, status: "versioned-replacement", version: "v2.0.0", original: &fixtureProviderModuleIdentity{fixtureProviderModule, "v1.2.3", "h1:original"}, effective: &fixtureProviderModuleIdentity{"example.org/fork", "v2.0.0", "h1:replacement"}},
		{name: "local replacement", info: &debug.BuildInfo{Deps: []*debug.Module{{Path: fixtureProviderModule, Version: "v1.2.3", Sum: "h1:original", Replace: &debug.Module{Path: "../local-provider"}}}}, ok: true, status: "local-replacement", original: &fixtureProviderModuleIdentity{fixtureProviderModule, "v1.2.3", "h1:original"}, effective: &fixtureProviderModuleIdentity{Path: "../local-provider"}},
		{name: "development dependency", info: &debug.BuildInfo{Deps: []*debug.Module{{Path: fixtureProviderModule, Version: "(devel)"}}}, ok: true, status: "development-module", original: &fixtureProviderModuleIdentity{fixtureProviderModule, "(devel)", ""}, effective: &fixtureProviderModuleIdentity{fixtureProviderModule, "(devel)", ""}},
		{name: "missing module version", info: &debug.BuildInfo{Deps: []*debug.Module{{Path: fixtureProviderModule}}}, ok: true, status: "development-module", original: &fixtureProviderModuleIdentity{Path: fixtureProviderModule}, effective: &fixtureProviderModuleIdentity{Path: fixtureProviderModule}},
		{name: "missing dependency", info: &debug.BuildInfo{Deps: []*debug.Module{{Path: "unrelated.example/module", Version: "v9.0.0"}}}, ok: true, status: "module-not-recorded"},
		{name: "missing build metadata", ok: false, status: "build-info-unavailable"},
		{name: "nil build metadata", ok: true, status: "build-info-unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fixtureProviderIdentity(tc.info, tc.ok)
			if got["provider"] != fixtureProviderModule || got["provider_version"] != tc.version || got["provider_identity_status"] != tc.status {
				t.Fatal("effective provider identity differs", got)
			}
			for field, want := range map[string]*fixtureProviderModuleIdentity{"provider_original_module": tc.original, "provider_effective_module": tc.effective} {
				value, exists := got[field]
				if want == nil {
					if exists {
						t.Fatal("invented module descriptor", field, value)
					}
					continue
				}
				if !exists || !reflect.DeepEqual(value, *want) {
					t.Fatal("module provenance lost", field, value, want)
				}
			}
			// Keep the JSON version key, but never inherit the original
			// version for unknown, local or development effective code.
			data, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(data, &decoded); err != nil || decoded["provider_version"] != tc.version {
				t.Fatal(string(data), err)
			}
		})
	}
}

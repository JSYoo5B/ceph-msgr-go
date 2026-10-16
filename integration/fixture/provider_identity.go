package main

import "runtime/debug"

const fixtureProviderModule = "github.com/JSYoo5B/ceph-testcontainers-go"

type fixtureProviderModuleIdentity struct {
	Path    string `json:"path"`
	Version string `json:"version,omitempty"`
	Sum     string `json:"sum,omitempty"`
}

func currentFixtureProviderIdentity() map[string]any {
	info, ok := debug.ReadBuildInfo()
	return fixtureProviderIdentity(info, ok)
}

// BuildInfo identifies the actual linked module. The selected module's version
// does not describe replacement code, so preserve the original descriptor
// separately. Missing, local and development metadata imply no release version.
func fixtureProviderIdentity(info *debug.BuildInfo, ok bool) map[string]any {
	result := map[string]any{"provider": fixtureProviderModule, "provider_version": "", "provider_identity_status": "build-info-unavailable"}
	if !ok || info == nil {
		return result
	}
	result["provider_identity_status"] = "module-not-recorded"
	for _, dependency := range info.Deps {
		if dependency == nil || dependency.Path != fixtureProviderModule {
			continue
		}
		module := func(m *debug.Module) fixtureProviderModuleIdentity {
			return fixtureProviderModuleIdentity{Path: m.Path, Version: m.Version, Sum: m.Sum}
		}
		result["provider_original_module"] = module(dependency)
		effective, status := dependency, "module"
		if dependency.Replace != nil {
			effective, status = dependency.Replace, "versioned-replacement"
			if effective.Version == "" {
				status = "local-replacement"
			}
		}
		result["provider_effective_module"] = module(effective)
		if effective.Version == "(devel)" || effective.Version == "" && dependency.Replace == nil {
			status = "development-module"
		}
		if effective.Version != "" && effective.Version != "(devel)" {
			result["provider_version"] = effective.Version
		}
		result["provider_identity_status"] = status
		return result
	}
	return result
}

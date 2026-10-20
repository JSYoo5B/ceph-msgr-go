package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func ignoreCacheTestProfile(t *testing.T) profile {
	t.Helper()
	p, err := loadProfile(func(name string) string {
		switch name {
		case "CEPH_MSGR_TEST_OSD":
			return "1"
		case "CEPH_MSGR_TEST_OSD_STORE":
			return "bluestore"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestIgnoreCacheProbeProfileIsNativeOnlyAndIsolated(t *testing.T) {
	p := ignoreCacheTestProfile(t)
	if err := validateIgnoreCacheProbeProfile(p); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*profile){func(p *profile) { p.osd = "0" }, func(p *profile) { p.osdReplicas = "3" }, func(p *profile) { p.osdStore = "memstore" }, func(p *profile) { p.osdProfile = "replicated-pg-v8" }, func(p *profile) { p.mdsFresh = "1" }, func(p *profile) { p.mdsCephFSData = "1" }, func(p *profile) { p.rbdBase = "1" }, func(p *profile) { p.testRun = "TestOther" }, func(p *profile) { p.monFreeze = "1" }, func(p *profile) { p.monPing = "1" }, func(p *profile) { p.shortTickets = "1" }, func(p *profile) { p.image = "unpinned" }, func(p *profile) { p.osdSoak = "4m" }} {
		changed := p
		change(&changed)
		if validateIgnoreCacheProbeProfile(changed) == nil {
			t.Fatal("accepted mixed probe", changed)
		}
	}
}
func ignoreCacheTestDump(attached bool) map[string]any {
	base := map[string]any{"pool": int64(7), "pool_name": "base", "type": 1, "size": 1, "min_size": 1, "tier_of": -1, "tiers": []int{}, "read_tier": -1, "write_tier": -1, "cache_mode": "none", "pool_snaps": []any{}}
	tier := map[string]any{"pool": int64(8), "pool_name": "tier", "type": 1, "size": 1, "min_size": 1, "tier_of": -1, "tiers": []int{}, "read_tier": -1, "write_tier": -1, "cache_mode": "none", "pool_snaps": []any{}, "hit_set_params": map[string]any{"type": "none"}}
	if attached {
		base["tiers"] = []int{8}
		tier["tier_of"] = 7
		tier["cache_mode"] = "writeback"
	}
	return map[string]any{"epoch": 12, "pools": []any{base, tier}}
}
func TestIgnoreCacheNativePoolShapeAndRelationship(t *testing.T) {
	for _, attached := range []bool{false, true} {
		raw, _ := json.Marshal(ignoreCacheTestDump(attached))
		if err := validateIgnoreCacheTier(raw, "base", "tier", attached); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"overlay", "foreign-tier", "snapshots", "hit-set", "ec", "ambiguous"} {
		dump := ignoreCacheTestDump(true)
		pools := dump["pools"].([]any)
		base, tier := pools[0].(map[string]any), pools[1].(map[string]any)
		switch field {
		case "overlay":
			base["read_tier"] = 8
		case "foreign-tier":
			tier["tier_of"] = 9
		case "snapshots":
			tier["pool_snaps"] = []any{map[string]any{"snapid": 1}}
		case "hit-set":
			tier["hit_set_params"] = map[string]any{"type": "bloom"}
		case "ec":
			tier["type"] = 3
		case "ambiguous":
			dump["pools"] = append(pools, base)
		}
		raw, _ := json.Marshal(dump)
		if validateIgnoreCacheTier(raw, "base", "tier", true) == nil {
			t.Fatal("accepted unsafe native pool shape", field)
		}
	}
}
func TestIgnoreCacheEvidenceCannotCopyPrivateIdentityOrOwnership(t *testing.T) {
	out, diagnostics := t.TempDir(), t.TempDir()
	for _, name := range []string{"admin.keyring", "osd-ignore-cache-rw.keyring", "fixture-ownership.json", "ownership.json"} {
		if err := os.WriteFile(filepath.Join(out, name), []byte("PRIVATE"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeIgnoreCacheJSON(out, "osd-ignore-cache-probe.json", ignoreCacheProbeReceipt{Schema: 1, NativeOnly: true}); err != nil {
		t.Fatal(err)
	}
	if err := saveIgnoreCacheEvidence(out, diagnostics); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name() != "osd-ignore-cache-probe.json" {
		t.Fatal(files)
	}
	raw, err := os.ReadFile(filepath.Join(diagnostics, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE") || !strings.Contains(string(raw), `"product_support_claimed": false`) {
		t.Fatal(string(raw))
	}
}

// These are synthetic receipt-schema checks, not native interoperability proof.
func TestIgnoreCacheProbeRequiresCompleteMatrixAndEveryPlacement(t *testing.T) {
	plan := ignoreCacheProbePlan{SourceCommit: nativeEncoderCommit, FSID: "owned", Token: "owned", Base: "base", Tier: "tier", Namespace: "cache-probe"}
	steps := make([]map[string]any, 66)
	for i := range steps {
		steps[i] = map[string]any{"state": "completed"}
	}
	var placements []map[string]any
	for _, pool := range []string{"base", "tier"} {
		for _, name := range []string{"miss", "write-normal", "write-ignore", "delete-normal", "delete-ignore"} {
			placements = append(placements, map[string]any{"pool_name": pool, "object": name, "namespace": "cache-probe", "address": "v2:127.0.0.1:6800/1", "epoch": 12, "primary_id": 0, "pg_shard": -1})
		}
	}
	valid := map[string]any{"source_commit": nativeEncoderCommit, "complete": true, "plan": plan, "steps": steps, "placements": placements}
	raw, _ := json.Marshal(valid)
	if err := validateIgnoreCacheNative(raw, plan); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"incomplete", "missing-step", "unknown", "missing-placement", "duplicate-placement", "foreign-plan"} {
		var value map[string]any
		_ = json.Unmarshal(raw, &value)
		switch kind {
		case "incomplete":
			value["complete"] = false
		case "missing-step":
			value["steps"] = value["steps"].([]any)[:65]
		case "unknown":
			value["steps"].([]any)[1].(map[string]any)["state"] = "unknown"
		case "missing-placement":
			value["placements"] = value["placements"].([]any)[:9]
		case "duplicate-placement":
			value["placements"].([]any)[1] = value["placements"].([]any)[0]
		case "foreign-plan":
			value["plan"].(map[string]any)["fsid"] = "other"
		}
		changed, _ := json.Marshal(value)
		if validateIgnoreCacheNative(changed, plan) == nil {
			t.Fatal("accepted incomplete or foreign native proof", kind)
		}
	}
}

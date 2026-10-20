package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUpmapTicketWindowKeepsOtherProfiles(t *testing.T) {
	selected := profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore", testRun: "^TestCephOSDUpmapIntegration$"}
	for _, tc := range []struct {
		name    string
		change  func(*profile)
		auth    time.Duration
		service time.Duration
	}{
		{"selected", func(*profile) {}, 120 * time.Second, 120 * time.Second},
		{"other selector", func(p *profile) { p.testRun = "^TestCephOSDObjectBackoffFailoverIntegration$" }, 12 * time.Second, 12 * time.Second},
		{"whole suite", func(p *profile) { p.testRun = "" }, 12 * time.Second, 12 * time.Second},
		{"one OSD", func(p *profile) { p.osdReplicas = "1" }, 12 * time.Second, 12 * time.Second},
		{"no OSD", func(p *profile) { p.osd = "0" }, 12 * time.Second, 12 * time.Second},
		{"other wire profile", func(p *profile) { p.osdProfile = "replicated-pg-reply-v8" }, 12 * time.Second, 12 * time.Second},
		{"other store", func(p *profile) { p.osdStore = "memstore" }, 12 * time.Second, 12 * time.Second},
		{"MDS workflow", func(p *profile) { p.mdsFresh = "1" }, 12 * time.Second, 12 * time.Second},
		{"fractional auth", func(p *profile) { p.shortTickets = "1" }, shortTicketAuthTTL, 12 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := selected
			tc.change(&p)
			auth, service := fixtureTicketTTLs(p)
			if auth != tc.auth || service != tc.service {
				t.Fatal("upmap ticket isolation changed another ticket contract", auth, service)
			}
		})
	}
}

func TestUpmapControlRejectsUnownedOrUnboundedRecipe(t *testing.T) {
	valid := profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore"}
	for _, tc := range []struct {
		name, input string
		profile     profile
	}{
		{"legacy", "1 prepare", profile{osd: "1", osdProfile: "legacy-v6", osdReplicas: "3", osdStore: "bluestore"}},
		{"no OSD", "1 prepare", profile{osd: "0", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore"}},
		{"one daemon", "1 prepare", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "1", osdStore: "bluestore"}},
		{"MemStore", "1 prepare", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "memstore"}},
		{"MDS fixture", "1 prepare", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore", mdsFresh: "1"}},
		{"unbounded action", "1 raw", valid},
		{"caller pool", "1 prepare outside-pool", valid},
		{"missing action", "1", valid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := t.TempDir()
			if err := os.WriteFile(filepath.Join(out, "verify-osd-upmap"), []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			c := &fixtureCluster{profile: tc.profile}
			if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned three-OSD upmap") {
				t.Fatal("unsafe recipe reached native control", err)
			}
			entries, err := os.ReadDir(out)
			if err != nil || len(entries) != 0 {
				t.Fatal("rejected recipe acknowledged or produced output", entries, err)
			}
		})
	}
}

func TestUpmapEvidenceCollectedWithoutPrivateFiles(t *testing.T) {
	out, diagnostics := t.TempDir(), t.TempDir()
	names := []string{"osd-upmap-result.json", "osd-upmap-results.json", "osd-upmap-command.json", "osd-upmap-owned.json", "osd-upmap-state.json", "osd-upmap-commands.json", "osd-upmap-native.json", "osd-upmap-failure.json", "osd-upmap-guard-result.json", "osd-upmap-guard.log", "osd-upmap-seed-items.bin", "osd-upmap-seed-full.bin"}
	for _, name := range append(append([]string(nil), names...), "osd-upmap-private.key") {
		if err := os.WriteFile(filepath.Join(out, name), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if data, err := os.ReadFile(filepath.Join(diagnostics, name)); err != nil || string(data) != "{}" {
			t.Fatal("upmap evidence omitted", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(diagnostics, "osd-upmap-private.key")); !os.IsNotExist(err) {
		t.Fatal("private file collected", err)
	}
}

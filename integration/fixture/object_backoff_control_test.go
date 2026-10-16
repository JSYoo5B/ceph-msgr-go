package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObjectBackoffControlRefusesUnownedOrUnboundedRecipe(t *testing.T) {
	three := profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore"}
	for _, test := range []struct {
		name, input string
		profile     profile
	}{
		{"legacy profile", "1 prepare", profile{osd: "1", osdProfile: "legacy-v6", osdReplicas: "3", osdStore: "bluestore"}},
		{"no OSD", "1 prepare", profile{osd: "0", osdProfile: "replicated-pg-v8", osdReplicas: "3", osdStore: "bluestore"}},
		{"single replica", "1 prepare", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "1", osdStore: "bluestore"}},
		{"memstore", "1 prepare", profile{osd: "1", osdProfile: "replicated-pg-reply-v8", osdReplicas: "3", osdStore: "memstore"}},
		{"unsupported action", "1 reset", three},
		{"extra argument", "1 prepare outside", three},
		{"missing action", "1", three},
	} {
		t.Run(test.name, func(t *testing.T) {
			out := t.TempDir()
			if err := os.WriteFile(filepath.Join(out, "verify-osd-object-backoff"), []byte(test.input), 0o600); err != nil {
				t.Fatal(err)
			}
			// No cluster or container is supplied: admission must reject before
			// making any daemon or native command call.
			c := &fixtureCluster{profile: test.profile}
			err := c.controls(context.Background(), out)
			if err == nil || !strings.Contains(err.Error(), "invalid owned three-replica object backoff") {
				t.Fatal("unsafe object backoff recipe was not rejected locally", err)
			}
			entries, err := os.ReadDir(out)
			if err != nil || len(entries) != 0 {
				t.Fatal("rejected recipe produced an acknowledgement or diagnostic", entries, err)
			}
		})
	}
}

func TestObjectBackoffActionsMatchOracle(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "tools", "osd_object_backoff_oracle.py"))
	if err != nil {
		t.Fatal(err)
	}
	quoted := make([]string, len(objectBackoffActions))
	for i, action := range objectBackoffActions {
		quoted[i] = `"` + action + `"`
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	if !strings.Contains(text, "ACTIONS = ("+strings.Join(quoted, ", ")+")") {
		t.Fatal("fixture action set differs from the native oracle")
	}
}

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMDSMapsControlRefusesUnownedOrUnboundedRecipe(t *testing.T) {
	for _, test := range []struct {
		name, input string
		profile     profile
	}{
		{"legacy profile", "1 start", profile{osd: "1", osdProfile: "legacy-v6", osdReplicas: "1"}},
		{"no OSD", "1 start", profile{osd: "0", osdProfile: "replicated-pg-v8", osdReplicas: "1"}},
		{"three replicas", "1 start", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "3"}},
		{"unsupported action", "1 reset", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "1"}},
		{"extra argument", "1 start outside", profile{osd: "1", osdProfile: "replicated-pg-v8", osdReplicas: "1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			out := t.TempDir()
			if err := os.WriteFile(filepath.Join(out, "verify-mds-maps"), []byte(test.input), 0o600); err != nil {
				t.Fatal(err)
			}
			// No cluster or container is supplied: admission must reject before
			// making any daemon or native command call.
			c := &fixtureCluster{profile: test.profile}
			err := c.controls(context.Background(), out)
			if err == nil || !strings.Contains(err.Error(), "invalid owned single-OSD MDS maps") {
				t.Fatal("unsafe MDS maps recipe was not rejected locally", err)
			}
			entries, err := os.ReadDir(out)
			if err != nil || len(entries) != 0 {
				t.Fatal("rejected recipe produced an acknowledgement or diagnostic", entries, err)
			}
		})
	}
}

package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
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

func TestObjectBackoffNestedWaitBudgets(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "tools", "osd_object_backoff_oracle.py"))
	if err != nil {
		t.Fatal(err)
	}
	seconds := func(name string) time.Duration {
		t.Helper()
		match := regexp.MustCompile(`(?m)^` + name + ` = ([0-9]+)$`).FindSubmatch(data)
		if len(match) != 2 {
			t.Fatalf("missing bounded oracle constant %s", name)
		}
		n, err := strconv.Atoi(string(match[1]))
		if err != nil {
			t.Fatal(err)
		}
		return time.Duration(n) * time.Second
	}
	down, command, native := seconds("DOWN_WAIT_SECONDS"), seconds("COMMAND_SECONDS"), seconds("NATIVE_SECONDS")
	// A deadline check can follow OSD dump and placement/PG queries. The
	// outer command must allow those queries to finish and retain a receipt.
	if down <= 60*time.Second || objectBackoffOracleTimeout < down+3*command {
		t.Fatal("outer command truncates the down/peering observation", down, command, objectBackoffOracleTimeout)
	}
	// The append starts before the 20-second in-flight poll and a kill whose
	// local Go outcome may take 60 seconds. Include those phases, the native
	// observation, the existing 45-second join and margin for daemon controls.
	beforeObservation := (20 + 60) * time.Second
	joinAndControls := (45 + 30) * time.Second
	if native <= beforeObservation+objectBackoffOracleTimeout+joinAndControls {
		t.Fatal("native append can expire before the failover observation", native, objectBackoffOracleTimeout)
	}
}

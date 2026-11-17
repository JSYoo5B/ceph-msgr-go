package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTierOpControlIsClosed(t *testing.T) {
	out := t.TempDir()
	c := &fixtureCluster{profile: profile{osd: "0"}}
	for _, tc := range []struct {
		osd, body string
	}{{"0", "10 " + tierOpGoAction}, {"1", "11 observe"}, {"1", "12 " + tierOpGoAction + " extra"}} {
		c.profile.osd = tc.osd
		if err := os.WriteFile(filepath.Join(out, "verify-osd-tier-op"), []byte(tc.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "tier-operation") {
			t.Fatal("foreign tier-operation observer request accepted", tc, err)
		}
	}
}

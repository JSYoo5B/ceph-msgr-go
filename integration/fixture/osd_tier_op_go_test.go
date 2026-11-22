package main

import (
	"context"
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
		if err := writeRequestFile(out, "verify-osd-tier-op", tc.body); err != nil {
			t.Fatal(err)
		}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "tier-operation") {
			t.Fatal("foreign tier-operation observer request accepted", tc, err)
		}
	}
}

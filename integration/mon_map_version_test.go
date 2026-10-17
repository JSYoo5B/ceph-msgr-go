package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// MapVersion's newest committed version equals the map epoch the native
// client's MON dump commands report, for the OSD, MON and filesystem maps.
func TestCephMonMapVersionIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for _, tc := range []struct {
		kind cephmsgr.MapKind
		key  string
	}{{cephmsgr.OSDMapKind, "osdmap"}, {cephmsgr.MonMapKind, "monmap"}, {cephmsgr.FSMapKind, "fsmap"}, {cephmsgr.MDSMapKind, "fsmap"}} {
		deadline := time.Now().Add(60 * time.Second)
		for {
			got, err := c.MapVersion(ctx, tc.kind)
			if err != nil {
				t.Fatal(tc.kind, err)
			}
			native := nativePool(t, ctx, "versions", map[string]any{}).Epochs[tc.key]
			if got.Oldest == 0 || got.Oldest > got.Newest {
				t.Fatal("implausible range", tc.kind, got)
			}
			if got.Newest == native {
				t.Logf("%s: newest %d (native %d), oldest %d", tc.kind, got.Newest, native, got.Oldest)
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: Go newest %d never matched native epoch %d", tc.kind, got.Newest, native)
			}
			time.Sleep(time.Second)
		}
	}
	// Peons answer MMonGetVersion from their own committed state; every
	// monitor converges on the native epoch.
	peons := 0
	for _, pinned := range pinnedMonitorClients(t, ctx, c) {
		if !pinned.Leader {
			peons++
		}
		native := nativePool(t, ctx, "versions", map[string]any{}).Epochs["osdmap"]
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := pinned.MapVersion(ctx, cephmsgr.OSDMapKind)
			if err != nil {
				t.Fatal(pinned.Seed, err)
			}
			if got.Newest == native {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: newest %d never matched native %d", pinned.Seed, got.Newest, native)
			}
			time.Sleep(time.Second)
		}
	}
	if peons == 0 {
		t.Fatal("no monitor was a peon")
	}
}

package main

import (
	"context"
	"errors"
	"time"
)

// The native-only cache-tier operation checkpoint reuses the live cache-pool
// fixture (owned base and writeback tier, no overlay) and its scoped
// identities. Its test runs alone with capability exclusive-osd-cache-pools
// and has its own oracle and receipt; it neither extends the IGNORE_CACHE
// probe's closed 66-call matrix nor uses the product client.
const cacheTierOpsAction = "tier-ops"

// The checkpoint is selected at most once per fixture. A failed or uncertain
// run is retained in its receipt and the fixture step, never reissued.
func (c *fixtureCluster) cacheTierOpsNative(ctx context.Context) error {
	x := c.cachePools
	if x == nil || x.Resources == nil {
		return errors.New("cache tier-ops requires prepared cache-pool resources")
	}
	if x.TierOps {
		return errors.New("cache tier-ops checkpoint was already selected")
	}
	x.TierOps = true
	return x.mutate("oracle-"+cacheTierOpsAction, func() error {
		return c.runOracleCommandWithTimeout(ctx, x.Out, []string{"python3", "/out/osd_cache_tier_ops_oracle.py", "/tmp/ceph-msgr-test", "/out"}, "osd-cache-tier-ops-command.json", "closed native cache-tier operation checkpoint", 4*time.Minute)
	})
}

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"osd-cache-tier-ops-command.json",
	"osd-cache-tier-ops-native.json",
	"osd-cache-tier-ops-results.json",
)

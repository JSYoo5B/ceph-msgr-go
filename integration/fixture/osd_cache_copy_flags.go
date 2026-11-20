package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// The copy source cache-flag test reuses the live cache-pool fixture (owned
// base and writeback tier) and its scoped identities. The product client
// sends the copies; the closed native observer
// (tools/osd_cache_copy_flags_oracle.py) only seeds, observes and removes its
// own heads. For the overlay cells the test asks this control to set the
// base pool's overlay to the tier and to remove it again; each change is
// confirmed by the native OSDMap dump and never retried. Its test runs alone
// with capability exclusive-osd-cache-pools.
const cacheCopyFlagsAction = "copy-flags"

var cacheCopyFlagsObjects = map[string]bool{"go-copy-dirty": true, "go-copy-miss-0": true, "go-copy-miss-1": true, "go-copy-dst-0": true, "go-copy-dst-1": true}

var _ = defineCachePoolAction(cacheCopyFlagsAction, func(fields []string) (func(*fixtureCluster, context.Context) error, error) {
	switch {
	case len(fields) == 4 && (fields[3] == "prepare" || fields[3] == "finish"):
		phase := fields[3]
		return func(c *fixtureCluster, ctx context.Context) error { return c.cacheCopyFlagsNative(ctx, phase, "") }, nil
	case len(fields) == 4 && (fields[3] == "overlay-set" || fields[3] == "overlay-remove"):
		set := fields[3] == "overlay-set"
		return func(c *fixtureCluster, ctx context.Context) error { return c.cacheCopyFlagsOverlay(ctx, set) }, nil
	case len(fields) == 5 && fields[3] == "observe" && cacheCopyFlagsObjects[fields[4]]:
		name := fields[4]
		return func(c *fixtureCluster, ctx context.Context) error {
			return c.cacheCopyFlagsNative(ctx, "observe", name)
		}, nil
	}
	return nil, errors.New("unsupported copy cache-flag observer control")
})

func (c *fixtureCluster) cacheCopyFlagsNative(ctx context.Context, phase, name string) error {
	x := c.cachePools
	if x == nil || x.Resources == nil {
		return errors.New("copy cache-flag observer requires prepared cache-pool resources")
	}
	argv := []string{"python3", "/out/osd_cache_copy_flags_oracle.py", "/tmp/ceph-msgr-test", "/out", phase}
	step := "oracle-" + cacheCopyFlagsAction + "-" + phase
	if name != "" {
		argv = append(argv, name)
		step += "-" + name
	}
	return x.mutate(step, func() error {
		return c.runOracleCommandWithTimeout(ctx, x.Out, argv, "osd-cache-copy-flags-command.json", "closed copy cache-flag native observer", 3*time.Minute)
	})
}

// cacheCopyFlagsOverlay sets the base pool's overlay to the tier or removes
// it, once, and confirms the result in the native OSDMap dump: both the read
// and the write tier of the base pool name the tier, or neither does.
func (c *fixtureCluster) cacheCopyFlagsOverlay(ctx context.Context, set bool) error {
	x := c.cachePools
	if x == nil || x.Resources == nil {
		return errors.New("the copy overlay requires prepared cache-pool resources")
	}
	before, err := cacheCopyFlagsTiers(ctx, c, x.Plan)
	if err != nil {
		return err
	}
	if before.overlay() == set || !before.attached() {
		return errors.New("the base pool overlay is not in the state this change expects")
	}
	argv, step := []string{"osd", "tier", "rm-overlay", x.Plan.Base}, "copy-flags-overlay-remove"
	if set {
		argv, step = []string{"osd", "tier", "set-overlay", x.Plan.Base, x.Plan.Tier}, "copy-flags-overlay-set"
	}
	if err := x.mutate(step, func() error { _, e := c.cluster.Ceph(ctx, argv...); return e }); err != nil {
		return err
	}
	after, err := cacheCopyFlagsTiers(ctx, c, x.Plan)
	if err != nil {
		return err
	}
	if after.overlay() != set || !after.attached() || after.Epoch <= before.Epoch {
		return errors.New("the native OSDMap does not show the requested base pool overlay")
	}
	raw, err := json.Marshal(after)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(x.Out, "osd-cache-copy-flags-"+step[len("copy-flags-"):]+".json"), raw, 0644)
}

type cacheCopyFlagsTierState struct {
	Epoch     uint32 `json:"epoch"`
	BaseID    int64  `json:"base_id"`
	TierID    int64  `json:"tier_id"`
	TierOf    int64  `json:"tier_of"`
	ReadTier  int64  `json:"read_tier"`
	WriteTier int64  `json:"write_tier"`
	CacheMode string `json:"cache_mode"`
}

func (s cacheCopyFlagsTierState) attached() bool {
	return s.TierOf == s.BaseID && s.CacheMode == "writeback" && s.BaseID != s.TierID
}

func (s cacheCopyFlagsTierState) overlay() bool {
	return s.ReadTier == s.TierID && s.WriteTier == s.TierID
}

func cacheCopyFlagsTiers(ctx context.Context, c *fixtureCluster, plan ignoreCacheProbePlan) (cacheCopyFlagsTierState, error) {
	raw, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return cacheCopyFlagsTierState{}, err
	}
	return parseCacheCopyFlagsTiers(raw, plan.Base, plan.Tier)
}

func parseCacheCopyFlagsTiers(raw []byte, base, tier string) (cacheCopyFlagsTierState, error) {
	var dump struct {
		Epoch uint32 `json:"epoch"`
		Pools []struct {
			ID        int64  `json:"pool"`
			Name      string `json:"pool_name"`
			TierOf    int64  `json:"tier_of"`
			ReadTier  int64  `json:"read_tier"`
			WriteTier int64  `json:"write_tier"`
			CacheMode string `json:"cache_mode"`
		} `json:"pools"`
	}
	state := cacheCopyFlagsTierState{BaseID: -1, TierID: -1}
	if json.Unmarshal(raw, &dump) != nil || dump.Epoch == 0 {
		return state, errors.New("invalid native OSDMap dump")
	}
	state.Epoch = dump.Epoch
	found := 0
	for _, p := range dump.Pools {
		switch p.Name {
		case base:
			found++
			state.BaseID, state.ReadTier, state.WriteTier = p.ID, p.ReadTier, p.WriteTier
		case tier:
			found += 2
			state.TierID, state.TierOf, state.CacheMode = p.ID, p.TierOf, p.CacheMode
		}
	}
	if found != 3 {
		return state, errors.New("the native OSDMap lacks the owned base or tier pool, or names one twice")
	}
	return state, nil
}

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"osd-cache-copy-flags-command.json",
	"osd-cache-copy-flags-native.json",
	"osd-cache-copy-flags-overlay-set.json",
	"osd-cache-copy-flags-overlay-remove.json",
	"osd-cache-copy-flags-results.json",
)

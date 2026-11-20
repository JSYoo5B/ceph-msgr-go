package main

import (
	"context"
	"testing"
)

func TestCacheCopyFlagsControlIsClosed(t *testing.T) {
	x := &osdCachePoolScope{Out: t.TempDir(), Plan: ignoreCacheProbePlan{Token: "owned"}, LastID: 9}
	c := &fixtureCluster{profile: profile{osdCachePools: "1"}, cachePools: x}
	for _, fields := range [][]string{
		{"10", "owned", cacheCopyFlagsAction},
		{"10", "owned", cacheCopyFlagsAction, "observe"},
		{"10", "owned", cacheCopyFlagsAction, "observe", "foreign-head"},
		{"10", "owned", cacheCopyFlagsAction, "prepare", "extra"},
		{"10", "owned", cacheCopyFlagsAction, "overlay"},
	} {
		if err := c.osdCachePoolsControl(context.Background(), x.Out, fields); err == nil || x.LastID != 9 || len(x.Steps) != 0 {
			t.Fatal("closed control accepted", fields, err)
		}
	}
	// Without owned resources nothing runs, though the identity is consumed.
	for i, phase := range []string{"prepare", "overlay-set", "overlay-remove", "finish"} {
		id := []string{"10", "11", "12", "13"}[i]
		if err := c.osdCachePoolsControl(context.Background(), x.Out, []string{id, "owned", cacheCopyFlagsAction, phase}); err == nil || len(x.Steps) != 0 {
			t.Fatal("ran without prepared resources", phase, err)
		}
	}
}

func TestParseCacheCopyFlagsTiers(t *testing.T) {
	dump := []byte(`{"epoch":9,"pools":[{"pool":4,"pool_name":"b","tier_of":-1,"read_tier":5,"write_tier":5,"cache_mode":"none"},{"pool":5,"pool_name":"t","tier_of":4,"read_tier":-1,"write_tier":-1,"cache_mode":"writeback"}]}`)
	s, err := parseCacheCopyFlagsTiers(dump, "b", "t")
	if err != nil || !s.attached() || !s.overlay() || s.Epoch != 9 {
		t.Fatal("overlay state", s, err)
	}
	s, err = parseCacheCopyFlagsTiers([]byte(`{"epoch":9,"pools":[{"pool":4,"pool_name":"b","tier_of":-1,"read_tier":-1,"write_tier":-1},{"pool":5,"pool_name":"t","tier_of":4,"cache_mode":"writeback"}]}`), "b", "t")
	if err != nil || !s.attached() || s.overlay() || !s.noOverlay() {
		t.Fatal("no-overlay state", s, err)
	}
	for _, bad := range []string{`{"epoch":0,"pools":[]}`, `{"epoch":9,"pools":[{"pool":4,"pool_name":"b"}]}`, `{"epoch":9,"pools":[{"pool":4,"pool_name":"b"},{"pool":6,"pool_name":"b"},{"pool":5,"pool_name":"t"}]}`} {
		if _, err := parseCacheCopyFlagsTiers([]byte(bad), "b", "t"); err == nil {
			t.Fatal("accepted", bad)
		}
	}
}

func TestCacheCopyFlagsOverlayNeedsBothRoutes(t *testing.T) {
	state := func(epoch uint32, read, write int64) cacheCopyFlagsTierState {
		return cacheCopyFlagsTierState{Epoch: epoch, BaseID: 4, TierID: 5, TierOf: 4, ReadTier: read, WriteTier: write, CacheMode: "writeback"}
	}
	full, none := state(9, 5, 5), state(9, -1, -1)
	partial := map[string]cacheCopyFlagsTierState{
		"read-only-tier":  state(9, 5, -1),
		"write-only-tier": state(9, -1, 5),
		"read-other-pool": state(9, 6, 5),
		"write-other":     state(9, -1, 6),
		"zero-routes":     state(9, 0, 0),
	}
	if cacheCopyFlagsOverlayReady(none, true) != nil || cacheCopyFlagsOverlayReady(full, false) != nil {
		t.Fatal("refused a change from the opposite full state")
	}
	if cacheCopyFlagsOverlayReady(full, true) == nil || cacheCopyFlagsOverlayReady(none, false) == nil {
		t.Fatal("accepted a change to the state already present")
	}
	if cacheCopyFlagsOverlayDone(none, state(10, 5, 5), true) != nil || cacheCopyFlagsOverlayDone(full, state(10, -1, -1), false) != nil {
		t.Fatal("refused a confirmed change")
	}
	if cacheCopyFlagsOverlayDone(none, state(9, 5, 5), true) == nil || cacheCopyFlagsOverlayDone(full, state(9, -1, -1), false) == nil {
		t.Fatal("accepted a change without a newer map")
	}
	for name, p := range partial {
		for _, set := range []bool{true, false} {
			if cacheCopyFlagsOverlayReady(p, set) == nil {
				t.Fatal("accepted a partial route before the change", name, set)
			}
			after := p
			after.Epoch = 10
			if cacheCopyFlagsOverlayDone(full, after, set) == nil || cacheCopyFlagsOverlayDone(none, after, set) == nil {
				t.Fatal("confirmed a partial route after the change", name, set)
			}
		}
	}
	detached := state(10, -1, -1)
	detached.TierOf = -1
	if cacheCopyFlagsOverlayDone(full, detached, false) == nil {
		t.Fatal("confirmed a removal on a detached tier")
	}
}

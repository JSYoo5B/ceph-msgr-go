package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Monitor-managed pool snapshots are disabled for pools attached to a
// filesystem (OSDMonitor prepare_pool_op). Two owned pools are attached to a
// test-only filesystem (no MDS is needed); a pool snapshot and a self-managed
// allocation on its data pool are both a known -EOPNOTSUPP that leaves the
// pool's snapshots and snap_seq unchanged. The filesystem is removed before
// the pools.
func TestCephMonPoolOpFilesystemPoolIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1, 2)
	const fs = "ceph-msgr-poolop-fs"
	mon := func(call context.Context, fields map[string]any) error {
		encoded, _ := json.Marshal(fields)
		_, err := c.MonCommand(call, cephmsgr.Command{JSON: encoded})
		return err
	}
	if err := mon(ctx, map[string]any{"prefix": "fs new", "fs_name": fs, "metadata": "ceph-msgr-owned-pool-1", "data": "ceph-msgr-owned-pool-2", "force": true}); err != nil {
		t.Fatal("fs new on the owned pools", err)
	}
	// Registered after the pools, so it runs before their removal.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		if err := mon(cleanup, map[string]any{"prefix": "fs fail", "fs_name": fs}); err != nil {
			t.Error("fs fail", err)
		}
		if err := mon(cleanup, map[string]any{"prefix": "fs rm", "fs_name": fs, "yes_i_really_mean_it": true}); err != nil {
			t.Error("fs rm", err)
		}
	})
	data := uint32(pools.ids[2])
	info := func() poolReceipt { return nativePool(t, ctx, "poolinfo", map[string]any{"slot": 2}) }
	before := info()
	var refused *cephmsgr.PoolOpError
	code := func(result cephmsgr.PoolOpResult, err error) int32 {
		var unknown *cephmsgr.OutcomeUnknownError
		if errors.As(err, &unknown) {
			t.Fatal("pool op ended unknown", err)
		}
		if err != nil && (!errors.As(err, &refused) || refused.Code != result.Code) {
			t.Fatal("pool op error", err)
		}
		return result.Code
	}
	snap := code(c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: data, Op: cephmsgr.PoolSnapshotCreate, Name: "fs-pool-snap"}))
	if snap != -95 {
		t.Fatal("a pool snapshot of a filesystem's data pool must be -EOPNOTSUPP", snap)
	}
	if after := info(); len(after.PoolSnaps) != len(before.PoolSnaps) || after.SnapSeq != before.SnapSeq {
		t.Fatalf("the refused pool snapshot changed the pool: %+v -> %+v", before, after)
	}
	managed := code(c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: data, Op: cephmsgr.SelfManagedSnapshotCreate}))
	if after := info(); managed != -95 || after.SnapSeq != before.SnapSeq {
		t.Fatal("a self-managed allocation on a filesystem's data pool must be -EOPNOTSUPP without consuming an id", managed, after.SnapSeq)
	}
	t.Logf("filesystem data pool %d: pool snapshot code %d, self-managed allocation code %d (snap_seq %d -> %d)", data, snap, managed, before.SnapSeq, info().SnapSeq)
}

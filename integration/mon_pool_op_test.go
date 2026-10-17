package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// MPoolOp is the only route to self-managed snapshot ids (no CLI command
// exists) and also creates and removes pool snapshots by name. The independent
// native client observes the OSDMap pool state and uses the ids end to end.
func TestCephMonPoolOpIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1, 2)
	selfPool, namedPool := uint32(pools.ids[1]), uint32(pools.ids[2])
	info := func(slot int) poolReceipt { return nativePool(t, ctx, "poolinfo", map[string]any{"slot": slot}) }
	var refused *cephmsgr.PoolOpError

	t.Run("self-managed ids", func(t *testing.T) {
		first, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotCreate})
		if err != nil {
			t.Fatal(err)
		}
		id1, err := first.SnapshotID()
		if err != nil || id1 == 0 || first.Code != 0 || first.Epoch == 0 {
			t.Fatalf("first allocation %+v %v", first, err)
		}
		second, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotCreate})
		id2, _ := second.SnapshotID()
		if err != nil || id2 != id1+1 {
			t.Fatalf("second allocation %d after %d: %v", id2, id1, err)
		}
		state := info(1)
		if state.SnapSeq != id2 || state.SnapMode != "selfmanaged" {
			t.Fatalf("native pool state after allocation: seq=%d mode=%q", state.SnapSeq, state.SnapMode)
		}
		// The independent native client continues the same id sequence.
		native := nativePool(t, ctx, "selfmanaged", map[string]any{"slot": 1, "action": "create"})
		if native.SnapID != id2+1 {
			t.Fatalf("native allocation %d does not follow Go's %d", native.SnapID, id2)
		}
		// End to end: the allocated id is a real SnapContext for a Go write.
		const object = "pool-op-object"
		original := []byte("before the snapshot")
		poolSeed(t, ctx, 1, object, "", original, nil)
		route := poolPlace(t, ctx, 1, object, "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		overwrite := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("after")})
		overwrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001100}
		overwrite.SnapContext = cephmsgr.OSDSnapContext{Sequence: native.SnapID, Snapshots: []uint64{native.SnapID}}
		if _, err := o.Request(ctx, overwrite); err != nil {
			t.Fatal("write under an allocated SnapContext", err)
		}
		clone := poolObserveSnap(t, ctx, 1, object, "", "", native.SnapID)
		head := poolObserve(t, ctx, 1, object, "", "")
		if !bytes.Equal(clone.Data, original) || !bytes.Equal(head.Data, []byte("after")) {
			t.Fatalf("clone/head differ: %q %q", clone.Data, head.Data)
		}
		// Removal: first reports success, a repeat is idempotent, an id beyond the
		// sequence is ENOENT.
		if _, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: id1}); err != nil {
			t.Fatal("remove", err)
		}
		if again, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: id1}); err != nil || again.Code != 0 {
			t.Fatal("repeated removal is not idempotent", again, err)
		}
		_, err = c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: native.SnapID + 100})
		if !errors.As(err, &refused) || refused.Code != -2 {
			t.Fatal("an id beyond the sequence was not ENOENT", err)
		}
		// Removing the id that holds a clone makes the OSDs trim it: the independent
		// native client stops seeing the snapshot.
		if _, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.SelfManagedSnapshotRemove, Snapshot: native.SnapID}); err != nil {
			t.Fatal("remove the cloned id", err)
		}
		deadline := time.Now().Add(120 * time.Second)
		for {
			if state := poolObserveSnap(t, ctx, 1, object, "", "", native.SnapID); state.Code == -2 || !bytes.Equal(state.Data, original) {
				t.Logf("clone for the removed id %d trimmed (native code %d)", native.SnapID, state.Code)
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the OSDs did not trim the clone of the removed self-managed snapshot")
			}
			time.Sleep(2 * time.Second)
		}
	})

	t.Run("pool snapshots", func(t *testing.T) {
		const object, name = "pool-op-named", "go-made-snap"
		original := []byte("original bytes")
		poolSeed(t, ctx, 2, object, "", original, nil)
		created, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.PoolSnapshotCreate, Name: name})
		if err != nil || len(created.Data) != 0 {
			t.Fatal("create", created, err)
		}
		if again, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.PoolSnapshotCreate, Name: name}); err != nil || again.Code != 0 {
			t.Fatal("repeated creation is not idempotent", again, err)
		}
		state := info(2)
		found := false
		for _, s := range state.PoolSnaps {
			found = found || s.Name == name
		}
		if !found || state.SnapMode != "pool" {
			t.Fatalf("native pool state lacks the snapshot: %+v mode=%q", state.PoolSnaps, state.SnapMode)
		}
		route := poolPlace(t, ctx, 2, object, "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		overwrite := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 3, Data: []byte("new")})
		overwrite.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001200}
		if _, err := o.Request(ctx, overwrite); err != nil {
			t.Fatal(err)
		}
		if clone := poolObserve(t, ctx, 2, object, "", name); !bytes.Equal(clone.Data, original) {
			t.Fatalf("the Go-created pool snapshot did not keep the original: %q", clone.Data)
		}
		if _, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.PoolSnapshotRemove, Name: name}); err != nil {
			t.Fatal("remove", err)
		}
		if gone := info(2); len(gone.PoolSnaps) != 0 {
			t.Fatalf("snapshot still listed after removal: %+v", gone.PoolSnaps)
		}
		if missing, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.PoolSnapshotRemove, Name: name}); err != nil || missing.Code != 0 {
			t.Fatal("removing an absent snapshot is not idempotent", missing, err)
		}
	})

	t.Run("refusals are known server codes", func(t *testing.T) {
		// Pool 1 is now self-managed, pool 2 uses pool snapshots only after its first
		// one; create one again to pin that mode before the cross-mode attempts.
		if _, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.PoolSnapshotCreate, Name: "pin-mode"}); err != nil {
			t.Fatal(err)
		}
		for name, request := range map[string]struct {
			req  cephmsgr.PoolOpRequest
			code int32
		}{
			"pool snapshot in a self-managed pool": {cephmsgr.PoolOpRequest{Pool: selfPool, Op: cephmsgr.PoolSnapshotCreate, Name: "x"}, -22},
			"self-managed id in a pool-snap pool":  {cephmsgr.PoolOpRequest{Pool: namedPool, Op: cephmsgr.SelfManagedSnapshotCreate}, -22},
			"snapshot operation on an absent pool": {cephmsgr.PoolOpRequest{Pool: 99999, Op: cephmsgr.SelfManagedSnapshotCreate}, -2},
			"pool snapshot removal on absent pool": {cephmsgr.PoolOpRequest{Pool: 99999, Op: cephmsgr.PoolSnapshotRemove, Name: "x"}, -2},
		} {
			result, err := c.PoolOp(ctx, request.req)
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &refused) || errors.As(err, &unknown) || refused.Code != request.code || result.Code != request.code {
				t.Fatal(name, "expected a known signed refusal", request.code, result, err)
			}
		}
	})
}

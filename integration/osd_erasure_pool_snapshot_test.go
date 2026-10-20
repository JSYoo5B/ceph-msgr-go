package integration_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Pool snapshots on an erasure-coded pool: the OSD takes the SnapContext from
// the pool, so a Go overwrite with none keeps the pre-snapshot bytes in a clone
// that Go reads at the pool snapshot id and the native client reads by name,
// while the head returns the new bytes and the clone list names the snapshot.
func TestCephOSDErasurePoolSnapshotIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("k=2,m=1 with overwrites needs three BlueStore OSDs")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, _, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	if created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true}); created.PoolType != 3 {
		t.Fatal("pool is not erasure-coded", created.PoolType)
	}
	const object = "ec-pool-snap-object"
	original := bytes.Repeat([]byte{0, 0xff, 'e', 'c', 's'}, 2000)
	poolSeed(t, ctx, 1, object, "", original, []nativeXattrEntry{{Name: []byte("ec-snap-attr"), Value: []byte{1, 2}}})
	snap := nativePool(t, ctx, "snapshot", map[string]any{"slot": 1, "name": "owned-ec-pool-snap", "action": "create"})
	if snap.SnapID == 0 {
		t.Fatal("native pool snapshot id missing")
	}
	route := poolPlace(t, ctx, 1, object, "")
	if route.PoolType != 3 || len(route.Acting) != 3 {
		t.Fatalf("native EC placement differs: %+v", route)
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	send := func(configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := request(route, ops...)
		if ops[0].Code != cephmsgr.OSDRead && ops[0].Code != cephmsgr.OSDListSnapshots {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003200}
		}
		if configure != nil {
			configure(&r)
		}
		return o.Request(ctx, r)
	}
	newHead := bytes.Repeat([]byte("new EC head\x00"), 700)
	written, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(newHead)), Data: newHead})
	if err != nil || written.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("head overwrite in a pool-snapshot EC pool", err)
	}
	head := poolObserve(t, ctx, 1, object, "", "")
	clone := poolObserve(t, ctx, 1, object, "", "owned-ec-pool-snap")
	if !bytes.Equal(head.Data, newHead) || head.Version != written.UserVersion || !bytes.Equal(clone.Data, original) || xattrInventory(clone)["ec-snap-attr"] != string([]byte{1, 2}) {
		t.Fatalf("native head/clone differ: head %d bytes, clone %d bytes", len(head.Data), len(clone.Data))
	}
	reply, err := send(func(r *cephmsgr.OSDRequest) { r.ReadSnapshot = snap.SnapID }, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(original) + 64)})
	if err != nil || !bytes.Equal(reply.Operations[0].Data, original) {
		t.Fatal("Go read at the pool snapshot", err)
	}
	headRead, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(newHead) + 64)})
	if err != nil || !bytes.Equal(headRead.Operations[0].Data, newHead) {
		t.Fatal("Go read of the head", err)
	}
	listed, err := send(nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
	if err != nil {
		t.Fatal("list snapshots", err)
	}
	found := false
	for _, entry := range listed.Operations[0].Snapshots.Clones {
		for _, id := range entry.Snapshots {
			found = found || id == snap.SnapID
		}
	}
	if !found {
		t.Fatalf("pool snapshot id %d missing from the clone list: %+v", snap.SnapID, listed.Operations[0].Snapshots)
	}
	t.Logf("EC pool snapshot %d: clone keeps %d bytes, head %d bytes at version %d", snap.SnapID, len(original), len(newHead), written.UserVersion)
}

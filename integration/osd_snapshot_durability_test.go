package integration_test

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// A write under a self-managed SnapContext creates a clone; after ONDISK the
// clone and the new head survive SIGKILL and restart of the object's primary,
// and LIST_SNAPS still names the snapshot.
func TestCephOSDSnapshotDurabilityIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon termination persistence needs the BlueStore fixture")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	const object = "snapshot-durability"
	original := bytes.Repeat([]byte("original\x00"), 100)
	poolSeed(t, ctx, 1, object, "", original, nil)
	allocated, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(pools.ids[1]), Op: cephmsgr.SelfManagedSnapshotCreate})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := allocated.SnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	route := poolPlace(t, ctx, 1, object, "")
	o := mutationConnection(t, c, ctx, route)
	updated := []byte("updated head")
	w := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(updated)), Data: updated})
	w.ModificationTime = cephmsgr.OSDTime{Seconds: 1700006000}
	w.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
	reply, err := o.Request(ctx, w)
	o.Close()
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("ONDISK snapshot write", err)
	}
	name := strconv.FormatInt(int64(route.ID), 10)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		if !restarted {
			recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
			defer stop()
			testcluster.ControlDaemon(recover, dir, "start", "osd", name)
			waitOSDFixtureClean(t, recover, base)
		}
	}()
	if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted = true
	waitOSDFixtureClean(t, ctx, base)
	fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
	publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh))
	if head := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(head.Data, updated) || head.Version != reply.UserVersion {
		t.Fatal("native head after restart differs", head.Version, reply.UserVersion)
	}
	if clone := poolObserveSnap(t, ctx, 1, object, "", "", snap); !bytes.Equal(clone.Data, original) {
		t.Fatal("native clone after restart differs")
	}
	route = poolPlace(t, ctx, 1, object, "")
	o = mutationConnection(t, c, ctx, route)
	defer o.Close()
	list, err := o.Request(ctx, pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}))
	if err != nil {
		t.Fatal("list snapshots", err)
	}
	found := false
	for _, clone := range list.Operations[0].Snapshots.Clones {
		for _, id := range clone.Snapshots {
			found = found || id == snap
		}
	}
	if !found {
		t.Fatalf("snapshot %d missing after restart: %+v", snap, list.Operations[0].Snapshots)
	}
}

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

// Self-managed snapshots on an EC pool use an id allocated through MPoolOp and
// a write SnapContext; a write accepted while one shard's OSD is down is
// recovered onto that shard after it returns.
func TestCephOSDErasureSnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, false)
}

// The same on an optimized pool, whose partial-stripe write path and recovery
// differ inside the OSDs; nothing differs on this client's wire.
func TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, true)
}

func erasureSnapshotScenario(t *testing.T, optimized bool) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("k=2,m=1 with overwrites needs three BlueStore OSDs")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
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
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true, "optimized": optimized})
	if created.ECOptimized != optimized {
		t.Fatal("native pool optimization state differs", created.ECOptimized)
	}
	write := func(t *testing.T, object string, data []byte, snap uint64) cephmsgr.OSDReply {
		t.Helper()
		route := poolPlace(t, ctx, 1, object, "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		r := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700004000}
		if snap != 0 {
			r.SnapContext = cephmsgr.OSDSnapContext{Sequence: snap, Snapshots: []uint64{snap}}
		}
		reply, err := o.Request(ctx, r)
		if err != nil {
			t.Fatal("write", object, err)
		}
		return reply
	}

	t.Run("self-managed snapshot", func(t *testing.T) {
		original := bytes.Repeat([]byte("ec-snap-original\x00"), 300)
		write(t, "ec-snap", original, 0)
		allocated, err := c.PoolOp(ctx, cephmsgr.PoolOpRequest{Pool: uint32(created.PoolID), Op: cephmsgr.SelfManagedSnapshotCreate})
		if err != nil {
			t.Fatal("allocate", err)
		}
		snap, err := allocated.SnapshotID()
		if err != nil {
			t.Fatal(err)
		}
		updated := bytes.Repeat([]byte("ec-snap-updated\xff"), 200)
		write(t, "ec-snap", updated, snap)
		route := poolPlace(t, ctx, 1, "ec-snap", "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		read := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(original) + 64)})
		read.ReadSnapshot = snap
		reply, err := o.Request(ctx, read)
		if err != nil || !bytes.Equal(reply.Operations[0].Data, original) {
			t.Fatal("Go read at the snapshot", err)
		}
		if clone := poolObserveSnap(t, ctx, 1, "ec-snap", "", "", snap); !bytes.Equal(clone.Data, original) {
			t.Fatal("native read at the snapshot differs")
		}
		if head := poolObserve(t, ctx, 1, "ec-snap", "", ""); !bytes.Equal(head.Data, updated) {
			t.Fatal("native head differs")
		}
	})

	t.Run("write while a shard is down is recovered", func(t *testing.T) {
		object := "ec-degraded-write"
		write(t, object, []byte("before"), 0)
		route := poolPlace(t, ctx, 1, object, "")
		var victim int32 = -1
		for _, id := range route.Acting {
			if id != route.ID {
				victim = id
				break
			}
		}
		name := strconv.FormatInt(int64(victim), 10)
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
		deadline := time.Now().Add(120 * time.Second)
		for slicesContains(poolPlace(t, ctx, 1, object, "").Acting, victim) {
			if time.Now().After(deadline) {
				t.Fatal("victim never left the acting set")
			}
			time.Sleep(2 * time.Second)
		}
		payload := bytes.Repeat([]byte{0x5a, 0, 0xff, 0x11}, 3000)
		reply := write(t, object, payload, 0)
		if reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("degraded write lacks ONDISK")
		}
		if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
			t.Fatal(err)
		}
		restarted = true
		waitOSDFixtureClean(t, ctx, base)
		after := poolObserve(t, ctx, 1, object, "", "")
		if !bytes.Equal(after.Data, payload) || after.Version != reply.UserVersion {
			t.Fatal("native state after recovery differs", after.Version, reply.UserVersion)
		}
		// With the PG clean again, kill a different shard's OSD: the read now
		// needs the recovered shard written while its OSD was down.
		fresh := poolPlace(t, ctx, 1, object, "")
		var other int32 = -1
		for _, id := range fresh.Acting {
			if id != fresh.ID && id != victim {
				other = id
			}
		}
		if other >= 0 {
			otherName := strconv.FormatInt(int64(other), 10)
			if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", otherName); err != nil {
				t.Fatal(err)
			}
			defer func() {
				recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
				defer stop()
				testcluster.ControlDaemon(recover, dir, "start", "osd", otherName)
				waitOSDFixtureClean(t, recover, base)
			}()
			for slicesContains(poolPlace(t, ctx, 1, object, "").Acting, other) {
				if time.Now().After(deadline.Add(120 * time.Second)) {
					t.Fatal("second victim never left the acting set")
				}
				time.Sleep(2 * time.Second)
			}
			if n := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(n.Data, payload) {
				t.Fatal("bytes written while degraded were not recovered onto the returned shard")
			}
			t.Logf("read with osd.%d down after recovery of osd.%d matched", other, victim)
		}
	})
}

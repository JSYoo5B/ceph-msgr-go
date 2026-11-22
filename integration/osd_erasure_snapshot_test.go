package integration_test

import (
	"bytes"
	"context"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Self-managed snapshots on an EC pool use an id allocated through MPoolOp and
// a write SnapContext. A write accepted while one shard's OSD is down still
// reads back natively after that OSD returns, the PGs are clean and another
// OSD is down.
//
//scenario:covers CLIENT to OSD
//fixture:isolated kills two OSDs; restart waits osd-ready clean (second start unchecked), no pg_temp proof
func TestCephOSDErasureSnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, erasureK2M1, false, "")
}

// The same on an optimized pool, whose partial-stripe write path and recovery
// differ inside the OSDs; nothing differs on this client's wire.
//
//fixture:isolated kills two OSDs; restart waits osd-ready clean (second start unchecked), no pg_temp proof
func TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, erasureK2M1, true, "")
}

// The same on the optimized isa k=2,m=2 pool over four OSDs: one killed OSD
// leaves exactly min_size 3 shards, so the PG stays active and accepts the
// degraded write. After clean recovery a different OSD is killed and the
// native object bytes must still match. Three shards then remain, and k=2
// of them suffice to decode, so this read does not show which bytes the
// returned shard holds.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2snapshotanddegradedwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//scenario:covers Daemon-to-daemon
//fixture:isolated kills two OSDs; restart waits osd-ready clean (second start unchecked), no pg_temp proof
func TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, erasureK2M2, true, "isa")
}

// The same on the optimized isa k=3,m=1 pool over four OSDs. One killed OSD
// leaves min_size 3 shards, so the degraded write is accepted. After clean
// recovery and a second single-OSD outage exactly k=3 shards remain,
// including the returned one, so the native full-object read cannot be
// answered without the returned shard's chunk: the object spans all three
// data chunks, and with one shard missing a k=3,m=1 code needs every
// remaining shard. This shows the returned shard's participation by
// arithmetic, not its bytes on disk.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1snapshotanddegradedwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//scenario:covers Daemon-to-daemon
//fixture:isolated kills two OSDs; restart waits osd-ready clean (second start unchecked), no pg_temp proof
func TestCephOSDErasureOptimizedK3M1SnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, erasureK3M1, true, "isa")
}

// The same on the optimized isa k=4,m=2 pool over six OSDs: one killed OSD
// leaves min_size 5 shards. After recovery five shards remain during the
// second outage, one more than k, so the read does not show which shards
// were used.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2snapshotanddegradedwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//scenario:covers Daemon-to-daemon
//fixture:isolated kills two OSDs; restart waits osd-ready clean (second start unchecked), no pg_temp proof
func TestCephOSDErasureOptimizedK4M2SnapshotAndDegradedWriteIntegration(t *testing.T) {
	erasureSnapshotScenario(t, erasureK4M2, true, "isa")
}

func erasureSnapshotScenario(t *testing.T, width erasureWidth, optimized bool, plugin string) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
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
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": optimized, "plugin": plugin})
	if created.PoolType != 3 || created.ECOptimized != optimized || created.Width != width.name || created.ErasureCodeProfile != width.profile {
		t.Fatal("native pool is not the requested erasure-coded shape", width, created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, optimized)
	t.Logf("EC pool %d %s optimized=%v size=%d min_size=%d nonprimary shards %v", created.PoolID, width, optimized, facts.size, facts.minSize, facts.nonprimary)
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
		facts.checkPlacement(t, route)
		var victim int32 = -1
		for _, id := range route.Acting {
			if id != route.ID {
				victim = id
				break
			}
		}
		t.Logf("placement epoch %d pg %s acting %v primary osd.%d shard %d; killing osd.%d on shard %d",
			route.Epoch, route.PGID, route.Acting, route.ID, route.Shard, victim, slices.Index(route.Acting, victim))
		name := strconv.FormatInt(int64(victim), 10)
		restarted := false
		defer func() {
			if !restarted {
				recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
				defer stop()
				testcluster.ControlDaemon(recover, dir, "start", "osd", name)
				waitOSDFixtureClean(t, recover, base)
			}
		}()
		if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
			t.Fatal(err)
		}
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
		degraded := poolPlace(t, ctx, 1, object, "")
		if n := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(n.Data, payload) || n.Version != reply.UserVersion {
			t.Fatal("native state while degraded differs", n.Version, reply.UserVersion)
		}
		t.Logf("degraded write at epoch %d acting %v primary osd.%d shard %d: user version %d", degraded.Epoch, degraded.Acting, degraded.ID, degraded.Shard, reply.UserVersion)
		if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", name); err != nil {
			t.Fatal(err)
		}
		restarted = true
		waitOSDFixtureClean(t, ctx, base)
		after := poolObserve(t, ctx, 1, object, "", "")
		if !bytes.Equal(after.Data, payload) || after.Version != reply.UserVersion {
			t.Fatal("native state after recovery differs", after.Version, reply.UserVersion)
		}
		// With the PG clean again, kill a different shard's OSD and read
		// natively. Whether the decode uses the returned shard depends on
		// the width (k of the remaining shards suffice), so this checks only
		// the object bytes, not the returned shard's chunk.
		fresh := poolPlace(t, ctx, 1, object, "")
		var other int32 = -1
		for _, id := range fresh.Acting {
			if id != fresh.ID && id != victim {
				other = id
			}
		}
		if other >= 0 {
			otherName := strconv.FormatInt(int64(other), 10)
			defer func() {
				recover, stop := context.WithTimeout(context.Background(), 120*time.Second)
				defer stop()
				testcluster.ControlDaemon(recover, dir, "start", "osd", otherName)
				waitOSDFixtureClean(t, recover, base)
			}()
			if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", otherName); err != nil {
				t.Fatal(err)
			}
			for slicesContains(poolPlace(t, ctx, 1, object, "").Acting, other) {
				if time.Now().After(deadline.Add(120 * time.Second)) {
					t.Fatal("second victim never left the acting set")
				}
				time.Sleep(2 * time.Second)
			}
			if n := poolObserve(t, ctx, 1, object, "", ""); !bytes.Equal(n.Data, payload) {
				t.Fatal("native object bytes after clean recovery and a second single-OSD outage differ")
			}
			// With exactly k shards left every one of them, the returned
			// one included, is needed to answer a read of all k data chunks.
			// The payload must reach the last data chunk of its first stripe
			// for that to cover every data shard.
			needed := facts.size-1 == width.k && len(payload) > (width.k-1)*facts.chunk && len(payload) <= width.k*facts.chunk
			t.Logf("native object bytes matched with osd.%d down after clean recovery of osd.%d (%d shards left, k=%d, chunk %d, returned shard needed: %v)",
				other, victim, facts.size-1, width.k, facts.chunk, needed)
			if width == erasureK3M1 && !needed {
				t.Fatal("the k=3,m=1 read does not need every remaining shard", len(payload), facts.chunk)
			}
		}
	})
}

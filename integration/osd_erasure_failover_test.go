package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// On a k=2,m=1 pool: PG listing addresses the primary shard; a read through a
// degraded PG (one non-primary shard's OSD killed) reconstructs the bytes; after
// the primary is killed the new primary holds a different shard, and only a
// request carrying that new shard is answered.
func TestCephOSDErasureFailoverIntegration(t *testing.T) {
	erasureFailoverScenario(t, erasureK2M1, false, "")
}

// With allow_ec_optimizations, raw shard 1 (the second data shard) may never be
// primary: when the primary is lost the MON picks a parity shard instead, and
// the caller's placement must carry that shard.
func TestCephOSDErasureOptimizedFailoverIntegration(t *testing.T) {
	erasureFailoverScenario(t, erasureK2M1, true, "")
}

// The same phases on an optimized isa k=2,m=2 pool over the four-OSD
// topology: size 4, min_size 3 and nonprimary shard {1}, all read back from
// the native map. One killed OSD leaves three shards (min_size), so the PG
// stays active; after the primary's loss a parity shard's OSD must lead and
// the request carries that shard in primary-first numbering for pool size 4.
func TestCephOSDErasureOptimizedK2M2FailoverIntegration(t *testing.T) {
	erasureFailoverScenario(t, erasureK2M2, true, "isa")
}

func erasureFailoverScenario(t *testing.T, width erasureWidth, optimized bool, plugin string) {
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
		t.Fatal("pool is not the requested erasure-coded shape", width, created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, optimized)
	t.Logf("EC pool %d %s optimized=%v size=%d min_size=%d nonprimary shards %v owned OSDs %v",
		created.PoolID, width, optimized, facts.size, facts.minSize, facts.nonprimary, facts.owned)
	payloads := map[string][]byte{}
	var routes = map[string]osdFixtureOracle{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("ec-object-%d", i)
		data := bytes.Repeat([]byte{byte(i), 0xff, 0, byte(0x40 + i)}, 2048+i)
		route := poolPlace(t, ctx, 1, name, "")
		o := mutationConnection(t, c, ctx, route)
		r := request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003000}
		if _, err := o.Request(ctx, r); err != nil {
			t.Fatal("seed", name, err)
		}
		o.Close()
		payloads[name], routes[name] = data, route
	}
	subject := "ec-object-0"
	route := routes[subject]
	facts.checkPlacement(t, route)
	// With eight PGs the subject's PG may hold no other seeded object; add
	// objects that share it so the PG listing spans several pages.
	for i, mates := 0, 0; hash && mates < 3; i++ {
		if i == 400 {
			t.Fatal("no further object name maps to the subject's PG")
		}
		name := fmt.Sprintf("ec-pg-mate-%d", i)
		mate := poolPlace(t, ctx, 1, name, "")
		if mate.PGID != route.PGID {
			continue
		}
		data := []byte(name)
		o := mutationConnection(t, c, ctx, mate)
		r := request(mate, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700003001}
		if _, err := o.Request(ctx, r); err != nil {
			t.Fatal("seed", name, err)
		}
		o.Close()
		routes[name] = mate
		mates++
	}

	t.Run("PG listing of the primary shard", func(t *testing.T) {
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		target := cephmsgr.OSDPGListTarget{Pool: uint64(route.Pool), Seed: route.PG, Primary: route.ID}
		if !hash {
			target.Seed = route.Hash
		} else {
			target.Erasure, target.Shard, target.PrimaryFirst = true, uint8(route.Shard), routePrimaryFirst(route)
		}
		reply, err := o.ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: route.Epoch, PG: target, Namespace: "", Count: 64, MaxOutputBytes: 64 << 10})
		if err != nil {
			t.Fatal("list", err)
		}
		listed := map[string]bool{}
		for _, e := range reply.Entries {
			listed[e.Object] = true
		}
		for name, r := range routes {
			if (r.PGID == route.PGID) != listed[name] {
				t.Fatalf("listing of %s disagrees with native placement: listed=%v pg=%s vs %s", name, listed[name], r.PGID, route.PGID)
			}
		}
		// Pages of one entry each, every request carrying the previous reply's
		// cursor, list the same objects in the same order.
		var paged []string
		var cursor []byte
		for page := 0; ; page++ {
			if page > len(reply.Entries)+2 {
				t.Fatal("paging did not end", paged)
			}
			next, err := o.ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: route.Epoch, PG: target, Namespace: "", Cursor: cursor, Count: 1, MaxOutputBytes: 64 << 10})
			if err != nil {
				t.Fatal("page", page, err)
			}
			for _, e := range next.Entries {
				paged = append(paged, e.Object)
			}
			if next.End {
				break
			}
			cursor = next.Cursor.Raw
		}
		var single []string
		for _, e := range reply.Entries {
			single = append(single, e.Object)
		}
		if !slices.Equal(paged, single) {
			t.Fatalf("paged listing %v differs from the single page %v", paged, single)
		}
		t.Logf("PG %s listed %d objects, end=%v; %d single-entry pages agree", route.PGID, len(reply.Entries), reply.End, len(paged))
	})

	kill := func(t *testing.T, id int32) func() { return erasureKillOSD(t, ctx, base, id) }
	read := func(t *testing.T, r osdFixtureOracle, edit func(*cephmsgr.OSDRequest), timeout time.Duration) ([]byte, error) {
		t.Helper()
		o := mutationConnection(t, c, ctx, r)
		defer o.Close()
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(payloads[subject]) + 64)})
		if edit != nil {
			edit(&req)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		reply, err := o.Request(callCtx, req)
		if err != nil {
			return nil, err
		}
		return reply.Operations[0].Data, nil
	}

	t.Run("degraded read reconstructs the object", func(t *testing.T) {
		var victim int32 = -1
		for _, id := range route.Acting {
			if id != route.ID {
				victim = id
				break
			}
		}
		restore := kill(t, victim)
		defer restore()
		// Read only once the map has dropped the victim from the acting set, so
		// the primary must reconstruct the missing shard from the others.
		var degraded osdFixtureOracle
		deadline := time.Now().Add(120 * time.Second)
		for {
			degraded = poolPlace(t, ctx, 1, subject, "")
			if degraded.Acting[route.Shard] == route.ID && !slicesContains(degraded.Acting, victim) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the map never marked the victim down", degraded.Acting)
			}
			time.Sleep(2 * time.Second)
		}
		data, err := read(t, degraded, nil, 60*time.Second)
		if err != nil || !bytes.Equal(data, payloads[subject]) {
			t.Fatal("degraded read", err)
		}
		if n := poolObserve(t, ctx, 1, subject, "", ""); !bytes.Equal(n.Data, payloads[subject]) {
			t.Fatal("native degraded read differs")
		}
		t.Logf("read with osd.%d down: primary osd.%d shard %d acting %v", victim, degraded.ID, degraded.Shard, degraded.Acting)
	})

	t.Run("primary failover changes the shard", func(t *testing.T) {
		old := poolPlace(t, ctx, 1, subject, "")
		restore := kill(t, old.ID)
		defer restore()
		fresh := erasureSettledPrimary(t, ctx, subject, old, facts)
		t.Logf("primary osd.%d shard %d -> osd.%d shard %d at epoch %d pg_temp %v primary-first %+v (acting %v -> %v)", old.ID, old.Shard, fresh.ID, fresh.Shard, fresh.Epoch, fresh.PGTemp, routePrimaryFirst(fresh), old.Acting, fresh.Acting)
		if slices.Contains(facts.nonprimary, int(old.Shard)) {
			t.Fatal("an optimized EC pool started with a nonprimary shard as primary", old.Shard, facts.nonprimary)
		}
		if fresh.Replicas != facts.size || fresh.Nonprimary != len(facts.nonprimary) {
			t.Fatal("placement after failover changed the pool facts", fresh.Replicas, fresh.Nonprimary)
		}
		// The OSD silently discards a request older than its PG's current
		// interval, and the map may still move while peering settles. Like
		// Objecter on a new map, the caller resends with the newer placement;
		// a request is never resent under the same epoch.
		var data []byte
		var err error
		for attempt := 0; ; attempt++ {
			data, err = read(t, fresh, nil, 15*time.Second)
			if err == nil {
				break
			}
			now := poolPlace(t, ctx, 1, subject, "")
			t.Logf("attempt %d at epoch %d (osd.%d shard %d pg_temp %v): %v; placement now epoch %d osd.%d shard %d acting %v", attempt, fresh.Epoch, fresh.ID, fresh.Shard, fresh.PGTemp, err, now.Epoch, now.ID, now.Shard, now.Acting)
			if now.Epoch == fresh.Epoch || attempt == 4 {
				n := poolObserve(t, ctx, 1, subject, "", "")
				t.Fatalf("read through the new primary: %v (native code %d equal=%v)", err, n.Code, bytes.Equal(n.Data, payloads[subject]))
			}
			fresh = now
		}
		if !bytes.Equal(data, payloads[subject]) {
			t.Fatal("read through the new primary differs")
		}
		if hash && optimized && fresh.PGTemp && fresh.Shard != 0 {
			// Control: sent in the actual numbering, the shard reaches another
			// shard's PG on this OSD after the OSD's pre-Tentacle conversion.
			if _, err := read(t, fresh, func(r *cephmsgr.OSDRequest) { r.Placement.PrimaryFirst = nil }, 5*time.Second); err == nil {
				t.Fatal("the OSD answered an optimized pool's shard sent without primary-first numbering")
			}
		}
		if hash && fresh.Shard != old.Shard {
			if _, err := read(t, fresh, func(r *cephmsgr.OSDRequest) { r.Placement.Shard = uint8(old.Shard) }, 5*time.Second); err == nil {
				t.Fatal("the new primary answered a request carrying the old shard")
			}
		}
	})
}

func slicesContains(ids []int32, id int32) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

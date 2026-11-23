package integration_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A write while a shard of an optimized erasure-coded PG is an async
// recovery target or a backfill target. The pinned source (Ceph v20.2.4)
// gives the native behaviour:
//
//   - Choosing the acting set, the primary backfills an up shard whose
//     last_update is older than the authoritative log_tail
//     (src/osd/PeeringState.cc:1779-1797, calc_ec_acting). It takes another
//     up shard out of the acting set as an async recovery target when its
//     approximate missing count (its num_objects_missing plus the versions
//     its last_update is behind the authoritative one) is above
//     osd_async_recovery_min_cost and the remaining shards reach min_size and
//     can recover (:2348-2414). Either way the wanted acting set has the
//     shard's position empty and the primary asks for it as a pg_temp
//     (:2632-2663); the up set keeps the shard. With the default cost of 100
//     a shard one write behind is recovered synchronously instead
//     (src/common/options/global.yaml.in), so the async control lowers the
//     cost to 0 for its run (owned-pool oracle mode asyncrecoverycost); the
//     backfill control bounds the PG log to four entries (mode pglogbounds).
//   - A write to an object an async recovery target is missing is not
//     degraded for it, and neither is one past a backfill target's
//     last_backfill that no backfill is copying
//     (src/osd/PrimaryLogPG.cc:655-690), so the write does not wait. Its
//     sub-write to the target carries the log entries and an empty
//     transaction (should_send_op false, src/osd/PrimaryLogPG.cc:568-595;
//     src/osd/ECCommon.cc:829-846), and the reply still waits for the
//     target's commit.
//   - norecover and nobackfill keep the PG from recovering or backfilling
//     the object to the target until they are unset. Once the PG has
//     recovered, the primary puts its targets back into the acting set
//     (src/osd/PeeringState.cc:6454-6470), a new interval.
//
// The flags discipline is local to this file: a native baseline with either
// flag already set is refused, the set is read back by its exact tokens, and
// a cleanup unsets them until a separate readback shows both gone. It
// belongs in the shared recovery-flag holder of the recovery-flag tests once
// that holder is on main. These consumer helpers are test-only.

// recoveryFlags is one owned hold of the norecover and nobackfill OSDMap
// flags (owned-pool oracle mode osdflags).
type recoveryFlags struct {
	r *ecEpochRun
	// held says that the flags may be set; the cleanup unsets them while it
	// is true.
	held bool
}

// flagTokens reports whether the map's flag list holds norecover and
// nobackfill as exact tokens.
func flagTokens(flags []string) (norecover, nobackfill bool) {
	return slices.Contains(flags, "norecover"), slices.Contains(flags, "nobackfill")
}

// holdRecoveryFlags refuses a native baseline with either flag set, arms the
// cleanup, sets both and reads them back.
func (r *ecEpochRun) holdRecoveryFlags() *recoveryFlags {
	r.t.Helper()
	baseline := nativePool(r.t, r.ctx, "osdflags", map[string]any{"action": "show"}).OSDMapFlags
	if a, b := flagTokens(baseline); a || b {
		r.t.Fatalf("the native baseline already has recovery flags %v; this test owns no hold it did not set", baseline)
	}
	h := &recoveryFlags{r: r, held: true}
	r.t.Cleanup(func() {
		if !h.held {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		nativePool(r.t, cleanup, "osdflags", map[string]any{"action": "unset"})
		if a, b := flagTokens(nativePool(r.t, cleanup, "osdflags", map[string]any{"action": "show"}).OSDMapFlags); a || b {
			r.t.Error("the recovery flags are still set after the cleanup")
		}
	})
	set := nativePool(r.t, r.ctx, "osdflags", map[string]any{"action": "set"}).OSDMapFlags
	if a, b := flagTokens(set); !a || !b {
		r.t.Fatalf("the recovery flags read back as %v", set)
	}
	r.t.Logf("norecover and nobackfill set: %v (baseline %v)", set, baseline)
	return h
}

// release unsets both flags and requires a separate readback without them.
func (h *recoveryFlags) release() {
	h.r.t.Helper()
	nativePool(h.r.t, h.r.ctx, "osdflags", map[string]any{"action": "unset"})
	shown := nativePool(h.r.t, h.r.ctx, "osdflags", map[string]any{"action": "show"}).OSDMapFlags
	if a, b := flagTokens(shown); a || b {
		h.r.t.Fatalf("the recovery flags read back as %v after the unset", shown)
	}
	h.held = false
}

// asyncRecoveryCost is the oracle's receipt of mode asyncrecoverycost.
type asyncRecoveryCost struct {
	Action    string                       `json:"action"`
	Claimed   bool                         `json:"claimed"`
	Lowered   bool                         `json:"lowered"`
	Restored  bool                         `json:"restored"`
	Failures  []string                     `json:"failures"`
	Effective map[string]map[string]uint64 `json:"effective"`
}

func (r *ecEpochRun) asyncRecoveryCost(ctx context.Context, action string) asyncRecoveryCost {
	r.t.Helper()
	var result struct {
		Cost asyncRecoveryCost `json:"async_recovery_cost"`
	}
	nativePoolResult(r.t, ctx, "asyncrecoverycost", map[string]any{"action": action}, &result)
	return result.Cost
}

// lowerAsyncRecoveryCost sets osd_async_recovery_min_cost to 0 for the run
// and restores it in a cleanup.
func (r *ecEpochRun) lowerAsyncRecoveryCost() (restore func()) {
	r.t.Helper()
	restored := false
	restore = func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if got := r.asyncRecoveryCost(cleanup, "restore"); !got.Restored || got.Claimed {
			r.t.Errorf("osd_async_recovery_min_cost was not restored: %q", got.Failures)
			return
		}
		restored = true
		r.t.Log("osd_async_recovery_min_cost restored")
	}
	r.t.Cleanup(restore)
	lowered := r.asyncRecoveryCost(r.ctx, "lower")
	if !lowered.Lowered || len(lowered.Failures) != 0 {
		r.t.Fatalf("osd_async_recovery_min_cost was not lowered: %q", lowered.Failures)
	}
	r.t.Logf("osd_async_recovery_min_cost is 0 on every up OSD: %v", lowered.Effective)
	return restore
}

// recoveryTarget is how a returned shard stays out of the acting set while
// its PG is active: as an async recovery target, or as a backfill target.
type recoveryTarget struct {
	name string
	// state is the PG state token of the target's wait besides active,
	// degraded, remapped and undersized; waiting is the other one admitted:
	// with norecover or nobackfill the PG may hold its reservation and report
	// recovering or backfilling while the OSD's recovery work does nothing for
	// it (src/osd/OSD.cc activate_map -> OSDService::pause_recovery). The
	// store captures show that the target was not written either way.
	state, waiting string
}

var (
	asyncRecoveryTarget = recoveryTarget{name: "async recovery", state: "recovery_wait", waiting: "recovering"}
	backfillTarget      = recoveryTarget{name: "backfill", state: "backfill_wait", waiting: "backfilling"}
)

// targets returns the query's sets of this kind and of the other kind.
func (k recoveryTarget) targets(q *pgIntervalQuery) (own, other []string) {
	if k == backfillTarget {
		return q.BackfillTargets, q.AsyncRecoveryTargets
	}
	return q.AsyncRecoveryTargets, q.BackfillTargets
}

// remappedTargetOn requires one read-only observation to show the PG active
// with the shard at position victim as its one target of kind: two
// identical placement records around a query that origin's unchanged
// primary answered for its own shard; the placement is origin's at a newer
// epoch with the same up set, under a pg_temp whose acting set has exactly
// that position empty; the primary reports those sets, active, undersized,
// degraded and remapped, at most also the kind's wait, the target as
// osd(shard) in the kind's set and nothing in the other set, in an interval
// that started after since.
func (o pgIntervalObservation) remappedTargetOn(origin osdFixtureOracle, victim int, since uint32, kind recoveryTarget) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	if origin.PGTemp || slices.Contains(origin.Acting, crushItemNone) || victim < 0 || victim >= len(origin.Acting) || victim == int(origin.Shard) || since == 0 {
		return fmt.Errorf("position %d of %+v, since epoch %d, cannot be a %s target under its own primary", victim, origin, since, kind.name)
	}
	holed := holedSet(origin.Acting, victim)
	want := origin
	want.Acting, want.PGTemp, want.Epoch = holed, true, o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not the recorded one at a newer epoch under a pg_temp with position %d empty: %+v", *o.Before, victim, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	states := pgStates(q.State)
	core := []string{"active", "degraded", "remapped", "undersized"}
	extra := slices.DeleteFunc(slices.Clone(states), func(s string) bool { return slices.Contains(core, s) })
	if len(states)-len(extra) != len(core) || len(extra) > 1 || len(extra) == 1 && extra[0] != kind.state && (kind.waiting == "" || extra[0] != kind.waiting) {
		return fmt.Errorf("the primary reports %q, not an active remapped PG with a %s target", q.State, kind.name)
	}
	target := strconv.Itoa(int(origin.Acting[victim])) + "(" + strconv.Itoa(victim) + ")"
	if own, other := kind.targets(q); !slices.Equal(own, []string{target}) || len(other) != 0 {
		return fmt.Errorf("the primary reports %s targets %v and other targets %v; want [%s] and none", kind.name, own, other, target)
	}
	if !slices.Equal(q.Up, origin.Acting) || !slices.Equal(q.Acting, holed) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the primary reports up %v and acting %v for %s; want %v and %v for shard %d", q.Up, q.Acting, q.Info.PGID, origin.Acting, holed, origin.Shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince || q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d and the PG at %d with the primary at epoch %d; the earlier interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, q.Epoch, since, o.Before.Epoch)
	}
	return nil
}

// lowerPGLogBounds sets the PG-log bounds of the duplicate-tier control for
// the run (owned-pool oracle mode pglogbounds) and restores them in a
// cleanup.
func (r *ecEpochRun) lowerPGLogBounds() (restore func()) {
	r.t.Helper()
	restored := false
	restore = func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		var got pgLogBoundsReceipt
		nativePoolResult(r.t, cleanup, "pglogbounds", map[string]any{"action": "restore"}, &got)
		if got.Bounds.Claimed || !got.Bounds.Restored {
			r.t.Errorf("PG-log bounds were not restored: %q, central entries %v", got.Bounds.Failures, got.Bounds.Entries)
			return
		}
		restored = true
		r.t.Log("PG-log bounds restored")
	}
	r.t.Cleanup(restore)
	var set pgLogBoundsReceipt
	nativePoolResult(r.t, r.ctx, "pglogbounds", map[string]any{"action": "lower"}, &set)
	if !set.Bounds.Lowered || len(set.Bounds.Failures) != 0 || len(set.Bounds.Effective) < r.facts.size {
		r.t.Fatalf("PG-log bounds were not lowered: %q", set.Bounds.Failures)
	}
	for osd, values := range set.Bounds.Effective {
		if !maps.Equal(values, dupHistoryBounds) {
			r.t.Fatalf("osd.%s applies %v; want %v", osd, values, dupHistoryBounds)
		}
	}
	r.t.Logf("PG-log bounds %v applied on osd %v", dupHistoryBounds, slices.Sorted(maps.Keys(set.Bounds.Effective)))
	return restore
}

// backfillWrites is how many full writes the backfill control makes while
// the shard is away: with the PG log bounded to four entries and trimmed on
// every write past them, the returned shard's last_update is then older
// than the authoritative log_tail (src/osd/PeeringState.cc:1779-1797,
// calc_ec_acting: such an up shard is backfilled).
const backfillWrites = 8

// erasureRecoveryTargetWriteScenario: a Go write to an object that the
// async recovery or backfill target of its optimized EC PG does not hold in
// its current version. The OSD of the last nonprimary data shard is killed
// and the object, written once before, is written again with new content
// while it is away: once for an async recovery target, with
// osd_async_recovery_min_cost lowered to 0, and backfillWrites times for a
// backfill target, with the PG-log bounds of the duplicate-tier control.
// With norecover and nobackfill set the OSD returns, and the PG becomes
// active with that shard as its one target of the kind, outside the acting
// set under a pg_temp (remappedTargetOn). The returned shard's own store
// still holds the first content's chunk.
//
// One Go WRITEFULL of a third content with a SETXATTR, through the unchanged
// primary, must be answered ONDISK once, each operation with result 0,
// without waiting for recovery, in that interval; the PG is observed with
// the same target after it. The native object then has the third content,
// the xattr, the reply's user version and the request's mtime, while the
// target's own store still holds the first content's chunk: the write's
// sub-write to it carried no bytes (an async recovery target that misses
// the object, or a backfill target past last_backfill, gets an empty
// transaction; src/osd/PrimaryLogPG.cc:568-595). Every other shard holds
// the third content's chunk. The flags are released; the PG recovers or
// backfills, the target returns to the acting set in a newer clean
// interval, and every shard, the target included, holds its chunk of the
// third content. Native and Go reads agree; the configuration and the
// flags are restored.
//
// Not established: a write while a push or a backfill scan is in flight
// (the flags hold the target before either starts), a write to an object
// below last_backfill, an async target that leads, two targets, and which
// chunks any read used.
func erasureRecoveryTargetWriteScenario(t *testing.T, width erasureWidth, kind recoveryTarget) {
	if os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	r := startECEpochRun(t, width, 1500*time.Second)
	const (
		object      = "ec-recovery-target"
		attr        = "ec-recovery-target"
		incarnation = 81
		mutated     = 1700014002
	)
	stripe, size := r.stripeWidth(), r.facts.size
	contents := [3][]byte{}
	for i := range contents {
		contents[i] = stripedContent(byte(i+11), 2, width.k, r.facts.chunk)
		if i > 0 {
			if err := distinctChunks(contents[i-1], contents[i], width, stripe); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := distinctChunks(contents[0], contents[2], width, stripe); err != nil {
		t.Fatal(err)
	}
	route, since := r.healthy(object)
	first := r.write("first full write", route, contents[0], 1700014000, cephmsgr.OSDSnapContext{})
	r.heldThrough("the first full write", object, route)
	r.requireContent("native read of the first write", object, route, contents[0], first.UserVersion)

	writes := 1
	var restore func()
	if kind == backfillTarget {
		restore, writes = r.lowerPGLogBounds(), backfillWrites
	} else {
		restore = r.lowerAsyncRecoveryCost()
	}
	flags := r.holdRecoveryFlags()
	victim := r.facts.nonprimary[len(r.facts.nonprimary)-1]
	outage := r.kill(route.Acting[victim])
	absent := r.waitState("active without the data shard", object, func(o pgIntervalObservation) error { return o.activeWithout(route, []int{victim}, since) })
	away := *absent.After
	var second cephmsgr.OSDReply
	for i := range writes {
		second = r.write(fmt.Sprintf("full write %d of %d without the data shard", i+1, writes), away, contents[1], 1700014001, cephmsgr.OSDSnapContext{})
	}
	r.heldThrough("the writes without the data shard", object, away)
	r.requireContent("native read without the data shard", object, away, contents[1], second.UserVersion)

	r.start(outage)
	label := "with the returned shard as " + kind.name + " target"
	seen := r.waitState(label, object, func(o pgIntervalObservation) error {
		return o.remappedTargetOn(route, victim, absent.Query.SameIntervalSince, kind)
	})
	target, targetSince := *seen.After, seen.Query.SameIntervalSince
	r.logPeers(label, seen)
	r.shardBytes(label+", before the write", object, target,
		map[string][]byte{"native": contents[1], "first": contents[0]}, positions(size, "native", map[int]string{victim: "first"}))

	client := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer client.Close()
	gid := client.Snapshot().GlobalID
	o := mutationConnection(t, client.Client, r.ctx, target)
	defer o.Close()
	value := []byte{0, 0xff, 'a'}
	request := r.request(target,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(contents[2])), Data: contents[2]},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value})
	request.ClientIncarnation = incarnation
	request.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	if request.Placement == nil || int(request.Placement.Shard) != int(target.Shard) || request.Placement.PrimaryFirst == nil {
		t.Fatalf("the request carries %+v; want the primary's actual shard %d with primary-first numbering for the pg_temp", request.Placement, target.Shard)
	}
	sent := time.Now()
	reply, err := o.Request(r.ctx, request)
	took := time.Since(sent)
	t.Logf("write %s: tid %d code %d flags %#x user version %d replay %s epoch %d, results %v, %v after it was sent",
		label, reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.MapEpoch, operationResults(reply), took.Round(time.Millisecond))
	if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 0 || reply.Transaction == 0 ||
		reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != incarnation ||
		!reflect.DeepEqual(reply.Submission.Placement, request.Placement) || len(reply.Operations) != 2 || reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 {
		t.Fatalf("the write %s: err=%v code=%d flags=%#x results %v; not resent", label, err, reply.Code, reply.Flags, operationResults(reply))
	}
	if reply.UserVersion <= second.UserVersion || reply.ReplayVersion.Epoch < targetSince {
		t.Fatalf("the write is at %s, user version %d (before: %d); the interval started at epoch %d", replayVersion(reply), reply.UserVersion, second.UserVersion, targetSince)
	}
	r.heldThrough("the write "+label, object, target)
	if err := r.state(object).remappedTargetOn(route, victim, absent.Query.SameIntervalSince, kind); err != nil {
		t.Fatal("the PG after the write:", err)
	}
	written := r.requireContent("native read "+label, object, target, contents[2], reply.UserVersion)
	if written.Seconds != mutated || xattrInventory(written)[attr] != string(value) {
		t.Fatalf("native state after the write %s: mtime %d, xattrs %v", label, written.Seconds, xattrInventory(written))
	}
	r.shardBytes(label+", after the write", object, target,
		map[string][]byte{"native": contents[2], "first": contents[0], "second": contents[1]}, positions(size, "native", map[int]string{victim: "first"}))
	t.Logf("shard %d (osd.%d) still holds the first content's chunk after the acknowledged write: its sub-write carried no bytes", victim, route.Acting[victim])

	flags.release()
	r.clean()
	var back osdFixtureOracle
	r.waitState("clean with the target back in the acting set", object, func(o pgIntervalObservation) error {
		var err error
		back, err = o.cleanAgainOn(route, targetSince)
		return err
	})
	recovered := r.requireContent("native read after the "+kind.name, object, back, contents[2], reply.UserVersion)
	if recovered.Seconds != written.Seconds || !maps.Equal(xattrInventory(recovered), xattrInventory(written)) {
		t.Fatalf("native state after the %s differs: mtime %d, xattrs %v", kind.name, recovered.Seconds, xattrInventory(recovered))
	}
	r.readContent("Go read after the "+kind.name, back, contents[2], reply.UserVersion)
	r.shardBytes("after the "+kind.name, object, back, map[string][]byte{"native": contents[2], "first": contents[0]}, positions(size, "native", nil))
	t.Logf("shard %d (osd.%d) holds the third content's chunk after the %s", victim, route.Acting[victim], kind.name)
	restore()
}

// logPeers logs the primary's view of every shard's info.
func (r *ecEpochRun) logPeers(label string, o pgIntervalObservation) {
	q := o.Query
	for _, peer := range append([]pgIntervalInfo{q.Info}, q.Peers...) {
		r.t.Logf("live primary %s: %s %s last_update %s last_complete %s log_tail %s", label, peer.PGID, peer.Peer, peer.LastUpdate, peer.LastComplete, peer.LogTail)
	}
	r.t.Logf("live primary %s: state %s, async recovery targets %v, backfill targets %v, acting_recovery_backfill %v", label, q.State, q.AsyncRecoveryTargets, q.BackfillTargets, q.ActingRecoveryBackfill)
}

// operationResults lists each operation's result of a reply.
func operationResults(reply cephmsgr.OSDReply) []int32 {
	results := make([]int32, 0, len(reply.Operations))
	for _, op := range reply.Operations {
		results = append(results, op.Result)
	}
	return results
}

// TestCephOSDErasureOptimizedK2M2AsyncRecoveryWriteIntegration is erasureRecoveryTargetWriteScenario with a async recovery target on optimized isa
// k=2,m=2 (four OSDs; nonprimary shard {1}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2asyncrecoverywrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the async recovery cost (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK2M2AsyncRecoveryWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK2M2, asyncRecoveryTarget)
}

// TestCephOSDErasureOptimizedK3M1AsyncRecoveryWriteIntegration is erasureRecoveryTargetWriteScenario with a async recovery target on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1asyncrecoverywrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the async recovery cost (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK3M1AsyncRecoveryWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK3M1, asyncRecoveryTarget)
}

// TestCephOSDErasureOptimizedK4M2AsyncRecoveryWriteIntegration is erasureRecoveryTargetWriteScenario with a async recovery target on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2asyncrecoverywrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the async recovery cost (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK4M2AsyncRecoveryWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK4M2, asyncRecoveryTarget)
}

// TestCephOSDErasureOptimizedK2M2BackfillWriteIntegration is erasureRecoveryTargetWriteScenario with a backfill target on optimized isa
// k=2,m=2 (four OSDs; nonprimary shard {1}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2backfillwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the PG-log bounds (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK2M2BackfillWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK2M2, backfillTarget)
}

// TestCephOSDErasureOptimizedK3M1BackfillWriteIntegration is erasureRecoveryTargetWriteScenario with a backfill target on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1backfillwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the PG-log bounds (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK3M1BackfillWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK3M1, backfillTarget)
}

// TestCephOSDErasureOptimizedK4M2BackfillWriteIntegration is erasureRecoveryTargetWriteScenario with a backfill target on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2backfillwrite key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD, sets norecover/nobackfill and the PG-log bounds (restored), pauses OSDs to copy
func TestCephOSDErasureOptimizedK4M2BackfillWriteIntegration(t *testing.T) {
	erasureRecoveryTargetWriteScenario(t, erasureK4M2, backfillTarget)
}

// remappedTargetObservation builds one synthetic observation of PG 3.7
// recorded on acting [3 2 1 0] at epoch 25 in the interval that started at
// 24, with position 1 a target of kind at epoch 31 in the interval that
// started at 31. No daemon produced it.
func remappedTargetObservation(kind recoveryTarget) (pgIntervalObservation, osdFixtureOracle) {
	origin := osdFixtureOracle{PGID: "3.7", Up: []int32{3, 2, 1, 0}, Acting: []int32{3, 2, 1, 0}, ID: 3, PrimaryID: 3, Shard: 0, Epoch: 25, PoolType: 3, Replicas: 4, MinSize: 3, Nonprimary: 1}
	p := origin
	p.Acting, p.PGTemp, p.Epoch = []int32{3, crushItemNone, 1, 0}, true, 31
	after := p
	q := &pgIntervalQuery{State: "active+" + kind.state + "+undersized+degraded+remapped", Epoch: 31, Up: origin.Acting, Acting: p.Acting,
		SameIntervalSince: 31, LastEpochStarted: 31, Info: pgIntervalInfo{PGID: "3.7s0", Shard: 0}}
	if kind == backfillTarget {
		q.BackfillTargets = []string{"2(1)"}
	} else {
		q.AsyncRecoveryTargets = []string{"2(1)"}
	}
	return pgIntervalObservation{Before: &p, After: &after, Query: q}, origin
}

func TestErasureRemappedTargetOn(t *testing.T) {
	for _, kind := range []recoveryTarget{asyncRecoveryTarget, backfillTarget} {
		o, origin := remappedTargetObservation(kind)
		if err := o.remappedTargetOn(origin, 1, 27, kind); err != nil {
			t.Fatal(kind.name, err)
		}
		states := []string{"active+undersized+degraded+remapped"}
		if kind.waiting != "" {
			states = append(states, "active+"+kind.waiting+"+undersized+degraded+remapped")
		}
		for _, state := range states {
			o, _ := remappedTargetObservation(kind)
			o.Query.State = state
			if err := o.remappedTargetOn(origin, 1, 27, kind); err != nil {
				t.Error(kind.name, state, err)
			}
		}
		other := backfillTarget
		if kind == backfillTarget {
			other = asyncRecoveryTarget
		}
		for name, change := range map[string]func(*pgIntervalObservation){
			"no pg_temp": func(o *pgIntervalObservation) { o.Before.PGTemp, o.After.PGTemp = false, false },
			"another hole": func(o *pgIntervalObservation) {
				o.Before.Acting, o.After.Acting = []int32{3, 2, crushItemNone, 0}, []int32{3, 2, crushItemNone, 0}
			},
			"holed up set":       func(o *pgIntervalObservation) { o.Before.Up, o.After.Up = o.Before.Acting, o.Before.Acting },
			"another primary":    func(o *pgIntervalObservation) { o.Before.PrimaryID, o.After.PrimaryID = 1, 1 },
			"moved around query": func(o *pgIntervalObservation) { o.After.Epoch = 32 },
			"no target":          func(o *pgIntervalObservation) { o.Query.AsyncRecoveryTargets, o.Query.BackfillTargets = nil, nil },
			"another target":     func(o *pgIntervalObservation) { own, _ := kind.targets(o.Query); own[0] = "1(2)" },
			"the other kind": func(o *pgIntervalObservation) {
				o.Query.AsyncRecoveryTargets, o.Query.BackfillTargets = o.Query.BackfillTargets, o.Query.AsyncRecoveryTargets
			},
			"both kinds": func(o *pgIntervalObservation) {
				o.Query.AsyncRecoveryTargets, o.Query.BackfillTargets = []string{"2(1)"}, []string{"2(1)"}
			},
			"two targets": func(o *pgIntervalObservation) {
				o.Query.AsyncRecoveryTargets = append(o.Query.AsyncRecoveryTargets, "1(2)")
				o.Query.BackfillTargets = append(o.Query.BackfillTargets, "1(2)")
			},
			"clean": func(o *pgIntervalObservation) { o.Query.State = "active+clean" },
			"peered": func(o *pgIntervalObservation) {
				o.Query.State = "peered+" + kind.state + "+undersized+degraded+remapped"
			},
			"the other wait": func(o *pgIntervalObservation) {
				o.Query.State = "active+" + other.state + "+undersized+degraded+remapped"
			},
			"acting with the shard": func(o *pgIntervalObservation) { o.Query.Acting = origin.Acting },
			"old interval":          func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 27 },
			"primary behind":        func(o *pgIntervalObservation) { o.Query.Epoch = 30 },
			"another shard's info":  func(o *pgIntervalObservation) { o.Query.Info = pgIntervalInfo{PGID: "3.7s2", Shard: 2} },
		} {
			o, _ := remappedTargetObservation(kind)
			change(&o)
			if err := o.remappedTargetOn(origin, 1, 27, kind); err == nil {
				t.Error(kind.name, "accepted", name)
			}
		}
		o, _ = remappedTargetObservation(kind)
		if err := o.remappedTargetOn(origin, 0, 27, kind); err == nil {
			t.Error(kind.name, "the primary's position accepted as a target")
		}
	}
	if a, b := flagTokens([]string{"sortbitwise", "norecover", "nobackfill"}); !a || !b {
		t.Error("flag tokens")
	}
	if a, b := flagTokens([]string{"norecover_x", "nobackfills"}); a || b {
		t.Error("flag tokens matched substrings")
	}
}

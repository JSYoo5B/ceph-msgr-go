package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// What the shards of an optimized erasure-coded PG hold after the OSD of its
// primary left the PG and returned, following a partial write that left the
// nonprimary data shard out, with one write while the primary was away. The
// pinned source (Ceph v20.2.4) makes the sequence deterministic:
//
//   - A shard listed in the pool's nonprimary_shards cannot lead. When the
//     map names its OSD first, that OSD asks for a pg_temp of the acting set
//     (src/osd/PeeringState.cc:2639-2664), the MON stores it with the shards
//     that can lead first (src/osd/OSDMap.cc:2968-2989) and the first member
//     of that order that is up becomes primary (src/osd/OSDMap.cc:3060-3093):
//     for an empty shard 0, the first parity shard.
//   - The new primary holds the partial-write range of the left-out shard,
//     which every shard that can be primary records at the roll-forward
//     (src/osd/PGBackend.cc:475-490). Its peering applies the range to the
//     left-out shard's info, finds it up to date and activates it with its
//     info alone (src/osd/PeeringState.cc:324-364, :422-428, :3026-3051), so
//     that shard does not receive the entry it was left out of.
//   - The write while the primary is away is the first of its interval and
//     names every shard (src/osd/ECCommon.cc:998-1010,
//     src/osd/ECTransaction.cc:597-603). The left-out shard is a member and
//     appends the entry above its head without the version in between
//     (src/osd/PGLog.h:653-675).
//   - At the return the original primary is behind by one entry, far below
//     the cost of an asynchronous recovery (src/osd/PeeringState.cc:2380-2392,
//     osd_async_recovery_min_cost 100), so the acting set the replacement
//     wants is the up set and it drops the pg_temp
//     (src/osd/PeeringState.cc:2652-2659). The original primary leads again,
//     takes the authoritative log from a shard that can be primary
//     (src/osd/PeeringState.cc:1696-1703, :2514-2517), merges the entry it
//     lacks (:3360-3489, src/osd/PGLog.cc:455-514) and recovers the object
//     to itself before the PG is clean.
//
// The OSD leaves through the fixture's forced kill of an owned OSD and
// returns through its start, as in the existing failover tests. These
// consumer helpers are test-only and nothing is exported.

// primaryFirstOrder restates the shard order of an optimized pool's pg_temp:
// the shards that can lead in shard order, then the nonprimary ones
// (src/osd/OSDMap.cc:2968-2989).
func primaryFirstOrder(size int, nonprimary []int) []int {
	var lead, rest []int
	for shard := range size {
		if slices.Contains(nonprimary, shard) {
			rest = append(rest, shard)
		} else {
			lead = append(lead, shard)
		}
	}
	return append(lead, rest...)
}

// checkPGReplaced rechecks one observation of the PG under another primary,
// independently of the oracle's judgement (tools/osd_pg_interval_oracle.py
// replaced_failures): two identical placement records around a query that
// the first member of the primary-first order answered for its own shard as
// active and undersized, on up and acting sets that equal the recorded ones
// except for the recorded primary's empty position, under a pg_temp, at a
// newer epoch, in an interval that started after since and in which the PG
// has started. It returns the new primary's shard.
func checkPGReplaced(r pgIntervalReceipt, origin osdFixtureOracle, nonprimary []int, since uint32) (int, error) {
	if r.Action != "replaced" || !r.Settled || len(r.Failures) != 0 || r.CommandSent || r.Command != nil {
		return 0, fmt.Errorf("not an accepted observation without a command: action %q, %q", r.Action, r.Failures)
	}
	return r.Observation.replacedOn(origin, nonprimary, since)
}

// replacedOn is the judgement of checkPGReplaced over one observation alone,
// whether a pginterval receipt or a read-only pgstate observation carries it.
func (o pgIntervalObservation) replacedOn(origin osdFixtureOracle, nonprimary []int, since uint32) (int, error) {
	gone := int(origin.Shard)
	if origin.PGTemp || slices.Contains(origin.Acting, crushItemNone) || !slices.Equal(origin.Up, origin.Acting) || gone < 0 || gone >= len(origin.Acting) ||
		origin.Acting[gone] != origin.PrimaryID || origin.ID != origin.PrimaryID || since == 0 || since > origin.Epoch {
		return 0, fmt.Errorf("%+v, with an interval since epoch %d, is not a complete acting set led by its own primary", origin, since)
	}
	if len(nonprimary) == 0 || len(nonprimary) != origin.Nonprimary || slices.Contains(nonprimary, gone) {
		return 0, fmt.Errorf("the nonprimary shards %v are not those of %+v", nonprimary, origin)
	}
	holed := slices.Clone(origin.Acting)
	holed[gone] = crushItemNone
	lead := -1
	for _, shard := range primaryFirstOrder(len(holed), nonprimary) {
		if holed[shard] != crushItemNone {
			lead = shard
			break
		}
	}
	if lead < 0 || slices.Contains(nonprimary, lead) {
		return 0, fmt.Errorf("no member of %v can lead", holed)
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return 0, fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	want := origin
	want.Up, want.Acting, want.Epoch, want.PGTemp = holed, holed, o.Before.Epoch, true
	want.ID, want.PrimaryID, want.Shard, want.Address = holed[lead], holed[lead], int8(lead), o.Before.Address
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch || o.Before.Address == "" || o.Before.Address == origin.Address {
		return 0, fmt.Errorf("the placement %+v is not the recorded one at a newer epoch without its primary, led by shard %d under a pg_temp: %+v", *o.Before, lead, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return 0, fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	states := strings.Split(q.State, "+")
	slices.Sort(states)
	if !slices.Equal(states, []string{"active", "undersized"}) && !slices.Equal(states, []string{"active", "degraded", "undersized"}) {
		return 0, fmt.Errorf("the primary reports %q, not an active undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) {
		return 0, fmt.Errorf("the primary reports up %v and acting %v; want %v", q.Up, q.Acting, holed)
	}
	if q.Info.PGID != origin.PGID+"s"+strconv.Itoa(lead) || q.Info.Shard != lead {
		return 0, fmt.Errorf("the query was answered for %s shard %d, not for shard %d of %s", q.Info.PGID, q.Info.Shard, lead, origin.PGID)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince {
		return 0, fmt.Errorf("the interval started at epoch %d and the PG at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, since, o.Before.Epoch)
	}
	if q.Epoch < o.Before.Epoch {
		return 0, fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, o.Before.Epoch)
	}
	return lead, nil
}

// restarted is the recorded placement with the address under which the
// placement record names the same primary after its OSD started again: a
// restarted daemon binds with a new nonce. Any other difference is left for
// the observation's judgement, which compares every field.
func restarted(origin, seen osdFixtureOracle) (osdFixtureOracle, bool) {
	if seen.PrimaryID != origin.PrimaryID || seen.ID != origin.ID || seen.Address == "" || seen.Address == origin.Address {
		return origin, false
	}
	origin.Address = seen.Address
	return origin, true
}

// primaryOutage is one owned outage of the OSD of the PG's primary.
type primaryOutage struct {
	origin osdFixtureOracle
	osd    int32
	// since is the epoch at which the interval of origin started, replaced
	// the accepted observation of the PG under another primary and lead that
	// primary's shard.
	since    uint32
	replaced pgIntervalReceipt
	lead     int
	// started says that the fixture started the OSD again.
	started bool
}

// observeReplaced takes one observation of the PG under another primary.
func (r *partialWriteRun) observeReplaced(ctx context.Context, o *primaryOutage) (pgIntervalReceipt, int, error) {
	seen := nativePool(r.t, ctx, "pginterval", map[string]any{"action": "replaced", "slot": partialWriteSlot, "object": partialWriteObject,
		"origin": o.origin, "since": o.since}).PGInterval
	lead, err := checkPGReplaced(seen, o.origin, r.facts.nonprimary, o.since)
	return seen, lead, err
}

// showReturned observes the PG once as clean on its recorded placement with
// the primary's new address, which it takes from a placement record that
// names the recorded primary under another address. It reports false when
// the map does not name the recorded primary yet.
func (r *partialWriteRun) showReturned(ctx context.Context, origin osdFixtureOracle) (pgIntervalReceipt, osdFixtureOracle, bool) {
	back, ok := restarted(origin, poolPlace(r.t, ctx, partialWriteSlot, partialWriteObject, ""))
	if !ok {
		return pgIntervalReceipt{}, origin, false
	}
	return nativePool(r.t, ctx, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": back}).PGInterval, back, true
}

// replacePrimary takes the OSD of the PG's primary out of the PG with the
// fixture's forced kill of an owned OSD and waits, within a bounded number of
// observations, until the PG is active under the primary the map names with
// that position empty. No observation may show an active PG led by a
// nonprimary shard. It then adopts the placement record of that state. The
// cleanup it registers starts the OSD again if the test did not, and proves
// that the PG is active+clean on its original up and acting sets, primary
// and shard, under the restarted primary's new address, without a pg_temp
// before the pool is removed.
func (r *partialWriteRun) replacePrimary(since uint32) *primaryOutage {
	r.t.Helper()
	o := &primaryOutage{origin: r.route, osd: r.route.PrimaryID, since: since}
	if o.origin.ID != o.osd || o.origin.PGTemp || int(o.origin.Shard) >= len(o.origin.Acting) || o.origin.Acting[o.origin.Shard] != o.osd {
		r.t.Fatalf("the placement %+v is not led by its own primary", o.origin)
	}
	dir, name := os.Getenv("CEPH_MSGR_CONTROL_DIR"), strconv.FormatInt(int64(o.osd), 10)
	r.t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 240*time.Second)
		defer stop()
		if !o.started {
			if err := testcluster.ControlDaemon(cleanup, dir, "start", "osd", name); err != nil {
				r.t.Error("restart the killed OSD:", err)
				return
			}
		}
		err := fmt.Errorf("the map does not name osd.%d as the primary under a new address", o.osd)
		for attempt := 0; attempt < shardOutageObservations && cleanup.Err() == nil; attempt++ {
			if shown, back, ok := r.showReturned(cleanup, o.origin); ok {
				if err = checkPGIntervalShown(shown, back); err == nil {
					r.t.Logf("restored: pg %s is active+clean at epoch %d on up %v acting %v primary osd.%d shard %d without a pg_temp; osd.%d is up and in at %s",
						o.origin.PGID, shown.Observation.Before.Epoch, shown.Observation.Before.Up, shown.Observation.Before.Acting, shown.Observation.Before.PrimaryID,
						shown.Observation.Before.Shard, o.osd, back.Address)
					return
				}
			}
			time.Sleep(shardOutagePause)
		}
		r.t.Error("the PG did not return to its original placement:", err)
	})
	if err := testcluster.ControlDaemon(r.ctx, dir, "kill", "osd", name); err != nil {
		r.t.Fatal("kill", o.osd, err)
	}
	var state struct {
		Daemon   string `json:"daemon"`
		ExitCode *int   `json:"exit_code"`
	}
	if data, err := os.ReadFile(filepath.Join(dir, "osd-crash-state.json")); err != nil || json.Unmarshal(data, &state) != nil || state.Daemon != "osd."+name || state.ExitCode == nil || *state.ExitCode != 137 {
		r.t.Fatal("no SIGKILL receipt of the owned OSD", name, err)
	}
	var err error
	seen := []string{}
	for attempt := 1; attempt <= shardOutageObservations; attempt++ {
		var receipt pgIntervalReceipt
		if receipt, o.lead, err = r.observeReplaced(r.ctx, o); err == nil {
			o.replaced = receipt
			q, p := receipt.Observation.Query, receipt.Observation.Before
			r.t.Logf("pg %s without osd.%d (shard %d): epoch %d -> %d, interval since %d -> %d, up %v acting %v primary osd.%d shard %d pg_temp %v at %s, state %s; observation %d, seen before it %v",
				o.origin.PGID, o.osd, o.origin.Shard, o.origin.Epoch, p.Epoch, since, q.SameIntervalSince, q.Up, q.Acting, p.PrimaryID, p.Shard, p.PGTemp, p.Address, q.State, attempt, seen)
			break
		}
		// A nonprimary shard may be named first by the map, but its OSD
		// must never report the PG active.
		if p, q := receipt.Observation.Before, receipt.Observation.Query; p != nil && q != nil {
			if slices.Contains(r.facts.nonprimary, int(p.Shard)) && strings.Contains(q.State, "active") {
				r.t.Fatalf("osd.%d on nonprimary shard %d reports the PG %s at epoch %d", p.PrimaryID, p.Shard, q.State, p.Epoch)
			}
			step := fmt.Sprintf("epoch %d osd.%d(%d) pg_temp %v %s", p.Epoch, p.PrimaryID, p.Shard, p.PGTemp, q.State)
			if len(seen) == 0 || seen[len(seen)-1] != step {
				seen = append(seen, step)
			}
		}
		time.Sleep(shardOutagePause)
	}
	if err != nil {
		r.t.Fatalf("the PG did not become active under another primary without osd.%d: %v", o.osd, err)
	}
	r.logLivePrimary("under the replacement", o.replaced)
	fresh := poolPlace(r.t, r.ctx, partialWriteSlot, partialWriteObject, "")
	if !samePlacement(fresh, *o.replaced.Observation.After) || fresh.Epoch < o.replaced.Observation.After.Epoch {
		r.t.Fatalf("placement moved after the PG became active under another primary: %+v vs %+v", fresh, *o.replaced.Observation.After)
	}
	if routePrimaryFirst(fresh) == nil {
		r.t.Fatalf("the placement %+v under a pg_temp gives no primary-first numbering", fresh)
	}
	r.adopt(fresh)
	return o
}

// checkPGStillReplaced rechecks a later observation of the PG under the
// replacement primary: checkPGReplaced's judgement, and the replacement of the
// accepted observation, the same OSD on the same shard under the same address.
func checkPGStillReplaced(r, accepted pgIntervalReceipt, origin osdFixtureOracle, nonprimary []int, since uint32) error {
	lead, err := checkPGReplaced(r, origin, nonprimary, since)
	if err != nil {
		return err
	}
	if got, want := r.Observation.Before, accepted.Observation.Before; lead != int(want.Shard) || got.PrimaryID != want.PrimaryID || got.Address != want.Address {
		return fmt.Errorf("the PG is led by osd.%d on shard %d at %s, not by the accepted replacement osd.%d on shard %d at %s",
			got.PrimaryID, lead, got.Address, want.PrimaryID, want.Shard, want.Address)
	}
	return nil
}

// reobserveReplaced observes the PG under the replacement primary again
// (reobserveInterval) until a held observation satisfies want.
func (r *partialWriteRun) reobserveReplaced(o *primaryOutage, want func(pgIntervalReceipt) bool) (pgIntervalReceipt, int, error) {
	return reobserveInterval(o.replaced, intervalReobservations, time.Second, func() (pgIntervalReceipt, error) {
		seen := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "replaced", "slot": partialWriteSlot, "object": partialWriteObject,
			"origin": o.origin, "since": o.since}).PGInterval
		return seen, checkPGStillReplaced(seen, o.replaced, o.origin, r.facts.nonprimary, o.since)
	}, want)
}

// waitReplacedRecords observes the live replacement primary until its own
// info holds exactly the partial-write records, within a bounded number of
// observations, in the interval in which the PG was accepted under it. No
// client reply waits for the roll-forward that writes them, and the peering
// at the return reads them.
func (r *partialWriteRun) waitReplacedRecords(o *primaryOutage, records []shardPartialWrite) {
	r.t.Helper()
	var got []shardPartialWrite
	seen, attempt, err := r.reobserveReplaced(o, func(seen pgIntervalReceipt) bool {
		got = seen.Observation.Query.Info.PartialWrites
		return slices.Equal(got, records)
	})
	if err != nil {
		r.t.Fatalf("the live replacement primary does not hold the partial-write records %+v (last held: %+v): %v", records, got, err)
	}
	r.t.Logf("the live replacement primary's info holds the partial-write records %+v at epoch %d (observation %d)", got, seen.Observation.Query.Info.PartialWritesEpoch, attempt)
}

// primaryBack starts the outage's OSD again and waits, within a bounded
// number of observations, until the PG is active+clean on its original
// placement, under the restarted primary's new address, in an interval newer
// than the one under the replacement. It then adopts the placement record of
// that state.
func (r *partialWriteRun) primaryBack(o *primaryOutage) pgIntervalReceipt {
	r.t.Helper()
	// The PG must still be active under the replacement, in the interval that
	// was observed, right before the OSD starts: one held observation, past
	// samples the primary answered from an older map.
	last, attempt, err := r.reobserveReplaced(o, anyHeld)
	if err != nil {
		r.t.Fatalf("the PG is not shown active under the replacement primary in its interval before osd.%d was started: %v", o.osd, err)
	}
	r.t.Logf("pg %s still active under osd.%d at epoch %d in the interval since %d (observation %d)", o.origin.PGID, last.Observation.Before.PrimaryID,
		last.Observation.Before.Epoch, last.Observation.Query.SameIntervalSince, attempt)
	replaced := last.Observation.Query.SameIntervalSince
	if err := testcluster.ControlDaemon(r.ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "start", "osd", strconv.FormatInt(int64(o.osd), 10)); err != nil {
		r.t.Fatal("start", o.osd, err)
	}
	o.started = true
	err = fmt.Errorf("the map does not name osd.%d as the primary under a new address", o.osd)
	for attempt := 1; attempt <= shardOutageObservations; attempt++ {
		shown, back, ok := r.showReturned(r.ctx, o.origin)
		if ok {
			if err = checkPGReturned(shown, back, replaced); err == nil {
				q := shown.Observation.Query
				r.t.Logf("pg %s with osd.%d back (shard %d): epoch %d, interval since %d (under the replacement: %d), up %v acting %v primary osd.%d shard %d at %s (was %s), state %s; observation %d",
					o.origin.PGID, o.osd, o.origin.Shard, shown.Observation.Before.Epoch, q.SameIntervalSince, replaced, q.Up, q.Acting,
					shown.Observation.Before.PrimaryID, shown.Observation.Before.Shard, back.Address, o.origin.Address, q.State, attempt)
				r.logLivePrimary("after the return", shown)
				// Every primary of a new interval has the MON record its
				// up_thru before it activates; the capture needs a map that
				// no longer moves.
				if err := testcluster.ControlDaemon(r.ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "verify", "osd-ready", "clean"); err != nil {
					r.t.Fatal("the fixture did not return to every OSD up and every PG clean:", err)
				}
				// One held observation in the interval in which the PG was
				// clean again, on the recorded placement under the same
				// address, past samples the primary answered from an older
				// map. A record under another address is a departure.
				again, seen, err := reobserveInterval(shown, intervalReobservations, shardOutagePause, func() (pgIntervalReceipt, error) {
					s := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": back}).PGInterval
					return s, checkPGReturned(s, back, replaced)
				}, anyHeld)
				if err != nil {
					r.t.Fatalf("the PG left the interval since epoch %d in which it was clean again under %s: %v", q.SameIntervalSince, back.Address, err)
				}
				r.t.Logf("pg %s still clean at epoch %d in the interval since %d under %s after the readiness control (observation %d)", o.origin.PGID,
					again.Observation.Before.Epoch, q.SameIntervalSince, back.Address, seen)
				fresh := poolPlace(r.t, r.ctx, partialWriteSlot, partialWriteObject, "")
				if !samePlacement(fresh, *again.Observation.After) || fresh.Epoch < again.Observation.After.Epoch {
					r.t.Fatalf("placement moved after the PG was clean again: %+v vs %+v", fresh, *again.Observation.After)
				}
				r.facts.checkPlacement(r.t, fresh)
				r.adopt(fresh)
				return again
			}
		}
		time.Sleep(shardOutagePause)
	}
	r.t.Fatalf("the PG did not return clean to its original placement with osd.%d: %v", o.osd, err)
	return pgIntervalReceipt{}
}

// After the partial overwrite of the first control, which leaves the
// nonprimary data shard out, the primary's OSD is taken out of the PG by the
// fixture's forced kill. The PG must become active under the first parity
// shard's OSD, which the map names under a pg_temp with shard 0's position
// empty, and that primary's own info must still hold the left-out shard's
// range. The object is read back natively and through the Go client. The Go
// client then overwrites a few bytes inside the absent shard 0's chunk,
// exactly once, through the replacement primary; the write must be
// acknowledged, and it is read back natively and through the Go client. The
// replacement's info must show the range reset to that write. The OSD is
// started again and the PG must be active+clean on its original placement,
// under its original primary, in a newer interval, which includes the
// recovery of the object to the returned primary. Every acting shard's store
// is then captured: the returned primary and both parity shards hold all
// three entries; the left-out shard holds the full write and the write
// during the outage, not the overwrite between them, with its head at its
// last entry. This states what each shard's own log holds at capture time;
// it claims nothing about eviction, loss or replay.
//
//fixture:isolated kills the primary OSD; its PG proven clean without pg_temp; CRUSH rule and pool-delete config left
func TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration(t *testing.T) {
	run := startPartialWriteRun(t, 900*time.Second)
	started := run.showInterval().Observation.Query.SameIntervalSince
	state := run.partialOverwrite()
	left := run.leftOutShard(state)
	records := []shardPartialWrite{{Shard: left, From: state.want.full, To: state.want.patch}}
	// The replacement's peering applies the range only if the primary that
	// can lead recorded it; every shard that can be primary records it.
	run.waitPrimaryRecords(records)

	outage := run.replacePrimary(started)
	if slices.Contains(run.facts.nonprimary, outage.lead) || outage.lead == int(outage.origin.Shard) {
		t.Fatalf("shard %d leads; it may not be a nonprimary shard or the absent one", outage.lead)
	}
	replaced := outage.replaced.Observation.Query.SameIntervalSince
	if got := outage.replaced.Observation.Query.Info; !slices.Equal(got.PartialWrites, records) || got.LastUpdate != state.want.patch || got.LastComplete != state.want.patch {
		t.Fatalf("the replacement primary's info is at %s/%s with the records %+v; want %s and %+v", got.LastUpdate, got.LastComplete, got.PartialWrites, state.want.patch, records)
	}
	const (
		awayReadIncarnation     = 12
		degradedIncarnation     = 13
		writtenReadIncarnation  = 14
		returnedReadIncarnation = 15
	)
	run.observe("primary's outage", state.patched)
	run.readVersion("read under the replacement", awayReadIncarnation, state.patched)

	degraded := []byte("primary\x00\xffaway")
	// A few bytes into the absent shard 0's chunk of the first stripe, after
	// the earlier overwrite: by its bytes the write leaves shard 1 out.
	offset := uint64(partialWriteOffset + 40)
	gone := int(outage.origin.Shard)
	if planned, skipped := run.plan(offset, len(degraded)); !slices.Contains(planned, gone) || !slices.Equal(skipped, []int{left}) {
		t.Fatalf("the write [%d,+%d) names shards %v and leaves out %v by its bytes; this control needs shard %d's chunk without shard %d", offset, len(degraded), planned, skipped, gone, left)
	}
	reply := run.overwrite("write under the replacement", degradedIncarnation, 1700009104, offset, degraded)
	if reply.UserVersion <= state.patched.UserVersion || reply.ReplayVersion.Epoch < replaced {
		t.Fatalf("the write has user version %d (overwrite %d) and replay version %s; the interval started at epoch %d",
			reply.UserVersion, state.patched.UserVersion, replayVersion(reply), replaced)
	}
	key := run.key(degradedIncarnation, reply)
	writes := []shardWrite{
		{"full write", state.want.full, run.everyShard()},
		{"partial overwrite", state.want.patch, state.want.written},
		// The first write of an interval names every shard, the left-out one
		// included.
		{"write under the replacement", replayVersion(reply), run.everyShard()},
	}
	t.Logf("write %s [%d,+%d) at %s, user version %d, through osd.%d (shard %d) without osd.%d (shard %d)",
		key, offset, len(degraded), writes[2].version, reply.UserVersion, run.route.PrimaryID, run.route.Shard, outage.osd, gone)
	run.readVersion("read after the write", writtenReadIncarnation, reply)
	// The return's peering reads the replacement's record: the write reset
	// the left-out shard's range to itself.
	reset := []shardPartialWrite{{Shard: left, From: writes[2].version, To: writes[2].version}}
	run.waitReplacedRecords(outage, reset)

	returned := run.primaryBack(outage)
	interval := returned.Observation.Query.SameIntervalSince
	primary, shards := run.capture([]pgHistoryTarget{state.fullKey, state.patchKey, key}, state.misses, nil)
	// No shard peered after the last write without being named by it, so
	// every shard holds exactly the writes that name it.
	run.requireShardLogs("after the return", primary, shards, state.want.object, writes, false, func(int) [][]shardPartialWrite { return [][]shardPartialWrite{reset} })
	for i, shard := range shards.Shards {
		if shard.MissingEntries != 0 {
			t.Fatalf("shard %s (osd.%d) still lists %d missing objects", shard.SPG, shard.OSD, shard.MissingEntries)
		}
		if i == gone && (shard.OSD != outage.osd || shard.LastComplete != writes[2].version) {
			t.Fatalf("the returned primary's copy %s (osd.%d) is complete to %s, not to %s", shard.SPG, shard.OSD, shard.LastComplete, writes[2].version)
		}
	}
	run.observe("return", reply)
	run.readVersion("read after the return", returnedReadIncarnation, reply)
	if ended := run.showInterval().Observation.Query.SameIntervalSince; ended != interval {
		t.Fatalf("the PG's interval started at epoch %d when the primary was back and at epoch %d after the last capture", interval, ended)
	}
	t.Logf("the PG stayed in the interval that started at epoch %d", interval)
}

// pgReplacedObservation builds one synthetic receipt of PG 3.7 without the
// member of its primary's position 0: recorded on acting [3 2 1 0] at epoch
// 25 in the interval that started at 24, then active on [NONE 2 1 0] under
// osd.1 on shard 2 with a pg_temp at epoch 28 in the interval that started at
// 27. No daemon produced it.
func pgReplacedObservation() (pgIntervalReceipt, osdFixtureOracle) {
	_, route := pgIntervalChange()
	holed := []int32{crushItemNone, 2, 1, 0}
	before := route
	before.Up, before.Acting, before.Epoch, before.PGTemp = slices.Clone(holed), slices.Clone(holed), 28, true
	before.ID, before.PrimaryID, before.Shard, before.Address = 1, 1, 2, "v2:192.0.2.8:6800/3"
	after := before
	after.Up, after.Acting = slices.Clone(holed), slices.Clone(holed)
	return pgIntervalReceipt{Action: "replaced", Observations: 1, Settled: true, Observation: pgIntervalObservation{Before: &before, After: &after,
		Query: &pgIntervalQuery{State: "active+undersized+degraded", Epoch: 28, Up: slices.Clone(holed), Acting: slices.Clone(holed), SameIntervalSince: 27, LastEpochStarted: 28,
			Info: pgIntervalInfo{PGID: "3.7s2", Shard: 2, LastUpdate: "25'2", LastComplete: "25'2"}}}}, route
}

func TestCheckPGReplaced(t *testing.T) {
	receipt, route := pgReplacedObservation()
	if lead, err := checkPGReplaced(receipt, route, []int{1}, 24); err != nil || lead != 2 {
		t.Fatal("active PG under the first parity shard refused:", lead, err)
	}
	if got := primaryFirstOrder(4, []int{1}); !slices.Equal(got, []int{0, 2, 3, 1}) {
		t.Fatal("primary-first order of k=2,m=2:", got)
	}
	if got := primaryFirstOrder(6, []int{1, 2, 3}); !slices.Equal(got, []int{0, 4, 5, 1, 2, 3}) {
		t.Fatal("primary-first order of k=4,m=2:", got)
	}
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"undersized only":                   func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+undersized" },
		"another token order":               func(r *pgIntervalReceipt) { r.Observation.Query.State = "undersized+degraded+active" },
		"interval at the placement's epoch": func(r *pgIntervalReceipt) { r.Observation.Query.SameIntervalSince = 28 },
		"primary ahead":                     func(r *pgIntervalReceipt) { r.Observation.Query.Epoch = 29 },
	} {
		changed, _ := pgReplacedObservation()
		edit(&changed)
		if _, err := checkPGReplaced(changed, route, []int{1}, 24); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	if _, err := checkPGReplaced(receipt, route, []int{1}, 25); err != nil {
		t.Error("recorded interval that started at the recorded epoch refused:", err)
	}
	both := func(edit func(*osdFixtureOracle)) func(*pgIntervalReceipt) {
		return func(r *pgIntervalReceipt) {
			edit(r.Observation.Before)
			edit(r.Observation.After)
		}
	}
	lead := func(osd int32, shard int8) func(*pgIntervalReceipt) {
		return func(r *pgIntervalReceipt) {
			both(func(p *osdFixtureOracle) { p.ID, p.PrimaryID, p.Shard = osd, osd, shard })(r)
			r.Observation.Query.Info.PGID, r.Observation.Query.Info.Shard = "3.7s"+strconv.Itoa(int(shard)), int(shard)
		}
	}
	sets := func(set []int32) func(*pgIntervalReceipt) {
		return func(r *pgIntervalReceipt) {
			both(func(p *osdFixtureOracle) { p.Up, p.Acting = slices.Clone(set), slices.Clone(set) })(r)
			r.Observation.Query.Up, r.Observation.Query.Acting = slices.Clone(set), slices.Clone(set)
		}
	}
	full := []int32{3, 2, 1, 0}
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"not settled":                   func(r *pgIntervalReceipt) { r.Settled = false },
		"a recorded failure":            func(r *pgIntervalReceipt) { r.Failures = []string{"the primary reports peering"} },
		"an absent receipt":             func(r *pgIntervalReceipt) { r.Action = "absent" },
		"a command was sent":            func(r *pgIntervalReceipt) { r.CommandSent = true },
		"no placement":                  func(r *pgIntervalReceipt) { r.Observation.Before = nil },
		"placement moved":               func(r *pgIntervalReceipt) { r.Observation.After.Epoch = 29 },
		"the left-out shard leads":      lead(2, 1),
		"the second parity shard leads": lead(0, 3),
		"no pg_temp":                    both(func(p *osdFixtureOracle) { p.PGTemp = false }),
		"the original address":          both(func(p *osdFixtureOracle) { p.Address = route.Address }),
		"no address":                    both(func(p *osdFixtureOracle) { p.Address = "" }),
		"target is not the primary":     both(func(p *osdFixtureOracle) { p.ID = 0 }),
		"the primary still acting":      sets(full),
		"another position is empty":     sets([]int32{3, crushItemNone, 1, 0}),
		"two positions are empty":       sets([]int32{crushItemNone, 2, crushItemNone, 0}),
		"another OSD took the position": sets([]int32{4, 2, 1, 0}),
		"only the up set has the hole":  both(func(p *osdFixtureOracle) { p.Acting = slices.Clone(full) }),
		"the same epoch":                both(func(p *osdFixtureOracle) { p.Epoch = 25 }),
		"another PG":                    both(func(p *osdFixtureOracle) { p.PGID = "3.6" }),
		"another min_size":              both(func(p *osdFixtureOracle) { p.MinSize = 2 }),
		"query refused":                 func(r *pgIntervalReceipt) { r.Observation.Code = -11 },
		"no query":                      func(r *pgIntervalReceipt) { r.Observation.Query = nil },
		"unknown":                       func(r *pgIntervalReceipt) { r.Observation.Query.State = "unknown" },
		"peering":                       func(r *pgIntervalReceipt) { r.Observation.Query.State = "peering" },
		"clean":                         func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+clean" },
		"remapped":                      func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+undersized+remapped" },
		"primary's acting is complete":  func(r *pgIntervalReceipt) { r.Observation.Query.Acting = slices.Clone(full) },
		"answered for the old shard":    func(r *pgIntervalReceipt) { r.Observation.Query.Info.PGID = "3.7s0" },
		"answered with another shard":   func(r *pgIntervalReceipt) { r.Observation.Query.Info.Shard = 3 },
		"the recorded interval": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 24, 25
		},
		"an interval after the placement": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 29, 29
		},
		"not started in its interval":  func(r *pgIntervalReceipt) { r.Observation.Query.LastEpochStarted = 25 },
		"primary behind the placement": func(r *pgIntervalReceipt) { r.Observation.Query.Epoch = 27 },
	} {
		changed, _ := pgReplacedObservation()
		edit(&changed)
		if _, err := checkPGReplaced(changed, route, []int{1}, 24); err == nil {
			t.Error("observation accepted:", name)
		}
	}
	for name, c := range map[string]struct {
		nonprimary []int
		since      uint32
		edit       func(*osdFixtureOracle)
	}{
		"no nonprimary shards":            {nil, 24, nil},
		"more than the map counts":        {[]int{1, 2}, 24, nil},
		"the primary's shard":             {[]int{0}, 24, nil},
		"no recorded interval":            {[]int{1}, 0, nil},
		"an interval after the placement": {[]int{1}, 26, nil},
		"a recorded hole": {[]int{1}, 24, func(p *osdFixtureOracle) {
			p.Up, p.Acting = []int32{3, crushItemNone, 1, 0}, []int32{3, crushItemNone, 1, 0}
		}},
		"recorded under a pg_temp":       {[]int{1}, 24, func(p *osdFixtureOracle) { p.PGTemp = true }},
		"recorded off its primary":       {[]int{1}, 24, func(p *osdFixtureOracle) { p.ID = 0 }},
		"recorded primary off its shard": {[]int{1}, 24, func(p *osdFixtureOracle) { p.Shard = 1 }},
		"only nonprimary shards remain": {[]int{1, 2, 3}, 24, func(p *osdFixtureOracle) {
			p.Nonprimary = 3
		}},
	} {
		origin := route
		if c.edit != nil {
			c.edit(&origin)
		}
		if _, err := checkPGReplaced(receipt, origin, c.nonprimary, c.since); err == nil {
			t.Error("request accepted:", name)
		}
	}

	// The restarted primary: the same placement under a new address only.
	seen := route
	seen.Address, seen.Epoch = "v2:192.0.2.7:6800/9", 32
	want := route
	want.Address = seen.Address
	if back, ok := restarted(route, seen); !ok || !reflect.DeepEqual(back, want) {
		t.Fatal("restarted primary not adopted:", back, ok)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"the same address":    func(p *osdFixtureOracle) { p.Address = route.Address },
		"no address":          func(p *osdFixtureOracle) { p.Address = "" },
		"another primary":     func(p *osdFixtureOracle) { p.PrimaryID, p.ID = 1, 1 },
		"target not primary":  func(p *osdFixtureOracle) { p.ID = 1 },
		"the replacement led": func(p *osdFixtureOracle) { p.PrimaryID, p.ID, p.Shard = 1, 1, 2 },
	} {
		changed := seen
		edit(&changed)
		if back, ok := restarted(route, changed); ok || !reflect.DeepEqual(back, route) {
			t.Error("restart accepted:", name)
		}
	}
}

// TestReobserveReplaced covers the later observations of the PG under the
// replacement primary: a sample from a primary behind the placement's epoch
// is retried, never accepted, and a confirmed change of interval or of the
// replacement fails at once.
func TestReobserveReplaced(t *testing.T) {
	accepted, route := pgReplacedObservation()
	held := func(epoch uint32) pgIntervalReceipt {
		r, _ := pgReplacedObservation()
		r.Observation.Before.Epoch, r.Observation.After.Epoch, r.Observation.Query.Epoch = epoch, epoch, epoch
		return r
	}
	edited := func(r pgIntervalReceipt, edit func(*pgIntervalReceipt)) pgIntervalReceipt {
		edit(&r)
		return r
	}
	// The MON published epoch 29 with the PG's sets unchanged, and the
	// replacement still answered from epoch 28. The oracle refuses it.
	lagging := func(interval uint32) pgIntervalReceipt {
		return edited(held(29), func(r *pgIntervalReceipt) {
			r.Observation.Query.Epoch, r.Observation.Query.SameIntervalSince = 28, interval
			r.Settled, r.Failures = false, []string{"the primary is at map epoch 28, behind the placement's epoch"}
		})
	}
	recorded := func(r pgIntervalReceipt) pgIntervalReceipt {
		return edited(r, func(r *pgIntervalReceipt) {
			r.Observation.Query.Info.PartialWrites = []shardPartialWrite{{Shard: 1, From: "28'3", To: "28'3"}}
		})
	}
	readdressed := func(r *pgIntervalReceipt) {
		r.Observation.Before.Address, r.Observation.After.Address = "v2:192.0.2.8:6800/4", "v2:192.0.2.8:6800/4"
	}
	// The second parity shard's OSD leads a later interval: the map no
	// longer names the accepted replacement.
	another := edited(held(30), func(r *pgIntervalReceipt) {
		for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
			p.ID, p.PrimaryID, p.Shard, p.Address = 0, 0, 3, "v2:192.0.2.9:6800/5"
		}
		r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 30, 30
		r.Observation.Query.Info.PGID, r.Observation.Query.Info.Shard = "3.7s3", 3
	})
	// The original primary is back in a later interval.
	back := edited(held(30), func(r *pgIntervalReceipt) {
		full := []int32{3, 2, 1, 0}
		for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
			p.Up, p.Acting, p.PGTemp, p.ID, p.PrimaryID, p.Shard = slices.Clone(full), slices.Clone(full), false, 3, 3, 0
		}
		q := r.Observation.Query
		q.State, q.Up, q.Acting, q.SameIntervalSince, q.LastEpochStarted = "active+clean", slices.Clone(full), slices.Clone(full), 30, 30
		q.Info.PGID, q.Info.Shard = "3.7s0", 0
	})
	hasRecords := func(r pgIntervalReceipt) bool { return len(r.Observation.Query.Info.PartialWrites) == 1 }
	for name, c := range map[string]struct {
		samples []pgIntervalReceipt
		want    func(pgIntervalReceipt) bool
		attempt int
		ok      bool
	}{
		"held at once":                          {[]pgIntervalReceipt{held(28)}, anyHeld, 1, true},
		"held at a newer epoch":                 {[]pgIntervalReceipt{held(29)}, anyHeld, 1, true},
		"lagging, then held":                    {[]pgIntervalReceipt{lagging(27), lagging(27), held(29)}, anyHeld, 3, true},
		"lagging, then the records":             {[]pgIntervalReceipt{held(28), lagging(27), recorded(held(29))}, hasRecords, 3, true},
		"another address, lagging, then held":   {[]pgIntervalReceipt{edited(lagging(27), readdressed), held(29)}, anyHeld, 2, true},
		"persistent lag":                        {[]pgIntervalReceipt{lagging(27), lagging(27), lagging(27), lagging(27)}, anyHeld, 4, false},
		"the records never appear":              {[]pgIntervalReceipt{held(28), held(28), held(28), held(28)}, hasRecords, 4, false},
		"the replacement under another address": {[]pgIntervalReceipt{edited(held(29), readdressed), held(29)}, anyHeld, 1, false},
		"another replacement":                   {[]pgIntervalReceipt{another, held(30)}, anyHeld, 1, false},
		"lagging, then another replacement":     {[]pgIntervalReceipt{lagging(27), another}, anyHeld, 2, false},
		"a later interval, lagging":             {[]pgIntervalReceipt{lagging(29), held(29)}, anyHeld, 1, false},
		"the original primary back":             {[]pgIntervalReceipt{back, held(30)}, anyHeld, 1, false},
		"lagging, then peering": {[]pgIntervalReceipt{lagging(27), edited(held(29), func(r *pgIntervalReceipt) { r.Observation.Query.State = "peering" }), held(29)},
			anyHeld, 2, false},
	} {
		next := 0
		observe := func() (pgIntervalReceipt, error) {
			r := c.samples[min(next, len(c.samples)-1)]
			next++
			return r, checkPGStillReplaced(r, accepted, route, []int{1}, 24)
		}
		seen, attempt, err := reobserveInterval(accepted, 4, 0, observe, c.want)
		if (err == nil) != c.ok || attempt != c.attempt || next != c.attempt {
			t.Errorf("%s: attempt %d after %d observations, error %v; want attempt %d, accepted %t", name, attempt, next, err, c.attempt, c.ok)
			continue
		}
		if c.ok {
			if err := checkPGStillReplaced(seen, accepted, route, []int{1}, 24); err != nil || seen.Observation.Query.SameIntervalSince != 27 {
				t.Errorf("%s: returned an observation that is not held in the interval since 27: %v", name, err)
			}
		}
	}
}

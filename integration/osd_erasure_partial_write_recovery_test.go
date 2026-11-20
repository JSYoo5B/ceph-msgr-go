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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// What a nonprimary data shard of an optimized erasure-coded PG holds after
// its OSD left the PG and returned, following a partial write that left the
// shard out. The pinned source (Ceph v20.2.4) makes two cases deterministic:
//
//   - Nothing was written while the OSD was away. The primary still holds the
//     partial-write range of the shard, applies it to the returning shard's
//     info, finds it up to date and activates it with its info alone
//     (src/osd/PeeringState.cc:324-364, :422-428, :3026-3051). As after any
//     interval change, the shard's head is then past its last entry and it
//     has not received the entry.
//   - One write was acknowledged while the OSD was away. It is the first
//     write of its interval and names every shard
//     (src/osd/ECCommon.cc:998-1010, src/osd/ECTransaction.cc:597-603). Its
//     roll-forward on a shard that can be primary resets the absent shard's
//     range to the empty range at that write (src/osd/PGBackend.cc:475-490),
//     which starts after the returning shard's last_update and so cannot
//     advance it (src/osd/PeeringState.cc:324-364). The primary then sends
//     every log entry after the shard's last_update
//     (src/osd/PeeringState.cc:3103-3112), the shard appends all of them
//     (src/osd/PGLog.cc:455-514, src/osd/PGLog.h:1422-1436) and only the
//     entries that name it make its object missing
//     (src/osd/osd_types.h:5161-5203). The returned shard
//     therefore holds the entry of the overwrite it was left out of, received
//     from the primary's log, and the object is recovered to it.
//
// The OSD leaves through the fixture's forced kill of an owned OSD and
// returns through its start, as in the existing degraded-write tests. These
// consumer helpers are test-only and nothing is exported.

// checkPGAbsent rechecks one observation of the PG without the member of one
// acting position, independently of the oracle's judgement
// (tools/osd_pg_interval_oracle.py absent_failures): two identical placement
// records around a query that the unchanged primary answered for its own
// shard as active and undersized, on up and acting sets that equal the
// recorded ones except for that one empty position, without a pg_temp, at a
// newer epoch, in an interval that started after since and in which the PG
// has started.
func checkPGAbsent(r pgIntervalReceipt, origin osdFixtureOracle, shard int, since uint32) error {
	if r.Action != "absent" || !r.Settled || len(r.Failures) != 0 || r.CommandSent || r.Command != nil {
		return fmt.Errorf("not an accepted observation without a command: action %q, %q", r.Action, r.Failures)
	}
	if shard < 0 || shard >= len(origin.Acting) || shard == int(origin.Shard) || slices.Contains(origin.Acting, crushItemNone) || since == 0 || since > origin.Epoch {
		return fmt.Errorf("position %d of %v, with an interval since epoch %d, is not an absent member of a complete acting set", shard, origin.Acting, since)
	}
	o := r.Observation
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	holed := slices.Clone(origin.Acting)
	holed[shard] = crushItemNone
	want := origin
	want.Up, want.Acting, want.Epoch = holed, holed, o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch || o.Before.PGTemp {
		return fmt.Errorf("the placement %+v is not the recorded one at a newer epoch with position %d alone empty: %+v", *o.Before, shard, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	states := strings.Split(q.State, "+")
	slices.Sort(states)
	if !slices.Equal(states, []string{"active", "undersized"}) && !slices.Equal(states, []string{"active", "degraded", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not an active undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) {
		return fmt.Errorf("the primary reports up %v and acting %v; want %v", q.Up, q.Acting, holed)
	}
	if q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the query was answered for %s shard %d, not for shard %d of %s", q.Info.PGID, q.Info.Shard, origin.Shard, origin.PGID)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince {
		return fmt.Errorf("the interval started at epoch %d and the PG at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, since, o.Before.Epoch)
	}
	if q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, o.Before.Epoch)
	}
	return nil
}

// checkPGReturned requires the clean observation after the OSD's return to
// be in an interval that started after the one in which the PG was active
// without it.
func checkPGReturned(r pgIntervalReceipt, origin osdFixtureOracle, absent uint32) error {
	if err := checkPGIntervalShown(r, origin); err != nil {
		return err
	}
	if q, epoch := r.Observation.Query, r.Observation.Before.Epoch; absent == 0 || q.SameIntervalSince <= absent || q.SameIntervalSince > epoch {
		return fmt.Errorf("the interval started at epoch %d; the PG was active without the member in the interval since epoch %d and the placement is at epoch %d", q.SameIntervalSince, absent, epoch)
	}
	return nil
}

// Bounds of the observations around an outage. The MON marks a killed OSD
// down once its peers report it, at the latest after their heartbeat grace,
// and a returning OSD boots, peers and recovers before the PG is clean.
const (
	shardOutageObservations = 150
	shardOutagePause        = time.Second
)

// shardOutage is one owned outage of the OSD that holds one acting shard.
type shardOutage struct {
	origin osdFixtureOracle
	shard  int
	osd    int32
	// since is the epoch at which the interval of origin started, absent the
	// accepted observation of the PG without the member.
	since  uint32
	absent pgIntervalReceipt
	// started says that the fixture started the OSD again.
	started bool
}

// adopt switches the run to a new placement record and a session to its
// primary.
func (r *partialWriteRun) adopt(fresh osdFixtureOracle) {
	r.t.Helper()
	r.connection.Close()
	r.route = fresh
	r.connection = mutationConnection(r.t, r.client, r.ctx, fresh)
}

// observeAbsent takes one observation of the PG without the outage's member.
func (r *partialWriteRun) observeAbsent(ctx context.Context, o *shardOutage) (pgIntervalReceipt, error) {
	seen := nativePool(r.t, ctx, "pginterval", map[string]any{"action": "absent", "slot": partialWriteSlot, "object": partialWriteObject,
		"origin": o.origin, "shard": o.shard, "since": o.since}).PGInterval
	return seen, checkPGAbsent(seen, o.origin, o.shard, o.since)
}

// leave takes the OSD of one nonprimary acting shard out of the PG with the
// fixture's forced kill of an owned OSD and waits, within a bounded number
// of observations, until the PG is active on its original primary with that
// one position empty. It then adopts the placement record of that state. The
// cleanup it registers starts the OSD again if the test did not, and proves
// that the PG is active+clean on its original up and acting sets and primary
// without a pg_temp before the pool is removed; an up set that names the OSD
// again means that the map has it up and in.
func (r *partialWriteRun) leave(shard int, since uint32) *shardOutage {
	r.t.Helper()
	o := &shardOutage{origin: r.route, shard: shard, since: since}
	if !slices.Contains(r.facts.nonprimary, shard) || shard >= len(o.origin.Acting) || shard == int(o.origin.Shard) {
		r.t.Fatalf("shard %d is not a nonprimary shard of acting %v led by shard %d", shard, o.origin.Acting, o.origin.Shard)
	}
	o.osd = o.origin.Acting[shard]
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
		var err error
		for attempt := 0; attempt < shardOutageObservations && cleanup.Err() == nil; attempt++ {
			shown := nativePool(r.t, cleanup, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": o.origin}).PGInterval
			if err = checkPGIntervalShown(shown, o.origin); err == nil {
				r.t.Logf("restored: pg %s is active+clean at epoch %d on up %v acting %v primary osd.%d without a pg_temp; osd.%d is up and in",
					o.origin.PGID, shown.Observation.Before.Epoch, shown.Observation.Before.Up, shown.Observation.Before.Acting, shown.Observation.Before.PrimaryID, o.osd)
				return
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
	states := []string{}
	for attempt := 1; attempt <= shardOutageObservations; attempt++ {
		var seen pgIntervalReceipt
		if seen, err = r.observeAbsent(r.ctx, o); err == nil {
			o.absent = seen
			q := seen.Observation.Query
			r.t.Logf("pg %s without osd.%d (shard %d): epoch %d -> %d, interval since %d -> %d, up %v acting %v primary osd.%d shard %d, state %s; observation %d, states before it %v",
				o.origin.PGID, o.osd, shard, o.origin.Epoch, seen.Observation.Before.Epoch, since, q.SameIntervalSince, q.Up, q.Acting,
				seen.Observation.Before.PrimaryID, seen.Observation.Before.Shard, q.State, attempt, states)
			break
		}
		if q := seen.Observation.Query; q != nil && (len(states) == 0 || states[len(states)-1] != q.State) {
			states = append(states, q.State)
		}
		time.Sleep(shardOutagePause)
	}
	if err != nil {
		r.t.Fatalf("the PG did not become active without osd.%d: %v", o.osd, err)
	}
	r.logLivePrimary("without the member", o.absent)
	fresh := poolPlace(r.t, r.ctx, partialWriteSlot, partialWriteObject, "")
	if !samePlacement(fresh, *o.absent.Observation.After) || fresh.Epoch < o.absent.Observation.After.Epoch {
		r.t.Fatalf("placement moved after the PG became active without osd.%d: %+v vs %+v", o.osd, fresh, *o.absent.Observation.After)
	}
	r.adopt(fresh)
	return o
}

func (r *partialWriteRun) logLivePrimary(label string, seen pgIntervalReceipt) {
	q := seen.Observation.Query
	for _, peer := range append([]pgIntervalInfo{q.Info}, q.Peers...) {
		r.t.Logf("live primary %s: %s %s last_update %s last_complete %s log_tail %s, partial-write records %+v at epoch %d",
			label, peer.PGID, peer.Peer, peer.LastUpdate, peer.LastComplete, peer.LogTail, peer.PartialWrites, peer.PartialWritesEpoch)
	}
}

// intervalReobservations bounds the later observations of a PG state after
// its first accepted observation. The MON may publish an epoch that leaves
// the PG's sets unchanged (another OSD's up_thru, for example) before the
// primary has it; the primary then answers from its older map and that sample
// shows nothing about the PG's interval at the placement's epoch.
const intervalReobservations = 30

// intervalHeld classifies one later observation of a PG state against the
// accepted observation of that state, given the state's own check verdict err
// on it. It is held when err is nil and the primary reports the accepted
// interval. A primary that reports another interval has left it: a primary's
// map epoch does not go back, and the accepted observation's primary was not
// behind its placement, so a later interval start is a confirmed change even
// from a primary behind the current placement. Without that, a sample shows
// nothing and is inconclusive when a placement record or the query reply is
// missing, the placement changed around the query, or the primary is behind
// the placement's epoch. Every other refusal is a confirmed departure. The
// second result is nil exactly when the sample is held or inconclusive.
func intervalHeld(r pgIntervalReceipt, err error, accepted pgIntervalReceipt) (bool, error) {
	o, interval := r.Observation, accepted.Observation.Query.SameIntervalSince
	if q := o.Query; q != nil && q.SameIntervalSince != interval {
		return false, fmt.Errorf("the primary reports the interval since epoch %d, not the accepted one since epoch %d (%v)", q.SameIntervalSince, interval, err)
	}
	if err == nil {
		return true, nil
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) || o.Query == nil || o.Query.Epoch < o.Before.Epoch {
		return false, nil
	}
	return false, err
}

// reobserveInterval observes a PG state again, within attempts observations
// spaced by pause, until one is held in the accepted interval (intervalHeld)
// and satisfies want. It fails at the first confirmed departure, and when no
// held observation satisfies want within the bound; an inconclusive sample
// never stands in for a held one. observe returns one sample and the state's
// own check verdict on it, unchanged.
func reobserveInterval(accepted pgIntervalReceipt, attempts int, pause time.Duration, observe func() (pgIntervalReceipt, error), want func(pgIntervalReceipt) bool) (pgIntervalReceipt, int, error) {
	last := "no observation"
	for attempt := 1; attempt <= attempts; attempt++ {
		seen, err := observe()
		held, left := intervalHeld(seen, err, accepted)
		if left != nil {
			return seen, attempt, fmt.Errorf("observation %d: %w", attempt, left)
		}
		if held && want(seen) {
			return seen, attempt, nil
		}
		if last = "held, without the wanted state"; !held {
			last = fmt.Sprintf("inconclusive: %v", err)
		}
		if attempt < attempts {
			time.Sleep(pause)
		}
	}
	return pgIntervalReceipt{}, attempts, fmt.Errorf("no held observation in the interval since epoch %d with the wanted state within %d observations; the last was %s",
		accepted.Observation.Query.SameIntervalSince, attempts, last)
}

// anyHeld wants every held observation.
func anyHeld(pgIntervalReceipt) bool { return true }

// waitAbsentRecords observes the live primary of the PG without the member
// until its own info holds exactly the partial-write records, within a
// bounded number of observations, in the interval in which the PG was
// accepted without the member. No client reply waits for the roll-forward
// that writes them, and the peering at the member's return reads them.
func (r *partialWriteRun) waitAbsentRecords(o *shardOutage, records []shardPartialWrite) {
	r.t.Helper()
	var got []shardPartialWrite
	seen, attempt, err := reobserveInterval(o.absent, intervalReobservations, time.Second,
		func() (pgIntervalReceipt, error) { return r.observeAbsent(r.ctx, o) },
		func(seen pgIntervalReceipt) bool {
			got = seen.Observation.Query.Info.PartialWrites
			return slices.Equal(got, records)
		})
	if err != nil {
		r.t.Fatalf("the live primary of the PG without osd.%d does not hold the partial-write records %+v (last held: %+v): %v", o.osd, records, got, err)
	}
	r.t.Logf("the live primary's info holds the partial-write records %+v at epoch %d (observation %d)", got, seen.Observation.Query.Info.PartialWritesEpoch, attempt)
}

// comeBack starts the outage's OSD again and waits, within a bounded number
// of observations, until the PG is active+clean on its original placement in
// an interval newer than the one in which it was active without the member.
// It then adopts the placement record of that state.
func (r *partialWriteRun) comeBack(o *shardOutage) pgIntervalReceipt {
	r.t.Helper()
	// The PG must still be active without the member, in the interval that
	// was observed, right before the OSD starts: one fresh accepted
	// observation in that interval, past samples the primary answered from
	// an older map.
	last, attempt, err := reobserveInterval(o.absent, intervalReobservations, shardOutagePause,
		func() (pgIntervalReceipt, error) { return r.observeAbsent(r.ctx, o) }, anyHeld)
	if err != nil {
		r.t.Fatalf("the PG is not shown active without osd.%d in its interval before the OSD was started: %v", o.osd, err)
	}
	absent := last.Observation.Query.SameIntervalSince
	r.t.Logf("pg %s still active without osd.%d at epoch %d in the interval since %d (observation %d)", o.origin.PGID, o.osd, last.Observation.Before.Epoch, absent, attempt)
	if err := testcluster.ControlDaemon(r.ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "start", "osd", strconv.FormatInt(int64(o.osd), 10)); err != nil {
		r.t.Fatal("start", o.osd, err)
	}
	o.started = true
	states := []string{}
	for attempt := 1; attempt <= shardOutageObservations; attempt++ {
		shown := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": o.origin}).PGInterval
		if err = checkPGReturned(shown, o.origin, absent); err == nil {
			q := shown.Observation.Query
			r.t.Logf("pg %s with osd.%d back (shard %d): epoch %d, interval since %d (without it: %d), up %v acting %v primary osd.%d shard %d, state %s; observation %d, states before it %v",
				o.origin.PGID, o.osd, o.shard, shown.Observation.Before.Epoch, q.SameIntervalSince, absent, q.Up, q.Acting,
				shown.Observation.Before.PrimaryID, shown.Observation.Before.Shard, q.State, attempt, states)
			r.logLivePrimary("after the return", shown)
			// The returned OSD leads other PGs, and each primary of a new
			// interval has the MON record its up_thru before it activates
			// (src/osd/PeeringState.cc:1482-1493, :8094-8099), which is one
			// more map epoch. The fixture's readiness control waits until
			// every OSD is up and in and every PG reports active+clean; the
			// capture needs a map that no longer moves.
			if err := testcluster.ControlDaemon(r.ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "verify", "osd-ready", "clean"); err != nil {
				r.t.Fatal("the fixture did not return to every OSD up and every PG clean:", err)
			}
			// One held observation in the interval in which the PG was clean
			// again, past samples the primary answered from an older map.
			again, seen, err := reobserveInterval(shown, intervalReobservations, shardOutagePause, func() (pgIntervalReceipt, error) {
				s := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": o.origin}).PGInterval
				return s, checkPGReturned(s, o.origin, absent)
			}, anyHeld)
			if err != nil {
				r.t.Fatalf("the PG left the interval since epoch %d in which it was clean again: %v", q.SameIntervalSince, err)
			}
			r.t.Logf("pg %s still clean at epoch %d in the interval since %d after the readiness control (observation %d)", o.origin.PGID, again.Observation.Before.Epoch, q.SameIntervalSince, seen)
			fresh := poolPlace(r.t, r.ctx, partialWriteSlot, partialWriteObject, "")
			if !samePlacement(fresh, *again.Observation.After) || fresh.Epoch < again.Observation.After.Epoch {
				r.t.Fatalf("placement moved after the PG was clean again: %+v vs %+v", fresh, *again.Observation.After)
			}
			r.facts.checkPlacement(r.t, fresh)
			r.adopt(fresh)
			return again
		}
		if q := shown.Observation.Query; q != nil && (len(states) == 0 || states[len(states)-1] != q.State) {
			states = append(states, q.State)
		}
		time.Sleep(shardOutagePause)
	}
	r.t.Fatalf("the PG did not return clean to its original placement with osd.%d: %v (states %v)", o.osd, err, states)
	return pgIntervalReceipt{}
}

// leftOutShard is the single nonprimary shard the partial overwrite left
// out.
func (r *partialWriteRun) leftOutShard(state partialOverwrite) int {
	r.t.Helper()
	skipped := state.want.skipped()
	if len(skipped) != 1 || !slices.Contains(r.facts.nonprimary, skipped[0]) {
		r.t.Fatalf("the overwrite left out shards %v; this control needs exactly one nonprimary shard", skipped)
	}
	return skipped[0]
}

// After the partial overwrite of the first control, which leaves the
// nonprimary data shard out, that shard's OSD is taken out of the PG by the
// fixture's forced kill and the PG must become active on its original
// primary with that position empty. The object is read back natively and
// through the Go client, which the primary must answer without that shard.
// Nothing is written. The OSD is started again and the PG must be
// active+clean on its original placement in a newer interval. Every acting
// shard's store is then captured: the returned shard still holds only the
// full write's entry, with its head, last_update and last_complete at the
// overwrite, past its last entry, and the partial-write range between them
// in its own info. This states what each shard's own log holds at capture
// time; it claims nothing about eviction, loss or replay.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwriteshardreturnshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-pg-interval,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD (restart verified osd-ready clean) and pauses each acting OSD for a store copy, no pg_temp proof
func TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration(t *testing.T) {
	run := startPartialWriteRun(t, 900*time.Second)
	started := run.showInterval().Observation.Query.SameIntervalSince
	state := run.partialOverwrite()
	left := run.leftOutShard(state)
	records := []shardPartialWrite{{Shard: left, From: state.want.full, To: state.want.patch}}
	// Both peerings below apply the range only if the primary recorded it.
	run.waitPrimaryRecords(records)

	outage := run.leave(left, started)
	if got := outage.absent.Observation.Query.Info; !slices.Equal(got.PartialWrites, records) || got.LastUpdate != state.want.patch {
		t.Fatalf("without the member the primary's info is at %s with the records %+v; want %s and %+v", got.LastUpdate, got.PartialWrites, state.want.patch, records)
	}
	run.observe("outage", state.patched)
	const (
		absentReadIncarnation   = 12
		returnedReadIncarnation = 13
	)
	run.readVersion("read without the member", absentReadIncarnation, state.patched)

	returned := run.comeBack(outage)
	interval := returned.Observation.Query.SameIntervalSince
	writes := []shardWrite{
		{"full write", state.want.full, run.everyShard()},
		{"partial overwrite", state.want.patch, state.want.written},
	}
	primary, shards := run.capture([]pgHistoryTarget{state.fullKey, state.patchKey}, state.misses, run.facts.nonprimary)
	exact := func(int) [][]shardPartialWrite { return [][]shardPartialWrite{records} }
	run.requireShardLogs("after the return", primary, shards, state.want.object, writes, true, exact)
	run.observe("return", state.patched)
	run.readVersion("read after the return", returnedReadIncarnation, state.patched)
	if ended := run.showInterval().Observation.Query.SameIntervalSince; ended != interval {
		t.Fatalf("the PG's interval started at epoch %d when the member was back and at epoch %d after the last capture", interval, ended)
	}
	t.Logf("the PG stayed in the interval that started at epoch %d", interval)
}

// shardLogRecovered restates what the pinned source leaves in the own log of
// every acting shard, the returned one included, once the PG is clean again
// after an outage of one member during which the last write was
// acknowledged, when the return's peering could not advance the returning
// shard through a partial-write range: the entry of every write, with the
// head at the last one. The first write created the head and the last one
// was the first of its interval, so both must name every shard.
func shardLogRecovered(writes []shardWrite, shards int) (shardLogState, error) {
	if len(writes) < 2 || shards < 2 || len(writes[0].named) != shards || len(writes[len(writes)-1].named) != shards {
		return shardLogState{}, fmt.Errorf("%d writes over %d shards, the first and the last of which must name every shard", len(writes), shards)
	}
	return shardLogState{Held: slices.Repeat([]bool{true}, len(writes)), Head: writes[len(writes)-1].version}, nil
}

// readVersion sends one Go read of the whole object exactly once and requires
// the expected bytes and the user version of the acknowledged write that
// produced them.
func (r *partialWriteRun) readVersion(label string, incarnation uint32, write cephmsgr.OSDReply) {
	r.t.Helper()
	if read := r.read(label, incarnation); read.UserVersion != write.UserVersion {
		r.t.Fatalf("%s returned user version %d; the acknowledged write's is %d", label, read.UserVersion, write.UserVersion)
	}
}

// recoveredShardLogs requires every acting shard's own log to be exactly
// what shardLogRecovered restates for the writes, and its partial-write
// records to be one of the lists records returns.
func (r *partialWriteRun) recoveredShardLogs(label string, primary pgHistorySnapshot, shards shardHistorySnapshot, object string, writes []shardWrite, records func(shard int) [][]shardPartialWrite) {
	r.t.Helper()
	for i, write := range writes {
		if _, err := writeEntry(primary.Targets[i].Locations, object, write.version); err != nil {
			r.t.Fatalf("%s, live primary, %s: %v", label, write.label, err)
		}
	}
	last := writes[len(writes)-1].version
	if primary.Head != last || primary.LogEntries != len(writes) {
		r.t.Fatalf("%s: the live primary's log holds %d entries and ends at %s, not the %d writes ending at %s", label, primary.LogEntries, primary.Head, len(writes), last)
	}
	expected, err := shardLogRecovered(writes, r.facts.size)
	if err != nil {
		r.t.Fatal(err)
	}
	for i, shard := range shards.Shards {
		role := erasureShardRole(i, r.width.k, r.facts.nonprimary)
		locations := make([][]pgHistoryLocation, len(writes))
		for j := range writes {
			locations[j] = shard.Targets[j].Locations
		}
		observed, err := shardLogObservation(shard, locations, object, writes, r.facts.size)
		if err != nil {
			r.t.Fatalf("%s: shard %s (osd.%d, %s) contradicts the acknowledged writes: %v", label, shard.SPG, shard.OSD, role, err)
		}
		r.t.Logf("%s: shard %s (osd.%d, %s) %s; last entry %s, last_update %s, last_complete %s, entries %+v, partial-write records %+v at epoch %d",
			label, shard.SPG, shard.OSD, role, observed, shard.LastEntry, shard.LastUpdate, shard.LastComplete, shard.Entries, shard.PartialWrites, shard.PartialWritesEpoch)
		if !reflect.DeepEqual(observed, expected) {
			r.t.Fatalf("%s: shard %s (osd.%d, %s) %s; the recovery after the writes %+v requires that it %s", label, shard.SPG, shard.OSD, role, observed, writes, expected)
		}
		if allowed := records(i); !partialWriteRecordsAmong(shard.PartialWrites, allowed...) {
			r.t.Fatalf("%s: shard %s (osd.%d, %s) holds the partial-write records %+v; want one of %+v", label, shard.SPG, shard.OSD, role, shard.PartialWrites, allowed)
		}
	}
}

// After the partial overwrite of the first control, which leaves the
// nonprimary data shard out, that shard's OSD is taken out of the PG by the
// fixture's forced kill. While the PG is active without it, the Go client
// overwrites a few bytes inside that shard's chunk, exactly once; the write
// must be acknowledged, and it is read back natively and through the Go
// client. The primary's own info must then show the absent shard's range
// reset to that write. The OSD is started again and the PG must be
// active+clean on its original placement in a newer interval, which includes
// the recovery of the object to the returned shard. Every acting shard's
// store is then captured: every shard, the returned one included, holds all
// three entries with its head at the degraded write and an empty missing
// set. The returned shard's copy therefore now holds the entry of the
// overwrite it was left out of, which names the other shards only; it
// received that entry from the primary's log at its return. The object is
// read back natively and through the Go client. This states what each
// shard's own log holds at capture time; it does not read the returned
// shard's chunk and claims nothing about eviction, loss or replay.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritedegradedwriteshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-pg-interval,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a shard OSD (restart verified osd-ready clean) and pauses each acting OSD for a store copy, no pg_temp proof
func TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration(t *testing.T) {
	run := startPartialWriteRun(t, 900*time.Second)
	started := run.showInterval().Observation.Query.SameIntervalSince
	state := run.partialOverwrite()
	left := run.leftOutShard(state)
	records := []shardPartialWrite{{Shard: left, From: state.want.full, To: state.want.patch}}
	run.waitPrimaryRecords(records)

	outage := run.leave(left, started)
	absent := outage.absent.Observation.Query.SameIntervalSince
	if got := outage.absent.Observation.Query.Info; !slices.Equal(got.PartialWrites, records) || got.LastUpdate != state.want.patch {
		t.Fatalf("without the member the primary's info is at %s with the records %+v; want %s and %+v", got.LastUpdate, got.PartialWrites, state.want.patch, records)
	}
	const (
		degradedIncarnation     = 13
		absentReadIncarnation   = 14
		returnedReadIncarnation = 15
	)
	degraded := []byte("degraded\x00\xffwrite")
	// A few bytes into the absent shard's chunk of the first stripe: the
	// bytes that change are the ones that shard stores.
	offset := uint64(left)*(run.stripe/uint64(run.width.k)) + 5
	if planned, _ := run.plan(offset, len(degraded)); !slices.Contains(planned, left) {
		t.Fatalf("the write [%d,+%d) names shards %v by its bytes; this control needs it inside shard %d's chunk", offset, len(degraded), planned, left)
	}
	reply := run.overwrite("degraded write", degradedIncarnation, 1700009104, offset, degraded)
	if reply.UserVersion <= state.patched.UserVersion || reply.ReplayVersion.Epoch < absent {
		t.Fatalf("the degraded write has user version %d (overwrite %d) and replay version %s; the interval started at epoch %d",
			reply.UserVersion, state.patched.UserVersion, replayVersion(reply), absent)
	}
	key := run.key(degradedIncarnation, reply)
	writes := []shardWrite{
		{"full write", state.want.full, run.everyShard()},
		{"partial overwrite", state.want.patch, state.want.written},
		// The first write of an interval names every shard, the absent one
		// included.
		{"degraded write", replayVersion(reply), run.everyShard()},
	}
	t.Logf("degraded write %s [%d,+%d) at %s, user version %d, without osd.%d (shard %d)", key, offset, len(degraded), writes[2].version, reply.UserVersion, outage.osd, left)
	run.readVersion("read without the member", absentReadIncarnation, reply)
	// The return's peering must find the range reset: it then cannot advance
	// the returning shard, which gets every entry after its last_update.
	reset := []shardPartialWrite{{Shard: left, From: writes[2].version, To: writes[2].version}}
	run.waitAbsentRecords(outage, reset)

	returned := run.comeBack(outage)
	interval := returned.Observation.Query.SameIntervalSince
	primary, shards := run.capture([]pgHistoryTarget{state.fullKey, state.patchKey, key}, state.misses, nil)
	run.recoveredShardLogs("after the recovery", primary, shards, state.want.object, writes, func(int) [][]shardPartialWrite { return [][]shardPartialWrite{reset} })
	run.observe("recovery", reply)
	run.readVersion("read after the recovery", returnedReadIncarnation, reply)
	if ended := run.showInterval().Observation.Query.SameIntervalSince; ended != interval {
		t.Fatalf("the PG's interval started at epoch %d when the member was back and at epoch %d after the last capture", interval, ended)
	}
	t.Logf("the PG stayed in the interval that started at epoch %d", interval)
}

// pgAbsentObservation builds one synthetic receipt of PG 3.7 without the
// member of acting position 1: recorded on acting [3 2 1 0] at epoch 25 in
// the interval that started at 24, then active on [3 NONE 1 0] at epoch 27 in
// the interval that started at 26. No daemon produced it.
func pgAbsentObservation() (pgIntervalReceipt, osdFixtureOracle) {
	_, route := pgIntervalChange()
	holed := []int32{3, crushItemNone, 1, 0}
	before, after := route, route
	before.Up, before.Acting, after.Up, after.Acting = slices.Clone(holed), slices.Clone(holed), slices.Clone(holed), slices.Clone(holed)
	before.Epoch, after.Epoch = 27, 27
	return pgIntervalReceipt{Action: "absent", Observations: 1, Settled: true, Observation: pgIntervalObservation{Before: &before, After: &after,
		Query: &pgIntervalQuery{State: "active+undersized+degraded", Epoch: 27, Up: slices.Clone(holed), Acting: slices.Clone(holed), SameIntervalSince: 26, LastEpochStarted: 27,
			Info: pgIntervalInfo{PGID: "3.7s0", Shard: 0, LastUpdate: "25'2", LastComplete: "25'2"}}}}, route
}

func TestCheckPGAbsent(t *testing.T) {
	receipt, route := pgAbsentObservation()
	if err := checkPGAbsent(receipt, route, 1, 24); err != nil {
		t.Fatal("active PG without the member refused:", err)
	}
	// The primary may not have counted its degraded objects yet, the interval
	// may start at the placement's epoch, the primary may be at a newer map,
	// and the recorded interval may have started at the recorded epoch.
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"undersized only":                   func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+undersized" },
		"another token order":               func(r *pgIntervalReceipt) { r.Observation.Query.State = "undersized+degraded+active" },
		"interval at the placement's epoch": func(r *pgIntervalReceipt) { r.Observation.Query.SameIntervalSince = 27 },
		"primary ahead":                     func(r *pgIntervalReceipt) { r.Observation.Query.Epoch = 28 },
	} {
		changed, _ := pgAbsentObservation()
		edit(&changed)
		if err := checkPGAbsent(changed, route, 1, 24); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	if err := checkPGAbsent(receipt, route, 1, 25); err != nil {
		t.Error("recorded interval that started at the recorded epoch refused:", err)
	}
	sets := func(up, acting []int32) func(*pgIntervalReceipt) {
		return func(r *pgIntervalReceipt) {
			for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
				p.Up, p.Acting = slices.Clone(up), slices.Clone(acting)
			}
			r.Observation.Query.Up, r.Observation.Query.Acting = slices.Clone(up), slices.Clone(acting)
		}
	}
	full, holed := []int32{3, 2, 1, 0}, []int32{3, crushItemNone, 1, 0}
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"not settled":                    func(r *pgIntervalReceipt) { r.Settled = false },
		"a recorded failure":             func(r *pgIntervalReceipt) { r.Failures = []string{"the primary reports peering"} },
		"a show receipt":                 func(r *pgIntervalReceipt) { r.Action = "show" },
		"a command was sent":             func(r *pgIntervalReceipt) { r.CommandSent = true },
		"a command is recorded":          func(r *pgIntervalReceipt) { r.Command = map[string]string{"prefix": "pg repeer", "pgid": "3.7"} },
		"no placement":                   func(r *pgIntervalReceipt) { r.Observation.Before = nil },
		"no second placement":            func(r *pgIntervalReceipt) { r.Observation.After = nil },
		"placement moved":                func(r *pgIntervalReceipt) { r.Observation.After.Epoch = 28 },
		"the member is still acting":     sets(full, full),
		"only the acting set has a hole": sets(full, holed),
		"only the up set has a hole":     sets(holed, full),
		"another position is empty":      sets([]int32{3, 2, crushItemNone, 0}, []int32{3, 2, crushItemNone, 0}),
		"two positions are empty":        sets([]int32{3, crushItemNone, crushItemNone, 0}, []int32{3, crushItemNone, crushItemNone, 0}),
		"another OSD took the position":  sets([]int32{3, 4, 1, 0}, []int32{3, 4, 1, 0}),
		"a shorter set":                  sets([]int32{3, 1, 0}, []int32{3, 1, 0}),
		"primary's acting is complete":   func(r *pgIntervalReceipt) { r.Observation.Query.Acting = slices.Clone(full) },
		"primary's up is complete":       func(r *pgIntervalReceipt) { r.Observation.Query.Up = slices.Clone(full) },
		"the same epoch":                 func(r *pgIntervalReceipt) { r.Observation.Before.Epoch, r.Observation.After.Epoch = 25, 25 },
		"under a pg_temp":                func(r *pgIntervalReceipt) { r.Observation.Before.PGTemp, r.Observation.After.PGTemp = true, true },
		"another address": func(r *pgIntervalReceipt) {
			r.Observation.Before.Address, r.Observation.After.Address = "v2:192.0.2.7:6801/2", "v2:192.0.2.7:6801/2"
		},
		"another PG":                func(r *pgIntervalReceipt) { r.Observation.Before.PGID, r.Observation.After.PGID = "3.6", "3.6" },
		"another min_size":          func(r *pgIntervalReceipt) { r.Observation.Before.MinSize, r.Observation.After.MinSize = 2, 2 },
		"target is not the primary": func(r *pgIntervalReceipt) { r.Observation.Before.ID, r.Observation.After.ID = 1, 1 },
		"another primary": func(r *pgIntervalReceipt) {
			for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
				p.PrimaryID, p.ID, p.Shard = 1, 1, 2
			}
			r.Observation.Query.Info.PGID, r.Observation.Query.Info.Shard = "3.7s2", 2
		},
		"query refused":                  func(r *pgIntervalReceipt) { r.Observation.Code = -11 },
		"query failure recorded":         func(r *pgIntervalReceipt) { r.Observation.QueryFailure = "pg query returned -11" },
		"no query":                       func(r *pgIntervalReceipt) { r.Observation.Query = nil },
		"clean":                          func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+clean" },
		"peering":                        func(r *pgIntervalReceipt) { r.Observation.Query.State = "peering" },
		"peered below min_size":          func(r *pgIntervalReceipt) { r.Observation.Query.State = "undersized+degraded+peered" },
		"recovering":                     func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+recovering+undersized+degraded" },
		"remapped":                       func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+undersized+remapped" },
		"active alone":                   func(r *pgIntervalReceipt) { r.Observation.Query.State = "active" },
		"degraded without undersized":    func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+degraded" },
		"a repeated token":               func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+undersized+undersized" },
		"answered for another shard":     func(r *pgIntervalReceipt) { r.Observation.Query.Info.PGID = "3.7s2" },
		"answered with another shard ID": func(r *pgIntervalReceipt) { r.Observation.Query.Info.Shard = 2 },
		"the recorded interval": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 24, 25
		},
		"an interval after the placement": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 28, 28
		},
		"not started in its interval":  func(r *pgIntervalReceipt) { r.Observation.Query.LastEpochStarted = 25 },
		"primary behind the placement": func(r *pgIntervalReceipt) { r.Observation.Query.Epoch = 26 },
	} {
		changed, _ := pgAbsentObservation()
		edit(&changed)
		if err := checkPGAbsent(changed, route, 1, 24); err == nil {
			t.Error("observation accepted:", name)
		}
	}
	// The request: only another position than the primary's, of a complete
	// recorded acting set, with an interval start that is not after it.
	for name, c := range map[string]struct {
		shard int
		since uint32
		edit  func(*osdFixtureOracle)
	}{
		"the primary's position":          {0, 24, nil},
		"a position beyond the set":       {4, 24, nil},
		"a negative position":             {-1, 24, nil},
		"another position":                {2, 24, nil},
		"no recorded interval":            {1, 0, nil},
		"an interval after the placement": {1, 26, nil},
		"a recorded hole":                 {2, 24, func(p *osdFixtureOracle) { p.Up, p.Acting = slices.Clone(holed), slices.Clone(holed) }},
		"recorded on another PG":          {1, 24, func(p *osdFixtureOracle) { p.PGID = "3.6" }},
		"recorded at a newer epoch":       {1, 24, func(p *osdFixtureOracle) { p.Epoch = 27 }},
		"recorded under a pg_temp":        {1, 24, func(p *osdFixtureOracle) { p.PGTemp = true }},
		"recorded on another primary":     {1, 24, func(p *osdFixtureOracle) { p.PrimaryID, p.ID = 0, 0 }},
	} {
		origin := route
		if c.edit != nil {
			c.edit(&origin)
		}
		if err := checkPGAbsent(receipt, origin, c.shard, c.since); err == nil {
			t.Error("request accepted:", name)
		}
	}

	// The return: a clean observation on the recorded placement in an
	// interval that started after the one without the member.
	changed, _ := pgIntervalChange()
	returned := pgIntervalReceipt{Action: "show", Observation: changed.Observation, Observations: 1, Settled: true}
	for _, absent := range []uint32{24, 26} {
		if err := checkPGReturned(returned, route, absent); err != nil {
			t.Errorf("clean PG in the interval since 27 refused after an interval since %d: %v", absent, err)
		}
	}
	for name, absent := range map[string]uint32{"the same interval": 27, "a later interval": 28, "no interval": 0} {
		if err := checkPGReturned(returned, route, absent); err == nil {
			t.Error("return accepted:", name)
		}
	}
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"not clean":                func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+recovering+degraded" },
		"an absent receipt":        func(r *pgIntervalReceipt) { r.Action = "absent" },
		"still without the member": sets(holed, holed),
		"interval after the placement": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 29, 29
		},
		"under a pg_temp": func(r *pgIntervalReceipt) { r.Observation.Before.PGTemp, r.Observation.After.PGTemp = true, true },
	} {
		changed, _ := pgIntervalChange()
		r := pgIntervalReceipt{Action: "show", Observation: changed.Observation, Observations: 1, Settled: true}
		edit(&r)
		if err := checkPGReturned(r, route, 26); err == nil {
			t.Error("return accepted:", name)
		}
	}
}

// TestReobserveAbsent covers the later observations of the PG without the
// member: a sample from a primary behind the placement's epoch is retried,
// never accepted, and a confirmed change of interval or state fails at once.
func TestReobserveAbsent(t *testing.T) {
	accepted, route := pgAbsentObservation()
	held := func(epoch uint32) pgIntervalReceipt {
		r, _ := pgAbsentObservation()
		r.Observation.Before.Epoch, r.Observation.After.Epoch, r.Observation.Query.Epoch = epoch, epoch, epoch
		return r
	}
	// The hosted sample: the MON published epoch 28 with the PG's sets
	// unchanged, and the primary still answered from epoch 27. The oracle
	// refused it.
	lagging := func(interval uint32) pgIntervalReceipt {
		r := held(28)
		r.Observation.Query.Epoch, r.Observation.Query.SameIntervalSince = 27, interval
		r.Settled, r.Failures = false, []string{"the primary is at map epoch 27, behind the placement's epoch"}
		return r
	}
	edited := func(r pgIntervalReceipt, edit func(*pgIntervalReceipt)) pgIntervalReceipt {
		edit(&r)
		return r
	}
	moved := edited(held(28), func(r *pgIntervalReceipt) { r.Observation.After.Epoch = 29 })
	noQuery := edited(held(28), func(r *pgIntervalReceipt) {
		r.Observation.Query, r.Observation.QueryFailure = nil, "pg query returned -11"
	})
	noRecord := edited(held(28), func(r *pgIntervalReceipt) { r.Observation.After = nil })
	later := edited(held(29), func(r *pgIntervalReceipt) {
		r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 29, 29
	})
	peering := edited(held(28), func(r *pgIntervalReceipt) { r.Observation.Query.State = "peering" })
	marked := edited(held(28), func(r *pgIntervalReceipt) {
		for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
			p.Up, p.Acting = []int32{3, 4, 1, 0}, []int32{3, 4, 1, 0}
		}
	})
	always := anyHeld
	for name, c := range map[string]struct {
		samples []pgIntervalReceipt
		want    func(pgIntervalReceipt) bool
		attempt int
		ok      bool
	}{
		"held at once":                      {[]pgIntervalReceipt{held(27)}, always, 1, true},
		"held at a newer epoch":             {[]pgIntervalReceipt{held(28)}, always, 1, true},
		"lagging, then held":                {[]pgIntervalReceipt{lagging(26), lagging(26), held(28)}, always, 3, true},
		"moved around the query, then held": {[]pgIntervalReceipt{moved, held(29)}, always, 2, true},
		"no query reply, then held":         {[]pgIntervalReceipt{noQuery, held(28)}, always, 2, true},
		"no placement record, then held":    {[]pgIntervalReceipt{noRecord, held(28)}, always, 2, true},
		"held until the wanted state": {[]pgIntervalReceipt{held(27), lagging(26), held(28)}, func(r pgIntervalReceipt) bool {
			return r.Observation.Before.Epoch == 28
		}, 3, true},
		"persistent lag":                     {[]pgIntervalReceipt{lagging(26), lagging(26), lagging(26), lagging(26)}, always, 4, false},
		"never the wanted state":             {[]pgIntervalReceipt{held(27), held(27), held(27), held(27)}, func(pgIntervalReceipt) bool { return false }, 4, false},
		"a later interval":                   {[]pgIntervalReceipt{later, held(29)}, always, 1, false},
		"a later interval, lagging":          {[]pgIntervalReceipt{lagging(28), held(28)}, always, 1, false},
		"lagging, then a later interval":     {[]pgIntervalReceipt{lagging(26), later}, always, 2, false},
		"lagging, then peering":              {[]pgIntervalReceipt{lagging(26), peering, held(28)}, always, 2, false},
		"another OSD took the position":      {[]pgIntervalReceipt{marked, held(28)}, always, 1, false},
		"an earlier interval from the reply": {[]pgIntervalReceipt{edited(held(28), func(r *pgIntervalReceipt) { r.Observation.Query.SameIntervalSince = 24 })}, always, 1, false},
	} {
		next := 0
		observe := func() (pgIntervalReceipt, error) {
			r := c.samples[min(next, len(c.samples)-1)]
			next++
			return r, checkPGAbsent(r, route, 1, 24)
		}
		seen, attempt, err := reobserveInterval(accepted, 4, 0, observe, c.want)
		if (err == nil) != c.ok || attempt != c.attempt || next != c.attempt {
			t.Errorf("%s: attempt %d after %d observations, error %v; want attempt %d, accepted %t", name, attempt, next, err, c.attempt, c.ok)
			continue
		}
		if c.ok {
			if err := checkPGAbsent(seen, route, 1, 24); err != nil || seen.Observation.Query.SameIntervalSince != 26 {
				t.Errorf("%s: returned an observation that is not held in the interval since 26: %v", name, err)
			}
		}
	}
}

func TestShardLogRecovered(t *testing.T) {
	const object = "3:e65c9b8f:::obj:head"
	_, _, writes := shardLogReceipt([3]bool{true, true, true}, "25'3", "25'3")
	want, err := shardLogRecovered(writes, 4)
	if err != nil || !reflect.DeepEqual(want, shardLogState{[]bool{true, true, true}, "25'3", false}) {
		t.Fatal(want, err)
	}
	// Only a shard that holds every entry, the one that does not name it
	// included, with its head at the last one, is the recovered shape.
	for name, c := range map[string]struct {
		held       [3]bool
		head, last string
		equal      bool
	}{
		"every entry":                           {[3]bool{true, true, true}, "25'3", "25'3", true},
		"without the entry it was left out of":  {[3]bool{true, false, true}, "25'3", "25'3", false},
		"only the first entry, head at the end": {[3]bool{true, false, false}, "25'3", "25'1", false},
		"only the first entry":                  {[3]bool{true, false, false}, "25'1", "25'1", false},
		"the first two, head at the end":        {[3]bool{true, true, false}, "25'3", "25'2", false},
	} {
		shard, locations, _ := shardLogReceipt(c.held, c.head, c.last)
		got, err := shardLogObservation(shard, locations, object, writes, 4)
		if err != nil && c.equal || err == nil && reflect.DeepEqual(got, want) != c.equal {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
	// The statement holds only for a sequence whose first and last write
	// name every shard.
	partial := []int{0, 2, 3}
	for name, c := range map[string]struct {
		writes []shardWrite
		shards int
	}{
		"no write":                      {nil, 4},
		"one write":                     {writes[:1], 4},
		"the last write names some":     {writes[:2], 4},
		"the first write names some":    {[]shardWrite{{"a", "25'1", partial}, writes[2]}, 4},
		"another pool size":             {writes, 5},
		"one shard":                     {[]shardWrite{{"a", "25'1", []int{0}}, {"b", "25'2", []int{0}}}, 1},
		"the last write names no shard": {[]shardWrite{writes[0], {"b", "25'2", nil}}, 4},
	} {
		if got, err := shardLogRecovered(c.writes, c.shards); err == nil {
			t.Errorf("%s accepted: %+v", name, got)
		}
	}
	if got, err := shardLogRecovered(writes[:1:1], 4); err == nil {
		t.Error("a single write accepted:", got)
	}
	two := []shardWrite{writes[0], writes[2]}
	if got, err := shardLogRecovered(two, 4); err != nil || !reflect.DeepEqual(got, shardLogState{[]bool{true, true}, "25'3", false}) {
		t.Error("two writes that name every shard:", got, err)
	}
}

// TestReobserveReturned covers the observation of the PG clean again after
// the fixture's readiness control: a sample from a primary behind the
// placement's epoch is retried, never accepted, and a later interval, another
// state or the primary under another address fails at once.
func TestReobserveReturned(t *testing.T) {
	_, route := pgIntervalChange()
	clean := func(epoch uint32) pgIntervalReceipt {
		changed, _ := pgIntervalChange()
		r := pgIntervalReceipt{Action: "show", Observation: changed.Observation, Observations: 1, Settled: true}
		r.Observation.Before.Epoch, r.Observation.After.Epoch, r.Observation.Query.Epoch = epoch, epoch, epoch
		return r
	}
	accepted := clean(28)
	edited := func(r pgIntervalReceipt, edit func(*pgIntervalReceipt)) pgIntervalReceipt {
		edit(&r)
		return r
	}
	// Another OSD's up_thru at epoch 29, and the primary still at epoch 28.
	lagging := func(interval uint32) pgIntervalReceipt {
		return edited(clean(29), func(r *pgIntervalReceipt) {
			r.Observation.Query.Epoch, r.Observation.Query.SameIntervalSince = 28, interval
			r.Settled, r.Failures = false, []string{"the primary is at map epoch 28, behind the placement's epoch"}
		})
	}
	// The primary restarted again: the map names it under another address.
	readdressed := func(r *pgIntervalReceipt) {
		r.Observation.Before.Address, r.Observation.After.Address = "v2:192.0.2.7:6800/9", "v2:192.0.2.7:6800/9"
		r.Settled, r.Failures = false, []string{"the placement differs from the recorded one"}
	}
	later := edited(clean(30), func(r *pgIntervalReceipt) {
		r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 30, 30
	})
	for name, c := range map[string]struct {
		samples []pgIntervalReceipt
		attempt int
		ok      bool
	}{
		"held at once":                        {[]pgIntervalReceipt{clean(28)}, 1, true},
		"held at a newer epoch":               {[]pgIntervalReceipt{clean(29)}, 1, true},
		"lagging, then held":                  {[]pgIntervalReceipt{lagging(27), lagging(27), clean(29)}, 3, true},
		"another address, lagging, then held": {[]pgIntervalReceipt{edited(lagging(27), readdressed), clean(29)}, 2, true},
		"persistent lag":                      {[]pgIntervalReceipt{lagging(27), lagging(27), lagging(27), lagging(27)}, 4, false},
		"a later interval":                    {[]pgIntervalReceipt{later, clean(30)}, 1, false},
		"a later interval, lagging":           {[]pgIntervalReceipt{lagging(29), clean(29)}, 1, false},
		"lagging, then a later interval":      {[]pgIntervalReceipt{lagging(27), later}, 2, false},
		"the primary under another address":   {[]pgIntervalReceipt{edited(clean(29), readdressed), clean(29)}, 1, false},
		"recovering again":                    {[]pgIntervalReceipt{edited(clean(29), func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+recovering" }), clean(29)}, 1, false},
	} {
		next := 0
		observe := func() (pgIntervalReceipt, error) {
			r := c.samples[min(next, len(c.samples)-1)]
			next++
			return r, checkPGReturned(r, route, 26)
		}
		seen, attempt, err := reobserveInterval(accepted, 4, 0, observe, anyHeld)
		if (err == nil) != c.ok || attempt != c.attempt || next != c.attempt {
			t.Errorf("%s: attempt %d after %d observations, error %v; want attempt %d, accepted %t", name, attempt, next, err, c.attempt, c.ok)
			continue
		}
		if c.ok {
			if err := checkPGReturned(seen, route, 26); err != nil || seen.Observation.Query.SameIntervalSince != 27 {
				t.Errorf("%s: returned an observation that is not held in the interval since 27: %v", name, err)
			}
		}
	}
}

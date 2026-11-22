package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// What a nonprimary data shard of an optimized erasure-coded PG holds after
// a partial write left it out, in the two later states the pinned source
// (Ceph v20.2.4) makes deterministic without any fault:
//
//   - A later write of the same interval that names the shard carries only
//     its own log entries (src/osd/ECCommon.cc:812-889 builds each sub-write
//     from the operation's entries), and a shard's log accepts an entry
//     whose version is newer than its head without the versions in between
//     (src/osd/PGLog.h:653-675). The shard then holds the entries before and
//     after the write it was left out of, and not that one.
//   - At the next peering the primary applies the partial-write range it
//     recorded for the shard to the shard's info (src/osd/PeeringState.cc
//     :324-364, :422-428), finds it up to date and activates it with its
//     info alone (:3026-3051). The shard merges the ranges and advances its
//     own last_update, last_complete and log head to the end of its range
//     (:369-434, :7296-7347) without receiving the entry. The first write of
//     the new interval names every shard (src/osd/ECCommon.cc:998-1010,
//     src/osd/ECTransaction.cc:597-603).
//
// These consumer helpers are test-only and nothing is exported.

// shardWrite is one acknowledged write of a control's object, in the order
// of the replies: the replay version of its reply and the shards the pinned
// primary names for it.
type shardWrite struct {
	label   string
	version string
	named   []int
}

// shardLogState is what one shard's own log shows for a sequence of
// acknowledged writes. A write that is not held is an observation of that
// copy at capture time; it says nothing about eviction or loss.
type shardLogState struct {
	// Held[i] says that the i-th write's entry is in the log.
	Held []bool
	// Head is the log's head. Past says that it is newer than the last
	// entry: the version of a write whose entry the shard does not hold.
	Head string
	Past bool
}

func (s shardLogState) String() string {
	text := "holds"
	for i, held := range s.Held {
		if held {
			text += " #" + strconv.Itoa(i+1)
		}
	}
	if s.Past {
		return text + ", head " + s.Head + " past its last entry"
	}
	return text + ", head " + s.Head + " at its last entry"
}

// shardLogObservation reads one shard's receipt as a selection of the
// acknowledged writes. Every held write must be exactly one ordinary
// successful modify entry of the object at the write's replay version that
// names the write's shards (the dump's empty set for every shard), the held
// entries must be all of the log's entries in the order of the writes, and
// the first write, which created the head, must be held. The head is either
// the last entry or, when the receipt reports it past the entries, the
// version of a later write that is not held. Every other shape contradicts
// the acknowledged writes and is refused.
func shardLogObservation(shard shardHistoryShard, locations [][]pgHistoryLocation, object string, writes []shardWrite, shards int) (shardLogState, error) {
	none := shardLogState{}
	if len(writes) == 0 || len(locations) != len(writes) || shards < 2 {
		return none, fmt.Errorf("%d writes with %d position lists over %d shards", len(writes), len(locations), shards)
	}
	state := shardLogState{Held: make([]bool, len(writes)), Head: shard.Head}
	next, last := 0, ""
	for i, write := range writes {
		if len(locations[i]) == 0 {
			continue
		}
		index, err := writeEntry(locations[i], object, write.version)
		if err != nil {
			return none, fmt.Errorf("%s: %w", write.label, err)
		}
		if index != next || index >= len(shard.Entries) || shard.Entries[index].Version != write.version {
			return none, fmt.Errorf("%s: its entry %d is not entry %d of the log: %+v", write.label, index, next, shard.Entries)
		}
		named := write.named
		if len(named) == shards {
			// The pinned writer stores the empty set for every shard.
			named = nil
		}
		if len(write.named) == 0 || !slices.Equal(shard.Entries[index].WrittenShards, named) {
			return none, fmt.Errorf("%s: its entry names shards %v; want %v", write.label, shard.Entries[index].WrittenShards, write.named)
		}
		state.Held[i], next, last = true, next+1, write.version
	}
	if !state.Held[0] {
		return none, fmt.Errorf("the %s, which created the head, is not held", writes[0].label)
	}
	if next != len(shard.Entries) || shard.LogEntries != len(shard.Entries) {
		return none, fmt.Errorf("the log holds %d entries (%d listed); %d belong to the acknowledged writes: %+v", shard.LogEntries, len(shard.Entries), next, shard.Entries)
	}
	if shard.LastEntry != last || shard.LastUpdate != shard.Head || shard.LastComplete != shard.Head {
		return none, fmt.Errorf("the log's last entry is %s, its head %s, last_update %s, last_complete %s; the last held write is at %s",
			shard.LastEntry, shard.Head, shard.LastUpdate, shard.LastComplete, last)
	}
	if !shard.HeadPastEntries {
		if shard.Head != last {
			return none, fmt.Errorf("the log's head %s is not its last entry %s", shard.Head, last)
		}
		return state, nil
	}
	state.Past = true
	// A head past the entries must be a later write that this shard was
	// left out of, not the last held one and not a foreign version.
	ahead := slices.IndexFunc(writes, func(write shardWrite) bool { return write.version == shard.Head })
	if ahead < 0 || slices.Contains(state.Held[ahead:], true) {
		return none, fmt.Errorf("the log's head %s is past its last entry %s and is not a later write without an entry", shard.Head, last)
	}
	return state, nil
}

// shardLogExpected restates what the pinned source leaves in one shard's own
// log after the writes, in a PG that holds nothing else, lost nothing and
// recovered nothing: exactly the entries of the writes that name the shard.
// Its head is its last entry. Only when the PG peered after the last write
// and that write did not name the shard is the head at that write, without
// its entry. The first write must name every shard.
func shardLogExpected(writes []shardWrite, shard, shards int, peered bool) (shardLogState, error) {
	if len(writes) == 0 || shard < 0 || shard >= shards || len(writes[0].named) != shards {
		return shardLogState{}, fmt.Errorf("%d writes over %d shards, the first of which must name every shard, do not cover shard %d", len(writes), shards, shard)
	}
	state := shardLogState{Held: make([]bool, len(writes))}
	for i, write := range writes {
		if state.Held[i] = slices.Contains(write.named, shard); state.Held[i] {
			state.Head = write.version
		}
	}
	if last := len(writes) - 1; peered && !state.Held[last] {
		state.Head, state.Past = writes[last].version, true
	}
	return state, nil
}

// partialWriteRecordsAmong says whether a shard's partial-write records are
// exactly one of the allowed lists.
func partialWriteRecordsAmong(got []shardPartialWrite, allowed ...[]shardPartialWrite) bool {
	return slices.ContainsFunc(allowed, func(list []shardPartialWrite) bool { return slices.Equal(got, list) })
}

// pgIntervalInfo is the projection of one pg_info_t of a native pg query
// reply: the primary's own info, or its view of one peer.
type pgIntervalInfo struct {
	Peer               string              `json:"peer"`
	PGID               string              `json:"pgid"`
	Shard              int                 `json:"shard"`
	LastUpdate         string              `json:"last_update"`
	LastComplete       string              `json:"last_complete"`
	LogTail            string              `json:"log_tail"`
	PartialWrites      []shardPartialWrite `json:"partial_writes_last_complete"`
	PartialWritesEpoch uint32              `json:"partial_writes_last_complete_epoch"`
}

type pgIntervalQuery struct {
	State             string           `json:"state"`
	Epoch             uint32           `json:"epoch"`
	Up                []int32          `json:"up"`
	Acting            []int32          `json:"acting"`
	SameIntervalSince uint32           `json:"same_interval_since"`
	LastEpochStarted  uint32           `json:"last_epoch_started"`
	Info              pgIntervalInfo   `json:"info"`
	Peers             []pgIntervalInfo `json:"peers"`
}

// pgIntervalObservation is two native placement records around one pg query
// of the primary. A record that could not be read is nil.
type pgIntervalObservation struct {
	Before           *osdFixtureOracle `json:"before"`
	After            *osdFixtureOracle `json:"after"`
	PlacementFailure string            `json:"placement_failure"`
	Code             int               `json:"code"`
	Query            *pgIntervalQuery  `json:"query"`
	QueryFailure     string            `json:"query_failure"`
}

// pgIntervalReceipt is the owned-pool oracle's record of one `pg repeer`
// command and of the observations around it (tools/osd_pg_interval_oracle.py),
// or of one observation without a command (action show).
type pgIntervalReceipt struct {
	Action            string                `json:"action"`
	Origin            osdFixtureOracle      `json:"origin"`
	OriginObservation pgIntervalObservation `json:"origin_observation"`
	Command           map[string]string     `json:"command"`
	CommandSent       bool                  `json:"command_sent"`
	CommandCode       *int                  `json:"command_code"`
	CommandStatus     string                `json:"command_status"`
	Observation       pgIntervalObservation `json:"observation"`
	Observations      int                   `json:"observations"`
	ElapsedMillis     int64                 `json:"elapsed_ms"`
	Settled           bool                  `json:"settled"`
	Failures          []string              `json:"failures"`
}

// cleanOn requires one observation to be two identical placement records
// around a query that the primary of that placement answered for its own
// shard as exactly active+clean, with the placement's sets and no pg_temp,
// at a map epoch that is not behind the placement's.
func (o pgIntervalObservation) cleanOn() error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	p := *o.Before
	if p.PGTemp || !slices.Equal(p.Up, p.Acting) || p.ID != p.PrimaryID || p.PoolType != 3 || p.Shard < 0 || int(p.Shard) >= len(p.Acting) || p.Acting[p.Shard] != p.PrimaryID {
		return fmt.Errorf("the placement is not a primary on its own shard of equal up and acting sets without a pg_temp: %+v", p)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if q.State != "active+clean" || !slices.Equal(q.Up, p.Acting) || !slices.Equal(q.Acting, p.Acting) {
		return fmt.Errorf("the primary reports %q with up %v and acting %v; the placement has %v", q.State, q.Up, q.Acting, p.Acting)
	}
	if q.Info.PGID != p.PGID+"s"+strconv.Itoa(int(p.Shard)) || q.Info.Shard != int(p.Shard) {
		return fmt.Errorf("the query was answered for %s shard %d, not for shard %d of %s", q.Info.PGID, q.Info.Shard, p.Shard, p.PGID)
	}
	if q.SameIntervalSince == 0 || q.SameIntervalSince > p.Epoch || q.LastEpochStarted < q.SameIntervalSince {
		return fmt.Errorf("the interval started at epoch %d and the PG at epoch %d; the placement is at epoch %d", q.SameIntervalSince, q.LastEpochStarted, p.Epoch)
	}
	if q.Epoch < p.Epoch {
		// A primary behind the placement's map may still have an interval
		// change ahead of it.
		return fmt.Errorf("the primary is at map epoch %d, behind the placement's epoch %d", q.Epoch, p.Epoch)
	}
	return nil
}

// samePlacement compares two placement records in everything but the epoch.
func samePlacement(a, b osdFixtureOracle) bool {
	a.Epoch = b.Epoch
	return reflect.DeepEqual(a, b)
}

// checkPGInterval rechecks the receipt of one owned interval change
// independently of the oracle's judgement: exactly one accepted `pg repeer`
// of the PG the requests used, a clean starting point on that placement,
// and a clean observation of the same placement at a newer epoch in an
// interval that started after the one before the command.
func checkPGInterval(r pgIntervalReceipt, route osdFixtureOracle) error {
	if r.Action != "repeer" || !r.Settled || len(r.Failures) != 0 {
		return fmt.Errorf("the PG did not settle in a newer interval: action %q, %q", r.Action, r.Failures)
	}
	if !r.CommandSent || r.CommandCode == nil || *r.CommandCode != 0 || !reflect.DeepEqual(r.Command, map[string]string{"prefix": "pg repeer", "pgid": route.PGID}) {
		return fmt.Errorf("not one accepted pg repeer of %s: sent %t, %v, %q", route.PGID, r.CommandSent, r.Command, r.CommandStatus)
	}
	if !samePlacement(r.Origin, route) || r.Origin.Epoch < route.Epoch {
		return fmt.Errorf("the command's starting placement %+v is not the one the requests used: %+v", r.Origin, route)
	}
	if err := r.OriginObservation.cleanOn(); err != nil {
		return fmt.Errorf("starting point: %w", err)
	}
	if !reflect.DeepEqual(*r.OriginObservation.Before, r.Origin) {
		return fmt.Errorf("the starting point was observed on %+v, not on %+v", *r.OriginObservation.Before, r.Origin)
	}
	if err := r.Observation.cleanOn(); err != nil {
		return fmt.Errorf("after the command: %w", err)
	}
	settled, before, after := *r.Observation.Before, r.OriginObservation.Query, r.Observation.Query
	if !samePlacement(settled, r.Origin) || settled.Epoch <= r.Origin.Epoch {
		return fmt.Errorf("the placement after the command %+v is not the starting placement at a newer epoch: %+v", settled, r.Origin)
	}
	if after.SameIntervalSince <= before.SameIntervalSince || after.SameIntervalSince <= r.Origin.Epoch {
		return fmt.Errorf("the interval started at epoch %d; before the command, at epoch %d, it had started at %d", after.SameIntervalSince, r.Origin.Epoch, before.SameIntervalSince)
	}
	if r.Observations < 1 {
		return errors.New("the receipt counts no observation after the command")
	}
	return nil
}

// checkPGIntervalShown rechecks one observation without a command: the PG
// is clean on the given placement, at its epoch or a newer one.
func checkPGIntervalShown(r pgIntervalReceipt, route osdFixtureOracle) error {
	if r.Action != "show" || !r.Settled || len(r.Failures) != 0 || r.CommandSent || r.Command != nil {
		return fmt.Errorf("not a clean observation without a command: action %q, %q", r.Action, r.Failures)
	}
	if err := r.Observation.cleanOn(); err != nil {
		return err
	}
	if seen := *r.Observation.Before; !samePlacement(seen, route) || seen.Epoch < route.Epoch {
		return fmt.Errorf("the PG is on %+v, not on %+v", seen, route)
	}
	return nil
}

// showInterval observes the PG without a command until it is clean on the
// run's placement, in at most fifteen observations a second apart: a PG of
// a pool that was just created, or one that was just written, may still be
// on its way there.
func (r *partialWriteRun) showInterval() pgIntervalReceipt {
	r.t.Helper()
	var err error
	for attempt := 0; attempt < 15; attempt++ {
		shown := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": r.route}).PGInterval
		if err = checkPGIntervalShown(shown, r.route); err == nil {
			return shown
		}
		time.Sleep(time.Second)
	}
	r.t.Fatalf("the PG is not clean on the placement the requests use: %v", err)
	return pgIntervalReceipt{}
}

// waitPrimaryRecords observes the live primary until its own info holds
// exactly the partial-write records, within a bounded number of
// observations. The primary writes them when it rolls the entry forward,
// which no client reply waits for; the interval change below relies on them.
// Every observation must report the interval since started, in which the
// writes, reads and captures before it were made: the accepted one shows
// that interval held through them.
func (r *partialWriteRun) waitPrimaryRecords(records []shardPartialWrite, started uint32) {
	r.t.Helper()
	var got []shardPartialWrite
	for attempt := 1; attempt <= 15; attempt++ {
		q := r.showInterval().Observation.Query
		if q.SameIntervalSince != started {
			r.t.Fatalf("unattributed: the PG's interval started at epoch %d before the writes and at epoch %d at observation %d of the partial-write records", started, q.SameIntervalSince, attempt)
		}
		if got = q.Info.PartialWrites; slices.Equal(got, records) {
			r.t.Logf("the live primary's info holds the partial-write records %+v (observation %d)", got, attempt)
			return
		}
		time.Sleep(time.Second)
	}
	r.t.Fatalf("the live primary's info holds the partial-write records %+v, not %+v", got, records)
}

// changeInterval ends the PG's interval once with the owned `pg repeer`
// control and requires the PG to settle clean on its original placement in
// a newer interval. No daemon stops and no other PG is touched. It then
// adopts the placement at the new epoch for the requests that follow. The
// cleanup proves that the PG is back on its original placement without a
// pg_temp before the pool is removed.
func (r *partialWriteRun) changeInterval() pgIntervalReceipt {
	r.t.Helper()
	origin := r.route
	r.t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		var err error
		for attempt := 0; attempt < 20; attempt++ {
			shown := nativePool(r.t, cleanup, "pginterval", map[string]any{"action": "show", "slot": partialWriteSlot, "object": partialWriteObject, "origin": origin}).PGInterval
			if err = checkPGIntervalShown(shown, origin); err == nil {
				r.t.Logf("restored: pg %s is active+clean at epoch %d on up %v acting %v primary osd.%d without a pg_temp", origin.PGID, shown.Observation.Before.Epoch,
					shown.Observation.Before.Up, shown.Observation.Before.Acting, shown.Observation.Before.PrimaryID)
				return
			}
			time.Sleep(time.Second)
		}
		r.t.Error("the PG did not return to its original placement:", err)
	})
	changed := nativePool(r.t, r.ctx, "pginterval", map[string]any{"action": "repeer", "slot": partialWriteSlot, "object": partialWriteObject}).PGInterval
	if text, err := json.Marshal(changed); err == nil {
		r.t.Logf("interval change receipt: %s", text)
	}
	if err := checkPGInterval(changed, origin); err != nil {
		r.t.Fatal(err)
	}
	before, after := changed.OriginObservation.Query, changed.Observation.Query
	r.t.Logf("pg %s: epoch %d -> %d, interval since %d -> %d, up %v acting %v primary osd.%d shard %d unchanged, %d observations in %d ms",
		origin.PGID, changed.Origin.Epoch, changed.Observation.Before.Epoch, before.SameIntervalSince, after.SameIntervalSince,
		origin.Up, origin.Acting, origin.PrimaryID, origin.Shard, changed.Observations, changed.ElapsedMillis)
	for _, peer := range append([]pgIntervalInfo{after.Info}, after.Peers...) {
		r.t.Logf("live primary after the change: %s %s last_update %s last_complete %s, partial-write records %+v at epoch %d",
			peer.PGID, peer.Peer, peer.LastUpdate, peer.LastComplete, peer.PartialWrites, peer.PartialWritesEpoch)
	}
	fresh := poolPlace(r.t, r.ctx, partialWriteSlot, partialWriteObject, "")
	if !reflect.DeepEqual(fresh, *changed.Observation.After) {
		r.t.Fatalf("placement moved after the PG settled: %+v vs %+v", fresh, *changed.Observation.After)
	}
	r.facts.checkPlacement(r.t, fresh)
	r.connection.Close()
	r.route = fresh
	r.connection = mutationConnection(r.t, r.client, r.ctx, fresh)
	return changed
}

// read sends one Go read of the whole object exactly once and requires the
// expected bytes.
func (r *partialWriteRun) read(label string, incarnation uint32) cephmsgr.OSDReply {
	r.t.Helper()
	message := r.request(r.route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(r.data) + 64)})
	message.ClientIncarnation = incarnation
	reply, err := r.connection.Request(r.ctx, message)
	if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 {
		r.t.Fatalf("%s: err=%v code=%d; not resent", label, err, reply.Code)
	}
	if !bytes.Equal(reply.Operations[0].Data, r.data) {
		r.t.Fatalf("%s returned %d bytes that differ from the %d expected", label, len(reply.Operations[0].Data), len(r.data))
	}
	return reply
}

// requireShardLogs requires every acting shard's own log to be exactly what
// shardLogExpected restates for the writes, and its partial-write records to
// be one of the lists records returns for that shard. peered says that the
// PG peered after the last write.
func (r *partialWriteRun) requireShardLogs(label string, primary pgHistorySnapshot, shards shardHistorySnapshot, object string, writes []shardWrite, peered bool, records func(shard int) [][]shardPartialWrite) {
	r.t.Helper()
	for i, write := range writes {
		if _, err := writeEntry(primary.Targets[i].Locations, object, write.version); err != nil {
			r.t.Fatalf("%s, live primary, %s: %v", label, write.label, err)
		}
	}
	if last := writes[len(writes)-1].version; primary.Head != last || primary.LogEntries != len(writes) {
		r.t.Fatalf("%s: the live primary's log holds %d entries and ends at %s, not the %d writes ending at %s", label, primary.LogEntries, primary.Head, len(writes), last)
	}
	for i, shard := range shards.Shards {
		// Acting index i is shard i: the capture selected and the store
		// returned collection <pgid>s<i>. The role follows from the native
		// width and nonprimary shards, never from the wire targets.
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
		expected, err := shardLogExpected(writes, i, r.facts.size, peered)
		if err != nil {
			r.t.Fatal(err)
		}
		if !reflect.DeepEqual(observed, expected) {
			r.t.Fatalf("%s: shard %s (osd.%d, %s) %s; the writes %+v require that it %s", label, shard.SPG, shard.OSD, role, observed, writes, expected)
		}
		if allowed := records(i); !partialWriteRecordsAmong(shard.PartialWrites, allowed...) {
			r.t.Fatalf("%s: shard %s (osd.%d, %s) holds the partial-write records %+v; want one of %+v", label, shard.SPG, shard.OSD, role, shard.PartialWrites, allowed)
		}
	}
}

func (r *partialWriteRun) everyShard() []int {
	shards := make([]int, r.facts.size)
	for i := range shards {
		shards[i] = i
	}
	return shards
}

// After the partial overwrite of the first control, which leaves the
// nonprimary data shard out, the Go client overwrites a few bytes inside
// that shard's chunk in the same interval. The write is acknowledged and
// read back natively, and every acting shard's store is captured again. The
// later write's entry must be held by every shard, the nonprimary data shard
// included, and name every shard. That shard must still not hold the entry
// of the overwrite it was left out of: its own log holds the full write and
// the later write, with its head at the later write. The PG must have stayed
// in one interval. This states what each shard's own log holds at capture
// time; it claims nothing about eviction, loss or replay.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritelaterwriteshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-pg-interval,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated pauses each acting OSD for a store copy; no wait for every PG clean without pg_temp afterwards
func TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration(t *testing.T) {
	run := startPartialWriteRun(t, 600*time.Second)
	started := run.showInterval().Observation.Query.SameIntervalSince
	state := run.partialOverwrite()
	const laterIncarnation = 11
	later := []byte("later\x00\xffwrite")
	// A few bytes into the nonprimary data shard's chunk of the first stripe.
	offset := run.stripe/uint64(run.width.k) + 5
	written, skipped := run.plan(offset, len(later))
	if len(skipped) != 0 {
		t.Fatalf("the later write [%d,+%d) names shards %v and leaves out %v; this control needs every shard named", offset, len(later), written, skipped)
	}
	reply := run.overwrite("later write", laterIncarnation, 1700009102, offset, later)
	if reply.UserVersion <= state.patched.UserVersion {
		t.Fatal("the later write did not advance the user version", state.patched.UserVersion, reply.UserVersion)
	}
	key := run.key(laterIncarnation, reply)
	writes := []shardWrite{
		{"full write", state.want.full, run.everyShard()},
		{"partial overwrite", state.want.patch, state.want.written},
		{"later write", replayVersion(reply), written},
	}
	t.Logf("later write %s [%d,+%d) at %s, user version %d, names shards %v", key, offset, len(later), writes[2].version, reply.UserVersion, written)

	// No shard may show a head past its entries: nothing peered.
	primary, shards := run.capture([]pgHistoryTarget{state.fullKey, state.patchKey, key}, state.misses, nil)
	run.requireShardLogs("after the later write", primary, shards, state.want.object, writes, false, func(shard int) [][]shardPartialWrite {
		if slices.Contains(run.facts.nonprimary, shard) {
			// A nonprimary shard records nothing (pinned
			// src/osd/PGBackend.cc:424-428).
			return [][]shardPartialWrite{nil}
		}
		// No reply waits for a roll-forward. That of the overwrite records
		// the range of each left-out shard; that of the later write, which
		// names every shard, resets an existing record to that write and
		// adds none (pinned :419-421, :474-490).
		var partial, reset []shardPartialWrite
		for _, left := range state.want.skipped() {
			partial = append(partial, shardPartialWrite{Shard: left, From: state.want.full, To: state.want.patch})
			reset = append(reset, shardPartialWrite{Shard: left, From: writes[2].version, To: writes[2].version})
		}
		allowed := [][]shardPartialWrite{partial, reset}
		if len(state.shards.Shards[shard].PartialWrites) == 0 {
			// The first capture preceded the overwrite's roll-forward on
			// this shard, so the second may precede it as well.
			allowed = append(allowed, nil)
		}
		return allowed
	})
	if ended := run.showInterval().Observation.Query.SameIntervalSince; ended != started {
		t.Fatalf("the PG's interval started at epoch %d before the writes and at epoch %d after the last capture", started, ended)
	}
	t.Logf("the PG stayed in the interval that started at epoch %d", started)
}

// After the partial overwrite of the first control, which leaves the
// nonprimary data shard out, the PG's interval is ended once by an owned,
// self-restoring `pg repeer` and the PG must settle clean on its original
// placement in a newer interval; no daemon stops and no data moves. Every
// acting shard's store is captured again: the shards that hold the
// overwrite's entry are unchanged, and the nonprimary data shard still does
// not hold it, but its own last_update, last_complete and log head are now
// at the overwrite, past its last entry, and its own info holds the
// partial-write range between them. The object is read back natively and
// through the Go client on the new placement record. The first Go write of
// the new interval, again confined to shard 0's chunk, must be held by every
// shard and name every shard; the nonprimary data shard then holds the full
// write and this write, and not the overwrite between them. This states what
// each shard's own log holds at capture time; it claims nothing about
// eviction, loss or replay, and it changes no primary.
// What the shards held in the intermediate interval, while the PG was not
// active, is not captured.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritenewintervalshardhistory key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-pg-interval,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated repeers its PG and proves it clean without pg_temp, but pauses each acting OSD for a store copy with no clean wait
func TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration(t *testing.T) {
	run := startPartialWriteRun(t, 600*time.Second)
	started := run.showInterval().Observation.Query.SameIntervalSince
	state := run.partialOverwrite()
	var records []shardPartialWrite
	for _, left := range state.want.skipped() {
		records = append(records, shardPartialWrite{Shard: left, From: state.want.full, To: state.want.patch})
	}
	// The interval change applies the range only if the primary recorded it.
	run.waitPrimaryRecords(records, started)
	changed := run.changeInterval()
	if before := changed.OriginObservation.Query; before.SameIntervalSince != started || !slices.Equal(before.Info.PartialWrites, records) {
		t.Fatalf("before the command the interval had started at epoch %d (%d at the first observation) and the primary's info held %+v", before.SameIntervalSince, started, before.Info.PartialWrites)
	}
	interval := changed.Observation.Query.SameIntervalSince

	writes := []shardWrite{
		{"full write", state.want.full, run.everyShard()},
		{"partial overwrite", state.want.patch, state.want.written},
	}
	keys := []pgHistoryTarget{state.fullKey, state.patchKey}
	// Only the pool's nonprimary shards may show a head past their entries.
	primary, shards := run.capture(keys, state.misses, run.facts.nonprimary)
	// Peering merges the primary's records into every acting shard's info.
	exact := func(int) [][]shardPartialWrite { return [][]shardPartialWrite{records} }
	run.requireShardLogs("after the interval change", primary, shards, state.want.object, writes, true, exact)
	run.observe("interval change", state.patched)

	const (
		readIncarnation  = 12
		firstIncarnation = 13
	)
	if read := run.read("read in the new interval", readIncarnation); read.UserVersion != state.patched.UserVersion {
		t.Fatalf("the Go read in the new interval returned user version %d; the overwrite's is %d", read.UserVersion, state.patched.UserVersion)
	}
	// Without the interval change this overwrite would leave the nonprimary
	// data shard out again; the first write of an interval names every shard.
	first := []byte("first\x00\xffwrite")
	const offset = 64
	if planned, left := run.plan(offset, len(first)); !slices.Equal(left, run.facts.nonprimary) {
		t.Fatalf("the overwrite [%d,+%d) names shards %v and leaves out %v by its bytes; this control needs the nonprimary shards %v left out", offset, len(first), planned, left, run.facts.nonprimary)
	}
	reply := run.overwrite("first write of the new interval", firstIncarnation, 1700009103, offset, first)
	if reply.UserVersion <= state.patched.UserVersion || reply.ReplayVersion.Epoch < interval {
		t.Fatalf("the first write of the new interval has user version %d (overwrite %d) and replay version %s; the interval started at epoch %d",
			reply.UserVersion, state.patched.UserVersion, replayVersion(reply), interval)
	}
	key := run.key(firstIncarnation, reply)
	writes = append(writes, shardWrite{"first write of the new interval", replayVersion(reply), run.everyShard()})
	t.Logf("first write of the new interval %s [%d,+%d) at %s, user version %d", key, offset, len(first), writes[2].version, reply.UserVersion)

	primary, shards = run.capture(append(keys, key), state.misses, nil)
	run.requireShardLogs("after the first write of the new interval", primary, shards, state.want.object, writes, false, func(shard int) [][]shardPartialWrite {
		if slices.Contains(run.facts.nonprimary, shard) {
			// A nonprimary shard never updates the records it merged.
			return [][]shardPartialWrite{records}
		}
		var reset []shardPartialWrite
		for _, left := range state.want.skipped() {
			reset = append(reset, shardPartialWrite{Shard: left, From: writes[2].version, To: writes[2].version})
		}
		return [][]shardPartialWrite{records, reset}
	})
	if ended := run.showInterval().Observation.Query.SameIntervalSince; ended != interval {
		t.Fatalf("the PG's interval started at epoch %d after the owned change and at epoch %d after the last capture", interval, ended)
	}
	t.Logf("the PG stayed in the interval that started at epoch %d", interval)
}

// shardLogReceipt builds one synthetic shard receipt and the positions of
// three writes in it; no daemon produced it. held selects the writes whose
// entries the shard holds; head is its log head, last its last entry.
func shardLogReceipt(held [3]bool, head, last string) (shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {
	zero := 0
	writes := []shardWrite{{"full write", "25'1", []int{0, 1, 2, 3}}, {"partial overwrite", "25'2", []int{0, 2, 3}}, {"later write", "25'3", []int{0, 1, 2, 3}}}
	shard := shardHistoryShard{Index: 1, SPG: "3.7s1", Head: head, LastEntry: last, LastUpdate: head, LastComplete: head, HeadPastEntries: head != last}
	locations := make([][]pgHistoryLocation, len(writes))
	for i, write := range writes {
		if !held[i] {
			continue
		}
		named := write.named
		if len(named) == 4 {
			named = nil
		}
		locations[i] = []pgHistoryLocation{{Tier: "log", Index: len(shard.Entries), Version: write.version, Op: "modify", Object: "3:e65c9b8f:::obj:head", ReturnCode: &zero}}
		shard.Entries = append(shard.Entries, shardLogEntry{Version: write.version, WrittenShards: named})
	}
	shard.LogEntries = len(shard.Entries)
	return shard, locations, writes
}

func TestShardLogObservation(t *testing.T) {
	const object = "3:e65c9b8f:::obj:head"
	// The shapes a clean PG can show for a full write, a partial overwrite
	// that leaves shard 1 out and a later write that names every shard.
	for name, c := range map[string]struct {
		held       [3]bool
		head, last string
		writes     int
		want       shardLogState
		text       string
	}{
		"a holder of every write":             {[3]bool{true, true, true}, "25'3", "25'3", 3, shardLogState{[]bool{true, true, true}, "25'3", false}, "holds #1 #2 #3, head 25'3 at its last entry"},
		"left out of the overwrite":           {[3]bool{true, false, false}, "25'1", "25'1", 2, shardLogState{[]bool{true, false}, "25'1", false}, "holds #1, head 25'1 at its last entry"},
		"left out, then named":                {[3]bool{true, false, true}, "25'3", "25'3", 3, shardLogState{[]bool{true, false, true}, "25'3", false}, "holds #1 #3, head 25'3 at its last entry"},
		"left out, head advanced by peering":  {[3]bool{true, false, false}, "25'2", "25'1", 2, shardLogState{[]bool{true, false}, "25'2", true}, "holds #1, head 25'2 past its last entry"},
		"a holder of the first two":           {[3]bool{true, true, false}, "25'2", "25'2", 2, shardLogState{[]bool{true, true}, "25'2", false}, "holds #1 #2, head 25'2 at its last entry"},
		"head advanced over two left-out":     {[3]bool{true, false, false}, "25'3", "25'1", 3, shardLogState{[]bool{true, false, false}, "25'3", true}, "holds #1, head 25'3 past its last entry"},
		"head advanced to the first left-out": {[3]bool{true, false, false}, "25'2", "25'1", 3, shardLogState{[]bool{true, false, false}, "25'2", true}, "holds #1, head 25'2 past its last entry"},
	} {
		shard, locations, writes := shardLogReceipt(c.held, c.head, c.last)
		got, err := shardLogObservation(shard, locations[:c.writes], object, writes[:c.writes], 4)
		if err != nil || !reflect.DeepEqual(got, c.want) || got.String() != c.text {
			t.Errorf("%s: %+v %q %v", name, got, got, err)
		}
	}
	failed, one := -5, 1
	type edit func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite)
	// Everything else contradicts the acknowledged writes and is never
	// reported as an entry that is merely not held.
	for name, c := range map[string]struct {
		held       [3]bool
		head, last string
		edit       edit
	}{
		"the first write is not held":       {[3]bool{false, true, true}, "25'3", "25'3", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"no write is held":                  {[3]bool{}, "25'1", "0'0", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"a write twice":                     {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[1] = append(l[1], l[1][0]) }},
		"a write only among the duplicates": {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[1][0].Tier = "dups" }},
		"a write as an extra request ID":    {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[2][0].ExtraIndex = &one }},
		"a write failed":                    {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[2][0].ReturnCode = &failed }},
		"a write without a return code":     {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[0][0].ReturnCode = nil }},
		"a write is a delete":               {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[2][0].Op = "delete" }},
		"a write on another object": {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) {
			l[2][0].Object = "3:e65c9b8f:::other:head"
		}},
		"a write at another version": {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) { l[2][0].Version = "25'4" }},
		"writes in another order": {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) {
			l[1][0].Index, l[2][0].Index = 2, 1
		}},
		"a write beyond the entry list": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.Entries = s.Entries[:2] }},
		"entry list at another version": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.Entries[1].Version = "25'4" }},
		"a foreign entry between two writes": {[3]bool{true, false, true}, "25'3", "25'3", func(s *shardHistoryShard, l [][]pgHistoryLocation, _ []shardWrite) {
			s.Entries = []shardLogEntry{s.Entries[0], {Version: "25'2", WrittenShards: []int{0, 2, 3}}, s.Entries[1]}
			s.LogEntries, l[2][0].Index = 3, 2
		}},
		"a foreign entry after the writes": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) {
			s.Entries, s.LogEntries = append(s.Entries, shardLogEntry{Version: "25'4"}), 4
		}},
		"entry count differs from the list": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.LogEntries = 4 }},
		"the overwrite names every shard":   {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.Entries[1].WrittenShards = nil }},
		"the overwrite names the left-out": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) {
			s.Entries[1].WrittenShards = []int{0, 1, 2, 3}
		}},
		"the later write names three shards": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) {
			s.Entries[2].WrittenShards = []int{0, 2, 3}
		}},
		"the full write names three shards": {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) {
			s.Entries[0].WrittenShards = []int{0, 2, 3}
		}},
		"a write that names no shard":         {[3]bool{true, true, true}, "25'3", "25'3", func(_ *shardHistoryShard, _ [][]pgHistoryLocation, w []shardWrite) { w[2].named = nil }},
		"head behind the last entry":          {[3]bool{true, true, true}, "25'2", "25'3", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"head ahead, not reported as past":    {[3]bool{true, false, false}, "25'2", "25'1", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.HeadPastEntries = false }},
		"reported past at its last entry":     {[3]bool{true, false, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.HeadPastEntries = true }},
		"last entry is not the last held":     {[3]bool{true, false, true}, "25'3", "25'1", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.HeadPastEntries = false }},
		"head past the entries at no write":   {[3]bool{true, false, false}, "25'4", "25'1", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"head past the entries, older epoch":  {[3]bool{true, false, false}, "24'2", "25'1", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"head past, at an earlier held write": {[3]bool{true, true, true}, "25'2", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.HeadPastEntries = true }},
		"head before a later held write":      {[3]bool{true, false, true}, "25'2", "25'3", func(*shardHistoryShard, [][]pgHistoryLocation, []shardWrite) {}},
		"last_update behind the head":         {[3]bool{true, true, true}, "25'3", "25'3", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.LastUpdate = "25'2" }},
		"last_complete behind the head":       {[3]bool{true, false, false}, "25'2", "25'1", func(s *shardHistoryShard, _ [][]pgHistoryLocation, _ []shardWrite) { s.LastComplete = "25'1" }},
	} {
		shard, locations, writes := shardLogReceipt(c.held, c.head, c.last)
		c.edit(&shard, locations, writes)
		if got, err := shardLogObservation(shard, locations, object, writes, 4); err == nil || !reflect.DeepEqual(got, shardLogState{}) {
			t.Errorf("contradictory shard log classified as %q: %s", got, name)
		}
	}
	shard, locations, writes := shardLogReceipt([3]bool{true, true, true}, "25'3", "25'3")
	for name, call := range map[string]func() (shardLogState, error){
		"no write":             func() (shardLogState, error) { return shardLogObservation(shard, nil, object, nil, 4) },
		"fewer position lists": func() (shardLogState, error) { return shardLogObservation(shard, locations[:2], object, writes, 4) },
		"more position lists":  func() (shardLogState, error) { return shardLogObservation(shard, locations, object, writes[:2], 4) },
		"one shard":            func() (shardLogState, error) { return shardLogObservation(shard, locations, object, writes, 1) },
		"another object": func() (shardLogState, error) {
			return shardLogObservation(shard, locations, "3:e65c9b8f:::other:head", writes, 4)
		},
		"a pool of another width":   func() (shardLogState, error) { return shardLogObservation(shard, locations, object, writes, 5) },
		"a pool of the named width": func() (shardLogState, error) { return shardLogObservation(shard, locations, object, writes, 3) },
	} {
		if got, err := call(); err == nil || !reflect.DeepEqual(got, shardLogState{}) {
			t.Errorf("shard log accepted: %s: %q", name, got)
		}
	}
}

func TestShardLogExpected(t *testing.T) {
	all := []int{0, 1, 2, 3}
	full, patch, later := shardWrite{"full write", "25'1", all}, shardWrite{"partial overwrite", "25'2", []int{0, 2, 3}}, shardWrite{"later write", "28'3", all}
	for name, c := range map[string]struct {
		writes []shardWrite
		shard  int
		peered bool
		want   shardLogState
	}{
		"holder after the overwrite":             {[]shardWrite{full, patch}, 0, false, shardLogState{[]bool{true, true}, "25'2", false}},
		"parity holder after the overwrite":      {[]shardWrite{full, patch}, 3, false, shardLogState{[]bool{true, true}, "25'2", false}},
		"left-out shard after the overwrite":     {[]shardWrite{full, patch}, 1, false, shardLogState{[]bool{true, false}, "25'1", false}},
		"holder after a peering":                 {[]shardWrite{full, patch}, 2, true, shardLogState{[]bool{true, true}, "25'2", false}},
		"left-out shard after a peering":         {[]shardWrite{full, patch}, 1, true, shardLogState{[]bool{true, false}, "25'2", true}},
		"left-out shard after a later write":     {[]shardWrite{full, patch, later}, 1, false, shardLogState{[]bool{true, false, true}, "28'3", false}},
		"left-out shard, later write, peered":    {[]shardWrite{full, patch, later}, 1, true, shardLogState{[]bool{true, false, true}, "28'3", false}},
		"holder after a later write":             {[]shardWrite{full, patch, later}, 0, false, shardLogState{[]bool{true, true, true}, "28'3", false}},
		"left out twice, peered":                 {[]shardWrite{full, patch, {"second overwrite", "25'3", []int{0, 2, 3}}}, 1, true, shardLogState{[]bool{true, false, false}, "25'3", true}},
		"left out twice, not peered":             {[]shardWrite{full, patch, {"second overwrite", "25'3", []int{0, 2, 3}}}, 1, false, shardLogState{[]bool{true, false, false}, "25'1", false}},
		"only the full write, peered":            {[]shardWrite{full}, 1, true, shardLogState{[]bool{true}, "25'1", false}},
		"another shard left out of k=4 writes":   {[]shardWrite{{"full write", "9'1", []int{0, 1, 2, 3, 4, 5}}, {"overwrite", "9'2", []int{0, 2, 4, 5}}}, 3, true, shardLogState{[]bool{true, false}, "9'2", true}},
		"a touched shard of the same k=4 writes": {[]shardWrite{{"full write", "9'1", []int{0, 1, 2, 3, 4, 5}}, {"overwrite", "9'2", []int{0, 2, 4, 5}}}, 2, true, shardLogState{[]bool{true, true}, "9'2", false}},
	} {
		shards := len(c.writes[0].named)
		if got, err := shardLogExpected(c.writes, c.shard, shards, c.peered); err != nil || !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
	for name, c := range map[string]struct {
		writes        []shardWrite
		shard, shards int
	}{
		"no write":                          {nil, 1, 4},
		"a shard beyond the pool":           {[]shardWrite{full, patch}, 4, 4},
		"a negative shard":                  {[]shardWrite{full, patch}, -1, 4},
		"first write names three shards":    {[]shardWrite{patch, full}, 1, 4},
		"first write of a narrower pool":    {[]shardWrite{full, patch}, 1, 5},
		"first write names too many":        {[]shardWrite{full, patch}, 1, 3},
		"first write names no shard at all": {[]shardWrite{{"full write", "25'1", nil}}, 0, 4},
	} {
		if got, err := shardLogExpected(c.writes, c.shard, c.shards, true); err == nil || !reflect.DeepEqual(got, shardLogState{}) {
			t.Errorf("expectation built: %s: %+v", name, got)
		}
	}
	// What the observation accepts for a shard is what the expectation
	// states for it, for every selection the two controls capture.
	for _, c := range []struct {
		held       [3]bool
		head, last string
		writes     int
		peered     bool
	}{
		{[3]bool{true, false, false}, "25'1", "25'1", 2, false},
		{[3]bool{true, false, false}, "25'2", "25'1", 2, true},
		{[3]bool{true, false, true}, "25'3", "25'3", 3, false},
	} {
		shard, locations, writes := shardLogReceipt(c.held, c.head, c.last)
		observed, err := shardLogObservation(shard, locations[:c.writes], "3:e65c9b8f:::obj:head", writes[:c.writes], 4)
		expected, wantErr := shardLogExpected(writes[:c.writes], 1, 4, c.peered)
		if err != nil || wantErr != nil || !reflect.DeepEqual(observed, expected) {
			t.Errorf("observation %+v (%v) differs from the expectation %+v (%v)", observed, err, expected, wantErr)
		}
		// The same receipt under the other peering assumption must differ
		// whenever the shard was left out of the last write.
		if other, _ := shardLogExpected(writes[:c.writes], 1, 4, !c.peered); c.writes == 2 && reflect.DeepEqual(observed, other) {
			t.Errorf("observation %+v also equals the expectation for peered=%t", observed, !c.peered)
		}
	}
	record := []shardPartialWrite{{Shard: 1, From: "25'1", To: "25'2"}}
	reset := []shardPartialWrite{{Shard: 1, From: "25'3", To: "25'3"}}
	for name, c := range map[string]struct {
		got     []shardPartialWrite
		allowed [][]shardPartialWrite
		want    bool
	}{
		"the one allowed list":         {record, [][]shardPartialWrite{record}, true},
		"the second allowed list":      {reset, [][]shardPartialWrite{record, reset}, true},
		"no record where none allowed": {nil, [][]shardPartialWrite{nil}, true},
		"an empty list for none":       {[]shardPartialWrite{}, [][]shardPartialWrite{nil}, true},
		"no record where one required": {nil, [][]shardPartialWrite{record}, false},
		"a record where none allowed":  {record, [][]shardPartialWrite{nil}, false},
		"another range":                {[]shardPartialWrite{{Shard: 1, From: "25'1", To: "25'3"}}, [][]shardPartialWrite{record, reset}, false},
		"another shard":                {[]shardPartialWrite{{Shard: 2, From: "25'1", To: "25'2"}}, [][]shardPartialWrite{record}, false},
		"both records":                 {append(slices.Clone(record), reset...), [][]shardPartialWrite{record, reset}, false},
		"nothing allowed":              {nil, nil, false},
	} {
		if got := partialWriteRecordsAmong(c.got, c.allowed...); got != c.want {
			t.Errorf("%s: %t", name, got)
		}
	}
}

// pgIntervalChange builds one synthetic receipt of an owned interval change
// of PG 3.7 on acting [3 2 1 0]: clean at epoch 25 in the interval that
// started at 24, then clean at epoch 28 in the interval that started at 27.
// No daemon produced it.
func pgIntervalChange() (pgIntervalReceipt, osdFixtureOracle) {
	route := osdFixtureOracle{Up: []int32{3, 2, 1, 0}, Acting: []int32{3, 2, 1, 0}, Replicas: 4, MinSize: 3, Pool: 3, Name: "obj", ID: 3, PrimaryID: 3, Shard: 0,
		PoolType: 3, Nonprimary: 1, Epoch: 25, Hash: 7, PG: 7, RawPGID: "3.7", PGID: "3.7", Address: "v2:192.0.2.7:6800/1"}
	observe := func(epoch, since, started uint32) pgIntervalObservation {
		before, after := route, route
		before.Up, before.Acting, after.Up, after.Acting = slices.Clone(route.Up), slices.Clone(route.Acting), slices.Clone(route.Up), slices.Clone(route.Acting)
		before.Epoch, after.Epoch = epoch, epoch
		return pgIntervalObservation{Before: &before, After: &after, Query: &pgIntervalQuery{State: "active+clean", Epoch: epoch, Up: slices.Clone(route.Up), Acting: slices.Clone(route.Acting),
			SameIntervalSince: since, LastEpochStarted: started, Info: pgIntervalInfo{PGID: "3.7s0", Shard: 0, LastUpdate: "25'2", LastComplete: "25'2"}}}
	}
	zero := 0
	return pgIntervalReceipt{Action: "repeer", Origin: route, OriginObservation: observe(25, 24, 25), Command: map[string]string{"prefix": "pg repeer", "pgid": "3.7"},
		CommandSent: true, CommandCode: &zero, Observation: observe(28, 27, 28), Observations: 6, ElapsedMillis: 3000, Settled: true}, route
}

func TestCheckPGInterval(t *testing.T) {
	receipt, route := pgIntervalChange()
	if err := checkPGInterval(receipt, route); err != nil {
		t.Fatal("settled interval change refused:", err)
	}
	// The interval may start at the very epoch of the settled placement,
	// and the requests may have used an older epoch of the same placement.
	edge, _ := pgIntervalChange()
	edge.Observation.Query.SameIntervalSince = 28
	older := route
	older.Epoch = 24
	if err := checkPGInterval(edge, older); err != nil {
		t.Error("interval that starts at the settled epoch refused:", err)
	}
	failed := -11
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"not settled":                      func(r *pgIntervalReceipt) { r.Settled = false },
		"a recorded failure":               func(r *pgIntervalReceipt) { r.Failures = []string{"the PG has a pg_temp"} },
		"a show receipt":                   func(r *pgIntervalReceipt) { r.Action = "show" },
		"no command sent":                  func(r *pgIntervalReceipt) { r.CommandSent = false },
		"no command code":                  func(r *pgIntervalReceipt) { r.CommandCode = nil },
		"command refused":                  func(r *pgIntervalReceipt) { r.CommandCode = &failed },
		"command for another PG":           func(r *pgIntervalReceipt) { r.Command["pgid"] = "3.6" },
		"another command":                  func(r *pgIntervalReceipt) { r.Command["prefix"] = "osd out" },
		"a command with one more argument": func(r *pgIntervalReceipt) { r.Command["force"] = "true" },
		"no command":                       func(r *pgIntervalReceipt) { r.Command = nil },
		"no observation counted":           func(r *pgIntervalReceipt) { r.Observations = 0 },
		"origin on another primary":        func(r *pgIntervalReceipt) { r.Origin.PrimaryID, r.Origin.ID = 1, 1 },
		"origin in another PG":             func(r *pgIntervalReceipt) { r.Origin.PGID = "3.6" },
		"origin in another acting order":   func(r *pgIntervalReceipt) { r.Origin.Acting = []int32{3, 1, 2, 0} },
		"origin at another address":        func(r *pgIntervalReceipt) { r.Origin.Address = "v2:192.0.2.7:6801/2" },
		"origin older than the requests":   func(r *pgIntervalReceipt) { r.Origin.Epoch = 24 },
		"origin observed at another epoch": func(r *pgIntervalReceipt) { r.OriginObservation.Before.Epoch, r.OriginObservation.After.Epoch = 26, 26 },
		"origin placement moved":           func(r *pgIntervalReceipt) { r.OriginObservation.After.Epoch = 26 },
		"origin without a placement":       func(r *pgIntervalReceipt) { r.OriginObservation.Before = nil },
		"origin without a query":           func(r *pgIntervalReceipt) { r.OriginObservation.Query = nil },
		"origin not clean":                 func(r *pgIntervalReceipt) { r.OriginObservation.Query.State = "active+clean+scrubbing" },
		"origin interval after its epoch": func(r *pgIntervalReceipt) {
			r.OriginObservation.Query.SameIntervalSince, r.OriginObservation.Query.LastEpochStarted = 26, 26
		},
		"origin under a pg_temp": func(r *pgIntervalReceipt) {
			r.OriginObservation.Before.PGTemp, r.OriginObservation.After.PGTemp, r.Origin.PGTemp = true, true, true
		},
		"no placement after the command":   func(r *pgIntervalReceipt) { r.Observation.Before = nil },
		"no second placement":              func(r *pgIntervalReceipt) { r.Observation.After = nil },
		"placement moved during the query": func(r *pgIntervalReceipt) { r.Observation.After.Epoch = 29 },
		"the same epoch":                   func(r *pgIntervalReceipt) { r.Observation.Before.Epoch, r.Observation.After.Epoch = 25, 25 },
		"an older epoch":                   func(r *pgIntervalReceipt) { r.Observation.Before.Epoch, r.Observation.After.Epoch = 24, 24 },
		"still under the pg_temp":          func(r *pgIntervalReceipt) { r.Observation.Before.PGTemp, r.Observation.After.PGTemp = true, true },
		"acting of the primary alone": func(r *pgIntervalReceipt) {
			r.Observation.Before.Acting, r.Observation.After.Acting = []int32{3}, []int32{3}
		},
		"up differs from acting": func(r *pgIntervalReceipt) {
			r.Observation.Before.Up, r.Observation.After.Up = []int32{3, 2, 1}, []int32{3, 2, 1}
		},
		"both sets in another order": func(r *pgIntervalReceipt) {
			for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
				p.Up, p.Acting = []int32{3, 1, 2, 0}, []int32{3, 1, 2, 0}
			}
			r.Observation.Query.Up, r.Observation.Query.Acting = []int32{3, 1, 2, 0}, []int32{3, 1, 2, 0}
		},
		"another primary": func(r *pgIntervalReceipt) {
			for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
				p.PrimaryID, p.ID, p.Shard = 1, 1, 2
			}
			r.Observation.Query.Info.PGID, r.Observation.Query.Info.Shard = "3.7s2", 2
		},
		"another address": func(r *pgIntervalReceipt) {
			r.Observation.Before.Address, r.Observation.After.Address = "v2:192.0.2.7:6801/2", "v2:192.0.2.7:6801/2"
		},
		"target is not the primary":      func(r *pgIntervalReceipt) { r.Observation.Before.ID, r.Observation.After.ID = 2, 2 },
		"a replicated placement":         func(r *pgIntervalReceipt) { r.Observation.Before.PoolType, r.Observation.After.PoolType = 1, 1 },
		"no shard":                       func(r *pgIntervalReceipt) { r.Observation.Before.Shard, r.Observation.After.Shard = -1, -1 },
		"query refused":                  func(r *pgIntervalReceipt) { r.Observation.Code = -11 },
		"query failure recorded":         func(r *pgIntervalReceipt) { r.Observation.QueryFailure = "pg query returned -11" },
		"no query":                       func(r *pgIntervalReceipt) { r.Observation.Query = nil },
		"peering":                        func(r *pgIntervalReceipt) { r.Observation.Query.State = "peering" },
		"remapped":                       func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+clean+remapped" },
		"scrubbing":                      func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+clean+scrubbing" },
		"primary's acting differs":       func(r *pgIntervalReceipt) { r.Observation.Query.Acting = []int32{3, 2, 1} },
		"primary's up differs":           func(r *pgIntervalReceipt) { r.Observation.Query.Up = []int32{3, 2, 1} },
		"answered for another shard":     func(r *pgIntervalReceipt) { r.Observation.Query.Info.PGID = "3.7s2" },
		"answered with another shard ID": func(r *pgIntervalReceipt) { r.Observation.Query.Info.Shard = 2 },
		"the same interval": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 24, 25
		},
		"an interval at the origin's epoch": func(r *pgIntervalReceipt) { r.Observation.Query.SameIntervalSince = 25 },
		"an interval after the placement": func(r *pgIntervalReceipt) {
			r.Observation.Query.SameIntervalSince, r.Observation.Query.LastEpochStarted = 29, 29
		},
		"no interval":                       func(r *pgIntervalReceipt) { r.Observation.Query.SameIntervalSince = 0 },
		"not started in its interval":       func(r *pgIntervalReceipt) { r.Observation.Query.LastEpochStarted = 25 },
		"primary behind the placement":      func(r *pgIntervalReceipt) { r.Observation.Query.Epoch = 27 },
		"origin's primary behind its epoch": func(r *pgIntervalReceipt) { r.OriginObservation.Query.Epoch = 24 },
	} {
		changed, _ := pgIntervalChange()
		edit(&changed)
		if err := checkPGInterval(changed, route); err == nil {
			t.Error("interval change accepted:", name)
		}
	}
	other := route
	other.PGID = "3.6"
	if err := checkPGInterval(receipt, other); err == nil {
		t.Error("interval change of another PG accepted")
	}
	// One observation after the command is enough.
	single, _ := pgIntervalChange()
	single.Observations = 1
	if err := checkPGInterval(single, route); err != nil {
		t.Error("interval change that settled at its first observation refused:", err)
	}
	// A placement that is unusable in itself is refused even when the
	// requests' placement and every record of the receipt agree on it.
	for name, edit := range map[string]func(*osdFixtureOracle){
		"under a pg_temp":                  func(p *osdFixtureOracle) { p.PGTemp = true },
		"up differs from acting":           func(p *osdFixtureOracle) { p.Up = []int32{3, 2, 1, 4} },
		"target is not the primary":        func(p *osdFixtureOracle) { p.ID = 2 },
		"replicated":                       func(p *osdFixtureOracle) { p.PoolType = 1 },
		"no shard":                         func(p *osdFixtureOracle) { p.Shard = -1 },
		"shard beyond the acting set":      func(p *osdFixtureOracle) { p.Shard = 4 },
		"primary is not on its shard":      func(p *osdFixtureOracle) { p.Shard = 2 },
		"primary is not in the acting set": func(p *osdFixtureOracle) { p.PrimaryID, p.ID = 5, 5 },
	} {
		changed, placement := pgIntervalChange()
		for _, record := range []*osdFixtureOracle{&placement, &changed.Origin, changed.OriginObservation.Before, changed.OriginObservation.After, changed.Observation.Before, changed.Observation.After} {
			edit(record)
		}
		for _, query := range []*pgIntervalQuery{changed.OriginObservation.Query, changed.Observation.Query} {
			query.Info.PGID, query.Info.Shard = "3.7s"+strconv.Itoa(int(placement.Shard)), int(placement.Shard)
		}
		if err := checkPGInterval(changed, placement); err == nil {
			t.Error("interval change on an unusable placement accepted:", name)
		}
		shown := pgIntervalReceipt{Action: "show", Observation: changed.Observation, Observations: 1, Settled: true}
		if err := checkPGIntervalShown(shown, placement); err == nil {
			t.Error("observation of an unusable placement accepted:", name)
		}
	}

	shown := func() pgIntervalReceipt {
		changed, _ := pgIntervalChange()
		return pgIntervalReceipt{Action: "show", Observation: changed.Observation, Observations: 1, Settled: true}
	}
	settled := route
	settled.Epoch = 28
	for _, placement := range []osdFixtureOracle{route, settled} {
		if err := checkPGIntervalShown(shown(), placement); err != nil {
			t.Errorf("clean observation at epoch 28 refused for the placement at epoch %d: %v", placement.Epoch, err)
		}
	}
	newer := route
	newer.Epoch = 29
	if err := checkPGIntervalShown(shown(), newer); err == nil {
		t.Error("observation older than the placement accepted")
	}
	for name, edit := range map[string]func(*pgIntervalReceipt){
		"a repeer receipt":      func(r *pgIntervalReceipt) { r.Action = "repeer" },
		"not settled":           func(r *pgIntervalReceipt) { r.Settled = false },
		"a recorded failure":    func(r *pgIntervalReceipt) { r.Failures = []string{"the PG has a pg_temp"} },
		"a command was sent":    func(r *pgIntervalReceipt) { r.CommandSent = true },
		"a command is recorded": func(r *pgIntervalReceipt) { r.Command = map[string]string{"prefix": "pg repeer", "pgid": "3.7"} },
		"under a pg_temp":       func(r *pgIntervalReceipt) { r.Observation.Before.PGTemp, r.Observation.After.PGTemp = true, true },
		"not clean":             func(r *pgIntervalReceipt) { r.Observation.Query.State = "active+recovering" },
		"on another primary": func(r *pgIntervalReceipt) {
			for _, p := range []*osdFixtureOracle{r.Observation.Before, r.Observation.After} {
				p.PrimaryID, p.ID, p.Shard = 1, 1, 2
			}
			r.Observation.Query.Info.PGID, r.Observation.Query.Info.Shard = "3.7s2", 2
		},
		"no placement":    func(r *pgIntervalReceipt) { r.Observation.Before = nil },
		"placement moved": func(r *pgIntervalReceipt) { r.Observation.After.Epoch = 29 },
		"no query":        func(r *pgIntervalReceipt) { r.Observation.Query = nil },
		"acting of the primary": func(r *pgIntervalReceipt) {
			r.Observation.Before.Acting, r.Observation.After.Acting = []int32{3}, []int32{3}
		},
	} {
		r := shown()
		edit(&r)
		if err := checkPGIntervalShown(r, route); err == nil {
			t.Error("observation accepted:", name)
		}
	}
}

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Backoffs of an optimized isa pool that the epoch-state controls in
// osd_erasure_epoch_state_test.go leave open: a PG-wide BLOCK from a primary
// that leads under a primary-first pg_temp, and an object-scope BLOCK of an
// object that a returned shard is missing. Both read the native op history
// for the ACK. Pinned source (Ceph v20.2.4):
//
//   - The OSD sends a BLOCK with its own spg_t (src/osd/PG.cc add_backoff,
//     add_pg_backoff), so a primary on actual shard 2 names shard 2 although
//     the request that drew it carried the shard in primary-first numbering.
//   - On a connection without SERVER_TENTACLE, which this client never
//     advertises, a fast-dispatch message for a shard other than 0 is routed
//     through dispatch_session_waiting, which reads its shard as primary-first
//     numbering and undoes it while the PG has a pg_temp
//     (src/osd/OSD.cc ms_fast_dispatch, dispatch_session_waiting;
//     src/osd/OSDMap.cc pgtemp_undo_primaryfirst). That applies to the ACK of
//     a BLOCK as well as to ops.
//   - An object op on an object degraded on an acting shard draws an object
//     BLOCK when osd_backoff_on_degraded is set (src/osd/PrimaryLogPG.cc
//     do_op), and the primary starts that object's recovery at once
//     (maybe_kick_recovery), which the norecover flag does not stop; the
//     recovered object's backoffs are released with UNBLOCK
//     (on_global_recover -> release_backoffs).
//
// The client neither follows the map nor resends; the test, as the caller,
// reads the native placement and decides. These helpers are test-only.

// ecBackoffAck is one ACK_BLOCK entry of an OSD's op history
// (tools/osd_pool_mode_backoffacks_oracle.py).
type ecBackoffAck struct {
	PGID      string   `json:"pgid"`
	Shard     int      `json:"shard"`
	ID        uint64   `json:"id"`
	Begin     string   `json:"begin"`
	End       string   `json:"end"`
	MapEpoch  uint32   `json:"map_epoch"`
	Client    string   `json:"client"`
	ReachedPG bool     `json:"reached_pg"`
	Done      bool     `json:"done"`
	Events    []string `json:"events"`
}

// backoffAck reads the op history of osd until it holds the ACK of block
// from client gid, within a bounded number of reads, and returns it. A
// history without it after the bound fails the test: the history is
// bounded, so that is not shown absent, only not observed.
func (r *ecEpochRun) backoffAck(object string, osd int32, block cephmsgr.OSDBackoffEvent, gid uint64) ecBackoffAck {
	r.t.Helper()
	client := "client." + strconv.FormatUint(gid, 10)
	var seen []ecBackoffAck
	for attempt := 1; attempt <= 20; attempt++ {
		var result struct {
			Acks struct {
				OSD  int32          `json:"osd"`
				PGID string         `json:"pgid"`
				Acks []ecBackoffAck `json:"acks"`
			} `json:"backoff_acks"`
		}
		nativePoolResult(r.t, r.ctx, "backoffacks", map[string]any{"slot": 1, "object": object, "osd": osd}, &result)
		seen = result.Acks.Acks
		for _, a := range seen {
			if a.ID == block.ID && a.Client == client && a.MapEpoch == block.MapEpoch && a.PGID == fmt.Sprintf("%d.%x", block.Pool, block.Seed) {
				r.t.Logf("osd.%d op history holds the ACK: %+v (read %d)", osd, a, attempt)
				return a
			}
		}
		time.Sleep(time.Second)
	}
	r.t.Fatalf("osd.%d op history never held the ACK of BLOCK %d (epoch %d) from %s: %+v", osd, block.ID, block.MapEpoch, client, seen)
	return ecBackoffAck{}
}

// peeredUnder requires one read-only observation to show the PG peered and
// not active under origin's primary, which leads under a primary-first
// pg_temp, with the members of the given acting positions gone as well: two
// identical placement records around a query that the unchanged primary
// answered for its own shard; the placement is origin except for a newer
// epoch and those positions empty in the up and acting sets, still under the
// pg_temp; the primary reports peered and undersized, optionally degraded,
// with those sets, in an interval that started after since and not after the
// placement's epoch, at a map epoch not behind the placement's.
func (o pgIntervalObservation) peeredUnder(origin osdFixtureOracle, holes []int, since uint32) error {
	if !origin.PGTemp || origin.ID != origin.PrimaryID || origin.Shard < 0 || int(origin.Shard) >= len(origin.Acting) || origin.Acting[origin.Shard] != origin.PrimaryID || since == 0 {
		return fmt.Errorf("%+v, with an interval since epoch %d, is not a PG led by its primary under a pg_temp", origin, since)
	}
	left := 0
	for _, osd := range origin.Acting {
		if osd != crushItemNone {
			left++
		}
	}
	if len(holes) == 0 || left-len(holes) >= origin.MinSize {
		return fmt.Errorf("%d more empty positions of %d members leave the PG at or above min_size %d", len(holes), left, origin.MinSize)
	}
	for i, position := range holes {
		if position < 0 || position >= len(origin.Acting) || position == int(origin.Shard) || origin.Acting[position] == crushItemNone || slices.Contains(holes[:i], position) {
			return fmt.Errorf("the empty positions %v are not distinct members other than the primary's %d of %v", holes, origin.Shard, origin.Acting)
		}
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	holed := holedSet(origin.Acting, holes...)
	want := origin
	want.Up, want.Acting, want.Epoch = holed, slices.Clone(holed), o.Before.Epoch
	if !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not %+v at a newer epoch with positions %v empty", *o.Before, origin, holes)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"peered", "undersized"}) && !slices.Equal(states, []string{"degraded", "peered", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not a peered undersized PG", q.State)
	}
	if !slices.Equal(q.Up, holed) || !slices.Equal(q.Acting, holed) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the primary reports up %v and acting %v for %s; want %v for shard %d", q.Up, q.Acting, q.Info.PGID, holed, origin.Shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.Epoch, since, o.Before.Epoch)
	}
	return nil
}

// activeUnder requires one read-only observation to show the PG active and
// undersized, optionally degraded, again under origin's primary on origin's
// placement, which leads under a primary-first pg_temp, at a newer epoch, in
// an interval that started after since.
func (o pgIntervalObservation) activeUnder(origin osdFixtureOracle, since uint32) error {
	if !origin.PGTemp || since == 0 {
		return fmt.Errorf("%+v, with an interval since epoch %d, is not a PG under a pg_temp", origin, since)
	}
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	if !samePlacement(*o.Before, origin) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not %+v at a newer epoch", *o.Before, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"active", "undersized"}) && !slices.Equal(states, []string{"active", "degraded", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not an active undersized PG", q.State)
	}
	if !slices.Equal(q.Up, origin.Acting) || !slices.Equal(q.Acting, origin.Acting) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the primary reports up %v and acting %v for %s; want %v for shard %d", q.Up, q.Acting, q.Info.PGID, origin.Acting, origin.Shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince || q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d and the PG at %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, q.Epoch, since, o.Before.Epoch)
	}
	return nil
}

// degradedOn requires one read-only observation to show the PG active and
// degraded, and not undersized, on origin's complete placement under the
// unchanged primary, without a pg_temp, in an interval that started after
// since: every shard's OSD is back, and the returned one is missing objects
// that the norecover and nobackfill flags keep it from recovering.
func (o pgIntervalObservation) degradedOn(origin osdFixtureOracle, since uint32) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	want := origin
	want.Epoch = o.Before.Epoch
	if origin.PGTemp || slices.Contains(origin.Acting, crushItemNone) || !reflect.DeepEqual(*o.Before, want) || o.Before.Epoch <= origin.Epoch {
		return fmt.Errorf("the placement %+v is not the complete recorded one at a newer epoch: %+v", *o.Before, origin)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	// With norecover the PG may hold its recovery reservation and report
	// recovering while the OSD's recovery work does nothing for it
	// (src/osd/OSD.cc activate_map -> OSDService::pause_recovery).
	if states := pgStates(q.State); !slices.Equal(states, []string{"active", "degraded"}) && !slices.Equal(states, []string{"active", "degraded", "recovery_wait"}) &&
		!slices.Equal(states, []string{"active", "degraded", "recovering"}) {
		return fmt.Errorf("the primary reports %q, not an active degraded PG waiting for recovery", q.State)
	}
	if !slices.Equal(q.Up, origin.Acting) || !slices.Equal(q.Acting, origin.Acting) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(int(origin.Shard)) || q.Info.Shard != int(origin.Shard) {
		return fmt.Errorf("the primary reports up %v and acting %v for %s; want %v for shard %d", q.Up, q.Acting, q.Info.PGID, origin.Acting, origin.Shard)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > o.Before.Epoch || q.LastEpochStarted < q.SameIntervalSince || q.Epoch < o.Before.Epoch {
		return fmt.Errorf("the interval started at epoch %d and the PG at %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d",
			q.SameIntervalSince, q.LastEpochStarted, q.Epoch, since, o.Before.Epoch)
	}
	return nil
}

// checkObjectBackoff requires a native backoff event to be the object-scope
// one of route's head object on its primary's actual shard: begin and end
// are the same head object of route's pool, hash and name.
func checkObjectBackoff(event cephmsgr.OSDBackoffEvent, op uint8, route osdFixtureOracle, globalID uint64) error {
	if event.Op != op || event.Pool != uint64(route.Pool) || event.Seed != route.PG || event.Shard != route.Shard || event.ID == 0 ||
		event.Source.ID != route.ID || event.GlobalID != globalID || event.SessionGeneration == 0 || len(event.RawFront) == 0 {
		return fmt.Errorf("event op %d pg %d.%x shard %d id %d from osd.%d gid %d is not op %d for %s shard %d from osd.%d gid %d",
			event.Op, event.Pool, event.Seed, event.Shard, event.ID, event.Source.ID, event.GlobalID, op, route.PGID, route.Shard, route.ID, globalID)
	}
	for _, c := range []cephmsgr.OSDPGCursor{event.Begin, event.End} {
		if c.Max || c.Pool != route.Pool || c.Hash != route.Hash || c.Object != route.Name || c.Key != "" || c.Namespace != "" || c.Snapshot != cephmsgr.OSDHeadSnapshot {
			return fmt.Errorf("the range bound %+v is not the head of %s (hash %#x)", c, route.Name, route.Hash)
		}
	}
	if !bytes.Equal(event.Begin.Raw, event.End.Raw) {
		return errors.New("the range does not begin and end at the same object")
	}
	return nil
}

// heldCall runs one request on its own goroutine, so that the test can take
// the backoff event the request draws while the call waits.
type heldCall struct {
	finished chan heldResult
	cancel   context.CancelFunc
}

type heldResult struct {
	reply cephmsgr.OSDReply
	err   error
}

func startHeldCall(ctx context.Context, o *cephmsgr.OSDConnection, request cephmsgr.OSDRequest) heldCall {
	callCtx, cancel := context.WithCancel(ctx)
	h := heldCall{finished: make(chan heldResult, 1), cancel: cancel}
	go func() {
		reply, err := o.Request(callCtx, request)
		h.finished <- heldResult{reply, err}
	}()
	return h
}

// block takes the next backoff event of o within 30 seconds while the call
// is unanswered.
func (h heldCall) block(t *testing.T, ctx context.Context, o *cephmsgr.OSDConnection) cephmsgr.OSDBackoffEvent {
	t.Helper()
	eventCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	event, err := o.NextBackoff(eventCtx)
	if err != nil {
		select {
		case early := <-h.finished:
			t.Fatal("no native BLOCK; the request ended", early.reply.Code, early.err, err)
		default:
			t.Fatal("no native BLOCK", err)
		}
	}
	select {
	case early := <-h.finished:
		t.Fatal("the request that drew the BLOCK was answered", early.reply.Code, early.err)
	default:
	}
	return event
}

// end cancels the local wait of a call nothing answers and returns its
// result, which must be an unknown outcome under the original identity.
func (h heldCall) end(t *testing.T, gid uint64, incarnation uint32, route osdFixtureOracle) cephmsgr.OSDSubmission {
	t.Helper()
	h.cancel()
	lost := <-h.finished
	var unknown *cephmsgr.OutcomeUnknownError
	sub := lost.reply.Submission
	if !errors.As(lost.err, &unknown) || !errors.Is(lost.err, context.Canceled) || sub.Transaction == 0 || sub.GlobalID != gid || sub.ClientIncarnation != incarnation ||
		sub.MapEpoch != route.Epoch || sub.Placement == nil || sub.Placement.Primary != route.ID || int8(sub.Placement.Shard) != route.Shard {
		t.Fatal("the dropped request did not keep its unknown outcome and original identity", lost.err, sub)
	}
	return sub
}

// refusedLocally requires request to be refused by the connection's backoff
// gate as a known unsent request.
func refusedLocally(t *testing.T, ctx context.Context, o *cephmsgr.OSDConnection, label string, request cephmsgr.OSDRequest) {
	t.Helper()
	reply, err := o.Request(ctx, request)
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.Is(err, cephmsgr.ErrOSDBackoff) || errors.As(err, &unknown) || reply.Transaction != 0 || len(reply.RawFront) != 0 ||
		reply.Submission.Transaction == 0 || !reflect.DeepEqual(reply.Submission.Placement, request.Placement) {
		t.Fatal(label, "was not refused locally as an unsent request", reply.Transaction, reply.Submission, err)
	}
	p := reply.Submission.Placement
	t.Logf("%s refused locally: tid %d, pg %d.%x shard %d primary-first %+v", label, reply.Submission.Transaction, p.PG.Pool, p.PG.Seed, p.Shard, p.PrimaryFirst)
}

// erasureReplacementPGBackoffScenario: a PG-wide backoff from a primary
// that leads under a primary-first pg_temp. The shard-0 primary's OSD is
// killed; once the PG is active under the first parity shard's OSD with the
// pg_temp, the OSD of the last parity shard is killed too, which leaves the
// nonprimary data shards and the replacement's shard, k shards, below
// min_size k+1. One Go APPEND+SETXATTR sent to the replacement with
// primary-first numbering must draw a BLOCK for the whole PG that names the
// replacement's actual shard, not the request's wire shard. After the
// caller's ACK, requests for the PG on that connection, also one naming the
// actual shard without primary-first numbering, are refused locally. The
// ACK does not reach the PG: the replacement's op history must hold it as
// received from this client with the BLOCK's PG, shard, ID and epoch, queued
// and never taken by the PG, because the OSD reads its shard as primary-first
// numbering on this connection (see the file comment and AckBackoff).
// Starting the last parity shard's OSD ends the interval: the replacement
// must send the matching UNBLOCK and the PG must be active under it again.
// The dropped APPEND keeps its unknown outcome with the native object
// unchanged. A STAT for the PG on the same connection then gets no answer
// within 20 seconds, as the session still holds the unacknowledged
// restriction. The caller closes that connection and resubmits the APPEND
// once on a new one, under the original identity, and it applies once. After
// the original primary returns and every PG is clean, the native state is
// unchanged. Not verified: a BLOCK from shard 0 under a pg_temp, and an ACK
// that a later map without the pg_temp would route.
//
// The replacement needs a second parity shard to lose, so m must be at
// least 2: on an optimized k=3,m=1 pool the replacement is the only parity
// shard and min_size is k (erasurePGBackoffScenario), and there is no row.
func erasureReplacementPGBackoffScenario(t *testing.T, width erasureWidth) {
	r := startECEpochRun(t, width, 900*time.Second)
	const (
		object          = "ec-replaced-pg-backoff"
		attr            = "ec-backoff"
		heldIncarnation = 51
		readIncarnation = 52
		seeded          = 1700010000
		mutated         = 1700010001
	)
	route, _ := r.healthy(object)
	seed := bytes.Repeat([]byte{0x52, 0, 0xff, 'P'}, 1500)
	r.write("seed", route, seed, seeded, cephmsgr.OSDSnapContext{})
	route, since := r.healthy(object)
	pre := poolObserve(t, r.ctx, 1, object, "", "")
	if pre.Code != 0 || !bytes.Equal(pre.Data, seed) || pre.Seconds != seeded || xattrInventory(pre)[attr] != "" {
		t.Fatal("native pre-state differs", pre.Code, pre.Version, pre.Seconds)
	}
	last := r.facts.size - 1
	if route.Shard != 0 || !slices.Equal(r.facts.nonprimary, width.nonprimaryShards(true)) || width.m < 2 || r.facts.minSize != width.k+1 {
		t.Fatalf("the placement %+v with nonprimary shards %v and min_size %d is not led by shard 0 of %s with two parity shards", route, r.facts.nonprimary, r.facts.minSize, width)
	}
	r.heldThrough("the pre-state read", object, route)

	primary := r.kill(route.ID)
	fresh, replacedSince := r.awaitReplacement(object, route, since)
	if fresh.Shard != int8(r.facts.size-width.m) || routePrimaryFirst(fresh) == nil {
		t.Fatalf("the replacement %+v is not the first parity shard's OSD under a primary-first pg_temp", fresh)
	}
	// The request's wire shard is the actual shard's position in the
	// primary-first order (pinned OSDMap::pgtemp_primaryfirst).
	wire := slices.Index(primaryFirstOrder(r.facts.size, r.facts.nonprimary), int(fresh.Shard))
	if wire < 0 || wire == int(fresh.Shard) {
		t.Fatalf("actual shard %d has wire shard %d in %v", fresh.Shard, wire, primaryFirstOrder(r.facts.size, r.facts.nonprimary))
	}
	t.Logf("replacement osd.%d on actual shard %d, wire shard %d under %+v, epoch %d, interval since %d", fresh.ID, fresh.Shard, wire, routePrimaryFirst(fresh), fresh.Epoch, replacedSince)

	client := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer client.Close()
	gid := client.Snapshot().GlobalID
	o := mutationConnection(t, client.Client, r.ctx, fresh)
	defer func() { o.Close() }()
	probe := r.request(fresh, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	probe.ClientIncarnation = readIncarnation
	if reply, err := o.Request(r.ctx, probe); err != nil || reply.Code != 0 {
		t.Fatal("STAT through the replacement before the second outage", err, reply.Code)
	}
	r.heldThrough("the STAT through the replacement", object, fresh)

	parity := r.kill(route.Acting[last])
	peered := r.waitState("peered under the replacement", object, func(o pgIntervalObservation) error {
		return o.peeredUnder(fresh, []int{last}, replacedSince)
	})
	now := *peered.After

	tail := []byte("replaced-backoff\x00\xff" + object)
	value := []byte{0, 0xff, 'r'}
	ops := []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
	}
	held := r.request(now, ops...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	if held.Placement.PrimaryFirst == nil || int8(held.Placement.Shard) != fresh.Shard {
		t.Fatal("the request does not carry the replacement's shard with primary-first numbering", held.Placement)
	}
	call := startHeldCall(r.ctx, o, held)
	block := call.block(t, r.ctx, o)
	if err := checkPGBackoff(block, cephmsgr.OSDBackoffBlock, now, r.pgNum, gid); err != nil {
		t.Fatal("native BLOCK:", err)
	}
	if block.MapEpoch < peered.Query.Epoch || block.Matched {
		t.Fatalf("BLOCK at epoch %d (matched %v); the peered primary was at epoch %d", block.MapEpoch, block.Matched, peered.Query.Epoch)
	}
	t.Logf("BLOCK id %d for pg %d.%x actual shard %d (request wire shard %d) at epoch %d, range [%+v, %+v)", block.ID, block.Pool, block.Seed, block.Shard, wire, block.MapEpoch, block.Begin, block.End)
	if err := o.AckBackoff(r.ctx, block); err != nil {
		t.Fatal("explicit ACK of the BLOCK", err)
	}
	read := r.request(now, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
	read.ClientIncarnation = readIncarnation
	refusedLocally(t, r.ctx, o, "READ of the blocked object with primary-first numbering", read)
	actual := r.request(now, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	actual.ClientIncarnation = readIncarnation
	actual.Placement.PrimaryFirst = nil
	refusedLocally(t, r.ctx, o, "STAT naming the actual shard without primary-first numbering", actual)
	// The ACK carries the BLOCK's actual shard. The OSD reads it as
	// primary-first numbering on this connection and undoes that while the
	// pg_temp lasts (k=2,m=2: shard 2 -> 3; k=4,m=2: shard 4 -> 2), and
	// drops it: this OSD holds no such shard of the PG. Its history shows
	// it received and never reached the PG.
	undone := primaryFirstOrder(r.facts.size, r.facts.nonprimary)[fresh.Shard]
	if undone == int(fresh.Shard) {
		t.Fatalf("actual shard %d is its own position in the primary-first order %v", fresh.Shard, primaryFirstOrder(r.facts.size, r.facts.nonprimary))
	}
	t.Logf("the ACK's shard %d read as primary-first numbering is actual shard %d, held by osd.%d", fresh.Shard, undone, route.Acting[undone])
	ack := r.backoffAck(object, fresh.ID, block, gid)
	if ack.Shard != int(fresh.Shard) || ack.ReachedPG || !ack.Done || !slices.Contains(ack.Events, "queued_for_pg") {
		t.Fatalf("the ACK in the replacement's op history %+v is not the received ACK of shard %d that never reached the PG", ack, fresh.Shard)
	}
	r.heldThrough("the BLOCK, the refusals and the ACK in the peered interval", object, now)

	r.start(parity)
	releaseCtx, stopRelease := context.WithTimeout(r.ctx, 180*time.Second)
	unblock, err := o.NextBackoff(releaseCtx)
	stopRelease()
	if err != nil {
		t.Fatal("no native UNBLOCK after the interval change", err)
	}
	if err := checkPGBackoff(unblock, cephmsgr.OSDBackoffUnblock, now, r.pgNum, gid); err != nil || !unblock.Matched || unblock.ID != block.ID ||
		unblock.SessionGeneration != block.SessionGeneration || unblock.MapEpoch <= block.MapEpoch ||
		!bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) || !bytes.Equal(unblock.End.Raw, block.End.Raw) {
		t.Fatalf("native UNBLOCK %+v does not release BLOCK %d of epoch %d: %v", unblock, block.ID, block.MapEpoch, err)
	}
	t.Logf("UNBLOCK id %d at epoch %d (BLOCK at %d), matched", unblock.ID, unblock.MapEpoch, block.MapEpoch)
	active := r.waitState("active again under the replacement", object, func(o pgIntervalObservation) error {
		return o.activeUnder(fresh, peered.Query.SameIntervalSince)
	})
	// The first interval change releases the BLOCK; the map may pass through
	// further intervals (a remapped one while the pg_temp is renewed) before
	// the PG is active again.
	if unblock.MapEpoch <= peered.Query.SameIntervalSince || unblock.MapEpoch > active.Query.SameIntervalSince {
		t.Fatalf("the UNBLOCK of epoch %d is not from an interval after the peered one (since %d) and not after the active one (since %d)",
			unblock.MapEpoch, peered.Query.SameIntervalSince, active.Query.SameIntervalSince)
	}

	sub := call.end(t, gid, heldIncarnation, now)
	n := poolObserve(t, r.ctx, 1, object, "", "")
	if n.Code != 0 || n.Version != pre.Version || !bytes.Equal(n.Data, seed) || n.Seconds != seeded || n.Nanos != pre.Nanos || !maps.Equal(xattrInventory(n), xattrInventory(pre)) {
		t.Fatal("native state after the UNBLOCK differs from the pre-state", n.Version, pre.Version, len(n.Data), n.Seconds)
	}
	t.Logf("APPEND tid %d (incarnation %d, epoch %d) ended unknown; native object unchanged at version %d", sub.Transaction, sub.ClientIncarnation, sub.MapEpoch, n.Version)

	// The ACK never reached the PG, so the session still holds the BLOCK,
	// now unblocked but not acknowledged (STATE_DELETING), and
	// Session::check_backoff drops every request for the PG on this
	// connection without an answer. A read shows it; a bounded wait cannot
	// show that no answer will ever come, only that none came.
	current := *active.After
	stale := r.request(current, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	stale.ClientIncarnation = readIncarnation
	staleCtx, stopStale := context.WithTimeout(r.ctx, 20*time.Second)
	dropped, err := o.Request(staleCtx, stale)
	stopStale()
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, context.DeadlineExceeded) || dropped.Submission.Transaction == 0 {
		t.Fatal("a STAT for the PG on the connection whose ACK was lost was answered or not sent", dropped.Code, dropped.Submission, err)
	}
	t.Logf("STAT tid %d on the same connection after the UNBLOCK: no answer in 20 s (%v)", dropped.Submission.Transaction, err)

	// The caller's explicit decisions: close the connection, which ends the
	// OSD's session and its backoffs (OSD::ms_handle_reset ->
	// Session::clear_backoffs), and resubmit the same operations once under
	// the original identity on a new one, routed by the current placement.
	o.Close()
	o = mutationConnection(t, client.Client, r.ctx, current)
	retry := r.request(current, ops...)
	retry.ClientIncarnation = heldIncarnation
	retry.ModificationTime = held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	retryCtx, stopRetry := context.WithTimeout(r.ctx, 30*time.Second)
	reply, err := o.Request(retryCtx, retry)
	stopRetry()
	if err != nil {
		t.Fatal("explicit resubmission on a new connection", err)
	}
	if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= pre.Version ||
		reply.Submission.GlobalID != gid || reply.Submission.ClientIncarnation != heldIncarnation {
		t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	want := append(bytes.Clone(seed), tail...)
	final := poolObserve(t, r.ctx, 1, object, "", "")
	if final.Code != 0 || !bytes.Equal(final.Data, want) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion || final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	r.heldThrough("the unanswered STAT and the resubmission in the active interval under the replacement", object, current)

	back, _ := r.primaryBack(object, route, primary, active.Query.SameIntervalSince)
	recovered := r.observeIn("the native read at the returned primary", object, back)
	if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || recovered.Seconds != final.Seconds || !maps.Equal(xattrInventory(recovered), xattrInventory(final)) {
		t.Fatal("native state after the return differs", recovered.Version, final.Version)
	}
	t.Logf("primary osd.%d back at %s; native object version %d", back.ID, back.Address, recovered.Version)
}

// erasureObjectBackoffScenario: an object-scope backoff on an optimized EC
// PG. With osd_backoff_on_degraded set in the disposable cluster, the OSD of
// the last parity shard is killed, the object is overwritten through the
// unchanged shard-0 primary while it is away, and the norecover and
// nobackfill flags keep the returned shard from recovering it: the PG is
// active and degraded on its full placement. One Go APPEND+SETXATTR of that
// object must draw a BLOCK for exactly that head object on the primary's
// actual shard. Before the ACK, a READ of the object on that connection is
// refused locally while a STAT of another object of the same PG completes.
// The primary recovers the object it backed off, and must send the matching
// UNBLOCK, with the flags still set; its op history must hold the ACK, which
// reached the PG. The dropped APPEND keeps its unknown outcome with the
// native object unchanged, and one explicit resubmission applies it once.
// The flags and the setting are restored and every PG is clean again.
func erasureObjectBackoffScenario(t *testing.T, width erasureWidth) {
	r := startECEpochRun(t, width, 900*time.Second)
	const (
		object          = "ec-object-backoff"
		attr            = "ec-object-backoff"
		heldIncarnation = 61
		readIncarnation = 62
		seeded          = 1700011000
		degradedAt      = 1700011001
		mutated         = 1700011002
	)
	route, _ := r.healthy(object)
	other := ""
	for i := range 64 {
		name := object + "-other-" + strconv.Itoa(i)
		if p := r.place(name); p.PGID == route.PGID {
			other = name
			break
		}
	}
	if other == "" {
		t.Fatal("no name of 64 candidates maps to the PG of", object, route.PGID)
	}
	seed := bytes.Repeat([]byte{0x4f, 0, 0xff, 'O'}, 1500)
	r.write("seed", route, seed, seeded, cephmsgr.OSDSnapContext{})
	otherData := []byte("another object of the same PG\x00\xff")
	r.write("seed the other object", r.place(other), otherData, seeded, cephmsgr.OSDSnapContext{})
	route, since := r.healthy(object)

	enabled := false
	r.t.Cleanup(func() {
		if !enabled {
			return
		}
		var result struct {
			Setting struct {
				Restored bool `json:"restored"`
				Claimed  bool `json:"claimed"`
			} `json:"backoff_degraded"`
		}
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		nativePoolResult(t, cleanup, "backoffdegraded", map[string]any{"action": "restore"}, &result)
		if result.Setting.Claimed && !result.Setting.Restored {
			t.Error("osd_backoff_on_degraded was not restored")
		}
	})
	enabled = true
	var setting struct {
		Setting struct {
			Prior *string `json:"prior"`
			Value *string `json:"value"`
		} `json:"backoff_degraded"`
	}
	nativePoolResult(t, r.ctx, "backoffdegraded", map[string]any{"action": "enable"}, &setting)
	if setting.Setting.Value == nil || *setting.Setting.Value != "true" {
		t.Fatal("osd_backoff_on_degraded was not set", setting.Setting.Value)
	}
	t.Logf("osd_backoff_on_degraded set (prior entry %v)", setting.Setting.Prior)

	last := r.facts.size - 1
	parity := r.kill(route.Acting[last])
	absent := r.waitState("active without the last parity shard", object, func(o pgIntervalObservation) error { return o.activeWithout(route, []int{last}, since) })
	degraded := bytes.Repeat([]byte("written while shard "+strconv.Itoa(last)+" is away\x01"), 120)
	written := r.write("overwrite while the shard is away", *absent.After, degraded, degradedAt, cephmsgr.OSDSnapContext{})
	r.heldThrough("the overwrite while the shard is away", object, *absent.After)

	flagged := false
	// Registered after the run's own cleanups, so it runs before their
	// recovery, which waits for clean PGs.
	r.t.Cleanup(func() {
		if flagged {
			cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			nativePool(t, cleanup, "osdflags", map[string]any{"action": "unset"})
		}
	})
	flagged = true
	flags := nativePool(t, r.ctx, "osdflags", map[string]any{"action": "set"})
	if !slices.Contains(flags.OSDMapFlags, "norecover") || !slices.Contains(flags.OSDMapFlags, "nobackfill") {
		t.Fatal("recovery flags were not set", flags.OSDMapFlags)
	}
	r.start(parity)
	seen := r.waitState("degraded on the returned shard", object, func(o pgIntervalObservation) error { return o.degradedOn(route, absent.Query.SameIntervalSince) })
	now := *seen.After
	pre := poolObserve(t, r.ctx, 1, object, "", "")
	if pre.Code != 0 || !bytes.Equal(pre.Data, degraded) || pre.Version != written.UserVersion || xattrInventory(pre)[attr] != "" {
		t.Fatal("native state before the BLOCK differs", pre.Code, pre.Version, written.UserVersion)
	}
	otherRoute := r.place(other)
	if otherRoute.PGID != now.PGID || otherRoute.ID != now.ID || otherRoute.Shard != now.Shard {
		t.Fatal("the other object left the PG or its primary", otherRoute, now)
	}

	client := dialErasureFaultClient(t, r.ctx, r.hash, "")
	defer client.Close()
	gid := client.Snapshot().GlobalID
	o := mutationConnection(t, client.Client, r.ctx, now)
	defer o.Close()
	tail := []byte("object-backoff\x00\xff" + object)
	value := []byte{0, 0xff, 'o'}
	ops := []cephmsgr.OSDOperation{
		{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail},
		{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: attr}, Data: value},
	}
	held := r.request(now, ops...)
	held.ClientIncarnation = heldIncarnation
	held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	call := startHeldCall(r.ctx, o, held)
	block := call.block(t, r.ctx, o)
	if err := checkObjectBackoff(block, cephmsgr.OSDBackoffBlock, now, gid); err != nil {
		t.Fatal("native BLOCK:", err)
	}
	if block.MapEpoch < now.Epoch || block.Matched {
		t.Fatalf("BLOCK at epoch %d (matched %v); the placement is at epoch %d", block.MapEpoch, block.Matched, now.Epoch)
	}
	t.Logf("BLOCK id %d for %s in pg %d.%x shard %d at epoch %d", block.ID, block.Begin.Object, block.Pool, block.Seed, block.Shard, block.MapEpoch)

	// Before the ACK: the blocked object is refused, another object of the
	// same PG is not.
	read := r.request(now, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
	read.ClientIncarnation = readIncarnation
	refusedLocally(t, r.ctx, o, "READ of the blocked object", read)
	stat := r.request(otherRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	stat.ClientIncarnation = readIncarnation
	if reply, err := o.Request(r.ctx, stat); err != nil || reply.Code != 0 || len(reply.Operations) != 1 {
		t.Fatal("STAT of another object of the blocked PG", err, reply.Code)
	}
	if err := o.AckBackoff(r.ctx, block); err != nil {
		t.Fatal("explicit ACK of the BLOCK", err)
	}
	releaseCtx, stopRelease := context.WithTimeout(r.ctx, 120*time.Second)
	unblock, err := o.NextBackoff(releaseCtx)
	stopRelease()
	if err != nil {
		t.Fatal("no native UNBLOCK while the recovery flags are set", err)
	}
	if err := checkObjectBackoff(unblock, cephmsgr.OSDBackoffUnblock, now, gid); err != nil || !unblock.Matched || unblock.ID != block.ID ||
		unblock.SessionGeneration != block.SessionGeneration || !bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) {
		t.Fatalf("native UNBLOCK %+v does not release BLOCK %d: %v", unblock, block.ID, err)
	}
	t.Logf("UNBLOCK id %d at epoch %d (BLOCK at %d), matched", unblock.ID, unblock.MapEpoch, block.MapEpoch)
	ack := r.backoffAck(object, now.ID, block, gid)
	if ack.Shard != int(now.Shard) || ack.Begin != ack.End || !strings.Contains(ack.Begin, ":"+object+":") || !ack.ReachedPG {
		t.Fatalf("the ACK in the primary's op history %+v is not the object ACK of shard %d that reached the PG", ack, now.Shard)
	}

	sub := call.end(t, gid, heldIncarnation, now)
	n := poolObserve(t, r.ctx, 1, object, "", "")
	if n.Code != 0 || n.Version != pre.Version || !bytes.Equal(n.Data, degraded) || n.Seconds != pre.Seconds || !maps.Equal(xattrInventory(n), xattrInventory(pre)) {
		t.Fatal("native state after the UNBLOCK differs", n.Version, pre.Version, len(n.Data))
	}
	retry := r.request(now, ops...)
	retry.ClientIncarnation = heldIncarnation
	retry.ModificationTime = held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	retryCtx, stopRetry := context.WithTimeout(r.ctx, 30*time.Second)
	reply, err := o.Request(retryCtx, retry)
	stopRetry()
	if err != nil || reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= pre.Version {
		t.Fatal("explicit resubmission after the UNBLOCK", err, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	want := append(bytes.Clone(degraded), tail...)
	final := poolObserve(t, r.ctx, 1, object, "", "")
	if final.Code != 0 || !bytes.Equal(final.Data, want) || bytes.Count(final.Data, tail) != 1 || final.Version != reply.UserVersion || final.Seconds != mutated || xattrInventory(final)[attr] != string(value) {
		t.Fatal("native state after the resubmission differs", len(final.Data), final.Version, reply.UserVersion, final.Seconds)
	}
	t.Logf("resubmission tid %d applied once: version %d -> %d", reply.Transaction, pre.Version, final.Version)

	nativePool(t, r.ctx, "osdflags", map[string]any{"action": "unset"})
	flagged = false
	r.clean()
	// The primary never stopped and recovery ends no interval.
	clean := r.waitState("clean again", object, func(o pgIntervalObservation) error {
		if err := o.cleanOn(); err != nil {
			return err
		}
		if !samePlacement(*o.Before, route) || o.Query.SameIntervalSince != seen.Query.SameIntervalSince {
			return fmt.Errorf("the placement %+v in the interval since %d is not %+v in the interval since %d", *o.Before, o.Query.SameIntervalSince, route, seen.Query.SameIntervalSince)
		}
		return nil
	})
	recovered := r.observeIn("the native read on the clean placement", object, *clean.After)
	if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || !maps.Equal(xattrInventory(recovered), xattrInventory(final)) {
		t.Fatal("native state after recovery differs", recovered.Version, final.Version)
	}
	var restored struct {
		Setting struct {
			Restored bool `json:"restored"`
		} `json:"backoff_degraded"`
	}
	nativePoolResult(t, r.ctx, "backoffdegraded", map[string]any{"action": "restore"}, &restored)
	enabled = false
	if !restored.Setting.Restored {
		t.Fatal("osd_backoff_on_degraded was not restored")
	}
}

// TestCephOSDErasureOptimizedK2M2ReplacementPGBackoffIntegration is erasureReplacementPGBackoffScenario on
// optimized isa k=2,m=2 (four OSDs; the replacement leads on shard 2).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2replacementpgbackoff key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills two OSDs; restart waits osd-ready clean, which does not prove no pg_temp
func TestCephOSDErasureOptimizedK2M2ReplacementPGBackoffIntegration(t *testing.T) {
	erasureReplacementPGBackoffScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK4M2ReplacementPGBackoffIntegration is erasureReplacementPGBackoffScenario on
// optimized isa k=4,m=2 (six OSDs; nonprimary shards {1,2,3}; the replacement leads on shard 4).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2replacementpgbackoff key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated kills two OSDs; restart waits osd-ready clean, which does not prove no pg_temp
func TestCephOSDErasureOptimizedK4M2ReplacementPGBackoffIntegration(t *testing.T) {
	erasureReplacementPGBackoffScenario(t, erasureK4M2)
}

// TestCephOSDErasureOptimizedK2M2ObjectBackoffIntegration is erasureObjectBackoffScenario on optimized isa k=2,m=2
// (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2objectbackoff key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills an OSD, sets norecover/nobackfill and osd_backoff_on_degraded; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2ObjectBackoffIntegration(t *testing.T) {
	erasureObjectBackoffScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1ObjectBackoffIntegration is erasureObjectBackoffScenario on optimized isa k=3,m=1
// (four OSDs; nonprimary shards {1,2}; three shards stay, min_size 3).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1objectbackoff key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated kills an OSD, sets norecover/nobackfill and osd_backoff_on_degraded; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK3M1ObjectBackoffIntegration(t *testing.T) {
	erasureObjectBackoffScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2ObjectBackoffIntegration is erasureObjectBackoffScenario on optimized isa k=4,m=2
// (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2objectbackoff key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated kills an OSD, sets norecover/nobackfill and osd_backoff_on_degraded; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2ObjectBackoffIntegration(t *testing.T) {
	erasureObjectBackoffScenario(t, erasureK4M2)
}

// replacedState builds one synthetic observation of PG 3.7 under osd.1 on
// actual shard 2 with a primary-first pg_temp (pgReplacedObservation's
// placement), at the given epoch and state with the given further positions
// empty, in an interval that started one epoch earlier. No daemon produced
// it.
func replacedState(epoch uint32, state string, holes ...int) (pgIntervalObservation, osdFixtureOracle) {
	receipt, _ := pgReplacedObservation()
	origin := *receipt.Observation.Before
	holed := holedSet(origin.Acting, holes...)
	before := origin
	before.Up, before.Acting, before.Epoch = slices.Clone(holed), slices.Clone(holed), epoch
	after := before
	after.Up, after.Acting = slices.Clone(holed), slices.Clone(holed)
	return pgIntervalObservation{Before: &before, After: &after, Query: &pgIntervalQuery{State: state, Epoch: epoch, Up: slices.Clone(holed), Acting: slices.Clone(holed),
		SameIntervalSince: epoch - 1, LastEpochStarted: epoch, Info: pgIntervalInfo{PGID: "3.7s2", Shard: 2, LastUpdate: "25'2", LastComplete: "25'2"}}}, origin
}

func TestPGPeeredUnderAndActiveUnder(t *testing.T) {
	seen, origin := replacedState(31, "undersized+degraded+peered", 3)
	if err := seen.peeredUnder(origin, []int{3}, 27); err != nil {
		t.Fatal("peered PG under the replacement without shard 3 refused:", err)
	}
	both := func(edit func(*osdFixtureOracle)) func(*pgIntervalObservation) {
		return func(o *pgIntervalObservation) { edit(o.Before); edit(o.After) }
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"active":           func(o *pgIntervalObservation) { o.Query.State = "active+undersized+degraded" },
		"remapped":         func(o *pgIntervalObservation) { o.Query.State = "undersized+peered+remapped" },
		"no pg_temp":       both(func(p *osdFixtureOracle) { p.PGTemp = false }),
		"another primary":  both(func(p *osdFixtureOracle) { p.ID, p.PrimaryID, p.Shard = 2, 2, 1 }),
		"placement moved":  func(o *pgIntervalObservation) { o.After.Epoch = 32 },
		"other shard":      func(o *pgIntervalObservation) { o.Query.Info.PGID, o.Query.Info.Shard = "3.7s1", 1 },
		"old interval":     func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 27 },
		"primary behind":   func(o *pgIntervalObservation) { o.Query.Epoch = 30 },
		"query on old set": func(o *pgIntervalObservation) { o.Query.Acting = slices.Clone(origin.Acting) },
	} {
		changed, _ := replacedState(31, "undersized+degraded+peered", 3)
		edit(&changed)
		if err := changed.peeredUnder(origin, []int{3}, 27); err == nil {
			t.Error("observation accepted:", name)
		}
	}
	for name, holes := range map[string][]int{"none": nil, "the primary": {2}, "an empty position": {0}, "repeated": {3, 3}} {
		if err := seen.peeredUnder(origin, holes, 27); err == nil {
			t.Error("positions accepted:", name)
		}
	}
	healthy := origin
	healthy.PGTemp = false
	if err := seen.peeredUnder(healthy, []int{3}, 27); err == nil {
		t.Error("an origin without a pg_temp accepted")
	}

	active, _ := replacedState(34, "active+undersized+degraded")
	if err := active.activeUnder(origin, 30); err != nil {
		t.Fatal("active PG under the replacement refused:", err)
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"peered":          func(o *pgIntervalObservation) { o.Query.State = "undersized+degraded+peered" },
		"clean":           func(o *pgIntervalObservation) { o.Query.State = "active+clean" },
		"a hole":          both(func(p *osdFixtureOracle) { p.Up[3], p.Acting[3] = crushItemNone, crushItemNone }),
		"another address": both(func(p *osdFixtureOracle) { p.Address = "v2:192.0.2.9:6800/1" }),
		"old interval":    func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 30 },
		"not started":     func(o *pgIntervalObservation) { o.Query.LastEpochStarted = 30 },
	} {
		changed, _ := replacedState(34, "active+undersized+degraded")
		edit(&changed)
		if err := changed.activeUnder(origin, 30); err == nil {
			t.Error("observation accepted:", name)
		}
	}
}

func TestPGDegradedOn(t *testing.T) {
	_, route := pgIntervalChange()
	observe := func(state string) pgIntervalObservation {
		o, _ := pgStateObservation(30, state)
		return o
	}
	for _, state := range []string{"active+recovery_wait+degraded", "active+degraded", "active+recovering+degraded"} {
		if err := observe(state).degradedOn(route, 27); err != nil {
			t.Errorf("%s refused: %v", state, err)
		}
	}
	for name, edit := range map[string]func(*pgIntervalObservation){
		"clean":           func(o *pgIntervalObservation) { o.Query.State = "active+clean" },
		"backfilling":     func(o *pgIntervalObservation) { o.Query.State = "active+backfilling+degraded" },
		"undersized":      func(o *pgIntervalObservation) { o.Query.State = "active+undersized+degraded" },
		"a hole":          func(o *pgIntervalObservation) { o.Before.Acting[3], o.After.Acting[3] = crushItemNone, crushItemNone },
		"a pg_temp":       func(o *pgIntervalObservation) { o.Before.PGTemp, o.After.PGTemp = true, true },
		"old interval":    func(o *pgIntervalObservation) { o.Query.SameIntervalSince = 27 },
		"recorded epoch":  func(o *pgIntervalObservation) { o.Before.Epoch, o.After.Epoch = 25, 25 },
		"placement moved": func(o *pgIntervalObservation) { o.After.Epoch = 31 },
	} {
		changed := observe("active+recovery_wait+degraded")
		edit(&changed)
		if err := changed.degradedOn(route, 27); err == nil {
			t.Error("observation accepted:", name)
		}
	}
}

func TestCheckObjectBackoff(t *testing.T) {
	_, route := pgIntervalChange()
	head := cephmsgr.OSDPGCursor{Object: route.Name, Snapshot: cephmsgr.OSDHeadSnapshot, Hash: route.Hash, Pool: route.Pool, Raw: []byte{1, 2, 3}}
	event := cephmsgr.OSDBackoffEvent{Op: cephmsgr.OSDBackoffBlock, Pool: uint64(route.Pool), Seed: route.PG, Shard: route.Shard, ID: 4, Begin: head, End: head,
		Source: cephmsgr.OSDTarget{ID: route.ID}, GlobalID: 9, SessionGeneration: 1, RawFront: []byte{1}}
	if err := checkObjectBackoff(event, cephmsgr.OSDBackoffBlock, route, 9); err != nil {
		t.Fatal("object BLOCK refused:", err)
	}
	for name, edit := range map[string]func(*cephmsgr.OSDBackoffEvent){
		"unblock":       func(e *cephmsgr.OSDBackoffEvent) { e.Op = cephmsgr.OSDBackoffUnblock },
		"another shard": func(e *cephmsgr.OSDBackoffEvent) { e.Shard = 2 },
		"another OSD":   func(e *cephmsgr.OSDBackoffEvent) { e.Source.ID = 1 },
		"another name":  func(e *cephmsgr.OSDBackoffEvent) { e.End.Object = "other" },
		"a snapshot":    func(e *cephmsgr.OSDBackoffEvent) { e.Begin.Snapshot = 4 },
		"a range":       func(e *cephmsgr.OSDBackoffEvent) { e.End.Raw = []byte{1, 2, 4} },
		"another hash":  func(e *cephmsgr.OSDBackoffEvent) { e.Begin.Hash++ },
		"the maximum":   func(e *cephmsgr.OSDBackoffEvent) { e.End.Max = true },
	} {
		changed := event
		edit(&changed)
		if err := checkObjectBackoff(changed, cephmsgr.OSDBackoffBlock, route, 9); err == nil {
			t.Error("event accepted:", name)
		}
	}
}

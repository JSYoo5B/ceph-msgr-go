package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

// A write held inside the EC backend of an optimized isa pool (k=2,m=2,
// k=3,m=1 or k=4,m=2) while
// it waits for its sub-write commits, and what an epoch change does to it.
// The OSD of the last parity shard is paused through the fixture's daemon
// control (heldPause, daemon_lease_test.go) before one
// Go APPEND of a full stripe plus SETXATTR is sent, so every shard is in the
// write plan. The pinned source (Ceph v20.2.4) gives the expected native
// behaviour:
//
//   - do_op marks the op started and executes it (src/osd/PrimaryLogPG.cc:2541);
//     the EC write pipeline applies the primary's own shard and sends every
//     other shard one MOSDECSubOpWrite, and the client op is answered only
//     after every shard's commit (src/osd/ECCommon.cc RMWPipeline,
//     try_finish_rmw; PrimaryLogPG::issue_repop, :4391 mark_commit_sent).
//     A shard marks sub_op_started when it handles the sub-write and
//     sub_op_committed when its transaction is durable, before it replies
//     (src/osd/ECBackend.cc:325-400). The primary sends the remote
//     sub-writes first and then handles its own shard on the client op
//     itself (src/osd/ECCommon.cc:885-903), so the client op carries the
//     primary's sub_op_started and sub_op_committed events. While the paused
//     shard cannot reply, the op stays at the flag point started with its
//     own shard committed, and the other shards hold the entry.
//   - On an interval change the primary cancels its in-progress writes and
//     requeues their client ops (src/osd/PrimaryLogPG.cc:13125 on_change ->
//     :12925 apply_and_flush_repops). In the new interval do_op finds a
//     requeued write's identity in the log and answers it as a duplicate with
//     the logged version (src/osd/PrimaryLogPG.cc:2274-2299).
//   - Peering of an EC pool takes the authoritative log from the shard with
//     the oldest last_update among the primary-eligible shards whose
//     last_epoch_started is the newest (src/osd/PeeringState.cc:1617-1760
//     calculate_maxles_and_minlua, find_best_info, with the nonprimary shards
//     excluded; :2513 calc_ec_acting); entries past it on other shards are
//     divergent and rolled back. A PG peered below min_size neither rolls its
//     log forward nor advances last_epoch_started (src/osd/PeeringState.cc:2904,
//     :3236 acting_set_writeable; doc/dev/osd_internals/last_epoch_started.rst).
//
// The paused OSD is SIGKILLed while still frozen in two of the controls: a
// fatal signal ends a frozen task without returning to user space, so the
// shard never reads the sub-write that waits in its socket. Every held-phase
// observation and the STAT run under the lease's held phase; one still
// outstanding at its end is abandoned and fails the test, and the lease
// resumes the OSD by its deadline all the same (heldPause). The op history
// is bounded and no count bounds what entered it in a window
// (opHistoryWitness), so a roll-forward it does not show, and the exact count
// of the ones it shows, are reported unverified; a roll-forward it shows,
// and the committed write, stay evidence. These consumer helpers are test-only.

// subWriteHoldEvent is the event and flag point do_op marks before it
// executes a write (src/osd/PrimaryLogPG.cc:2541, src/osd/OpRequest.h).
const subWriteHoldEvent = "started"

// subWriteLocalCommit are the events the primary's own shard marks on the
// client op after it started: handled, then durable
// (src/osd/ECCommon.cc:898-903, src/osd/ECBackend.cc:346, :390).
var subWriteLocalCommit = []string{"sub_op_started", "sub_op_committed"}

// subWriteStripe is one full stripe of a pool of k data chunks of chunk
// bytes (chunk from the native stripe_width): an APPEND of that length at a
// stripe boundary writes every shard.
func subWriteStripe(k, chunk int) int {
	return k * chunk
}

// subWriteRecord is one MOSDECSubOpWrite of the request in one OSD's op
// history (oracle mode subwrites, tools/osd_pool_mode_subwrites_oracle.py).
type subWriteRecord struct {
	PGID                    string   `json:"pgid"`
	Shard                   int      `json:"shard"`
	MapEpoch                uint32   `json:"map_epoch"`
	MinEpoch                uint32   `json:"min_epoch"`
	TID                     uint64   `json:"tid"`
	Reqid                   string   `json:"reqid"`
	AtVersion               string   `json:"at_version"`
	TrimTo                  string   `json:"trim_to"`
	PGCommittedTo           string   `json:"pg_committed_to"`
	BackfillOrAsyncRecovery bool     `json:"backfill_or_async_recovery"`
	Committed               bool     `json:"committed"`
	Done                    bool     `json:"done"`
	Events                  []string `json:"events"`
}

type subWriteReceipt struct {
	OSD     int32            `json:"osd"`
	PGID    string           `json:"pgid"`
	Epoch   uint32           `json:"epoch"`
	Reqid   string           `json:"reqid"`
	Writes  []subWriteRecord `json:"writes"`
	History opHistoryRecord  `json:"history"`
}

// opHistoryRecord is the op history a subwrites receipt read: its size and
// duration limits and one identity per kept op, in arrival order.
type opHistoryRecord struct {
	Size     int      `json:"size"`
	Duration int      `json:"duration"`
	Ops      []string `json:"ops"`
}

// subWriteHistoryRead is one subwrites receipt with the test's clock around
// the request that read it.
type subWriteHistoryRead struct {
	subWriteReceipt
	sent, read time.Time
}

// errOpHistoryInsertionsUnbounded is opHistoryWitness's answer once its
// readable terms are consistent: nothing bounds the ops that entered the
// history in the window, so its absences and counts stay unverified.
var errOpHistoryInsertionsUnbounded = errors.New("no independent count bounds the ops that entered the op history in the window")

// opHistoryWitness says whether the op history of after can show that no op
// that entered osd's history since before was read was dropped again. The
// pinned OpHistory drops, on every insert and dump, the ops initiated more
// than duration seconds ago, oldest first, and then the shortest ops while
// more than size are kept (src/common/TrackedOp.cc:86-124, :126-129). An
// op enters the history with its initiation time when it completes, through
// a queue the history thread drains later (OpHistoryServiceThread, :45-71;
// TrackedOp::put, src/common/TrackedOp.h:355-380). So two snapshots do not
// bound what was inserted and dropped in between: an op in flight at the
// first read can complete in the window, push a short roll-forward out by
// size, and then expire by its old initiation time, leaving a second
// snapshot below size that keeps every op of the first.
//
// A sound witness needs every insertion of the window bounded below size:
// the ops kept at before, every op in flight or completed and queued at
// before, and every op started on the OSD in the window, by a count read
// at both ends; and the duration checked against the earliest initiation
// any of them carries. The subwrites mode reads only the history, and the
// pinned OSD exposes no count of tracked ops: OpTracker's sequence number
// is not dumped (:290-297), and its perf counters count slow ops only
// (src/common/TrackedOp.h:75-106). Every missing term leaves the window
// unbounded, so the witness never holds against this daemon. It still
// refuses inconsistent reads first.
func opHistoryWitness(before, after subWriteHistoryRead) error {
	b, a := before.History, after.History
	if before.OSD != after.OSD || b.Size != a.Size || b.Duration != a.Duration || a.Size <= 0 {
		return fmt.Errorf("the op histories of osd.%d (size %d, duration %d) and osd.%d (size %d, duration %d) are not one bounded history", before.OSD, b.Size, b.Duration, after.OSD, a.Size, a.Duration)
	}
	if after.read.Before(before.sent) {
		return fmt.Errorf("the reads of osd.%d are out of order", after.OSD)
	}
	return fmt.Errorf("osd.%d: %w (%d ops kept before, %d after, of %d)", after.OSD, errOpHistoryInsertionsUnbounded, len(b.Ops), len(a.Ops), a.Size)
}

// errRollForwardUnproven is judgeRollForward's answer when a receipt shows
// no roll-forward of the write and the history may have dropped one.
var errRollForwardUnproven = errors.New("the roll-forward coverage is unproven")

// judgeRollForward judges the roll-forwards of write in one receipt.
// Present ones are evidence whatever the history kept: more than one, or
// one to another version, is refused, and one to the write's version is
// accepted, with exact true only when retained (opHistoryWitness's verdict)
// is nil, so that no second one can have been dropped. None is evidence of
// absence only when retained is nil; otherwise the answer wraps
// errRollForwardUnproven and the write's own commit stays evidence.
func judgeRollForward(write subWriteRecord, rolls []subWriteRecord, retained error) (exact bool, err error) {
	switch {
	case len(rolls) > 1:
		return false, fmt.Errorf("%d roll-forwards of the write at %s: %+v", len(rolls), write.AtVersion, rolls)
	case len(rolls) == 1 && rolls[0].PGCommittedTo != write.AtVersion:
		return false, fmt.Errorf("the roll-forward %+v is not to the write's %s", rolls[0], write.AtVersion)
	case len(rolls) == 1:
		return retained == nil, nil
	case retained == nil:
		return true, fmt.Errorf("no roll-forward of the write at %s in a history whose window is bounded", write.AtVersion)
	}
	return false, fmt.Errorf("%w: no roll-forward of the write at %s is kept, and %v", errRollForwardUnproven, write.AtVersion, retained)
}

// judgeNoRollForward judges a receipt that must hold no roll-forward of its
// write: a present one is refused whatever the history kept, and none is
// proven absent only when retained (opHistoryWitness's verdict) is nil.
func judgeNoRollForward(rolls []subWriteRecord, retained error) (proven bool, err error) {
	if len(rolls) != 0 {
		return false, fmt.Errorf("%d roll-forwards of the write: %+v", len(rolls), rolls)
	}
	return retained == nil, nil
}

// shardSubWrites reads osd's op history for the sub-writes of key to the
// object's PG, with the test's clock around the request.
func (r *ecEpochRun) shardSubWrites(ctx context.Context, object string, osd int32, key pgHistoryTarget) subWriteHistoryRead {
	r.t.Helper()
	var result struct {
		SubWrites *subWriteReceipt `json:"sub_writes"`
	}
	sent := time.Now()
	nativePoolResult(r.t, ctx, "subwrites", map[string]any{"slot": 1, "object": object, "osd": osd, "reqid": key}, &result)
	if result.SubWrites == nil {
		r.t.Fatal("the subwrites receipt is missing")
	}
	return subWriteHistoryRead{*result.SubWrites, sent, time.Now()}
}

// checkSubWriteHold judges one snapshot of the primary's op tracker,
// independently of the oracle's parse: a complete receipt between identical
// placement records that equal route but for an epoch not older than its
// own, answered by route's primary, holding exactly one op of the client
// entity and incarnation, for route's PG and shard at route's epoch, with its
// operations decoded for route's head object, executed after its arrival at
// the PG, with the primary's own shard committed and nothing after: the flag
// point is started and the events after started are exactly
// subWriteLocalCommit, so it was never answered (no commit_sent) and never
// delayed again. It returns that op.
func checkSubWriteHold(s opsInFlightSnapshot, route osdFixtureOracle, gid uint64, incarnation int32) (opsInFlightOp, error) {
	if !s.Complete || len(s.Failures) != 0 || s.Code != 0 || s.AbsenceEstablished {
		return opsInFlightOp{}, fmt.Errorf("incomplete in-flight snapshot: code %d, %q", s.Code, s.Failures)
	}
	if !samePlacement(s.Before, s.After) || s.Before.Epoch != s.After.Epoch || s.Before.Address != s.After.Address {
		return opsInFlightOp{}, fmt.Errorf("the placement records around the dump differ: %+v vs %+v", s.Before, s.After)
	}
	if !samePlacement(s.Before, route) || s.Before.Epoch < route.Epoch || s.OSD != route.PrimaryID || s.PGID != route.PGID {
		return opsInFlightOp{}, fmt.Errorf("the dump of osd.%d for %s at %+v is not the route's primary: %+v", s.OSD, s.PGID, s.Before, route)
	}
	if want := fmt.Sprintf("client.%d.%d", gid, incarnation); s.Client != want || len(s.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want 1 of %s)", len(s.Ops), s.Client, want)
	}
	op := s.Ops[0]
	if op.Reqid != fmt.Sprintf("client.%d.%d:%d", gid, incarnation, op.TID) || op.TID == 0 {
		return op, fmt.Errorf("op identity %s, tid %d", op.Reqid, op.TID)
	}
	if op.PGID != route.PGID+"s"+strconv.Itoa(int(route.Shard)) || op.Epoch != route.Epoch {
		return op, fmt.Errorf("op for %s at epoch %d; want %ss%d at %d", op.PGID, op.Epoch, route.PGID, route.Shard, route.Epoch)
	}
	if strings.Contains(op.Description, "(undecoded)") || !strings.Contains(op.Description, " "+hobjectHead(route)+" [") {
		return op, fmt.Errorf("op description %q does not show the decoded operations of %s", op.Description, hobjectHead(route))
	}
	reached, started := slices.Index(op.Events, "reached_pg"), slices.Index(op.Events, subWriteHoldEvent)
	if op.FlagPoint != subWriteHoldEvent || reached < 0 || started < reached || !slices.Equal(op.Events[started+1:], subWriteLocalCommit) {
		return op, fmt.Errorf("op is at %q with events %q; want it executed after reached_pg, its own shard committed (%q) and nothing after", op.FlagPoint, op.Events, subWriteLocalCommit)
	}
	return op, nil
}

// subWriteRollForward is the at_version of the transaction-empty sub-write
// that rolls a committed entry forward (src/osd/ECCommon.cc:955-995
// finish_rmw, ECDummyOp): it carries the write's request identity and the
// write's version as pg_committed_to.
const subWriteRollForward = "0'0"

// checkSubWriteCommitted judges one subwrites receipt of osd for the
// request key on shard of route's PG. Every sub-write it holds must be sent
// at route's epoch to that shard under the request's own identity, handled
// and committed by the shard, complete in the history and not a backfill or
// async-recovery write. Exactly one of them must be the write itself, with
// a version; it returns that one and the roll-forward sub-writes after it.
func checkSubWriteCommitted(s subWriteReceipt, route osdFixtureOracle, osd int32, shard int, key pgHistoryTarget) (subWriteRecord, []subWriteRecord, error) {
	if s.OSD != osd || s.PGID != route.PGID || s.Reqid != key.String() || s.Epoch < route.Epoch {
		return subWriteRecord{}, nil, fmt.Errorf("the receipt of osd.%d for %s %s at epoch %d is not osd.%d's for %s %s", s.OSD, s.PGID, s.Reqid, s.Epoch, osd, route.PGID, key)
	}
	var writes, rolls []subWriteRecord
	for _, w := range s.Writes {
		if w.PGID != route.PGID || w.Shard != shard || w.Reqid != key.String() || w.MapEpoch != route.Epoch || w.MinEpoch == 0 || w.MinEpoch > w.MapEpoch || w.TID == 0 {
			return subWriteRecord{}, nil, fmt.Errorf("sub-write %+v is not one of %s for %ss%d at epoch %d", w, key, route.PGID, shard, route.Epoch)
		}
		started := slices.Index(w.Events, "sub_op_started")
		if !w.Committed || !w.Done || started < 0 || !slices.Contains(w.Events[started+1:], "sub_op_committed") || w.BackfillOrAsyncRecovery {
			return subWriteRecord{}, nil, fmt.Errorf("sub-write of osd.%d has events %q (committed %v, backfill or async recovery %v); want it committed", osd, w.Events, w.Committed, w.BackfillOrAsyncRecovery)
		}
		if w.AtVersion == subWriteRollForward {
			rolls = append(rolls, w)
		} else {
			writes = append(writes, w)
		}
	}
	if len(writes) != 1 {
		return subWriteRecord{}, nil, fmt.Errorf("osd.%d's history holds %d writes of %s for %s (want 1): %+v", osd, len(writes), key, route.PGID, s.Writes)
	}
	for _, roll := range rolls {
		if roll.TID <= writes[0].TID {
			return subWriteRecord{}, nil, fmt.Errorf("roll-forward %+v of osd.%d does not follow the write %+v", roll, osd, writes[0])
		}
	}
	return writes[0], rolls, nil
}

// subWriteRun is one control's object and held write.
type subWriteRun struct {
	*ecEpochRun
	object string
	route  osdFixtureOracle
	since  uint32
	seed   cephmsgr.OSDReply
	pre    nativeXattrSnapshot
	data   []byte
	// other is another object of the same PG.
	other osdFixtureOracle

	fc          *erasureFaultClient
	o           *cephmsgr.OSDConnection
	endpoint    string
	dialed      map[string]int
	gid         uint64
	incarnation int32
	ops         []cephmsgr.OSDOperation
	held        cephmsgr.OSDRequest
	tail, value []byte
	attr        string
	// last is the acting position of the paused shard, the last parity
	// shard.
	last int
	// baseline is every remote shard OSD's op history read before the
	// pause, the first read of opHistoryWitness.
	baseline map[int32]subWriteHistoryRead
}

// startSubWriteRun seeds object with one full stripe and another object of
// its PG, reads the healthy placement, dials the fault client to the
// primary and builds the held write: an APPEND of one full stripe and a
// SETXATTR under incarnation.
func startSubWriteRun(t *testing.T, width erasureWidth, object string, incarnation int32, seeded, mutated uint32) *subWriteRun {
	r := &subWriteRun{ecEpochRun: startECEpochRun(t, width, 900*time.Second), object: object, incarnation: incarnation, attr: object}
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
	stripe := subWriteStripe(width.k, r.facts.chunk)
	if stripe <= 0 || stripe%16 != 0 {
		t.Fatalf("the stripe of k=%d chunks of %d bytes is not a positive multiple of 16", width.k, r.facts.chunk)
	}
	r.data = bytes.Repeat([]byte{0x53, 0, 0xff, 'S'}, stripe/4)
	r.seed = r.write("seed", route, r.data, seeded, cephmsgr.OSDSnapContext{})
	r.write("seed the other object", r.place(other), []byte("another object of the same PG\x00\xff"), seeded, cephmsgr.OSDSnapContext{})
	r.route, r.since = r.healthy(object)
	r.other = r.place(other)
	if r.other.PGID != r.route.PGID || r.other.ID != r.route.ID || r.other.Shard != r.route.Shard {
		t.Fatal("the other object left the PG or its primary", r.other, r.route)
	}
	r.pre = poolObserve(t, r.ctx, 1, object, "", "")
	if r.pre.Code != 0 || !bytes.Equal(r.pre.Data, r.data) || r.pre.Version != r.seed.UserVersion || r.pre.Seconds != int64(seeded) {
		t.Fatal("native state before the held write differs", r.pre.Code, r.pre.Version, r.seed.UserVersion)
	}
	r.last = r.facts.size - 1
	if slices.Contains(r.facts.nonprimary, r.last) || r.last == int(r.route.Shard) {
		t.Fatal("the last parity shard is not a primary-eligible shard other than the primary's", r.last, r.facts.nonprimary, r.route.Shard)
	}

	r.endpoint = osdEndpoint(t, r.route.Address)
	r.fc = dialErasureFaultClient(t, r.ctx, r.hash, "")
	t.Cleanup(func() { r.fc.Close() })
	r.gid = r.fc.Snapshot().GlobalID
	r.o = mutationConnection(t, r.fc.Client, r.ctx, r.route)
	t.Cleanup(func() { r.o.Close() })
	r.dialed = r.fc.dialSnapshot()
	if r.dialed[r.endpoint] != 1 {
		t.Fatal("the held write's connection was not dialed exactly once", r.dialed)
	}
	r.tail = bytes.Repeat([]byte("sub-write hold\x00\xff"), stripe/16)
	r.value = []byte{0, 0xff, 's'}
	r.ops = degradedWaitOps(r.tail, r.value, r.attr)
	r.held = r.request(r.route, r.ops...)
	r.held.ClientIncarnation = uint32(incarnation)
	r.held.ModificationTime = cephmsgr.OSDTime{Seconds: mutated}
	t.Logf("held write: APPEND %d bytes at %d and SETXATTR %s to %s on pg %s, primary osd.%d shard %d acting %v epoch %d; paused shard %d is osd.%d",
		len(r.tail), len(r.data), r.attr, object, r.route.PGID, r.route.ID, r.route.Shard, r.route.Acting, r.route.Epoch, r.last, r.route.Acting[r.last])
	return r
}

// key is the request identity of the held write with transaction tid.
func (r *subWriteRun) key(tid uint64) pgHistoryTarget {
	return pgHistoryTarget{Type: "client", Num: r.gid, Inc: r.incarnation, TID: tid}
}

// seedKey is the request identity of the seed write.
func (r *subWriteRun) seedKey() pgHistoryTarget {
	s := r.seed.Submission
	return pgHistoryTarget{Type: "client", Num: s.GlobalID, Inc: int32(s.ClientIncarnation), TID: s.Transaction}
}

// pauseAndHold reads the op history of every remote shard's OSD as the
// baseline of opHistoryWitness, pauses the OSD of the last parity shard,
// sends the held write and returns the lease, the call, the held op of the
// primary's op tracker and the committed sub-write of every other shard: the
// shards that are neither the primary's nor the paused one.
func (r *subWriteRun) pauseAndHold() (*heldPause, heldCall, opsInFlightOp, map[int]subWriteRecord) {
	t := r.t
	t.Helper()
	r.baseline = map[int32]subWriteHistoryRead{}
	for shard, osd := range r.route.Acting {
		if shard != int(r.route.Shard) {
			r.baseline[osd] = r.shardSubWrites(r.ctx, r.object, osd, r.seedKey())
		}
	}
	p := r.pause(r.route.Acting[r.last])
	call := startHeldCall(r.ctx, r.o, r.held)
	op := r.waitSubWriteHold(p, call)
	committed := map[int]subWriteRecord{}
	for shard, osd := range r.route.Acting {
		if shard == int(r.route.Shard) || shard == r.last {
			continue
		}
		committed[shard] = r.waitSubWriteCommitted(p, osd, shard, r.key(op.TID))
	}
	select {
	case early := <-call.finished:
		t.Fatal("the held write ended while the paused shard could not commit", early.reply.Code, early.err)
	default:
	}
	return p, call, op, committed
}

// waitSubWriteHold takes bounded snapshots of the primary's op tracker, each
// under the held phase, until one holds the write waiting for its sub-write
// commits.
func (r *subWriteRun) waitSubWriteHold(p *heldPause, call heldCall) opsInFlightOp {
	r.t.Helper()
	err := errors.New("no snapshot taken")
	for attempt := 1; attempt <= 10 && p.held.Err() == nil; attempt++ {
		s := primaryOpsInFlight(r.t, p.held, 1, r.object, r.gid, r.incarnation)
		r.checkHeld(p, fmt.Sprintf("in-flight snapshot %d", attempt))
		var op opsInFlightOp
		if op, err = checkSubWriteHold(s, r.route, r.gid, r.incarnation); err == nil {
			r.t.Logf("held at osd.%d (snapshot %d, %d ops in flight): %s, flag point %s, events %q", s.OSD, attempt, s.NumOps, op.Description, op.FlagPoint, op.Events)
			return op
		}
		r.t.Logf("snapshot %d: %v", attempt, err)
		select {
		case early := <-call.finished:
			r.t.Fatal("the held write ended", early.reply.Code, early.err)
		case <-p.held.Done():
		case <-time.After(300 * time.Millisecond):
		}
	}
	r.t.Fatal("the primary did not hold the write for its sub-write commits within the held phase of the pause:", err)
	return opsInFlightOp{}
}

// waitSubWriteCommitted reads osd's op history, each read under the held
// phase, until it holds the committed sub-write of key for shard. A
// roll-forward of it fails the test; its absence is proven only when the
// history's window is bounded (opHistoryWitness), and is logged
// as unverified otherwise.
func (r *subWriteRun) waitSubWriteCommitted(p *heldPause, osd int32, shard int, key pgHistoryTarget) subWriteRecord {
	r.t.Helper()
	err := errors.New("no history read")
	for attempt := 1; attempt <= 5 && p.held.Err() == nil; attempt++ {
		s := r.shardSubWrites(p.held, r.object, osd, key)
		r.checkHeld(p, fmt.Sprintf("op history %d of osd.%d", attempt, osd))
		var w subWriteRecord
		var rolls []subWriteRecord
		if w, rolls, err = checkSubWriteCommitted(s.subWriteReceipt, r.route, osd, shard, key); err == nil {
			retained := opHistoryWitness(r.baseline[osd], s)
			proven, err := judgeNoRollForward(rolls, retained)
			if err != nil {
				r.t.Fatalf("osd.%d of shard %d holds a roll-forward of %s while a shard cannot commit: %v", osd, shard, key, err)
			}
			r.t.Logf("shard %d (osd.%d) committed the sub-write: tid %d at %s, epoch %d/%d, events %q", shard, osd, w.TID, w.AtVersion, w.MapEpoch, w.MinEpoch, w.Events)
			if proven {
				r.t.Logf("shard %d (osd.%d) held no roll-forward: its history's window is bounded", shard, osd)
			} else {
				r.t.Logf("shard %d (osd.%d): the absence of a roll-forward during the hold is unverified: %v", shard, osd, retained)
			}
			return w
		}
		r.t.Logf("history %d of osd.%d: %v", attempt, osd, err)
		select {
		case <-p.held.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	r.t.Fatalf("osd.%d of shard %d did not show the committed sub-write of %s within the held phase: %v", osd, shard, key, err)
	return subWriteRecord{}
}

// waitRolledForward reads osd's op history, after the held phase, within
// a bounded number of reads, until it holds the committed write of key for
// shard and the roll-forward to its version that the primary sends once
// every shard committed it (judgeRollForward), and returns the write and
// whether the count of one is exact. When no roll-forward is kept and the
// history's window is not bounded (opHistoryWitness), it returns the write
// with coverage unproven (errRollForwardUnproven) instead of failing; a
// bounded history that shows none fails the test.
func (r *subWriteRun) waitRolledForward(osd int32, shard int, key pgHistoryTarget) (subWriteRecord, bool, error) {
	r.t.Helper()
	err := errors.New("no history read")
	for attempt := 1; attempt <= 10; attempt++ {
		s := r.shardSubWrites(r.ctx, r.object, osd, key)
		var w subWriteRecord
		var rolls []subWriteRecord
		if w, rolls, err = checkSubWriteCommitted(s.subWriteReceipt, r.route, osd, shard, key); err == nil {
			var exact bool
			if exact, err = judgeRollForward(w, rolls, opHistoryWitness(r.baseline[osd], s)); err == nil {
				r.t.Logf("shard %d (osd.%d) holds the write (tid %d at %s) and its roll-forward (tid %d, pg_committed_to %s); count exact: %v", shard, osd, w.TID, w.AtVersion, rolls[0].TID, rolls[0].PGCommittedTo, exact)
				return w, exact, nil
			}
			if errors.Is(err, errRollForwardUnproven) && attempt == 10 {
				return w, false, err
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	r.t.Fatalf("osd.%d of shard %d did not show the write of %s rolled forward: %v", osd, shard, key, err)
	return subWriteRecord{}, false, nil
}

// sameAtVersion requires every committed sub-write to carry version.
func sameAtVersion(committed map[int]subWriteRecord, version string) error {
	for shard, w := range committed {
		if w.AtVersion != version {
			return fmt.Errorf("shard %d committed the sub-write at %s, not at %s", shard, w.AtVersion, version)
		}
	}
	return nil
}

// applied requires the native object to hold the seed and the held write
// once, at userVersion.
func (r *subWriteRun) applied(label string, userVersion uint64) nativeXattrSnapshot {
	r.t.Helper()
	n := poolObserve(r.t, r.ctx, 1, r.object, "", "")
	want := append(bytes.Clone(r.data), r.tail...)
	if n.Code != 0 || !bytes.Equal(n.Data, want) || bytes.Count(n.Data, r.tail) != 1 || n.Version != userVersion ||
		n.Seconds != int64(r.held.ModificationTime.Seconds) || xattrInventory(n)[r.attr] != string(r.value) {
		r.t.Fatal(label+": native state differs from one application", n.Code, len(n.Data), n.Version, userVersion, n.Seconds)
	}
	return n
}

// unchanged requires the native object to equal its state before the held
// write.
func (r *subWriteRun) unchanged(label string) {
	r.t.Helper()
	n := poolObserve(r.t, r.ctx, 1, r.object, "", "")
	if n.Code != 0 || n.Version != r.pre.Version || !bytes.Equal(n.Data, r.data) || n.Seconds != r.pre.Seconds || n.Nanos != r.pre.Nanos || !maps.Equal(xattrInventory(n), xattrInventory(r.pre)) {
		r.t.Fatal(label+": native state differs from the state before the held write", n.Code, n.Version, r.pre.Version, len(n.Data), n.Seconds)
	}
}

// history reads the PG log of the object's current primary, requires it on
// route, the seed once at its own version and the near miss of tid absent,
// and returns the held identity's locations.
func (r *subWriteRun) history(label string, route osdFixtureOracle, tid uint64) []pgHistoryLocation {
	r.t.Helper()
	nearMiss := pgHistoryTarget{Type: "client", Num: r.gid, Inc: r.incarnation + 1, TID: tid}
	s := primaryPGHistory(r.t, r.ctx, 1, r.object, []pgHistoryTarget{r.seedKey(), r.key(tid), nearMiss})
	requirePGHistoryOnRoute(r.t, s, route)
	if got := s.Targets[0].Locations; len(got) != 1 || got[0].Tier != "log" || got[0].Object != hobjectHead(route) || got[0].Version != replayVersion(r.seed) {
		r.t.Fatalf("%s: the log of osd.%d does not hold the seed once at %s: %+v", label, s.OSD, replayVersion(r.seed), got)
	}
	if len(s.Targets[2].Locations) != 0 {
		r.t.Fatalf("%s: the log of osd.%d holds the near miss %s: %+v", label, s.OSD, nearMiss, s.Targets[2].Locations)
	}
	return s.Targets[1].Locations
}

// heldOnce requires locations to be one log entry of the object at version.
func heldOnce(locations []pgHistoryLocation, route osdFixtureOracle, version string) error {
	if len(locations) != 1 || locations[0].Tier != "log" || locations[0].Object != hobjectHead(route) || locations[0].Version != version {
		return fmt.Errorf("the held identity is at %+v, not once in the log at %s", locations, version)
	}
	return nil
}

// erasureSubWriteHoldScenario: a write held
// inside the EC backend of an optimized isa pool waiting for its
// sub-write commits. With the OSD of the last parity shard paused, one Go
// APPEND of a full stripe plus SETXATTR must be held at the primary: a
// bracketed dump_ops_in_flight shows exactly that op, decoded for the object
// at its PG shard and epoch, executed, with the primary's own shard
// committed and without commit_sent, and the op histories of the other
// remote shards (every shard but the primary's and the paused one) each show the MOSDECSubOpWrite of that request identity
// committed at the placement's epoch, and no roll-forward of it. A STAT of
// another object of the same PG completes on the same connection meanwhile.
// After the pause ends, the write must complete once with ONDISK under its
// original transaction at the version the shards committed, and the history
// of every remote shard, the paused one's included, must then hold the write
// and the roll-forward the primary sends once every shard committed it
// (pg_committed_to at that version). The op history may drop ops a window
// inserted, and nothing on the pinned daemon bounds those insertions
// (opHistoryWitness), so the absence of a roll-forward during the hold, a
// missing roll-forward after it and the exact count are logged as
// unverified, and the write's commit and any roll-forward shown stay
// evidence. The native bytes, xattr, user version and mtime must show one
// application, the paused OSD must have stayed up in the native map, and
// every PG must be clean in the same interval.
//
// The primary's own shard is seen committed on the client op itself. Not
// verified: a pause past the heartbeat grace, a write whose plan leaves a
// shard out, and other plugins and wider codes.
func erasureSubWriteHoldScenario(t *testing.T, width erasureWidth) {
	r := startSubWriteRun(t, width, "ec-subwrite-hold", 91, 1700014000, 1700014002)
	paused := r.route.Acting[r.last]
	before := r.osdState(paused)
	p, call, op, committed := r.pauseAndHold()

	stat := r.request(r.other, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	stat.ClientIncarnation = uint32(r.incarnation) + 1
	// The STAT reads no oracle input, so it is bounded by the held phase
	// directly; the lease resumes the OSD by its deadline even while it is
	// outstanding.
	statReply, statErr := r.o.Request(p.held, stat)
	r.checkHeld(p, "the STAT of another object of the PG")
	if statErr != nil || statReply.Code != 0 || len(statReply.Operations) != 1 {
		t.Fatal("STAT of another object of the PG while the write is held", statErr, statReply.Code)
	}
	select {
	case early := <-call.finished:
		t.Fatal("the held write ended while the last parity shard was paused", early.reply.Code, early.err)
	default:
	}
	r.resume(p)

	var done heldResult
	select {
	case done = <-call.finished:
	case <-time.After(60 * time.Second):
		call.cancel()
		t.Fatal("the held write did not complete within 60 s after the pause ended")
	}
	reply := done.reply
	t.Logf("held write completed: tid %d code %d flags %#x user version %d replay %s epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.MapEpoch)
	if done.err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Transaction != op.TID || reply.RetryAttempt != 0 ||
		reply.UserVersion <= r.pre.Version || reply.Submission.GlobalID != r.gid || reply.Submission.ClientIncarnation != uint32(r.incarnation) {
		t.Fatal("the held write's reply differs", done.err, reply.Code, reply.Flags, reply.Transaction, op.TID, reply.UserVersion)
	}
	if err := sameAtVersion(committed, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	committed[r.last] = subWriteRecord{}
	for _, shard := range slices.Sorted(maps.Keys(committed)) {
		w, exact, unproven := r.waitRolledForward(r.route.Acting[shard], shard, r.key(op.TID))
		if w.AtVersion != replayVersion(reply) {
			t.Fatalf("shard %d committed the write at %s, not at the reply's %s", shard, w.AtVersion, replayVersion(reply))
		}
		switch {
		case unproven != nil:
			t.Logf("shard %d (osd.%d): the roll-forward is unverified, the commit of the write stands: %v", shard, r.route.Acting[shard], unproven)
		case !exact:
			t.Logf("shard %d (osd.%d): one roll-forward is kept; that no second one was sent is unverified, since the history may have dropped ops", shard, r.route.Acting[shard])
		}
		committed[shard] = w
	}
	final := r.applied("after the held write", reply.UserVersion)
	if err := heldOnce(r.history("after the held write", r.route, op.TID), r.route, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	r.stayedUp(before, reply.MapEpoch)
	r.cleanInInterval(r.object, r.route, r.since)
	recovered := poolObserve(t, r.ctx, 1, r.object, "", "")
	if !bytes.Equal(recovered.Data, final.Data) || recovered.Version != final.Version || !maps.Equal(xattrInventory(recovered), xattrInventory(final)) {
		t.Fatal("native state after the clean check differs", recovered.Version, final.Version)
	}
	if extra := newDials(r.dialed, r.fc.dialSnapshot(), r.endpoint); len(extra) != 0 {
		t.Fatal("the client opened connections the caller did not ask for:", extra)
	}
}

// TestCephOSDErasureOptimizedK2M2SubWriteHoldIntegration is erasureSubWriteHoldScenario on optimized isa
// k=2,m=2 (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2subwritehold key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2SubWriteHoldIntegration(t *testing.T) {
	erasureSubWriteHoldScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1SubWriteHoldIntegration is erasureSubWriteHoldScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1subwritehold key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK3M1SubWriteHoldIntegration(t *testing.T) {
	erasureSubWriteHoldScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2SubWriteHoldIntegration is erasureSubWriteHoldScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2subwritehold key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated pauses an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2SubWriteHoldIntegration(t *testing.T) {
	erasureSubWriteHoldScenario(t, erasureK4M2)
}

// erasureSubWriteShardLostScenario: the held
// shard dies and the primary stays. The write is held as in
// erasureSubWriteHoldScenario, with the other remote shards' sub-writes
// committed. The paused OSD is SIGKILLed while frozen, so it never reads its sub-write. Every remaining shard holds the
// entry, so the new interval keeps it, and the primary requeues the client
// op and answers it from its log. Without any resubmission, the original
// call must complete with ONDISK under its original transaction, at the
// version the shards committed and at a map epoch of the new interval, in
// which the PG is active without the killed shard. The primary's log must
// hold the identity once, the native object must show one application, and
// after the OSD's start and clean PGs it must be unchanged. The client must
// open no connection the caller did not ask for.
//
// Not verified: the reply's per-op return data beyond the code (a duplicate
// answer carries the logged op returns), the loss of a shard the write plan
// left out, and other plugins and wider codes.
func erasureSubWriteShardLostScenario(t *testing.T, width erasureWidth) {
	r := startSubWriteRun(t, width, "ec-subwrite-shard-lost", 92, 1700015000, 1700015002)
	p, call, op, committed := r.pauseAndHold()
	lost := r.kill(r.route.Acting[r.last])
	r.checkHeld(p, "the kill of the paused shard's OSD")
	// The fixture's resume completes the lease of a container that is no
	// longer paused without starting it.
	r.resume(p)

	done, ended, late := endWithin(call.finished, 60*time.Second, 5*time.Second, call.cancel)
	if !ended {
		t.Fatal("the held write was not answered within 60 s after the paused shard's OSD was killed;", late)
	}
	reply := done.reply
	t.Logf("held write answered: tid %d code %d flags %#x user version %d replay %s epoch %d attempt %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.MapEpoch, reply.RetryAttempt)
	if done.err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.Transaction != op.TID || reply.RetryAttempt != 0 ||
		reply.UserVersion <= r.pre.Version || reply.Submission.GlobalID != r.gid || reply.Submission.ClientIncarnation != uint32(r.incarnation) {
		t.Fatal("the held write's answer differs", done.err, reply.Code, reply.Flags, reply.Transaction, op.TID, reply.UserVersion)
	}
	if err := sameAtVersion(committed, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	if reply.MapEpoch <= r.route.Epoch {
		t.Fatalf("the answer at epoch %d is from the interval of the hold (placement epoch %d), not after the shard's loss", reply.MapEpoch, r.route.Epoch)
	}
	active := r.waitState("active without the killed shard", r.object, func(o pgIntervalObservation) error { return o.activeWithout(r.route, []int{r.last}, r.since) })
	if reply.MapEpoch < active.Query.SameIntervalSince {
		t.Fatalf("the answer at epoch %d precedes the interval since %d without the killed shard", reply.MapEpoch, active.Query.SameIntervalSince)
	}
	without := *active.After
	r.applied("after the answer", reply.UserVersion)
	if err := heldOnce(r.history("without the killed shard", without, op.TID), without, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	r.heldThrough("the answer and the log read without the killed shard", r.object, without)

	r.start(lost)
	r.clean()
	back := r.waitState("clean with the shard back", r.object, func(o pgIntervalObservation) error {
		_, err := o.cleanAgainOn(r.route, active.Query.SameIntervalSince)
		return err
	})
	r.applied("after the shard's return", reply.UserVersion)
	if err := heldOnce(r.history("with the shard back", *back.After, op.TID), *back.After, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	r.heldThrough("the log read with the shard back", r.object, *back.After)
	if extra := newDials(r.dialed, r.fc.dialSnapshot(), r.endpoint); len(extra) != 0 {
		t.Fatal("the client opened connections the caller did not ask for:", extra)
	}
}

// TestCephOSDErasureOptimizedK2M2SubWriteShardLostIntegration is erasureSubWriteShardLostScenario on optimized isa
// k=2,m=2 (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2subwriteshardlost key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and kills an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2SubWriteShardLostIntegration(t *testing.T) {
	erasureSubWriteShardLostScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1SubWriteShardLostIntegration is erasureSubWriteShardLostScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1subwriteshardlost key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and kills an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK3M1SubWriteShardLostIntegration(t *testing.T) {
	erasureSubWriteShardLostScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2SubWriteShardLostIntegration is erasureSubWriteShardLostScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2subwriteshardlost key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and kills an OSD; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2SubWriteShardLostIntegration(t *testing.T) {
	erasureSubWriteShardLostScenario(t, erasureK4M2)
}

// peeredWithout requires one read-only observation to show the PG peered
// and not active with exactly the given acting positions of origin empty:
// two identical placement records around a query; the placement's acting
// set is origin's with those positions empty at a newer epoch, led by the
// first remaining member of the primary-first order on its own shard; that
// primary reports peered and undersized, optionally degraded, with that
// acting set, in an interval that started after since. Too few shards for
// min_size keep the PG from activating.
func (o pgIntervalObservation) peeredWithout(origin osdFixtureOracle, holes, nonprimary []int, since uint32) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	if len(holes) == 0 || len(origin.Acting)-len(holes) >= origin.MinSize || since == 0 {
		return fmt.Errorf("%d empty positions of %d leave the PG at or above min_size %d", len(holes), len(origin.Acting), origin.MinSize)
	}
	holed := holedSet(origin.Acting, holes...)
	lead := -1
	for _, shard := range primaryFirstOrder(len(holed), nonprimary) {
		if holed[shard] != crushItemNone {
			lead = shard
			break
		}
	}
	if lead < 0 || slices.Contains(nonprimary, lead) {
		return fmt.Errorf("no member of %v can lead", holed)
	}
	b := *o.Before
	if !slices.Equal(b.Acting, holed) || b.Epoch <= origin.Epoch || b.PGID != origin.PGID || b.ID != holed[lead] || b.PrimaryID != holed[lead] || int(b.Shard) != lead {
		return fmt.Errorf("the placement %+v is not %v at a newer epoch led by shard %d", b, holed, lead)
	}
	q := o.Query
	if q == nil || o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: code %d, %s", o.Code, o.QueryFailure)
	}
	if states := pgStates(q.State); !slices.Equal(states, []string{"peered", "undersized"}) && !slices.Equal(states, []string{"degraded", "peered", "undersized"}) {
		return fmt.Errorf("the primary reports %q, not a peered undersized PG", q.State)
	}
	if !slices.Equal(q.Acting, holed) || q.Info.PGID != origin.PGID+"s"+strconv.Itoa(lead) || q.Info.Shard != lead {
		return fmt.Errorf("the primary reports acting %v for %s; want %v for shard %d", q.Acting, q.Info.PGID, holed, lead)
	}
	if q.SameIntervalSince <= since || q.SameIntervalSince > b.Epoch || q.Epoch < b.Epoch {
		return fmt.Errorf("the interval started at epoch %d with the primary at epoch %d; the recorded interval started at %d and the placement is at epoch %d", q.SameIntervalSince, q.Epoch, since, b.Epoch)
	}
	return nil
}

// erasureSubWriteRollbackScenario: the primary
// dies, then the held shard. The write is held as in
// erasureSubWriteHoldScenario, with the other remote shards' sub-writes
// committed. The primary's OSD is SIGKILLed, then
// the paused one while still frozen, so it never reads its sub-write. The
// call must end with an OutcomeUnknownError that keeps its original
// identity, and the client must open no further OSD connection. With the
// two OSDs gone the PG is below min_size and must be peered and not active
// under the next primary-eligible shard, so nothing rolls the entry forward. The killed shard's OSD is started first: it returns with
// the older last_update on a primary-eligible shard, so its log is the
// authoritative one and the entry that the other shards committed must be
// rolled back. Every claim after a fault is judged on the observed placement
// and must be followed by that interval still holding (heldThrough). Under the replacement primary (primary-first pg_temp) the
// PG must be active, the native object unchanged, and the replacement's log
// must not hold the held identity. After the original primary's start and
// clean PGs, the same holds for the returned primary's log. The caller then
// resubmits the same operations once under the original transaction, which
// must apply once and appear once in that log, on exactly one new
// connection.
//
// Not verified: the other shards' own logs after the rollback (only the
// primary's log and the object are read), a held write whose entry reaches
// k primary-eligible shards before the primary dies, and other plugins and
// wider codes. There is no k=3,m=1 row: with shards 0 and 3 gone only the
// nonprimary shards 1 and 2 remain, and peering excludes nonprimary shards
// from the authoritative log (src/osd/PeeringState.cc:2513-2531
// choose_acting, find_best_info with exclude_nonprimary_shards), so the
// peered state this control needs does not exist on that width.
func erasureSubWriteRollbackScenario(t *testing.T, width erasureWidth) {
	r := startSubWriteRun(t, width, "ec-subwrite-rollback", 93, 1700016000, 1700016002)
	p, call, op, _ := r.pauseAndHold()
	primary := r.kill(r.route.ID)
	r.checkHeld(p, "the kill of the primary's OSD")
	lost := r.kill(r.route.Acting[r.last])
	r.checkHeld(p, "the kill of the paused shard's OSD")
	r.resume(p)

	// The killed primary's socket closes, so the session ends the call; the
	// caller's context stays live.
	gone, ended, late := endWithin(call.finished, 30*time.Second, 5*time.Second, call.cancel)
	if !ended {
		t.Fatal("the session did not end the held write within 30 s after the primary's OSD was killed;", late)
	}
	var unknown *cephmsgr.OutcomeUnknownError
	var server *cephmsgr.OSDError
	sub := gone.reply.Submission
	t.Logf("held write ended by the session: %v; submission tid %d global id %d incarnation %d epoch %d attempt %d placement %+v",
		gone.err, sub.Transaction, sub.GlobalID, sub.ClientIncarnation, sub.MapEpoch, sub.Attempt, sub.Placement)
	if !errors.As(gone.err, &unknown) || errors.As(gone.err, &server) || errors.Is(gone.err, context.Canceled) {
		t.Fatal("expected an unknown outcome without a server result or a local cancellation", gone.err)
	}
	if sub.Transaction != op.TID || sub.GlobalID != r.gid || sub.ClientIncarnation != uint32(r.incarnation) || sub.MapEpoch != r.route.Epoch ||
		sub.Attempt != 0 || sub.Placement == nil || sub.Placement.Primary != r.route.ID || int8(sub.Placement.Shard) != r.route.Shard {
		t.Fatal("the unknown outcome lost the identity the primary held", sub, op.TID)
	}
	r.o.Close()

	holes := []int{int(r.route.Shard), r.last}
	away := r.place(r.object)
	for attempt := 1; attempt < 30 && !slices.Equal(away.Acting, holedSet(r.route.Acting, holes...)); attempt++ {
		time.Sleep(time.Second)
		away = r.place(r.object)
	}
	if !slices.Equal(away.Acting, holedSet(r.route.Acting, holes...)) {
		t.Fatal("the placement does not show both killed OSDs gone within 30 s", away)
	}
	peered := r.waitState("peered without the primary and the held shard", r.object, func(o pgIntervalObservation) error {
		return o.peeredWithout(r.route, holes, r.facts.nonprimary, r.since)
	})

	r.start(lost)
	lead := -1
	seen := r.waitState("active under the replacement with the killed shard back", r.object, func(o pgIntervalObservation) error {
		var err error
		lead, err = o.replacedOn(r.route, r.facts.nonprimary, r.since)
		if err == nil && o.Query.SameIntervalSince <= peered.Query.SameIntervalSince {
			err = fmt.Errorf("the interval since %d is not after the peered one since %d", o.Query.SameIntervalSince, peered.Query.SameIntervalSince)
		}
		return err
	})
	replaced := r.place(r.object)
	if !samePlacement(replaced, *seen.After) || replaced.Epoch < seen.After.Epoch || int(replaced.Shard) != lead {
		t.Fatalf("placement moved after the PG became active under the replacement: %+v vs %+v", replaced, *seen.After)
	}
	r.unchanged("under the replacement")
	if got := r.history("under the replacement", replaced, op.TID); len(got) != 0 {
		t.Fatalf("the replacement's log holds the held identity after the rollback: %+v", got)
	}
	r.heldThrough("the native read and the log read under the replacement", r.object, replaced)

	back, _ := r.primaryBack(r.object, r.route, primary, seen.Query.SameIntervalSince)
	t.Logf("restored: epoch %d primary osd.%d shard %d acting %v at %s (was %s)", back.Epoch, back.ID, back.Shard, back.Acting, back.Address, r.route.Address)
	r.unchanged("after the primary's return")
	if got := r.history("after the primary's return", back, op.TID); len(got) != 0 {
		t.Fatalf("the returned primary's log holds the held identity: %+v", got)
	}
	if extra := newDials(r.dialed, r.fc.dialSnapshot(), r.endpoint); len(extra) != 0 {
		t.Fatal("the client connected again before the caller's decision:", extra)
	}

	// The caller's explicit decision: resubmit the same operations once
	// under the original identity to the returned primary.
	ro := mutationConnection(t, r.fc.Client, r.ctx, back)
	defer ro.Close()
	retry := r.request(back, r.ops...)
	retry.ClientIncarnation = uint32(r.incarnation)
	retry.ModificationTime = r.held.ModificationTime
	retry.Resubmit, retry.RetryAttempt = &sub, 1
	retryCtx, stop := context.WithTimeout(r.ctx, 30*time.Second)
	reply, err := ro.Request(retryCtx, retry)
	stop()
	if err != nil {
		t.Fatal("explicit resubmission to the returned primary:", err)
	}
	t.Logf("resubmission: tid %d code %d flags %#x user version %d replay %s attempt %d epoch %d", reply.Transaction, reply.Code, reply.Flags, reply.UserVersion, replayVersion(reply), reply.RetryAttempt, reply.MapEpoch)
	if reply.Transaction != sub.Transaction || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.RetryAttempt != 1 || reply.UserVersion <= r.pre.Version ||
		reply.Submission.Transaction != sub.Transaction || reply.Submission.MapEpoch != back.Epoch || reply.Submission.GlobalID != r.gid || reply.Submission.ClientIncarnation != uint32(r.incarnation) {
		t.Fatal("resubmission receipt differs", reply.Transaction, reply.Code, reply.Flags, reply.RetryAttempt, reply.UserVersion)
	}
	r.applied("after the resubmission", reply.UserVersion)
	if err := heldOnce(r.history("after the resubmission", back, op.TID), back, replayVersion(reply)); err != nil {
		t.Fatal(err)
	}
	if err := checkOneDial(r.dialed, r.fc.dialSnapshot(), r.endpoint, osdEndpoint(t, back.Address)); err != nil {
		t.Fatal("the resubmission did not open exactly its one connection:", err)
	}
	r.heldThrough("the log reads and the resubmission at the returned primary", r.object, back)
	t.Logf("applied once by the resubmission at %s, version %d -> %d; the original outcome stays unknown", replayVersion(reply), r.pre.Version, reply.UserVersion)
}

// TestCephOSDErasureOptimizedK2M2SubWriteRollbackIntegration is erasureSubWriteRollbackScenario on optimized isa
// k=2,m=2 (four OSDs).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2subwriterollback key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and kills OSDs; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK2M2SubWriteRollbackIntegration(t *testing.T) {
	erasureSubWriteRollbackScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK4M2SubWriteRollbackIntegration is erasureSubWriteRollbackScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2subwriterollback key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and kills OSDs; restart waits osd-ready clean
func TestCephOSDErasureOptimizedK4M2SubWriteRollbackIntegration(t *testing.T) {
	erasureSubWriteRollbackScenario(t, erasureK4M2)
}

// subWriteHoldSnapshot builds one synthetic receipt of PG 3.7's primary
// osd.3 holding client.4171.91:5, decoded and started, waiting for its
// sub-write commits at the placement's epoch 25. No daemon produced it.
func subWriteHoldSnapshot() (opsInFlightSnapshot, osdFixtureOracle) {
	s, route := heldSnapshot()
	s.Client = "client.4171.91"
	op := &s.Ops[0]
	op.Reqid, op.Epoch, op.FlagPoint = "client.4171.91:5", 25, subWriteHoldEvent
	op.Events = []string{"initiated", "throttled", "header_read", "all_read", "dispatched", "queued_for_pg", "reached_pg", subWriteHoldEvent, "sub_op_started", "sub_op_committed"}
	op.Description = "osd_op(client.4171.91:5 3.7s0 " + hobjectHead(route) + " [append 8192~8192,setxattr ec (3)] snapc 0=[] ondisk+write+known_if_redirected e25)"
	return s, route
}

func TestCheckSubWriteHold(t *testing.T) {
	s, route := subWriteHoldSnapshot()
	if op, err := checkSubWriteHold(s, route, 4171, 91); err != nil || op.TID != 5 {
		t.Fatal("held write refused:", op, err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot){
		"incomplete":              func(s *opsInFlightSnapshot) { s.Complete = false },
		"a recorded failure":      func(s *opsInFlightSnapshot) { s.Failures = []string{"placement or epoch changed"} },
		"claims absence":          func(s *opsInFlightSnapshot) { s.AbsenceEstablished = true },
		"placement moved":         func(s *opsInFlightSnapshot) { s.After.Epoch = 26 },
		"acting moved":            func(s *opsInFlightSnapshot) { s.After.Acting = []int32{3, 2, 1, crushItemNone} },
		"another primary":         func(s *opsInFlightSnapshot) { s.OSD = 2 },
		"another client":          func(s *opsInFlightSnapshot) { s.Client = "client.4171.90" },
		"two ops":                 func(s *opsInFlightSnapshot) { s.Ops = append(s.Ops, s.Ops[0]) },
		"identity of another tid": func(s *opsInFlightSnapshot) { s.Ops[0].Reqid = "client.4171.91:6" },
		"another shard":           func(s *opsInFlightSnapshot) { s.Ops[0].PGID = "3.7s2" },
		"another epoch":           func(s *opsInFlightSnapshot) { s.Ops[0].Epoch = 26 },
		"undecoded": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = "osd_op(client.4171.91:5 3.7s0 3.d6c2a1e7 (undecoded) ondisk+write+known_if_redirected e25)"
		},
		"another object": func(s *opsInFlightSnapshot) {
			s.Ops[0].Description = strings.Replace(s.Ops[0].Description, ":::obj:head", ":::other:head", 1)
		},
		"never executed":      func(s *opsInFlightSnapshot) { s.Ops[0].Events[len(s.Ops[0].Events)-1] = degradedWaitEvent },
		"delayed flag point":  func(s *opsInFlightSnapshot) { s.Ops[0].FlagPoint = degradedWaitEvent },
		"answered":            func(s *opsInFlightSnapshot) { s.Ops[0].Events = append(s.Ops[0].Events, "commit_sent") },
		"delayed after start": func(s *opsInFlightSnapshot) { s.Ops[0].Events = append(s.Ops[0].Events, "waiting for rw locks") },
		"never reached the PG": func(s *opsInFlightSnapshot) {
			s.Ops[0].Events = []string{"initiated", "queued_for_pg", subWriteHoldEvent}
		},
		"started before the PG": func(s *opsInFlightSnapshot) {
			s.Ops[0].Events = []string{"initiated", subWriteHoldEvent, "reached_pg", subWriteHoldEvent}
		},
	} {
		changed, _ := subWriteHoldSnapshot()
		edit(&changed)
		if _, err := checkSubWriteHold(changed, route, 4171, 91); err == nil {
			t.Error("snapshot accepted:", name)
		}
	}
	if _, err := checkSubWriteHold(s, route, 4171, 92); err == nil {
		t.Error("another incarnation accepted")
	}
}

// subWriteReceiptSample is one synthetic subwrites receipt of osd.1 holding
// the committed sub-write of client.4171.91:5 to shard 2 of PG 3.7 at the
// placement's epoch 25. No daemon produced it.
func subWriteReceiptSample() (subWriteReceipt, osdFixtureOracle, pgHistoryTarget) {
	_, route := heldSnapshot()
	key := pgHistoryTarget{Type: "client", Num: 4171, Inc: 91, TID: 5}
	return subWriteReceipt{OSD: 1, PGID: route.PGID, Epoch: route.Epoch, Reqid: key.String(), Writes: []subWriteRecord{{
		PGID: route.PGID, Shard: 2, MapEpoch: route.Epoch, MinEpoch: route.Epoch - 1, TID: 12, Reqid: key.String(), AtVersion: "25'3",
		Committed: true, Done: true, Events: []string{"initiated", "queued_for_pg", "reached_pg", "sub_op_started", "sub_op_committed", "done"},
	}}}, route, key
}

func TestCheckSubWriteCommitted(t *testing.T) {
	s, route, key := subWriteReceiptSample()
	if w, rolls, err := checkSubWriteCommitted(s, route, 1, 2, key); err != nil || w.AtVersion != "25'3" || len(rolls) != 0 {
		t.Fatal("committed sub-write refused:", w, rolls, err)
	}
	rolled, _, _ := subWriteReceiptSample()
	roll := rolled.Writes[0]
	roll.TID, roll.AtVersion, roll.PGCommittedTo = 13, subWriteRollForward, "25'3"
	rolled.Writes = append(rolled.Writes, roll)
	w, rolls, err := checkSubWriteCommitted(rolled, route, 1, 2, key)
	if _, judged := judgeRollForward(w, rolls, nil); err != nil || w.TID != 12 || judged != nil {
		t.Fatal("a write and its roll-forward refused:", w, rolls, err, judged)
	}
	before := slices.Clone(rolled.Writes)
	before[1].TID = 11
	if _, _, err := checkSubWriteCommitted(subWriteReceipt{OSD: 1, PGID: route.PGID, Epoch: route.Epoch, Reqid: key.String(), Writes: before}, route, 1, 2, key); err == nil {
		t.Error("a roll-forward before its write accepted")
	}
	only := subWriteReceipt{OSD: 1, PGID: route.PGID, Epoch: route.Epoch, Reqid: key.String(), Writes: []subWriteRecord{roll}}
	if _, _, err := checkSubWriteCommitted(only, route, 1, 2, key); err == nil {
		t.Error("a roll-forward without its write accepted")
	}
	for name, edit := range map[string]func(*subWriteReceipt){
		"another OSD":           func(s *subWriteReceipt) { s.OSD = 2 },
		"another PG":            func(s *subWriteReceipt) { s.PGID = "3.6" },
		"another request":       func(s *subWriteReceipt) { s.Reqid = "client.4171.91:6" },
		"an older epoch":        func(s *subWriteReceipt) { s.Epoch = route.Epoch - 1 },
		"none":                  func(s *subWriteReceipt) { s.Writes = nil },
		"two":                   func(s *subWriteReceipt) { s.Writes = append(s.Writes, s.Writes[0]) },
		"another shard":         func(s *subWriteReceipt) { s.Writes[0].Shard = 3 },
		"sent at another epoch": func(s *subWriteReceipt) { s.Writes[0].MapEpoch = route.Epoch + 1 },
		"min epoch after it":    func(s *subWriteReceipt) { s.Writes[0].MinEpoch = route.Epoch + 1 },
		"no min epoch":          func(s *subWriteReceipt) { s.Writes[0].MinEpoch = 0 },
		"zero tid":              func(s *subWriteReceipt) { s.Writes[0].TID = 0 },
		"not committed":         func(s *subWriteReceipt) { s.Writes[0].Committed = false },
		"not done":              func(s *subWriteReceipt) { s.Writes[0].Done = false },
		"a recovery write":      func(s *subWriteReceipt) { s.Writes[0].BackfillOrAsyncRecovery = true },
		"never started": func(s *subWriteReceipt) {
			s.Writes[0].Events = []string{"initiated", "sub_op_committed", "done"}
		},
		"committed before the start": func(s *subWriteReceipt) {
			s.Writes[0].Events = []string{"initiated", "sub_op_committed", "sub_op_started", "done"}
		},
	} {
		changed, _, _ := subWriteReceiptSample()
		edit(&changed)
		if _, _, err := checkSubWriteCommitted(changed, route, 1, 2, key); err == nil {
			t.Error("receipt accepted:", name)
		}
	}
	if _, _, err := checkSubWriteCommitted(s, route, 1, 1, key); err == nil {
		t.Error("a sub-write for another shard than asked accepted")
	}
}

func TestSameAtVersionAndHeldOnce(t *testing.T) {
	_, route := heldSnapshot()
	committed := map[int]subWriteRecord{1: {AtVersion: "25'3"}, 2: {AtVersion: "25'3"}}
	if err := sameAtVersion(committed, "25'3"); err != nil {
		t.Fatal(err)
	}
	committed[2] = subWriteRecord{AtVersion: "25'4"}
	if sameAtVersion(committed, "25'3") == nil {
		t.Error("a sub-write at another version accepted")
	}
	once := []pgHistoryLocation{{Tier: "log", Object: hobjectHead(route), Version: "25'3"}}
	if err := heldOnce(once, route, "25'3"); err != nil {
		t.Fatal(err)
	}
	for name, locations := range map[string][]pgHistoryLocation{
		"absent":          nil,
		"twice":           append(slices.Clone(once), once[0]),
		"a dup":           {{Tier: "dups", Object: hobjectHead(route), Version: "25'3"}},
		"another object":  {{Tier: "log", Object: "3:00000000:::other:head", Version: "25'3"}},
		"another version": {{Tier: "log", Object: hobjectHead(route), Version: "25'4"}},
	} {
		if heldOnce(locations, route, "25'3") == nil {
			t.Error("accepted:", name)
		}
	}
}

// historyReads is a baseline and a later read of osd.1's op history, one
// second apart in a history of size 20 and duration 600 s, the later one
// keeping every earlier op and two more. No daemon produced them.
func historyReads() (subWriteHistoryRead, subWriteHistoryRead) {
	start := time.Unix(1700014000, 0)
	before := subWriteHistoryRead{subWriteReceipt{OSD: 1, History: opHistoryRecord{Size: 20, Duration: 600, Ops: []string{"a", "b"}}}, start, start.Add(100 * time.Millisecond)}
	after := subWriteHistoryRead{subWriteReceipt{OSD: 1, History: opHistoryRecord{Size: 20, Duration: 600, Ops: []string{"a", "b", "c", "d"}}}, start.Add(time.Second), start.Add(1100 * time.Millisecond)}
	return before, after
}

func TestOpHistoryWitness(t *testing.T) {
	// The review's counterexample: size 3, duration 60 s. At t=100 the
	// baseline keeps only B while O, initiated at t=42, is in flight. O
	// completes at t=101, then the write W and its short roll-forward R;
	// inserting R makes four ops, and the shortest, R, is dropped. At t=103
	// O expires by its initiation time. The second read keeps B and W, below
	// size, with every baseline op kept: the snapshot-only rule accepted
	// these reads, and no witness may.
	start := time.Unix(1700000100, 0)
	before := subWriteHistoryRead{subWriteReceipt{OSD: 1, History: opHistoryRecord{Size: 3, Duration: 60, Ops: []string{"B"}}}, start, start}
	after := subWriteHistoryRead{subWriteReceipt{OSD: 1, History: opHistoryRecord{Size: 3, Duration: 60, Ops: []string{"B", "W"}}, Writes: []subWriteRecord{{TID: 12, AtVersion: "25'3"}}}, start.Add(3 * time.Second), start.Add(3 * time.Second)}
	retained := opHistoryWitness(before, after)
	if !errors.Is(retained, errOpHistoryInsertionsUnbounded) {
		t.Fatal("the review's counterexample was not left unbounded:", retained)
	}
	if proven, err := judgeNoRollForward(nil, retained); err != nil || proven {
		t.Fatal("the counterexample proved an absence:", proven, err)
	}
	if exact, err := judgeRollForward(after.Writes[0], nil, retained); !errors.Is(err, errRollForwardUnproven) || exact {
		t.Fatal("the counterexample's missing roll-forward was not left unproven:", exact, err)
	}
	// Consistent reads that keep every op below size are still unbounded.
	if b, a := historyReads(); !errors.Is(opHistoryWitness(b, a), errOpHistoryInsertionsUnbounded) {
		t.Error("consistent reads were taken as a bounded window")
	}
	for name, edit := range map[string]func(b, a *subWriteHistoryRead){
		"another OSD":        func(b, a *subWriteHistoryRead) { a.OSD = 2 },
		"another size":       func(b, a *subWriteHistoryRead) { a.History.Size = 30 },
		"another duration":   func(b, a *subWriteHistoryRead) { a.History.Duration = 300 },
		"no size":            func(b, a *subWriteHistoryRead) { b.History.Size, a.History.Size = 0, 0 },
		"reads out of order": func(b, a *subWriteHistoryRead) { a.read = b.sent.Add(-time.Second) },
	} {
		b, a := historyReads()
		edit(&b, &a)
		if err := opHistoryWitness(b, a); err == nil || errors.Is(err, errOpHistoryInsertionsUnbounded) {
			t.Error("inconsistent reads were not refused first:", name, err)
		}
	}
}

func TestJudgeRollForward(t *testing.T) {
	write := subWriteRecord{TID: 12, AtVersion: "25'3"}
	roll := subWriteRecord{TID: 13, AtVersion: subWriteRollForward, PGCommittedTo: "25'3"}
	lost := errors.New("osd.1 dropped an op of its history since the first read")
	if exact, err := judgeRollForward(write, []subWriteRecord{roll}, nil); err != nil || !exact {
		t.Fatal("one roll-forward in a kept history refused or not exact:", exact, err)
	}
	if exact, err := judgeRollForward(write, []subWriteRecord{roll}, lost); err != nil || exact {
		t.Fatal("one kept roll-forward in a history that may have dropped one claimed exact:", exact, err)
	}
	for name, rolls := range map[string][]subWriteRecord{
		"two":             {roll, roll},
		"another version": {{TID: 13, AtVersion: subWriteRollForward, PGCommittedTo: "25'2"}},
	} {
		for _, retained := range []error{nil, lost} {
			if _, err := judgeRollForward(write, rolls, retained); err == nil || errors.Is(err, errRollForwardUnproven) {
				t.Error("present roll-forwards accepted or left unproven:", name, retained, err)
			}
		}
	}
	if _, err := judgeRollForward(write, nil, nil); err == nil || errors.Is(err, errRollForwardUnproven) {
		t.Error("a roll-forward missing from a kept history was not refused:", err)
	}
	// The write is present, and the history may have dropped its
	// roll-forward: the coverage is unproven, not refused and not absent.
	s, route, key := subWriteReceiptSample()
	w, rolls, err := checkSubWriteCommitted(s, route, 1, 2, key)
	if err != nil || len(rolls) != 0 {
		t.Fatal("the committed write refused:", err)
	}
	if exact, err := judgeRollForward(w, rolls, lost); !errors.Is(err, errRollForwardUnproven) || exact {
		t.Error("a missing roll-forward in a history that may have dropped it was not left unproven:", exact, err)
	}
}

func TestJudgeNoRollForward(t *testing.T) {
	lost := errors.New("osd.1 keeps 20 ops, its whole history of 20")
	if proven, err := judgeNoRollForward(nil, nil); err != nil || !proven {
		t.Fatal("an absence in a kept history not proven:", proven, err)
	}
	if proven, err := judgeNoRollForward(nil, lost); err != nil || proven {
		t.Fatal("an absence in a history that may have dropped ops claimed proven:", proven, err)
	}
	roll := subWriteRecord{TID: 13, AtVersion: subWriteRollForward, PGCommittedTo: "25'3"}
	for _, retained := range []error{nil, lost} {
		if _, err := judgeNoRollForward([]subWriteRecord{roll}, retained); err == nil {
			t.Error("a roll-forward during the hold accepted:", retained)
		}
	}
}

// A stalled held-phase read must not keep the paused shard frozen. The
// fixture ends a lease by the deadline its pause request carries, beside a
// still running oracle request (integration/fixture/daemon_lease.go;
// TestDaemonLeaseExpiresBesideAStalledOwnedPoolOracle); on the test side the
// held phase ends the reserve before it, a read under it is abandoned there,
// and the test's resume is published and settled while the read is still
// outstanding. The test plays the fixture on a real control directory.
//
// Each read is proven started before the held phase ends: the stalled
// observation is a real owned-pool subwrites request, and the test waits
// for its envelope to be published; the stalled STAT is a send that signals
// its entry and then ignores its context until the test releases it, the
// worst case of a STAT under the held phase. After the resume settled, the
// observation's envelope is still pending, unclaimed and unanswered, and
// the STAT has still not returned. A read that started only after the
// resume fails the entry wait. The only waits are bounded polls for
// published files and channel receives.
func TestSubWriteHeldPhaseStallsEndThePauseByItsDeadline(t *testing.T) {
	for _, stall := range []string{"observation", "STAT"} {
		t.Run(stall, func(t *testing.T) {
			dir := t.TempDir()
			deadline := time.Now().Add(time.Minute)
			published := func(request string) []string {
				t.Helper()
				paths, err := filepath.Glob(filepath.Join(dir, request+".*.request"))
				if err != nil {
					t.Fatal(err)
				}
				return paths
			}
			// answer plays the fixture for one request of action: it claims
			// the oldest envelope and publishes record's acknowledgement. It
			// runs off the test goroutine, so it reports a failure on the
			// returned channel instead of failing the test itself.
			answer := func(action string, record func(r []string) testcluster.DaemonLeaseRecord) <-chan error {
				answered := make(chan error, 1)
				go func() {
					for range 10000 {
						paths, err := filepath.Glob(filepath.Join(dir, action+".*.request"))
						if err != nil {
							answered <- err
							return
						}
						if len(paths) != 0 {
							slices.Sort(paths)
							data, err := os.ReadFile(paths[0])
							if err == nil {
								err = os.Remove(paths[0])
							}
							var ack []byte
							r := record(strings.Fields(string(data)))
							if err == nil {
								ack, err = json.Marshal(r)
							}
							if err == nil {
								err = os.WriteFile(filepath.Join(dir, "daemon-lease-"+r.State+"."+r.Lease), ack, 0o600)
							}
							answered <- err
							return
						}
						time.Sleep(time.Millisecond)
					}
					answered <- fmt.Errorf("no %s request was published", action)
				}()
				return answered
			}
			paused := answer("pause-daemon", func(r []string) testcluster.DaemonLeaseRecord {
				return testcluster.DaemonLeaseRecord{Lease: r[1], Daemon: r[2] + "." + r[3], State: testcluster.DaemonLeasePaused, PausedAt: 1, Deadline: deadline.UnixNano()}
			})
			parent, end := context.WithCancel(context.Background())
			defer end()
			p := startHeldPause(t, parent, dir, "osd", "3", deadline, 10*time.Second)
			if err := <-paused; err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			returned := make(chan error, 1)
			read := func() {
				if stall == "observation" {
					returned <- testcluster.ControlDaemonInput(p.held, dir, "verify", "osd-pool", "subwrites", testcluster.ControlInput{Name: "osd-pool-input.json", Data: []byte(`{"osd":3}`)})
					return
				}
				close(entered)
				<-release
				returned <- p.check("the STAT of another object of the PG")
			}
			go read()
			// The read has started: the observation's request is published,
			// or the STAT signalled its entry.
			started := false
			for attempt := 0; attempt < 5000 && !started; attempt++ {
				if stall == "observation" {
					started = len(published("verify-osd-pool")) == 1
				} else {
					select {
					case <-entered:
						started = true
					default:
					}
				}
				if !started {
					time.Sleep(time.Millisecond)
				}
			}
			endRecord := func(r []string) testcluster.DaemonLeaseRecord {
				return testcluster.DaemonLeaseRecord{Lease: r[1], State: testcluster.DaemonLeaseEnded, Reason: testcluster.DaemonLeaseResumed, PausedAt: 1, EndedAt: deadline.UnixNano() - int64(time.Second), Deadline: deadline.UnixNano()}
			}
			if !started {
				// The lease's cleanup end is answered as the fixture would.
				answer("end-daemon-lease", endRecord)
				t.Fatal("the read did not start before the held phase was to end")
			}
			// The held phase ends (here through its parent; its deadline
			// does the same) with the read outstanding, and the test
			// resumes: the end must be published and settled while the read
			// is still outstanding.
			end()
			resumed := answer("end-daemon-lease", endRecord)
			if left, err := p.resume(); err != nil || left != time.Second {
				t.Fatal("the resume with a read outstanding", left, err)
			}
			if err := <-resumed; err != nil {
				t.Fatal(err)
			}
			if stall == "observation" {
				// Abandoned by the test at the held phase's end, the request
				// is still pending in the fixture: neither claimed nor
				// answered.
				acks, _ := filepath.Glob(filepath.Join(dir, "osd-pool-verify.*"))
				if len(published("verify-osd-pool")) != 1 || len(acks) != 0 {
					t.Fatal("the observation was answered or withdrawn before the resume settled", acks)
				}
			} else {
				select {
				case err := <-returned:
					t.Fatal("the STAT returned before the resume settled:", err)
				default:
				}
			}
			if p.lease.Deadline != deadline {
				t.Fatal("the lease does not carry the deadline the fixture enforces", p.lease.Deadline, deadline)
			}
			close(release)
			if err := <-returned; err == nil {
				t.Fatal("a read that outlived the held phase was accepted")
			}
		})
	}
}

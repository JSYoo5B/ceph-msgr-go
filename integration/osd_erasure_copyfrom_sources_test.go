package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Multi-chunk COPY_FROM and COPY_FROM2 from an optimized erasure-coded
// source, and a loss between the chunks of a copy onto an optimized
// erasure-coded destination. The barrier is holdAtChunk of
// osd_copyfrom_multichunk_sources_test.go. The pinned source (Ceph v20.2.4)
// gives the cells their meaning:
//
//   - do_copy_get (src/osd/PrimaryLogPG.cc:9295-9486) on an erasure-coded
//     source primary queues the chunk's data as an asynchronous read
//     (9318-9320 and 9364-9378) and answers the copy-get when it completes;
//     the source pool supports no OMAP (osd_types.h:1787), so the cursor's
//     OMAP is complete at once (9398-9399). The destination primary's chunk
//     limit is not rounded by the source's stripe width: get_copy_chunk_size
//     (PrimaryLogPG.h:1398-1407) reads the destination pool.
//   - _copy_some (PrimaryLogPG.cc:9564-9573) asserts the version the CopyOp
//     knows from its first chunk on, when the request pinned it
//     (PrimaryLogPG.h:184), and from the second chunk otherwise; ASSERT_VER
//     (6529-6542) fails it with -ERANGE once a write raised the source's user
//     version, and the destination answers that with reply_op_error
//     (344-368), which logs nothing.
//   - On an erasure-coded destination each shard records the temporary
//     object a sub-write adds (src/osd/ECBackend.cc:401-418), and
//     PGBackend::on_change_cleanup (src/osd/PGBackend.cc:507-521) removes it
//     at the interval change a lost OSD causes, as on a replica; on_change
//     (PrimaryLogPG.cc:13125-13216) cancels the copy and requeues the client
//     op on a primary that stays.

// copyErasureSourceCell is one opcode and one copy from the erasure-coded
// source: completed or with the head changed between the chunks, unversioned
// or pinned.
type copyErasureSourceCell struct {
	copyPrimaryChangeCase
	changed, pinned bool
}

func (c copyErasureSourceCell) label() string {
	source := "/data"
	if c.changed {
		source = "/head-change"
	}
	return c.name + source + copyPinnedSuffix(c.pinned)
}

// For COPY_FROM and COPY_FROM2, a copy from the head of an owned optimized
// isa k=2,m=2 pool onto a natively seeded head of an owned replicated pool
// on the four-OSD topology runs in two chunks under the destination
// primary's osd_copyfrom_max_chunk lowered to 32 KiB, and the barrier holds
// it before the second: the erasure-coded source primary is frozen while the
// first copy-get is in flight, and the second stays paused and unsent with
// one temporary object listed on every OSD of the destination PG. The copy
// is unversioned, or pinned to the user version a native read of the source
// gave, which the first copy-get must then assert too.
//
// Completed copy: after pause is cleared the same client op is answered once
// on its original connection with 0, the source's user version and ONDISK;
// the destination is natively the source (bytes, xattrs, user version,
// mtime), every OSD drops the temporary object, the destination primary's PG
// log holds the copy once bound to the source write, and a fresh session
// reads the source bytes with nothing changing later.
//
// Head changed between the chunks: a Go write replaces the source head, so
// the held copy-get's assertion fails and the copy is answered once with
// -ERANGE and per-op -EINPROGRESS; the seed is kept natively, every OSD drops
// the temporary object, the PG log names neither the copy nor a source write,
// and a fresh session reads the seed. The source head is natively the write.
//
// Not verified: snapshot sources of an erasure-coded pool, other widths and
// plugins as sources, unoptimized EC sources, and an erasure-coded source
// onto an erasure-coded destination.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2copyfromsourcechunks key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDErasureOptimizedK2M2CopyFromSourceChunksIntegration(t *testing.T) {
	ctx, pools, recovery, facts := copyErasureChunkFixture(t, 1800*time.Second, erasureK2M2, true)
	t.Logf("source pool %d: %s, stripe width %d, nonprimary shards %v", facts.pool, erasureK2M2, facts.chunk*erasureK2M2.k, facts.nonprimary)
	keep := copyChunkReceipts(t, recovery, "osd-erasure-copyfrom-source-chunks-results.json")
	for _, tc := range copyPrimaryChangeCases {
		for _, changed := range []bool{false, true} {
			for _, pinned := range []bool{false, true} {
				cell := copyErasureSourceCell{tc, changed, pinned}
				t.Run(cell.label(), func(t *testing.T) {
					mutation := "none"
					if changed {
						mutation = "head-write"
					}
					receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: mutation, Chunk: multiChunkLimit}
					defer func() {
						if !t.Skipped() {
							keep(receipt)
						}
					}()
					copyErasureSource(t, ctx, pools, recovery, facts, cell, &receipt)
				})
			}
		}
	}
}

func copyErasureSource(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, facts erasureFacts, cell copyErasureSourceCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecsrc-%d-%t%s", cell.code, cell.changed, copyPinnedSuffix(cell.pinned)))
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	facts.checkPlacement(t, r.srcRoute)
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*41 + i>>8 + 13)
	}
	write := r.writeSourceHead("source write", 1700009600, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != write.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, write.UserVersion)
	}
	receipt.SourceBytes, receipt.CopiedUser, receipt.Cursors = len(data), copied.Version, []int{0, 0}
	sourceKeys := []pgHistoryTarget{{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	firstAssert := pinCopy(&request, cell.pinned, copied.Version)
	receipt.SourceVersion = request.Operations[0].CopyFrom.SourceVersion
	// The source has no OMAP: both copy-gets carry an empty OMAP position.
	h := r.holdAtChunk(request, route, src, 2, firstAssert, copied.Version, receipt.Cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	want, user := int32(0), copied.Version
	var changed cephmsgr.OSDReply
	if cell.changed {
		changed = r.writeSourceHead("head write between chunks", 1700009620, []byte("EC source changed between chunks"), []byte{4}, cephmsgr.OSDSnapContext{})
		receipt.MutationUser = changed.UserVersion
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: changed.Transaction})
		if changed.UserVersion <= copied.Version {
			t.Fatalf("the write between chunks has user version %d, not above the copied %d", changed.UserVersion, copied.Version)
		}
		want, user = -34, 0
	}
	reply := r.awaitHeld(h, receipt)
	if err := judgeChunkAnswer(want, reply, h.op.TID, user); err != nil {
		t.Fatal(err)
	}
	// The source is observed natively only now that pauserd is clear.
	head := poolObserve(t, ctx, 1, r.source, "", "")
	switch {
	case cell.changed && (head.Code != 0 || head.Version != changed.UserVersion || string(head.Data) != "EC source changed between chunks"):
		t.Fatalf("native source head is not the write between chunks: code %d version %d (reply %d)", head.Code, head.Version, changed.UserVersion)
	case !cell.changed && !sameNative(head, copied):
		t.Fatalf("the copy changed its source: %s; was %s", nativeDigest(head), nativeDigest(copied))
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, true, receipt)
}

// copyErasureLossReceipt is the archived public receipt of one cell of the
// erasure-coded destination loss test.
type copyErasureLossReceipt struct {
	copyChunkLossReceipt
	Width    string          `json:"width"`
	Steps    []copyChunkStep `json:"chunks"`
	Lost     int32           `json:"lost_osd"`
	Shard    int             `json:"lost_shard"`
	CopyGets int             `json:"copy_get_max"`
	Held     *copyHeldCommit `json:"held_commit,omitempty"`
}

// copyHeldCommit is the part of a receipt that records a final copy
// transaction held before the destination primary's local commit.
type copyHeldCommit struct {
	Blocker          string   `json:"blocker_object"`
	BlockerShard     int      `json:"blocker_read_shard"`
	Frozen           int32    `json:"frozen_osd"`
	IntervalEpoch    uint32   `json:"cache_discarded_by_epoch"`
	IntervalSince    uint32   `json:"destination_interval_since"`
	BlockerReqid     string   `json:"blocker_reqid"`
	BlockerPoint     string   `json:"blocker_flag_point"`
	CopyPoint        string   `json:"copy_flag_point"`
	CopyEvents       []string `json:"copy_events"`
	Observations     int      `json:"held_observations"`
	HeldFor          string   `json:"commit_held_for"`
	FrozenFor        string   `json:"frozen_for"`
	PrimaryStaging   string   `json:"primary_staging_sha256"`
	BlockerOutcome   string   `json:"blocker_outcome"`
	BlockerSubmitTID uint64   `json:"blocker_submission_tid"`
	BlockerNative    string   `json:"blocker_native_after"`
}

// copyHeldBlockerIncarnation is the client incarnation of the write that
// holds the PG's EC write queue.
const copyHeldBlockerIncarnation = 35

// copyErasureLostShard picks the OSD a shard cell loses: a member of the
// destination's acting set that is neither its primary nor the source
// primary, preferring one outside the source PG's acting set. It returns the
// OSD and the shard it holds.
func copyErasureLostShard(dst, src osdFixtureOracle) (int32, int, error) {
	pick := -1
	for shard, osd := range dst.Acting {
		if osd == dst.ID || osd == src.ID || osd == crushItemNone {
			continue
		}
		if pick < 0 || slices.Contains(src.Acting, dst.Acting[pick]) && !slices.Contains(src.Acting, osd) {
			pick = shard
		}
	}
	if pick < 0 {
		return -1, -1, fmt.Errorf("no shard of %v is neither the primary osd.%d nor the source primary osd.%d", dst.Acting, dst.ID, src.ID)
	}
	return dst.Acting[pick], pick, nil
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of a source of two
// chunks of data from an owned replicated pool onto a natively seeded head
// of an owned optimized isa k=2,m=2 pool on the four-OSD topology is held
// before its second chunk as in the destination cells above (the barrier,
// one temporary object in every shard collection). Then one OSD of the
// destination PG is SIGKILLed and marked down; three shards keep the PG at
// its min_size.
//
// Destination primary lost: the caller's wait ends as an unknown outcome
// carrying the barrier's transaction, and the seed is kept natively. The PG
// moves to another primary (under a pg_temp, as an optimized pool leads from
// a shard that may be primary), whose PG log names neither the copy nor the
// source write, and every surviving shard drops the temporary object. A
// second Go write replaces the source, and one explicit caller
// resubmission to the new primary executes the copy once, copying the
// second source. After recovery the original primary leads again with that
// single copy (fresh-session read, delayed native readback and PG log), and
// no shard lists a temporary object.
//
// A shard lost (neither primary, outside the source PG when the placement
// allows): the destination primary cancels the copy at the interval change,
// requeues the client op and restarts the copy from a first copy-get, held
// unsent under pauserd, with the temporary object gone from every surviving
// shard. After pauserd is cleared the copy runs both chunks on the degraded
// PG, and the same client op is answered once on its original connection
// with the source version; the destination is the complete copy natively,
// logged once and bound to the source write, and after recovery the
// restarted shard lists no temporary object.
//
// Destination primary lost before its local commit: the last chunk is let
// through while an earlier write of the PG holds its EC write queue, and the
// primary is lost with the copy's final transaction not applied
// (copyErasureHeldCommitLoss below).
//
// Not verified: other widths under a loss, and a source primary lost between
// chunks onto an erasure-coded destination.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2copyfromchunkloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDErasureOptimizedK2M2CopyFromChunkLossIntegration(t *testing.T) {
	width := erasureK2M2
	ctx, pools, recovery, facts := copyErasureChunkFixture(t, 1800*time.Second, width, false)
	limit := erasureCopyGetMax(facts, width)
	t.Logf("destination pool %d: %s, stripe width %d, copy-get max %d, nonprimary shards %v", facts.pool, width, facts.chunk*width.k, limit, facts.nonprimary)
	var mu sync.Mutex
	var receipts []copyErasureLossReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-erasure-copyfrom-chunk-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("EC chunk loss receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, lost := range []string{"destination", "shard", "destination-before-commit"} {
			cell := copyChunkLossCell{tc, lost}
			t.Run(cell.name+"/"+lost, func(t *testing.T) {
				receipt := copyErasureLossReceipt{copyChunkLossReceipt: copyChunkLossReceipt{Cell: cell.name + "/" + lost, Chunk: uint64(limit), Paused: -1}, Width: width.name, Lost: -1, Shard: -1, CopyGets: limit}
				defer func() {
					if t.Skipped() {
						return
					}
					mu.Lock()
					receipts = append(receipts, receipt)
					mu.Unlock()
				}()
				if lost == "destination-before-commit" {
					copyErasureHeldCommitLoss(t, ctx, pools, recovery, facts, limit, cell, &receipt)
					return
				}
				copyErasureChunkLoss(t, ctx, pools, recovery, facts, limit, cell, &receipt)
			})
		}
	}
}

func copyErasureChunkLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, facts erasureFacts, limit int, cell copyChunkLossCell, receipt *copyErasureLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecloss-%d-%s", cell.code, cell.lost))
	facts.checkPlacement(t, r.dstRoute)
	r.chunkMax = limit
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, 2*limit)
	for i := range data {
		data[i] = byte(i*43 + i>>7 + 17)
	}
	write := r.writeSourceHead("source write", 1700009700, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	receipt.SourceBytes, receipt.SourceUser = len(data), write.UserVersion

	route, src := r.placeForCopy()
	h := r.holdAtChunk(r.copyRequest(cell.code, route), route, src, 2, 0, write.UserVersion, []int{0, 0}, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}
	if cell.lost == "destination" {
		receipt.Lost, receipt.Shard = route.ID, int(route.Shard)
		r.loseDestinationBetweenChunks(h, cell.code, seeded, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	} else {
		lost, shard, err := copyErasureLostShard(route, src)
		if err != nil {
			t.Fatal(err)
		}
		receipt.Lost, receipt.Shard = lost, shard
		r.loseReplicaBetweenChunks(h, lost, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	}
	// The restarted OSD cleared every temporary object when it started; the
	// others removed theirs at the interval change or the final rename.
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
}

func TestCopyFromErasureSourceJudgements(t *testing.T) {
	for _, c := range []struct {
		cell copyErasureSourceCell
		want string
	}{
		{copyErasureSourceCell{copyPrimaryChangeCases[0], false, false}, "COPY_FROM/data"},
		{copyErasureSourceCell{copyPrimaryChangeCases[1], false, true}, "COPY_FROM2/data-pinned"},
		{copyErasureSourceCell{copyPrimaryChangeCases[0], true, false}, "COPY_FROM/head-change"},
		{copyErasureSourceCell{copyPrimaryChangeCases[1], true, true}, "COPY_FROM2/head-change-pinned"},
	} {
		if got := c.cell.label(); got != c.want {
			t.Errorf("label %q, want %q", got, c.want)
		}
	}

	dst := osdFixtureOracle{Acting: []int32{1, 0, 2, 3}, ID: 1}
	for _, c := range []struct {
		src   osdFixtureOracle
		osd   int32
		shard int
	}{
		// osd.3 is outside the source PG, so it is preferred.
		{osdFixtureOracle{Acting: []int32{0, 2, 1}, ID: 0}, 3, 3},
		// Every candidate replicates the source: the first one.
		{osdFixtureOracle{Acting: []int32{2, 3, 0}, ID: 2}, 0, 1},
		{osdFixtureOracle{Acting: []int32{0, 3, 2}, ID: 0}, 2, 2},
	} {
		osd, shard, err := copyErasureLostShard(dst, c.src)
		if err != nil || osd != c.osd || shard != c.shard {
			t.Errorf("lost shard for source %v: osd.%d shard %d (%v); want osd.%d shard %d", c.src.Acting, osd, shard, err, c.osd, c.shard)
		}
	}
	if _, _, err := copyErasureLostShard(osdFixtureOracle{Acting: []int32{1, 0}, ID: 1}, osdFixtureOracle{ID: 0}); err == nil {
		t.Error("a shard was picked where only the two primaries hold the PG")
	}
	if osd, _, err := copyErasureLostShard(osdFixtureOracle{Acting: []int32{1, crushItemNone, 2}, ID: 1}, osdFixtureOracle{ID: 0}); err != nil || osd != 2 {
		t.Error("an empty position was picked", osd, err)
	}

	// The held final transaction: the source read is complete, the client op
	// is unanswered and the primary's own shard still lists the temporary
	// object.
	const gid = 4641
	route := osdFixtureOracle{Up: []int32{1, 0, 2, 3}, Acting: []int32{1, 0, 2, 3}, Replicas: 4, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: 0, PoolType: 3, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	temp := "#-6:c7a5d36b:::temp_4.3s0_0_4123_1:head#"
	ops := opsInFlightSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.3s0", Epoch: 30, FlagPoint: "started", Events: []string{"queued_for_pg", "reached_pg", "started"}}}}
	objecter := objecterRequestsSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3"}
	staging := copyStagingSnapshot{OSD: 1, PGID: "4.3", Shard: 0, Collection: "4.3s0_head", Max: 4096, Objects: 3, Complete: true, Temp: []string{"0" + temp}}
	if op, err := checkCommitHeld(ops, objecter, staging, route, gid, 5, temp); err != nil || op.TID != 5 {
		t.Fatal("held commit refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot, *copyStagingSnapshot){
		"source read pending": func(_ *opsInFlightSnapshot, o *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops = []objecterRequestsOp{{TID: 9, PG: "3.5", OSD: 0, Attempts: 1}}
		},
		"temporary renamed": func(_ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, s *copyStagingSnapshot) { s.Temp = nil },
		"another temporary": func(_ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, s *copyStagingSnapshot) {
			s.Temp = []string{"0#-6:c7a5d36b:::temp_4.3s0_0_4123_2:head#"}
		},
		"copy answered": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) { o.Ops = nil },
		"commit sent": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops[0].Events = append(slices.Clone(o.Ops[0].Events), "commit_sent")
		},
		"another op": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops[0].TID, o.Ops[0].Reqid = 6, "client.4641.33:6"
		},
		"absence claimed": func(_ *opsInFlightSnapshot, o *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.AbsenceEstablished = true
		},
		"incomplete listing": func(_ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, s *copyStagingSnapshot) { s.Complete = false },
	} {
		o, b, st := ops, objecter, staging
		o.Ops = slices.Clone(ops.Ops)
		edit(&o, &b, &st)
		if _, err := checkCommitHeld(o, b, st, route, gid, 5, temp); err == nil {
			t.Error("held commit accepted:", name)
		}
	}

	// A k=3 destination rounds the lowered chunk up: every copy-get prints
	// that max, and one printing the configured 32 KiB is not the chunk.
	limit := copyErasureChunkLimit(multiChunkLimit, 3*4096)
	rounded := []string{"assert-version v5", fmt.Sprintf("copy-get max %d in=29b", limit)}
	if limit != 36864 || !copyLaterChunk(rounded, limit, 5, 0) || copyLaterChunk(rounded, multiChunkLimit, 5, 0) ||
		copyLaterChunk([]string{"assert-version v5", "copy-get max 32768 in=29b"}, limit, 5, 0) || !copyChunkShape(rounded[1:], limit, 0, 0) {
		t.Fatal("rounded copy-get max misjudged")
	}
	if erasureCopyGetMax(erasureFacts{chunk: 4096}, erasureK3M1) != 36864 || erasureCopyGetMax(erasureFacts{chunk: 4096}, erasureK2M2) != multiChunkLimit ||
		erasureCopyGetMax(erasureFacts{chunk: 16384}, erasureK4M2) != 65536 {
		t.Fatal("erasure copy-get max misjudged")
	}
}

// sourceBesides gives the source a fresh name, when it needs one, whose
// primary is none of avoid, and returns that primary.
func (r *copyPrimaryChangeRun) sourceBesides(avoid ...int32) int32 {
	t, ctx := r.t, r.ctx
	t.Helper()
	base := r.source
	for i := 0; slices.Contains(avoid, r.srcRoute.ID); i++ {
		if i == 64 {
			t.Fatalf("no fresh source name in 64 has a primary other than %v", avoid)
		}
		name := fmt.Sprintf("%s-%d", base, i)
		route := poolPlace(t, ctx, 1, name, "")
		if route.PrimaryID == route.ID && poolObserve(t, ctx, 1, name, "", "").Code == -2 {
			r.source, r.srcRoute = name, route
		}
	}
	return r.srcRoute.ID
}

// blockerObject names an object of the destination's PG, on its placement,
// that does not exist yet.
func (r *copyPrimaryChangeRun) blockerObject() (string, osdFixtureOracle) {
	t, ctx := r.t, r.ctx
	t.Helper()
	for i := 0; i < 256; i++ {
		name := fmt.Sprintf("%s-blocker-%d", r.dest, i)
		route := poolPlace(t, ctx, 2, name, "")
		if route.PGID == r.dstRoute.PGID && route.ID == r.dstRoute.ID && slices.Equal(route.Acting, r.dstRoute.Acting) && poolObserve(t, ctx, 2, name, "", "").Code == -2 {
			return name, route
		}
	}
	t.Fatalf("no fresh name in 256 maps to the destination PG %s", r.dstRoute.PGID)
	return "", osdFixtureOracle{}
}

// checkCommitHeld judges one round of observations of a copy whose source
// read completed and whose final transaction was not applied on the
// destination primary: its Objecter holds no op of the source any more, its
// op tracker still holds the copy's client op (checkCopyClientOp) unanswered,
// and its own shard collection still lists the temporary object, which the
// final transaction renames over the destination.
func checkCommitHeld(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, staging copyStagingSnapshot, route osdFixtureOracle, gid uint64, tid uint64, temp string) (opsInFlightOp, error) {
	op, err := checkCopyClientOp(ops, objecter, route, gid, copyPrimaryCopyIncarnation)
	if err != nil {
		return op, err
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished || len(objecter.Ops) != 0 {
		return op, fmt.Errorf("the destination primary's Objecter still holds %d ops of the source", len(objecter.Ops))
	}
	if op.TID != tid || slices.Contains(op.Events, "commit_sent") || strings.HasPrefix(op.FlagPoint, "commit sent") {
		return op, fmt.Errorf("the client op is %s at %q, not the held copy tid %d", op.Reqid, op.FlagPoint, tid)
	}
	got, err := checkCopyTemp(staging, route.ID, route, 1)
	if err != nil {
		return op, err
	}
	if got != temp {
		return op, fmt.Errorf("the primary lists %q, not the staged %q", got, temp)
	}
	return op, nil
}

// For COPY_FROM and COPY_FROM2 onto the optimized isa k=2,m=2 destination,
// the interval between the copy's source read completing and the destination
// primary's local commit is held, and the primary is lost there. The pinned
// source gives the hold:
//
//   - CopyFromCallback::finish (src/osd/PrimaryLogPG.cc:344-368) runs the
//     copy's final transaction as soon as the last chunk completes the
//     cursor; on an erasure-coded PG it enters start_rmw
//     (src/osd/ECCommon.cc:748-771), whose cache ops join the PG's one FIFO
//     (ECExtentCache::execute, src/osd/ECExtentCache.cc:331-337). Only
//     cache_ready (ECCommon.cc:773-916) generates the shard transactions and
//     queues the local one, and cache_maybe_ready (ECExtentCache.cc:216-242)
//     calls it in order, stopping at the first op whose reads are not done.
//   - A write of part of a page of an existing stripe must read that page
//     first (ECTransaction.cc:146-178), unless the extent cache holds it; an
//     interval change discards the cache (ECBackend::on_change,
//     src/osd/ECBackend.cc:855-860; ECExtentCache.cc:322-323).
//
// So: an object of the same PG (the blocker) is seeded natively with one
// stripe, and the live OSD of shard 1 is marked down once, which ends the
// PG's interval twice as it marks itself up again. The copy is held
// before its second chunk by the barrier. The OSD of shard 1, a data shard
// that is neither primary, is frozen under a fixture lease; a Go write of 16
// bytes inside shard 1's first page of the blocker is sent and observed in
// the primary's op tracker. pauserd is cleared. Twice, two seconds apart,
// bracketed snapshots then show the source read complete (no op of the
// source in the primary's Objecter), the copy's client op unanswered, and
// the temporary object still listed in the primary's own shard collection:
// the final transaction was not applied there. Those reads are bracketed by
// placement only, so a copyinterval observation after the second round
// must show the PG still in the interval the copy was sent in (a same-member
// interval change would have canceled the copy, emptying the Objecter while
// the staged object stays listed). The primary is SIGKILLed,
// the frozen OSD resumed, and the primary marked down.
//
// The copy's wait ends as an unknown outcome with the barrier's
// transaction, and so does the blocker's. The seed is kept natively, the new
// primary's PG log names neither the copy nor the source write, every
// surviving shard drops the temporary object, and one explicit resubmission
// of the copy to the new primary executes once. The blocker keeps its seed
// natively. After recovery the original primary leads with that single copy
// and no shard lists a temporary object.
//
// Not verified: the same interval on a replicated destination (its final
// transaction is queued locally in the same call, ReplicatedBackend.cc:
// 551-640, and no stock control holds a BlueStore commit; see #7), other
// widths, and a loss of a replica in this interval.
func copyErasureHeldCommitLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, facts erasureFacts, limit int, cell copyChunkLossCell, receipt *copyErasureLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("echeld-%d", cell.code))
	facts.checkPlacement(t, r.dstRoute)
	r.chunkMax = limit
	primary, frozen := r.dstRoute.ID, r.dstRoute.Acting[1]
	held := &copyHeldCommit{Frozen: frozen, BlockerShard: 1}
	receipt.Held, receipt.Lost, receipt.Shard = held, primary, int(r.dstRoute.Shard)
	receipt.SourcePrimary, receipt.Primary = r.sourceBesides(primary, frozen), primary
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	// The blocker: one stripe, seeded before the interval change so that no
	// page of it is in the primary's extent cache afterwards.
	blocker, blockerRoute := r.blockerObject()
	held.Blocker = blocker
	stripe := make([]byte, facts.chunk*2)
	for i := range stripe {
		stripe[i] = byte(i*47 + i>>9 + 19)
	}
	blockerSeed := poolSeed(t, ctx, 2, blocker, "", stripe, nil)
	// The live shard OSD is marked down once and marks itself up again; both
	// interval changes discard the primary's extent cache. Its daemon is not
	// stopped, but it binds new addresses, so restore republishes routing.
	r.recovery.restarted = true
	r.recovery.markDown(t, ctx, frozen)
	for deadline := time.Now().Add(120 * time.Second); ; {
		now := poolPlace(t, ctx, 2, blocker, "")
		if samePlacement(now, blockerRoute) && now.Epoch >= blockerRoute.Epoch+2 {
			held.IntervalEpoch = now.Epoch
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the PG did not return to its placement after osd.%d was marked down: %+v", frozen, now)
		}
		time.Sleep(time.Second)
	}
	waitOSDFixtureClean(t, ctx, r.recovery.base)
	t.Logf("osd.%d marked down and up again: pg %s back at epoch %d; blocker %s seeded with %d bytes before", frozen, blockerRoute.PGID, held.IntervalEpoch, blocker, len(stripe))

	data := make([]byte, 2*limit)
	for i := range data {
		data[i] = byte(i*53 + i>>7 + 23)
	}
	write := r.writeSourceHead("source write", 1700009800, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	receipt.SourceBytes, receipt.SourceUser = len(data), write.UserVersion
	route, src := r.placeForCopy()
	h := r.holdAtChunk(r.copyRequest(cell.code, route), route, src, 2, 0, write.UserVersion, []int{0, 0}, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}

	start := time.Now()
	// Resumed well inside the 20 s heartbeat grace and the 16 s read lease;
	// the fixture resumes it at the lease's deadline all the same.
	thawDeadline := start.Add(16 * time.Second)
	work, stopWork := context.WithDeadline(ctx, thawDeadline.Add(-3*time.Second))
	defer stopWork()
	r.recovery.freeze(t, work, frozen, thawDeadline)
	blockerAt := poolPlace(t, work, 2, blocker, "")
	o := mutationConnection(t, r.client, ctx, blockerAt)
	t.Cleanup(func() { o.Close() })
	request := r.pools.request(blockerAt, cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: uint64(facts.chunk) + 100, Length: 16, Data: []byte("blocks the queue")})
	request.ClientIncarnation, request.ModificationTime = copyHeldBlockerIncarnation, cephmsgr.OSDTime{Seconds: 1700009810}
	blocked := make(chan copyChunkOutcome, 1)
	call, cancel := context.WithTimeout(ctx, 300*time.Second)
	t.Cleanup(cancel)
	go func() {
		reply, err := o.Request(call, request)
		blocked <- copyChunkOutcome{reply, err}
	}()
	seen, raw := -1, ""
	for {
		if copyBudgetSpent(work) {
			t.Fatalf("the blocker write was not in flight within the freeze budget: the last snapshot held %d of its ops\nop tracker: %s", seen, raw)
		}
		ops := copyOpsInFlight(t, work, 2, blocker, nil, r.gid, copyHeldBlockerIncarnation)
		if ops.Complete && !ops.AbsenceEstablished && len(ops.Ops) == 1 && !slices.Contains(ops.Ops[0].Events, "commit_sent") && work.Err() == nil {
			held.BlockerReqid, held.BlockerPoint = ops.Ops[0].Reqid, ops.Ops[0].FlagPoint
			break
		}
		seen, raw = len(ops.Ops), boundedRaw(ops.Raw)
		time.Sleep(100 * time.Millisecond)
	}
	r.recovery.clearPauseReads(t, work)
	var first time.Time
	verdict, raws := errors.New("no observation"), ""
	for held.Observations < 2 {
		h.pending(t, "while its final transaction was held")
		if copyBudgetSpent(work) {
			t.Fatalf("the final copy transaction was not held within the freeze budget: %v%s", verdict, raws)
		}
		select {
		case got := <-blocked:
			t.Fatalf("the blocker write was answered while osd.%d was frozen: code %d err %v", frozen, got.reply.Code, got.err)
		default:
		}
		ops := copyOpsInFlight(t, work, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		staging := copyStaging(t, work, primary, route)
		op, err := checkCommitHeld(ops, objecter, staging, route, r.gid, h.op.TID, h.temp)
		if err == nil && work.Err() == nil {
			held.Observations++
			held.CopyPoint, held.CopyEvents, held.PrimaryStaging = op.FlagPoint, op.Events, staging.SHA256
			if held.Observations == 1 {
				first = time.Now()
				time.Sleep(2 * time.Second)
			}
			continue
		}
		if held.Observations != 0 {
			t.Fatalf("the hold ended before the loss: %v", err)
		}
		verdict, raws = err, fmt.Sprintf("\nop tracker: %s\nobjecter: %s", boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
		time.Sleep(100 * time.Millisecond)
	}
	// Each round's reads are bracketed by placement only. The interval the
	// copy was sent in must have held through both rounds: its start only
	// grows, so one observation after the second round covers them.
	held.IntervalSince = r.requireIntervalHeld(work, route, "the held final transaction")
	held.HeldFor = time.Since(first).Round(10 * time.Millisecond).String()
	t.Logf("source read complete and final transaction held on osd.%d for %s: copy %s at %q, blocker %s at %q, temporary %s still listed",
		primary, held.HeldFor, h.op.Reqid, held.CopyPoint, held.BlockerReqid, held.BlockerPoint, h.temp)
	receipt.KillExit = r.recovery.kill(t, ctx, primary)
	if err := r.recovery.thaw(frozen); err != nil {
		t.Fatalf("resume osd.%d within the freeze budget: %v", frozen, err)
	}
	stopWork()
	held.FrozenFor = time.Since(start).Round(10 * time.Millisecond).String()

	var lost copyChunkOutcome
	select {
	case lost = <-blocked:
	case <-time.After(60 * time.Second):
		t.Fatal("the blocker's wait did not end after the primary was killed")
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyHeldBlockerIncarnation {
		t.Fatalf("the blocker did not end as an unknown outcome: %+v %v", lost.reply, lost.err)
	}
	held.BlockerOutcome, held.BlockerSubmitTID = outcomeCause(lost.err), lost.reply.Submission.Transaction
	o.Close()
	r.settleDestinationLoss(h, cell.code, seeded, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	after := poolObserve(t, ctx, 2, blocker, "", "")
	held.BlockerNative = nativeDigest(after)
	if !sameNative(after, blockerSeed) {
		t.Fatalf("the held blocker write reached the blocker: %s; seed %s", nativeDigest(after), nativeDigest(blockerSeed))
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
}

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
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

// copyErasurePairLossReceipt adds both pool identities and independently
// observed source/seed state to the existing loss and cleanup observations.
// It contains no connection addresses or credentials.
type copyErasurePairLossReceipt struct {
	copyErasureLossReceipt
	SourcePool        int64                  `json:"source_pool"`
	DestinationPool   int64                  `json:"destination_pool"`
	SourceWidth       string                 `json:"source_width"`
	SourceActing      []int32                `json:"source_acting"`
	DestinationActing []int32                `json:"destination_acting"`
	LostSourceShard   int                    `json:"lost_source_shard"`
	SourceBefore      string                 `json:"source_before"`
	SourceAfter       string                 `json:"source_after_recovery"`
	Seed              string                 `json:"destination_seed"`
	SourceVersion     uint64                 `json:"requested_source_version"`
	MovedSource       *copyErasureSourceMove `json:"moved_source,omitempty"`
	// SnapID and Clone name the copied clone of a snapshot cell.
	SnapID uint64 `json:"snap_id,omitempty"`
	Clone  string `json:"clone,omitempty"`
	// ShardHistory is the offline capture of every acting shard's own log
	// after recovery (osd_erasure_copyfrom_loss_history_test.go).
	ShardHistory []copyLossShardReceipt `json:"shard_history,omitempty"`
}

type copyErasureSourceMove struct {
	PGID          string  `json:"pgid"`
	Primary       int32   `json:"primary"`
	Shard         int8    `json:"shard"`
	Acting        []int32 `json:"acting"`
	Epoch         uint32  `json:"epoch"`
	IntervalSince uint32  `json:"interval_since"`
}

// copyErasurePairLossCell is one opcode, the OSD lost and the source of an
// EC-to-EC copy: the head or a clone, unversioned or pinned.
type copyErasurePairLossCell struct {
	copyPrimaryChangeCase
	// lost is destination, shard or source between chunks, or
	// destination-before-commit or replica-before-commit in the held
	// interval before the destination primary's local commit.
	lost             string
	pinned, snapshot bool
}

// held says whether the cell holds the copy's final transaction
// (copyErasureHeldCommitLoss) instead of holding it between chunks.
func (c copyErasurePairLossCell) held() bool {
	return c.lost == "destination-before-commit" || c.lost == "replica-before-commit"
}

func (c copyErasurePairLossCell) label() string {
	source := ""
	if c.snapshot {
		source = "snapshot/"
	}
	return c.name + "/" + source + c.lost + copyPinnedSuffix(c.pinned)
}

// copyErasurePairHeadLossCells are the head cells every pair runs: the
// destination primary, a shared shard and the source primary lost between
// chunks (the last also pinned), the destination primary lost before its
// local commit (also pinned), and a destination replica lost there.
func copyErasurePairHeadLossCells() []copyErasurePairLossCell {
	var cells []copyErasurePairLossCell
	for _, tc := range copyPrimaryChangeCases {
		cells = append(cells,
			copyErasurePairLossCell{tc, "destination", false, false},
			copyErasurePairLossCell{tc, "shard", false, false},
			copyErasurePairLossCell{tc, "source", false, false},
			copyErasurePairLossCell{tc, "source", true, false},
			copyErasurePairLossCell{tc, "destination-before-commit", false, false},
			copyErasurePairLossCell{tc, "destination-before-commit", true, false},
			copyErasurePairLossCell{tc, "replica-before-commit", false, false})
	}
	return cells
}

// COPY_FROM and COPY_FROM2 lose an OSD between two chunks of a head copy
// from an owned optimized isa k=2,m=2 source onto an owned optimized
// isa k=3,m=1 destination. The four OSDs hold both PGs, with distinct primaries;
// each loss therefore removes one shard from each pool. Both pools retain
// min_size (three shards). The source is two destination-rounded chunks
// (36864 bytes each), independently read before the copy; the destination
// starts with a distinguishable native seed.
//
// holdAtChunk establishes the existing source-backed barrier: the second
// copy-get asserts the source version, is paused and never sent, and the same
// temporary object is listed in all four destination shard collections. The
// destination's interval must still be the one the copy was sent in. Only
// then is an ordinary daemon SIGKILL injected, with one of three targets:
//
//   - Destination primary: the caller retains an unknown outcome with the
//     original GlobalID/incarnation/TID/epoch. Before any resubmission the seed
//     is unchanged, no copy identity is in the new primary's observed PG log,
//     and surviving shards have removed staging. An explicit caller-selected
//     resubmission with the original identity and attempt 1 then copies a
//     distinguishable second source once, bound to its write in the PG log.
//   - Shared non-primary shard: neither primary is lost. The destination's
//     interval change cancels the partial copy and restarts its first chunk
//     under the same client op. That new copy-get is observed unsent under
//     pause, with staging removed on the survivors. The original connection
//     then receives one successful attempt-0 reply and the whole first source.
//   - Source primary: the destination stays on its original primary and
//     restarts its first copy-get. The source PG answers natively on a new
//     primary shard in a newer interval while reads remain paused, with
//     three survivors in their original slots. Both unversioned and pinned
//     head copies complete on the original client request. In the pinned
//     case, the first copy-get carries the same version assertion both
//     before and after loss.
//
// A fourth cell loses the destination primary after the source read is
// complete and before the copy's final transaction is generated: the last
// chunk is let through while an earlier partial-page write of the
// destination PG waits for a read from a frozen data shard, which holds the
// PG's one EC write queue (copyErasureHeldCommitLoss,
// osd_erasure_copyfrom_sources_test.go). The source is named so that the
// frozen OSD holds none of its data shards, so the copy's own source read
// never touches it. The copy and the blocker end as unknown outcomes, the
// seed and the blocker's seed are kept, and one explicit resubmission
// executes once. The destination's final transaction is generated only in
// ECCommon::cache_ready (src/osd/ECCommon.cc:773-916), so no shard
// transaction of the copy exists when the primary is lost; a replicated
// destination needs the BlueStore commit stall of
// osd_copyfrom_precommit_test.go instead. The same hold runs with the copy
// pinned to the source write, which the resubmission keeps, and with a
// destination replica lost instead of the primary: the copy and the
// blocker are requeued, wait for the PG to peer with the frozen OSD, and
// are then answered once at attempt 0 (osd_erasure_copyfrom_held_test.go).
//
// These are the same cancellation and temporary-object rules exercised by
// copyErasureChunkLoss (Ceph v20.2.4, src/osd/PrimaryLogPG.cc on_change,
// cancel_copy and process_copy_chunk); the source is now erasure-coded too.
// The shared loss helpers retain signed aggregate/per-op results, independently
// compare bytes/xattrs/version/mtime, inspect the serving primary's PG log and
// check a fresh-session read plus delayed native readback after recovery.
// Those live PG-log observations do not prove every shard's history; the
// offline per-shard capture of osd_erasure_copyfrom_loss_history_test.go
// does that for the two destination-primary losses.
//
// After recovery both pool placements are checked again, source and destination
// are read natively and every destination shard is free of temporary objects.
// Publication follows cleanup and retains the chunk-setting restore readback.
// The fixture restores owned daemon/flag state and removes both pools/profiles.
// The same cells run on two more width pairs (osd_erasure_copyfrom_pairs_test.go),
// and copies from a snapshot on this pair in
// osd_erasure_copyfrom_snapshot_loss_test.go.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedcopyfromerasuretoerasureloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated freezes and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, restores owned pools and profiles
func TestCephOSDErasureOptimizedCopyFromErasureToErasureLossIntegration(t *testing.T) {
	runCopyErasurePairLoss(t, erasurePairK2M2ToK3M1, "osd-erasure-copyfrom-pair-loss-results.json", copyErasurePairHeadLossCells(), copyErasurePairLossHooks{})
}

// copyErasurePairLossAfter runs after a cell's own checks, with the cell's
// run, hold, destination facts and receipt.
type copyErasurePairLossAfter func(*testing.T, context.Context, *copyPrimaryChangeRun, *copyChunkHold, erasureFacts, *copyErasurePairLossReceipt)

// copyErasurePairLossHooks are a row's additions to its cells; nil hooks add
// nothing.
type copyErasurePairLossHooks struct {
	after copyErasurePairLossAfter
}

// runCopyErasurePairLoss runs cells on the owned pools of pair and publishes
// their receipts as name after every cleanup. A non-nil hooks.after follows
// every cell.
func runCopyErasurePairLoss(t *testing.T, pair erasureCopyPair, name string, cells []copyErasurePairLossCell, hooks copyErasurePairLossHooks) {
	ctx, pools, recovery, srcFacts, dstFacts := copyErasureToErasureFixture(t, 1800*time.Second, pair)
	limit := erasureCopyGetMax(dstFacts, pair.dest)
	t.Logf("source pool %d: %s, stripe width %d; destination pool %d: %s, stripe width %d, copy-get max %d", srcFacts.pool, pair.source, srcFacts.chunk*pair.source.k,
		dstFacts.pool, pair.dest, dstFacts.chunk*pair.dest.k, limit)
	var mu sync.Mutex
	var receipts []copyErasurePairLossReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, name), append(raw, '\n'))
		}
		if err != nil {
			t.Error("EC-to-EC loss receipt", err)
		}
	})
	for _, cell := range cells {
		label := cell.label()
		t.Run(label, func(t *testing.T) {
			receipt := copyErasurePairLossReceipt{copyErasureLossReceipt: copyErasureLossReceipt{
				copyChunkLossReceipt: copyChunkLossReceipt{Cell: label, Chunk: uint64(limit), Paused: -1},
				Width:                pair.dest.name, Lost: -1, Shard: -1, CopyGets: limit},
				SourcePool: srcFacts.pool, DestinationPool: dstFacts.pool, SourceWidth: pair.source.name, LostSourceShard: -1}
			publishAfterCleanups(t, func() {
				mu.Lock()
				receipts = append(receipts, receipt)
				mu.Unlock()
			})
			var r *copyPrimaryChangeRun
			var h *copyChunkHold
			if cell.held() {
				kind := copyHeldCopy{pinned: cell.pinned, snapshot: cell.snapshot, replica: cell.lost == "replica-before-commit"}
				if kind.replica && (cell.pinned || cell.snapshot) {
					t.Fatal("the replica lost in the held interval runs an unversioned head copy only")
				}
				var want copySnapshotLossWant
				r, h, want = copyErasureHeldCommitLoss(t, ctx, pools, recovery, dstFacts, pair.dest, limit, copyChunkLossCell{cell.copyPrimaryChangeCase, cell.lost},
					&receipt.copyErasureLossReceipt, pair.source.k, kind)
				if cell.pinned {
					receipt.SourceVersion = h.write.UserVersion
				}
				if cell.snapshot {
					receipt.SnapID, receipt.Clone, receipt.SourceBefore, receipt.SourceAfter = want.snap, nativeDigest(want.clone), nativeDigest(want.head), nativeDigest(poolObserve(t, ctx, 1, r.source, "", ""))
				}
				srcFacts.checkPlacement(t, h.source)
				receipt.SourceActing, receipt.DestinationActing = slices.Clone(h.source.Acting), slices.Clone(h.route.Acting)
				receipt.LostSourceShard = slices.Index(h.source.Acting, receipt.Lost)
				if slices.Contains(h.source.Acting[:pair.source.k], receipt.Held.Frozen) {
					t.Fatalf("the frozen osd.%d holds a data shard of the source %v", receipt.Held.Frozen, h.source.Acting)
				}
			} else {
				r, h = copyErasurePairLoss(t, ctx, pools, recovery, srcFacts, dstFacts, limit, cell, &receipt)
			}
			if hooks.after != nil {
				hooks.after(t, ctx, r, h, dstFacts, &receipt)
			}
		})
	}
}

// copySnapshotLossWant is what a copy from a clone of the source must leave
// whichever OSD is lost: the destination a copy of the clone at its user
// version, the clone and the source head natively unchanged, and none of
// the source requests (the write and the cloning write) in the destination
// PG log, since a copy from a clone carries no request ID.
type copySnapshotLossWant struct {
	snap        uint64
	clone, head nativeXattrSnapshot
	sourceKeys  []pgHistoryTarget
}

// snapshotWant clones the source head, natively source after the write of
// data whose identity is writeKey, under a fresh self-managed snapshot and
// returns what a copy of that clone must leave. The clone keeps the written
// head's data, xattrs and user version.
func (r *copyPrimaryChangeRun) snapshotWant(source nativeXattrSnapshot, data []byte, writeKey pgHistoryTarget) copySnapshotLossWant {
	t, ctx := r.t, r.ctx
	t.Helper()
	var want copySnapshotLossWant
	var cloning cephmsgr.OSDReply
	want.snap, _, cloning = r.cloneSource(source)
	want.clone, want.head = source, poolObserve(t, ctx, 1, r.source, "", "")
	if want.head.Code != 0 || !bytes.Equal(want.head.Data, data) || want.head.Version != cloning.UserVersion || want.head.Version <= source.Version {
		t.Fatalf("native source head after the cloning write: code %d version %d (cloning reply %d, clone %d)", want.head.Code, want.head.Version, cloning.UserVersion, source.Version)
	}
	want.sourceKeys = []pgHistoryTarget{writeKey, {Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction}}
	return want
}

func copyErasurePairLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, srcFacts, dstFacts erasureFacts, limit int, cell copyErasurePairLossCell, receipt *copyErasurePairLossReceipt) (*copyPrimaryChangeRun, *copyChunkHold) {
	snapshot := ""
	if cell.snapshot {
		snapshot = "-snap"
	}
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecpairloss-%d-%s%s%s", cell.code, cell.lost, snapshot, copyPinnedSuffix(cell.pinned)))
	r.chunkMax = limit
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	if cell.lost == "source" {
		// The lost source primary must hold a destination shard too. Every
		// OSD of the four-OSD pairs does; on the six-OSD pair the
		// destination PG spans four of the six.
		dst := r.dstRoute
		receipt.SourcePrimary = r.sourceWhere(fmt.Sprintf("a primary other than osd.%d among the destination's %v", dst.ID, dst.Acting), func(src osdFixtureOracle) bool {
			return src.ID == src.PrimaryID && src.ID != dst.ID && slices.Contains(dst.Acting, src.ID)
		})
	}
	srcFacts.checkPlacement(t, r.srcRoute)
	dstFacts.checkPlacement(t, r.dstRoute)
	receipt.SourceActing, receipt.DestinationActing = slices.Clone(r.srcRoute.Acting), slices.Clone(r.dstRoute.Acting)
	r.lowerCopyChunk(&receipt.ChunkRestore, &receipt.ChunkReadback)
	seeded := r.seedDestination()
	receipt.Seed = nativeDigest(seeded)
	data := make([]byte, 2*limit)
	for i := range data {
		data[i] = byte(i*67 + i>>8 + 37)
	}
	write, source := r.writeSource("EC source write", 1700010100, data, []byte{9, 0xfe})
	receipt.SourceBefore, receipt.SourceBytes, receipt.SourceUser = nativeDigest(source), len(data), source.Version
	copyKey := func(h *copyChunkHold) pgHistoryTarget {
		return pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}
	var want copySnapshotLossWant
	if cell.snapshot {
		want = r.snapshotWant(source, data, writeKey)
		receipt.SnapID, receipt.Clone, receipt.SourceBefore = want.snap, nativeDigest(source), nativeDigest(want.head)
	}
	route, src := r.placeForCopy()
	srcFacts.checkPlacement(t, src)
	dstFacts.checkPlacement(t, route)
	request := r.copyRequest(cell.code, route)
	if cell.snapshot {
		request.Operations[0].CopyFrom.SourceSnapshot = want.snap
	}
	firstAssert := pinCopy(&request, cell.pinned, source.Version)
	receipt.SourceVersion = request.Operations[0].CopyFrom.SourceVersion
	h := r.holdAtChunk(request, route, src, 2, firstAssert, source.Version, []int{0, 0}, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	receipt.Lost, receipt.Shard = route.ID, int(route.Shard)
	switch cell.lost {
	case "shard":
		var err error
		receipt.Lost, receipt.Shard, err = copyErasureLostShard(route, src)
		if err != nil {
			t.Fatal(err)
		}
	case "source":
		receipt.Lost, receipt.Shard = src.ID, slices.Index(route.Acting, src.ID)
	}
	receipt.LostSourceShard = slices.Index(src.Acting, receipt.Lost)
	if receipt.Shard < 0 || receipt.LostSourceShard < 0 || (receipt.Lost == src.ID) != (cell.lost == "source") {
		t.Fatal("the lost OSD does not hold the required shards", receipt.Lost, src.Acting, route.Acting)
	}
	switch {
	case cell.lost == "destination" && cell.snapshot:
		receipt.KillExit = r.recovery.kill(t, ctx, route.ID)
		r.settleSnapshotDestinationLoss(h, cell.code, request, seeded, copyKey(h), want, &receipt.copyChunkLossReceipt)
	case cell.lost == "destination":
		r.loseDestinationBetweenChunks(h, cell.code, seeded, copyKey(h), writeKey, data, &receipt.copyChunkLossReceipt)
	default:
		now := r.restartCopyAfterReplicaLoss(h, receipt.Lost, firstAssert, &receipt.copyChunkLossReceipt)
		if cell.lost == "source" {
			receipt.MovedSource = r.waitErasureCopySourceMoved(src)
			h.pending(t, "after observing the source primary change")
		}
		if cell.snapshot {
			r.finishSnapshotAfterReplicaLoss(h, now, copyKey(h), want, &receipt.copyChunkLossReceipt)
		} else {
			r.finishCopyAfterReplicaLoss(h, now, copyKey(h), writeKey, data, &receipt.copyChunkLossReceipt)
		}
	}
	srcFacts.checkPlacement(t, poolPlace(t, ctx, 1, r.source, ""))
	dstFacts.checkPlacement(t, poolPlace(t, ctx, 2, r.dest, ""))
	after := poolObserve(t, ctx, 1, r.source, "", "")
	dest := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.SourceAfter = nativeDigest(after)
	if cell.snapshot {
		clone := poolObserveSnap(t, ctx, 1, r.source, "", "", want.snap)
		if !sameNative(after, want.head) || !sameNative(clone, want.clone) || !sameCopy(dest, clone, clone.Version) {
			t.Fatalf("native EC source/clone/destination mismatch after recovery: head %s, clone %s, destination %s", nativeDigest(after), nativeDigest(clone), nativeDigest(dest))
		}
	} else if after.Code != 0 || !sameCopy(dest, after, after.Version) || cell.lost != "destination" && !sameNative(after, source) {
		t.Fatalf("native EC source/destination mismatch after recovery: source %s, destination %s", nativeDigest(after), nativeDigest(dest))
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
	return r, h
}

func (r *copyPrimaryChangeRun) waitErasureCopySourceMoved(before osdFixtureOracle) *copyErasureSourceMove {
	r.t.Helper()
	work, cancel := context.WithTimeout(r.ctx, 60*time.Second)
	defer cancel()
	var last error
	for !copyBudgetSpent(work) {
		observation := copyInterval(r.t, work, 1, r.source)
		moved, err := checkErasureCopySourceMoved(before, observation)
		if err == nil {
			r.t.Logf("source PG %s moved from osd.%d to osd.%d shard %d, epoch %d interval %d, acting %v", moved.PGID, before.ID, moved.Primary, moved.Shard, moved.Epoch, moved.IntervalSince, moved.Acting)
			return &moved
		}
		last = err
		time.Sleep(250 * time.Millisecond)
	}
	r.t.Fatalf("the source PG did not activate on another primary shard: %v", last)
	return nil
}

func checkErasureCopySourceMoved(before osdFixtureOracle, observation copyIntervalObservation) (copyErasureSourceMove, error) {
	if observation.Before == nil {
		return copyErasureSourceMove{}, fmt.Errorf("missing source placement: %s", observation.PlacementFailure)
	}
	now := *observation.Before
	if err := checkPrimaryMoved(before, now); err != nil {
		return copyErasureSourceMove{}, err
	}
	shard := int(now.Shard)
	if now.PoolType != before.PoolType || now.MinSize != before.MinSize || now.Replicas != before.Replicas ||
		shard < 0 || shard >= len(now.Acting) || now.Acting[shard] != now.ID {
		return copyErasureSourceMove{}, fmt.Errorf("replacement source primary does not name its shard: primary %d shard %d acting %v", now.ID, now.Shard, now.Acting)
	}
	want := slices.Clone(before.Acting)
	for i, osd := range want {
		if osd == before.ID {
			want[i] = crushItemNone
		}
	}
	if !slices.Equal(now.Acting, want) || !slices.Equal(now.Up, want) {
		return copyErasureSourceMove{}, fmt.Errorf("source survivors must keep their shard slots: want %v, up %v, acting %v", want, now.Up, now.Acting)
	}
	if err := copyIntervalHeld(observation, now); err != nil {
		return copyErasureSourceMove{}, err
	}
	q := observation.Query
	if !strings.HasPrefix(q.State, "active+") || q.SameIntervalSince <= before.Epoch {
		return copyErasureSourceMove{}, fmt.Errorf("replacement source primary is not active in a newer interval: %s, interval %d, original epoch %d", q.State, q.SameIntervalSince, before.Epoch)
	}
	return copyErasureSourceMove{
		PGID: now.PGID, Primary: now.ID, Shard: now.Shard, Acting: now.Acting, Epoch: q.Epoch, IntervalSince: q.SameIntervalSince,
	}, nil
}

func TestErasureCopySourceMoved(t *testing.T) {
	before := osdFixtureOracle{
		Pool: 3, PoolType: 3, Name: "source", PGID: "3.2", Replicas: 4, MinSize: 3, Epoch: 30,
		ID: 1, PrimaryID: 1, Shard: 0, Up: []int32{1, 0, 2, 3}, Acting: []int32{1, 0, 2, 3},
	}
	for _, tc := range []struct {
		name   string
		change func(*osdFixtureOracle, *copyIntervalQuery)
	}{
		{"accept-new-primary", nil},
		{"old-shard", func(p *osdFixtureOracle, _ *copyIntervalQuery) { p.Shard = 0 }},
		{"remapped-survivors", func(p *osdFixtureOracle, q *copyIntervalQuery) {
			p.Acting = []int32{crushItemNone, 0, 3, 2}
			q.Acting = p.Acting
		}},
		{"old-interval", func(_ *osdFixtureOracle, q *copyIntervalQuery) { q.SameIntervalSince = 30 }},
		{"wrong-query-shard", func(_ *osdFixtureOracle, q *copyIntervalQuery) { q.PGID = "3.2s0" }},
		{"not-active", func(_ *osdFixtureOracle, q *copyIntervalQuery) { q.State = "peering" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := before
			now.ID, now.PrimaryID, now.Shard, now.Epoch = 0, 0, 1, 33
			now.Acting = []int32{crushItemNone, 0, 2, 3}
			now.Up = slices.Clone(now.Acting)
			query := copyIntervalQuery{State: "active+undersized+degraded", Epoch: 33, Up: now.Up, Acting: now.Acting, PGID: "3.2s1", SameIntervalSince: 32}
			if tc.change != nil {
				tc.change(&now, &query)
			}
			code := 0
			got, err := checkErasureCopySourceMoved(before, copyIntervalObservation{Before: &now, After: &now, Code: &code, Query: &query})
			if tc.change != nil {
				if err == nil {
					t.Fatalf("invalid observation was accepted: %+v", got)
				}
			} else if err != nil || got.Primary != 0 || got.Shard != 1 || got.IntervalSince != 32 {
				t.Fatalf("new source observation: %+v, %v", got, err)
			}
		})
	}
}

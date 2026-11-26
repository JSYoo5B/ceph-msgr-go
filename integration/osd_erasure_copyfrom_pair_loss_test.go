package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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
}

type copyErasureSourceMove struct {
	PGID          string  `json:"pgid"`
	Primary       int32   `json:"primary"`
	Shard         int8    `json:"shard"`
	Acting        []int32 `json:"acting"`
	Epoch         uint32  `json:"epoch"`
	IntervalSince uint32  `json:"interval_since"`
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
// These are the same cancellation and temporary-object rules exercised by
// copyErasureChunkLoss (Ceph v20.2.4, src/osd/PrimaryLogPG.cc on_change,
// cancel_copy and process_copy_chunk); the source is now erasure-coded too.
// The shared loss helpers retain signed aggregate/per-op results, independently
// compare bytes/xattrs/version/mtime, inspect the serving primary's PG log and
// check a fresh-session read plus delayed native readback after recovery.
// Those live PG-log observations do not prove every shard's history.
//
// After recovery both pool placements are checked again, source and destination
// are read natively and every destination shard is free of temporary objects.
// Publication follows cleanup and retains the chunk-setting restore readback.
// The fixture restores owned daemon/flag state and removes both pools/profiles.
// This row covers the optimized isa width pair and head copies. Snapshot
// sources and loss during the final transaction require separate evidence.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedcopyfromerasuretoerasureloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, restores owned pools and profiles
func TestCephOSDErasureOptimizedCopyFromErasureToErasureLossIntegration(t *testing.T) {
	ctx, pools, recovery, srcFacts, dstFacts := copyErasureToErasureFixture(t, 1800*time.Second)
	limit := erasureCopyGetMax(dstFacts, erasureK3M1)
	var receipts []copyErasurePairLossReceipt
	t.Cleanup(func() {
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-erasure-copyfrom-pair-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("EC-to-EC loss receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, loss := range []struct {
			target string
			pinned bool
		}{{"destination", false}, {"shard", false}, {"source", false}, {"source", true}} {
			cell := copyChunkLossCell{tc, loss.target}
			label := cell.name + "/" + cell.lost + copyPinnedSuffix(loss.pinned)
			t.Run(label, func(t *testing.T) {
				receipt := copyErasurePairLossReceipt{copyErasureLossReceipt: copyErasureLossReceipt{
					copyChunkLossReceipt: copyChunkLossReceipt{Cell: label, Chunk: uint64(limit), Paused: -1},
					Width:                erasureK3M1.name, Lost: -1, Shard: -1, CopyGets: limit},
					SourcePool: srcFacts.pool, DestinationPool: dstFacts.pool, SourceWidth: erasureK2M2.name, LostSourceShard: -1}
				publishAfterCleanups(t, func() { receipts = append(receipts, receipt) })
				copyErasurePairLoss(t, ctx, pools, recovery, srcFacts, dstFacts, limit, cell, loss.pinned, &receipt)
			})
		}
	}
}

func copyErasurePairLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, srcFacts, dstFacts erasureFacts, limit int, cell copyChunkLossCell, pinned bool, receipt *copyErasurePairLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecpairloss-%d-%s%s", cell.code, cell.lost, copyPinnedSuffix(pinned)))
	r.chunkMax = limit
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
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
	route, src := r.placeForCopy()
	srcFacts.checkPlacement(t, src)
	dstFacts.checkPlacement(t, route)
	request := r.copyRequest(cell.code, route)
	firstAssert := pinCopy(&request, pinned, source.Version)
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
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}
	switch cell.lost {
	case "destination":
		r.loseDestinationBetweenChunks(h, cell.code, seeded, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	case "source":
		now := r.restartCopyAfterReplicaLoss(h, receipt.Lost, firstAssert, &receipt.copyChunkLossReceipt)
		receipt.MovedSource = r.waitErasureCopySourceMoved(src)
		h.pending(t, "after observing the source primary change")
		r.finishCopyAfterReplicaLoss(h, now, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	case "shard":
		r.loseReplicaBetweenChunks(h, receipt.Lost, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	}
	srcFacts.checkPlacement(t, poolPlace(t, ctx, 1, r.source, ""))
	dstFacts.checkPlacement(t, poolPlace(t, ctx, 2, r.dest, ""))
	after := poolObserve(t, ctx, 1, r.source, "", "")
	dest := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.SourceAfter = nativeDigest(after)
	if after.Code != 0 || !sameCopy(dest, after, after.Version) || cell.lost != "destination" && !sameNative(after, source) {
		t.Fatalf("native EC source/destination mismatch after recovery: source %s, destination %s", nativeDigest(after), nativeDigest(dest))
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
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

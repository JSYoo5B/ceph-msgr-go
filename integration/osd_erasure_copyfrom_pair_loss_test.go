package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// copyErasurePairLossReceipt adds both pool identities and independently
// observed source/seed state to the existing loss and cleanup observations.
// It contains no connection addresses or credentials.
type copyErasurePairLossReceipt struct {
	copyErasureLossReceipt
	SourcePool        int64   `json:"source_pool"`
	DestinationPool   int64   `json:"destination_pool"`
	SourceWidth       string  `json:"source_width"`
	SourceActing      []int32 `json:"source_acting"`
	DestinationActing []int32 `json:"destination_acting"`
	LostSourceShard   int     `json:"lost_source_shard"`
	SourceBefore      string  `json:"source_before"`
	SourceAfter       string  `json:"source_after_recovery"`
	Seed              string  `json:"destination_seed"`
}

// COPY_FROM and COPY_FROM2 lose an OSD between two chunks of an unversioned
// head copy from an owned optimized isa k=2,m=2 source onto an owned optimized
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
// then is an ordinary daemon SIGKILL injected, with one of two targets:
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
// This cell does not cover source-primary loss, pinned or snapshot sources,
// other width pairs/plugins, or loss during the final-transaction interval.
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
		for _, lost := range []string{"destination", "shard"} {
			cell := copyChunkLossCell{tc, lost}
			t.Run(cell.name+"/"+lost, func(t *testing.T) {
				receipt := copyErasurePairLossReceipt{copyErasureLossReceipt: copyErasureLossReceipt{
					copyChunkLossReceipt: copyChunkLossReceipt{Cell: cell.name + "/" + lost, Chunk: uint64(limit), Paused: -1},
					Width:                erasureK3M1.name, Lost: -1, Shard: -1, CopyGets: limit},
					SourcePool: srcFacts.pool, DestinationPool: dstFacts.pool, SourceWidth: erasureK2M2.name, LostSourceShard: -1}
				publishAfterCleanups(t, func() { receipts = append(receipts, receipt) })
				copyErasurePairLoss(t, ctx, pools, recovery, srcFacts, dstFacts, limit, cell, &receipt)
			})
		}
	}
}

func copyErasurePairLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, srcFacts, dstFacts erasureFacts, limit int, cell copyChunkLossCell, receipt *copyErasurePairLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecpairloss-%d-%s", cell.code, cell.lost))
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
	h := r.holdAtChunk(r.copyRequest(cell.code, route), route, src, 2, 0, source.Version, []int{0, 0}, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	receipt.Lost, receipt.Shard = route.ID, int(route.Shard)
	if cell.lost == "shard" {
		var err error
		receipt.Lost, receipt.Shard, err = copyErasureLostShard(route, src)
		if err != nil {
			t.Fatal(err)
		}
	}
	receipt.LostSourceShard = slices.Index(src.Acting, receipt.Lost)
	if receipt.LostSourceShard < 0 || receipt.Lost == src.ID {
		t.Fatal("the loss must remove a non-primary source shard", receipt.Lost, src.Acting, src.ID)
	}
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}
	if cell.lost == "destination" {
		r.loseDestinationBetweenChunks(h, cell.code, seeded, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	} else {
		r.loseReplicaBetweenChunks(h, receipt.Lost, copyKey, writeKey, data, &receipt.copyChunkLossReceipt)
	}
	srcFacts.checkPlacement(t, poolPlace(t, ctx, 1, r.source, ""))
	dstFacts.checkPlacement(t, poolPlace(t, ctx, 2, r.dest, ""))
	after := poolObserve(t, ctx, 1, r.source, "", "")
	dest := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.SourceAfter = nativeDigest(after)
	if after.Code != 0 || !sameCopy(dest, after, after.Version) || cell.lost == "shard" && !sameNative(after, source) {
		t.Fatalf("native EC source/destination mismatch after recovery: source %s, destination %s", nativeDigest(after), nativeDigest(dest))
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
}

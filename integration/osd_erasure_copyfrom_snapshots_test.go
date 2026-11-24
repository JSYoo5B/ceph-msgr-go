package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Multi-chunk COPY_FROM and COPY_FROM2 from snapshots and removed heads of
// an optimized erasure-coded source, and from an optimized erasure-coded
// source onto an optimized erasure-coded destination of another width. The
// barrier is holdAtChunk of osd_copyfrom_multichunk_sources_test.go; the
// erasure-coded source and destination cells of
// osd_erasure_copyfrom_sources_test.go and osd_erasure_copyfrom_chunks_test.go
// give the per-pool meanings. The pinned source (Ceph v20.2.4) adds:
//
//   - A write under a SnapContext newer than the head's snapset clones the
//     head first (make_writeable, src/osd/PrimaryLogPG.cc:8806-8950) on any
//     pool type; on an erasure-coded PG the clone is part of the same
//     transaction every shard applies (ECTransaction generates the clone per
//     shard), and the clone keeps the head's user version.
//   - do_copy_get (PrimaryLogPG.cc:9295-9486) runs on the object context the
//     copy-get's snap id selects (find_object_context, 12067-12264), so every
//     chunk of a snapshot copy reads the clone through the erasure-coded
//     asynchronous read (9318-9320 and 9364-9378); a later head write leaves
//     the clone alone, and a removed head is a whiteout with the clone still
//     readable at its snap id. A snap id in the OSDMap removed_snaps_queue is
//     -ENOENT for every read at it (12232-12237).
//   - The destination primary's copy-get max is its own pool's: the lowered
//     limit rounded up to the destination's stripe width (get_copy_chunk_size,
//     PrimaryLogPG.h:1398-1407), whatever the source's width.

// copyErasureSnapshotCell is one opcode and one copy from a snapshot of the
// erasure-coded source with its head changed between the chunks, unversioned
// or pinned.
type copyErasureSnapshotCell struct {
	copyPrimaryChangeCase
	pinned bool
}

func (c copyErasureSnapshotCell) label() string {
	return c.name + "/snapshot-head-change" + copyPinnedSuffix(c.pinned)
}

// For COPY_FROM and COPY_FROM2, a copy from a snapshot of an owned optimized
// isa k=2,m=2 pool onto a natively seeded head of an owned replicated pool
// on the four-OSD topology runs in two chunks under the destination
// primary's osd_copyfrom_max_chunk lowered to 32 KiB, and the barrier holds
// it before the second: the erasure-coded source primary is frozen while the
// first copy-get is in flight, and the second stays paused and unsent with
// one temporary object listed on every OSD of the destination PG. The
// source is a clone of a self-managed snapshot made by one Go SETXATTR under
// the snapshot's context, natively the source write. The copy is
// unversioned, or pinned to the clone's user version, which the first
// copy-get must then assert too.
//
// Snapshot source, head changed between the chunks: a Go write replaces the
// head under the same context. Every chunk reads the clone, so the copy is
// answered once on its original connection with 0, the clone's user version
// and ONDISK; the destination is natively the clone (bytes, xattrs, user
// version, mtime), every OSD drops the temporary object, the destination
// primary's PG log holds the copy once without any source request ID, and a
// fresh session reads the clone's bytes with nothing changing later. The
// source head is natively the write and the clone is unchanged.
//
// Removals between the chunks, unversioned or pinned, as the replicated
// cells of osd_copyfrom_multichunk_mutations_test.go
// (copyChunkSourceRemoval): a Go DELETE of a head without clones is -ENOENT
// with per-op -EINPROGRESS and the seed kept; a Go DELETE of the head of a
// snapshot source under its context leaves a whiteout and the clone, so the
// copy is answered 0 with the clone; the snapshot removed natively under
// nosnaptrim, observed in the OSDMap removed_snaps_queue and the source PG's
// snap_trimq, is -ENOENT with per-op -EINPROGRESS and the seed kept, while a
// native read at the snap id is -ENOENT and native listsnaps still lists
// the clone. Every failure leaves the destination PG log naming neither the
// copy nor a source request.
//
// Not verified: a trimmed clone, snapshots of other widths and plugins, and
// pool snapshots.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2copyfromsnapshotsource key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates and removes owned snapshots, sets nosnaptrim
func TestCephOSDErasureOptimizedK2M2CopyFromSnapshotSourceIntegration(t *testing.T) {
	ctx, pools, recovery, facts := copyErasureChunkFixture(t, 2400*time.Second, erasureK2M2, true)
	t.Logf("source pool %d: %s, stripe width %d, nonprimary shards %v", facts.pool, erasureK2M2, facts.chunk*erasureK2M2.k, facts.nonprimary)
	keep := copyChunkReceipts(t, recovery, "osd-erasure-copyfrom-snapshot-source-results.json")
	for _, tc := range copyPrimaryChangeCases {
		for _, pinned := range []bool{false, true} {
			cell := copyErasureSnapshotCell{tc, pinned}
			t.Run(cell.label(), func(t *testing.T) {
				receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: "head-write", Chunk: multiChunkLimit}
				publishAfterCleanups(t, func() { keep(receipt) })
				copyErasureSnapshotChange(t, ctx, pools, recovery, facts, cell, &receipt)
			})
			for _, removal := range []string{"head-of-head-source", "head-of-snapshot-source", "snapshot-of-snapshot-source"} {
				cell := copyRemovalCell{tc, removal, pinned}
				t.Run(cell.label(), func(t *testing.T) {
					receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: removal, Chunk: multiChunkLimit}
					publishAfterCleanups(t, func() { keep(receipt) })
					copyChunkSourceRemoval(t, ctx, pools, recovery, cell, &receipt)
				})
			}
		}
	}
}

func copyErasureSnapshotChange(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, facts erasureFacts, cell copyErasureSnapshotCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ecsnap-%d%s", cell.code, copyPinnedSuffix(cell.pinned)))
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	facts.checkPlacement(t, r.srcRoute)
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*59 + i>>8 + 29)
	}
	write := r.writeSourceHead("source write", 1700009900, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != write.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, write.UserVersion)
	}
	receipt.SourceBytes, receipt.CopiedUser, receipt.Cursors = len(data), copied.Version, []int{0, 0}
	snap, snapc, cloning := r.cloneSource(copied)
	receipt.SnapID = snap
	sourceKeys := []pgHistoryTarget{
		{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction},
		{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction},
	}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	request.Operations[0].CopyFrom.SourceSnapshot = snap
	firstAssert := pinCopy(&request, cell.pinned, copied.Version)
	receipt.SourceVersion = request.Operations[0].CopyFrom.SourceVersion
	// The source has no OMAP: both copy-gets carry an empty OMAP position.
	h := r.holdAtChunk(request, route, src, 2, firstAssert, copied.Version, receipt.Cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	changed := r.writeSourceHead("head write between chunks", 1700009920, []byte("EC snapshot head changed between chunks"), []byte{4}, snapc)
	receipt.MutationUser = changed.UserVersion
	sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: changed.Transaction})
	if changed.UserVersion <= copied.Version {
		t.Fatalf("the write between chunks has user version %d, not above the copied %d", changed.UserVersion, copied.Version)
	}
	reply := r.awaitHeld(h, receipt)
	if err := judgeChunkAnswer(0, reply, h.op.TID, copied.Version); err != nil {
		t.Fatal(err)
	}
	// The source is observed natively only now that pauserd is clear.
	if head := poolObserve(t, ctx, 1, r.source, "", ""); head.Code != 0 || head.Version != changed.UserVersion || string(head.Data) != "EC snapshot head changed between chunks" {
		t.Fatalf("native source head is not the write between chunks: code %d version %d (reply %d)", head.Code, head.Version, changed.UserVersion)
	}
	if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", snap); !sameNative(clone, copied) {
		t.Fatalf("the head write between chunks changed the clone: %s; was %s", nativeDigest(clone), nativeDigest(copied))
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, false, receipt)
}

// copyErasureToErasureFixture starts the four-OSD fixture with an owned
// optimized isa k=2,m=2 source pool (slot 1) and an owned optimized isa
// k=3,m=1 destination pool (slot 2), each with its own owned profile, and
// returns the run-level recovery and both pools' facts.
func copyErasureToErasureFixture(t *testing.T, bound time.Duration) (context.Context, ownedPools, *copyPrimaryRecovery, erasureFacts, erasureFacts) {
	t.Helper()
	source, dest := erasureK2M2, erasureK3M1
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != source.topology || source.topology != dest.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s and %s with overwrites need the BlueStore OSD topology %q", source, dest, source.topology)
	}
	if !isReplicatedPGFixtureProfile() {
		t.Skip("separate source and destination primaries need the hashed owned pools of a replicated-PG profile")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	t.Cleanup(cancel)
	waitOSDFixtureClean(t, ctx, base)
	recovery := &copyPrimaryRecovery{c: c, base: base, dir: os.Getenv("CEPH_MSGR_CONTROL_DIR"), out: map[int32]bool{}, frozen: map[int32]bool{}, killed: map[int32]bool{}}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		r := nativePool(t, cleanup, "cleanup", nil)
		if !r.PoolsRemoved {
			t.Error("owned pools were not removed")
		}
		for _, width := range []erasureWidth{source, dest} {
			if slices.Contains(r.ECProfilesRemaining, width.profile) {
				t.Error("owned erasure-code profile was not removed", width.profile)
			}
		}
	})
	// The pools follow the fixture's hash policy, which the replicated-PG
	// profile admitted above sets.
	hash := isReplicatedPGFixtureProfile()
	created := map[int]poolReceipt{}
	for _, slot := range []int{1, 2} {
		width := source
		if slot == 2 {
			width = dest
		}
		pool := nativePool(t, ctx, "create", map[string]any{"slot": slot, "pg_num": 8, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
		if pool.PoolType != 3 || !pool.ECOptimized || pool.Width != width.name || pool.ErasureCodeProfile != width.profile || pool.PoolID == base.Pool {
			t.Fatal("owned pool is not the requested erasure-coded shape", slot, pool.PoolType, pool.ECOptimized, pool.Width, pool.ErasureCodeProfile)
		}
		created[slot] = pool
	}
	if created[1].PoolID == created[2].PoolID {
		t.Fatal("the owned pools are one pool", created[1].PoolID)
	}
	// Both facts are read from a map that carries both pools.
	srcFacts := erasurePoolMapFacts(t, ctx, c, created[1], source, true)
	dstFacts := erasurePoolMapFacts(t, ctx, c, created[2], dest, true)
	// Registered after the pools, so it runs before their removal.
	t.Cleanup(func() { recovery.restore(t) })
	return ctx, ownedPools{ids: map[int]int64{1: created[1].PoolID, 2: created[2].PoolID}, request: replicatedPGRequest}, recovery, srcFacts, dstFacts
}

// copyErasureToErasureCell is one opcode and one copy from the k=2,m=2
// source onto the k=3,m=1 destination: the head completed, the head changed
// between the chunks, or a snapshot with its head changed between them;
// unversioned or pinned.
type copyErasureToErasureCell struct {
	copyPrimaryChangeCase
	source string
	pinned bool
}

func (c copyErasureToErasureCell) label() string {
	return c.name + "/" + c.source + copyPinnedSuffix(c.pinned)
}

// copyErasureToErasureAnswer is the answer each cell must get: the copied
// object's user version, or -ERANGE once a head source changed.
func copyErasureToErasureAnswer(source string, copiedUser uint64) (int32, uint64) {
	if source == "head-change" {
		return -34, 0
	}
	return 0, copiedUser
}

// For COPY_FROM and COPY_FROM2, a copy from an owned optimized isa k=2,m=2
// pool onto a natively seeded head of an owned optimized isa k=3,m=1 pool on
// the four-OSD topology runs under the destination primary's
// osd_copyfrom_max_chunk lowered to 32 KiB, which the destination's stripe
// of three data chunks rounds up: every copy-get prints that max
// (copyErasureChunkLimit), and the source holds two of them. The barrier
// holds the copy before its second chunk, with the erasure-coded source
// primary frozen while the first copy-get is in flight and the second paused
// and unsent, and one temporary object <pgid>s<shard> of the destination
// primary listed in every shard collection of the destination PG. The copy
// is unversioned or pinned to the user version a native read of the copied
// object gave, which the first copy-get then asserts too.
//
// Head data: after pause is cleared the same client op is answered once on
// its original connection with 0, the source's user version and ONDISK; the
// destination is natively the source (bytes, xattrs, user version, mtime),
// every shard drops the temporary object, the destination primary's PG log
// holds the copy once bound to the source write, and a fresh session reads
// the source bytes with nothing changing later.
//
// Head changed between the chunks: the held copy-get's assertion fails, so
// the copy is answered once with -ERANGE and per-op -EINPROGRESS; the seed is
// kept natively, every shard drops the temporary object, the PG log names
// neither the copy nor a source write, and a fresh session reads the seed.
//
// Snapshot with its head changed between the chunks (a clone of a
// self-managed snapshot made by a Go SETXATTR under its context): every
// chunk reads the clone, so the copy is answered 0 with the clone's user
// version, the destination is natively the clone, and the PG log holds the
// copy once without any source request ID.
//
// Not verified: other pairs of widths and plugins, unoptimized pools, a loss
// between chunks of an erasure-coded copy onto an erasure-coded
// destination, and a held final transaction there.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedcopyfromerasuretoerasure key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates owned snapshots
func TestCephOSDErasureOptimizedCopyFromErasureToErasureIntegration(t *testing.T) {
	ctx, pools, recovery, srcFacts, dstFacts := copyErasureToErasureFixture(t, 2400*time.Second)
	limit := erasureCopyGetMax(dstFacts, erasureK3M1)
	t.Logf("source pool %d: %s, stripe width %d; destination pool %d: %s, stripe width %d, copy-get max %d", srcFacts.pool, erasureK2M2, srcFacts.chunk*erasureK2M2.k,
		dstFacts.pool, erasureK3M1, dstFacts.chunk*erasureK3M1.k, limit)
	keep := copyChunkReceipts(t, recovery, "osd-erasure-copyfrom-erasure-to-erasure-results.json")
	for _, tc := range copyPrimaryChangeCases {
		for _, source := range []string{"data", "head-change", "snapshot-head-change"} {
			for _, pinned := range []bool{false, true} {
				cell := copyErasureToErasureCell{tc, source, pinned}
				t.Run(cell.label(), func(t *testing.T) {
					mutation := "none"
					if source != "data" {
						mutation = "head-write"
					}
					receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: mutation, Chunk: uint64(limit)}
					publishAfterCleanups(t, func() { keep(receipt) })
					copyErasureToErasure(t, ctx, pools, recovery, srcFacts, dstFacts, limit, cell, &receipt)
				})
			}
		}
	}
}

func copyErasureToErasure(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, srcFacts, dstFacts erasureFacts, limit int, cell copyErasureToErasureCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ec2ec-%d-%s%s", cell.code, cell.source, copyPinnedSuffix(cell.pinned)))
	dstFacts.checkPlacement(t, r.dstRoute)
	r.chunkMax = limit
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	srcFacts.checkPlacement(t, r.srcRoute)
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	data := make([]byte, 2*limit)
	for i := range data {
		data[i] = byte(i*61 + i>>8 + 31)
	}
	write := r.writeSourceHead("source write", 1700010000, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != write.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, write.UserVersion)
	}
	receipt.SourceBytes, receipt.CopiedUser, receipt.Cursors = len(data), copied.Version, []int{0, 0}
	sourceKeys := []pgHistoryTarget{{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}}
	snapshot := cell.source == "snapshot-head-change"
	var snapc cephmsgr.OSDSnapContext
	if snapshot {
		var cloning cephmsgr.OSDReply
		receipt.SnapID, snapc, cloning = r.cloneSource(copied)
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: cloning.Transaction})
	}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	if snapshot {
		request.Operations[0].CopyFrom.SourceSnapshot = receipt.SnapID
	}
	firstAssert := pinCopy(&request, cell.pinned, copied.Version)
	receipt.SourceVersion = request.Operations[0].CopyFrom.SourceVersion
	// The source has no OMAP: both copy-gets carry an empty OMAP position.
	h := r.holdAtChunk(request, route, src, 2, firstAssert, copied.Version, receipt.Cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp

	var changed cephmsgr.OSDReply
	if cell.source != "data" {
		changed = r.writeSourceHead("head write between chunks", 1700010020, []byte("EC-to-EC source changed between chunks"), []byte{4}, snapc)
		receipt.MutationUser = changed.UserVersion
		sourceKeys = append(sourceKeys, pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: changed.Transaction})
		if changed.UserVersion <= copied.Version {
			t.Fatalf("the write between chunks has user version %d, not above the copied %d", changed.UserVersion, copied.Version)
		}
	}
	reply := r.awaitHeld(h, receipt)
	want, user := copyErasureToErasureAnswer(cell.source, copied.Version)
	if err := judgeChunkAnswer(want, reply, h.op.TID, user); err != nil {
		t.Fatal(err)
	}
	// The source is observed natively only now that pauserd is clear.
	head := poolObserve(t, ctx, 1, r.source, "", "")
	switch {
	case cell.source != "data" && (head.Code != 0 || head.Version != changed.UserVersion || string(head.Data) != "EC-to-EC source changed between chunks"):
		t.Fatalf("native source head is not the write between chunks: code %d version %d (reply %d)", head.Code, head.Version, changed.UserVersion)
	case cell.source == "data" && !sameNative(head, copied):
		t.Fatalf("the copy changed its source: %s; was %s", nativeDigest(head), nativeDigest(copied))
	}
	if snapshot {
		if clone := poolObserveSnap(t, ctx, 1, r.source, "", "", receipt.SnapID); !sameNative(clone, copied) {
			t.Fatalf("the head write between chunks changed the clone: %s; was %s", nativeDigest(clone), nativeDigest(copied))
		}
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, cell.source == "data", receipt)
}

func TestCopyFromErasureSnapshotJudgements(t *testing.T) {
	for _, c := range []struct {
		label string
		want  string
	}{
		{copyErasureSnapshotCell{copyPrimaryChangeCases[0], false}.label(), "COPY_FROM/snapshot-head-change"},
		{copyErasureSnapshotCell{copyPrimaryChangeCases[1], true}.label(), "COPY_FROM2/snapshot-head-change-pinned"},
		{copyErasureToErasureCell{copyPrimaryChangeCases[0], "data", false}.label(), "COPY_FROM/data"},
		{copyErasureToErasureCell{copyPrimaryChangeCases[1], "snapshot-head-change", true}.label(), "COPY_FROM2/snapshot-head-change-pinned"},
	} {
		if c.label != c.want {
			t.Errorf("label %q, want %q", c.label, c.want)
		}
	}
	for source, want := range map[string]struct {
		code int32
		user uint64
	}{"data": {0, 7}, "head-change": {-34, 0}, "snapshot-head-change": {0, 7}} {
		if code, user := copyErasureToErasureAnswer(source, 7); code != want.code || user != want.user {
			t.Errorf("%s answered %d with user %d; want %d with %d", source, code, user, want.code, want.user)
		}
	}
	// The k=3 destination's max, not the k=2 source's stripe, bounds every
	// copy-get of an EC-to-EC copy.
	if got := erasureCopyGetMax(erasureFacts{chunk: 4096}, erasureK3M1); got != 36864 {
		t.Fatalf("EC-to-EC copy-get max %d, want 36864", got)
	}
}

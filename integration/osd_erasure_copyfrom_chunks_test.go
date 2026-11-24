package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// Multi-chunk COPY_FROM and COPY_FROM2 into an optimized erasure-coded
// destination from a replicated source. The pinned source (Ceph v20.2.4)
// admits the copy and gives the cells their meaning:
//
//   - src/osd/PrimaryLogPG.cc:8106-8197 (do_osd_ops COPY_FROM/COPY_FROM2)
//     starts the copy on any pool; nothing refuses an erasure-coded
//     destination.
//   - src/osd/PrimaryLogPG.h:1398-1407 (get_copy_chunk_size) rounds the
//     destination primary's osd_copyfrom_max_chunk up to the destination
//     pool's required_alignment, its stripe_width (src/osd/osd_types.h:1794):
//     a 32 KiB limit is kept by a stripe width that divides it, and a k=3
//     stripe of three data chunks rounds it up. The Objecter prints that
//     max with every copy-get.
//     _write_copy_chunk (PrimaryLogPG.cc:10031-10095) writes the data of a
//     chunk that leaves the cursor incomplete to the temporary object on
//     every shard, and writes OMAP only on a pool that supports it.
//   - do_copy_get (9295-9486) runs on the source primary and walks the
//     source's OMAP when the source pool supports OMAP, whatever the
//     destination. A chunk of data spends the whole budget, so the first
//     chunk of a source with one chunk of data and an OMAP carries the data
//     only and the second the header and entries.
//   - process_copy_chunk (9694-9890) turns any chunk that carries an OMAP
//     header or entries into -EOPNOTSUPP when the destination pool does not
//     support OMAP (9713-9716), before it looks at the cursor; the copy is
//     answered with reply_op_error (CopyFromCallback::finish, 344-368), which
//     logs nothing, and a started temporary object is removed afterwards. So
//     an erasure-coded destination refuses every source with OMAP or a
//     header: after its data was staged when the OMAP comes in a later
//     chunk, at once when it comes in the first.
//   - An erasure-coded PG keeps one collection per shard, and the primary
//     names its temporary object after its own <pgid>s<shard>
//     (generate_temp_object, 8954-8962), which every shard stores under that
//     name (checkCopyTemp).
//
// The barrier is holdAtChunk of osd_copyfrom_multichunk_sources_test.go: the
// replicated source primary, which also holds a shard of the destination PG,
// is frozen while a copy-get is in flight, pauserd is set before it is
// resumed, and the next copy-get stays paused and unsent with one temporary
// object listed in every shard's collection.

// copyErasureChunkFixture starts the fixture of width's topology with an
// owned source pool (slot 1) and an owned destination pool (slot 2), one of
// them an optimized isa pool of width and the other replicated at the
// fixture's size: the destination is erasure-coded unless ecSource says the
// source is. It returns the run-level recovery and the erasure-coded pool's
// facts.
func copyErasureChunkFixture(t *testing.T, bound time.Duration, width erasureWidth, ecSource bool) (context.Context, ownedPools, *copyPrimaryRecovery, erasureFacts) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != width.topology || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skipf("%s with overwrites needs the BlueStore OSD topology %q", width, width.topology)
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
		if slices.Contains(r.ECProfilesRemaining, width.profile) {
			t.Error("owned erasure-code profile was not removed", width.profile)
		}
	})
	// The pools follow the fixture's hash policy, which the replicated-PG
	// profile admitted above sets.
	pg, hash := 1, isReplicatedPGFixtureProfile()
	if hash {
		pg = 8
	}
	ecSlot, replicatedSlot := 2, 1
	if ecSource {
		ecSlot, replicatedSlot = 1, 2
	}
	replicated := nativePool(t, ctx, "create", map[string]any{"slot": replicatedSlot, "pg_num": pg, "size": base.Replicas, "min_size": base.MinSize, "hashpspool": hash})
	if replicated.PoolID <= 0 || replicated.PoolID == base.Pool || replicated.PoolType != 1 {
		t.Fatal("owned pool is not a separate replicated pool", replicatedSlot, replicated.PoolID, replicated.PoolType)
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": ecSlot, "pg_num": pg, "hashpspool": hash, "erasure": true, "width": width.name, "optimized": true, "plugin": "isa"})
	if created.PoolType != 3 || !created.ECOptimized || created.Width != width.name || created.ErasureCodeProfile != width.profile || created.PoolID == replicated.PoolID {
		t.Fatal("owned pool is not the requested erasure-coded shape", ecSlot, created.PoolType, created.ECOptimized, created.Width, created.ErasureCodeProfile)
	}
	facts := erasurePoolMapFacts(t, ctx, c, created, width, true)
	// Registered after the pools, so it runs before their removal.
	t.Cleanup(func() { recovery.restore(t) })
	return ctx, ownedPools{ids: map[int]int64{ecSlot: created.PoolID, replicatedSlot: replicated.PoolID}, request: replicatedPGRequest}, recovery, facts
}

// copyErasureChunkLimit is the copy-get max the destination primary uses
// under the lowered limit: osd_copyfrom_max_chunk rounded up to the stripe
// width (get_copy_chunk_size).
func copyErasureChunkLimit(limit, stripe int) int {
	if stripe > 0 && limit%stripe != 0 {
		limit += stripe - limit%stripe
	}
	return limit
}

// copyErasureCell is one opcode and one source: 64 KiB of data, 32 KiB of
// data with an OMAP (the OMAP in the second chunk), or a small source with an
// OMAP (one chunk).
type copyErasureCell struct {
	copyPrimaryChangeCase
	source string
}

func (c copyErasureCell) label() string { return c.name + "/" + c.source }

// For COPY_FROM and COPY_FROM2, an unversioned copy from an owned replicated
// pool onto a natively seeded head of an owned optimized isa k=2,m=2 pool
// runs under the destination primary's osd_copyfrom_max_chunk lowered to
// 32 KiB and rounded up to the pool's native stripe_width (the copy-get
// max, copyErasureChunkLimit); the barrier requires every copy-get to print
// exactly that max.
//
// Data source (two chunks of data and one xattr): the copy is held before
// its second chunk by the barrier described above, with the same temporary
// object <pgid>s<shard> of the primary listed in every shard collection.
// After pause is cleared the same client op is answered once on its
// original connection with 0, the source's user version and ONDISK; the
// destination is natively the source (bytes, xattrs, user version, mtime),
// every shard drops the temporary object, the destination primary's PG log
// holds the copy once bound to the source write, and a fresh session reads
// the source bytes with nothing changing later.
//
// OMAP source in two chunks (one chunk of data, one xattr, a header and four
// entries): held the same way before the second chunk, with the data staged
// in the temporary object. The second chunk carries the OMAP, so the copy is
// answered once with -EOPNOTSUPP and per-op -EINPROGRESS; the seed is kept
// natively, every shard drops the temporary object, the PG log names neither
// the copy nor the source write, and a fresh session reads the seed.
//
// OMAP source in one chunk (a few bytes, a header and one entry), sent once
// without a barrier: -EOPNOTSUPP with per-op -EINPROGRESS at once, the seed
// kept, no temporary object on any shard before or after, nothing in the PG
// log.
//
// This classifies a server restriction: an erasure-coded destination refuses
// a source with OMAP or a header. The client does not inspect the source and
// does not emulate the copy. The k=3,m=1 and k=4,m=2 destinations, EC
// sources and a loss between chunks follow. Not verified: other plugins,
// unoptimized EC pools, and pg_temp placements.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2copyfromchunks key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDErasureOptimizedK2M2CopyFromChunksIntegration(t *testing.T) {
	runCopyErasureChunks(t, erasureK2M2, "osd-erasure-copyfrom-chunks-results.json")
}

// The cells above on an optimized isa k=3,m=1 destination on the four-OSD
// topology: its stripe width is three data chunks, which never divides
// 32 KiB, so every copy-get prints the limit rounded up to it, and the
// sources are sized from that max. There are two nonprimary data shards.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1copyfromchunks key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDErasureOptimizedK3M1CopyFromChunksIntegration(t *testing.T) {
	runCopyErasureChunks(t, erasureK3M1, "osd-erasure-copyfrom-chunks-k3m1-results.json")
}

// The cells above on an optimized isa k=4,m=2 destination on the six-OSD
// topology, whose six shard collections each list the temporary object.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2copyfromchunks key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, waits osd-ready clean without pg_temp proof
func TestCephOSDErasureOptimizedK4M2CopyFromChunksIntegration(t *testing.T) {
	runCopyErasureChunks(t, erasureK4M2, "osd-erasure-copyfrom-chunks-k4m2-results.json")
}

// erasureCopyGetMax is the copy-get max of a copy onto the erasure-coded
// pool of facts: the lowered limit rounded up to its stripe width.
func erasureCopyGetMax(facts erasureFacts, width erasureWidth) int {
	return copyErasureChunkLimit(multiChunkLimit, facts.chunk*width.k)
}

func runCopyErasureChunks(t *testing.T, width erasureWidth, name string) {
	ctx, pools, recovery, facts := copyErasureChunkFixture(t, 1500*time.Second, width, false)
	limit := erasureCopyGetMax(facts, width)
	t.Logf("destination pool %d: %s, stripe width %d, copy-get max %d, nonprimary shards %v", facts.pool, width, facts.chunk*width.k, limit, facts.nonprimary)
	keep := copyChunkReceipts(t, recovery, name)
	for _, tc := range copyPrimaryChangeCases {
		for _, source := range []string{"data", "omap-second-chunk", "omap-single-chunk"} {
			cell := copyErasureCell{tc, source}
			t.Run(cell.label(), func(t *testing.T) {
				receipt := copyChunkMutationReceipt{Cell: cell.label(), Mutation: "none", Chunk: uint64(limit)}
				defer func() {
					if !t.Skipped() {
						keep(receipt)
					}
				}()
				copyErasureChunks(t, ctx, pools, recovery, facts, limit, cell, &receipt)
			})
		}
	}
}

func copyErasureChunks(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, facts erasureFacts, limit int, cell copyErasureCell, receipt *copyChunkMutationReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("ec-%d-%s", cell.code, cell.source))
	facts.checkPlacement(t, r.dstRoute)
	r.chunkMax = limit
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), r.dstRoute.ID
	r.lowerCopyChunk(&receipt.ChunkRestore)
	seeded := r.seedDestination()

	var write cephmsgr.OSDReply
	var copied nativeXattrSnapshot
	switch cell.source {
	case "data":
		data := make([]byte, 2*limit)
		for i := range data {
			data[i] = byte(i*19 + i>>8 + 11)
		}
		write = r.writeSourceHead("source write", 1700009500, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
		copied = poolObserve(t, ctx, 1, r.source, "", "")
		if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != write.UserVersion {
			t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, write.UserVersion)
		}
	default:
		// One chunk of data spends the first copy-get's whole budget, so the
		// OMAP comes in the second.
		data := make([]byte, limit)
		entries := []cephmsgr.OSDOmapEntry{{Key: "ec-key-0", Value: []byte("v0")}, {Key: "ec-key-1", Value: []byte{}}, {Key: "ec-key-2", Value: []byte{0, 0xff}}, {Key: "ec-key-3", Value: []byte("v3")}}
		if cell.source == "omap-single-chunk" {
			data, entries = []byte("small OMAP source"), entries[:1]
		} else {
			for i := range data {
				data[i] = byte(i*37 + i>>6 + 1)
			}
		}
		var state omapWriteState
		write, state = r.writeOmapSource(data, []byte("ec header"), entries)
		copied, receipt.OmapEntries, receipt.SourceOmap = state.nativeXattrSnapshot, len(entries), omapDigest(t, state)
	}
	receipt.SourceBytes, receipt.CopiedUser = len(copied.Data), copied.Version
	sourceKeys := []pgHistoryTarget{{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}}

	route, src := r.placeForCopy()
	request := r.copyRequest(cell.code, route)
	if cell.source == "omap-single-chunk" {
		// One chunk: the refusal comes with the first copy-get's answer and
		// no temporary object is started.
		o := mutationConnection(t, r.client, ctx, route)
		reply, err := o.Request(ctx, request)
		o.Close()
		receipt.Reply = newCopyChunkReply(reply)
		t.Logf("answered: code=%d ops=%v tid=%d attempt=%d err=%v", reply.Code, receipt.Reply.OpResults, reply.Transaction, reply.RetryAttempt, err)
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || server.Code != reply.Code || errors.As(err, &unknown) {
			t.Fatalf("the single-chunk copy ended with %v (code %d)", err, reply.Code)
		}
		if err := judgeChunkAnswer(-95, reply, reply.Submission.Transaction, 0); err != nil || reply.Transaction == 0 {
			t.Fatal(err)
		}
		h := &copyChunkHold{route: route, op: opsInFlightOp{TID: reply.Transaction}}
		r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, false, receipt)
		return
	}
	cursors := []int{0, 0}
	receipt.Cursors = cursors
	h := r.holdAtChunk(request, route, src, 2, 0, copied.Version, cursors, &receipt.Steps)
	receipt.BarrierReqid, receipt.HeldChunk, receipt.Temporary = h.op.Reqid, h.read.OSDOps, h.temp
	reply := r.awaitHeld(h, receipt)
	want, user := int32(0), copied.Version
	if cell.source != "data" {
		want, user = -95, 0
	}
	if err := judgeChunkAnswer(want, reply, h.op.TID, user); err != nil {
		t.Fatal(err)
	}
	if now := poolObserve(t, ctx, 1, r.source, "", ""); !sameNative(now, copied) {
		t.Fatalf("the copy changed its source: %s; was %s", nativeDigest(now), nativeDigest(copied))
	}
	r.settleChunkCopy(h, reply, seeded, copied, sourceKeys, true, receipt)
}

func TestCopyFromErasureChunkJudgements(t *testing.T) {
	for _, c := range []struct{ limit, stripe, want int }{{32768, 8192, 32768}, {32768, 16384, 32768}, {32768, 32768, 32768}, {32768, 12288, 36864}, {32768, 65536, 65536}, {32768, 0, 32768}} {
		if got := copyErasureChunkLimit(c.limit, c.stripe); got != c.want {
			t.Errorf("chunk %d at stripe %d: %d, want %d", c.limit, c.stripe, got, c.want)
		}
	}

	// The op tracker of an erasure-coded primary names the client op by the
	// primary's shard of the PG; a replicated PG's op names the PG.
	const gid, inc = 4641, 33
	route := osdFixtureOracle{Up: []int32{1, 0, 2, 3}, Acting: []int32{1, 0, 2, 3}, Replicas: 4, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: 0, PoolType: 3, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	ops := opsInFlightSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.3s0", Epoch: 30, FlagPoint: "started"}}}
	objecter := objecterRequestsSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3"}
	if op, err := checkCopyClientOp(ops, objecter, route, gid, inc); err != nil || op.TID != 5 {
		t.Fatal("EC client op refused:", err)
	}
	for name, pgid := range map[string]string{"no shard": "4.3", "raw PG": "4.d6c2a1e3", "another shard": "4.3s1", "another PG": "4.4s0"} {
		o := ops
		o.Ops = []opsInFlightOp{ops.Ops[0]}
		o.Ops[0].PGID = pgid
		if _, err := checkCopyClientOp(o, objecter, route, gid, inc); err == nil {
			t.Error("EC client op accepted:", name)
		}
	}
	replicated := route
	replicated.Shard, replicated.PoolType, replicated.Replicas = -1, 1, 3
	o := ops
	o.Before, o.After, o.Ops = replicated, replicated, []opsInFlightOp{ops.Ops[0]}
	r := objecter
	r.Before, r.After = replicated, replicated
	for name, c := range map[string]struct {
		pgid string
		ok   bool
	}{"the PG": {"4.3", true}, "the raw PG": {"4.d6c2a1e3", true}, "a shard": {"4.3s0", false}} {
		o.Ops[0].PGID = c.pgid
		if _, err := checkCopyClientOp(o, r, replicated, gid, inc); (err == nil) != c.ok {
			t.Error("replicated client op misjudged:", name, err)
		}
	}
}

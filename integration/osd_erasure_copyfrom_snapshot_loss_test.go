package integration_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// copyErasurePairSnapshotLossCells are the snapshot cells: each loss
// between chunks of a copy from a clone, unversioned and pinned.
func copyErasurePairSnapshotLossCells() []copyErasurePairLossCell {
	var cells []copyErasurePairLossCell
	for _, tc := range copyPrimaryChangeCases {
		for _, lost := range []string{"destination", "shard", "source"} {
			for _, pinned := range []bool{false, true} {
				cells = append(cells, copyErasurePairLossCell{tc, lost, pinned, true})
			}
		}
	}
	return cells
}

// For COPY_FROM and COPY_FROM2, the loss cells of
// TestCephOSDErasureOptimizedCopyFromErasureToErasureLossIntegration with a
// copy from a snapshot: the source head of the owned optimized isa k=2,m=2
// pool is written once, a self-managed snapshot is allocated and one Go
// SETXATTR under its context clones the head (make_writeable,
// src/osd/PrimaryLogPG.cc:8641-8812; on an erasure-coded PG every shard
// applies the clone in the same transaction). The copy names the snap id
// and is unversioned, or pinned to the clone's user version, which the
// first copy-get then asserts too. It is held before its second chunk onto
// the owned optimized isa k=3,m=1 destination, and one OSD is SIGKILLed.
//
// Every chunk reads the clone: do_copy_get runs on the object context the
// copy-get's snap id selects (find_object_context, PrimaryLogPG.cc:12067-12264),
// also after the copy restarts, and on a source primary that changed. So:
//
//   - Destination primary lost: the caller's wait ends as an unknown outcome
//     with the barrier's transaction, the seed is kept natively, the new
//     primary (under a pg_temp) names neither the copy nor a source request
//     in its PG log, and every survivor drops the temporary object. One
//     explicit resubmission with the original identity, attempt 1 and the
//     same snap id and version executes once: it is answered 0 with the
//     clone's user version, and the destination is natively the clone. A
//     second source write would not change what this copy reads, so the
//     execution is told from a replay by the seed and the empty PG log seen
//     before it.
//   - Shared non-primary shard lost: the copy restarts from its first
//     copy-get under the same client op, which is seen unsent under pauserd
//     with the temporary object gone from the survivors; the original
//     connection is then answered once, at attempt 0, with the clone.
//   - Source primary lost: the destination keeps its primary and restarts
//     the copy; the source PG activates on another primary shard in a newer
//     interval with the survivors in their slots (checkErasureCopySourceMoved),
//     the clone is read there, and the original connection is answered once
//     with the clone.
//
// Each cell keeps the raw aggregate and per-op results. The destination
// primary's PG log holds the copy once as a successful modify without any
// source request ID: the final copy-get returns the request IDs of the
// copied object's own source log entries (get_object_reqids,
// src/osd/PGLog.h:477-507, called at PrimaryLogPG.cc:9439-9446), and the
// entry that creates a clone carries an empty request ID
// (PrimaryLogPG.cc:8777-8782).
// The source head and the clone are natively unchanged. After recovery the
// original destination primary leads again with that single entry, a fresh
// session reads the clone and a delayed native readback agrees, and no
// destination shard lists a temporary object.
//
// Not verified: snapshot sources of the other width pairs, a trimmed clone,
// pool snapshots and a held final transaction of a snapshot copy.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedcopyfromerasuretoerasuresnapshotloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, allocates owned snapshots
func TestCephOSDErasureOptimizedCopyFromErasureToErasureSnapshotLossIntegration(t *testing.T) {
	runCopyErasurePairLoss(t, erasurePairK2M2ToK3M1, "osd-erasure-copyfrom-snapshot-loss-results.json", copyErasurePairSnapshotLossCells(), nil)
}

// checkSnapshotSource reads the source head and the clone natively: both
// must be what they were when the copy was sent.
func (r *copyPrimaryChangeRun) checkSnapshotSource(want copySnapshotLossWant) {
	r.t.Helper()
	if head := poolObserve(r.t, r.ctx, 1, r.source, "", ""); !sameNative(head, want.head) {
		r.t.Fatalf("the source head changed: %s; was %s", nativeDigest(head), nativeDigest(want.head))
	}
	if clone := poolObserveSnap(r.t, r.ctx, 1, r.source, "", "", want.snap); !sameNative(clone, want.clone) {
		r.t.Fatalf("the clone at snap %d changed: %s; was %s", want.snap, nativeDigest(clone), nativeDigest(want.clone))
	}
}

// entryOnly checks a destination PG-log snapshot for the copy of a clone at
// version on route's head: the copy's identity once and no source request.
func (r *copyPrimaryChangeRun) entryOnly(route osdFixtureOracle, copyKey pgHistoryTarget, want copySnapshotLossWant, version string) error {
	r.t.Helper()
	s := r.history(route, copyKey, want.sourceKeys...)
	var others [][]pgHistoryLocation
	for i := range want.sourceKeys {
		others = append(others, s.Targets[1+i].Locations)
	}
	return copyEntryOnly(s.Targets[0].Locations, hobjectHead(route), version, others...)
}

// settleSnapshotDestinationLoss is settleDestinationLoss for a copy from a
// clone: the unknown outcome, the kept seed, the temporary object's removal
// on the survivors, an empty PG log on the new primary and one explicit
// resubmission of request, which must copy the clone once.
func (r *copyPrimaryChangeRun) settleSnapshotDestinationLoss(h *copyChunkHold, code cephmsgr.OSDOpcode, request cephmsgr.OSDRequest, seeded nativeXattrSnapshot, copyKey pgHistoryTarget, want copySnapshotLossWant, receipt *copyChunkLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	primary := h.route.ID
	var lost copyChunkOutcome
	select {
	case lost = <-h.done:
	case <-time.After(60 * time.Second):
		t.Fatal("the copy's caller did not return after its primary was killed")
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != h.op.TID || lost.reply.Submission.GlobalID != r.gid ||
		lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != h.route.Epoch {
		t.Fatalf("lost copy metadata (barrier tid %d): %+v %v", h.op.TID, lost.reply, lost.err)
	}
	receipt.Unknown = &copyChunkUnknown{TID: lost.reply.Submission.Transaction, GlobalID: lost.reply.Submission.GlobalID,
		Incarnation: lost.reply.Submission.ClientIncarnation, Epoch: lost.reply.Submission.MapEpoch, Cause: outcomeCause(lost.err)}
	h.o.Close()
	t.Logf("osd.%d killed (exit %d) between chunks of the snapshot copy; it ended unknown (%s): %v", primary, receipt.KillExit, receipt.Unknown.Cause, lost.err)
	r.recovery.markDown(t, ctx, primary)
	r.restartedPrimary = true
	moved := r.waitMoved()
	receipt.NewPrimary, receipt.MovedEpoch = moved.ID, moved.Epoch
	// Survivors keep their shards under the pg_temp (settleDestinationLoss).
	listing := moved
	if moved.PGTemp && h.route.Shard >= 0 {
		listing = h.route
	}
	for _, osd := range h.route.Acting {
		if osd != primary {
			r.waitTempRemoved(osd, listing)
			receipt.TempRemoved = append(receipt.TempRemoved, osd)
		}
	}
	r.recovery.clearPauseReads(t, ctx)

	native := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(native)
	if !sameNative(native, seeded) {
		t.Fatalf("the destination is not the seed after the loss: %s; seed %s", nativeDigest(native), nativeDigest(seeded))
	}
	r.checkSnapshotSource(want)
	s := r.history(moved, copyKey, want.sourceKeys...)
	for i := range 1 + len(want.sourceKeys) {
		if len(s.Targets[i].Locations) != 0 {
			t.Fatalf("the new primary's PG log names %s: %+v", s.Targets[i].Reqid, s.Targets[i].Locations)
		}
	}
	t.Logf("seed kept after losing osd.%d between chunks of the snapshot copy: new primary osd.%d at epoch %d; temporary removed on osd %v", primary, moved.ID, moved.Epoch, receipt.TempRemoved)

	again := r.copyRequest(code, moved)
	again.Operations[0].CopyFrom = request.Operations[0].CopyFrom
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission of the snapshot copy to the new primary", moved, again)
	receipt.recordReply(answered)
	t.Logf("resubmitted to osd.%d: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, receipt.Reply.OpResults,
		answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if answered.Transaction != h.op.TID || answered.RetryAttempt != 1 || len(answered.Operations) != 1 || answered.Operations[0].Result != 0 || answered.UserVersion != want.clone.Version {
		t.Fatalf("the resubmission did not copy the clone once: tid %d attempt %d ops %v user %d (clone %d)", answered.Transaction, answered.RetryAttempt, receipt.Reply.OpResults, answered.UserVersion, want.clone.Version)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	if !sameCopy(after, want.clone, want.clone.Version) {
		t.Fatalf("the resubmission did not copy the clone: %s; clone %s", nativeDigest(after), nativeDigest(want.clone))
	}
	if err := r.entryOnly(moved, copyKey, want, replayVersion(answered)); err != nil {
		t.Fatalf("new primary osd.%d after the resubmission: %v", moved.ID, err)
	}
	r.finishEntryOnly(after, answered.Transaction, copyKey, want, replayVersion(answered))
	receipt.Final = nativeDigest(after)
}

// finishSnapshotAfterReplicaLoss is finishCopyAfterReplicaLoss for a copy
// from a clone: the original connection must be answered once, at attempt
// 0, with the clone's user version, and the destination must be the clone.
func (r *copyPrimaryChangeRun) finishSnapshotAfterReplicaLoss(h *copyChunkHold, now osdFixtureOracle, copyKey pgHistoryTarget, want copySnapshotLossWant, receipt *copyChunkLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	r.recovery.clearPauseReads(t, ctx)
	var got copyChunkOutcome
	select {
	case got = <-h.done:
	case <-time.After(120 * time.Second):
		t.Fatal("the restarted snapshot copy was not answered after pause was cleared")
	}
	receipt.recordReply(got.reply)
	t.Logf("answered on the original connection: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s err=%v", got.reply.Code, receipt.Reply.OpResults,
		got.reply.Transaction, got.reply.RetryAttempt, got.reply.Flags, got.reply.UserVersion, replayVersion(got.reply), got.err)
	if got.err != nil || got.reply.Code != 0 || got.reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || got.reply.Transaction != h.op.TID || got.reply.RetryAttempt != 0 ||
		len(got.reply.Operations) != 1 || got.reply.Operations[0].Result != 0 || got.reply.UserVersion != want.clone.Version {
		t.Fatalf("the restarted snapshot copy was not answered once with the clone: %+v %v", receipt.Reply, got.err)
	}
	h.o.Close()
	r.checkSnapshotSource(want)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(after)
	if !sameCopy(after, want.clone, want.clone.Version) {
		t.Fatalf("the destination is not the clone: %s; clone %s", nativeDigest(after), nativeDigest(want.clone))
	}
	if err := r.entryOnly(now, copyKey, want, replayVersion(got.reply)); err != nil {
		t.Fatalf("osd.%d after the snapshot copy: %v", now.ID, err)
	}
	r.finishEntryOnly(after, got.reply.Transaction, copyKey, want, replayVersion(got.reply))
	receipt.Final = nativeDigest(after)
}

// finishEntryOnly is finish for a copy from a clone: after recovery the
// original destination primary leads again, a fresh session reads want, a
// delayed native readback agrees, and the PG log holds the copy once at
// version without any source request.
func (r *copyPrimaryChangeRun) finishEntryOnly(want nativeXattrSnapshot, last uint64, copyKey pgHistoryTarget, source copySnapshotLossWant, version string) {
	r.t.Helper()
	r.recovery.restore(r.t)
	if r.t.Failed() {
		r.t.FailNow()
	}
	route := r.waitReturned()
	read := r.pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want.Data) + 64)})
	o := mutationConnection(r.t, r.client, r.ctx, route)
	reply, err := o.Request(r.ctx, read)
	o.Close()
	if err != nil || reply.Code != 0 || reply.Transaction <= last || !bytes.Equal(reply.Operations[0].Data, want.Data) {
		r.t.Fatalf("fresh-session read: err=%v code=%d tid=%d (last %d) %d bytes", err, reply.Code, reply.Transaction, last, len(reply.Operations[0].Data))
	}
	time.Sleep(2 * time.Second)
	if after := poolObserve(r.t, r.ctx, 2, r.dest, "", ""); !sameNative(after, want) {
		r.t.Fatalf("destination changed after recovery: version %d -> %d, %d bytes", want.Version, after.Version, len(after.Data))
	}
	if err := r.entryOnly(route, copyKey, source, version); err != nil {
		r.t.Fatalf("returned primary osd.%d: %v", route.ID, err)
	}
	r.t.Logf("after recovery: primary osd.%d epoch %d, fresh read tid %d, destination version %d unchanged, copy %s once in the PG log without a source request",
		route.ID, route.Epoch, reply.Transaction, want.Version, copyKey)
}

func TestCopyFromErasureSnapshotLossCells(t *testing.T) {
	head, snapshot := copyErasurePairHeadLossCells(), copyErasurePairSnapshotLossCells()
	var labels []string
	for _, cell := range append(slices.Clone(head), snapshot...) {
		labels = append(labels, cell.label())
	}
	want := []string{
		"COPY_FROM/destination", "COPY_FROM/shard", "COPY_FROM/source", "COPY_FROM/source-pinned", "COPY_FROM/destination-before-commit",
		"COPY_FROM2/destination", "COPY_FROM2/shard", "COPY_FROM2/source", "COPY_FROM2/source-pinned", "COPY_FROM2/destination-before-commit",
		"COPY_FROM/snapshot/destination", "COPY_FROM/snapshot/destination-pinned", "COPY_FROM/snapshot/shard", "COPY_FROM/snapshot/shard-pinned",
		"COPY_FROM/snapshot/source", "COPY_FROM/snapshot/source-pinned",
		"COPY_FROM2/snapshot/destination", "COPY_FROM2/snapshot/destination-pinned", "COPY_FROM2/snapshot/shard", "COPY_FROM2/snapshot/shard-pinned",
		"COPY_FROM2/snapshot/source", "COPY_FROM2/snapshot/source-pinned",
	}
	if !slices.Equal(labels, want) {
		t.Fatalf("cells %q", labels)
	}
	for _, cell := range head {
		if cell.snapshot || cell.pinned && cell.lost != "source" {
			t.Fatalf("head cell %s", cell.label())
		}
	}
}

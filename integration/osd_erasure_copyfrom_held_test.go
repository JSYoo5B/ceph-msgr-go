package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// The held final transaction of copyErasureHeldCommitLoss
// (osd_erasure_copyfrom_sources_test.go) with a pinned head copy, a copy
// from a clone, and a destination replica lost instead of the primary. The
// hold is the same in every case: the copy's source read is complete, its
// final transaction waits in the destination PG's EC write queue behind a
// blocker write that reads a page from a frozen data shard, and the
// temporary object is still listed on the primary.
//
// A pinned head copy and a copy from a clone, with the primary lost: the
// copy's final transaction was never generated (ECCommon.cc:773-916), so
// the outcome is unknown, the seed is kept and the new primary's PG log
// names neither the copy nor a source request. One explicit resubmission
// with the original identity, attempt 1, the same snap id and the same
// pinned version then executes once on the new primary. No second source
// write precedes it: a pinned copy would refuse a newer source (ASSERT_VER,
// src/osd/PrimaryLogPG.cc:6529-6542), and a copy from a clone reads the clone
// whatever the head holds. Execution is told from a replay by the kept seed
// and the empty PG log seen before the resubmission.
//
// A replica lost in the held interval (the destination shard that is
// neither the primary, the frozen OSD nor the source primary):
//
//   - The primary stays, so the interval change runs on_change on it
//     (PrimaryLogPG.cc:13125-13216): apply_and_flush_repops requeues the
//     client ops of every pending repop, the copy's and the blocker's
//     (PrimaryLogPG.cc:12925-12971), and the temporary object is removed
//     with the primary's other temporary objects (PGBackend::on_change_cleanup,
//     src/osd/PGBackend.cc:507-521). The copy's final transaction was never
//     generated, so no shard holds any of it.
//   - The requeued ops are not discarded, since the primary did not change
//     (PG::can_discard_op, src/osd/PG.cc:1926-1937), and wait for the PG to
//     peer. Peering queries every member that is up for its info and waits
//     for each answer (PeeringState::GetInfo::get_infos,
//     src/osd/PeeringState.cc:7520-7558), so the PG cannot peer while the
//     frozen OSD, still up, cannot answer.
//
// So after the replica's SIGKILL and its mark-down, before the frozen OSD is
// resumed, bracketed snapshots of the primary must show the PG without the
// replica under the same primary, the copy's client op still unanswered with
// no op of the source in the primary's Objecter (the copy did not restart),
// the blocker's op unanswered, and no temporary object listed on the
// primary. The frozen OSD is then resumed. The PG peers, and the copy runs
// again from its first chunk under the same client op: both the copy and the
// blocker are answered once on their original connections at attempt 0.
// The destination is natively the source at its user version, the blocker
// holds its 16 bytes, and the primary's PG log holds the copy once, bound to
// the source write, and the blocker once. After recovery a fresh session
// reads the copy, a delayed readback agrees, and no shard lists a temporary
// object.

// copyHeldReplicaLoss is the part of a held cell's receipt that records a
// destination replica lost in the held interval.
type copyHeldReplicaLoss struct {
	OSD            int32    `json:"osd"`
	Shard          int      `json:"shard"`
	PlacementEpoch uint32   `json:"after_kill_placement_epoch"`
	Acting         []int32  `json:"after_kill_acting"`
	CopyPoint      string   `json:"copy_flag_point_after_kill"`
	CopyEvents     []string `json:"copy_events_after_kill"`
	BlockerPoint   string   `json:"blocker_flag_point_after_kill"`
	PrimaryStaging string   `json:"primary_staging_sha256_after_kill"`
	ObservedAfter  string   `json:"observed_after_freeze"`
	BlockerCode    int32    `json:"blocker_code"`
	BlockerAttempt int32    `json:"blocker_attempt"`
	BlockerUser    uint64   `json:"blocker_user_version"`
	BlockerReplay  string   `json:"blocker_replay_version"`
}

// copyHeldLostReplica picks the destination shard a replica-before-commit
// cell loses: the first member of the acting set in shard order that is
// neither the primary, the frozen OSD nor the source primary. It returns the
// OSD and its shard.
func copyHeldLostReplica(dst osdFixtureOracle, frozen, sourcePrimary int32) (int32, int, error) {
	for shard, osd := range dst.Acting {
		if osd != dst.ID && osd != frozen && osd != sourcePrimary && osd != crushItemNone {
			return osd, shard, nil
		}
	}
	return -1, -1, fmt.Errorf("no shard of %v is neither the primary osd.%d, the frozen osd.%d nor the source primary osd.%d", dst.Acting, dst.ID, frozen, sourcePrimary)
}

// checkHeldReplicaLoss judges one round of snapshots of the destination
// primary after a replica was lost in the held interval and before the
// frozen OSD is resumed: the copy's client op (checkCopyClientOp at the
// copy's epoch) is still the barrier's tid and unanswered, the primary's
// Objecter holds no op of the source, the blocker's op (reqid) is still
// unanswered, and the primary's own shard collection lists no temporary
// object. It returns the copy's and the blocker's ops.
func checkHeldReplicaLoss(ops, blocker opsInFlightSnapshot, objecter objecterRequestsSnapshot, staging copyStagingSnapshot, at osdFixtureOracle, gid, tid uint64, blockerReqid string) (opsInFlightOp, opsInFlightOp, error) {
	op, err := checkCopyClientOp(ops, objecter, at, gid, copyPrimaryCopyIncarnation)
	if err != nil {
		return op, opsInFlightOp{}, err
	}
	if op.TID != tid || slices.Contains(op.Events, "commit_sent") || strings.HasPrefix(op.FlagPoint, "commit sent") {
		return op, opsInFlightOp{}, fmt.Errorf("the client op is %s at %q, not the held copy tid %d", op.Reqid, op.FlagPoint, tid)
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished || len(objecter.Ops) != 0 {
		return op, opsInFlightOp{}, fmt.Errorf("the destination primary's Objecter holds %d ops of the source: the copy restarted", len(objecter.Ops))
	}
	if !blocker.Complete || len(blocker.Failures) != 0 || blocker.Code != 0 || blocker.AbsenceEstablished || blocker.OSD != at.ID || len(blocker.Ops) != 1 {
		return op, opsInFlightOp{}, fmt.Errorf("the blocker snapshot of osd.%d holds %d ops: code %d, %q", blocker.OSD, len(blocker.Ops), blocker.Code, blocker.Failures)
	}
	b := blocker.Ops[0]
	if b.Reqid != blockerReqid || slices.Contains(b.Events, "commit_sent") || strings.HasPrefix(b.FlagPoint, "commit sent") {
		return op, b, fmt.Errorf("the blocker op is %s at %q, not the unanswered %s", b.Reqid, b.FlagPoint, blockerReqid)
	}
	if _, err := checkCopyTemp(staging, at.ID, at, 0); err != nil {
		return op, b, err
	}
	return op, b, nil
}

// loseHeldReplica SIGKILLs the cell's lost replica (receipt.Lost) in the
// held interval of e and checks what the package comment above describes.
func (r *copyPrimaryChangeRun) loseHeldReplica(e *copyErasureHold, receipt *copyErasureLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	h, held, lost := e.h, e.held, receipt.Lost
	loss := &copyHeldReplicaLoss{OSD: lost, Shard: receipt.Shard}
	held.Replica = loss
	// The observations end a second before the fixture resumes the frozen
	// OSD itself.
	work, stop := context.WithDeadline(ctx, e.thawDeadline.Add(-time.Second))
	defer stop()
	receipt.KillExit = r.recovery.kill(t, work, lost)
	r.recovery.markDown(t, work, lost)
	var now osdFixtureOracle
	var verdict error = errors.New("no observation")
	raws := ""
	for observed := false; !observed; {
		if copyBudgetSpent(work) {
			t.Fatalf("the replica loss in the held interval was not observed before the frozen osd.%d had to be resumed: %v%s", e.frozen, verdict, raws)
		}
		h.pending(t, "after the replica was lost in the held interval")
		select {
		case got := <-e.blocked:
			t.Fatalf("the blocker write was answered while osd.%d was frozen: code %d err %v", e.frozen, got.reply.Code, got.err)
		default:
		}
		// The op tracker's own placement brackets give the PG without the
		// replica, so no separate placement read spends the freeze.
		ops := copyOpsInFlight(t, work, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		now = ops.Before
		if verdict = checkReplicaLost(h.route, now, lost); verdict != nil {
			raws = fmt.Sprintf("\nop tracker: %s", boundedRaw(ops.Raw))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// The client ops keep the epoch they were sent at.
		at := now
		at.Epoch = h.route.Epoch
		blocker := copyOpsInFlight(t, work, 2, e.blocker, nil, r.gid, copyHeldBlockerIncarnation)
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		staging := copyStaging(t, work, e.primary, now)
		op, b, err := checkHeldReplicaLoss(ops, blocker, objecter, staging, at, r.gid, h.op.TID, held.BlockerReqid)
		if verdict = err; err == nil && work.Err() == nil {
			loss.PlacementEpoch, loss.Acting = now.Epoch, slices.Clone(now.Acting)
			loss.CopyPoint, loss.CopyEvents, loss.BlockerPoint, loss.PrimaryStaging = op.FlagPoint, op.Events, b.FlagPoint, staging.SHA256
			loss.ObservedAfter = time.Since(e.start).Round(10 * time.Millisecond).String()
			observed = true
			continue
		}
		raws = fmt.Sprintf("\nop tracker: %s\nblocker: %s\nobjecter: %s", boundedRaw(ops.Raw), boundedRaw(blocker.Raw), boundedRaw(objecter.Raw))
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("osd.%d (shard %d) lost in the held interval: pg at epoch %d acting %v; copy %s at %q and blocker %s at %q unanswered, no source op, no temporary object on osd.%d, %s after the freeze",
		lost, loss.Shard, now.Epoch, now.Acting, h.op.Reqid, loss.CopyPoint, held.BlockerReqid, loss.BlockerPoint, e.primary, loss.ObservedAfter)
	if err := r.recovery.thaw(e.frozen); err != nil {
		t.Fatalf("resume osd.%d within the freeze budget: %v", e.frozen, err)
	}
	e.stopWork()
	held.FrozenFor = time.Since(e.start).Round(10 * time.Millisecond).String()

	var got copyChunkOutcome
	select {
	case got = <-h.done:
	case <-time.After(120 * time.Second):
		t.Fatal("the requeued copy was not answered after the frozen OSD was resumed")
	}
	receipt.recordReply(got.reply)
	t.Logf("copy answered on the original connection: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s err=%v", got.reply.Code, receipt.Reply.OpResults,
		got.reply.Transaction, got.reply.RetryAttempt, got.reply.Flags, got.reply.UserVersion, replayVersion(got.reply), got.err)
	if got.err != nil || got.reply.Code != 0 || got.reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || got.reply.Transaction != h.op.TID || got.reply.RetryAttempt != 0 ||
		len(got.reply.Operations) != 1 || got.reply.Operations[0].Result != 0 || got.reply.UserVersion != h.write.UserVersion {
		t.Fatalf("the requeued copy was not answered once for the held write: %+v %v", receipt.Reply, got.err)
	}
	var answered copyChunkOutcome
	select {
	case answered = <-e.blocked:
	case <-time.After(120 * time.Second):
		t.Fatal("the requeued blocker was not answered after the frozen OSD was resumed")
	}
	b := answered.reply
	loss.BlockerCode, loss.BlockerAttempt, loss.BlockerUser, loss.BlockerReplay = b.Code, b.RetryAttempt, b.UserVersion, replayVersion(b)
	held.BlockerOutcome, held.BlockerSubmitTID = "answered", b.Submission.Transaction
	if answered.err != nil || b.Code != 0 || b.Flags&cephmsgr.OSDReplyOnDisk == 0 || b.RetryAttempt != 0 || b.Transaction == 0 || b.Submission.Transaction != b.Transaction ||
		fmt.Sprintf("client.%d.%d:%d", r.gid, copyHeldBlockerIncarnation, b.Transaction) != held.BlockerReqid || len(b.Operations) != 1 || b.Operations[0].Result != 0 {
		t.Fatalf("the requeued blocker was not answered once: code %d attempt %d tid %d (op %s) err %v", b.Code, b.RetryAttempt, b.Transaction, held.BlockerReqid, answered.err)
	}
	h.o.Close()
	e.blockerConn.Close()

	written := r.observeWritten(h, e.data)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(after)
	if !sameCopy(after, written, h.write.UserVersion) {
		t.Fatalf("the destination is not the complete copy: %s; source %s", nativeDigest(after), nativeDigest(written))
	}
	wantBlocker := slices.Clone(e.blockerSeed.Data)
	copy(wantBlocker[e.blockerWrite.Offset:], e.blockerWrite.Data)
	blocker := poolObserve(t, ctx, 2, e.blocker, "", "")
	held.BlockerNative = nativeDigest(blocker)
	if blocker.Code != 0 || !bytes.Equal(blocker.Data, wantBlocker) || blocker.Version != b.UserVersion {
		t.Fatalf("the blocker does not hold its one write: code %d version %d (reply %d) %d bytes", blocker.Code, blocker.Version, b.UserVersion, len(blocker.Data))
	}
	route := poolPlace(t, ctx, 2, r.dest, "")
	blockerKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyHeldBlockerIncarnation, TID: b.Transaction}
	binding := copyFromHistoryWant{object: hobjectHead(route), version: replayVersion(got.reply), sourceUserVersion: h.write.UserVersion}
	s := r.history(route, e.copyKey, e.writeKey, blockerKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("osd.%d after the requeued copy: %v", route.ID, err)
	}
	if err := copyEntryOnly(s.Targets[2].Locations, hobjectHead(e.blockerAt), replayVersion(b)); err != nil {
		t.Fatalf("osd.%d after the requeued blocker: %v", route.ID, err)
	}
	r.finish(after, got.reply.Transaction, e.copyKey, e.writeKey, binding)
	receipt.Final = nativeDigest(after)
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
}

// settlePinnedDestinationLoss is settleDestinationLoss for a copy pinned to
// the held source write: the unknown outcome, the kept seed, the temporary
// object's removal on the survivors and an empty PG log on the new primary,
// then one explicit resubmission of request (the same pinned version) with
// the original identity and attempt 1, which must copy that write once.
func (r *copyPrimaryChangeRun) settlePinnedDestinationLoss(h *copyChunkHold, code cephmsgr.OSDOpcode, request cephmsgr.OSDRequest, seeded nativeXattrSnapshot, copyKey, writeKey pgHistoryTarget, data []byte, receipt *copyChunkLossReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	if request.Operations[0].CopyFrom.SourceVersion != h.write.UserVersion {
		t.Fatalf("the copy is pinned to version %d, not the held write's %d", request.Operations[0].CopyFrom.SourceVersion, h.write.UserVersion)
	}
	lost, moved := r.settleLostCopy(h, seeded, receipt)
	written := r.observeWritten(h, data)
	if s := r.history(moved, copyKey, writeKey); len(s.Targets[0].Locations) != 0 || len(s.Targets[1].Locations) != 0 {
		t.Fatalf("the new primary's PG log names the lost copy: %+v %+v", s.Targets[0].Locations, s.Targets[1].Locations)
	}
	t.Logf("seed kept after losing osd.%d while it held the pinned copy: new primary osd.%d at epoch %d; temporary removed on osd %v", h.route.ID, moved.ID, moved.Epoch, receipt.TempRemoved)

	again := r.copyRequest(code, moved)
	again.Operations[0].CopyFrom = request.Operations[0].CopyFrom
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission of the pinned copy to the new primary", moved, again)
	receipt.recordReply(answered)
	t.Logf("resubmitted to osd.%d: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, receipt.Reply.OpResults,
		answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if answered.Transaction != h.op.TID || answered.RetryAttempt != 1 || len(answered.Operations) != 1 || answered.Operations[0].Result != 0 || answered.UserVersion != h.write.UserVersion {
		t.Fatalf("the resubmission did not copy the pinned write once: tid %d attempt %d ops %v user %d (write %d)", answered.Transaction, answered.RetryAttempt, receipt.Reply.OpResults, answered.UserVersion, h.write.UserVersion)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	if !sameCopy(after, written, h.write.UserVersion) {
		t.Fatalf("the resubmission did not copy the pinned write: %s; source %s", nativeDigest(after), nativeDigest(written))
	}
	binding := copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(answered), sourceUserVersion: h.write.UserVersion}
	s := r.history(moved, copyKey, writeKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("new primary osd.%d after the resubmission: %v", moved.ID, err)
	}
	r.finish(after, answered.Transaction, copyKey, writeKey, binding)
	receipt.Final = nativeDigest(after)
}

func TestCopyHeldLostReplica(t *testing.T) {
	dst := osdFixtureOracle{ID: 0, PrimaryID: 0, Acting: []int32{0, 1, 2, 3}}
	for _, c := range []struct {
		name          string
		acting        []int32
		frozen, src   int32
		osd           int32
		shard         int
		refuseOrValid bool
	}{
		{"source primary on shard 2", []int32{0, 1, 2, 3}, 1, 2, 3, 3, true},
		{"source primary on shard 3", []int32{0, 1, 2, 3}, 1, 3, 2, 2, true},
		{"source primary outside the PG", []int32{0, 1, 2, 3}, 1, 5, 2, 2, true},
		{"an empty position is skipped", []int32{0, 1, crushItemNone, 3}, 1, 5, 3, 3, true},
		{"no candidate", []int32{0, 1, 2}, 1, 2, -1, -1, false},
	} {
		route := dst
		route.Acting = c.acting
		osd, shard, err := copyHeldLostReplica(route, c.frozen, c.src)
		if (err == nil) != c.refuseOrValid || osd != c.osd || shard != c.shard {
			t.Errorf("%s: osd.%d shard %d err %v", c.name, osd, shard, err)
		}
	}
}

func TestCheckHeldReplicaLoss(t *testing.T) {
	const gid, tid = 4722, 31
	at := osdFixtureOracle{Up: []int32{0, 1, crushItemNone, 3}, Acting: []int32{0, 1, crushItemNone, 3}, Replicas: 4, Pool: 4, Name: "dst", PoolType: 3,
		ID: 0, PrimaryID: 0, Shard: 0, Epoch: 30, PGID: "4.2", Address: "v2:192.0.2.11:6800/1"}
	bracket := at
	bracket.Epoch = 33
	blockerReqid := fmt.Sprintf("client.%d.%d:%d", gid, copyHeldBlockerIncarnation, 32)
	build := func() (opsInFlightSnapshot, opsInFlightSnapshot, objecterRequestsSnapshot, copyStagingSnapshot) {
		ops := opsInFlightSnapshot{Complete: true, Before: bracket, After: bracket, OSD: 0, PGID: "4.2", Client: fmt.Sprintf("client.%d.%d", gid, copyPrimaryCopyIncarnation),
			Ops: []opsInFlightOp{{Reqid: fmt.Sprintf("client.%d.%d:%d", gid, copyPrimaryCopyIncarnation, tid), TID: tid, PGID: "4.2s0", Epoch: 30, FlagPoint: "waiting for peered", Events: []string{"initiated", "queued_for_pg"}}}}
		blocker := opsInFlightSnapshot{Complete: true, Before: bracket, After: bracket, OSD: 0, PGID: "4.2", Client: fmt.Sprintf("client.%d.%d", gid, copyHeldBlockerIncarnation),
			Ops: []opsInFlightOp{{Reqid: blockerReqid, TID: 32, PGID: "4.2s0", Epoch: 30, FlagPoint: "waiting for peered"}}}
		objecter := objecterRequestsSnapshot{Complete: true, Before: bracket, After: bracket, OSD: 0, PGID: "4.2"}
		staging := copyStagingSnapshot{Complete: true, OSD: 0, PGID: "4.2", Shard: 0, Collection: "4.2s0_head", Objects: 3, Max: 1024}
		return ops, blocker, objecter, staging
	}
	ops, blocker, objecter, staging := build()
	if _, _, err := checkHeldReplicaLoss(ops, blocker, objecter, staging, at, gid, tid, blockerReqid); err != nil {
		t.Fatal("the expected shape was refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *opsInFlightSnapshot, *objecterRequestsSnapshot, *copyStagingSnapshot){
		"copy answered": func(o, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops[0].Events = []string{"commit_sent"}
		},
		"another copy op": func(o, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops[0].TID = tid + 1
		},
		"copy op gone": func(o, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) { o.Ops = nil },
		"copy restarted its read": func(_, _ *opsInFlightSnapshot, j *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			j.Ops = []objecterRequestsOp{{TID: 9}}
		},
		"blocker answered": func(_, b *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			b.Ops[0].FlagPoint = "commit sent; apply or cleanup"
		},
		"blocker gone": func(_, b *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) { b.Ops = nil },
		"another blocker": func(_, b *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			b.Ops[0].Reqid = "client.4722.35:33"
		},
		"blocker of another OSD": func(_, b *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) { b.OSD = 1 },
		"blocker snapshot failed": func(_, b *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			b.Complete = false
		},
		"temporary object listed": func(_, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, s *copyStagingSnapshot) {
			s.Temp = []string{"0#-6:00000000:::temp_4.2s0_0_31_1:head#"}
		},
		"incomplete listing": func(_, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, s *copyStagingSnapshot) {
			s.Complete = false
		},
		"placement still has it": func(o, _ *opsInFlightSnapshot, j *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Before.Acting, o.After.Acting = []int32{0, 1, 2, 3}, []int32{0, 1, 2, 3}
		},
		"snapshot of another OSD": func(o, _ *opsInFlightSnapshot, j *objecterRequestsSnapshot, _ *copyStagingSnapshot) { j.OSD = 3 },
		"absence claimed": func(o, _ *opsInFlightSnapshot, j *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			j.AbsenceEstablished = true
		},
		"copy op at another epoch": func(o, _ *opsInFlightSnapshot, _ *objecterRequestsSnapshot, _ *copyStagingSnapshot) {
			o.Ops[0].Epoch = 33
		},
	} {
		ops, blocker, objecter, staging := build()
		edit(&ops, &blocker, &objecter, &staging)
		if _, _, err := checkHeldReplicaLoss(ops, blocker, objecter, staging, at, gid, tid, blockerReqid); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

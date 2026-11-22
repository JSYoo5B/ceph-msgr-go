package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Destination primary loss after a COPY_FROM or COPY_FROM2 finished reading
// its source and committed locally, while the primary still awaited replica
// commits. The pinned source (Ceph v20.2.4) gives the barrier its meaning:
//
//   - src/osd/PrimaryLogPG.cc:344-368 (CopyFromCallback::finish) runs the
//     client's op context again with execute_ctx once the source read is
//     complete (process_copy_chunk fetches more while the cursor is
//     incomplete), so the final copy transaction is issued under the client's
//     own op. A temporary-object write of a multi-chunk copy is a separate
//     op context without the client's op.
//   - src/osd/ReplicatedBackend.cc:1134-1187 (issue_op) marks that op
//     "waiting for subops from <replicas>" before it dispatches the repops, with the
//     flag point "waiting for sub ops"; ReplicatedBackend.cc:641-664
//     (op_commit) marks "op_commit" at the primary's own commit and
//     ReplicatedBackend.cc:666-721 (do_repop_reply) "sub_op_commit_rec" at
//     each replica's commit. The client is answered only once every acting
//     replica committed.
//
// A destination replica that is paused before the copy is sent therefore
// holds the client op at that point: the source read is complete and the
// primary has committed locally and still awaits at least one replica commit. The destination primary is then SIGKILLed.

// copyLossCell is one opcode and the number of replicas that commit before
// the loss: one (one replica paused) or none (both paused).
type copyLossCell struct {
	copyPrimaryChangeCase
	commits int
}

func (c copyLossCell) label() string {
	if c.commits == 0 {
		return c.name + "/primary-only"
	}
	return c.name + "/replica-committed"
}

// copyLossReceipt is the archived public receipt of every cell: identities,
// versions, digests and the native observations, never keys or addresses.
type copyLossReceipt struct {
	Cell           string   `json:"cell"`
	Commits        int      `json:"replica_commits"`
	SourcePrimary  int32    `json:"source_primary"`
	Primary        int32    `json:"destination_primary"`
	Frozen         []int32  `json:"paused_replicas"`
	BarrierEpoch   uint32   `json:"barrier_epoch"`
	BarrierReqid   string   `json:"barrier_reqid"`
	BarrierPoint   string   `json:"barrier_flag_point"`
	BarrierEvents  []string `json:"barrier_events"`
	OpsSHA256      string   `json:"ops_in_flight_sha256"`
	ObjecterSHA256 string   `json:"objecter_requests_sha256"`
	ObjecterOps    int      `json:"objecter_source_ops"`
	KillExit       int      `json:"kill_exit_code"`
	Unknown        struct {
		TID         uint64 `json:"tid"`
		GlobalID    uint64 `json:"global_id"`
		Incarnation uint32 `json:"incarnation"`
		Epoch       uint32 `json:"epoch"`
		// Ended records how the wait ended. Cause stores only the error
		// class because the error text can carry endpoints.
		Ended string `json:"ended"`
		Cause string `json:"cause"`
	} `json:"unknown_outcome"`
	PausedFor   string `json:"paused_for"`
	NewPrimary  int32  `json:"new_primary"`
	MovedEpoch  uint32 `json:"moved_epoch"`
	Outcome     string `json:"outcome"`
	Native      string `json:"native_after_loss"`
	LogVersion  string `json:"log_version,omitempty"`
	Resubmitted struct {
		Code        int32  `json:"code"`
		OpResult    *int32 `json:"op_result,omitempty"`
		TID         uint64 `json:"tid"`
		Attempt     int32  `json:"attempt"`
		Flags       uint32 `json:"flags"`
		UserVersion uint64 `json:"user_version"`
		Replay      string `json:"replay_version"`
	} `json:"resubmission"`
	Final string `json:"native_final"`
}

// nativeDigest summarises one native observation for the receipt.
func nativeDigest(n nativeXattrSnapshot) string {
	digest := sha256.Sum256(n.Data)
	names := make([]string, 0, len(n.Attributes))
	for name := range attrNames(n) {
		names = append(names, name)
	}
	slices.Sort(names)
	return fmt.Sprintf("code %d version %d mtime %d.%09d %d bytes sha256 %s attrs %q", n.Code, n.Version, n.Seconds, n.Nanos, len(n.Data), hex.EncodeToString(digest[:]), names)
}

// copyStagedEvents is the barrier's judgement of the client op's events:
// exactly one "waiting for subops from" event that names every replica of
// the route, then exactly one "op_commit" of the primary's own commit, and
// exactly commits "sub_op_commit_rec" events after the sub-op event, at the
// flag point "waiting for sub ops" and without "commit_sent", so no reply
// was sent. replicas is the route's acting set without its primary; the
// pinned set<pg_shard_t> prints them in ascending order, separated by
// commas, and a replicated shard prints as its OSD id
// (src/osd/osd_types.cc:208-215).
func copyStagedEvents(op opsInFlightOp, replicas []int32, commits int) error {
	if op.FlagPoint != "waiting for sub ops" {
		return fmt.Errorf("flag point %q; want waiting for sub ops", op.FlagPoint)
	}
	sorted := slices.Clone(replicas)
	slices.Sort(sorted)
	names := make([]string, len(sorted))
	for i, osd := range sorted {
		names[i] = strconv.Itoa(int(osd))
	}
	want := "waiting for subops from " + strings.Join(names, ",")
	sent, local, received := -1, -1, 0
	for i, event := range op.Events {
		switch {
		case event == want:
			if sent >= 0 {
				return fmt.Errorf("%q twice in %q", want, op.Events)
			}
			sent = i
		case strings.HasPrefix(event, "waiting for subops from "):
			return fmt.Errorf("sub-op event %q does not name the replicas %v", event, sorted)
		case event == "op_commit":
			if local >= 0 || sent < 0 {
				return fmt.Errorf("op_commit repeated or before the sub-op marker: %q", op.Events)
			}
			local = i
		case event == "sub_op_commit_rec":
			if sent < 0 {
				return fmt.Errorf("a replica commit before the sub-op marker: %q", op.Events)
			}
			received++
		case event == "commit_sent":
			return fmt.Errorf("the reply was already sent: %q", op.Events)
		}
	}
	if sent < 0 || local < 0 || received != commits {
		return fmt.Errorf("events %q: sub-op marker present %t, local commit %t, %d replica commits (want %d)", op.Events, sent >= 0, local >= 0, received, commits)
	}
	return nil
}

// checkCopyStaged judges the barrier from two bracketed snapshots of the
// destination primary: the client op (checkCopyClientOp) staged with commits
// replica commits (copyStagedEvents), and an internal Objecter that holds no
// op of the owned source, so no source read is outstanding. The snapshots
// show what the primary held at that moment, not absence from history.
func checkCopyStaged(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, route osdFixtureOracle, gid uint64, incarnation int32, commits int) (opsInFlightOp, error) {
	op, err := checkCopyClientOp(ops, objecter, route, gid, incarnation)
	if err != nil {
		return op, err
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished {
		return op, errors.New("a snapshot claims absence")
	}
	if len(objecter.Ops) != 0 {
		return op, fmt.Errorf("the objecter still holds %d ops of the source: %+v", len(objecter.Ops), objecter.Ops)
	}
	replicas := slices.DeleteFunc(slices.Clone(route.Acting), func(osd int32) bool { return osd == route.ID })
	if len(replicas) != len(route.Acting)-1 || len(replicas) == 0 {
		return op, fmt.Errorf("acting set %v does not hold primary osd.%d once with replicas", route.Acting, route.ID)
	}
	return op, copyStagedEvents(op, replicas, commits)
}

// copyLossFrozen chooses the destination replicas to pause: both for a cell
// without replica commits, otherwise one that is not the source's primary,
// whose source read must still be served.
func copyLossFrozen(acting []int32, primary, sourcePrimary int32, commits int) ([]int32, error) {
	replicas := slices.DeleteFunc(slices.Clone(acting), func(osd int32) bool { return osd == primary })
	if len(replicas) != 2 || len(acting) != 3 {
		return nil, fmt.Errorf("acting set %v is not a primary osd.%d with two replicas", acting, primary)
	}
	if commits == 0 {
		if sourcePrimary != primary {
			return nil, fmt.Errorf("source primary osd.%d is not the destination primary osd.%d", sourcePrimary, primary)
		}
		return replicas, nil
	}
	for _, osd := range replicas {
		if osd != sourcePrimary {
			return []int32{osd}, nil
		}
	}
	return nil, fmt.Errorf("no replica of %v outside source primary osd.%d", replicas, sourcePrimary)
}

// For COPY_FROM and COPY_FROM2, a copy of a fresh source head onto a natively
// seeded destination head is held after its source read: destination
// replicas are paused before it is sent, so the primary stages the final
// transaction and waits for their commits. Two bracketed native snapshots of
// the destination primary are the barrier: its op tracker holds the client op
// waiting for sub ops, with its own commit and as many replica commits as
// replicas left running, and its Objecter holds no source read. The primary
// is then SIGKILLed, the caller's wait ends as an unknown outcome, the
// replicas are resumed and the dead OSD is marked down.
//
// With one replica committed the copy must survive: native readback under the
// new primary is the complete copy and the new primary's PG log holds it
// once, bound to the source write. With no replica committed the barrier does
// not decide the result, so the test records which complete state native
// readback shows, the seed or the full copy, requires the PG log to agree,
// and refuses anything else. The barrier records primary-side progress;
// receiver-side delivery time is unknown. A second Go write changes the source and
// one explicit resubmission goes to the new primary: it must be answered
// from the log if the copy survived and must execute once, copying the
// second source, if it did not. After recovery the original primary must lead
// again, and a fresh-session read, a delayed native readback and its PG log
// must show the same single copy.
//
// Not verified: the loss of a transaction staged on the primary alone (the
// kept-seed outcome has only unit coverage), which paused replica applied the
// copy or when, multi-chunk staging, OMAP, EC destinations, pinned source
// versions, snapshot sources and host power loss.
//
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs and restarts them, waits osd-ready clean without pg_temp proof; pool delete
func TestCephOSDCopyFromStagedDestinationLossIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var mu sync.Mutex
	var receipts []copyLossReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-destination-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("destination-loss receipt", err)
		}
	})
	for _, tc := range copyPrimaryChangeCases {
		for _, commits := range []int{1, 0} {
			cell := copyLossCell{tc, commits}
			t.Run(cell.label(), func(t *testing.T) {
				t.Cleanup(func() { recovery.restore(t) })
				receipt := copyLossReceipt{Cell: cell.label(), Commits: commits}
				defer func() {
					if t.Skipped() {
						return
					}
					mu.Lock()
					receipts = append(receipts, receipt)
					mu.Unlock()
				}()
				copyDestinationLoss(t, ctx, pools, recovery, cell, &receipt)
			})
		}
	}
}

func copyDestinationLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, cell copyLossCell, receipt *copyLossReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("loss-%d-%d", cell.code, cell.commits))
	primary := r.dstRoute.ID
	if cell.commits == 0 && r.srcRoute.ID != primary {
		// The paused replicas cannot serve the source read, so the source
		// PG must have the destination primary as its primary too.
		if !isReplicatedPGFixtureProfile() {
			t.Skipf("the one-PG legacy owned pools put the source on osd.%d and the destination on osd.%d", r.srcRoute.ID, primary)
		}
		found := false
		for i := 0; i < 64 && !found; i++ {
			name := fmt.Sprintf("%s-%d", r.source, i)
			route := poolPlace(t, ctx, 1, name, "")
			if route.ID == primary && route.PrimaryID == primary && poolObserve(t, ctx, 1, name, "", "").Code == -2 {
				r.source, r.srcRoute, found = name, route, true
			}
		}
		if !found {
			t.Fatalf("no fresh source name in 64 has primary osd.%d", primary)
		}
	}
	frozen, err := copyLossFrozen(r.dstRoute.Acting, primary, r.srcRoute.ID, cell.commits)
	if err != nil {
		t.Fatal(err)
	}
	receipt.SourcePrimary, receipt.Primary, receipt.Frozen = r.srcRoute.ID, primary, frozen
	first, firstSource := r.writeSource("first source write", 1700009000, []byte("copy-loss\x00\xff first source bytes"), []byte{5, 0xff})
	poolSeed(t, ctx, 2, r.dest, "", []byte("seeded destination bytes"), []nativeXattrEntry{{Name: []byte("seed-attr"), Value: []byte{7}}})
	seeded := poolObserve(t, ctx, 2, r.dest, "", "")
	if seeded.Code != 0 || seeded.Version == 0 {
		t.Fatal("native destination seed", seeded.Code)
	}
	route := poolPlace(t, ctx, 2, r.dest, "")
	source := poolPlace(t, ctx, 1, r.source, "")
	if !samePlacement(route, r.dstRoute) || source.ID != r.srcRoute.ID || slices.Contains(frozen, source.ID) {
		t.Fatalf("placements moved before the copy: destination %+v, source primary osd.%d", route, source.ID)
	}

	o := mutationConnection(t, r.client, ctx, route)
	defer o.Close()
	paused := time.Now()
	// Reserve the last four seconds for thaw within a single pause interval.
	thawDeadline := paused.Add(16 * time.Second)
	workDeadline := thawDeadline.Add(-4 * time.Second)
	work, stopWork := context.WithDeadline(ctx, workDeadline)
	defer stopWork()
	// Every frozen replica has its own lease, all with one deadline; the
	// fixture resumes each by it also while a snapshot below is still
	// outstanding.
	for _, osd := range frozen {
		recovery.freeze(t, work, osd, thawDeadline)
	}
	call, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	type outcome struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		reply, err := o.Request(call, r.copyRequest(cell.code, route))
		done <- outcome{reply, err}
	}()
	pending := func(stage string) {
		t.Helper()
		select {
		case got := <-done:
			t.Fatalf("the staged copy was answered %s: code %d err %v", stage, got.reply.Code, got.err)
		default:
		}
	}
	// The barrier must be reached well inside the 20 s heartbeat grace of
	// the paused replicas, so that they are resumed, not marked down.
	var staged opsInFlightOp
	for {
		pending("before the barrier")
		ops := copyOpsInFlight(t, work, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, pools.ids[1], r.source)
		if !time.Now().Before(workDeadline) {
			t.Fatal("pause budget expired before the staged barrier")
		}
		if staged, err = checkCopyStaged(ops, objecter, route, r.gid, copyPrimaryCopyIncarnation, cell.commits); err == nil {
			receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = ops.Before.Epoch, staged.Reqid, staged.FlagPoint, staged.Events
			receipt.OpsSHA256, receipt.ObjecterSHA256, receipt.ObjecterOps = ops.SHA256, objecter.SHA256, len(objecter.Ops)
			t.Logf("barrier on osd.%d epoch %d with osd %v paused: op %s flag point %q events %q; objecter holds %d source ops",
				ops.OSD, ops.Before.Epoch, frozen, staged.Reqid, staged.FlagPoint, staged.Events, len(objecter.Ops))
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	pending("at the barrier")

	receipt.KillExit = recovery.kill(t, work, primary)
	// The killed daemon's connection ends the wait; a bound well inside the
	// paused replicas' heartbeat grace keeps them from being marked down.
	var lost outcome
	select {
	case lost = <-done:
		receipt.Unknown.Ended = "connection"
	case <-work.Done():
		t.Fatal("pause budget expired waiting for the unknown outcome", work.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != staged.TID ||
		lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != route.Epoch {
		t.Fatalf("lost copy metadata (barrier tid %d): %+v %v", staged.TID, lost.reply, lost.err)
	}
	receipt.Unknown.TID, receipt.Unknown.GlobalID = lost.reply.Submission.Transaction, lost.reply.Submission.GlobalID
	receipt.Unknown.Incarnation, receipt.Unknown.Epoch, receipt.Unknown.Cause = lost.reply.Submission.ClientIncarnation, lost.reply.Submission.MapEpoch, outcomeCause(lost.err)
	if !time.Now().Before(workDeadline) {
		t.Fatal("pause budget expired before thaw")
	}
	for _, osd := range frozen {
		if err := recovery.thaw(osd); err != nil {
			t.Fatalf("resume osd.%d within the pause budget: %v", osd, err)
		}
	}
	receipt.PausedFor = time.Since(paused).Round(10 * time.Millisecond).String()
	// Release the old endpoint before reconnecting to the restarted OSD.
	o.Close()
	t.Logf("osd.%d killed (exit %d) at the barrier; the copy ended unknown (%s, %s): %v; osd %v resumed after %s",
		primary, receipt.KillExit, receipt.Unknown.Ended, receipt.Unknown.Cause, lost.err, frozen, receipt.PausedFor)
	recovery.markDown(t, ctx, primary)
	r.restartedPrimary = true
	moved := r.waitMoved()
	receipt.NewPrimary, receipt.MovedEpoch = moved.ID, moved.Epoch

	// Native evidence of the loss's result, before any resubmission.
	native := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(native)
	if after := poolObserve(t, ctx, 1, r.source, "", ""); !sameNative(after, firstSource) {
		t.Fatal("the source changed", after.Version, firstSource.Version)
	}
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: staged.TID}
	firstKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction}
	s := r.history(moved, copyKey, firstKey)
	in := copyLossInputs{commits: cell.commits, object: hobjectHead(moved), seeded: seeded, firstSource: firstSource, firstUser: first.UserVersion}
	survived, logVersion, err := judgeCopyLoss(in, native, s.Targets[0].Locations, s.Targets[1].Locations)
	if err != nil {
		t.Fatalf("after losing osd.%d, new primary osd.%d: %v", primary, moved.ID, err)
	}
	receipt.Outcome, receipt.LogVersion = "seed kept", logVersion
	if survived {
		receipt.Outcome = "copy survived"
	}
	t.Logf("%s after losing osd.%d: new primary osd.%d at epoch %d; destination %s", receipt.Outcome, primary, moved.ID, moved.Epoch, receipt.Native)

	second, secondSource := r.writeSource("second source write", 1700009200, []byte("copy-loss second source bytes, longer"), []byte{6, 0xfe})
	in.secondSource, in.secondUser = secondSource, second.UserVersion
	secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
	again := r.copyRequest(cell.code, moved)
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission to the new primary", moved, again)
	receipt.Resubmitted.OpResult = &answered.Operations[0].Result
	receipt.Resubmitted.Code, receipt.Resubmitted.TID, receipt.Resubmitted.Attempt = answered.Code, answered.Transaction, answered.RetryAttempt
	receipt.Resubmitted.Flags, receipt.Resubmitted.UserVersion, receipt.Resubmitted.Replay = uint32(answered.Flags), answered.UserVersion, replayVersion(answered)
	t.Logf("resubmitted to osd.%d: code=%d op=%d tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, answered.Operations[0].Result, answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if answered.Transaction != staged.TID || answered.RetryAttempt != 1 || answered.Operations[0].Result != 0 {
		t.Fatalf("resubmission: tid %d attempt %d op %d", answered.Transaction, answered.RetryAttempt, answered.Operations[0].Result)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	binding, err := judgeCopyResubmission(in, survived, logVersion, native, after, answered.UserVersion, replayVersion(answered))
	if err != nil {
		t.Fatal(err)
	}
	bound, unbound := firstKey, secondKey
	if !survived {
		bound, unbound = secondKey, firstKey
	}
	s = r.history(moved, copyKey, bound, unbound)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("new primary osd.%d after the resubmission: %v", moved.ID, err)
	}
	if got := s.Targets[2].Locations; len(got) != 0 {
		t.Fatalf("the other source write %s is bound at the destination: %+v", unbound, got)
	}
	r.finish(after, answered.Transaction, copyKey, bound, binding)
	receipt.Final = nativeDigest(after)
}

// outcomeCause is the class of an unknown outcome's cause for the receipt.
func outcomeCause(err error) string {
	switch {
	case errors.Is(err, io.EOF):
		return "EOF"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset"
	default:
		return "other"
	}
}

// copyLossInputs is what the post-loss judgements compare against: the
// cell's replica commits, the destination head's hobject, the seeded
// destination, both source writes and their user versions.
type copyLossInputs struct {
	commits                   int
	object                    string
	seeded                    nativeXattrSnapshot
	firstSource, secondSource nativeXattrSnapshot
	firstUser, secondUser     uint64
}

// sameCopy says whether a native destination is a complete copy of source:
// its bytes, xattrs, mtime and the source write's user version (a copy gives
// the destination the source's user version and mtime,
// src/osd/PrimaryLogPG.cc:355-356 and finish_copyfrom).
func sameCopy(dest, source nativeXattrSnapshot, user uint64) bool {
	return dest.Code == 0 && bytes.Equal(dest.Data, source.Data) && reflect.DeepEqual(attrNames(dest), attrNames(source)) &&
		dest.Version == user && dest.Seconds == source.Seconds && dest.Nanos == source.Nanos
}

// judgeCopyLoss decides the native result of the loss, before any
// resubmission, from the destination's native readback and the new
// primary's PG-log positions of the copy and of the first source write. The
// copy survived when the destination is a complete copy of the first source
// and the log holds the copy once, bound to that write; it returns the log
// entry's version. The seed was kept when the destination is the unchanged
// seed and the log names neither identity, which a cell with a replica
// commit refuses. Anything else is refused.
func judgeCopyLoss(in copyLossInputs, native nativeXattrSnapshot, copyLocs, firstLocs []pgHistoryLocation) (bool, string, error) {
	switch {
	case sameCopy(native, in.firstSource, in.firstUser):
		if len(copyLocs) != 1 {
			return true, "", fmt.Errorf("the surviving copy is in the PG log %d times: %+v", len(copyLocs), copyLocs)
		}
		version := copyLocs[0].Version
		if err := copyFromDestinationBinding(copyLocs, firstLocs, copyFromHistoryWant{object: in.object, version: version, sourceUserVersion: in.firstUser}); err != nil {
			return true, version, err
		}
		return true, version, nil
	case in.commits > 0:
		return false, "", fmt.Errorf("a replica committed the copy before the loss, yet the destination is %s", nativeDigest(native))
	case sameNative(native, in.seeded):
		if len(copyLocs) != 0 || len(firstLocs) != 0 {
			return false, "", fmt.Errorf("the seeded destination's PG log names the copy: %+v %+v", copyLocs, firstLocs)
		}
		return false, "", nil
	default:
		return false, "", fmt.Errorf("the destination is neither the seed nor a complete copy: %s; seed %s", nativeDigest(native), nativeDigest(in.seeded))
	}
}

// judgeCopyResubmission checks the explicit resubmission after the loss,
// sent once the second source write replaced the source: if the copy
// survived it must be answered from the log (the first source's user
// version, the log entry's replay version, an unchanged destination); if the
// seed was kept it must execute once and copy the second source. It returns
// the PG-log binding the destination must then show.
func judgeCopyResubmission(in copyLossInputs, survived bool, logVersion string, before, after nativeXattrSnapshot, user uint64, replay string) (copyFromHistoryWant, error) {
	if survived {
		if user != in.firstUser || replay != logVersion || !sameNative(after, before) {
			return copyFromHistoryWant{}, fmt.Errorf("the resubmission was not answered from the log: user %d (want %d) replay %s (want %s); destination %s",
				user, in.firstUser, replay, logVersion, nativeDigest(after))
		}
		return copyFromHistoryWant{object: in.object, version: logVersion, sourceUserVersion: in.firstUser}, nil
	}
	if user != in.secondUser || !sameCopy(after, in.secondSource, user) {
		return copyFromHistoryWant{}, fmt.Errorf("the resubmission did not copy the second source once: user %d (source %d); destination %s", user, in.secondUser, nativeDigest(after))
	}
	return copyFromHistoryWant{object: in.object, version: replay, sourceUserVersion: in.secondUser}, nil
}

func TestCopyFromDestinationLossJudgements(t *testing.T) {
	const gid, inc = 4641, 33
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.0", RawPGID: "4.d6c2a1e7", Address: "v2:192.0.2.11:6800/1"}
	at := route
	at.Epoch = 31
	events := []string{"initiated", "header_read", "throttled", "all_read", "dispatched", "queued_for_pg", "reached_pg", "started", "waiting for subops from 0,2", "op_commit", "sub_op_commit_rec"}
	staged := opsInFlightSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.0", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.0", Epoch: 30, FlagPoint: "waiting for sub ops", Events: events}}}
	idle := objecterRequestsSnapshot{Complete: true, Before: at, After: at, OSD: 1, PGID: "4.0"}
	if op, err := checkCopyStaged(staged, idle, route, gid, inc, 1); err != nil || op.TID != 5 {
		t.Fatal("staged copy with one replica commit refused:", err)
	}
	primaryOnly := staged
	primaryOnly.Ops = []opsInFlightOp{staged.Ops[0]}
	primaryOnly.Ops[0].Events = events[:len(events)-1]
	if _, err := checkCopyStaged(primaryOnly, idle, route, gid, inc, 0); err != nil {
		t.Fatal("staged copy without replica commits refused:", err)
	}
	if _, err := checkCopyStaged(primaryOnly, idle, route, gid, inc, 1); err == nil {
		t.Error("a missing replica commit accepted")
	}
	if _, err := checkCopyStaged(staged, idle, route, gid, inc, 0); err == nil {
		t.Error("an unexpected replica commit accepted")
	}
	edited := func(edit func([]string) []string) []string { return edit(slices.Clone(events)) }
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot){
		"incomplete":           func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Complete = false },
		"objecter failure":     func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.Failures = []string{"x"} },
		"brackets differ":      func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.After.Epoch = 32 },
		"another OSD answered": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) { r.OSD = 2 },
		"another incarnation":  func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Client = "client.4641.34" },
		"op at another epoch":  func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Epoch = 31 },
		"absence claimed":      func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.AbsenceEstablished = true },
		"source read pending": func(_ *opsInFlightSnapshot, r *objecterRequestsSnapshot) {
			r.Ops = []objecterRequestsOp{{TID: 9, OSDOps: []string{"copy-get max 8388608 in=29b"}}}
		},
		"still started": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].FlagPoint = "started" },
		"reply sent": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].FlagPoint = "commit sent; apply or cleanup"
		},
		"commit_sent event": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(o.Ops[0].Events, "commit_sent")
		},
		"no sub-ops sent": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { return slices.Delete(e, 8, 9) })
		},
		"other replicas": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { e[8] = "waiting for subops from 0"; return e })
		},
		"unsorted replicas": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { e[8] = "waiting for subops from 2,0"; return e })
		},
		"sub-ops sent twice": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { return slices.Insert(e, 9, e[8]) })
		},
		"no local commit": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { return slices.Delete(e, 9, 10) })
		},
		"local commit twice": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { return slices.Insert(e, 9, "op_commit") })
		},
		"local commit too soon": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { e[8], e[9] = e[9], e[8]; return e })
		},
		"replica commit before the sub-ops": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = edited(func(e []string) []string { return append(append(e[:8:8], "sub_op_commit_rec"), e[8:10]...) })
		},
		"two replica commits": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(o.Ops[0].Events, "sub_op_commit_rec")
		},
	} {
		o, r := staged, idle
		o.Ops = slices.Clone(staged.Ops)
		o.Ops[0].Events = slices.Clone(events)
		edit(&o, &r)
		if _, err := checkCopyStaged(o, r, route, gid, inc, 1); err == nil {
			t.Error("staged barrier accepted:", name)
		}
	}

	for _, tc := range []struct {
		acting          []int32
		primary, source int32
		commits         int
		want            []int32
		refused         bool
	}{
		{[]int32{1, 0, 2}, 1, 1, 0, []int32{0, 2}, false},
		{[]int32{1, 0, 2}, 1, 0, 0, nil, true},
		{[]int32{1, 0, 2}, 1, 0, 1, []int32{2}, false},
		{[]int32{1, 0, 2}, 1, 2, 1, []int32{0}, false},
		{[]int32{1, 0, 2}, 1, 1, 1, []int32{0}, false},
		{[]int32{1, 0}, 1, 1, 1, nil, true},
		{[]int32{0, 2, 3}, 1, 1, 1, nil, true},
	} {
		got, err := copyLossFrozen(tc.acting, tc.primary, tc.source, tc.commits)
		if (err != nil) != tc.refused || !slices.Equal(got, tc.want) {
			t.Errorf("copyLossFrozen(%v, %d, %d, %d) = %v, %v; want %v refused %t", tc.acting, tc.primary, tc.source, tc.commits, got, err, tc.want, tc.refused)
		}
	}

	attr := func(name string, value byte) []nativeXattrEntry {
		return []nativeXattrEntry{{Name: []byte(name), Value: []byte{value}}}
	}
	seed := nativeXattrSnapshot{Version: 2, Seconds: 1700000001, Data: []byte("seed"), Attributes: attr("seed-attr", 7)}
	src1 := nativeXattrSnapshot{Version: 1, Seconds: 1700009000, Data: []byte("first"), Attributes: attr("copy-primary-attr", 5)}
	src2 := nativeXattrSnapshot{Version: 3, Seconds: 1700009200, Data: []byte("second"), Attributes: attr("copy-primary-attr", 6)}
	in := copyLossInputs{commits: 0, object: "4:e7854363:::dst:head", seeded: seed, firstSource: src1, secondSource: src2, firstUser: 1, secondUser: 3}
	zero, one := 0, uint64(1)
	copyAt := func(version string) []pgHistoryLocation {
		return []pgHistoryLocation{{Tier: "log", Index: 4, Version: version, Op: "modify", Object: in.object, ReturnCode: &zero}}
	}
	extraAt := func(version string, user uint64) []pgHistoryLocation {
		return []pgHistoryLocation{{Tier: "extra_reqids", Index: 4, ExtraIndex: &zero, Version: version, Op: "modify", Object: in.object, UserVersion: &user}}
	}
	if survived, version, err := judgeCopyLoss(in, src1, copyAt("28'3"), extraAt("28'3", 1)); err != nil || !survived || version != "28'3" {
		t.Fatal("surviving copy refused:", survived, version, err)
	}
	if survived, _, err := judgeCopyLoss(in, seed, nil, nil); err != nil || survived {
		t.Fatal("kept seed refused:", survived, err)
	}
	committed := in
	committed.commits = 1
	if _, _, err := judgeCopyLoss(committed, seed, nil, nil); err == nil {
		t.Error("a kept seed accepted after a replica commit")
	}
	partial := src1
	partial.Attributes = nil
	newer := src1
	newer.Version = 2
	touched := src1
	touched.Nanos = 1
	for name, c := range map[string]struct {
		native      nativeXattrSnapshot
		copy, first []pgHistoryLocation
	}{
		"copy without its xattr":    {partial, copyAt("28'3"), extraAt("28'3", 1)},
		"copy at another version":   {newer, copyAt("28'3"), extraAt("28'3", 1)},
		"copy with another mtime":   {touched, copyAt("28'3"), extraAt("28'3", 1)},
		"copy not in the log":       {src1, nil, nil},
		"copy twice in the log":     {src1, append(copyAt("28'3"), copyAt("28'4")...), extraAt("28'3", 1)},
		"copy bound to nothing":     {src1, copyAt("28'3"), nil},
		"copy bound elsewhere":      {src1, copyAt("28'3"), extraAt("28'4", 1)},
		"copy of another version":   {src1, copyAt("28'3"), extraAt("28'3", 3)},
		"seed with the copy logged": {seed, copyAt("28'3"), nil},
		"seed with the write bound": {seed, nil, extraAt("28'3", 1)},
		"neither seed nor copy":     {src2, nil, nil},
		"absent destination":        {nativeXattrSnapshot{Code: -2}, nil, nil},
	} {
		if _, _, err := judgeCopyLoss(in, c.native, c.copy, c.first); err == nil {
			t.Error("post-loss state accepted:", name)
		}
	}

	if want, err := judgeCopyResubmission(in, true, "28'3", src1, src1, 1, "28'3"); err != nil || want.version != "28'3" || want.sourceUserVersion != one {
		t.Fatal("answer from the log refused:", want, err)
	}
	executed := src2
	if want, err := judgeCopyResubmission(in, false, "", seed, executed, 3, "40'2"); err != nil || want.version != "40'2" || want.sourceUserVersion != 3 {
		t.Fatal("single execution refused:", want, err)
	}
	for name, c := range map[string]struct {
		survived      bool
		before, after nativeXattrSnapshot
		user          uint64
		replay        string
	}{
		"survived but executed again":  {true, src1, src2, 3, "40'2"},
		"survived, another replay":     {true, src1, src1, 1, "40'2"},
		"survived, another user":       {true, src1, src1, 3, "28'3"},
		"seed kept, nothing executed":  {false, seed, seed, 2, "28'3"},
		"seed kept, first source":      {false, seed, src1, 1, "40'2"},
		"seed kept, reply version off": {false, seed, src2, 1, "40'2"},
	} {
		if _, err := judgeCopyResubmission(in, c.survived, "28'3", c.before, c.after, c.user, c.replay); err == nil {
			t.Error("resubmission accepted:", name)
		}
	}

	restarted := route
	restarted.Epoch, restarted.Address = 40, "v2:192.0.2.11:6800/99"
	if err := checkPrimaryRestarted(route, restarted); err != nil {
		t.Fatal("restarted primary refused:", err)
	}
	if checkPrimaryReturned(route, restarted) == nil {
		t.Error("the strict return check accepted a new address")
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"no address":      func(p *osdFixtureOracle) { p.Address = "" },
		"another primary": func(p *osdFixtureOracle) { p.ID, p.PrimaryID = 0, 0 },
		"pg_temp left":    func(p *osdFixtureOracle) { p.PGTemp = true },
		"acting differs":  func(p *osdFixtureOracle) { p.Acting = []int32{1, 2, 0} },
		"not newer":       func(p *osdFixtureOracle) { p.Epoch = route.Epoch },
	} {
		p := restarted
		p.Acting = slices.Clone(restarted.Acting)
		edit(&p)
		if checkPrimaryRestarted(route, p) == nil {
			t.Error("restarted primary accepted:", name)
		}
	}
}

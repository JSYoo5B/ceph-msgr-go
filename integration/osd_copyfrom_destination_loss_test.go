package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Destination primary loss after a COPY_FROM or COPY_FROM2 finished reading
// its source and staged its final transaction, before the destination
// commit. The pinned source (Ceph v20.2.4) gives the barrier its meaning:
//
//   - src/osd/PrimaryLogPG.cc:344-368 (CopyFromCallback::finish) runs the
//     client's op context again with execute_ctx once the source read is
//     complete (process_copy_chunk fetches more while the cursor is
//     incomplete), so the final copy transaction is issued under the client's
//     own op. A temporary-object write of a multi-chunk copy is a separate
//     op context without the client's op.
//   - src/osd/ReplicatedBackend.cc:1134-1160 (issue_op) marks that op
//     "waiting for subops from <replicas>" when it sends the repops, with the
//     flag point "waiting for sub ops"; ReplicatedBackend.cc:641-664
//     (op_commit) marks "op_commit" at the primary's own commit and
//     ReplicatedBackend.cc:666-713 (do_repop_reply) "sub_op_commit_rec" at
//     each replica's commit. The client is answered only once every acting
//     replica committed.
//
// A destination replica that is paused before the copy is sent therefore
// holds the client op at that point: the source read is complete and the
// transaction is staged on the primary (and on any replica not paused), but
// not committed. The destination primary is then SIGKILLed.

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
		Error       string `json:"error"`
	} `json:"unknown_outcome"`
	PausedFor   string `json:"paused_for"`
	NewPrimary  int32  `json:"new_primary"`
	MovedEpoch  uint32 `json:"moved_epoch"`
	Outcome     string `json:"outcome"`
	Native      string `json:"native_after_loss"`
	LogVersion  string `json:"log_version,omitempty"`
	Resubmitted struct {
		Code        int32  `json:"code"`
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
				return fmt.Errorf("op_commit repeated or before the sub-ops were sent: %q", op.Events)
			}
			local = i
		case event == "sub_op_commit_rec":
			if sent < 0 {
				return fmt.Errorf("a replica commit before the sub-ops were sent: %q", op.Events)
			}
			received++
		case event == "commit_sent":
			return fmt.Errorf("the reply was already sent: %q", op.Events)
		}
	}
	if sent < 0 || local < 0 || received != commits {
		return fmt.Errorf("events %q: sub-ops sent %t, local commit %t, %d replica commits (want %d)", op.Events, sent >= 0, local >= 0, received, commits)
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
// and refuses anything else. A second Go write then changes the source and
// one explicit resubmission goes to the new primary: it must be answered
// from the log if the copy survived and must execute once, copying the
// second source, if it did not. After recovery the original primary must lead
// again, and a fresh-session read, a delayed native readback and its PG log
// must show the same single copy.
//
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
	for _, osd := range frozen {
		recovery.freeze(t, ctx, osd)
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
	for deadline := time.Now().Add(12 * time.Second); ; {
		pending("before the barrier")
		ops := copyOpsInFlight(t, ctx, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, ctx, 2, r.dest, nil, pools.ids[1], r.source)
		if staged, err = checkCopyStaged(ops, objecter, route, r.gid, copyPrimaryCopyIncarnation, cell.commits); err == nil {
			receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = ops.Before.Epoch, staged.Reqid, staged.FlagPoint, staged.Events
			receipt.OpsSHA256, receipt.ObjecterSHA256, receipt.ObjecterOps = ops.SHA256, objecter.SHA256, len(objecter.Ops)
			t.Logf("barrier on osd.%d epoch %d with osd %v paused: op %s flag point %q events %q; objecter holds %d source ops",
				ops.OSD, ops.Before.Epoch, frozen, staged.Reqid, staged.FlagPoint, staged.Events, len(objecter.Ops))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no staged barrier: %v\nop tracker: %s\nobjecter: %s", err, boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
		}
		time.Sleep(200 * time.Millisecond)
	}
	pending("at the barrier")

	receipt.KillExit = recovery.kill(t, ctx, primary)
	var lost outcome
	select {
	case lost = <-done:
	case <-time.After(60 * time.Second):
		cancel()
		lost = <-done
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != staged.TID ||
		lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != route.Epoch {
		t.Fatalf("lost copy metadata (barrier tid %d): %+v %v", staged.TID, lost.reply, lost.err)
	}
	receipt.Unknown.TID, receipt.Unknown.GlobalID = lost.reply.Submission.Transaction, lost.reply.Submission.GlobalID
	receipt.Unknown.Incarnation, receipt.Unknown.Epoch, receipt.Unknown.Error = lost.reply.Submission.ClientIncarnation, lost.reply.Submission.MapEpoch, lost.err.Error()
	o.Close()
	for _, osd := range frozen {
		recovery.thaw(t, ctx, osd)
	}
	receipt.PausedFor = time.Since(paused).Round(10 * time.Millisecond).String()
	t.Logf("osd.%d killed (exit %d) at the barrier; the copy ended unknown: %v; osd %v resumed after %s", primary, receipt.KillExit, lost.err, frozen, receipt.PausedFor)
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
	survived := native.Code == 0 && bytes.Equal(native.Data, firstSource.Data)
	var binding copyFromHistoryWant
	switch {
	case survived:
		receipt.Outcome = "copy survived"
		// A copy gives the destination the source's user version and mtime
		// (src/osd/PrimaryLogPG.cc:355-356 and finish_copyfrom).
		if !reflect.DeepEqual(attrNames(native), attrNames(firstSource)) || native.Version != first.UserVersion || native.Seconds != firstSource.Seconds || native.Nanos != firstSource.Nanos {
			t.Fatalf("partial copy after the loss: %s; source %s", nativeDigest(native), nativeDigest(firstSource))
		}
		if len(s.Targets[0].Locations) != 1 {
			t.Fatalf("the surviving copy is in the new primary's PG log %d times: %+v", len(s.Targets[0].Locations), s.Targets[0].Locations)
		}
		receipt.LogVersion = s.Targets[0].Locations[0].Version
		binding = copyFromHistoryWant{object: hobjectHead(moved), version: receipt.LogVersion, sourceUserVersion: first.UserVersion}
		if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
			t.Fatalf("new primary osd.%d: %v", moved.ID, err)
		}
	case cell.commits > 0:
		t.Fatalf("a replica committed the copy before the loss, yet the destination is %s", nativeDigest(native))
	case sameNative(native, seeded):
		receipt.Outcome = "seed kept"
		if len(s.Targets[0].Locations) != 0 || len(s.Targets[1].Locations) != 0 {
			t.Fatalf("the seeded destination's new primary names the copy: %+v %+v", s.Targets[0].Locations, s.Targets[1].Locations)
		}
	default:
		t.Fatalf("the destination after the loss is neither the seed nor the copy: %s; seed %s", nativeDigest(native), nativeDigest(seeded))
	}
	t.Logf("%s after losing osd.%d: new primary osd.%d at epoch %d; destination %s", receipt.Outcome, primary, moved.ID, moved.Epoch, receipt.Native)

	second, secondSource := r.writeSource("second source write", 1700009200, []byte("copy-loss second source bytes, longer"), []byte{6, 0xfe})
	secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
	again := r.copyRequest(cell.code, moved)
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission to the new primary", moved, again)
	receipt.Resubmitted.Code, receipt.Resubmitted.TID, receipt.Resubmitted.Attempt = answered.Code, answered.Transaction, answered.RetryAttempt
	receipt.Resubmitted.Flags, receipt.Resubmitted.UserVersion, receipt.Resubmitted.Replay = uint32(answered.Flags), answered.UserVersion, replayVersion(answered)
	t.Logf("resubmitted to osd.%d: code=%d tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if answered.Transaction != staged.TID || answered.RetryAttempt != 1 || answered.Operations[0].Result != 0 {
		t.Fatalf("resubmission: tid %d attempt %d op %d", answered.Transaction, answered.RetryAttempt, answered.Operations[0].Result)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	bound, unbound := firstKey, secondKey
	if survived {
		// Answered from the log: the first source stays on the destination.
		if answered.UserVersion != first.UserVersion || replayVersion(answered) != receipt.LogVersion || !sameNative(after, native) {
			t.Fatalf("the resubmission was not answered from the log: user %d (want %d) replay %s (want %s); destination %s",
				answered.UserVersion, first.UserVersion, replayVersion(answered), receipt.LogVersion, nativeDigest(after))
		}
	} else {
		// Executed once: the destination is the second source.
		if answered.UserVersion != second.UserVersion || after.Code != 0 || !bytes.Equal(after.Data, secondSource.Data) || !reflect.DeepEqual(attrNames(after), attrNames(secondSource)) ||
			after.Version != answered.UserVersion || after.Seconds != secondSource.Seconds || after.Nanos != secondSource.Nanos {
			t.Fatalf("the resubmission did not copy the second source once: user %d (source %d); destination %s", answered.UserVersion, second.UserVersion, nativeDigest(after))
		}
		binding = copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(answered), sourceUserVersion: second.UserVersion}
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

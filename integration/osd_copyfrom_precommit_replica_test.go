package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type copyPrecommitReplicaLoss struct {
	AfterKillPlacementEpoch uint32             `json:"after_kill_placement_epoch"`
	AfterKillActing         []int32            `json:"after_kill_acting"`
	OSD                     int32              `json:"osd"`
	Seed                    string             `json:"destination_seed"`
	Source                  string             `json:"source_before"`
	AfterKillEvents         []string           `json:"after_kill_events"`
	AfterKillInterval       *copyIntervalQuery `json:"after_kill_interval"`
	ClosedAfter             string             `json:"caller_closed_after_arming"`
	PrimaryStopped          bool               `json:"primary_stopped_to_undo_stall"`
	CopyLogVersion          string             `json:"copy_log_version"`
	SecondSource            string             `json:"second_source"`
}

// Both COPY_FROM opcodes lose a destination replica, never either primary,
// in the replicated precommit barrier documented in osd_copyfrom_precommit_test.go:
// the source read is complete, both replicas committed the final transaction,
// and the destination primary has not committed it. The same owned two-chunk
// source, populated destination seed, trigger write and bounded commitstall
// control are used. After the replica's confirmed SIGKILL, the original copy
// must still be unanswered and the primary must still report both replica
// commits, no local commit, no source read, and the original PG interval.
// The MON placement must have removed exactly the lost replica; the stalled
// PG must still report its original acting set and interval at an older map.
//
// The caller then closes its copy connection and cancels the trigger wait;
// both must retain unknown outcomes with the original submission identities.
// There is no resubmission. To undo the armed stall, the fixture separately
// stops and restarts the primary and restarts the lost replica. This recovery
// action is recorded separately: recovery is not evidence of what replica
// loss alone would eventually do with the primary still stalled.
//
// After recovery the native destination must be the first source, and the
// serving primary's PG log must bind the copy identity once to the first
// source write. A distinguishable second source write followed by a fresh
// session read, delayed native readback and another log check must leave
// that first copy unchanged. Every destination replica must list no staging.
// Cleanup verifies the primary's original stall options, chunk setting and
// claim release before publishing, and removes both owned pools. This does
// not cover replicas that had not committed, EC, snapshots, pinned versions,
// or shard-by-shard persisted log history.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprecommitreplicaloss key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprecommitreplicaloss key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated loses an owned replica while its primary has an owned bounded commit stall
func TestCephOSDCopyFromPrecommitReplicaLossIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var receipts []copyPrecommitReceipt
	t.Cleanup(func() {
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-precommit-replica-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("precommit replica-loss receipt", err)
		}
	})
	gate := &copyStallGate{}
	for _, tc := range copyPrimaryChangeCases {
		t.Run(tc.name+"/replica-before-primary-commit", func(t *testing.T) {
			if err := gate.admit(); err != nil {
				t.Fatal(err)
			}
			receipt := copyPrecommitReceipt{copyChunkLossReceipt: copyChunkLossReceipt{Cell: tc.name + "/replica-before-primary-commit", Chunk: multiChunkLimit, Paused: -1}, Replica: &copyPrecommitReplicaLoss{OSD: -1}}
			publishAfterCleanups(t, func() { receipts = append(receipts, receipt) })
			copyPrecommitLoss(t, ctx, pools, recovery, gate, tc, &receipt)
		})
	}
}

func (r *copyPrimaryChangeRun) losePrecommitReplica(work context.Context, armed time.Time, guard *copyStallGuard, h *copyChunkHold, copied nativeXattrSnapshot, copyKey, writeKey pgHistoryTarget, receipt *copyPrecommitReceipt, triggerConn *cephmsgr.OSDConnection, blocked <-chan copyChunkOutcome, cancelTrigger context.CancelFunc, triggerEpoch uint32, trigger string, written []byte) {
	t, ctx := r.t, r.ctx
	loss, stalled := receipt.Replica, &receipt.Stalled
	loss.Source = nativeDigest(copied)
	for _, osd := range h.route.Acting {
		if osd != h.route.ID && osd != h.source.ID {
			loss.OSD = osd
			break
		}
	}
	if loss.OSD < 0 {
		t.Fatal("no destination replica distinct from both primaries")
	}
	receipt.KillExit = r.recovery.kill(t, work, loss.OSD)
	stalled.KilledAfter = time.Since(armed).Round(10 * time.Millisecond).String()
	r.recovery.markDown(t, work, loss.OSD)
	current := poolPlace(t, work, 2, r.dest, "")
	h.pending(t, "after the replica was killed before the primary commit")
	ops := copyOpsInFlight(t, work, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
	objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
	interval := copyInterval(t, work, 2, r.dest)
	op, err := checkReplicaLossStall(ops, objecter, interval, h.route, current, *stalled.Interval, loss.OSD, r.gid, h.op.TID)
	if err != nil {
		t.Fatal("after replica loss:", err)
	}
	loss.AfterKillPlacementEpoch, loss.AfterKillActing = current.Epoch, slices.Clone(current.Acting)
	loss.AfterKillEvents, loss.AfterKillInterval = op.Events, interval.Query
	if work.Err() != nil {
		t.Fatal("replica-loss observations exceeded the stall budget", work.Err())
	}
	h.pending(t, "before the caller closes its connection")
	h.o.Close()
	lost := awaitPrecommitUnknown(t, work, h.done, r.gid, copyPrimaryCopyIncarnation, h.op.TID, h.route.Epoch)
	receipt.Unknown = &copyChunkUnknown{TID: lost.reply.Submission.Transaction, GlobalID: lost.reply.Submission.GlobalID, Incarnation: lost.reply.Submission.ClientIncarnation, Epoch: lost.reply.Submission.MapEpoch, Cause: outcomeCause(lost.err)}
	cancelTrigger()
	triggerLost := awaitPrecommitUnknown(t, work, blocked, r.gid, copyStallTriggerIncarnation, stalled.TriggerSubmitTID, triggerEpoch)
	triggerConn.Close()
	stalled.TriggerOutcome, stalled.TriggerSubmitTID = outcomeCause(triggerLost.err), triggerLost.reply.Submission.Transaction
	loss.ClosedAfter = time.Since(armed).Round(10 * time.Millisecond).String()
	// The explicit stop ends the injected stall; it is not another loss injection.
	guard.stop(work)
	if guard.stopErr != nil {
		t.Fatal("undo primary commit stall:", guard.stopErr)
	}
	loss.PrimaryStopped = true
	r.recovery.restore(t)
	if t.Failed() {
		return
	}
	route := r.waitReturned()
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	source := poolObserve(t, ctx, 1, r.source, "", "")
	receipt.Native = nativeDigest(after)
	if !sameCopy(after, copied, copied.Version) || !sameNative(source, copied) {
		t.Fatalf("native copy after replica loss and stall recovery differs: destination %s source %s; expected %s", nativeDigest(after), nativeDigest(source), nativeDigest(copied))
	}
	s := r.history(route, copyKey, writeKey)
	if len(s.Targets[0].Locations) != 1 {
		t.Fatalf("copy identity occurs %d times", len(s.Targets[0].Locations))
	}
	loss.CopyLogVersion = s.Targets[0].Locations[0].Version
	binding := copyFromHistoryWant{object: hobjectHead(route), version: loss.CopyLogVersion, sourceUserVersion: copied.Version}
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatal(err)
	}
	second, secondSource := r.writeSource("second source write", 1700010200, []byte("replica loss second source bytes"), []byte{6, 0xfe})
	stalled.SecondUser, loss.SecondSource = second.UserVersion, nativeDigest(secondSource)
	if sameNative(secondSource, copied) || second.UserVersion <= copied.Version {
		t.Fatal("second source is not distinguishable")
	}
	r.finish(after, second.Transaction, copyKey, writeKey, binding)
	receipt.Final = nativeDigest(after)
	for _, osd := range route.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, route), osd, route, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
	triggered := poolObserve(t, ctx, 2, trigger, "", "")
	stalled.TriggerNative = nativeDigest(triggered)
	if triggered.Code != 0 || !bytes.Equal(triggered.Data, written) {
		t.Fatal("native trigger differs from its write", nativeDigest(triggered))
	}
	t.Logf("replica osd.%d killed before primary commit; caller closed with %s, no resubmission; recovered copy once at %s", loss.OSD, receipt.Unknown.Cause, loss.CopyLogVersion)
}

func awaitPrecommitUnknown(t *testing.T, ctx context.Context, done <-chan copyChunkOutcome, gid uint64, incarnation uint32, tid uint64, epoch uint32) copyChunkOutcome {
	t.Helper()
	var got copyChunkOutcome
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("caller wait did not end within the stall budget", ctx.Err())
	}
	var unknown *cephmsgr.OutcomeUnknownError
	s := got.reply.Submission
	if !errors.As(got.err, &unknown) || got.reply.Transaction != 0 || s.Transaction == 0 || s.Transaction != tid || s.GlobalID != gid || s.ClientIncarnation != incarnation || s.MapEpoch != epoch || s.Attempt != 0 {
		t.Fatalf("unknown submission identity mismatch: %+v %v (want gid %d inc %d tid %d epoch %d)", got.reply, got.err, gid, incarnation, tid, epoch)
	}
	return got
}

// The MON map has removed the lost replica, while the stalled primary's PG
// has not consumed that map. Judge the bracketed snapshots against the new
// placement and the request/PG interval against their original identities.
func checkReplicaLossStall(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, interval copyIntervalObservation, original, current osdFixtureOracle, prior copyIntervalQuery, lost int32, gid, tid uint64) (opsInFlightOp, error) {
	want := original
	want.Up = slices.DeleteFunc(slices.Clone(original.Up), func(id int32) bool { return id == lost })
	want.Acting = slices.DeleteFunc(slices.Clone(original.Acting), func(id int32) bool { return id == lost })
	if lost == original.ID || len(want.Acting) != len(original.Acting)-1 || !samePlacement(current, want) || current.Epoch <= original.Epoch {
		return opsInFlightOp{}, fmt.Errorf("placement is not the original primary and surviving replicas: got %+v, original %+v, lost %d", current, original, lost)
	}
	// checkCopyClientOp uses route.Epoch for the client's submission epoch;
	// its other route fields describe the placement bracketing the snapshots.
	submitted := current
	submitted.Epoch = original.Epoch
	op, err := checkCopyClientOp(ops, objecter, submitted, gid, copyPrimaryCopyIncarnation)
	if err != nil {
		return op, err
	}
	if ops.Before.Epoch < current.Epoch || objecter.Before.Epoch < current.Epoch || op.TID != tid || ops.AbsenceEstablished || objecter.AbsenceEstablished || len(objecter.Ops) != 0 {
		return op, errors.New("post-loss snapshots do not retain the original copy with its source read complete")
	}
	if err := checkStalledEvents(op, len(original.Acting)-1); err != nil {
		return op, err
	}
	q := interval.Query
	if interval.Before == nil || interval.After == nil || !reflect.DeepEqual(interval.Before, interval.After) || !samePlacement(*interval.Before, current) || interval.Before.Epoch < current.Epoch || interval.Code == nil || *interval.Code != 0 || interval.QueryFailure != "" || q == nil {
		return op, errors.New("post-loss PG query is not bracketed by the surviving placement")
	}
	if q.PGID != copyRouteSPG(original) || q.Epoch < original.Epoch || q.Epoch >= current.Epoch || !slices.Equal(q.Up, original.Up) || !slices.Equal(q.Acting, original.Acting) || q.SameIntervalSince == 0 || q.SameIntervalSince != prior.SameIntervalSince {
		return op, fmt.Errorf("stalled primary no longer reports the original PG interval: %+v", q)
	}
	return op, nil
}

func TestCopyPrecommitReplicaLossJudgement(t *testing.T) {
	type observation struct {
		original, current osdFixtureOracle
		ops               opsInFlightSnapshot
		objecter          objecterRequestsSnapshot
		interval          copyIntervalObservation
		prior             copyIntervalQuery
	}
	sample := func() observation {
		original := osdFixtureOracle{Up: []int32{2, 0, 1}, Acting: []int32{2, 0, 1}, Replicas: 3, MinSize: 2, Pool: 4, Name: "destination", ID: 2, PrimaryID: 2, Shard: -1, Epoch: 23, PGID: "4.1", RawPGID: "4.abc"}
		current := original
		current.Up, current.Acting, current.Epoch = []int32{2, 0}, []int32{2, 0}, 26
		ops := opsInFlightSnapshot{Complete: true, Before: current, After: current, OSD: 2, PGID: "4.1", Client: "client.4758.33", Ops: []opsInFlightOp{{Reqid: "client.4758.33:2", TID: 2, PGID: "4.1", Epoch: 23, FlagPoint: "waiting for sub ops", Events: []string{"waiting for subops from 0,1", "sub_op_commit_rec", "sub_op_commit_rec"}}}}
		objecter := objecterRequestsSnapshot{Complete: true, Before: current, After: current, OSD: 2, PGID: "4.1"}
		q := copyIntervalQuery{State: "active+clean", Epoch: 24, Up: []int32{2, 0, 1}, Acting: []int32{2, 0, 1}, PGID: "4.1", SameIntervalSince: 21}
		code := 0
		before, after := current, current
		return observation{original, current, ops, objecter, copyIntervalObservation{Before: &before, After: &after, Code: &code, Query: &q}, q}
	}
	judge := func(o observation) error {
		_, err := checkReplicaLossStall(o.ops, o.objecter, o.interval, o.original, o.current, o.prior, 1, 4758, 2)
		return err
	}
	if err := judge(sample()); err != nil {
		t.Fatal("valid lagging PG refused:", err)
	}
	for name, change := range map[string]func(*observation){
		"replica still in placement":   func(o *observation) { o.current = o.original },
		"primary changed":              func(o *observation) { o.current.ID = 0 },
		"wrong survivors":              func(o *observation) { o.current.Acting = []int32{2, 1} },
		"placement epoch not advanced": func(o *observation) { o.current.Epoch = 23 },
		"unbracketed tracker":          func(o *observation) { o.ops.After.Epoch++ },
		"stale objecter":               func(o *observation) { o.objecter.Before.Epoch = 25; o.objecter.After.Epoch = 25 },
		"source read pending":          func(o *observation) { o.objecter.Ops = []objecterRequestsOp{{TID: 4}} },
		"different copy":               func(o *observation) { o.ops.Ops[0].TID = 3; o.ops.Ops[0].Reqid = "client.4758.33:3" },
		"new request epoch":            func(o *observation) { o.ops.Ops[0].Epoch = 26 },
		"local commit":                 func(o *observation) { o.ops.Ops[0].Events = append(o.ops.Ops[0].Events, "op_commit") },
		"one replica commit":           func(o *observation) { o.ops.Ops[0].Events = o.ops.Ops[0].Events[:2] },
		"PG consumed new map":          func(o *observation) { o.interval.Query.Epoch = 26 },
		"PG changed interval":          func(o *observation) { o.interval.Query.SameIntervalSince = 26 },
		"PG changed acting":            func(o *observation) { o.interval.Query.Acting = []int32{2, 0} },
		"missing query":                func(o *observation) { o.interval.Query = nil },
		"unbracketed query":            func(o *observation) { o.interval.After.Epoch++ },
		"query failed":                 func(o *observation) { *o.interval.Code = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			o := sample()
			change(&o)
			if err := judge(o); err == nil {
				t.Fatal("invalid post-loss observation accepted")
			}
		})
	}
}

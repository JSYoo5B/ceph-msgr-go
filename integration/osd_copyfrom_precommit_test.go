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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The interval between the source read of a multi-chunk COPY_FROM or
// COPY_FROM2 and the local commit of its final transaction on a replicated
// destination primary. The pinned source (Ceph v20.2.4) gives the hold:
//
//   - CopyFromCallback::finish (src/osd/PrimaryLogPG.cc:344-368) runs the
//     copy's final transaction as soon as the last chunk completes the
//     cursor. ReplicatedBackend::submit_transaction
//     (src/osd/ReplicatedBackend.cc:551-640) sends it to every replica
//     (issue_op) and queues the local transaction in the same call, so
//     nothing in the OSD holds it between the source read and BlueStore.
//     Each replica's commit reply marks the client op sub_op_commit_rec
//     (do_repop_reply, 666-720), and only the local commit callback marks it
//     op_commit (op_commit, 641-664); the client is answered when both are
//     in.
//   - BlueStore commits a transaction in its kv sync thread after a flush of
//     the block device (src/os/bluestore/BlueStore.cc:15073-15345), and runs
//     its commit callbacks only after that. With the stock debug option
//     bdev_inject_crash set at runtime, the first flush with I/O since the
//     last one sleeps bdev_inject_crash_flush_delay seconds under the
//     device's flush mutex and then exits the OSD
//     (src/blk/kernel/KernelDevice.cc:502-543), so no later transaction of
//     that OSD commits locally (tools/osd_pool_mode_commitstall_oracle.py).
//   - The OSD shares a new map with its Objecter when it receives it
//     (src/osd/OSD.cc:8215-8217) but its PGs consume the map only once the
//     map's transaction commits (C_OnMapCommit, _committed_osd_maps,
//     OSD.cc:8444 and 8546), so a stalled primary resends a paused copy-get
//     at once and its PGs stay in the map they had: no interval change can
//     cancel the copy there while the stall lasts.
//   - A resubmitted write whose request ID the PG log holds is answered from
//     the log without running again (do_op, PrimaryLogPG.cc:2274-2301;
//     OSDService::reply_op_error, src/osd/OSD.cc:1635-1649).

// copyStallTriggerIncarnation is the client incarnation of the write that
// starts the destination primary's commit stall.
const copyStallTriggerIncarnation = 37

// copyCommitStallDelay is the stall the oracle arms (DELAY): the destination
// primary exits on its own that long after its first flush.
const copyCommitStallDelay = 60 * time.Second

// copyCommitStall is the receipt of one commitstall oracle request.
type copyCommitStall struct {
	OSD      int32    `json:"osd"`
	Action   string   `json:"action"`
	Crash    int64    `json:"crash"`
	Delay    int64    `json:"delay"`
	Set      bool     `json:"set"`
	Armed    bool     `json:"armed"`
	Failures []string `json:"failures"`
}

func commitStall(t *testing.T, ctx context.Context, osd int32, action string) copyCommitStall {
	t.Helper()
	var result struct {
		Stall copyCommitStall `json:"commit_stall"`
	}
	nativePoolResult(t, ctx, "commitstall", map[string]any{"osd": osd, "action": action}, &result)
	return result.Stall
}

// copyStalledCommit is the part of a receipt that records the stalled local
// commit.
type copyStalledCommit struct {
	Baseline         copyCommitStall    `json:"stall_baseline"`
	Stall            copyCommitStall    `json:"stall"`
	Settled          *copyCommitStall   `json:"stall_after_restore"`
	Trigger          string             `json:"trigger_object"`
	TriggerReqid     string             `json:"trigger_reqid"`
	TriggerPoint     string             `json:"trigger_flag_point"`
	TriggerEvents    []string           `json:"trigger_events"`
	CopyPoint        string             `json:"copy_flag_point"`
	CopyEvents       []string           `json:"copy_events"`
	Observations     int                `json:"held_observations"`
	HeldFor          string             `json:"commit_held_for"`
	Renamed          []int32            `json:"replicas_without_temporary"`
	Interval         *copyIntervalQuery `json:"primary_pg_query"`
	PlacementEpoch   uint32             `json:"placement_epoch"`
	KilledAfter      string             `json:"killed_after_arming"`
	TriggerOutcome   string             `json:"trigger_outcome"`
	TriggerSubmitTID uint64             `json:"trigger_submission_tid"`
	TriggerNative    string             `json:"trigger_native_after"`
	SecondUser       uint64             `json:"second_source_user_version"`
}

// copyPrecommitReceipt is the archived public receipt of one cell.
type copyPrecommitReceipt struct {
	copyChunkLossReceipt
	Steps   []copyChunkStep   `json:"chunks"`
	Stalled copyStalledCommit `json:"stalled_commit"`
}

// checkStalledEvents judges the events of a write op whose transaction went
// to the replicas: each of them committed it (one sub_op_commit_rec per
// replica) and the primary did not (no op_commit), so it was not answered.
func checkStalledEvents(op opsInFlightOp, replicas int) error {
	commits := 0
	for _, event := range op.Events {
		if event == "sub_op_commit_rec" {
			commits++
		}
	}
	switch {
	case slices.Contains(op.Events, "op_commit") || slices.Contains(op.Events, "commit_sent") || strings.HasPrefix(op.FlagPoint, "commit sent"):
		return fmt.Errorf("op %s committed locally: %q at %q", op.Reqid, op.Events, op.FlagPoint)
	case commits != replicas:
		return fmt.Errorf("op %s has %d replica commits of %d: %q", op.Reqid, commits, replicas, op.Events)
	}
	return nil
}

// checkTriggerStalled judges one bracketed snapshot of the destination
// primary's op tracker after the trigger write was sent: exactly one op of
// the trigger's client entity, for the trigger's PG, committed by every
// replica and not by the primary.
func checkTriggerStalled(ops opsInFlightSnapshot, route osdFixtureOracle, gid uint64) (opsInFlightOp, error) {
	if !ops.Complete || len(ops.Failures) != 0 || ops.Code != 0 || ops.AbsenceEstablished {
		return opsInFlightOp{}, fmt.Errorf("incomplete op tracker snapshot of the destination primary: %q", ops.Failures)
	}
	if !reflect.DeepEqual(ops.Before, ops.After) || !samePlacement(ops.Before, route) || ops.OSD != route.ID || ops.PGID != route.PGID {
		return opsInFlightOp{}, fmt.Errorf("the snapshot of osd.%d for %s is not bracketed by the trigger's route of osd.%d for %s", ops.OSD, ops.PGID, route.ID, route.PGID)
	}
	if want := fmt.Sprintf("client.%d.%d", gid, copyStallTriggerIncarnation); ops.Client != want || len(ops.Ops) != 1 {
		return opsInFlightOp{}, fmt.Errorf("%d in-flight ops of %s (want the one trigger write of %s)", len(ops.Ops), ops.Client, want)
	}
	op := ops.Ops[0]
	if op.TID == 0 || op.PGID != route.PGID && op.PGID != route.RawPGID {
		return op, fmt.Errorf("the trigger write %s is for %s, not %s", op.Reqid, op.PGID, route.PGID)
	}
	return op, checkStalledEvents(op, len(route.Acting)-1)
}

// checkCommitStalled judges one round of observations of a copy whose
// source read completed and whose final transaction every replica committed
// while the destination primary did not: its Objecter holds no op of the
// source, and its op tracker holds the copy's client op (checkCopyClientOp)
// unanswered with one replica commit per replica and no local commit.
func checkCommitStalled(ops opsInFlightSnapshot, objecter objecterRequestsSnapshot, route osdFixtureOracle, gid, tid uint64) (opsInFlightOp, error) {
	op, err := checkCopyClientOp(ops, objecter, route, gid, copyPrimaryCopyIncarnation)
	if err != nil {
		return op, err
	}
	if objecter.AbsenceEstablished || ops.AbsenceEstablished || len(objecter.Ops) != 0 {
		return op, fmt.Errorf("the destination primary's Objecter still holds %d ops of the source", len(objecter.Ops))
	}
	if op.TID != tid {
		return op, fmt.Errorf("the client op is %s, not the held copy tid %d", op.Reqid, tid)
	}
	return op, checkStalledEvents(op, len(route.Acting)-1)
}

// copyStalledInterval judges the copyinterval observation of a stalled
// primary, as copyIntervalHeld does, except that its PG may be behind the
// placement's epoch: the stalled OSD does not commit, and so does not
// consume, the maps it receives. The PG's own interval must be the one the
// copy was sent in, at a map not older than the copy's.
func copyStalledInterval(o copyIntervalObservation, route osdFixtureOracle) error {
	if o.Before == nil || o.After == nil || !reflect.DeepEqual(o.Before, o.After) {
		return fmt.Errorf("the placement records around the query differ or are missing: %s", o.PlacementFailure)
	}
	q := o.Query
	if q == nil || o.Code == nil || *o.Code != 0 || o.QueryFailure != "" {
		return fmt.Errorf("no complete pg query reply: %s", o.QueryFailure)
	}
	switch {
	case !samePlacement(*o.Before, route) || o.Before.Epoch < route.Epoch:
		return fmt.Errorf("%w: the placement is now %+v, not %+v", errCopyIntervalChanged, *o.Before, route)
	case q.Epoch < route.Epoch:
		return fmt.Errorf("the primary is at map epoch %d, behind the copy's epoch %d", q.Epoch, route.Epoch)
	case q.PGID != copyRouteSPG(route):
		return fmt.Errorf("%w: the query was answered for %s, not %s", errCopyIntervalChanged, q.PGID, copyRouteSPG(route))
	case !slices.Equal(q.Up, route.Up) || !slices.Equal(q.Acting, route.Acting):
		return fmt.Errorf("%w: the primary reports up %v and acting %v; the copy was sent with %v and %v", errCopyIntervalChanged, q.Up, q.Acting, route.Up, route.Acting)
	case q.SameIntervalSince == 0 || q.SameIntervalSince > route.Epoch:
		return fmt.Errorf("%w: the interval started at epoch %d, after the copy's epoch %d", errCopyIntervalChanged, q.SameIntervalSince, route.Epoch)
	}
	return nil
}

// settleCommitStall decides, from what it observes and never from elapsed
// time, whether an armed or partly armed commit stall was undone. Unless the
// test itself SIGKILLed the OSD with its 137 receipt (killed), stop reports
// the fixture's stop of the owned daemon, which returns only once Docker
// reports the container stopped, whether it was still running (armed,
// stalled or not) or had exited on its own. restore is the cell's final
// recovery attempt and must leave the OSD started and clean. show reads both
// runtime options from the running OSD, which must be the baseline read
// before arming: `config set` is runtime only, so only a daemon that kept
// running since the arm can still show them. No step is skipped because an
// earlier one failed; any failure fails the settle.
func settleCommitStall(killed bool, baseline copyCommitStall, stop, restore func() error, show func() copyCommitStall) (copyCommitStall, error) {
	var failed []error
	if !killed {
		if err := stop(); err != nil {
			failed = append(failed, fmt.Errorf("stop the armed osd.%d: %w", baseline.OSD, err))
		}
	}
	if err := restore(); err != nil {
		failed = append(failed, fmt.Errorf("restart osd.%d after the stall: %w", baseline.OSD, err))
	}
	now := show()
	switch {
	case len(now.Failures) != 0:
		failed = append(failed, fmt.Errorf("osd.%d's runtime options were not read after the stall: %q", baseline.OSD, now.Failures))
	case now.OSD != baseline.OSD || now.Crash != baseline.Crash || now.Delay != baseline.Delay:
		failed = append(failed, fmt.Errorf("osd.%d's runtime options are %s=%d %s=%d after the stall; the baseline was %d and %d",
			baseline.OSD, "bdev_inject_crash", now.Crash, "bdev_inject_crash_flush_delay", now.Delay, baseline.Crash, baseline.Delay))
	}
	return now, errors.Join(failed...)
}

// copyStallGate keeps the cells of one run behind verified stall cleanup:
// once a cell's settle failed, every later cell is refused before it touches
// the cluster.
type copyStallGate struct{ unverified error }

func (g *copyStallGate) admit() error {
	if g.unverified != nil {
		return fmt.Errorf("not run: an earlier cell's commit stall cleanup is unverified: %w", g.unverified)
	}
	return nil
}

// copyStallGuard is one cell's commit stall: its baseline, whether the arm
// was requested, whether the test SIGKILLed the OSD itself, and the result of
// the stop that undoes it otherwise.
type copyStallGuard struct {
	r            *copyPrimaryChangeRun
	osd          int32
	stalled      *copyStalledCommit
	armRequested bool
	killed       bool
	stopErr      error
}

// guardCommitStall registers the verification of osd's commit stall. It must
// be registered before lowerCopyChunk: cleanups run in reverse, so it runs
// after every other recovery of the cell, makes the final restore attempt
// itself (restore is idempotent) and reads the options last
// (settleCommitStall). A failed settle fails the cell and closes gate.
func (r *copyPrimaryChangeRun) guardCommitStall(gate *copyStallGate, osd int32, stalled *copyStalledCommit) *copyStallGuard {
	t := r.t
	t.Helper()
	g := &copyStallGuard{r: r, osd: osd, stalled: stalled}
	t.Cleanup(func() {
		if !g.armRequested {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		restore := func() error {
			r.recovery.restore(t)
			if r.recovery.killed[osd] || r.recovery.restarted {
				return errors.New("restore did not start it again and republish routing")
			}
			return nil
		}
		show := func() copyCommitStall { return commitStall(t, cleanup, osd, "show") }
		settled, err := settleCommitStall(g.killed, stalled.Baseline, func() error { return g.stopErr }, restore, show)
		stalled.Settled = &settled
		if err != nil {
			gate.unverified = err
			t.Error(err)
		}
	})
	return g
}

// arm reads the OSD's two runtime options as the baseline, registers the
// stop that undoes the stall unless the test SIGKILLs the OSD itself, and
// arms it. The stop is registered after lowerCopyChunk's cleanups, so it runs
// before them: no other recovery step runs against an armed daemon. It
// returns the time before the arm request, before which the stall cannot
// have started.
func (g *copyStallGuard) arm() time.Time {
	r, t, osd, stalled := g.r, g.r.t, g.osd, g.stalled
	t.Helper()
	stalled.Baseline = commitStall(t, r.ctx, osd, "show")
	if b := stalled.Baseline; b.OSD != osd || b.Crash != 0 || len(b.Failures) != 0 {
		t.Fatalf("the commit stall of osd.%d is not unarmed before the test arms it: %+v", osd, b)
	}
	t.Cleanup(func() {
		if g.killed {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		// Recorded first, so an uncertain stop is still started again.
		r.recovery.killed[osd] = true
		g.stopErr = testcluster.ControlDaemon(cleanup, r.recovery.dir, "stop", "osd", fmt.Sprint(osd))
	})
	g.armRequested = true
	armed := time.Now()
	stalled.Stall = commitStall(t, r.ctx, osd, "arm")
	if s := stalled.Stall; !s.Armed || !s.Set || s.Crash != 1<<30 || s.Delay != int64(copyCommitStallDelay/time.Second) || len(s.Failures) != 0 {
		t.Fatalf("the commit stall of osd.%d was not armed: %+v", osd, s)
	}
	return armed
}

// publishAfterCleanups registers publish as a cleanup of the subtest t; it
// must be its first registration, so that it runs after every cleanup
// registered later (they run in reverse) and publishes the receipt fields
// those cleanups write. A skipped subtest publishes nothing.
func publishAfterCleanups(t *testing.T, publish func()) {
	t.Cleanup(func() {
		if !t.Skipped() {
			publish()
		}
	})
}

// For COPY_FROM and COPY_FROM2, an unversioned copy of a 64 KiB source from
// an owned replicated pool onto a natively seeded head of another owned
// replicated pool runs in two chunks under the destination primary's
// osd_copyfrom_max_chunk lowered to 32 KiB, and the barrier of
// osd_copyfrom_multichunk_sources_test.go holds it before the second, with
// one temporary object listed on all three OSDs of the destination PG.
//
// The destination primary's commit stall is armed (bdev_inject_crash with a
// 60 s flush delay, read back), and a Go WRITEFULL of 64 KiB to a fresh
// object of the same PG, the trigger, is sent. The trigger must be seen in
// the primary's op tracker committed by both replicas and not by the
// primary, twice a second apart: the primary's kv sync is stalled. pauserd
// is cleared. Twice, two seconds apart, bracketed snapshots then show the
// source read complete (no op of the source in the primary's Objecter) and
// the copy's client op unanswered with a sub_op_commit_rec from each replica
// and no op_commit: the replicas committed the final transaction and the
// primary did not. Both replicas list no temporary object any more (the
// final transaction renamed it there), and a copyinterval observation shows
// the primary's PG still in the interval the copy was sent in. The primary
// is SIGKILLed before the stall's own exit.
//
// The copy's wait ends as an unknown outcome with the barrier's
// transaction, and so does the trigger's. After the primary is marked down
// the PG leads from a replica, whose log holds the copy once, bound to the
// source write, and the destination is natively the complete copy: the copy
// executed although its primary never committed it. A second Go write
// replaces the source, and one explicit resubmission of the copy (attempt 1)
// to the new primary is answered from the PG log: the original transaction,
// 0, the first source's user version, and the destination still the first
// source natively, its log naming no second copy. The trigger object is
// natively the trigger write. After recovery the original primary leads with
// that single copy, its fresh-session read and delayed native readback agree,
// and no OSD lists a temporary object.
//
// This replaces the earlier reason this interval had no replicated cell
// (no stock control held a BlueStore commit). Not verified: an interval in
// which the replicas did not commit either (a frozen replica would apply the
// in-flight sub-op when resumed), a loss of a replica in this interval, and
// the legacy one-PG pools when their primaries coincide.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprecommitloss key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprecommitloss key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated pauses and SIGKILLs OSDs, sets pause, lowers a copy chunk and arms a commit stall at runtime
func TestCephOSDCopyFromPrecommitLossIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var mu sync.Mutex
	var receipts []copyPrecommitReceipt
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-precommit-loss-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("pre-commit loss receipt", err)
		}
	})
	gate := &copyStallGate{}
	for _, tc := range copyPrimaryChangeCases {
		t.Run(tc.name+"/destination-before-commit", func(t *testing.T) {
			if err := gate.admit(); err != nil {
				t.Fatal(err)
			}
			receipt := copyPrecommitReceipt{copyChunkLossReceipt: copyChunkLossReceipt{Cell: tc.name + "/destination-before-commit", Chunk: multiChunkLimit, Paused: -1}}
			publishAfterCleanups(t, func() {
				mu.Lock()
				receipts = append(receipts, receipt)
				mu.Unlock()
			})
			copyPrecommitLoss(t, ctx, pools, recovery, gate, tc, &receipt)
		})
	}
}

func copyPrecommitLoss(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, gate *copyStallGate, tc copyPrimaryChangeCase, receipt *copyPrecommitReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("precommit-%d", tc.code))
	primary := r.dstRoute.ID
	stalled := &receipt.Stalled
	receipt.SourcePrimary, receipt.Primary = r.separateSource(), primary
	// Before lowerCopyChunk, so it verifies the stall after its recovery.
	guard := r.guardCommitStall(gate, primary, stalled)
	r.lowerCopyChunk(&receipt.ChunkRestore)
	r.seedDestination()
	trigger, triggerRoute := r.blockerObject()
	stalled.Trigger = trigger

	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*67 + i>>8 + 37)
	}
	write := r.writeSourceHead("source write", 1700010100, data, []byte{9, 0xfe}, cephmsgr.OSDSnapContext{})
	copied := poolObserve(t, ctx, 1, r.source, "", "")
	if copied.Code != 0 || !bytes.Equal(copied.Data, data) || copied.Version != write.UserVersion {
		t.Fatalf("native source differs from the Go write: code %d version %d (reply %d)", copied.Code, copied.Version, write.UserVersion)
	}
	receipt.SourceBytes, receipt.SourceUser = len(data), write.UserVersion
	route, src := r.placeForCopy()
	h := r.holdAtChunk(r.copyRequest(tc.code, route), route, src, 2, 0, write.UserVersion, []int{0, 0}, &receipt.Steps)
	h.write = write
	receipt.FirstChunkTID, receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = h.first.TID, route.Epoch, h.op.Reqid, h.op.FlagPoint, h.op.Events
	receipt.HeldChunk, receipt.HeldTID, receipt.Temporary = h.read.OSDOps, h.read.TID, h.temp
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: h.op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: write.Transaction}

	armed := guard.arm()
	// Every observation of the stalled primary ends well before its own exit.
	work, stopWork := context.WithDeadline(ctx, armed.Add(copyCommitStallDelay-20*time.Second))
	defer stopWork()
	triggerAt := poolPlace(t, work, 2, trigger, "")
	if !samePlacement(triggerAt, triggerRoute) || triggerAt.PGID != route.PGID {
		t.Fatalf("the trigger object moved off the destination PG: %+v", triggerAt)
	}
	o := mutationConnection(t, r.client, ctx, triggerAt)
	t.Cleanup(func() { o.Close() })
	written := make([]byte, 64<<10)
	for i := range written {
		written[i] = byte(i*71 + i>>9 + 41)
	}
	request := r.pools.request(triggerAt, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(written)), Data: written})
	request.ClientIncarnation, request.ModificationTime = copyStallTriggerIncarnation, cephmsgr.OSDTime{Seconds: 1700010110}
	blocked := make(chan copyChunkOutcome, 1)
	call, cancel := context.WithTimeout(ctx, 300*time.Second)
	t.Cleanup(cancel)
	go func() {
		reply, err := o.Request(call, request)
		blocked <- copyChunkOutcome{reply, err}
	}()
	verdict, raw := errors.New("no observation"), ""
	for seen := 0; seen < 2; {
		if copyBudgetSpent(work) {
			t.Fatalf("the trigger write was not stalled on osd.%d within the budget: %v\nop tracker: %s", primary, verdict, raw)
		}
		select {
		case got := <-blocked:
			t.Fatalf("the trigger write was answered on the armed osd.%d: code %d err %v", primary, got.reply.Code, got.err)
		default:
		}
		ops := copyOpsInFlight(t, work, 2, trigger, nil, r.gid, copyStallTriggerIncarnation)
		op, err := checkTriggerStalled(ops, triggerAt, r.gid)
		if err == nil && work.Err() == nil {
			seen++
			stalled.TriggerReqid, stalled.TriggerPoint, stalled.TriggerEvents = op.Reqid, op.FlagPoint, op.Events
			time.Sleep(time.Second)
			continue
		}
		if seen != 0 {
			t.Fatalf("the trigger write's stall ended: %v", err)
		}
		verdict, raw = err, boundedRaw(ops.Raw)
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("osd.%d commit stalled: trigger %s at %q with %q", primary, stalled.TriggerReqid, stalled.TriggerPoint, stalled.TriggerEvents)

	r.recovery.clearPauseReads(t, work)
	var first time.Time
	verdict, raws := errors.New("no observation"), ""
	for stalled.Observations < 2 {
		h.pending(t, "while its local commit was stalled")
		if copyBudgetSpent(work) {
			t.Fatalf("the final copy transaction was not held before the local commit within the budget: %v%s", verdict, raws)
		}
		select {
		case got := <-blocked:
			t.Fatalf("the trigger write was answered on the stalled osd.%d: code %d err %v", primary, got.reply.Code, got.err)
		default:
		}
		ops := copyOpsInFlight(t, work, 2, r.dest, nil, r.gid, copyPrimaryCopyIncarnation)
		objecter := copyObjecterRequests(t, work, 2, r.dest, nil, r.pools.ids[1], r.source)
		op, err := checkCommitStalled(ops, objecter, route, r.gid, h.op.TID)
		if err == nil && work.Err() == nil {
			stalled.Observations++
			stalled.CopyPoint, stalled.CopyEvents = op.FlagPoint, op.Events
			if stalled.Observations == 1 {
				first = time.Now()
				time.Sleep(2 * time.Second)
			}
			continue
		}
		if stalled.Observations != 0 {
			t.Fatalf("the stall ended before the loss: %v", err)
		}
		verdict, raws = err, fmt.Sprintf("\nop tracker: %s\nobjecter: %s", boundedRaw(ops.Raw), boundedRaw(objecter.Raw))
		time.Sleep(100 * time.Millisecond)
	}
	// The replicas applied the final transaction, which renames the
	// temporary object over the destination. The stalled primary's own
	// collection is not listed: a listing reads its store.
	for _, osd := range route.Acting {
		if osd == primary {
			continue
		}
		if _, err := checkCopyTemp(copyStaging(t, work, osd, route), osd, route, 0); err != nil {
			t.Fatalf("replica osd.%d after the final transaction: %v", osd, err)
		}
		stalled.Renamed = append(stalled.Renamed, osd)
	}
	interval := copyInterval(t, work, 2, r.dest)
	if err := copyStalledInterval(interval, route); err != nil {
		t.Fatal("the held final transaction:", err)
	}
	stalled.Interval, stalled.PlacementEpoch = interval.Query, interval.Before.Epoch
	stalled.HeldFor = time.Since(first).Round(10 * time.Millisecond).String()
	t.Logf("source read complete and final transaction committed by replicas %v but not by osd.%d for %s: copy %s at %q %q; primary PG at epoch %d (placement %d), interval since %d",
		stalled.Renamed, primary, stalled.HeldFor, h.op.Reqid, stalled.CopyPoint, stalled.CopyEvents, interval.Query.Epoch, stalled.PlacementEpoch, interval.Query.SameIntervalSince)
	if work.Err() != nil {
		t.Fatal("the observations of the stalled primary outlasted their budget")
	}
	receipt.KillExit = r.recovery.kill(t, ctx, primary)
	guard.killed = true
	stalled.KilledAfter = time.Since(armed).Round(10 * time.Millisecond).String()
	stopWork()

	var lost copyChunkOutcome
	select {
	case lost = <-blocked:
	case <-time.After(60 * time.Second):
		t.Fatal("the trigger's wait did not end after the primary was killed")
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.GlobalID != r.gid || lost.reply.Submission.ClientIncarnation != copyStallTriggerIncarnation {
		t.Fatalf("the trigger did not end as an unknown outcome: %+v %v", lost.reply, lost.err)
	}
	stalled.TriggerOutcome, stalled.TriggerSubmitTID = outcomeCause(lost.err), lost.reply.Submission.Transaction
	o.Close()
	r.settleCommittedLoss(h, tc.code, copied, copyKey, writeKey, receipt)
	if r.t.Failed() {
		return
	}
	// The restarted primary runs with the baseline options again.
	now := commitStall(t, ctx, primary, "show")
	stalled.Settled = &now
	if len(now.Failures) != 0 || now.Crash != stalled.Baseline.Crash || now.Delay != stalled.Baseline.Delay {
		t.Fatalf("the restarted osd.%d still has a commit stall option: %+v; baseline %+v", primary, now, stalled.Baseline)
	}
	for _, osd := range r.dstRoute.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, r.dstRoute), osd, r.dstRoute, 0); err != nil {
			t.Fatal("after recovery:", err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
	after := poolObserve(t, ctx, 2, trigger, "", "")
	stalled.TriggerNative = nativeDigest(after)
	if after.Code != 0 || !bytes.Equal(after.Data, written) {
		t.Fatalf("the trigger object is not the trigger write after recovery: %s", nativeDigest(after))
	}
}

// settleCommittedLoss checks what a destination primary SIGKILLed with the
// copy's final transaction committed on its replicas only left: the unknown
// outcome, the copy executed natively and logged once on the new primary,
// one explicit resubmission answered from that log, and after recovery the
// single copy on the original primary (finish).
func (r *copyPrimaryChangeRun) settleCommittedLoss(h *copyChunkHold, code cephmsgr.OSDOpcode, copied nativeXattrSnapshot, copyKey, writeKey pgHistoryTarget, receipt *copyPrecommitReceipt) {
	t, ctx := r.t, r.ctx
	t.Helper()
	primary := h.route.ID
	var lost copyChunkOutcome
	select {
	case lost = <-h.done:
	case <-time.After(60 * time.Second):
		t.Fatal("the copy's wait did not end after its primary was killed")
	}
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) || lost.reply.Transaction != 0 || lost.reply.Submission.Transaction != h.op.TID || lost.reply.Submission.GlobalID != r.gid ||
		lost.reply.Submission.ClientIncarnation != copyPrimaryCopyIncarnation || lost.reply.Submission.MapEpoch != h.route.Epoch {
		t.Fatalf("lost copy metadata (barrier tid %d): %+v %v", h.op.TID, lost.reply, lost.err)
	}
	receipt.Unknown = &copyChunkUnknown{TID: lost.reply.Submission.Transaction, GlobalID: lost.reply.Submission.GlobalID,
		Incarnation: lost.reply.Submission.ClientIncarnation, Epoch: lost.reply.Submission.MapEpoch, Cause: outcomeCause(lost.err)}
	h.o.Close()
	t.Logf("osd.%d killed (exit %d) before its local commit; the copy ended unknown (%s): %v", primary, receipt.KillExit, receipt.Unknown.Cause, lost.err)
	r.recovery.markDown(t, ctx, primary)
	r.restartedPrimary = true
	moved := r.waitMoved()
	receipt.NewPrimary, receipt.MovedEpoch = moved.ID, moved.Epoch
	for _, osd := range h.route.Acting {
		if osd != primary {
			r.waitTempRemoved(osd, moved)
			receipt.TempRemoved = append(receipt.TempRemoved, osd)
		}
	}

	// Native evidence of the loss's result, before any resubmission: the
	// replicas' commit is the copy.
	native := poolObserve(t, ctx, 2, r.dest, "", "")
	receipt.Native = nativeDigest(native)
	if !sameCopy(native, copied, copied.Version) {
		t.Fatalf("the destination is not the copy after the loss: %s; source %s", nativeDigest(native), nativeDigest(copied))
	}
	if s := r.history(moved, copyKey, writeKey); len(s.Targets[0].Locations) != 1 {
		t.Fatalf("the new primary's PG log holds the copy %d times: %+v", len(s.Targets[0].Locations), s.Targets[0].Locations)
	}

	second, secondSource := r.writeSource("second source write", 1700010200, []byte("pre-commit loss second source bytes"), []byte{6, 0xfe})
	receipt.Stalled.SecondUser = second.UserVersion
	if second.UserVersion <= copied.Version || sameNative(secondSource, copied) {
		t.Fatalf("the second source write is not distinguishable from the first: user %d (first %d)", second.UserVersion, copied.Version)
	}
	again := r.copyRequest(code, moved)
	again.Resubmit, again.RetryAttempt = &lost.reply.Submission, 1
	answered := r.send("resubmission to the new primary", moved, again)
	receipt.recordReply(answered)
	t.Logf("resubmitted to osd.%d: code=%d ops=%v tid=%d attempt=%d flags=%#x user=%d replay=%s", moved.ID, answered.Code, receipt.Reply.OpResults,
		answered.Transaction, answered.RetryAttempt, answered.Flags, answered.UserVersion, replayVersion(answered))
	if err := judgeLoggedResubmission(answered, h.op.TID, copied.Version); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	if !sameNative(after, native) {
		t.Fatalf("the resubmission changed the destination: %s; was %s", nativeDigest(after), nativeDigest(native))
	}
	secondKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: second.Transaction}
	binding := copyFromHistoryWant{object: hobjectHead(moved), version: replayVersion(answered), sourceUserVersion: copied.Version}
	s := r.history(moved, copyKey, writeKey, secondKey)
	if err := copyFromDestinationBinding(s.Targets[0].Locations, s.Targets[1].Locations, binding); err != nil {
		t.Fatalf("new primary osd.%d after the resubmission: %v", moved.ID, err)
	}
	if len(s.Targets[2].Locations) != 0 {
		t.Fatalf("the destination PG log names the second source write: %+v", s.Targets[2].Locations)
	}
	r.finish(after, answered.Transaction, copyKey, writeKey, binding)
	receipt.Final = nativeDigest(after)
}

// judgeLoggedResubmission decides whether a resubmission of a copy the PG
// log already holds was answered from the log: the original transaction,
// attempt 1, one op, 0, and the user version of the copied object, not of a
// later source.
func judgeLoggedResubmission(reply cephmsgr.OSDReply, tid, user uint64) error {
	if reply.Transaction != tid || reply.RetryAttempt != 1 || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || reply.UserVersion != user {
		return fmt.Errorf("the resubmission was not answered from the log: tid %d attempt %d code %d ops %d user %d; want tid %d, attempt 1, 0, user %d",
			reply.Transaction, reply.RetryAttempt, reply.Code, len(reply.Operations), reply.UserVersion, tid, user)
	}
	return nil
}

// TestPublishAfterCleanups checks that a receipt is published after the
// subtest's later cleanups wrote into it.
func TestPublishAfterCleanups(t *testing.T) {
	var published []copyPrecommitReceipt
	t.Run("cleanup writes", func(t *testing.T) {
		var receipt copyPrecommitReceipt
		publishAfterCleanups(t, func() { published = append(published, receipt) })
		t.Cleanup(func() {
			receipt.ChunkRestore = true
			receipt.Stalled.Settled = &copyCommitStall{OSD: 2, Delay: 2}
		})
		receipt.Cell = "COPY_FROM/destination-before-commit"
	})
	if len(published) != 1 || !published[0].ChunkRestore || published[0].Stalled.Settled == nil || published[0].Cell == "" {
		t.Fatalf("published %+v", published)
	}
}

func TestCopyFromPrecommitJudgements(t *testing.T) {
	const gid = 4641
	route := osdFixtureOracle{Up: []int32{1, 0, 2}, Acting: []int32{1, 0, 2}, Replicas: 3, Pool: 4, Name: "dst", ID: 1, PrimaryID: 1, Shard: -1, Epoch: 30, PGID: "4.3", RawPGID: "4.d6c2a1e3", Address: "v2:192.0.2.11:6800/1"}
	stalledEvents := []string{"queued_for_pg", "reached_pg", "started", "waiting for subops from 0,2", "sub_op_commit_rec", "sub_op_commit_rec"}
	ops := opsInFlightSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3", Client: "client.4641.33",
		Ops: []opsInFlightOp{{Reqid: "client.4641.33:5", TID: 5, PGID: "4.3", Epoch: 30, FlagPoint: "waiting for sub ops", Events: stalledEvents}}}
	objecter := objecterRequestsSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3"}
	if op, err := checkCommitStalled(ops, objecter, route, gid, 5); err != nil || op.TID != 5 {
		t.Fatal("stalled commit refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot, *objecterRequestsSnapshot){
		"source read pending": func(_ *opsInFlightSnapshot, o *objecterRequestsSnapshot) {
			o.Ops = []objecterRequestsOp{{TID: 9, PG: "3.5", OSD: 0, Attempts: 1}}
		},
		"committed locally": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events = append(slices.Clone(stalledEvents), "op_commit")
		},
		"commit sent": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].Events, o.Ops[0].FlagPoint = append(slices.Clone(stalledEvents), "commit_sent"), "commit sent; apply or cleanup"
		},
		"one replica": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Events = stalledEvents[:5] },
		"not issued":  func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops[0].Events = stalledEvents[:3] },
		"another op": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) {
			o.Ops[0].TID, o.Ops[0].Reqid = 6, "client.4641.33:6"
		},
		"copy answered": func(o *opsInFlightSnapshot, _ *objecterRequestsSnapshot) { o.Ops = nil },
		"absence claimed": func(_ *opsInFlightSnapshot, o *objecterRequestsSnapshot) {
			o.AbsenceEstablished = true
		},
	} {
		o, b := ops, objecter
		o.Ops = slices.Clone(ops.Ops)
		edit(&o, &b)
		if _, err := checkCommitStalled(o, b, route, gid, 5); err == nil {
			t.Error("stalled commit accepted:", name)
		}
	}

	trigger := opsInFlightSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: "4.3", Client: "client.4641.37",
		Ops: []opsInFlightOp{{Reqid: "client.4641.37:7", TID: 7, PGID: "4.3", Epoch: 30, FlagPoint: "waiting for sub ops", Events: stalledEvents}}}
	if _, err := checkTriggerStalled(trigger, route, gid); err != nil {
		t.Fatal("stalled trigger refused:", err)
	}
	for name, edit := range map[string]func(*opsInFlightSnapshot){
		"committed":      func(o *opsInFlightSnapshot) { o.Ops[0].Events = append(slices.Clone(stalledEvents), "op_commit") },
		"replica behind": func(o *opsInFlightSnapshot) { o.Ops[0].Events = stalledEvents[:5] },
		"copy client":    func(o *opsInFlightSnapshot) { o.Client = "client.4641.33" },
		"answered":       func(o *opsInFlightSnapshot) { o.Ops = nil },
		"another PG":     func(o *opsInFlightSnapshot) { o.Ops[0].PGID = "4.4" },
		"other primary":  func(o *opsInFlightSnapshot) { o.OSD = 0 },
		"incomplete":     func(o *opsInFlightSnapshot) { o.Complete = false },
	} {
		o := trigger
		o.Ops = slices.Clone(trigger.Ops)
		edit(&o)
		if _, err := checkTriggerStalled(o, route, gid); err == nil {
			t.Error("stalled trigger accepted:", name)
		}
	}

	// A stalled primary may answer from a map behind the placement, never
	// from one older than the copy's or from another interval.
	code := 0
	at := route
	at.Epoch = 32
	observation := func(q copyIntervalQuery) copyIntervalObservation {
		before, after := at, at
		return copyIntervalObservation{Before: &before, After: &after, Code: &code, Query: &q}
	}
	held := copyIntervalQuery{State: "active+clean", Epoch: 31, Up: route.Up, Acting: route.Acting, PGID: "4.3", SameIntervalSince: 28}
	if err := copyStalledInterval(observation(held), route); err != nil {
		t.Fatal("stalled interval refused:", err)
	}
	if err := copyIntervalHeld(observation(held), route); err == nil {
		t.Fatal("the ordinary witness accepted a primary behind the placement")
	}
	for name, edit := range map[string]func(*copyIntervalQuery){
		"older map":    func(q *copyIntervalQuery) { q.Epoch = 29 },
		"new interval": func(q *copyIntervalQuery) { q.SameIntervalSince = 31 },
		"other acting": func(q *copyIntervalQuery) { q.Acting = []int32{0, 1, 2} },
		"other PG":     func(q *copyIntervalQuery) { q.PGID = "4.4" },
	} {
		q := held
		edit(&q)
		if err := copyStalledInterval(observation(q), route); err == nil {
			t.Error("stalled interval accepted:", name)
		}
	}

	// The stall's undo: every path that the test did not end with its own
	// SIGKILL stops the daemon, and every path restarts it and reads the
	// baseline back.
	baseline := copyCommitStall{OSD: 2, Action: "show", Crash: 0, Delay: 2}
	armedNow := copyCommitStall{OSD: 2, Action: "show", Crash: 1 << 30, Delay: 60}
	type undo struct {
		stops, restores int
		stopErr         error
		restoreErr      error
		shown           copyCommitStall
	}
	run := func(killed bool, u *undo) (copyCommitStall, error) {
		return settleCommitStall(killed, baseline,
			func() error { u.stops++; return u.stopErr },
			func() error { u.restores++; return u.restoreErr },
			func() copyCommitStall { return u.shown })
	}
	for name, c := range map[string]struct {
		killed bool
		u      undo
		stops  int
		ok     bool
	}{
		// The arm and its readback succeeded and the trigger failed; the arm
		// or its readback failed; the arm took long; the daemon is still
		// running or exited on its own: all are stopped and restarted.
		"trigger failed after the arm":  {false, undo{shown: baseline}, 1, true},
		"arm readback failed":           {false, undo{shown: baseline}, 1, true},
		"killed by the test":            {true, undo{shown: baseline}, 0, true},
		"stop refused":                  {false, undo{stopErr: errors.New("no such container"), shown: baseline}, 1, false},
		"restart failed":                {false, undo{restoreErr: errors.New("start failed"), shown: baseline}, 1, false},
		"still armed after the restart": {false, undo{shown: armedNow}, 1, false},
		"delay left behind":             {false, undo{shown: copyCommitStall{OSD: 2, Delay: 60}}, 1, false},
		"options not read":              {false, undo{shown: copyCommitStall{OSD: 2, Failures: []string{"refused"}}}, 1, false},
		"another OSD answered":          {false, undo{shown: copyCommitStall{OSD: 1, Delay: 2}}, 1, false},
		"killed, restarted still armed": {true, undo{shown: armedNow}, 0, false},
	} {
		u := c.u
		_, err := run(c.killed, &u)
		// A failed stop never skips the final restart or the readback.
		if (err == nil) != c.ok || u.stops != c.stops || u.restores != 1 {
			t.Errorf("%s: err %v, %d stops, %d restores", name, err, u.stops, u.restores)
		}
	}
	// A stop error stays an error even when the readback is the baseline.
	if _, err := run(false, &undo{stopErr: errors.New("timed out"), shown: baseline}); err == nil || !strings.Contains(err.Error(), "stop the armed osd.2") {
		t.Errorf("a failed stop was settled: %v", err)
	}

	// An unverified settle closes the gate for every later cell.
	gate := &copyStallGate{}
	if err := gate.admit(); err != nil {
		t.Fatal("an open gate refused a cell:", err)
	}
	gate.unverified = errors.New("osd.2's runtime options were not read after the stall")
	if err := gate.admit(); err == nil || !strings.Contains(err.Error(), "not run") {
		t.Fatal("a closed gate admitted a cell:", err)
	}

	answered := cephmsgr.OSDReply{Transaction: 5, RetryAttempt: 1, UserVersion: 12, Operations: []cephmsgr.OSDOperationResult{{Result: 0}}}
	if err := judgeLoggedResubmission(answered, 5, 12); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*cephmsgr.OSDReply){
		"executed again": func(r *cephmsgr.OSDReply) { r.UserVersion = 14 },
		"new request":    func(r *cephmsgr.OSDReply) { r.Transaction, r.RetryAttempt = 9, 0 },
		"refused":        func(r *cephmsgr.OSDReply) { r.Code = -2 },
	} {
		r := answered
		edit(&r)
		if err := judgeLoggedResubmission(r, 5, 12); err == nil {
			t.Error("resubmission accepted:", name)
		}
	}
}

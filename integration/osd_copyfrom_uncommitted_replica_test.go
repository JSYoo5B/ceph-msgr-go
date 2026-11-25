package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type copyReplicaTransaction struct {
	Reqid     string   `json:"reqid"`
	PGID      string   `json:"pgid"`
	Epoch     uint32   `json:"epoch"`
	MinEpoch  uint32   `json:"min_epoch"`
	Object    string   `json:"object"`
	Version   string   `json:"version"`
	FlagPoint string   `json:"flag_point"`
	Events    []string `json:"events"`
}

type copyReplicaSnapshot struct {
	opsInFlightSnapshot
	Repops []copyReplicaTransaction `json:"repops"`
}

func copyReceiver(t *testing.T, ctx context.Context, r *copyPrimaryChangeRun, object string, receiver int32, incarnation int32) copyReplicaSnapshot {
	t.Helper()
	var result struct {
		Snapshot copyReplicaSnapshot `json:"repops_in_flight"`
	}
	nativePoolResult(t, ctx, "repopinflight", map[string]any{"slot": 2, "object": object, "osd": receiver, "entity": map[string]any{"type": "client", "num": r.gid, "inc": incarnation}}, &result)
	return result.Snapshot
}

func checkCopyReceiver(s copyReplicaSnapshot, route osdFixtureOracle, receiver int32, reqid string) (copyReplicaTransaction, error) {
	var empty copyReplicaTransaction
	if !s.Complete || len(s.Failures) != 0 || s.AbsenceEstablished || s.OSD != receiver || s.PGID != route.PGID || receiver == route.ID || !slices.Contains(route.Acting, receiver) || !samePlacement(s.Before, route) || !samePlacement(s.After, route) || s.Before.Epoch != route.Epoch || s.After.Epoch != route.Epoch || len(s.Repops) != 1 {
		return empty, fmt.Errorf("replica snapshot does not bind one transaction to its unchanged placement: %+v", s)
	}
	op := s.Repops[0]
	if op.Reqid != reqid || op.PGID != route.PGID || op.Epoch != route.Epoch || op.MinEpoch == 0 || op.MinEpoch > op.Epoch || op.Object != hobjectHead(route) || op.FlagPoint != "started" || op.Version == "" || op.Version == "0'0" || !slices.Contains(op.Events, "started") || slices.Contains(op.Events, "commit_sent") || slices.Contains(op.Events, "done") {
		return empty, fmt.Errorf("receiver transaction identity or precommit events differ: %+v", op)
	}
	return op, nil
}

type copyUncommittedLoss struct {
	OSD            int32  `json:"osd"`
	Seed           string `json:"destination_seed"`
	Source         string `json:"source_before"`
	CopyLogVersion string `json:"copy_log_version"`
	SecondSource   string `json:"second_source"`
}

type copyUncommittedReceipt struct {
	Cell            string                 `json:"cell"`
	SourcePrimary   int32                  `json:"source_primary"`
	Primary         int32                  `json:"destination_primary"`
	SourceBytes     int                    `json:"source_bytes"`
	SourceUser      uint64                 `json:"source_user_version"`
	BarrierEpoch    uint32                 `json:"barrier_epoch"`
	BarrierReqid    string                 `json:"barrier_reqid"`
	BarrierPoint    string                 `json:"barrier_flag_point"`
	BarrierEvents   []string               `json:"barrier_events"`
	KillExit        int                    `json:"kill_exit_code"`
	Unknown         *copyChunkUnknown      `json:"unknown"`
	Reply           *json.RawMessage       `json:"reply"`
	Native          string                 `json:"native_after_loss"`
	Final           string                 `json:"native_final"`
	TempAfter       []int32                `json:"osds_without_temporary"`
	Replica         *copyUncommittedLoss   `json:"replica_loss"`
	Stalled         copyStalledCommit      `json:"stalled_commit"`
	Receiver        copyReplicaTransaction `json:"receiver_transaction"`
	TriggerReceiver copyReplicaTransaction `json:"trigger_receiver_transaction"`
	ReceiverSHA     string                 `json:"receiver_snapshot_sha256"`
}

// A stock BlueStore commit stall is armed on the destination replica that
// leads neither source nor destination. Unlike a pause lease ended before
// SIGKILL, this hold never resumes the replica before loss. A same-PG 64 KiB
// trigger reaches that receiver after arming; the primary commits and receives
// exactly one replica commit. Then a single-chunk head copy finishes its
// source read and reaches the same phase. The receiver's fully decoded
// MOSDRepOp binds the original request, PG, epoch and transaction version,
// with started but no commit_sent. The armed device flush cannot complete
// before daemon exit (the stock control is documented in the commitstall
// oracle); absence of an ACK alone is not the barrier.
//
// Both waits are closed with unknown outcomes before the held receiver is
// SIGKILLed, within the 40 s work budget of the 60 s stall. No copy is replayed.
// Recovery restores only the replica; native state and the primary PG log
// must show the original copy once, at the receiver transaction's version.
// A distinguishable source write, fresh-session read and delayed native
// readback retain that result. This covers ordinary replicated head copies,
// not multichunk, snapshot, pinned, EC or per-shard offline persistence.
//
//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromuncommittedreplica key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:row osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromuncommittedreplica key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=3 osd_profile=replicated-pg-v8
//scenario:covers CLIENT to OSD
//fixture:isolated arms a bounded replica commit stall and kills the owned receiver before commit
func TestCephOSDCopyFromUncommittedReplicaIntegration(t *testing.T) {
	ctx, pools, recovery := copyPrimaryChangeFixture(t, 1500*time.Second)
	var receipts []copyUncommittedReceipt
	t.Cleanup(func() {
		raw, err := json.MarshalIndent(receipts, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(recovery.dir, "osd-copyfrom-uncommitted-replica-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	})
	gate := &copyStallGate{}
	for _, tc := range copyPrimaryChangeCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := gate.admit(); err != nil {
				t.Fatal(err)
			}
			receipt := copyUncommittedReceipt{}
			receipt.Cell = tc.name + "/uncommitted-replica"
			receipt.Replica = &copyUncommittedLoss{OSD: -1}
			publishAfterCleanups(t, func() { receipts = append(receipts, receipt) })
			copyUncommittedReplica(t, ctx, pools, recovery, gate, tc, &receipt)
		})
	}
}

func copyUncommittedReplica(t *testing.T, ctx context.Context, pools ownedPools, recovery *copyPrimaryRecovery, gate *copyStallGate, tc copyPrimaryChangeCase, receipt *copyUncommittedReceipt) {
	r := newCopyPrimaryChangeRun(t, ctx, pools, recovery, fmt.Sprintf("uncommitted-%d", tc.code))
	receipt.SourcePrimary = r.separateSource()
	receipt.Primary = r.dstRoute.ID
	frozen, err := copyLossFrozen(r.dstRoute.Acting, r.dstRoute.ID, r.srcRoute.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	receiver := frozen[0]
	receipt.Replica.OSD = receiver
	guard := r.guardCommitStall(gate, receiver, &receipt.Stalled)
	first, copied := r.writeSource("source write", 1700010300, []byte("uncommitted replica original source\x00\xff"), []byte{7, 0xfe})
	receipt.SourceBytes, receipt.SourceUser = len(copied.Data), first.UserVersion
	receipt.Replica.Seed = nativeDigest(r.seedDestination())
	receipt.Replica.Source = nativeDigest(copied)
	route, _ := r.placeForCopy()
	trigger, triggerRoute := r.blockerObject()
	receipt.Stalled.Trigger = trigger
	triggerConn := mutationConnection(t, r.client, ctx, triggerRoute)
	copyConn := mutationConnection(t, r.client, ctx, route)
	t.Cleanup(func() { triggerConn.Close(); copyConn.Close() })
	armed := guard.arm()
	work, cancel := context.WithDeadline(ctx, armed.Add(copyCommitStallDelay-20*time.Second))
	defer cancel()
	written := bytes.Repeat([]byte{0x57}, 64<<10)
	request := r.pools.request(triggerRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(written)), Data: written})
	request.ClientIncarnation = copyStallTriggerIncarnation
	request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700010310}
	triggerDone := make(chan copyChunkOutcome, 1)
	go func() { reply, err := triggerConn.Request(ctx, request); triggerDone <- copyChunkOutcome{reply, err} }()
	observe := func(object string, at osdFixtureOracle, inc int32, done <-chan copyChunkOutcome) (opsInFlightOp, copyReplicaTransaction, string) {
		var prior copyReplicaTransaction
		var verdict error
		for seen := 0; seen < 2; {
			if work.Err() != nil {
				t.Fatalf("receiver barrier budget: %v; last judgment: %v", work.Err(), verdict)
			}
			select {
			case got := <-done:
				t.Fatalf("request answered before held replica loss: %+v %v", got.reply, got.err)
			default:
			}
			ops := copyOpsInFlight(t, work, 2, object, nil, r.gid, inc)
			objecter := copyObjecterRequests(t, work, 2, object, nil, pools.ids[1], r.source)
			op, e := checkCopyStaged(ops, objecter, at, r.gid, inc, 1)
			if e != nil {
				if seen != 0 {
					t.Fatalf("receiver barrier changed between observations: %v", e)
				}
				verdict = e
				time.Sleep(100 * time.Millisecond)
				continue
			}
			snapshot := copyReceiver(t, work, r, object, receiver, inc)
			rep, e := checkCopyReceiver(snapshot, at, receiver, op.Reqid)
			if e != nil {
				if seen != 0 {
					t.Fatalf("receiver barrier changed between observations: %v", e)
				}
				verdict = e
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if seen != 0 && (prior.Version != rep.Version || prior.Reqid != rep.Reqid) {
				t.Fatal("receiver transaction changed across barrier")
			}
			prior = rep
			seen++
			if seen == 2 {
				return op, rep, snapshot.SHA256
			}
			time.Sleep(time.Second)
		}
		panic("unreachable")
	}
	triggerOp, triggerRep, _ := observe(trigger, triggerRoute, copyStallTriggerIncarnation, triggerDone)
	receipt.TriggerReceiver = triggerRep
	receipt.Stalled.TriggerReqid, receipt.Stalled.TriggerEvents, receipt.Stalled.TriggerSubmitTID = triggerOp.Reqid, triggerOp.Events, triggerOp.TID
	receipt.Stalled.TriggerPoint = triggerOp.FlagPoint
	done := make(chan copyChunkOutcome, 1)
	go func() {
		reply, err := copyConn.Request(ctx, r.copyRequest(tc.code, route))
		done <- copyChunkOutcome{reply, err}
	}()
	heldStart := time.Now()
	op, rep, sha := observe(r.dest, route, copyPrimaryCopyIncarnation, done)
	receipt.Receiver, receipt.ReceiverSHA = rep, sha
	receipt.Stalled.Observations = 2
	receipt.Stalled.HeldFor = time.Since(heldStart).String()
	receipt.BarrierEpoch, receipt.BarrierReqid, receipt.BarrierPoint, receipt.BarrierEvents = route.Epoch, op.Reqid, op.FlagPoint, op.Events
	receipt.Stalled.CopyPoint, receipt.Stalled.CopyEvents = op.FlagPoint, op.Events
	copyConn.Close()
	triggerConn.Close()
	lost := awaitPrecommitUnknown(t, work, done, r.gid, copyPrimaryCopyIncarnation, op.TID, route.Epoch)
	receipt.Unknown = &copyChunkUnknown{TID: lost.reply.Submission.Transaction, GlobalID: r.gid, Incarnation: copyPrimaryCopyIncarnation, Epoch: route.Epoch, Cause: outcomeCause(lost.err)}
	triggerLost := awaitPrecommitUnknown(t, work, triggerDone, r.gid, copyStallTriggerIncarnation, triggerOp.TID, triggerRoute.Epoch)
	receipt.Stalled.TriggerOutcome = outcomeCause(triggerLost.err)
	if work.Err() != nil {
		t.Fatal("stall work budget expired before kill")
	}
	receipt.KillExit = recovery.kill(t, work, receiver)
	guard.killed = true
	receipt.Stalled.KilledAfter = time.Since(armed).String()
	if work.Err() != nil {
		t.Fatal("stall work budget expired during kill")
	}
	recovery.markDown(t, ctx, receiver)
	recovery.restore(t)
	if t.Failed() {
		return
	}
	returned := r.waitReturned()
	after := poolObserve(t, ctx, 2, r.dest, "", "")
	if !sameCopy(after, copied, copied.Version) || !sameNative(poolObserve(t, ctx, 1, r.source, "", ""), copied) {
		t.Fatal("native recovered copy differs from original source")
	}
	receipt.Native = nativeDigest(after)
	copyKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryCopyIncarnation, TID: op.TID}
	writeKey := pgHistoryTarget{Type: "client", Num: r.gid, Inc: copyPrimaryWriteIncarnation, TID: first.Transaction}
	history := r.history(returned, copyKey, writeKey)
	binding := copyFromHistoryWant{object: hobjectHead(returned), version: rep.Version, sourceUserVersion: copied.Version}
	if err := copyFromDestinationBinding(history.Targets[0].Locations, history.Targets[1].Locations, binding); err != nil {
		t.Fatal(err)
	}
	receipt.Replica.CopyLogVersion = rep.Version
	second, changed := r.writeSource("second source write", 1700010400, []byte("uncommitted replica second source"), []byte{8})
	if sameNative(changed, copied) || second.UserVersion <= copied.Version {
		t.Fatal("second source not distinguishable")
	}
	receipt.Stalled.SecondUser, receipt.Replica.SecondSource = second.UserVersion, nativeDigest(changed)
	r.finish(after, second.Transaction, copyKey, writeKey, binding)
	receipt.Final = nativeDigest(after)
	for _, osd := range returned.Acting {
		if _, err := checkCopyTemp(copyStaging(t, ctx, osd, returned), osd, returned, 0); err != nil {
			t.Fatal(err)
		}
		receipt.TempAfter = append(receipt.TempAfter, osd)
	}
	triggered := poolObserve(t, ctx, 2, trigger, "", "")
	receipt.Stalled.TriggerNative = nativeDigest(triggered)
	if triggered.Code != 0 || !bytes.Equal(triggered.Data, written) {
		t.Fatal("recovered trigger differs")
	}
}

func TestCopyUncommittedReceiverJudgement(t *testing.T) {
	route := osdFixtureOracle{Up: []int32{2, 0, 1}, Acting: []int32{2, 0, 1}, Replicas: 3, MinSize: 2, Pool: 4, Name: "destination", ID: 2, PrimaryID: 2, Shard: -1, Epoch: 23, PGID: "4.1", RawPGID: "4.abc"}
	sample := func() copyReplicaSnapshot {
		return copyReplicaSnapshot{opsInFlightSnapshot: opsInFlightSnapshot{Complete: true, Before: route, After: route, OSD: 1, PGID: route.PGID}, Repops: []copyReplicaTransaction{{Reqid: "client.47.33:2", PGID: "4.1", Epoch: 23, MinEpoch: 21, Object: hobjectHead(route), FlagPoint: "started", Version: "23'4", Events: []string{"initiated", "started"}}}}
	}
	if _, err := checkCopyReceiver(sample(), route, 1, "client.47.33:2"); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*copyReplicaSnapshot){
		"incomplete":                     func(s *copyReplicaSnapshot) { s.Complete = false },
		"failure":                        func(s *copyReplicaSnapshot) { s.Failures = []string{"native error"} },
		"absence claim":                  func(s *copyReplicaSnapshot) { s.AbsenceEstablished = true },
		"wrong receiver":                 func(s *copyReplicaSnapshot) { s.OSD = 0 },
		"moved epoch":                    func(s *copyReplicaSnapshot) { s.After.Epoch++ },
		"no receiver transaction":        func(s *copyReplicaSnapshot) { s.Repops = nil },
		"duplicate receiver transaction": func(s *copyReplicaSnapshot) { s.Repops = append(s.Repops, s.Repops[0]) },
		"wrong identity":                 func(s *copyReplicaSnapshot) { s.Repops[0].Reqid = "client.47.33:3" },
		"wrong PG":                       func(s *copyReplicaSnapshot) { s.Repops[0].PGID = "4.2" },
		"wrong epoch":                    func(s *copyReplicaSnapshot) { s.Repops[0].Epoch++ },
		"bad minimum epoch":              func(s *copyReplicaSnapshot) { s.Repops[0].MinEpoch = 24 },
		"wrong object":                   func(s *copyReplicaSnapshot) { s.Repops[0].Object = "other" },
		"wrong receiver point":           func(s *copyReplicaSnapshot) { s.Repops[0].FlagPoint = "commit sent" },
		"wrong snapshot PG":              func(s *copyReplicaSnapshot) { s.PGID = "4.2" },
		"no version":                     func(s *copyReplicaSnapshot) { s.Repops[0].Version = "" },
		"zero version":                   func(s *copyReplicaSnapshot) { s.Repops[0].Version = "0'0" },
		"not started":                    func(s *copyReplicaSnapshot) { s.Repops[0].Events = []string{"initiated"} },
		"committed":                      func(s *copyReplicaSnapshot) { s.Repops[0].Events = append(s.Repops[0].Events, "commit_sent") },
		"done":                           func(s *copyReplicaSnapshot) { s.Repops[0].Events = append(s.Repops[0].Events, "done") },
	} {
		t.Run(name, func(t *testing.T) {
			s := sample()
			edit(&s)
			if _, err := checkCopyReceiver(s, route, 1, "client.47.33:2"); err == nil {
				t.Fatal("invalid replica evidence accepted")
			}
		})
	}
}

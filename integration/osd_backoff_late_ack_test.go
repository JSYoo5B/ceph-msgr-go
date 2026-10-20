package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeLateAckReceipt struct {
	Token        string `json:"token"`
	GlobalID     uint64 `json:"global_id"`
	ID           uint64 `json:"id"`
	MapEpoch     uint32 `json:"map_epoch"`
	PG           string `json:"pgid"`
	Op           uint8  `json:"op"`
	SourceType   uint8  `json:"source_type"`
	SourceNumber uint64 `json:"source_number"`
	OSD          int32  `json:"osd_id"`
	ReachedPG    bool   `json:"reached_pg"`
	Evidence     string `json:"evidence"`
	Source       string `json:"source_commit"`
}

// Unlike the normal-ACK acceptance, this leaves the native BLOCK unacknowledged
// until after activation delivered UNBLOCK. The same authenticated connection
// must retain a bounded receipt and local gate until the original ACK is queued.
func TestCephOSDBackoffLateAckIntegration(t *testing.T) {
	if !isReplicatedPGFixtureProfile() || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("requires an opt-in single-OSD replicated-PG native late-ACK fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	var o *cephmsgr.OSDConnection
	var pendingCancel context.CancelFunc
	prepared, started, restored, cleaned, lateAckQueued := false, false, false, false, false
	var name string
	var start, finish nativeBackoffScope
	var block, unblock cephmsgr.OSDBackoffEvent
	var pendingReply, blockedReply, appendReply, readReply cephmsgr.OSDReply
	var before, afterRestore, afterBlocked, afterAppend, afterRead mutationSnapshot
	var cleanupError string
	var nativeAck nativeLateAckReceipt
	ackInputPath := filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-backoff-ack-input.json")
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Profile                                                    string `json:"profile"`
				Start, Finish                                              nativeBackoffScope
				Block, Unblock                                             cephmsgr.OSDBackoffEvent
				Pending, Blocked, Append, Read                             cephmsgr.OSDReply
				Before, AfterRestore, AfterBlocked, AfterAppend, AfterRead mutationSnapshot
				LateAckQueued, Restored, Cleaned                           bool
				CleanupError                                               string
				NativeAck                                                  nativeLateAckReceipt `json:"native_ack_receipt"`
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), start, finish, block, unblock, pendingReply, blockedReply, appendReply, readReply, before, afterRestore, afterBlocked, afterAppend, afterRead, lateAckQueued, restored, cleaned, cleanupError, nativeAck})
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-backoff-late-ack-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if pendingCancel != nil {
			pendingCancel()
		}
		// Restore the owned pool before any object cleanup, even after a failed
		// delayed ACK. Closing this connection drops its server-side receipt.
		if started && !restored {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			var err error
			finish, err = backoffControl(cleanup, "finish", name)
			stop()
			if err != nil {
				cleanupError = err.Error()
				t.Error("late-ACK native pool restoration unconfirmed", err)
			} else {
				restored = finish.Done && finish.Restored
			}
		}
		if err := os.Remove(ackInputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error("remove owned late-ACK input", err)
		}
		if o != nil {
			_ = o.Close()
		}
		if prepared && (!started || restored) {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			cleaned = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !cleaned {
				t.Error("late-ACK native owned cleanup incomplete")
			}
		}
		if prepared && started && !restored {
			t.Error("late-ACK owned head retained because native pool restoration was not confirmed")
		}
	})
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte{0, 255, 128, 'l', 'a', 't', 'e', '-', 'a', 'c', 'k'}}})
	prepared = true
	if len(owned.Objects) != 1 {
		t.Fatal("native late-ACK recipe needs one first owned object")
	}
	name = owned.Objects[0]
	native := replicatedPGNative(t, ctx, name)
	before = native.Actual
	route := native.Placement
	o = replicatedPGConnection(t, c, ctx, route)
	if err := os.Remove(ackInputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal("clear previous completed scope ACK input", err)
	}
	started = true
	var err error
	start, err = backoffControl(ctx, "start", name)
	if err != nil {
		t.Fatal("start late-ACK inactive PG recipe", err)
	}
	if !start.Ready || start.Done || start.Restored || start.Inactive.Pool.Size != 2 || start.Inactive.Pool.MinSize != 2 {
		t.Fatal("late-ACK native scope did not become inactive", start)
	}
	route = nativeBackoffRoute(t, route, start.Inactive)
	pendingCtx, cancelPending := context.WithCancel(ctx)
	pendingCancel = cancelPending
	request := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	request.ClientIncarnation = 83
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan completion, 1)
	go func() { r, e := o.Request(pendingCtx, request); done <- completion{r, e} }()
	block, err = o.NextBackoff(ctx)
	if err != nil {
		t.Fatal("actual native BLOCK before delayed ACK", err)
	}
	if block.Op != cephmsgr.OSDBackoffBlock || block.Pool != uint64(route.Pool) || block.Seed != route.PG || block.Shard != -1 || block.MapEpoch != start.Inactive.PG.Epoch || block.ID == 0 || block.Source.ID != route.ID || block.GlobalID != c.Snapshot().GlobalID || block.SessionGeneration == 0 || len(block.RawFront) == 0 {
		t.Fatal("late-ACK BLOCK identity differs", block, route)
	}
	cancelPending()
	var original completion
	select {
	case original = <-done:
	case <-ctx.Done():
		t.Fatal("late-ACK sent STAT cancellation did not return", ctx.Err())
	}
	pendingReply = original.reply
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(original.err, &unknown) || !errors.Is(original.err, context.Canceled) || pendingReply.Submission.Transaction == 0 || pendingReply.Submission.ClientIncarnation != 83 || !reflect.DeepEqual(pendingReply.Submission.Placement, request.Placement) || pendingReply.Transaction != 0 || len(pendingReply.RawFront) != 0 {
		t.Fatal("late-ACK sent STAT lost unknown outcome", pendingReply, original.err)
	}
	// No AckBackoff call occurs before restoration. Native activation therefore
	// releases an unacknowledged BLOCK, exercising its deleting-receipt branch.
	finish, err = backoffControl(ctx, "finish", name)
	if err != nil {
		t.Fatal("restore PG before late ACK", err)
	}
	restored = finish.Done && finish.Restored
	if !restored || finish.Token != start.Token || !reflect.DeepEqual(finish.Before.ObjectState, finish.After.ObjectState) {
		t.Fatal("late-ACK native recipe changed owned state or scope", finish)
	}
	unblock, err = o.NextBackoff(ctx)
	if err != nil {
		t.Fatal("native UNBLOCK before original ACK", err)
	}
	if unblock.Op != cephmsgr.OSDBackoffUnblock || !unblock.Matched || unblock.ID != block.ID || unblock.Pool != block.Pool || unblock.Seed != block.Seed || unblock.Shard != block.Shard || unblock.MapEpoch < block.MapEpoch || unblock.GlobalID != block.GlobalID || unblock.SessionGeneration != block.SessionGeneration || !bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) || !bytes.Equal(unblock.End.Raw, block.End.Raw) {
		t.Fatal("late-ACK native UNBLOCK identity differs", unblock, block)
	}
	freshNative := replicatedPGNative(t, ctx, name)
	afterRestore = freshNative.Actual
	route = freshNative.Placement
	if !reflect.DeepEqual(before, afterRestore) {
		t.Fatal("late-ACK canceled STAT or native restoration altered bytes/version")
	}
	tail := []byte{0, 255, 'l', 'a', 't', 'e', '-', 'A', 'C', 'K'}
	blockedRequest := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	blockedRequest.ClientIncarnation = 84
	blockedRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000970, Nanoseconds: 31415926}
	blockedReply, err = o.Request(ctx, blockedRequest)
	if !errors.Is(err, cephmsgr.ErrOSDBackoff) || errors.As(err, &unknown) || blockedReply.Submission.Transaction == 0 || blockedReply.Submission.ClientIncarnation != 84 || !reflect.DeepEqual(blockedReply.Submission.Placement, blockedRequest.Placement) || blockedReply.Transaction != 0 || len(blockedReply.RawFront) != 0 {
		t.Fatal("UNBLOCK released local admission before original ACK", blockedReply, err)
	}
	afterBlocked = replicatedPGNative(t, ctx, name).Actual
	if !reflect.DeepEqual(before, afterBlocked) {
		t.Fatal("late-ACK locally refused APPEND reached native daemon")
	}
	// ACK uses the retained BLOCK receipt and its original epoch, not UNBLOCK's
	// newer epoch. Same-writer order places this ACK before a later object send.
	ackCtx, cancelAck := context.WithCancel(ctx)
	if err := o.AckBackoff(ackCtx, block); err != nil {
		cancelAck()
		t.Fatal("explicit late original-epoch ACK_BLOCK", err)
	}
	lateAckQueued = true
	// A successfully queued control is irrevocable through its enqueue context.
	cancelAck()
	ackInput, err := json.Marshal(map[string]any{"global_id": block.GlobalID, "id": block.ID, "map_epoch": block.MapEpoch, "pgid": fmt.Sprintf("%d.%x", block.Pool, block.Seed)})
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(ackInputPath, ackInput); err != nil {
		t.Fatal(err)
	}

	sendCtx, stopSend := context.WithTimeout(ctx, 3*time.Second)
	defer stopSend()
	admitted := replicatedPGRequest(route, blockedRequest.Operations...)
	admitted.ClientIncarnation = 85
	admitted.ModificationTime = blockedRequest.ModificationTime
	appendReply, err = o.Request(sendCtx, admitted)
	if err != nil {
		t.Fatal("first admitted append after late ACK; residual native backoff gate", appendReply, err)
	}
	checkReplicatedPGReply(t, appendReply, route)
	afterAppend = replicatedPGNative(t, ctx, name).Actual
	want := append(bytes.Clone(before.Data), tail...)
	if appendReply.Transaction <= blockedReply.Submission.Transaction || appendReply.Transaction <= pendingReply.Submission.Transaction || appendReply.Flags&cephmsgr.OSDReplyOnDisk == 0 || afterAppend.Code != 0 || !bytes.Equal(afterAppend.Data, want) || afterAppend.Version != appendReply.UserVersion || afterAppend.Version <= before.Version || afterAppend.Seconds != admitted.ModificationTime.Seconds || afterAppend.Nanos != admitted.ModificationTime.Nanoseconds {
		t.Fatal("late-ACK native one-append postcondition differs", appendReply, afterAppend)
	}
	readCtx, stopRead := context.WithTimeout(ctx, 3*time.Second)
	defer stopRead()
	readReply, err = o.Request(readCtx, replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(want) + 32)}))
	if err != nil {
		t.Fatal("same-session stat/read after late ACK; residual daemon gate", err)
	}
	checkReplicatedPGReply(t, readReply, route)
	afterRead = replicatedPGNative(t, ctx, name).Actual
	if len(readReply.Operations) != 2 || !bytes.Equal(readReply.Operations[1].Data, want) || readReply.UserVersion != afterAppend.Version || !reflect.DeepEqual(afterAppend, afterRead) {
		t.Fatal("late-ACK follow-up read changed native bytes/version or replayed an append", readReply, afterRead)
	}
	ackObserved, err := backoffControl(ctx, "observe", name)
	if err != nil || ackObserved.Token != start.Token || !ackObserved.Restored || !ackObserved.Done {
		t.Fatal("native delayed ACK reception observation", ackObserved, err)
	}
	ackRaw, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-backoff-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Ack nativeLateAckReceipt `json:"ack_receipt"`
	}
	if err := json.Unmarshal(ackRaw, &receipt); err != nil {
		t.Fatal(err)
	}
	nativeAck = receipt.Ack
	if nativeAck.Token != start.Token || nativeAck.GlobalID != block.GlobalID || nativeAck.SourceNumber != block.GlobalID || nativeAck.SourceType != 8 || nativeAck.ID != block.ID || nativeAck.MapEpoch != block.MapEpoch || nativeAck.PG != fmt.Sprintf("%d.%x", block.Pool, block.Seed) || nativeAck.Op != 2 || nativeAck.OSD != route.ID || !nativeAck.ReachedPG || nativeAck.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || nativeAck.Evidence != "native completed op history; reception only" {
		t.Fatal("native delayed ACK source/PG/id/original epoch receipt differs", nativeAck, block)
	}

	t.Logf("native UNBLOCK preceded explicit ACK: PG=%s ID=%d BLOCK/UNBLOCK epochs=%d/%d; gate stayed local until late ACK, one append and same-session read completed without residual daemon block", route.PGID, block.ID, block.MapEpoch, unblock.MapEpoch)
}

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type nativeBackoffState struct {
	Epoch uint32 `json:"epoch"`
	Pool  struct {
		ID      int64  `json:"pool"`
		Name    string `json:"pool_name"`
		Size    int    `json:"size"`
		MinSize int    `json:"min_size"`
		PGNum   int    `json:"pg_num"`
	} `json:"pool"`
	Placement struct {
		PoolID     int64  `json:"pool_id"`
		Object     string `json:"objname"`
		RawPG      string `json:"raw_pgid"`
		PG         string `json:"pgid"`
		Primary    int32  `json:"acting_primary"`
		Acting, Up []int32
	} `json:"placement"`
	PG struct {
		State string `json:"state"`
		Epoch uint32 `json:"epoch"`
		Info  struct {
			PG string `json:"pgid"`
		} `json:"info"`
		Acting []int32 `json:"acting"`
	} `json:"pg"`
	ObjectState nativeXattrSnapshot `json:"object_state"`
}
type nativeBackoffScope struct {
	Schema                  int    `json:"schema_version"`
	Token                   string `json:"token"`
	Source                  string `json:"source_commit"`
	Object                  string `json:"object"`
	Ready, Done, Restored   bool
	Before, Inactive, After nativeBackoffState
	Error                   string               `json:"error"`
	AckReceipt              nativeLateAckReceipt `json:"ack_receipt"`
}

func backoffControl(ctx context.Context, action, name string) (nativeBackoffScope, error) {
	var result nativeBackoffScope
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(map[string]string{"object": name})
	if err != nil {
		return result, err
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-backoff-input.json"), data); err != nil {
		return result, err
	}
	if err := os.Remove(filepath.Join(dir, "osd-backoff-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-backoff", action); err != nil {
		return result, err
	}
	file, err := os.Open(filepath.Join(dir, "osd-backoff-result.json"))
	if err != nil {
		return result, err
	}
	defer file.Close()
	data, err = io.ReadAll(io.LimitReader(file, (512<<10)+1))
	if err != nil {
		return result, err
	}
	if len(data) > 512<<10 {
		return result, errors.New("native backoff scope exceeds evidence bound")
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.Schema != 1 || result.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Object != name || len(result.Token) != 32 || result.Error != "" {
		return result, errors.New("native backoff scope provenance or state differs")
	}
	return result, nil
}
func nativeBackoffRoute(t *testing.T, base osdFixtureOracle, state nativeBackoffState) osdFixtureOracle {
	t.Helper()
	p := state.Placement
	parse := func(value string) (int64, uint32) {
		parts := strings.Split(value, ".")
		if len(parts) != 2 {
			t.Fatal("invalid native PG", value)
		}
		pool, e1 := strconv.ParseInt(parts[0], 10, 64)
		seed, e2 := strconv.ParseUint(parts[1], 16, 32)
		if e1 != nil || e2 != nil {
			t.Fatal("invalid native PG", value)
		}
		return pool, uint32(seed)
	}
	pool, pg := parse(p.PG)
	rawPool, hash := parse(p.RawPG)
	if pool != base.Pool || rawPool != pool || p.PoolID != pool || p.Object != base.Name || p.Primary != base.ID || len(p.Acting) == 0 || p.Acting[0] != p.Primary || state.PG.Info.PG != p.PG || state.PG.Epoch == 0 || state.PG.Epoch < state.Epoch || state.Pool.ID != pool || state.Pool.PGNum != 8 {
		t.Fatal("native backoff actual PG/epoch/primary differs", state)
	}
	base.PG, base.Hash, base.Epoch = pg, hash, state.PG.Epoch
	base.PGID, base.RawPGID = p.PG, p.RawPG
	base.Acting, base.Up = p.Acting, p.Up
	return base
}

func TestCephOSDBackoffIntegration(t *testing.T) {
	if !isReplicatedPGFixtureProfile() || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("requires an opt-in single-OSD replicated-PG native backoff fixture")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	var o *cephmsgr.OSDConnection
	var cancelPending context.CancelFunc
	prepared, started, restored, cleaned := false, false, false, false
	var name string
	var start, observed, finish nativeBackoffScope
	var block, unblock cephmsgr.OSDBackoffEvent
	var pendingReply, blockedReply, newReply cephmsgr.OSDReply
	var finalNative mutationSnapshot
	var cleanupError string
	var nativeAck nativeLateAckReceipt
	ackInputPath := filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-backoff-ack-input.json")
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Profile                      string `json:"profile"`
				Start, Observed, Finish      nativeBackoffScope
				Block, Unblock               cephmsgr.OSDBackoffEvent
				Pending, Blocked, NewRequest cephmsgr.OSDReply
				FinalNative                  mutationSnapshot
				Restored, Cleaned            bool
				CleanupError                 string
				NativeAck                    nativeLateAckReceipt `json:"native_ack_receipt"`
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), start, observed, finish, block, unblock, pendingReply, blockedReply, newReply, finalNative, restored, cleaned, cleanupError, nativeAck})
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-backoff-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		if cancelPending != nil {
			cancelPending()
		}
		if started && !restored {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			var err error
			finish, err = backoffControl(cleanup, "finish", name)
			stop()
			if err != nil {
				cleanupError = err.Error()
				t.Error("native pool restoration unconfirmed", err)
			} else {
				restored = finish.Done && finish.Restored
			}
		}
		if err := os.Remove(ackInputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error("remove owned normal-ACK input", err)
		}
		if o != nil {
			_ = o.Close()
		}
		if prepared && (!started || restored) {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			cleaned = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !cleaned {
				t.Error("native backoff object cleanup incomplete")
			}
		}
		if prepared && started && !restored {
			t.Error("owned object retained because restoring the inactive PG was not confirmed")
		}
	})
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte{0, 255, 128, 'b', 'a', 'c', 'k', 'o', 'f', 'f'}}})
	prepared = true
	if len(owned.Objects) != 1 {
		t.Fatal("native backoff ownership requires one object")
	}
	name = owned.Objects[0]
	before := replicatedPGNative(t, ctx, name)
	route := before.Placement
	o = replicatedPGConnection(t, c, ctx, route)
	started = true
	var err error
	start, err = backoffControl(ctx, "start", name)
	if err != nil {
		t.Fatal("start inactive PG recipe", err)
	}
	if !start.Ready || start.Done || start.Restored || start.Before.Pool.Size != 1 || start.Before.Pool.MinSize != 1 || start.Inactive.Pool.Size != 2 || start.Inactive.Pool.MinSize != 2 || !strings.Contains(start.Inactive.PG.State, "peered") || strings.Contains(start.Inactive.PG.State, "active") {
		t.Fatal("native PG was not independently peered/inactive", start)
	}
	route = nativeBackoffRoute(t, route, start.Inactive)
	pendingCtx, pendingCancel := context.WithCancel(ctx)
	cancelPending = pendingCancel
	type completion struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan completion, 1)
	pending := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
	pending.ClientIncarnation = 71
	go func() { r, e := o.Request(pendingCtx, pending); done <- completion{r, e} }()
	eventCtx, stopEvent := context.WithTimeout(ctx, 30*time.Second)
	defer stopEvent()
	block, err = o.NextBackoff(eventCtx)
	if err != nil {
		select {
		case r := <-done:
			t.Fatal("no native BLOCK; original request completed", r.reply, r.err, err)
		default:
			t.Fatal("no native BLOCK", err)
		}
	}
	if block.Op != cephmsgr.OSDBackoffBlock || block.Pool != uint64(route.Pool) || block.Seed != route.PG || block.Shard != -1 || block.MapEpoch != start.Inactive.PG.Epoch || block.ID == 0 || block.Source.ID != route.ID || block.GlobalID != c.Snapshot().GlobalID || block.SessionGeneration == 0 || len(block.RawFront) == 0 {
		t.Fatal("native BLOCK identity/applied epoch differs", block, route)
	}
	if err := o.AckBackoff(ctx, block); err != nil {
		t.Fatal("explicit original-epoch ACK_BLOCK", err)
	}
	if err := o.AckBackoff(ctx, block); !errors.Is(err, cephmsgr.ErrOSDBackoffAcknowledged) {
		t.Fatal("duplicate BLOCK ACK admitted", err)
	}
	ackInput, err := json.Marshal(map[string]any{"global_id": block.GlobalID, "id": block.ID, "map_epoch": block.MapEpoch, "pgid": route.PGID})
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(ackInputPath, ackInput); err != nil {
		t.Fatal(err)
	}
	observed, err = backoffControl(ctx, "observe", name)
	if err != nil || observed.Token != start.Token || !observed.Ready || observed.Done {
		t.Fatal("native inactive scope lost while ACKing", observed, err)
	}
	nativeAck = observed.AckReceipt
	if nativeAck.Token != start.Token || nativeAck.GlobalID != block.GlobalID || nativeAck.SourceNumber != block.GlobalID || nativeAck.SourceType != 8 || nativeAck.ID != block.ID || nativeAck.MapEpoch != block.MapEpoch || nativeAck.PG != route.PGID || nativeAck.Op != 2 || nativeAck.OSD != route.ID || !nativeAck.ReachedPG || nativeAck.Source != "7f793731f1b39eb4f465e960113d2363c311b964" || nativeAck.Evidence != "native completed op history; reception only" {
		t.Fatal("native original-epoch ACK source/PG/id reception receipt differs", nativeAck, block)
	}
	tail := []byte{0, 255, 'e', 'x', 'p', 'l', 'i', 'c', 'i', 't'}
	blockedRequest := replicatedPGRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
	blockedRequest.ClientIncarnation = 73
	blockedRequest.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000950, Nanoseconds: 24680}
	blockedReply, err = o.Request(ctx, blockedRequest)
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.Is(err, cephmsgr.ErrOSDBackoff) || errors.As(err, &unknown) || blockedReply.Submission.Transaction == 0 || blockedReply.Submission.ClientIncarnation != 73 || blockedReply.Submission.MapEpoch != route.Epoch || !reflect.DeepEqual(blockedReply.Submission.Placement, blockedRequest.Placement) || blockedReply.Transaction != 0 || len(blockedReply.RawFront) != 0 || len(blockedReply.RawData) != 0 {
		t.Fatal("backoff refusal was not a known local unsent outcome", blockedReply, err)
	}
	pendingCancel()
	var original completion
	select {
	case original = <-done:
	case <-ctx.Done():
		t.Fatal("pending STAT cancellation was not released", ctx.Err())
	}
	pendingReply = original.reply
	if !errors.As(original.err, &unknown) || !errors.Is(original.err, context.Canceled) || pendingReply.Submission.Transaction == 0 || pendingReply.Submission.ClientIncarnation != 71 || !reflect.DeepEqual(pendingReply.Submission.Placement, pending.Placement) || pendingReply.Transaction != 0 || len(pendingReply.RawFront) != 0 {
		t.Fatal("previously sent STAT lost unknown outcome", pendingReply, original.err)
	}
	finish, err = backoffControl(ctx, "finish", name)
	if err != nil {
		t.Fatal("restore native PG before observing UNBLOCK", err)
	}
	restored = finish.Done && finish.Restored
	if !restored || finish.Token != start.Token || !reflect.DeepEqual(finish.Before.ObjectState, finish.After.ObjectState) {
		t.Fatal("native restoration changed object state or scope", finish)
	}
	releaseCtx, stopRelease := context.WithTimeout(ctx, 30*time.Second)
	defer stopRelease()
	unblock, err = o.NextBackoff(releaseCtx)
	if err != nil {
		t.Fatal("native UNBLOCK", err)
	}
	if unblock.Op != cephmsgr.OSDBackoffUnblock || !unblock.Matched || unblock.ID != block.ID || unblock.Pool != block.Pool || unblock.Seed != block.Seed || unblock.Shard != block.Shard || unblock.MapEpoch < block.MapEpoch || unblock.GlobalID != block.GlobalID || unblock.SessionGeneration != block.SessionGeneration || !bytes.Equal(unblock.Begin.Raw, block.Begin.Raw) || !bytes.Equal(unblock.End.Raw, block.End.Raw) {
		t.Fatal("native UNBLOCK lost matched control identity", unblock, block)
	}
	afterRestore := replicatedPGNative(t, ctx, name)
	if !reflect.DeepEqual(before.Actual, afterRestore.Actual) {
		t.Fatal("locally refused APPEND was executed or replayed during UNBLOCK", before, afterRestore)
	}
	route = afterRestore.Placement
	// This is the first admitted send of the explicit append, chosen by the
	// caller after observing UNBLOCK. It does not retry the canceled STAT.
	fresh := replicatedPGRequest(route, blockedRequest.Operations...)
	fresh.ClientIncarnation = 75
	fresh.ModificationTime = blockedRequest.ModificationTime
	newReply, err = o.Request(ctx, fresh)
	if err != nil {
		t.Fatal("caller first send after UNBLOCK", err)
	}
	checkReplicatedPGReply(t, newReply, route)
	final := replicatedPGNative(t, ctx, name)
	finalNative = final.Actual
	want := append(bytes.Clone(before.Actual.Data), tail...)
	if newReply.Transaction <= blockedReply.Submission.Transaction || newReply.Transaction <= pendingReply.Submission.Transaction || newReply.UserVersion != final.Actual.Version || newReply.Flags&cephmsgr.OSDReplyOnDisk == 0 || final.Actual.Version <= before.Actual.Version || !bytes.Equal(final.Actual.Data, want) || final.Actual.Seconds != fresh.ModificationTime.Seconds || final.Actual.Nanos != fresh.ModificationTime.Nanoseconds {
		t.Fatal("native exactly-one append after explicit caller admission differs", newReply, final)
	}
	if err := o.AckBackoff(ctx, block); !errors.Is(err, cephmsgr.ErrOSDBackoffIdentity) {
		t.Fatal("released control retained ACK authority", err)
	}
	t.Logf("native BLOCK/exact-source ACK reception/local unsent APPEND/UNBLOCK preserved PG=%s epochs=%d/%d IDs=%d; canceled STAT stayed unknown and native append executed once", route.PGID, block.MapEpoch, unblock.MapEpoch, block.ID)
}

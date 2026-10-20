package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The cluster is made full by lowering its ratios. A plain write, which a full
// primary holds without answering, is in flight when that primary is
// SIGKILLed: it ends as an unknown outcome and never executes. At the new
// primary FULL_TRY is still refused with -ENOSPC and FULL_FORCE writes. The
// ratios are restored before the killed OSD restarts (backfill would otherwise
// be held as too full).
func TestCephOSDFullFailoverIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || isReplicatedPGFixtureProfile() {
		// The shared restoration helper is legacy-v6 only.
		t.Skip("requires three legacy-v6 BlueStore replicas")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 480*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1)
	poolSeed(t, ctx, 1, "full-failover-seed", "", []byte("seeded before full"), nil)
	ratios := nativeFullRatios(t, ctx, nil)
	if ratios.Full <= 0 || len(ratios.FullOSDs) != 0 {
		t.Fatalf("cluster is not in a known non-full state: %+v", ratios)
	}
	restore := map[string]float64{"nearfull": ratios.Nearfull, "backfillfull": ratios.Backfillfull, "full": ratios.Full}
	// A down OSD cannot report that it is no longer full, so while one is
	// down only the pool flag is awaited (osds false).
	waitFlag := func(ctx context.Context, want, osds bool) fullRatios {
		deadline := time.Now().Add(180 * time.Second)
		for {
			r := nativeFullRatios(t, ctx, nil)
			if hasPoolFlag(r.OwnedFlags["1"], "full") == want && (!osds || (len(r.FullOSDs) > 0) == want) {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatalf("pool full flag never became %v: %+v", want, r)
			}
			time.Sleep(3 * time.Second)
		}
	}
	ratiosRestored, victim, restarted := false, "", false
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 300*time.Second)
		defer stop()
		if !ratiosRestored {
			nativeFullRatios(t, cleanup, restore)
		}
		if victim != "" && !restarted {
			// The restarted OSD must report itself non-full before recovery
			// to it can proceed.
			if err := testcluster.ControlDaemon(cleanup, dir, "start", "osd", victim); err != nil {
				t.Error("restart killed OSD", err)
				return
			}
			waitFlag(cleanup, false, true)
			recoverMutationFixture(t, c, base, original, victim, false)
		} else if !ratiosRestored {
			waitFlag(cleanup, false, true)
		}
	})
	nativeFullRatios(t, ctx, map[string]float64{"nearfull": 0.0005, "backfillfull": 0.0008, "full": 0.001})
	full := waitFlag(ctx, true, true)
	t.Logf("full at epoch %d: OSDs %v", full.Epoch, full.FullOSDs)

	request := func(route osdFixtureOracle, configure func(*cephmsgr.OSDRequest), data string) cephmsgr.OSDRequest {
		r := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: []byte(data)})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009000}
		if configure != nil {
			configure(&r)
		}
		return r
	}
	held := poolPlace(t, ctx, 1, "full-failover-held", "")
	o := mutationConnection(t, c, ctx, held)
	type outcome struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		call, stop := context.WithTimeout(ctx, 120*time.Second)
		defer stop()
		reply, err := o.Request(call, request(held, nil, "held while full"))
		done <- outcome{reply, err}
	}()
	select {
	case r := <-done:
		t.Fatal("a write to a full cluster was answered", r.reply.Code, r.err)
	case <-time.After(2 * time.Second):
	}
	victim = strconv.Itoa(int(held.ID))
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", victim); err != nil {
		t.Fatal("kill primary", err)
	}
	lost := <-done
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(lost.err, &unknown) {
		t.Fatal("the held write must end as an unknown outcome when its primary dies", lost.reply.Code, lost.err)
	}
	o.Close()
	moved := poolPlace(t, ctx, 1, "full-failover-held", "")
	for deadline := time.Now().Add(90 * time.Second); moved.ID == held.ID; moved = poolPlace(t, ctx, 1, "full-failover-held", "") {
		if time.Now().After(deadline) {
			t.Fatal("the killed primary was not replaced")
		}
		time.Sleep(2 * time.Second)
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	// Each object goes to its own current primary; the killed OSD is down.
	send := func(object string, configure func(*cephmsgr.OSDRequest), data string) (cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, 1, object, "")
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		call, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		return connections[route.ID].Request(call, request(route, configure, data))
	}
	_, err := send("full-failover-held", func(r *cephmsgr.OSDRequest) { r.FullTry = true }, "try")
	var server *cephmsgr.OSDError
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -28 {
		t.Fatal("FULL_TRY at the new primary must be a known -ENOSPC", err)
	}
	forced, err := send("full-failover-forced", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, "forced after failover")
	if err != nil || forced.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("FULL_FORCE at the new primary", err)
	}
	if state := poolObserve(t, ctx, 1, "full-failover-forced", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("forced after failover")) || state.Version != forced.UserVersion {
		t.Fatal("native object differs after the forced write", state.Code)
	}
	t.Logf("primary osd.%d -> osd.%d (epoch %d -> %d); held write ended unknown (%v); FULL_TRY -28, FULL_FORCE ONDISK", held.ID, moved.ID, held.Epoch, moved.Epoch, lost.err)

	for _, connection := range connections {
		connection.Close()
	}
	clear(connections)
	nativeFullRatios(t, ctx, restore)
	ratiosRestored = true
	waitFlag(ctx, false, false)
	if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", victim); err != nil {
		t.Fatal("restart killed OSD", err)
	}
	restarted = true
	waitFlag(ctx, false, true)
	recoverMutationFixture(t, c, base, original, victim, false)
	time.Sleep(2 * time.Second)
	if state := poolObserve(t, ctx, 1, "full-failover-held", "", ""); state.Code != -2 {
		t.Fatal("the write held by the killed primary executed", state.Code, state.Data)
	}
	if state := poolObserve(t, ctx, 1, "full-failover-forced", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("forced after failover")) || state.Version != forced.UserVersion {
		t.Fatal("native forced object differs after recovery", state.Code, state.Version)
	}
	restored := nativeFullRatios(t, ctx, nil)
	if restored.Nearfull != ratios.Nearfull || restored.Backfillfull != ratios.Backfillfull || restored.Full != ratios.Full || len(restored.FullOSDs) != 0 || hasPoolFlag(restored.OwnedFlags["1"], "full") {
		t.Fatal("native full ratios or flags were not restored", restored)
	}
	receipt, err := json.Marshal(map[string]any{
		"original_ratios": ratios, "restored_ratios": restored,
		"original_primary": held.ID, "replacement_primary": moved.ID,
		"original_epoch": held.Epoch, "replacement_epoch": moved.Epoch,
		"held_outcome_unknown": true, "held_native_code_after_recovery": -2,
		"full_try_code": -28, "forced_ondisk": true,
		"forced_native_version_after_recovery": forced.UserVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-full-failover-results.json"), receipt); err != nil {
		t.Fatal(err)
	}
	t.Logf("native recovery complete: restored ratios %.4f/%.4f/%.4f at epoch %d; held object absent; forced bytes/version %d preserved", restored.Nearfull, restored.Backfillfull, restored.Full, restored.Epoch, forced.UserVersion)
}

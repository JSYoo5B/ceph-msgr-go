package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A pool over its object quota is flagged full_quota. Writes without a full
// flag are never answered; FULL_TRY gets the server's -EDQUOT with no effect;
// FULL_FORCE proceeds. Native observation checks every outcome independently.
func TestCephOSDFullPoolFlagsIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	for _, name := range []string{"full-a", "full-b", "full-c"} {
		poolSeed(t, ctx, 1, name, "", []byte("seed-"+name), nil)
	}
	nativePool(t, ctx, "quota", map[string]any{"slot": 1, "max_objects": 2})
	deadline := time.Now().Add(240 * time.Second)
	for {
		info := nativePool(t, ctx, "poolinfo", map[string]any{"slot": 1})
		if strings.Contains(info.FlagNames, "full_quota") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool never reached full_quota", info.FlagNames)
		}
		time.Sleep(3 * time.Second)
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	send := func(timeout time.Duration, object string, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, 1, object, "")
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		r := pools.request(route, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000800}
		if configure != nil {
			configure(&r)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		return connections[route.ID].Request(callCtx, r)
	}
	write := func(data string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: []byte(data)}
	}
	t.Run("plain write is never answered", func(t *testing.T) {
		_, err := send(5*time.Second, "full-plain", nil, write("plain"))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("a write to a full pool must end as an unknown outcome, not a reply", err)
		}
		if state := poolObserve(t, ctx, 1, "full-plain", "", ""); state.Code != -2 {
			t.Fatal("the dropped write created an object", state.Code)
		}
	})
	t.Run("FULL_TRY gets the quota refusal", func(t *testing.T) {
		_, err := send(20*time.Second, "full-try", func(r *cephmsgr.OSDRequest) { r.FullTry = true }, write("try"))
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -122 {
			t.Fatal("expected a known -EDQUOT", err)
		}
		if state := poolObserve(t, ctx, 1, "full-try", "", ""); state.Code != -2 {
			t.Fatal("the refused write created an object", state.Code)
		}
	})
	t.Run("FULL_FORCE writes and deletes despite the flag", func(t *testing.T) {
		reply, err := send(20*time.Second, "full-force", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, write("forced"))
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("forced write", err)
		}
		if state := poolObserve(t, ctx, 1, "full-force", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("forced")) || reply.UserVersion != state.Version {
			t.Fatal("native object differs after the forced write", state.Code)
		}
		reply, err = send(20*time.Second, "full-a", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("forced delete", err)
		}
		if state := poolObserve(t, ctx, 1, "full-a", "", ""); state.Code != -2 {
			t.Fatal("forced delete left the object", state.Code)
		}
	})
	t.Run("misuse is refused before submission", func(t *testing.T) {
		route := poolPlace(t, ctx, 1, "full-b", "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		for _, edit := range []func(*cephmsgr.OSDRequest){
			func(r *cephmsgr.OSDRequest) { r.FullTry, r.FullForce = true, true },
		} {
			r := pools.request(route, write("x"))
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000801}
			edit(&r)
			if reply, err := o.Request(ctx, r); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
				t.Fatal("conflicting full flags were submitted", err)
			}
		}
		read := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 4})
		read.FullTry = true
		if reply, err := o.Request(ctx, read); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
			t.Fatal("a read vector carried a full flag", err)
		}
	})
}

type fullRatios struct {
	Nearfull, Backfillfull, Full float64
	Epoch                        uint32
	FullOSDs                     []int32
	OwnedFlags                   map[string]string
}

func nativeFullRatios(t *testing.T, ctx context.Context, set map[string]float64) fullRatios {
	t.Helper()
	input := map[string]any{}
	if set != nil {
		input["set"] = set
	}
	r := nativePool(t, ctx, "fullratios", input)
	return fullRatios{r.NearfullRatio, r.BackfillfullRatio, r.FullRatio, r.Epoch, r.FullOSDs, r.OwnedFlags}
}

func hasPoolFlag(names, flag string) bool {
	for _, name := range strings.Split(names, ",") {
		if name == flag {
			return true
		}
	}
	return false
}

// Lowering the cluster's full ratios below the OSDs' actual use makes the
// OSDs report full and the MON flag every pool full (not full_quota): a plain
// write is never answered, FULL_TRY gets -ENOSPC, FULL_FORCE and reads
// proceed, and after the ratios are restored and the flag clears a plain
// write is answered again. The ratios are restored even on failure.
func TestCephOSDFullRatioIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("OSD fullness is measured on the BlueStore fixture")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	poolSeed(t, ctx, 1, "ratio-seed", "", []byte("seeded before full"), nil)
	original := nativeFullRatios(t, ctx, nil)
	if original.Full <= 0 || len(original.FullOSDs) != 0 {
		t.Fatalf("cluster is not in a known non-full state: %+v", original)
	}
	restore := map[string]float64{"nearfull": original.Nearfull, "backfillfull": original.Backfillfull, "full": original.Full}
	waitFlag := func(ctx context.Context, want bool) fullRatios {
		deadline := time.Now().Add(180 * time.Second)
		for {
			r := nativeFullRatios(t, ctx, nil)
			if hasPoolFlag(r.OwnedFlags["1"], "full") == want && (len(r.FullOSDs) > 0) == want {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatalf("pool full flag never became %v: %+v", want, r)
			}
			time.Sleep(3 * time.Second)
		}
	}
	restored := false
	t.Cleanup(func() {
		if restored {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 200*time.Second)
		defer stop()
		nativeFullRatios(t, cleanup, restore)
		waitFlag(cleanup, false)
	})
	nativeFullRatios(t, ctx, map[string]float64{"nearfull": 0.0005, "backfillfull": 0.0008, "full": 0.001})
	full := waitFlag(ctx, true)
	t.Logf("full at epoch %d: OSDs %v, owned pool flags %q", full.Epoch, full.FullOSDs, full.OwnedFlags["1"])
	if hasPoolFlag(full.OwnedFlags["1"], "full_quota") {
		t.Fatal("OSD fullness was reported as a quota")
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	send := func(timeout time.Duration, object string, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, 1, object, "")
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		r := pools.request(route, ops...)
		if ops[0].Code != cephmsgr.OSDRead {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000900}
		}
		if configure != nil {
			configure(&r)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		return connections[route.ID].Request(callCtx, r)
	}
	write := func(data string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: []byte(data)}
	}

	_, err := send(5*time.Second, "ratio-plain", nil, write("plain"))
	var unknown *cephmsgr.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatal("a write to a full cluster must end as an unknown outcome, not a reply", err)
	}
	if state := poolObserve(t, ctx, 1, "ratio-plain", "", ""); state.Code != -2 {
		t.Fatal("the dropped write created an object", state.Code)
	}
	_, err = send(20*time.Second, "ratio-try", func(r *cephmsgr.OSDRequest) { r.FullTry = true }, write("try"))
	var server *cephmsgr.OSDError
	if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -28 {
		t.Fatal("expected a known -ENOSPC", err)
	}
	if state := poolObserve(t, ctx, 1, "ratio-try", "", ""); state.Code != -2 {
		t.Fatal("the refused write created an object", state.Code)
	}
	reply, err := send(20*time.Second, "ratio-force", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, write("forced"))
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("forced write", err)
	}
	if state := poolObserve(t, ctx, 1, "ratio-force", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("forced")) || state.Version != reply.UserVersion {
		t.Fatal("native object differs after the forced write", state.Code)
	}
	read, err := send(20*time.Second, "ratio-seed", nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 64})
	if err != nil || !bytes.Equal(read.Operations[0].Data, []byte("seeded before full")) {
		t.Fatal("read from a full cluster", err)
	}

	nativeFullRatios(t, ctx, restore)
	cleared := waitFlag(ctx, false)
	restored = true
	if cleared.Full != original.Full || cleared.Nearfull != original.Nearfull || cleared.Backfillfull != original.Backfillfull {
		t.Fatalf("ratios were not restored: %+v vs %+v", cleared, original)
	}
	reply, err = send(20*time.Second, "ratio-after", nil, write("after"))
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("plain write after the full flag cleared", err)
	}
	if state := poolObserve(t, ctx, 1, "ratio-after", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("after")) {
		t.Fatal("native object differs after the flag cleared", state.Code)
	}
	t.Logf("full flag cleared at epoch %d", cleared.Epoch)
}

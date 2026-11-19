package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

//scenario:row osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-manifesttierevictlostreply key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6
//fixture:isolated owned-pool cleanup leaves central config mon/mon_allow_pool_delete=true set
func TestCephManifestTierEvictLostReplyIntegration(t *testing.T) {
	const source, target = "tier-evict-lost-reply", "tier-evict-chunk-target"
	const length = 16
	setupClient, _, base, _ := osdFixture(t)
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
	defer stop()
	opts := integrationOptions(t)
	if opts.ConnectionMode != cephmsgr.CRCMode {
		t.Fatal("exact transaction observation requires the CRC fixture")
	}
	pools := createOwnedPools(t, ctx, base, 1, 2)
	tierGoBind(t, ctx, pools)
	seed, targetData := []byte("local-chunk-data:keep this suffix"), []byte("remote-chunk-src:target tail")
	poolSeed(t, ctx, 1, source, "", seed, nil)
	poolSeed(t, ctx, 2, target, "", targetData, nil)
	route := poolPlace(t, ctx, 1, source, "")
	setup := mutationConnection(t, setupClient, ctx, route)
	defer setup.Close()
	apply := func(op cephmsgr.OSDOperation) {
		t.Helper()
		req := pools.request(route, op)
		req.ModificationTime = cephmsgr.OSDTime{Seconds: tierGoMTime}
		if op.Code == cephmsgr.OSDSetChunk {
			req.ModificationTime = cephmsgr.OSDTime{}
		}
		r, err := setup.Request(ctx, req)
		if err != nil || r.Code != 0 || len(r.Operations) != 1 || r.Operations[0].Result != 0 || r.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatalf("owned manifest operation %#x failed: %v, code %d", op.Code, err, r.Code)
		}
	}
	apply(cephmsgr.OSDOperation{Code: cephmsgr.OSDSetChunk, Flags: cephmsgr.OSDWithReference,
		Chunk: cephmsgr.OSDChunkInput{SourceLength: length, TargetPool: pools.ids[2], TargetName: target}})
	apply(cephmsgr.OSDOperation{Code: cephmsgr.OSDUndirty})
	before := tierGoObserve(t, ctx, source)
	chunk := poolObserve(t, ctx, 2, target, "", "")
	refs, ok := setChunkGoRefsAttr(chunk)
	wantRefs := setChunkGoRefs(pools.ids[1], setChunkGoRef{source, route.Hash})
	if !tierGoHolds(before, seed, false, 0) || before.Version == 0 || chunk.Code != 0 || !bytes.Equal(chunk.Data, targetData) || !ok || !bytes.Equal(refs, wantRefs) {
		t.Fatal("the owned clean manifest and its single reference were not established")
	}

	// Only the subject uses this gate. Its only OSD request addresses source;
	// its object name in the request front marks the transfer, including any
	// later attempt on another OSD connection opened by this client.
	gate := testcluster.NewOSDReplyGate(true, 4096)
	record := map[string]any{"source_commit": mdsFreshSource, "profile": "legacy-v6", "opcode": cephmsgr.OSDTierEvict,
		"pool_ids": pools.ids, "before": before, "chunk_before": chunk}
	defer func() {
		record["gate"] = gate.State()
		data, err := json.Marshal(record)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-tier-evict-lost-reply.json"), data)
		}
		if err != nil {
			t.Error("write tier evict receipt", err)
		}
	}()
	dial := opts.DialContext
	opts.EnableOSD = true
	opts.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		_, kind, _ := testcluster.SplitPeer(addr)
		if err == nil && strings.HasPrefix(kind, "osd.") {
			conn = gate.Wrap(conn)
		}
		return conn, err
	}
	client, err := cephmsgr.Dial(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	handle := mutationConnection(t, client, ctx, route)
	defer handle.Close()
	if err := gate.Arm([]byte(source)); err != nil {
		t.Fatal(err)
	}
	request := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDTierEvict})
	request.IgnoreCache, request.IgnoreOverlay = true, true
	request.ClientIncarnation = 71
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type callResult struct {
		reply cephmsgr.OSDReply
		err   error
	}
	done := make(chan callResult, 1)
	go func() {
		reply, err := handle.Request(callCtx, request)
		done <- callResult{reply, err}
	}()
	select {
	case <-gate.Held():
	case got := <-done:
		t.Fatal("tier evict returned before its reply was withheld", got.err)
	case <-ctx.Done():
		t.Fatal("tier evict reply was never withheld", ctx.Err())
	}
	checkState := func(label string) {
		t.Helper()
		after := tierGoObserve(t, ctx, source, 0, length)
		current := poolObserve(t, ctx, 2, target, "", "")
		record[label] = map[string]any{"local": after, "chunk": current}
		if !tierGoHolds(after, tierGoEvicted(seed, length), false, before.Version) ||
			after.Seconds != before.Seconds || after.Nanos != before.Nanos ||
			!tierGoProxied(after, targetData[:length]) || !setChunkGoSame(current, chunk) {
			t.Fatal("evicted extent, proxied bytes, timestamps or target reference differ", label)
		}
	}
	checkState("while_reply_held")
	cancel()
	var got callResult
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("tier evict caller did not return after cancellation")
	}
	s := got.reply.Submission
	var unknown *cephmsgr.OutcomeUnknownError
	record["submission"], record["outcome_unknown"], record["canceled"] = s, errors.As(got.err, &unknown), errors.Is(got.err, context.Canceled)
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || s.Transaction == 0 ||
		s.GlobalID == 0 || s.GlobalID != client.Snapshot().GlobalID || s.ClientIncarnation != request.ClientIncarnation ||
		s.MapEpoch != route.Epoch || s.Attempt != 0 || got.reply.Transaction != 0 || len(got.reply.Operations) != 0 {
		t.Fatal("uncertain tier evict lost its submission identity or exposed a reply", s, got.err)
	}
	if err := gate.CloseHeld(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	time.Sleep(500 * time.Millisecond)
	checkState("after_cancellation")
	record["post_cancel_observation_ns"] = time.Since(start).Nanoseconds()
	state := gate.State()
	if state.Error != "" || !state.Decoded || !state.Bound || state.Transaction == nil || *state.Transaction != s.Transaction ||
		state.Held == nil || state.Held.Connection != state.BoundConnection || state.Held.Transaction != s.Transaction ||
		state.MarkedRequests == nil || *state.MarkedRequests != 1 || len(state.MarkedConnections) != 1 || state.MarkedConnections[0] != state.BoundConnection {
		t.Fatal("withheld tier evict reply or marked request count differs", state)
	}

	apply(cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
	if after := tierGoObserve(t, ctx, source); after.Code != -2 {
		t.Fatal("deleted manifest still present", after.Code)
	}
	for i := 1; i <= tierGoPolls; i++ {
		after := poolObserve(t, ctx, 2, target, "", "")
		if after.Code == -2 {
			record["source_absent"], record["last_reference_released"], record["release_polls"] = true, true, i
			t.Logf("tier evict tid=%d held connection=%d marked=%d; released target after %d observations", s.Transaction, state.BoundConnection, *state.MarkedRequests, i)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("deleting the manifest did not release the target's last reference")
}

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func monMapEpoch(batch cephmsgr.OSDMapBatch) uint32 {
	var epoch uint32
	for _, blobs := range [][]cephmsgr.OSDMapBlob{batch.FullMaps, batch.IncrementalMaps} {
		for _, blob := range blobs {
			epoch = max(epoch, blob.Epoch)
		}
	}
	return epoch
}

func verifyNativeMonMaps(t *testing.T, ctx context.Context, control string, batches ...cephmsgr.OSDMapBatch) {
	t.Helper()
	var records []map[string]any
	var fsid string
	seen := make(map[string]bool)
	for _, batch := range batches {
		if fsid != "" && fsid != batch.FSID {
			t.Fatal("map FSID changed")
		}
		fsid = batch.FSID
		for _, set := range []struct {
			kind  string
			blobs []cephmsgr.OSDMapBlob
		}{{"full", batch.FullMaps}, {"incremental", batch.IncrementalMaps}} {
			for _, blob := range set.blobs {
				name := fmt.Sprintf("osd-map-%s-%d.bin", set.kind, blob.Epoch)
				if seen[name] {
					continue
				}
				seen[name] = true
				if err := os.WriteFile(filepath.Join(control, name), blob.Data, 0600); err != nil {
					t.Fatal(err)
				}
				records = append(records, map[string]any{"kind": set.kind, "epoch": blob.Epoch})
			}
		}
	}
	manifest, err := json.Marshal(map[string]any{"fsid": fsid, "source": "mon", "blobs": records})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "osd-map-manifest.json"), manifest, 0644); err != nil {
		t.Fatal(err)
	}
	verify, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	if err := testcluster.ControlDaemon(verify, control, "verify", "osd-maps", "received"); err != nil {
		t.Fatal("native MON map decoder and negotiated-feature encoder", err)
	}
}

func changeMonMapOnce(t *testing.T, ctx context.Context, c *cephmsgr.Client, set bool) uint32 {
	t.Helper()
	prefix := "osd unset"
	if set {
		prefix = "osd set"
	}
	command, _ := json.Marshal(map[string]string{"prefix": prefix, "key": "noout"})
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: command}); err != nil {
		t.Fatal("once-only disposable map update", err)
	}
	result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	var current struct {
		Epoch uint32 `json:"epoch"`
	}
	if err != nil || json.Unmarshal(result.Data, &current) != nil || current.Epoch == 0 {
		t.Fatal("read confirmed native map epoch", err)
	}
	return current.Epoch
}

func waitMonOSDEpoch(t *testing.T, ctx context.Context, s *cephmsgr.OSDMapStream, epoch uint32) cephmsgr.OSDMapBatch {
	t.Helper()
	for {
		batch, err := s.Next(ctx)
		if err != nil {
			t.Fatal("wait MON map epoch", epoch, err)
		}
		if monMapEpoch(batch) >= epoch {
			return batch
		}
	}
}

func TestCephMonOSDMapsIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	c, ctx := fixtureClient(t, 60*time.Second)
	initial := c.Snapshot()
	latest, err := c.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{})
	if err != nil || latest.FSID != initial.FSID || len(latest.FullMaps) != 1 || len(latest.IncrementalMaps) != 0 {
		t.Fatal("native latest full map", err, len(latest.FullMaps))
	}
	epoch := latest.FullMaps[0].Epoch
	full, err := c.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{FullFirst: epoch, FullLast: epoch})
	if err != nil || len(full.FullMaps) != 1 || full.FullMaps[0].Epoch != epoch || !bytes.Equal(full.FullMaps[0].Data, latest.FullMaps[0].Data) {
		t.Fatal("native fixed-epoch full request", err)
	}
	stream, err := c.WatchOSDMaps(ctx, cephmsgr.OSDMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	baseline := waitMonOSDEpoch(t, ctx, stream, epoch)
	if len(baseline.FullMaps) != 1 {
		t.Fatal("continuous zero cursor did not provide full baseline")
	}
	newEpoch := changeMonMapOnce(t, ctx, c, true)
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		changeMonMapOnce(t, restore, c, false)
	}()
	publication := waitMonOSDEpoch(t, ctx, stream, newEpoch)
	incremental, err := c.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{IncrementalFirst: newEpoch, IncrementalLast: newEpoch})
	if err != nil || len(incremental.IncrementalMaps) != 1 || incremental.IncrementalMaps[0].Epoch != newEpoch || len(incremental.FullMaps) != 0 {
		t.Fatal("native fixed-epoch incremental request", err)
	}
	// This live watch and two concurrent private requests must not steal each
	// other's uncorrelated replies or change the main CephX identity.
	type received struct {
		batch cephmsgr.OSDMapBatch
		err   error
	}
	results := make(chan received, 2)
	for _, request := range []cephmsgr.OSDMapRequest{{FullFirst: newEpoch, FullLast: newEpoch}, {IncrementalFirst: newEpoch, IncrementalLast: newEpoch}} {
		go func(r cephmsgr.OSDMapRequest) { b, e := c.RequestOSDMaps(ctx, r); results <- received{b, e} }(request)
	}
	var fullSeen, incSeen bool
	for range 2 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		fullSeen = fullSeen || len(got.batch.FullMaps) == 1
		incSeen = incSeen || len(got.batch.IncrementalMaps) == 1
	}
	if !fullSeen || !incSeen {
		t.Fatal("concurrent range replies crossed")
	}
	empty, err := c.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{FullFirst: math.MaxUint32, FullLast: math.MaxUint32})
	if err != nil || len(empty.FullMaps)+len(empty.IncrementalMaps) != 0 || empty.NewestMap < newEpoch {
		t.Fatal("future epoch did not return empty native envelope", err)
	}
	if empty.OldestMap > 1 {
		// The opt-in OSD fixture has independently trimmed early history. A
		// direct range request reports absence, unlike subscription fallback.
		trimmed, err := c.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{FullFirst: 1, FullLast: 1})
		if err != nil || len(trimmed.FullMaps)+len(trimmed.IncrementalMaps) != 0 || trimmed.OldestMap <= 1 {
			t.Fatal("trimmed exact range manufactured a full fallback", err)
		}
		t.Logf("trimmed epoch 1 returned an empty envelope with native retained boundary %d", trimmed.OldestMap)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := stream.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("local Next cancellation", err)
	}
	state := c.Snapshot()
	if state.GlobalID != initial.GlobalID || state.Manager.Ready || !state.OSDTicket.Expires.IsZero() {
		t.Fatal("MON maps changed shared identity or opened data service", state)
	}
	verifyNativeMonMaps(t, ctx, control, latest, full, incremental, publication)
	t.Logf("MON latest full, exact full/incremental ranges, future empty reply, concurrent private requests and live subscription verified at epoch %d without OSD ticket/connection", newEpoch)
}

func TestCephMonOSDMapsRecoveryIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	fault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fault.cleanupAfterClientClose(t, c)
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	stream, err := c.WatchOSDMaps(ctx, cephmsgr.OSDMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	initial, err := stream.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	previous := c.Snapshot()
	var batches = []cephmsgr.OSDMapBatch{initial}
	for cycle := 1; cycle <= 2; cycle++ {
		deadline := previous.AuthTicket.Expires
		renewal, stop := context.WithDeadline(ctx, deadline)
		updated := waitClientState(t, c, renewal, func(s cephmsgr.State) bool {
			return s.Monitor.Ready && s.AuthTicket.Expires.After(previous.AuthTicket.Expires)
		})
		stop()
		if !time.Now().Before(deadline) || updated.GlobalID != previous.GlobalID {
			t.Fatal("map watch renewal crossed expiry or changed identity")
		}
		epoch := changeMonMapOnce(t, ctx, admin, cycle == 1)
		batches = append(batches, waitMonOSDEpoch(t, ctx, stream, epoch))
		previous = updated
	}
	if err := fault.interrupt(); err != nil {
		t.Fatal(err)
	}
	recovered := waitClientState(t, c, ctx, func(s cephmsgr.State) bool { return s.Monitor.Ready && s.Monitor.Endpoint != previous.Monitor.Endpoint })
	epoch := changeMonMapOnce(t, ctx, admin, true)
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		changeMonMapOnce(t, restore, admin, false)
	}()
	batches = append(batches, waitMonOSDEpoch(t, ctx, stream, epoch))
	if recovered.GlobalID != previous.GlobalID || recovered.FSID != initial.FSID || recovered.Manager.Ready || !recovered.OSDTicket.Expires.IsZero() {
		t.Fatal("map recovery changed identity or opened data connection")
	}
	verifyNativeMonMaps(t, ctx, control, batches...)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("MON command after watch close", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	fault.assertHeld(t)
	t.Logf("raw map FIFO survived two pre-expiry renewals and sole-seed TCP loss, recovered through %s; all blobs matched native map encoder", recovered.Monitor.Endpoint)
}

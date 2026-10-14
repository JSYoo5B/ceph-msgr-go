package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephOSDMapDeliveryIntegration(t *testing.T) {
	c, ctx, oracle, data := osdFixture(t)
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd set","key":"noout"}`)}); err != nil {
		t.Fatal("advance disposable map epoch", err)
	}
	t.Cleanup(func() {
		restore, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.MonCommand(restore, cephmsgr.Command{JSON: []byte(`{"prefix":"osd unset","key":"noout"}`)}); err != nil {
			t.Error("restore disposable map option", err)
		}
	})
	if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-epoch", "advance "+strconv.Itoa(int(oracle.ID))); err != nil {
		t.Fatal("native OSD applied epoch", err)
	}
	encoded, err := os.ReadFile(filepath.Join(directory, "osd-map-epoch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var current struct {
		Epoch uint32 `json:"epoch"`
		FSID  string `json:"fsid"`
	}
	if err := json.Unmarshal(encoded, &current); err != nil || current.Epoch <= oracle.Epoch {
		t.Fatal("native map epoch did not advance", err)
	}
	encoded, err = os.ReadFile(filepath.Join(directory, "osd-map-applied.json"))
	if err != nil {
		t.Fatal(err)
	}
	var applied struct {
		Who    *int32 `json:"whoami"`
		FSID   string `json:"cluster_fsid"`
		Newest string `json:"newest_map"`
		Trim   uint32 `json:"cluster_osdmap_trim_lower_bound"`
	}
	if err := json.Unmarshal(encoded, &applied); err != nil {
		t.Fatal(err)
	}
	newest, err := strconv.ParseUint(applied.Newest, 10, 32)
	if err != nil || applied.Who == nil || *applied.Who != oracle.ID || applied.FSID != current.FSID || current.FSID == "" || newest < uint64(current.Epoch) || applied.Trim < 2 || uint64(applied.Trim) > newest {
		t.Fatalf("native selected OSD applied-map receipt: target=%d who=%v fsid=%s newest=%s trim=%d error=%v", oracle.ID, applied.Who, applied.FSID, applied.Newest, applied.Trim, err)
	}
	target := cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address}
	var blobs []map[string]any
	for phase, epoch := range []uint32{oracle.Epoch, 1} {
		phaseName := []string{"incremental", "full"}[phase]
		// Fresh sessions are necessary: OSD projected_epoch suppresses maps it
		// already sent, regardless of a later request's older epoch.
		o, err := c.OpenOSD(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		defer o.Close()
		request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 127})
		request.MapEpoch = epoch
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		mapContext, cancelMap := context.WithTimeout(ctx, 5*time.Second)
		defer cancelMap()
		completed := make(chan error, 1)
		go func() {
			reply, err := o.Request(call, request)
			if err == nil && !bytes.Equal(reply.Operations[0].Data, data[:127]) {
				err = fmt.Errorf("stale read bytes differ from native object")
			}
			completed <- err
		}()
		mapResult := make(chan osdMapWaitResult, 1)
		go func() { batch, err := o.NextMap(mapContext); mapResult <- osdMapWaitResult{batch, err} }()
		batch, readError, err := waitOSDMapAndRead(mapContext, completed, mapResult, phase == 1)
		cancelMap()
		if err != nil {
			cancel()
			t.Fatalf("native OSD map delivery: phase=%s target=%d request-epoch=%d newest=%d trim=%d read=%v error=%v", phaseName, oracle.ID, epoch, newest, applied.Trim, readError, err)
		}
		if batch.FSID != current.FSID || batch.Version != 1 || batch.CompatVersion != 1 || batch.OldestMap != 0 || batch.NewestMap != 0 {
			cancel()
			t.Fatal("negotiated native map envelope metadata", batch.Version, batch.CompatVersion)
		}
		if phase == 0 && len(batch.IncrementalMaps) == 0 || phase == 1 && len(batch.FullMaps) == 0 {
			cancel()
			t.Fatalf("native map history: phase=%d request epoch=%d trim=%d full=%d incremental=%d", phase, epoch, applied.Trim, len(batch.FullMaps), len(batch.IncrementalMaps))
		}
		for _, set := range []struct {
			kind string
			maps []cephmsgr.OSDMapBlob
		}{{"full", batch.FullMaps}, {"incremental", batch.IncrementalMaps}} {
			for _, blob := range set.maps {
				if blob.Epoch == 0 || len(blob.Data) == 0 {
					t.Fatal("invalid map blob epoch/bytes")
				}
				name := fmt.Sprintf("osd-map-%s-%d.bin", set.kind, blob.Epoch)
				if err := os.WriteFile(filepath.Join(directory, name), blob.Data, 0644); err != nil {
					t.Fatal(err)
				}
				blobs = append(blobs, map[string]any{"kind": set.kind, "epoch": blob.Epoch})
			}
		}
		cancel()
		if phase == 0 && readError != nil {
			t.Fatal("stale read was not completed", readError)
		}
		// Ancient placement may be refused or dropped by the server. Map receipt
		// never makes that request successful or causes an automatic replay.
		request.MapEpoch = current.Epoch
		if reply, err := o.Request(ctx, request); err != nil || !bytes.Equal(reply.Operations[0].Data, data[:127]) {
			t.Fatal("explicit current-epoch read after map delivery", err)
		}
		if err := o.Close(); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := json.Marshal(map[string]any{"fsid": current.FSID, "blobs": blobs})
	if err != nil {
		t.Fatal(err)
	}
	// Non-secret fixture evidence must also be readable by the host runner
	// when this test binary runs as root inside its disposable container.
	if err := os.WriteFile(filepath.Join(directory, "osd-map-manifest.json"), manifest, 0644); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-maps", "received"); err != nil {
		t.Fatal("independent native map decoder and negotiated-feature encoding", err)
	}
	t.Logf("received full/incremental native OSD maps (%d blobs), matched native decoder/encoder; explicit epoch reads preserved object bytes", len(blobs))
}

type osdMapWaitResult struct {
	batch cephmsgr.OSDMapBatch
	err   error
}

func waitOSDMapAndRead(ctx context.Context, read <-chan error, maps <-chan osdMapWaitResult, ancient bool) (cephmsgr.OSDMapBatch, error, error) {
	var batch cephmsgr.OSDMapBatch
	var readError, mapError error
	for read != nil || maps != nil {
		select {
		case <-ctx.Done():
			// Results already completed at the boundary still carry independent
			// causes, even if cancellation wins the outer select first.
			select {
			case err := <-read:
				readError = err
			default:
			}
			select {
			case result := <-maps:
				batch, mapError = result.batch, result.err
			default:
			}
			return batch, readError, errors.Join(ctx.Err(), mapError, readError)
		case err := <-read:
			readError, read = err, nil
			if err != nil && !ancient {
				// Report this RPC immediately, but retain a map result that is
				// already available regardless of which ready channel won select.
				select {
				case result := <-maps:
					batch, mapError = result.batch, result.err
				default:
				}
				return batch, err, errors.Join(fmt.Errorf("stale read failed: %w", err), mapError)
			}
		case result := <-maps:
			batch, mapError, maps = result.batch, result.err, nil
			// Keep observing the bounded RPC when map delivery fails first.
			// Returning here would discard an already buffered RPC refusal.
		}
	}
	if mapError != nil {
		return batch, readError, errors.Join(mapError, readError)
	}
	return batch, readError, nil
}

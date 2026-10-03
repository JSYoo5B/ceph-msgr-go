package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-epoch", "advance"); err != nil {
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
		Trim uint32 `json:"cluster_osdmap_trim_lower_bound"`
	}
	if err := json.Unmarshal(encoded, &applied); err != nil {
		t.Fatal(err)
	}
	if applied.Trim < 2 {
		t.Fatal("native retained full-map trim boundary", applied.Trim)
	}
	target := cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address}
	var blobs []map[string]any
	for phase, epoch := range []uint32{oracle.Epoch, 1} {
		// Fresh sessions are necessary: OSD projected_epoch suppresses maps it
		// already sent, regardless of a later request's older epoch.
		o, err := c.OpenOSD(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		request := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 127})
		request.MapEpoch = epoch
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		completed := make(chan error, 1)
		go func() {
			reply, err := o.Request(call, request)
			if err == nil && !bytes.Equal(reply.Operations[0].Data, data[:127]) {
				err = fmt.Errorf("stale read bytes differ from native object")
			}
			completed <- err
		}()
		batch, err := o.NextMap(ctx)
		if err != nil {
			cancel()
			t.Fatal("native OSD map delivery", err)
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
				if err := os.WriteFile(filepath.Join(directory, name), blob.Data, 0600); err != nil {
					t.Fatal(err)
				}
				blobs = append(blobs, map[string]any{"kind": set.kind, "epoch": blob.Epoch})
			}
		}
		readError := <-completed
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

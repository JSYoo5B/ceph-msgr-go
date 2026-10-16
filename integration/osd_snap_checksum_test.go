package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// Native setup and read-snap selection belong only to this disposable test.
// Each checksum is compared directly with librados at the same read snap;
// the product neither discovers snapshots nor computes local expected hashes.
func TestCephOSDSnapshotChecksumIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
		}
	})
	baseline := []byte{0, 255, 'u', 'n', 'c', 'h', 'a', 'n', 'g', 'e', 'd', 0}
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 3})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 4 {
		t.Fatal("native checksum snapshot ownership missing")
	}
	y, x, z, w := owned.Objects[0], owned.Objects[1], owned.Objects[2], owned.Objects[3]
	snaps := nativeSnapCreate(t, ctx, base, 2)
	s1, s2 := snaps[0], snaps[1]
	write := func(object string, snapc snapOracleContext, actions ...snapOracleAction) {
		t.Helper()
		r := nativeSnapWrite(t, ctx, base, object, nil, snapc, false, omapWriteMTime{Seconds: 1760200400, Nanos: 123}, actions...)
		if r.SnapContext == nil || r.SnapContext.Code != 0 || r.Aggregate == nil || *r.Aggregate != 0 {
			t.Fatal("native snapshot checksum setup write failed", r)
		}
	}
	oldX := []byte{0, 255, 1, 128, 0, 0, 0, 0, 't', 'a', 'i', 'l'}
	oldW := []byte("deleted-tail")
	newX := []byte("new-head-0123456789ab")
	newZ := []byte("new-only")
	write(x, snapOracleContext{}, snapWrite(0, oldX[:4]), snapWrite(8, oldX[8:]))
	write(w, snapOracleContext{}, snapWriteFull(oldW))
	c1 := snapOracleContext{Seq: s1, Snaps: []uint64{s1}}
	write(x, c1, snapWriteFull(newX))
	write(z, c1, snapWriteFull(newZ))
	write(w, c1, snapDelete())
	for _, object := range []string{x, y, z, w} {
		listing := nativeListSnaps(t, ctx, base, object)
		t.Logf("native listsnaps %s: %s", object, listing.JSON)
	}
	routes := map[string]osdFixtureOracle{}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for _, object := range []string{x, y, z, w} {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": object, "namespace": ""}).Placement
		if route.Name != object || route.Pool != base.Pool || route.Address == "" || route.ID < 0 {
			t.Fatal("explicit native placement differs", route)
		}
		routes[object] = route
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
	}
	type step struct {
		Name   string                 `json:"name"`
		Type   uint8                  `json:"type"`
		Seed   uint64                 `json:"seed"`
		Snap   uint64                 `json:"snap"`
		Native checksumNativeReceipt  `json:"native"`
		Go     cephmsgr.OSDReply      `json:"go"`
		After  *checksumNativeReceipt `json:"after_go,omitempty"`
	}
	var evidence []step
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 512<<10 {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-snap-checksum-results.json"), data)
		} else if err == nil {
			err = errors.New("snapshot checksum evidence exceeds limit")
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, algorithm := range []struct {
		name string
		kind uint8
		seed uint64
	}{{"CRC32C", cephmsgr.OSDChecksumCRC32C, 0xffffffff}, {"XXHASH32", cephmsgr.OSDChecksumXXHash32, 0x12345678}, {"XXHASH64", cephmsgr.OSDChecksumXXHash64, 0xfedcba9812345678}} {
		for _, tc := range []struct {
			name, object         string
			snap, offset, length uint64
			chunk                uint32
			code                 int32
			selected             []byte
		}{
			{"clone whole trimmed", x, s1, 0, 16, 0, 0, oldX},
			{"clone chunks with hole", x, s1, 0, 12, 4, 0, oldX},
			{"clone aligned trim", x, s1, 8, 8, 4, 0, oldX},
			{"clone unaligned trim", x, s1, 6, 8, 4, -22, oldX},
			{"clone exact EOF", x, s1, 12, 8, 4, 0, oldX},
			{"past clone EOF inside head", x, s1, 16, 4, 4, 0, oldX},
			{"later snap resolves head", x, s2, 0, 20, 4, 0, newX},
			{"above sequence resolves head", x, s2 + 1000, 0, 20, 0, 0, newX},
			{"unchanged snapshot uses head", y, s1, 0, 12, 4, 0, baseline},
			{"created after snapshot", z, s1, 0, 8, 4, -2, nil},
			{"created object later snapshot", z, s2, 0, 8, 0, 0, newZ},
			{"deleted head retained clone", w, s1, 0, 12, 4, 0, oldW},
			{"deleted head read", w, 0, 0, 8, 4, -2, nil},
		} {
			t.Run(algorithm.name+"/"+tc.name, func(t *testing.T) {
				native := nativeChecksum(t, ctx, tc.object, tc.offset, tc.length, tc.chunk, algorithm.seed, algorithm.kind, tc.snap)
				selected := &native.Before.nativeXattrSnapshot
				if tc.snap != 0 {
					selected = native.SelectedBefore
					if native.ReadSnap != tc.snap || selected == nil || !reflect.DeepEqual(selected, native.SelectedAfter) {
						t.Fatal("native snapshot selection or unchanged state differs", native)
					}
				}
				if !reflect.DeepEqual(native.Before, native.After) || native.Code != tc.code || !bytes.Equal(selected.Data, tc.selected) || (selected.Code == -2) != (tc.selected == nil) {
					t.Fatal("native selected state differs from clone/head model", native)
				}
				route := routes[tc.object]
				req := osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDChecksum, Offset: tc.offset, Length: tc.length, Checksum: cephmsgr.OSDChecksumInput{Type: algorithm.kind, ChunkSize: tc.chunk, Seed: algorithm.seed}})
				req.ReadSnapshot = tc.snap
				reply, err := connections[route.ID].Request(ctx, req)
				evidence = append(evidence, step{tc.name, algorithm.kind, algorithm.seed, tc.snap, native, reply, nil})
				if tc.code < 0 {
					var server *cephmsgr.OSDError
					if !errors.As(err, &server) || server.Code != tc.code {
						t.Fatal("snapshot checksum signed error differs", reply, err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID == 0 || reply.Object != tc.object || reply.PG.Seed != route.Hash || reply.MapEpoch < route.Epoch || reply.Code != native.Code || len(reply.Operations) != 1 {
					t.Fatal("snapshot checksum identity or aggregate differs", reply, native)
				}
				op := reply.Operations[0]
				if op.Checksum.Type != algorithm.kind || op.Checksum.ChunkSize != tc.chunk || !bytes.Equal(op.Data, native.Data) || !bytes.Equal(reply.RawData, native.Data) || !reflect.DeepEqual(op.Checksum.Values, native.Values) && (len(op.Checksum.Values) != 0 || len(native.Values) != 0) || tc.code >= 0 && reply.UserVersion != selected.Version {
					t.Fatal("snapshot checksum differs from independent librados", reply, native)
				}
				after := nativeChecksum(t, ctx, tc.object, tc.offset, tc.length, tc.chunk, algorithm.seed, algorithm.kind, tc.snap)
				evidence[len(evidence)-1].After = &after
				if !reflect.DeepEqual(native.Before, after.Before) || !reflect.DeepEqual(native.After, after.After) || !reflect.DeepEqual(native.SelectedBefore, after.SelectedBefore) || !reflect.DeepEqual(native.SelectedAfter, after.SelectedAfter) || native.Code != after.Code || !bytes.Equal(native.Data, after.Data) {
					t.Fatal("Go checksum changed native head or selected snapshot state", native, after)
				}
			})
			if t.Failed() {
				return
			}
		}
	}
}

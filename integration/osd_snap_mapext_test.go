package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
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

type nativeMapExtRange struct {
	Commit         string               `json:"source_commit"`
	Object         string               `json:"object"`
	Code           int32                `json:"code"`
	APICode        int32                `json:"native_api_code"`
	Offset         uint64               `json:"offset"`
	Length         uint64               `json:"length"`
	Snap           *uint64              `json:"snap"`
	Extents        []cephmsgr.OSDExtent `json:"extents"`
	Before         nativeXattrSnapshot  `json:"before"`
	After          nativeXattrSnapshot  `json:"after"`
	SelectedBefore nativeXattrSnapshot  `json:"selected_before"`
	SelectedAfter  nativeXattrSnapshot  `json:"selected_after"`
	Version        uint64               `json:"version"`
	VersionSource  string               `json:"version_source"`
	Exact          bool                 `json:"native_same_range"`
}

func mapExtRangeOracle(t *testing.T, ctx context.Context, object string, offset, length, snap uint64) nativeMapExtRange {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	fields := map[string]any{"object": object, "offset": offset, "length": length}
	if snap != 0 {
		fields["snap"] = snap
	}
	data, err := json.Marshal(fields)
	if err == nil {
		err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-mapext-range-input.json"), data)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-mapext-range", "observe"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-mapext-range-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r nativeMapExtRange
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || r.Object != object || r.Offset != offset || r.Length != length || (r.Snap == nil) != (snap == 0) || r.Snap != nil && *r.Snap != snap || !r.Exact || r.VersionSource != "independent selected-snapshot stat2" || !reflect.DeepEqual(r.Before, r.After) || !reflect.DeepEqual(r.SelectedBefore, r.SelectedAfter) || r.Code >= 0 && int(r.APICode) != len(r.Extents) || r.Code < 0 && r.APICode != r.Code {
		t.Fatal("independent native exact-range identity/state differs", r)
	}
	return r
}

func TestCephOSDSnapshotMapExtIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_MAPEXT_ORACLE") != "1" {
		t.Skip("requires the explicitly staged pinned native MAPEXT oracle")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
		}
	})
	baseline := []byte("unchanged-0123456")
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 3})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 4 {
		t.Fatal("native MAPEXT snapshot ownership missing")
	}
	y, x, z, w := owned.Objects[0], owned.Objects[1], owned.Objects[2], owned.Objects[3]
	snaps := nativeSnapCreate(t, ctx, base, 2)
	s1, s2 := snaps[0], snaps[1]
	write := func(object string, snapc snapOracleContext, actions ...snapOracleAction) {
		t.Helper()
		r := nativeSnapWrite(t, ctx, base, object, nil, snapc, false, omapWriteMTime{Seconds: 1760200500, Nanos: 123}, actions...)
		if r.SnapContext == nil || r.SnapContext.Code != 0 || r.Aggregate == nil || *r.Aggregate != 0 {
			t.Fatal("native snapshot setup write failed", r)
		}
	}
	// The selected clone has a hole; later head data and size differ. Store
	// allocation granularity is decided by the native exact-range operation.
	oldX := make([]byte, 20<<10)
	copy(oldX, bytes.Repeat([]byte{0, 255, 1, 128}, 1<<10))
	copy(oldX[16<<10:], bytes.Repeat([]byte{'t', 'a', 'i', 'l'}, 1<<10))
	oldW := bytes.Repeat([]byte("deleted-tail"), 1<<10)
	newX := bytes.Repeat([]byte("head-123"), 4<<10)
	newZ := []byte("new-only")
	write(x, snapOracleContext{}, snapWrite(0, oldX[:4<<10]), snapWrite(16<<10, oldX[16<<10:]))
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
		if route.Name != object || route.Pool != base.Pool || route.ID < 0 || route.Address == "" {
			t.Fatal("explicit placement differs", route)
		}
		routes[object] = route
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
	}
	type step struct {
		Name   string             `json:"name"`
		Native nativeMapExtRange  `json:"native"`
		Go     cephmsgr.OSDReply  `json:"go"`
		After  *nativeMapExtRange `json:"after_go,omitempty"`
	}
	var evidence []step
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 8<<20 {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-snap-mapext-results.json"), data)
		} else if err == nil {
			err = errors.New("snapshot MAPEXT evidence exceeds limit")
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range []struct {
		name, object         string
		snap, offset, length uint64
		selected             []byte
	}{
		{"clone full exact range", x, s1, 0, ^uint64(0), oldX},
		{"clone range crosses hole", x, s1, 2 << 10, 16 << 10, oldX},
		{"clone hole only", x, s1, 6 << 10, 4 << 10, oldX},
		{"clone EOF clipped", x, s1, 18 << 10, 16 << 10, oldX},
		{"clone exact EOF", x, s1, 20 << 10, 4 << 10, oldX},
		{"past clone EOF inside head", x, s1, 24 << 10, 4 << 10, oldX},
		{"head of cloned object", x, 0, 0, ^uint64(0), newX},
		{"later snap resolves head", x, s2, 0, ^uint64(0), newX},
		{"above sequence resolves head", x, s2 + 1000, 0, ^uint64(0), newX},
		{"unchanged snapshot uses head", y, s1, 0, ^uint64(0), baseline},
		{"unchanged head", y, 0, 0, ^uint64(0), baseline},
		{"created after snapshot", z, s1, 0, ^uint64(0), nil},
		{"created object later snapshot", z, s2, 0, ^uint64(0), newZ},
		{"deleted head retained clone", w, s1, 0, ^uint64(0), oldW},
		{"deleted head clone partial", w, s1, 2 << 10, 4 << 10, oldW},
		{"deleted head read", w, 0, 0, ^uint64(0), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := mapExtRangeOracle(t, ctx, tc.object, tc.offset, tc.length, tc.snap)
			wantCode := int32(0)
			if tc.selected == nil {
				wantCode = -2
			}
			if native.Code != wantCode || !bytes.Equal(native.SelectedBefore.Data, tc.selected) || (native.SelectedBefore.Code == -2) != (tc.selected == nil) {
				t.Fatal("native clone/head selection differs from setup model", native)
			}
			route := routes[tc.object]
			req := osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDMapExt, Offset: tc.offset, Length: tc.length, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 4096}})
			req.ReadSnapshot = tc.snap
			reply, err := connections[route.ID].Request(ctx, req)
			evidence = append(evidence, step{tc.name, native, reply, nil})
			if native.Code < 0 {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != native.Code || reply.Code != native.Code || len(reply.RawData) != 0 {
					t.Fatal("snapshot MAPEXT error differs", native, reply, err)
				}
			} else {
				if err != nil || reply.Code != 0 || len(reply.Operations) != 1 {
					t.Fatal("snapshot MAPEXT failed", reply, err)
				}
				op := reply.Operations[0]
				if op.Result != 0 || op.Offset != tc.offset || op.Length != tc.length || !reflect.DeepEqual(op.Extents, native.Extents) || reply.UserVersion != native.Version {
					t.Fatal("exact native map/range/stat2 version differs", reply, native)
				}
				raw := make([]byte, 4+16*len(native.Extents))
				binary.LittleEndian.PutUint32(raw, uint32(len(native.Extents)))
				for i, extent := range native.Extents {
					binary.LittleEndian.PutUint64(raw[4+16*i:], extent.Offset)
					binary.LittleEndian.PutUint64(raw[12+16*i:], extent.Length)
				}
				if !bytes.Equal(op.Data, raw) || !bytes.Equal(reply.RawData, raw) {
					t.Fatal("raw native map bytes differ")
				}
			}
			if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID == 0 || reply.Object != tc.object || reply.PG.Seed != route.Hash || reply.MapEpoch < route.Epoch {
				t.Fatal("snapshot MAPEXT identity differs", reply)
			}
			after := mapExtRangeOracle(t, ctx, tc.object, tc.offset, tc.length, tc.snap)
			evidence[len(evidence)-1].After = &after
			if !reflect.DeepEqual(native.Before, after.Before) || !reflect.DeepEqual(native.After, after.After) || !reflect.DeepEqual(native.SelectedBefore, after.SelectedBefore) || !reflect.DeepEqual(native.SelectedAfter, after.SelectedAfter) || native.Code != after.Code || !reflect.DeepEqual(native.Extents, after.Extents) {
				t.Fatal("Go MAPEXT changed native head or selected state", native, after)
			}
		})
		if t.Failed() {
			return
		}
	}
}

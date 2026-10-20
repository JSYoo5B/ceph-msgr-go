package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephOSDSnapshotSparseReadIntegration compares Go SPARSE_READ at a read
// snapshot with the independent native C++ IoCtx::sparse_read after
// IoCtx::snap_set_read of the same snap id, on the same object, offset and
// length: the result code, the extent map, the bytes, the raw encoded output
// and the reply user version. Objects and self-managed snap ids are created
// natively (librados C API through the snapshot oracle); the object layouts
// cover a snap id that resolves to a clone with holes, one that resolves to
// the head, one at which the object did not exist, a range past the clone's
// end and a deleted head whose clone remains. The allocation map is the
// store's (BlueStore logical extents, MemStore whole ranges); the native
// client decides the expected map on the active store.
func TestCephOSDSnapshotSparseReadIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE") != "1" {
		t.Skip("requires staged independent native sparse-read oracle; set CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE=1 after staging the matching binary")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	store := os.Getenv("CEPH_MSGR_TEST_OSD_STORE")
	if store == "" {
		store = "memstore"
	}
	k := uint64(1) << 10
	block := func(label string, n uint64) []byte {
		var data []byte
		for counter := 0; uint64(len(data)) < n; counter++ {
			sum := sha256.Sum256([]byte(label + ":" + string(rune('0'+counter%10)) + ":" + string(rune('a'+counter/10%26))))
			data = append(data, sum[:]...)
		}
		return data[:n]
	}
	baseline := []byte("snap-sparse-head-unchanged-0123")
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanupSnapOracle(t, base)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 3})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 4 || !bytes.Equal(owned.State.Data, baseline) {
		t.Fatal("exclusive native snapshot sparse-read baseline differs")
	}
	// Y keeps its pre-snapshot state; X is rewritten after s1 (clone s1 with
	// holes, head extended); Z is created after s1; W is deleted after s1.
	y, x, z, w := owned.Objects[0], owned.Objects[1], owned.Objects[2], owned.Objects[3]
	snaps := nativeSnapCreate(t, ctx, base, 2)
	s1, s2 := snaps[0], snaps[1]
	above := s2 + 1000
	t.Logf("allocated self-managed snaps %d and %d", s1, s2)

	type step struct {
		Name   string             `json:"name"`
		Native any                `json:"native,omitempty"`
		Go     *cephmsgr.OSDReply `json:"go_reply,omitempty"`
		Object string             `json:"object,omitempty"`
		Snap   uint64             `json:"snap"`
		Offset uint64             `json:"offset"`
		Length uint64             `json:"length"`
		Store  string             `json:"store,omitempty"`
	}
	var evidence []step
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-snap-sparse-read-results.json"), data)
		}
		if err != nil {
			t.Error("bounded snapshot sparse-read evidence", err)
		}
	}()
	tick := uint32(0)
	write := func(name, object string, snapc snapOracleContext, ops ...snapOracleAction) {
		t.Helper()
		tick++
		result := nativeSnapWrite(t, ctx, base, object, nil, snapc, false, omapWriteMTime{Seconds: 1760200000 + tick, Nanos: tick}, ops...)
		evidence = append(evidence, step{Name: name, Native: result, Object: object})
		if result.SnapContext == nil || result.SnapContext.Code != 0 || result.Aggregate == nil || *result.Aggregate != 0 {
			t.Fatalf("%s: native snapshot write differs: snapc=%+v aggregate=%v", name, result.SnapContext, result.Aggregate)
		}
	}
	none := snapOracleContext{}
	c1 := snapOracleContext{Seq: s1, Snaps: []uint64{s1}}
	// Logical models (zero-filled) of each object at the head and at s1.
	model := map[string]map[uint64][]byte{}
	apply := func(object string, at uint64, offset uint64, data []byte) {
		if model[object] == nil {
			model[object] = map[uint64][]byte{}
		}
		cur := model[object][at]
		if end := offset + uint64(len(data)); uint64(len(cur)) < end {
			cur = append(cur, make([]byte, end-uint64(len(cur)))...)
		}
		copy(cur[offset:], data)
		model[object][at] = cur
	}
	apply(y, 0, 0, baseline)
	pre := map[string][]struct {
		offset uint64
		data   []byte
	}{
		y: {{12 * k, block("y-pre-12k", 4*k)}},
		x: {{0, block("x-pre-0k", 4*k)}, {16 * k, block("x-pre-16k", 4*k)}},
		w: {{0, block("w-pre-0k", 4*k)}, {12 * k, block("w-pre-12k", 4*k)}},
	}
	for _, object := range []string{y, x, w} {
		var ops []snapOracleAction
		for _, e := range pre[object] {
			ops = append(ops, snapWrite(e.offset, e.data))
			apply(object, 0, e.offset, e.data)
		}
		write("pre-snapshot writes "+object, object, none, ops...)
	}
	// The state every object had when s1 was taken.
	for _, object := range []string{y, x, w} {
		model[object][s1] = bytes.Clone(model[object][0])
	}
	xPost := []struct {
		offset uint64
		data   []byte
	}{{8 * k, block("x-post-8k", 4*k)}, {28 * k, block("x-post-28k", 4*k)}}
	write("post-snapshot writes X under s1", x, c1, snapWrite(xPost[0].offset, xPost[0].data), snapWrite(xPost[1].offset, xPost[1].data))
	for _, e := range xPost {
		apply(x, 0, e.offset, e.data)
	}
	zData := block("z-post-4k", 4*k)
	write("create Z under s1", z, c1, snapWrite(4*k, zData))
	apply(z, 0, 4*k, zData)
	write("delete W under s1", w, c1, snapDelete())
	delete(model[w], 0)

	// Listings make the resolved objects visible: X and W have clone s1, Y
	// and Z only a head (W no head).
	for _, object := range []string{y, x, z, w} {
		listing := nativeListSnaps(t, ctx, base, object)
		evidence = append(evidence, step{Name: "native listsnaps", Native: listing, Object: object})
		t.Logf("native listsnaps %s: %s", object, listing.JSON)
	}

	routes := map[string]osdFixtureOracle{}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for _, name := range []string{y, x, z, w} {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if route.Name != name || route.Pool != base.Pool || route.ID < 0 || route.Address == "" {
			t.Fatal("caller-owned native placement differs", name)
		}
		routes[name] = route
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
	}
	expected := func(object string, snap uint64, offset, length uint64) ([]byte, bool) {
		at := snap
		switch {
		case object == x && snap == s1, object == w && snap == s1:
			at = s1
		case object == z && snap == s1:
			return nil, false
		default:
			at = 0
		}
		data, exists := model[object][at]
		if !exists {
			return nil, false
		}
		if offset >= uint64(len(data)) {
			return []byte{}, true
		}
		return data[offset:min(uint64(len(data)), offset+length)], true
	}
	for _, tc := range []struct {
		name           string
		object         string
		snap           uint64
		offset, length uint64
	}{
		{"clone with holes, whole object", x, s1, 0, 64 * k},
		{"clone with holes, range across a hole", x, s1, 2 * k, 16 * k},
		{"clone hole only", x, s1, 6 * k, 4 * k},
		{"clone EOF clipped", x, s1, 14 * k, 16 * k},
		{"clone exact EOF", x, s1, 20 * k, 4 * k},
		{"past clone EOF, inside head", x, s1, 28 * k, 4 * k},
		{"head of the cloned object", x, 0, 0, 64 * k},
		{"later snap resolves to head", x, s2, 0, 64 * k},
		{"snap above every seq resolves to head", x, above, 0, 64 * k},
		{"unchanged object at s1 is the head", y, s1, 0, 64 * k},
		{"unchanged object head", y, 0, 0, 64 * k},
		{"created after s1, read at s1", z, s1, 0, 64 * k},
		{"created after s1, read at s2", z, s2, 0, 64 * k},
		{"deleted head, clone at s1", w, s1, 0, 64 * k},
		{"deleted head, clone range across a hole", w, s1, 2 * k, 12 * k},
		{"deleted head read", w, 0, 0, 64 * k},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := sparseReadOracleAt(t, ctx, tc.object, tc.offset, tc.length, tc.snap)
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: tc.offset, Length: tc.length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 1 << 17}}
			route := routes[tc.object]
			req := osdObjectRequest(route, op)
			req.ReadSnapshot = tc.snap
			reply, err := connections[route.ID].Request(ctx, req)
			evidence = append(evidence, step{Name: tc.name, Native: native, Go: &reply, Object: tc.object, Snap: tc.snap, Offset: tc.offset, Length: tc.length, Store: store})
			want, exists := expected(tc.object, tc.snap, tc.offset, tc.length)
			if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != tc.object || reply.PG.Seed != route.Hash || reply.RetryAttempt != 0 || reply.MapEpoch < route.Epoch {
				t.Fatalf("reply identity differs: %+v", reply)
			}
			if native.Code < 0 {
				var server *cephmsgr.OSDError
				if exists || !errors.As(err, &server) || server.Code != native.Code || reply.Code != native.Code || len(reply.RawData) != 0 {
					t.Fatal("native snapshot SPARSE_READ error differs", native.Code, reply.Code, err, exists)
				}
				t.Logf("%s: snap=%d -> %d (Go %d), reply flags %#x user_version %d native version %d", tc.name, tc.snap, native.Code, reply.Code, reply.Flags, reply.UserVersion, native.Version)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := reply.Operations[0]
			if !exists || reply.Code != 0 || got.Result != 0 || got.Offset != tc.offset || got.Length != uint64(len(native.Data)) || !reflect.DeepEqual(got.SparseRead.Extents, native.Extents) ||
				!bytes.Equal(got.SparseRead.Data, native.Data) || native.Code != int32(len(native.Extents)) || reply.UserVersion != native.Version {
				t.Fatalf("native snapshot map/range/version differs: Go %+v (user version %d) native %+v", got.SparseRead.Extents, reply.UserVersion, native)
			}
			expanded, err := rbdExpandSparse(got.SparseRead.Extents, got.SparseRead.Data, tc.offset, uint64(len(want)))
			if len(got.SparseRead.Extents) != 0 && got.SparseRead.Extents[len(got.SparseRead.Extents)-1].Offset+got.SparseRead.Extents[len(got.SparseRead.Extents)-1].Length > tc.offset+uint64(len(want)) {
				t.Fatal("sparse extents reach past the resolved object's end", got.SparseRead.Extents, len(want))
			}
			if err != nil || !bytes.Equal(expanded, want) {
				t.Fatalf("snapshot sparse bytes differ from the object model at snap %d: %v", tc.snap, err)
			}
			raw := make([]byte, 8+16*len(native.Extents)+len(native.Data))
			binary.LittleEndian.PutUint32(raw, uint32(len(native.Extents)))
			for i, e := range native.Extents {
				binary.LittleEndian.PutUint64(raw[4+i*16:], e.Offset)
				binary.LittleEndian.PutUint64(raw[12+i*16:], e.Length)
			}
			binary.LittleEndian.PutUint32(raw[4+16*len(native.Extents):], uint32(len(native.Data)))
			copy(raw[8+16*len(native.Extents):], native.Data)
			if !bytes.Equal(got.Data, raw) || !bytes.Equal(reply.RawData, raw) {
				t.Fatal("raw encoded snapshot extent map differs from the native result")
			}
			// The same range by READ at the same snap: the product's
			// characterized snapshot READ returns the zero-filled bytes.
			readReq := osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: tc.offset, Length: tc.length})
			readReq.ReadSnapshot = tc.snap
			readReply, err := connections[route.ID].Request(ctx, readReq)
			if err != nil || !bytes.Equal(readReply.Operations[0].Data, want) || readReply.UserVersion != reply.UserVersion {
				t.Fatal("READ at the same snap differs from the expanded SPARSE_READ", err, len(readReply.Operations[0].Data), readReply.UserVersion)
			}
			after := sparseReadOracleAt(t, ctx, tc.object, tc.offset, tc.length, tc.snap)
			if !reflect.DeepEqual(after.Extents, native.Extents) || !bytes.Equal(after.Data, native.Data) || after.Version != native.Version || !reflect.DeepEqual(after.After, native.Before) {
				t.Fatal("Go snapshot SPARSE_READ altered the object or its clone")
			}
			t.Logf("%s: snap=%d %d~%d -> extents %+v (%d bytes, raw %d), user_version %d, flags %#x, store %s", tc.name, tc.snap, tc.offset, tc.length, got.SparseRead.Extents, len(got.SparseRead.Data), len(got.Data), reply.UserVersion, reply.Flags, store)
		})
	}
}

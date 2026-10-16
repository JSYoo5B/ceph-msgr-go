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

type nativeSparseRead struct {
	Commit  string               `json:"source_commit"`
	Object  string               `json:"object"`
	Code    int32                `json:"code"`
	Offset  uint64               `json:"offset"`
	Length  uint64               `json:"length"`
	Snap    *uint64              `json:"snap"`
	Extents []cephmsgr.OSDExtent `json:"extents"`
	Data    []byte               `json:"data"`
	Version uint64               `json:"version"`
	Before  omapNativeSnapshot   `json:"before"`
	After   omapNativeSnapshot   `json:"after"`
	Exact   bool                 `json:"native_same_range"`
}

func sparseReadOracle(t *testing.T, ctx context.Context, name string, offset, length uint64) nativeSparseRead {
	t.Helper()
	return sparseReadOracleAt(t, ctx, name, offset, length, 0)
}

// sparseReadOracleAt runs the native IoCtx::sparse_read at read snap id snap
// (IoCtx::snap_set_read), or at the head when snap is 0.
func sparseReadOracleAt(t *testing.T, ctx context.Context, name string, offset, length, snap uint64) nativeSparseRead {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	input := map[string]any{"object": name, "offset": offset, "length": length}
	if snap != 0 {
		input["snap"] = snap
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-sparse-read-input.json"), data); err != nil {
		t.Fatal(err)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-sparse-read", "observe"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-sparse-read-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result nativeSparseRead
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Object != name || result.Offset != offset || result.Length != length || (result.Snap == nil) != (snap == 0) || result.Snap != nil && *result.Snap != snap ||
		result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !reflect.DeepEqual(result.Before, result.After) {
		t.Fatal("native SPARSE_READ identity/state differs", result)
	}
	return result
}

func TestCephOSDSparseReadIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE") != "1" {
		t.Skip("requires staged independent native sparse-read oracle; set CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE=1 after staging the matching binary")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned SPARSE_READ cleanup failed")
			}
		}
	})
	baseline := bytes.Repeat([]byte{0, 255, 65, 10}, 8)
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 2})
	prepared = true
	if len(owned.Objects) != 3 || !bytes.Equal(owned.State.Data, baseline) {
		t.Fatal("native SPARSE_READ ownership/baseline missing")
	}
	main, sparse, absent := owned.Objects[0], owned.Objects[1], owned.Objects[2]
	routes := map[string]osdFixtureOracle{}
	connections := map[string]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	for _, name := range owned.Objects {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		routes[name] = route
		connections[name] = mutationConnection(t, c, ctx, route)
	}
	// Explicit consumer-side writes create an owned sparse object. Allocation
	// granularity differs between BlueStore and MemStore; native sparse-read decides
	// the expected map for the active store.
	write := cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 8192, Length: 4, Data: []byte{0, 255, 66, 10}}
	if reply, err := connections[sparse].Request(ctx, osdObjectRequest(routes[sparse], write)); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("owned sparse object write", reply, err)
	}
	type receipt struct {
		Name   string            `json:"name"`
		Native nativeSparseRead  `json:"native"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
	}
	var evidence []receipt
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-sparse-read-results.json"), data)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range []struct {
		name, object   string
		offset, length uint64
	}{
		{"dense full native operation", main, 0, 32},
		{"partial native map intersection", main, 3, 7},
		{"EOF clipped extent", main, 29, 100},
		{"beyond EOF empty", main, 64, 1},
		{"sparse full native operation", sparse, 0, 8196},
		{"sparse range native map intersection", sparse, 8190, 10},
		{"sparse hole only", sparse, 4096, 16},
		{"exact EOF", main, 32, 16},
		{"absent signed ENOENT", absent, 0, 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := sparseReadOracle(t, ctx, tc.object, tc.offset, tc.length)
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: tc.offset, Length: tc.length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 32768}}
			reply, err := connections[tc.object].Request(ctx, osdObjectRequest(routes[tc.object], op))
			evidence = append(evidence, receipt{tc.name, native, reply})
			if native.Code < 0 {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != native.Code || reply.Code != native.Code || len(reply.RawData) != 0 {
					t.Fatal("native SPARSE_READ server error differs", native, reply, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := reply.Operations[0]
			if reply.Code != 0 || got.Result != 0 || got.Offset != tc.offset || got.Length != uint64(len(native.Data)) || !reflect.DeepEqual(got.SparseRead.Extents, native.Extents) || !bytes.Equal(got.SparseRead.Data, native.Data) || reply.UserVersion != native.Before.Version || native.Version != native.Before.Version || native.Code != int32(len(native.Extents)) {
				t.Fatal("native map/range/version differs", got, native)
			}
			expected := make([]byte, 8+16*len(native.Extents)+len(native.Data))
			binary.LittleEndian.PutUint32(expected, uint32(len(native.Extents)))
			for i, e := range native.Extents {
				binary.LittleEndian.PutUint64(expected[4+i*16:], e.Offset)
				binary.LittleEndian.PutUint64(expected[12+i*16:], e.Length)
			}
			binary.LittleEndian.PutUint32(expected[4+16*len(native.Extents):], uint32(len(native.Data)))
			copy(expected[8+16*len(native.Extents):], native.Data)
			if !bytes.Equal(got.Data, expected) || !bytes.Equal(reply.RawData, expected) {
				t.Fatal("raw encoded native extent map differs")
			}
			if !native.Exact {
				t.Fatal("native exact/model scope mislabeled")
			}
			after := sparseReadOracle(t, ctx, tc.object, tc.offset, tc.length)
			if !reflect.DeepEqual(after.After, native.Before) {
				t.Fatal("Go SPARSE_READ altered bytes/mtime/version")
			}
			t.Logf("%s: %d extents, %d sparse bytes, raw=%d bytes", tc.name, len(got.SparseRead.Extents), len(got.SparseRead.Data), len(got.Data))
		})
	}
	t.Run("local output budget invalidates only its connection", func(t *testing.T) {
		// OpenOSD reuses an existing handle for the same OSD on one client.
		// A separately authenticated client supplies an independent session.
		options := integrationOptions(t)
		options.EnableOSD = true
		independent, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer independent.Close()
		limited := mutationConnection(t, independent, ctx, routes[main])
		defer limited.Close()
		op := cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Length: 32, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 8}}
		reply, err := limited.Request(ctx, osdObjectRequest(routes[main], op))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || reply.Submission.Transaction == 0 || len(reply.RawData) <= 8 {
			t.Fatal("SPARSE_READ budget did not preserve submitted reply", reply, err)
		}
		op.SparseRead.MaxOutputBytes = 32768
		if _, err := connections[main].Request(ctx, osdObjectRequest(routes[main], op)); err != nil {
			t.Fatal("independent connection damaged", err)
		}
	})
}

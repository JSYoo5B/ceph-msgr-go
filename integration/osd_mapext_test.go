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

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeMapExt struct {
	Commit  string               `json:"source_commit"`
	Object  string               `json:"object"`
	Code    int32                `json:"code"`
	Offset  uint64               `json:"offset"`
	Length  uint64               `json:"length"`
	Extents []cephmsgr.OSDExtent `json:"extents"`
	Full    []cephmsgr.OSDExtent `json:"native_full_extents"`
	Before  omapNativeSnapshot   `json:"before"`
	After   omapNativeSnapshot   `json:"after"`
	Exact   bool                 `json:"native_same_range"`
	Scope   string               `json:"range_scope"`
}

func mapExtOracle(t *testing.T, ctx context.Context, name string, offset, length uint64) nativeMapExt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(map[string]any{"object": name, "offset": offset, "length": length})
	if err != nil {
		t.Fatal(err)
	}
	if err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-mapext-input.json"), data); err != nil {
		t.Fatal(err)
	}
	if err = testcluster.ControlDaemon(ctx, dir, "verify", "osd-mapext", "observe"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-mapext-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result nativeMapExt
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Object != name || result.Offset != offset || result.Length != length || result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !reflect.DeepEqual(result.Before, result.After) {
		t.Fatal("native MAPEXT identity/state differs", result)
	}
	return result
}

func TestCephOSDMapExtIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned MAPEXT cleanup failed")
			}
		}
	})
	baseline := bytes.Repeat([]byte{0, 255, 65, 10}, 8)
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 2})
	prepared = true
	if len(owned.Objects) != 3 || !bytes.Equal(owned.State.Data, baseline) {
		t.Fatal("native MAPEXT ownership/baseline missing")
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
	// granularity differs between BlueStore and MemStore; native mapext decides
	// the expected map for the active store.
	write := cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 8192, Length: 4, Data: []byte{0, 255, 66, 10}}
	if reply, err := connections[sparse].Request(ctx, osdObjectRequest(routes[sparse], write)); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("owned sparse object write", reply, err)
	}
	type receipt struct {
		Name   string            `json:"name"`
		Native nativeMapExt      `json:"native"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
	}
	var evidence []receipt
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-mapext-results.json"), data)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range []struct {
		name, object   string
		offset, length uint64
	}{
		{"full native operation", main, 0, ^uint64(0)},
		{"partial native map intersection", main, 3, 7},
		{"EOF clipped extent", main, 29, 100},
		{"beyond EOF empty", main, 64, 1},
		{"sparse full native operation", sparse, 0, ^uint64(0)},
		{"sparse range native map intersection", sparse, 8190, 10},
		{"absent signed ENOENT", absent, 0, ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := mapExtOracle(t, ctx, tc.object, tc.offset, tc.length)
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDMapExt, Offset: tc.offset, Length: tc.length, MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 4096}}
			reply, err := connections[tc.object].Request(ctx, osdObjectRequest(routes[tc.object], op))
			evidence = append(evidence, receipt{tc.name, native, reply})
			if native.Code < 0 {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != native.Code || reply.Code != native.Code || len(reply.RawData) != 0 {
					t.Fatal("native MAPEXT server error differs", native, reply, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := reply.Operations[0]
			if reply.Code != 0 || got.Result != 0 || got.Offset != tc.offset || got.Length != tc.length || !reflect.DeepEqual(got.Extents, native.Extents) || reply.UserVersion != native.Before.Version {
				t.Fatal("native map/range/version differs", got, native)
			}
			expected := make([]byte, 4+16*len(native.Extents))
			binary.LittleEndian.PutUint32(expected, uint32(len(native.Extents)))
			for i, e := range native.Extents {
				binary.LittleEndian.PutUint64(expected[4+i*16:], e.Offset)
				binary.LittleEndian.PutUint64(expected[12+i*16:], e.Length)
			}
			if !bytes.Equal(got.Data, expected) || !bytes.Equal(reply.RawData, expected) {
				t.Fatal("raw encoded native extent map differs")
			}
			if native.Exact != (tc.offset == 0 && tc.length == ^uint64(0)) {
				t.Fatal("native exact/model scope mislabeled")
			}
			after := mapExtOracle(t, ctx, tc.object, tc.offset, tc.length)
			if !reflect.DeepEqual(after.After, native.Before) {
				t.Fatal("Go MAPEXT altered bytes/mtime/version")
			}
			t.Logf("%s: %d extents, raw=%d bytes, native scope=%s", tc.name, len(got.Extents), len(got.Data), native.Scope)
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
		op := cephmsgr.OSDOperation{Code: cephmsgr.OSDMapExt, Length: ^uint64(0), MapExt: cephmsgr.OSDMapExtInput{MaxOutputBytes: 4}}
		reply, err := limited.Request(ctx, osdObjectRequest(routes[main], op))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || reply.Submission.Transaction == 0 || len(reply.RawData) <= 4 {
			t.Fatal("MAPEXT budget did not preserve submitted reply", reply, err)
		}
		op.MapExt.MaxOutputBytes = 4096
		if _, err := connections[main].Request(ctx, osdObjectRequest(routes[main], op)); err != nil {
			t.Fatal("independent connection damaged", err)
		}
	})
}

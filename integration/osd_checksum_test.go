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

type checksumNativeReceipt struct {
	Code   int32          `json:"code"`
	Data   []byte         `json:"data"`
	Values []uint32       `json:"values"`
	Before omapWriteState `json:"before"`
	After  omapWriteState `json:"after"`
}

func nativeChecksum(t *testing.T, ctx context.Context, object string, offset, length uint64, chunk, seed uint32) checksumNativeReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	input, err := json.Marshal(map[string]any{"object": object, "offset": offset, "length": length, "chunk_size": chunk, "seed": seed, "type": 2})
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "osd-checksum-input.json"), input, 0600)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-checksum", "compare"); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(filepath.Join(dir, "osd-checksum-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt checksumNativeReceipt
	if err := json.Unmarshal(output, &receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func TestCephOSDChecksumIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	baseline := []byte{0, 255, 'c', 'r', 'c', '3', '2', 'c', 0, 128, 'x', 'y'}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("checksum fixture cleanup failed")
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}, Reserve: 1})
	prepared = true
	if len(owned.Objects) != 2 {
		t.Fatal("checksum fixture ownership missing")
	}
	main, absent := owned.Objects[0], owned.Objects[1]
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()
	var evidence []struct {
		Name   string                `json:"name"`
		Native checksumNativeReceipt `json:"native"`
		Go     cephmsgr.OSDReply     `json:"go"`
	}
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 512<<10 {
			err = os.WriteFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-checksum-results.json"), data, 0644)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range []struct {
		name, object   string
		offset, length uint64
		chunk, seed    uint32
		code           int32
	}{
		{"single CRC32C", main, 0, 12, 0, 0xffffffff, 0},
		{"chunked binary extent", main, 2, 8, 4, 0x12345678, 0},
		{"trimmed aligned extent", main, 8, 8, 4, 0, 0},
		{"trimmed unaligned signed EINVAL", main, 6, 8, 4, 0xffffffff, -22},
		{"exact EOF empty output", main, 12, 8, 4, 0xffffffff, 0},
		{"past EOF empty output", main, 20, 8, 4, 0xffffffff, 0},
		{"absent signed ENOENT", absent, 0, 8, 4, 0xffffffff, -2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := nativeChecksum(t, ctx, tc.object, tc.offset, tc.length, tc.chunk, tc.seed)
			if native.Code != tc.code || !reflect.DeepEqual(native.Before, native.After) {
				t.Fatal("native checksum result/state", native)
			}
			selected := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": tc.object, "namespace": ""}).Placement
			if connections[selected.ID] == nil {
				connections[selected.ID] = mutationConnection(t, c, ctx, selected)
			}
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDChecksum, Offset: tc.offset, Length: tc.length, Checksum: cephmsgr.OSDChecksumInput{Type: cephmsgr.OSDChecksumCRC32C, ChunkSize: tc.chunk, Seed: tc.seed}}
			reply, err := connections[selected.ID].Request(ctx, osdObjectRequest(selected, op))
			if tc.code < 0 {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatal("checksum signed error", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// The C convenience API exposes only the aggregate result. An early
			// missing-object reply has aggregate -ENOENT with per-op result zero.
			// Keep raw per-op metadata without inventing a C per-op observation.
			if reply.Code != native.Code || len(reply.Operations) != 1 || tc.code >= 0 && reply.Operations[0].Result != 0 || !bytes.Equal(reply.Operations[0].Data, native.Data) || !reflect.DeepEqual(reply.Operations[0].Checksum.Values, native.Values) && len(native.Values) != 0 {
				t.Fatal("Go checksum differs from independent librados", reply, native)
			}
			if reply.Operations[0].Checksum.Type != 2 || reply.Operations[0].Checksum.ChunkSize != tc.chunk {
				t.Fatal("checksum echoed union lost", reply.Operations[0])
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object}).State
			if after.Code != native.After.Code || after.Version != native.After.Version || !bytes.Equal(after.Data, native.After.Data) {
				t.Fatal("checksum changed bytes/version", after, native.After)
			}
			evidence = append(evidence, struct {
				Name   string                `json:"name"`
				Native checksumNativeReceipt `json:"native"`
				Go     cephmsgr.OSDReply     `json:"go"`
			}{tc.name, native, reply})
		})
		if t.Failed() {
			return
		}
	}
}

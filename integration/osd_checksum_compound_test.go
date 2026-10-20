package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

var checksumCompoundBinary = []byte{0, 255, 67, 72, 75, 0, 128, 13, 10, 17, 34, 51, 68, 85, 102, 119}

type checksumCompoundAction struct {
	Op      string `json:"op"`
	Type    uint8  `json:"type"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Chunk   uint32 `json:"chunk_size"`
	Seed    uint64 `json:"seed"`
	Version uint64 `json:"version"`
	Data    []byte `json:"data"`
}
type checksumCompoundOutput struct {
	Request         checksumCompoundAction `json:"request"`
	PrvalAvailable  bool                   `json:"prval_available"`
	Prval           *int32                 `json:"callback_prval"`
	Data            []byte                 `json:"data"`
	Buffer          []byte                 `json:"buffer_raw"`
	BufferUnchanged bool                   `json:"buffer_unchanged"`
	BytesRead       uint64                 `json:"bytes_read"`
	CountUnchanged  bool                   `json:"byte_count_unchanged"`
	Values          []uint64               `json:"values"`
	Size            uint64                 `json:"size"`
	Seconds         int64                  `json:"seconds"`
	Nanos           int64                  `json:"nanos"`
}
type checksumCompoundNative struct {
	Source       string                   `json:"source_commit"`
	Schema       int                      `json:"schema_version"`
	Case         string                   `json:"case"`
	Object       string                   `json:"object"`
	Aggregate    int32                    `json:"aggregate"`
	Version      uint64                   `json:"version"`
	OperateCalls int                      `json:"operate_calls"`
	RawPerOp     bool                     `json:"raw_wire_per_op_results_available"`
	AssertPrval  bool                     `json:"assert_version_prval_available"`
	ExtentUnion  bool                     `json:"returned_extent_union_available"`
	Before       nativeXattrSnapshot      `json:"before"`
	After        nativeXattrSnapshot      `json:"after"`
	MainBefore   nativeXattrSnapshot      `json:"main_before"`
	MainAfter    nativeXattrSnapshot      `json:"main_after"`
	Operations   []checksumCompoundOutput `json:"operations"`
	FSID         string                   `json:"native_fsid"`
	Epoch        uint32                   `json:"native_pool_epoch"`
	PoolType     int                      `json:"native_pool_type"`
	Identity     struct {
		Name string `json:"name"`
		GID  uint64 `json:"global_id"`
	} `json:"native_identity"`
}

// Native callback prvals are convenience API outputs, not raw wire rvals.
// ASSERT_VER has none; an untouched STAT suffix can become -EIO on decode.
func checksumCompoundNativeOnce(t *testing.T, ctx context.Context, name, which string) (checksumCompoundNative, map[string]any) {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	input, _ := json.Marshal(map[string]string{"object": name, "case": which})
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-checksum-compound-input.json"), input); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-checksum-compound", "compare"); err != nil {
		t.Fatal("native compound", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "osd-checksum-compound-result.json"))
	var n checksumCompoundNative
	if err != nil || len(raw) > 512<<10 || json.Unmarshal(raw, &n) != nil {
		t.Fatal("bounded native compound receipt", err)
	}
	if n.Schema != 1 || n.Source != mdsFreshSource || n.Case != which || n.Object != name || n.OperateCalls != 1 || n.RawPerOp || n.AssertPrval || n.ExtentUnion || n.Identity.Name != "client.test" || n.Identity.GID == 0 || n.Epoch == 0 || n.PoolType != 1 {
		t.Fatal("native compound identity/scope differs", n)
	}
	return n, map[string]any{"receipt": n, "sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "bytes": len(raw)}
}

func TestCephOSDChecksumCompoundIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	client, _, base, _ := fixture(t)
	if base.PoolType == 3 {
		t.Skip("CHECKSUM compound acceptance requires a replicated pool; EC is outside this subset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	prepared := false
	t.Cleanup(func() {
		if prepared {
			clean, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			if !nativeOmapWrite(t, clean, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned checksum heads cleanup failed")
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: checksumCompoundBinary}, Reserve: 1})
	prepared = true
	if len(owned.Objects) != 2 || !bytes.Equal(owned.State.Data, checksumCompoundBinary) {
		t.Fatal("immutable binary16 ownership differs")
	}
	main, missing := owned.Objects[0], owned.Objects[1]
	facts := []map[string]any{}
	defer func() {
		raw, err := json.MarshalIndent(map[string]any{"source_commit": mdsFreshSource, "objects": owned.Objects, "binary16": checksumCompoundBinary, "native_raw_per_op_available": false, "assert_version_native_prval_available": false, "native_stat_callback_may_report_EIO_for_raw_zero": true, "cases": facts}, "", "  ")
		if err == nil && len(raw) > 2<<20 {
			err = errors.New("compound evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-checksum-compound-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	}()
	checksum := func(kind uint8, offset, length uint64, chunk uint32, seed uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDChecksum, Offset: offset, Length: length, Checksum: cephmsgr.OSDChecksumInput{Type: kind, ChunkSize: chunk, Seed: seed}}
	}
	read := func(offset, length uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length}
	}
	stat := cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}
	assertion := cephmsgr.OSDOperation{Code: cephmsgr.OSDAssertVersion, Version: owned.State.Version + 1}
	good := append([]byte(nil), checksumCompoundBinary[:4]...)
	bad := append([]byte(nil), good...)
	bad[2] ^= 255
	cmp := func(data []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareExtent, Length: uint64(len(data)), Data: data}
	}
	cases := []struct {
		name    string
		ops     []cephmsgr.OSDOperation
		code    int32
		failure int
	}{
		{"mixed", []cephmsgr.OSDOperation{checksum(2, 0, 8, 4, 0x1234), stat, read(4, 8), checksum(1, 0, 16, 0, 0xfedcba9876543210)}, 0, -1},
		{"distinct-seeds", []cephmsgr.OSDOperation{checksum(0, 0, 8, 0, 0xffffffff), checksum(1, 8, 8, 4, 0x8000000000000001)}, 0, -1},
		{"eof-prefix", []cephmsgr.OSDOperation{checksum(2, 16, 8, 4, 1), read(0, 8), stat}, 0, -1},
		{"trimmed", []cephmsgr.OSDOperation{checksum(2, 12, 8, 0, 7), stat}, 0, -1},
		{"checksum-before-assert", []cephmsgr.OSDOperation{checksum(2, 0, 8, 4, 1), assertion, checksum(1, 0, 8, 0, 2), read(0, 4)}, -75, 1},
		{"assert-first", []cephmsgr.OSDOperation{assertion, checksum(2, 0, 8, 0, 1), stat}, -75, 0},
		{"unaligned-after-read", []cephmsgr.OSDOperation{read(0, 4), checksum(2, 12, 8, 8, 3), stat, checksum(0, 0, 8, 0, 4)}, -22, 1},
		{"checksum-before-cmpext", []cephmsgr.OSDOperation{checksum(2, 0, 8, 0, 5), cmp(bad), stat}, -4097, 1},
		{"cmpext-first", []cephmsgr.OSDOperation{cmp(good), checksum(1, 0, 8, 0, 6), read(8, 8)}, 0, -1},
		{"missing", []cephmsgr.OSDOperation{stat, checksum(2, 0, 8, 0, 1), read(0, 4)}, -2, -2},
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	for _, tc := range cases {
		if !t.Run(tc.name, func(t *testing.T) {
			name := main
			if tc.name == "missing" {
				name = missing
			}
			n, nativeEvidence := checksumCompoundNativeOnce(t, ctx, name, tc.name)
			entry := map[string]any{"case": tc.name, "native": nativeEvidence}
			facts = append(facts, entry)
			if n.Aggregate != tc.code || n.FSID != client.Snapshot().FSID || !reflect.DeepEqual(n.Before, n.After) || !reflect.DeepEqual(n.MainBefore, n.MainAfter) || !reflect.DeepEqual(n.MainBefore, owned.State.nativeXattrSnapshot) || len(n.Operations) != len(tc.ops) {
				t.Fatal("native aggregate/immutable state differs", n.Aggregate, tc.code)
			}
			route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
			if route.Pool != base.Pool || route.Name != name || route.PoolType != 1 || route.Shard != -1 || route.ID != route.PrimaryID {
				t.Fatal("compound target is not explicit replicated primary", route)
			}
			if connections[route.ID] == nil {
				connections[route.ID] = mutationConnection(t, client, ctx, route)
			}
			o := connections[route.ID]
			request := osdObjectRequest(route, tc.ops...)
			if isReplicatedPGFixtureProfile() {
				request = replicatedPGRequest(route, tc.ops...)
			}
			reply, err := o.Request(ctx, request)
			text := ""
			if err != nil {
				text = err.Error()
			}
			entry["request"], entry["reply"], entry["error"], entry["route"] = request, reply, text, route
			var unknown *cephmsgr.OutcomeUnknownError
			var remote *cephmsgr.OSDError
			if errors.As(err, &unknown) || reply.Code != tc.code || len(reply.Operations) != len(tc.ops) || tc.code < 0 && (!errors.As(err, &remote) || remote.Code != tc.code) || tc.code >= 0 && err != nil {
				t.Fatal("known Go compound result differs", reply.Code, err)
			}
			seed := route.Hash
			if request.Placement != nil {
				seed = request.Placement.PG.Seed
			}
			if reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.GlobalID != client.Snapshot().GlobalID || reply.Submission.GlobalID != reply.GlobalID || reply.Object != name || reply.PG.Pool != uint64(route.Pool) || reply.PG.Seed != seed || reply.MapEpoch < request.MapEpoch || len(reply.RawFront) == 0 {
				t.Fatal("compound request/reply identity differs")
			}
			if tc.code == 0 && reply.UserVersion != n.Before.Version {
				t.Fatal("successful compound version differs from independent stat")
			}
			data := []byte{}
			for i, input := range tc.ops {
				got, want := reply.Operations[i], n.Operations[i]
				if got.Code != input.Code || got.Flags != uint32(input.Flags) || got.PayloadLength != uint32(len(got.Data)) {
					t.Fatal("operation order/flags/payload extent differs", i)
				}
				expected := int32(0)
				if i == tc.failure {
					expected = tc.code
				}
				if got.Result != expected {
					t.Fatal("raw signed per-op boundary differs", i, got.Result, expected)
				}
				suffix := tc.failure == -2 || tc.failure >= 0 && i > tc.failure
				if suffix && len(got.Data) != 0 {
					t.Fatal("unexecuted/early ambiguous suffix contains output", i)
				}
				expectedCallback := got.Result
				if input.Code == cephmsgr.OSDStat && suffix {
					expectedCallback = -5
				}
				if input.Code == cephmsgr.OSDAssertVersion {
					if want.Request.Op != "assert" || want.Request.Version != input.Version || want.PrvalAvailable || want.Prval != nil {
						t.Fatal("C ASSERT_VER does not expose a raw/per-op prval")
					}
				} else if !want.PrvalAvailable || want.Prval == nil || *want.Prval != expectedCallback {
					t.Fatal("C convenience callback differs from bounded source expectation", i, want.Prval, expectedCallback)
				}
				if input.Code != cephmsgr.OSDStat && !bytes.Equal(got.Data, want.Data) {
					t.Fatal("per-operation native buffer output differs", i)
				}
				switch input.Code {
				case cephmsgr.OSDChecksum:
					if want.Request.Op != "checksum" || want.Request.Type != input.Checksum.Type || want.Request.Seed != input.Checksum.Seed || want.Request.Offset != input.Offset || want.Request.Length != input.Length || want.Request.Chunk != input.Checksum.ChunkSize {
						t.Fatal("native checksum inputs differ", i)
					}
					length := input.Length
					if tc.name == "trimmed" && i == 0 || tc.name == "unaligned-after-read" && i == 1 {
						length = 4
					}
					if got.Offset != input.Offset || got.Length != length || got.Checksum.Type != input.Checksum.Type || got.Checksum.ChunkSize != input.Checksum.ChunkSize {
						t.Fatal("returned CHECKSUM union differs", i)
					}
					if !reflect.DeepEqual(got.Checksum.Values, want.Values) && (len(got.Checksum.Values) != 0 || len(want.Values) != 0) {
						t.Fatal("validated prefix Checksum.Values differs", i)
					}
					if len(got.Data) > 0 {
						width := 4
						if input.Checksum.Type == 1 {
							width = 8
						}
						count := int(binary.LittleEndian.Uint32(got.Data))
						if len(got.Data) != 4+width*count || count != len(got.Checksum.Values) {
							t.Fatal("checksum count consumed adjacent payload")
						}
					}
				case cephmsgr.OSDRead:
					if want.Request.Op != "read" || want.Request.Offset != input.Offset || want.Request.Length != input.Length || got.Offset != input.Offset || got.Length != input.Length {
						t.Fatal("read extent differs", i)
					}
					if !suffix && (want.CountUnchanged || want.BytesRead != uint64(len(got.Data))) {
						t.Fatal("C read byte-count not established", i)
					}
				case cephmsgr.OSDStat:
					if want.Request.Op != "stat" {
						t.Fatal("native STAT request differs")
					}
					if suffix && (want.Size != (1<<63)-1 || want.Seconds != -7 || want.Nanos != -11) {
						t.Fatal("untouched STAT callback overwrote sentinel metadata", i)
					}
					if !suffix && (len(got.Data) != 16 || binary.LittleEndian.Uint64(got.Data) != want.Size || binary.LittleEndian.Uint32(got.Data[8:]) != uint32(want.Seconds) || binary.LittleEndian.Uint32(got.Data[12:]) != uint32(want.Nanos)) {
						t.Fatal("native STAT2 callback and raw stat payload differ")
					}
				case cephmsgr.OSDAssertVersion:
					if got.Offset != 0 || got.Length != input.Version {
						t.Fatal("ASSERT_VER reply union differs", i)
					}
				case cephmsgr.OSDCompareExtent:
					if want.Request.Op != "cmpext" || want.Request.Offset != input.Offset || !bytes.Equal(want.Request.Data, input.Data) {
						t.Fatal("native comparison guard differs")
					}
				}
				data = append(data, got.Data...)
			}
			if !bytes.Equal(data, reply.RawData) {
				t.Fatal("per-op payload split did not consume RawData exactly")
			}
			if tc.code < 0 {
				// A valid signed error preserves the same OSD connection for a fresh call.
				fresh := osdObjectRequest(route, read(0, 4))
				if isReplicatedPGFixtureProfile() {
					fresh = replicatedPGRequest(route, read(0, 4))
				}
				follow, followErr := o.Request(ctx, fresh)
				entry["fresh_read_reply"] = follow
				if tc.name == "missing" {
					if follow.Code != -2 || !errors.As(followErr, &remote) {
						t.Fatal("same-connection fresh absent read failed", followErr)
					}
				} else if followErr != nil || follow.Code != 0 || len(follow.Operations) != 1 || !bytes.Equal(follow.Operations[0].Data, checksumCompoundBinary[:4]) {
					t.Fatal("same-connection fresh read failed", followErr)
				}
				if follow.Submission.Transaction == reply.Submission.Transaction || follow.Submission.GlobalID != reply.Submission.GlobalID {
					t.Fatal("fresh call reused old identity/TID")
				}
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: main})
			entry["native_after_Go"] = after.State
			if !reflect.DeepEqual(after.State, owned.State) {
				t.Fatal("Go read-only compound changed native bytes/version/mtime")
			}
		}) {
			return
		}
	}
}

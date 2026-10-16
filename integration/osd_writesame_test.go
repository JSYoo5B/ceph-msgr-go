package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type writeSameNativeInput struct {
	Phase   string `json:"phase"`
	Object  string `json:"object"`
	Mirror  string `json:"mirror"`
	Exists  bool   `json:"exists"`
	Action  string `json:"action"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Pattern []byte `json:"pattern"`
	Flags   uint32 `json:"flags"`
	Snap    uint64 `json:"snap"`
}

type writeSameNativeReceipt struct {
	Commit       string               `json:"source_commit"`
	Phase        string               `json:"phase"`
	Object       string               `json:"object"`
	Mirror       string               `json:"mirror"`
	Before       nativeXattrSnapshot  `json:"before"`
	After        nativeXattrSnapshot  `json:"after"`
	MirrorBefore nativeXattrSnapshot  `json:"mirror_before"`
	MirrorAfter  nativeXattrSnapshot  `json:"mirror_after"`
	Clone        *nativeXattrSnapshot `json:"clone"`
	MirrorClone  *nativeXattrSnapshot `json:"mirror_clone"`
	Native       *struct {
		Code         int32  `json:"code"`
		Version      uint64 `json:"version"`
		RawWriteSame bool   `json:"raw_writesame_per_op_result_available"`
	} `json:"native"`
}

func nativeWriteSame(t *testing.T, ctx context.Context, input writeSameNativeInput) writeSameNativeReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-writesame-input.json"), raw); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-writesame", "compare"); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(dir, "osd-writesame-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result writeSameNativeReceipt
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Phase != input.Phase || result.Object != input.Object || result.Mirror != input.Mirror || !xattrSnapshotEqual(result.Before, result.After, true) {
		t.Fatal("native writesame identity or subject preservation", result)
	}
	if input.Phase == "execute" && (result.Native == nil || result.Native.RawWriteSame) {
		t.Fatal("missing or invented native writesame result")
	}
	return result
}

func TestCephOSDWriteSameIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_WRITESAME_ORACLE") != "1" {
		t.Skip("requires staged pinned public C++ WRITESAME oracle")
	}
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned writesame object/snapshot cleanup failed")
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("owned-writesame-baseline\x00\xff")}, Reserve: 1})
	prepared = true
	if len(owned.Objects) != 2 {
		t.Fatal("independent owned subject/mirror missing")
	}
	subject, mirror := owned.Objects[0], owned.Objects[1]
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": subject, "namespace": ""}).Placement
	connection := mutationConnection(t, c, ctx, route)
	defer connection.Close()
	type testCase struct {
		name, action   string
		offset, length uint64
		pattern        []byte
		exists, clip   bool
		flags          uint32
	}
	cases := []testCase{
		{name: "binary full prefix", action: "plain", length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "partial existing range", action: "plain", offset: 2, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "single pattern", action: "plain", offset: 1, length: 3, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "extension with hole", action: "plain", offset: 64, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "missing object create", action: "plain", offset: 3, length: 12, pattern: []byte{0, 255, 'x'}},
		{name: "zero existing prefix", action: "plain", pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "zero existing past end", action: "plain", offset: 64, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "zero missing prefix", action: "plain", pattern: []byte{0, 255, 'x'}},
		{name: "zero missing nonzero offset", action: "plain", offset: 64, pattern: []byte{0, 255, 'x'}},
		{name: "bounded one byte expansion", action: "plain", length: 4096, pattern: []byte{0xff}, exists: true},
		{name: "bounded maximum pattern", action: "plain", length: 4096, pattern: bytes.Repeat([]byte{0, 255, 0x80, 'x'}, 64), exists: true},
		{name: "ordered append and metadata", action: "append", offset: 2, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "ordered create append metadata", action: "append", length: 12, pattern: []byte{0, 255, 'x'}},
		{name: "guard rolls back staged pattern", action: "guard", offset: 2, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "known oversized offset", action: "plain", offset: 1 << 32, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "FAILOK oversized writesame commits append", action: "failok", offset: 1 << 32, length: 12, pattern: []byte{0, 255, 'x'}, exists: true, flags: 2},
		{name: "snapshot write context clone", action: "snapshot", offset: 2, length: 12, pattern: []byte{0, 255, 'x'}, exists: true},
		{name: "truncate sequence clips full prefix", action: "plain", length: 12, pattern: []byte{0, 255, 'x'}, exists: true, clip: true},
		{name: "truncate sequence clips partial range", action: "plain", offset: 3, length: 12, pattern: []byte{0, 255, 'x'}, exists: true, clip: true},
	}
	type evidence struct {
		Name        string                 `json:"name"`
		Input       writeSameNativeInput   `json:"input"`
		Seed        writeSameNativeReceipt `json:"seed"`
		Preparation []cephmsgr.OSDReply    `json:"go_truncate_preparation,omitempty"`
		Native      writeSameNativeReceipt `json:"native"`
		Go          cephmsgr.OSDReply      `json:"go_reply"`
		GoError     string                 `json:"go_error,omitempty"`
		After       writeSameNativeReceipt `json:"after"`
	}
	var receipts []evidence
	defer func() {
		raw, err := json.Marshal(receipts)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-writesame-results.json"), raw)
		}
		if err != nil {
			t.Error("writesame evidence", err)
		}
	}()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := writeSameNativeInput{Phase: "seed", Object: subject, Mirror: mirror, Exists: tc.exists, Action: tc.action, Offset: tc.offset, Length: tc.length, Pattern: tc.pattern, Flags: tc.flags}
			if tc.action == "snapshot" {
				input.Action = "plain"
			}
			seed := nativeWriteSame(t, ctx, input)
			row := evidence{Name: tc.name, Input: input, Seed: seed}
			index := len(receipts)
			receipts = append(receipts, row)
			if tc.clip {
				mirrorRoute := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": mirror, "namespace": ""}).Placement
				if mirrorRoute.ID != route.ID {
					t.Fatal("closed fixture clipping preparation needs one explicitly verified primary")
				}
				for _, target := range []osdFixtureOracle{route, mirrorRoute} {
					op := cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: 6, TruncateSize: 6, TruncateSequence: 2}
					req := osdObjectRequest(target, op)
					if isReplicatedPGFixtureProfile() {
						req = replicatedPGRequest(target, op)
					}
					prepared, err := connection.Request(ctx, req)
					receipts[index].Preparation = append(receipts[index].Preparation, prepared)
					if err != nil {
						t.Fatal("Go sequenced-TRUNCATE preparation", err)
					}
				}
				input.Phase = "observe"
				seed = nativeWriteSame(t, ctx, input)
			}
			if tc.action == "snapshot" {
				input.Action = "snapshot"
				input.Snap = nativeSnapCreate(t, ctx, base, 1)[0]
			}
			input.Phase = "execute"
			receipts[index].Input = input
			native := nativeWriteSame(t, ctx, input)
			receipts[index].Native = native
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteSame, Offset: tc.offset, Length: tc.length, Data: tc.pattern, Flags: cephmsgr.OSDOperationFlags(tc.flags)}
			ops := []cephmsgr.OSDOperation{op}
			expected := []int32{0}
			aggregate := int32(0)
			if tc.action == "guard" {
				ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker", Mode: cephmsgr.OSDXattrString, Comparison: cephmsgr.OSDXattrEqual}, Data: []byte("never")})
				expected = append(expected, -125)
				aggregate = -125
			}
			if tc.action == "append" || tc.action == "failok" {
				ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: 6, Data: []byte{'t', 'a', 'i', 'l', 0, 255}})
				expected = append(expected, 0)
			}
			if tc.action == "append" || tc.action == "guard" || tc.action == "failok" {
				ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte("writesame-native-marker")}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: "remove"}})
				expected = append(expected, 0, 0)
			}
			if tc.offset == 1<<32 {
				expected[0] = -27
				if tc.flags == 0 {
					aggregate = -27
				}
			}
			request := osdObjectRequest(route, ops...)
			if isReplicatedPGFixtureProfile() {
				request = replicatedPGRequest(route, ops...)
			}
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000001}
			if input.Snap > 0 {
				request.SnapContext = cephmsgr.OSDSnapContext{Sequence: input.Snap, Snapshots: []uint64{input.Snap}}
			}
			reply, err := connection.Request(ctx, request)
			receipts[index].Go = reply
			if err != nil {
				receipts[index].GoError = err.Error()
			}
			input.Phase = "observe"
			after := nativeWriteSame(t, ctx, input)
			receipts[index].After = after
			if native.Native.Code != aggregate || reply.Code != aggregate {
				t.Fatal("independent native aggregate differs", native.Native.Code, reply.Code, aggregate, err)
			}
			if aggregate < 0 {
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if !errors.As(err, &server) || server.Code != aggregate || errors.As(err, &unknown) {
					t.Fatal("known signed refusal", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID == 0 || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.MapEpoch != route.Epoch || reply.MapEpoch < route.Epoch || reply.Object != subject || reply.RetryAttempt != 0 || len(reply.Operations) != len(ops) || len(reply.RawData) != 0 {
				t.Fatal("raw writesame identity differs", reply)
			}
			for i, op := range reply.Operations {
				if op.Code != ops[i].Code || op.Result != expected[i] || op.PayloadLength != 0 || len(op.Data) != 0 {
					t.Fatal("per-op signed results", i, op, expected)
				}
			}
			ws := reply.Operations[0]
			if ws.Offset != tc.offset || ws.Length != tc.length || ws.WriteSamePatternLength != uint64(len(tc.pattern)) || ws.TruncateSize != 0 || ws.TruncateSequence != 0 {
				t.Fatal("logical/pattern union lost or clipped echo fabricated", ws)
			}
			if aggregate >= 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("successful vector lacks ONDISK")
			}
			if !xattrSnapshotEqual(after.After, native.MirrorAfter, false) {
				t.Fatal("Go complete bytes/stat2/attrs differ from independent native", after.After, native.MirrorAfter)
			}
			if aggregate < 0 && (!xattrSnapshotEqual(after.After, seed.After, true) || !xattrSnapshotEqual(native.MirrorBefore, native.MirrorAfter, true)) {
				t.Fatal("failed vector committed staged pattern or metadata")
			}
			if aggregate >= 0 && reply.UserVersion != after.After.Version {
				t.Fatal("receipt user version differs from native observation", reply.UserVersion, after.After.Version)
			}
			if tc.action == "snapshot" {
				if after.Clone == nil || after.MirrorClone == nil || !xattrSnapshotEqual(*after.Clone, seed.After, false) || !xattrSnapshotEqual(*after.MirrorClone, seed.MirrorAfter, false) {
					t.Fatal("snapshot context did not preserve original clone", after.Clone, after.MirrorClone)
				}
			}
			if tc.clip && len(after.After.Data) != 6 {
				t.Fatal("sequenced truncate clipping bytes", len(after.After.Data))
			}
			t.Logf("native=%d Go=%d results=%v logical=%d pattern=%d before_version=%d after_version=%d exists_before=%d exists_after=%d", native.Native.Code, reply.Code, expected, tc.length, len(tc.pattern), seed.After.Version, after.After.Version, seed.After.Code, after.After.Code)
		})
	}
}

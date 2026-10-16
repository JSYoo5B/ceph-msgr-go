package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type u64XattrNativeInput struct {
	Phase      string  `json:"phase"`
	Object     string  `json:"object"`
	Mirror     string  `json:"mirror"`
	Stored     *[]byte `json:"stored"`
	Exists     bool    `json:"exists"`
	Action     string  `json:"action"`
	Comparison uint8   `json:"comparison"`
	Operand    uint64  `json:"operand"`
	Flags      uint32  `json:"flags"`
}

type u64XattrNativeReceipt struct {
	Commit       string              `json:"source_commit"`
	Phase        string              `json:"phase"`
	Object       string              `json:"object"`
	Mirror       string              `json:"mirror"`
	Before       nativeXattrSnapshot `json:"before"`
	After        nativeXattrSnapshot `json:"after"`
	MirrorBefore nativeXattrSnapshot `json:"mirror_before"`
	MirrorAfter  nativeXattrSnapshot `json:"mirror_after"`
	Native       *struct {
		Code          int32  `json:"code"`
		Version       uint64 `json:"version"`
		RawComparison bool   `json:"raw_cmp_per_op_result_available"`
	} `json:"native"`
}

func nativeU64Xattr(t *testing.T, ctx context.Context, input u64XattrNativeInput) u64XattrNativeReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-u64-xattr-input.json"), data); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-u64-xattr", "compare"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-u64-xattr-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result u64XattrNativeReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Phase != input.Phase || result.Object != input.Object || result.Mirror != input.Mirror || !xattrSnapshotEqual(result.Before, result.After, true) {
		t.Fatal("native U64 identity or subject preservation differs", result)
	}
	if input.Phase == "execute" && (result.Native == nil || result.Native.RawComparison) {
		t.Fatal("missing or invented native U64 result")
	}
	return result
}

func TestCephOSDU64XattrIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_U64_XATTR_ORACLE") != "1" {
		t.Skip("requires staged pinned native C++ U64 comparison oracle")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
	defer cancel()
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			if !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
				t.Error("owned U64 comparison cleanup failed")
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("owned-u64-baseline\x00\xff")}, Reserve: 1})
	prepared = true
	if len(owned.Objects) != 2 {
		t.Fatal("U64 subject/mirror ownership missing")
	}
	subject, mirror := owned.Objects[0], owned.Objects[1]
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": subject, "namespace": ""}).Placement
	connection := mutationConnection(t, c, ctx, route)
	defer connection.Close()
	type testCase struct {
		name, action    string
		stored          *[]byte
		operand         uint64
		comparison      cephmsgr.OSDXattrComparison
		flags           cephmsgr.OSDOperationFlags
		exists          bool
		code, cmpResult int32
	}
	stored := func(s string) *[]byte { b := []byte(s); return &b }
	var cases []testCase
	const high uint64 = 1<<63 | 19
	for _, action := range []string{"read", "write"} {
		for comparison := cephmsgr.OSDXattrEqual; comparison <= cephmsgr.OSDXattrLessEqual; comparison++ {
			for index, operand := range []uint64{high - 1, high, high + 1} {
				truth := []bool{false, operand == high, operand != high, operand > high, operand >= high, operand < high, operand <= high}[comparison]
				code, cmp := int32(0), int32(1)
				if !truth {
					code, cmp = -125, -125
				} else if action == "read" {
					code = 1
				}
				cases = append(cases, testCase{name: fmt.Sprintf("%s/op-%d/order-%d", action, comparison, index-1), action: action, stored: stored(fmt.Sprint(high)), operand: operand, comparison: comparison, exists: true, code: code, cmpResult: cmp})
			}
		}
	}
	for _, tc := range []struct {
		name    string
		stored  *[]byte
		operand uint64
		code    int32
	}{
		{"max uint64", stored("18446744073709551615"), ^uint64(0), 1},
		{"empty stored zero", stored(""), 0, 1}, {"missing attr zero", nil, 0, 1},
		{"decimal prefix", stored("19suffix"), 19, 1}, {"NUL suffix", stored("19\x00tail"), 19, 1}, {"leading zeros", stored("00019"), 19, 1},
		{"whitespace", stored(" 19"), 19, -22}, {"plus sign", stored("+19"), 19, -22}, {"minus sign", stored("-19"), 19, -22},
		{"non numeric", stored("bad"), 0, -22}, {"NUL prefix", stored("\x0019"), 19, -22}, {"overflow", stored("18446744073709551616"), 0, -22},
	} {
		cases = append(cases, testCase{name: tc.name, action: "read", stored: tc.stored, operand: tc.operand, comparison: cephmsgr.OSDXattrEqual, exists: true, code: tc.code, cmpResult: tc.code})
	}
	cases = append(cases,
		testCase{name: "missing object", action: "read", comparison: cephmsgr.OSDXattrEqual, exists: false, code: -2, cmpResult: 0},
		testCase{name: "staged data rollback", action: "staged-write", stored: stored("19"), operand: 18, comparison: cephmsgr.OSDXattrEqual, exists: true, code: -125, cmpResult: -125},
		testCase{name: "FAILOK false guard commits", action: "write", stored: stored("19"), operand: 18, comparison: cephmsgr.OSDXattrEqual, flags: cephmsgr.OSDFailOK, exists: true, cmpResult: -125},
		testCase{name: "FAILOK invalid numeric commits", action: "write", stored: stored("bad"), operand: 18, comparison: cephmsgr.OSDXattrEqual, flags: cephmsgr.OSDFailOK, exists: true, cmpResult: -22},
	)
	type evidence struct {
		Name   string                `json:"name"`
		Input  u64XattrNativeInput   `json:"input"`
		Native u64XattrNativeReceipt `json:"native"`
		Go     cephmsgr.OSDReply     `json:"go_reply"`
		After  u64XattrNativeReceipt `json:"after"`
	}
	var receipts []evidence
	defer func() {
		data, err := json.Marshal(receipts)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-u64-xattr-results.json"), data)
		}
		if err != nil {
			t.Error("U64 comparison evidence", err)
		}
	}()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := u64XattrNativeInput{Phase: "seed", Object: subject, Mirror: mirror, Stored: tc.stored, Exists: tc.exists, Action: tc.action, Comparison: uint8(tc.comparison), Operand: tc.operand, Flags: uint32(tc.flags)}
			seed := nativeU64Xattr(t, ctx, input)
			input.Phase = "execute"
			native := nativeU64Xattr(t, ctx, input)
			if native.Native.Code != tc.code || !xattrSnapshotEqual(seed.After, native.Before, true) {
				t.Fatal("independent native result differs", native.Native, tc.code)
			}
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, tc.operand)
			ops := []cephmsgr.OSDOperation{}
			if tc.action == "staged-write" {
				ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 1, Length: 3, Data: []byte{'N', 0, 255}})
			}
			cmpIndex := len(ops)
			ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareXattr, Flags: tc.flags, Xattr: cephmsgr.OSDXattrInput{Name: "number", Comparison: tc.comparison, Mode: cephmsgr.OSDXattrU64}, Data: data})
			if tc.action != "read" {
				ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "marker"}, Data: []byte("u64-native-marker")}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: "remove"}})
			}
			request := osdObjectRequest(route, ops...)
			if tc.action != "read" {
				request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000001}
			}
			reply, err := connection.Request(ctx, request)
			if tc.code < 0 {
				var server *cephmsgr.OSDError
				var unknown *cephmsgr.OutcomeUnknownError
				if !errors.As(err, &server) || server.Code != tc.code || errors.As(err, &unknown) {
					t.Fatal("known numeric refusal differs", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if reply.Code != tc.code || len(reply.Operations) != len(ops) || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.MapEpoch < route.Epoch || reply.Object != subject || reply.RetryAttempt != 0 || len(reply.RawData) != 0 {
				t.Fatal("numeric reply identity/result lost", reply)
			}
			result := reply.Operations[cmpIndex]
			if result.Result != tc.cmpResult || result.Xattr.ComparisonMode != 2 || result.Xattr.ValueLength != 8 || len(result.Data) != 0 {
				t.Fatal("raw numeric union/result lost", result)
			}
			if tc.action != "read" && tc.code >= 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("numeric guarded write lacks ONDISK")
			}
			input.Phase = "observe"
			after := nativeU64Xattr(t, ctx, input)
			if !xattrSnapshotEqual(after.After, native.MirrorAfter, false) {
				t.Fatal("Go guarded effects differ from independent native", after.After, native.MirrorAfter)
			}
			if tc.action == "read" || tc.code < 0 {
				if !xattrSnapshotEqual(after.After, seed.After, true) || !xattrSnapshotEqual(native.MirrorAfter, native.MirrorBefore, true) {
					t.Fatal("read/refused vector changed object state")
				}
				if tc.code >= 0 && reply.UserVersion != seed.After.Version {
					t.Fatal("read user version differs")
				}
			} else if after.After.Version <= seed.After.Version || native.MirrorAfter.Version <= native.MirrorBefore.Version {
				t.Fatal("successful guarded write did not advance version")
			}
			if tc.name == "staged data rollback" && !bytes.Equal(after.After.Data, seed.After.Data) {
				t.Fatal("failed numeric guard retained staged data")
			}
			receipts = append(receipts, evidence{tc.name, input, native, reply, after})
			t.Logf("%s: native=%d Go=%d per-op=%d mode=%d caller=%d", tc.name, native.Native.Code, reply.Code, result.Result, result.Xattr.ComparisonMode, tc.operand)
		})
	}
}

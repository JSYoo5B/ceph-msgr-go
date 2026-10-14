package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func nativeReadonlyXattr(t *testing.T, ctx context.Context, oracle osdFixtureOracle, input xattrNativeInput) xattrNativeResult {
	t.Helper()
	input.ClientRole = "readonly"
	result := nativeXattrCompare(t, ctx, oracle, input)
	// Preserve this validated, non-secret receipt before a later observation
	// or deferred cleanup replaces the generic native result file.
	if len(input.Operations) != 1 {
		t.Fatal("permission fixture requires one independently observed native operation")
	}
	op := input.Operations[0].Op
	if op != "list" && op != "set" && op != "remove" {
		t.Fatal("unsupported permission receipt phase")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := os.ReadFile(filepath.Join(dir, "osd-xattr-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-xattr-permission-"+op+".json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("native client.readonly %s aggregate=%d mirror version %d->%d unchanged=%t", op, result.Vector.Code, result.Before.Version, result.After.Version, xattrSnapshotEqual(result.Before, result.After, true))
	return result
}

func TestCephOSDXattrPermissionsIntegration(t *testing.T) {
	admin, _, oracle, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	name := "go-xattr-permission-\xff"
	value := []byte{0, 0xff, 'r', 'e', 'a', 'd', 0, 10}
	b64 := base64.StdEncoding.EncodeToString
	observation := xattrNativeInput{Exists: true, Kind: "read", Names: []string{b64([]byte(name))}}
	initial := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if initial.Code != 0 || !bytes.Equal(initial.Data, original) {
		t.Fatal("native permission baseline differs from fixture")
	}
	if _, exists := xattrInventory(initial)[name]; exists {
		t.Fatal("owned permission attribute already exists")
	}
	writer := mutationConnection(t, admin, ctx, oracle)
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := writer.Close(); err != nil {
			t.Error("close permission seed handle", err)
		}
		cleanup, err := admin.OpenOSD(restore, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
		if err != nil {
			t.Error("open explicitly targeted permission cleanup", err)
			return
		}
		defer cleanup.Close()
		r := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: oracle.Seconds, Nanoseconds: oracle.Nanoseconds}
		if _, err := cleanup.Request(restore, r); err != nil {
			t.Error("remove owned permission attr and restore fixture mtime", err)
			return
		}
		after := nativeXattrCompare(t, restore, oracle, observation).Actual
		if !xattrSnapshotEqual(initial, after, false) {
			t.Error("permission cleanup changed original bytes/attrs/mtime")
		}
	}()
	seed := osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value})
	seed.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000700, Nanoseconds: 123456789}
	seedReply, err := writer.Request(ctx, seed)
	if err != nil || seedReply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("privileged seed for permission test", err)
	}
	before := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if before.Version != seedReply.UserVersion || !bytes.Equal(before.Data, original) || xattrInventory(before)[name] != string(value) {
		t.Fatal("native seed inventory/version differs")
	}
	options := integrationOptions(t)
	options.EnableOSD, options.Identity = true, "client.readonly"
	key, err := os.ReadFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
	clear(key)
	if err != nil {
		t.Fatal("parse fixture readonly key", err)
	}
	reader, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate explicit readonly identity", err)
	}
	defer reader.Close()
	o := mutationConnection(t, reader, ctx, oracle)
	defer o.Close()
	all := cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 64 << 10}}
	read := func(t *testing.T) cephmsgr.OSDReply {
		t.Helper()
		r, err := o.Request(ctx, osdObjectRequest(oracle,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name, MaxOutputBytes: uint32(len(value))}}, all))
		if err != nil {
			t.Fatal("GET/GETALL under native allow-r capability", err)
		}
		checkOSDMetadata(t, r, oracle)
		if len(r.Operations) != 2 || r.Operations[0].Result != 0 || r.Operations[1].Result != 0 || !bytes.Equal(r.Operations[0].Data, value) || r.Operations[0].Xattr.NameLength != uint32(len(name)) || r.Operations[0].Xattr.ValueLength != uint32(len(value)) || !reflect.DeepEqual(xattrEncodedInventory(t, r.Operations[1].Data), xattrInventory(before)) {
			t.Fatal("readonly binary GET or opaque map differs from independent native inventory")
		}
		if !bytes.Equal(r.RawData, append(bytes.Clone(r.Operations[0].Data), r.Operations[1].Data...)) {
			t.Fatal("readonly xattr output boundaries differ from raw data")
		}
		return r
	}
	initialRead := read(t)
	nativeInput := xattrNativeInput{Compare: true, Exists: true, Kind: "read", Baseline: b64(original), Attributes: before.Attributes, Names: observation.Names, Operations: []xattrNativeAction{{Op: "list"}}}
	nativeRead := nativeReadonlyXattr(t, ctx, oracle, nativeInput)
	if nativeRead.Vector.Code != 0 || len(nativeRead.Vector.Lists) != 1 || nativeRead.Vector.Lists[0].Code != 0 || !reflect.DeepEqual(xattrInventory(nativeXattrSnapshot{Attributes: nativeRead.Vector.Lists[0].Entries}), xattrInventory(before)) || !xattrSnapshotEqual(nativeRead.Before, nativeRead.After, true) {
		t.Fatal("independent readonly native list changed state or differed")
	}
	for _, tc := range []struct {
		name   string
		op     cephmsgr.OSDOperation
		native xattrNativeAction
	}{
		{name: "SET denied", op: cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: []byte{0xff, 0, 'x'}}, native: xattrNativeAction{Op: "set", Name: b64([]byte(name)), Value: b64([]byte{0xff, 0, 'x'})}},
		{name: "RM denied", op: cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}}, native: xattrNativeAction{Op: "remove", Name: b64([]byte(name))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := nativeInput
			input.Kind, input.Seconds, input.Nanos, input.Operations = "write", 1700000701, 987654321, []xattrNativeAction{tc.native}
			native := nativeReadonlyXattr(t, ctx, oracle, input)
			// Tentacle's object admission uses EPERM, independently of the
			// command transport's EACCES. Verify the observed native code.
			// PrimaryLogPG.cc at the pinned source, lines 2083-2084.
			if native.Vector.Code != -1 || !xattrSnapshotEqual(native.Before, native.After, true) {
				t.Fatalf("native readonly write aggregate=%d; want EPERM=-1 with unchanged mirror", native.Vector.Code)
			}
			request := osdObjectRequest(oracle, tc.op)
			request.ClientIncarnation = 53
			request.ModificationTime = cephmsgr.OSDTime{Seconds: input.Seconds, Nanoseconds: input.Nanos}
			reply, err := o.Request(ctx, request)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &server) || server.Code != -1 || errors.As(err, &unknown) || reply.Code != -1 || len(reply.Operations) != 1 || reply.Transaction <= initialRead.Transaction || reply.Submission != (cephmsgr.OSDSubmission{Transaction: reply.Transaction, GlobalID: reader.Snapshot().GlobalID, ClientIncarnation: 53, MapEpoch: oracle.Epoch}) || len(reply.RawFront) == 0 || len(reply.RawData) != 0 {
				t.Fatal("known permission refusal lost raw result/local submission or became unknown", reply, err)
			}
			op := reply.Operations[0]
			// Capability refusal may precede operation execution. Preserve the
			// returned raw per-op code; do not synthesize -1 from aggregate.
			if op.Code != tc.op.Code || op.Xattr.NameLength != uint32(len(name)) || op.Xattr.ValueLength != uint32(len(tc.op.Data)) || op.PayloadLength != 0 || len(op.Data) != 0 {
				t.Fatal("permission refusal lost original packed operation metadata", op)
			}
			t.Logf("native and Go aggregate EPERM=-1; raw Go per-op=%d completion flags=%#x; C API raw per-op unavailable", op.Result, reply.Flags)
			after := nativeXattrCompare(t, ctx, oracle, observation).Actual
			if !xattrSnapshotEqual(before, after, true) {
				t.Fatal("denied Go SET/RM changed independent native bytes/attrs/version/mtime")
			}
			later := read(t)
			if later.Transaction <= reply.Transaction {
				t.Fatal("same readonly session did not remain usable after known permission refusal")
			}
			evidence := struct {
				NativeCode  int32               `json:"native_aggregate"`
				Reply       cephmsgr.OSDReply   `json:"go_reply"`
				Unknown     bool                `json:"outcome_unknown"`
				Before      nativeXattrSnapshot `json:"actual_before"`
				After       nativeXattrSnapshot `json:"actual_after"`
				RecoveryTID uint64              `json:"same_session_read_tid"`
				RecoveryGID uint64              `json:"same_session_global_id"`
			}{NativeCode: native.Vector.Code, Reply: reply, Unknown: errors.As(err, &unknown), Before: before, After: after, RecoveryTID: later.Transaction, RecoveryGID: later.GlobalID}
			data, err := json.Marshal(evidence)
			if err != nil || len(data) > 512<<10 {
				t.Fatal("permission observation exceeds evidence bound", err)
			}
			if err := os.WriteFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-xattr-permission-go-"+tc.native.Op+".json"), data, 0644); err != nil {
				t.Fatal(err)
			}
		})
		if t.Failed() {
			break
		}
	}
	if t.Failed() {
		return
	}
	t.Log("native allow-r accepted binary GET/GETALL and refused SET/RM with known EPERM; per-op output remains raw and no ONDISK condition is imposed on refusal; no automatic retry or product native dependency")
}

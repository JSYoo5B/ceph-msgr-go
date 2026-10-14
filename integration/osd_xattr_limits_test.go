package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// Both the caller-targeted object and native mirror can map to any owned OSD.
// The fixture gates every owned daemon's effective limits before this test sends.
func TestCephOSDXattrServerLimitsIntegration(t *testing.T) {
	c, _, oracle, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	o := mutationConnection(t, c, ctx, oracle)
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	b64 := base64.StdEncoding.EncodeToString
	names := []string{"go-xattr-limit-value", "go-xattr-limit-stage", strings.Repeat("n", 32), strings.Repeat("n", 33)}
	encoded := make([]string, len(names))
	for i, name := range names {
		encoded[i] = b64([]byte(name))
	}
	observe := xattrNativeInput{Exists: true, Kind: "read", Names: encoded}
	initial := nativeXattrCompare(t, ctx, oracle, observe).Actual
	if initial.Code != 0 || !bytes.Equal(initial.Data, original) {
		t.Fatal("native initial bytes differ")
	}
	for _, entry := range initial.Attributes {
		if len(entry.Name) > 32 || len(entry.Value) > 32 {
			t.Fatal("native baseline attribute exceeds configured server limits")
		}
	}
	for _, name := range names {
		if _, ok := xattrInventory(initial)[name]; ok {
			t.Fatal("server-limit test name already exists", name)
		}
	}
	set := func(name string, value []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value}
	}
	remove := func(name string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}}
	}
	active := false
	restoreLimits := func(ctx context.Context) error {
		if !active {
			return nil
		}
		if err := testcluster.ControlDaemon(ctx, dir, "limit", "osd-xattr", "restore"); err != nil {
			return err
		}
		active = false
		return nil
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if err := restoreLimits(cleanupCtx); err != nil {
			t.Error("restore owned server limits", err)
			return
		}
		_ = o.Close()
		cleanup, err := c.OpenOSD(cleanupCtx, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
		if err != nil {
			t.Error(err)
			return
		}
		defer cleanup.Close()
		var ops []cephmsgr.OSDOperation
		for _, name := range names {
			ops = append(ops, remove(name))
		}
		r := osdObjectRequest(oracle, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: oracle.Seconds, Nanoseconds: oracle.Nanoseconds}
		if _, err := cleanup.Request(cleanupCtx, r); err != nil {
			t.Error("remove owned limit-test attributes/restore mtime", err)
			return
		}
		after := nativeXattrCompare(t, cleanupCtx, oracle, observe).Actual
		if !xattrSnapshotEqual(initial, after, false) {
			t.Error("limit test cleanup changed original bytes/attrs/mtime")
		}
	}()
	if err := testcluster.ControlDaemon(ctx, dir, "limit", "osd-xattr", "apply"); err != nil {
		t.Fatal("apply effective server limits", err)
	}
	active = true
	readReceipt := func() xattrLimitNativeReceipt {
		t.Helper()
		var result xattrLimitNativeReceipt
		f, err := os.Open(filepath.Join(dir, "osd-xattr-limits.json"))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		data, err := io.ReadAll(io.LimitReader(f, 16385))
		if err != nil || len(data) > 16384 {
			t.Fatal("limit receipt exceeds bound", err)
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	receipt := readReceipt()
	if receipt.Schema != 1 || receipt.Section != "osd" || len(receipt.Prior) != 2 || len(receipt.Before) != oracle.Replicas || len(receipt.Owned) != oracle.Replicas || len(receipt.Before) != len(receipt.Applied) || receipt.Before["osd."+strconv.Itoa(int(oracle.ID))] == nil {
		t.Fatal("effective owned OSD receipt identity differs")
	}
	for daemon, options := range receipt.Applied {
		if len(options) != 2 || options["osd_max_attr_size"] != "32" || options["osd_max_attr_name_len"] != "32" || receipt.Before[daemon] == nil || receipt.Owned[daemon] == "" {
			t.Fatal("every owned OSD must enforce both server limits", daemon, options)
		}
	}
	type limitCase struct {
		name    string
		ops     []cephmsgr.OSDOperation
		code    int32
		results []int32
	}
	cases := []limitCase{
		{"value 32 accepted", []cephmsgr.OSDOperation{set(names[0], bytes.Repeat([]byte{0xff}, 32))}, 0, []int32{0}},
		{"value 33 EFBIG", []cephmsgr.OSDOperation{set(names[0], bytes.Repeat([]byte{0xff}, 33))}, -27, []int32{-27}},
		{"name 32 accepted", []cephmsgr.OSDOperation{set(names[2], []byte{0, 0xff})}, 0, []int32{0}},
		{"name 33 ENAMETOOLONG", []cephmsgr.OSDOperation{set(names[3], []byte{0, 0xff})}, -36, []int32{-36}},
		{"earlier SET rolled back by value refusal", []cephmsgr.OSDOperation{set(names[1], []byte("staged")), set(names[0], bytes.Repeat([]byte{0xff}, 33))}, -27, []int32{0, -27}},
		{"earlier SET rolled back by name refusal", []cephmsgr.OSDOperation{set(names[1], []byte("staged")), set(names[3], []byte{0, 0xff})}, -36, []int32{0, -36}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reset []cephmsgr.OSDOperation
			for _, name := range names {
				reset = append(reset, remove(name))
			}
			seed := osdObjectRequest(oracle, reset...)
			seed.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000000, Nanoseconds: 123456789}
			if _, err := o.Request(ctx, seed); err != nil {
				t.Fatal("reset owned attrs", err)
			}
			input := xattrNativeInput{Compare: true, Exists: true, Kind: "write", Baseline: b64(original), Attributes: initial.Attributes, Names: encoded, Seconds: 1700000001, Nanos: 987654321}
			for _, op := range tc.ops {
				input.Operations = append(input.Operations, xattrNativeAction{Op: "set", Name: b64([]byte(op.Xattr.Name)), Value: b64(op.Data)})
			}
			native := nativeXattrCompare(t, ctx, oracle, input)
			if native.Vector.Code != tc.code {
				t.Fatal("independent native server aggregate differs", native.Vector.Code, tc.code)
			}
			r := osdObjectRequest(oracle, tc.ops...)
			r.ModificationTime = cephmsgr.OSDTime{Seconds: input.Seconds, Nanoseconds: input.Nanos}
			reply, err := o.Request(ctx, r)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code || errors.As(err, &unknown) {
					t.Fatal("native-known refusal changed to unknown/local rejection", err)
				}
			} else if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("successful server boundary lacks ONDISK", err)
			}
			if reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.ClientIncarnation != r.ClientIncarnation || reply.Submission.MapEpoch != r.MapEpoch || reply.Submission.Attempt != 0 || reply.Code != tc.code || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Object != oracle.Name || reply.MapEpoch < oracle.Epoch || reply.PG.Pool != uint64(oracle.Pool) || reply.PG.Seed != oracle.Hash || len(reply.Operations) != len(tc.ops) {
				t.Fatal("raw limit reply identity/aggregate differs", reply)
			}
			for i, result := range reply.Operations {
				if result.Result != tc.results[i] || result.Code != cephmsgr.OSDSetXattr || result.Xattr.NameLength != uint32(len(tc.ops[i].Xattr.Name)) || result.Xattr.ValueLength != uint32(len(tc.ops[i].Data)) || result.PayloadLength != 0 || len(result.Data) != 0 {
					t.Fatal("raw server per-op result/union differs", i, result)
				}
			}
			after := nativeXattrCompare(t, ctx, oracle, observe).Actual
			if !xattrSnapshotEqual(after, native.After, false) {
				t.Fatal("Go state differs from independent native limit vector")
			}
			if tc.code < 0 {
				if !xattrSnapshotEqual(after, native.Actual, true) {
					t.Fatal("server refusal changed bytes/attrs/version/mtime")
				}
			} else if after.Version <= native.Actual.Version || after.Seconds != int64(input.Seconds) || after.Nanos != int64(input.Nanos) {
				t.Fatal("successful limit commit lacks native version/mtime")
			}
		})
		if t.Failed() {
			break
		}
	}
	if err := restoreLimits(ctx); err != nil {
		t.Fatal("restore effective limits", err)
	}
	restored := readReceipt()
	if !reflect.DeepEqual(receipt.Prior, restored.Prior) || !reflect.DeepEqual(receipt.Before, restored.Restored) {
		t.Fatal("server limits were not restored to exact prior effective values")
	}
}

// Only the selected public config keys and owned daemon identities are emitted.
type xattrLimitNativeReceipt struct {
	Schema   int                          `json:"schema_version"`
	Section  string                       `json:"section"`
	Owned    map[string]string            `json:"owned_osds"`
	Prior    map[string]json.RawMessage   `json:"prior_entries"`
	Before   map[string]map[string]string `json:"effective_before"`
	Applied  map[string]map[string]string `json:"effective_applied"`
	Restored map[string]map[string]string `json:"effective_restored"`
}

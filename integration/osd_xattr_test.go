package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type xattrNativeAction struct {
	Op         string `json:"op"`
	Name       string `json:"name,omitempty"`
	Value      string `json:"value,omitempty"`
	Comparison string `json:"comparison,omitempty"`
	Flags      uint32 `json:"flags"`
}

// SET/CMP empty operands must be explicit base64 strings, not JSON null or
// omitted values. Names use base64 too, preserving non-UTF-8 byte identity.
func (a xattrNativeAction) MarshalJSON() ([]byte, error) {
	result := map[string]any{"op": a.Op, "flags": a.Flags}
	if a.Op != "list" {
		result["name"] = a.Name
	}
	if a.Op == "set" || a.Op == "cmp" {
		result["value"] = a.Value
	}
	if a.Op == "cmp" {
		result["comparison"] = a.Comparison
	}
	return json.Marshal(result)
}

type xattrNativeInput struct {
	Object     string              `json:"object,omitempty"`
	Namespace  string              `json:"namespace,omitempty"`
	ClientRole string              `json:"client_role,omitempty"`
	Compare    bool                `json:"compare"`
	Exists     bool                `json:"exists"`
	Kind       string              `json:"kind"`
	Baseline   string              `json:"baseline"`
	Attributes []nativeXattrEntry  `json:"attributes"`
	Names      []string            `json:"get_names"`
	Operations []xattrNativeAction `json:"operations"`
	Seconds    uint32              `json:"seconds"`
	Nanos      uint32              `json:"nanos"`
}

type xattrNativeResult struct {
	Schema        int    `json:"schema_version"`
	Commit        string `json:"source_commit"`
	DaemonVersion string `json:"fixture_daemon_version"`
	Fixture       struct {
		Pool   string `json:"pool"`
		ID     int64  `json:"pool_id"`
		Object string `json:"object"`
	} `json:"fixture"`
	Object         string              `json:"object"`
	Namespace      string              `json:"namespace"`
	ClientIdentity string              `json:"client_identity"`
	Actual         nativeXattrSnapshot `json:"actual"`
	Before         nativeXattrSnapshot `json:"before"`
	After          nativeXattrSnapshot `json:"after"`
	Vector         struct {
		Code          int32 `json:"code"`
		RawComparison bool  `json:"raw_cmp_per_op_result_available"`
		Lists         []struct {
			Index   int                `json:"index"`
			Code    int32              `json:"code"`
			Entries []nativeXattrEntry `json:"entries"`
		} `json:"lists"`
	} `json:"vector"`
	Scope struct {
		RawComparison bool `json:"raw_cmp_per_op_result_available"`
		RawMutation   bool `json:"raw_set_remove_per_op_result_available"`
		CompoundGet   bool `json:"single_get_compound_available"`
	} `json:"scope"`
	Cleanup struct {
		Touched bool   `json:"mirror_touched"`
		Removed bool   `json:"object_removed"`
		Code    *int32 `json:"code"`
	} `json:"cleanup"`
}

func nativeXattrCompare(t *testing.T, ctx context.Context, oracle osdFixtureOracle, input xattrNativeInput) xattrNativeResult {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if input.Attributes == nil {
		input.Attributes = []nativeXattrEntry{}
	}
	if input.Operations == nil {
		input.Operations = []xattrNativeAction{}
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-xattr-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-xattr-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-xattr", "compare"); err != nil {
		t.Fatal("native xattr comparison", err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-xattr-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err = io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("native xattr receipt exceeds bound", err)
	}
	var result xattrNativeResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	object := input.Object
	if object == "" {
		object = oracle.Name
	}
	if result.Schema != 1 || result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || result.Fixture.ID != oracle.Pool || result.Fixture.Object != oracle.Name || result.Object != object || result.Namespace != input.Namespace || !strings.Contains(result.DaemonVersion, result.Commit) || !strings.Contains(result.DaemonVersion, "20.2.4") {
		t.Fatal("native xattr receipt differs from selected fixture/source", result)
	}
	if input.ClientRole == "readonly" && result.ClientIdentity != "client.readonly" {
		t.Fatal("native xattr receipt differs from selected read-only identity", result.ClientIdentity)
	}
	if result.Scope.RawComparison || result.Scope.RawMutation || result.Scope.CompoundGet || result.Vector.RawComparison {
		t.Fatal("native C API invented unavailable raw results", result.Scope)
	}
	if input.Compare {
		if !result.Cleanup.Touched || !result.Cleanup.Removed || result.Cleanup.Code == nil || *result.Cleanup.Code != 0 && *result.Cleanup.Code != -2 {
			t.Fatal("native mirror cleanup failed", result.Cleanup)
		}
	} else if result.Cleanup.Touched {
		t.Fatal("read-only observation mutated native mirror", result.Cleanup)
	}
	return result
}

func xattrSnapshotEqual(a, b nativeXattrSnapshot, versions bool) bool {
	return a.Code == b.Code && (!versions || a.Version == b.Version) && a.Seconds == b.Seconds && a.Nanos == b.Nanos && bytes.Equal(a.Data, b.Data) && reflect.DeepEqual(xattrInventory(a), xattrInventory(b))
}

func xattrEncodedInventory(t *testing.T, data []byte) map[string]string {
	t.Helper()
	d := wire.NewDecoder(data)
	count := d.Count(8, 1024)
	result := make(map[string]string, count)
	previous := ""
	for i := 0; i < count; i++ {
		name, value := string(d.Bytes()), string(d.Bytes())
		if name == "" || i > 0 && name <= previous {
			t.Fatal("native encoded xattr map has invalid name/order")
		}
		result[name], previous = value, name
	}
	if err := d.Done(); err != nil {
		t.Fatal("bounded test-only xattr map decoder", err)
	}
	return result
}

func TestCephOSDXattrsIntegration(t *testing.T) {
	c, _, oracle, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	o := mutationConnection(t, c, ctx, oracle)
	b64 := base64.StdEncoding.EncodeToString
	names := []string{"go-xattr-binary", "go-xattr-empty", "_go-xattr-prefixed", "go-xattr-\xff", "go-xattr-compare", "go-xattr-marker", "go-xattr-missing", "go-xattr-removed"}
	encodedNames := make([]string, len(names))
	for i, name := range names {
		encodedNames[i] = b64([]byte(name))
	}
	observation := xattrNativeInput{Exists: true, Kind: "read", Names: encodedNames}
	initial := nativeXattrCompare(t, ctx, oracle, observation).Actual
	if initial.Code != 0 || !bytes.Equal(initial.Data, original) {
		t.Fatal("initial native object differs from fixture bytes", initial.Code)
	}
	for _, name := range names {
		if _, exists := xattrInventory(initial)[name]; exists {
			t.Fatal("isolated test xattr already exists", []byte(name))
		}
	}
	set := func(name string, value []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value}
	}
	remove := func(name string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}}
	}
	get := func(name string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name, MaxOutputBytes: 1024}}
	}
	all := cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 64 << 10}}
	cmp := func(name string, comparison cephmsgr.OSDXattrComparison, operand []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareXattr, Xattr: cephmsgr.OSDXattrInput{Name: name, Comparison: comparison, Mode: cephmsgr.OSDXattrString}, Data: operand}
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		// Cleanup is a separate caller-selected operation, never a replay of
		// the tested vector. A failed session must not prevent fixture repair.
		if err := o.Close(); err != nil {
			t.Error("close xattr acceptance session before cleanup", err)
		}
		cleanup, err := c.OpenOSD(restore, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
		if err != nil {
			t.Error("open explicitly targeted xattr cleanup session", err)
			return
		}
		defer cleanup.Close()
		ops := make([]cephmsgr.OSDOperation, 0, len(names)+1)
		for _, name := range names {
			ops = append(ops, remove(name))
		}
		ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(original)), Data: original})
		r := osdObjectRequest(oracle, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: oracle.Seconds, Nanoseconds: oracle.Nanoseconds}
		if _, err := cleanup.Request(restore, r); err != nil {
			t.Error("restore fixture bytes/mtime and remove owned test attrs", err)
			return
		}
		after := nativeXattrCompare(t, restore, oracle, observation).Actual
		if !xattrSnapshotEqual(initial, after, false) {
			t.Error("fixture cleanup changed preexisting bytes/attrs/mtime")
		}
	}()
	baseline := []byte{0, 0xff, 'x', 'a', 't', 't', 'r', 0, 10, 0x80, 'z'}
	attrs := map[string][]byte{names[0]: {0, 'A', 0xff, 'B', 0}, names[1]: {}, names[2]: {0xff, 0, 'p'}, names[3]: {0, 0xff}, names[4]: []byte("m\x00z"), names[7]: []byte("remove-me")}
	nativeAttrs := func(values map[string][]byte) []nativeXattrEntry {
		allAttrs := make(map[string][]byte, len(values)+len(initial.Attributes))
		for _, entry := range initial.Attributes {
			allAttrs[string(entry.Name)] = entry.Value
		}
		for name, value := range values {
			allAttrs[name] = value
		}
		keys := make([]string, 0, len(allAttrs))
		for name := range allAttrs {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		result := make([]nativeXattrEntry, 0, len(keys))
		for _, name := range keys {
			value := append([]byte{}, allAttrs[name]...)
			result = append(result, nativeXattrEntry{Name: []byte(name), Value: value})
		}
		return result
	}
	seed := func(t *testing.T) {
		t.Helper()
		ops := []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWriteFull, Length: uint64(len(baseline)), Data: baseline}}
		for _, name := range names {
			ops = append(ops, remove(name))
		}
		for _, name := range names {
			if value, exists := attrs[name]; exists {
				ops = append(ops, set(name, value))
			}
		}
		r := osdObjectRequest(oracle, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000000, Nanoseconds: 123456789}
		if reply, err := o.Request(ctx, r); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("seed xattr native baseline", err)
		}
	}
	operators := []string{"", "eq", "ne", "gt", "gte", "lt", "lte"}
	nativeOps := func(ops []cephmsgr.OSDOperation) []xattrNativeAction {
		result := make([]xattrNativeAction, 0, len(ops))
		for _, op := range ops {
			action := xattrNativeAction{Name: b64([]byte(op.Xattr.Name)), Value: b64(op.Data), Flags: uint32(op.Flags)}
			switch op.Code {
			case cephmsgr.OSDCompareXattr:
				action.Op, action.Comparison = "cmp", operators[op.Xattr.Comparison]
			case cephmsgr.OSDSetXattr:
				action.Op = "set"
			case cephmsgr.OSDRemoveXattr:
				action.Op = "remove"
			case cephmsgr.OSDGetXattrs:
				action.Op = "list"
			default:
				t.Fatal("operation unavailable in native C compound API", op.Code)
			}
			result = append(result, action)
		}
		return result
	}
	type xattrCase struct {
		name    string
		ops     []cephmsgr.OSDOperation
		results []int32
		code    int32
		write   bool
		missing bool
	}
	cases := []xattrCase{{name: "binary empty underscore nonUTF8 SET and RM", write: true,
		ops: []cephmsgr.OSDOperation{set(names[0], []byte{0xff, 0, 'r', 0}), set(names[1], nil), set(names[2], []byte{0, 0xff}), set(names[3], []byte{0xff, 0, 10}), remove(names[7])}, results: []int32{0, 0, 0, 0, 0}}}
	for _, kind := range []string{"read", "write"} {
		for comparison := cephmsgr.OSDXattrEqual; comparison <= cephmsgr.OSDXattrLessEqual; comparison++ {
			for index, operand := range [][]byte{[]byte("m\x00y"), []byte("m\x00z"), []byte("m\x00{")} {
				ordering := bytes.Compare(operand, attrs[names[4]])
				truth := []bool{false, ordering == 0, ordering != 0, ordering > 0, ordering >= 0, ordering < 0, ordering <= 0}[comparison]
				entry := xattrCase{name: fmt.Sprintf("%s %s caller-order-%d", kind, operators[comparison], index-1), write: kind == "write", ops: []cephmsgr.OSDOperation{cmp(names[4], comparison, operand)}}
				if entry.write {
					entry.ops = append(entry.ops, set(names[5], []byte("marker\x00")), remove(names[1]))
				} else {
					entry.ops = append(entry.ops, all)
				}
				entry.results = make([]int32, len(entry.ops))
				entry.results[0] = 1
				if !truth {
					entry.code, entry.results[0] = -125, -125
				}
				cases = append(cases, entry)
			}
		}
	}
	falseGuard := cmp(names[4], cephmsgr.OSDXattrEqual, []byte("wrong\x00"))
	cases = append(cases,
		// SET/RM before CMP is intentionally unsupported: the getter reads
		// committed attrs. Keep the native-only staged rollback experiment
		// separate, and verify this supported guard-before-write ordering.
		xattrCase{name: "false guard prevents later SET and RM", write: true, code: -125, ops: []cephmsgr.OSDOperation{falseGuard, set(names[0], []byte("uncommitted\x00")), remove(names[1])}, results: []int32{-125, 0, 0}},
		xattrCase{name: "FAILOK false guard allows SET RM commit", write: true, ops: []cephmsgr.OSDOperation{func() cephmsgr.OSDOperation { op := falseGuard; op.Flags = cephmsgr.OSDFailOK; return op }(), set(names[0], []byte("committed\x00")), remove(names[1])}, results: []int32{-125, 0, 0}},
		xattrCase{name: "missing attribute compares equal to empty STRING", ops: []cephmsgr.OSDOperation{cmp(names[6], cephmsgr.OSDXattrEqual, nil), all}, results: []int32{1, 0}},
		xattrCase{name: "missing attribute false comparison", code: -125, ops: []cephmsgr.OSDOperation{cmp(names[6], cephmsgr.OSDXattrNotEqual, nil), all}, results: []int32{-125, 0}},
		xattrCase{name: "missing object is known ENOENT", missing: true, code: -2, ops: []cephmsgr.OSDOperation{cmp(names[6], cephmsgr.OSDXattrEqual, nil)}, results: []int32{0}},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed(t)
			input := xattrNativeInput{Compare: true, Exists: !tc.missing, Kind: "read", Baseline: b64(baseline), Attributes: nativeAttrs(attrs), Names: encodedNames, Operations: nativeOps(tc.ops)}
			request := osdObjectRequest(oracle, tc.ops...)
			if tc.write {
				input.Kind, input.Seconds, input.Nanos = "write", 1700000001, 987654321
				request.ModificationTime = cephmsgr.OSDTime{Seconds: input.Seconds, Nanoseconds: input.Nanos}
			}
			if tc.missing {
				input.Object = oracle.Name + "-absent"
				input.Attributes = []nativeXattrEntry{}
				request.Object.Name = input.Object
			}
			native := nativeXattrCompare(t, ctx, oracle, input)
			if native.Vector.Code != tc.code {
				t.Fatalf("independent native aggregate=%d; want=%d", native.Vector.Code, tc.code)
			}
			reply, err := o.Request(ctx, request)
			var server *cephmsgr.OSDError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatalf("known native refusal differs: aggregate=%d error=%v", reply.Code, err)
				}
			} else if err != nil {
				t.Fatal("Go xattr vector", err)
			}
			if reply.Code != tc.code || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Object != request.Object.Name || reply.PG.Pool != uint64(oracle.Pool) || reply.PG.Seed != oracle.Hash || reply.MapEpoch < oracle.Epoch || len(reply.Operations) != len(tc.ops) {
				t.Fatal("xattr reply routing/result identity differs", reply)
			}
			if tc.write && tc.code >= 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("metadata mutation lacks server ONDISK completion")
			}
			var raw []byte
			for i, result := range reply.Operations {
				if result.Code != tc.ops[i].Code || result.Result != tc.results[i] || result.Xattr.NameLength != uint32(len(tc.ops[i].Xattr.Name)) || result.Offset != 0 || result.Length != 0 || result.PayloadLength != uint32(len(result.Data)) {
					t.Fatalf("op %d raw signed result/union differs: %+v", i, result)
				}
				if result.Code == cephmsgr.OSDCompareXattr && (result.Xattr.ComparisonOp != uint8(tc.ops[i].Xattr.Comparison) || result.Xattr.ComparisonMode != 1 || result.Xattr.ValueLength != uint32(len(tc.ops[i].Data))) {
					t.Fatal("STRING union/operand length changed", result)
				}
				raw = append(raw, result.Data...)
			}
			if !bytes.Equal(raw, reply.RawData) {
				t.Fatal("ordered xattr outputs do not match raw reply data")
			}
			observe := observation
			observe.Object = input.Object
			after := nativeXattrCompare(t, ctx, oracle, observe).Actual
			if !xattrSnapshotEqual(after, native.After, false) {
				t.Fatal("Go committed bytes/attrs/stat2 differ from independent native vector", after, native.After)
			}
			if tc.write && tc.code >= 0 {
				if after.Version <= native.Actual.Version || after.Seconds != int64(input.Seconds) || after.Nanos != int64(input.Nanos) {
					t.Fatal("metadata commit lacks native version/explicit mtime update")
				}
			} else if !xattrSnapshotEqual(after, native.Actual, true) {
				t.Fatal("read-only or known refusal changed native state")
			}
			for _, list := range native.Vector.Lists {
				if list.Code >= 0 && tc.code >= 0 {
					inventory := nativeXattrSnapshot{Attributes: list.Entries}
					if !reflect.DeepEqual(xattrEncodedInventory(t, reply.Operations[list.Index].Data), xattrInventory(inventory)) {
						t.Fatal("GETALL opaque native encoding differs from independent iterator")
					}
				}
			}
		})
		if t.Failed() {
			break
		}
	}
	if t.Failed() {
		return
	}
	t.Run("GET GETALL READ STAT and missing attr", func(t *testing.T) {
		seed(t)
		before := nativeXattrCompare(t, ctx, oracle, observation).Actual
		ops := []cephmsgr.OSDOperation{get(names[0]), all, get(names[1]), get(names[2]), get(names[3]), {Code: cephmsgr.OSDRead, Offset: 1, Length: 4}, {Code: cephmsgr.OSDStat}}
		reply, err := o.Request(ctx, osdObjectRequest(oracle, ops...))
		if err != nil {
			t.Fatal("mixed raw xattr/object reads", err)
		}
		checkOSDMetadata(t, reply, oracle)
		if len(reply.Operations) != len(ops) {
			t.Fatal("mixed GET/read/stat vector count")
		}
		wantValues := map[int][]byte{0: attrs[names[0]], 2: attrs[names[1]], 3: attrs[names[2]], 4: attrs[names[3]], 5: baseline[1:5]}
		var joined []byte
		for i, result := range reply.Operations {
			if result.Result != 0 || result.PayloadLength != uint32(len(result.Data)) {
				t.Fatal("mixed read result/output boundary", result)
			}
			if value, exists := wantValues[i]; exists && !bytes.Equal(result.Data, value) {
				t.Fatal("binary GET/read value differs", i, result.Data)
			}
			if ops[i].Code == cephmsgr.OSDGetXattr && (result.Xattr.NameLength != uint32(len(ops[i].Xattr.Name)) || result.Xattr.ValueLength != uint32(len(result.Data))) {
				t.Fatal("GET output normalization differs", result)
			}
			joined = append(joined, result.Data...)
		}
		if !bytes.Equal(joined, reply.RawData) || !reflect.DeepEqual(xattrEncodedInventory(t, reply.Operations[1].Data), xattrInventory(before)) {
			t.Fatal("raw ordered data/native attr inventory changed")
		}
		stat := reply.Operations[6].Data
		if len(stat) != 16 || binary.LittleEndian.Uint64(stat) != uint64(len(baseline)) || binary.LittleEndian.Uint32(stat[8:]) != uint32(before.Seconds) || binary.LittleEndian.Uint32(stat[12:]) != uint32(before.Nanos) {
			t.Fatal("mixed native stat2 output differs")
		}
		missing, err := o.Request(ctx, osdObjectRequest(oracle, get(names[6])))
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != -61 || missing.Code != -61 || len(missing.Operations) != 1 || missing.Operations[0].Result != -61 || len(missing.Operations[0].Data) != 0 {
			t.Fatal("missing attribute differs from known ENODATA", missing, err)
		}
		after := nativeXattrCompare(t, ctx, oracle, observation).Actual
		if !xattrSnapshotEqual(before, after, true) {
			t.Fatal("read-only GET/GETALL/read/stat or attr refusal changed native state")
		}
	})
	t.Logf("%d Go/native xattr vectors matched aggregate/state; actual Go CMP +1 and FAILOK -125 retained; C API raw CMP result unavailable; read-only checks make no durability claim", len(cases))
}

package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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

type omapNativeEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type omapNativeAction struct {
	Op         string   `json:"op"`
	Flags      uint32   `json:"flags"`
	StartAfter []byte   `json:"start_after,omitempty"`
	Prefix     []byte   `json:"prefix,omitempty"`
	MaxReturn  uint64   `json:"max_return"`
	Keys       [][]byte `json:"keys,omitempty"`
}

// Keep operation variants exact; native BYKEYS has no page count/cursor fields.
func (a omapNativeAction) MarshalJSON() ([]byte, error) {
	result := map[string]any{"op": a.Op, "flags": a.Flags}
	if a.Op == "bykeys" {
		keys := a.Keys
		if keys == nil {
			keys = [][]byte{}
		}
		result["keys"] = keys
	} else {
		start := a.StartAfter
		if start == nil {
			start = []byte{}
		}
		result["start_after"] = start
		result["max_return"] = a.MaxReturn
		if a.Op == "vals" {
			prefix := a.Prefix
			if prefix == nil {
				prefix = []byte{}
			}
			result["prefix"] = prefix
		}
	}
	return json.Marshal(result)
}

type omapNativeInput struct {
	Object        string             `json:"object,omitempty"`
	Namespace     string             `json:"namespace,omitempty"`
	Baseline      []byte             `json:"baseline,omitempty"`
	Attributes    []nativeXattrEntry `json:"attributes,omitempty"`
	Entries       []omapNativeEntry  `json:"entries,omitempty"`
	Operations    []omapNativeAction `json:"operations,omitempty"`
	InventoryKeys [][]byte           `json:"inventory_keys,omitempty"`
}

type omapNativeVector struct {
	Aggregate  int32 `json:"aggregate"`
	RawWire    bool  `json:"raw_wire_payload_available"`
	Operations []struct {
		Index   int               `json:"index"`
		Op      string            `json:"op"`
		Opcode  uint16            `json:"opcode"`
		Result  int32             `json:"prval"`
		More    *bool             `json:"more"`
		Entries []omapNativeEntry `json:"entries"`
	} `json:"operations"`
}

type omapNativeSnapshot struct {
	nativeXattrSnapshot
	Omap *omapNativeVector `json:"omap"`
}

type omapNativeReceipt struct {
	Schema        int    `json:"schema_version"`
	Commit        string `json:"source_commit"`
	Mode          string `json:"mode"`
	DaemonVersion string `json:"fixture_daemon_version"`
	Fixture       struct {
		Pool   string `json:"pool"`
		ID     int64  `json:"pool_id"`
		Object string `json:"object"`
	} `json:"fixture"`
	Object    string             `json:"object"`
	Namespace string             `json:"namespace"`
	SeedCode  *int32             `json:"seed_aggregate"`
	Before    omapNativeSnapshot `json:"before"`
	After     omapNativeSnapshot `json:"after"`
	Vector    omapNativeVector   `json:"vector"`
	Scope     struct {
		Independent bool `json:"independent_native_c_api"`
		RawWire     bool `json:"raw_wire_payload_available"`
		Header      bool `json:"header_verified"`
		Comparison  bool `json:"comparison_verified"`
		Writes      bool `json:"product_writes_verified"`
		NulCursor   bool `json:"pagination_nul_cursors_supported"`
		EmptyByKeys bool `json:"inventory_empty_key_via_bykeys2"`
	} `json:"scope"`
	Cleanup struct {
		Removed  bool   `json:"object_removed"`
		Prepared bool   `json:"prepared_for_explicit_reads"`
		Touched  bool   `json:"object_touched"`
		Code     *int32 `json:"code"`
	} `json:"cleanup"`
}

func nativeOmapRead(t *testing.T, ctx context.Context, base osdFixtureOracle, mode string, input omapNativeInput) omapNativeReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 1<<20 {
		t.Fatal("OMAP native input exceeds bound")
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-omap-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-omap-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-omap", mode); err != nil {
		t.Fatal("native OMAP "+mode, err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-omap-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err = io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("OMAP native receipt exceeds bound", err)
	}
	var result omapNativeReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if result.Schema != 1 || result.Commit != source || result.Mode != mode || result.Fixture.ID != base.Pool || result.Fixture.Object != base.Name || result.Namespace != input.Namespace || !strings.Contains(result.DaemonVersion, source) || !strings.Contains(result.DaemonVersion, "20.2.4") {
		t.Fatal("native OMAP receipt provenance differs")
	}
	if !result.Scope.Independent || result.Scope.RawWire || result.Scope.Header || result.Scope.Comparison || result.Scope.Writes || result.Scope.NulCursor || !result.Scope.EmptyByKeys || result.Vector.RawWire {
		t.Fatal("native C oracle fabricated unsupported wire/header/write/cursor evidence")
	}
	if mode == "prepare" || mode == "cleanup" {
		prefix := base.Name + "-native-omap-"
		suffix := strings.TrimPrefix(result.Object, prefix)
		if !strings.HasPrefix(result.Object, prefix) || len(suffix) != 32 || strings.IndexFunc(suffix, func(r rune) bool { return !strings.ContainsRune("0123456789abcdef", r) }) >= 0 {
			t.Fatal("native owned object differs from exclusive UUID scope")
		}
	} else if result.Object != input.Object {
		t.Fatal("native read receipt object changed")
	}
	if mode == "prepare" {
		if result.SeedCode == nil || *result.SeedCode != 0 || !result.Cleanup.Prepared || result.Cleanup.Removed {
			t.Fatal("exclusive native preparation lacks ownership")
		}
	} else if mode == "cleanup" {
		if !result.Cleanup.Removed || result.Cleanup.Code == nil || *result.Cleanup.Code != 0 && *result.Cleanup.Code != -2 {
			t.Fatal("owned native OMAP cleanup failed")
		}
	} else {
		if result.Cleanup.Removed || result.Cleanup.Touched || !reflect.DeepEqual(result.Before, result.After) {
			t.Fatal("native read changed object state or touched owned mirror")
		}
	}
	return result
}

// Interpret the native encoded collection only in this consumer test. Product
// operations preserve Data/RawData and perform no map projection or pagination.
func decodeOmapRead(t *testing.T, code cephmsgr.OSDOpcode, data []byte) ([]omapNativeEntry, *bool) {
	t.Helper()
	d := wire.NewDecoderLimit(data, 64<<10)
	minimum := uint32(8)
	if code == cephmsgr.OSDGetOmapKeys {
		minimum = 4
	}
	count := d.Count(minimum, 128)
	entries := make([]omapNativeEntry, 0, count)
	var previous []byte
	for i := 0; i < count; i++ {
		key := bytes.Clone(d.Bytes())
		value := []byte{}
		if code != cephmsgr.OSDGetOmapKeys {
			value = append(value, d.Bytes()...)
		}
		if i > 0 && bytes.Compare(key, previous) <= 0 {
			t.Fatal("raw OMAP keys not strictly ordered")
		}
		entries = append(entries, omapNativeEntry{Key: key, Value: value})
		previous = key
	}
	var more *bool
	if code != cephmsgr.OSDGetOmapValuesByKeys {
		value := d.Bool()
		more = &value
	}
	if err := d.Done(); err != nil {
		t.Fatal("bounded OMAP raw output decode", err)
	}
	return entries, more
}

func TestCephOSDOmapReadsIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := []byte{0, 0xff, 'o', 'm', 'a', 'p', 0, 10, 0x80, 'x'}
	entries := []omapNativeEntry{{[]byte{}, []byte{}}, {[]byte("\x00key"), []byte{0, 0xff}}, {[]byte("a"), []byte{}}, {[]byte("a0"), []byte("zero")}, {[]byte("a1"), []byte{0x80, 0}}, {[]byte("b"), []byte("b-value")}, {[]byte("b0"), []byte{}}, {[]byte("b\x00x"), []byte{0, 0xff, 'B'}}, {[]byte("c"), []byte("c")}, {[]byte("\xffkey"), []byte{0xff, 0, 10}}}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	keys := make([][]byte, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	attrs := []nativeXattrEntry{{Name: []byte("omap-fixture-binary"), Value: []byte{0, 0xff, 'X'}}, {Name: []byte("omap-fixture-empty"), Value: []byte{}}}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		nativeOmapRead(t, cleanup, base, "cleanup", omapNativeInput{})
		prepared = false
	})
	seed := nativeOmapRead(t, ctx, base, "prepare", omapNativeInput{Baseline: baseline, Attributes: attrs, Entries: entries})
	prepared = true
	if seed.Before.Code != 0 || !bytes.Equal(seed.Before.Data, baseline) || seed.Before.Version == 0 || seed.Before.Omap == nil || len(seed.Before.Omap.Operations) != 2 || !reflect.DeepEqual(seed.Before.Omap.Operations[1].Entries, entries) {
		t.Fatal("exclusive native baseline byte/OMAP inventory differs")
	}
	routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": seed.Object, "namespace": ""})
	oracle := routing.Placement
	if routing.Actual.Code != 0 || !bytes.Equal(routing.Actual.Data, baseline) || oracle.Name != seed.Object || oracle.Namespace != "" || oracle.Pool != base.Pool || oracle.Epoch < base.Epoch || oracle.ID < 0 || oracle.Address == "" || oracle.RawPGID != fmt.Sprintf("%d.%x", oracle.Pool, oracle.Hash) || oracle.PGID != fmt.Sprintf("%d.%x", oracle.Pool, oracle.PG) {
		t.Fatal("caller native UUID placement differs")
	}
	acting := false
	for _, id := range oracle.Acting {
		acting = acting || id == oracle.ID
	}
	if !acting {
		t.Fatal("explicit UUID primary absent from acting set")
	}
	t.Logf("caller placement object=%s rawPG=%s mappedPG=%s epoch=%d primary=%d endpoint=%s", oracle.Name, oracle.RawPGID, oracle.PGID, oracle.Epoch, oracle.ID, oracle.Address)
	o := mutationConnection(t, c, ctx, oracle)
	defer o.Close()
	page := func(code cephmsgr.OSDOpcode, start, prefix string, count uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: code, Omap: cephmsgr.OSDOmapInput{StartAfter: start, Prefix: prefix, MaxEntries: count, MaxOutputBytes: 64 << 10}}
	}
	bykeys := func(names ...string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapValuesByKeys, Omap: cephmsgr.OSDOmapInput{Keys: names, MaxOutputBytes: 64 << 10}}
	}
	actions := func(ops []cephmsgr.OSDOperation) []omapNativeAction {
		result := make([]omapNativeAction, 0, len(ops))
		for _, op := range ops {
			action := omapNativeAction{Flags: uint32(op.Flags), StartAfter: []byte(op.Omap.StartAfter), Prefix: []byte(op.Omap.Prefix), MaxReturn: op.Omap.MaxEntries}
			switch op.Code {
			case cephmsgr.OSDGetOmapKeys:
				action.Op = "keys"
			case cephmsgr.OSDGetOmapValues:
				action.Op = "vals"
			case cephmsgr.OSDGetOmapValuesByKeys:
				action.Op = "bykeys"
				for _, key := range op.Omap.Keys {
					action.Keys = append(action.Keys, []byte(key))
				}
			default:
				continue
			}
			result = append(result, action)
		}
		return result
	}
	type readCase struct {
		name    string
		ops     []cephmsgr.OSDOperation
		missing bool
	}
	var cases []readCase
	for _, code := range []cephmsgr.OSDOpcode{cephmsgr.OSDGetOmapKeys, cephmsgr.OSDGetOmapValues} {
		for _, count := range []uint64{0, 1, 2, 9, 10, math.MaxUint64} {
			cases = append(cases, readCase{name: fmt.Sprintf("opcode-%x-count-%d", code, count), ops: []cephmsgr.OSDOperation{page(code, "", "", count)}})
		}
		cases = append(cases, readCase{name: fmt.Sprintf("opcode-%x-final-empty", code), ops: []cephmsgr.OSDOperation{page(code, "\xffkey", "", 1)}})
	}
	for _, tc := range []struct {
		name, start, prefix string
		count               uint64
	}{{"prefix-after-cursor-limited", "a", "b", 2}, {"prefix-after-cursor-final", "a", "b", 3}, {"prefix-equals-cursor", "b", "b", 2}, {"prefix-before-cursor", "c", "b", 2}, {"prefix-no-match", "", "z", 2}} {
		cases = append(cases, readCase{name: tc.name, ops: []cephmsgr.OSDOperation{page(cephmsgr.OSDGetOmapValues, tc.start, tc.prefix, tc.count)}})
	}
	cases = append(cases, readCase{name: "bykeys binary empty missing distinct", ops: []cephmsgr.OSDOperation{bykeys("missing", "\xffkey", "", "a", "\x00key", "b\x00x")}}, readCase{name: "bykeys empty request", ops: []cephmsgr.OSDOperation{bykeys()}}, readCase{name: "mixed OMAP READ STAT", ops: []cephmsgr.OSDOperation{page(cephmsgr.OSDGetOmapKeys, "", "", 2), {Code: cephmsgr.OSDRead, Offset: 1, Length: 4}, page(cephmsgr.OSDGetOmapValues, "a", "b", 2), {Code: cephmsgr.OSDStat}, bykeys("a", "\x00key")}}, readCase{name: "missing object known ENOENT", missing: true, ops: []cephmsgr.OSDOperation{page(cephmsgr.OSDGetOmapValues, "", "", 2), bykeys("a")}})
	type caseEvidence struct {
		Name      string             `json:"name"`
		Placement osdFixtureOracle   `json:"caller_placement"`
		Native    omapNativeReceipt  `json:"native"`
		Go        cephmsgr.OSDReply  `json:"go_reply"`
		After     omapNativeSnapshot `json:"after"`
	}
	evidence := struct {
		Schema    int              `json:"schema_version"`
		Source    string           `json:"source_commit"`
		Placement osdFixtureOracle `json:"caller_placement"`
		Cases     []caseEvidence   `json:"cases"`
	}{Schema: 1, Source: seed.Commit, Placement: oracle}
	defer func() {
		data, err := json.Marshal(evidence)
		if err != nil || len(data) > 512<<10 {
			t.Error("bounded OMAP acceptance evidence", err)
			return
		}
		if err := os.WriteFile(filepath.Join(dir, "osd-omap-read-results.json"), data, 0644); err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			selected := oracle
			connection := o
			input := omapNativeInput{Object: oracle.Name, InventoryKeys: keys, Operations: actions(tc.ops)}
			if tc.missing {
				input.Object = oracle.Name + "-absent"
				missing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": input.Object, "namespace": ""})
				selected = missing.Placement
				if missing.Actual.Code != -2 || selected.Name != input.Object || selected.Namespace != "" || selected.Pool != oracle.Pool || selected.Epoch < oracle.Epoch || selected.ID < 0 || selected.Address == "" || selected.RawPGID != fmt.Sprintf("%d.%x", selected.Pool, selected.Hash) || selected.PGID != fmt.Sprintf("%d.%x", selected.Pool, selected.PG) {
					t.Fatal("caller-owned missing-object placement/absence differs")
				}
				connection = mutationConnection(t, c, ctx, selected)
				defer connection.Close()
			}
			request := osdObjectRequest(selected, tc.ops...)
			native := nativeOmapRead(t, ctx, base, "observe", input)
			reply, err := connection.Request(ctx, request)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if tc.missing {
				if native.Vector.Aggregate != -2 || !errors.As(err, &server) || server.Code != -2 || errors.As(err, &unknown) {
					t.Fatal("missing object native-known refusal differs", native.Vector.Aggregate, err)
				}
			} else if err != nil || native.Vector.Aggregate != 0 {
				t.Fatal("Go/native OMAP read", native.Vector.Aggregate, err)
			}
			if reply.Code != native.Vector.Aggregate || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Submission.Transaction != reply.Transaction || reply.Submission.GlobalID != reply.GlobalID || reply.Submission.MapEpoch != request.MapEpoch || reply.Object != request.Object.Name || reply.PG.Pool != uint64(selected.Pool) || reply.PG.Seed != selected.Hash || reply.MapEpoch < selected.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != len(tc.ops) {
				t.Fatal("OMAP raw reply identity/result count differs")
			}
			var raw []byte
			nativeIndex := 0
			for i, result := range reply.Operations {
				op := tc.ops[i]
				raw = append(raw, result.Data...)
				if result.Code != op.Code || result.PayloadLength != uint32(len(result.Data)) {
					t.Fatal("ordered OMAP/object output boundaries differ", i)
				}
				if tc.missing {
					t.Logf("missing object op=%x C prval=%d raw Go result=%d", op.Code, native.Vector.Operations[nativeIndex].Result, result.Result)
					if len(result.Data) != 0 {
						t.Fatal("missing object invented collection output")
					}
					nativeIndex++
					continue
				}
				if op.Code == cephmsgr.OSDRead {
					if result.Result != 0 || !bytes.Equal(result.Data, baseline[1:5]) {
						t.Fatal("mixed native object bytes differ")
					}
					continue
				}
				if op.Code == cephmsgr.OSDStat {
					if result.Result != 0 || len(result.Data) != 16 || binary.LittleEndian.Uint64(result.Data) != uint64(len(baseline)) || binary.LittleEndian.Uint32(result.Data[8:]) != uint32(seed.Before.Seconds) || binary.LittleEndian.Uint32(result.Data[12:]) != uint32(seed.Before.Nanos) {
						t.Fatal("mixed native stat2 differs")
					}
					continue
				}
				if nativeIndex >= len(native.Vector.Operations) {
					t.Fatal("native OMAP per-op receipt missing")
				}
				expected := native.Vector.Operations[nativeIndex]
				nativeIndex++
				got, more := decodeOmapRead(t, op.Code, result.Data)
				if result.Result != expected.Result || expected.Opcode != uint16(op.Code) || !reflect.DeepEqual(got, expected.Entries) || !reflect.DeepEqual(more, expected.More) {
					t.Fatalf("raw Go OMAP collection differs from independent native: op=%x result=%d/%d entries=%+v/%+v more=%v/%v", op.Code, result.Result, expected.Result, got, expected.Entries, more, expected.More)
				}
			}
			if nativeIndex != len(native.Vector.Operations) || !bytes.Equal(raw, reply.RawData) {
				t.Fatal("native vector count/raw data concatenation differs")
			}
			after := nativeOmapRead(t, ctx, base, "observe", input)
			if !reflect.DeepEqual(native.Before, after.After) || !tc.missing && !reflect.DeepEqual(seed.Before, after.After) {
				t.Fatal("Go read changed native bytes/xattrs/OMAP/version/mtime")
			}
			evidence.Cases = append(evidence.Cases, caseEvidence{Name: tc.name, Placement: selected, Native: native, Go: reply, After: after.After})
		})
		if t.Failed() {
			break
		}
	}
}

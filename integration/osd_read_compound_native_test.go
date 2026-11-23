package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// readCompoundOp is one operation of a read-only compound with the action
// tools/osd_pool_mode_readcompound_oracle.py builds natively for it, and the
// source-derived per-op result it has when it executes successfully.
type readCompoundOp struct {
	op     cephmsgr.OSDOperation
	native map[string]any
	want   int32
	// output is the source-derived output of a successful CALL.
	output []byte
}

func nb(data []byte) []byte {
	if data == nil {
		return []byte{}
	}
	return data
}

func rcRead(offset, length uint64) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: offset, Length: length},
		native: map[string]any{"op": "read", "offset": offset, "length": length}}
}

func rcStat() readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, native: map[string]any{"op": "stat"}}
}

func rcAssert(version uint64) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDAssertVersion, Version: version},
		native: map[string]any{"op": "assert", "version": version}}
}

func rcCmpExt(offset uint64, data []byte) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareExtent, Offset: offset, Length: uint64(len(data)), Data: data},
		native: map[string]any{"op": "cmpext", "offset": offset, "data": nb(data)}}
}

func rcXattrs() readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 64 << 10}},
		native: map[string]any{"op": "getxattrs"}}
}

var rcXattrComparisons = map[cephmsgr.OSDXattrComparison]string{cephmsgr.OSDXattrEqual: "eq", cephmsgr.OSDXattrNotEqual: "ne",
	cephmsgr.OSDXattrGreater: "gt", cephmsgr.OSDXattrGreaterEqual: "gte", cephmsgr.OSDXattrLess: "lt", cephmsgr.OSDXattrLessEqual: "lte"}

// rcCmpXattr is a STRING comparison; the pinned do_xattr_cmp_str returns 1
// for a true comparison, which stays the operation's result.
func rcCmpXattr(name string, comparison cephmsgr.OSDXattrComparison, value []byte) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareXattr, Xattr: cephmsgr.OSDXattrInput{Name: name, Comparison: comparison, Mode: cephmsgr.OSDXattrString}, Data: value},
		native: map[string]any{"op": "cmpxattr", "name": []byte(name), "comparison": rcXattrComparisons[comparison], "value": nb(value)}, want: 1}
}

func rcKeys(after string, max uint64) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapKeys, Omap: cephmsgr.OSDOmapInput{StartAfter: after, MaxEntries: max, MaxOutputBytes: 64 << 10}},
		native: map[string]any{"op": "keys", "start_after": nb([]byte(after)), "max_return": max}}
}

func rcVals(after, prefix string, max uint64) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapValues, Omap: cephmsgr.OSDOmapInput{StartAfter: after, Prefix: prefix, MaxEntries: max, MaxOutputBytes: 64 << 10}},
		native: map[string]any{"op": "vals", "start_after": nb([]byte(after)), "prefix": nb([]byte(prefix)), "max_return": max}}
}

func rcByKeys(keys ...string) readCompoundOp {
	raw := [][]byte{}
	for _, k := range keys {
		raw = append(raw, nb([]byte(k)))
	}
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapValuesByKeys, Omap: cephmsgr.OSDOmapInput{Keys: keys, MaxOutputBytes: 64 << 10}},
		native: map[string]any{"op": "bykeys", "keys": raw}}
}

func rcOmapCmp(key string, value []byte, comparison cephmsgr.OSDOmapComparison) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareOmap, Omap: cephmsgr.OSDOmapInput{Assertions: []cephmsgr.OSDOmapAssertion{{Key: key, Value: value, Comparison: comparison}}}},
		native: map[string]any{"op": "omapcmp", "key": nb([]byte(key)), "comparison": int(comparison), "value": nb(value)}}
}

// rcCall is a read-mode CALL; output is what the method returns when it
// succeeds.
func rcCall(class, method string, input, output []byte) readCompoundOp {
	return readCompoundOp{op: compoundCallOp(cephmsgr.OSDCallRead, class, method, input),
		native: map[string]any{"op": "exec", "class": class, "method": method, "input": nb(input)}, output: output}
}

func rcChecksum(kind uint8, offset, length uint64, chunk uint32, seed uint64) readCompoundOp {
	return readCompoundOp{op: cephmsgr.OSDOperation{Code: cephmsgr.OSDChecksum, Offset: offset, Length: length, Checksum: cephmsgr.OSDChecksumInput{Type: kind, ChunkSize: chunk, Seed: seed}},
		native: map[string]any{"op": "checksum", "type": kind, "offset": offset, "length": length, "chunk_size": chunk, "seed": seed}}
}

// rcHello is cls_hello say_hello's output for input (pinned
// src/cls/hello/cls_hello.cc): "Hello, world!" for no input.
func rcHello(input string) []byte {
	if input == "" {
		input = "world"
	}
	return []byte("Hello, " + input + "!")
}

type nativeReadCompoundOutput struct {
	Op             string             `json:"op"`
	PrvalAvailable bool               `json:"prval_available"`
	Prval          *int32             `json:"prval"`
	Data           []byte             `json:"data"`
	CountUnchanged bool               `json:"count_unchanged"`
	Size           uint64             `json:"size"`
	Seconds        int64              `json:"seconds"`
	Nanos          int64              `json:"nanos"`
	Unchanged      bool               `json:"unchanged"`
	Attributes     []nativeXattrEntry `json:"attributes"`
	Entries        []omapNativeEntry  `json:"entries"`
	More           *bool              `json:"more"`
	Output         []byte             `json:"output"`
	Values         []uint64           `json:"values"`
}

type nativeReadCompound struct {
	Pool   string               `json:"pool"`
	Object string               `json:"object"`
	Action string               `json:"action"`
	Code   int                  `json:"code"`
	State  *nativeXattrSnapshot `json:"state"`
	Native struct {
		Aggregate    int32                      `json:"aggregate"`
		Version      uint64                     `json:"version"`
		OperateCalls int                        `json:"operate_calls"`
		RawPerOp     bool                       `json:"raw_wire_per_op_results_available"`
		Operations   []nativeReadCompoundOutput `json:"operations"`
	} `json:"native"`
}

func readCompoundOracle(t *testing.T, ctx context.Context, input map[string]any) nativeReadCompound {
	t.Helper()
	var result struct {
		ReadCompound nativeReadCompound `json:"read_compound"`
	}
	nativePoolResult(t, ctx, "readcompound", input, &result)
	r := result.ReadCompound
	if r.Object != input["object"] || r.Action != input["action"] {
		t.Fatal("native read-compound receipt identity differs", r.Object, r.Action)
	}
	return r
}

// readCompoundModel is a seeded head: its bytes, xattrs and OMAP, and the
// native stat of the seed.
type readCompoundModel struct {
	object  string
	data    []byte
	attrs   map[string][]byte
	omap    map[string][]byte
	version uint64
	seconds int64
	nanos   int64
}

// seedReadCompound writes one owned head natively and returns its model.
func seedReadCompound(t *testing.T, ctx context.Context, slot int, object string, data []byte, attrs, omap map[string][]byte) readCompoundModel {
	t.Helper()
	attributes, entries := []map[string][]byte{}, []map[string][]byte{}
	for name, value := range attrs {
		attributes = append(attributes, map[string][]byte{"name": []byte(name), "value": nb(value)})
	}
	for key, value := range omap {
		entries = append(entries, map[string][]byte{"key": nb([]byte(key)), "value": nb(value)})
	}
	r := readCompoundOracle(t, ctx, map[string]any{"action": "seed", "slot": slot, "object": object, "data": nb(data), "attributes": attributes, "entries": entries})
	if r.Code != 0 || r.State == nil || r.State.Code != 0 || !bytes.Equal(r.State.Data, data) || r.State.Version == 0 {
		t.Fatal("native read-compound seed differs", object, r.Code)
	}
	got := map[string][]byte{}
	for _, a := range r.State.Attributes {
		got[string(a.Name)] = a.Value
	}
	if len(got) != len(attrs) {
		t.Fatal("native seed xattrs differ", object, got)
	}
	for name, value := range attrs {
		if !bytes.Equal(got[name], value) {
			t.Fatal("native seed xattr differs", object, name)
		}
	}
	return readCompoundModel{object: object, data: data, attrs: attrs, omap: omap, version: r.State.Version, seconds: r.State.Seconds, nanos: r.State.Nanos}
}

func nativeReadCompoundRun(t *testing.T, ctx context.Context, slot int, object string, ops []readCompoundOp) nativeReadCompound {
	t.Helper()
	actions := make([]map[string]any, len(ops))
	for i, op := range ops {
		actions[i] = op.native
	}
	r := readCompoundOracle(t, ctx, map[string]any{"action": "execute", "slot": slot, "object": object, "operations": actions})
	if r.Native.OperateCalls != 1 || r.Native.RawPerOp || len(r.Native.Operations) != len(ops) {
		t.Fatal("native read-compound scope differs", object, r.Native.OperateCalls, len(r.Native.Operations))
	}
	for i, op := range ops {
		if r.Native.Operations[i].Op != op.native["op"] {
			t.Fatal("native read-compound operation order differs", i, r.Native.Operations[i].Op)
		}
	}
	return r
}

// decodeRCXattrs decodes a raw GETXATTRS output, map<string, bufferlist>.
func decodeRCXattrs(t testing.TB, data []byte) map[string][]byte {
	t.Helper()
	d := wire.NewDecoderLimit(data, 64<<10)
	n := d.Count(8, 64)
	result := map[string][]byte{}
	for i := 0; i < n; i++ {
		name := string(d.Bytes())
		result[name] = bytes.Clone(d.Bytes())
	}
	if err := d.Done(); err != nil {
		t.Fatal("bounded GETXATTRS raw output decode", err)
	}
	return result
}

func rcNativeAttrs(entries []nativeXattrEntry) map[string][]byte {
	result := map[string][]byte{}
	for _, e := range entries {
		result[string(e.Name)] = e.Value
	}
	return result
}

// rcOmapPage is the model's OMAP page strictly after after, with prefix, of
// at most max entries, and whether entries remain beyond it.
func (m readCompoundModel) rcOmapPage(after, prefix string, max uint64) ([]omapNativeEntry, bool) {
	keys := []string{}
	for k := range m.omap {
		if k > after && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	more := uint64(len(keys)) > max
	if more {
		keys = keys[:max]
	}
	page := []omapNativeEntry{}
	for _, k := range keys {
		page = append(page, omapNativeEntry{Key: []byte(k), Value: m.omap[k]})
	}
	return page, more
}

func sameOmapEntries(a, b []omapNativeEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

func sameXattrMaps(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}
	return true
}

// rcCarriesResult reports an operation that leaves do_osd_ops' running
// result as it was when it succeeds (pinned PrimaryLogPG.cc at v20.2.4:
// STAT, ASSERT_VER, the OMAP page and by-keys reads and OMAP_CMP assign it
// only on failure), so its per-op result is the previous operation's, for
// example the 1 of a true CMPXATTR. Every other operation here assigns its
// own result.
func rcCarriesResult(code cephmsgr.OSDOpcode) bool {
	switch code {
	case cephmsgr.OSDStat, cephmsgr.OSDAssertVersion, cephmsgr.OSDGetOmapKeys, cephmsgr.OSDGetOmapValues, cephmsgr.OSDGetOmapValuesByKeys, cephmsgr.OSDCompareOmap:
		return true
	}
	return false
}

// checkReadCompound is the verdict of one compound against its native run
// and the seed model. failing is the index of the operation the OSD stops
// at with code, or -1 for a successful vector. It checks the aggregate, the
// identity and result of every operation (a carried result included), the output of every executed one
// against both the native C outputs and the model, and that no operation
// after failing reports a result or output, in Go and natively. Native
// prvals of operations after failing are convenience-callback results and
// are reported, not compared.
func checkReadCompound(t testing.TB, name string, ops []readCompoundOp, code int32, failing int, model readCompoundModel, reply cephmsgr.OSDReply, err error, native nativeReadCompound) {
	t.Helper()
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	if code < 0 {
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != code {
			t.Fatalf("%s: want a known server code %d, got code %d: %v", name, code, reply.Code, err)
		}
	} else if err != nil {
		t.Fatalf("%s: want success, got %v", name, err)
	}
	if reply.Code != code || native.Native.Aggregate != code || len(reply.Operations) != len(ops) || reply.RetryAttempt != 0 ||
		reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.Object != model.object {
		t.Fatalf("%s: aggregate Go %d native %d want %d; identity %q attempt %d", name, reply.Code, native.Native.Aggregate, code, reply.Object, reply.RetryAttempt)
	}
	var raw []byte
	carry := int32(0)
	for i, op := range ops {
		got, n := reply.Operations[i], native.Native.Operations[i]
		if got.Code != op.op.Code || n.Op != op.native["op"] {
			t.Fatalf("%s: op %d identity Go %v native %s", name, i, got.Code, n.Op)
		}
		raw = append(raw, got.Data...)
		if failing >= 0 && i > failing {
			// Never executed: no result and no output in either client.
			if got.Result != 0 || got.PayloadLength != 0 || len(got.Data) != 0 {
				t.Fatalf("%s: op %d after the failure reports result %d, %d bytes", name, i, got.Result, got.PayloadLength)
			}
			if len(n.Data) != 0 || n.Attributes != nil || n.Entries != nil || len(n.Output) != 0 || len(n.Values) != 0 || op.op.Code == cephmsgr.OSDStat && !n.Unchanged {
				t.Fatalf("%s: native op %d after the failure reports output", name, i)
			}
			continue
		}
		want := op.want
		if rcCarriesResult(op.op.Code) {
			want = carry
		}
		if i == failing {
			want = code
		}
		if got.Result != want || n.PrvalAvailable && (n.Prval == nil || *n.Prval != want) {
			t.Fatalf("%s: op %d result Go %d native %v want %d", name, i, got.Result, n.Prval, want)
		}
		carry = want
		if i == failing {
			if got.PayloadLength != 0 || len(got.Data) != 0 || len(n.Data) != 0 || n.Attributes != nil || n.Entries != nil || len(n.Output) != 0 || len(n.Values) != 0 {
				t.Fatalf("%s: failing op %d returned output (Go %d bytes)", name, i, got.PayloadLength)
			}
			continue
		}
		switch op.op.Code {
		case cephmsgr.OSDRead:
			end := min(op.op.Offset+op.op.Length, uint64(len(model.data)))
			var expect []byte
			if op.op.Offset < end {
				expect = model.data[op.op.Offset:end]
			}
			if !bytes.Equal(got.Data, expect) || !bytes.Equal(n.Data, expect) || n.CountUnchanged || got.Length != uint64(len(expect)) {
				t.Fatalf("%s: READ %d bytes differ from native and the model", name, i)
			}
		case cephmsgr.OSDStat:
			stat := binary.LittleEndian.AppendUint64(nil, uint64(len(model.data)))
			stat = binary.LittleEndian.AppendUint32(stat, uint32(model.seconds))
			stat = binary.LittleEndian.AppendUint32(stat, uint32(model.nanos))
			if !bytes.Equal(got.Data, stat) || n.Size != uint64(len(model.data)) || n.Seconds != model.seconds || n.Nanos != model.nanos {
				t.Fatalf("%s: STAT %d output %x differs from native size %d mtime %d.%d", name, i, got.Data, n.Size, n.Seconds, n.Nanos)
			}
		case cephmsgr.OSDGetXattrs:
			if g := decodeRCXattrs(t, got.Data); !sameXattrMaps(g, model.attrs) || !sameXattrMaps(rcNativeAttrs(n.Attributes), model.attrs) {
				t.Fatalf("%s: GETXATTRS %d differs from native and the model", name, i)
			}
		case cephmsgr.OSDGetOmapKeys, cephmsgr.OSDGetOmapValues:
			page, more := model.rcOmapPage(op.op.Omap.StartAfter, op.op.Omap.Prefix, op.op.Omap.MaxEntries)
			if op.op.Code == cephmsgr.OSDGetOmapKeys {
				for j := range page {
					page[j].Value = []byte{}
				}
			}
			entries, gotMore := decodeOmapRead(t, op.op.Code, got.Data)
			if !sameOmapEntries(entries, page) || gotMore == nil || *gotMore != more || !sameOmapEntries(n.Entries, page) || n.More == nil || *n.More != more {
				t.Fatalf("%s: OMAP page %d differs from native and the model: Go %q more %v, native %q more %v, model %q more %v", name, i, entries, rcBool(gotMore), n.Entries, rcBool(n.More), page, more)
			}
		case cephmsgr.OSDGetOmapValuesByKeys:
			page := []omapNativeEntry{}
			keys := slices.Clone(op.op.Omap.Keys)
			sort.Strings(keys)
			for _, k := range keys {
				if v, ok := model.omap[k]; ok {
					page = append(page, omapNativeEntry{Key: []byte(k), Value: v})
				}
			}
			entries, _ := decodeOmapRead(t, op.op.Code, got.Data)
			if !sameOmapEntries(entries, page) || !sameOmapEntries(n.Entries, page) {
				t.Fatalf("%s: OMAP by-keys %d differs from native and the model", name, i)
			}
		case cephmsgr.OSDCall:
			if !bytes.Equal(got.Data, op.output) || !bytes.Equal(n.Output, op.output) {
				t.Fatalf("%s: CALL %d output %q differs from native %q or the method's %q", name, i, got.Data, n.Output, op.output)
			}
		case cephmsgr.OSDChecksum:
			if len(got.Checksum.Values) == 0 || !reflect.DeepEqual(got.Checksum.Values, n.Values) {
				t.Fatalf("%s: CHECKSUM %d values %v differ from native %v", name, i, got.Checksum.Values, n.Values)
			}
		default:
			if got.PayloadLength != 0 {
				t.Fatalf("%s: guard %d returned output", name, i)
			}
		}
	}
	if !bytes.Equal(reply.RawData, raw) {
		t.Fatalf("%s: per-op payloads do not partition the raw reply data", name)
	}
}

func rcBool(b *bool) string {
	if b == nil {
		return "nil"
	}
	return fmt.Sprint(*b)
}

// rcSummary is a one-line record of one compound for the test log.
func rcSummary(ops []readCompoundOp, reply cephmsgr.OSDReply) string {
	parts := make([]string, len(ops))
	for i, op := range ops {
		result := int32(0)
		if i < len(reply.Operations) {
			result = reply.Operations[i].Result
		}
		parts[i] = fmt.Sprintf("%v=%d", op.native["op"], result)
	}
	return fmt.Sprintf("aggregate %d [%s]", reply.Code, strings.Join(parts, " "))
}

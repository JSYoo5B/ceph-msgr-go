package osd

import (
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Every extent read the EC primary completes asynchronously beside every
// operation with output is classified, in either order and at any position;
// guards without output, other extent reads and vectors without an extent
// read are not.
func TestErasureReexecutedVectorClassifiesEachPair(t *testing.T) {
	async := []uint16{Read, SparseRead, Checksum, CompareExtent}
	output := []uint16{Stat, Isdirty, GetXattr, GetXattrs, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, Call, ListWatchers, ListSnaps, MapExt}
	quiet := []uint16{AssertVersion, CompareXattr, OmapCompare}
	ops := func(codes ...uint16) []Operation {
		result := make([]Operation, len(codes))
		for i, code := range codes {
			result[i] = Operation{Code: code}
		}
		return result
	}
	for _, a := range async {
		for _, o := range output {
			for _, vector := range [][]uint16{{a, o}, {o, a}, {AssertVersion, o, CompareXattr, a}, {a, a, o}} {
				if !ErasureReexecutedVector(ops(vector...)) {
					t.Fatalf("%#x beside %#x not classified: %#x", a, o, vector)
				}
			}
		}
		if ErasureReexecutedVector(ops(a)) || ErasureReexecutedVector(ops(a, a)) {
			t.Fatalf("extent reads alone classified: %#x", a)
		}
		for _, q := range quiet {
			if ErasureReexecutedVector(ops(a, q, a)) || ErasureReexecutedVector(ops(q, a)) {
				t.Fatalf("an output-free guard %#x beside %#x classified", q, a)
			}
		}
	}
	for _, o := range output {
		if ErasureReexecutedVector(ops(o)) || ErasureReexecutedVector(ops(o, AssertVersion, o)) {
			t.Fatalf("a vector without an extent read classified: %#x", o)
		}
	}
}

// The v8 encoder refuses the classified vector on an EC shard before building
// a message, and still encodes it for NO_SHARD and an admitted EC vector on a
// shard. The v9 encoder refuses every EC shard already.
func TestErasureReexecutedVectorRefusedOnShardEncoders(t *testing.T) {
	read := Operation{Code: Read, Length: 8}
	refused := map[string][]Operation{
		"read-stat":           {read, {Code: Stat}},
		"stat-read":           {{Code: Stat}, read},
		"getxattrs-read":      {{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}}, read},
		"cmpext-getxattr":     {{Code: CompareExtent, Length: 1, Data: []byte{1}}, {Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 64}}},
		"read-omap-values":    {read, {Code: OmapGetValues, Omap: OmapInput{MaxEntries: 4, MaxOutputBytes: 64}}},
		"read-read-mode-call": {read, {Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 64}}},
	}
	admitted := map[string][]Operation{
		"read-cmpxattr": {read, {Code: CompareXattr, Data: []byte("v"), Xattr: XattrInput{Name: "a", ComparisonOp: XattrCompareEqual, ComparisonMode: XattrCompareString}}},
		"assert-read":   {{Code: AssertVersion, Version: 9}, read},
		"stat-call":     {{Code: Stat}, {Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 64}}},
	}
	encoders := map[string]func(Request, int8) (msgr.MessageData, error){
		"v8": func(r Request, shard int8) (msgr.MessageData, error) {
			return EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 1}, Shard: shard}, v8OracleFeatures, wire.DefaultLimit)
		},
	}
	for version, encode := range encoders {
		for name, ops := range refused {
			if !ErasureReexecutedVector(ops) {
				t.Fatal("refused vector not classified", name)
			}
			if m, err := encode(sparseReadCompoundRequest(ops), 0); !errors.Is(err, ErrOperation) || m.Type != 0 || len(m.Front) != 0 || len(m.Data) != 0 {
				t.Fatal("EC re-executed vector encoded on a shard", version, name, err)
			}
			if m, err := encode(sparseReadCompoundRequest(ops), -1); err != nil || m.Type != RequestMessage {
				t.Fatal("replicated vector refused", version, name, err)
			}
		}
		for name, ops := range admitted {
			if m, err := encode(sparseReadCompoundRequest(ops), 2); err != nil || m.Type != RequestMessage {
				t.Fatal("admitted EC vector refused", version, name, err)
			}
		}
	}
}

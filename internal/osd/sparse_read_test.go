package osd

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func sparseReadBytes(data []byte, extents ...Extent) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(extents)))
	for _, x := range extents {
		e.U64(x.Offset)
		e.U64(x.Length)
	}
	e.Bytes(data)
	return e.Data
}

func TestSparseReadBoundsAndIsolation(t *testing.T) {
	op := Operation{Code: SparseRead, Offset: 3, Length: 97, SparseRead: SparseReadInput{MaxOutputBytes: 512}}
	if flags, n, err := ValidateOperations([]Operation{op}, 1024); err != nil || flags != FlagRead || n != 0 {
		t.Fatal(flags, n, err)
	}
	for _, change := range []func(*Operation){
		func(o *Operation) { o.Length = 0 }, func(o *Operation) { o.Offset = math.MaxUint64 },
		func(o *Operation) { o.Length = 1025 }, func(o *Operation) { o.SparseRead.MaxOutputBytes = 7 },
		func(o *Operation) { o.SparseRead.MaxOutputBytes = 1025 }, func(o *Operation) { o.Flags = OpFailOK },
		func(o *Operation) { o.Data = []byte{1} }, func(o *Operation) { o.Xattr.Name = "x" },
		func(o *Operation) { o.AllocHint.ExpectedWriteSize = 1 }, func(o *Operation) { o.Omap.Keys = []string{"x"} },
		func(o *Operation) { o.Class.Class = "x" }, func(o *Operation) { o.Snap = 1 },
		func(o *Operation) { o.ListSnaps.MaxOutputBytes = 10 }, func(o *Operation) { o.ListWatchers.MaxOutputBytes = 10 },
		func(o *Operation) { o.MapExt.MaxOutputBytes = 10 }, func(o *Operation) { o.Checksum.Type = 2 },
	} {
		bad := op
		change(&bad)
		if _, _, err := ValidateOperations([]Operation{bad}, 1024); err == nil {
			t.Fatal("accepted invalid sparse read", bad)
		}
	}
	if _, _, err := ValidateOperations([]Operation{op, {Code: Stat}}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("compound accepted", err)
	}
	if _, _, err := ValidateOperations([]Operation{{Code: Stat, SparseRead: op.SparseRead}}, 1024); err == nil {
		t.Fatal("inactive budget accepted")
	}
	r := sparseReadNativeRequest()
	if _, err := EncodeRequest(r, oracleFeatures, 650); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate reply budget escaped", err)
	}
	r.ReadSnap = 1
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal("snapshot sparse read rejected", err)
	}
	head := sparseReadNativeRequest()
	head.ReadSnap = 0
	plain, err := EncodeRequest(head, oracleFeatures, 1024)
	if err != nil || len(m.Front) != len(plain.Front) || bytes.Equal(m.Front, plain.Front) {
		t.Fatal("snapshot sparse read did not carry its snap id", err)
	}
	r.SnapSeq = 1
	if _, err := EncodeRequest(r, oracleFeatures, 1024); err == nil {
		t.Fatal("read snapshot with a SnapContext accepted")
	}
}

func TestSparseReadMalformedAndReplyBounds(t *testing.T) {
	data := []byte{'A', 0, 'B', 255, 'Z', '1', '2', '3', '4', '5', '6', '7', '8'}
	valid := sparseReadBytes(data, Extent{3, 5}, Extent{20, 8})
	got, err := DecodeSparseRead(valid)
	if err != nil || !reflect.DeepEqual(got.Extents, []Extent{{3, 5}, {20, 8}}) || !bytes.Equal(got.Data, data) {
		t.Fatal(got, err)
	}
	malformed := [][]byte{nil, append(bytes.Clone(valid), 0), sparseReadBytes(nil, Extent{3, 1}), sparseReadBytes(data), sparseReadBytes([]byte{1}, Extent{3, 0}), sparseReadBytes([]byte{1}, Extent{math.MaxUint64, 1}), sparseReadBytes([]byte{1, 2}, Extent{3, 1}, Extent{3, 1}), sparseReadBytes([]byte{1, 2}, Extent{4, 1}, Extent{3, 1})}
	for cut := 0; cut < len(valid); cut++ {
		malformed = append(malformed, valid[:cut])
	}
	for _, data := range malformed {
		if _, err := DecodeSparseRead(data); err == nil {
			t.Fatalf("accepted malformed sparse read %x", data)
		}
	}
	requested := sparseReadNativeRequest().Operations[0]
	reply := OperationResult{Code: SparseRead, Offset: 3, Length: 13, PayloadLength: uint32(len(valid)), Data: valid}
	if err := ValidateSparseReadReply(requested, reply, true); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*OperationResult){
		func(r *OperationResult) { r.Offset++ }, func(r *OperationResult) { r.Length-- }, func(r *OperationResult) { r.TruncateSize = 1 },
		func(r *OperationResult) { r.PayloadLength-- },
		func(r *OperationResult) {
			r.Data = sparseReadBytes([]byte{1}, Extent{2, 1})
			r.PayloadLength = 25
			r.Length = 1
		},
		func(r *OperationResult) {
			r.Data = sparseReadBytes([]byte{1}, Extent{100, 1})
			r.PayloadLength = 25
			r.Length = 1
		},
	} {
		bad := reply
		change(&bad)
		if err := ValidateSparseReadReply(requested, bad, true); err == nil {
			t.Fatal("invalid sparse reply accepted", bad)
		}
	}
	small := requested
	small.SparseRead.MaxOutputBytes = 8
	if err := ValidateSparseReadReply(small, reply, true); err == nil {
		t.Fatal("budget ignored")
	}
	if err := ValidateSparseReadReply(requested, reply, false); err == nil {
		t.Fatal("failed output accepted")
	}
	empty := OperationResult{Code: SparseRead, Offset: 3, Length: 0, PayloadLength: 8, Data: sparseReadBytes(nil)}
	if err := ValidateSparseReadReply(requested, empty, true); err != nil {
		t.Fatal(err)
	}
	empty.Data = nil
	empty.PayloadLength = 0
	if err := ValidateSparseReadReply(requested, empty, true); err == nil {
		t.Fatal("missing successful buffer accepted")
	}
	empty.Length = requested.Length
	if err := ValidateSparseReadReply(requested, empty, false); err != nil {
		t.Fatal(err)
	}
}

func sparseReadNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "sparse-read-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{{Code: SparseRead, Offset: 3, Length: 97, SparseRead: SparseReadInput{MaxOutputBytes: 512}}}}
}

func TestSparseReadRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := sparseReadNativeRequest()
	got, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "sparse-read-native-request-v6-map")
	if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("native sparse read request/header differs")
	}
	r.Operations[0].SparseRead.MaxOutputBytes = 1024
	changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) {
		t.Fatal("local budget altered wire", err)
	}
}

func TestDecodeFreshNativeSparseReadReplies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int32
		extents []Extent
		length  uint64
	}{{"populated", 0, []Extent{{3, 5}, {20, 8}}, 13}, {"empty", 0, []Extent{}, 0}, {"enoent", -2, nil, 97}, {"eacces", -13, nil, 97}} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, "sparse-read-native-reply-v6-"+tc.name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Result != tc.code || r.Object != "sparse-read-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.MapEpoch != 11 || r.UserVersion != 19 || r.Operations[0].Length != tc.length {
				t.Fatal("native metadata lost", r)
			}
			if err := ValidateSparseReadReply(sparseReadNativeRequest().Operations[0], r.Operations[0], tc.code == 0); err != nil {
				t.Fatal(err)
			}
			if tc.code == 0 {
				result, err := DecodeSparseRead(r.RawData)
				if err != nil || !reflect.DeepEqual(result.Extents, tc.extents) || uint64(len(result.Data)) != tc.length {
					t.Fatal(result, err)
				}
				if tc.length != 0 && !bytes.Equal(result.Data, []byte{'A', 0, 'B', 255, 'Z', '1', '2', '3', '4', '5', '6', '7', '8'}) {
					t.Fatal("binary native data lost")
				}
			}
		})
	}
}

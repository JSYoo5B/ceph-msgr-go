package osd

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func mapExtBytes(extents ...Extent) []byte {
	e := wire.Encoder{}
	e.U32(uint32(len(extents)))
	for _, x := range extents {
		e.U64(x.Offset)
		e.U64(x.Length)
	}
	return e.Data
}

func TestMapExtBoundsAndIsolation(t *testing.T) {
	op := Operation{Code: MapExt, Offset: 3, Length: 97, MapExt: MapExtInput{MaxOutputBytes: 128}}
	if flags, n, err := ValidateOperations([]Operation{op}, 1024); err != nil || flags != FlagRead || n != 0 {
		t.Fatal(flags, n, err)
	}
	for _, change := range []func(*Operation){
		func(o *Operation) { o.Length = 0 }, func(o *Operation) { o.Offset = math.MaxUint64 },
		func(o *Operation) { o.MapExt.MaxOutputBytes = 3 }, func(o *Operation) { o.MapExt.MaxOutputBytes = 1025 },
		func(o *Operation) { o.Data = []byte{1} }, func(o *Operation) { o.Xattr.Name = "x" },
		func(o *Operation) { o.AllocHint.ExpectedWriteSize = 1 }, func(o *Operation) { o.Omap.Keys = []string{"x"} },
		func(o *Operation) { o.Class.Class = "x" }, func(o *Operation) { o.Snap = 1 },
		func(o *Operation) { o.ListSnaps.MaxOutputBytes = 10 }, func(o *Operation) { o.ListWatchers.MaxOutputBytes = 10 },
		func(o *Operation) { o.Flags = OpFailOK },
	} {
		bad := op
		change(&bad)
		if _, _, err := ValidateOperations([]Operation{bad}, 1024); err == nil {
			t.Fatal("accepted invalid MAPEXT", bad)
		}
	}
	if _, _, err := ValidateOperations([]Operation{op, {Code: Stat}}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("compound accepted", err)
	}
	if _, _, err := ValidateOperations([]Operation{{Code: Stat, MapExt: op.MapExt}}, 1024); err == nil {
		t.Fatal("inactive budget accepted")
	}
	r := watchersNativeRequest()
	r.Operations = []Operation{op}
	if _, err := EncodeRequest(r, oracleFeatures, 350); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate reply budget escaped", err)
	}
	r.ReadSnap = 1
	if _, err := EncodeRequest(r, oracleFeatures, 1024); err == nil {
		t.Fatal("snapshot accepted")
	}
}

func TestMapExtMalformedWireAndReplyBounds(t *testing.T) {
	valid := mapExtBytes(Extent{3, 5}, Extent{20, 8})
	got, err := DecodeMapExt(valid)
	if err != nil || !reflect.DeepEqual(got, []Extent{{3, 5}, {20, 8}}) {
		t.Fatal(got, err)
	}
	malformed := [][]byte{nil, {0, 0, 0}, append(bytes.Clone(valid), 0), mapExtBytes(Extent{3, 0}), mapExtBytes(Extent{math.MaxUint64, 1}), mapExtBytes(Extent{3, 5}, Extent{3, 1}), mapExtBytes(Extent{20, 8}, Extent{3, 5}), mapExtBytes(Extent{3, 5}, Extent{7, 1})}
	for cut := 4; cut < len(valid); cut++ {
		malformed = append(malformed, valid[:cut])
	}
	for _, data := range malformed {
		if _, err := DecodeMapExt(data); err == nil {
			t.Fatalf("accepted malformed map %x", data)
		}
	}
	requested := Operation{Code: MapExt, Offset: 3, Length: 97, MapExt: MapExtInput{MaxOutputBytes: 128}}
	reply := OperationResult{Code: MapExt, Offset: 3, Length: 97, PayloadLength: uint32(len(valid)), Data: valid}
	if err := ValidateMapExtReply(requested, reply, true); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*OperationResult){
		func(r *OperationResult) { r.Offset++ }, func(r *OperationResult) { r.Length-- },
		func(r *OperationResult) { r.TruncateSize = 1 }, func(r *OperationResult) { r.PayloadLength-- },
		func(r *OperationResult) { r.Data = mapExtBytes(Extent{2, 1}); r.PayloadLength = 20 },
		func(r *OperationResult) { r.Data = mapExtBytes(Extent{99, 2}); r.PayloadLength = 20 },
		func(r *OperationResult) { r.Data = mapExtBytes(Extent{100, 1}); r.PayloadLength = 20 },
	} {
		bad := reply
		change(&bad)
		if err := ValidateMapExtReply(requested, bad, true); err == nil {
			t.Fatal("accepted invalid reply", bad)
		}
	}
	small := requested
	small.MapExt.MaxOutputBytes = 4
	if err := ValidateMapExtReply(small, reply, true); err == nil {
		t.Fatal("output budget ignored")
	}
	if err := ValidateMapExtReply(requested, reply, false); err == nil {
		t.Fatal("failed reply output accepted")
	}
	empty := reply
	empty.PayloadLength = 0
	empty.Data = nil
	if err := ValidateMapExtReply(requested, empty, false); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMapExtReply(requested, empty, true); err == nil {
		t.Fatal("successful missing map accepted")
	}
}

func mapExtNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "mapext-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{{Code: MapExt, Offset: 3, Length: 97, MapExt: MapExtInput{MaxOutputBytes: 512}}}}
}

func TestMapExtRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := mapExtNativeRequest()
	got, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "mapext-native-request-v6-map")
	if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("native MAPEXT request/header differs")
	}
	r.Operations[0].MapExt.MaxOutputBytes = 1024
	changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) {
		t.Fatal("local budget altered wire", err)
	}
}

func TestDecodeFreshNativeMapExtReplies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int32
		extents []Extent
	}{{"populated", 0, []Extent{{3, 5}, {20, 8}}}, {"empty", 0, []Extent{}}, {"enoent", -2, nil}, {"eacces", -13, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, "mapext-native-reply-v6-"+tc.name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Result != tc.code || r.Object != "mapext-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.MapEpoch != 11 || r.UserVersion != 19 {
				t.Fatal("native metadata lost", r)
			}
			if err := ValidateMapExtReply(mapExtNativeRequest().Operations[0], r.Operations[0], tc.code == 0); err != nil {
				t.Fatal(err)
			}
			if tc.code == 0 {
				extents, err := DecodeMapExt(r.RawData)
				if err != nil || !reflect.DeepEqual(extents, tc.extents) {
					t.Fatal(extents, err)
				}
			}
		})
	}
}

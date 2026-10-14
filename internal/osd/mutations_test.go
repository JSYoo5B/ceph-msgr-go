package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestMutationVectorWireAndOwnership(t *testing.T) {
	r := oracleRequest()
	r.MTimeSeconds, r.MTimeNanoseconds = 1700000001, 123456789
	r.Operations = []Operation{{Code: AssertVersion, Version: 19}, {Code: CompareExtent, Offset: 2, Length: 2, Data: []byte{0, 255}}, {Code: WriteFull, Length: 3, Data: []byte{128, 0, 10}}, {Code: Append, Length: 1, Data: []byte{254}}}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	// The independently specified fixed v6 prefix is unchanged from the native
	// stat/read fixture except for mode bits and explicit utime_t. Check packed
	// op union slots and data segmentation independently, without decoding with
	// our request encoder's matching implementation.
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagWrite|FlagOnDisk|FlagReturnVec) || binary.LittleEndian.Uint32(m.Front[12:16]) != r.MTimeSeconds || binary.LittleEndian.Uint32(m.Front[16:20]) != r.MTimeNanoseconds {
		t.Fatal("request modes or utime_t")
	}
	const start = 113 // native request-v6 front: locator + pg_t + object + count.
	if binary.LittleEndian.Uint16(m.Front[start-2:start]) != 4 {
		t.Fatal("op count")
	}
	if binary.LittleEndian.Uint16(m.Front[start:start+2]) != 0x1208 || binary.LittleEndian.Uint64(m.Front[start+6:start+14]) != 0 || binary.LittleEndian.Uint64(m.Front[start+14:start+22]) != 19 {
		t.Fatal("assert version must use second union uint64")
	}
	for i, length := range []uint32{0, 2, 3, 1} {
		if binary.LittleEndian.Uint32(m.Front[start+38*i+34:start+38*i+38]) != length {
			t.Fatal("packed per-op payload length", i)
		}
	}
	if !bytes.Equal(m.Data, []byte{0, 255, 128, 0, 10, 254}) {
		t.Fatal("ordered input segmentation")
	}
	r.Operations[1].Data[0] = 99
	if m.Data[0] != 0 {
		t.Fatal("request borrowed caller input")
	}
}

func TestMutationInputBoundsAndUnsupportedFields(t *testing.T) {
	for _, op := range []Operation{
		{Code: Write, Length: 2, Data: []byte{1}},
		{Code: Write, Offset: math.MaxUint64, Length: 1, Data: []byte{1}},
		{Code: WriteFull, Offset: 1}, {Code: Append, Offset: 1},
		{Code: Truncate, Length: 1}, {Code: Zero, Data: []byte{1}},
		{Code: Create, Offset: 1}, {Code: Delete, Data: []byte{1}},
		{Code: AssertVersion}, {Code: AssertVersion, Version: 1, Length: 1},
		{Code: CompareExtent, Length: 1}, {Code: Write, Version: 1},
		{Code: Write, Flags: OpExclusive}, {Code: Create, Flags: 4},
		{Code: 0x2301}, // xattr is an explicit later milestone.
	} {
		r := oracleRequest()
		r.Operations = []Operation{op}
		if _, err := EncodeRequest(r, oracleFeatures, 1024); err == nil {
			t.Fatalf("accepted invalid operation: %+v", op)
		}
	}
	r := oracleRequest()
	r.Operations = []Operation{{Code: Read, Length: math.MaxUint64}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("read reply budget overflow", err)
	}
	r.Operations = []Operation{{Code: WriteFull, Length: 500, Data: make([]byte, 500)}, {Code: Append, Length: 500, Data: make([]byte, 500)}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("front plus aggregate data exceeded limit", err)
	}
	r.Operations = []Operation{{Code: WriteFull}}
	r.MTimeNanoseconds = 1e9
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrRequest) {
		t.Fatal("invalid nanosecond field", err)
	}
	r = oracleRequest()
	r.MTimeSeconds = 1
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrRequest) {
		t.Fatal("read-only mtime would be ignored", err)
	}
}

func TestRejectFullReadAfterStagedWrite(t *testing.T) {
	for _, code := range []uint16{Write, WriteFull, Append} {
		r := oracleRequest()
		r.Operations = []Operation{{Code: code, Length: 3, Data: []byte{99, 0, 254}}, {Code: Read, Length: 20}}
		if _, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
			t.Fatal("unsafe native staged digest combination was sent", code, err)
		}
		r.Operations[1].Offset, r.Operations[1].Length = 1, 2
		if _, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit); err != nil {
			t.Fatal("partial read after staged replacement", code, err)
		}
		r.Operations = []Operation{{Code: Read, Length: 20}, {Code: code, Length: 3, Data: []byte{99, 0, 254}}}
		if _, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit); err != nil {
			t.Fatal("read before replacement is safe", code, err)
		}
	}
	r := oracleRequest()
	r.Operations = []Operation{
		{Code: WriteFull, Length: 3, Data: []byte{99, 0, 254}},
		{Code: WriteFull, Flags: OpFailOK, Length: 10, Data: make([]byte, 10)},
		{Code: Read, Length: 3},
	}
	if _, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit); !errors.Is(err, ErrOperation) {
		t.Fatal("later FAILOK write erased an earlier dangerous digest boundary", err)
	}
}

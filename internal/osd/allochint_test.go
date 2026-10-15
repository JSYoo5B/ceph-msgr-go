package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Source-derived literal for the pinned alloc_hint union; fresh native
// encoder vectors are separate evidence.
func TestAllocHintSourceLiteralEncodingAndAdmission(t *testing.T) {
	r := oracleRequest()
	r.MTimeSeconds = 1
	hint := AllocHintInput{ExpectedObjectSize: 4 << 20, ExpectedWriteSize: 64 << 10, Flags: 1 | 256}
	r.Operations = []Operation{{Code: SetAllocHint, Flags: OpFailOK, AllocHint: hint}}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	want := xattrLiteral(t, "2322 02000000 0000400000000000 0000010000000000 01010000 0000000000000000 00000000")
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec) || !bytes.Equal(m.Front[113:151], want) || len(m.Data) != 0 {
		t.Fatalf("allocation hint union differs: %x", m.Front[113:151])
	}
	for _, op := range []Operation{
		{Code: SetAllocHint, AllocHint: AllocHintInput{Flags: 1 << 11}},
		{Code: SetAllocHint, Length: 1},
		{Code: SetAllocHint, Data: []byte{1}},
		{Code: SetAllocHint, Version: 1},
		{Code: SetAllocHint, Xattr: XattrInput{Name: "x"}},
		{Code: SetAllocHint, Omap: OmapInput{Header: []byte{}}},
		{Code: SetAllocHint, Class: ClassInput{Class: "hello"}},
		{Code: Stat, AllocHint: AllocHintInput{ExpectedObjectSize: 1}},
		{Code: Write, Length: 1, Data: []byte{1}, AllocHint: AllocHintInput{Flags: 1}},
	} {
		if _, _, err := ValidateOperations([]Operation{op}, 1024); !errors.Is(err, ErrRequest) {
			t.Fatal("invalid allocation hint variant admitted", op.Code, err)
		}
	}
	if _, _, err := ValidateOperations([]Operation{{Code: SetAllocHint, Flags: OpExclusive}}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("EXCL allocation hint admitted", err)
	}
	r.Operations = []Operation{{Code: SetAllocHint, AllocHint: AllocHintInput{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: AllocHintFlags}}, {Code: Read, Length: 1}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); err != nil {
		t.Fatal("max hint before a read rejected", err)
	}
	call := Operation{Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassWrite, MaxOutputBytes: 8}}
	if _, _, err := ValidateOperations([]Operation{{Code: SetAllocHint}, call}, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("CALL after a staged allocation hint admitted", err)
	}
	if _, _, err := ValidateOperations([]Operation{call, {Code: SetAllocHint}}, 1024); err != nil {
		t.Fatal("allocation hint after CALL rejected", err)
	}
}

func TestAllocHintReplyValidation(t *testing.T) {
	op := Operation{Code: SetAllocHint, AllocHint: AllocHintInput{ExpectedObjectSize: 4096, ExpectedWriteSize: 512, Flags: 2}}
	valid := OperationResult{Code: SetAllocHint, AllocHint: op.AllocHint}
	if err := ValidateAllocHintReply(op, valid); err != nil {
		t.Fatal(err)
	}
	refused := valid
	refused.Result = -95
	if err := ValidateAllocHintReply(op, refused); err != nil {
		t.Fatal("known refusal rejected", err)
	}
	for _, edit := range []func(*OperationResult){
		func(r *OperationResult) { r.AllocHint.ExpectedObjectSize++ },
		func(r *OperationResult) { r.AllocHint.Flags = 0 },
		func(r *OperationResult) { r.PayloadLength, r.Data = 1, []byte{0} },
		func(r *OperationResult) { r.Code = Write },
	} {
		result := valid
		edit(&result)
		if err := ValidateAllocHintReply(op, result); err == nil {
			t.Fatal("contradictory allocation hint reply accepted")
		}
	}
	m := oracleRequest()
	m.MTimeSeconds = 1
	m.Operations = []Operation{op}
	encoded, err := EncodeRequest(m, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	// Decode the request's own union bytes with the reply decoder layout.
	union := encoded.Front[113+6 : 113+6+28]
	d := wire.NewDecoderLimit(union, 64)
	if got := decodeAllocHintUnion(d); d.Done() != nil || got != op.AllocHint {
		t.Fatal("allocation hint union round trip differs", got)
	}
	bad := append([]byte(nil), union...)
	bad[27] = 1
	d = wire.NewDecoderLimit(bad, 64)
	decodeAllocHintUnion(d)
	if d.Done() == nil {
		t.Fatal("nonzero allocation hint padding accepted")
	}
}

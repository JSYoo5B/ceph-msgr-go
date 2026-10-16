package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func u64XattrOperation(value uint64) Operation {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, value)
	return Operation{Code: CompareXattr, Xattr: XattrInput{Name: "number", ComparisonOp: XattrCompareGreater, ComparisonMode: XattrCompareU64}, Data: data}
}

// These explicit literals check the pinned packed fields. Independently
// generated native vectors and live server semantics are separate evidence.
func TestXattrU64BoundedWireAndReply(t *testing.T) {
	for _, version := range []int{6, 8} {
		r := nativeV8Request("read")
		op := u64XattrOperation(0xfedcba9876543210)
		r.Operations = []Operation{op}
		var m msgr.MessageData
		var err error
		if version == 6 {
			m, err = EncodeRequest(r.Request, oracleFeatures, 1024)
		} else {
			m, err = EncodeRequestV8(r, v8OracleFeatures, 1024)
		}
		if err != nil {
			t.Fatal(version, err)
		}
		union := xattrLiteral(t, "0313 00000000 06000000 08000000 0302 000000000000000000000000000000000000 0e000000")
		if !bytes.Contains(m.Front, union) || !bytes.Equal(m.Data, xattrLiteral(t, "6e756d626572 1032547698badcfe")) {
			t.Fatal("U64 request differs from pinned fields", version, m)
		}
		op.Data[0] = 99
		if m.Data[6] != 0x10 {
			t.Fatal("U64 payload borrowed caller storage")
		}
	}
	op := u64XattrOperation(^uint64(0))
	union := xattrLiteral(t, "06000000 08000000 0302 000000000000000000000000000000000000")
	for _, result := range []int32{1, -125, -22} {
		m := xattrTestReply(t, CompareXattr, union, result, nil)
		reply, err := DecodeReply(m, 1024)
		if err != nil {
			t.Fatal(err)
		}
		got := reply.Operations[0]
		if got.Result != result || got.Xattr.ComparisonMode != XattrCompareU64 || got.Xattr.ValueLength != 8 || len(got.Data) != 0 {
			t.Fatal("U64 reply metadata lost", got)
		}
		if err := ValidateXattrReply(op, got, result >= 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, length := range []uint32{0, 1, 7, 9, ^uint32(0)} {
		bad := append([]byte(nil), union...)
		binary.LittleEndian.PutUint32(bad[4:8], length)
		if _, err := DecodeReply(xattrTestReply(t, CompareXattr, bad, 1, nil), 1024); !errors.Is(err, msgr.ErrFrame) {
			t.Fatal("non-u64-width echo accepted", length, err)
		}
	}
}

func TestXattrU64InputLimitsAndOrdering(t *testing.T) {
	op := u64XattrOperation(1 << 63)
	for n := 0; n <= 16; n++ {
		bad := op
		bad.Data = make([]byte, n)
		_, _, err := ValidateOperations([]Operation{bad}, 1024)
		if (n != 8) != errors.Is(err, ErrRequest) {
			t.Fatal("wrong U64 operand width admission", n, err)
		}
	}
	if _, _, err := ValidateOperations([]Operation{op}, 13); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("name+U64 budget ignored", err)
	}
	if flags, bytes, err := ValidateOperations([]Operation{op}, 14); err != nil || flags != FlagRead || bytes != 14 {
		t.Fatal(flags, bytes, err)
	}
	for _, code := range []uint16{SetXattr, RemoveXattr} {
		first := Operation{Code: code, Xattr: XattrInput{Name: "a"}}
		if _, _, err := ValidateOperations([]Operation{first, op}, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("staged xattr comparison was enabled", code, err)
		}
	}
	for _, value := range []uint64{0, 1 << 63, ^uint64(0)} {
		op := u64XattrOperation(value)
		for _, flags := range []uint32{0, OpFailOK} {
			op.Flags = flags
			if _, _, err := ValidateOperations([]Operation{op, {Code: SetXattr, Xattr: XattrInput{Name: "marker"}, Data: []byte("v")}}, 1024); err != nil {
				t.Fatal("valid guard-before-write rejected", err)
			}
		}
	}
}

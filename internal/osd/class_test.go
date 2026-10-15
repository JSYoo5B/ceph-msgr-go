package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func callOp(mode uint8, data string) Operation {
	return Operation{Code: Call, Data: []byte(data), Class: ClassInput{Class: "hello", Method: "say_hello", Mode: mode, MaxOutputBytes: 64}}
}

// Source-derived literal following pinned rados.h cls union and Objecter.h
// add_call; fresh native encoder vectors are separate evidence.
func TestClassCallSourceLiteralEncoding(t *testing.T) {
	r := oracleRequest()
	r.Operations = []Operation{callOp(ClassRead, "Tester")}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("read-mode CALL changed request flags")
	}
	want := xattrLiteral(t, "0114 00000000 05 09 00 06000000 "+strings.Repeat("00", 21)+" 14000000")
	if !bytes.Equal(m.Front[113:151], want) || !bytes.Equal(m.Data, []byte("hellosay_helloTester")) {
		t.Fatalf("CALL union/data differs: %x / %q", m.Front[113:151], m.Data)
	}
	r.MTimeSeconds = 1
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrRequest) {
		t.Fatal("read-mode CALL admitted a mutation mtime", err)
	}
	r.Operations = []Operation{callOp(ClassWrite, "")}
	if m, err = EncodeRequest(r, oracleFeatures, 1024); err != nil || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec) || !bytes.Equal(m.Data, []byte("hellosay_hello")) {
		t.Fatal("write-mode CALL with empty input differs", err)
	}
}

func TestClassCallAdmission(t *testing.T) {
	long := strings.Repeat("c", 256)
	for _, tc := range []struct {
		name string
		edit func(*Operation)
		want error
	}{
		{"empty class", func(op *Operation) { op.Class.Class = "" }, ErrRequest},
		{"empty method", func(op *Operation) { op.Class.Method = "" }, ErrRequest},
		{"class longer than u8", func(op *Operation) { op.Class.Class = long }, ErrRequest},
		{"method longer than u8", func(op *Operation) { op.Class.Method = long }, ErrRequest},
		{"NUL class", func(op *Operation) { op.Class.Class = "he\x00llo" }, ErrRequest},
		{"NUL method", func(op *Operation) { op.Class.Method = "say\x00" }, ErrRequest},
		{"missing mode", func(op *Operation) { op.Class.Mode = 0 }, ErrRequest},
		{"unknown mode", func(op *Operation) { op.Class.Mode = 3 }, ErrRequest},
		{"zero budget", func(op *Operation) { op.Class.MaxOutputBytes = 0 }, ErrRequest},
		{"frame budget", func(op *Operation) { op.Class.MaxOutputBytes = 1025 }, wire.ErrLimit},
		{"FAILOK", func(op *Operation) { op.Flags = OpFailOK }, ErrOperation},
		{"EXCL", func(op *Operation) { op.Flags = OpExclusive }, ErrOperation},
		{"caller extent", func(op *Operation) { op.Length = 1 }, ErrRequest},
		{"caller version", func(op *Operation) { op.Version = 1 }, ErrRequest},
		{"xattr variant", func(op *Operation) { op.Xattr.Name = "x" }, ErrRequest},
		{"omap variant", func(op *Operation) { op.Omap.Keys = []string{} }, ErrRequest},
		{"large input", func(op *Operation) { op.Data = make([]byte, 1024) }, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := callOp(ClassRead, "x")
			tc.edit(&op)
			r := oracleRequest()
			r.Operations = []Operation{op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid CALL admitted", err)
			}
		})
	}
	maxName := strings.Repeat("m", 255)
	op := callOp(ClassRead, "")
	op.Class.Class, op.Class.Method = maxName, maxName
	if _, _, err := ValidateOperations([]Operation{op}, 4096); err != nil {
		t.Fatal("255-byte class and method rejected", err)
	}
	for _, base := range []Operation{{Code: Stat}, {Code: Write, Length: 1, Data: []byte{1}}, {Code: OmapClear}, {Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 8}}} {
		base.Class = ClassInput{Class: "hello"}
		if _, _, err := ValidateOperations([]Operation{base}, 1024); !errors.Is(err, ErrRequest) {
			t.Fatal("non-CALL operation admitted a class variant", base.Code, err)
		}
	}
}

func TestClassCallStagedOrdering(t *testing.T) {
	call := callOp(ClassWrite, "")
	readCall := callOp(ClassRead, "")
	read := Operation{Code: Read, Length: 4}
	getxattr := Operation{Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 8}}
	cmpxattr := Operation{Code: CompareXattr, Data: []byte("v"), Xattr: XattrInput{Name: "a", ComparisonOp: XattrCompareEqual, ComparisonMode: XattrCompareString}}
	omapGet := Operation{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 8}}
	failOKSet := Operation{Code: SetXattr, Flags: OpFailOK, Data: []byte("v"), Xattr: XattrInput{Name: "a"}}
	for _, ops := range [][]Operation{
		{{Code: Write, Length: 1, Data: []byte{1}}, call}, {failOKSet, call}, {{Code: Delete}, call},
		{{Code: OmapSetValues}, call}, {call, read}, {call, {Code: Stat}}, {call, getxattr},
		{call, cmpxattr}, {call, omapGet}, {call, {Code: AssertVersion, Version: 1}},
		{{Code: Create, Flags: OpFailOK}, call}, {call, {Code: Write, Length: 1, Data: []byte{1}}, call},
		{{Code: Create}, {Code: SetXattr, Data: []byte("v"), Xattr: XattrInput{Name: "a"}}, call}, {readCall, call, read},
		{call, call}, {call, readCall}, {readCall, call, readCall}, {{Code: Create}, call, call},
	} {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("unsupported CALL ordering admitted", ops[0].Code, ops[1].Code, err)
		}
	}
	for _, ops := range [][]Operation{
		{read, call}, {cmpxattr, call}, {{Code: Stat}, omapGet, call},
		{call, {Code: Write, Length: 1, Data: []byte{1}}}, {call, failOKSet}, {call, {Code: OmapClear}},
		{readCall, call}, {cmpxattr, readCall, readCall, call, {Code: OmapClear}}, {{Code: Create}, call},
		{{Code: Create, Flags: OpExclusive}, readCall, call}, {readCall, {Code: Create, Flags: OpExclusive}, call},
	} {
		if _, _, err := ValidateOperations(ops, 1024); err != nil {
			t.Fatal("guard-before-CALL, CREATE-before-CALL, CALL-after-CALL or write-after-CALL rejected", err)
		}
	}
}

func TestClassCallReplyValidation(t *testing.T) {
	op := callOp(ClassRead, "Tester")
	output := []byte("Hello, Tester!")
	valid := OperationResult{Code: Call, PayloadLength: uint32(len(output)), Data: output,
		Class: ClassResult{ClassLength: 5, MethodLength: 9, InputLength: 6, OutputLength: uint64(len(output))}}
	if err := ValidateClassReply(op, valid); err != nil {
		t.Fatal("valid CALL reply rejected", err)
	}
	positive := valid
	positive.Result = 42
	if err := ValidateClassReply(op, positive); err != nil {
		t.Fatal("positive method result rejected", err)
	}
	overflow := OperationResult{Code: Call, Result: -75, Class: ClassResult{ClassLength: 5, MethodLength: 9, InputLength: 6, OutputLength: 520}}
	if err := ValidateClassReply(op, overflow); err != nil {
		t.Fatal("EOVERFLOW-cleared output rejected", err)
	}
	unexecuted := OperationResult{Code: Call, Class: ClassResult{ClassLength: 5, MethodLength: 9, InputLength: 6}}
	if err := ValidateClassReply(op, unexecuted); err != nil {
		t.Fatal("unexecuted CALL union echo rejected", err)
	}
	for _, edit := range []func(*OperationResult){
		func(r *OperationResult) { r.Class.ClassLength = 4 },
		func(r *OperationResult) { r.Class.MethodLength = 10 },
		func(r *OperationResult) { r.Class.InputLength = 7 },
		func(r *OperationResult) { r.Class.OutputLength = 13 },
		func(r *OperationResult) { r.PayloadLength = 13 },
		func(r *OperationResult) { r.Code = Read },
		func(r *OperationResult) { r.Offset = 1 },
		func(r *OperationResult) { r.Data, r.PayloadLength, r.Class.OutputLength, r.Result = nil, 0, 520, -22 },
	} {
		result := valid
		edit(&result)
		if err := ValidateClassReply(op, result); err == nil {
			t.Fatal("contradictory CALL reply accepted", result)
		}
	}
	small := op
	small.Class.MaxOutputBytes = 13
	if err := ValidateClassReply(small, valid); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("CALL local output budget ignored", err)
	}
	union := func(pad byte, argc byte) []byte {
		e := wire.Encoder{}
		e.U8(5)
		e.U8(9)
		e.U8(argc)
		e.U32(6)
		e.U8(pad)
		e.U64(14)
		e.U64(0)
		e.U32(0)
		return e.Data
	}
	d := wire.NewDecoderLimit(union(0, 0), 64)
	if c := decodeClassUnion(d); d.Done() != nil || c.OutputLength != 14 || c.InputLength != 6 {
		t.Fatal("valid CALL reply union not decoded", c)
	}
	for _, bad := range [][]byte{union(1, 0), union(0, 1)} {
		d := wire.NewDecoderLimit(bad, 64)
		decodeClassUnion(d)
		if d.Done() == nil {
			t.Fatal("CALL reply union with nonzero padding/argc accepted")
		}
	}
}

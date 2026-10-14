package osd

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func xattrLiteral(t testing.TB, text string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.ReplaceAll(text, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// These union literals independently follow pinned rados.h fields, not a
// product decoder or a fresh native encoder. Native proof is separate.
func TestXattrRequestPackedUnionAndBinaryOwnership(t *testing.T) {
	r := oracleRequest()
	r.Operations = []Operation{
		{Code: GetXattr, Xattr: XattrInput{Name: "bin", MaxOutputBytes: 16}},
		{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}},
		{Code: CompareXattr, Flags: OpFailOK, Xattr: XattrInput{Name: "bin", ComparisonOp: XattrCompareLessEqual, ComparisonMode: XattrCompareString}, Data: []byte{0, 255, 65, 10}},
		{Code: SetXattr, Xattr: XattrInput{Name: "bin"}, Data: []byte{0, 255, 65, 10}},
		{Code: RemoveXattr, Xattr: XattrInput{Name: "bin"}},
	}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagWrite|FlagOnDisk|FlagReturnVec) {
		t.Fatal("xattrs did not preserve vector request mode/completion flags")
	}
	const reserved = "000000000000000000000000000000000000"
	for i, literal := range []string{
		"0113 00000000 03000000 00000000 0000 " + reserved + " 03000000",
		"0213 00000000 00000000 00000000 0000 " + reserved + " 00000000",
		"0313 02000000 03000000 04000000 0601 " + reserved + " 07000000",
		"0123 00000000 03000000 04000000 0000 " + reserved + " 07000000",
		"0423 00000000 03000000 00000000 0000 " + reserved + " 03000000",
	} {
		if !bytes.Equal(m.Front[113+i*38:113+(i+1)*38], xattrLiteral(t, literal)) {
			t.Fatal("packed xattr union/data length differs from source literal", i)
		}
	}
	if !bytes.Equal(m.Data, xattrLiteral(t, "62696e 62696e00ff410a 62696e00ff410a 62696e")) {
		t.Fatal("name/value input concatenation lost binary bytes")
	}
	r.Operations[2].Data[0], r.Operations[3].Data[0] = 9, 10
	if m.Data[6] != 0 || m.Data[13] != 0 {
		t.Fatal("xattr request borrowed caller value bytes")
	}
}

func TestXattrInputValidationAndBudgets(t *testing.T) {
	get := Operation{Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 16}}
	cmp := Operation{Code: CompareXattr, Xattr: XattrInput{Name: "a", ComparisonOp: 1, ComparisonMode: 1}}
	set := Operation{Code: SetXattr, Xattr: XattrInput{Name: "a"}}
	for _, tc := range []struct {
		name string
		op   Operation
		edit func(*Operation)
		want error
	}{
		{"empty-name", get, func(o *Operation) { o.Xattr.Name = "" }, ErrRequest},
		{"leading-nul-name", get, func(o *Operation) { o.Xattr.Name = "\x00a" }, ErrRequest},
		{"embedded-nul-name", set, func(o *Operation) { o.Xattr.Name = "a\x00b" }, ErrRequest},
		{"trailing-nul-name", cmp, func(o *Operation) { o.Xattr.Name = "a\x00" }, ErrRequest},
		{"zero-output-budget", get, func(o *Operation) { o.Xattr.MaxOutputBytes = 0 }, ErrRequest},
		{"huge-output-budget", get, func(o *Operation) { o.Xattr.MaxOutputBytes = math.MaxUint32 }, wire.ErrLimit},
		{"extent-offset", get, func(o *Operation) { o.Offset = 1 }, ErrRequest},
		{"extent-length", set, func(o *Operation) { o.Length = 1 }, ErrRequest},
		{"version", cmp, func(o *Operation) { o.Version = 19 }, ErrRequest},
		{"get-input-value", get, func(o *Operation) { o.Data = []byte{0} }, ErrRequest},
		{"non-cmp-op", get, func(o *Operation) { o.Xattr.ComparisonOp = 1 }, ErrRequest},
		{"non-cmp-mode", set, func(o *Operation) { o.Xattr.ComparisonMode = 1 }, ErrRequest},
		{"unsupported-u64", cmp, func(o *Operation) { o.Xattr.ComparisonMode = 2 }, ErrOperation},
		{"unknown-cmp", cmp, func(o *Operation) { o.Xattr.ComparisonOp = 7 }, ErrOperation},
		{"missing-cmp", cmp, func(o *Operation) { o.Xattr.ComparisonOp = 0 }, ErrOperation},
		{"cmp-output-budget", cmp, func(o *Operation) { o.Xattr.MaxOutputBytes = 1 }, ErrRequest},
		{"set-output-budget", set, func(o *Operation) { o.Xattr.MaxOutputBytes = 1 }, ErrRequest},
		{"exclusive-xattr", set, func(o *Operation) { o.Flags = OpExclusive }, ErrOperation},
		{"unknown-flags", cmp, func(o *Operation) { o.Flags = 4 }, ErrOperation},
		{"getall-name", Operation{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 16}}, func(o *Operation) { o.Xattr.Name = "a" }, ErrRequest},
		{"remove-input-value", Operation{Code: RemoveXattr, Xattr: XattrInput{Name: "a"}}, func(o *Operation) { o.Data = []byte{0} }, ErrRequest},
		{"old-op-xattr-fields", Operation{Code: Stat}, func(o *Operation) { o.Xattr.Name = "a" }, ErrRequest},
		{"bulk-set-unsupported", Operation{Code: 0x2302}, func(*Operation) {}, ErrOperation},
		{"bulk-reset-unsupported", Operation{Code: 0x2303}, func(*Operation) {}, ErrOperation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.edit(&tc.op)
			r := oracleRequest()
			r.Operations = []Operation{tc.op}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
				t.Fatal("invalid xattr operation escaped pre-transmission validation", err)
			}
		})
	}
	for _, name := range []string{"_name", "\xffname", strings.Repeat("a", 101)} {
		r := oracleRequest()
		r.Operations = []Operation{{Code: SetXattr, Xattr: XattrInput{Name: name}}}
		if _, err := EncodeRequest(r, oracleFeatures, 1024); err != nil {
			t.Fatal("name/value bytes were normalized or server name bound hardcoded", err)
		}
	}
	r := oracleRequest()
	r.Operations = []Operation{{Code: SetXattr, Xattr: XattrInput{Name: strings.Repeat("a", 50)}, Data: bytes.Repeat([]byte{255}, 50)}}
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	limit := uint32(41 + len(m.Front) + len(m.Data))
	if _, err := EncodeRequest(r, oracleFeatures, limit); err != nil {
		t.Fatal("exact aggregate front/name/value limit rejected", err)
	}
	if _, err := EncodeRequest(r, oracleFeatures, limit-1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("header plus name/value budget not enforced", err)
	}
	r.Operations = []Operation{{Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 200}}, {Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 200}}}
	if _, err := EncodeRequest(r, oracleFeatures, 600); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("aggregate output budgets escaped frame limit", err)
	}
	if _, err := boundedBytes(math.MaxUint32, math.MaxUint64, 1); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("overflowing budget accepted", err)
	}
}

func TestXattrStagedReadsRemainUnsupported(t *testing.T) {
	for _, mutation := range []uint16{SetXattr, RemoveXattr} {
		for _, getter := range []Operation{
			{Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 16}},
			{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 16}},
			{Code: CompareXattr, Xattr: XattrInput{Name: "a", ComparisonOp: 1, ComparisonMode: 1}},
		} {
			r := oracleRequest()
			r.Operations = []Operation{{Code: mutation, Flags: OpFailOK, Xattr: XattrInput{Name: "a"}}, {Code: Stat}, getter}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrOperation) {
				t.Fatal("staged xattr read/guard accepted", mutation, getter.Code, err)
			}
			r.Operations = []Operation{getter, {Code: mutation, Xattr: XattrInput{Name: "a"}}}
			if _, err := EncodeRequest(r, oracleFeatures, 1024); err != nil {
				t.Fatal("read/guard before xattr mutation rejected", err)
			}
		}
	}
	r := oracleRequest()
	r.Operations = []Operation{{Code: WriteFull, Length: 3, Data: []byte("new")}, {Code: SetXattr, Xattr: XattrInput{Name: "a"}}, {Code: Read, Length: 3}}
	if _, err := EncodeRequest(r, oracleFeatures, 1024); !errors.Is(err, ErrOperation) {
		t.Fatal("xattr operation erased staged data digest guard", err)
	}
}

// Reuse a native v6 envelope only as a framing template, replacing its op
// with source-derived independent scalar bytes. No product reply encoder exists.
func xattrTestReply(t testing.TB, code uint16, union []byte, result int32, data []byte) msgr.MessageData {
	t.Helper()
	encoded, err := os.ReadFile("testdata/reply-v6-success.front.hex")
	if err != nil {
		t.Fatal(err)
	}
	front := xattrLiteral(t, strings.TrimSpace(string(encoded)))
	e := wire.Encoder{Data: bytes.Clone(front[:62])}
	binary.LittleEndian.PutUint32(e.Data[58:62], 1)
	e.U16(code)
	e.U32(0)
	e.Raw(union)
	e.U32(uint32(len(data)))
	e.U32(0) // retry attempt
	e.U32(uint32(result))
	e.Raw(front[150:]) // native versions, redirect and trace suffix
	return msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2, Front: e.Data, Data: data}
}

func TestXattrReplyRawUnionAndMalformedReservedBytes(t *testing.T) {
	union := xattrLiteral(t, "03000000 04000000 0000 000000000000000000000000000000000000")
	m := xattrTestReply(t, GetXattr, union, 0, []byte{0, 255, 65, 10})
	r, err := DecodeReply(m, 1024)
	if err != nil || len(r.Operations) != 1 {
		t.Fatal(err)
	}
	op := r.Operations[0]
	if op.Xattr != (XattrResult{NameLength: 3, ValueLength: 4}) || op.Offset != 0 || op.Length != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 || !bytes.Equal(op.Data, m.Data) || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
		t.Fatal("xattr union was reinterpreted as extents or bytes lost", op)
	}
	m.Data[0], m.Front[68] = 9, 10
	if op.Data[0] != 0 || r.RawFront[68] != 3 {
		t.Fatal("decoded xattr metadata/output borrowed input")
	}
	for _, tc := range []struct {
		name string
		code uint16
		edit func(*msgr.MessageData)
		want error
	}{
		{"reserved-first", GetXattr, func(m *msgr.MessageData) { m.Front[78] = 1 }, msgr.ErrFrame},
		{"reserved-last", GetXattr, func(m *msgr.MessageData) { m.Front[95] = 1 }, msgr.ErrFrame},
		{"non-cmp-operator", GetXattr, func(m *msgr.MessageData) { m.Front[76] = 1 }, msgr.ErrFrame},
		{"non-cmp-mode", GetXattr, func(m *msgr.MessageData) { m.Front[77] = 1 }, msgr.ErrFrame},
		{"unsupported-u64", CompareXattr, func(m *msgr.MessageData) { m.Front[76], m.Front[77] = 1, 2 }, ErrOperation},
		{"unknown-operator", CompareXattr, func(m *msgr.MessageData) { m.Front[76], m.Front[77] = 7, 1 }, ErrOperation},
		{"huge-payload", GetXattr, func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[96:100], math.MaxUint32) }, wire.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrTestReply(t, tc.code, union, 0, []byte{0, 255, 65, 10})
			tc.edit(&m)
			if _, err := DecodeReply(m, 1024); !errors.Is(err, tc.want) {
				t.Fatal("malformed xattr metadata accepted", err)
			}
		})
	}
	m = xattrTestReply(t, GetXattr, union, 0, []byte{0, 255, 65, 10})
	for length := 0; length < len(m.Front); length++ {
		truncated := m
		truncated.Front = m.Front[:length]
		if _, err := DecodeReply(truncated, 1024); err == nil {
			t.Fatal("truncated xattr reply accepted", length)
		}
	}
}

func TestValidateXattrReplyNormalizationAndOutputBounds(t *testing.T) {
	get := Operation{Code: GetXattr, Xattr: XattrInput{Name: "bin", MaxOutputBytes: 4}}
	getReply := OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 3, ValueLength: 4}, PayloadLength: 4}
	for _, tc := range []struct {
		name    string
		in      Operation
		out     OperationResult
		success bool
		valid   bool
	}{
		{"get-normalized", get, getReply, true, true},
		{"get-empty", get, OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 3}}, true, true},
		{"get-unexecuted-request-echo", get, OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 3}}, false, true},
		{"get-inconsistent-value-length", get, OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 3, ValueLength: 2}, PayloadLength: 4}, true, false},
		{"get-over-budget", get, OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 3, ValueLength: 5}, PayloadLength: 5}, true, false},
		{"get-wrong-name-length", get, OperationResult{Code: GetXattr, Xattr: XattrResult{NameLength: 2}}, false, false},
		{"get-not-an-extent", get, OperationResult{Code: GetXattr, Offset: 3, Xattr: XattrResult{NameLength: 3}}, false, false},
		{"getall-empty-map", Operation{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 4}}, OperationResult{Code: GetXattrs, PayloadLength: 4}, true, true},
		{"getall-unexecuted", Operation{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 4}}, OperationResult{Code: GetXattrs}, false, true},
		{"getall-missing-count", Operation{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 4}}, OperationResult{Code: GetXattrs, PayloadLength: 3}, true, false},
		{"getall-over-budget", Operation{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 4}}, OperationResult{Code: GetXattrs, PayloadLength: 5}, false, false},
		{"set-original-input-length", Operation{Code: SetXattr, Xattr: XattrInput{Name: "bin"}, Data: []byte{0, 255}}, OperationResult{Code: SetXattr, Xattr: XattrResult{NameLength: 3, ValueLength: 2}}, false, true},
		{"set-unsupported-output", Operation{Code: SetXattr, Xattr: XattrInput{Name: "bin"}}, OperationResult{Code: SetXattr, Xattr: XattrResult{NameLength: 3}, PayloadLength: 1}, true, false},
		{"cmp-positive-result", Operation{Code: CompareXattr, Xattr: XattrInput{Name: "bin", ComparisonOp: 1, ComparisonMode: 1}}, OperationResult{Code: CompareXattr, Xattr: XattrResult{NameLength: 3, ComparisonOp: 1, ComparisonMode: 1}, Result: 1}, true, true},
		{"cmp-wrong-mode", Operation{Code: CompareXattr, Xattr: XattrInput{Name: "bin", ComparisonOp: 1, ComparisonMode: 1}}, OperationResult{Code: CompareXattr, Xattr: XattrResult{NameLength: 3, ComparisonOp: 1, ComparisonMode: 2}}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateXattrReply(tc.in, tc.out, tc.success)
			if (err == nil) != tc.valid {
				t.Fatal("xattr reply normalization/budget rule differs", err)
			}
		})
	}
}

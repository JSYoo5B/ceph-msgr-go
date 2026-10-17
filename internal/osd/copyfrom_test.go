package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func copyFromOperation(code uint16) Operation {
	return Operation{Code: code, CopyFrom: CopyFromInput{Object: "src\x01é", Locator: Locator{Pool: 7, Key: "k", Namespace: "ns", Hash: -1}, Version: 0x0102030405060708}}
}

// expectedCopyFromData is the object_t and object_locator_t encoding written
// independently of the production helper.
func expectedCopyFromData(in CopyFromInput) []byte {
	var b bytes.Buffer
	put := func(v any) { _ = binary.Write(&b, binary.LittleEndian, v) }
	put(uint32(len(in.Object)))
	b.WriteString(in.Object)
	var loc bytes.Buffer
	lput := func(v any) { _ = binary.Write(&loc, binary.LittleEndian, v) }
	lput(in.Locator.Pool)
	lput(int32(-1))
	lput(uint32(len(in.Locator.Key)))
	loc.WriteString(in.Locator.Key)
	lput(uint32(len(in.Locator.Namespace)))
	loc.WriteString(in.Locator.Namespace)
	lput(int64(-1))
	b.Write([]byte{6, 3})
	put(uint32(loc.Len()))
	b.Write(loc.Bytes())
	if in.Flags&CopyFromTruncateSequence != 0 {
		put(in.TruncateSequence)
		put(in.TruncateSize)
	}
	return b.Bytes()
}

func TestCopyFromV6V8UnionAndIndata(t *testing.T) {
	for _, code := range []uint16{CopyFrom, CopyFrom2} {
		for _, truncate := range []bool{false, true} {
			if truncate && code == CopyFrom {
				continue
			}
			for _, version := range []int{6, 8} {
				t.Run(fmt.Sprintf("%#x/%v/v%d", code, truncate, version), func(t *testing.T) {
					r := nativeV8Request("mutation")
					op := copyFromOperation(code)
					if truncate {
						op.CopyFrom.Flags, op.CopyFrom.TruncateSequence, op.CopyFrom.TruncateSize = CopyFromTruncateSequence, 9, 0xfedcba98
						op.CopyFrom.Snap = 6 // a snapshot source changes only the union's snapid
					}
					r.Operations = []Operation{op}
					r.SnapSeq, r.Snaps = 4, []uint64{4, 2}
					var m msgr.MessageData
					var err error
					tailBytes := 32
					if version == 6 {
						m, err = EncodeRequest(r.Request, oracleFeatures, 1024)
						tailBytes = 59
					} else {
						m, err = EncodeRequestV8(r, v8OracleFeatures, 1024)
					}
					if err != nil {
						t.Fatal(err)
					}
					start := len(m.Front) - tailBytes - 16 - opWireSize
					union := m.Front[start : start+opWireSize]
					want := expectedCopyFromData(op.CopyFrom)
					if binary.LittleEndian.Uint16(union) != code || binary.LittleEndian.Uint32(union[2:]) != 0 ||
						binary.LittleEndian.Uint64(union[6:]) != copyFromSnap(op.CopyFrom) || binary.LittleEndian.Uint64(union[14:]) != op.CopyFrom.Version ||
						union[22] != op.CopyFrom.Flags || binary.LittleEndian.Uint32(union[23:]) != 0 || !bytes.Equal(union[27:34], make([]byte, 7)) ||
						binary.LittleEndian.Uint32(union[34:]) != uint32(len(want)) {
						t.Fatalf("packed copy_from union %x", union)
					}
					if !bytes.Equal(m.Data, want) {
						t.Fatalf("indata\n got %x\nwant %x", m.Data, want)
					}
					r.Operations[0].CopyFrom.Object = "mutated"
					if !bytes.Equal(m.Data, want) {
						t.Fatal("indata borrowed caller memory")
					}
				})
			}
		}
	}
}

func TestCopyFromValidationBounds(t *testing.T) {
	base := copyFromOperation(CopyFrom2)
	trunc := func(o *Operation) {
		o.CopyFrom.Flags, o.CopyFrom.TruncateSequence, o.CopyFrom.TruncateSize = CopyFromTruncateSequence, 2, 1
	}
	bad := map[string]func(*Operation){
		"snap dir":            func(o *Operation) { o.CopyFrom.Snap = SnapDir },
		"empty source":        func(o *Operation) { o.CopyFrom.Object = "" },
		"nul source":          func(o *Operation) { o.CopyFrom.Object = "a\x00b" },
		"negative pool":       func(o *Operation) { o.CopyFrom.Locator.Pool = -1 },
		"explicit hash":       func(o *Operation) { o.CopyFrom.Locator.Hash = 3 },
		"unknown flag":        func(o *Operation) { o.CopyFrom.Flags = 2 },
		"flush flag":          func(o *Operation) { o.CopyFrom.Flags = 1 },
		"orphan sequence":     func(o *Operation) { o.CopyFrom.TruncateSequence = 2 },
		"orphan size":         func(o *Operation) { o.CopyFrom.TruncateSize = 2 },
		"zero sequence":       func(o *Operation) { trunc(o); o.CopyFrom.TruncateSequence = 0 },
		"rewritten pair":      func(o *Operation) { trunc(o); o.CopyFrom.TruncateSequence, o.CopyFrom.TruncateSize = 1, math.MaxUint64 },
		"extent":              func(o *Operation) { o.Offset = 1 },
		"length":              func(o *Operation) { o.Length = 1 },
		"outer version":       func(o *Operation) { o.Version = 1 },
		"payload":             func(o *Operation) { o.Data = []byte{1} },
		"snap":                func(o *Operation) { o.Snap = 3 },
		"xattr":               func(o *Operation) { o.Xattr.Name = "x" },
		"extent truncate":     func(o *Operation) { o.TruncateSize, o.TruncateSequence = 1, 2 },
		"class":               func(o *Operation) { o.Class.Class = "c" },
		"exclusive":           func(o *Operation) { o.Flags = OpExclusive },
		"failok":              func(o *Operation) { o.Flags = OpFailOK },
		"copy-from truncate":  func(o *Operation) { o.Code = CopyFrom; trunc(o) },
		"copy-from any flags": func(o *Operation) { o.Code = CopyFrom; o.CopyFrom.Flags = 4 },
	}
	for name, edit := range bad {
		op := base
		edit(&op)
		if _, _, err := ValidateOperations([]Operation{op}, 4096); err == nil {
			t.Fatal("accepted", name)
		}
	}
	for _, op := range []Operation{base, func() Operation { o := base; trunc(&o); return o }(), func() Operation { o := base; o.Code = CopyFrom; return o }()} {
		flags, n, err := ValidateOperations([]Operation{op}, 4096)
		if err != nil || flags != FlagWrite || n != copyFromDataBytes(op) || n != uint64(len(expectedCopyFromData(op.CopyFrom))) {
			t.Fatal("valid copy", flags, n, err)
		}
	}
	// Other opcodes may not carry the union, and COPY_FROM never composes.
	other := Operation{Code: Stat, CopyFrom: base.CopyFrom}
	if _, _, err := ValidateOperations([]Operation{other}, 4096); !errors.Is(err, ErrRequest) {
		t.Fatal("stat carried copy input", err)
	}
	for _, ops := range [][]Operation{{{Code: Create}, base}, {base, {Code: Stat}}, {base, base}} {
		if _, _, err := ValidateOperations(ops, 4096); !errors.Is(err, ErrOperation) {
			t.Fatal("copy composed", err)
		}
	}
	long := base
	long.CopyFrom.Object = string(bytes.Repeat([]byte{'a'}, 4096))
	if _, _, err := ValidateOperations([]Operation{long}, 4096); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("source name escaped frame bound", err)
	}
	// A write SnapContext applies; a read snapshot does not.
	r := nativeV8Request("mutation")
	r.Operations = []Operation{base}
	r.ReadSnap = 3
	if _, err := EncodeRequest(r.Request, oracleFeatures, 4096); !errors.Is(err, ErrRequest) {
		t.Fatal("copy at a read snapshot", err)
	}
}

func copyFromReply(t *testing.T, version int, code uint16, union []byte, payload uint32, result int32) msgr.MessageData {
	t.Helper()
	m := oracleReply(t, "reply-v6-success")
	e := wire.Encoder{Data: bytes.Clone(m.Front[:62])}
	binary.LittleEndian.PutUint32(e.Data[58:], 1)
	e.U16(code)
	e.U32(0)
	e.Raw(union)
	e.U32(payload)
	e.U32(0)
	e.U32(uint32(result))
	e.Raw(m.Front[150:])
	m.Front, m.Data = e.Data, make([]byte, payload)
	if version == 8 {
		m.Version = 8
		m.Front = append(bytes.Clone(m.Front[:128]), append([]byte{0}, m.Front[len(m.Front)-24:]...)...)
	}
	return m
}

func TestCopyFromReplyUnionEchoAndValidation(t *testing.T) {
	op := copyFromOperation(CopyFrom2)
	op.CopyFrom.Flags, op.CopyFrom.TruncateSequence, op.CopyFrom.TruncateSize = CopyFromTruncateSequence, 3, 4
	snapOp := op
	snapOp.CopyFrom.Snap = 9
	union := func(snap uint64) []byte {
		e := wire.Encoder{}
		e.U64(snap)
		e.U64(op.CopyFrom.Version)
		e.U8(op.CopyFrom.Flags)
		e.U32(0)
		e.Raw(make([]byte, 7))
		return e.Data
	}
	for _, version := range []int{6, 8} {
		decode := func(m msgr.MessageData) (Reply, error) {
			if version == 6 {
				return DecodeReply(m, 1024)
			}
			return DecodeReplyV8(m, 1024)
		}
		for _, result := range []int32{0, -34, -22} {
			r, err := decode(copyFromReply(t, version, CopyFrom2, union(NoSnap), 0, result))
			if err != nil {
				t.Fatal(version, err)
			}
			got := r.Operations[0]
			if got.CopyFrom != (CopyFromResult{Snap: NoSnap, Version: op.CopyFrom.Version, Flags: CopyFromTruncateSequence}) || got.Result != result || got.Offset != 0 || got.Length != 0 {
				t.Fatal("union not preserved", got)
			}
			if err := ValidateCopyFromReply(op, got); err != nil {
				t.Fatal(err)
			}
			if ValidateCopyFromReply(snapOp, got) == nil {
				t.Fatal("head echo accepted for a snapshot source")
			}
			for name, edit := range map[string]func(*OperationResult){
				"code":    func(o *OperationResult) { o.Code = CopyFrom },
				"snap":    func(o *OperationResult) { o.CopyFrom.Snap = 3 },
				"version": func(o *OperationResult) { o.CopyFrom.Version++ },
				"flags":   func(o *OperationResult) { o.CopyFrom.Flags = 0 },
				"fadvise": func(o *OperationResult) { o.CopyFrom.Fadvise = 1 },
				"extent":  func(o *OperationResult) { o.Length = 1 },
				"payload": func(o *OperationResult) { o.PayloadLength, o.Data = 1, []byte{1} },
			} {
				altered := got
				edit(&altered)
				if ValidateCopyFromReply(op, altered) == nil {
					t.Fatal("accepted altered", name)
				}
			}
		}
		pad := copyFromReply(t, version, CopyFrom2, union(NoSnap), 0, 0)
		pad.Front[62+6+8+8+1+4] = 1
		if _, err := decode(pad); !errors.Is(err, msgr.ErrFrame) {
			t.Fatal("nonzero union padding", version, err)
		}
	}
	if err := ValidateCopyFromReply(Operation{Code: Stat}, OperationResult{}); !errors.Is(err, ErrOperation) {
		t.Fatal(err)
	}
}

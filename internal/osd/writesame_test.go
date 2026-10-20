package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestWriteSameV6V8PatternSnapshotAndOwnership(t *testing.T) {
	for _, version := range []int{6, 8} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			r := nativeV8Request("mutation")
			r.Operations = []Operation{{Code: WriteSame, Offset: 0xfedcba9876543200, Length: 12, Data: []byte{0, 255, 'x'}}}
			r.SnapSeq, r.Snaps, r.OrderSnap = 7, []uint64{7, 5}, true
			encode := func(r V8Request) (msgr.MessageData, error) {
				if version == 6 {
					return EncodeRequest(r.Request, oracleFeatures, 1024)
				}
				return EncodeRequestV8(r, v8OracleFeatures, 1024)
			}
			m, err := encode(r)
			if err != nil {
				t.Fatal(err)
			}
			tailBytes, flagOffset := 32, 32
			if version == 6 {
				tailBytes, flagOffset = 59, 8
			}
			start := len(m.Front) - tailBytes - 16 - opWireSize
			op := m.Front[start : start+opWireSize]
			if binary.LittleEndian.Uint16(op) != 0x2226 || binary.LittleEndian.Uint64(op[6:]) != r.Operations[0].Offset || binary.LittleEndian.Uint64(op[14:]) != 12 || binary.LittleEndian.Uint64(op[22:]) != 3 || binary.LittleEndian.Uint32(op[30:]) != 0 || binary.LittleEndian.Uint32(op[34:]) != 3 || !bytes.Equal(m.Data, []byte{0, 255, 'x'}) {
				t.Fatal("pattern bytes, packed union or logical extent", op, m.Data)
			}
			tail := start + opWireSize
			if binary.LittleEndian.Uint64(m.Front[tail:]) != NoSnap || binary.LittleEndian.Uint64(m.Front[tail+8:]) != 7 || binary.LittleEndian.Uint32(m.Front[tail+16:]) != 2 || binary.LittleEndian.Uint64(m.Front[tail+20:]) != 7 || binary.LittleEndian.Uint64(m.Front[tail+28:]) != 5 || binary.LittleEndian.Uint32(m.Front[flagOffset:]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec|FlagOrderSnap) {
				t.Fatal("write snapshot tail or mode")
			}
			r.Operations[0].Data[0] = 42
			if m.Data[0] != 0 {
				t.Fatal("pattern borrowed caller memory")
			}
			for _, edit := range []func(*V8Request){func(r *V8Request) { r.ReadSnap = 1 }, func(r *V8Request) { r.ReadSnap = SnapDir }, func(r *V8Request) { r.Snaps = []uint64{5, 7} }, func(r *V8Request) { r.Operations[0].TruncateSize = 4; r.Operations[0].TruncateSequence = 2 }} {
				bad := r
				bad.Operations = append([]Operation(nil), r.Operations...)
				edit(&bad)
				if _, err := encode(bad); !errors.Is(err, ErrRequest) {
					t.Fatal("invalid write snapshot or mixed union", err)
				}
			}
		})
	}
}

func TestWriteSameBoundsAndCompoundSafety(t *testing.T) {
	base := Operation{Code: WriteSame, Length: 12, Data: []byte{0, 1, 2}}
	for _, edit := range []func(*Operation){func(o *Operation) { o.Data = nil }, func(o *Operation) { o.Data = nil; o.Length = 0 }, func(o *Operation) { o.Length = 11 }, func(o *Operation) { o.Length = 2 }, func(o *Operation) { o.Offset = math.MaxUint64 - 10 }, func(o *Operation) { o.TruncateSequence = 2 }, func(o *Operation) { o.Version = 1 }, func(o *Operation) { o.Flags = OpExclusive }, func(o *Operation) { o.Xattr.Name = "mixed" }} {
		op := base
		edit(&op)
		if _, _, err := ValidateOperations([]Operation{op}, 1024); err == nil {
			t.Fatal("invalid writesame", op)
		}
	}
	for _, length := range []uint64{0, 3, 12, 1023} {
		op := base
		op.Length = length
		if flags, n, err := ValidateOperations([]Operation{op}, 1024); err != nil || flags != FlagWrite || n != 3 {
			t.Fatal("valid pattern/zero length changed wire size", length, flags, n, err)
		}
	}
	// Two one-byte patterns must not bypass the sum of expanded logical work.
	for _, ops := range [][]Operation{{{Code: WriteSame, Length: 1025, Data: []byte{1}}}, {{Code: WriteSame, Length: 600, Data: []byte{1}}, {Code: WriteSame, Flags: OpFailOK, Length: 425, Data: []byte{2}}}, {{Code: WriteSame, Length: math.MaxUint64, Data: []byte{1}}}} {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("logical expansion escaped budget", err)
		}
	}
	for _, code := range []uint16{Write, WriteFull, Append} {
		ops := []Operation{{Code: WriteSame, Length: 1024, Data: []byte{1}}, {Code: code, Length: 1, Data: []byte{2}}}
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("mixed literal/expanded work escaped total", code, err)
		}
		ops[0], ops[1] = ops[1], ops[0]
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("reverse mixed logical budget", code, err)
		}
	}
	if _, n, err := ValidateOperations([]Operation{{Code: WriteSame, Length: 600, Data: []byte{1}}, {Code: WriteSame, Length: 424, Data: []byte{2}}}, 1024); err != nil || n != 2 {
		t.Fatal("exact logical sum", n, err)
	}
	if _, _, err := ValidateOperations([]Operation{{Code: WriteSame, Data: make([]byte, 1025)}}, 1024); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("zero logical work escaped actual input budget", err)
	}
	for _, read := range []uint16{Read, CompareExtent} {
		tail := Operation{Code: read, Length: 6}
		if read == CompareExtent {
			tail.Data = make([]byte, 6)
		}
		ops := []Operation{base, {Code: WriteSame, Flags: OpFailOK, Length: 30, Data: []byte{1}}, tail}
		tail.Length = 12
		ops[2] = tail
		if read == CompareExtent {
			ops[2].Data = make([]byte, 12)
		}
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("staged full digest after FAILOK allowed", read, err)
		}
		ops[2].Offset = 1
		if _, _, err := ValidateOperations(ops, 1024); err != nil {
			t.Fatal("partial read disallowed", err)
		}
	}
	for _, code := range []uint16{Read, CompareExtent} {
		for _, stagedTruncate := range []bool{false, true} {
			for _, flags := range []uint32{0, OpFailOK} {
				ops := []Operation{}
				if stagedTruncate {
					ops = append(ops, Operation{Code: Truncate, Offset: 6, TruncateSize: 6, TruncateSequence: 2})
				}
				ws := base
				ws.Flags = flags
				ops = append(ops, ws, Operation{Code: WriteSame, Offset: 1, Length: 18, Data: []byte{1}, Flags: OpFailOK})
				read := Operation{Code: code, Length: 6}
				if code == CompareExtent {
					read.Data = make([]byte, 6)
				}
				ops = append(ops, read)
				if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
					t.Fatal("clipped staged digest prefix accepted", code, stagedTruncate, flags, err)
				}
				ops[len(ops)-1].Offset = 1
				if _, _, err := ValidateOperations(ops, 1024); err != nil {
					t.Fatal("nonprefix bounded read refused", err)
				}
			}
		}
	}
	unsafe := [][]Operation{{base, {Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 8}}}, {base, {Code: SetXattr, Xattr: XattrInput{Name: "a"}}, {Code: GetXattr, Xattr: XattrInput{Name: "a", MaxOutputBytes: 8}}}, {base, {Code: OmapClear}, {Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 8}}}}
	for _, ops := range unsafe {
		if _, _, err := ValidateOperations(ops, 1024); !errors.Is(err, ErrOperation) {
			t.Fatal("existing staged class/metadata refusal lost", err)
		}
	}
}

func TestWriteSameReplyUnionIsIndependentOfExtentTruncate(t *testing.T) {
	for _, version := range []int{6, 8} {
		m := oracleReply(t, "reply-v6-success")
		e := wire.Encoder{Data: bytes.Clone(m.Front[:62])}
		binary.LittleEndian.PutUint32(e.Data[58:], 1)
		e.U16(WriteSame)
		e.U32(OpFailOK)
		e.U64(17)
		e.U64(12)
		e.U64(3)
		e.U32(0)
		e.U32(0)
		e.U32(0)
		e.U32(uint32(0xffffffe5))
		e.Raw(m.Front[150:])
		m.Front, m.Data = e.Data, nil
		if version == 8 {
			m.Version = 8
			m.Front = append(bytes.Clone(m.Front[:128]), append([]byte{0}, m.Front[len(m.Front)-24:]...)...)
		}
		decode := func(m msgr.MessageData) (Reply, error) {
			if version == 6 {
				return DecodeReply(m, 1024)
			}
			return DecodeReplyV8(m, 1024)
		}
		r, err := decode(m)
		if err != nil {
			t.Fatal(version, err)
		}
		op := r.Operations[0]
		if op.WriteSamePatternLength != 3 || op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Result != -27 || op.Length != 12 || op.Offset != 17 {
			t.Fatal("pattern aliased extent union", op)
		}
		if err := ValidateWriteSameReply(Operation{Code: WriteSame, Offset: 17, Length: 12, Data: []byte{1, 2, 3}}, op); err != nil {
			t.Fatal(err)
		}
		bad := m
		bad.Front = bytes.Clone(m.Front)
		bad.Front[92] = 1
		if _, err := decode(bad); !errors.Is(err, msgr.ErrFrame) {
			t.Fatal("nonzero writesame padding", version, err)
		}
	}
}

package osd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// snapTail reads hobj.snap, snap_seq and snaps after the oracle request's ops,
// which start at front offset 113 (messages/MOSDOp.h:330-332).
func snapTail(t *testing.T, front []byte, ops int) (uint64, uint64, []uint64) {
	t.Helper()
	d := wire.NewDecoder(front[113+ops*opWireSize:])
	snap, seq := d.U64(), d.U64()
	snaps := make([]uint64, d.Count(8, 1024))
	for i := range snaps {
		snaps[i] = d.U64()
	}
	if d.Err() != nil || d.Remaining() != 4+8+27 {
		t.Fatalf("snapshot tail is malformed: %v remaining %d", d.Err(), d.Remaining())
	}
	return snap, seq, snaps
}

func TestSnapshotRequestFieldsAreEncodedAfterOperations(t *testing.T) {
	r := oracleRequest()
	m, err := EncodeRequest(r, oracleFeatures, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if snap, seq, snaps := snapTail(t, m.Front, 2); snap != NoSnap || seq != 0 || len(snaps) != 0 {
		t.Fatal("head request changed its snapshot fields", snap, seq, snaps)
	}
	r.ReadSnap = 4
	if m, err = EncodeRequest(r, oracleFeatures, 1024); err != nil {
		t.Fatal(err)
	}
	if snap, _, _ := snapTail(t, m.Front, 2); snap != 4 || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("read snapshot is not transmitted as hobj.snap", snap)
	}

	w := oracleRequest()
	w.MTimeSeconds = 1
	w.Operations = []Operation{{Code: WriteFull, Length: 2, Data: []byte("v2")}}
	w.SnapSeq, w.Snaps, w.OrderSnap = 7, []uint64{7, 4, 2}, true
	if m, err = EncodeRequest(w, oracleFeatures, 1024); err != nil {
		t.Fatal(err)
	}
	if snap, seq, snaps := snapTail(t, m.Front, 1); snap != NoSnap || seq != 7 || !equalSnaps(snaps, []uint64{7, 4, 2}) ||
		binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOrderSnap|FlagOnDisk|FlagReturnVec) {
		t.Fatal("write SnapContext or ORDERSNAP differs", snap, seq, snaps)
	}

	rb := oracleRequest()
	rb.Operations = []Operation{{Code: Rollback, Snap: 4}}
	rb.SnapSeq, rb.Snaps = 7, []uint64{7, 4, 2}
	if m, err = EncodeRequest(rb, oracleFeatures, 1024); err != nil {
		t.Fatal(err)
	}
	want := xattrLiteral(t, "0e22 00000000 0400000000000000 0000000000000000 0000000000000000 00000000 00000000")
	if !bytes.Equal(m.Front[113:151], want) || len(m.Data) != 0 || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagWrite|FlagOnDisk|FlagReturnVec) {
		t.Fatalf("rollback union differs: %x", m.Front[113:151])
	}

	ls := oracleRequest()
	ls.Operations = []Operation{{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 512}}}
	if m, err = EncodeRequest(ls, oracleFeatures, 1024); err != nil {
		t.Fatal(err)
	}
	if snap, _, _ := snapTail(t, m.Front, 1); snap != SnapDir || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(FlagRead|FlagOnDisk|FlagReturnVec) {
		t.Fatal("snapshot listing is not sent at SNAPDIR", snap)
	}
}

func equalSnaps(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSnapshotRequestRefusalsBeforeEncoding(t *testing.T) {
	read := func(edit func(*Request)) Request {
		r := oracleRequest()
		edit(&r)
		return r
	}
	write := func(edit func(*Request)) Request {
		r := oracleRequest()
		r.Operations = []Operation{{Code: Write, Length: 1, Data: []byte{1}}}
		edit(&r)
		return r
	}
	rollback := Operation{Code: Rollback, Snap: 4}
	list := Operation{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 64}}
	for _, tc := range []struct {
		name string
		r    Request
		want error
	}{
		{"read at SNAPDIR", read(func(r *Request) { r.ReadSnap = SnapDir }), ErrRequest},
		{"write at a snapshot", write(func(r *Request) { r.ReadSnap = 4 }), ErrRequest},
		{"rollback at a snapshot", write(func(r *Request) { r.Operations = []Operation{rollback}; r.ReadSnap = 3 }), ErrRequest},
		{"SnapContext on a read", read(func(r *Request) { r.SnapSeq = 1 }), ErrRequest},
		{"snaps on a read", read(func(r *Request) { r.SnapSeq, r.Snaps = 2, []uint64{2} }), ErrRequest},
		{"ORDERSNAP on a read", read(func(r *Request) { r.OrderSnap = true }), ErrRequest},
		{"ascending snaps", write(func(r *Request) { r.SnapSeq, r.Snaps = 7, []uint64{2, 4} }), ErrRequest},
		{"duplicate snaps", write(func(r *Request) { r.SnapSeq, r.Snaps = 7, []uint64{4, 4} }), ErrRequest},
		{"snap above seq", write(func(r *Request) { r.SnapSeq, r.Snaps = 3, []uint64{4} }), ErrRequest},
		{"trailing zero snap", write(func(r *Request) { r.SnapSeq, r.Snaps = 5, []uint64{5, 0} }), ErrRequest},
		{"seq above MAXSNAP", write(func(r *Request) { r.SnapSeq = NoSnap }), ErrRequest},
		{"rollback with another op", write(func(r *Request) { r.Operations = []Operation{{Code: AssertVersion, Version: 1}, rollback} }), ErrOperation},
		{"rollback to snap 0", write(func(r *Request) { r.Operations = []Operation{{Code: Rollback}} }), ErrRequest},
		{"rollback above MAXSNAP", write(func(r *Request) { r.Operations = []Operation{{Code: Rollback, Snap: NoSnap}} }), ErrRequest},
		{"FAILOK rollback", write(func(r *Request) { r.Operations = []Operation{{Code: Rollback, Snap: 4, Flags: OpFailOK}} }), ErrOperation},
		{"rollback with data", write(func(r *Request) { r.Operations = []Operation{{Code: Rollback, Snap: 4, Data: []byte{1}}} }), ErrRequest},
		{"rollback with extent", write(func(r *Request) { r.Operations = []Operation{{Code: Rollback, Snap: 4, Offset: 1}} }), ErrRequest},
		{"rollback with listing budget", write(func(r *Request) {
			r.Operations = []Operation{{Code: Rollback, Snap: 4, ListSnaps: ListSnapsInput{MaxOutputBytes: 1}}}
		}), ErrRequest},
		{"listing with another op", read(func(r *Request) { r.Operations = []Operation{list, {Code: Stat}} }), ErrOperation},
		{"listing at a snapshot", read(func(r *Request) { r.Operations = []Operation{list}; r.ReadSnap = 4 }), ErrRequest},
		{"listing without budget", read(func(r *Request) { r.Operations = []Operation{{Code: ListSnaps}} }), ErrRequest},
		{"listing budget above frame", read(func(r *Request) {
			r.Operations = []Operation{{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 1025}}}
		}), ErrRequest},
		{"FAILOK listing", read(func(r *Request) {
			r.Operations = []Operation{{Code: ListSnaps, Flags: OpFailOK, ListSnaps: ListSnapsInput{MaxOutputBytes: 64}}}
		}), ErrOperation},
		{"listing with snap id", read(func(r *Request) {
			r.Operations = []Operation{{Code: ListSnaps, Snap: 1, ListSnaps: ListSnapsInput{MaxOutputBytes: 64}}}
		}), ErrRequest},
		{"snap id on another opcode", read(func(r *Request) { r.Operations = []Operation{{Code: Stat, Snap: 1}} }), ErrRequest},
		{"listing budget on another opcode", read(func(r *Request) {
			r.Operations = []Operation{{Code: Stat, ListSnaps: ListSnapsInput{MaxOutputBytes: 1}}}
		}), ErrRequest},
		{"snaps beyond the frame", write(func(r *Request) {
			r.SnapSeq = 1 << 20
			r.Snaps = make([]uint64, 200)
			for i := range r.Snaps {
				r.Snaps[i] = r.SnapSeq - uint64(i)
			}
		}), wire.ErrLimit},
	} {
		if _, err := EncodeRequest(tc.r, oracleFeatures, 1024); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, err, tc.want)
		}
	}
	admitted := write(func(r *Request) { r.SnapSeq, r.Snaps, r.OrderSnap = 9, []uint64{9, 5, 1}, true })
	if _, err := EncodeRequest(admitted, oracleFeatures, 1024); err != nil {
		t.Fatal("valid SnapContext refused", err)
	}
	ordered := write(func(r *Request) { r.OrderSnap = true })
	if _, err := EncodeRequest(ordered, oracleFeatures, 1024); err != nil {
		t.Fatal("ORDERSNAP with an empty SnapContext refused", err)
	}
	head := read(func(r *Request) { r.ReadSnap = NoSnap })
	if _, err := EncodeRequest(head, oracleFeatures, 1024); err != nil {
		t.Fatal("explicit head read refused", err)
	}
	classRead := read(func(r *Request) {
		r.ReadSnap = MaxSnap
		r.Operations = []Operation{{Code: Call, Class: ClassInput{Class: "hello", Method: "say_hello", Mode: ClassRead, MaxOutputBytes: 8}}}
	})
	if _, err := EncodeRequest(classRead, oracleFeatures, 1024); err != nil {
		t.Fatal("read-mode CALL at a snapshot refused", err)
	}
}

func encodeSnapList(version, compat uint8, clones []Clone, seq uint64, extra []byte) []byte {
	body := wire.Encoder{}
	body.U32(uint32(len(clones)))
	for _, c := range clones {
		entry := wire.Encoder{}
		entry.U64(c.ID)
		entry.U32(uint32(len(c.Snaps)))
		for _, s := range c.Snaps {
			entry.U64(s)
		}
		entry.U32(uint32(len(c.Overlap)))
		for _, o := range c.Overlap {
			entry.U64(o.Offset)
			entry.U64(o.Length)
		}
		entry.U64(c.Size)
		body.Struct(1, 1, entry.Data)
	}
	if version >= 2 {
		body.U64(seq)
	}
	body.Raw(extra)
	out := wire.Encoder{}
	out.Struct(version, compat, body.Data)
	return out.Data
}

func TestDecodeSnapListBoundaries(t *testing.T) {
	clones := []Clone{{ID: 2, Snaps: []uint64{2}, Overlap: []Extent{{0, 3}}, Size: 3}, {ID: NoSnap, Snaps: []uint64{}, Overlap: []Extent{}, Size: 11}}
	list, err := DecodeSnapList(encodeSnapList(2, 1, clones, 7, nil))
	if err != nil || list.Seq != 7 || len(list.Clones) != 2 || list.Clones[0].ID != 2 || list.Clones[0].Overlap[0] != (Extent{0, 3}) || list.Clones[1].Size != 11 {
		t.Fatal("valid snapshot listing differs", list, err)
	}
	if list, err = DecodeSnapList(encodeSnapList(1, 1, clones, 0, nil)); err != nil || list.Seq != NoSnap {
		t.Fatal("version 1 listing must report NoSnap seq", list, err)
	}
	if _, err = DecodeSnapList(encodeSnapList(3, 2, clones, 7, []byte{9})); err != nil {
		t.Fatal("newer compatible listing with an appended field refused", err)
	}
	whiteout := []Clone{{ID: 2, Snaps: []uint64{2}, Size: 3}}
	if list, err = DecodeSnapList(encodeSnapList(2, 1, whiteout, 2, nil)); err != nil || len(list.Clones) != 1 {
		t.Fatal("listing without a head refused", list, err)
	}
	valid := encodeSnapList(2, 1, clones, 7, nil)
	for name, data := range map[string][]byte{
		"incompatible version": encodeSnapList(3, 3, clones, 7, nil),
		"trailing field in v2": encodeSnapList(2, 1, clones, 7, []byte{1}),
		"trailing bytes":       append(bytes.Clone(valid), 0),
		"truncated":            valid[:len(valid)-1],
		"head not last":        encodeSnapList(2, 1, []Clone{clones[1], clones[0]}, 7, nil),
		"head with snaps":      encodeSnapList(2, 1, []Clone{{ID: NoSnap, Snaps: []uint64{1}}}, 7, nil),
		"SNAPDIR clone":        encodeSnapList(2, 1, []Clone{{ID: SnapDir}}, 7, nil),
		"oversized count":      append(xattrLiteral(t, "0201 04000000 ffffffff"), make([]byte, 0)...),
	} {
		if _, err := DecodeSnapList(data); err == nil {
			t.Errorf("%s: malformed listing accepted", name)
		}
	}
}

func TestSnapshotReplyValidation(t *testing.T) {
	rollback := Operation{Code: Rollback, Snap: 4}
	if err := ValidateRollbackReply(rollback, OperationResult{Code: Rollback, Snap: 4}); err != nil {
		t.Fatal("rollback echo rejected", err)
	}
	for _, reply := range []OperationResult{
		{Code: Rollback, Snap: 5},
		{Code: Rollback, Snap: 4, PayloadLength: 1, Data: []byte{1}},
		{Code: Rollback, Snap: 4, Length: 1},
		{Code: Write, Snap: 4},
	} {
		if ValidateRollbackReply(rollback, reply) == nil {
			t.Fatal("changed rollback reply accepted", reply)
		}
	}
	d := wire.NewDecoder(append(append([]byte{4, 0, 0, 0, 0, 0, 0, 0}, make([]byte, 19)...), 1))
	if decodeRollbackUnion(d); d.Err() == nil {
		t.Fatal("nonzero rollback union padding accepted")
	}

	list := Operation{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 64}}
	data := encodeSnapList(2, 1, []Clone{{ID: NoSnap, Size: 1}}, 0, nil)
	if err := ValidateListSnapsReply(list, OperationResult{Code: ListSnaps, PayloadLength: uint32(len(data)), Data: data}, true); err != nil {
		t.Fatal("listing reply rejected", err)
	}
	if err := ValidateListSnapsReply(list, OperationResult{Code: ListSnaps}, false); err != nil {
		t.Fatal("failed listing without output rejected", err)
	}
	for name, tc := range map[string]struct {
		reply   OperationResult
		success bool
	}{
		"over budget":         {OperationResult{Code: ListSnaps, PayloadLength: 65, Data: make([]byte, 65)}, true},
		"failure with output": {OperationResult{Code: ListSnaps, PayloadLength: uint32(len(data)), Data: data}, false},
		"empty success":       {OperationResult{Code: ListSnaps}, true},
		"changed union":       {OperationResult{Code: ListSnaps, Offset: 1, PayloadLength: uint32(len(data)), Data: data}, true},
	} {
		if ValidateListSnapsReply(list, tc.reply, tc.success) == nil {
			t.Errorf("%s: invalid listing reply accepted", name)
		}
	}
}

package osd

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The native wrapper built the same logical inputs with MOSDOp's snapshot
// setters and the pinned rollback/list_snaps builders, which add no per-op
// flag. See testdata/snap-native-README.md.
func snapNativeRequest(ops ...Operation) Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "snap-codec",
		Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: ops}
}

func snapNativeWrite(r Request) Request {
	r.MTimeSeconds, r.MTimeNanoseconds = 1739999999, 5
	r.SnapSeq, r.Snaps = 7, []uint64{7, 4, 2}
	return r
}

var (
	snapNativeRead     = []Operation{{Code: Read, Length: 4}, {Code: Stat}}
	snapNativeFull     = Operation{Code: WriteFull, Length: 2, Data: []byte("v2")}
	snapNativeRollback = Operation{Code: Rollback, Snap: 4}
	snapNativeList     = Operation{Code: ListSnaps, ListSnaps: ListSnapsInput{MaxOutputBytes: 512}}
)

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestSnapshotRequestsMatchFreshNativeEncoder(t *testing.T) {
	read := snapNativeRequest(snapNativeRead...)
	read.ReadSnap = 4
	ordered := snapNativeWrite(snapNativeRequest(snapNativeFull))
	ordered.OrderSnap = true
	for _, tc := range []struct {
		name  string
		r     Request
		front int
		data  string
		flags uint64
	}{
		{"snap-native-request-v6-read", read, 249, "", FlagRead},
		{"snap-native-request-v6-write", ordered, 235, "v2", FlagWrite | FlagOrderSnap},
		{"snap-native-request-v6-rollback", snapNativeWrite(snapNativeRequest(snapNativeRollback)), 235, "", FlagWrite},
		{"snap-native-request-v6-list", snapNativeRequest(snapNativeList), 211, "", FlagRead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := EncodeRequest(tc.r, oracleFeatures, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
			native := xattrNativeMessage(t, tc.name)
			if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != tc.front || string(native.Data) != tc.data ||
				!bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
				t.Fatalf("Go snapshot request differs from fresh native encoding\nGo     front %x\nnative front %x", m.Front, native.Front)
			}
			if flags := uint64(binary.LittleEndian.Uint32(native.Front[8:12])); flags != tc.flags|FlagOnDisk|FlagReturnVec {
				t.Fatalf("native snapshot request flags changed: %#x", flags)
			}
		})
	}
	// The pinned rollback builder: code 0x220e, no flag, le64 snapid 4 at
	// union offset 0, 20 zero union bytes and payload_len 0.
	native := xattrNativeMessage(t, "snap-native-request-v6-rollback")
	if !bytes.Contains(native.Front, xattrLiteral(t, "0e22 00000000 0400000000000000 0000000000000000 0000000000000000 00000000 00000000")) {
		t.Fatalf("native ROLLBACK op differs from the pinned snap union: %x", native.Front)
	}
	// The SnapContext follows hobj.snap: NOSNAP, seq 7 and [7, 4, 2].
	if !bytes.Contains(native.Front, xattrLiteral(t, "feffffffffffffff 0700000000000000 03000000 0700000000000000 0400000000000000 0200000000000000")) {
		t.Fatalf("native SnapContext layout changed: %x", native.Front)
	}
}

func TestDecodeFreshNativeSnapshotReplies(t *testing.T) {
	read := FlagRead | FlagAck | FlagOnDisk | FlagReturnVec
	for _, tc := range []struct {
		name    string
		ops     []Operation
		code    int32
		flags   uint64
		legacy  Version
		replay  Version
		user    uint64
		payload []uint32
	}{
		{"snap-native-reply-v6-list", []Operation{snapNativeList}, 0, read, Version{Version: 20}, Version{}, 20, []uint32{180}},
		{"snap-native-reply-v6-rollback", []Operation{snapNativeRollback}, 0, FlagWrite | FlagAck | FlagOnDisk | FlagReturnVec, Version{Epoch: 11, Version: 21}, Version{Epoch: 11, Version: 21}, 21, []uint32{0}},
		// reply_op_error copies only the request's ACK/ONDISK bits; the
		// ORDERSNAP request flag is echoed and no version is reported.
		{"snap-native-reply-v6-eoldsnapc", []Operation{snapNativeFull}, OldSnapContext, FlagWrite | FlagOrderSnap | FlagOnDisk | FlagReturnVec, Version{}, Version{}, 0, []uint32{0}},
		{"snap-native-reply-v6-read", snapNativeRead, 0, read, Version{Version: 3}, Version{}, 3, []uint32{4, 16}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Object != "snap-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.Flags != tc.flags || r.MapEpoch != 11 ||
				r.LegacyVersion != tc.legacy || r.ReplayVersion != tc.replay || r.UserVersion != tc.user || len(r.Operations) != len(tc.ops) {
				t.Fatalf("native snapshot reply identity, flags or versions changed: %+v", r)
			}
			for i, op := range r.Operations {
				if op.Code != tc.ops[i].Code || op.Flags != 0 || op.Result != 0 || op.PayloadLength != tc.payload[i] {
					t.Fatal("native snapshot per-op metadata changed", op)
				}
			}
			switch tc.ops[0].Code {
			case Rollback:
				if r.Operations[0].Snap != 4 || ValidateRollbackReply(snapNativeRollback, r.Operations[0]) != nil {
					t.Fatal("native ROLLBACK echo rejected", r.Operations[0])
				}
				if ValidateRollbackReply(Operation{Code: Rollback, Snap: 3}, r.Operations[0]) == nil {
					t.Fatal("native ROLLBACK echo accepted for another snap id")
				}
			case ListSnaps:
				if err := ValidateListSnapsReply(snapNativeList, r.Operations[0], true); err != nil {
					t.Fatal("native listing rejected", err)
				}
				list, err := DecodeSnapList(r.Operations[0].Data)
				want := []Clone{{ID: 2, Snaps: []uint64{2}, Overlap: []Extent{{0, 3}}, Size: 3},
					{ID: 7, Snaps: []uint64{4, 7}, Overlap: []Extent{{0, 2}, {4, 4}}, Size: 9},
					{ID: NoSnap, Snaps: []uint64{}, Overlap: []Extent{}, Size: 11}}
				if err != nil || list.Seq != 7 || len(list.Clones) != len(want) {
					t.Fatal("native listing decode differs", list, err)
				}
				for i, c := range list.Clones {
					if c.ID != want[i].ID || c.Size != want[i].Size || !equalSnaps(c.Snaps, want[i].Snaps) || len(c.Overlap) != len(want[i].Overlap) {
						t.Fatalf("native clone %d differs: %+v", i, c)
					}
					for j, o := range c.Overlap {
						if o != want[i].Overlap[j] {
							t.Fatalf("native clone %d overlap differs: %+v", i, c.Overlap)
						}
					}
				}
				small := snapNativeList
				small.ListSnaps.MaxOutputBytes = 179
				if ValidateListSnapsReply(small, r.Operations[0], true) == nil {
					t.Fatal("native listing accepted above the local budget")
				}
			case Read:
				stat := r.Operations[1].Data
				if string(r.Operations[0].Data) != "v1v1" || r.Operations[0].Length != 4 || binary.LittleEndian.Uint64(stat) != 4 ||
					binary.LittleEndian.Uint32(stat[8:]) != 1739990000 || binary.LittleEndian.Uint32(stat[12:]) != 3 {
					t.Fatal("native read-at-snapshot outputs changed", r.Operations)
				}
			}
		})
	}
}

package osd

import (
	"bytes"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The native wrapper received the same endpoints as length-aware std::string
// values passed to omap_rm_range(std::string_view, std::string_view): a binary
// range with an embedded NUL and a high byte, an empty [empty, empty) range
// with FAILOK, and an empty begin with a single high-byte end.
func omapRangeNativeOperations() []Operation {
	return []Operation{
		{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeBegin: "b\x00", RangeEnd: "c\xff"}},
		{Code: OmapRemoveKeyRange, Flags: OpFailOK},
		{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeEnd: "\xff"}},
	}
}

func omapRangeNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "omap-range-codec",
		Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: omapRangeNativeOperations()}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestOmapRemoveKeyRangeRequestMatchesFreshNativeEncoder(t *testing.T) {
	m, err := EncodeRequest(omapRangeNativeRequest(), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "omap-range-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 293 || len(native.Data) != 29 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatalf("Go OMAP range removal request differs from fresh native encoding\nGo     front %x\nnative front %x\nGo     data %x\nnative data %x", m.Front, native.Front, m.Data, native.Data)
	}
	// Native data is begin then end, each a u32 little-endian length prefix
	// followed by raw bytes, with no collection count.
	want := []byte("\x02\x00\x00\x00b\x00\x02\x00\x00\x00c\xff" +
		"\x00\x00\x00\x00\x00\x00\x00\x00" +
		"\x00\x00\x00\x00\x01\x00\x00\x00\xff")
	if !bytes.Equal(native.Data, want) {
		t.Fatalf("native OMAP range removal data differs from begin/end string framing: %x", native.Data)
	}
}

func TestDecodeFreshNativeOmapRemoveKeyRangeReplies(t *testing.T) {
	single := []Operation{{Code: OmapRemoveKeyRange, Omap: OmapInput{RangeBegin: "a", RangeEnd: "b"}}}
	failOK := []Operation{
		{Code: OmapRemoveKeyRange, Flags: OpFailOK, Omap: OmapInput{RangeBegin: "a", RangeEnd: "b"}},
		{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k", Value: []byte("v")}}}},
	}
	for _, tc := range []struct {
		name    string
		ops     []Operation
		flags   uint64
		code    int32
		results []int32
		lengths []uint64
		front   int
	}{
		{name: "omap-range-native-reply-v6-success", ops: omapRangeNativeOperations(),
			flags: FlagOnDisk | FlagWrite | FlagReturnVec, results: []int32{0, 0, 0}, lengths: []uint64{12, 8, 9}, front: 291},
		{name: "omap-range-native-reply-v6-enoent", ops: single,
			flags: FlagAck | FlagOnDisk | FlagWrite | FlagReturnVec, code: -2, results: []int32{-2}, lengths: []uint64{10}, front: 207},
		{name: "omap-range-native-reply-v6-failok", ops: failOK,
			flags: FlagOnDisk | FlagWrite | FlagReturnVec, results: []int32{-2, 0}, lengths: []uint64{10, 14}, front: 249},
		{name: "omap-range-native-reply-v6-eopnotsupp", ops: single,
			flags: FlagAck | FlagOnDisk | FlagWrite | FlagReturnVec, code: -95, results: []int32{-95}, lengths: []uint64{10}, front: 207},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 || len(m.Front) != tc.front || len(m.Data) != 0 {
				t.Fatal("native OMAP range removal reply envelope changed", len(m.Front), len(m.Data))
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native flags=%#x result=%d legacy=%+v replay=%+v user=%d", r.Flags, r.Result, r.LegacyVersion, r.ReplayVersion, r.UserVersion)
			if r.Object != "omap-range-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != 19 ||
				r.ReplayVersion != (Version{Epoch: 11, Version: 20}) || r.LegacyVersion != (Version{Epoch: 11, Version: 19}) || r.Flags != tc.flags ||
				len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) || len(r.RawData) != 0 {
				t.Fatal("native OMAP range removal reply lost identity, flags, versions or raw payload", r)
			}
			// The native constructor keeps the request mode bits (OMAPRMKEYRANGE
			// is WR); only the completion bits are explicit logical inputs.
			mode, _, err := ValidateOperations(tc.ops, wire.DefaultLimit)
			if err != nil || mode != FlagWrite || r.Flags&(FlagRead|FlagWrite) != mode {
				t.Fatal("native reply mode bits differ from Go request admission", mode, err)
			}
			if r.Result >= 0 && (!r.OnDisk() || r.Flags&FlagAck != 0) {
				t.Fatal("native successful range removal lacks plain ONDISK completion", r.Flags)
			}
			for i, op := range r.Operations {
				t.Logf("op %d code=%#x flags=%d result=%d length=%d payload=%d", i, op.Code, op.Flags, op.Result, op.Length, op.PayloadLength)
				if op.Code != tc.ops[i].Code || op.Flags != tc.ops[i].Flags || op.Result != tc.results[i] || op.Offset != 0 || op.Length != tc.lengths[i] ||
					op.PayloadLength != 0 || len(op.Data) != 0 {
					t.Fatal("native OMAP range removal reply changed per-op union, result or output", op)
				}
				success := r.Result >= 0 && op.Result >= 0
				if err := ValidateOmapReply(tc.ops[i], op, success); err != nil {
					t.Fatal("native OMAP range removal reply rejected by strict wire validation", err)
				}
			}
		})
	}
}

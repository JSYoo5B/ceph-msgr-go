package osd

import (
	"bytes"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Assertions are deliberately unsorted. The native wrapper received the same
// logical members as a std::map<std::string, std::pair<bufferlist, int>>, so
// equality proves Go's canonical byte order rather than a caller order.
func omapCompareNativeOperations() []Operation {
	return []Operation{
		{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{
			{Key: "z", Value: []byte("Z"), Comparison: OmapCompareEqual},
			{Key: "b\x80", Value: []byte("\x00\xffA\n"), Comparison: OmapCompareGreater},
			{Key: "", Comparison: OmapCompareEqual},
			{Key: "\x00", Value: []byte{0x80}, Comparison: OmapCompareLess},
		}}},
		{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k", Value: []byte("v")}}}},
	}
}

func omapCompareNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "omap-cmp-codec",
		Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: omapCompareNativeOperations()}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestOmapCompareRequestMatchesFreshNativeEncoder(t *testing.T) {
	m, err := EncodeRequest(omapCompareNativeRequest(), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "omap-cmp-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 253 || len(native.Data) != 76 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatalf("Go OMAP comparison request differs from fresh native encoding\nGo     front %x\nnative front %x\nGo     data %x\nnative data %x", m.Front, native.Front, m.Data, native.Data)
	}
}

func TestDecodeFreshNativeOmapCompareReplies(t *testing.T) {
	failOK := []Operation{
		{Code: OmapCompare, Flags: OpFailOK, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "k", Value: []byte("x"), Comparison: OmapCompareEqual}}}},
		{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k", Value: []byte("v")}}}},
	}
	missing := []Operation{{Code: OmapCompare, Omap: OmapInput{Assertions: []OmapAssertion{{Key: "k", Comparison: OmapCompareEqual}}}}}
	for _, tc := range []struct {
		name    string
		ops     []Operation
		flags   uint64
		code    int32
		results []int32
		lengths []uint64
		front   int
	}{
		{name: "omap-cmp-native-reply-v6-success", ops: omapCompareNativeOperations(),
			flags: FlagOnDisk | FlagRead | FlagWrite | FlagReturnVec, results: []int32{0, 0}, lengths: []uint64{62, 14}, front: 247},
		{name: "omap-cmp-native-reply-v6-canceled", ops: omapCompareNativeOperations(),
			flags: FlagAck | FlagOnDisk | FlagRead | FlagWrite | FlagReturnVec, code: -125, results: []int32{-125, 0}, lengths: []uint64{62, 14}, front: 247},
		{name: "omap-cmp-native-reply-v6-failok", ops: failOK,
			flags: FlagOnDisk | FlagRead | FlagWrite | FlagReturnVec, results: []int32{-125, 0}, lengths: []uint64{18, 14}, front: 247},
		{name: "omap-cmp-native-reply-v6-enoent", ops: missing,
			flags: FlagAck | FlagOnDisk | FlagRead | FlagReturnVec, code: -2, results: []int32{-2}, lengths: []uint64{17}, front: 205},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 || len(m.Front) != tc.front || len(m.Data) != 0 {
				t.Fatal("native OMAP comparison reply envelope changed", len(m.Front), len(m.Data))
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native flags=%#x result=%d legacy=%+v replay=%+v user=%d", r.Flags, r.Result, r.LegacyVersion, r.ReplayVersion, r.UserVersion)
			if r.Object != "omap-cmp-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != 19 ||
				r.ReplayVersion != (Version{Epoch: 11, Version: 20}) || r.LegacyVersion != (Version{Epoch: 11, Version: 19}) || r.Flags != tc.flags ||
				len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) || len(r.RawData) != 0 {
				t.Fatal("native OMAP comparison reply lost identity, flags, versions or raw payload", r)
			}
			// The native constructor keeps the request mode bits (OMAP_CMP is
			// RD); only the completion bits are explicit logical inputs.
			mode, _, err := ValidateOperations(tc.ops, wire.DefaultLimit)
			if err != nil || r.Flags&(FlagRead|FlagWrite) != mode {
				t.Fatal("native reply mode bits differ from Go request admission", mode, err)
			}
			if mode&FlagWrite != 0 && r.Result >= 0 && !r.OnDisk() {
				t.Fatal("native successful comparison-guarded mutation lacks ONDISK", r.Flags)
			}
			for i, op := range r.Operations {
				t.Logf("op %d code=%#x flags=%d result=%d length=%d payload=%d", i, op.Code, op.Flags, op.Result, op.Length, op.PayloadLength)
				if op.Code != tc.ops[i].Code || op.Flags != tc.ops[i].Flags || op.Result != tc.results[i] || op.Offset != 0 || op.Length != tc.lengths[i] ||
					op.PayloadLength != 0 || len(op.Data) != 0 {
					t.Fatal("native OMAP comparison reply changed per-op union, result or output", op)
				}
				success := r.Result >= 0 && op.Result >= 0
				if err := ValidateOmapReply(tc.ops[i], op, success); err != nil {
					t.Fatal("native OMAP comparison reply rejected by strict wire validation", err)
				}
			}
		})
	}
}

package osd

import (
	"bytes"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Entries and keys are deliberately unsorted. The native wrapper received the
// same logical members as std::map/std::set, so equality proves Go's canonical
// byte order rather than a caller-supplied order.
func omapWriteNativeOperations() []Operation {
	return []Operation{
		{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 64}},
		{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{
			{Key: "z", Value: []byte("Z")}, {Key: "b\x80", Value: []byte("\x00\xffA\n")},
			{Key: ""}, {Key: "\x00", Value: []byte("\xff\x80")},
		}}},
		{Code: OmapSetHeader, Omap: OmapInput{Header: []byte("\x00\xffH\n\x80")}},
		{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{"b", "", "\x00"}}},
		{Code: OmapClear, Flags: OpFailOK},
	}
}

func omapWriteNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "omap-write-codec",
		Locator:      Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: omapWriteNativeOperations()}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestOmapWriteRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := omapWriteNativeRequest()
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "omap-write-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 369 || len(native.Data) != 70 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatalf("Go OMAP header/write request differs from fresh native encoding\nGo     front %x\nnative front %x\nGo     data %x\nnative data %x", m.Front, native.Front, m.Data, native.Data)
	}
	for _, budget := range []uint32{1, 5, 4096} {
		r.Operations[0].Omap.MaxOutputBytes = budget
		changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
		if err != nil || !bytes.Equal(changed.Front, native.Front) || !bytes.Equal(changed.Data, native.Data) {
			t.Fatal("local OMAP header output budget changed the wire bytes", budget, err)
		}
	}
}

func TestDecodeFreshNativeOmapWriteReplies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ops      []Operation
		flags    uint64
		code     int32
		results  []int32
		lengths  []uint64
		outputs  [][]byte
		dataSize int
	}{
		{name: "omap-write-native-reply-v6-all-success", ops: omapWriteNativeOperations(),
			flags: FlagOnDisk | FlagRead | FlagWrite | FlagReturnVec, results: []int32{0, 0, 0, 0, 0},
			lengths: []uint64{0, 47, 5, 18, 0}, outputs: [][]byte{[]byte("\x00\xffH\n\x80"), nil, nil, nil, nil}, dataSize: 5},
		{name: "omap-write-native-reply-v6-header-empty", ops: []Operation{{Code: OmapGetHeader, Omap: OmapInput{MaxOutputBytes: 64}}},
			flags: FlagAck | FlagRead | FlagReturnVec, results: []int32{0}, lengths: []uint64{0}, outputs: [][]byte{nil}},
		{name: "omap-write-native-reply-v6-rmkeys-enoent", ops: []Operation{{Code: OmapRemoveKeys, Omap: OmapInput{Keys: []string{"k"}}}},
			flags: FlagWrite | FlagReturnVec, code: -2, results: []int32{-2}, lengths: []uint64{9}, outputs: [][]byte{nil}},
		{name: "omap-write-native-reply-v6-failok-clear", ops: []Operation{
			{Code: OmapClear, Flags: OpFailOK},
			{Code: OmapSetValues, Omap: OmapInput{Entries: []OmapEntry{{Key: "k", Value: []byte("v")}}}},
		}, flags: FlagOnDisk | FlagWrite | FlagReturnVec, results: []int32{-2, 0}, lengths: []uint64{0, 14}, outputs: [][]byte{nil, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 || len(m.Data) != tc.dataSize {
				t.Fatal("native OMAP header/write reply envelope changed")
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("native flags=%#x result=%d legacy=%+v replay=%+v user=%d", r.Flags, r.Result, r.LegacyVersion, r.ReplayVersion, r.UserVersion)
			if r.Object != "omap-write-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != tc.code || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != 19 ||
				r.ReplayVersion != (Version{Epoch: 11, Version: 20}) || r.LegacyVersion != (Version{Epoch: 11, Version: 19}) || r.Flags != tc.flags ||
				len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
				t.Fatal("native OMAP header/write reply lost identity, flags, versions or raw data", r)
			}
			// The native constructor keeps the request mode bits; only the
			// completion bit is an explicit logical input of the wrapper.
			mode, _, err := ValidateOperations(tc.ops, wire.DefaultLimit)
			if err != nil || r.Flags&(FlagRead|FlagWrite) != mode {
				t.Fatal("native reply mode bits differ from Go request admission", mode, err)
			}
			if mode&FlagWrite != 0 && r.OnDisk() != (r.Result >= 0) {
				t.Fatal("native mutation completion differs from the logical case", r.Flags)
			}
			var joined []byte
			for i, op := range r.Operations {
				t.Logf("op %d code=%#x flags=%d result=%d length=%d payload=%d data=%x", i, op.Code, op.Flags, op.Result, op.Length, op.PayloadLength, op.Data)
				if op.Code != tc.ops[i].Code || op.Flags != tc.ops[i].Flags || op.Result != tc.results[i] || op.Offset != 0 || op.Length != tc.lengths[i] ||
					op.PayloadLength != uint32(len(tc.outputs[i])) || !bytes.Equal(op.Data, tc.outputs[i]) {
					t.Fatal("native OMAP header/write reply changed per-op union, result or output", op)
				}
				success := r.Result >= 0 && op.Result >= 0
				if err := ValidateOmapReply(tc.ops[i], op, success); err != nil {
					t.Fatal("native OMAP output rejected by strict bounded wire validation", err)
				}
				joined = append(joined, op.Data...)
			}
			if !bytes.Equal(joined, m.Data) {
				t.Fatal("native OMAP outputs lost their original segment boundaries")
			}
		})
	}
}

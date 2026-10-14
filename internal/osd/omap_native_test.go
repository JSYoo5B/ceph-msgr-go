package osd

import (
	"bytes"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func omapNativeOperations() []Operation {
	return []Operation{
		{Code: OmapGetKeys, Omap: OmapInput{MaxOutputBytes: 64}},
		{Code: OmapGetKeys, Omap: OmapInput{StartAfter: "a\x00\xff", MaxEntries: 2, MaxOutputBytes: 64}},
		{Code: OmapGetValues, Omap: OmapInput{StartAfter: "a\x00\xff", Prefix: "b\x00\x80", MaxEntries: 2, MaxOutputBytes: 64}},
		{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: []string{"z", "b\x80", "", "a\x00\xff"}, MaxOutputBytes: 64}},
	}
}

// These payloads came from fresh native encoders and separate native decoders.
// Their headers use explicit offline sequence/ACK inputs, not a live session.
func TestOmapRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "omap-codec",
		Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: omapNativeOperations()}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	// The shared helper checks the exact native 41-byte envelope fields.
	native := xattrNativeMessage(t, "omap-native-request-v6-read-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 325 || len(native.Data) != 75 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("Go OMAP request header/front/data differ from fresh native encoding")
	}
	for i := range r.Operations {
		r.Operations[i].Omap.MaxOutputBytes = 128
	}
	changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) || !bytes.Equal(changed.Data, native.Data) {
		t.Fatal("local OMAP output budget changed the wire bytes", err)
	}
}

func TestDecodeFreshNativeOmapReplies(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []Operation
	}{
		{"omap-native-reply-v6-read-mixed", omapNativeOperations()},
		{"omap-native-reply-v6-keys-final", []Operation{{Code: OmapGetKeys, Omap: OmapInput{MaxEntries: 2, MaxOutputBytes: 64}}}},
		{"omap-native-reply-v6-vals-empty", []Operation{{Code: OmapGetValues, Omap: OmapInput{MaxEntries: 1, MaxOutputBytes: 64}}}},
		{"omap-native-reply-v6-bykeys-empty-value", []Operation{{Code: OmapGetValuesByKeys, Omap: OmapInput{Keys: []string{"a\x00\xff"}, MaxOutputBytes: 64}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 {
				t.Fatal("native OMAP reply envelope changed")
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Object != "omap-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.Result != 0 || r.MapEpoch != 11 || r.UserVersion != 19 || len(r.Operations) != len(tc.ops) || r.Flags != FlagRead|FlagAck|FlagReturnVec || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
				t.Fatal("native OMAP reply lost identity, flags, versions or raw data", r)
			}
			var joined []byte
			for i, op := range r.Operations {
				if op.Result != 0 || op.Flags != tc.ops[i].Flags {
					t.Fatal("native OMAP reply changed per-op result/flags", op)
				}
				if err := ValidateOmapReply(tc.ops[i], op, true); err != nil {
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

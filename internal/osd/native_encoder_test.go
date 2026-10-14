package osd

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These logical inputs are independently constructed in the compiled native
// C++ wrapper. The stored bytes came from direct encode_payload calls on fresh
// objects, not native decoder re-export or a Go encode/decode round trip.
func nativeEncoderOperations() []Operation {
	return []Operation{
		{Code: Create, Flags: OpExclusive},
		{Code: Write, Offset: 2, Length: 4, Data: []byte{0, 255, 65, 10}},
		{Code: WriteFull, Length: 3, Data: []byte("new")},
		{Code: Append, Length: 2, Data: []byte{128, 33}},
		{Code: Truncate, Offset: 7},
		{Code: Zero, Offset: 1, Length: 2},
		{Code: AssertVersion, Version: 19},
		{Code: CompareExtent, Flags: OpFailOK, Offset: 1, Length: 2, Data: []byte{0, 255}},
		{Code: Delete},
	}
}

func nativeEncoderMessage(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	header := oracleBytes(t, name+".header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{
		header, oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data"),
	}})
	if err != nil {
		t.Fatal("cannot decode independently serialized native v2 header", err)
	}
	if m.Sequence != 1 || m.Transaction != 77 || m.AckSequence != 3 ||
		m.Priority != 127 || m.Version != 6 || !bytes.Equal(m.Frame().Segments[0], header) {
		t.Fatal("native header identity/default priority/CRC-enabled COMPLETE flag differs from Go", m)
	}
	return m
}

func TestMutationRequestMatchesFreshNativeEncoder(t *testing.T) {
	r := Request{ClientInc: 1, MapEpoch: 11,
		Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		Object:  "mutation-codec", Hash: 0x11223344,
		MTimeSeconds: 1700000000, MTimeNanoseconds: 123456789,
		Operations: nativeEncoderOperations()}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	// Sequence and ACK are explicit offline transport inputs. TID 77 comes
	// from the native MOSDOp constructor; Go Session.Call supplies it in use.
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	native := nativeEncoderMessage(t, "mutation-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 ||
		!bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("Go request header differs from native envelope/MessageFrame output")
	}
	if !bytes.Equal(m.Front, oracleBytes(t, "mutation-native-request-v6-all.front")) ||
		!bytes.Equal(m.Data, oracleBytes(t, "mutation-native-request-v6-all.data")) {
		t.Fatal("Go mutation payload differs from freshly constructed native MOSDOp::encode_payload output")
	}
}

func TestDecodeFreshNativeEncoderMutationReplies(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		ops  []Operation
	}{
		{"mutation-native-reply-v6-all-success", 0, nativeEncoderOperations()},
		{"mutation-native-reply-v6-exclusive-eexist", -17, []Operation{{Code: Create, Flags: OpExclusive}}},
		{"mutation-native-reply-v6-cmpext-mismatch", -4097, []Operation{{Code: CompareExtent, Offset: 1, Length: 2}}},
		{"mutation-native-reply-v6-truncate", 0, []Operation{{Code: Truncate, Offset: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeEncoderMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 {
				t.Fatal("native MOSDOpReply constructor/encoder header metadata changed", m)
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Object != "mutation-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) ||
				r.Flags != FlagRead|FlagWrite|FlagOnDisk|FlagReturnVec || r.Result != tc.code ||
				r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != 19 ||
				r.LegacyVersion != (Version{Version: 19, Epoch: 11}) ||
				r.ReplayVersion != (Version{Version: 20, Epoch: 11}) ||
				!r.Redirect.Empty() || r.Trace != ([3]uint64{}) ||
				!bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) ||
				len(r.Operations) != len(tc.ops) {
				t.Fatal("lost independently encoded native reply metadata", r)
			}
			for i, want := range tc.ops {
				op := r.Operations[i]
				length := want.Length
				if want.Code == AssertVersion {
					length = want.Version
				}
				if op.Code != want.Code || op.Flags != want.Flags || op.Offset != want.Offset ||
					op.Length != length || op.TruncateSize != 0 || op.TruncateSequence != 0 ||
					op.Result != tc.code || op.PayloadLength != 0 || len(op.Data) != 0 {
					t.Fatal("lost native packed operation union/flags/result", i, op)
				}
			}
		})
	}
}

func TestFreshNativeEnvelopeLengths(t *testing.T) {
	// Messenger v2 transports segment lengths outside its 41-byte header.
	// These lengths are independently set by native Message::encode and
	// retained in the receipt; they are not fabricated Go header fields.
	var proof struct {
		Verified bool `json:"native_message_header_verified"`
		Fixtures map[string]struct {
			Header struct {
				Type    uint16 `json:"type"`
				Version uint16 `json:"version"`
				Compat  uint16 `json:"compat"`
				TID     uint64 `json:"tid"`
				Front   uint32 `json:"front_len"`
				Middle  uint32 `json:"middle_len"`
				Data    uint32 `json:"data_len"`
				Flags   uint8  `json:"flags"`
			} `json:"native_header"`
		} `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/mutation-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if !proof.Verified || len(proof.Fixtures) != 5 {
		t.Fatal("missing native header receipt")
	}
	for name, fixture := range proof.Fixtures {
		t.Run(name, func(t *testing.T) {
			m := nativeEncoderMessage(t, name)
			h := fixture.Header
			if h.Type != m.Type || h.Version != m.Version || h.Compat != m.CompatVersion ||
				h.TID != m.Transaction || h.Flags != 1 || h.Middle != 0 ||
				int(h.Front) != len(m.Front) || int(h.Data) != len(m.Data) {
				t.Fatal("native Message envelope identity/lengths/CRC policy disagree with stored segments", h)
			}
		})
	}
}

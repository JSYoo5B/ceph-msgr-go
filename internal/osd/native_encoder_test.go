package osd

import (
	"bytes"
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
			// Type/version/compat below select the decoder branch. The fixture
			// emits front/data only; it does not independently verify header bytes.
			m := msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2,
				Front: oracleBytes(t, tc.name+".front"), Data: oracleBytes(t, tc.name+".data")}
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

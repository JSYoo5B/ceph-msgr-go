package osd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func xattrNativeOperations() []Operation {
	ops := []Operation{
		{Code: GetXattr, Xattr: XattrInput{Name: "_raw\xff", MaxOutputBytes: 16}},
		{Code: GetXattrs, Xattr: XattrInput{MaxOutputBytes: 64}},
	}
	for comparison := uint8(1); comparison <= 6; comparison++ {
		op := Operation{Code: CompareXattr, Xattr: XattrInput{Name: "_raw\xff", ComparisonOp: comparison, ComparisonMode: 1}, Data: []byte{0, 255, 65, 10}}
		if comparison == 6 {
			op.Flags = OpFailOK
		}
		ops = append(ops, op)
	}
	return append(ops,
		Operation{Code: SetXattr, Xattr: XattrInput{Name: "_raw\xff"}, Data: []byte{0, 255, 65, 10}},
		Operation{Code: SetXattr, Xattr: XattrInput{Name: "empty"}},
		Operation{Code: RemoveXattr, Xattr: XattrInput{Name: "_remove"}},
	)
}

func xattrNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11,
		Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1},
		Object:  "xattr-codec", Hash: 0x11223344,
		MTimeSeconds: 1700000000, MTimeNanoseconds: 123456789,
		Operations: xattrNativeOperations()}
}

func xattrNativeMessage(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	header := oracleBytes(t, name+".header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{
		header, oracleBytes(t, name+".front"), nil, oracleBytes(t, name+".data"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Sequence and ACK are explicit offline inputs in native MessageFrame,
	// not evidence of a real session or a server acknowledging these requests.
	if m.Sequence != 1 || m.Transaction != 77 || m.AckSequence != 3 || m.Priority != 127 || m.Version != 6 || !bytes.Equal(m.Frame().Segments[0], header) {
		t.Fatal("native xattr envelope differs from Go header serialization", m)
	}
	return m
}

// Fresh native MOSDOp/MOSDOpReply encode_payload plus separate native decoders
// produced these fixtures. The logical operations are independently supplied
// to that C++ wrapper; Go never supplies encoder input bytes to the wrapper.
// This is byte interoperability proof, not daemon execution or stored attrs.
func TestXattrRequestMatchesFreshNativeEncoder(t *testing.T) {
	m, err := EncodeRequest(xattrNativeRequest(), oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "xattr-native-request-v6-all")
	if native.Type != RequestMessage || native.CompatVersion != 3 || len(native.Front) != 592 || len(native.Data) != 80 || !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("Go xattr request header/front/data differ from fresh native encoding")
	}
	// Caller-local budgets have no wire field: changing them cannot change
	// the native request bytes after passing aggregate admission.
	r := xattrNativeRequest()
	r.Operations[0].Xattr.MaxOutputBytes, r.Operations[1].Xattr.MaxOutputBytes = 32, 128
	changed, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) || !bytes.Equal(changed.Data, native.Data) {
		t.Fatal("local xattr output budget altered the wire request", err)
	}
}

func TestDecodeFreshNativeEncoderXattrReplies(t *testing.T) {
	all := xattrNativeOperations()
	readOps := []Operation{all[0], all[1], all[2]}
	for _, tc := range []struct {
		name    string
		code    int32
		flags   uint64
		ops     []Operation
		results []int32
		outputs [][]byte
	}{
		{"xattr-native-reply-v6-read-mixed", 0, FlagRead | FlagReturnVec | FlagAck,
			readOps, []int32{0, 0, 1}, [][]byte{[]byte{0, 255, 65, 10}, xattrLiteral(t, "02000000 050000005f726177ff 0400000000ff410a 05000000656d707479 00000000"), nil}},
		{"xattr-native-reply-v6-cmp-false", -125, FlagRead | FlagReturnVec,
			[]Operation{all[2]}, []int32{-125}, nil},
		{"xattr-native-reply-v6-set-refused", -27, FlagWrite | FlagReturnVec,
			[]Operation{all[8]}, []int32{-27}, nil},
		{"xattr-native-reply-v6-rm-refused", -2, FlagWrite | FlagReturnVec,
			[]Operation{all[10]}, []int32{-2}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, tc.name)
			if m.Type != ReplyMessage || m.CompatVersion != 2 {
				t.Fatal("native xattr reply header changed")
			}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Object != "xattr-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.Flags != tc.flags || r.Result != tc.code || r.UserVersion != 19 || r.LegacyVersion != (Version{Epoch: 11, Version: 19}) || r.ReplayVersion != (Version{Epoch: 11, Version: 20}) || !r.Redirect.Empty() || r.Trace != ([3]uint64{}) || len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
				t.Fatal("native reply identity/completion/versions/raw data lost", r)
			}
			for i, requested := range tc.ops {
				op := r.Operations[i]
				valueLength := uint32(len(requested.Data))
				if requested.Code == GetXattr {
					valueLength = 4
				}
				want := XattrResult{NameLength: uint32(len(requested.Xattr.Name)), ValueLength: valueLength, ComparisonOp: requested.Xattr.ComparisonOp, ComparisonMode: requested.Xattr.ComparisonMode}
				var output []byte
				if tc.outputs != nil {
					output = tc.outputs[i]
				}
				if op.Code != requested.Code || op.Flags != requested.Flags || op.Xattr != want || op.Result != tc.results[i] || op.PayloadLength != uint32(len(output)) || !bytes.Equal(op.Data, output) || op.Offset != 0 || op.Length != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 {
					t.Fatal("native xattr union/per-op signed result/output boundary lost", i, op)
				}
				if err := ValidateXattrReply(requested, op, tc.code >= 0 && op.Result >= 0); err != nil {
					t.Fatal("native xattr normalization/refusal rejected", err)
				}
			}
			if len(m.Data) != 0 {
				m.Data[0] = 7
				if r.Operations[0].Data[0] != 0 || r.RawData[0] != 0 {
					t.Fatal("native decoded output borrowed source bytes")
				}
			}
		})
	}
}

func TestNativeXattrDecoderReceiptMatchesGoMetadata(t *testing.T) {
	var receipt struct {
		Commit   string `json:"ceph_commit"`
		Encoder  bool   `json:"native_payload_encoder_verified"`
		Decoder  bool   `json:"native_payload_decoder_verified"`
		Header   bool   `json:"native_message_header_verified"`
		Cleaned  bool   `json:"builder_termination_completed"`
		Fixtures map[string]struct {
			Operations []struct {
				Code           uint16 `json:"code"`
				Flags          uint32 `json:"flags"`
				NameLength     uint32 `json:"name_len"`
				ValueLength    uint32 `json:"value_len"`
				ComparisonOp   uint8  `json:"cmp_op"`
				ComparisonMode uint8  `json:"cmp_mode"`
				PayloadLength  uint32 `json:"payload_len"`
				Result         int32  `json:"result"`
				Output         string `json:"outdata_hex"`
			} `json:"decoded_operations"`
		} `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/xattr-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !receipt.Encoder || !receipt.Decoder || !receipt.Header || !receipt.Cleaned || len(receipt.Fixtures) != 5 {
		t.Fatal("native xattr proof lacks pinned fresh encoder/decoder/cleanup receipt")
	}
	for name, fixture := range receipt.Fixtures {
		if name == "xattr-native-request-v6-all" {
			continue // Exact request/header/data bytes are compared independently above.
		}
		r, err := DecodeReply(xattrNativeMessage(t, name), wire.DefaultLimit)
		if err != nil || len(r.Operations) != len(fixture.Operations) {
			t.Fatal(name, err)
		}
		for i, native := range fixture.Operations {
			op := r.Operations[i]
			output, err := hex.DecodeString(native.Output)
			if err != nil {
				t.Fatal(err)
			}
			if op.Code != native.Code || op.Flags != native.Flags || op.Xattr != (XattrResult{NameLength: native.NameLength, ValueLength: native.ValueLength, ComparisonOp: native.ComparisonOp, ComparisonMode: native.ComparisonMode}) || op.PayloadLength != native.PayloadLength || op.Result != native.Result || !bytes.Equal(op.Data, output) {
				t.Fatal("Go metadata differs from separately run native decoder", name, i)
			}
		}
	}
}

func TestMalformedNativeXattrReplyRetainsRawEvidenceWithoutCompletingDecode(t *testing.T) {
	m := xattrNativeMessage(t, "xattr-native-reply-v6-read-mixed")
	// First union's reserved byte; object xattr-codec makes op start byte 64.
	m.Front[80] = 1
	r, err := DecodeReply(m, wire.DefaultLimit)
	if err == nil || len(r.Operations) != 3 || r.Operations[0].Xattr.NameLength != 5 || r.Operations[0].Xattr.ValueLength != 4 || len(r.Operations[0].Data) != 0 || !bytes.Equal(r.RawFront, m.Front) || !bytes.Equal(r.RawData, m.Data) {
		t.Fatal("malformed native reply completed or discarded partial/raw evidence", r, err)
	}
}

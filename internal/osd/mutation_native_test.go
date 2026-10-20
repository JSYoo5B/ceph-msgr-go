package osd

import (
	"bytes"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestMutationRequestMatchesIndependentNativeDecoderInput(t *testing.T) {
	// Independent Python field definitions and the pinned native decoder,
	// not a native payload encoder or a self-generated Go roundtrip.
	r := oracleRequest()
	r.Object = "mutation-codec"
	r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 123456789
	r.Operations = []Operation{
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
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Front, oracleBytes(t, "mutation-request-v6-all.front")) || !bytes.Equal(m.Data, oracleBytes(t, "mutation-request-v6-all.data")) {
		t.Fatal("mutation unions/flags/data differ from independent bytes accepted by the pinned native decoder")
	}
}

func TestDecodeIndependentNativeMutationReplyInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
		ops  []uint16
	}{
		{"mutation-reply-v6-all-success", 0, []uint16{Create, Write, WriteFull, Append, Truncate, Zero, AssertVersion, CompareExtent, Delete}},
		{"mutation-reply-v6-exclusive-eexist", -17, []uint16{Create}},
		{"mutation-reply-v6-cmpext-mismatch", -4097, []uint16{CompareExtent}},
		{"mutation-reply-v6-zero-as-truncate", 0, []uint16{Truncate}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := msgr.MessageData{Type: ReplyMessage, Version: 6, CompatVersion: 2,
				Front: oracleBytes(t, tc.name+".front"), Data: oracleBytes(t, tc.name+".data")}
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Result != tc.code || r.Object != "mutation-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || !r.OnDisk() || r.MapEpoch != 11 || r.RetryAttempt != 0 || r.UserVersion != 19 || len(r.Operations) != len(tc.ops) || !bytes.Equal(r.RawFront, m.Front) {
				t.Fatal("mutation reply lost completion/identity/vector metadata", r)
			}
			for i, code := range tc.ops {
				if r.Operations[i].Code != code || r.Operations[i].Result != tc.code || len(r.Operations[i].Data) != 0 || r.Operations[i].PayloadLength != 0 {
					t.Fatal("per-op error or output boundary", r.Operations[i])
				}
			}
			if len(tc.ops) == 9 && (r.Operations[6].Length != 19 || r.Operations[7].Flags != OpFailOK) {
				t.Fatal("version/FAILOK union metadata")
			}
		})
	}
}

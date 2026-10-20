package osd

import (
	"bytes"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
	"testing"
)

func writeSameNativeOperations() []Operation {
	return []Operation{{Code: WriteSame, Offset: 17, Length: 12, Data: []byte{0, 255, 'x'}}, {Code: WriteSame, Offset: 7, Data: []byte{0xaa}}, {Code: Append, Length: 6, Data: []byte{'t', 'a', 'i', 'l', 0, 255}}}
}

func TestWriteSameMatchesFreshNativeVectors(t *testing.T) {
	r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "writesame-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: writeSameNativeOperations(), MTimeSeconds: 1700000000, MTimeNanoseconds: 123456789}
	m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "writesame-native-request-v6-all")
	if !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("WRITESAME header/front/pattern bytes differ from fresh native objects")
	}
	for _, kind := range []string{"success", "refused", "failok", "zero"} {
		native := xattrNativeMessage(t, "writesame-native-reply-v6-"+kind)
		reply, err := DecodeReply(native, wire.DefaultLimit)
		if err != nil {
			t.Fatal(kind, err)
		}
		ops := writeSameNativeOperations()
		aggregate := int32(0)
		first := int32(0)
		if kind == "refused" {
			aggregate, first = -27, -27
		}
		if kind == "failok" {
			first = -27
			ops[0].Flags = OpFailOK
		}
		if kind == "zero" {
			ops = ops[1:2]
		}
		if native.Version != 6 || native.CompatVersion != 2 || reply.Result != aggregate || len(reply.Operations) != len(ops) || reply.Operations[0].Result != first || !bytes.Equal(reply.RawFront, native.Front) || !bytes.Equal(reply.RawData, native.Data) {
			t.Fatal("native raw/signed result lost", kind, reply)
		}
		for i, op := range ops {
			result := reply.Operations[i]
			if op.Code == WriteSame {
				if err := ValidateWriteSameReply(op, result); err != nil {
					t.Fatal(kind, i, err)
				}
			}
			if result.Flags != op.Flags || result.TruncateSize != 0 || result.TruncateSequence != 0 || result.PayloadLength != 0 {
				t.Fatal("pattern aliased extent or output", kind, result)
			}
		}
	}
}

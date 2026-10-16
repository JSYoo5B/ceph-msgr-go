package osd

import (
	"bytes"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// This suite consumes only the separate retained five-vector proof produced
// by fresh native objects and independently decoded by pinned Ceph, not by Go.
func TestXattrU64MatchesFreshNativeEncoder(t *testing.T) {
	for _, write := range []bool{false, true} {
		op := u64XattrOperation(0xfedcba9876543210)
		r := Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "u64-xattr-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{op}}
		name := "u64-xattr-native-request-v6-read"
		if write {
			name = "u64-xattr-native-request-v6-write"
			r.Operations = append(r.Operations, Operation{Code: SetXattr, Xattr: XattrInput{Name: "marker"}, Data: []byte("v")})
			r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 123456789
		}
		m, err := EncodeRequest(r, oracleFeatures, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		m.Sequence, m.Transaction, m.AckSequence = 1, 77, 3
		native := xattrNativeMessage(t, name)
		if !bytes.Equal(m.Front, native.Front) || !bytes.Equal(m.Data, native.Data) || !bytes.Equal(m.Frame().Segments[0], native.Frame().Segments[0]) {
			t.Fatal("U64 request differs from fresh native encoder", name)
		}
	}
	for _, tc := range []struct {
		name string
		code int32
	}{{"true", 1}, {"false", -125}, {"invalid", -22}} {
		native := xattrNativeMessage(t, "u64-xattr-native-reply-v6-"+tc.name)
		if native.Version != 6 || native.CompatVersion != 2 {
			t.Fatal("native reply envelope differs")
		}
		reply, err := DecodeReply(native, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		if reply.Result != tc.code || len(reply.Operations) != 1 || reply.Operations[0].Result != tc.code || !bytes.Equal(reply.RawFront, native.Front) || !bytes.Equal(reply.RawData, native.Data) {
			t.Fatal("native signed result or raw bytes lost", reply)
		}
		op := reply.Operations[0]
		if op.Code != CompareXattr || op.Xattr.NameLength != 6 || op.Xattr.ValueLength != 8 || op.Xattr.ComparisonMode != 2 || op.Xattr.ComparisonOp != 3 || op.PayloadLength != 0 {
			t.Fatal("native U64 union differs", op)
		}
		if err := ValidateXattrReply(u64XattrOperation(0xfedcba9876543210), op, tc.code >= 0); err != nil {
			t.Fatal(err)
		}
	}
}

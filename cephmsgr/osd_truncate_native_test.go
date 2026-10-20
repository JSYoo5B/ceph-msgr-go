package cephmsgr

import (
	"os"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func nativeTruncateReply(t *testing.T, name string) osd.Reply {
	t.Helper()
	segment := func(s string) []byte {
		p, err := os.ReadFile("../internal/osd/testdata/" + name + "." + s + ".hex")
		if err != nil {
			t.Fatal(err)
		}
		return osdOmapLiteral(t, string(p))
	}
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{segment("header"), segment("front"), nil, segment("data")}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := osd.DecodeReply(m, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The fresh native replies with truncate fields (a clipped READ, a trimmed
// old WRITE and a no-op sequenced TRUNCATE) pass the public reply check for
// their requests, and fail it when the request carried other truncate fields.
func TestOSDTruncateFieldNativeRepliesValidate(t *testing.T) {
	request := func(op osd.Operation) osd.Request {
		return osd.Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "trunc-codec",
			Locator: osd.Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []osd.Operation{op}}
	}
	for _, tc := range []struct {
		name string
		op   osd.Operation
	}{
		{"truncate-native-reply-v6-read-clipped", osd.Operation{Code: osd.Read, Offset: 4, Length: 12, TruncateSize: 10, TruncateSequence: 3}},
		{"truncate-native-reply-v6-write-trimmed", osd.Operation{Code: osd.Write, Offset: 8, Length: 8, Data: []byte("truncate"), TruncateSize: 10, TruncateSequence: 2}},
		{"truncate-native-reply-v6-trunc-noop", osd.Operation{Code: osd.Truncate, Offset: 10, TruncateSize: 10, TruncateSequence: 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := nativeTruncateReply(t, tc.name)
			if err := validateOSDReply(request(tc.op), r, false); err != nil {
				t.Fatal("native truncate-field reply refused", err)
			}
			other := tc.op
			other.TruncateSequence++
			if err := validateOSDReply(request(other), r, false); err == nil {
				t.Fatal("native reply accepted for another truncate sequence")
			}
			if tc.op.Code != osd.Truncate {
				plain := tc.op
				plain.TruncateSize, plain.TruncateSequence = 0, 0
				if err := validateOSDReply(request(plain), r, false); err == nil {
					t.Fatal("native reply accepted for a request without truncate fields")
				}
			}
		})
	}
}

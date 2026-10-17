package cephmsgr

import (
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// A read the OSD proxies to a manifest redirect target echoes the requested
// extent length with a shorter payload (observed live); a local read reports
// the payload length. Both are accepted; a payload above the request is not.
func TestOSDReadReplyAcceptsProxiedLengthEcho(t *testing.T) {
	input := osd.Request{Object: "o", Hash: 3, Locator: osd.Locator{Pool: 7, Hash: -1}, Operations: []osd.Operation{{Code: osd.Read, Length: 4096}}}
	reply := func(length uint64, payload int) osd.Reply {
		data := make([]byte, payload)
		return osd.Reply{Object: "o", PG: osd.PG{Pool: 7, Seed: 3}, Operations: []osd.OperationResult{{Code: osd.Read, Length: length, PayloadLength: uint32(payload), Data: data}}}
	}
	for _, tc := range []struct {
		length  uint64
		payload int
		ok      bool
	}{{23, 23, true}, {4096, 23, true}, {4096, 4096, true}, {100, 23, false}, {4096, 4097, false}} {
		err := validateOSDReply(input, reply(tc.length, tc.payload), false)
		if (err == nil) != tc.ok {
			t.Fatalf("length %d payload %d: %v", tc.length, tc.payload, err)
		}
	}
}

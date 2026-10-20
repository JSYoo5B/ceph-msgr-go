package osd

import (
	"bytes"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func watchersNativeRequest() Request {
	return Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "watchers-codec", Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Operations: []Operation{{Code: ListWatchers, ListWatchers: ListWatchersInput{MaxOutputBytes: 512}}}}
}

func TestWatchersRequestMatchesFreshNativeEncoder(t *testing.T) {
	req := watchersNativeRequest()
	got, err := EncodeRequest(req, oracleFeatures, wire.DefaultLimit)
	if err != nil {
		t.Fatal(err)
	}
	got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
	native := xattrNativeMessage(t, "watchers-native-request-v6-list")
	if !bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || !bytes.Equal(got.Frame().Segments[0], native.Frame().Segments[0]) {
		t.Fatal("native request/header differ")
	}
	req.Operations[0].ListWatchers.MaxOutputBytes = 1024
	changed, err := EncodeRequest(req, oracleFeatures, wire.DefaultLimit)
	if err != nil || !bytes.Equal(changed.Front, native.Front) {
		t.Fatal("local budget changed native wire", err)
	}
}

func TestDecodeFreshNativeWatchersReplies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int32
		entries int
		length  int
	}{
		{"populated", 0, 2, 146}, {"empty", 0, 0, 10}, {"enoent", -2, 0, 0}, {"eacces", -13, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := xattrNativeMessage(t, "watchers-native-reply-v6-"+tc.name)
			r, err := DecodeReply(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if r.Result != tc.code || r.Object != "watchers-codec" || r.PG != (PG{Pool: 7, Seed: 0x11223344}) || len(r.RawData) != tc.length || r.UserVersion != 19 || r.MapEpoch != 11 {
				t.Fatal("native reply metadata lost", r)
			}
			op := r.Operations[0]
			if err := ValidateListWatchersReply(watchersNativeRequest().Operations[0], op, tc.code == 0); err != nil {
				t.Fatal(err)
			}
			if tc.code < 0 {
				return
			}
			entries, err := DecodeWatchers(op.Data)
			if err != nil || len(entries) != tc.entries {
				t.Fatal(entries, err)
			}
			if tc.entries == 2 {
				if entries[0].NameType != 8 || entries[0].NameNumber != 42 || entries[0].Cookie != 10 || entries[0].TimeoutSeconds != 30 || entries[0].Address.Nonce != 1000 || entries[0].Address.Endpoint != netip.MustParseAddrPort("192.0.2.1:6800") || entries[0].Address.Type != 2 {
					t.Fatal(entries[0])
				}
				if entries[1].NameNumber != 43 || entries[1].Cookie != ^uint64(0) || entries[1].TimeoutSeconds != 61 || entries[1].Address.Nonce != 1001 || entries[1].Address.Endpoint != netip.MustParseAddrPort("[2001:db8::2]:6801") {
					t.Fatal(entries[1])
				}
			}
		})
	}
}

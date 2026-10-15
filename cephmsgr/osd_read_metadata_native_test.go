package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// These synthetic peers send independently generated native envelopes. Actual
// daemon/native-librados acceptance is covered by the opt-in integration tests.
func TestOSDReadMetadataPublicNativePayloads(t *testing.T) {
	for _, tc := range []struct {
		kind, request, reply, suffix string
		op                           OSDOperation
		code                         int32
	}{
		{"mapext", "mapext-native-request-v6-map", "mapext-native-reply-v6-", "populated", OSDOperation{Code: OSDMapExt, Offset: 3, Length: 97, MapExt: OSDMapExtInput{MaxOutputBytes: 512}}, 0},
		{"mapext", "mapext-native-request-v6-map", "mapext-native-reply-v6-", "empty", OSDOperation{Code: OSDMapExt, Offset: 3, Length: 97, MapExt: OSDMapExtInput{MaxOutputBytes: 512}}, 0},
		{"mapext", "mapext-native-request-v6-map", "mapext-native-reply-v6-", "enoent", OSDOperation{Code: OSDMapExt, Offset: 3, Length: 97, MapExt: OSDMapExtInput{MaxOutputBytes: 512}}, -2},
		{"mapext", "mapext-native-request-v6-map", "mapext-native-reply-v6-", "eacces", OSDOperation{Code: OSDMapExt, Offset: 3, Length: 97, MapExt: OSDMapExtInput{MaxOutputBytes: 512}}, -13},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "success", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: ^uint32(0)}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "trimmed", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: ^uint32(0)}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "eof", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: ^uint32(0)}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "unaligned", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: ^uint32(0)}}, -22},
	} {
		t.Run(tc.kind+"/"+tc.suffix, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: tc.kind + "-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: []OSDOperation{tc.op}}
			type response struct {
				r OSDReply
				e error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			if !bytes.Equal(sent.Front, nativeAllocHintFixture(t, tc.request, "front")) || !bytes.Equal(sent.Data, nativeAllocHintFixture(t, tc.request, "data")) {
				t.Fatal("public native request differs")
			}
			front, data := nativeAllocHintFixture(t, tc.reply+tc.suffix, "front"), nativeAllocHintFixture(t, tc.reply+tc.suffix, "data")
			_ = osdMapsWrite(peer, msgr.MessageData{Type: osd.ReplyMessage, Version: 6, CompatVersion: 2, Transaction: sent.Transaction, Front: front, Data: data})
			got := <-done
			if tc.code < 0 {
				var server *OSDError
				if !errors.As(got.e, &server) || server.Code != tc.code {
					t.Fatal(got.e)
				}
			} else if got.e != nil {
				t.Fatal(got.e)
			}
			if !bytes.Equal(got.r.RawData, data) || !bytes.Equal(got.r.RawFront, front) || got.r.Submission.Transaction != sent.Transaction || got.r.MapEpoch != 11 {
				t.Fatal("metadata raw output or identity lost")
			}
			op := got.r.Operations[0]
			if tc.kind == "checksum" {
				if op.Checksum.Type != 2 || op.Checksum.ChunkSize != 4 {
					t.Fatal("checksum union lost", op)
				}
				want := 0
				if tc.suffix == "success" {
					want = 2
				}
				if tc.suffix == "trimmed" {
					want = 1
				}
				if len(op.Checksum.Values) != want || want > 0 && op.Checksum.Values[0] != 0x11223344 {
					t.Fatal("checksum values lost", op)
				}
			} else if tc.suffix == "populated" && len(op.Extents) != 2 {
				t.Fatal("extent decode lost", op)
			}
		})
	}
}

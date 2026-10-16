package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "success", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: uint64(^uint32(0))}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "trimmed", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: uint64(^uint32(0))}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "eof", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: uint64(^uint32(0))}}, 0},
		{"checksum", "checksum-native-request-v6-crc32c", "checksum-native-reply-v6-", "unaligned", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: 2, ChunkSize: 4, Seed: uint64(^uint32(0))}}, -22},
		{"checksum", "checksum-native-request-v6-xxhash32", "checksum-native-reply-v6-xxhash32-", "success", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash32, ChunkSize: 4, Seed: 0x89abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash32", "checksum-native-reply-v6-xxhash32-", "trimmed", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash32, ChunkSize: 4, Seed: 0x89abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash32", "checksum-native-reply-v6-xxhash32-", "eof", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash32, ChunkSize: 4, Seed: 0x89abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash32", "checksum-native-reply-v6-xxhash32-", "unaligned", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash32, ChunkSize: 4, Seed: 0x89abcdef}}, -22},
		{"checksum", "checksum-native-request-v6-xxhash64", "checksum-native-reply-v6-xxhash64-", "success", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash64, ChunkSize: 4, Seed: 0x0123456789abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash64", "checksum-native-reply-v6-xxhash64-", "trimmed", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash64, ChunkSize: 4, Seed: 0x0123456789abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash64", "checksum-native-reply-v6-xxhash64-", "eof", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash64, ChunkSize: 4, Seed: 0x0123456789abcdef}}, 0},
		{"checksum", "checksum-native-request-v6-xxhash64", "checksum-native-reply-v6-xxhash64-", "unaligned", OSDOperation{Code: OSDChecksum, Offset: 2, Length: 8, Checksum: OSDChecksumInput{Type: OSDChecksumXXHash64, ChunkSize: 4, Seed: 0x0123456789abcdef}}, -22},
	} {
		snapshots := []uint64{0, 4}
		for _, snapshot := range snapshots {
			t.Run(fmt.Sprintf("%s/%s/snap-%d", tc.request, tc.suffix, snapshot), func(t *testing.T) {
				c, peers, dials := osdTestClient(t)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
				if err != nil {
					t.Fatal(err)
				}
				peer := <-peers
				input := OSDRequest{ReadSnapshot: snapshot, ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: tc.kind + "-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: []OSDOperation{tc.op}}
				type response struct {
					r OSDReply
					e error
				}
				done := make(chan response, 1)
				go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
				sent := peer.next(t)
				expectedFront := bytes.Clone(nativeAllocHintFixture(t, tc.request, "front"))
				// This composes the separately verified read-snapshot tail with
				// the retained head metadata vector; it is not a fresh native
				// snapshot metadata vector. Actual combination coverage is live.
				if snapshot != 0 {
					binary.LittleEndian.PutUint64(expectedFront[len(expectedFront)-59:], snapshot)
				}
				if !bytes.Equal(sent.Front, expectedFront) || !bytes.Equal(sent.Data, nativeAllocHintFixture(t, tc.request, "data")) {
					t.Fatal("public native request differs")
				}
				front, data := nativeAllocHintFixture(t, tc.reply+tc.suffix, "front"), nativeAllocHintFixture(t, tc.reply+tc.suffix, "data")
				_ = osdMapsWrite(peer, msgr.MessageData{Type: osd.ReplyMessage, Version: 6, CompatVersion: 2, Transaction: sent.Transaction, Front: front, Data: data})
				got := <-done
				if tc.code < 0 {
					var unknown *OutcomeUnknownError
					if errors.As(got.e, &unknown) {
						t.Fatal("known metadata refusal became unknown", got.e)
					}
					var server *OSDError
					if !errors.As(got.e, &server) || server.Code != tc.code {
						t.Fatal(got.e)
					}
				} else if got.e != nil {
					t.Fatal(got.e)
				}
				if !bytes.Equal(got.r.RawData, data) || !bytes.Equal(got.r.RawFront, front) || got.r.Submission.Transaction != sent.Transaction || got.r.Transaction != sent.Transaction || got.r.MapEpoch != 11 {
					t.Fatal("metadata raw output or identity lost")
				}
				op := got.r.Operations[0]
				if tc.kind == "checksum" {
					if op.Checksum.Type != tc.op.Checksum.Type || op.Checksum.ChunkSize != 4 {
						t.Fatal("checksum union lost", op)
					}
					want := 0
					if tc.suffix == "success" {
						want = 2
					}
					if tc.suffix == "trimmed" {
						want = 1
					}
					wantFirst := uint64(0x11223344)
					if tc.op.Checksum.Type == OSDChecksumXXHash64 {
						wantFirst = 0x1122334455667788
					}
					if len(op.Checksum.Values) != want || want > 0 && op.Checksum.Values[0] != wantFirst {
						t.Fatal("checksum values lost", op)
					}
				} else if tc.suffix == "populated" && len(op.Extents) != 2 {
					t.Fatal("extent decode lost", op)
				}
				if o.session.Err() != nil || dials.Load() != 1 {
					t.Fatal("metadata read reconnected or invalidated a healthy session")
				}
				select {
				case replay := <-peer.requests:
					t.Fatal("metadata read replayed", replay.Transaction)
				default:
				}
			})
		}
	}
}

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

func TestOSDXattrU64PublicFreshNativeReplies(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int32
	}{{"true", 1}, {"false", -125}, {"invalid", -22}} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			op := publicU64XattrOperation(0xfedcba9876543210)
			op.Xattr.Name = "number"
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: "u64-xattr-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: []OSDOperation{op}}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			if !bytes.Equal(sent.Front, nativeAllocHintFixture(t, "u64-xattr-native-request-v6-read", "front")) || !bytes.Equal(sent.Data, nativeAllocHintFixture(t, "u64-xattr-native-request-v6-read", "data")) {
				t.Fatal("public U64 request differs from native")
			}
			front := nativeAllocHintFixture(t, "u64-xattr-native-reply-v6-"+tc.name, "front")
			data := nativeAllocHintFixture(t, "u64-xattr-native-reply-v6-"+tc.name, "data")
			if err := osdMapsWrite(peer, msgr.MessageData{Type: osd.ReplyMessage, Version: 6, CompatVersion: 2, Transaction: sent.Transaction, Front: front, Data: data}); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if tc.code < 0 {
				var server *OSDError
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal(got.err)
				}
			} else if got.err != nil {
				t.Fatal(got.err)
			}
			result := got.reply.Operations[0]
			if result.Result != tc.code || got.reply.Code != tc.code || result.Xattr.ComparisonMode != 2 || result.Xattr.ValueLength != 8 || result.Xattr.ComparisonOp != 3 || got.reply.Submission.Transaction != sent.Transaction || !bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) {
				t.Fatal("native U64 result metadata lost", got.reply)
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("native known refusal reconnected")
			}
			select {
			case m := <-peer.requests:
				t.Fatal("U64 native refusal replayed", m.Transaction)
			default:
			}
		})
	}
}

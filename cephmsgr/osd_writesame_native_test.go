package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"testing"
	"time"
)

func TestOSDWriteSamePublicFreshNativeReplies(t *testing.T) {
	for _, kind := range []string{"success", "refused", "failok", "zero"} {
		t.Run(kind, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			ops := []OSDOperation{{Code: OSDWriteSame, Offset: 17, Length: 12, Data: []byte{0, 255, 'x'}}, {Code: OSDWriteSame, Offset: 7, Data: []byte{0xaa}}, {Code: OSDAppend, Length: 6, Data: []byte{'t', 'a', 'i', 'l', 0, 255}}}
			aggregate, perop := int32(0), int32(0)
			if kind == "refused" {
				aggregate, perop = -27, -27
			}
			if kind == "failok" {
				ops[0].Flags = OSDFailOK
				perop = -27
			}
			if kind == "zero" {
				ops = ops[1:2]
			}
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: "writesame-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, ModificationTime: OSDTime{Seconds: 1700000000, Nanoseconds: 123456789}, Operations: ops}
			type result struct {
				r OSDReply
				e error
			}
			done := make(chan result, 1)
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			sent := peer.next(t)
			if kind == "success" || kind == "refused" {
				if !bytes.Equal(sent.Front, nativeAllocHintFixture(t, "writesame-native-request-v6-all", "front")) || !bytes.Equal(sent.Data, nativeAllocHintFixture(t, "writesame-native-request-v6-all", "data")) {
					t.Fatal("public request differs from fresh native input")
				}
			}
			name := "writesame-native-reply-v6-" + kind
			front, data := nativeAllocHintFixture(t, name, "front"), nativeAllocHintFixture(t, name, "data")
			if err := osdMapsWrite(peer, msgr.MessageData{Type: osd.ReplyMessage, Version: 6, CompatVersion: 2, Transaction: sent.Transaction, Front: front, Data: data}); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if aggregate < 0 {
				var server *OSDError
				var unknown *OutcomeUnknownError
				if !errors.As(got.e, &server) || server.Code != aggregate || errors.As(got.e, &unknown) {
					t.Fatal("native signed refusal lost", got.e)
				}
			} else if got.e != nil {
				t.Fatal(got.e)
			}
			ws := got.r.Operations[0]
			if got.r.Code != aggregate || ws.Result != perop || ws.Offset != ops[0].Offset || ws.Length != ops[0].Length || ws.WriteSamePatternLength != uint64(len(ops[0].Data)) || ws.TruncateSize != 0 || ws.TruncateSequence != 0 || got.r.Submission.Transaction != sent.Transaction || !bytes.Equal(got.r.RawFront, front) || !bytes.Equal(got.r.RawData, data) {
				t.Fatal("native pattern, extent, signed result or raw receipt lost", got.r)
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("known native refusal reconnected")
			}
			select {
			case m := <-peer.requests:
				t.Fatal("native writesame replayed", m.Transaction)
			default:
			}
		})
	}
}

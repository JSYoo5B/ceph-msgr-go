package cephmsgr

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func TestOSDWatcherLocalRejectionNeverSends(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	for _, edit := range []func(*OSDRequest){
		func(r *OSDRequest) { r.Operations[0].ListWatchers.MaxOutputBytes = 0 },
		func(r *OSDRequest) { r.Operations[0].Flags = OSDFailOK },
		func(r *OSDRequest) { r.ReadSnapshot = 4 },
		func(r *OSDRequest) { r.SnapContext = OSDSnapContext{Sequence: 4} },
		func(r *OSDRequest) { r.Operations = append(r.Operations, OSDOperation{Code: OSDStat}) },
	} {
		input := osdValidationRequest()
		input.ModificationTime = OSDTime{}
		input.Operations = []OSDOperation{{Code: OSDListWatchers, ListWatchers: OSDListWatchersInput{MaxOutputBytes: 512}}}
		edit(&input)
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if err == nil || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("invalid watcher request submitted", err)
		}
		select {
		case <-peer.requests:
			t.Fatal("invalid listing transmitted")
		default:
		}
	}
}

func watcherTestData() []byte {
	item := wire.Encoder{}
	item.U8(8)
	item.U64(77)
	item.U64(123)
	item.U32(37)
	msgr.Address{Type: 2, Nonce: 456}.Encode(&item)
	body := wire.Encoder{}
	body.U32(1)
	body.Struct(2, 1, item.Data)
	out := wire.Encoder{}
	out.Struct(1, 1, body.Data)
	return out.Data
}

func TestOSDWatcherPublicResultAndMalformedResponse(t *testing.T) {
	for _, kind := range []string{"valid", "malformed", "budget", "union", "refusal"} {
		t.Run(kind, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			data := watcherTestData()
			input := osdValidationRequest()
			input.ModificationTime = OSDTime{}
			input.Operations = []OSDOperation{{Code: OSDListWatchers, ListWatchers: OSDListWatchersInput{MaxOutputBytes: 512}}}
			if kind == "budget" {
				input.Operations[0].ListWatchers.MaxOutputBytes = 10
			}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			request := peer.next(t)
			op := OSDOperation{Code: OSDListWatchers}
			code := int32(0)
			if kind == "malformed" {
				data = data[:len(data)-1]
			}
			if kind == "union" {
				op.Length = 1
			}
			if kind == "refusal" {
				data = nil
				code = -13
			}
			reply := osdXattrSyntheticReply(t, []OSDOperation{op}, [][]byte{data}, []int32{code}, code, uint64(osd.FlagOnDisk|osd.FlagAck))
			reply.Transaction = request.Transaction
			_ = osdMapsWrite(peer, reply)
			got := <-done
			switch kind {
			case "valid":
				if got.err != nil || len(got.reply.Operations[0].Watchers) != 1 {
					t.Fatal(got.err)
				}
				w := got.reply.Operations[0].Watchers[0]
				if w.NameNumber != 77 || w.Cookie != 123 || w.TimeoutSeconds != 37 || w.Address.Nonce != 456 || w.Address.Endpoint.IsValid() || string(got.reply.RawData) != string(data) {
					t.Fatal("watcher metadata lost", w)
				}
			case "refusal":
				var server *OSDError
				if !errors.As(got.err, &server) || server.Code != -13 {
					t.Fatal(got.err)
				}
			default:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("invalid listing completed", got.err)
				}
			}
			o.Close()
			if dials.Load() != 1 {
				t.Fatal("listing reconnected")
			}
			select {
			case <-peer.requests:
				t.Fatal("listing resent")
			default:
			}
		})
	}
}

// The public path uses the fresh native request and response envelopes. These
// bytes were generated independently from the Go encoder; this peer is synthetic.
func TestOSDWatcherPublicWireMatchesFreshNative(t *testing.T) {
	for _, name := range []string{"populated", "empty", "enoent", "eacces"} {
		t.Run(name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: "watchers-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: []OSDOperation{{Code: OSDListWatchers, ListWatchers: OSDListWatchersInput{MaxOutputBytes: 512}}}}
			type response struct {
				r OSDReply
				e error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			request := peer.next(t)
			if string(request.Front) != string(nativeAllocHintFixture(t, "watchers-native-request-v6-list", "front")) || len(request.Data) != 0 {
				t.Fatal("public request differs from native")
			}
			front := nativeAllocHintFixture(t, "watchers-native-reply-v6-"+name, "front")
			data := nativeAllocHintFixture(t, "watchers-native-reply-v6-"+name, "data")
			reply := msgr.MessageData{Type: osd.ReplyMessage, Version: 6, CompatVersion: 2, Transaction: request.Transaction, Front: front, Data: data}
			_ = osdMapsWrite(peer, reply)
			got := <-done
			var server *OSDError
			if name == "enoent" || name == "eacces" {
				code := int32(-2)
				if name == "eacces" {
					code = -13
				}
				if !errors.As(got.e, &server) || server.Code != code || got.r.Code != code {
					t.Fatal(got.e)
				}
			} else if got.e != nil {
				t.Fatal(got.e)
			}
			if string(got.r.RawFront) != string(front) || string(got.r.RawData) != string(data) || got.r.Submission.Transaction != request.Transaction {
				t.Fatal("raw reply or identity lost")
			}
			if name == "populated" && (len(got.r.Operations[0].Watchers) != 2 || got.r.Operations[0].Watchers[1].Cookie != ^uint64(0) || got.r.Operations[0].Watchers[1].Address.Endpoint.String() != "[2001:db8::2]:6801") {
				t.Fatal("native public watcher conversion differs")
			}
		})
	}
}

func TestOSDWatcherPendingCancellationAndCloseDoNotReplay(t *testing.T) {
	for _, closeConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeConnection], func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			input.ModificationTime = OSDTime{}
			input.Operations = []OSDOperation{{Code: OSDListWatchers, ListWatchers: OSDListWatchersInput{MaxOutputBytes: 512}}}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			if closeConnection {
				o.Close()
			} else {
				cancel()
			}
			select {
			case got := <-done:
				var unknown *OutcomeUnknownError
				if !errors.As(got.err, &unknown) || got.reply.Submission.Transaction != sent.Transaction {
					t.Fatal("pending listing lost identity/cause", got.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("listing remained pending")
			}
			o.Close()
			if dials.Load() != 1 {
				t.Fatal("listing reconnected")
			}
			select {
			case <-peer.requests:
				t.Fatal("listing replayed")
			default:
			}
		})
	}
}

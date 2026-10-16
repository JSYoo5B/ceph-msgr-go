package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
	"reflect"
	"testing"
	"time"
)

func TestPublicSparseReadPreservesConcatenation(t *testing.T) {
	e := wire.Encoder{}
	e.U32(2)
	e.U64(3)
	e.U64(2)
	e.U64(8)
	e.U64(1)
	e.Bytes([]byte{0, 255, 65})
	got := publicSparseRead(e.Data)
	if !reflect.DeepEqual(got.Extents, []OSDExtent{{Offset: 3, Length: 2}, {Offset: 8, Length: 1}}) || !bytes.Equal(got.Data, []byte{0, 255, 65}) {
		t.Fatal(got)
	}
	if got := publicSparseRead(e.Data[:len(e.Data)-1]); len(got.Extents) != 0 || len(got.Data) != 0 {
		t.Fatal("malformed conversion accepted", got)
	}
}

// The peer replays independently constructed native wire outputs; live native
// daemon comparisons are separate opt-in integration tests.
func TestOSDSparseReadPublicFreshNativePayloads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int32
		count  int
		length uint64
	}{{"populated", 0, 2, 13}, {"empty", 0, 0, 0}, {"enoent", -2, 0, 97}, {"eacces", -13, 0, 97}} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11, Object: OSDObject{Pool: 7, Name: "sparse-read-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: []OSDOperation{{Code: OSDSparseRead, Offset: 3, Length: 97, SparseRead: OSDSparseReadInput{MaxOutputBytes: 512}}}}
			type response struct {
				r OSDReply
				e error
			}
			done := make(chan response, 1)
			go func() { r, e := o.Request(ctx, input); done <- response{r, e} }()
			sent := peer.next(t)
			if !bytes.Equal(sent.Front, nativeAllocHintFixture(t, "sparse-read-native-request-v6-map", "front")) || !bytes.Equal(sent.Data, nativeAllocHintFixture(t, "sparse-read-native-request-v6-map", "data")) {
				t.Fatal("public native request differs")
			}
			front, data := nativeAllocHintFixture(t, "sparse-read-native-reply-v6-"+tc.name, "front"), nativeAllocHintFixture(t, "sparse-read-native-reply-v6-"+tc.name, "data")
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
				t.Fatal("raw output or identity lost")
			}
			op := got.r.Operations[0]
			if op.Length != tc.length || len(op.SparseRead.Extents) != tc.count {
				t.Fatal("extent metadata lost", op)
			}
			if tc.count > 0 && (!bytes.Equal(op.SparseRead.Data, []byte{'A', 0, 'B', 255, 'Z', '1', '2', '3', '4', '5', '6', '7', '8'}) || op.SparseRead.Extents[1].Offset != 20) {
				t.Fatal("sparse data changed", op)
			}
		})
	}
}

func TestOSDSparseReadLocalRejectionNeverSends(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	for _, edit := range []func(*OSDRequest){
		func(r *OSDRequest) { r.Operations[0].SparseRead.MaxOutputBytes = 7 },
		func(r *OSDRequest) { r.Operations[0].Length = 0 },
		func(r *OSDRequest) { r.SnapContext = OSDSnapContext{Sequence: 4} },
		func(r *OSDRequest) { r.Operations[0].Flags = OSDFailOK },
		func(r *OSDRequest) { r.Operations = append(r.Operations, OSDOperation{Code: OSDStat}) },
	} {
		input := osdValidationRequest()
		input.ModificationTime = OSDTime{}
		input.Operations = []OSDOperation{{Code: OSDSparseRead, Length: 16, SparseRead: OSDSparseReadInput{MaxOutputBytes: 512}}}
		edit(&input)
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if err == nil || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("invalid sparse read submitted", err)
		}
		select {
		case <-peer.requests:
			t.Fatal("invalid sparse read transmitted")
		default:
		}
	}
}

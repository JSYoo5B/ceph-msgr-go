package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func pgListRequest() OSDPGListRequest {
	return OSDPGListRequest{MapEpoch: 11, PG: OSDPGListTarget{Pool: 7, Seed: 0}, Namespace: "tenant", Count: 2, StartEpoch: 9, MaxOutputBytes: 512}
}
func pgListData() []byte {
	c := wire.Encoder{}
	c.String("route")
	c.String("last")
	c.U64(osd.NoSnap)
	c.U32(42)
	c.U8(0)
	c.String("tenant")
	c.U64(7)
	b := wire.Encoder{}
	b.Struct(4, 3, c.Data)
	b.U32(1)
	b.String("tenant")
	b.String("one")
	b.String("route")
	e := wire.Encoder{}
	e.Struct(1, 1, b.Data)
	return e.Data
}
func pgListSyntheticReply(t *testing.T, r OSDPGListRequest, data []byte, code int32) msgr.MessageData {
	t.Helper()
	op := OSDOperation{Code: OSDOpcode(osd.PGNLS), Offset: r.Count, Length: uint64(r.StartEpoch)}
	result := int32(0)
	if code < 0 {
		result = code
	}
	m := osdXattrSyntheticReply(t, []OSDOperation{op}, [][]byte{data}, []int32{result}, code, osd.FlagOnDisk|osd.FlagAck)
	// This independent synthetic helper's object identity is osd-codec (9 bytes).
	n := int(binary.LittleEndian.Uint32(m.Front))
	m.Front = append(make([]byte, 4), m.Front[4+n:]...)
	binary.LittleEndian.PutUint32(m.Front[13:], r.PG.Seed)
	return m
}
func TestOSDPGListLocalRejectionNeverSends(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	peer := <-peers
	for _, edit := range []func(*OSDPGListRequest){func(r *OSDPGListRequest) { r.PG.Primary = 99 }, func(r *OSDPGListRequest) { r.MapEpoch = 0 }, func(r *OSDPGListRequest) { r.Count = 0 }, func(r *OSDPGListRequest) { r.Cursor = []byte{0} }, func(r *OSDPGListRequest) { r.MaxOutputBytes = 48 }} {
		r := pgListRequest()
		edit(&r)
		before := c.osdNext.Load()
		reply, err := o.ListPG(context.Background(), r)
		if err == nil || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatal("invalid listing submitted", err)
		}
		select {
		case <-peer.requests:
			t.Fatal("invalid listing transmitted")
		default:
		}
	}
}
func TestOSDPGListDecodedResultAndMalformedReply(t *testing.T) {
	for _, kind := range []string{"page", "end", "refusal", "truncated", "budget", "count", "identity", "unknown-result"} {
		t.Run(kind, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			peer := <-peers
			r := pgListRequest()
			if kind == "budget" {
				r.MaxOutputBytes = 49
			}
			if kind == "count" {
				r.Count = 1
			}
			type outcome struct {
				r OSDPGListReply
				e error
			}
			done := make(chan outcome, 1)
			go func() { reply, err := o.ListPG(ctx, r); done <- outcome{reply, err} }()
			sent := peer.next(t)
			data, code := pgListData(), int32(0)
			switch kind {
			case "end":
				code = 1
			case "refusal":
				code = -13
				data = nil
			case "truncated":
				data = data[:len(data)-1]
			case "unknown-result":
				code = 2
			case "count":
				binary.LittleEndian.PutUint32(data[6+6+5+4+4+4+8+4+1+4+6+8:], 2)
			}
			reply := pgListSyntheticReply(t, r, data, code)
			reply.Transaction = sent.Transaction
			if kind == "identity" {
				reply.Front[13]++
			}
			_ = osdMapsWrite(peer, reply)
			got := <-done
			switch kind {
			case "page", "end":
				if got.e != nil || got.r.End != (kind == "end") || len(got.r.Entries) != 1 || got.r.Cursor.Object != "last" || got.r.Cursor.Key != "route" || got.r.Cursor.Hash != 42 || got.r.Entries[0].Object != "one" || got.r.Entries[0].Namespace != "tenant" || !bytes.Equal(got.r.RawData, data) || got.r.Submission.Transaction != sent.Transaction {
					t.Fatal(got.r, got.e)
				}
			case "refusal":
				var server *OSDError
				if !errors.As(got.e, &server) || server.Code != -13 {
					t.Fatal(got.e)
				}
			default:
				var unknown *OutcomeUnknownError
				if !errors.As(got.e, &unknown) || !errors.Is(got.e, msgr.ErrFrame) {
					t.Fatal("bad listing completed", got.e)
				}
			}
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

func TestOSDPGListCancellationAndCloseNeverReplay(t *testing.T) {
	for _, closeConnection := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[closeConnection], func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			peer := <-peers
			type outcome struct {
				r OSDPGListReply
				e error
			}
			done := make(chan outcome, 1)
			go func() { r, e := o.ListPG(ctx, pgListRequest()); done <- outcome{r, e} }()
			sent := peer.next(t)
			if closeConnection {
				o.Close()
			} else {
				cancel()
			}
			got := <-done
			var unknown *OutcomeUnknownError
			if !errors.As(got.e, &unknown) || got.r.Submission.Transaction != sent.Transaction || got.r.Submission.MapEpoch != 11 {
				t.Fatal("submitted listing outcome lost", got.r, got.e)
			}
			if closeConnection && !errors.Is(got.e, ErrClosed) {
				t.Fatal("Close identity lost", got.e)
			}
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

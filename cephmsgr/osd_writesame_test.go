package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func writeSameSyntheticReply(t *testing.T, input OSDRequest, results []int32, aggregate int32, flags uint64, v8 bool) msgr.MessageData {
	t.Helper()
	outputs := make([][]byte, len(input.Operations))
	var m msgr.MessageData
	if v8 {
		m = replyV8ProfileMessage(input, outputs, results, aggregate, flags, false)
	} else {
		m = osdXattrSyntheticReply(t, input.Operations, outputs, results, aggregate, flags)
	}
	// Fault-injection reply construction only: the third union u64 is pattern
	// length, independent of extent truncate fields and output payload length.
	start := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
	for i, op := range input.Operations {
		if op.Code == OSDWriteSame {
			binary.LittleEndian.PutUint64(m.Front[start+38*i+22:], uint64(len(op.Data)))
		}
	}
	return m
}

func TestOSDWriteSamePublicRawReceiptAndCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		aggregate int32
		results   []int32
		flags     uint64
		failok    bool
		edit      func(*msgr.MessageData)
		invalid   bool
	}{
		{name: "committed", results: []int32{0}, flags: OSDReplyOnDisk},
		{name: "known refusal", aggregate: -27, results: []int32{-27}},
		{name: "FAILOK with following append", results: []int32{-27, 0}, flags: OSDReplyOnDisk, failok: true},
		{name: "ACK alone", results: []int32{0}, flags: OSDReplyAck, invalid: true},
		{name: "unmasked per-op error", results: []int32{-27}, flags: OSDReplyOnDisk, invalid: true},
		{name: "changed pattern echo", results: []int32{0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[84:], 2) }},
		{name: "changed logical extent", results: []int32{0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[76:], 3) }},
		{name: "nonzero padding", results: []int32{0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { m.Front[92] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			input.ClientIncarnation = 19
			input.Operations = []OSDOperation{{Code: OSDWriteSame, Offset: 17, Length: 12, Data: []byte{0, 255, 'x'}}}
			if tc.failok {
				input.Operations[0].Flags = OSDFailOK
				input.Operations = append(input.Operations, OSDOperation{Code: OSDAppend, Length: 1, Data: []byte{9}})
			}
			type result struct {
				r OSDReply
				e error
			}
			done := make(chan result, 1)
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			sent := peer.next(t)
			wantData := []byte{0, 255, 'x'}
			if tc.failok {
				wantData = append(wantData, 9)
			}
			if !bytes.Equal(sent.Data, wantData) || binary.LittleEndian.Uint32(sent.Front[8:]) != uint32(osd.FlagWrite|osd.FlagOnDisk|osd.FlagReturnVec) {
				t.Fatal("pattern was expanded or modes changed", sent.Data)
			}
			reply := writeSameSyntheticReply(t, input, tc.results, tc.aggregate, tc.flags, false)
			reply.Transaction = sent.Transaction
			if tc.edit != nil {
				tc.edit(&reply)
			}
			if err := osdMapsWrite(peer, reply); err != nil {
				t.Fatal(err)
			}
			got := <-done
			var unknown *OutcomeUnknownError
			var server *OSDError
			if tc.invalid {
				if !errors.As(got.e, &unknown) {
					t.Fatal("contradictory receipt was accepted", got.e)
				}
				return
			}
			if tc.aggregate < 0 {
				if !errors.As(got.e, &server) || server.Code != tc.aggregate || errors.As(got.e, &unknown) {
					t.Fatal("known signed refusal lost", got.e)
				}
			} else if got.e != nil {
				t.Fatal(got.e)
			}
			op := got.r.Operations[0]
			if op.WriteSamePatternLength != 3 || op.TruncateSize != 0 || op.Length != 12 || op.Result != tc.results[0] || !bytes.Equal(got.r.RawFront, reply.Front) || !bytes.Equal(got.r.RawData, reply.Data) || got.r.Transaction != sent.Transaction || got.r.Submission.Transaction != sent.Transaction || got.r.Submission.ClientIncarnation != 19 {
				t.Fatal("raw pattern/extent/identity lost", got.r)
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("known refusal ended session")
			}
			select {
			case m := <-peer.requests:
				t.Fatal("writesame replayed", m.Transaction)
			default:
			}
		})
	}
}

func TestOSDWriteSamePublicV8AndUnknownOutcome(t *testing.T) {
	t.Run("v8", func(t *testing.T) {
		_, o, peer, ctx, _ := replyV8ProfileOpen(t)
		input := backoffTransportRequest()
		input.Placement.Primary = o.target.ID
		input.Operations = []OSDOperation{{Code: OSDWriteSame, Offset: 2, Length: 8, Data: []byte{0, 255}}}
		type result struct {
			r OSDReply
			e error
		}
		done := make(chan result, 1)
		go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
		sent := peer.next(t)
		reply := writeSameSyntheticReply(t, input, []int32{0}, 0, OSDReplyOnDisk, true)
		reply.Transaction = sent.Transaction
		if err := osdMapsWrite(peer, reply); err != nil {
			t.Fatal(err)
		}
		got := <-done
		if got.e != nil || got.r.Operations[0].WriteSamePatternLength != 2 || got.r.Submission.Placement == nil || got.r.Submission.Placement.PG != input.Placement.PG || !bytes.Equal(got.r.RawFront, reply.Front) {
			t.Fatal("v8 pattern or actual PG identity", got.r, got.e)
		}
	})
	t.Run("sent cancellation", func(t *testing.T) {
		c, peers, _ := osdTestClient(t)
		o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
		if err != nil {
			t.Fatal(err)
		}
		peer := <-peers
		input := osdValidationRequest()
		input.Operations = []OSDOperation{{Code: OSDWriteSame, Length: 8, Data: []byte{0, 255}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		type result struct {
			r OSDReply
			e error
		}
		done := make(chan result, 1)
		go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
		sent := peer.next(t)
		cancel()
		got := <-done
		var unknown *OutcomeUnknownError
		if !errors.As(got.e, &unknown) || !errors.Is(got.e, context.Canceled) || got.r.Submission.Transaction != sent.Transaction || got.r.Transaction != 0 || len(got.r.RawFront) != 0 {
			t.Fatal("unknown outcome identity", got.r, got.e)
		}
		o.Close()
		select {
		case m := <-peer.requests:
			t.Fatal("uncertain writesame replayed", m.Transaction)
		default:
		}
	})
}

func TestOSDWriteSameRefusedBeforeSubmission(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	for _, ops := range [][]OSDOperation{{{Code: OSDWriteSame}}, {{Code: OSDWriteSame, Length: 3, Data: []byte{1, 2}}}, {{Code: OSDWriteSame, Length: 6, Data: []byte{1}}, {Code: OSDRead, Length: 6}}, {{Code: OSDWriteSame, Length: ^uint64(0), Data: []byte{1}}}} {
		input := osdValidationRequest()
		input.Operations = ops
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if err == nil || reply.Submission != (OSDSubmission{}) || before != c.osdNext.Load() {
			t.Fatal("invalid writesame allocated submission", reply, err)
		}
	}
	select {
	case m := <-peer.requests:
		t.Fatal("invalid writesame sent", m.Transaction)
	default:
	}
}

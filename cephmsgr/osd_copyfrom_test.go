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

func copyFromRequest(code OSDOpcode) OSDOperation {
	return OSDOperation{Code: code, CopyFrom: OSDCopyFromInput{SourcePool: 3, SourceName: "src", SourceKey: "k", SourceNamespace: "ns", SourceVersion: 0x0102030405060708}}
}

// copyFromSyntheticReply is fault-injection construction only: the packed
// union is the request echo (snapid, source version, flags, fadvise, padding).
func copyFromSyntheticReply(t *testing.T, input OSDRequest, results []int32, aggregate int32, flags uint64, v8 bool) msgr.MessageData {
	t.Helper()
	outputs := make([][]byte, len(input.Operations))
	var m msgr.MessageData
	if v8 {
		m = replyV8ProfileMessage(input, outputs, results, aggregate, flags, false)
	} else {
		m = osdXattrSyntheticReply(t, input.Operations, outputs, results, aggregate, flags)
	}
	start := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
	for i, op := range input.Operations {
		if op.Code == OSDCopyFrom || op.Code == OSDCopyFrom2 {
			u := m.Front[start+38*i+6:]
			snap := op.CopyFrom.SourceSnapshot
			if snap == 0 {
				snap = osd.NoSnap
			}
			binary.LittleEndian.PutUint64(u, snap)
			binary.LittleEndian.PutUint64(u[8:], op.CopyFrom.SourceVersion)
			clear(u[16:28])
			if op.CopyFrom.TruncateSequence != 0 {
				u[16] = osd.CopyFromTruncateSequence
			}
		}
	}
	return m
}

func TestOSDCopyFromPublicRawReceiptAndCompletion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      OSDOpcode
		truncate  bool
		aggregate int32
		flags     uint64
		badEcho   bool
		invalid   bool
	}{
		{name: "committed copy-from2", code: OSDCopyFrom2, flags: OSDReplyOnDisk},
		{name: "committed copy-from", code: OSDCopyFrom, flags: OSDReplyOnDisk},
		{name: "committed truncate sequence", code: OSDCopyFrom2, truncate: true, flags: OSDReplyOnDisk},
		{name: "source missing", code: OSDCopyFrom2, aggregate: -2},
		{name: "source version changed", code: OSDCopyFrom2, aggregate: -34},
		{name: "ACK alone", code: OSDCopyFrom2, flags: OSDReplyAck, invalid: true},
		{name: "changed source version echo", code: OSDCopyFrom2, flags: OSDReplyOnDisk, invalid: true, badEcho: true},
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
			input.ClientIncarnation = 23
			input.Operations = []OSDOperation{copyFromRequest(tc.code)}
			if tc.truncate {
				input.Operations[0].CopyFrom.TruncateSequence, input.Operations[0].CopyFrom.TruncateSize = 5, 9
			}
			type result struct {
				r OSDReply
				e error
			}
			done := make(chan result, 1)
			go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
			sent := peer.next(t)
			if binary.LittleEndian.Uint32(sent.Front[8:]) != uint32(osd.FlagWrite|osd.FlagOnDisk|osd.FlagReturnVec) || len(sent.Data) == 0 || !bytes.Contains(sent.Data, []byte("src")) {
				t.Fatal("copy source not carried as indata or modes changed", sent.Data)
			}
			reply := copyFromSyntheticReply(t, input, []int32{tc.aggregate}, tc.aggregate, tc.flags, false)
			reply.Transaction = sent.Transaction
			if tc.badEcho {
				start := 4 + len(input.Object.Name) + 17 + 8 + 4 + 12 + 4 + 4
				reply.Front[start+6+8] ^= 1
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
			if op.CopyFromVersion != input.Operations[0].CopyFrom.SourceVersion || op.Result != tc.aggregate || !bytes.Equal(got.r.RawFront, reply.Front) || got.r.Transaction != sent.Transaction || got.r.Submission.ClientIncarnation != 23 {
				t.Fatal("raw union/identity lost", got.r)
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("known refusal ended session")
			}
			select {
			case m := <-peer.requests:
				t.Fatal("copy-from replayed", m.Transaction)
			default:
			}
		})
	}
}

func TestOSDCopyFromPublicV8AndUnknownOutcome(t *testing.T) {
	t.Run("v8", func(t *testing.T) {
		_, o, peer, ctx, _ := replyV8ProfileOpen(t)
		input := backoffTransportRequest()
		input.Placement.Primary = o.target.ID
		input.Operations = []OSDOperation{copyFromRequest(OSDCopyFrom2)}
		type result struct {
			r OSDReply
			e error
		}
		done := make(chan result, 1)
		go func() { r, e := o.Request(ctx, input); done <- result{r, e} }()
		sent := peer.next(t)
		reply := copyFromSyntheticReply(t, input, []int32{0}, 0, OSDReplyOnDisk, true)
		reply.Transaction = sent.Transaction
		if err := osdMapsWrite(peer, reply); err != nil {
			t.Fatal(err)
		}
		got := <-done
		if got.e != nil || got.r.Submission.Placement == nil || got.r.Submission.Placement.PG != input.Placement.PG || !bytes.Equal(got.r.RawFront, reply.Front) {
			t.Fatal("v8 copy or actual PG identity", got.r, got.e)
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
		input.Operations = []OSDOperation{copyFromRequest(OSDCopyFrom2)}
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
			t.Fatal("uncertain copy replayed", m.Transaction)
		default:
		}
	})
}

func TestOSDCopyFromRefusedBeforeSubmission(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	good := copyFromRequest(OSDCopyFrom2)
	edit := func(f func(*OSDOperation)) OSDOperation { o := good; f(&o); return o }
	for _, ops := range [][]OSDOperation{
		{{Code: OSDCopyFrom2}},
		{edit(func(o *OSDOperation) { o.CopyFrom.SourceName = "" })},
		{edit(func(o *OSDOperation) { o.CopyFrom.SourcePool = -1 })},
		{edit(func(o *OSDOperation) { o.CopyFrom.TruncateSize = 4 })},
		{edit(func(o *OSDOperation) { o.Code = OSDCopyFrom; o.CopyFrom.TruncateSequence = 2 })},
		{edit(func(o *OSDOperation) { o.Flags = OSDFailOK })},
		{good, {Code: OSDStat}},
		{{Code: OSDStat, CopyFrom: good.CopyFrom}},
	} {
		input := osdValidationRequest()
		input.Operations = ops
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if err == nil || reply.Submission != (OSDSubmission{}) || before != c.osdNext.Load() {
			t.Fatal("invalid copy allocated submission", reply, err)
		}
	}
	select {
	case m := <-peer.requests:
		t.Fatal("invalid copy sent", m.Transaction)
	default:
	}
}

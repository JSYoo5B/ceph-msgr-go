package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func TestOSDAllocHintAdmissionAndReplies(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	defer func() {
		select {
		case m := <-peer.requests:
			t.Fatal("invalid allocation hint was transmitted", m.Transaction)
		default:
		}
	}()
	hint := OSDOperation{Code: OSDSetAllocHint, Flags: OSDFailOK, AllocHint: OSDAllocHint{ExpectedObjectSize: 4 << 20, ExpectedWriteSize: 64 << 10, Flags: OSDAllocHintSequentialWrite | OSDAllocHintCompressible}}
	request := func(ops ...OSDOperation) OSDRequest {
		r := osdValidationRequest()
		r.ModificationTime, r.Operations = OSDTime{Seconds: 1}, ops
		return r
	}
	for _, tc := range []struct {
		name string
		op   OSDOperation
		want error
	}{
		{"unknown hint flag", OSDOperation{Code: OSDSetAllocHint, AllocHint: OSDAllocHint{Flags: 1 << 11}}, osd.ErrRequest},
		{"hint with data", OSDOperation{Code: OSDSetAllocHint, Data: []byte{1}}, osd.ErrRequest},
		{"hint on another opcode", OSDOperation{Code: OSDStat, AllocHint: OSDAllocHint{ExpectedObjectSize: 1}}, osd.ErrRequest},
		{"exclusive hint", OSDOperation{Code: OSDSetAllocHint, Flags: OSDExclusiveCreate}, ErrUnsupportedOSDOperation},
	} {
		before := c.osdNext.Load()
		reply, err := o.Request(ctx, request(tc.op))
		if !errors.Is(err, tc.want) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s reached submission or lost cause: %v", tc.name, err)
		}
	}
	type response struct {
		reply OSDReply
		err   error
	}
	for _, tc := range []struct {
		name    string
		echo    OSDAllocHint
		result  int32
		code    int32
		flags   uint64
		invalid bool
	}{
		{name: "hint commits", echo: hint.AllocHint, flags: uint64(osd.FlagOnDisk | osd.FlagAck)},
		{name: "FAILOK refusal retained", echo: hint.AllocHint, result: -95, flags: uint64(osd.FlagOnDisk | osd.FlagAck)},
		{name: "hint echo changed", echo: OSDAllocHint{ExpectedObjectSize: 1}, flags: uint64(osd.FlagOnDisk), invalid: true},
		{name: "commit without ONDISK", echo: hint.AllocHint, flags: uint64(osd.FlagAck), invalid: true},
	} {
		c, peers, _ := osdTestClient(t)
		o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
		if err != nil {
			t.Fatal(err)
		}
		peer := <-peers
		done := make(chan response, 1)
		go func() { reply, err := o.Request(ctx, request(hint)); done <- response{reply, err} }()
		m := peer.next(t)
		if binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(osd.FlagWrite|osd.FlagOnDisk|osd.FlagReturnVec) || len(m.Data) != 0 {
			t.Fatal(tc.name, "allocation hint request flags/data differ")
		}
		// The synthetic helper writes the union through Offset/Length; place the
		// alloc_hint fields there: expected sizes, then flags in the next u64.
		replyOp := hint
		replyOp.Offset, replyOp.Length = tc.echo.ExpectedObjectSize, tc.echo.ExpectedWriteSize
		reply := osdXattrSyntheticReply(t, []OSDOperation{replyOp}, [][]byte{nil}, []int32{tc.result}, tc.code, tc.flags)
		binary.LittleEndian.PutUint32(reply.Front[62+6+16:], uint32(tc.echo.Flags))
		reply.Transaction = m.Transaction
		_ = osdMapsWrite(peer, reply)
		got := <-done
		var unknown *OutcomeUnknownError
		if tc.invalid {
			if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
				t.Fatal(tc.name, "contradictory allocation hint reply completed", got.err)
			}
			continue // the invalid reply retired this case's session
		}
		if got.err != nil || got.reply.Operations[0].AllocHint != hint.AllocHint || got.reply.Operations[0].Result != tc.result {
			t.Fatal(tc.name, "valid allocation hint reply changed", got.err, got.reply.Operations)
		}
	}
	if dials.Load() != 1 {
		t.Fatal("allocation hint validation reconnected")
	}
}

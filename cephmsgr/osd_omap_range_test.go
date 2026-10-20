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

func TestOSDOmapRangeRemovalAdmissionAndReplies(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	request := func(ops ...OSDOperation) OSDRequest {
		r := osdValidationRequest()
		r.ModificationTime, r.Operations = OSDTime{Seconds: 1}, ops
		return r
	}
	rmrange := func(flags OSDOperationFlags, begin, end string) OSDOperation {
		return OSDOperation{Code: OSDRemoveOmapKeyRange, Flags: flags, Omap: OSDOmapInput{RangeBegin: begin, RangeEnd: end}}
	}
	for _, tc := range []struct {
		name string
		ops  []OSDOperation
		want error
	}{
		{"reversed range", []OSDOperation{rmrange(0, "b", "a")}, osd.ErrRequest},
		{"range output budget", []OSDOperation{{Code: OSDRemoveOmapKeyRange, Omap: OSDOmapInput{MaxOutputBytes: 1}}}, osd.ErrRequest},
		{"range fields on another opcode", []OSDOperation{{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{RangeBegin: "a"}}}, osd.ErrRequest},
		{"OMAP read after staged range removal", []OSDOperation{rmrange(0, "a", "b"), {Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 8}}}, ErrUnsupportedOSDOperation},
	} {
		before := c.osdNext.Load()
		reply, err := o.Request(ctx, request(tc.ops...))
		if !errors.Is(err, tc.want) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s reached submission or lost cause: %v", tc.name, err)
		}
		select {
		case m := <-peer.requests:
			t.Fatalf("%s was transmitted as %d", tc.name, m.Transaction)
		default:
		}
	}
	type response struct {
		reply OSDReply
		err   error
	}
	for _, tc := range []struct {
		name    string
		ops     []OSDOperation
		results []int32
		code    int32
		length  uint64
		invalid bool
	}{
		{name: "range removal commits", ops: []OSDOperation{rmrange(0, "b\x00", "c\xff")}, length: 12},
		{name: "absent object is known ENOENT", ops: []OSDOperation{rmrange(0, "a", "b")}, results: []int32{-2}, code: -2, length: 10},
		{name: "range removal changed input length", ops: []OSDOperation{rmrange(0, "a", "b")}, length: 9, invalid: true},
	} {
		done := make(chan response, 1)
		go func() { reply, err := o.Request(ctx, request(tc.ops...)); done <- response{reply, err} }()
		m := peer.next(t)
		if m.Type != msgr.OSDOpMessage || binary.LittleEndian.Uint32(m.Front[8:12]) != uint32(osd.FlagWrite|osd.FlagOnDisk|osd.FlagReturnVec) {
			t.Fatal(tc.name, "range removal request mode/completion flags differ")
		}
		if tc.name == "range removal commits" && !bytes.Equal(m.Data, osdOmapLiteral(t, "02000000 6200 02000000 63ff")) {
			t.Fatalf("range removal input bytes changed: %x", m.Data)
		}
		replyOps := []OSDOperation{tc.ops[0]}
		replyOps[0].Length = tc.length
		results := []int32{0}
		if tc.results != nil {
			results = tc.results
		}
		reply := osdXattrSyntheticReply(t, replyOps, [][]byte{nil}, results, tc.code, uint64(osd.FlagOnDisk|osd.FlagAck))
		reply.Transaction = m.Transaction
		_ = osdMapsWrite(peer, reply)
		got := <-done
		var unknown *OutcomeUnknownError
		var server *OSDError
		switch {
		case tc.invalid:
			if !errors.As(got.err, &unknown) {
				t.Fatal(tc.name, "contradictory range reply completed", got.err)
			}
		case tc.code < 0:
			if !errors.As(got.err, &server) || server.Code != tc.code || got.reply.Operations[0].Result != tc.code {
				t.Fatal(tc.name, "known range refusal changed", got.err)
			}
		default:
			if got.err != nil || got.reply.Operations[0].Length != tc.length {
				t.Fatal(tc.name, "valid range removal rejected", got.err)
			}
		}
	}
	if dials.Load() != 1 {
		t.Fatal("range removal validation reconnected")
	}
}

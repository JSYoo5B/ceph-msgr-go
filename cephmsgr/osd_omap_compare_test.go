package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func TestOSDOmapCompareInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	check := func(name string, want error, ops ...OSDOperation) {
		t.Helper()
		input := osdValidationRequest()
		input.Operations = ops
		for _, op := range ops {
			if op.Code&0x2000 != 0 { // native CEPH_OSD_OP_MODE_WR
				input.ModificationTime = OSDTime{Seconds: 1}
			}
		}
		before := c.osdNext.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		reply, err := o.Request(ctx, input)
		if !errors.Is(err, want) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s reached submission or lost cause: %+v %v", name, reply, err)
		}
		select {
		case m := <-peer.requests:
			t.Fatalf("%s sent object request %d", name, m.Transaction)
		default:
		}
	}
	valid := OSDOmapAssertion{Key: "k", Value: []byte{0x80}, Comparison: OSDOmapLess}
	compare := OSDOperation{Code: OSDCompareOmap, Omap: OSDOmapInput{Assertions: []OSDOmapAssertion{valid}}}
	for _, id := range []OSDOmapComparison{0, 2, 4, 6, -1} {
		op := compare
		op.Omap.Assertions = []OSDOmapAssertion{valid, {Key: "z", Comparison: id}}
		check("unsupported comparison identifier", ErrUnsupportedOSDOperation, op)
	}
	for _, edit := range []func(*OSDOperation){
		func(op *OSDOperation) { op.Omap.MaxOutputBytes = 1 },
		func(op *OSDOperation) { op.Omap.Keys = []string{} },
		func(op *OSDOperation) { op.Omap.Header = []byte{} },
		func(op *OSDOperation) { op.Data = []byte{1} },
		func(op *OSDOperation) {
			op.Omap.Assertions = []OSDOmapAssertion{valid, {Key: "k", Comparison: OSDOmapEqual}}
		},
	} {
		op := compare
		edit(&op)
		check("comparison variant or duplicate", osd.ErrRequest, op)
	}
	check("assertions on another opcode", osd.ErrRequest, OSDOperation{Code: OSDStat, Omap: OSDOmapInput{Assertions: []OSDOmapAssertion{}}})
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k"}}}}
	check("comparison after staged OMAP write", ErrUnsupportedOSDOperation, set, compare)
	check("comparison after staged DELETE", ErrUnsupportedOSDOperation, OSDOperation{Code: OSDDelete}, compare)
	if err := osdOmapEntryBound([]OSDOperation{{Code: OSDCompareOmap, Omap: OSDOmapInput{Assertions: make([]OSDOmapAssertion, 6)}}}, 64); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("impossible assertion count was copied before encoding", err)
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local OMAP comparison rejection failed/reconnected session", o.session.Err(), dials.Load())
	}
}

func TestOSDOmapCompareSentRepliesAndTerminalFaults(t *testing.T) {
	assertions := []OSDOmapAssertion{{Key: "z", Value: []byte("Z"), Comparison: OSDOmapEqual}, {Key: "\x00", Value: []byte{0x80}, Comparison: OSDOmapLess}}
	compare := OSDOperation{Code: OSDCompareOmap, Omap: OSDOmapInput{Assertions: assertions}}
	failOK := compare
	failOK.Flags = OSDFailOK
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k", Value: []byte("v")}}}}
	ondisk := uint64(osd.FlagOnDisk | osd.FlagAck)
	// Source-derived literals are synthetic inputs, not native encoder proof.
	for _, tc := range []struct {
		name    string
		ops     []OSDOperation
		input   string
		lengths []uint64
		outputs []string
		results []int32
		code    int32
		flags   uint64
		edit    func(*msgr.MessageData)
		invalid bool
	}{
		{name: "read-only comparison succeeds", ops: []OSDOperation{compare}, flags: ondisk, lengths: []uint64{32}, outputs: []string{""},
			input: "02000000 01000000 00 01000000 80 05000000 01000000 7a 01000000 5a 01000000"},
		{name: "compare-before-write commits", ops: []OSDOperation{compare, set}, flags: ondisk, lengths: []uint64{32, 14}, outputs: []string{"", ""}},
		{name: "failed assertion cancels the vector", ops: []OSDOperation{compare, set}, code: -125, flags: ondisk, results: []int32{-125, 0}, lengths: []uint64{32, 14}, outputs: []string{"", ""}},
		{name: "FAILOK assertion failure before committed write", ops: []OSDOperation{failOK, set}, flags: ondisk, results: []int32{-125, 0}, lengths: []uint64{32, 14}, outputs: []string{"", ""}},
		{name: "missing object is known ENOENT", ops: []OSDOperation{compare}, code: -2, flags: ondisk, results: []int32{-2}, lengths: []uint64{32}, outputs: []string{""}},
		{name: "ordinary failed comparison in successful vector", ops: []OSDOperation{compare, set}, flags: ondisk, results: []int32{-125, 0}, lengths: []uint64{32, 14}, outputs: []string{"", ""}, invalid: true},
		{name: "comparison returns data", ops: []OSDOperation{compare}, flags: ondisk, lengths: []uint64{32}, outputs: []string{"00"}, invalid: true},
		{name: "comparison reply changed input length", ops: []OSDOperation{compare}, flags: ondisk, lengths: []uint64{31}, outputs: []string{""}, invalid: true},
		{name: "comparison reply changed offset", ops: []OSDOperation{compare}, flags: ondisk, lengths: []uint64{32}, outputs: []string{""}, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[68:76], 1) }},
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
			input.ClientIncarnation, input.Operations = 67, tc.ops
			if len(tc.ops) > 1 {
				input.ModificationTime = OSDTime{Seconds: 1700000000, Nanoseconds: 9}
			}
			callerAssertions := append([]OSDOmapAssertion(nil), assertions...)
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			mode := uint64(osd.FlagRead)
			if len(tc.ops) > 1 {
				mode |= osd.FlagWrite
			}
			if request.Type != msgr.OSDOpMessage || binary.LittleEndian.Uint32(request.Front[8:12]) != uint32(mode|osd.FlagOnDisk|osd.FlagReturnVec) {
				t.Fatal("OMAP comparison request type/mode/completion flags differ")
			}
			if tc.input != "" && !bytes.Equal(request.Data, osdOmapLiteral(t, tc.input)) {
				t.Fatalf("source literal comparison bytes changed: %x", request.Data)
			}
			replyOps := make([]OSDOperation, len(tc.ops))
			outputs := make([][]byte, len(tc.ops))
			results := make([]int32, len(tc.ops))
			for i, op := range tc.ops {
				replyOps[i] = op
				replyOps[i].Length = tc.lengths[i]
				outputs[i] = osdOmapLiteral(t, tc.outputs[i])
				if tc.results != nil {
					results[i] = tc.results[i]
				}
			}
			m := osdXattrSyntheticReply(t, replyOps, outputs, results, tc.code, tc.flags)
			if tc.edit != nil {
				tc.edit(&m)
			}
			m.Transaction = request.Transaction
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("OMAP comparison validation left the request pending", ctx.Err())
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 67, MapEpoch: 11}
			if got.reply.Submission != want || !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
				t.Fatal("OMAP comparison identity or raw reply lost", got.reply, got.err)
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &server) {
					t.Fatal("contradictory OMAP comparison reply completed", got.err)
				}
			case tc.code < 0:
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("known OMAP comparison refusal became unknown", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid OMAP comparison reply rejected", got.err)
				}
			}
			if !tc.invalid {
				for i, op := range got.reply.Operations {
					if op.Code != tc.ops[i].Code || op.Result != results[i] || op.Length != tc.lengths[i] || len(op.Data) != 0 {
						t.Fatal("OMAP comparison per-op result rewritten", i, op)
					}
				}
			}
			if !reflect.DeepEqual(callerAssertions, assertions) {
				t.Fatal("canonical comparison encoding sorted caller-owned assertions")
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OMAP comparison peer cleanup stalled", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("OMAP comparison validation automatically reconnected")
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed an OMAP comparison vector", replay.Transaction)
			default:
			}
		})
	}
}

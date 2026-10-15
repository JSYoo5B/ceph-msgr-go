package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

func osdCallOp(mode OSDCallMode, method string, input []byte, budget uint32) OSDOperation {
	return OSDOperation{Code: OSDCall, Data: input, Class: OSDClassInput{Class: "hello", Method: method, Mode: mode, MaxOutputBytes: budget}}
}

// osdCallReplyOp places the packed cls union echo in the synthetic helper's
// extent fields: class_len, method_len, argc, indata_len and padding occupy
// the extent.offset bytes, and the OSD output length is extent.length.
func osdCallReplyOp(op OSDOperation, outputLength uint64) OSDOperation {
	op.Offset = uint64(len(op.Class.Class)) | uint64(len(op.Class.Method))<<8 | uint64(len(op.Data))<<24
	op.Length = outputLength
	return op
}

func TestOSDCallInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	check := func(name string, want error, mtime bool, ops ...OSDOperation) {
		t.Helper()
		input := osdValidationRequest()
		input.Operations = ops
		if mtime {
			input.ModificationTime = OSDTime{Seconds: 1}
		}
		before := c.osdNext.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		reply, err := o.Request(ctx, input)
		if !errors.Is(err, want) || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s reached submission or lost cause: %v", name, err)
		}
		select {
		case m := <-peer.requests:
			t.Fatalf("%s was transmitted as %d", name, m.Transaction)
		default:
		}
	}
	valid := osdCallOp(OSDCallRead, "say_hello", nil, 64)
	for _, edit := range []func(*OSDOperation){
		func(op *OSDOperation) { op.Class.Class = "" },
		func(op *OSDOperation) { op.Class.Method = strings.Repeat("m", 256) },
		func(op *OSDOperation) { op.Class.Class = "he\x00llo" },
		func(op *OSDOperation) { op.Class.Mode = 0 },
		func(op *OSDOperation) { op.Class.MaxOutputBytes = 0 },
		func(op *OSDOperation) { op.Length = 1 },
		func(op *OSDOperation) { op.Xattr.Name = "x" },
		func(op *OSDOperation) { op.Omap.Header = []byte{} },
	} {
		op := valid
		edit(&op)
		check("invalid CALL input", osd.ErrRequest, false, op)
	}
	failOK := valid
	failOK.Flags = OSDFailOK
	check("FAILOK CALL", ErrUnsupportedOSDOperation, false, failOK)
	check("read-mode CALL with mtime", osd.ErrRequest, true, valid)
	check("class variant on another opcode", osd.ErrRequest, false, OSDOperation{Code: OSDStat, Class: OSDClassInput{Class: "hello"}})
	write := OSDOperation{Code: OSDWrite, Length: 1, Data: []byte{1}}
	check("CALL after staged write", ErrUnsupportedOSDOperation, true, write, valid)
	check("read after CALL", ErrUnsupportedOSDOperation, false, valid, OSDOperation{Code: OSDStat})
	check("CALL after CALL", ErrUnsupportedOSDOperation, false, valid, valid)
	big := valid
	big.Data = make([]byte, c.options.MaxFrameSize)
	check("oversized CALL input", ErrLimitExceeded, false, big)
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local CALL rejection failed/reconnected the session")
	}
}

func TestOSDCallSentRepliesPreserveResultsAndOutput(t *testing.T) {
	sayHello := osdCallOp(OSDCallRead, "say_hello", []byte("Tester"), 64)
	writeReturn := osdCallOp(OSDCallWrite, "write_return_data", nil, 64)
	ondisk := uint64(osd.FlagOnDisk | osd.FlagAck)
	for _, tc := range []struct {
		name         string
		op           OSDOperation
		outputLength uint64
		output       []byte
		result, code int32
		flags        uint64
		invalid      bool
	}{
		{name: "read method output", op: sayHello, outputLength: 14, output: []byte("Hello, Tester!"), flags: ondisk},
		{name: "positive write method result and output", op: writeReturn, outputLength: 18, output: []byte("you might see this"), result: 42, code: 42, flags: ondisk},
		{name: "write output cleared by EOVERFLOW", op: writeReturn, outputLength: 520, result: -75, code: -75, flags: ondisk},
		{name: "pre-execution refusal", op: sayHello, code: -95, flags: ondisk},
		{name: "output above the local budget", op: osdCallOp(OSDCallRead, "say_hello", []byte("Tester"), 13), outputLength: 14, output: []byte("Hello, Tester!"), flags: ondisk, invalid: true},
		{name: "output length differs from data", op: sayHello, outputLength: 15, output: []byte("Hello, Tester!"), flags: ondisk, invalid: true},
		{name: "positive write without ONDISK", op: writeReturn, outputLength: 18, output: []byte("you might see this"), result: 42, code: 42, flags: uint64(osd.FlagAck), invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, peers, _ := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := osdValidationRequest()
			input.Operations = []OSDOperation{tc.op}
			if tc.op.Class.Mode == OSDCallWrite {
				input.ModificationTime = OSDTime{Seconds: 1700000000}
			}
			type response struct {
				reply OSDReply
				err   error
			}
			done := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); done <- response{reply, err} }()
			m := peer.next(t)
			mode := uint32(osd.FlagRead)
			if tc.op.Class.Mode == OSDCallWrite {
				mode = uint32(osd.FlagWrite)
			}
			if binary.LittleEndian.Uint32(m.Front[8:12]) != mode|uint32(osd.FlagOnDisk|osd.FlagReturnVec) || !bytes.Equal(m.Data, append([]byte("hello"+tc.op.Class.Method), tc.op.Data...)) {
				t.Fatalf("CALL request flags/data differ: %q", m.Data)
			}
			reply := osdXattrSyntheticReply(t, []OSDOperation{osdCallReplyOp(tc.op, tc.outputLength)}, [][]byte{tc.output}, []int32{tc.result}, tc.code, tc.flags)
			reply.Transaction = m.Transaction
			_ = osdMapsWrite(peer, reply)
			got := <-done
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) {
					t.Fatal("contradictory CALL reply completed", got.err)
				}
			case tc.code < 0:
				if !errors.As(got.err, &server) || server.Code != tc.code {
					t.Fatal("known CALL refusal changed", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid CALL reply rejected", got.err)
				}
			}
			if !tc.invalid {
				op := got.reply.Operations[0]
				if got.reply.Code != tc.code || op.Result != tc.result || !bytes.Equal(op.Data, tc.output) || op.Class.OutputLength != tc.outputLength ||
					op.Class.ClassLength != 5 || op.Class.MethodLength != uint8(len(tc.op.Class.Method)) || op.Class.InputLength != uint32(len(tc.op.Data)) {
					t.Fatalf("CALL result, output or union echo changed: %+v", op)
				}
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("CALL was resent", replay.Transaction)
			default:
			}
		})
	}
}

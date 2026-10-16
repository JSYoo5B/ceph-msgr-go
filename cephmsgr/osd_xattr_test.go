package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func osdXattrOp(code OSDOpcode) OSDOperation {
	op := OSDOperation{Code: code, Xattr: OSDXattrInput{Name: "attr"}}
	switch code {
	case OSDGetXattr:
		op.Xattr.MaxOutputBytes = 32
	case OSDGetXattrs:
		op.Xattr.Name, op.Xattr.MaxOutputBytes = "", 64
	case OSDCompareXattr:
		op.Xattr.Comparison, op.Data = OSDXattrGreater, []byte{0xff, 0, 10}
	case OSDSetXattr:
		op.Data = []byte{0xff, 0, 10}
	}
	return op
}

func TestOSDXattrInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	check := func(name string, ops []OSDOperation) {
		t.Helper()
		input := osdValidationRequest()
		input.Operations = ops
		before := c.osdNext.Load()
		reply, err := o.Request(context.Background(), input)
		if err == nil || reply.Submission != (OSDSubmission{}) || c.osdNext.Load() != before {
			t.Fatalf("%s: invalid input reached submission: %+v %v", name, reply, err)
		}
		select {
		case request := <-peer.requests:
			t.Fatalf("%s: rejected input sent object request %d", name, request.Transaction)
		default:
		}
	}
	for _, code := range []OSDOpcode{OSDGetXattr, OSDGetXattrs, OSDCompareXattr, OSDSetXattr, OSDRemoveXattr} {
		base := osdXattrOp(code)
		if code != OSDGetXattrs {
			for _, name := range []string{"", "\x00attr", "at\x00tr", "attr\x00"} {
				op := base
				op.Xattr.Name = name
				check(fmt.Sprintf("opcode %x name %q", code, name), []OSDOperation{op})
			}
		}
		for _, flags := range []OSDOperationFlags{OSDExclusiveCreate, 1 << 31} {
			op := base
			op.Flags = flags
			check("unsupported flags", []OSDOperation{op})
		}
		for _, field := range []string{"offset", "length", "version"} {
			op := base
			switch field {
			case "offset":
				op.Offset = 1
			case "length":
				op.Length = 1
			case "version":
				op.Version = 1
			}
			check("conflicting "+field, []OSDOperation{op})
		}
		if code != OSDCompareXattr {
			op := base
			op.Xattr.Comparison = OSDXattrEqual
			check("comparison on non-CMP", []OSDOperation{op})
		} else {
			for _, comparison := range []OSDXattrComparison{0, 7, 255} {
				op := base
				op.Xattr.Comparison = comparison
				check("unknown comparison", []OSDOperation{op})
			}
		}
		if code == OSDGetXattr || code == OSDGetXattrs {
			for _, budget := range []uint32{0, c.options.MaxFrameSize, ^uint32(0)} {
				op := base
				op.Xattr.MaxOutputBytes = budget
				check("missing or oversized output budget", []OSDOperation{op})
			}
			op := base
			op.Data = []byte{1}
			check("GET operand", []OSDOperation{op})
		} else {
			op := base
			op.Xattr.MaxOutputBytes = 1
			check("budget on mutation/comparison", []OSDOperation{op})
		}
	}
	all := osdXattrOp(OSDGetXattrs)
	all.Xattr.Name = "attr"
	check("GETALL named", []OSDOperation{all})
	rm := osdXattrOp(OSDRemoveXattr)
	rm.Data = []byte{1}
	check("RM operand", []OSDOperation{rm})
	read := OSDOperation{Code: OSDRead, Length: 1, Xattr: OSDXattrInput{Name: "attr"}}
	check("xattr union on extent opcode", []OSDOperation{read})
	get := osdXattrOp(OSDGetXattr)
	get.Xattr.MaxOutputBytes = c.options.MaxFrameSize / 2
	check("aggregate reply budget", []OSDOperation{get, get})
	for _, mutation := range []OSDOpcode{OSDSetXattr, OSDRemoveXattr} {
		for _, read := range []OSDOpcode{OSDGetXattr, OSDGetXattrs, OSDCompareXattr} {
			for _, flags := range []OSDOperationFlags{0, OSDFailOK} {
				op := osdXattrOp(mutation)
				op.Flags = flags
				check("staged xattr read across intervening operation", []OSDOperation{op, {Code: OSDStat}, osdXattrOp(read)})
			}
		}
	}
	for _, write := range []OSDOpcode{OSDWriteFull, OSDAppend, OSDWrite} {
		for _, read := range []OSDOpcode{OSDRead, OSDCompareExtent} {
			last := OSDOperation{Code: read, Length: 3}
			if read == OSDCompareExtent {
				last.Data = []byte{1, 2, 3}
			}
			check("full-write checksum guard across xattr", []OSDOperation{
				{Code: write, Length: 3, Data: []byte{1, 2, 3}}, osdXattrOp(OSDSetXattr), last,
			})
		}
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local rejection damaged or reconnected the established session", o.session.Err(), dials.Load())
	}
}

// Construct fault-injection replies within an independently checked native
// envelope. These synthetic unions/results are not native encoder proof or
// observed server behavior. Native fixture/interoperability tests are separate.
func osdXattrSyntheticReply(t *testing.T, ops []OSDOperation, outputs [][]byte, results []int32, aggregate int32, flags uint64) msgr.MessageData {
	t.Helper()
	base := osdValidationFixture(t, "reply-v6-success")
	e := wire.Encoder{Data: bytes.Clone(base.Front[:62])}
	binary.LittleEndian.PutUint64(e.Data[30:38], flags)
	binary.LittleEndian.PutUint32(e.Data[38:42], uint32(aggregate))
	binary.LittleEndian.PutUint32(e.Data[58:62], uint32(len(ops)))
	var data []byte
	for i, op := range ops {
		e.U16(uint16(op.Code))
		e.U32(uint32(op.Flags))
		switch op.Code {
		case OSDGetXattr, OSDGetXattrs, OSDCompareXattr, OSDSetXattr, OSDRemoveXattr:
			e.U32(uint32(len(op.Xattr.Name)))
			valueLength := len(op.Data)
			if op.Code == OSDGetXattr {
				valueLength = len(outputs[i])
			}
			e.U32(uint32(valueLength))
			e.U8(uint8(op.Xattr.Comparison))
			mode := uint8(0)
			if op.Code == OSDCompareXattr {
				mode = 1
			}
			e.U8(mode)
			e.Raw(make([]byte, 18))
		default:
			e.U64(op.Offset)
			e.U64(op.Length)
			e.U64(op.TruncateSize)
			e.U32(op.TruncateSequence)
		}
		e.U32(uint32(len(outputs[i])))
		data = append(data, outputs[i]...)
	}
	e.U32(0)
	for _, result := range results {
		e.U32(uint32(result))
	}
	e.Raw(base.Front[150:])
	return msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Front: e.Data, Data: data}
}

func TestOSDXattrSentVectorPreservesResultsAndFailsMalformedReplies(t *testing.T) {
	mapBytes := []byte{1, 0, 0, 0, 4, 0, 0, 0, '_', 0xff, 'a', 'b', 3, 0, 0, 0, 0, 0xff, 10}
	for _, tc := range []struct {
		name      string
		ops       []OSDOperation
		outputs   [][]byte
		results   []int32
		aggregate int32
		flags     uint64
		edit      func(*msgr.MessageData)
		invalid   bool
		wantLimit bool
	}{
		{name: "normalized binary GET", ops: []OSDOperation{osdXattrOp(OSDGetXattr)}, outputs: [][]byte{{0xff, 0, 10}}, results: []int32{0}},
		{name: "empty value remains distinct from removal", ops: []OSDOperation{osdXattrOp(OSDGetXattr)}, outputs: [][]byte{nil}, results: []int32{0}},
		{name: "opaque GETALL bytes", ops: []OSDOperation{osdXattrOp(OSDGetXattrs)}, outputs: [][]byte{mapBytes}, results: []int32{0}},
		{name: "positive comparison result", ops: []OSDOperation{osdXattrOp(OSDCompareXattr)}, outputs: [][]byte{nil}, results: []int32{1}, aggregate: 1},
		{name: "known negative compare refusal", ops: []OSDOperation{osdXattrOp(OSDCompareXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil, nil}, results: []int32{-125, 0}, aggregate: -125},
		{name: "FAILOK retains negative comparison", ops: []OSDOperation{func() OSDOperation { op := osdXattrOp(OSDCompareXattr); op.Flags = OSDFailOK; return op }(), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil, nil}, results: []int32{-125, 0}, flags: OSDReplyOnDisk},
		{name: "mixed GET and SET output ownership", ops: []OSDOperation{osdXattrOp(OSDGetXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{{0, 0xff}, nil}, results: []int32{0, 0}, flags: OSDReplyOnDisk},
		{name: "ordinary negative comparison in success", ops: []OSDOperation{osdXattrOp(OSDCompareXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil, nil}, results: []int32{-125, 0}, flags: OSDReplyOnDisk, invalid: true},
		{name: "mixed SET ACK only", ops: []OSDOperation{osdXattrOp(OSDGetXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{{0}, nil}, results: []int32{0, 0}, flags: OSDReplyAck, invalid: true},
		{name: "GET exceeds caller budget after sent mutation", ops: []OSDOperation{osdXattrOp(OSDGetXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{make([]byte, 33), nil}, results: []int32{0, 0}, flags: OSDReplyOnDisk, invalid: true, wantLimit: true},
		{name: "GET normalized length mismatch", ops: []OSDOperation{osdXattrOp(OSDGetXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{{1, 2}, nil}, results: []int32{0, 0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[72:76], 1) }},
		{name: "changed echoed name length", ops: []OSDOperation{osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil}, results: []int32{0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint32(m.Front[68:72], 5) }},
		{name: "SET returns unexpected output", ops: []OSDOperation{osdXattrOp(OSDSetXattr)}, outputs: [][]byte{{1}}, results: []int32{0}, flags: OSDReplyOnDisk, invalid: true},
		{name: "nonzero packed reserved byte", ops: []OSDOperation{osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil}, results: []int32{0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { m.Front[78] = 1 }},
		{name: "unsupported comparison mode", ops: []OSDOperation{osdXattrOp(OSDCompareXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{nil, nil}, results: []int32{1, 0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { m.Front[77] = 2 }},
		{name: "truncated mixed reply data", ops: []OSDOperation{osdXattrOp(OSDGetXattr), osdXattrOp(OSDSetXattr)}, outputs: [][]byte{{1, 2}, nil}, results: []int32{0, 0}, flags: OSDReplyOnDisk, invalid: true, edit: func(m *msgr.MessageData) { m.Data = m.Data[:1] }},
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
			input.ClientIncarnation, input.Operations = 9, tc.ops
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if request.Type != msgr.OSDOpMessage || request.Transaction == 0 {
				t.Fatal("xattr vector was not sent", request)
			}
			m := osdXattrSyntheticReply(t, tc.ops, tc.outputs, tc.results, tc.aggregate, tc.flags)
			if tc.edit != nil {
				tc.edit(&m)
			}
			m.Transaction = request.Transaction
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("xattr validation left request pending", ctx.Err())
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 9, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
				t.Fatal("local submission or raw partial reply lost", got.reply, got.err)
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &server) {
					t.Fatal("bad reply completed sent mutation", got.err)
				}
				if tc.wantLimit && !errors.Is(got.err, wire.ErrLimit) {
					t.Fatal("received budget failure lost limit cause", got.err)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("bad xattr reply did not fail OSD session", err)
				}
			case tc.aggregate < 0:
				if !errors.As(got.err, &server) || server.Code != tc.aggregate || errors.As(got.err, &unknown) {
					t.Fatal("known refusal became unknown", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid xattr result rejected", got.err)
				}
			}
			if !tc.invalid {
				if got.reply.Code != tc.aggregate || len(got.reply.Operations) != len(tc.ops) || o.session.Err() != nil {
					t.Fatal("valid vector results/session lost", got.reply, o.session.Err())
				}
				for i, result := range got.reply.Operations {
					if result.Code != tc.ops[i].Code || result.Result != tc.results[i] || !bytes.Equal(result.Data, tc.outputs[i]) || result.Offset != 0 || result.Length != 0 || result.Xattr.NameLength != uint32(len(tc.ops[i].Xattr.Name)) || result.Xattr.ComparisonOp != uint8(tc.ops[i].Xattr.Comparison) {
						t.Fatal("xattr union/output/result changed", result)
					}
					if result.Code == OSDGetXattr && result.Xattr.ValueLength != uint32(len(tc.outputs[i])) {
						t.Fatal("GET normalized output length lost", result)
					}
				}
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OSD cleanup stalled", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("xattr validation automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("xattr vector automatically replayed", replay.Transaction)
			default:
			}
		})
	}
}

// These payloads were independently produced by fresh native MOSDOpReply
// objects. Sending them through a synthetic peer checks the public conversion
// and validation path, not daemon execution or xattr interoperability.
func TestOSDXattrPublicResultsMatchFreshNativePayloads(t *testing.T) {
	name := "_raw\xff"
	value := []byte{0, 0xff, 'A', 10}
	get := OSDOperation{Code: OSDGetXattr, Xattr: OSDXattrInput{Name: name, MaxOutputBytes: 4}}
	all := OSDOperation{Code: OSDGetXattrs, Xattr: OSDXattrInput{MaxOutputBytes: 34}}
	cmp := OSDOperation{Code: OSDCompareXattr, Xattr: OSDXattrInput{Name: name, Comparison: OSDXattrEqual}, Data: value}
	for _, tc := range []struct {
		fixture string
		ops     []OSDOperation
		results []int32
		code    int32
	}{
		{fixture: "xattr-native-reply-v6-read-mixed", ops: []OSDOperation{get, all, cmp}, results: []int32{0, 0, 1}},
		{fixture: "xattr-native-reply-v6-cmp-false", ops: []OSDOperation{cmp}, results: []int32{-125}, code: -125},
		{fixture: "xattr-native-reply-v6-set-refused", ops: []OSDOperation{{Code: OSDSetXattr, Xattr: OSDXattrInput{Name: name}, Data: value}}, results: []int32{-27}, code: -27},
		{fixture: "xattr-native-reply-v6-rm-refused", ops: []OSDOperation{{Code: OSDRemoveXattr, Xattr: OSDXattrInput{Name: "_remove"}}}, results: []int32{-2}, code: -2},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			read := func(segment string) []byte {
				p, err := os.ReadFile("../internal/osd/testdata/" + tc.fixture + "." + segment + ".hex")
				if err != nil {
					t.Fatal(err)
				}
				p, err = hex.DecodeString(strings.TrimSpace(string(p)))
				if err != nil {
					t.Fatal(err)
				}
				return p
			}
			front, data := read("front"), read("data")
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
				Object: OSDObject{Pool: 7, Name: "xattr-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: tc.ops}
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Transaction: request.Transaction, Front: front, Data: data}
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("native payload left request pending", ctx.Err())
			}
			if tc.code < 0 {
				var server *OSDError
				if !errors.As(got.err, &server) || server.Code != tc.code {
					t.Fatal("native signed refusal lost", got.err)
				}
			} else if got.err != nil {
				t.Fatal(got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Code != tc.code || got.reply.Object != "xattr-codec" || got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || !bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) || len(got.reply.Operations) != len(tc.ops) {
				t.Fatal("native reply identity/raw bytes lost", got.reply)
			}
			for i, result := range got.reply.Operations {
				if result.Code != tc.ops[i].Code || result.Result != tc.results[i] || result.Xattr.NameLength != uint32(len(tc.ops[i].Xattr.Name)) || result.Offset != 0 || result.Length != 0 || result.TruncateSize != 0 || result.TruncateSequence != 0 {
					t.Fatal("native per-op code or xattr union changed", result)
				}
			}
			if tc.code == 0 {
				if got.reply.Flags&OSDReplyOnDisk != 0 || !bytes.Equal(got.reply.Operations[0].Data, value) || got.reply.Operations[0].Xattr.ValueLength != 4 || got.reply.Operations[2].Xattr.ComparisonOp != 1 || got.reply.Operations[2].Xattr.ComparisonMode != 1 {
					t.Fatal("native read-only/comparison result changed", got.reply)
				}
				wantMap := []byte{2, 0, 0, 0, 5, 0, 0, 0, '_', 'r', 'a', 'w', 0xff, 4, 0, 0, 0, 0, 0xff, 'A', 10, 5, 0, 0, 0, 'e', 'm', 'p', 't', 'y', 0, 0, 0, 0}
				if !bytes.Equal(got.reply.Operations[1].Data, wantMap) || got.reply.Operations[1].Xattr != (OSDXattrResult{}) {
					t.Fatal("native encoded map was rewritten", got.reply.Operations[1])
				}
			}
			if o.session.Err() != nil || dials.Load() != 1 {
				t.Fatal("valid native codec reply failed/reconnected session", o.session.Err(), dials.Load())
			}
		})
	}
}

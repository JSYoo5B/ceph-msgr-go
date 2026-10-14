package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func osdOmapOp(code OSDOpcode) OSDOperation {
	op := OSDOperation{Code: code, Omap: OSDOmapInput{MaxOutputBytes: 64}}
	switch code {
	case OSDGetOmapKeys:
		op.Omap.StartAfter, op.Omap.MaxEntries = "\x00", 2
	case OSDGetOmapValues:
		op.Omap.StartAfter, op.Omap.Prefix, op.Omap.MaxEntries = "a", "b", 2
	case OSDGetOmapValuesByKeys:
		op.Omap.Keys = []string{"missing", "\xff", "", "\x00"}
	}
	return op
}

func TestOSDOmapInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
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
			t.Fatalf("%s reached submission: %+v %v", name, reply, err)
		}
		select {
		case m := <-peer.requests:
			t.Fatalf("%s sent object request %d", name, m.Transaction)
		default:
		}
	}
	for _, code := range []OSDOpcode{OSDGetOmapKeys, OSDGetOmapValues, OSDGetOmapValuesByKeys} {
		base := osdOmapOp(code)
		for _, budget := range []uint32{0, 3, c.options.MaxFrameSize, math.MaxUint32} {
			op := base
			op.Omap.MaxOutputBytes = budget
			check("missing/oversized complete reply budget", []OSDOperation{op})
		}
		if code != OSDGetOmapValuesByKeys {
			op := base
			op.Omap.MaxOutputBytes = 4
			check("page budget omits required more byte", []OSDOperation{op})
			op = base
			op.Omap.Keys = []string{}
			check("even empty explicit keys conflict with page", []OSDOperation{op})
		}
		for _, flags := range []OSDOperationFlags{OSDExclusiveCreate, 1 << 31} {
			op := base
			op.Flags = flags
			check("unsupported OMAP flags", []OSDOperation{op})
		}
		for _, field := range []string{"offset", "length", "version", "data", "xattr"} {
			op := base
			switch field {
			case "offset":
				op.Offset = 1
			case "length":
				op.Length = 1
			case "version":
				op.Version = 1
			case "data":
				op.Data = []byte{1}
			case "xattr":
				op.Xattr.Name = "attr"
			}
			check("conflicting "+field+" union", []OSDOperation{op})
		}
	}
	keys := osdOmapOp(OSDGetOmapKeys)
	keys.Omap.Prefix = "prefix"
	check("prefix on keys-only page", []OSDOperation{keys})
	for _, field := range []string{"cursor", "prefix", "count", "duplicates"} {
		op := osdOmapOp(OSDGetOmapValuesByKeys)
		switch field {
		case "cursor":
			op.Omap.StartAfter = "a"
		case "prefix":
			op.Omap.Prefix = "a"
		case "count":
			op.Omap.MaxEntries = 1
		case "duplicates":
			op.Omap.Keys = []string{"a", "\x00", "a"}
		}
		check("by-key invalid "+field, []OSDOperation{op})
	}
	large := strings.Repeat("x", int(c.options.MaxFrameSize))
	keys = osdOmapOp(OSDGetOmapKeys)
	keys.Omap.StartAfter = large
	check("large cursor", []OSDOperation{keys})
	values := osdOmapOp(OSDGetOmapValues)
	values.Omap.Prefix = large
	check("large prefix", []OSDOperation{values})
	bykeys := osdOmapOp(OSDGetOmapValuesByKeys)
	bykeys.Omap.Keys = []string{large}
	check("large explicit key", []OSDOperation{bykeys})
	values = osdOmapOp(OSDGetOmapValues)
	values.Omap.MaxOutputBytes = c.options.MaxFrameSize / 2
	check("aggregate vector output budget", []OSDOperation{values, values})
	for _, code := range []OSDOpcode{OSDRead, OSDStat, OSDGetXattrs, OSDGetOmapHeader, 0x1219, OSDSetOmapValues, OSDSetOmapHeader, OSDClearOmap, 0x222c} {
		op := OSDOperation{Code: code, Omap: OSDOmapInput{Keys: []string{}}}
		if code == OSDRead {
			op.Length = 1
		}
		if code == OSDGetOmapHeader {
			op.Omap.MaxOutputBytes = 8 // only the Keys variant is invalid
		}
		if code == OSDGetXattrs {
			op.Xattr.MaxOutputBytes = 4
		}
		check(fmt.Sprintf("wrong union or unsupported compare/range removal %x", code), []OSDOperation{op})
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local OMAP rejection failed/reconnected session", o.session.Err(), dials.Load())
	}
}

func osdOmapLiteral(t *testing.T, literal string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.Join(strings.Fields(literal), ""))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestOSDOmapSentReplyCollectionsAndTerminalFaults(t *testing.T) {
	keys, values, bykeys := osdOmapOp(OSDGetOmapKeys), osdOmapOp(OSDGetOmapValues), osdOmapOp(OSDGetOmapValuesByKeys)
	// Explicit source-derived wire literals are synthetic fault inputs, not
	// native encoder proof or observed server behavior. Values remain opaque.
	for _, tc := range []struct {
		name           string
		op             OSDOperation
		input, output  string
		code, result   int32
		edit           func(*msgr.MessageData)
		invalid, limit bool
	}{
		{name: "binary keys and more retained", op: keys, input: "01000000 00 0200000000000000", output: "02000000 02000000 6100 01000000 ff 01"},
		{name: "filtered binary map and empty value", op: values, input: "01000000 61 0200000000000000 01000000 62", output: "02000000 01000000 62 00000000 02000000 6200 02000000 00ff 00"},
		{name: "bykeys omit missing and preserve empty NUL FF keys", op: bykeys, input: "04000000 00000000 01000000 00 07000000 6d697373696e67 01000000 ff", output: "03000000 00000000 00000000 01000000 00 02000000 00ff 01000000 ff 00000000"},
		{name: "zero count page retains true more", op: OSDOperation{Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxOutputBytes: 5}}, input: "00000000 0000000000000000", output: "00000000 01"},
		{name: "empty explicit key set", op: OSDOperation{Code: OSDGetOmapValuesByKeys, Omap: OSDOmapInput{Keys: []string{}, MaxOutputBytes: 4}}, input: "00000000", output: "00000000"},
		{name: "uint64 count retained bounded empty page", op: OSDOperation{Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxEntries: math.MaxUint64, MaxOutputBytes: 5}}, input: "00000000 ffffffffffffffff", output: "00000000 00"},
		{name: "known ENOENT does not require a collection", op: keys, input: "01000000 00 0200000000000000", code: -2},
		{name: "failed FAILOK output remains negative", op: func() OSDOperation { op := keys; op.Flags = OSDFailOK; return op }(), input: "01000000 00 0200000000000000", result: -61},
		{name: "count exceeds requested page", op: keys, output: "03000000 01000000 61 01000000 62 01000000 63 00", invalid: true},
		{name: "malformed count", op: keys, output: "ffffffff 00", invalid: true},
		{name: "missing Tentacle more suffix", op: keys, output: "00000000", invalid: true},
		{name: "invalid more bool", op: keys, output: "00000000 02", invalid: true},
		{name: "trailing page bytes", op: keys, output: "00000000 00 00", invalid: true},
		{name: "duplicate returned keys", op: keys, output: "02000000 01000000 61 01000000 61 00", invalid: true},
		{name: "descending returned keys", op: keys, output: "02000000 01000000 62 01000000 61 00", invalid: true},
		{name: "key at cursor is excluded", op: keys, output: "01000000 01000000 00 00", invalid: true},
		{name: "key outside byte prefix", op: values, output: "01000000 01000000 63 00000000 00", invalid: true},
		{name: "truncated value length", op: values, output: "01000000 01000000 62 ffffffff 00", invalid: true},
		{name: "bykeys unrequested member", op: bykeys, output: "01000000 01000000 61 00000000", invalid: true},
		{name: "bykeys invented more suffix", op: bykeys, output: "00000000 00", invalid: true},
		{name: "received output exceeds caller budget", op: OSDOperation{Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxEntries: 1, MaxOutputBytes: 5}}, output: "01000000 01000000 61 00", invalid: true, limit: true},
		{name: "reply changed encoded input length", op: keys, output: "00000000 00", invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[76:84], 0) }},
		{name: "reply changed offset", op: keys, output: "00000000 00", invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[68:76], 1) }},
		{name: "reply changed truncate union", op: keys, output: "00000000 00", invalid: true, edit: func(m *msgr.MessageData) { m.Front[95] = 1 }},
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
			input.ClientIncarnation, input.Operations = 59, []OSDOperation{tc.op}
			callerKeys := append([]string{}, tc.op.Omap.Keys...)
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if request.Type != msgr.OSDOpMessage || request.Transaction == 0 {
				t.Fatal("OMAP page did not reach object submission")
			}
			if tc.input != "" && !bytes.Equal(request.Data, osdOmapLiteral(t, tc.input)) {
				t.Fatalf("source literal request bytes changed: %x", request.Data)
			}
			// Reuse only the synthetic native-envelope helper; this explicit
			// reply union length echoes the received encoded input byte count.
			replyOp := tc.op
			replyOp.Length = uint64(len(request.Data))
			data := osdOmapLiteral(t, tc.output)
			m := osdXattrSyntheticReply(t, []OSDOperation{replyOp}, [][]byte{data}, []int32{tc.result}, tc.code, 0)
			if tc.edit != nil {
				tc.edit(&m)
			}
			m.Transaction = request.Transaction
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("OMAP validation left submitted page pending", ctx.Err())
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 59, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
				t.Fatal("OMAP local identity or raw partial reply lost", got.reply, got.err)
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &server) {
					t.Fatal("malformed collection completed a page", got.err)
				}
				if tc.limit && !errors.Is(got.err, wire.ErrLimit) {
					t.Fatal("received OMAP budget cause lost", got.err)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("invalid collection did not terminate OSD session", err)
				}
			case tc.code < 0:
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("known page refusal became unknown", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid raw page or FAILOK result rejected", got.err)
				}
			}
			if !tc.invalid {
				if o.session.Err() != nil || len(got.reply.Operations) != 1 || got.reply.Code != tc.code {
					t.Fatal("valid collection failed session or lost aggregate", got.reply, o.session.Err())
				}
				op := got.reply.Operations[0]
				if op.Code != tc.op.Code || op.Result != tc.result || op.Offset != 0 || op.Length != uint64(len(request.Data)) || op.Xattr != (OSDXattrResult{}) || op.PayloadLength != uint32(len(data)) || !bytes.Equal(op.Data, data) {
					t.Fatal("OMAP opaque data/extent/signed result rewritten", op)
				}
			}
			if !reflect.DeepEqual(callerKeys, append([]string{}, tc.op.Omap.Keys...)) {
				t.Fatal("canonical input encoding sorted caller-owned key slice")
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OMAP mock cleanup stalled", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("collection validation automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed or automatically paginated raw page", replay.Transaction)
			default:
			}
		})
	}
}

// Fresh native SDK objects encoded these payloads independently of Go. The
// synthetic peer verifies public correlation/conversion and raw collections;
// it does not establish live filtering, pagination, storage or authentication.
func TestOSDOmapPublicResultsMatchFreshNativePayloads(t *testing.T) {
	cursor, prefix := "a\x00\xff", "b\x00\x80"
	for _, tc := range []struct {
		fixture            string
		ops                []OSDOperation
		lengths            []uint64
		outputs            []string
		matchNativeRequest bool
	}{
		{fixture: "omap-native-reply-v6-read-mixed", matchNativeRequest: true,
			ops: []OSDOperation{
				{Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxOutputBytes: 5}},
				{Code: OSDGetOmapKeys, Omap: OSDOmapInput{StartAfter: cursor, MaxEntries: 2, MaxOutputBytes: 21}},
				{Code: OSDGetOmapValues, Omap: OSDOmapInput{StartAfter: cursor, Prefix: prefix, MaxEntries: 2, MaxOutputBytes: 21}},
				{Code: OSDGetOmapValuesByKeys, Omap: OSDOmapInput{Keys: []string{"z", "b\x80", "", cursor}, MaxOutputBytes: 29}},
			}, lengths: []uint64{12, 15, 22, 26}, outputs: []string{
				"00000000 01", "02000000 04000000 62008078 04000000 62008079 01",
				"01000000 04000000 62008078 04000000 00ff410a 01",
				"02000000 03000000 6100ff 00000000 02000000 6280 04000000 00ff410a",
			}},
		{fixture: "omap-native-reply-v6-keys-final", ops: []OSDOperation{{Code: OSDGetOmapKeys, Omap: OSDOmapInput{MaxEntries: 2, MaxOutputBytes: 18}}}, lengths: []uint64{12}, outputs: []string{"02000000 03000000 6100ff 02000000 6280 00"}},
		{fixture: "omap-native-reply-v6-vals-empty", ops: []OSDOperation{{Code: OSDGetOmapValues, Omap: OSDOmapInput{MaxEntries: 1, MaxOutputBytes: 5}}}, lengths: []uint64{16}, outputs: []string{"00000000 00"}},
		{fixture: "omap-native-reply-v6-bykeys-empty-value", ops: []OSDOperation{{Code: OSDGetOmapValuesByKeys, Omap: OSDOmapInput{Keys: []string{cursor}, MaxOutputBytes: 15}}}, lengths: []uint64{11}, outputs: []string{"01000000 03000000 6100ff 00000000"}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			readFixture := func(name, segment string) []byte {
				p, err := os.ReadFile("../internal/osd/testdata/" + name + "." + segment + ".hex")
				if err != nil {
					t.Fatal(err)
				}
				return osdOmapLiteral(t, string(p))
			}
			front, data := readFixture(tc.fixture, "front"), readFixture(tc.fixture, "data")
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
				Object: OSDObject{Pool: 7, Name: "omap-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: tc.ops}
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if tc.matchNativeRequest {
				if !bytes.Equal(request.Front, readFixture("omap-native-request-v6-read-all", "front")) || !bytes.Equal(request.Data, readFixture("omap-native-request-v6-read-all", "data")) {
					t.Fatal("public OMAP request differs from fresh native front/data")
				}
			}
			m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Transaction: request.Transaction, Front: front, Data: data}
			_ = osdMapsWrite(peer, m)
			var got response
			select {
			case got = <-finished:
			case <-ctx.Done():
				t.Fatal("fresh native payload left public request pending", ctx.Err())
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.GlobalID != 42 || got.reply.Code != 0 || got.reply.Object != input.Object.Name || got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || got.reply.Flags&OSDReplyAck == 0 || got.reply.Flags&OSDReplyOnDisk != 0 || !bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) || len(got.reply.Operations) != len(tc.ops) {
				t.Fatal("native OMAP identity, completion flags or raw payload changed", got.reply)
			}
			var joined []byte
			for i, result := range got.reply.Operations {
				output := osdOmapLiteral(t, tc.outputs[i])
				if result.Code != tc.ops[i].Code || result.Result != 0 || result.Offset != 0 || result.Length != tc.lengths[i] || result.TruncateSize != 0 || result.TruncateSequence != 0 || result.Xattr != (OSDXattrResult{}) || result.PayloadLength != uint32(len(output)) || !bytes.Equal(result.Data, output) {
					t.Fatal("native raw collection or echoed input union changed", result)
				}
				joined = append(joined, output...)
			}
			if !bytes.Equal(joined, data) || o.session.Err() != nil {
				t.Fatal("native ordered output boundary or valid session lost", o.session.Err())
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("native codec peer cleanup stalled", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("read-only codec reply automatically reconnected")
			}
			select {
			case page := <-peer.requests:
				t.Fatal("native more=true automatically requested another page", page.Transaction)
			default:
			}
		})
	}
}

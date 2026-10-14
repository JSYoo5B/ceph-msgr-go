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
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestOSDOmapWriteInvalidInputsNeverAllocateTIDOrSend(t *testing.T) {
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
				input.ModificationTime = OSDTime{Seconds: 1} // read-only mtime is itself invalid
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
	header := OSDOperation{Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 8}}
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "a"}}}}
	setHeader := OSDOperation{Code: OSDSetOmapHeader, Omap: OSDOmapInput{Header: []byte{1}}}
	remove := OSDOperation{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"a"}}}
	clear := OSDOperation{Code: OSDClearOmap}
	zeroBudget, frameBudget := header, header
	zeroBudget.Omap.MaxOutputBytes, frameBudget.Omap.MaxOutputBytes = 0, c.options.MaxFrameSize+1
	check("header zero budget", osd.ErrRequest, zeroBudget)
	check("header frame budget", ErrLimitExceeded, frameBudget)
	for _, edit := range []func(*OSDOperation){
		func(op *OSDOperation) { op.Omap.StartAfter = "a" },
		func(op *OSDOperation) { op.Omap.Keys = []string{} },
		func(op *OSDOperation) { op.Omap.Entries = []OSDOmapEntry{} },
		func(op *OSDOperation) { op.Omap.Header = []byte{} },
		func(op *OSDOperation) { op.Data = []byte{1} },
		func(op *OSDOperation) { op.Length = 1 },
	} {
		op := header
		edit(&op)
		check("header variant", osd.ErrRequest, op)
	}
	for _, write := range []OSDOperation{set, setHeader, remove, clear} {
		op := write
		op.Omap.MaxOutputBytes = 1
		check("mutation output budget", osd.ErrRequest, op)
		op = write
		op.Flags = OSDExclusiveCreate
		check("mutation exclusive flag", ErrUnsupportedOSDOperation, op)
		op = write
		op.Offset = 1
		check("mutation caller extent", osd.ErrRequest, op)
		op = write
		op.Xattr.Name = "attr"
		check("mutation xattr union", osd.ErrRequest, op)
	}
	check("set duplicate keys", osd.ErrRequest, OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "\x00"}, {Key: "\x00", Value: []byte{1}}}}})
	check("remove duplicate keys", osd.ErrRequest, OSDOperation{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"", ""}}})
	check("set keys variant", osd.ErrRequest, OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Keys: []string{}}})
	check("set header variant", osd.ErrRequest, OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Header: []byte{}}})
	check("remove entries variant", osd.ErrRequest, OSDOperation{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Entries: []OSDOmapEntry{}}})
	check("clear header variant", osd.ErrRequest, OSDOperation{Code: OSDClearOmap, Omap: OSDOmapInput{Header: []byte{}}})
	check("non-OMAP entries variant", osd.ErrRequest, OSDOperation{Code: OSDStat, Omap: OSDOmapInput{Entries: []OSDOmapEntry{}}})
	check("non-OMAP header variant", osd.ErrRequest, OSDOperation{Code: OSDStat, Omap: OSDOmapInput{Header: []byte{}}})
	check("large header", ErrLimitExceeded, OSDOperation{Code: OSDSetOmapHeader, Omap: OSDOmapInput{Header: make([]byte, c.options.MaxFrameSize)}})
	check("large value", ErrLimitExceeded, OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k", Value: make([]byte, c.options.MaxFrameSize)}}}})
	failOK := clear
	failOK.Flags = OSDFailOK
	for _, ops := range [][]OSDOperation{{set, header}, {setHeader, header}, {failOK, osdOmapOp(OSDGetOmapKeys)}, {remove, {Code: OSDStat}, osdOmapOp(OSDGetOmapValuesByKeys)}} {
		check("OMAP read after staged OMAP write", ErrUnsupportedOSDOperation, ops...)
	}
	if o.session.Err() != nil || dials.Load() != 1 {
		t.Fatal("local OMAP mutation rejection failed/reconnected session", o.session.Err(), dials.Load())
	}
}

func TestOSDOmapEntryPreBoundRejectsImpossibleMaps(t *testing.T) {
	entries := make([]OSDOmapEntry, 9)
	if err := osdOmapEntryBound([]OSDOperation{{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: entries[:8]}}}, 64); err != nil {
		t.Fatal("encodable entry count rejected by the pre-bound", err)
	}
	if err := osdOmapEntryBound([]OSDOperation{{Code: OSDStat}, {Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: entries}}}, 64); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("impossible entry count was copied before encoding", err)
	}
	shared := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: entries[:5]}}
	if err := osdOmapEntryBound([]OSDOperation{shared, shared}, 64); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal("per-operation bound let a vector copy an impossible request-wide entry count", err)
	}
}

func TestOSDOmapWriteSentRepliesAndTerminalFaults(t *testing.T) {
	header := OSDOperation{Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 8}}
	entries := []OSDOmapEntry{{Key: "z", Value: []byte("Z")}, {Key: ""}, {Key: "\x00", Value: []byte{0xff}}}
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: entries}}
	setHeader := OSDOperation{Code: OSDSetOmapHeader, Omap: OSDOmapInput{Header: []byte{0, 0xff}}}
	remove := OSDOperation{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"b", "\x00"}}}
	clear := OSDOperation{Code: OSDClearOmap}
	failOKClear := clear
	failOKClear.Flags = OSDFailOK
	ondisk := uint64(osd.FlagOnDisk)
	// Explicit source-derived wire literals are synthetic fault inputs, not
	// native encoder proof or observed server behavior.
	for _, tc := range []struct {
		name           string
		ops            []OSDOperation
		input          string
		lengths        []uint64
		outputs        []string
		results        []int32
		code           int32
		flags          uint64
		edit           func(*msgr.MessageData)
		invalid, limit bool
	}{
		{name: "header then all mutations commit with ONDISK", ops: []OSDOperation{header, set, setHeader, remove, clear}, flags: ondisk,
			input:   "03000000 00000000 00000000 01000000 00 01000000 ff 01000000 7a 01000000 5a 00ff 02000000 01000000 00 01000000 62",
			lengths: []uint64{0, 32, 2, 14, 0}, outputs: []string{"00ff48", "", "", "", ""}},
		{name: "empty header read", ops: []OSDOperation{header}, flags: uint64(osd.FlagAck), lengths: []uint64{0}, outputs: []string{""}},
		{name: "empty set map remains a mutation", ops: []OSDOperation{{Code: OSDSetOmapValues}}, flags: ondisk, input: "00000000", lengths: []uint64{4}, outputs: []string{""}},
		{name: "known ENOENT removal", ops: []OSDOperation{remove}, code: -2, results: []int32{-2}, lengths: []uint64{14}, outputs: []string{""}},
		{name: "FAILOK clear refusal before committed set", ops: []OSDOperation{failOKClear, set}, flags: ondisk, results: []int32{-2, 0}, lengths: []uint64{0, 32}, outputs: []string{"", ""}},
		{name: "successful mutation lacks ONDISK", ops: []OSDOperation{set}, flags: uint64(osd.FlagAck), lengths: []uint64{32}, outputs: []string{""}, invalid: true},
		{name: "ordinary failed mutation in successful vector", ops: []OSDOperation{setHeader}, flags: ondisk, results: []int32{-22}, lengths: []uint64{2}, outputs: []string{""}, invalid: true},
		{name: "mutation returns data", ops: []OSDOperation{setHeader}, flags: ondisk, lengths: []uint64{2}, outputs: []string{"00"}, invalid: true},
		{name: "mutation reply changed input length", ops: []OSDOperation{setHeader}, flags: ondisk, lengths: []uint64{3}, outputs: []string{""}, invalid: true},
		{name: "clear reply invents input length", ops: []OSDOperation{clear}, flags: ondisk, lengths: []uint64{1}, outputs: []string{""}, invalid: true},
		{name: "header reply changed offset", ops: []OSDOperation{header}, lengths: []uint64{0}, outputs: []string{""}, invalid: true, edit: func(m *msgr.MessageData) { binary.LittleEndian.PutUint64(m.Front[68:76], 1) }},
		{name: "header exceeds local budget", ops: []OSDOperation{{Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 2}}}, lengths: []uint64{0}, outputs: []string{"000102"}, invalid: true, limit: true},
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
			input.ClientIncarnation, input.ModificationTime, input.Operations = 61, OSDTime{Seconds: 1700000000, Nanoseconds: 7}, tc.ops
			write := false
			for _, op := range tc.ops {
				write = write || op.Code != OSDGetOmapHeader
			}
			if !write {
				input.ModificationTime = OSDTime{}
			}
			callerEntries := append([]OSDOmapEntry(nil), entries...)
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if request.Type != msgr.OSDOpMessage || request.Transaction == 0 {
				t.Fatal("OMAP mutation did not reach object submission")
			}
			mode := uint64(osd.FlagRead)
			if write {
				mode = osd.FlagWrite
				if tc.ops[0].Code == OSDGetOmapHeader {
					mode |= osd.FlagRead
				}
			}
			if binary.LittleEndian.Uint32(request.Front[8:12]) != uint32(mode|osd.FlagOnDisk|osd.FlagReturnVec) {
				t.Fatal("OMAP mutation request mode/completion flags differ")
			}
			if tc.input != "" && !bytes.Equal(request.Data, osdOmapLiteral(t, tc.input)) {
				t.Fatalf("source literal mutation bytes changed: %x", request.Data)
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
				t.Fatal("OMAP mutation validation left the request pending", ctx.Err())
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 61, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || !bytes.Equal(got.reply.RawFront, m.Front) || !bytes.Equal(got.reply.RawData, m.Data) {
				t.Fatal("OMAP mutation identity or raw partial reply lost", got.reply, got.err)
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			switch {
			case tc.invalid:
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, msgr.ErrFrame) || errors.As(got.err, &server) {
					t.Fatal("contradictory OMAP mutation reply completed", got.err)
				}
				if tc.limit && !errors.Is(got.err, wire.ErrLimit) {
					t.Fatal("received OMAP header budget cause lost", got.err)
				}
				if _, err := o.NextMap(ctx); !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("invalid OMAP mutation reply did not terminate OSD session", err)
				}
			case tc.code < 0:
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("known OMAP mutation refusal became unknown", got.err)
				}
			default:
				if got.err != nil {
					t.Fatal("valid OMAP header/mutation reply rejected", got.err)
				}
			}
			if !tc.invalid {
				if o.session.Err() != nil || len(got.reply.Operations) != len(tc.ops) || got.reply.Code != tc.code {
					t.Fatal("valid OMAP reply failed session or lost aggregate", got.reply, o.session.Err())
				}
				for i, op := range got.reply.Operations {
					if op.Code != tc.ops[i].Code || op.Result != results[i] || op.Offset != 0 || op.Length != tc.lengths[i] || op.Xattr != (OSDXattrResult{}) || !bytes.Equal(op.Data, outputs[i]) {
						t.Fatal("OMAP raw header/mutation result rewritten", i, op)
					}
				}
			}
			if !reflect.DeepEqual(callerEntries, entries) {
				t.Fatal("canonical mutation encoding sorted caller-owned entries")
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.done:
			case <-ctx.Done():
				t.Fatal("OMAP mutation peer cleanup stalled", ctx.Err())
			}
			if dials.Load() != 1 {
				t.Fatal("OMAP mutation validation automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed an OMAP mutation", replay.Transaction)
			default:
			}
		})
	}
}

func TestOSDOmapMutationUnknownOutcomeRetainsSubmission(t *testing.T) {
	c, peers, _ := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	for _, op := range []OSDOperation{
		{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k", Value: []byte{1}}}}},
		{Code: OSDSetOmapHeader, Omap: OSDOmapInput{Header: []byte{2}}},
		{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"k"}}},
		{Code: OSDClearOmap},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		type result struct {
			reply OSDReply
			err   error
		}
		finished := make(chan result, 1)
		r := osdTestRequest()
		r.ClientIncarnation = 13
		r.Operations = []OSDOperation{op}
		go func() { reply, err := o.Request(ctx, r); finished <- result{reply, err} }()
		m := peer.next(t)
		cancel()
		got := <-finished
		var unknown *OutcomeUnknownError
		if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != m.Transaction || got.reply.Submission.ClientIncarnation != 13 || got.reply.Transaction != 0 || len(got.reply.RawFront) != 0 {
			t.Fatalf("OMAP mutation submission/reply distinction lost: %+v %v", got.reply, got.err)
		}
	}
	if err := o.session.Err(); err != nil {
		t.Fatal("local cancellation failed the shared OSD session", err)
	}
	o.Close()
	select {
	case replay := <-peer.requests:
		t.Fatal("uncertain OMAP mutation replayed", replay.Transaction)
	default:
	}
}

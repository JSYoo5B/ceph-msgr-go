package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/osd"
)

// Fresh native SDK objects encoded these payloads independently of Go. The
// synthetic peer verifies public submission, ONDISK acceptance, FAILOK and
// known-refusal conversion. It does not establish live OMAP comparison,
// execution, staging, durability or authentication.
func TestOSDOmapComparePublicResultsMatchFreshNativePayloads(t *testing.T) {
	// Deliberately unsorted; the native request received a std::map.
	compare := OSDOperation{Code: OSDCompareOmap, Omap: OSDOmapInput{Assertions: []OSDOmapAssertion{
		{Key: "b\x80", Value: []byte("\x00\xffA\n"), Comparison: OSDOmapGreater},
		{Key: "z", Value: []byte("Z"), Comparison: OSDOmapEqual},
		{Key: "\x00", Value: []byte{0x80}, Comparison: OSDOmapLess},
		{Key: "", Comparison: OSDOmapEqual},
	}}}
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k", Value: []byte("v")}}}}
	failOKCompare := OSDOperation{Code: OSDCompareOmap, Flags: OSDFailOK, Omap: OSDOmapInput{Assertions: []OSDOmapAssertion{{Key: "k", Value: []byte("x"), Comparison: OSDOmapEqual}}}}
	missing := OSDOperation{Code: OSDCompareOmap, Omap: OSDOmapInput{Assertions: []OSDOmapAssertion{{Key: "k", Comparison: OSDOmapEqual}}}}
	read, write := uint64(osd.FlagRead), uint64(osd.FlagWrite)
	returnVec := uint64(osd.FlagReturnVec)
	for _, tc := range []struct {
		fixture            string
		ops                []OSDOperation
		flags              uint64
		code               int32
		results            []int32
		lengths            []uint64
		matchNativeRequest bool
	}{
		{fixture: "omap-cmp-native-reply-v6-success", matchNativeRequest: true, ops: []OSDOperation{compare, set},
			flags: OSDReplyOnDisk | read | write | returnVec, results: []int32{0, 0}, lengths: []uint64{62, 14}},
		{fixture: "omap-cmp-native-reply-v6-canceled", ops: []OSDOperation{compare, set},
			flags: OSDReplyAck | OSDReplyOnDisk | read | write | returnVec, code: -125, results: []int32{-125, 0}, lengths: []uint64{62, 14}},
		{fixture: "omap-cmp-native-reply-v6-failok", ops: []OSDOperation{failOKCompare, set},
			flags: OSDReplyOnDisk | read | write | returnVec, results: []int32{-125, 0}, lengths: []uint64{18, 14}},
		{fixture: "omap-cmp-native-reply-v6-enoent", ops: []OSDOperation{missing},
			flags: OSDReplyAck | OSDReplyOnDisk | read | returnVec, code: -2, results: []int32{-2}, lengths: []uint64{17}},
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
			if len(data) != 0 {
				t.Fatal("native OMAP comparison reply unexpectedly carries data")
			}
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
				Object: OSDObject{Pool: 7, Name: "omap-cmp-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: tc.ops}
			if tc.flags&write != 0 {
				input.ModificationTime = OSDTime{Seconds: 1700000000, Nanoseconds: 5}
			}
			type response struct {
				reply OSDReply
				err   error
			}
			finished := make(chan response, 1)
			go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
			request := peer.next(t)
			if tc.matchNativeRequest {
				if !bytes.Equal(request.Front, readFixture("omap-cmp-native-request-v6-all", "front")) || !bytes.Equal(request.Data, readFixture("omap-cmp-native-request-v6-all", "data")) {
					t.Fatal("public OMAP comparison request differs from fresh native front/data")
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
			var unknown *OutcomeUnknownError
			var server *OSDError
			if tc.code < 0 {
				// A known server refusal is not an uncertain outcome.
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("native known OMAP comparison refusal did not remain a server result", got.err)
				}
			} else if got.err != nil {
				t.Fatal("native OMAP comparison reply rejected", got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.GlobalID != 42 || got.reply.Code != tc.code || got.reply.Object != input.Object.Name ||
				got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || got.reply.Flags != tc.flags || got.reply.MapEpoch != 11 || got.reply.UserVersion != 19 ||
				!bytes.Equal(got.reply.RawFront, front) || len(got.reply.RawData) != 0 || len(got.reply.Operations) != len(tc.ops) {
				t.Fatal("native OMAP comparison identity, completion flags or raw payload changed", got.reply)
			}
			for i, result := range got.reply.Operations {
				if result.Code != tc.ops[i].Code || result.Flags != uint32(tc.ops[i].Flags) || result.Result != tc.results[i] || result.Offset != 0 || result.Length != tc.lengths[i] ||
					result.TruncateSize != 0 || result.TruncateSequence != 0 || result.Xattr != (OSDXattrResult{}) || result.PayloadLength != 0 || len(result.Data) != 0 {
					t.Fatal("native per-op result or echoed comparison input union changed", i, result)
				}
			}
			if o.session.Err() != nil {
				t.Fatal("native OMAP comparison reply invalidated the session", o.session.Err())
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
				t.Fatal("native OMAP comparison reply automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed a native OMAP comparison request", replay.Transaction)
			default:
			}
		})
	}
}

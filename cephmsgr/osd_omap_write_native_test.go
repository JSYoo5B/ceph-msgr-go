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
// known-refusal conversion with raw outputs. It does not establish live OMAP
// execution, staging, durability, header storage or authentication.
func TestOSDOmapWritePublicResultsMatchFreshNativePayloads(t *testing.T) {
	header := OSDOperation{Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 5}}
	set := OSDOperation{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{
		{Key: "z", Value: []byte("Z")}, {Key: "b\x80", Value: []byte("\x00\xffA\n")},
		{Key: ""}, {Key: "\x00", Value: []byte("\xff\x80")},
	}}}
	setHeader := OSDOperation{Code: OSDSetOmapHeader, Omap: OSDOmapInput{Header: []byte("\x00\xffH\n\x80")}}
	remove := OSDOperation{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"b", "", "\x00"}}}
	failOKClear := OSDOperation{Code: OSDClearOmap, Flags: OSDFailOK}
	read, write := uint64(osd.FlagRead), uint64(osd.FlagWrite)
	returnVec := uint64(osd.FlagReturnVec)
	for _, tc := range []struct {
		fixture            string
		ops                []OSDOperation
		flags              uint64
		code               int32
		results            []int32
		lengths            []uint64
		outputs            []string
		matchNativeRequest bool
	}{
		{fixture: "omap-write-native-reply-v6-all-success", matchNativeRequest: true,
			ops:   []OSDOperation{header, set, setHeader, remove, failOKClear},
			flags: OSDReplyOnDisk | read | write | returnVec, results: []int32{0, 0, 0, 0, 0},
			lengths: []uint64{0, 47, 5, 18, 0}, outputs: []string{"00ff480a80", "", "", "", ""}},
		{fixture: "omap-write-native-reply-v6-header-empty", ops: []OSDOperation{{Code: OSDGetOmapHeader, Omap: OSDOmapInput{MaxOutputBytes: 1}}},
			flags: OSDReplyAck | read | returnVec, results: []int32{0}, lengths: []uint64{0}, outputs: []string{""}},
		{fixture: "omap-write-native-reply-v6-rmkeys-enoent", ops: []OSDOperation{{Code: OSDRemoveOmapKeys, Omap: OSDOmapInput{Keys: []string{"k"}}}},
			flags: write | returnVec, code: -2, results: []int32{-2}, lengths: []uint64{9}, outputs: []string{""}},
		{fixture: "omap-write-native-reply-v6-failok-clear", ops: []OSDOperation{failOKClear,
			{Code: OSDSetOmapValues, Omap: OSDOmapInput{Entries: []OSDOmapEntry{{Key: "k", Value: []byte("v")}}}}},
			flags: OSDReplyOnDisk | write | returnVec, results: []int32{-2, 0}, lengths: []uint64{0, 14}, outputs: []string{"", ""}},
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
				Object: OSDObject{Pool: 7, Name: "omap-write-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344}, Operations: tc.ops}
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
				if !bytes.Equal(request.Front, readFixture("omap-write-native-request-v6-all", "front")) || !bytes.Equal(request.Data, readFixture("omap-write-native-request-v6-all", "data")) {
					t.Fatal("public OMAP header/write request differs from fresh native front/data")
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
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("native known OMAP refusal did not remain a server result", got.err)
				}
			} else if got.err != nil {
				t.Fatal("native OMAP header/write reply rejected", got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.GlobalID != 42 || got.reply.Code != tc.code || got.reply.Object != input.Object.Name ||
				got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || got.reply.Flags != tc.flags || got.reply.MapEpoch != 11 || got.reply.UserVersion != 19 ||
				!bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) || len(got.reply.Operations) != len(tc.ops) {
				t.Fatal("native OMAP header/write identity, completion flags or raw payload changed", got.reply)
			}
			var joined []byte
			for i, result := range got.reply.Operations {
				output := osdOmapLiteral(t, tc.outputs[i])
				if result.Code != tc.ops[i].Code || result.Flags != uint32(tc.ops[i].Flags) || result.Result != tc.results[i] || result.Offset != 0 || result.Length != tc.lengths[i] ||
					result.TruncateSize != 0 || result.TruncateSequence != 0 || result.Xattr != (OSDXattrResult{}) || result.PayloadLength != uint32(len(output)) || !bytes.Equal(result.Data, output) {
					t.Fatal("native raw header, per-op result or echoed input union changed", i, result)
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
				t.Fatal("native OMAP header/write reply automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed a native OMAP header/write request", replay.Transaction)
			default:
			}
		})
	}
}

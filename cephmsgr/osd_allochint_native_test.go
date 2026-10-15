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
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func nativeAllocHintFixture(t *testing.T, name, segment string) []byte {
	t.Helper()
	p, err := os.ReadFile("../internal/osd/testdata/" + name + "." + segment + ".hex")
	if err != nil {
		t.Fatal(err)
	}
	return osdOmapLiteral(t, string(p))
}

// The pinned native set_alloc_hint builder always adds per-op FAILOK, so the
// public operations request it explicitly to submit the same wire request.
var nativeAllocHint = OSDAllocHint{ExpectedObjectSize: 4194304, ExpectedWriteSize: 65536, Flags: OSDAllocHintSequentialWrite | OSDAllocHintCompressible}

func nativeAllocHintOperation(hint OSDAllocHint) OSDOperation {
	return OSDOperation{Code: OSDSetAllocHint, Flags: OSDFailOK, AllocHint: hint}
}

var nativeAllocHintWriteFull = OSDOperation{Code: OSDWriteFull, Length: 3, Data: []byte("abc")}

func nativeAllocHintRequest(ops ...OSDOperation) OSDRequest {
	return OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
		Object:           OSDObject{Pool: 7, Name: "alloc-hint-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344},
		ModificationTime: OSDTime{Seconds: 1700000000, Nanoseconds: 5}, Operations: ops}
}

// The public builder must submit the same front/data as fresh native SDK
// objects. The local wait is then cancelled after transmission, so the sent
// write vector remains an uncertain outcome and is never resent.
func TestOSDAllocHintPublicRequestMatchesFreshNativePayload(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	input := nativeAllocHintRequest(nativeAllocHintOperation(nativeAllocHint), nativeAllocHintWriteFull, nativeAllocHintOperation(OSDAllocHint{}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type response struct {
		reply OSDReply
		err   error
	}
	finished := make(chan response, 1)
	go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
	request := peer.next(t)
	if !bytes.Equal(request.Front, nativeAllocHintFixture(t, "alloc-hint-native-request-v6-all", "front")) || !bytes.Equal(request.Data, nativeAllocHintFixture(t, "alloc-hint-native-request-v6-all", "data")) {
		t.Fatalf("public SETALLOCHINT request differs from fresh native front/data\nfront %x\ndata  %x", request.Front, request.Data)
	}
	cancel()
	var got response
	select {
	case got = <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled SETALLOCHINT wait did not return")
	}
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != request.Transaction || got.reply.Submission.GlobalID != 42 || got.reply.Transaction != 0 {
		t.Fatal("sent SETALLOCHINT lost its uncertain outcome or submission identity", got.reply, got.err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case replay := <-peer.requests:
		t.Fatal("client replayed a sent SETALLOCHINT", replay.Transaction)
	default:
	}
	if dials.Load() != 1 {
		t.Fatal("cancelled SETALLOCHINT automatically reconnected", dials.Load())
	}
}

// Fresh native SDK objects encoded these payloads independently of Go. The
// synthetic peer verifies public result conversion: a FAILOK -EOPNOTSUPP is
// retained in a successful vector, while the -ENOSPC aggregate stays a known
// server refusal. It does not establish live hint recording, object store
// allocation, staging, durability, older-OSD behavior or authentication.
func TestOSDAllocHintPublicResultsMatchFreshNativeReplies(t *testing.T) {
	max := OSDAllocHint{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: OSDAllocHintFlags(osd.AllocHintFlags)}
	committed := OSDReplyAck | OSDReplyOnDisk | uint64(osd.FlagWrite) | uint64(osd.FlagReturnVec)
	for _, tc := range []struct {
		fixture        string
		ops            []OSDOperation
		code           int32
		results        []int32
		user           uint64
		replay, legacy OSDVersion
	}{
		{fixture: "alloc-hint-native-reply-v6-success", ops: []OSDOperation{nativeAllocHintOperation(nativeAllocHint), nativeAllocHintWriteFull, nativeAllocHintOperation(OSDAllocHint{})},
			results: []int32{0, 0, 0}, user: 20, replay: OSDVersion{Epoch: 11, Version: 20}, legacy: OSDVersion{Epoch: 11, Version: 20}},
		{fixture: "alloc-hint-native-reply-v6-failok-refused", ops: []OSDOperation{nativeAllocHintOperation(nativeAllocHint), nativeAllocHintWriteFull},
			results: []int32{-95, 0}, user: 20, replay: OSDVersion{Epoch: 11, Version: 20}, legacy: OSDVersion{Epoch: 11, Version: 20}},
		{fixture: "alloc-hint-native-reply-v6-enospc", ops: []OSDOperation{nativeAllocHintOperation(nativeAllocHint)},
			code: -28, results: []int32{-28}, user: 19, replay: OSDVersion{Epoch: 11, Version: 20}, legacy: OSDVersion{Epoch: 11, Version: 19}},
		{fixture: "alloc-hint-native-reply-v6-max", ops: []OSDOperation{nativeAllocHintOperation(max)},
			results: []int32{0}, user: 20, replay: OSDVersion{Epoch: 11, Version: 20}, legacy: OSDVersion{Epoch: 11, Version: 20}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			front, data := nativeAllocHintFixture(t, tc.fixture, "front"), nativeAllocHintFixture(t, tc.fixture, "data")
			if len(data) != 0 {
				t.Fatal("native SETALLOCHINT reply carries output data", data)
			}
			c, peers, dials := osdTestClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			o, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
			if err != nil {
				t.Fatal(err)
			}
			peer := <-peers
			input := nativeAllocHintRequest(tc.ops...)
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
				t.Fatal("fresh native payload left public SETALLOCHINT pending", ctx.Err())
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			if tc.code < 0 {
				// A known server refusal is not an uncertain outcome.
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("native known SETALLOCHINT refusal did not remain a server result", got.err)
				}
			} else if got.err != nil {
				t.Fatal("native SETALLOCHINT reply rejected", got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.GlobalID != 42 || got.reply.Code != tc.code || got.reply.Object != input.Object.Name ||
				got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || got.reply.Flags != committed || got.reply.MapEpoch != 11 || got.reply.UserVersion != tc.user ||
				got.reply.ReplayVersion != tc.replay || got.reply.LegacyVersion != tc.legacy ||
				!bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) || len(got.reply.Operations) != len(tc.ops) {
				t.Fatal("native SETALLOCHINT identity, completion flags, versions or raw payload changed", got.reply)
			}
			for i, result := range got.reply.Operations {
				requested := tc.ops[i]
				if result.Code != requested.Code || result.Flags != uint32(requested.Flags) || result.Result != tc.results[i] || result.PayloadLength != 0 || len(result.Data) != 0 ||
					result.TruncateSize != 0 || result.TruncateSequence != 0 || result.Xattr != (OSDXattrResult{}) || result.Class != (OSDClassResult{}) {
					t.Fatal("native per-op result, flags or output changed", i, result)
				}
				if requested.Code == OSDSetAllocHint {
					if result.AllocHint != requested.AllocHint || result.Offset != 0 || result.Length != 0 {
						t.Fatal("native SETALLOCHINT union echo changed", result)
					}
				} else if result.AllocHint != (OSDAllocHint{}) || result.Offset != 0 || result.Length != 3 {
					t.Fatal("native WRITEFULL echo changed", result)
				}
			}
			if o.session.Err() != nil {
				t.Fatal("native SETALLOCHINT reply invalidated the session", o.session.Err())
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
				t.Fatal("native SETALLOCHINT reply automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed a native SETALLOCHINT request", replay.Transaction)
			default:
			}
		})
	}
}

// Product validation stays strict against the same native replies: the echo
// must equal the submitted hint and FAILOK flag, and only a requested FAILOK
// may keep the -EOPNOTSUPP per-op result inside a successful vector.
func TestOSDAllocHintNativeRepliesRejectContradictoryRequests(t *testing.T) {
	decode := func(name string) osd.Reply {
		m := msgr.MessageData{Type: msgr.OSDOpReplyMessage, Version: 6, CompatVersion: 2, Front: nativeAllocHintFixture(t, name, "front"), Data: nativeAllocHintFixture(t, name, "data")}
		r, err := osd.DecodeReply(m, wire.DefaultLimit)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	request := func(ops ...osd.Operation) osd.Request {
		return osd.Request{ClientInc: 1, MapEpoch: 11, Hash: 0x11223344, Object: "alloc-hint-codec",
			Locator: osd.Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, MTimeSeconds: 1700000000, MTimeNanoseconds: 5, Operations: ops}
	}
	hint := osd.AllocHintInput{ExpectedObjectSize: 4194304, ExpectedWriteSize: 65536, Flags: 1 | 256}
	max := osd.AllocHintInput{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: osd.AllocHintFlags}
	writeFull := osd.Operation{Code: osd.WriteFull, Length: 3, Data: []byte("abc")}
	failok := func(h osd.AllocHintInput) osd.Operation {
		return osd.Operation{Code: osd.SetAllocHint, Flags: osd.OpFailOK, AllocHint: h}
	}
	maxReply := decode("alloc-hint-native-reply-v6-max")
	if err := validateOSDReply(request(failok(max)), maxReply, false); err != nil {
		t.Fatal("native max reply rejected for its own request", err)
	}
	for _, other := range []osd.AllocHintInput{
		{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), Flags: osd.AllocHintFlags &^ 1024},
		{ExpectedObjectSize: ^uint64(0) - 1, ExpectedWriteSize: ^uint64(0), Flags: osd.AllocHintFlags},
		{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: 0, Flags: osd.AllocHintFlags},
	} {
		if err := validateOSDReply(request(failok(other)), maxReply, false); err == nil {
			t.Fatal("native max echo accepted for a different submitted hint", other)
		}
	}
	if err := validateOSDReply(request(osd.Operation{Code: osd.SetAllocHint, AllocHint: max}), maxReply, false); err == nil {
		t.Fatal("native FAILOK echo accepted for a FAILOK-free request")
	}
	refused := decode("alloc-hint-native-reply-v6-failok-refused")
	if err := validateOSDReply(request(failok(hint), writeFull), refused, false); err != nil {
		t.Fatal("native FAILOK -95 reply rejected", err)
	}
	if err := validateOSDReply(request(osd.Operation{Code: osd.SetAllocHint, AllocHint: hint}, writeFull), refused, false); err == nil {
		t.Fatal("successful vector with an ordinary failed SETALLOCHINT accepted")
	}
	// A committed success without ONDISK is never a durability receipt.
	success := decode("alloc-hint-native-reply-v6-success")
	success.Flags &^= osd.FlagOnDisk
	if err := validateOSDReply(request(failok(hint), writeFull, failok(osd.AllocHintInput{})), success, false); err == nil {
		t.Fatal("SETALLOCHINT success without ONDISK accepted")
	}
}

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

func nativeCallFixture(t *testing.T, name, segment string) []byte {
	t.Helper()
	p, err := os.ReadFile("../internal/osd/testdata/" + name + "." + segment + ".hex")
	if err != nil {
		t.Fatal(err)
	}
	return osdOmapLiteral(t, string(p))
}

func nativeCallOperation(mode OSDCallMode, method, input string) OSDOperation {
	return OSDOperation{Code: OSDCall, Data: []byte(input), Class: OSDClassInput{Class: "hello", Method: method, Mode: mode, MaxOutputBytes: 64}}
}

// The public builder must submit the same front/data as fresh native SDK
// objects: a CMPXATTR guard, then a write-mode CALL with binary input. The
// local wait is then cancelled after transmission, so the sent CALL remains
// an uncertain outcome and is never resent.
func TestOSDCallPublicRequestMatchesFreshNativePayload(t *testing.T) {
	c, peers, dials := osdTestClient(t)
	o, err := c.OpenOSD(context.Background(), OSDTarget{Address: "192.0.2.1:6804"})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-peers
	input := OSDRequest{ClientIncarnation: 1, MapEpoch: 11,
		Object:           OSDObject{Pool: 7, Name: "call-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344},
		ModificationTime: OSDTime{Seconds: 1700000000, Nanoseconds: 5}, Operations: []OSDOperation{
			{Code: OSDCompareXattr, Data: []byte("v"), Xattr: OSDXattrInput{Name: "a", Comparison: OSDXattrEqual}},
			nativeCallOperation(OSDCallWrite, "write_return_data", "T\x00\xff"),
		}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type response struct {
		reply OSDReply
		err   error
	}
	finished := make(chan response, 1)
	go func() { reply, err := o.Request(ctx, input); finished <- response{reply, err} }()
	request := peer.next(t)
	if !bytes.Equal(request.Front, nativeCallFixture(t, "call-native-request-v6-all", "front")) || !bytes.Equal(request.Data, nativeCallFixture(t, "call-native-request-v6-all", "data")) {
		t.Fatalf("public CALL request differs from fresh native front/data\nfront %x\ndata  %x", request.Front, request.Data)
	}
	cancel()
	var got response
	select {
	case got = <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled CALL wait did not return")
	}
	var unknown *OutcomeUnknownError
	if !errors.As(got.err, &unknown) || !errors.Is(got.err, context.Canceled) || got.reply.Submission.Transaction != request.Transaction || got.reply.Submission.GlobalID != 42 || got.reply.Transaction != 0 {
		t.Fatal("sent CALL lost its uncertain outcome or submission identity", got.reply, got.err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case replay := <-peer.requests:
		t.Fatal("client replayed a sent CALL", replay.Transaction)
	default:
	}
	if dials.Load() != 1 {
		t.Fatal("cancelled CALL automatically reconnected", dials.Load())
	}
}

// Fresh native SDK objects encoded these payloads independently of Go. The
// synthetic peer verifies public result conversion: a positive method result
// stays a successful reply, while -EOVERFLOW and -EOPNOTSUPP stay known server
// refusals. It does not establish live class loading, method execution,
// staging, durability, the write-reply bound or authentication.
func TestOSDCallPublicResultsMatchFreshNativeReplies(t *testing.T) {
	read, write := uint64(osd.FlagRead), uint64(osd.FlagWrite)
	returnVec := uint64(osd.FlagReturnVec)
	for _, tc := range []struct {
		fixture        string
		op             OSDOperation
		mtime          OSDTime
		flags          uint64
		code, result   int32
		class          OSDClassResult
		output         string
		user           uint64
		replay, legacy OSDVersion
	}{
		{fixture: "call-native-reply-v6-read-output", op: nativeCallOperation(OSDCallRead, "say_hello", "Tester"),
			flags: OSDReplyAck | OSDReplyOnDisk | read | returnVec, class: OSDClassResult{ClassLength: 5, MethodLength: 9, InputLength: 6, OutputLength: 14},
			output: "Hello, Tester!", user: 19, legacy: OSDVersion{Version: 19}},
		{fixture: "call-native-reply-v6-positive", op: nativeCallOperation(OSDCallWrite, "write_return_data", ""), mtime: OSDTime{Seconds: 1700000000, Nanoseconds: 5},
			flags: OSDReplyAck | OSDReplyOnDisk | write | returnVec, code: 42, result: 42, class: OSDClassResult{ClassLength: 5, MethodLength: 17, OutputLength: 18},
			output: "you might see this", user: 20, replay: OSDVersion{Epoch: 11, Version: 20}, legacy: OSDVersion{Epoch: 11, Version: 20}},
		{fixture: "call-native-reply-v6-eoverflow", op: nativeCallOperation(OSDCallWrite, "write_too_much_return_data", ""), mtime: OSDTime{Seconds: 1700000000, Nanoseconds: 5},
			flags: OSDReplyAck | OSDReplyOnDisk | write | returnVec, code: -75, result: -75, class: OSDClassResult{ClassLength: 5, MethodLength: 26, OutputLength: 520}},
		{fixture: "call-native-reply-v6-eopnotsupp", op: nativeCallOperation(OSDCallRead, "nope", ""),
			flags: OSDReplyOnDisk | read | returnVec, code: -95, class: OSDClassResult{ClassLength: 5, MethodLength: 4}},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			front, data := nativeCallFixture(t, tc.fixture, "front"), nativeCallFixture(t, tc.fixture, "data")
			if string(data) != tc.output {
				t.Fatal("native CALL reply data differs from the logical method output", data)
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
				Object:           OSDObject{Pool: 7, Name: "call-codec", Key: "route-key", Namespace: "tenant", Hash: 0x11223344},
				ModificationTime: tc.mtime, Operations: []OSDOperation{tc.op}}
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
				t.Fatal("fresh native payload left public CALL pending", ctx.Err())
			}
			var unknown *OutcomeUnknownError
			var server *OSDError
			if tc.code < 0 {
				// A known server refusal is not an uncertain outcome.
				if !errors.As(got.err, &server) || server.Code != tc.code || errors.As(got.err, &unknown) {
					t.Fatal("native known CALL refusal did not remain a server result", got.err)
				}
			} else if got.err != nil {
				t.Fatal("native CALL reply rejected", got.err)
			}
			want := OSDSubmission{Transaction: request.Transaction, GlobalID: 42, ClientIncarnation: 1, MapEpoch: 11}
			if got.reply.Submission != want || got.reply.Transaction != request.Transaction || got.reply.GlobalID != 42 || got.reply.Code != tc.code || got.reply.Object != input.Object.Name ||
				got.reply.PG != (OSDPG{Pool: 7, Seed: 0x11223344}) || got.reply.Flags != tc.flags || got.reply.MapEpoch != 11 || got.reply.UserVersion != tc.user ||
				got.reply.ReplayVersion != tc.replay || got.reply.LegacyVersion != tc.legacy ||
				!bytes.Equal(got.reply.RawFront, front) || !bytes.Equal(got.reply.RawData, data) || len(got.reply.Operations) != 1 {
				t.Fatal("native CALL identity, completion flags, versions or raw payload changed", got.reply)
			}
			result := got.reply.Operations[0]
			if result.Code != OSDCall || result.Flags != 0 || result.Result != tc.result || result.Class != tc.class || result.Offset != 0 || result.Length != 0 ||
				result.TruncateSize != 0 || result.TruncateSequence != 0 || result.Xattr != (OSDXattrResult{}) || result.PayloadLength != uint32(len(tc.output)) || string(result.Data) != tc.output {
				t.Fatal("native per-op CALL result, union echo or output changed", result)
			}
			if o.session.Err() != nil {
				t.Fatal("native CALL reply invalidated the session", o.session.Err())
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
				t.Fatal("native CALL reply automatically reconnected", dials.Load())
			}
			select {
			case replay := <-peer.requests:
				t.Fatal("client replayed a native CALL request", replay.Transaction)
			default:
			}
		})
	}
}

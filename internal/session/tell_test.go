package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type tellCallResult struct {
	message msgr.MessageData
	err     error
}

type tellTestPeer struct {
	session *Session
	conn    net.Conn
	reader  *msgr.Reader
	writer  *msgr.Writer
	ctx     context.Context
	calls   sync.WaitGroup
}

func newTellTestPeer(t *testing.T) *tellTestPeer {
	t.Helper()
	client, peer := net.Pipe()
	if err := peer.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &tellTestPeer{
		session: New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 10 * time.Second}, nil, nil),
		conn:    peer, reader: msgr.NewReader(peer, 0), writer: msgr.NewWriter(peer, 0), ctx: ctx,
	}
	t.Cleanup(func() {
		cancel()
		h.session.Fail(ErrClosed)
		peer.Close()
		h.session.Wait()
		h.calls.Wait()
	})
	return h
}

func (h *tellTestPeer) call(ctx context.Context, message msgr.MessageData) <-chan tellCallResult {
	done := make(chan tellCallResult, 1)
	h.calls.Add(1)
	go func() {
		defer h.calls.Done()
		message, err := h.session.Call(ctx, message)
		done <- tellCallResult{message: message, err: err}
	}()
	return done
}

func awaitTellCall(t *testing.T, done <-chan tellCallResult) tellCallResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish after its peer response or local cancellation")
		return tellCallResult{}
	}
}

func writeTellTestReply(t *testing.T, h *tellTestPeer, request msgr.MessageData, replyType uint16, sequence uint64) {
	t.Helper()
	// The session routes complete opaque replies. Payload decoding and the
	// independent MCommandReply wire vector are tested by internal/msgr.
	message := msgr.MessageData{
		Type: replyType, Transaction: request.Transaction, Sequence: sequence,
		Front: append([]byte("reply:"), request.Front...),
		Data:  append([]byte{0, 0xff, 7}, request.Data...),
	}
	if err := h.writer.Write(message.Frame()); err != nil {
		t.Fatal(err)
	}
}

func TestTellAndManagementRepliesRouteConcurrentTransactions(t *testing.T) {
	for _, normal := range []struct {
		name    string
		request uint16
		reply   uint16
	}{
		{"MON", msgr.MonCommandMessage, msgr.MonCommandReplyMessage},
		{"MGR", msgr.MgrCommandMessage, msgr.MgrCommandReplyMessage},
	} {
		t.Run(normal.name, func(t *testing.T) {
			h := newTellTestPeer(t)
			h.session.Start()
			const count = 12
			done := make([]<-chan tellCallResult, count)
			for i := range done {
				requestType := normal.request
				if i%2 != 0 {
					requestType = msgr.TellCommandMessage
				}
				done[i] = h.call(h.ctx, msgr.MessageData{
					Type: requestType, Front: []byte(fmt.Sprintf("request-%d", i)), Data: []byte{byte(i)},
				})
			}
			requests := make([]msgr.MessageData, count)
			transactions := make(map[uint64]bool, count)
			for i := range requests {
				requests[i] = readRequest(t, h.reader)
				if requests[i].Transaction == 0 || transactions[requests[i].Transaction] {
					t.Fatalf("normal and tell calls reused TID %d", requests[i].Transaction)
				}
				transactions[requests[i].Transaction] = true
			}
			// Reverse wire order so completion cannot accidentally depend on the
			// order in which concurrent calls registered or were transmitted.
			for i := count - 1; i >= 0; i-- {
				replyType := normal.reply
				if requests[i].Type == msgr.TellCommandMessage {
					replyType = msgr.TellCommandReplyMessage
				}
				writeTellTestReply(t, h, requests[i], replyType, uint64(count-i))
			}
			for i, result := range done {
				got := awaitTellCall(t, result)
				replyType := normal.reply
				if i%2 != 0 {
					replyType = msgr.TellCommandReplyMessage
				}
				if got.err != nil || got.message.Type != replyType || string(got.message.Front) != fmt.Sprintf("reply:request-%d", i) || !bytes.Equal(got.message.Data, []byte{0, 0xff, 7, byte(i)}) {
					t.Fatalf("call %d received another call's reply: %#v, %v", i, got.message, got.err)
				}
			}
			if h.session.sent.Load() != count || h.session.Err() != nil {
				t.Fatal("mixed requests were replayed or left a failed session", h.session.Err())
			}
		})
	}
}

func TestTellReplyTypeMismatchFailsSentCallsWithoutReplay(t *testing.T) {
	for _, mismatch := range []struct {
		name    string
		request uint16
		reply   uint16
	}{
		{"tell receives MON reply", msgr.TellCommandMessage, msgr.MonCommandReplyMessage},
		{"tell receives MGR reply", msgr.TellCommandMessage, msgr.MgrCommandReplyMessage},
		{"MON receives tell reply", msgr.MonCommandMessage, msgr.TellCommandReplyMessage},
		{"MGR receives tell reply", msgr.MgrCommandMessage, msgr.TellCommandReplyMessage},
	} {
		t.Run(mismatch.name, func(t *testing.T) {
			h := newTellTestPeer(t)
			h.session.Start()
			target := h.call(h.ctx, msgr.MessageData{Type: mismatch.request, Front: []byte("target")})
			other := h.call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("other")})
			first, second := readRequest(t, h.reader), readRequest(t, h.reader)
			if string(first.Front) != "target" {
				first, second = second, first
			}
			if string(first.Front) != "target" || string(second.Front) != "other" {
				t.Fatal("requests were not both transmitted")
			}
			writeTellTestReply(t, h, first, mismatch.reply, 1)
			for _, done := range []<-chan tellCallResult{target, other} {
				got := awaitTellCall(t, done)
				var unknown *OutcomeUnknownError
				if !errors.Is(got.err, msgr.ErrFrame) || !errors.As(got.err, &unknown) {
					t.Fatal("wrong reply type lost the sent command's protocol/unknown outcome", got.err)
				}
			}
			h.session.Wait()
			if h.session.sent.Load() != 2 {
				t.Fatal("protocol failure replayed a sent request")
			}
			_, err := h.session.Call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage})
			var unknown *OutcomeUnknownError
			if !errors.Is(err, msgr.ErrFrame) || errors.As(err, &unknown) {
				t.Fatal("new request on the failed session was not rejected before transmission", err)
			}
			if _, err := h.reader.Read(); !errors.Is(err, io.EOF) {
				t.Fatal("failed session emitted another frame instead of closing", err)
			}
		})
	}
}

func TestCanceledTellConsumesLateReplyAndKeepsOtherCallsAlive(t *testing.T) {
	h := newTellTestPeer(t)
	h.session.Start()
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	canceled := h.call(ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("canceled")})
	live := h.call(h.ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
	first, second := readRequest(t, h.reader), readRequest(t, h.reader)
	if string(first.Front) != "canceled" {
		first, second = second, first
	}
	cancel()
	got := awaitTellCall(t, canceled)
	var unknown *OutcomeUnknownError
	if !errors.Is(got.err, context.Canceled) || !errors.As(got.err, &unknown) {
		t.Fatal("sent tell cancellation lost its unknown execution outcome", got.err)
	}
	writeTellTestReply(t, h, first, msgr.TellCommandReplyMessage, 1)
	newTell := h.call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("new")})
	third := readRequest(t, h.reader)
	if string(third.Front) != "new" || third.Transaction <= first.Transaction || third.Transaction <= second.Transaction || third.Sequence != 3 {
		t.Fatal("canceled tell was replayed or its TID reused", third)
	}
	writeTellTestReply(t, h, third, msgr.TellCommandReplyMessage, 2)
	writeTellTestReply(t, h, second, msgr.MonCommandReplyMessage, 3)
	for _, expected := range []struct {
		name string
		done <-chan tellCallResult
	}{
		{"new", newTell}, {"live", live},
	} {
		got := awaitTellCall(t, expected.done)
		if got.err != nil || string(got.message.Front) != "reply:"+expected.name {
			t.Fatal("late canceled reply completed or failed another request", got.message, got.err)
		}
	}
	if h.session.sent.Load() != 3 || h.session.Err() != nil {
		t.Fatal("cancellation replayed a command or closed the session", h.session.Err())
	}
}

func TestTellCloseDistinguishesStartedAndQueuedCalls(t *testing.T) {
	h := newTellTestPeer(t)
	h.session.Start()
	started := h.call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("started")})
	// Reading only a prefix makes transmission start while the writer remains
	// blocked on this frame. The next request therefore cannot have been sent.
	if _, err := io.ReadFull(h.conn, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	queued := h.call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("queued")})
	select {
	case request := <-h.session.queue:
		h.session.queue <- request
	case <-time.After(5 * time.Second):
		t.Fatal("second tell did not enter the queue behind the blocked writer")
	}
	h.session.Fail(ErrClosed)
	for _, expected := range []struct {
		name    string
		done    <-chan tellCallResult
		unknown bool
	}{
		{"started", started, true}, {"queued", queued, false},
	} {
		got := awaitTellCall(t, expected.done)
		var unknown *OutcomeUnknownError
		if !errors.Is(got.err, ErrClosed) || errors.As(got.err, &unknown) != expected.unknown {
			t.Fatalf("%s tell has wrong shutdown outcome: %v", expected.name, got.err)
		}
	}
	h.session.Wait()
	if h.session.sent.Load() != 1 {
		t.Fatal("queued tell was sent or started tell was replayed during shutdown")
	}
	_, err := h.session.Call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
		t.Fatal("closed session accepted another tell or reported an unsent outcome as unknown", err)
	}
}

func TestTransportACKDoesNotCompleteTell(t *testing.T) {
	h := newTellTestPeer(t)
	h.session.Start()
	done := h.call(h.ctx, msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("tell")})
	request := readRequest(t, h.reader)
	ack := make([]byte, 8)
	binary.LittleEndian.PutUint64(ack, request.Sequence)
	if err := h.writer.Write(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{ack}}); err != nil {
		t.Fatal(err)
	}
	// A keepalive round trip is a causal barrier: the read loop has handled the
	// preceding ACK before the peer can receive this echo.
	if err := h.writer.Write(msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{ack}}); err != nil {
		t.Fatal(err)
	}
	frame, err := h.reader.Read()
	if err != nil || frame.Tag != msgr.KeepaliveAck {
		t.Fatal("ACK processing barrier failed", frame.Tag, err)
	}
	select {
	case got := <-done:
		t.Fatal("transport ACK completed tell without a daemon response", got.err)
	default:
	}
	writeTellTestReply(t, h, request, msgr.TellCommandReplyMessage, 1)
	if got := awaitTellCall(t, done); got.err != nil || string(got.message.Front) != "reply:tell" {
		t.Fatal("tell response did not complete the still-pending operation", got.message, got.err)
	}
}

package session

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func testSession(t *testing.T) (*Session, *msgr.Reader, *msgr.Writer) {
	t.Helper()
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, time.Second, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	return s, msgr.NewReader(peer, 0), msgr.NewWriter(peer, 0)
}
func readRequest(t *testing.T, r *msgr.Reader) msgr.MessageData {
	t.Helper()
	for {
		f, err := r.Read()
		if err != nil {
			t.Fatal(err)
		}
		if f.Tag == msgr.Message {
			m, err := msgr.DecodeMessage(f)
			if err != nil {
				t.Fatal(err)
			}
			return m
		}
	}
}
func reply(t *testing.T, w *msgr.Writer, m msgr.MessageData, seq uint64) {
	t.Helper()
	m.Type = msgr.MonCommandReplyMessage
	m.Sequence = seq
	m.AckSequence = 0
	if err := w.Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
}

func TestCancelOneConcurrentRequestAndConsumeLateReply(t *testing.T) {
	s, r, w := testSession(t)
	s.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("a")})
		a <- err
	}()
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("b")})
		b <- err
	}()
	first, second := readRequest(t, r), readRequest(t, r)
	if string(first.Front) == "b" {
		first, second = second, first
	}
	cancel()
	err := <-a
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
		t.Fatal("sent cancellation lost unknown outcome", err)
	}
	reply(t, w, second, 1)
	if err := <-b; err != nil {
		t.Fatal("other request affected", err)
	}
	reply(t, w, first, 2) // Late reply must not be reassigned to another request.
	c := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("c")})
		c <- err
	}()
	third := readRequest(t, r)
	if string(third.Front) != "c" {
		t.Fatal("stream corrupted")
	}
	reply(t, w, third, 3)
	if err := <-c; err != nil {
		t.Fatal(err)
	}
}

func TestQueuedCancellationIsNeverTransmitted(t *testing.T) {
	s, r, w := testSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("cancelled")})
		done <- err
	}()
	queued := <-s.queue
	cancel()
	err := <-done
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
		t.Fatal("queued cancellation", err)
	}
	s.queue <- queued
	s.Start()
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
		done <- err
	}()
	live := readRequest(t, r)
	if string(live.Front) != "live" || live.Sequence != 1 {
		t.Fatal("cancelled request transmitted")
	}
	reply(t, w, live, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTransportACKDoesNotCompleteCommand(t *testing.T) {
	s, r, w := testSession(t)
	s.Start()
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
		done <- err
	}()
	m := readRequest(t, r)
	e := wire.Encoder{}
	e.U64(m.Sequence)
	if err := w.Write(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{e.Data}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatal("ACK completed command", err)
	default:
	}
	reply(t, w, m, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCloseReleasesRequestsAndWorkers(t *testing.T) {
	s, r, _ := testSession(t)
	s.Start()
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
		done <- err
	}()
	readRequest(t, r)
	s.Fail(ErrClosed)
	err := <-done
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrClosed) || !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	s.Wait()
	if _, err := s.Call(context.Background(), msgr.MessageData{}); !errors.Is(err, ErrClosed) {
		t.Fatal("closed session accepted request")
	}
}

func TestCancellationDuringDispatchRemainsConservative(t *testing.T) {
	s, _, _ := testSession(t)
	r := &request{started: true}
	s.pending[1] = r
	s.mu.Lock()
	s.remove(1)
	s.mu.Unlock() // Dispatch has removed it but hasn't delivered its result yet.
	var unknown *OutcomeUnknownError
	if err := s.abandon(1, r, context.Canceled); !errors.As(err, &unknown) {
		t.Fatal("race lost delivery state", err)
	}
}

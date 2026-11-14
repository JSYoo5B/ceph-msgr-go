package session

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// A call that meets the loss of its transport is classified only by whether
// the writer had started its frame. Started: an unknown outcome. Admitted but
// not started, or issued after the session recorded the loss: the bare cause,
// known not to have been sent. The same condition (the peer is gone) therefore
// has both results, and neither is a transmitted command reported as unsent.
func TestTransportLossClassifiesCallsByTransmissionStart(t *testing.T) {
	open := func(t *testing.T) (*Session, net.Conn) {
		t.Helper()
		client, peer := net.Pipe()
		peer.SetDeadline(time.Now().Add(3 * time.Second))
		s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: time.Second}, nil, nil)
		t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
		return s, peer
	}
	type outcome struct {
		message msgr.MessageData
		err     error
	}
	call := func(s *Session) <-chan outcome {
		done := make(chan outcome, 1)
		go func() {
			m, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
			done <- outcome{m, err}
		}()
		return done
	}
	var unknown *OutcomeUnknownError
	t.Run("started before the loss, then issued after it", func(t *testing.T) {
		s, peer := open(t)
		s.Start()
		done := call(s)
		// The peer received the complete frame, so its write had started.
		sent := readRequest(t, msgr.NewReader(peer, 0))
		peer.Close()
		got := <-done
		if !errors.As(got.err, &unknown) || errors.Is(got.err, ErrClosed) || got.message.Transaction != sent.Transaction || sent.Transaction == 0 {
			t.Fatal("a started call lost its unknown outcome or its transaction", got.err, got.message.Transaction)
		}
		<-s.Done()
		cause := s.Err()
		if cause == nil || errors.As(cause, &unknown) {
			t.Fatal("the session's terminal cause", cause)
		}
		// The session has recorded the loss: nothing is admitted.
		for range 2 {
			m, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
			if err != cause || m.Transaction != 0 {
				t.Fatal("a call on the ended session was admitted or lost the session's cause", err, m.Transaction)
			}
		}
	})
	t.Run("admitted but not started", func(t *testing.T) {
		// Without a writer the call stays queued, as it does between its
		// admission and the writer's next turn.
		s, _ := open(t)
		done := call(s)
		for len(s.queue) == 0 {
			select {
			case got := <-done:
				t.Fatal("the call ended before it was queued", got.err)
			case <-time.After(time.Millisecond):
			}
		}
		s.Fail(io.ErrUnexpectedEOF)
		got := <-done
		if got.err != io.ErrUnexpectedEOF || got.message.Transaction == 0 {
			t.Fatal("an unstarted call was not refused with the bare cause and its allocated transaction", got.err, got.message.Transaction)
		}
	})
}

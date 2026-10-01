package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestCancellationDuringPartialWriteFinishesFrameAndPreservesOtherCall(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 2 * time.Second}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("cancelled")})
		a <- err
	}()
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	// net.Pipe blocks the remaining bytes. Cancellation must release the
	// caller but cannot abandon the frame or change the shared deadline.
	cancel()
	var unknown *OutcomeUnknownError
	if err := <-a; !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
		t.Fatal("partial write lost uncertain outcome", err)
	}
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
		b <- err
	}()
	r := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	w := msgr.NewWriter(peer, 0)
	first, second := readRequest(t, r), readRequest(t, r)
	if string(first.Front) != "cancelled" || string(second.Front) != "live" || second.Sequence != first.Sequence+1 {
		t.Fatal("partial frame interrupted stream", first, second)
	}
	reply(t, w, first, 1) // The canceled call's late response is ignored.
	reply(t, w, second, 2)
	if err := <-b; err != nil {
		t.Fatal("another call was affected by cancellation", err)
	}
}

func TestTruncatedResponseCannotEstablishCommandOutcome(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: time.Second}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
		done <- err
	}()
	m := readRequest(t, msgr.NewReader(peer, 0))
	m.Type, m.Sequence, m.Front = msgr.MonCommandReplyMessage, 1, []byte("response")
	var encoded bytes.Buffer
	if err := msgr.NewWriter(&encoded, 0).Write(m.Frame()); err != nil {
		t.Fatal(err)
	}
	if err := msgr.WriteFull(peer, encoded.Bytes()[:encoded.Len()-1]); err != nil {
		t.Fatal(err)
	}
	peer.Close()
	var unknown *OutcomeUnknownError
	if err := <-done; !errors.As(err, &unknown) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("partial response classified as completion", err)
	}
	s.Wait()
}

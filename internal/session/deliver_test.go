package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func pipeSession(t *testing.T) (*Session, net.Conn) {
	t.Helper()
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 2 * time.Second, Inbound: true}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	return s, peer
}

// TestDeliverReportsWrittenNotSentAndUnknown holds the writer inside a
// first frame (nothing reads the pipe), so a second Deliver stays queued:
// its canceled wait withdraws it (ErrNotSent) and it is never written. The
// first Deliver returns nil only once its frame was read in full. A third
// Deliver whose write starts and then fails is uncertain.
func TestDeliverReportsWrittenNotSentAndUnknown(t *testing.T) {
	s, peer := pipeSession(t)
	first := make(chan error, 1)
	go func() { first <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("first")}) }()
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-first:
		t.Fatal("Deliver returned before its frame was written", err)
	case <-time.After(50 * time.Millisecond):
	}
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		second <- s.Deliver(ctx, msgr.MessageData{Type: 98, Front: []byte("withdrawn")})
	}()
	for len(s.queue) == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-second; !errors.Is(err, ErrNotSent) || !errors.Is(err, context.Canceled) {
		t.Fatal("a queued, canceled Deliver must be known not sent", err)
	}
	third := make(chan error, 1)
	go func() { third <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("third")}) }()
	reader := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	if m := readRequest(t, reader); string(m.Front) != "first" {
		t.Fatal("first frame", m)
	}
	if err := <-first; err != nil {
		t.Fatal("a written Deliver must report nil", err)
	}
	if m := readRequest(t, reader); string(m.Front) != "third" || m.Sequence != 2 {
		t.Fatal("the withdrawn message was written, or consumed a sequence", m)
	}
	if err := <-third; err != nil {
		t.Fatal(err)
	}
	// A fourth write starts, then the transport fails mid-frame.
	fourth := make(chan error, 1)
	go func() { fourth <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: bytes.Repeat([]byte{1}, 4096)}) }()
	if _, err := io.ReadFull(peer, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	peer.Close()
	var unknown *OutcomeUnknownError
	if err := <-fourth; !errors.As(err, &unknown) || errors.Is(err, ErrNotSent) {
		t.Fatal("a started, failed write must be uncertain", err)
	}
	if err := s.Deliver(context.Background(), msgr.MessageData{Type: 98}); !errors.Is(err, ErrNotSent) {
		t.Fatal("Deliver after the session failed must be known not sent", err)
	}
}

// TestDeliverSessionFailureWithdrawsQueuedMessage fails the session while a
// Deliver is queued behind a held frame: it is known not sent.
func TestDeliverSessionFailureWithdrawsQueuedMessage(t *testing.T) {
	s, peer := pipeSession(t)
	go s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("held")})
	if _, err := io.ReadFull(peer, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() { queued <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("queued")}) }()
	for len(s.queue) == 0 {
		time.Sleep(time.Millisecond)
	}
	cause := errors.New("test failure")
	s.Fail(cause)
	if err := <-queued; !errors.Is(err, ErrNotSent) || !errors.Is(err, cause) {
		t.Fatal("queued Deliver at session failure", err)
	}
}

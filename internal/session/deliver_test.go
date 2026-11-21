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
	return pipeSessionWith(t, Config{WriteTimeout: 2 * time.Second, Inbound: true})
}

func pipeSessionWith(t *testing.T, config Config) (*Session, net.Conn) {
	t.Helper()
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, config, nil, nil)
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
	go func() {
		fourth <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: bytes.Repeat([]byte{1}, 4096)})
	}()
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

// TestDeliverCanceledBeforeWriterAdmissionIsNeverWritten queues a Deliver
// request behind a held frame and cancels its ctx with no waiter to
// withdraw it (the waiter's withdrawal has not run): the writer itself
// sees the end, reports it not sent and never writes it, and the next
// message takes the next sequence.
func TestDeliverCanceledBeforeWriterAdmissionIsNeverWritten(t *testing.T) {
	s, peer := pipeSession(t)
	first := make(chan error, 1)
	go func() { first <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("held")}) }()
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := s.queueDelivery(ctx, msgr.MessageData{Type: 98, Front: []byte("canceled")})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	reader := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	if m := readRequest(t, reader); string(m.Front) != "held" {
		t.Fatal("held frame", m)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-r.delivered; !errors.Is(err, ErrNotSent) || !errors.Is(err, context.Canceled) {
		t.Fatal("a request canceled before admission must be known not sent", err)
	}
	third := make(chan error, 1)
	go func() { third <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("next")}) }()
	if m := readRequest(t, reader); string(m.Front) != "next" || m.Sequence != 2 {
		t.Fatal("the canceled request was written, or consumed a sequence", m)
	}
	if err := <-third; err != nil {
		t.Fatal(err)
	}
}

// TestDeliverCanceledDuringBeforeWriteIsNeverStarted ends the ctx inside
// the writer's admission callback, after the writer's first context check:
// its last check before the start sees the end, so the request is not sent
// and the next message takes the first sequence.
func TestDeliverCanceledDuringBeforeWriteIsNeverStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, peer := pipeSessionWith(t, Config{WriteTimeout: 2 * time.Second, Inbound: true, BeforeWrite: func(m msgr.MessageData) error {
		if string(m.Front) == "canceled" {
			cancel()
		}
		return nil
	}})
	r, err := s.queueDelivery(ctx, msgr.MessageData{Type: 98, Front: []byte("canceled")})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-r.delivered; !errors.Is(err, ErrNotSent) || !errors.Is(err, context.Canceled) {
		t.Fatal("a request canceled during admission must be known not sent", err)
	}
	next := make(chan error, 1)
	go func() { next <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("next")}) }()
	if m := readRequest(t, msgr.NewReader(peer, 0)); string(m.Front) != "next" || m.Sequence != 1 {
		t.Fatal("the canceled request was written, or consumed a sequence", m)
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
}

// TestDeliverCanceledWhileWriterWaitsForLockIsNeverStarted holds the
// session lock once the writer has passed every check outside it and is
// about to take the lock for the start decision, cancels the ctx, and only
// then releases the lock. No waiter can withdraw the request (it is queued
// directly), so only the writer's decision under the lock stands between
// the ended ctx and the wire: the request is known not sent and the next
// message takes the first sequence.
func TestDeliverCanceledWhileWriterWaitsForLockIsNeverStarted(t *testing.T) {
	s, peer := pipeSession(t)
	reached, held := make(chan struct{}), make(chan struct{})
	s.beforeStartLock = func() {
		reached <- struct{}{}
		<-held
		s.beforeStartLock = nil // the writer alone reads it, after this
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := s.queueDelivery(ctx, msgr.MessageData{Type: 98, Front: []byte("canceled")})
	if err != nil {
		t.Fatal(err)
	}
	<-reached
	s.mu.Lock()
	close(held)
	cancel()
	s.mu.Unlock()
	if err := <-r.delivered; !errors.Is(err, ErrNotSent) || !errors.Is(err, context.Canceled) {
		t.Fatal("a request canceled while the writer waited for the lock must be known not sent", err)
	}
	next := make(chan error, 1)
	go func() { next <- s.Deliver(context.Background(), msgr.MessageData{Type: 98, Front: []byte("next")}) }()
	if m := readRequest(t, msgr.NewReader(peer, 0)); string(m.Front) != "next" || m.Sequence != 1 {
		t.Fatal("the canceled request was written, or consumed a sequence", m)
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
}

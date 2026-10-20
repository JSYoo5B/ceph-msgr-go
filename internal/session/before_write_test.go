package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestBeforeWriteRejectsLocallyAndPreservesOtherTraffic(t *testing.T) {
	s, reader, writer := testSession(t)
	blocked := errors.New("object blocked")
	var inspected atomic.Bool
	s.config.BeforeWrite = func(m msgr.MessageData) error {
		// Calling Err would deadlock if the writer held Session.mu here.
		if err := s.Err(); err != nil {
			return err
		}
		inspected.Store(true)
		if m.Type == msgr.OSDOpMessage {
			return blocked
		}
		return nil
	}
	s.Start()
	rejected, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage, Front: []byte("blocked")})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, blocked) || errors.As(err, &unknown) || rejected.Transaction == 0 || s.Err() != nil || s.sent.Load() != 0 || !inspected.Load() {
		t.Fatal("hook rejection was not a local unsent result", rejected, err)
	}
	if err := s.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// One-way rejection is discarded, while the next protocol ACK progresses.
	if err := s.Send(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage}); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), msgr.MessageData{Type: 61, Front: []byte("ack-block")}); err != nil {
		t.Fatal(err)
	}
	ack := readRequest(t, reader)
	if ack.Type != 61 || ack.Sequence != 1 {
		t.Fatal("rejected calls consumed sequence or blocked ACK", ack)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
		done <- err
	}()
	live := readRequest(t, reader)
	if live.Sequence != 2 || string(live.Front) != "live" {
		t.Fatal("unrelated command failed to progress", live)
	}
	reply(t, writer, live, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBeforeWriteSnapshotSurvivesConcurrentCancellationOrFailure(t *testing.T) {
	for _, kind := range []string{"cancel", "fail"} {
		t.Run(kind, func(t *testing.T) {
			s, reader, writer := testSession(t)
			entered, released := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(released) }) }
			t.Cleanup(release)
			preserved := make(chan bool, 1)
			s.config.BeforeWrite = func(m msgr.MessageData) error {
				if string(m.Front) != "paused" {
					return nil
				}
				close(entered)
				<-released
				preserved <- m.Type == msgr.MonCommandMessage && string(m.Front) == "paused" && bytes.Equal(m.Data, []byte{0, 255, 1})
				return errors.New("later hook refusal")
			}
			s.Start()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("paused"), Data: []byte{0, 255, 1}})
				done <- err
			}()
			awaitAdmission(t, entered)
			cause := error(context.Canceled)
			if kind == "fail" {
				cause = io.EOF
				s.Fail(cause)
			}
			cancel()
			var unknown *OutcomeUnknownError
			if err := <-done; !errors.Is(err, cause) || errors.As(err, &unknown) {
				t.Fatal("unstarted hook wait changed first outcome", err)
			}
			release()
			if !<-preserved {
				t.Fatal("cancellation/failure cleared the callback snapshot")
			}
			if kind == "cancel" {
				go func() {
					_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
					done <- err
				}()
				m := readRequest(t, reader)
				if string(m.Front) != "live" || m.Sequence != 1 {
					t.Fatal("canceled callback input was transmitted", m)
				}
				reply(t, writer, m, 1)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			} else {
				s.Wait()
				if s.sent.Load() != 0 {
					t.Fatal("failed session transmitted callback input")
				}
			}
		})
	}
}

func TestBeforeWriteRechecksCancellationAfterHook(t *testing.T) {
	s, reader, writer := testSession(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.config.BeforeWrite = func(m msgr.MessageData) error {
		if string(m.Front) == "canceled-in-hook" {
			cancel()
		}
		return nil
	}
	s.Start()
	_, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("canceled-in-hook")})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
		t.Fatal("hook canceled an unstarted request but outcome is unknown", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("live")})
		done <- err
	}()
	m := readRequest(t, reader)
	if string(m.Front) != "live" || m.Sequence != 1 {
		t.Fatal("context canceled by callback was transmitted", m)
	}
	reply(t, writer, m, 1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBeforeWriteStartedObjectPrecedesQueuedBackoffAck(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	var blocked atomic.Bool
	refusal := errors.New("backoff active")
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 2 * time.Second, BeforeWrite: func(m msgr.MessageData) error {
		if m.Type == msgr.OSDOpMessage && blocked.Load() {
			return refusal
		}
		return nil
	}}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.Call(ctx, msgr.MessageData{Type: msgr.OSDOpMessage, Front: []byte("started-before-block")})
		firstDone <- err
	}()
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	// BLOCK becomes active after transmission started. A cancellation now is
	// uncertain, and the writer must finish this frame before any ACK_BLOCK.
	blocked.Store(true)
	cancel()
	var unknown *OutcomeUnknownError
	if err := <-firstDone; !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
		t.Fatal("started object lost its unknown outcome", err)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage, Front: []byte("queued-after-block")})
		secondDone <- err
	}()
	// The writer remains blocked midway through the first frame, so observing
	// this queue entry deterministically places the second object before ACK.
	select {
	case queued := <-s.queue:
		s.queue <- queued
	case <-time.After(time.Second):
		t.Fatal("object was not queued")
	}
	if err := s.Send(context.Background(), msgr.MessageData{Type: 61, Front: []byte("ack-block")}); err != nil {
		t.Fatal(err)
	}
	reader := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	first, ack := readRequest(t, reader), readRequest(t, reader)
	if first.Type != msgr.OSDOpMessage || string(first.Front) != "started-before-block" || first.Sequence != 1 || ack.Type != 61 || ack.Sequence != 2 {
		t.Fatal("started frame did not precede ACK or blocked object escaped", first, ack)
	}
	if err := <-secondDone; !errors.Is(err, refusal) || errors.As(err, &unknown) {
		t.Fatal("queued object was not refused locally", err)
	}
	if s.Err() != nil {
		t.Fatal("local backoff rejection failed the session", s.Err())
	}
}

func TestBeforeWriteCompletedRejectionWinsLaterCancellation(t *testing.T) {
	s, _, _ := testSession(t)
	rejection := errors.New("first local refusal")
	r := &request{ctx: context.Background(), message: msgr.MessageData{Transaction: 1}, result: make(chan response, 1), completedErr: rejection}
	// The writer has already completed and removed this unstarted request.
	if err := s.abandon(1, r, context.Canceled); !errors.Is(err, rejection) {
		t.Fatal("later cancellation replaced completed local refusal", err)
	}
}

func TestSendControlQueuedCallerCancellationCannotDiscardProtocolMessage(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 2 * time.Second}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	// Hold one started frame so the control remains queued when ctx cancels.
	if err := s.Send(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.SendControl(ctx, msgr.MessageData{Type: 61, Front: []byte("retained-ack")}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := s.Send(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage, Front: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}
	reader := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	first, ack, fresh := readRequest(t, reader), readRequest(t, reader), readRequest(t, reader)
	if string(first.Front) != "first" || ack.Type != 61 || string(ack.Front) != "retained-ack" || string(fresh.Front) != "fresh" || ack.Sequence != first.Sequence+1 || fresh.Sequence != ack.Sequence+1 {
		t.Fatal("caller cancellation dropped ACK or let fresh traffic pass it", first, ack, fresh)
	}
	if err := s.SendControl(ctx, msgr.MessageData{Type: 61}); !errors.Is(err, context.Canceled) {
		t.Fatal("pre-canceled control entered writer queue", err)
	}
}

func TestBeforeWriteLateAckTurnRejectsAheadObjectBeforeOpeningGate(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	var blocked atomic.Bool
	refusal := errors.New("released backoff pending ACK writer turn")
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 2 * time.Second, BeforeWrite: func(m msgr.MessageData) error {
		if m.Type == 61 {
			blocked.Store(false)
			return nil
		}
		if m.Type == msgr.OSDOpMessage && blocked.Load() {
			return refusal
		}
		return nil
	}}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	// Hold the writer in another started frame while both an earlier object
	// and the explicit late ACK enter the queue. Queue admission alone must
	// leave the gate closed for that earlier object.
	if err := s.Send(context.Background(), msgr.MessageData{Type: msgr.TellCommandMessage, Front: []byte("writer-barrier")}); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 16)
	if _, err := io.ReadFull(peer, prefix); err != nil {
		t.Fatal(err)
	}
	blocked.Store(true)
	aheadDone := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage, Front: []byte("ahead")})
		aheadDone <- err
	}()
	select {
	case queued := <-s.queue:
		s.queue <- queued
	case <-time.After(time.Second):
		t.Fatal("ahead object did not enter the queue")
	}
	ackCtx, cancelAck := context.WithCancel(context.Background())
	if err := s.SendControl(ackCtx, msgr.MessageData{Type: 61, Front: []byte("late-ack")}); err != nil {
		t.Fatal(err)
	}
	cancelAck()
	if !blocked.Load() {
		t.Fatal("ACK queue admission opened the writer gate")
	}
	freshDone := make(chan error, 1)
	go func() {
		_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.OSDOpMessage, Front: []byte("fresh")})
		freshDone <- err
	}()
	reader := msgr.NewReader(io.MultiReader(bytes.NewReader(prefix), peer), 0)
	barrier, ack, fresh := readRequest(t, reader), readRequest(t, reader), readRequest(t, reader)
	var unknown *OutcomeUnknownError
	if err := <-aheadDone; !errors.Is(err, refusal) || errors.As(err, &unknown) {
		t.Fatal("ahead object escaped or became unknown", err)
	}
	if string(barrier.Front) != "writer-barrier" || ack.Type != 61 || string(ack.Front) != "late-ack" || fresh.Type != msgr.OSDOpMessage || string(fresh.Front) != "fresh" || ack.Sequence != barrier.Sequence+1 || fresh.Sequence != ack.Sequence+1 {
		t.Fatal("queued object overtook the late ACK", barrier, ack, fresh)
	}
	fresh.Type = msgr.OSDOpReplyMessage
	fresh.Sequence = 1
	fresh.AckSequence = 0
	if err := msgr.NewWriter(peer, 0).Write(fresh.Frame()); err != nil {
		t.Fatal(err)
	}
	if err := <-freshDone; err != nil {
		t.Fatal(err)
	}
}

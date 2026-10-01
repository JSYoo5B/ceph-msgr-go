package session

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestKeepaliveAcknowledgmentsKeepAnIdleSessionAlive(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	config := Config{WriteTimeout: time.Second, KeepaliveInterval: 20 * time.Millisecond, KeepaliveTimeout: 200 * time.Millisecond}
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, config, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	r, w := msgr.NewReader(peer, 0), msgr.NewWriter(peer, 0)
	started := time.Now()
	for time.Since(started) < 3*config.KeepaliveTimeout {
		f, err := r.Read()
		if err != nil || f.Tag != msgr.Keepalive {
			t.Fatal("idle connection did not send a valid probe", f.Tag, err)
		}
		if err := w.Write(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: f.Segments}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage}); finished <- err }()
	readRequest(t, r)
	// Consume outgoing probes without acknowledging them. TCP stays open
	// until the Messenger liveness detector terminates the silent session.
	for {
		if _, err := r.Read(); err != nil {
			break
		}
	}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, ErrKeepaliveTimeout) || !errors.As(err, &unknown) {
			t.Fatal("silent connection lost the started command's outcome", err)
		}
	case <-ctx.Done():
		t.Fatal("silence did not release the command", ctx.Err())
	}
	s.Wait()
}

func TestWriteStallSeparatesStartedAndQueuedOutcomes(t *testing.T) {
	client, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: 200 * time.Millisecond}, nil, nil)
	t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
	s.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := s.Call(ctx, msgr.MessageData{Type: msgr.MonCommandMessage}); finished <- err }()
	if _, err := io.ReadFull(peer, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	// The rest of the first frame blocks. A second request has no opportunity
	// to start transmission, so its local deadline has a known outcome.
	queued, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err := s.Call(queued, msgr.MessageData{Type: msgr.MonCommandMessage})
	stop()
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("queued request was marked uncertain", err)
	}
	select {
	case err := <-finished:
		var timeout net.Error
		if !errors.As(err, &unknown) || !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatal("partial write timeout lost uncertainty or timeout cause", err)
		}
	case <-ctx.Done():
		t.Fatal("write deadline did not terminate the stalled session", ctx.Err())
	}
	s.Wait()
}

package session

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type bulkAllocation [1 << 20]byte
type bulkContextKey struct{}

func queueBulk(s *Session, ctx context.Context) (weak.Pointer[bulkAllocation], <-chan error) {
	payload := new(bulkAllocation)
	ref := weak.Make(payload)
	finished := make(chan error, 1)
	callCtx := context.WithValue(ctx, bulkContextKey{}, payload)
	go func(data []byte, callCtx context.Context) {
		_, err := s.Call(callCtx, msgr.MessageData{Type: msgr.MonCommandMessage, Data: data})
		finished <- err
		close(finished)
	}(payload[:], callCtx)
	return ref, finished
}

func TestCompletedQueuedCallsReleaseBulkMemory(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		name := "cancellation"
		if closeSession {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			client, peer := net.Pipe()
			s := New(&Transport{Conn: client, Reader: msgr.NewReader(client, 0), Writer: msgr.NewWriter(client, 0)}, Config{WriteTimeout: time.Minute}, nil, nil)
			t.Cleanup(func() { s.Fail(ErrClosed); peer.Close(); s.Wait() })
			s.Start()
			first := make(chan error, 1)
			go func() {
				_, err := s.Call(context.Background(), msgr.MessageData{Type: msgr.MonCommandMessage})
				first <- err
			}()
			peer.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := io.ReadFull(peer, make([]byte, 16)); err != nil {
				t.Fatal(err)
			}
			// Hold the first frame's partial write so the bulk call stays in
			// the queue. Its caller must be released with a known outcome.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ref, finished := queueBulk(s, ctx)
			deadline := time.Now().Add(3 * time.Second)
			for len(s.queue) == 0 {
				if time.Now().After(deadline) {
					t.Fatal("bulk call was not queued")
				}
				runtime.Gosched()
			}
			cause := error(context.Canceled)
			if closeSession {
				cause = ErrClosed
				s.Fail(ErrClosed)
				s.Wait()
				<-first
			} else {
				cancel()
			}
			select {
			case err := <-finished:
				var unknown *OutcomeUnknownError
				if !errors.Is(err, cause) || errors.As(err, &unknown) {
					t.Fatal("queued call lost its known outcome", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("queued call was not released")
			}
			for i := 0; i < 5; i++ {
				runtime.GC()
				if ref.Value() == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if ref.Value() != nil {
				t.Error("completed queued call retains its bulk allocation")
			}
			// Keep the session reachable: GC must release the finished call's
			// bytes even while the caller retains the closed or stalled client.
			runtime.KeepAlive(s)
		})
	}
}

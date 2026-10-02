package session

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// Later Close calls return immediately even if the first caller is still
// finishing cleanup. net.Conn permits methods to be invoked concurrently.
type cancellationCleanupConn struct {
	net.Conn
	writeStarted, closing, cleaned, release chan struct{}
	writeOnce                               sync.Once
	closed                                  atomic.Bool
}

func (c *cancellationCleanupConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	return c.Conn.Write(p)
}

func (c *cancellationCleanupConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	err := c.Conn.Close()
	close(c.closing)
	<-c.release
	close(c.cleaned)
	return err
}

// Cancellation deadlines can also arrive through an opaque parent's Done.
// Hide the socket deadline to make the cancellation callback the Close owner,
// independently of the ordering of the context and network deadline timers.
type opaqueDeadlineContext struct{ context.Context }

func (opaqueDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func TestHandshakeWaitsForCancellationCallbackCleanup(t *testing.T) {
	for _, mode := range []string{"cancellation", "context deadline", "transport failure"} {
		t.Run(mode, func(t *testing.T) {
			conn, peer := net.Pipe()
			release := make(chan struct{})
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			held := &cancellationCleanupConn{Conn: conn, writeStarted: make(chan struct{}), closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			t.Cleanup(func() { finish(); held.Close(); peer.Close() })
			ctx := context.Background()
			cancel := func() {}
			want := error(io.ErrClosedPipe)
			switch mode {
			case "cancellation":
				ctx, cancel = context.WithCancel(ctx)
				want = context.Canceled
			case "context deadline":
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				ctx = opaqueDeadlineContext{ctx}
				want = context.DeadlineExceeded
			case "transport failure":
				peer.Close()
			}
			t.Cleanup(cancel)
			finished := make(chan error, 1)
			go func() {
				_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), 4096, time.Second)
				finished <- err
			}()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(time.Second):
					t.Fatal("handshake did not reach the cleanup barrier")
				}
			}
			if mode != "transport failure" {
				await(held.writeStarted)
			}
			if mode == "cancellation" {
				cancel()
			}
			await(held.closing)
			// Demonstrate that repeated Close does not await the held owner.
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			returned := false
			select {
			case err := <-finished:
				returned = true
				t.Error("Handshake returned before connection cleanup", err)
			case <-time.After(20 * time.Millisecond):
			}
			finish()
			if !returned {
				select {
				case err := <-finished:
					if !errors.Is(err, want) {
						t.Fatal("handshake lost its cancellation or transport cause", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Handshake did not finish after connection cleanup")
				}
			}
			await(held.cleaned)
		})
	}
}

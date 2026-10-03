package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
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
				_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), SecureMode, 4096, time.Second)
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

// An opaque parent can deliver its deadline through Done without exposing a
// socket deadline. Trigger that cause after the cleanup gate, so a slow runner
// cannot expire the context before the peer's independent refusal.
type cleanupCauseContext struct{ context.Context }

func (c cleanupCauseContext) Err() error {
	if c.Context.Err() != nil && context.Cause(c.Context) == context.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	return c.Context.Err()
}

type corruptedHelloCRCConn struct {
	net.Conn
	read int
}

func (c *corruptedHelloCRCConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	// The first frame follows the 26-byte banner. Corrupt its preamble CRC.
	const offset = 26 + 28
	if c.read <= offset && offset < c.read+n {
		p[offset-c.read] ^= 1
	}
	c.read += n
	return n, err
}

func TestHandshakePreservesRefusalBeforeCleanupContextEnds(t *testing.T) {
	for _, test := range []struct {
		name            string
		peer            handshakePeerConfig
		want            error
		wantText        string
		authCause       error
		authRejection   bool
		corruptCRC      bool
		exhaustNonce    bool
		expectedID      uint64
		peerReadFailure bool
	}{
		{name: "malformed HELLO", peer: handshakePeerConfig{mode: 2, malformedTag: msgr.Hello}, want: msgr.ErrFrame},
		{name: "explicit auth rejection", authRejection: true},
		{name: "tampered transcript", peer: handshakePeerConfig{mode: 2, badSignature: true}, want: msgr.ErrAuthentication},
		{name: "different connection mode", peer: handshakePeerConfig{mode: 1}, wantText: "server selected connection mode 1, requested 2"},
		{name: "wrong role", peer: handshakePeerConfig{mode: 2, peerRole: 16}, wantText: "unexpected daemon role", peerReadFailure: true},
		{name: "wrong daemon ID", peer: handshakePeerConfig{mode: 2, serverFlags: 1}, expectedID: 99, wantText: "unexpected daemon ID", peerReadFailure: true},
		{name: "unsupported policy", peer: handshakePeerConfig{mode: 2}, want: msgr.ErrFeatures, peerReadFailure: true},
		{name: "corrupt CRC", peer: handshakePeerConfig{mode: 2}, corruptCRC: true, want: msgr.ErrCRC, peerReadFailure: true},
		{name: "exhausted nonce", peer: handshakePeerConfig{mode: 2}, exhaustNonce: true, want: msgr.ErrNonce, peerReadFailure: true},
		{name: "wrapped refusal with pipe error", peer: handshakePeerConfig{mode: 2, authMore: true}, authCause: fmt.Errorf("challenge refused: %w", errors.Join(msgr.ErrFrame, io.ErrClosedPipe)), want: msgr.ErrFrame},
	} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(test.name+"/"+cause.Error(), func(t *testing.T) {
				base, cancel := context.WithCancelCause(context.Background())
				defer cancel(context.Canceled)
				ctx := cleanupCauseContext{base}
				client, peer := net.Pipe()
				release := make(chan struct{})
				var once sync.Once
				finish := func() { once.Do(func() { close(release) }) }
				var connection net.Conn = client
				if test.corruptCRC {
					connection = &corruptedHelloCRCConn{Conn: client}
				}
				held := &cancellationCleanupConn{Conn: connection, writeStarted: make(chan struct{}), closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
				t.Cleanup(func() { finish(); held.Close(); peer.Close() })
				auth := fixtureAuthData()
				if test.exhaustNonce {
					for _, start := range []int{20, 32} {
						for i := start; i < start+8; i++ {
							auth.secret[i] = 0xff
						}
					}
				}
				peerDone := make(chan error, 1)
				go func() { peerDone <- handshakePeer(peer, auth, test.peer) }()
				var method Authenticator = auth
				if test.authCause != nil {
					method = failedProofAuth{fixtureAuth: auth, cause: test.authCause}
				}
				finished := make(chan error, 1)
				go func() {
					_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 1, test.expectedID, method, SecureMode, 4096, 30*time.Second)
					finished <- err
				}()
				select {
				case <-held.closing:
				case <-time.After(5 * time.Second):
					t.Fatal("refusal did not reach its cleanup gate")
				}
				if ctx.Err() != nil {
					t.Fatal("context ended before the independent refusal", ctx.Err())
				}
				select {
				case err := <-peerDone:
					if (err != nil) != test.peerReadFailure {
						t.Fatal("peer did not finish its intended refusal", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("peer did not finish after refusal")
				}
				// Close is already cleaning up an independently selected failure.
				cancel(cause)
				finish()
				select {
				case err := <-finished:
					var rejection *cephx.AuthenticationError
					switch {
					case test.authRejection:
						if !errors.As(err, &rejection) || rejection.Method != 2 || rejection.Code != -13 {
							t.Fatal("prior authentication rejection was overwritten", err)
						}
					case test.wantText != "":
						if err == nil || !strings.Contains(err.Error(), test.wantText) {
							t.Fatal("prior handshake refusal was overwritten", err)
						}
					default:
						if !errors.Is(err, test.want) {
							t.Fatal("prior protocol refusal was overwritten", err)
						}
					}
					if errors.Is(err, cause) {
						t.Fatal("later context termination replaced the refusal", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("handshake did not finish cleanup")
				}
				select {
				case <-held.cleaned:
				default:
					t.Fatal("handshake returned before cleanup completed")
				}
			})
		}
	}
}

func TestHandshakeTransportFailureMapsContextDuringCleanup(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			base, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			ctx := cleanupCauseContext{base}
			client, peer := net.Pipe()
			peer.Close()
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			held := &cancellationCleanupConn{Conn: client, writeStarted: make(chan struct{}), closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			t.Cleanup(func() { finish(); held.Close() })
			finished := make(chan error, 1)
			go func() {
				_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), SecureMode, 4096, 30*time.Second)
				finished <- err
			}()
			select {
			case <-held.closing:
			case <-time.After(5 * time.Second):
				t.Fatal("transport failure did not reach cleanup")
			}
			if ctx.Err() != nil {
				t.Fatal("context ended before transport failure cleanup")
			}
			cancel(cause)
			finish()
			select {
			case err := <-finished:
				if !errors.Is(err, cause) {
					t.Fatal("transport failure lost its context mapping", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("transport cleanup did not finish")
			}
		})
	}
}

type clearingDeadlineConn struct {
	net.Conn
	clearing, release chan struct{}
}

func (c clearingDeadlineConn) SetDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		close(c.clearing)
		<-c.release
	}
	return c.Conn.SetDeadline(deadline)
}

func TestHandshakeSuccessfulSetupCanceledBeforePublication(t *testing.T) {
	client, peer := net.Pipe()
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	held := clearingDeadlineConn{Conn: client, clearing: make(chan struct{}), release: release}
	t.Cleanup(func() { finish(); client.Close(); peer.Close() })
	auth := fixtureAuthData()
	peerDone := make(chan error, 1)
	go func() { peerDone <- handshakePeer(peer, auth, handshakePeerConfig{mode: 2, serverFlags: 1}) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := Handshake(ctx, held, msgr.Address{Type: 2}, 1, 0, auth, SecureMode, 4096, 30*time.Second)
		finished <- err
	}()
	select {
	case <-held.clearing:
	case <-time.After(5 * time.Second):
		t.Fatal("successful setup did not reach publication gate")
	}
	if ctx.Err() != nil {
		t.Fatal("context ended before the successful setup gate")
	}
	cancel()
	finish()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled successful setup was accepted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled setup did not return")
	}
	select {
	case err := <-peerDone:
		if err == nil {
			t.Fatal("canceled successful connection remained open")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled successful connection was not closed")
	}
}

func TestSetupRetryAndCleanupTransportPolicies(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		retry     bool
		transport bool
	}{
		{name: "nil"},
		{name: "EOF", err: io.EOF, retry: true, transport: true},
		{name: "truncated transport", err: io.ErrUnexpectedEOF, retry: true, transport: true},
		{name: "closed socket", err: net.ErrClosed, retry: true, transport: true},
		{name: "endpoint deadline", err: context.DeadlineExceeded, retry: true, transport: true},
		{name: "caller cancellation", err: context.Canceled, transport: true},
		{name: "closed pipe", err: io.ErrClosedPipe, transport: true},
		{name: "short write", err: io.ErrShortWrite, transport: true},
		{name: "network wrapper", err: &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}, retry: true, transport: true},
		{name: "joined local IO", err: fmt.Errorf("setup: %w", errors.Join(io.EOF, io.ErrClosedPipe)), transport: true},
		{name: "joined refusal", err: fmt.Errorf("setup: %w", errors.Join(msgr.ErrFrame, io.ErrClosedPipe))},
		{name: "explicit auth rejection", err: &cephx.AuthenticationError{Method: 2, Code: -13}},
		{name: "plain refusal", err: errors.New("ceph messenger: compression unsupported")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := RetryableSetupError(test.err); got != test.retry {
				t.Fatal("bootstrap retry policy changed", got, test.err)
			}
			if got := setupTransportError(test.err, true); got != test.transport {
				t.Fatal("cleanup misclassified its transport cause", got, test.err)
			}
		})
	}
	for _, refusal := range []error{msgr.ErrFrame, msgr.ErrCRC, msgr.ErrAborted, msgr.ErrNonce, msgr.ErrAuthentication, msgr.ErrFeatures, wire.ErrLimit, wire.ErrVersion, cephx.ErrIntegrity, cephx.ErrKey, cephx.ErrTicket} {
		if RetryableSetupError(refusal) || setupTransportError(refusal, true) {
			t.Error("protocol or authentication refusal was classified as transport", refusal)
		}
	}
}

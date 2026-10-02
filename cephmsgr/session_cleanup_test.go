package cephmsgr

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func cleanupClient(t *testing.T, mgr, malformed bool) (*Client, context.Context, *managerCleanupConn, func(), *atomic.Uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	options := mockOptions(t, 20, [16]byte{1})
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	var wrapped atomic.Bool
	connections := make(chan *managerCleanupConn, 1)
	commands := new(atomic.Uint32)
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, peer := net.Pipe()
		role, id := uint8(1), uint64(0)
		targetMgr := strings.HasSuffix(endpoint, ":6800")
		if targetMgr {
			role, id = 16, 99
		}
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: role, id: id,
			command: func(msgr.MessageData) { commands.Add(1) },
			reply: func(m *msgr.MessageData) {
				if malformed {
					m.Front = m.Front[:len(m.Front)-1]
				}
			},
		})
		if targetMgr == mgr && wrapped.CompareAndSwap(false, true) {
			held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			connections <- held
			return held, nil
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	t.Cleanup(func() {
		finish()
		if c != nil {
			c.Close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if mgr {
		if err := c.WaitMgrReady(ctx); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case held := <-connections:
		return c, ctx, held, finish, commands
	case <-ctx.Done():
		t.Fatal("target connection was not established", ctx.Err())
		return nil, nil, nil, nil, nil
	}
}

func awaitCleanup(t *testing.T, ctx context.Context, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatal("connection cleanup did not reach its barrier", ctx.Err())
	}
}

func TestConcurrentCloseWaitsForEstablishedConnectionCleanup(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			c, ctx, held, finish, _ := cleanupClient(t, mgr, false)
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- c.Close() }()
			awaitCleanup(t, ctx, held.closing)
			go func() { second <- c.Close() }()
			secondReturned := false
			select {
			case err := <-second:
				secondReturned = true
				t.Error("concurrent Close returned before connection cleanup", err)
			case <-time.After(20 * time.Millisecond):
			}
			finish()
			for _, done := range []<-chan error{first, second} {
				if secondReturned && done == second {
					continue
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("Close did not finish after cleanup", ctx.Err())
				}
			}
			awaitCleanup(t, ctx, held.cleaned)
		})
	}
}

func TestCloseWaitsForMalformedReplyConnectionCleanup(t *testing.T) {
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			c, ctx, held, finish, commands := cleanupClient(t, mgr, true)
			call := c.MonCommand
			if mgr {
				call = c.MgrCommand
			}
			input := []byte{0, 255, 1, 0}
			type commandResult struct {
				result Result
				err    error
			}
			called := make(chan commandResult, 1)
			go func() {
				result, err := call(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`), Input: input})
				called <- commandResult{result, err}
			}()
			// The application caller owns Fail here, after decoding a complete
			// malformed response. Workers can exit while its Close is blocked.
			awaitCleanup(t, ctx, held.closing)
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			closeReturned := false
			select {
			case err := <-closed:
				closeReturned = true
				t.Error("Close returned before malformed response connection cleanup", err)
			case <-time.After(20 * time.Millisecond):
			}
			finish()
			if !closeReturned {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("Close did not finish after response cleanup", ctx.Err())
				}
			}
			awaitCleanup(t, ctx, held.cleaned)
			select {
			case result := <-called:
				var unknown *OutcomeUnknownError
				if !errors.As(result.err, &unknown) || !errors.Is(result.err, ErrMalformedMessage) || !errors.Is(result.err, io.ErrUnexpectedEOF) || !bytes.Equal(result.result.Data, input) || commands.Load() != 1 {
					t.Fatal("malformed mutation lost its output or uncertain outcome, or was replayed", result.result, result.err, commands.Load())
				}
			case <-ctx.Done():
				t.Fatal("mutation caller remained after cleanup", ctx.Err())
			}
		})
	}
}

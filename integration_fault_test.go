package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Drops receive bytes only after authentication. The daemon can still execute
// a submitted command; closing the wrapper releases the blocked read worker.
type lostReplyConn struct {
	net.Conn
	armed         *atomic.Bool
	signalBlocked func()
	closed        chan struct{}
	closeOnce     sync.Once
}

func (c *lostReplyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.armed.Load() {
		c.signalBlocked()
		<-c.closed
		return 0, net.ErrClosed
	}
	return n, err
}

func (c *lostReplyConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func TestCephLostMutationReplyIntegration(t *testing.T) {
	// fixtureClient checks the control directory before any mutation.
	observer, ctx := fixtureClient(t, 45*time.Second)
	options := integrationOptions(t)
	var armed atomic.Bool
	blocked := make(chan struct{})
	var blockOnce sync.Once
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		wrapped := &lostReplyConn{Conn: conn, armed: &armed, closed: make(chan struct{})}
		// A short initial ticket may renew between bootstrap and the
		// command. Observe receive loss across this client's connections.
		wrapped.signalBlocked = func() { blockOnce.Do(func() { close(blocked) }) }
		return wrapped, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	armed.Store(true)
	key := fmt.Sprintf("native-lost-reply-%d", time.Now().UnixNano())
	input := []byte("server execution survives a canceled local wait")
	encoded, _ := json.Marshal(map[string]string{"prefix": "config-key set", "key": key})
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(callCtx, Command{JSON: encoded, Input: input})
		finished <- err
	}()
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("mutation finished before receive fault", err)
	case <-ctx.Done():
		t.Fatal("receive fault did not activate")
	}
	read, _ := json.Marshal(map[string]string{"prefix": "config-key get", "key": key})
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := observer.MonCommand(ctx, Command{JSON: read})
		if err == nil {
			if !bytes.Equal(result.Data, input) {
				t.Fatal("server mutation has incorrect data", string(result.Data))
			}
			break
		}
		var server *CommandError
		var uncertain *OutcomeUnknownError
		var connection *net.OpError
		missing := errors.As(err, &server) && server.Code == -2
		transient := errors.As(err, &uncertain) || errors.As(err, &connection) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
		if !missing && !transient {
			t.Fatal("independent client could not observe the server mutation", err, string(result.Data))
		}
		select {
		case err := <-finished:
			t.Fatal("mutation finished while receive bytes were blocked", err)
		case <-ctx.Done():
			t.Fatal("server mutation did not become visible", ctx.Err())
		case <-ticker.C:
		}
	}
	// Observe server execution before canceling the local wait. Only the read
	// probe is repeated; the mutation is submitted exactly once.
	cancel()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) {
			t.Fatal("lost response did not preserve uncertain mutation", err)
		}
	case <-ctx.Done():
		t.Fatal("canceling the local wait did not release the caller", ctx.Err())
	}
	if err := c.Close(); err != nil {
		t.Fatal("shutdown with blocked receive", err)
	}
	t.Log("server applied mutation while the caller received OutcomeUnknownError")
}

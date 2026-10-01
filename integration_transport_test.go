package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	observer, ctx := fixtureClient(t, 30*time.Second)
	probe, _ := json.Marshal(map[string]string{"prefix": "config-key set", "key": "native-receive-fault-probe"})
	if _, err := observer.MonCommand(ctx, Command{JSON: probe, Input: []byte("server execution survives a canceled local wait")}); err != nil {
		t.Fatal("small unaligned input before receive fault", err)
	}
	options := integrationOptions(t)
	var armed atomic.Bool
	blocked := make(chan struct{})
	var blockOnce sync.Once
	dialer := &net.Dialer{}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, endpoint)
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
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = c.MonCommand(callCtx, Command{JSON: encoded, Input: input})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("lost response did not preserve uncertain mutation", err)
	}
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("receive fault did not activate")
	}
	encoded, _ = json.Marshal(map[string]string{"prefix": "config-key get", "key": key})
	result, err := observer.MonCommand(ctx, Command{JSON: encoded})
	if err != nil || !bytes.Equal(result.Data, input) {
		t.Fatal("independent client did not observe the server mutation", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("shutdown with blocked receive", err)
	}
	t.Log("server applied mutation while the caller received OutcomeUnknownError")
}

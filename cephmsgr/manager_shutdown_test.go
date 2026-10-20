package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type managerCleanupConn struct {
	net.Conn
	closing, cleaned, release chan struct{}
	once                      sync.Once
}

func (c *managerCleanupConn) Close() error {
	c.once.Do(func() {
		c.Conn.Close() // Unblock I/O before finishing the connection's cleanup.
		close(c.closing)
		<-c.release
		close(c.cleaned)
	})
	return nil
}

func TestCloseWaitsForManagerHandshakeConnectionCleanup(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	conn, peer := net.Pipe()
	release := make(chan struct{})
	var releaseOnce sync.Once
	finishCleanup := func() { releaseOnce.Do(func() { close(release) }) }
	held := &managerCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
	started := make(chan struct{})
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") {
			close(started)
			return held, nil
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	t.Cleanup(func() {
		finishCleanup()
		peer.Close()
		held.Close()
		if c != nil {
			c.Close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
		called <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-held.closing:
	case <-ctx.Done():
		t.Fatal("Close did not cancel MGR handshake I/O", ctx.Err())
	}
	select {
	case err := <-closed:
		t.Fatal("Close returned before MGR handshake connection cleanup", err)
	case <-time.After(20 * time.Millisecond):
	}
	finishCleanup()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish after handshake cleanup", ctx.Err())
	}
	select {
	case <-held.cleaned:
	default:
		t.Fatal("connection cleanup outlived Close")
	}
	select {
	case err := <-called:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("unsubmitted MGR call lost its known closed outcome", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not release MGR setup caller", ctx.Err())
	}
}

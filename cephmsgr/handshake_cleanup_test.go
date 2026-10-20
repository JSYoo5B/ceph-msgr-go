package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type detachedHandshakeCleanupConn struct {
	net.Conn
	closing, cleaned, release chan struct{}
	closed                    atomic.Bool
}

func (c *detachedHandshakeCleanupConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	err := c.Conn.Close()
	close(c.closing)
	<-c.release
	close(c.cleaned)
	return err
}

func TestConcurrentCloseWaitsForHandshakeCancellationCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	conn, peer := net.Pipe()
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	held := &detachedHandshakeCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
	started := make(chan struct{})
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") {
			close(started)
			return held, nil
		}
		return dial(ctx, network, endpoint)
	}
	c, err := Dial(ctx, options)
	t.Cleanup(func() {
		finish()
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
		_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`)})
		called <- err
	}()
	awaitCleanup(t, ctx, started)
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- c.Close() }()
	awaitCleanup(t, ctx, held.closing)
	go func() { second <- c.Close() }()
	returned := make(map[<-chan error]bool)
	for _, done := range []<-chan error{first, second} {
		select {
		case err := <-done:
			returned[done] = true
			t.Error("Close returned before handshake cancellation callback cleanup", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	finish()
	for _, done := range []<-chan error{first, second} {
		if returned[done] {
			continue
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("Close did not finish after handshake cleanup", ctx.Err())
		}
	}
	awaitCleanup(t, ctx, held.cleaned)
	select {
	case err := <-called:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("unsubmitted mutation lost its known closed outcome", err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not release MGR handshake caller", ctx.Err())
	}
}

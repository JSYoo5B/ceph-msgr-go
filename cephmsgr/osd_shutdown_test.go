package cephmsgr

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestOSDConcurrentClientCloseWaitsForHandshakeCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableOSD = true
	conn, peer := net.Pipe()
	release := make(chan struct{})
	var releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	held := &detachedHandshakeCleanupConn{Conn: conn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if endpoint == "192.0.2.1:6804" {
			return held, nil
		}
		client, server := net.Pipe()
		go mockDaemon(server, peerConfig{role: 1, release: 20, fsid: [16]byte{1}, services: cephx.ServiceAuth | cephx.ServiceMgr | cephx.ServiceOSD})
		return client, nil
	}
	var c *Client
	t.Cleanup(func() {
		finish()
		peer.Close()
		held.Close()
		if c != nil {
			c.Close()
		}
	})
	var err error
	c, err = Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	// Read the client's banner but never send the peer banner. This barrier
	// proves the OSD handshake reached transport I/O before shutdown starts.
	bannerRead := make(chan error, 1)
	go func() {
		_, err := msgr.ReadBanner(peer, msgr.Banner{Supported: msgr.Revision1 | msgr.Compression, Required: msgr.Revision1})
		bannerRead <- err
	}()
	type openResult struct {
		handle *OSDConnection
		err    error
	}
	opened := make(chan openResult, 1)
	go func() {
		handle, err := c.OpenOSD(ctx, OSDTarget{Address: "192.0.2.1:6804"})
		opened <- openResult{handle, err}
	}()
	select {
	case err := <-bannerRead:
		if err != nil {
			t.Fatal("OSD handshake did not reach the banner barrier", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- c.Close() }()
	awaitCleanup(t, ctx, held.closing)
	go func() { second <- c.Close() }()
	returned := make(map[<-chan error]bool)
	for _, done := range []<-chan error{first, second} {
		select {
		case err := <-done:
			returned[done] = true
			t.Error("Client.Close returned before OSD handshake cleanup", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	select {
	case result := <-opened:
		t.Error("OpenOSD returned before cancellation cleanup", result.err)
		opened <- result
	default:
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
			t.Fatal("Client.Close did not finish after OSD cleanup", ctx.Err())
		}
	}
	awaitCleanup(t, ctx, held.cleaned)
	select {
	case result := <-opened:
		var unknown *OutcomeUnknownError
		if result.handle != nil || !errors.Is(result.err, ErrClosed) || errors.As(result.err, &unknown) {
			t.Fatal("unsubmitted OSD setup lost its known closed outcome", result.err)
		}
	case <-ctx.Done():
		t.Fatal("Client.Close did not release OSD setup", ctx.Err())
	}
	c.mu.Lock()
	remaining := len(c.osds)
	c.mu.Unlock()
	if remaining != 0 {
		t.Fatal("failed OSD setup retained a connection slot", remaining)
	}
}

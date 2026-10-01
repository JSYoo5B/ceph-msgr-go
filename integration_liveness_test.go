package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCephManagerReceiveStallIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires a disposable fixture")
	}
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.KeepaliveInterval = time.Second
	options.KeepaliveTimeout = 3 * time.Second
	var armed, wrapped atomic.Bool
	blocked := make(chan struct{})
	dial := options.DialContext
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(endpoint)
		if err == nil && (port == "36800" || port == "36801") && wrapped.CompareAndSwap(false, true) {
			return &lostReplyConn{Conn: conn, armed: &armed, closed: make(chan struct{}), signalBlocked: func() { close(blocked) }}, nil
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	command := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if _, err := c.MgrCommand(ctx, command); err != nil {
		t.Fatal("establish MGR", err)
	}
	c.mu.Lock()
	old := c.mgr
	c.mu.Unlock()
	// No application command is needed to keep the idle MGR alive. Actual
	// Ceph keepalive acknowledgments must sustain it beyond the timeout.
	select {
	case <-time.After(4 * time.Second):
	case <-old.Done():
		t.Fatal("healthy idle MGR timed out", old.Err())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	armed.Store(true)
	short, stop := context.WithCancel(ctx)
	defer stop()
	finished := make(chan error, 1)
	go func() { _, err := c.MgrCommand(short, command); finished <- err }()
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("call ended before receiving blocked server bytes", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	select {
	case err = <-finished:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) {
		t.Fatal("receive stall lost the canceled command's uncertain outcome", err)
	}
	// An operation's deadline must not terminate an otherwise shared MON.
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("MGR receive stall affected MON command", err)
	}
	_, err = c.MgrCommand(ctx, command)
	if !errors.Is(err, ErrKeepaliveTimeout) || !errors.As(err, &unknown) {
		t.Fatal("silent established session did not preserve its liveness cause", err)
	}
	result, err = c.MgrCommand(ctx, command)
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("fresh MGR session did not recover", err)
	}
	c.mu.Lock()
	fresh := c.mgr
	c.mu.Unlock()
	if fresh == old || !errors.Is(old.Err(), ErrKeepaliveTimeout) {
		t.Fatal("stalled MGR was reused", old.Err())
	}
	c.Close()
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Errorf("workers remain after receive stall: before=%d after=%d", baseline, after)
	}
	t.Log("idle keepalive, local cancellation, silence detection and fresh MGR recovery passed")
}

// A standard net.Pipe supplies real write deadlines while a Go relay speaks
// to the actual Ceph daemon. Pausing the relay mid-frame blocks subsequent
// client writes without needing privileged host networking or native tools.
type writeStallRelay struct {
	net.Conn
	peer, remote    net.Conn
	armed           atomic.Bool
	blocked, closed chan struct{}
	once            sync.Once
	wg              sync.WaitGroup
}

func newWriteStallRelay(remote net.Conn) *writeStallRelay {
	client, peer := net.Pipe()
	p := &writeStallRelay{Conn: client, peer: peer, remote: remote, blocked: make(chan struct{}), closed: make(chan struct{})}
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		defer p.Close()
		buffer := make([]byte, 4096)
		for {
			n, err := peer.Read(buffer)
			if n > 0 {
				// Let small ACK/probe writes pass so the fault specifically
				// pauses the large command's frame, not an earlier control.
				if p.armed.Load() && n == len(buffer) {
					close(p.blocked)
					<-p.closed
					return
				}
				if _, err := io.CopyN(remote, bytes.NewReader(buffer[:n]), int64(n)); err != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	go func() { defer p.wg.Done(); defer p.Close(); io.Copy(peer, remote) }()
	return p
}

func (p *writeStallRelay) Close() error {
	p.once.Do(func() {
		close(p.closed)
		p.Conn.Close()
		p.peer.Close()
		p.remote.Close()
	})
	return nil
}

func TestCephMonitorWriteStallIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires a disposable fixture")
	}
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.ConnectTimeout = 2 * time.Second
	dial := options.DialContext
	var wrapped atomic.Bool
	var relay *writeStallRelay
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		if wrapped.CompareAndSwap(false, true) {
			relay = newWriteStallRelay(conn)
			return relay, nil
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	if relay != nil {
		defer func() { relay.Close(); relay.wg.Wait() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("establish MON through relay", err)
	}
	relay.armed.Store(true)
	key := fmt.Sprintf("native-write-stall-%d", time.Now().UnixNano())
	encoded, _ := json.Marshal(map[string]string{"prefix": "config-key set", "key": key})
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(ctx, Command{JSON: encoded, Input: make([]byte, 40<<10)})
		finished <- err
	}()
	select {
	case <-relay.blocked:
	case err := <-finished:
		t.Fatal("mutation ended before the write fault", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = c.MonCommand(short, Command{JSON: []byte(`{"prefix":"status"}`)})
	stop()
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("untransmitted queued request was marked uncertain", err)
	}
	select {
	case err := <-finished:
		var timeout net.Error
		if !errors.As(err, &unknown) || !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatal("partial write lost its uncertain timeout", err)
		}
	case <-ctx.Done():
		t.Fatal("write deadline did not release the mutation", ctx.Err())
	}
	read, _ := json.Marshal(map[string]string{"prefix": "config-key get", "key": key})
	result, err := c.MonCommand(ctx, Command{JSON: read})
	var server *CommandError
	if !errors.As(err, &server) || server.Code != -2 || result.Code != -2 {
		t.Fatal("recovery replayed the stalled mutation or failed to reconnect", err)
	}
	c.Close()
	relay.wg.Wait()
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Errorf("workers remain after write stall: before=%d after=%d", baseline, after)
	}
	t.Log("partial write timeout, known queued cancellation and fresh MON recovery passed")
}

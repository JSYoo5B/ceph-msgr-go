package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// seedPathFault affects only this client's fixture MON-a TCP path. It leaves
// daemon processes and quorum running and delegates every allowed dial to the
// original fixture dialer (or the default Go dialer).
type seedPathFault struct {
	mu          sync.Mutex
	dial        func(context.Context, string, string) (net.Conn, error)
	latest      net.Conn
	blocked     bool
	successful  int
	beforeFault int
	rejected    int
}

type seedPathFaultStats struct {
	Blocked     bool
	Successful  int
	BeforeFault int
	Rejected    int
}

func installSeedPathFault(options *cephmsgr.Options) *seedPathFault {
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	f := &seedPathFault{dial: dial}
	options.DialContext = f.dialContext
	return f
}

func seedPathBlocked(ctx context.Context, network string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &net.OpError{Op: "dial", Net: network, Err: io.EOF}
}

func (f *seedPathFault) dialContext(ctx context.Context, network, endpoint string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_, port, err := net.SplitHostPort(endpoint)
	isSeed := err == nil && port == "33300"
	f.mu.Lock()
	blocked := isSeed && f.blocked
	if blocked {
		f.rejected++
	}
	f.mu.Unlock()
	if blocked {
		return seedPathBlocked(ctx, network)
	}
	conn, err := f.dial(ctx, network, endpoint)
	if err != nil || !isSeed {
		return conn, err
	}
	f.mu.Lock()
	if f.blocked {
		// A dial started before the fault must not win after the blacklist.
		f.rejected++
		f.mu.Unlock()
		conn.Close()
		return seedPathBlocked(ctx, network)
	}
	f.latest = conn
	f.successful++
	f.mu.Unlock()
	return conn, nil
}

func (f *seedPathFault) interrupt() error {
	f.mu.Lock()
	if f.blocked {
		f.mu.Unlock()
		return fmt.Errorf("fixture seed path is already blocked")
	}
	f.blocked = true // Blacklist new dials before closing the recorded socket.
	f.beforeFault = f.successful
	conn := f.latest
	f.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("no successful sole-seed TCP connection was recorded")
	}
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (f *seedPathFault) cleanupAfterClientClose(t *testing.T, c *cephmsgr.Client) {
	t.Helper()
	t.Cleanup(func() {
		// This cleanup is independent of an expired test context and needs no
		// daemon-control IPC. Close joins workers before the seed is restored.
		if err := c.Close(); err != nil {
			t.Error("close client before restoring its seed path", err)
		}
		f.mu.Lock()
		f.blocked = false
		f.mu.Unlock()
	})
}

func (f *seedPathFault) stats() seedPathFaultStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return seedPathFaultStats{f.blocked, f.successful, f.beforeFault, f.rejected}
}

func (f *seedPathFault) assertHeld(t *testing.T) seedPathFaultStats {
	t.Helper()
	stats := f.stats()
	if !stats.Blocked || stats.Successful != stats.BeforeFault || stats.Rejected == 0 {
		t.Fatal("sole-seed blacklist was not sustained through recovery and Close", stats)
	}
	return stats
}

type seedPathProbeConn struct {
	net.Conn
	closed  atomic.Bool
	onClose func()
}

func (c *seedPathProbeConn) Close() error {
	c.onClose()
	c.closed.Store(true)
	return c.Conn.Close()
}

type seedPathProbeContext struct {
	context.Context
	onErr func()
}

func (ctx seedPathProbeContext) Err() error {
	ctx.onErr()
	return ctx.Context.Err()
}

func TestSeedPathFaultBlocksLateDialAndClosesLatestOutsideLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var fault *seedPathFault
	var peers []*seedPathProbeConn
	for i := 0; i < 4; i++ {
		local, remote := net.Pipe()
		t.Cleanup(func() { local.Close(); remote.Close() })
		peers = append(peers, &seedPathProbeConn{Conn: local, onClose: func() { fault.stats() }})
	}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	options := cephmsgr.Options{DialContext: func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		index := int(calls.Add(1)) - 1
		if index == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return peers[index], nil
	}}
	fault = installSeedPathFault(&options)
	for i := 0; i < 2; i++ {
		if conn, err := options.DialContext(ctx, "tcp", "[::ffff:7f00:1]:33300"); err != nil || conn != peers[i] {
			t.Fatal("original seed dialer changed", conn, err)
		}
	}
	late := make(chan error, 1)
	go func() {
		conn, err := options.DialContext(ctx, "tcp", "[::ffff:7f00:1]:33300")
		if conn != nil || !errors.Is(err, io.EOF) {
			late <- fmt.Errorf("late successful dial escaped the blacklist: conn=%v err=%v", conn, err)
			return
		}
		late <- nil
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("late delegated dial did not start", ctx.Err())
	}
	interrupted := make(chan error, 1)
	go func() { interrupted <- fault.interrupt() }()
	select {
	case err := <-interrupted:
		if err != nil || peers[0].closed.Load() || !peers[1].closed.Load() {
			t.Fatal("fault did not close only the latest seed outside its lock", err)
		}
	case <-ctx.Done():
		t.Fatal("socket Close could not reenter fault stats", ctx.Err())
	}
	close(release)
	select {
	case err := <-late:
		if err != nil || !peers[2].closed.Load() {
			t.Fatal("late seed dial was not closed before rejection", err)
		}
	case <-ctx.Done():
		t.Fatal("late dial did not complete", ctx.Err())
	}
	// A user context callback can acquire the helper lock at both Err checks.
	checked := make(chan error, 1)
	go func() {
		_, err := options.DialContext(seedPathProbeContext{ctx, func() { fault.stats() }}, "tcp", "127.0.0.1:33300")
		checked <- err
	}()
	select {
	case err := <-checked:
		var op *net.OpError
		if !errors.As(err, &op) || op.Op != "dial" || op.Net != "tcp" || !errors.Is(err, io.EOF) {
			t.Fatal("blocked seed lost retryable transport classification", err)
		}
	case <-ctx.Done():
		t.Fatal("context Err could not reenter fault stats", ctx.Err())
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := options.DialContext(canceled, "tcp", "127.0.0.1:33300"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled seed dial lost context error", err)
	}
	if conn, err := options.DialContext(ctx, "tcp", "127.0.0.1:33301"); err != nil || conn != peers[3] {
		t.Fatal("learned peer was not delegated to the original dialer", conn, err)
	}
	if stats := fault.assertHeld(t); stats.Successful != 2 || stats.BeforeFault != 2 || stats.Rejected != 2 || calls.Load() != 4 {
		t.Fatal("seed fault tracking changed", stats, calls.Load())
	}
}

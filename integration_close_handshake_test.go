package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCephCloseDuringManagerHandshakeIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, 30*time.Second)
	query := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if _, err := observer.MgrCommand(ctx, query); err != nil {
		t.Fatal("establish independent MON/MGR observer", err)
	}
	baseline := fixtureFDCount()
	options := integrationOptions(t)
	dial := options.DialContext
	var armed atomic.Bool
	armed.Store(true)
	blocked, release := make(chan struct{}), make(chan struct{})
	var blockOnce, releaseOnce sync.Once
	finishCleanup := func() { releaseOnce.Do(func() { close(release) }) }
	connections := make(chan *managerCleanupConn, 1)
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		_, port, _ := net.SplitHostPort(endpoint)
		if port == "36800" || port == "36801" {
			stalled := &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
			held := &managerCleanupConn{Conn: stalled, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			connections <- held
			return held, nil
		}
		return conn, nil
	}
	c, err := Dial(ctx, options)
	t.Cleanup(func() {
		finishCleanup()
		if c != nil {
			c.Close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan error, 1)
	go func() { _, err := c.MgrCommand(ctx, query); called <- err }()
	var held *managerCleanupConn
	select {
	case held = <-connections:
	case err := <-called:
		t.Fatal("MGR command ended before opening its handshake connection", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-blocked:
	case err := <-called:
		t.Fatal("MGR handshake ended before actual server receive bytes were withheld", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-held.closing:
	case <-ctx.Done():
		t.Fatal("Close did not cancel the actual MGR handshake socket", ctx.Err())
	}
	select {
	case err := <-closed:
		t.Fatal("Close returned before the actual handshake's connection cleanup", err)
	case <-time.After(20 * time.Millisecond):
	}
	finishCleanup()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish after releasing connection cleanup", ctx.Err())
	}
	select {
	case <-held.cleaned:
	default:
		t.Fatal("actual handshake connection cleanup outlived Close")
	}
	select {
	case err := <-called:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("unsubmitted MGR command lost its known closed outcome", err)
		}
	case <-ctx.Done():
		t.Fatal("Close left the handshake caller waiting", ctx.Err())
	}
	result, err := observer.MgrCommand(ctx, query)
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("closing the partial handshake affected the independent client", err)
	}
	final := fixtureFDCount()
	if baseline >= 0 && final > baseline+2 {
		t.Fatal("file descriptors remained after handshake Close", baseline, final)
	}
	t.Logf("actual MGR handshake canceled before command submission; connection cleanup completed before Close, fd-before=%d fd-after=%d", baseline, final)
}

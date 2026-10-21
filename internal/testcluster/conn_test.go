package testcluster

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestLostReplyConnDiscardedWriteNeverReachesPeer(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	var armed, drop atomic.Bool
	drop.Store(true)
	var discarded atomic.Int32
	c := &LostReplyConn{Conn: local, Armed: &armed, DropWrites: &drop,
		SignalWriteDropped: func() { discarded.Add(1) }, Closed: make(chan struct{})}
	defer c.Close()
	if n, err := c.Write([]byte("discarded")); err != nil || n != 9 || discarded.Load() != 1 {
		t.Fatal("fault did not report the local write", n, err)
	}
	peer.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if n, err := peer.Read(make([]byte, 16)); n != 0 || err == nil {
		t.Fatal("discarded bytes reached the peer", n, err)
	}
}

func TestLostReplyConnCloseReleasesWithheldRead(t *testing.T) {
	local, peer := net.Pipe()
	defer peer.Close()
	var armed atomic.Bool
	armed.Store(true)
	blocked, result := make(chan struct{}), make(chan error, 1)
	c := &LostReplyConn{Conn: local, Armed: &armed, Closed: make(chan struct{}),
		SignalBlocked: func() { close(blocked) }}
	defer c.Close()
	go func() { _, err := c.Read(make([]byte, 16)); result <- err }()
	if _, err := peer.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("read fault did not activate")
	}
	select {
	case err := <-result:
		t.Fatal("withheld read returned early", err)
	default:
	}
	c.Close()
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal("withheld read lost close cause", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not release the read")
	}
}

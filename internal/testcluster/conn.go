package testcluster

import (
	"net"
	"sync"
	"sync/atomic"
)

// LostReplyConn stops receive bytes when Armed; mutation tests arm it after authentication.
// Closing the wrapper releases the blocked read worker.
type LostReplyConn struct {
	net.Conn
	Armed         *atomic.Bool
	SignalBlocked func()
	// DropWrites discards outgoing bytes while reporting a successful local
	// write. It never proves peer receipt. Arm only after authentication and
	// disable automatic beacons when isolating a service close.
	DropWrites         *atomic.Bool
	SignalWriteDropped func()
	Closed             chan struct{}
	closeOnce          sync.Once
}

func (c *LostReplyConn) Write(p []byte) (int, error) {
	if len(p) > 0 && c.DropWrites != nil && c.DropWrites.Load() {
		if c.SignalWriteDropped != nil {
			c.SignalWriteDropped()
		}
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *LostReplyConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.Armed.Load() {
		c.SignalBlocked()
		<-c.Closed
		return 0, net.ErrClosed
	}
	return n, err
}

func (c *LostReplyConn) Close() error {
	c.closeOnce.Do(func() { close(c.Closed) })
	return c.Conn.Close()
}

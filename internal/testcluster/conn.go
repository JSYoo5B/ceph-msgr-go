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
	Closed        chan struct{}
	closeOnce     sync.Once
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

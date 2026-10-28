package cephmsgr

import "context"

// beginManagerSetup serializes command and service connection setup. An
// admitted setup belongs to the client before its session is attached, so
// Close must wait until even a canceled handshake has cleaned up its connection.
// The caller releases exactly once after all setup cleanup has completed.
func (c *Client) beginManagerSetup(ctx context.Context) (func(), error) {
	select {
	case c.mgrGate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.mgrGate
		return nil, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	return func() {
		<-c.mgrGate
		c.wg.Done()
	}, nil
}

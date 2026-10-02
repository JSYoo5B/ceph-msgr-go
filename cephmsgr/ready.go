package cephmsgr

import "context"

// WaitMonReady waits for an authenticated MON session and verified MonMap.
// It submits no management command. An explicit authentication rejection is
// returned immediately; cancellation only ends this wait. Success describes
// an observed ready session and does not guarantee a later command succeeds.
func (c *Client) WaitMonReady(ctx context.Context) error {
	return c.waitReady(ctx, false)
}

// WaitMgrReady waits for MON admission, discovers the active MGR and opens an
// authenticated MGR connection when needed. It submits no management command
// and does not reserve a MaxInFlight command slot. Cancellation only ends this
// wait; the client owns an established connection until Close or replacement.
// Success does not guarantee a later command succeeds.
func (c *Client) WaitMgrReady(ctx context.Context) error {
	return c.waitReady(ctx, true)
}

func (c *Client) waitReady(ctx context.Context, mgr bool) error {
	resolve := c.monitor
	if mgr {
		resolve = c.manager
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.ctx.Err() != nil {
			return ErrClosed
		}
		s, err := resolve(ctx)
		if err != nil {
			return err
		}
		// A resolved session can have been replaced or rejected while setup
		// completed. Recheck admission without replaying any command.
		c.mu.Lock()
		closed, authErr := c.closed, c.authErr
		ready := c.monReady && c.mon != nil && c.mon.Err() == nil
		if mgr {
			ready = ready && c.mgr == s && s.Err() == nil
		} else {
			ready = ready && c.mon == s
		}
		c.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if closed {
			return ErrClosed
		}
		if authErr != nil {
			return authErr
		}
		if ready {
			return nil
		}
	}
}

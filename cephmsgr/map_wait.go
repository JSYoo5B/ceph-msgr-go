package cephmsgr

import "context"

// WaitMonMap waits for a last authenticated MonMap epoch strictly newer than
// afterEpoch. Zero accepts any existing nonzero epoch. It returns an independent
// snapshot of the latest observed map, which may skip intermediate epochs and
// does not promise a ready peer. Cancellation ends only this wait; Close returns
// ErrClosed, and an explicit authentication rejection is returned immediately.
// It sends no command or subscription and waits through transient MON recovery.
func (c *Client) WaitMonMap(ctx context.Context, afterEpoch uint32) (MonitorState, error) {
	state, err := c.waitMap(ctx, afterEpoch, false)
	return state.Monitor, err
}

// WaitMgrMap waits for a last authenticated MgrMap epoch strictly newer than
// afterEpoch. Zero accepts any existing nonzero epoch. The independent snapshot
// describes advertised state, not peer readiness. It opens no MGR connection and
// sends no command or subscription. Cancellation, Close, authentication rejection
// and transient MON recovery follow the same rules as WaitMonMap.
func (c *Client) WaitMgrMap(ctx context.Context, afterEpoch uint32) (ManagerState, error) {
	state, err := c.waitMap(ctx, afterEpoch, true)
	return state.Manager, err
}

func (c *Client) waitMap(ctx context.Context, afterEpoch uint32, mgr bool) (State, error) {
	for {
		if err := ctx.Err(); err != nil {
			return State{}, err
		}
		if c.ctx.Err() != nil {
			return State{}, ErrClosed
		}
		c.mu.Lock()
		closed, authErr, changed := c.closed, c.authErr, c.changed
		epoch := c.monMap.Epoch
		if mgr {
			epoch = c.mgrMap.Epoch
		}
		c.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return State{}, err
		}
		if closed {
			return State{}, ErrClosed
		}
		if authErr != nil {
			return State{}, authErr
		}
		if epoch > afterEpoch {
			state := c.Snapshot()
			// Snapshot owns the returned collections. Recheck termination after
			// copying, as admission or Close can change during that operation.
			c.mu.Lock()
			closed, authErr = c.closed, c.authErr
			c.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return State{}, err
			}
			if closed {
				return State{}, ErrClosed
			}
			if authErr != nil {
				return State{}, authErr
			}
			return state, nil
		}
		// Capture changed with the epoch check so a map published before this
		// select closes the captured channel instead of losing the wakeup.
		select {
		case <-changed:
		case <-ctx.Done():
		case <-c.ctx.Done():
		}
	}
}

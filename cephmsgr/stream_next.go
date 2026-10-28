package cephmsgr

import "context"

// nextStreamItem owns only the common local-wait policy. pollLocked transfers
// an accepted item or snapshots the terminal cause and notification under c.mu;
// each stream retains its own queue, accounting, admission and stop semantics.
// In particular, stop runs outside c.mu because cancellation and session-error
// inspection may invoke caller code. No registration or network work occurs here.
func nextStreamItem[T any](ctx context.Context, c *Client, watch context.Context, pollLocked func() (T, bool, error, <-chan struct{}), stop func(error)) (T, error) {
	var zero T
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		c.mu.Lock()
		next, available, err, changed := pollLocked()
		c.mu.Unlock()
		if available {
			return next, nil
		}
		if err != nil {
			return zero, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-watch.Done():
			stop(watch.Err())
		case <-c.ctx.Done():
			stop(ErrClosed)
		}
	}
}

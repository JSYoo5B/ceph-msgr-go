package cephmsgr

import (
	"context"
	"errors"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func retryableSetup(err error) bool {
	return session.RetryableSetupError(err)
}

func (c *Client) bootstrap(ctx context.Context) error {
	err := c.connectMonitor(ctx)
	if ctx.Err() != nil || !retryableSetup(err) {
		return err
	}
	// Tentacle can close a new authentication connection when temporarily
	// busy, or an individual endpoint can time out while the caller remains
	// live. No command has been admitted. Bound additional setup attempts by
	// ConnectTimeout and the caller's context, preserving both failure causes.
	retryCtx, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()
	backoff := 250 * time.Millisecond
	if short := c.options.ConnectTimeout / 4; short < backoff {
		backoff = max(short, time.Nanosecond)
	}
	for {
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-retryCtx.Done():
			timer.Stop()
			return errors.Join(err, retryCtx.Err())
		}
		err = c.connectMonitor(retryCtx)
		if retryCtx.Err() != nil {
			return errors.Join(err, retryCtx.Err())
		}
		if !retryableSetup(err) {
			return err
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

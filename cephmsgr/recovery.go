package cephmsgr

import (
	"context"
	"errors"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// Recovery opens fresh lossy sessions. It never replays an outstanding command.
// Reauthentication also renews tickets without sharing mutable CephX state
// with MGR handshakes or imposing deadlines on the old session's requests.
func (c *Client) supervise() {
	defer c.wg.Done()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	backoff := 250 * time.Millisecond
	var nextAttempt time.Time
	for {
		now := time.Now()
		needed, deadline, closed := c.renewalState(now)
		if closed {
			return
		}
		if needed && !now.Before(nextAttempt) {
			err := c.connectMonitor(c.ctx)
			if err != nil {
				var rejection *AuthenticationError
				if errors.As(err, &rejection) {
					c.rejectAuthentication(err)
				}
			}
			now = time.Now()
			stillNeeded, _, closed := c.renewalState(now)
			if closed {
				return
			}
			if err != nil || stillNeeded {
				// Even a successful exchange can return tickets whose renewal
				// time passed during setup. It did not advance the schedule;
				// back off instead of repeatedly reconnecting without waiting.
				nextAttempt = now.Add(backoff)
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			} else {
				backoff = 250 * time.Millisecond
				nextAttempt = time.Time{}
			}
			continue
		}
		if needed {
			deadline = nextAttempt
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Until(deadline), 0))
		select {
		case <-c.wake:
		case <-timer.C:
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) renewalState(now time.Time) (needed bool, deadline time.Time, closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false, time.Time{}, true
	}
	needed = !c.monReady || c.mon == nil || c.mon.Err() != nil
	for service := uint32(1); service <= cephx.ServiceAuth; service <<= 1 {
		if c.auth.Services()&service == 0 {
			continue
		}
		ticket, ok := c.auth.Tickets[service]
		if !ok || !now.Before(ticket.RenewAfter) {
			needed = true
		}
		if deadline.IsZero() || ticket.RenewAfter.Before(deadline) {
			deadline = ticket.RenewAfter
		}
	}
	return needed, deadline, false
}

func (c *Client) rejectAuthentication(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.authErr = err
	c.monReady = false
	c.mgr = nil
	configWatch, logWatch := c.configWatch, c.logWatch
	digestWatch := c.digestWatch
	if configWatch != nil && !configWatch.stopLocked(err) {
		configWatch = nil
	}
	if logWatch != nil && !logWatch.stopLocked(err) {
		logWatch = nil
	}
	if digestWatch != nil && !digestWatch.stopLocked(err) {
		digestWatch = nil
	}
	all := make([]*session.Session, 0, len(c.sessions))
	for s := range c.sessions {
		// Close admission before publishing the rejection. A caller that
		// resolved a previous session must not start a new command on it.
		s.Retire()
		all = append(all, s)
	}
	c.signal()
	c.mu.Unlock()
	if configWatch != nil {
		configWatch.cancel()
	}
	if logWatch != nil {
		logWatch.cancel()
	}
	if digestWatch != nil {
		digestWatch.cancel()
	}
	for _, s := range all {
		// Session.Fail preserves uncertainty for requests whose transmission
		// started, while keeping the explicit authentication cause available.
		s.Fail(err)
	}
}

func (c *Client) retireMonitor(s *session.Session) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		s.Fail(ErrClosed)
		return
	}
	s.Retire()
	c.wg.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wg.Done()
		ctx, cancel := context.WithTimeout(c.ctx, c.options.ConnectTimeout)
		defer cancel()
		s.WaitIdle(ctx)
		s.Fail(session.ErrRetired)
	}()
}

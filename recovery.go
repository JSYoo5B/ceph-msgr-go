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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	backoff := 250 * time.Millisecond
	var nextAttempt time.Time
	for {
		c.mu.Lock()
		needed := !c.monReady || c.mon == nil || c.mon.Err() != nil
		if !needed {
			for _, service := range []uint32{cephx.ServiceAuth, cephx.ServiceMgr} {
				ticket, ok := c.auth.Tickets[service]
				if !ok || !time.Now().Before(ticket.RenewAfter) {
					needed = true
				}
			}
		}
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		if needed && !time.Now().Before(nextAttempt) {
			if err := c.connectMonitor(c.ctx); err != nil {
				var rejection *AuthenticationError
				if errors.As(err, &rejection) {
					c.rejectAuthentication(err)
				}
				nextAttempt = time.Now().Add(backoff)
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			} else {
				backoff = 250 * time.Millisecond
				nextAttempt = time.Time{}
			}
		}
		select {
		case <-c.wake:
		case <-ticker.C:
		case <-c.ctx.Done():
			return
		}
	}
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
	all := make([]*session.Session, 0, len(c.sessions))
	for s := range c.sessions {
		// Close admission before publishing the rejection. A caller that
		// resolved a previous session must not start a new command on it.
		s.Retire()
		all = append(all, s)
	}
	c.signal()
	c.mu.Unlock()
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

package cephmsgr

import (
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// monAdmission owns the candidate-local message state of one MON dial
// candidate until its first accepted MonMap. Before admission it drops
// subscription payloads and retains at most one early MgrMap, the highest
// epoch seen, because a MON can send mgrmap before monmap. The first MonMap
// the client accepts establishes cluster and release identity: that already
// validated map becomes the admission result, the retained MgrMap is applied
// and ready closes.
//
// The client still owns publication of tickets and readiness, stale-session
// checks, the auth-epoch refresh barrier and rollback to the previous session.
// Only the candidate's session reader calls handle, so the fields below need
// no lock; connectMonitor reads the result only after ready closes.
type monAdmission struct {
	client  *Client
	session *session.Session
	ready   chan struct{}

	admitted      bool
	monMap        maps.Mon // first accepted MonMap; valid once ready is closed
	bufferedMgr   *msgr.MessageData
	bufferedEpoch uint32
}

func newMonAdmission(c *Client) *monAdmission {
	return &monAdmission{client: c, ready: make(chan struct{})}
}

// result returns the candidate's first accepted MonMap. Call it only after
// ready has closed.
func (a *monAdmission) result() maps.Mon { return a.monMap }

// handle is the candidate session's message callback.
func (a *monAdmission) handle(m msgr.MessageData) error {
	c, s := a.client, a.session
	switch m.Type {
	case mds.FSMapUserMessage, mds.MDSMapMessageType, msgr.OSDMapMessage, msgr.MgrDigestMessage, msgr.ServiceMapMessage, msgr.ConfigMessage, msgr.LogMessage:
		if !a.admitted {
			return nil
		}
		return c.handleMonSubscription(s, m)
	case msgr.MgrMapMessage:
		if !a.admitted {
			return a.bufferManagerMap(m)
		}
	}
	accepted, err := c.handleMap(s, m)
	if err != nil {
		a.bufferedMgr = nil
		return err
	}
	if accepted == nil || a.admitted {
		return nil
	}
	a.admitted, a.monMap = true, *accepted
	if buffered := a.bufferedMgr; buffered != nil {
		a.bufferedMgr = nil
		if _, err := c.handleMap(s, *buffered); err != nil {
			return err
		}
	}
	close(a.ready)
	return nil
}

// bufferManagerMap validates an early MgrMap from the current candidate and
// keeps it only when its epoch is not older than the retained one.
func (a *monAdmission) bufferManagerMap(m msgr.MessageData) error {
	c := a.client
	c.mu.Lock()
	current := c.mon == a.session
	c.mu.Unlock()
	if !current {
		return nil
	}
	if err := validateMapMessageHeader(m); err != nil {
		a.bufferedMgr = nil
		return err
	}
	mgr, err := maps.DecodeMgr(m.Front)
	if err != nil {
		a.bufferedMgr = nil
		return fmt.Errorf("%w: invalid MGR map: %w", msgr.ErrFrame, err)
	}
	if a.bufferedMgr == nil || mgr.Epoch >= a.bufferedEpoch {
		a.bufferedMgr = &msgr.MessageData{Type: m.Type, Front: m.Front}
		a.bufferedEpoch = mgr.Epoch
	}
	return nil
}

// handleMonSubscription dispatches a subscription payload from an admitted MON
// session to its watch handler.
func (c *Client) handleMonSubscription(s *session.Session, m msgr.MessageData) error {
	switch m.Type {
	case mds.FSMapUserMessage, mds.MDSMapMessageType:
		return c.handleMonMDSMap(s, m)
	case msgr.OSDMapMessage:
		return c.handleMonOSDMap(s, m)
	case msgr.MgrDigestMessage:
		return c.handleDigest(s, m)
	case msgr.ServiceMapMessage:
		return c.handleServiceMap(s, m)
	case msgr.ConfigMessage:
		return c.handleConfig(s, m)
	case msgr.LogMessage:
		return c.handleLog(s, m)
	}
	return nil
}

package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// ErrManagerNotFound means the exact requested name is absent from the last
// authenticated MgrMap. No command was sent.
var ErrManagerNotFound = errors.New("ceph: named manager not found")

// ErrManagerNotActive means the requested name is not the available active
// daemon. MgrMap provides no standby command address. No command was sent.
var ErrManagerNotActive = errors.New("ceph: named manager is not active")

// ErrManagerTargetChanged means the active daemon binding selected for a named
// tell changed before completion. A started command also returns an
// OutcomeUnknownError; the library never substitutes another active daemon.
var ErrManagerTargetChanged = fmt.Errorf("ceph: named manager target changed: %w", ErrManagerChanged)

// MgrTellTo sends a daemon-local command to the exact bare name of the current
// available active MGR. It shares the ordinary MGR session and service ticket.
// Empty names, prefix removal, rank interpretation, wildcards and standby
// connections are unsupported. A standby name is rejected locally rather than
// waiting for that daemon to become active.
//
// The operation binds the selected name, daemon GID, exact map address vector
// and authenticated client identity. A changed binding never selects another
// active daemon. ctx governs setup and the local reply wait; after transmission
// starts cancellation or loss leaves an OutcomeUnknownError. No uncertain
// command is replayed. The server requires allow-all MGR capabilities for tell.
func (c *Client) MgrTellTo(ctx context.Context, name string, command Command) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if c.ctx.Err() != nil {
		return Result{}, ErrClosed
	}
	if name == "" {
		return Result{}, errors.New("ceph: named manager requires an exact nonempty name")
	}
	return c.command(ctx, command, managerTell, name)
}

// managerBinding is immutable once selected. Epoch-only updates do not change
// the daemon binding, and ticket renewal with the same client ID is permitted.
type managerBinding struct {
	name      string
	gid       uint64
	addresses []msgr.Address
	clientID  uint64
}

func bindManager(m maps.Mgr, clientID uint64) managerBinding {
	return managerBinding{m.Name, m.GlobalID, append([]msgr.Address(nil), m.Addresses...), clientID}
}

func (b managerBinding) matches(m maps.Mgr, clientID uint64) bool {
	return m.Available && m.Name == b.name && m.GlobalID == b.gid &&
		clientID == b.clientID && reflect.DeepEqual(m.Addresses, b.addresses)
}

func (c *Client) namedManagerBinding(ctx context.Context, name string) (*managerBinding, error) {
	for {
		primary, err := c.monitor(ctx)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		if c.authErr != nil {
			err = c.authErr
			c.mu.Unlock()
			return nil, err
		}
		if c.mon != primary || !c.monReady || primary.Err() != nil {
			c.mu.Unlock()
			continue
		}
		m, changed := c.mgrMap, c.changed
		if m.Epoch == 0 {
			c.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.ctx.Done():
				return nil, ErrClosed
			}
		}
		if m.Name == name && m.Available {
			b := bindManager(m, c.auth.GlobalID)
			c.mu.Unlock()
			return &b, nil
		}
		found := m.Name == name
		for _, standby := range m.Standbys {
			found = found || standby.Name == name
		}
		c.mu.Unlock()
		if found {
			return nil, fmt.Errorf("%w: %q", ErrManagerNotActive, name)
		}
		return nil, fmt.Errorf("%w: %q", ErrManagerNotFound, name)
	}
}

// managerBeforeWrite checks the session's immutable binding at the existing
// single-writer admission boundary. A map can change after this local decision;
// that cannot establish remote non-execution of a started command.
func (c *Client) managerBeforeWrite(s *session.Session, b managerBinding) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.authErr != nil {
		return c.authErr
	}
	if c.mgr != s || !b.matches(c.mgrMap, c.auth.GlobalID) {
		return ErrManagerChanged
	}
	if !c.monReady || c.mon == nil || c.mon.Err() != nil {
		return session.ErrRetired
	}
	return nil
}

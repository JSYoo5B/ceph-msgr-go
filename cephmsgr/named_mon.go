package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// ErrMonitorNotFound means the exact requested name is absent from the
// currently admitted MON map. No command has been sent.
var ErrMonitorNotFound = errors.New("ceph: named monitor not found")

// ErrMonitorTargetChanged means the independently received MON map no longer
// associates the requested name with the address selected for this operation.
// During admission no command has been sent. A later change can be the cause
// of an OutcomeUnknownError for a command whose transmission already started.
var ErrMonitorTargetChanged = errors.New("ceph: named monitor target changed")

// MonTellTo sends a daemon-local command to one named MON through a separately
// authenticated connection. name is the exact bare MonMap name, such as "a";
// no prefix removal, numeric-rank interpretation or wildcard expansion occurs.
// The primary MON/MGR connections, authentication identity and log watch are
// preserved. The main MON must first be admitted, and the target independently
// proves the same FSID, supported release and name-to-address association.
//
// ctx governs this whole operation. ConnectTimeout bounds each endpoint's
// setup, not the subsequent command wait. An unavailable named MON is never
// replaced with another MON. After transmission starts, cancellation or loss
// leaves an OutcomeUnknownError and the command is never automatically replayed.
// The daemon requires MON read, write and execute capabilities for Tell.
func (c *Client) MonTellTo(ctx context.Context, name string, command Command) (Result, error) {
	var result Result
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	if uint64(len(command.JSON))+uint64(len(command.Input))+256 > uint64(c.options.MaxFrameSize) {
		return result, fmt.Errorf("ceph: command exceeds frame limit: %w", ErrLimitExceeded)
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-c.ctx.Done():
		return result, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil {
		return result, ErrClosed
	}
	var object struct {
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(command.JSON, &object); err != nil || strings.TrimSpace(object.Prefix) == "" {
		return result, errors.New("ceph: command JSON requires a string prefix")
	}
	jsonCommand := string(command.JSON)
	input := append([]byte(nil), command.Input...)

	// Own setup before opening a socket, including a DialContext or handshake
	// still in progress when Close starts. Keep context methods and callbacks
	// outside c.mu; custom contexts and connections can invoke caller code.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return result, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()

	fsid, addresses, err := c.namedMonitorAddresses(operation, name)
	if err != nil {
		return result, c.namedMonitorError(err)
	}
	var failures []error
	seen := make(map[msgr.Address]bool)
	for _, address := range addresses {
		if address.Type != 2 || !address.Endpoint.IsValid() || seen[address] {
			continue
		}
		seen[address] = true
		if err := operation.Err(); err != nil {
			return result, c.namedMonitorError(errors.Join(append(failures, err)...))
		}
		s, err := c.openNamedMonitor(operation, name, fsid, address)
		if err != nil {
			failures = append(failures, err)
			if operation.Err() != nil || !retryableSetup(err) {
				return result, c.namedMonitorError(errors.Join(failures...))
			}
			continue
		}
		// This connection serves one command. Its lifetime extends through
		// cleanup, including a started write or a pending reply on cancellation.
		defer func() { s.Fail(ErrClosed); s.Wait() }()
		reply, err := s.Call(operation, msgr.TellCommand(fsid, []string{jsonCommand}, input))
		if err != nil {
			return result, c.namedMonitorError(err)
		}
		code, message, err := msgr.CommandReply(reply, c.options.MaxFrameSize)
		if err != nil {
			result.Data = append([]byte(nil), reply.Data...)
			cause := fmt.Errorf("%w: invalid command reply: %w", msgr.ErrFrame, err)
			s.Fail(cause)
			return result, &OutcomeUnknownError{Cause: cause}
		}
		result = Result{Data: append([]byte(nil), reply.Data...), Message: message, Code: code}
		if code < 0 {
			return result, &CommandError{Code: code, Message: message}
		}
		return result, nil
	}
	if len(failures) != 0 {
		return result, c.namedMonitorError(errors.Join(failures...))
	}
	return result, errors.New("ceph: named monitor has no Messenger v2 address")
}

func (c *Client) namedMonitorError(err error) error {
	// Client cancellation can win the race with the session's ErrClosed result.
	// Preserve every original cause and any transmission uncertainty while
	// identifying shutdown to callers; do not turn a prior refusal into cancel.
	if c.ctx.Err() == nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return err
	}
	var unknown *OutcomeUnknownError
	if errors.As(err, &unknown) {
		return &OutcomeUnknownError{Cause: errors.Join(unknown.Cause, ErrClosed)}
	}
	return errors.Join(err, ErrClosed)
}

func (c *Client) namedMonitorAddresses(ctx context.Context, name string) ([16]byte, []msgr.Address, error) {
	for {
		primary, err := c.monitor(ctx)
		if err != nil {
			return [16]byte{}, nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return [16]byte{}, nil, ErrClosed
		}
		if c.authErr != nil {
			err = c.authErr
			c.mu.Unlock()
			return [16]byte{}, nil, err
		}
		if c.mon != primary || !c.monReady || primary.Err() != nil {
			c.mu.Unlock()
			continue
		}
		fsid := c.fsid
		for _, member := range c.monMap.Members {
			if member.Name == name {
				addresses := append([]msgr.Address(nil), member.Addresses...)
				c.mu.Unlock()
				return fsid, addresses, nil
			}
		}
		c.mu.Unlock()
		return [16]byte{}, nil, fmt.Errorf("%w: %q", ErrMonitorNotFound, name)
	}
}

func (c *Client) openNamedMonitor(ctx context.Context, name string, fsid [16]byte, address msgr.Address) (_ *session.Session, err error) {
	// Tentacle's directed Tell connections start a full MON CephX exchange
	// with global ID zero. Their private credentials never replace c.auth.
	auth, err := cephx.NewClient(c.options.Identity, c.options.Key.value, 0)
	if err != nil {
		return nil, err
	}
	setup, release := c.linkedContext(ctx)
	defer release()
	transport, err := c.open(setup, dialAddress(address), address, 1, 0, session.MonAuth{Client: auth})
	if err != nil {
		return nil, err
	}
	ready := make(chan struct{})
	var admitted sync.Once
	s := session.New(transport, c.sessionConfig(), func(m msgr.MessageData) error {
		if m.Type != msgr.MonMapMessage {
			return nil // No map/log subscriptions or shared-state publication.
		}
		if err := validateMapMessageHeader(m); err != nil {
			return err
		}
		mon, err := maps.DecodeMon(m.Front)
		if err != nil {
			if errors.Is(err, maps.ErrRelease) {
				return err
			}
			return fmt.Errorf("%w: invalid named MON map: %w", msgr.ErrFrame, err)
		}
		if mon.FSID != fsid {
			return fmt.Errorf("%w: named MON FSID mismatch", msgr.ErrAuthentication)
		}
		for _, member := range mon.Members {
			if member.Name == name && slices.Contains(member.Addresses, address) {
				admitted.Do(func() { close(ready) })
				return nil
			}
		}
		return fmt.Errorf("%w: %q", ErrMonitorTargetChanged, name)
	}, nil)
	defer func() {
		if err != nil {
			s.Fail(err)
			s.Wait()
		}
	}()
	c.mu.Lock()
	attached := c.attach(s)
	c.mu.Unlock()
	if !attached {
		return nil, ErrClosed
	}
	if err = s.Send(setup, msgr.GetMonMap()); err == nil {
		err = waitMonitorAdmission(setup, s, ready)
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

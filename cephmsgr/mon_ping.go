package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// PingMonitor sends one MPing to the exact bare name in the admitted MonMap
// and returns the reply's raw string bytes. It does not interpret the MON's
// JSON. A separately authenticated connection independently verifies the FSID,
// supported release and name-to-address association before sending the ping.
// The primary identity, subscriptions and command transaction IDs are preserved.
//
// MPing has no transaction correlation: each invocation owns one connection
// and sends one ping. Parallel invocations share MaxInFlight with commands.
// ctx governs admission, setup, transmission and the reply wait; Client.Close
// joins cleanup. After queue admission, loss or cancellation without a valid
// reply returns OutcomeUnknownError, even though this operation is read-only.
// No endpoint fallback, automatic retry or remote cancellation is performed.
//
// It is the Messenger primitive behind go-ceph rados.Conn.PingMonitor, not a
// MON command or a Messenger keepalive. Only the selected member's first valid
// v2 address is attempted, with the client's connection mode and no downgrade;
// a native MON's ability to answer a ping before command capability checks
// does not bypass this admission. A failure before queue admission is known
// unsent. Both directions are message type 2 with TID zero; the MON's reply
// is version 1 with compat 0, one bounded Ceph string in the front and no
// middle or data. Empty and binary strings are returned as sent. A valid
// accepted reply is kept when it races local cancellation or teardown.
// Source: ceph/ceph/blob/v20.2.4/src/messages/MPing.h and
// ceph/ceph/blob/v20.2.4/src/mon/Monitor.cc (handle_ping).
func (c *Client) PingMonitor(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()

	fsid, addresses, err := c.namedMonitorAddresses(operation, name)
	if err != nil {
		return nil, c.namedMonitorError(err)
	}
	var address msgr.Address
	for _, candidate := range addresses {
		if candidate.Type == 2 && candidate.Endpoint.IsValid() {
			address = candidate
			break
		}
	}
	if !address.Endpoint.IsValid() {
		return nil, errors.New("ceph: named monitor has no Messenger v2 address")
	}
	response := make(chan []byte, 1)
	var armed atomic.Bool
	s, err := c.openPrivateMonitor(operation, name, fsid, address, func(m msgr.MessageData) error {
		if !armed.Load() {
			return nil
		}
		raw, err := msgr.PingReply(m, c.options.MaxFrameSize)
		if err != nil {
			return fmt.Errorf("%w: invalid MON ping reply: %w", msgr.ErrFrame, err)
		}
		select {
		case response <- raw:
		default:
		}
		return nil
	})
	if err != nil {
		return nil, c.namedMonitorError(err)
	}
	defer func() { s.Fail(ErrClosed); s.Wait() }()
	armed.Store(true)
	if err := s.Send(operation, msgr.Ping()); err != nil {
		return nil, c.namedMonitorError(err)
	}
	select {
	case raw := <-response:
		return raw, nil
	case <-s.Done():
	case <-operation.Done():
	}
	// A fully decoded reply wins over concurrent cancellation or source loss.
	select {
	case raw := <-response:
		return raw, nil
	default:
	}
	cause := s.Err()
	if cause == nil || !fatalLogSessionError(cause) && operation.Err() != nil {
		cause = operation.Err()
	}
	return nil, &OutcomeUnknownError{Cause: c.namedMonitorError(cause)}
}

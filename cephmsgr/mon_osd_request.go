package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// OSDMapRequest selects inclusive native full and incremental ranges. Each
// pair must be both zero (disabled) or satisfy 0 < First <= Last.
// The all-zero value instead requests one latest full map.
type OSDMapRequest struct {
	FullFirst, FullLast               uint32
	IncrementalFirst, IncrementalLast uint32
}

// RequestOSDMaps receives one raw MOSDMap on a private authenticated MON
// connection. The main MON's identity, subscriptions and commands are preserved.
// Parallel requests each own a connection, bounded by MaxInFlight alongside
// commands. ctx governs setup, transmission and the wait; Client.Close joins
// their workers. No OSD ticket or data connection is needed.
//
// The zero request subscribes once for the latest full map. Tentacle silently
// drops this subscription without MON osd read capability; a timeout does not
// identify a permission rejection. Nonzero ranges use MMonGetOSDMap directly.
// Tentacle clamps ranges to retained/current history and applies a shared count
// and byte budget, with full maps taking priority. One empty or partial batch is
// a normal reply, not proof that the requested range was obtained. NewestMap is
// advertised metadata, not a completion cursor. The caller's map layer decides
// whether more requests or a new full baseline are needed.
//
// MOSDMap replies have no request correlation ID. A fresh connection isolates
// this request from watches and prior replies. No automatic replay occurs after
// transmission; cancellation ends only the local wait.
func (c *Client) RequestOSDMaps(ctx context.Context, request OSDMapRequest) (OSDMapBatch, error) {
	if err := ctx.Err(); err != nil {
		return OSDMapBatch{}, err
	}
	if c.ctx.Err() != nil {
		return OSDMapBatch{}, ErrClosed
	}
	for _, pair := range [][2]uint32{{request.FullFirst, request.FullLast}, {request.IncrementalFirst, request.IncrementalLast}} {
		if (pair[0] == 0) != (pair[1] == 0) || pair[0] > pair[1] {
			return OSDMapBatch{}, errors.New("ceph: invalid OSD map epoch range")
		}
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return OSDMapBatch{}, ctx.Err()
	case <-c.ctx.Done():
		return OSDMapBatch{}, ErrClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return OSDMapBatch{}, ErrClosed
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()
	if _, err := c.monitor(operation); err != nil {
		return OSDMapBatch{}, c.namedMonitorError(err)
	}
	c.mu.Lock()
	fsid := c.fsid
	members := append([]maps.MonMember(nil), c.monMap.Members...)
	c.mu.Unlock()
	var failures []error
	for _, member := range members {
		for _, address := range member.Addresses {
			if address.Type != 2 || !address.Endpoint.IsValid() {
				continue
			}
			if err := operation.Err(); err != nil {
				return OSDMapBatch{}, c.namedMonitorError(errors.Join(append(failures, err)...))
			}
			response := make(chan OSDMapBatch, 1)
			var armed atomic.Bool
			s, err := c.openPrivateMonitor(operation, member.Name, fsid, address, func(m msgr.MessageData) error {
				if m.Type != msgr.OSDMapMessage || !armed.Load() {
					return nil
				}
				decoded, err := maps.DecodeOSDMessage(m, c.options.MaxFrameSize)
				if err == nil && decoded.FSID != fsid {
					err = errors.New("MON OSD map belongs to a different cluster")
				}
				if err != nil {
					return fmt.Errorf("%w: invalid requested OSD map: %w", msgr.ErrFrame, err)
				}
				select {
				case response <- publicOSDMapBatch(m, decoded):
				default:
				}
				return nil
			})
			if err != nil {
				failures = append(failures, err)
				if operation.Err() != nil || !retryableSetup(err) {
					return OSDMapBatch{}, c.namedMonitorError(errors.Join(failures...))
				}
				continue
			}
			defer func() { s.Fail(ErrClosed); s.Wait() }()
			message := msgr.GetOSDMaps(request.FullFirst, request.FullLast, request.IncrementalFirst, request.IncrementalLast)
			if request == (OSDMapRequest{}) {
				message = msgr.OSDMapSubscribe(0, true, c.options.Hostname)
			}
			armed.Store(true)
			if err := s.Send(operation, message); err != nil {
				return OSDMapBatch{}, c.namedMonitorError(err)
			}
			select {
			case batch := <-response:
				return batch, nil
			case <-s.Done():
				// Preserve a fully accepted reply if connection cleanup wins
				// the select after delivery. No remote work is inferred here.
				select {
				case batch := <-response:
					return batch, nil
				default:
				}
				return OSDMapBatch{}, c.namedMonitorError(s.Err())
			case <-operation.Done():
				return OSDMapBatch{}, c.namedMonitorError(operation.Err())
			}
		}
	}
	if len(failures) != 0 {
		return OSDMapBatch{}, c.namedMonitorError(errors.Join(failures...))
	}
	return OSDMapBatch{}, errors.New("ceph: MON map has no Messenger v2 address")
}

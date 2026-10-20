package cephmsgr

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// ErrServiceMapWatchActive means another watch owns this client's service map stream.
var ErrServiceMapWatchActive = errors.New("ceph: a service map watch is already active")

// ErrServiceMapStreamClosed is the terminal cause of an explicitly closed watch.
var ErrServiceMapStreamClosed = errors.New("ceph: service map stream closed")

// ErrServiceMapOverflow terminates a watch that cannot retain a full map.
// The previously accepted unread map and shared MON connection remain intact.
var ErrServiceMapOverflow = errors.New("ceph: service map watch buffer exceeded")

// ServiceMapOptions bounds the unread full map retained by a watch.
type ServiceMapOptions struct {
	// MaxBufferedBytes bounds the raw size of one unread map plus 512 bytes.
	// The default is the client's MaxFrameSize; accepted values are 1 KiB–1 GiB.
	MaxBufferedBytes uint32
}

// ServiceDaemon is one daemon registered under a service (for example an RGW
// or rbd-mirror instance): its client GID, address, the service map epoch it
// started in, its start time and its raw metadata and task status strings.
type ServiceDaemon struct {
	GID                  uint64
	Address              MDSMapAddress
	StartEpoch           uint32
	Started              MDSMapTime
	Metadata, TaskStatus map[string]string
}

// ServiceMap is one full MGR service map as the MON publishes it: its epoch,
// modification time, services by name (each with daemons by name and the MGR's
// summary string) and the raw encoded map. This layer interprets no metadata.
type ServiceMap struct {
	Epoch    uint32
	Modified MDSMapTime
	Services map[string]ServiceEntry
	Raw      []byte
}

// ServiceEntry is one named service in a ServiceMap.
type ServiceEntry struct {
	Daemons map[string]ServiceDaemon
	Summary string
}

func publicServiceMap(m msgr.ServiceMap) ServiceMap {
	out := ServiceMap{Epoch: m.Epoch, Modified: MDSMapTime{Seconds: m.ModifiedSec, Nanoseconds: m.ModifiedNs}, Services: make(map[string]ServiceEntry, len(m.Services)), Raw: m.Raw}
	for name, s := range m.Services {
		entry := ServiceEntry{Summary: s.Summary, Daemons: make(map[string]ServiceDaemon, len(s.Daemons))}
		for dn, d := range s.Daemons {
			entry.Daemons[dn] = ServiceDaemon{GID: d.GID, Address: MDSMapAddress{Type: d.Address.Type, Nonce: d.Address.Nonce, Endpoint: d.Address.Endpoint, FlowInfo: d.Address.FlowInfo, ScopeID: d.Address.ScopeID},
				StartEpoch: d.StartEpoch, Started: MDSMapTime{Seconds: d.StartSec, Nanoseconds: d.StartNsec}, Metadata: d.Metadata, TaskStatus: d.TaskStatus}
		}
		out.Services[name] = entry
	}
	return out
}

// ServiceMapStream receives the MGR service map from the admitted MON with a
// continuous "servicemap" subscription: the current map, then each new epoch.
// Every message is a full map, so one unread map is retained and a later one
// replaces it; concurrent Next calls each receive a map at most once.
//
// Recovery resubscribes for the current full map; intermediate epochs and
// lossless delivery are not guaranteed, and the epoch lets the caller see
// skips or repeats. Close ends only this watch, without a remote unsubscribe.
// Accepted data drains before the stable first terminal error.
type ServiceMapStream struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	limit  uint64

	// All fields below are protected by client.mu.
	source   *session.Session
	observed *session.Session
	pending  *ServiceMap // nil means no unread map.
	terminal error
	changed  chan struct{}
}

// WatchServiceMap registers the client's single active service map watch.
// ctx governs the watch's full lifetime, including MON recovery; each Next
// context governs only that local wait. It submits no command, takes no command
// slot and does not open a MGR connection. Success registers a local worker,
// not a server permission or delivery acknowledgement. The MON sends a map only
// once the MGR has published one (epoch 1 or later).
func (c *Client) WatchServiceMap(ctx context.Context, options ServiceMapOptions) (*ServiceMapStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	limit := options.MaxBufferedBytes
	if limit == 0 {
		limit = c.options.MaxFrameSize
	}
	if limit < 1<<10 || limit > 1<<30 {
		return nil, errors.New("ceph: invalid service map buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &ServiceMapStream{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if c.serviceMapWatch != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrServiceMapWatchActive
	}
	c.serviceMapWatch = s
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()
	return s, nil
}

func (s *ServiceMapStream) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *ServiceMapStream) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal, s.source, s.observed = err, nil, nil
	if s.client.serviceMapWatch == s {
		s.client.serviceMapWatch = nil
	}
	s.signalLocked()
	return true
}

func (s *ServiceMapStream) stop(err error) {
	s.client.mu.Lock()
	s.stopLocked(err)
	s.client.mu.Unlock()
	s.cancel()
}

// Close releases the watch slot and joins its registration worker. The unread
// map remains readable. ErrServiceMapStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *ServiceMapStream) Close() error {
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			s.failCurrent(source, err)
		}
	}
	s.stop(ErrServiceMapStreamClosed)
	<-s.done
	return nil
}

// Next returns the latest unread full map, transferring its ownership. A live
// context drains accepted data before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes nothing.
func (s *ServiceMapStream) Next(ctx context.Context) (ServiceMap, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ServiceMap{}, err
		}
		s.client.mu.Lock()
		if s.pending != nil {
			next := *s.pending
			s.pending = nil
			s.client.mu.Unlock()
			return next, nil
		}
		err, changed := s.terminal, s.changed
		s.client.mu.Unlock()
		if err != nil {
			return ServiceMap{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ServiceMap{}, ctx.Err()
		case <-s.ctx.Done():
			s.stop(s.ctx.Err())
		case <-s.client.ctx.Done():
			s.stop(ErrClosed)
		}
	}
}

// Error inspection and cancellation may invoke custom code; neither runs
// under the client lock. Record a registered source's fatal cause before a
// replacement can make its delayed onClose callback stale.
func (c *Client) stopServiceMapForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watch := c.serviceMapWatch
	if c.mon == source && watch != nil && watch.observed == source {
		if !watch.stopLocked(err) {
			watch = nil
		}
	} else {
		watch = nil
	}
	c.mu.Unlock()
	if watch != nil {
		watch.cancel()
	}
}

func (s *ServiceMapStream) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && c.serviceMapWatch == s && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}

func (s *ServiceMapStream) run() {
	defer s.client.wg.Done()
	defer close(s.done)
	for {
		if err := s.ctx.Err(); err != nil {
			s.stop(err)
			return
		}
		c := s.client
		c.mu.Lock()
		terminal, closed, authErr := s.terminal, c.closed, c.authErr
		mon, ready, changed := c.mon, c.monReady, c.changed
		previous := s.observed
		if mon != previous || !ready {
			s.source = nil
		}
		c.mu.Unlock()
		if terminal != nil {
			return
		}
		if closed {
			s.stop(ErrClosed)
			return
		}
		if authErr != nil {
			s.stop(authErr)
			return
		}
		if previous != nil && previous == mon && fatalLogSessionError(previous.Err()) {
			if s.failCurrent(previous, previous.Err()) {
				return
			}
			continue
		}
		if mon != nil && ready && mon.Err() == nil {
			c.mu.Lock()
			register := s.terminal == nil && c.mon == mon && c.monReady && !c.closed && c.authErr == nil && s.source != mon
			if register {
				s.source, s.observed = mon, mon
			}
			c.mu.Unlock()
			if register {
				err := mon.Send(s.ctx, msgr.ServiceMapSubscribe(0, c.options.Hostname))
				if err != nil {
					if watchErr := s.ctx.Err(); watchErr != nil {
						s.stop(watchErr)
						return
					}
					if fatalLogSessionError(err) {
						if s.failCurrent(mon, err) {
							return
						}
						continue
					}
					c.wakeMonitor()
				}
			}
		}
		var monDone <-chan struct{}
		if mon != nil {
			monDone = mon.Done()
		}
		select {
		case <-changed:
		case <-monDone:
			if (ready || previous == mon) && fatalLogSessionError(mon.Err()) {
				if s.failCurrent(mon, mon.Err()) {
					return
				}
			}
			select {
			case <-changed:
			case <-s.ctx.Done():
			case <-c.ctx.Done():
			}
		case <-s.ctx.Done():
		case <-c.ctx.Done():
		}
	}
}

func (c *Client) handleServiceMap(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.serviceMapWatch
	accept := s != nil && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeServiceMap(m, c.options.MaxFrameSize)
	bytes := uint64(512) + uint64(len(decoded.Raw))
	c.mu.Lock()
	if c.serviceMapWatch != s || s.terminal != nil || s.source != source || c.mon != source || !c.monReady || c.closed || c.authErr != nil {
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		s.stopLocked(err)
		c.mu.Unlock()
		s.cancel()
		return err
	}
	if bytes > s.limit {
		s.stopLocked(ErrServiceMapOverflow)
		c.mu.Unlock()
		s.cancel()
		return nil
	}
	published := publicServiceMap(decoded)
	s.pending = &published
	s.signalLocked()
	c.mu.Unlock()
	return nil
}

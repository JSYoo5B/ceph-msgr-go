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
	watch monSnapshotWatch[ServiceMapStream]

	// Protected by client.mu.
	pending *ServiceMap // nil means no unread map.
}

func newServiceMapStream(c *Client, ctx context.Context, cancel context.CancelFunc, limit uint64) *ServiceMapStream {
	s := &ServiceMapStream{}
	s.watch = newMonSnapshotWatch(c, ctx, cancel, limit, &c.serviceMapWatch, s, s.registerSource)
	return s
}

// registerSource subscribes from epoch 0 so an admitted MON session sends its
// current full service map, then each later epoch.
func (s *ServiceMapStream) registerSource(ctx context.Context, mon *session.Session, _ string) error {
	return mon.Send(ctx, msgr.ServiceMapSubscribe(0, s.watch.client.options.Hostname))
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
	s := newServiceMapStream(c, watchCtx, cancel, uint64(limit))
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
	go s.watch.run()
	return s, nil
}

// Close releases the watch slot and joins its registration worker. The unread
// map remains readable. ErrServiceMapStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *ServiceMapStream) Close() error {
	s.watch.closeWatch(ErrServiceMapStreamClosed)
	return nil
}

// Next returns the latest unread full map, transferring its ownership. A live
// context drains accepted data before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes nothing.
func (s *ServiceMapStream) Next(ctx context.Context) (ServiceMap, error) {
	return nextStreamItem(ctx, s.watch.client, s.watch.ctx, func() (ServiceMap, bool, error, <-chan struct{}) {
		if s.pending != nil {
			next := *s.pending
			s.pending = nil
			return next, true, nil, nil
		}
		return ServiceMap{}, false, s.watch.terminal, s.watch.changed
	}, s.watch.stop)
}

func (c *Client) handleServiceMap(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.serviceMapWatch
	accept := s != nil && s.watch.acceptsLocked(source)
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeServiceMap(m, c.options.MaxFrameSize)
	bytes := uint64(512) + uint64(len(decoded.Raw))
	c.mu.Lock()
	if !s.watch.acceptsLocked(source) {
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		s.watch.stopLocked(err)
		c.mu.Unlock()
		s.watch.cancel()
		return err
	}
	if bytes > s.watch.limit {
		s.watch.stopLocked(ErrServiceMapOverflow)
		c.mu.Unlock()
		s.watch.cancel()
		return nil
	}
	published := publicServiceMap(decoded)
	s.pending = &published
	s.watch.signalLocked()
	c.mu.Unlock()
	return nil
}

package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

var ErrOSDMapWatchActive = errors.New("ceph: an OSD map watch is already active")

// ErrOSDMapWatchOverflow ends only the watch, preserving queued batches and
// other operations on the shared MON connection.
var ErrOSDMapWatchOverflow = errors.New("ceph: MON OSD map watch buffer exceeded")
var ErrOSDMapStreamClosed = errors.New("ceph: OSD map stream closed")

type OSDMapOptions struct {
	// StartEpoch is inclusive. Zero requests a latest full baseline. A positive
	// epoch requests available history, with a full fallback when it was trimmed.
	StartEpoch uint32
	// MaxBufferedBytes defaults to MaxFrameSize; valid values are 1 KiB to 1 GiB.
	// Conservative envelope/slice accounting and a 64-batch cap bound the FIFO.
	MaxBufferedBytes uint32
}

// OSDMapStream delivers raw MON maps without applying them. Next calls can run
// concurrently; each batch is delivered once. Accepted batches drain before the
// stable terminal cause. Ordinary MON disconnection triggers resubscription.
// History can be trimmed and batches duplicated; no complete-chain guarantee
// is implied. Overflow ends this watch instead of dropping incremental data.
type OSDMapStream struct {
	client     *Client
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	startEpoch uint64
	limit      uint64

	// Protected by client.mu.
	source, observed *session.Session
	queue            []queuedOSDMap
	bufferedBytes    uint64
	lastEpoch        uint64
	hasEpoch         bool
	terminal         error
	changed          chan struct{}
}

// WatchOSDMaps creates the client's single explicitly requested MON OSDMap
// subscription. It requires no OSD connection or EnableOSD ticket opt-in.
// ctx owns the watch lifetime and recovery; Next's context owns only that wait.
// Success means local registration, not server permission or map delivery.
// Tentacle silently declines subscriptions without MON osd read capability.
// Reconnection requests the greatest accepted blob epoch + 1, not NewestMap or
// the epoch last consumed by Next. This is a delivery cursor, not an applied map.
// Closing is local: no remote unsubscribe is implied. MOSDMap has no generation
// identifier, so late batches from a prior watch on the same MON may arrive.
func (c *Client) WatchOSDMaps(ctx context.Context, options OSDMapOptions) (*OSDMapStream, error) {
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
		return nil, errors.New("ceph: invalid OSD map watch buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &OSDMapStream{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), startEpoch: uint64(options.StartEpoch), limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if c.osdMapWatch != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrOSDMapWatchActive
	}
	c.osdMapWatch = s
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()
	return s, nil
}

func (s *OSDMapStream) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *OSDMapStream) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal, s.source, s.observed = err, nil, nil
	if s.client.osdMapWatch == s {
		s.client.osdMapWatch = nil
	}
	s.signalLocked()
	return true
}

func (s *OSDMapStream) stop(err error) {
	// Lifetime cancellation can run before a failed source finishes custom
	// connection cleanup/onClose. Preserve its already published fatal cause,
	// as Close does, before releasing the watch slot. Inspect errors outside mu.
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		if previous := source.Err(); fatalLogSessionError(previous) {
			s.failCurrent(source, previous)
		}
	}
	s.client.mu.Lock()
	s.stopLocked(err)
	s.client.mu.Unlock()
	s.cancel()
}

// Close releases the watch slot and waits for its registration worker to exit.
// Accepted batches remain readable. Closing an active watch sets its terminal
// cause to ErrOSDMapStreamClosed; an existing terminal cause remains unchanged.
func (s *OSDMapStream) Close() error {
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			s.failCurrent(source, err)
		}
	}
	s.stop(ErrOSDMapStreamClosed)
	<-s.done
	return nil
}

// Next waits for one accepted batch. Canceling ctx ends only this call and
// leaves the watch active. An already canceled ctx consumes no queued batch.
// With a live ctx, accepted batches drain before the stable terminal error.
func (s *OSDMapStream) Next(ctx context.Context) (OSDMapBatch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return OSDMapBatch{}, err
		}
		s.client.mu.Lock()
		if len(s.queue) != 0 {
			next := s.queue[0]
			s.queue[0] = queuedOSDMap{}
			s.queue = s.queue[1:]
			s.bufferedBytes -= next.bytes
			s.client.mu.Unlock()
			return next.batch, nil
		}
		err, changed := s.terminal, s.changed
		s.client.mu.Unlock()
		if err != nil {
			return OSDMapBatch{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return OSDMapBatch{}, ctx.Err()
		case <-s.ctx.Done():
			s.stop(s.ctx.Err())
		case <-s.client.ctx.Done():
			s.stop(ErrClosed)
		}
	}
}

// Preserve a registered source's terminal protocol error before a replacement
// can make it stale. Custom error inspection and cancellation run outside mu.
func (c *Client) stopOSDMapForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watch := c.osdMapWatch
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

func (s *OSDMapStream) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && c.osdMapWatch == s && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}

func (s *OSDMapStream) run() {
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
			next := s.startEpoch
			if register {
				if s.hasEpoch {
					next = max(s.startEpoch, s.lastEpoch+1) // uint64 also represents MaxUint32 + 1.
				}
				s.source, s.observed = mon, mon
			}
			c.mu.Unlock()
			if register {
				if err := mon.Send(s.ctx, msgr.OSDMapSubscribe(next, false, c.options.Hostname)); err != nil {
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
			// onClose publishes changed after the session becomes terminal.
			// Wait on changed below rather than spinning on a closed Done.
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

func (c *Client) handleMonOSDMap(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.osdMapWatch
	accept := s != nil && s.acceptsLocked(source)
	fsid := c.fsid
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := maps.DecodeOSDMessage(m, c.options.MaxFrameSize)
	if err == nil && decoded.FSID != fsid {
		err = errors.New("MON OSD map belongs to a different cluster")
	}
	if err != nil {
		err = fmt.Errorf("%w: invalid MON OSD map: %w", msgr.ErrFrame, err)
	}
	batch := publicOSDMapBatch(m, decoded)
	size := osdMapBatchBytes(batch)
	// A terminal source may still be inside delayed connection cleanup while
	// a previously entered callback finishes. Do not accept its late data or
	// let local overflow replace an already published fatal session cause.
	// Error inspection can invoke caller code and stays outside client.mu.
	if previous := source.Err(); previous != nil {
		c.stopOSDMapForFailedSession(source, previous)
		return err // A late decode error cannot replace Session.Fail's winner.
	}
	c.mu.Lock()
	if !s.acceptsLocked(source) {
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		// Session.Fail arbitrates the first cause, including a writer failure
		// published while decoding. The worker and onClose observe its winner.
		c.mu.Unlock()
		return err
	}
	if len(s.queue) >= 64 || size > s.limit-s.bufferedBytes {
		s.stopLocked(ErrOSDMapWatchOverflow)
		c.mu.Unlock()
		s.cancel()
		return nil
	}
	s.queue = append(s.queue, queuedOSDMap{batch: batch, bytes: size})
	s.bufferedBytes += size
	for _, blobs := range [][]OSDMapBlob{batch.FullMaps, batch.IncrementalMaps} {
		for _, blob := range blobs {
			if !s.hasEpoch || uint64(blob.Epoch) > s.lastEpoch {
				s.lastEpoch, s.hasEpoch = uint64(blob.Epoch), true
			}
		}
	}
	s.signalLocked()
	c.mu.Unlock()
	return nil
}

func (s *OSDMapStream) acceptsLocked(source *session.Session) bool {
	c := s.client
	return c.osdMapWatch == s && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
}

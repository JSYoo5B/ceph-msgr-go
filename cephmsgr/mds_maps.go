package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

var (
	ErrFSMapUserWatchActive  = errors.New("ceph: a filesystem directory watch is already active")
	ErrMDSMapWatchActive     = errors.New("ceph: an MDS map watch is already active")
	ErrFSMapUserStreamClosed = errors.New("ceph: filesystem directory stream closed")
	ErrMDSMapStreamClosed    = errors.New("ceph: MDS map stream closed")
	// ErrMDSMapSourceBound prevents associating untagged MMDSMap replies with a
	// second watch on the same MON session, even after the first watch closes.
	ErrMDSMapSourceBound    = errors.New("ceph: MON session already bound to another MDS map watch")
	ErrMDSMapWatchOverflow  = errors.New("ceph: MON map watch buffer exceeded")
	ErrMDSMapEpochExhausted = errors.New("ceph: MON map epoch exhausted")
)

// FSMapUserOptions controls the raw filesystem directory stream.
type FSMapUserOptions struct {
	// StartEpoch is inclusive; zero asks for the latest observation.
	StartEpoch uint32
	// MaxBufferedBytes defaults to MaxFrameSize; valid values are 1 KiB to 1 GiB.
	// Accounting includes decoded metadata; the FIFO also permits at most 64 maps.
	MaxBufferedBytes uint32
}

// MDSMapOptions controls an explicitly identified filesystem's raw map stream.
type MDSMapOptions struct {
	// StartEpoch is inclusive; zero asks for the latest observation.
	StartEpoch uint32
	// MaxBufferedBytes defaults to MaxFrameSize; valid values are 1 KiB to 1 GiB.
	MaxBufferedBytes uint32
}

// FSMapUserStream delivers raw filesystem directory observations. Duplicate
// and decreasing epochs are ignored. Recovery requests the greatest accepted
// epoch + 1; retained history and delivery of every epoch are not guaranteed.
// Close and lifetime cancellation are local and preserve accepted queued maps.
// A new watch may observe late same-topic frames from an earlier watch if they
// satisfy StartEpoch. SourceGeneration is local delivery context, not proof a
// map was generated or sent after registration.
type FSMapUserStream struct{ watch *monMDSMapWatch }

// MDSMapStream delivers raw metadata for a caller-supplied filesystem ID.
// MMDSMap does not carry that ID. One watch owns each MON source permanently
// until the source closes; closing this stream does not permit a second MDS
// map watch on that same MON session. Use a fresh Client for another watch.
// Recovery on a fresh MON retains the explicit ID and highest accepted epoch.
// Duplicate/decreasing epochs are ignored; no complete history is promised.
// This API establishes no MDS daemon session or CephFS operation support.
type MDSMapStream struct{ watch *monMDSMapWatch }

type mdsMapBinding struct {
	watchID      uint64
	filesystemID int32
}
type queuedMONMDSMap struct {
	next  *queuedMONMDSMap
	fs    FSMapUserUpdate
	mds   MDSMapUpdate
	bytes uint64
}
type monMDSMapWatch struct {
	client       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	isMDS        bool
	filesystemID int32
	id           uint64
	startEpoch   uint64
	limit        uint64
	// All remaining fields are protected by client.mu.
	source, observed     *session.Session
	generation           uint64
	requested            uint64
	queueHead, queueTail *queuedMONMDSMap
	queueCount           int
	bufferedBytes        uint64
	lastEpoch            uint32
	hasEpoch             bool
	terminal             error
	changed              chan struct{}
}

// WatchFSMapUser requires EnableMDSMaps set before Dial. Success means local
// registration, not MON permission or map delivery. ctx owns the stream's
// lifetime; a Next context controls only that wait. No filesystem is selected.
func (c *Client) WatchFSMapUser(ctx context.Context, options FSMapUserOptions) (*FSMapUserStream, error) {
	s, err := c.watchMONMDSMap(ctx, false, 0, options.StartEpoch, options.MaxBufferedBytes)
	if err != nil {
		return nil, err
	}
	return &FSMapUserStream{watch: s}, nil
}

// WatchMDSMaps requires EnableMDSMaps and an explicit nonnegative filesystem
// ID. It never resolves a name or chooses a default filesystem. Closing is a
// local action, without a wire unsubscribe or remote cancellation guarantee.
func (c *Client) WatchMDSMaps(ctx context.Context, filesystemID int32, options MDSMapOptions) (*MDSMapStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if filesystemID < 0 {
		return nil, errors.New("ceph: invalid filesystem ID")
	}
	s, err := c.watchMONMDSMap(ctx, true, filesystemID, options.StartEpoch, options.MaxBufferedBytes)
	if err != nil {
		return nil, err
	}
	return &MDSMapStream{watch: s}, nil
}

func (c *Client) watchMONMDSMap(ctx context.Context, isMDS bool, id int32, epoch, limit uint32) (*monMDSMapWatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if !c.options.EnableMDSMaps {
		return nil, fmt.Errorf("%w: EnableMDSMaps was not selected before Dial", ErrUnsupportedFeatures)
	}
	if limit == 0 {
		limit = c.options.MaxFrameSize
	}
	if limit < 1<<10 || limit > 1<<30 {
		return nil, errors.New("ceph: invalid MON map watch buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &monMDSMapWatch{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), isMDS: isMDS, filesystemID: id, startEpoch: uint64(epoch), limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	var err error
	switch {
	case c.closed || c.ctx.Err() != nil:
		err = ErrClosed
	case c.authErr != nil:
		err = c.authErr
	case isMDS && c.mdsMapWatch != nil:
		err = ErrMDSMapWatchActive
	case !isMDS && c.fsMapWatch != nil:
		err = ErrFSMapUserWatchActive
	case isMDS && c.mdsMapBindings[c.mon].watchID != 0:
		err = ErrMDSMapSourceBound
	case c.nextMDSMapWatchID == math.MaxUint64:
		err = ErrMDSMapEpochExhausted
	}
	if err != nil {
		c.mu.Unlock()
		cancel()
		return nil, err
	}
	c.nextMDSMapWatchID++
	s.id = c.nextMDSMapWatchID
	if isMDS {
		c.mdsMapWatch = s
	} else {
		c.fsMapWatch = s
	}
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()
	return s, nil
}

// Next consumes one observation. An already canceled context consumes nothing.
// Queued observations drain before the stable terminal cause.
func (s *FSMapUserStream) Next(ctx context.Context) (FSMapUserUpdate, error) {
	q, e := s.watch.next(ctx)
	return q.fs, e
}
func (s *MDSMapStream) Next(ctx context.Context) (MDSMapUpdate, error) {
	q, e := s.watch.next(ctx)
	return q.mds, e
}

// Close waits for the registration worker; accepted observations remain readable.
func (s *FSMapUserStream) Close() error { return s.watch.close(ErrFSMapUserStreamClosed) }
func (s *MDSMapStream) Close() error    { return s.watch.close(ErrMDSMapStreamClosed) }
func (s *monMDSMapWatch) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }
func (s *monMDSMapWatch) activeLocked() bool {
	if s.isMDS {
		return s.client.mdsMapWatch == s
	}
	return s.client.fsMapWatch == s
}
func (s *monMDSMapWatch) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal = err
	s.source = nil
	s.observed = nil
	if s.isMDS {
		if s.client.mdsMapWatch == s {
			s.client.mdsMapWatch = nil
		}
	} else if s.client.fsMapWatch == s {
		s.client.fsMapWatch = nil
	}
	s.signalLocked()
	return true
}
func (s *monMDSMapWatch) stop(err error) {
	c := s.client
	c.mu.Lock()
	source := s.observed
	c.mu.Unlock()
	if source != nil {
		if previous := source.Err(); fatalLogSessionError(previous) {
			s.failCurrent(source, previous)
		}
	}
	c.mu.Lock()
	s.stopLocked(err)
	c.mu.Unlock()
	s.cancel()
}
func (s *monMDSMapWatch) close(err error) error { s.stop(err); <-s.done; return nil }
func (s *monMDSMapWatch) next(ctx context.Context) (queuedMONMDSMap, error) {
	for {
		if err := ctx.Err(); err != nil {
			return queuedMONMDSMap{}, err
		}
		s.client.mu.Lock()
		if s.queueHead != nil {
			q := *s.queueHead
			s.queueHead = q.next
			q.next = nil
			s.queueCount--
			if s.queueHead == nil {
				s.queueTail = nil
			}
			s.bufferedBytes -= q.bytes
			s.client.mu.Unlock()
			return q, nil
		}
		err, changed := s.terminal, s.changed
		s.client.mu.Unlock()
		if err != nil {
			return queuedMONMDSMap{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return queuedMONMDSMap{}, ctx.Err()
		case <-s.ctx.Done():
			s.stop(s.ctx.Err())
		case <-s.client.ctx.Done():
			s.stop(ErrClosed)
		}
	}
}
func (s *monMDSMapWatch) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && s.activeLocked() && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}
func (c *Client) stopMDSMapsForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watches := []*monMDSMapWatch{c.fsMapWatch, c.mdsMapWatch}
	var stopped []*monMDSMapWatch
	for _, s := range watches {
		if s != nil && c.mon == source && s.observed == source && s.stopLocked(err) {
			stopped = append(stopped, s)
		}
	}
	c.mu.Unlock()
	for _, s := range stopped {
		s.cancel()
	}
}
func (c *Client) resetMDSMapSourcesLocked() {
	for _, s := range []*monMDSMapWatch{c.fsMapWatch, c.mdsMapWatch} {
		if s != nil {
			s.source = nil
		}
	}
}
func (c *Client) stopMDSMapWatchesLocked(err error) []*monMDSMapWatch {
	var stopped []*monMDSMapWatch
	for _, s := range []*monMDSMapWatch{c.fsMapWatch, c.mdsMapWatch} {
		if s != nil && s.stopLocked(err) {
			stopped = append(stopped, s)
		}
	}
	return stopped
}
func (s *monMDSMapWatch) run() {
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
			register := s.terminal == nil && s.activeLocked() && c.mon == mon && c.monReady && !c.closed && c.authErr == nil && s.source != mon
			next := s.startEpoch
			var localErr error
			if register {
				if s.hasEpoch {
					next = max(s.startEpoch, uint64(s.lastEpoch)+1)
				}
				if s.isMDS {
					binding := c.mdsMapBindings[mon]
					if binding.watchID != 0 && (binding.watchID != s.id || binding.filesystemID != s.filesystemID) {
						localErr = ErrMDSMapSourceBound
					} else {
						if c.mdsMapBindings == nil {
							c.mdsMapBindings = make(map[*session.Session]mdsMapBinding)
						}
						c.mdsMapBindings[mon] = mdsMapBinding{s.id, s.filesystemID}
					}
				}
				generation := c.mdsMapSourceGenerations[mon]
				if generation == 0 && c.mdsMapGeneration == math.MaxUint64 {
					localErr = ErrMDSMapEpochExhausted
				}
				if localErr == nil {
					if generation == 0 {
						c.mdsMapGeneration++
						generation = c.mdsMapGeneration
						if c.mdsMapSourceGenerations == nil {
							c.mdsMapSourceGenerations = make(map[*session.Session]uint64)
						}
						c.mdsMapSourceGenerations[mon] = generation
					}
					s.generation = generation
					s.requested = next
					s.source, s.observed = mon, mon
				} else {
					s.stopLocked(localErr)
				}
			}
			c.mu.Unlock()
			if localErr != nil {
				s.cancel()
				return
			}
			if register {
				message := msgr.FSMapUserSubscribe(next, c.options.Hostname)
				if s.isMDS {
					message = msgr.MDSMapSubscribe(s.filesystemID, next, c.options.Hostname)
				}
				if err := mon.Send(s.ctx, message); err != nil {
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
func (s *monMDSMapWatch) acceptsLocked(source *session.Session) bool {
	c := s.client
	return s.activeLocked() && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
}
func (c *Client) handleMonMDSMap(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.fsMapWatch
	if m.Type == mds.MDSMapMessageType {
		s = c.mdsMapWatch
	}
	accept := s != nil && s.acceptsLocked(source)
	fsid := c.fsid
	c.mu.Unlock()
	if !accept {
		return nil
	}
	var q queuedMONMDSMap
	var epoch uint32
	var err error
	if s.isMDS {
		var decoded mds.MDSMapMessage
		decoded, err = mds.DecodeMDSMapMessage(m, c.options.MaxFrameSize)
		if err == nil && decoded.FSID != fsid {
			err = errors.New("MDS map belongs to a different cluster")
		}
		if err == nil {
			q.mds = publicMDSMap(decoded, s.filesystemID, 0)
			epoch = decoded.Epoch
		}
	} else {
		var decoded mds.FSMapUser
		decoded, err = mds.DecodeFSMapUser(m, c.options.MaxFrameSize)
		if err == nil {
			q.fs = publicFSMapUser(decoded, 0)
			epoch = decoded.Epoch
		}
	}
	if err != nil {
		err = fmt.Errorf("%w: invalid MON filesystem map: %w", msgr.ErrFrame, err)
	}
	if previous := source.Err(); previous != nil {
		c.stopMDSMapsForFailedSession(source, previous)
		return err
	}
	c.mu.Lock()
	if !s.acceptsLocked(source) {
		c.mu.Unlock()
		return nil
	}
	if err != nil {
		c.mu.Unlock()
		return err
	}
	if uint64(epoch) < s.requested || (s.hasEpoch && epoch <= s.lastEpoch) {
		c.mu.Unlock()
		return nil
	}
	// Reserve the fixed linked FIFO node (including the large public map
	// value) plus conservative owned-front/decoded collection storage. One
	// node per queued map avoids unused backing-array capacity retaining
	// uncharged nodes after FIFO consumption.
	q.bytes = 768 + 8*uint64(len(m.Front))
	if s.queueCount >= 64 || q.bytes > s.limit-s.bufferedBytes {
		s.stopLocked(ErrMDSMapWatchOverflow)
		c.mu.Unlock()
		s.cancel()
		return nil
	}
	q.fs.SourceGeneration = s.generation
	q.mds.SourceGeneration = s.generation
	if s.queueTail == nil {
		s.queueHead = &q
	} else {
		s.queueTail.next = &q
	}
	s.queueTail = &q
	s.queueCount++
	s.bufferedBytes += q.bytes
	s.lastEpoch, s.hasEpoch = epoch, true
	s.signalLocked()
	exhausted := epoch == math.MaxUint32
	if exhausted {
		s.stopLocked(ErrMDSMapEpochExhausted)
	}
	c.mu.Unlock()
	if exhausted {
		s.cancel()
	}
	return nil
}

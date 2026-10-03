package cephmsgr

import (
	"context"
	"errors"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// ErrConfigWatchActive means another watch owns this client's config stream.
var ErrConfigWatchActive = errors.New("ceph: a config watch is already active")

// ErrConfigStreamClosed is the terminal cause of an explicitly closed watch.
var ErrConfigStreamClosed = errors.New("ceph: config stream closed")

// ErrConfigOverflow terminates a watch that cannot retain an effective map.
// The previously accepted unread map and shared MON connection remain intact.
var ErrConfigOverflow = errors.New("ceph: config watch buffer exceeded")

// ConfigOptions bounds the unread full map retained by a config watch.
type ConfigOptions struct {
	// MaxBufferedBytes bounds a conservative estimate of one unread full map:
	// 512 bytes plus 128 bytes per entry and its key/value byte lengths. The
	// default is the client's MaxFrameSize; accepted values are 1 KiB–1 GiB.
	// This estimate is not an exact Go heap measurement.
	MaxBufferedBytes uint32
}

// ConfigStream receives full effective configuration maps for the authenticated
// client identity. Keys and values remain raw strings and are never applied to
// Go client Options. One unread map is retained; a later full map replaces it,
// including deletion of absent keys. Concurrent Next calls each receive a map
// at most once. The returned map belongs to that caller.
//
// Config payloads have no subscription generation, correlated request ID,
// configuration revision, cursor or FSID. A late response from a prior watch
// on the same admitted MON session cannot be distinguished. Recovery requests
// a new full map; intermediate
// changes and lossless delivery are not guaranteed. Close ends only this watch,
// without a remote unsubscribe. Already accepted data drains before the stable
// first terminal error.
type ConfigStream struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	limit  uint64

	// All fields below are protected by client.mu.
	source   *session.Session
	observed *session.Session
	pending  map[string]string // nil means no unread map; an empty map is valid.
	terminal error
	changed  chan struct{}
}

// WatchConfig registers the client's single active effective-config watch.
// ctx governs the watch's full lifetime, including MON recovery; each Next
// context governs only that local wait. It submits no command, takes no command
// slot and does not open a MGR connection. Success registers a local worker,
// not a server permission or delivery acknowledgement. Every source registration
// requests a full map for the authenticated client.* identity and the client's
// Options.Hostname, with an empty device-class mask, including a new watch on
// an existing MON session.
func (c *Client) WatchConfig(ctx context.Context, options ConfigOptions) (*ConfigStream, error) {
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
		return nil, errors.New("ceph: invalid config buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &ConfigStream{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if c.configWatch != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrConfigWatchActive
	}
	c.configWatch = s
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()
	return s, nil
}

func (s *ConfigStream) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *ConfigStream) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal, s.source, s.observed = err, nil, nil
	if s.client.configWatch == s {
		s.client.configWatch = nil
	}
	s.signalLocked()
	return true
}

func (s *ConfigStream) stop(err error) {
	s.client.mu.Lock()
	s.stopLocked(err)
	s.client.mu.Unlock()
	s.cancel()
}

// Close releases the watch slot and joins its registration worker. The unread
// map remains readable. ErrConfigStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *ConfigStream) Close() error {
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			s.failCurrent(source, err)
		}
	}
	s.stop(ErrConfigStreamClosed)
	<-s.done
	return nil
}

// Next returns the latest unread full map, transferring its ownership. A live
// context drains the accepted map before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes
// nothing. An empty non-nil map is a valid full replacement.
func (s *ConfigStream) Next(ctx context.Context) (map[string]string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.client.mu.Lock()
		if s.pending != nil {
			next := s.pending
			s.pending = nil
			s.client.mu.Unlock()
			return next, nil
		}
		err, changed := s.terminal, s.changed
		s.client.mu.Unlock()
		if err != nil {
			return nil, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
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
func (c *Client) stopConfigForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watch := c.configWatch
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

func (s *ConfigStream) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && c.configWatch == s && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}

func (s *ConfigStream) run() {
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
			identity := ""
			if register {
				s.source, s.observed = mon, mon
				identity = c.auth.Name
			}
			c.mu.Unlock()
			if register {
				err := mon.Send(s.ctx, msgr.ConfigSubscribe(c.options.Hostname))
				if err == nil {
					err = mon.Send(s.ctx, msgr.GetConfig(identity, c.options.Hostname))
				}
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

func (c *Client) handleConfig(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.configWatch
	accept := s != nil && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeConfig(m, c.options.MaxFrameSize)
	bytes := uint64(512)
	if err == nil {
		for key, value := range decoded {
			bytes += 128 + uint64(len(key)) + uint64(len(value))
		}
	}
	c.mu.Lock()
	if c.configWatch != s || s.terminal != nil || s.source != source || c.mon != source || !c.monReady || c.closed || c.authErr != nil {
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
		s.stopLocked(ErrConfigOverflow)
		c.mu.Unlock()
		s.cancel()
		return nil
	}
	s.pending = decoded
	s.signalLocked()
	c.mu.Unlock()
	return nil
}

package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

// ErrDigestWatchActive means another watch owns this client's digest stream.
var ErrDigestWatchActive = errors.New("ceph: a digest watch is already active")

// ErrDigestStreamClosed is the terminal cause of an explicitly closed watch.
var ErrDigestStreamClosed = errors.New("ceph: digest stream closed")

// ErrDigestOverflow terminates a watch that cannot retain a full digest.
// The previously accepted unread digest and shared MON connection remain intact.
var ErrDigestOverflow = errors.New("ceph: digest watch buffer exceeded")

// DigestOptions bounds the unread full digest retained by a watch.
type DigestOptions struct {
	// MaxBufferedBytes bounds a conservative estimate of one unread digest:
	// 512 bytes plus the byte lengths of MonStatus and Health. The default is
	// the client's MaxFrameSize; accepted values are 1 KiB–1 GiB. This estimate
	// is not an exact Go heap measurement.
	MaxBufferedBytes uint32
}

// ClusterDigest contains the raw MON status and health buffers from one server
// publication. They are preserved without JSON validation or normalization.
// Both slices belong to the caller returned by Next.
type ClusterDigest struct {
	MonStatus json.RawMessage
	Health    json.RawMessage
}

// DigestStream receives periodic full cluster digests from the admitted MON.
// One unread pair is retained; a later digest replaces both buffers together.
// Concurrent Next calls each receive a digest at most once.
//
// Digest payloads have no subscription generation, correlated request ID,
// revision, cursor or FSID. A late response from a prior watch on the same MON
// session cannot be distinguished. Recovery resubscribes for a new full digest;
// intermediate publications and lossless delivery are not guaranteed. Close
// ends only this watch, without a remote unsubscribe. Accepted data drains
// before the stable first terminal error recorded by this watch.
type DigestStream struct {
	client *Client
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	limit  uint64

	// All fields below are protected by client.mu.
	source   *session.Session
	observed *session.Session
	pending  *ClusterDigest // nil means no unread digest; two empty buffers are valid.
	terminal error
	changed  chan struct{}
}

// WatchDigest registers the client's single active cluster-digest watch.
// ctx governs the watch's full lifetime, including MON recovery; each Next
// context governs only that local wait. It submits no command, takes no command
// slot and does not open a MGR connection. Success registers a local worker,
// not a server permission or delivery acknowledgement. The MON determines the
// publication schedule; a new registration does not guarantee immediate data.
func (c *Client) WatchDigest(ctx context.Context, options DigestOptions) (*DigestStream, error) {
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
		return nil, errors.New("ceph: invalid digest buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &DigestStream{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if c.digestWatch != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrDigestWatchActive
	}
	c.digestWatch = s
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()
	return s, nil
}

func (s *DigestStream) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *DigestStream) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal, s.source, s.observed = err, nil, nil
	if s.client.digestWatch == s {
		s.client.digestWatch = nil
	}
	s.signalLocked()
	return true
}

func (s *DigestStream) stop(err error) {
	s.client.mu.Lock()
	s.stopLocked(err)
	s.client.mu.Unlock()
	s.cancel()
}

// Close releases the watch slot and joins its registration worker. The unread
// digest remains readable. ErrDigestStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *DigestStream) Close() error {
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			s.failCurrent(source, err)
		}
	}
	s.stop(ErrDigestStreamClosed)
	<-s.done
	return nil
}

// Next returns the latest unread full digest, transferring both buffers' ownership.
// A live context drains accepted data before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes
// nothing. A pair of empty buffers is a valid full digest.
func (s *DigestStream) Next(ctx context.Context) (ClusterDigest, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ClusterDigest{}, err
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
			return ClusterDigest{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ClusterDigest{}, ctx.Err()
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
func (c *Client) stopDigestForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watch := c.digestWatch
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

func (s *DigestStream) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && c.digestWatch == s && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}

func (s *DigestStream) run() {
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
				err := mon.Send(s.ctx, msgr.DigestSubscribe(c.options.Hostname))
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

func (c *Client) handleDigest(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.digestWatch
	accept := s != nil && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeDigest(m, c.options.MaxFrameSize)
	bytes := uint64(512) + uint64(len(decoded.MonStatus)) + uint64(len(decoded.Health))
	c.mu.Lock()
	if c.digestWatch != s || s.terminal != nil || s.source != source || c.mon != source || !c.monReady || c.closed || c.authErr != nil {
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
		s.stopLocked(ErrDigestOverflow)
		c.mu.Unlock()
		s.cancel()
		return nil
	}
	s.pending = &ClusterDigest{MonStatus: decoded.MonStatus, Health: decoded.Health}
	s.signalLocked()
	c.mu.Unlock()
	return nil
}

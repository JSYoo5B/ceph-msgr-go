package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
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
	watch monSnapshotWatch[DigestStream]

	// Protected by client.mu.
	pending *ClusterDigest // nil means no unread digest; two empty buffers are valid.
}

func newDigestStream(c *Client, ctx context.Context, cancel context.CancelFunc, limit uint64) *DigestStream {
	s := &DigestStream{}
	s.watch = newMonSnapshotWatch(c, ctx, cancel, limit, &c.digestWatch, s, s.registerSource)
	return s
}

// registerSource subscribes to periodic full digests on an admitted MON session.
func (s *DigestStream) registerSource(ctx context.Context, mon *session.Session, _ string) error {
	return mon.Send(ctx, msgr.DigestSubscribe(s.watch.client.options.Hostname))
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
	s := newDigestStream(c, watchCtx, cancel, uint64(limit))
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
	go s.watch.run()
	return s, nil
}

// Close releases the watch slot and joins its registration worker. The unread
// digest remains readable. ErrDigestStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *DigestStream) Close() error {
	s.watch.closeWatch(ErrDigestStreamClosed)
	return nil
}

// Next returns the latest unread full digest, transferring both buffers' ownership.
// A live context drains accepted data before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes
// nothing. A pair of empty buffers is a valid full digest.
func (s *DigestStream) Next(ctx context.Context) (ClusterDigest, error) {
	return nextStreamItem(ctx, s.watch.client, s.watch.ctx, func() (ClusterDigest, bool, error, <-chan struct{}) {
		if s.pending != nil {
			next := *s.pending
			s.pending = nil
			return next, true, nil, nil
		}
		return ClusterDigest{}, false, s.watch.terminal, s.watch.changed
	}, s.watch.stop)
}

func (c *Client) handleDigest(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.digestWatch
	accept := s != nil && s.watch.acceptsLocked(source)
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeDigest(m, c.options.MaxFrameSize)
	bytes := uint64(512) + uint64(len(decoded.MonStatus)) + uint64(len(decoded.Health))
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
		s.watch.stopLocked(ErrDigestOverflow)
		c.mu.Unlock()
		s.watch.cancel()
		return nil
	}
	s.pending = &ClusterDigest{MonStatus: decoded.MonStatus, Health: decoded.Health}
	s.watch.signalLocked()
	c.mu.Unlock()
	return nil
}

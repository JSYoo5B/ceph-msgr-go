package cephmsgr

import (
	"context"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
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
	watch monSnapshotWatch[ConfigStream]

	// Protected by client.mu.
	pending map[string]string // nil means no unread map; an empty map is valid.
}

func newConfigStream(c *Client, ctx context.Context, cancel context.CancelFunc, limit uint64) *ConfigStream {
	s := &ConfigStream{}
	s.watch = newMonSnapshotWatch(c, ctx, cancel, limit, &c.configWatch, s, s.registerSource)
	return s
}

// registerSource requests config updates and an immediate full map for the
// authenticated identity on an admitted MON session.
func (s *ConfigStream) registerSource(ctx context.Context, mon *session.Session, identity string) error {
	hostname := s.watch.client.options.Hostname
	if err := mon.Send(ctx, msgr.ConfigSubscribe(hostname)); err != nil {
		return err
	}
	return mon.Send(ctx, msgr.GetConfig(identity, hostname))
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
	s := newConfigStream(c, watchCtx, cancel, uint64(limit))
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
	go s.watch.run()
	return s, nil
}

// Close releases the watch slot and joins its registration worker. The unread
// map remains readable. ErrConfigStreamClosed becomes the terminal cause only
// when no earlier cause was recorded.
func (s *ConfigStream) Close() error {
	s.watch.closeWatch(ErrConfigStreamClosed)
	return nil
}

// Next returns the latest unread full map, transferring its ownership. A live
// context drains the accepted map before returning the stable terminal cause.
// Cancellation affects only this call; an already canceled context consumes
// nothing. An empty non-nil map is a valid full replacement.
func (s *ConfigStream) Next(ctx context.Context) (map[string]string, error) {
	return nextStreamItem(ctx, s.watch.client, s.watch.ctx, func() (map[string]string, bool, error, <-chan struct{}) {
		if s.pending != nil {
			next := s.pending
			s.pending = nil
			return next, true, nil, nil
		}
		return nil, false, s.watch.terminal, s.watch.changed
	}, s.watch.stop)
}

func (c *Client) handleConfig(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.configWatch
	accept := s != nil && s.watch.acceptsLocked(source)
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
		s.watch.stopLocked(ErrConfigOverflow)
		c.mu.Unlock()
		s.watch.cancel()
		return nil
	}
	s.pending = decoded
	s.watch.signalLocked()
	c.mu.Unlock()
	return nil
}

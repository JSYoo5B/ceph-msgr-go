package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// ErrLogWatchActive means another watch still owns this client's log stream.
var ErrLogWatchActive = errors.New("ceph: a log watch is already active")

// ErrLogOverflow terminates a watch whose bounded queue cannot accept a batch.
// It does not terminate the shared MON connection or discard accepted batches.
var ErrLogOverflow = errors.New("ceph: log watch buffer exceeded")

// ErrLogStreamClosed is the terminal cause of an explicitly closed watch.
var ErrLogStreamClosed = errors.New("ceph: log stream closed")

// ErrLogCursorOverflow rejects a service version that cannot be incremented.
var ErrLogCursorOverflow = errors.New("ceph: log service version cannot be resumed")

// LogLevel requests a minimum cluster-log priority from the server. Its zero
// value is info. Delivered entries retain their raw Priority; Ceph can send a
// lower-priority warning when history was skipped.
type LogLevel uint8

const (
	LogInfo LogLevel = iota
	LogDebug
	LogSec
	LogWarn
	LogError
)

type LogOptions struct {
	Level LogLevel
	// StartVersion is an inclusive log-service cursor. Zero requests Ceph's
	// current last committed batch. A caller can resume across clients using
	// the previous batch's Version + 1; MaxUint64 cannot be resumed.
	StartVersion uint64
	// MaxBufferedBytes limits a conservative estimate of owned queued data,
	// including text, addresses and entry metadata. It defaults to the client's
	// MaxFrameSize, with accepted values from 1 KiB to 1 GiB. The estimate is
	// not an exact Go heap measurement. At most 64 batches are buffered.
	MaxBufferedBytes uint32
}

// LogBatch preserves one MON log-service update. Version is the service cursor,
// distinct from Messenger message versions and individual entry sequences.
// Ceph can trim history; a server entry reporting skipped logs is preserved.
type LogBatch struct {
	FSID    string
	Version uint64
	Entries []LogEntry
}

// LogEntry preserves the server's entity, timestamp and text without formatting
// or normalizing them. Nanoseconds is the raw utime_t fractional field.
type LogEntry struct {
	NameType             uint32
	NameID               string
	RankType             uint8
	RankNumber           int64
	Addresses            []LogAddress
	Seconds, Nanoseconds uint32
	Sequence             uint64
	Priority             uint16
	Message, Channel     string
}

// LogAddress retains the wire address family and its nonce, flow and scope.
type LogAddress struct {
	Type, Nonce       uint32
	Endpoint          netip.AddrPort
	FlowInfo, ScopeID uint32
}

type queuedLogBatch struct {
	batch LogBatch
	bytes uint64
}

// LogStream is a bounded MON log subscription. Next calls can run concurrently;
// each accepted batch is returned once to one caller. A subscription acknowledgement
// does not establish read permission: Ceph may silently decline a log subscription.
// After termination, Next drains already accepted batches before returning the
// same terminal error. The client owns the connection; Close ends only this watch.
type LogStream struct {
	client       *Client
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	level        string
	startVersion uint64
	limit        uint64

	// All fields below are protected by client.mu.
	source        *session.Session
	observed      *session.Session // Last session admitted to this watch.
	queue         []queuedLogBatch
	bufferedBytes uint64
	lastVersion   uint64
	hasVersion    bool
	terminal      error
	changed       chan struct{}
}

// WatchLogs creates the client's single active cluster-log watch. ctx governs
// the whole watch, including recovery; a Next context only governs that wait.
// Registration and recovery submit no command and use no MaxInFlight slot.
// Success registers a local worker; it does not confirm server registration or
// read permission. A zero StartVersion requests Ceph's last committed batch;
// other values request that inclusive cursor. Reconnection resumes after the
// last accepted service version. Cancellation is local; no remote unsubscribe or
// lossless-delivery guarantee is implied. MLog has no subscription-generation
// identifier; a late batch from a prior watch on the same session can arrive.
func (c *Client) WatchLogs(ctx context.Context, options LogOptions) (*LogStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	if options.StartVersion == math.MaxUint64 {
		return nil, ErrLogCursorOverflow
	}
	var level string
	switch options.Level {
	case LogDebug:
		level = "debug"
	case LogInfo:
		level = "info"
	case LogSec:
		level = "sec"
	case LogWarn:
		level = "warn"
	case LogError:
		level = "error"
	default:
		return nil, errors.New("ceph: invalid log level")
	}
	limit := options.MaxBufferedBytes
	if limit == 0 {
		limit = c.options.MaxFrameSize
	}
	if limit < 1<<10 || limit > 1<<30 {
		return nil, errors.New("ceph: invalid log buffer limit")
	}
	watchCtx, cancel := context.WithCancel(ctx)
	s := &LogStream{client: c, ctx: watchCtx, cancel: cancel, done: make(chan struct{}), level: level, startVersion: options.StartVersion, limit: uint64(limit), changed: make(chan struct{})}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	if c.logWatch != nil {
		c.mu.Unlock()
		cancel()
		return nil, ErrLogWatchActive
	}
	c.logWatch = s
	c.wg.Add(1) // Close cannot begin waiting before this worker is owned.
	c.mu.Unlock()
	go s.run()
	return s, nil
}

func (s *LogStream) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *LogStream) stopLocked(err error) bool {
	if s.terminal != nil {
		return false
	}
	s.terminal, s.source, s.observed = err, nil, nil
	if s.client.logWatch == s {
		s.client.logWatch = nil
	}
	s.signalLocked()
	return true
}

func (s *LogStream) stop(err error) {
	s.client.mu.Lock()
	s.stopLocked(err)
	s.client.mu.Unlock()
	s.cancel()
}

// Close releases the watch slot and waits for its registration worker to exit.
// Accepted batches remain readable. Closing an active watch sets its terminal
// cause to ErrLogStreamClosed; an existing terminal cause remains unchanged.
func (s *LogStream) Close() error {
	s.client.mu.Lock()
	source := s.observed
	s.client.mu.Unlock()
	if source != nil {
		err := source.Err()
		if fatalLogSessionError(err) {
			s.failCurrent(source, err)
		}
	}
	s.stop(ErrLogStreamClosed)
	<-s.done
	return nil
}

// Next waits for one accepted batch. Canceling ctx ends only this call and
// leaves the watch active. An already canceled ctx consumes no queued batch.
// With a live ctx, accepted batches drain before the stable terminal error.
func (s *LogStream) Next(ctx context.Context) (LogBatch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return LogBatch{}, err
		}
		s.client.mu.Lock()
		if len(s.queue) != 0 {
			next := s.queue[0]
			s.queue[0] = queuedLogBatch{}
			s.queue = s.queue[1:]
			s.bufferedBytes -= next.bytes
			s.client.mu.Unlock()
			return next.batch, nil
		}
		err, changed := s.terminal, s.changed
		s.client.mu.Unlock()
		if err != nil {
			return LogBatch{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return LogBatch{}, ctx.Err()
		case <-s.ctx.Done():
			s.stop(s.ctx.Err())
		case <-s.client.ctx.Done():
			s.stop(ErrClosed)
		}
	}
}

func fatalLogSessionError(err error) bool {
	var rejection *AuthenticationError
	return errors.As(err, &rejection) || errors.Is(err, msgr.ErrFrame) || errors.Is(err, msgr.ErrCRC) || errors.Is(err, msgr.ErrAuthentication) || errors.Is(err, msgr.ErrNonce) || errors.Is(err, wire.ErrVersion) || errors.Is(err, wire.ErrLimit) || errors.Is(err, cephx.ErrIntegrity)
}

// Preserve a registered source's terminal protocol error before a replacement
// can make it stale. Custom error inspection and cancellation run outside mu.
func (c *Client) stopLogForFailedSession(source *session.Session, err error) {
	if !fatalLogSessionError(err) {
		return
	}
	c.mu.Lock()
	watch := c.logWatch
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

func (s *LogStream) failCurrent(source *session.Session, err error) bool {
	c := s.client
	c.mu.Lock()
	current := s.terminal == nil && c.logWatch == s && c.mon == source && s.observed == source && !c.closed && c.authErr == nil
	if current {
		s.stopLocked(err)
	}
	c.mu.Unlock()
	if current {
		s.cancel()
	}
	return current
}

func (s *LogStream) run() {
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
			next := s.startVersion
			if register {
				if s.hasVersion {
					next = s.lastVersion + 1 // MaxUint64 is rejected before acceptance.
				}
				s.source, s.observed = mon, mon
			}
			c.mu.Unlock()
			if register {
				if err := mon.Send(s.ctx, msgr.LogSubscribe(s.level, next, c.options.Hostname)); err != nil {
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

func (c *Client) handleLog(source *session.Session, m msgr.MessageData) error {
	c.mu.Lock()
	s := c.logWatch
	accept := s != nil && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
	c.mu.Unlock()
	if !accept {
		return nil
	}
	decoded, err := msgr.DecodeLog(m, c.options.MaxFrameSize)
	if err != nil {
		c.mu.Lock()
		current := c.logWatch == s && s.terminal == nil && s.source == source && c.mon == source && c.monReady && !c.closed && c.authErr == nil
		if current {
			s.stopLocked(err)
		}
		c.mu.Unlock()
		if !current {
			return nil
		}
		s.cancel()
		return err
	}
	batch := LogBatch{FSID: fmt.Sprintf("%x-%x-%x-%x-%x", decoded.FSID[:4], decoded.FSID[4:6], decoded.FSID[6:8], decoded.FSID[8:10], decoded.FSID[10:]), Version: decoded.Version}
	bytes := uint64(512)
	for _, entry := range decoded.Entries {
		item := LogEntry{NameType: entry.NameType, NameID: entry.NameID, RankType: entry.RankType, RankNumber: entry.RankNumber, Seconds: entry.Seconds, Nanoseconds: entry.Nanoseconds, Sequence: entry.Sequence, Priority: entry.Priority, Message: entry.Message, Channel: entry.Channel}
		bytes += 512 + uint64(len(entry.NameID)) + uint64(len(entry.Message)) + uint64(len(entry.Channel)) + uint64(len(entry.Addresses))*128
		for _, address := range entry.Addresses {
			item.Addresses = append(item.Addresses, LogAddress{Type: address.Type, Nonce: address.Nonce, Endpoint: address.Endpoint, FlowInfo: address.FlowInfo, ScopeID: address.ScopeID})
		}
		batch.Entries = append(batch.Entries, item)
	}
	c.mu.Lock()
	if c.logWatch != s || s.terminal != nil || s.source != source || c.mon != source || !c.monReady || c.closed || c.authErr != nil {
		c.mu.Unlock()
		return nil
	}
	if decoded.FSID != c.fsid {
		err = fmt.Errorf("%w: log FSID mismatch", msgr.ErrFrame)
	} else if decoded.Version == math.MaxUint64 {
		err = ErrLogCursorOverflow
	} else if decoded.Version < s.startVersion {
		c.mu.Unlock()
		return nil
	} else if s.hasVersion && decoded.Version <= s.lastVersion {
		c.mu.Unlock()
		return nil
	} else if len(s.queue) == 64 || bytes > s.limit-s.bufferedBytes {
		err = ErrLogOverflow
	}
	if err != nil {
		s.stopLocked(err)
		c.mu.Unlock()
		s.cancel()
		if errors.Is(err, msgr.ErrFrame) {
			return err
		}
		return nil
	}
	s.queue = append(s.queue, queuedLogBatch{batch: batch, bytes: bytes})
	s.bufferedBytes += bytes
	s.lastVersion, s.hasVersion = decoded.Version, true
	s.signalLocked()
	c.mu.Unlock()
	return nil
}

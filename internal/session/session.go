package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

var ErrClosed = errors.New("ceph: client closed")
var ErrRetired = errors.New("ceph: session retired")
var ErrKeepaliveTimeout = errors.New("ceph messenger: keepalive timeout")

// OutcomeUnknownError means a request may have reached the daemon. It is not
// safe to infer that a mutation failed or to re-execute it automatically.
type OutcomeUnknownError struct{ Cause error }

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("ceph: request outcome unknown: %v", e.Cause)
}
func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

type response struct {
	message msgr.MessageData
	err     error
}
type request struct {
	ctx     context.Context
	message msgr.MessageData
	started bool
	result  chan response
	// completedErr preserves the first completed local/session failure when
	// cancellation races with a writer admission rejection or Fail.
	completedErr error // guarded by Session.mu
}

// Called with the session lock held. Completed calls can remain in the
// writer's queue; release their input and context without affecting a frame
// whose write has already started.
func (r *request) releaseUnstarted() {
	if !r.started {
		r.ctx = nil
		r.message = msgr.MessageData{}
	}
}

type Config struct {
	WriteTimeout, KeepaliveInterval, KeepaliveTimeout time.Duration
	// OSD request identities must not restart on a new connection belonging to
	// the same client. The coordinator can supply a client-wide transaction source.
	NextTransaction func() (uint64, error)
	// BeforeWrite runs on the single writer immediately before a queued
	// message starts transmission, outside the session lock. The message is
	// a read-only snapshot: the callback must not mutate or retain its bytes.
	// An error completes an unstarted Call locally, without failing the
	// transport or consuming a sequence number. A queued Send is discarded.
	// The callback must return promptly and must not wait for this writer.
	BeforeWrite func(msgr.MessageData) error
	// Resume, when set, makes the session stateful (lossless): a transport
	// fault resumes the Messenger session on a new transport (pinned
	// ProtocolV2 SESSION_RECONNECT) instead of failing. It receives the
	// current ResumeState with InSeq filled and returns a resumed transport
	// whose PeerSequence acknowledges retained messages. Protocol, CRC and
	// authentication errors still fail the session.
	Resume func(context.Context, ResumeState) (*Transport, error)
	// ResumeTimeout bounds one resume episode (default 30s). MaxUnacked
	// bounds retained unacknowledged messages (default 1024).
	ResumeTimeout time.Duration
	MaxUnacked    int
	// OnResume runs after each successful resume, outside session locks.
	OnResume func()
}

type Session struct {
	transport      *Transport
	config         Config
	mu             sync.Mutex
	pending        map[uint64]*request
	nextID         uint64
	err            error
	retiring       bool
	idle           chan struct{}
	queue          chan *request
	controls       chan msgr.Frame
	ack            chan struct{}
	done           chan struct{}
	cleanupDone    chan struct{}
	wg             sync.WaitGroup
	startOnce      sync.Once
	received, sent atomic.Uint64
	lastReceive    atomic.Pointer[time.Time]
	onMessage      func(msgr.MessageData) error
	onClose        func(error)
	// Stateful resume (Config.Resume); guarded by mu.
	epoch       uint64
	stop        chan struct{}
	loops       *sync.WaitGroup
	resuming    bool
	unacked     []msgr.MessageData
	resumeState ResumeState
	resumes     atomic.Uint64
}

func New(t *Transport, config Config, onMessage func(msgr.MessageData) error, onClose func(error)) *Session {
	if config.KeepaliveInterval == 0 {
		config.KeepaliveInterval = 15 * time.Second
	}
	if config.KeepaliveTimeout == 0 {
		config.KeepaliveTimeout = 45 * time.Second
	}
	if config.ResumeTimeout == 0 {
		config.ResumeTimeout = 30 * time.Second
	}
	if config.MaxUnacked == 0 {
		config.MaxUnacked = 1024
	}
	return &Session{transport: t, config: config, pending: make(map[uint64]*request), idle: make(chan struct{}), queue: make(chan *request, 64), controls: make(chan msgr.Frame, 16), ack: make(chan struct{}, 1), done: make(chan struct{}), cleanupDone: make(chan struct{}), onMessage: onMessage, onClose: onClose, resumeState: t.Resume}
}

// Resumes counts successful stateful transport resumes.
func (s *Session) Resumes() uint64 { return s.resumes.Load() }

// startLoopsLocked starts one transport generation's workers; s.mu is held.
func (s *Session) startLoopsLocked(t *Transport, resend []msgr.MessageData) {
	loops := &sync.WaitGroup{}
	stop := make(chan struct{})
	s.loops, s.stop = loops, stop
	epoch := s.epoch
	s.wg.Add(3)
	loops.Add(3)
	go s.readLoop(t, epoch, stop, loops)
	go s.writeLoop(t, epoch, stop, loops, resend)
	go s.keepaliveLoop(epoch, stop, loops)
}
func (s *Session) Start() {
	s.startOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.err != nil {
			return
		}
		now := time.Now()
		s.lastReceive.Store(&now)
		s.startLoopsLocked(s.transport, nil)
	})
}
func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) RemoteAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transport.Conn.RemoteAddr()
}
func (s *Session) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Session) Wait() {
	// Fail closes admission under mu before done, so Start cannot add workers
	// after this barrier. The failure owner can also be an application caller;
	// its connection and callback cleanup must finish before the session exits.
	<-s.done
	s.wg.Wait()
	<-s.cleanupDone
}

// Retire stops admission without interrupting requests already registered.
func (s *Session) Retire() {
	s.mu.Lock()
	s.retiring = true
	s.mu.Unlock()
}
func (s *Session) Fail(err error) {
	if err == nil {
		err = ErrClosed
	}
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	close(s.done)
	for _, r := range s.pending {
		cause := err
		if r.started {
			cause = &OutcomeUnknownError{Cause: err}
		}
		r.completedErr = cause
		r.releaseUnstarted()
		r.result <- response{err: cause}
	}
	s.pending = make(map[uint64]*request)
	close(s.idle)
	s.idle = make(chan struct{})
	s.unacked = nil
	conn := s.transport.Conn
	s.mu.Unlock()
	defer close(s.cleanupDone)
	conn.Close()
	if s.onClose != nil {
		s.onClose(err)
	}
}
func (s *Session) WaitIdle(ctx context.Context) error {
	for {
		s.mu.Lock()
		n, changed := len(s.pending), s.idle
		s.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return s.Err()
		}
	}
}
func (s *Session) remove(id uint64) *request {
	r := s.pending[id]
	delete(s.pending, id)
	if r != nil && len(s.pending) == 0 {
		close(s.idle)
		s.idle = make(chan struct{})
	}
	return r
}
func (s *Session) abandon(id uint64, r *request, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(id)
	if r.completedErr != nil {
		return r.completedErr
	}
	if r.started {
		return &OutcomeUnknownError{Cause: err}
	}
	r.releaseUnstarted()
	return err
}
func (s *Session) Call(ctx context.Context, m msgr.MessageData) (msgr.MessageData, error) {
	if err := ctx.Err(); err != nil {
		return msgr.MessageData{}, err
	}
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return msgr.MessageData{}, err
	}
	if s.retiring {
		s.mu.Unlock()
		return msgr.MessageData{}, ErrRetired
	}
	if s.config.NextTransaction == nil && s.nextID == ^uint64(0) {
		s.mu.Unlock()
		return msgr.MessageData{}, errors.New("ceph: transaction IDs exhausted")
	}
	if s.config.NextTransaction != nil {
		id, err := s.config.NextTransaction()
		if err != nil || id == 0 {
			s.mu.Unlock()
			if err == nil {
				err = errors.New("ceph: invalid transaction ID")
			}
			return msgr.MessageData{}, err
		}
		m.Transaction = id
	} else {
		s.nextID++
		m.Transaction = s.nextID
	}
	r := &request{ctx: ctx, message: m, result: make(chan response, 1)}
	s.pending[m.Transaction] = r
	s.mu.Unlock()
	finish := func(result response) (msgr.MessageData, error) {
		if result.err != nil && result.message.Transaction == 0 {
			result.message.Transaction = m.Transaction
		}
		return result.message, result.err
	}
	select {
	case s.queue <- r:
	case <-ctx.Done():
		return msgr.MessageData{Transaction: m.Transaction}, s.abandon(m.Transaction, r, ctx.Err())
	case <-s.done:
		result := <-r.result
		return finish(result)
	}
	select {
	case result := <-r.result:
		return finish(result)
	case <-ctx.Done():
		// Prefer a completed result if dispatch won the race with cancellation.
		select {
		case result := <-r.result:
			return finish(result)
		default:
		}
		return msgr.MessageData{Transaction: m.Transaction}, s.abandon(m.Transaction, r, ctx.Err())
	}
}
func (s *Session) Send(ctx context.Context, m msgr.MessageData) error {
	r := &request{ctx: ctx, message: m}
	select {
	case <-s.done:
		return s.Err()
	case <-ctx.Done():
		return ctx.Err()
	case s.queue <- r:
		return nil
	}
}

// SendControl queues a protocol message on the ordinary ordered writer.
// ctx governs queue admission only: after success, later caller cancellation
// cannot discard this message before a later application message. Session
// failure still stops transmission. Success is not proof of a full write or
// peer receipt. The caller must keep the message bytes immutable after entry.
func (s *Session) SendControl(ctx context.Context, m msgr.MessageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r := &request{ctx: context.Background(), message: m}
	select {
	case <-s.done:
		return s.Err()
	case <-ctx.Done():
		return ctx.Err()
	case s.queue <- r:
		return nil
	}
}

func (s *Session) control(f msgr.Frame) error {
	select {
	case s.controls <- f:
		return nil
	case <-s.done:
		return s.Err()
	default:
		return errors.New("ceph messenger: control queue overflow")
	}
}
func (s *Session) writeFrame(t *Transport, f msgr.Frame) error {
	if err := t.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}
	return t.Writer.Write(f)
}

// retain records a message about to be written so a stateful resume can
// retransmit it until the peer acknowledges its sequence.
func (s *Session) retain(m msgr.MessageData) error {
	if s.config.Resume == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.unacked) >= s.config.MaxUnacked {
		return fmt.Errorf("%w: unacknowledged message bound exceeded", wire.ErrLimit)
	}
	s.unacked = append(s.unacked, m)
	return nil
}

// acknowledged drops retained messages the peer has received.
func (s *Session) acknowledged(seq uint64) {
	if s.config.Resume == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for n < len(s.unacked) && s.unacked[n].Sequence <= seq {
		s.unacked[n] = msgr.MessageData{}
		n++
	}
	s.unacked = s.unacked[n:]
}

func (s *Session) writeLoop(t *Transport, epoch uint64, stop <-chan struct{}, loops *sync.WaitGroup, resend []msgr.MessageData) {
	defer s.wg.Done()
	defer loops.Done()
	// Retained messages of the previous transport go first, with their
	// original sequences, so the peer sees one ordered stream.
	for _, m := range resend {
		m.AckSequence = s.received.Load()
		if err := s.writeFrame(t, m.Frame()); err != nil {
			s.fault(epoch, err)
			return
		}
	}
	for {
		var err error
		select {
		case <-s.done:
			return
		case <-stop:
			return
		case f := <-s.controls:
			err = s.writeFrame(t, f)
		case <-s.ack:
			e := wire.Encoder{}
			e.U64(s.received.Load())
			err = s.writeFrame(t, msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{e.Data}})
		case r := <-s.queue:
			s.mu.Lock()
			id, ctx, m := r.message.Transaction, r.ctx, r.message
			valid := s.err == nil && (r.result == nil || s.pending[id] == r)
			s.mu.Unlock()
			// Both context and admission callbacks run outside mu. Snapshot
			// the message under mu because cancellation/Fail may clear the
			// queued request while either callback is inspecting its bytes.
			if !valid || ctx.Err() != nil {
				continue
			}
			var admissionErr error
			if s.config.BeforeWrite != nil {
				admissionErr = s.config.BeforeWrite(m)
				if ctx.Err() != nil {
					continue
				}
			}
			s.mu.Lock()
			valid = s.err == nil && (r.result == nil || s.pending[id] == r)
			if valid && admissionErr != nil {
				if r.result != nil {
					s.remove(id)
					r.completedErr = admissionErr
					r.releaseUnstarted()
					r.result <- response{message: msgr.MessageData{Transaction: id}, err: admissionErr}
				} else {
					r.releaseUnstarted()
				}
				s.mu.Unlock()
				continue
			}
			if valid && r.result != nil {
				r.started = true
			}
			s.mu.Unlock()
			if !valid {
				continue
			}
			m.Sequence = s.sent.Add(1)
			m.AckSequence = s.received.Load()
			if err := s.retain(m); err != nil {
				s.Fail(err)
				return
			}
			err = s.writeFrame(t, m.Frame())
		}
		if err != nil {
			s.fault(epoch, err)
			return
		}
	}
}

func (s *Session) readLoop(t *Transport, epoch uint64, stop <-chan struct{}, loops *sync.WaitGroup) {
	defer s.wg.Done()
	defer loops.Done()
	for {
		f, err := t.Reader.Read()
		aborted := errors.Is(err, msgr.ErrAborted)
		if err != nil && !aborted {
			select {
			case <-stop:
				return // this transport generation was retired by a fault
			default:
			}
			s.fault(epoch, err)
			return
		}
		// Keep Go's monotonic clock component when publishing activity.
		now := time.Now()
		s.lastReceive.Store(&now)
		if aborted {
			// A complete late-aborted frame proves receive activity, but
			// must not dispatch a message or advance its sequence/ACK.
			continue
		}
		switch f.Tag {
		case msgr.Message:
			m, err := msgr.DecodeMessage(f)
			if err != nil {
				s.Fail(err)
				return
			}
			if m.Sequence == 0 || m.AckSequence > s.sent.Load() {
				s.Fail(msgr.ErrFrame)
				return
			}
			s.acknowledged(m.AckSequence)
			if m.Sequence <= s.received.Load() {
				select {
				case s.ack <- struct{}{}:
				default:
				}
				continue
			}
			if s.config.Resume != nil && m.Sequence != s.received.Load()+1 {
				// A stateful stream is gapless; a gap means lost messages.
				s.Fail(fmt.Errorf("%w: stateful message sequence gap", msgr.ErrFrame))
				return
			}
			s.received.Store(m.Sequence)
			select {
			case s.ack <- struct{}{}:
			default:
			}
			if m.Type == msgr.MonCommandReplyMessage || m.Type == msgr.MgrCommandReplyMessage || m.Type == msgr.TellCommandReplyMessage || m.Type == msgr.OSDOpReplyMessage {
				s.mu.Lock()
				r := s.pending[m.Transaction]
				if r != nil {
					expected := msgr.MonCommandReplyMessage
					switch r.message.Type {
					case msgr.MgrCommandMessage:
						expected = msgr.MgrCommandReplyMessage
					case msgr.TellCommandMessage:
						expected = msgr.TellCommandReplyMessage
					case msgr.OSDOpMessage:
						expected = msgr.OSDOpReplyMessage
					}
					if !r.started || m.Type != expected {
						// Leave it pending so Fail preserves whether transmission
						// started, for this request and every other affected call.
						s.mu.Unlock()
						s.Fail(msgr.ErrFrame)
						return
					}
					s.remove(m.Transaction)
				}
				s.mu.Unlock()
				if r != nil {
					r.result <- response{message: m}
				}
			} else if s.onMessage != nil {
				if err := s.onMessage(m); err != nil {
					s.Fail(err)
					return
				}
			}
		case msgr.Keepalive:
			if len(f.Segments) != 1 || len(f.Segments[0]) != 8 {
				s.Fail(msgr.ErrFrame)
				return
			}
			if err := s.control(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: f.Segments}); err != nil {
				s.Fail(err)
				return
			}
		case msgr.KeepaliveAck, msgr.Ack:
			if len(f.Segments) != 1 || len(f.Segments[0]) != 8 {
				s.Fail(msgr.ErrFrame)
				return
			}
			if f.Tag == msgr.Ack {
				seq := wire.NewDecoder(f.Segments[0]).U64()
				if seq > s.sent.Load() {
					s.Fail(msgr.ErrFrame)
					return
				}
				s.acknowledged(seq)
			}
		default:
			s.Fail(fmt.Errorf("%w: unexpected established tag %d", msgr.ErrFrame, f.Tag))
			return
		}
	}
}

func (s *Session) keepaliveLoop(epoch uint64, stop <-chan struct{}, loops *sync.WaitGroup) {
	defer s.wg.Done()
	defer loops.Done()
	timer := time.NewTicker(s.config.KeepaliveInterval)
	defer timer.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-stop:
			return
		case now := <-timer.C:
			if now.Sub(*s.lastReceive.Load()) >= s.config.KeepaliveTimeout {
				s.fault(epoch, ErrKeepaliveTimeout)
				return
			}
			e := wire.Encoder{}
			e.U32(uint32(now.Unix()))
			e.U32(uint32(now.Nanosecond()))
			if err := s.control(msgr.Frame{Tag: msgr.Keepalive, Segments: [][]byte{e.Data}}); err != nil {
				s.Fail(err)
				return
			}
		}
	}
}

// resumableFault reports transport-level failures a stateful session may
// survive. Malformed or unauthenticated frames and protocol refusals are
// terminal; so is a peer RESET of the stateful session.
func resumableFault(err error) bool {
	switch {
	case err == nil, errors.Is(err, msgr.ErrFrame), errors.Is(err, msgr.ErrCRC), errors.Is(err, msgr.ErrAuthentication),
		errors.Is(err, msgr.ErrFeatures), errors.Is(err, wire.ErrLimit), errors.Is(err, wire.ErrVersion),
		errors.Is(err, ErrSessionReset), errors.Is(err, ErrClosed), errors.Is(err, context.Canceled):
		return false
	}
	return true
}

// fault ends one transport generation. Lossy sessions fail; stateful
// sessions retire the transport and resume in the background.
func (s *Session) fault(epoch uint64, err error) {
	if s.config.Resume == nil || !resumableFault(err) {
		s.Fail(err)
		return
	}
	s.mu.Lock()
	if s.err != nil || s.epoch != epoch || s.resuming {
		s.mu.Unlock()
		return
	}
	s.resuming = true
	t, loops, stop := s.transport, s.loops, s.stop
	s.wg.Add(1)
	s.mu.Unlock()
	close(stop)
	t.Conn.Close()
	go s.resume(err, loops)
}

func (s *Session) resume(cause error, loops *sync.WaitGroup) {
	defer s.wg.Done()
	// Every worker of the old generation must stop before a new writer may
	// retransmit, so the retained list is final.
	loops.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ResumeTimeout)
	defer cancel()
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-s.done:
			cancel()
		case <-stopWatch:
		}
	}()
	backoff := 50 * time.Millisecond
	for {
		s.mu.Lock()
		state := s.resumeState
		s.mu.Unlock()
		state.InSeq = s.received.Load()
		t, err := s.config.Resume(ctx, state)
		if err == nil {
			s.install(t, cause)
			return
		}
		if !resumableFault(err) || ctx.Err() != nil {
			s.Fail(fmt.Errorf("%w; stateful resume failed: %w", cause, err))
			return
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			s.Fail(fmt.Errorf("%w; stateful resume failed: %w", cause, ctx.Err()))
			return
		}
		backoff = min(2*backoff, 2*time.Second)
	}
}

// install publishes a resumed transport: retained messages the peer already
// received are dropped and the rest are retransmitted first.
func (s *Session) install(t *Transport, cause error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		t.Conn.Close()
		return
	}
	if t.PeerSequence > s.sent.Load() {
		s.mu.Unlock()
		t.Conn.Close()
		s.Fail(fmt.Errorf("%w: peer acknowledged unsent messages on resume", msgr.ErrFrame))
		return
	}
	n := 0
	for n < len(s.unacked) && s.unacked[n].Sequence <= t.PeerSequence {
		n++
	}
	s.unacked = s.unacked[n:]
	resend := append([]msgr.MessageData(nil), s.unacked...)
	s.transport, s.resumeState = t, t.Resume
	s.epoch++
	s.resuming = false
	now := time.Now()
	s.lastReceive.Store(&now)
	// Count the resume before retransmitting, so a peer that sees the
	// resent frames never observes a stale count.
	s.resumes.Add(1)
	s.startLoopsLocked(t, resend)
	s.mu.Unlock()
	if s.config.OnResume != nil {
		s.config.OnResume()
	}
}

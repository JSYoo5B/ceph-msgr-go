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

// OutcomeUnknownError means a command may have reached the daemon. It is not
// safe to infer that a mutation failed or to re-execute it automatically.
type OutcomeUnknownError struct{ Cause error }

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("ceph: command outcome unknown: %v", e.Cause)
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
}

type Session struct {
	transport      *Transport
	writeTimeout   time.Duration
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
	wg             sync.WaitGroup
	startOnce      sync.Once
	received, sent atomic.Uint64
	lastReceive    atomic.Int64
	onMessage      func(msgr.MessageData) error
	onClose        func(error)
}

func New(t *Transport, writeTimeout time.Duration, onMessage func(msgr.MessageData) error, onClose func(error)) *Session {
	return &Session{transport: t, writeTimeout: writeTimeout, pending: make(map[uint64]*request), idle: make(chan struct{}), queue: make(chan *request, 64), controls: make(chan msgr.Frame, 16), ack: make(chan struct{}, 1), done: make(chan struct{}), onMessage: onMessage, onClose: onClose}
}
func (s *Session) Start() {
	s.startOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.err != nil {
			return
		}
		s.lastReceive.Store(time.Now().UnixNano())
		s.wg.Add(3)
		go s.readLoop()
		go s.writeLoop()
		go s.keepaliveLoop()
	})
}
func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) RemoteAddr() net.Addr  { return s.transport.Conn.RemoteAddr() }
func (s *Session) Err() error            { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *Session) Wait()                 { s.wg.Wait() }

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
		r.result <- response{err: cause}
	}
	s.pending = make(map[uint64]*request)
	close(s.idle)
	s.idle = make(chan struct{})
	s.mu.Unlock()
	s.transport.Conn.Close()
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
	if r.started {
		return &OutcomeUnknownError{Cause: err}
	}
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
	if s.nextID == ^uint64(0) {
		s.mu.Unlock()
		return msgr.MessageData{}, errors.New("ceph: transaction IDs exhausted")
	}
	s.nextID++
	m.Transaction = s.nextID
	r := &request{ctx: ctx, message: m, result: make(chan response, 1)}
	s.pending[m.Transaction] = r
	s.mu.Unlock()
	select {
	case s.queue <- r:
	case <-ctx.Done():
		return msgr.MessageData{}, s.abandon(m.Transaction, r, ctx.Err())
	case <-s.done:
		result := <-r.result
		return result.message, result.err
	}
	select {
	case result := <-r.result:
		return result.message, result.err
	case <-ctx.Done():
		// Prefer a completed result if dispatch won the race with cancellation.
		select {
		case result := <-r.result:
			return result.message, result.err
		default:
		}
		return msgr.MessageData{}, s.abandon(m.Transaction, r, ctx.Err())
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
func (s *Session) writeFrame(f msgr.Frame) error {
	if err := s.transport.Conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		return err
	}
	return s.transport.Writer.Write(f)
}
func (s *Session) writeLoop() {
	defer s.wg.Done()
	for {
		var err error
		select {
		case <-s.done:
			return
		case f := <-s.controls:
			err = s.writeFrame(f)
		case <-s.ack:
			e := wire.Encoder{}
			e.U64(s.received.Load())
			err = s.writeFrame(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{e.Data}})
		case r := <-s.queue:
			s.mu.Lock()
			valid := s.err == nil && r.ctx.Err() == nil
			if r.result != nil {
				valid = valid && s.pending[r.message.Transaction] == r
				if valid {
					r.started = true
				}
			}
			s.mu.Unlock()
			if !valid {
				continue
			}
			m := r.message
			m.Sequence = s.sent.Add(1)
			m.AckSequence = s.received.Load()
			err = s.writeFrame(m.Frame())
		}
		if err != nil {
			s.Fail(err)
			return
		}
	}
}
func (s *Session) readLoop() {
	defer s.wg.Done()
	for {
		f, err := s.transport.Reader.Read()
		if errors.Is(err, msgr.ErrAborted) {
			continue
		}
		if err != nil {
			s.Fail(err)
			return
		}
		s.lastReceive.Store(time.Now().UnixNano())
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
			if m.Sequence <= s.received.Load() {
				select {
				case s.ack <- struct{}{}:
				default:
				}
				continue
			}
			s.received.Store(m.Sequence)
			select {
			case s.ack <- struct{}{}:
			default:
			}
			if m.Type == msgr.MonCommandReplyMessage || m.Type == msgr.MgrCommandReplyMessage {
				s.mu.Lock()
				r := s.pending[m.Transaction]
				if r != nil {
					expected := msgr.MonCommandReplyMessage
					if r.message.Type == msgr.MgrCommandMessage {
						expected = msgr.MgrCommandReplyMessage
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
			if f.Tag == msgr.Ack && wire.NewDecoder(f.Segments[0]).U64() > s.sent.Load() {
				s.Fail(msgr.ErrFrame)
				return
			}
		default:
			s.Fail(fmt.Errorf("%w: unexpected established tag %d", msgr.ErrFrame, f.Tag))
			return
		}
	}
}
func (s *Session) keepaliveLoop() {
	defer s.wg.Done()
	timer := time.NewTicker(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-timer.C:
			if now.Sub(time.Unix(0, s.lastReceive.Load())) > 45*time.Second {
				s.Fail(errors.New("ceph messenger: keepalive timeout"))
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

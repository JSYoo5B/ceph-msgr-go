package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// OpenSession sends one REQUEST_OPEN on this connection and waits for OPEN.
// An explicit REJECT returns *MDSSessionRejectedError and a server CLOSE
// returns ErrMDSSessionClosed; both leave no session. When ctx ends after the
// request was queued, OpenSession returns the still-opening session with an
// OutcomeUnknownError: the MDS may open it later. Keep waiting with WaitOpen
// or end it with Close; an abandoned open session is eventually evicted.
func (m *MDSConnection) OpenSession(ctx context.Context, options MDSSessionOptions) (*MDSSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := m.client
	if err := validateSessionOptions(&options); err != nil {
		return nil, err
	}
	if uint32(len(options.Metadata)) > mds.MaxSessionMetadata {
		return nil, fmt.Errorf("ceph: MDS session metadata entry count: %w", ErrLimitExceeded)
	}
	features := mds.ClientFeatures
	if c.options.EnableCephFS {
		features = mds.CephFSClientFeatures
	}
	request := mds.EncodeRequestOpen(options.Metadata, features)
	if uint64(len(request.Front))+41 > uint64(c.options.MaxFrameSize) {
		return nil, fmt.Errorf("ceph: MDS session metadata exceeds frame limit: %w", ErrLimitExceeded)
	}
	return m.beginSession(ctx, options, []msgr.MessageData{request}, 0, nil)
}

func validateSessionOptions(options *MDSSessionOptions) error {
	if options.RenewInterval <= 0 {
		return errors.New("ceph: MDS session requires a positive RenewInterval")
	}
	if options.MaxEvents == 0 {
		options.MaxEvents = 64
	}
	if options.MaxEvents < 1 || options.MaxEvents > 4096 {
		return errors.New("ceph: MDS session MaxEvents must be between 1 and 4096")
	}
	return nil
}

// beginSession admits one session on m, queues its opening messages
// (REQUEST_OPEN, or the MClientReconnect sequence) and waits for OPEN.
func (m *MDSConnection) beginSession(ctx context.Context, options MDSSessionOptions, messages []msgr.MessageData, reconnectFrom uint64, prepare func(*MDSSession) error) (*MDSSession, error) {
	c := m.client
	c.mu.Lock()
	if c.closed || m.closed || m.ctx.Err() != nil {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	s := m.session
	if err := s.Err(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if c.auth.GlobalID != m.globalID {
		c.mu.Unlock()
		return nil, ErrMDSIdentityChanged
	}
	if m.cephfs != nil {
		c.mu.Unlock()
		return nil, ErrMDSSessionExists
	}
	if c.mdsUnresolved[m.address] {
		c.mu.Unlock()
		return nil, ErrMDSSessionActive
	}
	for _, other := range c.mdss {
		if other != m && other.address == m.address && other.cephfs != nil {
			c.mu.Unlock()
			return nil, ErrMDSSessionActive
		}
	}
	if c.mdsSessionGeneration == ^uint64(0) {
		c.mu.Unlock()
		return nil, errors.New("ceph: MDS session generations exhausted")
	}
	c.mdsSessionGeneration++
	session := &MDSSession{conn: m, generation: c.mdsSessionGeneration, renewInterval: options.RenewInterval, maxEvents: options.MaxEvents,
		renewNow: make(chan struct{}, 1), closeNow: make(chan struct{}, 1), done: make(chan struct{}), stopped: make(chan struct{}), state: MDSSessionOpening,
		changed: make(chan struct{}), renewSent: make(map[uint64]time.Time), reconnectFrom: reconnectFrom, uuid: options.Metadata["uuid"]}
	m.cephfs = session
	c.mu.Unlock()
	if prepare != nil {
		// Resubmitted requests precede the opening messages, as natively.
		if err := prepare(session); err != nil {
			session.mu.Lock()
			queued := session.queued
			session.mu.Unlock()
			session.finish(MDSSessionLost, err, queued)
			close(session.stopped)
			if queued {
				return nil, &OutcomeUnknownError{Cause: err}
			}
			return nil, err
		}
	}
	// Admission only: after success each queued message is ordered before
	// every later message even if ctx ends.
	for _, message := range messages {
		session.mu.Lock()
		err := s.SendControl(ctx, message)
		if err == nil {
			session.queued = true
		}
		session.mu.Unlock()
		if err == nil {
			continue
		}
		// After the first message the MDS may hold a partial session.
		session.mu.Lock()
		queued := session.queued
		session.mu.Unlock()
		session.finish(MDSSessionLost, err, queued)
		close(session.stopped)
		if queued {
			return nil, &OutcomeUnknownError{Cause: err}
		}
		if s.Err() != nil {
			return nil, err
		}
		return nil, m.operationError(err)
	}
	go session.renewLoop()
	if _, err := session.WaitOpen(ctx); err != nil {
		if session.Status().State == MDSSessionOpening && ctx.Err() != nil {
			return session, &OutcomeUnknownError{Cause: err}
		}
		return nil, err
	}
	return session, nil
}

// Close sends REQUEST_CLOSE (with the current push sequence) once and waits
// for the MDS CLOSE, a terminal transport failure or ctx. It returns nil after
// a confirmed CLOSE or REJECT. A ctx error leaves the session Closing; Close
// can be called again to keep waiting. If the transport ends first, the remote
// session is unconfirmed: the error wraps ErrMDSSessionActive and this client
// refuses further connections to that MDS address.
func (s *MDSSession) Close(ctx context.Context) error {
	if err := s.lockRequestAdmission(ctx); err != nil {
		return s.closeResult(err)
	}
	s.mu.Lock()
	send := !s.state.terminal() && !s.closeSent
	if send {
		s.closeSent = true
	}
	seq := s.push
	s.mu.Unlock()
	if send {
		message, err := mds.EncodeSessionControl(mds.SessionRequestClose, seq)
		if err != nil {
			s.unlockRequestAdmission()
			return err
		}
		if err := s.conn.session.SendControl(ctx, message); err != nil {
			s.mu.Lock()
			if !s.state.terminal() {
				s.closeSent = false
			}
			s.mu.Unlock()
			s.unlockRequestAdmission()
			return s.closeResult(err)
		}
		s.mu.Lock()
		retry := false
		if !s.state.terminal() {
			s.setStateLocked(MDSSessionClosing)
			// A counted push may arrive while the selected CLOSE waits for
			// queue admission. Its handler still saw Open and could not
			// trigger closeNow; the MDS drops our old sequence unanswered.
			retry = s.push != seq
		}
		s.mu.Unlock()
		if retry {
			select {
			case s.closeNow <- struct{}{}:
			default:
			}
		}
	}
	s.unlockRequestAdmission()
	for {
		s.mu.Lock()
		state, err, changed := s.state, s.err, s.changed
		s.mu.Unlock()
		if state.terminal() {
			return s.closeResult(err)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// lockRequestAdmission orders request registration/transmission against Close.
// The reader and terminal resolver use only mu and can make progress while
// SendControl is blocked. Each waiter keeps its own cancellation boundary.
func (s *MDSSession) lockRequestAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.requestAdmission == nil {
		s.requestAdmission = make(chan struct{}, 1)
	}
	gate := s.requestAdmission
	s.mu.Unlock()
	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.Err()
	}
}

func (s *MDSSession) unlockRequestAdmission() { <-s.requestAdmission }

func (s *MDSSession) closeResult(cause error) error {
	s.mu.Lock()
	state := s.state
	s.mu.Unlock()
	switch state {
	case MDSSessionClosed, MDSSessionRejected:
		return nil
	case MDSSessionLost:
		return fmt.Errorf("%w: %w", ErrMDSSessionActive, s.Err())
	}
	return cause
}

func (s *MDSSession) setStateLocked(state MDSSessionState) {
	s.state = state
	close(s.changed)
	s.changed = make(chan struct{})
}

// finish publishes a terminal state once and releases the connection's
// session slot. unresolved records the address because the MDS may still hold
// the session; the caller must not hold s.mu or the client mutex.
func (s *MDSSession) finish(state MDSSessionState, err error, unresolved bool) {
	m := s.conn
	c := m.client
	// Match the client-before-session lock order used for admission. Retire
	// the slot and record uncertainty before publishing any terminal signal.
	c.mu.Lock()
	defer c.mu.Unlock()
	s.mu.Lock()
	if s.state.terminal() {
		s.mu.Unlock()
		return
	}
	if m.cephfs == s {
		m.cephfs = nil
	}
	if unresolved {
		if c.mdsUnresolved == nil {
			c.mdsUnresolved = make(map[msgr.Address]bool)
		}
		c.mdsUnresolved[m.address] = true
	}
	s.err = err
	s.setStateLocked(state)
	s.renewSent = nil
	s.failRequestsLocked(err)
	close(s.done)
	s.mu.Unlock()
}

// lost is called when the owning transport ends. A session whose
// REQUEST_OPEN was queued may exist remotely and becomes unresolved.
func (s *MDSSession) lost(cause error) {
	s.mu.Lock()
	queued := s.queued
	s.mu.Unlock()
	s.finish(MDSSessionLost, cause, queued)
}

// mdsTransportEnded marks a live CephFS session as lost when its transport
// ends; called from the Session close callback.
func (m *MDSConnection) mdsTransportEnded(cause error) {
	c := m.client
	c.mu.Lock()
	s := m.cephfs
	c.mu.Unlock()
	if s != nil {
		s.lost(cause)
	}
}

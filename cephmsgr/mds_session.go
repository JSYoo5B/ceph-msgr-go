package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

var (
	ErrMDSSessionExists = errors.New("ceph: MDS connection already owns a CephFS session")
	// ErrMDSSessionActive refuses a second connection or session at an MDS
	// address where this client's CephFS session is active or was never
	// confirmed closed. The MDS binds sessions to the client GlobalID, so a new
	// connection would take over the remote session; reconnect is unsupported.
	ErrMDSSessionActive   = errors.New("ceph: this client has an active or unconfirmed CephFS session at that MDS address")
	ErrMDSSessionClosed   = errors.New("ceph: MDS closed the CephFS session")
	ErrMDSSessionOverflow = errors.New("ceph: CephFS session event queue overflow")
)

// MDSSessionRejectedError is an explicit MDS REJECT. Reason is the raw
// error_string metadata ("" when absent); Blocklisted reports the
// SESSION_BLOCKLISTED flag. Required-feature, filesystem-capability, root
// claim, refused-session and blocklist refusals all arrive this way.
type MDSSessionRejectedError struct {
	Reason      string
	Flags       uint32
	Blocklisted bool
	Metadata    map[string]string
}

func (e *MDSSessionRejectedError) Error() string {
	return fmt.Sprintf("ceph: MDS rejected the CephFS session: %q", e.Reason)
}

// MDSSessionState is the local view of one CephFS session.
type MDSSessionState uint8

const (
	MDSSessionOpening MDSSessionState = iota + 1
	MDSSessionOpen
	MDSSessionStale
	MDSSessionClosing
	// Terminal states. Closed and Rejected are confirmed by the MDS. Lost means
	// the transport ended first; the remote session may remain open until the
	// MDS times it out (stale, then autoclose eviction, usually blocklisting).
	MDSSessionClosed
	MDSSessionRejected
	MDSSessionLost
)

func (s MDSSessionState) terminal() bool { return s >= MDSSessionClosed }

// Raw MClientSession operations delivered as events.
const (
	MDSSessionOpOpen          = mds.SessionOpen
	MDSSessionOpStale         = mds.SessionStale
	MDSSessionOpRecallState   = mds.SessionRecallState
	MDSSessionOpFlushMessage  = mds.SessionFlushMessage
	MDSSessionOpForceReadOnly = mds.SessionForceReadOnly
)

// MDSSessionClientFeatures is the CephFS client feature bitset a session
// declares without EnableCephFS: bits 0-8 (raw little-endian feature_bitset_t).
func MDSSessionClientFeatures() []byte { return slices.Clone(mds.ClientFeatures) }

// MDSCephFSClientFeatures is the bitset an EnableCephFS session declares:
// bits 0-9, 15 and 18-20 (see MDS_CEPHFS_CONTRACT.md).
func MDSCephFSClientFeatures() []byte { return slices.Clone(mds.CephFSClientFeatures) }

// MDSClientFeatures is the exact bitset this client declares in REQUEST_OPEN.
// It is fixed by the selected profile and cannot be widened by the caller.
func (c *Client) MDSClientFeatures() []byte {
	if c.options.EnableCephFS {
		return MDSCephFSClientFeatures()
	}
	return MDSSessionClientFeatures()
}

// MDSFeatureBitsetHas tests one bit in a raw feature_bitset_t byte image, such
// as MDSSessionOpenInfo.SupportedFeatures or MDSMap.RequiredClientFeatures.
func MDSFeatureBitsetHas(raw []byte, bit uint) bool { return mds.FeatureBitsetHas(raw, bit) }

// MDSCapAuth is one raw MDS capability grant reported in the session OPEN.
// UID -1 is MDS_AUTH_UID_ANY. The library enforces none of these grants.
type MDSCapAuth struct {
	UID                             int64
	GIDs                            []uint32
	Path, FilesystemName            string
	RootSquash, Readable, Writeable bool
}

// MDSSessionOpenInfo preserves the server OPEN reply.
type MDSSessionOpenInfo struct {
	Sequence          uint64
	SupportedFeatures []byte
	MetricFlags       []byte
	Flags             uint32
	Metadata          map[string]string
	CapAuths          []MDSCapAuth
}

// MDSSessionOptions configures OpenSession.
type MDSSessionOptions struct {
	// Metadata is sent unchanged as client metadata. The MDS interprets some
	// keys: "root" is a claimed mount root checked against MDS caps and "uuid"
	// is refused when another session already uses it. Nothing is added.
	Metadata map[string]string
	// RenewInterval is required. While the session is open or stale the
	// library sends REQUEST_RENEWCAPS on this period (and once immediately
	// after OPEN or STALE, as the native client does). Pick it below the
	// MDSMap session_timeout; the library has no map of its own.
	RenewInterval time.Duration
	// MaxEvents bounds the FIFO of unsolicited session events. Zero means 64.
	// Overflow fails the MDS connection: events are never dropped.
	MaxEvents int
}

// MDSSessionEvent is one unsolicited MDS message after OPEN. Type is the raw
// Messenger type. For MClientSession (22) it is a duplicate OPEN, STALE,
// RECALL_STATE, FLUSHMSG or FORCE_RO with Op as the raw wire value. With
// EnableCephFS it can also be a counted push: Caps (0x310, Transaction is the
// flush TID of FLUSH_ACK), Lease (0x311), Snap (0x312) or Quota (0x314).
type MDSSessionEvent struct {
	// Arrival orders every reply, push and session event of this session in
	// receive order (shared with MDSReply.Arrival), starting at 1.
	Arrival            uint64
	Type               uint16
	Transaction        uint64
	Caps               *MDSCaps
	Lease              *MDSLease
	Snap               *MDSSnap
	Quota              *MDSQuota
	Op                 uint32
	Sequence           uint64
	MaxCaps, MaxLeases uint32
	Flags              uint32
	Metadata           map[string]string
	SupportedFeatures  []byte
	Received           time.Time
}

// MDSSessionRenewal reports one acknowledged REQUEST_RENEWCAPS. Requested is
// the local send time; native clients extend their capability TTL from it by
// the MDSMap session_timeout when the matching RENEWCAPS reply arrives.
type MDSSessionRenewal struct {
	Sequence             uint64
	Requested, Confirmed time.Time
}

// MDSSessionStatus is a copy of local session state. Generation counts
// OpenSession and ReconnectSession calls of this Client. PushSequence counts
// the capability, lease, snapshot and quota messages the MDS sequences
// against REQUEST_CLOSE (EnableCephFS only; zero otherwise).
type MDSSessionStatus struct {
	State                                      MDSSessionState
	Generation, ConnectionGeneration, GlobalID uint64
	Target                                     MDSTarget
	PushSequence, RenewSequence                uint64
	ReconnectedFrom                            uint64
	LastRenewal                                MDSSessionRenewal
	Open                                       MDSSessionOpenInfo
	ReadOnly                                   bool
	Err                                        error
}

// MDSSession is one CephFS client session on an MDSConnection: open, renew,
// close and session events, plus (with EnableCephFS) metadata requests, MDS
// pushes, explicit capability/lease answers and reconnect. The library keeps
// no inode, capability or cache state of its own.
type MDSSession struct {
	conn          *MDSConnection
	generation    uint64
	renewInterval time.Duration
	maxEvents     int
	renewNow      chan struct{}
	closeNow      chan struct{}
	done          chan struct{}
	stopped       chan struct{}

	mu        sync.Mutex
	state     MDSSessionState
	err       error
	changed   chan struct{}
	open      MDSSessionOpenInfo
	push      uint64
	renewSeq  uint64
	renewSent map[uint64]time.Time
	renewal   MDSSessionRenewal
	readOnly  bool
	events    []MDSSessionEvent
	closeSent bool
	// queued records that REQUEST_OPEN entered the ordered writer; from then on
	// the MDS may hold the session even if the transport ends.
	queued bool
	// requests tracks submitted CephFS metadata requests by TID.
	requests map[uint64]*MDSRequestCall
	// requestAdmission serializes metadata request and REQUEST_CLOSE queue
	// admission without holding mu across transport backpressure. Initialized
	// lazily under mu so the zero value also works for synthetic sessions.
	requestAdmission chan struct{}
	// reconnectFrom is the generation of the Lost session this one
	// reconnects (zero for a fresh REQUEST_OPEN).
	reconnectFrom uint64
	// arrival numbers dispatched replies, pushes and session messages.
	arrival uint64
	// uuid is the client metadata "uuid" this session opened with, which
	// Reclaim refuses to target; reclaim holds the one pending reclaim wait.
	uuid    string
	reclaim chan mds.ReclaimReply
	// reclaimUncertain is set when a reclaim wait ended without its reply. The
	// reply carries no request identity, so a late one could not be told from
	// the answer to a new reclaim; further reclaims on this session are refused.
	reclaimUncertain bool
}

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

// WaitOpen waits until the session leaves Opening. It returns the OPEN reply,
// or the terminal cause. Its ctx bounds only this local wait.
func (s *MDSSession) WaitOpen(ctx context.Context) (MDSSessionOpenInfo, error) {
	for {
		s.mu.Lock()
		state, err, changed, open := s.state, s.err, s.changed, cloneOpenInfo(s.open)
		s.mu.Unlock()
		if state.terminal() {
			return MDSSessionOpenInfo{}, err
		}
		if state != MDSSessionOpening {
			return open, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MDSSessionOpenInfo{}, ctx.Err()
		}
	}
}

// Done closes when the session reaches a terminal state.
func (s *MDSSession) Done() <-chan struct{} { return s.done }

// Err returns the terminal cause, or nil while the session is live. A
// confirmed Close leaves ErrMDSSessionClosed.
func (s *MDSSession) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

func (s *MDSSession) Status() MDSSessionStatus {
	m := s.conn
	s.mu.Lock()
	defer s.mu.Unlock()
	return MDSSessionStatus{State: s.state, Generation: s.generation, ConnectionGeneration: m.generation, GlobalID: m.globalID, Target: m.target,
		PushSequence: s.push, RenewSequence: s.renewSeq, ReconnectedFrom: s.reconnectFrom, LastRenewal: s.renewal, Open: cloneOpenInfo(s.open), ReadOnly: s.readOnly, Err: s.err}
}

// Next returns the oldest unsolicited session event. Accepted events remain
// readable after the session ends; then Next returns the terminal cause.
// ctx bounds only this wait. A FLUSHMSG event must be answered with
// AcknowledgeFlush once the caller has sent its capability releases.
func (s *MDSSession) Next(ctx context.Context) (MDSSessionEvent, error) {
	if err := ctx.Err(); err != nil {
		return MDSSessionEvent{}, err
	}
	for {
		s.mu.Lock()
		if len(s.events) != 0 {
			event := s.events[0]
			s.events[0] = MDSSessionEvent{}
			s.events = s.events[1:]
			s.mu.Unlock()
			return event, nil
		}
		state, err, changed := s.state, s.err, s.changed
		s.mu.Unlock()
		if state.terminal() {
			return MDSSessionEvent{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MDSSessionEvent{}, ctx.Err()
		}
	}
}

// Renew sends one REQUEST_RENEWCAPS and waits for a RENEWCAPS reply with the
// same or a later sequence. ctx bounds only the local wait.
func (s *MDSSession) Renew(ctx context.Context) (MDSSessionRenewal, error) {
	if err := ctx.Err(); err != nil {
		return MDSSessionRenewal{}, err
	}
	seq, err := s.sendRenewal(ctx)
	if err != nil {
		return MDSSessionRenewal{}, err
	}
	for {
		s.mu.Lock()
		renewal, state, terminal, changed := s.renewal, s.state, s.err, s.changed
		s.mu.Unlock()
		if renewal.Sequence >= seq {
			return renewal, nil
		}
		if state.terminal() {
			return MDSSessionRenewal{}, terminal
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MDSSessionRenewal{}, ctx.Err()
		}
	}
}

// FlushJournal asks the MDS to flush its journal now (REQUEST_FLUSH_MDLOG,
// as native Client::flush_mdlog does) so pending unsafe requests become safe
// sooner. The MDS sends no answer; safe replies remain the only completion
// signal. ctx governs write admission only.
func (s *MDSSession) FlushJournal(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	state, err, features := s.state, s.err, s.open.SupportedFeatures
	s.mu.Unlock()
	if state.terminal() {
		return err
	}
	if state == MDSSessionOpening {
		return errors.New("ceph: MDS session is not open")
	}
	// Native clients send this only to MDSs advertising LUMINOUS (bit 7).
	if !mds.FeatureBitsetHas(features, 7) {
		return ErrMDSRequestFeatures
	}
	message, err := mds.EncodeSessionControl(mds.SessionRequestFlushMDLog, 0)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
}

// MDSReclaimReply is the MDS's answer to Reclaim. Result is the signed code
// (zero on success). Addresses are the evicted session's addresses; an empty
// list with Result 0 means the MDS found no session with that uuid, which the
// native client reports as ENOENT. Epoch is the OSD map epoch the MDS held
// when it evicted the target, for the caller's blocklist wait.
type MDSReclaimReply struct {
	Result    int32
	Epoch     uint32
	Addresses []MDSMapAddress
}

// Reclaim asks the MDS to evict the session whose client metadata uuid is
// uuid and blocklist its address, then answer (MClientReclaim with
// CEPH_RECLAIM_RESET, as Client::start_reclaim sends it). The target must
// belong to the same CephX identity (the MDS answers EPERM otherwise) and this
// session must not use that uuid itself. The MDS must advertise
// RECLAIM_CLIENT. One reclaim may be pending per session. ctx bounds only the
// local wait; after the message is sent a cancelled wait or session end
// returns an OutcomeUnknownError because the target may already be evicted.
func (s *MDSSession) Reclaim(ctx context.Context, uuid string) (MDSReclaimReply, error) {
	if err := ctx.Err(); err != nil {
		return MDSReclaimReply{}, err
	}
	message, err := mds.EncodeReclaim(uuid, mds.ReclaimReset)
	if err != nil {
		return MDSReclaimReply{}, err
	}
	s.mu.Lock()
	state, terminal, features := s.state, s.err, s.open.SupportedFeatures
	switch {
	case state.terminal():
		s.mu.Unlock()
		return MDSReclaimReply{}, terminal
	case state == MDSSessionOpening:
		s.mu.Unlock()
		return MDSReclaimReply{}, errors.New("ceph: MDS session is not open")
	case !mds.FeatureBitsetHas(features, mds.FeatureReclaimClient):
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimUnsupported
	case uuid == s.uuid:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimSelf
	case s.reclaim != nil:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimPending
	case s.reclaimUncertain:
		s.mu.Unlock()
		return MDSReclaimReply{}, ErrMDSReclaimUncertain
	}
	wait := make(chan mds.ReclaimReply, 1)
	s.reclaim = wait
	s.mu.Unlock()
	clear := func(abandoned bool) {
		s.mu.Lock()
		if s.reclaim == wait {
			s.reclaim = nil
			if abandoned {
				s.reclaimUncertain = true
			}
		}
		s.mu.Unlock()
	}
	if err := s.conn.session.SendControl(ctx, message); err != nil {
		clear(false)
		return MDSReclaimReply{}, err
	}
	select {
	case reply := <-wait:
		return publicReclaimReply(reply), nil
	case <-s.done:
		clear(true)
		select {
		case reply := <-wait:
			return publicReclaimReply(reply), nil
		default:
		}
		return MDSReclaimReply{}, &OutcomeUnknownError{Cause: s.Err()}
	case <-ctx.Done():
		clear(true)
		select {
		case reply := <-wait:
			return publicReclaimReply(reply), nil
		default:
		}
		return MDSReclaimReply{}, &OutcomeUnknownError{Cause: ctx.Err()}
	}
}

// FinishReclaim sends MClientReclaim with only FLAG_FINISH, as
// Client::finish_reclaim does after a successful reclaim. The MDS sends no
// answer and, after a RESET reclaim, has nothing left to finish. ctx governs
// write admission only.
func (s *MDSSession) FinishReclaim(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	state, err, features := s.state, s.err, s.open.SupportedFeatures
	s.mu.Unlock()
	if state.terminal() {
		return err
	}
	if state == MDSSessionOpening {
		return errors.New("ceph: MDS session is not open")
	}
	if !mds.FeatureBitsetHas(features, mds.FeatureReclaimClient) {
		return ErrMDSReclaimUnsupported
	}
	message, err := mds.EncodeReclaim("", mds.ReclaimFinish)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
}

func publicReclaimReply(r mds.ReclaimReply) MDSReclaimReply {
	out := MDSReclaimReply{Result: r.Result, Epoch: r.Epoch}
	for _, a := range r.Addresses {
		out.Addresses = append(out.Addresses, MDSMapAddress{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID})
	}
	return out
}

// deliverReclaim hands a reply to the pending Reclaim. A reply with no waiter
// is a late answer to an abandoned wait and is ignored.
func (s *MDSSession) deliverReclaim(reply mds.ReclaimReply) {
	s.mu.Lock()
	wait := s.reclaim
	s.reclaim = nil
	s.mu.Unlock()
	if wait != nil {
		wait <- reply
	}
}

// AcknowledgeFlush sends FLUSHMSG_ACK for a FLUSHMSG event's sequence. The
// library never acknowledges a flush on the caller's behalf. ctx governs only
// write admission; success is not proof that the MDS processed the ack.
func (s *MDSSession) AcknowledgeFlush(ctx context.Context, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	state, err := s.state, s.err
	s.mu.Unlock()
	if state.terminal() {
		return err
	}
	message, err := mds.EncodeSessionControl(mds.SessionFlushMessageAck, seq)
	if err != nil {
		return err
	}
	return s.conn.session.SendControl(ctx, message)
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

func (s *MDSSession) sendRenewal(ctx context.Context) (uint64, error) {
	s.mu.Lock()
	if s.state.terminal() {
		err := s.err
		s.mu.Unlock()
		return 0, err
	}
	if s.state == MDSSessionOpening {
		s.mu.Unlock()
		return 0, errors.New("ceph: MDS session is not open")
	}
	if s.renewSeq == ^uint64(0) {
		s.mu.Unlock()
		return 0, errors.New("ceph: MDS session renewal sequences exhausted")
	}
	s.renewSeq++
	seq := s.renewSeq
	s.renewSent[seq] = time.Now()
	// Bound outstanding renewals: a reply covers every older sequence.
	if len(s.renewSent) > 64 {
		delete(s.renewSent, slices.Min(slices.Collect(maps.Keys(s.renewSent))))
	}
	s.mu.Unlock()
	message, err := mds.EncodeSessionControl(mds.SessionRequestRenewCaps, seq)
	if err != nil {
		return 0, err
	}
	if err := s.conn.session.SendControl(ctx, message); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *MDSSession) renewLoop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
		case <-s.renewNow:
		case <-s.closeNow:
			// A counted push raised the sequence while closing; the MDS drops
			// a REQUEST_CLOSE that is behind, so resend it as natively.
			s.mu.Lock()
			state, seq := s.state, s.push
			s.mu.Unlock()
			if state != MDSSessionClosing {
				continue
			}
			message, err := mds.EncodeSessionControl(mds.SessionRequestClose, seq)
			if err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(s.conn.ctx, s.renewInterval)
			err = s.conn.session.SendControl(ctx, message)
			cancel()
			if err != nil && s.conn.session.Err() != nil {
				return
			}
			continue
		}
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()
		if state != MDSSessionOpen && state != MDSSessionStale {
			continue
		}
		ctx, cancel := context.WithTimeout(s.conn.ctx, s.renewInterval)
		_, err := s.sendRenewal(ctx)
		cancel()
		if err != nil && s.conn.session.Err() != nil {
			return
		}
	}
}

func (s *MDSSession) triggerRenewal() {
	select {
	case s.renewNow <- struct{}{}:
	default:
	}
}

func (s *MDSSession) pushEventLocked(event MDSSessionEvent) error {
	if len(s.events) >= s.maxEvents {
		return ErrMDSSessionOverflow
	}
	s.events = append(s.events, event)
	return nil
}

// dispatch runs on the transport read loop and never blocks. An error fails
// the MDS transport (and with it the session as Lost).
func (s *MDSSession) dispatch(message mds.ClientSession, received time.Time) error {
	s.mu.Lock()
	if s.state.terminal() {
		s.mu.Unlock()
		return fmt.Errorf("%w: MDS session message after terminal state", msgr.ErrFrame)
	}
	s.arrival++
	event := MDSSessionEvent{Arrival: s.arrival, Type: mds.ClientSessionMessage, Op: message.Op, Sequence: message.Sequence, MaxCaps: message.MaxCaps, MaxLeases: message.MaxLeases,
		Flags: message.Flags, Metadata: message.Metadata, SupportedFeatures: message.SupportedFeatures, Received: received}
	var terminal MDSSessionState
	var cause error
	renew := false
	var err error
	switch message.Op {
	case mds.SessionOpen:
		info := MDSSessionOpenInfo{Sequence: message.Sequence, SupportedFeatures: message.SupportedFeatures, MetricFlags: message.MetricFlags,
			Flags: message.Flags, Metadata: message.Metadata, CapAuths: publicCapAuths(message.CapAuths)}
		if s.push == 0 && message.Sequence != 0 {
			s.push = message.Sequence
		}
		if s.state == MDSSessionOpening {
			s.open = info
			s.state = MDSSessionOpen
			renew = true
		} else {
			// Native clients refresh MDS features from a later OPEN.
			s.open.SupportedFeatures, s.open.MetricFlags = info.SupportedFeatures, info.MetricFlags
			err = s.pushEventLocked(event)
		}
	case mds.SessionClose:
		terminal, cause = MDSSessionClosed, ErrMDSSessionClosed
		if s.reconnectFrom != 0 && s.state == MDSSessionOpening {
			cause = fmt.Errorf("%w: %w", ErrMDSSessionReconnectDenied, ErrMDSSessionClosed)
		}
	case mds.SessionReject:
		reason := message.Metadata["error_string"]
		terminal, cause = MDSSessionRejected, &MDSSessionRejectedError{Reason: reason, Flags: message.Flags, Blocklisted: message.Flags&mds.SessionFlagBlocklisted != 0, Metadata: message.Metadata}
	case mds.SessionRenewCaps:
		if s.state == MDSSessionOpening {
			err = fmt.Errorf("%w: MDS RENEWCAPS before OPEN", msgr.ErrFrame)
			break
		}
		if sent, ok := s.renewSent[message.Sequence]; ok {
			for seq := range s.renewSent {
				if seq <= message.Sequence {
					delete(s.renewSent, seq)
				}
			}
			s.renewal = MDSSessionRenewal{Sequence: message.Sequence, Requested: sent, Confirmed: received}
			if s.state == MDSSessionStale && message.Sequence == s.renewSeq {
				s.state = MDSSessionOpen
			}
		} else if message.Sequence == 0 || message.Sequence > s.renewSeq {
			err = fmt.Errorf("%w: MDS RENEWCAPS for an unsent sequence", msgr.ErrFrame)
		}
	case mds.SessionStale:
		if s.state == MDSSessionOpening {
			err = fmt.Errorf("%w: MDS STALE before OPEN", msgr.ErrFrame)
			break
		}
		if s.state == MDSSessionOpen {
			s.state = MDSSessionStale
		}
		renew = true
		err = s.pushEventLocked(event)
	case mds.SessionRecallState, mds.SessionFlushMessage:
		if s.state == MDSSessionOpening {
			err = fmt.Errorf("%w: MDS session control before OPEN", msgr.ErrFrame)
			break
		}
		err = s.pushEventLocked(event)
	case mds.SessionForceReadOnly:
		s.readOnly = true
		err = s.pushEventLocked(event)
	default:
		err = fmt.Errorf("%w: unexpected MDS session operation %d", msgr.ErrFrame, message.Op)
	}
	if err == nil && terminal == 0 {
		// Wake Next/Renew/WaitOpen waiters for queued events or renewals.
		s.setStateLocked(s.state)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if terminal != 0 {
		// CLOSE and REJECT are confirmed by the MDS; nothing stays unresolved.
		s.finish(terminal, cause, false)
		return nil
	}
	if renew {
		s.triggerRenewal()
	}
	return nil
}

func publicCapAuths(in []mds.CapAuth) []MDSCapAuth {
	out := make([]MDSCapAuth, len(in))
	for i, a := range in {
		out[i] = MDSCapAuth{UID: a.UID, GIDs: slices.Clone(a.GIDs), Path: a.Path, FilesystemName: a.FilesystemName, RootSquash: a.RootSquash, Readable: a.Readable, Writeable: a.Writeable}
	}
	return out
}

func cloneOpenInfo(in MDSSessionOpenInfo) MDSSessionOpenInfo {
	out := in
	out.SupportedFeatures, out.MetricFlags, out.Metadata = slices.Clone(in.SupportedFeatures), slices.Clone(in.MetricFlags), maps.Clone(in.Metadata)
	out.CapAuths = make([]MDSCapAuth, len(in.CapAuths))
	for i, a := range in.CapAuths {
		out.CapAuths[i] = a
		out.CapAuths[i].GIDs = slices.Clone(a.GIDs)
	}
	if in.CapAuths == nil {
		out.CapAuths = nil
	}
	return out
}

// dispatchMDSMessage routes one non-reply MDS message. MClientSession is
// always implemented; replies, forwards and counted pushes only with
// EnableCephFS. Anything else fails the transport as unsupported.
func (m *MDSConnection) dispatchMDSMessage(message msgr.MessageData) error {
	if message.Type == mds.ClientReclaimReplyMessage {
		decoded, err := mds.DecodeReclaimReply(message, m.client.options.MaxFrameSize)
		if err != nil {
			return fmt.Errorf("%w: invalid MDS reclaim reply: %w", msgr.ErrFrame, err)
		}
		m.client.mu.Lock()
		s := m.cephfs
		m.client.mu.Unlock()
		if s == nil {
			return fmt.Errorf("%w: MDS reclaim reply without a local session", msgr.ErrFrame)
		}
		s.deliverReclaim(decoded)
		return nil
	}
	if message.Type != mds.ClientSessionMessage {
		return m.dispatchCephFS(message)
	}
	c := m.client
	decoded, err := mds.DecodeClientSession(message, c.options.MaxFrameSize)
	if err != nil {
		return fmt.Errorf("%w: invalid MDS session message: %w", msgr.ErrFrame, err)
	}
	c.mu.Lock()
	s := m.cephfs
	c.mu.Unlock()
	if s == nil {
		return fmt.Errorf("%w: MDS session message without a local session", msgr.ErrFrame)
	}
	return s.dispatch(decoded, time.Now())
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

// dispatchCephFS routes replies, forwards and counted pushes (EnableCephFS).
func (m *MDSConnection) dispatchCephFS(message msgr.MessageData) error {
	c := m.client
	cephfs := c.options.EnableCephFS
	switch message.Type {
	case mds.ClientReplyMessage, mds.ClientRequestForwardMessage:
	case mds.ClientCapsMessage, mds.ClientLeaseMessage, mds.ClientSnapMessage, mds.ClientQuotaMessage:
	default:
		cephfs = false
	}
	if !cephfs {
		return fmt.Errorf("%w: unsupported MDS message %d", msgr.ErrFeatures, message.Type)
	}
	c.mu.Lock()
	s := m.cephfs
	c.mu.Unlock()
	if s == nil {
		return fmt.Errorf("%w: CephFS message without a local session", msgr.ErrFrame)
	}
	received := time.Now()
	if message.Type == mds.ClientReplyMessage || message.Type == mds.ClientRequestForwardMessage {
		s.mu.Lock()
		err := s.dispatchReplyLocked(message, received)
		if err == nil {
			s.setStateLocked(s.state)
		}
		s.mu.Unlock()
		return err
	}
	event, err := decodePush(message, c.options.MaxFrameSize)
	if err != nil {
		return err
	}
	event.Received = received
	s.mu.Lock()
	if s.state.terminal() || s.state == MDSSessionOpening {
		s.mu.Unlock()
		return fmt.Errorf("%w: MDS push outside an open session", msgr.ErrFrame)
	}
	if s.push == ^uint64(0) {
		s.mu.Unlock()
		return fmt.Errorf("%w: MDS push sequence exhausted", msgr.ErrFrame)
	}
	// Every capability, lease, snapshot and quota push is counted by the MDS
	// (send_message_client_counted) and by the native client (got_mds_push).
	s.push++
	s.arrival++
	event.Arrival = s.arrival
	closing := s.state == MDSSessionClosing
	err = s.pushEventLocked(event)
	if err == nil {
		s.setStateLocked(s.state)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if closing {
		select {
		case s.closeNow <- struct{}{}:
		default:
		}
	}
	return nil
}

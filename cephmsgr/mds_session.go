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
// declares without EnableCephFS: bits 0-8 and 10 (raw little-endian
// feature_bitset_t).
func MDSSessionClientFeatures() []byte { return slices.Clone(mds.ClientFeatures) }

// MDSCephFSClientFeatures is the bitset an EnableCephFS session declares:
// bits 0-10, 12, 15 and 18-20 (see MDS_CEPHFS_CONTRACT.md).
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

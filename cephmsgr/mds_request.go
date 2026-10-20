package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// CephFS wire structures shared with the internal codecs. They are raw wire
// values: the library interprets none of them as filesystem state.
type (
	MDSTime           = mds.Time
	MDSFilePath       = mds.FilePath
	MDSRequestRelease = mds.RequestRelease
	MDSReplyCap       = mds.ReplyCap
	MDSFileLayout     = mds.FileLayout
	MDSFragSplit      = mds.FragSplit
	MDSInodeStat      = mds.InodeStat
	MDSDirStat        = mds.DirStat
	MDSLeaseStat      = mds.LeaseStat
	MDSTrace          = mds.Trace
	MDSReaddirEntry   = mds.ReaddirEntry
	MDSReaddir        = mds.Readdir
	MDSSnapRealm      = mds.SnapRealm
)

// Metadata operation codes and request flags (CEPH_MDS_OP_*, CEPH_MDS_FLAG_*).
const (
	MDSOpLookup       = mds.OpLookup
	MDSOpGetattr      = mds.OpGetattr
	MDSOpLookupHash   = mds.OpLookupHash
	MDSOpLookupParent = mds.OpLookupParent
	MDSOpLookupIno    = mds.OpLookupIno
	MDSOpLookupName   = mds.OpLookupName
	MDSOpSetxattr     = mds.OpSetxattr
	MDSOpRmxattr      = mds.OpRmxattr
	MDSOpSetlayout    = mds.OpSetlayout
	MDSOpSetattr      = mds.OpSetattr
	MDSOpSetFileLock  = mds.OpSetFileLock
	MDSOpGetFileLock  = mds.OpGetFileLock
	MDSOpSetDirLayout = mds.OpSetDirLayout
	MDSOpMknod        = mds.OpMknod
	MDSOpLink         = mds.OpLink
	MDSOpUnlink       = mds.OpUnlink
	MDSOpRename       = mds.OpRename
	MDSOpMkdir        = mds.OpMkdir
	MDSOpRmdir        = mds.OpRmdir
	MDSOpSymlink      = mds.OpSymlink
	MDSOpCreate       = mds.OpCreate
	MDSOpOpen         = mds.OpOpen
	MDSOpReaddir      = mds.OpReaddir
	MDSOpLookupSnap   = mds.OpLookupSnap
	MDSOpMksnap       = mds.OpMksnap
	MDSOpRmsnap       = mds.OpRmsnap
	MDSOpLssnap       = mds.OpLssnap
	MDSOpRenamesnap   = mds.OpRenamesnap

	MDSRequestFlagWantDentry = mds.RequestFlagWantDentry
)

var (
	// ErrCephFSDisabled refuses CephFS metadata operations on a client
	// without Options.EnableCephFS.
	ErrCephFSDisabled = errors.New("ceph: CephFS metadata operations require EnableCephFS")
	// ErrMDSRequestFeatures refuses requests when the MDS's OPEN did not
	// advertise the CephFS features this request/reply codec requires.
	ErrMDSRequestFeatures = errors.New("ceph: MDS does not advertise the required CephFS request features")
	// ErrMDSReclaimUnsupported refuses a reclaim when the MDS's OPEN did not
	// advertise RECLAIM_CLIENT.
	ErrMDSReclaimUnsupported = errors.New("ceph: MDS does not advertise RECLAIM_CLIENT")
	// ErrMDSReclaimSelf refuses reclaiming this session's own uuid.
	ErrMDSReclaimSelf = errors.New("ceph: a session cannot reclaim its own uuid")
	// ErrMDSReclaimUncertain refuses a reclaim on a session whose earlier reclaim
	// ended without its reply: the reply has no request identity, so a late one
	// could not be told apart from the answer to a new request. Open a new
	// session instead.
	ErrMDSReclaimUncertain = errors.New("ceph: an earlier MDS reclaim on this session has an unknown outcome")
	// ErrMDSReclaimPending refuses a second concurrent reclaim on a session.
	ErrMDSReclaimPending = errors.New("ceph: an MDS reclaim is already pending on this session")
)

// MDSRequestForwardedError completes a request the MDS forwarded to another
// rank. The library does not resend: the caller chooses whether to submit
// the request to DestinationMDS with Forward attempts recorded.
type MDSRequestForwardedError struct {
	// Arrival orders the forward among the session's replies and pushes.
	Arrival        uint64
	Transaction    uint64
	DestinationMDS int32
	Forward        int32
	MustResend     bool
}

func (e *MDSRequestForwardedError) Error() string {
	return fmt.Sprintf("ceph: MDS forwarded request %d to mds.%d (forward %d)", e.Transaction, e.DestinationMDS, e.Forward)
}

// MDSRequest is one raw MClientRequest. Args is the packed op-specific
// ceph_mds_request_args union, prepared by the caller. Retry and Forward are
// the attempt counters the caller wants to report. Resubmission is always a
// caller decision: ResubmitForwarded and ReconnectSession keep the original
// TID, while a new Submit gets a new one.
// Stamp zero means the local send time.
type MDSRequest struct {
	Op                   uint32
	Flags                uint32
	MDSMapEpoch          uint32
	Retry, Forward       uint32
	CallerUID, CallerGID uint32
	OwnerUID, OwnerGID   uint32
	GIDs                 []uint64
	Args                 [mds.RequestArgsBytes]byte
	Path, Path2          MDSFilePath
	Releases             []MDSRequestRelease
	AlternateName        []byte
	FscryptAuth          []byte
	FscryptFile          []byte
	Stamp                MDSTime
	// Data is the data segment: a setxattr value or a create's encoded
	// xattr map. It is copied before Submit returns.
	Data []byte
	// ino is the replayed request's target inode (REPLAY only, set by an
	// explicit resubmission after reconnect).
	ino uint64
}

// MDSReply is one MClientReply. Result is the signed wire code (Linux
// numbering); Trace, Extra and SnapTrace are raw blobs, decodable with the
// Decode* methods. Unsafe replies precede safe completion of the same request.
// Safe completion of in-memory operations such as SETFILELOCK does not imply
// journal durability. Op is the MDS's: a retried MKNOD, MKDIR or SYMLINK already in
// the MDS's completed-request record can be answered as LOOKUP or GETATTR.
type MDSReply struct {
	// Arrival orders this reply among the session's replies, pushes and
	// session events in receive order (see MDSSessionEvent.Arrival).
	Arrival                  uint64
	Transaction              uint64
	Op                       uint32
	Result                   int32
	MDSMapEpoch              uint32
	Safe, IsDentry, IsTarget bool
	Trace, Extra, SnapTrace  []byte
	Received                 time.Time
}

func (r MDSReply) DecodeTrace() (MDSTrace, error) {
	return mds.DecodeTrace(r.Trace, r.IsDentry, r.IsTarget)
}

// DecodeReaddir decodes the Extra blob of a successful readdir reply.
func (r MDSReply) DecodeReaddir() (MDSReaddir, error) { return mds.DecodeReaddir(r.Extra) }

func (r MDSReply) DecodeSnapTrace() ([]MDSSnapRealm, error) { return mds.DecodeSnapTrace(r.SnapTrace) }

// DecodeMDSSnapTrace decodes a SnapRealmInfoNew snap trace blob, such as
// MDSSnap.Trace or MDSCaps.SnapTrace from a push.
func DecodeMDSSnapTrace(raw []byte) ([]MDSSnapRealm, error) { return mds.DecodeSnapTrace(raw) }

// DecodeMDSXattrs decodes an InodeStat xattr blob into name→value.
func DecodeMDSXattrs(raw []byte) (map[string][]byte, error) { return mds.DecodeXattrs(raw) }

// MDSRequestCall tracks one submitted request until its safe reply, a
// forward, or the end of the session. It holds one MaxInFlight slot until
// then; abandoning the local wait does not release it early.
type MDSRequestCall struct {
	session     *MDSSession
	transaction uint64
	op          uint32
	submitted   time.Time
	// request keeps the submitted request (stamp fixed) so the caller can
	// explicitly resubmit it with the same TID after an MDS reconnect.
	request MDSRequest
	// forwarded marks a forwarded call already resubmitted (session.mu).
	forwarded bool
	// guarded by session.mu
	first, safe *MDSReply
	err         error
	resolved    bool
}

func (call *MDSRequestCall) Transaction() uint64 { return call.transaction }

// Wait returns the first reply (unsafe or safe). A forward returns
// *MDSRequestForwardedError; a session that ends first returns its cause
// wrapped in OutcomeUnknownError. ctx bounds only this local wait.
func (call *MDSRequestCall) Wait(ctx context.Context) (MDSReply, error) {
	return call.wait(ctx, false)
}

// WaitSafe returns the safe reply. If the session ends after an unsafe reply
// the journaled outcome is unknown and an OutcomeUnknownError is returned.
func (call *MDSRequestCall) WaitSafe(ctx context.Context) (MDSReply, error) {
	return call.wait(ctx, true)
}

func (call *MDSRequestCall) wait(ctx context.Context, safe bool) (MDSReply, error) {
	s := call.session
	for {
		s.mu.Lock()
		reply, err, resolved, changed := call.first, call.err, call.resolved, s.changed
		if safe {
			reply = call.safe
		}
		s.mu.Unlock()
		if reply != nil {
			return cloneReply(*reply), nil
		}
		if resolved {
			return MDSReply{}, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return MDSReply{}, ctx.Err()
		}
	}
}

func cloneReply(r MDSReply) MDSReply {
	r.Trace, r.Extra, r.SnapTrace = slices.Clone(r.Trace), slices.Clone(r.Extra), slices.Clone(r.SnapTrace)
	return r
}

// Submit queues one metadata request on an open (or stale) session. ctx
// governs slot admission and write admission only; once Submit returns, the
// request is ordered for transmission. REPLAY and ASYNC flags, a preset
// inode number and an MDS lacking REPLY_ENCODING, 32-bit retry/forward,
// new snaprealm info or owner UID/GID support are refused before sending.
func (s *MDSSession) Submit(ctx context.Context, request MDSRequest) (*MDSRequestCall, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.conn.client.options.EnableCephFS {
		return nil, ErrCephFSDisabled
	}
	if request.Flags&^mds.RequestFlagWantDentry != 0 {
		return nil, errors.New("ceph: MDS request flags other than WANT_DENTRY are unsupported")
	}
	s.mu.Lock()
	state, terminal, features := s.state, s.err, s.open.SupportedFeatures
	s.mu.Unlock()
	if state.terminal() {
		return nil, terminal
	}
	if state != MDSSessionOpen && state != MDSSessionStale {
		return nil, errors.New("ceph: MDS session is not open")
	}
	if err := requestFeatures(features); err != nil {
		return nil, err
	}
	if err := s.acquireSlot(ctx); err != nil {
		return nil, err
	}
	transaction, err := s.conn.client.nextMDSTransaction()
	if err != nil {
		<-s.conn.client.calls
		return nil, err
	}
	return s.submit(ctx, request, features, transaction)
}

func requestFeatures(features []byte) error {
	for _, bit := range []uint{mds.FeatureReplyEncoding, mds.Feature32BitsRetryFwd, mds.FeatureNewSnaprealmInfo, mds.FeatureHasOwnerUIDGID} {
		if !mds.FeatureBitsetHas(features, bit) {
			return ErrMDSRequestFeatures
		}
	}
	return nil
}

func (s *MDSSession) acquireSlot(ctx context.Context) error {
	select {
	case s.conn.client.calls <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.Err()
	}
}

// submit registers and queues one request with an already allocated TID
// and an acquired MaxInFlight slot, which it releases on failure.
func (s *MDSSession) submit(ctx context.Context, request MDSRequest, features []byte, transaction uint64) (*MDSRequestCall, error) {
	m := s.conn
	c := m.client
	release := func() { <-c.calls }
	if uint32(len(request.Releases)) > mds.MaxRequestReleases || uint32(len(request.GIDs)) > mds.MaxRequestGIDs {
		release()
		return nil, fmt.Errorf("ceph: MDS request releases or GIDs: %w", ErrLimitExceeded)
	}
	call := &MDSRequestCall{session: s, transaction: transaction, op: request.Op, submitted: time.Now()}
	if request.Stamp == (MDSTime{}) {
		request.Stamp = MDSTime{Seconds: uint32(call.submitted.Unix()), Nanoseconds: uint32(call.submitted.Nanosecond())}
	}
	request.GIDs, request.Releases, request.AlternateName = slices.Clone(request.GIDs), slices.Clone(request.Releases), slices.Clone(request.AlternateName)
	request.FscryptAuth, request.FscryptFile, request.Data = slices.Clone(request.FscryptAuth), slices.Clone(request.FscryptFile), slices.Clone(request.Data)
	call.request = request
	if err := s.lockRequestAdmission(ctx); err != nil {
		release()
		return nil, err
	}
	defer s.unlockRequestAdmission()
	s.mu.Lock()
	if s.state.terminal() {
		err := s.err
		s.mu.Unlock()
		release()
		return nil, err
	}
	if s.state == MDSSessionClosing {
		s.mu.Unlock()
		release()
		return nil, errors.New("ceph: MDS session is closing")
	}
	if s.requests == nil {
		s.requests = make(map[uint64]*MDSRequestCall)
	}
	if s.requests[transaction] != nil {
		s.mu.Unlock()
		release()
		return nil, errors.New("ceph: MDS request TID already tracked")
	}
	s.requests[transaction] = call
	// SETFILELOCK changes only the MDS's in-memory lock state and is not
	// recorded among journaled completed requests. Native Client::make_request
	// and unregister_request exclude it from the completed-request trim cursor.
	var oldest uint64
	for tid, pending := range s.requests {
		if pending.op != mds.OpSetFileLock && (oldest == 0 || tid < oldest) {
			oldest = tid
		}
	}
	s.mu.Unlock()
	wire := mds.ClientRequest{OldestClientTID: oldest, MDSMapEpoch: request.MDSMapEpoch, Flags: request.Flags, Retry: request.Retry, Forward: request.Forward,
		Op: request.Op, CallerUID: request.CallerUID, CallerGID: request.CallerGID, Ino: request.ino, Args: request.Args, OwnerUID: request.OwnerUID, OwnerGID: request.OwnerGID,
		Path: request.Path, Path2: request.Path2, Releases: request.Releases, Stamp: request.Stamp, GIDs: request.GIDs,
		AlternateName: request.AlternateName, FscryptAuth: request.FscryptAuth, FscryptFile: request.FscryptFile, Data: request.Data}
	message, err := mds.EncodeClientRequest(wire, features)
	if err == nil && uint64(len(message.Front))+uint64(len(message.Data))+41 > uint64(c.options.MaxFrameSize) {
		err = fmt.Errorf("ceph: MDS request exceeds frame limit: %w", ErrLimitExceeded)
	}
	if err == nil {
		message.Transaction = transaction
		err = m.session.SendControl(ctx, message)
	}
	if err != nil {
		s.mu.Lock()
		// Session loss may already have resolved this tracked call while
		// SendControl was waiting for admission. The resolver owns the one
		// slot release; do not release it again or consume another call's slot.
		s.resolveLocked(call, err)
		s.mu.Unlock()
		return nil, err
	}
	return call, nil
}

// ResubmitForwarded sends a forwarded call again on this session, with its
// original TID and stamp and the forward count the MDS reported, as native
// clients resend a forwarded request to the new rank. Retry restarts at zero
// on the destination. This session must name the forward's destination rank
// on another connection of the same Client and filesystem label. Original
// embedded releases describe source-rank state and are omitted; the caller
// may send destination capability/lease controls explicitly. Each forwarded
// call can be resubmitted once. The MDS forwards before
// executing, so this is not a replay of executed work.
func (s *MDSSession) ResubmitForwarded(ctx context.Context, call *MDSRequestCall) (*MDSRequestCall, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.conn.client.options.EnableCephFS {
		return nil, ErrCephFSDisabled
	}
	if call == nil || call.session == s || call.session.conn.client != s.conn.client {
		return nil, errors.New("ceph: forwarded call does not belong to another session of this client")
	}
	previous := call.session
	if previous.conn.target.FilesystemID != s.conn.target.FilesystemID {
		return nil, ErrMDSTargetChanged
	}
	previous.mu.Lock()
	var forward *MDSRequestForwardedError
	ok := call.resolved && errors.As(call.err, &forward) && !call.forwarded
	request := call.request
	if ok {
		call.forwarded = true
	}
	previous.mu.Unlock()
	if !ok {
		return nil, errors.New("ceph: only a forwarded call can be resubmitted once")
	}
	undo := func() {
		previous.mu.Lock()
		call.forwarded = false
		previous.mu.Unlock()
	}
	if forward.DestinationMDS != s.conn.target.Rank {
		undo()
		return nil, ErrMDSTargetChanged
	}
	s.mu.Lock()
	state, terminal, features := s.state, s.err, s.open.SupportedFeatures
	s.mu.Unlock()
	if state.terminal() {
		undo()
		return nil, terminal
	}
	if state != MDSSessionOpen && state != MDSSessionStale {
		undo()
		return nil, errors.New("ceph: MDS session is not open")
	}
	if err := requestFeatures(features); err != nil {
		undo()
		return nil, err
	}
	// Client::handle_client_request_forward resets the per-rank retry
	// counter before rebuilding this logical request for its destination.
	request.Retry = 0
	// Native clients rebuild releases from destination-rank cache state.
	// This transport cannot reuse source cap IDs/sequences or dentry leases.
	request.Releases = nil
	if forward.Forward >= 0 {
		request.Forward = uint32(forward.Forward)
	}
	if err := s.acquireSlot(ctx); err != nil {
		undo()
		return nil, err
	}
	resent, err := s.submit(ctx, request, features, call.transaction)
	if err != nil {
		undo()
	}
	return resent, err
}

// resolveLocked completes a call and releases its slot. s.mu is held.
func (s *MDSSession) resolveLocked(call *MDSRequestCall, err error) {
	if call.resolved {
		return
	}
	call.resolved, call.err = true, err
	delete(s.requests, call.transaction)
	<-s.conn.client.calls
}

// failRequestsLocked resolves every pending call when the session ends:
// queued requests may have reached the MDS, so the outcome is unknown.
func (s *MDSSession) failRequestsLocked(cause error) {
	for _, call := range s.requests {
		s.resolveLocked(call, &OutcomeUnknownError{Cause: cause})
	}
}

// dispatchReply routes MClientReply and MClientRequestForward. s.mu is held.
// convertedCompletedReply reports the one reply whose operation legitimately
// differs from its request. A retried non-REPLAY MKNOD, MKDIR or SYMLINK whose
// journaled completed-request record holds the inode it created is rewritten
// during clientreplay into LOOKUP (dentry wanted) or GETATTR
// (Server::handle_client_request), and MClientReply copies the rewritten op.
func convertedCompletedReply(request MDSRequest, op uint32) bool {
	if request.Retry == 0 || request.Flags&mds.RequestFlagReplay != 0 {
		return false
	}
	switch request.Op {
	case mds.OpMknod, mds.OpMkdir, mds.OpSymlink:
	default:
		return false
	}
	if request.Flags&mds.RequestFlagWantDentry != 0 {
		return op == mds.OpLookup
	}
	return op == mds.OpGetattr
}

func (s *MDSSession) dispatchReplyLocked(message msgr.MessageData, received time.Time) error {
	call := s.requests[message.Transaction]
	if call == nil {
		// Calls stay tracked until their safe reply or forward, after which the
		// MDS sends nothing more for that TID on this connection. Any other
		// reply (including one for a raw command TID) is a protocol error.
		return fmt.Errorf("%w: MDS reply for an untracked request", msgr.ErrFrame)
	}
	if message.Type == mds.ClientRequestForwardMessage {
		forward, err := mds.DecodeRequestForward(message, s.conn.client.options.MaxFrameSize)
		if err != nil {
			return fmt.Errorf("%w: invalid MDS request forward: %w", msgr.ErrFrame, err)
		}
		if call.first != nil {
			return fmt.Errorf("%w: MDS forwarded a request after replying", msgr.ErrFrame)
		}
		s.arrival++
		s.resolveLocked(call, &MDSRequestForwardedError{Arrival: s.arrival, Transaction: call.transaction, DestinationMDS: forward.DestinationMDS, Forward: forward.Forward, MustResend: forward.MustResend})
		return nil
	}
	decoded, err := mds.DecodeClientReply(message, s.conn.client.options.MaxFrameSize)
	if err != nil {
		return fmt.Errorf("%w: invalid MDS reply: %w", msgr.ErrFrame, err)
	}
	if decoded.Op != call.op && !convertedCompletedReply(call.request, decoded.Op) {
		return fmt.Errorf("%w: MDS reply operation differs from its request", msgr.ErrFrame)
	}
	s.arrival++
	reply := &MDSReply{Arrival: s.arrival, Transaction: call.transaction, Op: decoded.Op, Result: decoded.Result, MDSMapEpoch: decoded.MDSMapEpoch, Safe: decoded.Safe,
		IsDentry: decoded.IsDentry, IsTarget: decoded.IsTarget, Trace: decoded.Trace, Extra: decoded.Extra, SnapTrace: decoded.Snap, Received: received}
	switch {
	case call.first == nil:
		call.first = reply
	case !call.first.Safe && reply.Safe:
	default:
		return fmt.Errorf("%w: duplicate MDS reply", msgr.ErrFrame)
	}
	if reply.Safe {
		call.safe = reply
		s.resolveLocked(call, nil)
	}
	return nil
}

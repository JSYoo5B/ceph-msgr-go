package cephmsgr

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// Reconnect state supplied by the caller's capability cache.
type (
	MDSCapReconnect   = mds.CapReconnect
	MDSRealmReconnect = mds.RealmReconnect
)

// ErrMDSSessionReconnectDenied reports an MDS CLOSE answering a reconnect:
// the MDS was not in its reconnect phase, refused sessions, or no longer held
// the session. It wraps ErrMDSSessionClosed.
var ErrMDSSessionReconnectDenied = errors.New("ceph: MDS denied the CephFS session reconnect")

// MDSReconnectState is what the MDS needs to restore a client's capabilities
// after it restarts: one entry per held head-inode capability (with its short
// path, wanted/issued masks, realm and opaque file-lock blob) and the snap
// realms they belong to. The caller owns these values; native clients reset
// cap sequence numbers before reconnecting.
type MDSReconnectState struct {
	Caps   []MDSCapReconnect
	Realms []MDSRealmReconnect
	// Resubmit lists calls of the Lost session that the caller explicitly
	// chooses to send again with their original TIDs, before the reconnect
	// message (as native clients do). A call that received an unsafe reply
	// is sent with the REPLAY flag and the target inode from that reply; a
	// call without any reply is sent with its retry count increased. Cap
	// releases are dropped (they predate the capability reconnect). The MDS
	// answers from its completed-request record when it already journaled
	// the TID, so this is not a blind re-execution, but a request it never
	// received does execute. Safe-replied and forwarded calls are refused.
	Resubmit []*MDSRequestCall
}

// ReconnectSession reattaches the CephFS session of a Lost previous session
// to a new incarnation of the same MDS rank during that MDS's reconnect phase
// (pinned Server::handle_client_reconnect). m must be a new connection, opened
// by the caller to the new daemon address with the same filesystem label and
// rank, by the same Client identity; close the previous connection first (its
// failed handle keeps the filesystem/rank cache slot). The caller learns the
// new address and the reconnect state from its MDSMap watch; a reconnect sent
// to an MDS that already left its reconnect phase is denied. The state is sent as MClientReconnect v5
// (split into several messages with the "more" flag when it exceeds a frame);
// the MDS answers OPEN, which starts a new local session generation with push
// sequence zero, or CLOSE (ErrMDSSessionReconnectDenied). REJECT returns
// *MDSSessionRejectedError. Requests of the previous session are not
// resubmitted. ctx follows OpenSession: after the first message is queued its
// expiry returns the still-opening session with an OutcomeUnknownError.
func (m *MDSConnection) ReconnectSession(ctx context.Context, previous *MDSSession, state MDSReconnectState, options MDSSessionOptions) (*MDSSession, []*MDSRequestCall, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	c := m.client
	if !c.options.EnableCephFS {
		return nil, nil, ErrCephFSDisabled
	}
	if err := validateSessionOptions(&options); err != nil {
		return nil, nil, err
	}
	if previous == nil || previous.conn.client != c || previous.conn == m {
		return nil, nil, errors.New("ceph: MDS reconnect requires a previous session of this client on another connection")
	}
	status := previous.Status()
	if status.State != MDSSessionLost {
		return nil, nil, errors.New("ceph: MDS reconnect requires a Lost previous session")
	}
	if status.GlobalID != m.globalID {
		return nil, nil, ErrMDSIdentityChanged
	}
	if status.Target.FilesystemID != m.target.FilesystemID || status.Target.Rank != m.target.Rank {
		return nil, nil, ErrMDSTargetChanged
	}
	if previous.conn.address == m.address {
		return nil, nil, ErrMDSSessionActive
	}
	features := status.Open.SupportedFeatures
	if !mds.FeatureBitsetHas(features, mds.FeatureMultiReconnect) {
		return nil, nil, ErrMDSRequestFeatures
	}
	resends, err := resubmissions(previous, state.Resubmit)
	if err != nil {
		return nil, nil, err
	}
	if len(resends) != 0 {
		if err := requestFeatures(features); err != nil {
			return nil, nil, err
		}
	}
	messages, err := reconnectMessages(state, c.options.MaxFrameSize)
	if err != nil {
		return nil, nil, err
	}
	// Every resubmitted call holds a MaxInFlight slot like a new request.
	acquired := 0
	for range resends {
		select {
		case c.calls <- struct{}{}:
			acquired++
			continue
		case <-ctx.Done():
			err = ctx.Err()
		case <-c.ctx.Done():
			err = ErrClosed
		}
		for range acquired {
			<-c.calls
		}
		return nil, nil, err
	}
	var calls []*MDSRequestCall
	prepare := func(session *MDSSession) error {
		for i, r := range resends {
			call, err := session.submit(ctx, r.request, features, r.transaction)
			if err != nil {
				for range len(resends) - i - 1 {
					<-c.calls
				}
				return err
			}
			session.mu.Lock()
			session.queued = true
			session.mu.Unlock()
			calls = append(calls, call)
		}
		return nil
	}
	session, err := m.beginSession(ctx, options, messages, status.Generation, prepare)
	if err != nil {
		var unknown *OutcomeUnknownError
		if session != nil && errors.As(err, &unknown) {
			return session, calls, err
		}
		return nil, nil, err
	}
	return session, calls, nil
}

type resubmission struct {
	request     MDSRequest
	transaction uint64
}

// resubmissions validates the caller's choice and derives each resend.
func resubmissions(previous *MDSSession, chosen []*MDSRequestCall) ([]resubmission, error) {
	var out []resubmission
	seen := make(map[uint64]bool, len(chosen))
	for _, call := range chosen {
		if call == nil || call.session != previous {
			return nil, errors.New("ceph: resubmitted call does not belong to the Lost session")
		}
		previous.mu.Lock()
		first, safe, resolved, cause := call.first, call.safe, call.resolved, call.err
		request := call.request
		previous.mu.Unlock()
		var unknown *OutcomeUnknownError
		if !resolved || safe != nil || !errors.As(cause, &unknown) || seen[call.transaction] {
			return nil, errors.New("ceph: only distinct calls with an unknown outcome can be resubmitted")
		}
		seen[call.transaction] = true
		if request.Retry == ^uint32(0) {
			return nil, errors.New("ceph: MDS request retry count exhausted")
		}
		request.Retry++
		request.Releases = nil
		if first != nil && !first.Safe {
			request.Flags |= mds.RequestFlagReplay
			if first.IsTarget {
				if trace, err := mds.DecodeTrace(first.Trace, first.IsDentry, true); err == nil && trace.Target != nil {
					request.ino = trace.Target.Ino
				}
			}
		}
		out = append(out, resubmission{request: request, transaction: call.transaction})
	}
	return out, nil
}

// reconnectMessages splits the state into frame-sized MClientReconnect
// messages: caps in inode order, all realms in the last message, "more" set
// on every message but the last. Sizes follow the v5 data layout.
func reconnectMessages(state MDSReconnectState, limit uint32) ([]msgr.MessageData, error) {
	caps := slices.Clone(state.Caps)
	slices.SortFunc(caps, func(a, b MDSCapReconnect) int { return cmp.Compare(a.Ino, b.Ino) })
	const envelope = 4 + 4 + 1 + 41 // cap count, realm count, more flag, message header
	capSize := func(c MDSCapReconnect) uint64 { return 8 + 6 + 4 + uint64(len(c.Path)) + 36 + uint64(len(c.Flock)) + 8 }
	realms := uint64(len(state.Realms)) * (6 + 24)
	if envelope+realms > uint64(limit) {
		return nil, fmt.Errorf("ceph: MDS reconnect realms exceed the frame limit: %w", ErrLimitExceeded)
	}
	var messages []msgr.MessageData
	emit := func(chunk []MDSCapReconnect, realms []MDSRealmReconnect, more bool) error {
		message, err := mds.EncodeReconnect(chunk, realms, more)
		if err != nil {
			return fmt.Errorf("ceph: MDS reconnect state: %w", errors.Join(ErrLimitExceeded, err))
		}
		messages = append(messages, message)
		return nil
	}
	start, used := 0, uint64(envelope)
	for i, c := range caps {
		size := capSize(c)
		if envelope+size > uint64(limit) {
			return nil, fmt.Errorf("ceph: MDS reconnect capability exceeds the frame limit: %w", ErrLimitExceeded)
		}
		if used+size > uint64(limit) {
			if err := emit(caps[start:i], nil, true); err != nil {
				return nil, err
			}
			start, used = i, envelope
		}
		used += size
	}
	if used+realms > uint64(limit) {
		if err := emit(caps[start:], nil, true); err != nil {
			return nil, err
		}
		start = len(caps)
	}
	if err := emit(caps[start:], state.Realms, false); err != nil {
		return nil, err
	}
	return messages, nil
}

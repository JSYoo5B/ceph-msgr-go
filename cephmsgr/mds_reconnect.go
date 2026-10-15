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
func (m *MDSConnection) ReconnectSession(ctx context.Context, previous *MDSSession, state MDSReconnectState, options MDSSessionOptions) (*MDSSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c := m.client
	if !c.options.EnableCephFS {
		return nil, ErrCephFSDisabled
	}
	if err := validateSessionOptions(&options); err != nil {
		return nil, err
	}
	if previous == nil || previous.conn.client != c || previous.conn == m {
		return nil, errors.New("ceph: MDS reconnect requires a previous session of this client on another connection")
	}
	status := previous.Status()
	if status.State != MDSSessionLost {
		return nil, errors.New("ceph: MDS reconnect requires a Lost previous session")
	}
	if status.GlobalID != m.globalID {
		return nil, ErrMDSIdentityChanged
	}
	if status.Target.FilesystemID != m.target.FilesystemID || status.Target.Rank != m.target.Rank {
		return nil, ErrMDSTargetChanged
	}
	if previous.conn.address == m.address {
		return nil, ErrMDSSessionActive
	}
	if !mds.FeatureBitsetHas(status.Open.SupportedFeatures, mds.FeatureMultiReconnect) {
		return nil, ErrMDSRequestFeatures
	}
	messages, err := reconnectMessages(state, c.options.MaxFrameSize)
	if err != nil {
		return nil, err
	}
	return m.beginSession(ctx, options, messages, status.Generation)
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

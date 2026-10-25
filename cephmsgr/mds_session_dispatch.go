package cephmsgr

import (
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

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

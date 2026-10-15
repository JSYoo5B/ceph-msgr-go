package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// Capability, lease, snapshot-realm and quota wire messages. The caller owns
// every capability and cache decision: the library delivers MDS pushes as
// session events and sends client messages only when asked.
type (
	MDSCaps    = mds.Caps
	MDSCapPeer = mds.CapPeer
	MDSCapItem = mds.CapItem
	MDSLease   = mds.Lease
	MDSSnap    = mds.Snap
	MDSQuota   = mds.Quota
)

const (
	MDSCapOpGrant        = mds.CapOpGrant
	MDSCapOpRevoke       = mds.CapOpRevoke
	MDSCapOpTrunc        = mds.CapOpTrunc
	MDSCapOpExport       = mds.CapOpExport
	MDSCapOpImport       = mds.CapOpImport
	MDSCapOpUpdate       = mds.CapOpUpdate
	MDSCapOpDrop         = mds.CapOpDrop
	MDSCapOpFlush        = mds.CapOpFlush
	MDSCapOpFlushAck     = mds.CapOpFlushAck
	MDSCapOpFlushSnap    = mds.CapOpFlushSnap
	MDSCapOpFlushSnapAck = mds.CapOpFlushSnapAck
	MDSCapOpRelease      = mds.CapOpRelease
	MDSCapOpRenew        = mds.CapOpRenew

	MDSLeaseRevoke  = mds.LeaseRevoke
	MDSLeaseRelease = mds.LeaseRelease
	MDSLeaseRenew   = mds.LeaseRenew

	MDSSnapOpUpdate  = mds.SnapOpUpdate
	MDSSnapOpCreate  = mds.SnapOpCreate
	MDSSnapOpDestroy = mds.SnapOpDestroy
	MDSSnapOpSplit   = mds.SnapOpSplit
)

// sendable reports whether the session can carry a client CephFS message.
func (s *MDSSession) sendable() error {
	if !s.conn.client.options.EnableCephFS {
		return ErrCephFSDisabled
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.terminal() {
		return s.err
	}
	if s.state == MDSSessionOpening {
		return errors.New("ceph: MDS session is not open")
	}
	return nil
}

func (s *MDSSession) sendMessage(ctx context.Context, message msgr.MessageData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.sendable(); err != nil {
		return err
	}
	if uint64(len(message.Front))+uint64(len(message.Middle))+41 > uint64(s.conn.client.options.MaxFrameSize) {
		return fmt.Errorf("ceph: MDS message exceeds frame limit: %w", ErrLimitExceeded)
	}
	return s.conn.session.SendControl(ctx, message)
}

// SendCaps sends one client MClientCaps (UPDATE, DROP, FLUSH, FLUSHSNAP,
// RELEASE or RENEW) in the current v12 layout. flushTID is the caller's flush
// transaction for FLUSH/FLUSHSNAP, echoed in the MDS FLUSH_ACK event. ctx
// governs write admission only; success is not proof of MDS processing.
func (s *MDSSession) SendCaps(ctx context.Context, caps MDSCaps, flushTID uint64) error {
	switch caps.Op {
	case mds.CapOpUpdate, mds.CapOpDrop, mds.CapOpFlush, mds.CapOpFlushSnap, mds.CapOpRelease, mds.CapOpRenew:
	default:
		return fmt.Errorf("ceph: MDS capability operation %d is not a client operation", caps.Op)
	}
	caps.SnapTrace, caps.Xattrs, caps.Flock = slices.Clone(caps.SnapTrace), slices.Clone(caps.Xattrs), slices.Clone(caps.Flock)
	caps.InlineData, caps.FscryptAuth, caps.FscryptFile = slices.Clone(caps.InlineData), slices.Clone(caps.FscryptAuth), slices.Clone(caps.FscryptFile)
	return s.sendMessage(ctx, mds.EncodeCaps(caps, flushTID))
}

// ReleaseCaps sends one MClientCapRelease batch. osdEpochBarrier is the
// caller's OSD map barrier for the released capabilities.
func (s *MDSSession) ReleaseCaps(ctx context.Context, items []MDSCapItem, osdEpochBarrier uint32) error {
	message, err := mds.EncodeCapRelease(slices.Clone(items), osdEpochBarrier)
	if err != nil {
		return fmt.Errorf("ceph: MDS capability release: %w", ErrLimitExceeded)
	}
	return s.sendMessage(ctx, message)
}

// SendLease sends a client MClientLease (RELEASE or RENEW).
func (s *MDSSession) SendLease(ctx context.Context, lease MDSLease) error {
	if lease.Action != mds.LeaseRelease && lease.Action != mds.LeaseRenew {
		return fmt.Errorf("ceph: MDS lease action %d is not a client action", lease.Action)
	}
	return s.sendMessage(ctx, mds.EncodeLease(lease))
}

// decodePush decodes one counted MDS push into an event. Only server-side
// operations are accepted.
func decodePush(message msgr.MessageData, limit uint32) (MDSSessionEvent, error) {
	event := MDSSessionEvent{Type: message.Type, Transaction: message.Transaction}
	var err error
	switch message.Type {
	case mds.ClientCapsMessage:
		var caps mds.Caps
		caps, err = mds.DecodeCaps(message, limit)
		switch caps.Op {
		case mds.CapOpGrant, mds.CapOpRevoke, mds.CapOpTrunc, mds.CapOpExport, mds.CapOpImport, mds.CapOpFlushAck, mds.CapOpFlushSnapAck:
		default:
			if err == nil {
				err = fmt.Errorf("client capability operation %d from MDS", caps.Op)
			}
		}
		event.Caps = &caps
	case mds.ClientLeaseMessage:
		var lease mds.Lease
		lease, err = mds.DecodeLease(message, limit)
		if err == nil && lease.Action != mds.LeaseRevoke && lease.Action != mds.LeaseRenew {
			err = fmt.Errorf("client lease action %d from MDS", lease.Action)
		}
		event.Lease = &lease
	case mds.ClientSnapMessage:
		var snap mds.Snap
		snap, err = mds.DecodeSnap(message, limit)
		event.Snap = &snap
	case mds.ClientQuotaMessage:
		var quota mds.Quota
		quota, err = mds.DecodeQuota(message, limit)
		event.Quota = &quota
	default:
		return event, fmt.Errorf("%w: unsupported MDS message %d", msgr.ErrFeatures, message.Type)
	}
	if err != nil {
		return MDSSessionEvent{}, fmt.Errorf("%w: invalid MDS push: %w", msgr.ErrFrame, err)
	}
	return event, nil
}

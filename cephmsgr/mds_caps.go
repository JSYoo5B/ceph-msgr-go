package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
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
// transaction for a dirty UPDATE, FLUSH or FLUSHSNAP, echoed in the MDS
// FLUSH_ACK or FLUSHSNAP_ACK event. ctx
// governs write admission only; success is not proof of MDS processing.
func (s *MDSSession) SendCaps(ctx context.Context, caps MDSCaps, flushTID uint64) error {
	message, err := encodeMDSCaps(caps, flushTID, s.conn.client.options.MaxFrameSize)
	if err != nil {
		return err
	}
	return s.sendMessage(ctx, message)
}

// mdsCapsFrameBytes includes the message header and every variable-length
// field before allocating the encoded front or cloning the xattr middle.
// The encoder supplies the fixed v12 size rather than maintaining a
// second copy of that layout here.
func mdsCapsFrameBytes(caps MDSCaps) uint64 {
	base := mds.EncodeCaps(MDSCaps{}, 0)
	return uint64(41+len(base.Front)) + uint64(len(caps.SnapTrace)) + uint64(len(caps.Xattrs)) +
		uint64(len(caps.Flock)) + uint64(len(caps.InlineData)) + uint64(len(caps.Layout.PoolNamespace)) +
		uint64(len(caps.FscryptAuth)) + uint64(len(caps.FscryptFile))
}

func encodeMDSCaps(caps MDSCaps, flushTID uint64, limit uint32) (msgr.MessageData, error) {
	switch caps.Op {
	case mds.CapOpUpdate, mds.CapOpDrop, mds.CapOpFlush, mds.CapOpFlushSnap, mds.CapOpRelease, mds.CapOpRenew:
	default:
		return msgr.MessageData{}, fmt.Errorf("ceph: MDS capability operation %d is not a client operation", caps.Op)
	}
	if mdsCapsFrameBytes(caps) > uint64(limit) {
		return msgr.MessageData{}, fmt.Errorf("ceph: MDS capability message exceeds frame limit: %w", ErrLimitExceeded)
	}
	// EncodeCaps copies front fields and clones the middle; queued messages
	// therefore retain no caller-owned byte slices.
	return mds.EncodeCaps(caps, flushTID), nil
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

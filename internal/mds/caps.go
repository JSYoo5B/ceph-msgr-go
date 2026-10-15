package mds

import (
	"bytes"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientCaps.h,
// messages/MClientCapRelease.h, messages/MClientLease.h,
// messages/MClientSnap.h, messages/MClientQuota.h and include/ceph_fs.h
// (ceph_mds_caps_head, bodies, cap peer/item, lease, snap head).
// LGPL-2.1 sources; no code copied.

const (
	ClientCapsMessage       uint16 = 0x310
	ClientLeaseMessage      uint16 = 0x311
	ClientSnapMessage       uint16 = 0x312
	ClientCapReleaseMessage uint16 = 0x313
	ClientQuotaMessage      uint16 = 0x314

	// capsVersion is MClientCaps HEAD_VERSION, emitted when the connection
	// negotiated FLOCK and EXPORT_PEER (older versions are not decoded).
	capsVersion         = 12
	capsHeadBytes       = 92
	capsBodyBytes       = 84
	capPeerBytes        = 21
	capItemBytes        = 24
	leaseHeadBytes      = 35
	snapHeadBytes       = 24
	MaxCapReleaseItems  = 1 << 16
	MaxSnapSplitEntries = 1 << 20
)

// Capability operations (CEPH_CAP_OP_*).
const (
	CapOpGrant uint32 = iota
	CapOpRevoke
	CapOpTrunc
	CapOpExport
	CapOpImport
	CapOpUpdate
	CapOpDrop
	CapOpFlush
	CapOpFlushAck
	CapOpFlushSnap
	CapOpFlushSnapAck
	CapOpRelease
	CapOpRenew
)

// Lease actions (CEPH_MDS_LEASE_*) and snapshot operations (CEPH_SNAP_OP_*).
const (
	LeaseRevoke  uint8 = 1
	LeaseRelease uint8 = 2
	LeaseRenew   uint8 = 3

	SnapOpUpdate  uint32 = 0
	SnapOpCreate  uint32 = 1
	SnapOpDestroy uint32 = 2
	SnapOpSplit   uint32 = 3
)

// CapPeer is ceph_mds_cap_peer (EXPORT target or IMPORT source).
type CapPeer struct {
	CapID          uint64
	IssueSeq, MSeq uint32
	MDS            int32
	Flags          uint8
}

// Caps is one MClientCaps message. Transaction (the Messenger TID) carries a
// client flush TID for FLUSH/FLUSH_ACK. Xattrs travels in the middle segment;
// XattrLen/SnapTraceLen are derived. For EXPORT only Peer is meaningful in the
// body, which is zero-padded on the wire.
type Caps struct {
	Op                          uint32
	Ino, Realm, CapID           uint64
	Seq, IssueSeq               uint32
	Caps, Wanted, Dirty         uint32
	MigrateSeq                  uint32
	SnapFollows                 uint64
	UID, GID, Mode, NLink       uint32
	XattrVersion                uint64
	Size, MaxSize, TruncateSize uint64
	TruncateSeq                 uint32
	MTime, ATime, CTime         Time
	Layout                      FileLayout
	TimeWarpSeq                 uint32
	Peer                        CapPeer
	SnapTrace                   []byte
	Xattrs                      []byte
	Flock                       []byte
	InlineVersion               uint64
	InlineData                  []byte
	OSDEpochBarrier             uint32
	OldestFlushTID              uint64
	CallerUID, CallerGID        uint32
	BTime                       Time
	ChangeAttr                  uint64
	Flags                       uint32
	Files, Subdirs              int64
	FscryptAuth, FscryptFile    []byte
}

func encodeTime(e *wire.Encoder, t Time) { e.U32(t.Seconds); e.U32(t.Nanoseconds) }

func encodeLegacyLayout(e *wire.Encoder, l FileLayout) {
	for _, v := range []uint32{l.StripeUnit, l.StripeCount, l.ObjectSize, l.CASHash, l.ObjectStripeUnit, l.Unused, uint32(l.Pool)} {
		e.U32(v)
	}
}

func encodePeer(e *wire.Encoder, p CapPeer) {
	e.U64(p.CapID)
	e.U32(p.IssueSeq)
	e.U32(p.MSeq)
	e.U32(uint32(p.MDS))
	e.U8(p.Flags)
}

func decodePeer(d *wire.Decoder) CapPeer {
	return CapPeer{CapID: d.U64(), IssueSeq: d.U32(), MSeq: d.U32(), MDS: int32(d.U32()), Flags: d.U8()}
}

// EncodeCaps builds the v12 MClientCaps a FLOCK/EXPORT_PEER/MDS_INLINE_DATA
// connection carries. tid is the client flush TID (zero when none).
func EncodeCaps(c Caps, tid uint64) msgr.MessageData {
	e := wire.Encoder{}
	e.U32(c.Op)
	e.U64(c.Ino)
	e.U64(c.Realm)
	e.U64(c.CapID)
	e.U32(c.Seq)
	e.U32(c.IssueSeq)
	e.U32(c.Caps)
	e.U32(c.Wanted)
	e.U32(c.Dirty)
	e.U32(c.MigrateSeq)
	e.U64(c.SnapFollows)
	e.U32(uint32(len(c.SnapTrace)))
	e.U32(c.UID)
	e.U32(c.GID)
	e.U32(c.Mode)
	e.U32(c.NLink)
	e.U32(uint32(len(c.Xattrs)))
	e.U64(c.XattrVersion)
	if c.Op == CapOpExport {
		encodePeer(&e, c.Peer)
		e.Raw(make([]byte, capsBodyBytes-capPeerBytes))
	} else {
		e.U64(c.Size)
		e.U64(c.MaxSize)
		e.U64(c.TruncateSize)
		e.U32(c.TruncateSeq)
		encodeTime(&e, c.MTime)
		encodeTime(&e, c.ATime)
		encodeTime(&e, c.CTime)
		encodeLegacyLayout(&e, c.Layout)
		e.U32(c.TimeWarpSeq)
	}
	e.Raw(c.SnapTrace)
	e.Bytes(c.Flock)
	if c.Op == CapOpImport {
		encodePeer(&e, c.Peer)
	}
	e.U64(c.InlineVersion)
	e.Bytes(c.InlineData)
	e.U32(c.OSDEpochBarrier)
	e.U64(c.OldestFlushTID)
	e.U32(c.CallerUID)
	e.U32(c.CallerGID)
	e.String(c.Layout.PoolNamespace)
	encodeTime(&e, c.BTime)
	e.U64(c.ChangeAttr)
	e.U32(c.Flags)
	e.U64(uint64(c.Files))
	e.U64(uint64(c.Subdirs))
	e.Bytes(c.FscryptAuth)
	e.Bytes(c.FscryptFile)
	return msgr.MessageData{Type: ClientCapsMessage, Priority: 127, Version: capsVersion, CompatVersion: 1, Transaction: tid, Front: e.Data, Middle: bytes.Clone(c.Xattrs)}
}

// DecodeCaps decodes a v12 MClientCaps; the xattr blob must exactly fill the
// middle segment. Older versions (connections without FLOCK/EXPORT_PEER) are
// refused rather than silently missing fields such as the OSD epoch barrier.
func DecodeCaps(m msgr.MessageData, limit uint32) (Caps, error) {
	var c Caps
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return c, wire.ErrLimit
	}
	if m.Type != ClientCapsMessage || len(m.Data) != 0 {
		return c, msgr.ErrFrame
	}
	if m.Version != capsVersion || m.CompatVersion != 1 {
		return c, wire.ErrVersion
	}
	front := bytes.Clone(m.Front)
	d := wire.NewDecoderLimit(front, uint32(len(front)))
	c.Op, c.Ino, c.Realm, c.CapID = d.U32(), d.U64(), d.U64(), d.U64()
	c.Seq, c.IssueSeq, c.Caps, c.Wanted, c.Dirty, c.MigrateSeq = d.U32(), d.U32(), d.U32(), d.U32(), d.U32(), d.U32()
	c.SnapFollows = d.U64()
	snapLen := d.U32()
	c.UID, c.GID, c.Mode, c.NLink = d.U32(), d.U32(), d.U32(), d.U32()
	xattrLen := d.U32()
	c.XattrVersion = d.U64()
	if c.Op == CapOpExport {
		c.Peer = decodePeer(d)
		if pad := d.Raw(capsBodyBytes - capPeerBytes); pad != nil && !bytes.Equal(pad, make([]byte, len(pad))) {
			d.Fail(ErrRequest)
		}
	} else {
		c.Size, c.MaxSize, c.TruncateSize, c.TruncateSeq = d.U64(), d.U64(), d.U64(), d.U32()
		c.MTime, c.ATime, c.CTime = decodeTime(d), decodeTime(d), decodeTime(d)
		c.Layout = FileLayout{StripeUnit: d.U32(), StripeCount: d.U32(), ObjectSize: d.U32(), CASHash: d.U32(), ObjectStripeUnit: d.U32(), Unused: d.U32(), Pool: int32(d.U32())}
		c.TimeWarpSeq = d.U32()
	}
	c.SnapTrace = cloneOrNil(d.Raw(snapLen))
	if uint64(xattrLen) != uint64(len(m.Middle)) {
		d.Fail(msgr.ErrFrame)
	}
	c.Xattrs = cloneOrNil(m.Middle)
	c.Flock = cloneOrNil(d.Bytes())
	if c.Op == CapOpImport {
		c.Peer = decodePeer(d)
	}
	c.InlineVersion, c.InlineData = d.U64(), cloneOrNil(d.Bytes())
	c.OSDEpochBarrier, c.OldestFlushTID, c.CallerUID, c.CallerGID = d.U32(), d.U64(), d.U32(), d.U32()
	c.Layout.PoolNamespace = d.String()
	c.BTime, c.ChangeAttr, c.Flags = decodeTime(d), d.U64(), d.U32()
	c.Files, c.Subdirs = int64(d.U64()), int64(d.U64())
	c.FscryptAuth, c.FscryptFile = cloneOrNil(d.Bytes()), cloneOrNil(d.Bytes())
	if err := d.Done(); err != nil {
		return Caps{}, err
	}
	return c, nil
}

// CapItem is ceph_mds_cap_item, one released capability.
type CapItem struct {
	Ino, CapID           uint64
	MigrateSeq, IssueSeq uint32
}

// EncodeCapRelease builds MClientCapRelease v2/compat1.
func EncodeCapRelease(items []CapItem, osdEpochBarrier uint32) (msgr.MessageData, error) {
	if len(items) > MaxCapReleaseItems {
		return msgr.MessageData{}, wire.ErrLimit
	}
	e := wire.Encoder{}
	e.U32(uint32(len(items)))
	for _, item := range items {
		e.U64(item.Ino)
		e.U64(item.CapID)
		e.U32(item.MigrateSeq)
		e.U32(item.IssueSeq)
	}
	e.U32(osdEpochBarrier)
	return msgr.MessageData{Type: ClientCapReleaseMessage, Priority: 127, Version: 2, CompatVersion: 1, Front: e.Data}, nil
}

// DecodeCapRelease decodes the exact v2 layout (vector comparison and peers).
func DecodeCapRelease(m msgr.MessageData, limit uint32) ([]CapItem, uint32, error) {
	raw, err := plainFront(m, ClientCapReleaseMessage, 2, 1, limit)
	if err != nil {
		return nil, 0, err
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	n := d.U32()
	if n > MaxCapReleaseItems || uint64(n)*capItemBytes > uint64(d.Remaining()) {
		d.Fail(wire.ErrLimit)
		n = 0
	}
	items := make([]CapItem, n)
	for i := range items {
		items[i] = CapItem{Ino: d.U64(), CapID: d.U64(), MigrateSeq: d.U32(), IssueSeq: d.U32()}
	}
	barrier := d.U32()
	if err := d.Done(); err != nil {
		return nil, 0, err
	}
	return items, barrier, nil
}

// Lease is MClientLease: ceph_mds_lease and a dentry name.
type Lease struct {
	Action      uint8
	Mask        uint16
	Ino         uint64
	First, Last uint64
	Seq         uint32
	DurationMS  uint32
	DentryName  string
}

// EncodeLease builds MClientLease (header 1/1).
func EncodeLease(l Lease) msgr.MessageData {
	e := wire.Encoder{}
	e.U8(l.Action)
	e.U16(l.Mask)
	e.U64(l.Ino)
	e.U64(l.First)
	e.U64(l.Last)
	e.U32(l.Seq)
	e.U32(l.DurationMS)
	e.String(l.DentryName)
	return msgr.MessageData{Type: ClientLeaseMessage, Priority: 127, Version: 1, CompatVersion: 1, Front: e.Data}
}

func DecodeLease(m msgr.MessageData, limit uint32) (Lease, error) {
	raw, err := plainFront(m, ClientLeaseMessage, 1, 1, limit)
	if err != nil {
		return Lease{}, err
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	l := Lease{Action: d.U8(), Mask: d.U16(), Ino: d.U64(), First: d.U64(), Last: d.U64(), Seq: d.U32(), DurationMS: d.U32(), DentryName: d.String()}
	if err := d.Done(); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// Snap is MClientSnap: an operation, optional split realm with the inodes and
// child realms moving into it, and a raw snap trace blob.
type Snap struct {
	Op                     uint32
	Split                  uint64
	SplitInos, SplitRealms []uint64
	Trace                  []byte
}

func DecodeSnap(m msgr.MessageData, limit uint32) (Snap, error) {
	raw, err := plainFront(m, ClientSnapMessage, 1, 1, limit)
	if err != nil {
		return Snap{}, err
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	s := Snap{Op: d.U32(), Split: d.U64()}
	inos, realms, trace := d.U32(), d.U32(), d.U32()
	if inos > MaxSnapSplitEntries || realms > MaxSnapSplitEntries || (uint64(inos)+uint64(realms))*8+uint64(trace) > uint64(d.Remaining()) {
		d.Fail(wire.ErrLimit)
		inos, realms, trace = 0, 0, 0
	}
	s.SplitInos, s.SplitRealms = readU64s(d, inos), readU64s(d, realms)
	s.Trace = cloneOrNil(d.Raw(trace))
	if err := d.Done(); err != nil {
		return Snap{}, err
	}
	return s, nil
}

// Quota is MClientQuota: recursive statistics and the quota of one inode.
type Quota struct {
	Ino                      uint64
	RCTime                   Time
	RBytes, RFiles, RSubdirs int64
	MaxBytes, MaxFiles       int64
}

func DecodeQuota(m msgr.MessageData, limit uint32) (Quota, error) {
	raw, err := plainFront(m, ClientQuotaMessage, 1, 1, limit)
	if err != nil {
		return Quota{}, err
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	q := Quota{Ino: d.U64(), RCTime: decodeTime(d), RBytes: int64(d.U64()), RFiles: int64(d.U64()), RSubdirs: int64(d.U64())}
	b := currentStruct(d, 1, 1)
	q.MaxBytes, q.MaxFiles = int64(b.U64()), int64(b.U64())
	d.Fail(b.Done())
	if err := d.Done(); err != nil {
		return Quota{}, err
	}
	return q, nil
}

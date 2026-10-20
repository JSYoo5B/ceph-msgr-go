package mds

import (
	"bytes"
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientRequest.h,
// messages/MClientRequestForward.h, messages/MClientReply.h,
// include/ceph_fs.h (ceph_mds_request_head, request args, releases, reply
// head) and include/filepath.h. LGPL-2.1 sources; no code copied.

const (
	ClientRequestMessage        uint16 = 24
	ClientRequestForwardMessage uint16 = 25
	ClientReplyMessage          uint16 = 26

	// RequestHeadVersion is CEPH_MDS_REQUEST_HEAD_VERSION; it requires the
	// MDS to advertise CEPHFS_FEATURE_32BITS_RETRY_FWD and HAS_OWNER_UIDGID.
	RequestHeadVersion = 3
	// RequestArgsBytes is sizeof(union ceph_mds_request_args) (packed; the
	// setattr member is the largest).
	RequestArgsBytes = 56
	// RequestHeadBytes is sizeof(struct ceph_mds_request_head), the
	// struct_len a v3 head reports.
	RequestHeadBytes = 118
	// requestReleaseBytes is the packed ceph_mds_request_release prefix.
	requestReleaseBytes = 44

	MaxRequestReleases uint32 = 1024
	MaxRequestGIDs     uint32 = 4096
)

// CephFS client feature bits from mds/cephfs_features.h used by this codec.
const (
	FeatureReplyEncoding           = 9
	Feature32BitsRetryFwd          = 18
	FeatureNewSnaprealmInfo        = 19
	FeatureHasOwnerUIDGID          = 20
	RequestFlagReplay              = 1
	RequestFlagWantDentry          = 2
	RequestFlagAsync               = 4
	ReaddirReplyBitflags           = 1
	ReaddirFragEnd                 = 1
	ReaddirFragComplete            = 1 << 8
	ReaddirHashOrder               = 1 << 9
	ReaddirOffsetHash              = 1 << 10
	OpWriteBit                     = 0x001000
	CapFlagAuth                    = 1
	CapFlagRelease                 = 2
	DirAuthParent                  = -1
	NoSnap                  uint64 = 0xfffffffffffffffe
)

// Metadata operation codes (CEPH_MDS_OP_*).
const (
	OpLookup          uint32 = 0x00100
	OpGetattr         uint32 = 0x00101
	OpLookupHash      uint32 = 0x00102
	OpLookupParent    uint32 = 0x00103
	OpLookupIno       uint32 = 0x00104
	OpLookupName      uint32 = 0x00105
	OpGetVxattr       uint32 = 0x00106
	OpSetxattr        uint32 = 0x01105
	OpRmxattr         uint32 = 0x01106
	OpSetlayout       uint32 = 0x01107
	OpSetattr         uint32 = 0x01108
	OpSetFileLock     uint32 = 0x01109
	OpGetFileLock     uint32 = 0x00110
	OpSetDirLayout    uint32 = 0x0110a
	OpMknod           uint32 = 0x01201
	OpLink            uint32 = 0x01202
	OpUnlink          uint32 = 0x01203
	OpRename          uint32 = 0x01204
	OpMkdir           uint32 = 0x01220
	OpRmdir           uint32 = 0x01221
	OpSymlink         uint32 = 0x01222
	OpCreate          uint32 = 0x01301
	OpOpen            uint32 = 0x00302
	OpReaddir         uint32 = 0x00305
	OpLookupSnap      uint32 = 0x00400
	OpMksnap          uint32 = 0x01400
	OpRmsnap          uint32 = 0x01401
	OpLssnap          uint32 = 0x00402
	OpRenamesnap      uint32 = 0x01403
	OpReaddirSnapdiff uint32 = 0x01404
	OpFileBlockdiff   uint32 = 0x01405
)

var ErrRequest = errors.New("ceph mds: invalid client request or reply wire data")

// FilePath is the filepath encoding: a base inode (0 for a purely relative
// path) and a relative path string.
type FilePath struct {
	Ino  uint64
	Path string
}

// RequestRelease is one ceph_mds_request_release record followed by its
// dentry name. The caller owns the capability/lease state it describes.
type RequestRelease struct {
	Ino, CapID                                   uint64
	Caps, Wanted, Seq, IssueSeq, MSeq, DentrySeq uint32
	DentryName                                   string
}

// ClientRequest is one MClientRequest (header v6/compat1, request head v3).
// Args is the raw packed ceph_mds_request_args union; op-specific builders
// fill it. Retry and Forward are the 32-bit attempt counts; their low bytes
// are also written to the legacy u8 fields as the native encoder does.
type ClientRequest struct {
	OldestClientTID      uint64
	MDSMapEpoch          uint32
	Flags                uint32
	Retry, Forward       uint32
	Op                   uint32
	CallerUID, CallerGID uint32
	Ino                  uint64
	Args                 [RequestArgsBytes]byte
	OwnerUID, OwnerGID   uint32
	Path, Path2          FilePath
	Releases             []RequestRelease
	Stamp                Time
	GIDs                 []uint64
	AlternateName        []byte
	FscryptAuth          []byte
	FscryptFile          []byte
	// Data is the request's data segment: a setxattr value, or the encoded
	// xattr map of a create. It is not part of the front.
	Data []byte
}

// EncodeClientRequest builds the current MClientRequest front. tid is placed
// in the Messenger header by the caller. The MDS's advertised CephFS features
// must include 32BITS_RETRY_FWD and HAS_OWNER_UIDGID: older head versions are
// not produced.
func EncodeClientRequest(r ClientRequest, mdsFeatures []byte) (msgr.MessageData, error) {
	if !FeatureBitsetHas(mdsFeatures, Feature32BitsRetryFwd) || !FeatureBitsetHas(mdsFeatures, FeatureHasOwnerUIDGID) {
		return msgr.MessageData{}, ErrRequest
	}
	if uint32(len(r.Releases)) > MaxRequestReleases || uint32(len(r.GIDs)) > MaxRequestGIDs || len(r.Releases) > 0xffff {
		return msgr.MessageData{}, wire.ErrLimit
	}
	e := wire.Encoder{}
	e.U16(RequestHeadVersion)
	e.U64(r.OldestClientTID)
	e.U32(r.MDSMapEpoch)
	e.U32(r.Flags)
	e.U8(uint8(r.Retry))
	e.U8(uint8(r.Forward))
	e.U16(uint16(len(r.Releases)))
	e.U32(r.Op)
	e.U32(r.CallerUID)
	e.U32(r.CallerGID)
	e.U64(r.Ino)
	e.Raw(r.Args[:])
	e.U32(r.Retry)
	e.U32(r.Forward)
	e.U32(RequestHeadBytes)
	e.U32(r.OwnerUID)
	e.U32(r.OwnerGID)
	encodeFilePath(&e, r.Path)
	encodeFilePath(&e, r.Path2)
	for _, rel := range r.Releases {
		e.U64(rel.Ino)
		e.U64(rel.CapID)
		e.U32(rel.Caps)
		e.U32(rel.Wanted)
		e.U32(rel.Seq)
		e.U32(rel.IssueSeq)
		e.U32(rel.MSeq)
		e.U32(rel.DentrySeq)
		e.U32(uint32(len(rel.DentryName)))
		e.Raw([]byte(rel.DentryName))
	}
	e.U32(r.Stamp.Seconds)
	e.U32(r.Stamp.Nanoseconds)
	e.U32(uint32(len(r.GIDs)))
	for _, gid := range r.GIDs {
		e.U64(gid)
	}
	e.Bytes(r.AlternateName)
	e.Bytes(r.FscryptAuth)
	e.Bytes(r.FscryptFile)
	return msgr.MessageData{Type: ClientRequestMessage, Priority: 127, Version: 6, CompatVersion: 1, Front: e.Data, Data: bytes.Clone(r.Data)}, nil
}

func encodeFilePath(e *wire.Encoder, p FilePath) {
	e.U8(1)
	e.U64(p.Ino)
	e.String(p.Path)
}

// DecodeClientRequest decodes the exact current layout produced by
// EncodeClientRequest. It exists for independent vector comparison and
// synthetic peers; the MDS-side legacy head versions are not accepted.
func DecodeClientRequest(m msgr.MessageData, limit uint32) (ClientRequest, error) {
	var r ClientRequest
	data := m.Data
	m.Data = nil
	raw, err := plainFront(m, ClientRequestMessage, 6, 1, limit)
	if err != nil {
		return r, err
	}
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(raw))+uint64(len(data))+41 > uint64(limit) {
		return r, wire.ErrLimit
	}
	r.Data = cloneOrNil(data)
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	if d.U16() != RequestHeadVersion {
		d.Fail(wire.ErrVersion)
	}
	r.OldestClientTID, r.MDSMapEpoch, r.Flags = d.U64(), d.U32(), d.U32()
	legacyRetry, legacyForward := d.U8(), d.U8()
	releases := uint32(d.U16())
	r.Op, r.CallerUID, r.CallerGID, r.Ino = d.U32(), d.U32(), d.U32(), d.U64()
	copy(r.Args[:], d.Raw(RequestArgsBytes))
	r.Retry, r.Forward = d.U32(), d.U32()
	if d.U32() != RequestHeadBytes || legacyRetry != uint8(r.Retry) || legacyForward != uint8(r.Forward) {
		d.Fail(ErrRequest)
	}
	r.OwnerUID, r.OwnerGID = d.U32(), d.U32()
	r.Path, r.Path2 = decodeFilePath(d), decodeFilePath(d)
	if releases > MaxRequestReleases || uint64(releases)*requestReleaseBytes > uint64(d.Remaining()) {
		d.Fail(wire.ErrLimit)
		releases = 0
	}
	if releases != 0 {
		r.Releases = make([]RequestRelease, releases)
	}
	for i := range r.Releases {
		rel := RequestRelease{Ino: d.U64(), CapID: d.U64(), Caps: d.U32(), Wanted: d.U32(), Seq: d.U32(), IssueSeq: d.U32(), MSeq: d.U32(), DentrySeq: d.U32()}
		rel.DentryName = string(d.Raw(d.U32()))
		r.Releases[i] = rel
	}
	r.Stamp = decodeTime(d)
	n := d.Count(8, MaxRequestGIDs)
	if n != 0 {
		r.GIDs = make([]uint64, n)
	}
	for i := range r.GIDs {
		r.GIDs[i] = d.U64()
	}
	r.AlternateName, r.FscryptAuth, r.FscryptFile = cloneOrNil(d.Bytes()), cloneOrNil(d.Bytes()), cloneOrNil(d.Bytes())
	if err := d.Done(); err != nil {
		return ClientRequest{}, err
	}
	return r, nil
}

func decodeFilePath(d *wire.Decoder) FilePath {
	if d.U8() != 1 {
		d.Fail(wire.ErrVersion)
	}
	return FilePath{Ino: d.U64(), Path: d.String()}
}

func cloneOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

// plainFront validates the envelope of a message whose payload is entirely
// in the front segment.
func plainFront(m msgr.MessageData, typ, version, compat uint16, limit uint32) ([]byte, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return nil, wire.ErrLimit
	}
	if m.Type != typ || len(m.Middle) != 0 || len(m.Data) != 0 {
		return nil, msgr.ErrFrame
	}
	if m.Version != version || m.CompatVersion != compat {
		return nil, wire.ErrVersion
	}
	return bytes.Clone(m.Front), nil
}

// RequestForward is MClientRequestForward: the MDS forwarded the request to
// another rank. The TID is in the Messenger header. Native MDSs always set
// MustResend; resending is the caller's decision.
type RequestForward struct {
	DestinationMDS int32
	Forward        int32
	MustResend     bool
}

// DecodeRequestForward accepts the native constructor's header (version 1,
// compat 1 after canonical encode normalization).
func DecodeRequestForward(m msgr.MessageData, limit uint32) (RequestForward, error) {
	raw, err := plainFront(m, ClientRequestForwardMessage, 1, 1, limit)
	if err != nil {
		return RequestForward{}, err
	}
	d := wire.NewDecoder(raw)
	f := RequestForward{DestinationMDS: int32(d.U32()), Forward: int32(d.U32()), MustResend: d.Bool()}
	if err := d.Done(); err != nil {
		return RequestForward{}, err
	}
	return f, nil
}

// ReplyHead is ceph_mds_reply_head. Result is the signed wire code (Linux
// numbering). Safe distinguishes an unsafe (not yet journaled) reply from the
// safe one; IsDentry/IsTarget select trace records.
type ReplyHead struct {
	Op                       uint32
	Result                   int32
	MDSMapEpoch              uint32
	Safe, IsDentry, IsTarget bool
}

// ClientReply is one MClientReply with its raw trace, extra and snap blobs.
type ClientReply struct {
	ReplyHead
	Trace, Extra, Snap []byte
}

// DecodeClientReply decodes the reply envelope. Pinned MClientReply uses the
// default header version/compat (1/1 after canonical encode normalization).
func DecodeClientReply(m msgr.MessageData, limit uint32) (ClientReply, error) {
	var r ClientReply
	raw, err := plainFront(m, ClientReplyMessage, 1, 1, limit)
	if err != nil {
		return r, err
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	r.Op, r.Result, r.MDSMapEpoch = d.U32(), int32(d.U32()), d.U32()
	r.Safe, r.IsDentry, r.IsTarget = d.Bool(), d.Bool(), d.Bool()
	r.Trace, r.Extra, r.Snap = cloneOrNil(d.Bytes()), cloneOrNil(d.Bytes()), cloneOrNil(d.Bytes())
	if err := d.Done(); err != nil {
		return ClientReply{}, err
	}
	return r, nil
}

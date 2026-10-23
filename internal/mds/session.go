package mds

import (
	"bytes"
	"errors"
	"sort"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientSession.h,
// include/ceph_fs.h (ceph_mds_session_head, session ops), mds/mdstypes.{h,cc}
// (feature_bitset_t, metric_spec_t), mds/MDSAuthCaps.h (MDSCapMatch,
// MDSCapAuth) and mds/cephfs_features.h. LGPL-2.1 sources; no code copied.

const (
	ClientSessionMessage uint16 = 22
	// SessionHeadBytes is the packed ceph_mds_session_head size.
	SessionHeadBytes          = 28
	MaxSessionMetadata uint32 = 1024
	MaxSessionCapAuths uint32 = 4096
	MaxSessionCapGIDs  uint32 = 4096
)

// Session operations from include/ceph_fs.h. The numeric values are wire data.
const (
	SessionRequestOpen       uint32 = 0
	SessionOpen              uint32 = 1
	SessionRequestClose      uint32 = 2
	SessionClose             uint32 = 3
	SessionRequestRenewCaps  uint32 = 4
	SessionRenewCaps         uint32 = 5
	SessionStale             uint32 = 6
	SessionRecallState       uint32 = 7
	SessionFlushMessage      uint32 = 8
	SessionFlushMessageAck   uint32 = 9
	SessionForceReadOnly     uint32 = 10
	SessionReject            uint32 = 11
	SessionRequestFlushMDLog uint32 = 12

	// SessionFlagBlocklisted is MClientSession::SESSION_BLOCKLISTED.
	SessionFlagBlocklisted uint32 = 1
)

// ClientFeatures is the exact CephFS client feature bitset the session
// profile declares: bits 0-8 (the reserved release bits through MIMIC) and
// RECLAIM_CLIENT (10). MIMIC means v7 session replies (error_string metadata,
// MDS supported features) are decoded. RECLAIM_CLIENT is never read by the
// pinned MDS for behavior; it only satisfies a filesystem requirement, and the
// MClientReclaim exchange it names is implemented (reclaim.go).
// REPLY_ENCODING(9) and every other later protocol feature are withheld until
// their message formats are implemented. The value is one little-endian 64-bit
// block, as feature_bitset_t::encode emits it.
var ClientFeatures = []byte{0xff, 0x05, 0, 0, 0, 0, 0, 0}

// CephFSClientFeatures is declared by the CephFS metadata profile: bits 0-8,
// REPLY_ENCODING (9: versioned InodeStat/DirStat/LeaseStat replies),
// RECLAIM_CLIENT (10: as in ClientFeatures), MULTI_RECONNECT (12: bounded
// caller-supplied reconnect state split across v5 messages), ALTERNATE_NAME (15: alternate
// names in requests and leases), 32BITS_RETRY_FWD (18: 32-bit retry/forward
// request fields), NEW_SNAPREALM_INFO (19: SnapRealmInfoNew snap traces) and
// HAS_OWNER_UIDGID (20: owner UID/GID in the v3 request head). Lazy wanted
// caps, delegated inodes, metrics, notify-session-state,
// getvxattr, client auth-cap checks, charmap and blockdiff stay withheld.
var CephFSClientFeatures = []byte{0xff, 0x97, 0x1c, 0, 0, 0, 0, 0}

// NoOldestClientTID is the native MClientSession default. The MDS ignores it
// for completed-request trimming; no metadata request exists in this stage.
const NoOldestClientTID = ^uint64(0)

var ErrSession = errors.New("ceph mds: invalid client session wire data")

// CapAuth is one decoded MDSCapAuth entry from a session OPEN. UID -1 is
// MDS_AUTH_UID_ANY. Values are raw server grants, not enforced locally.
type CapAuth struct {
	UID                             int64
	GIDs                            []uint32
	Path, FilesystemName            string
	RootSquash, Readable, Writeable bool
}

// ClientSession is one MClientSession payload. Version is the header version
// (1 for head-only messages, 7 otherwise). Feature and metric bitsets keep
// their raw little-endian bytes.
type ClientSession struct {
	Version            uint16
	Op                 uint32
	Sequence           uint64
	Stamp              Time
	MaxCaps, MaxLeases uint32
	Metadata           map[string]string
	SupportedFeatures  []byte
	MetricFlags        []byte
	Flags              uint32
	CapAuths           []CapAuth
	OldestClientTID    uint64
}

// FeatureBitsetHas tests bit in a raw feature_bitset_t byte image.
func FeatureBitsetHas(raw []byte, bit uint) bool {
	index := bit / 8
	return uint64(index) < uint64(len(raw)) && raw[index]&(1<<(bit%8)) != 0
}

func encodeHead(e *wire.Encoder, op uint32, seq uint64) {
	e.U32(op)
	e.U64(seq)
	e.U64(0) // ceph_timespec stamp: the client constructors zero the head.
	e.U32(0)
	e.U32(0)
}

func sessionMessage(version uint16, front []byte) msgr.MessageData {
	return msgr.MessageData{Type: ClientSessionMessage, Priority: 127, Version: version, CompatVersion: 1, Front: front}
}

// EncodeRequestOpen builds the v7 REQUEST_OPEN that a current Tentacle client
// sends: metadata in key order, the declared feature bitset (ClientFeatures or
// CephFSClientFeatures), an empty metric spec (no metrics are sent), flags 0,
// no cap auths and the default oldest TID.
func EncodeRequestOpen(metadata map[string]string, features []byte) msgr.MessageData {
	e := wire.Encoder{}
	encodeHead(&e, SessionRequestOpen, 0)
	keys := make([]string, 0, len(metadata))
	for k := range metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.U32(uint32(len(keys)))
	for _, k := range keys {
		e.String(k)
		e.String(metadata[k])
	}
	e.Bytes(features)
	metric := wire.Encoder{}
	metric.Bytes(nil)
	e.Struct(1, 1, metric.Data)
	e.U32(0)
	e.U32(0)
	e.U64(NoOldestClientTID)
	return sessionMessage(7, e.Data)
}

// EncodeSessionControl builds a head-only client message (REQUEST_RENEWCAPS,
// REQUEST_CLOSE, FLUSHMSG_ACK or REQUEST_FLUSH_MDLOG). With empty metadata and features the native
// encoder selects header version 1, so oldest_client_tid is not transmitted.
func EncodeSessionControl(op uint32, seq uint64) (msgr.MessageData, error) {
	if op != SessionRequestRenewCaps && op != SessionRequestClose && op != SessionFlushMessageAck && op != SessionRequestFlushMDLog {
		return msgr.MessageData{}, ErrSession
	}
	e := wire.Encoder{}
	encodeHead(&e, op, seq)
	return sessionMessage(1, e.Data), nil
}

// DecodeClientSession decodes a server MClientSession. Pinned Tentacle MDS
// producers emit only header version 1 (head only) or 7; other versions and
// trailing bytes are refused rather than guessed.
func DecodeClientSession(m msgr.MessageData, limit uint32) (ClientSession, error) {
	var result ClientSession
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return result, wire.ErrLimit
	}
	if m.Type != ClientSessionMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return result, msgr.ErrFrame
	}
	if m.CompatVersion != 1 || m.Version != 1 && m.Version != 7 {
		return result, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(bytes.Clone(m.Front), limit)
	result.Version = m.Version
	result.Op, result.Sequence = d.U32(), d.U64()
	result.Stamp = decodeTime(d)
	result.MaxCaps, result.MaxLeases = d.U32(), d.U32()
	result.OldestClientTID = NoOldestClientTID
	if m.Version == 7 {
		n := d.Count(8, MaxSessionMetadata)
		result.Metadata = make(map[string]string, n)
		for range n {
			k, v := d.String(), d.String()
			if _, exists := result.Metadata[k]; exists {
				d.Fail(ErrSession)
			}
			result.Metadata[k] = v
		}
		result.SupportedFeatures = decodeBitset(d)
		metric := currentStruct(d, 1, 1)
		result.MetricFlags = decodeBitset(metric)
		d.Fail(metric.Done())
		result.Flags = d.U32()
		n = d.Count(6+6+8+4+4+4+1+2, MaxSessionCapAuths)
		result.CapAuths = make([]CapAuth, n)
		for i := range result.CapAuths {
			auth := currentStruct(d, 1, 1)
			match := currentStruct(auth, 1, 1)
			entry := CapAuth{UID: int64(match.U64())}
			g := match.Count(4, MaxSessionCapGIDs)
			entry.GIDs = make([]uint32, g)
			for j := range entry.GIDs {
				entry.GIDs[j] = match.U32()
			}
			entry.Path, entry.FilesystemName, entry.RootSquash = match.String(), match.String(), match.Bool()
			auth.Fail(match.Done())
			entry.Readable, entry.Writeable = auth.Bool(), auth.Bool()
			d.Fail(auth.Done())
			result.CapAuths[i] = entry
		}
		result.OldestClientTID = d.U64()
	}
	if err := d.Done(); err != nil {
		return ClientSession{}, err
	}
	return result, nil
}

// decodeBitset keeps the exact native byte image (u32 byte length, then
// whole and partial little-endian blocks).
func decodeBitset(d *wire.Decoder) []byte {
	n := d.U32()
	if n > MaxClientFeatureBytes {
		d.Fail(wire.ErrLimit)
		return nil
	}
	return bytes.Clone(d.Raw(n))
}

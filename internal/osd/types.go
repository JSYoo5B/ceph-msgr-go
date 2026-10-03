// Package osd implements the bounded Tentacle wire codecs for explicitly
// targeted, read-only object requests. It does not discover or route objects.
package osd

import "errors"

// These wire definitions are independently implemented from Ceph v20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: messages/MOSDOp.h,
// messages/MOSDOpReply.h, osd/osd_types.{h,cc}, and include/rados.h.
// Those sources carry LGPL-2.1 notices; no native implementation is copied.
const (
	RequestMessage uint16 = 42
	ReplyMessage   uint16 = 43
	Read           uint16 = 0x1201
	Stat           uint16 = 0x1202
	FlagAck        uint64 = 0x0001
	FlagOnNVRAM    uint64 = 0x0002
	FlagOnDisk     uint64 = 0x0004
	FlagRead       uint64 = 0x0010
	FlagReturnVec  uint64 = 0x4000000
	NoSnap         uint64 = ^uint64(0) - 1
	MaxOperations         = 64
)

const (
	featureObjectLocator uint64 = 1 << 8
	featurePGID64        uint64 = 1 << 9
	featureNewOSDOp      uint64 = 1 << 56
	featureNewOSDReply   uint64 = 1 << 58
	opWireSize                  = 38
)

var (
	ErrFeatures  = errors.New("ceph osd: unsupported negotiated encoding features")
	ErrOperation = errors.New("ceph osd: unsupported operation")
	ErrRequest   = errors.New("ceph osd: invalid read-only request")
	ErrRedirect  = errors.New("ceph osd: automatic redirect unsupported")
)

type Locator struct {
	Pool      int64
	Key       string
	Namespace string
	Hash      int64 // -1 means no explicit locator hash.
}

type PG struct {
	Pool uint64
	Seed uint32
}

type Operation struct {
	Code           uint16
	Flags          uint32
	Offset, Length uint64
}

// Request.Hash is the caller's raw object hash. MOSDOp v6 transmits this in
// raw pg_t, not an independently chosen actual PG or erasure-code shard.
// The server maps it using the supplied pool and its OSDMap.
type Request struct {
	ClientInc  uint32
	MapEpoch   uint32
	Locator    Locator
	Object     string
	Hash       uint32
	Operations []Operation
}

type Version struct {
	Version uint64
	Epoch   uint32
}

type OperationResult struct {
	Code                         uint16
	Flags                        uint32
	Offset, Length, TruncateSize uint64
	TruncateSequence             uint32
	PayloadLength                uint32
	Result                       int32
	Data                         []byte
}

type Redirect struct {
	Locator            Locator
	Object             string
	LegacyInstructions []byte
}

func (r Redirect) Empty() bool {
	return r.Locator.Pool == -1 && r.Object == "" && len(r.LegacyInstructions) == 0
}

type Reply struct {
	Object                       string
	PG                           PG
	Flags                        uint64
	Result                       int32
	MapEpoch                     uint32
	RetryAttempt                 int32
	LegacyVersion, ReplayVersion Version
	UserVersion                  uint64
	Operations                   []OperationResult
	Redirect                     Redirect
	Trace                        [3]uint64
	RawFront, RawData            []byte
}

func (r Reply) OnDisk() bool { return r.Flags&FlagOnDisk != 0 }

// Package osd implements the bounded Tentacle wire codecs for explicitly
// targeted object requests. It does not discover or route objects.
package osd

import "errors"

// These wire definitions are independently implemented from Ceph v20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: messages/MOSDOp.h,
// messages/MOSDOpReply.h, osd/osd_types.{h,cc}, and include/rados.h.
// Those sources carry LGPL-2.1 notices; no native implementation is copied.
const (
	RequestMessage      uint16 = 42
	ReplyMessage        uint16 = 43
	Read                uint16 = 0x1201
	Stat                uint16 = 0x1202
	AssertVersion       uint16 = 0x1208
	CompareExtent       uint16 = 0x1220
	OmapGetKeys         uint16 = 0x1211
	OmapGetValues       uint16 = 0x1212
	OmapGetHeader       uint16 = 0x1213
	OmapGetValuesByKeys uint16 = 0x1214
	OmapCompare         uint16 = 0x1219
	OmapSetValues       uint16 = 0x2215
	OmapSetHeader       uint16 = 0x2216
	OmapClear           uint16 = 0x2217
	OmapRemoveKeys      uint16 = 0x2218
	OmapRemoveKeyRange  uint16 = 0x222c
	GetXattr            uint16 = 0x1301
	GetXattrs           uint16 = 0x1302
	CompareXattr        uint16 = 0x1303
	SetXattr            uint16 = 0x2301
	RemoveXattr         uint16 = 0x2304
	Write               uint16 = 0x2201
	WriteFull           uint16 = 0x2202
	WriteSame           uint16 = 0x2226
	Truncate            uint16 = 0x2203
	Zero                uint16 = 0x2204
	Delete              uint16 = 0x2205
	Append              uint16 = 0x2206
	Create              uint16 = 0x220d
	OpExclusive         uint32 = 1
	OpFailOK            uint32 = 2
	// OpFadvise* are include/rados.h CEPH_OSD_OP_FLAG_FADVISE_* cache hints.
	// They pass unchanged to the object store on the data paths that take
	// per-op flags; the OSD may add DONTNEED to a write itself.
	OpFadviseRandom     uint32 = 0x4
	OpFadviseSequential uint32 = 0x8
	OpFadviseWillNeed   uint32 = 0x10
	OpFadviseDontNeed   uint32 = 0x20
	OpFadviseNoCache    uint32 = 0x40
	OpFadviseMask              = OpFadviseRandom | OpFadviseSequential | OpFadviseWillNeed | OpFadviseDontNeed | OpFadviseNoCache
	FlagAck             uint64 = 0x0001
	FlagOnNVRAM         uint64 = 0x0002
	FlagOnDisk          uint64 = 0x0004
	FlagRead            uint64 = 0x0010
	FlagWrite           uint64 = 0x0020
	FlagReturnVec       uint64 = 0x4000000
	NoSnap              uint64 = ^uint64(0) - 1
	MaxOperations              = 64
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
	ErrRequest   = errors.New("ceph osd: invalid object request")
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
	Omap           OmapInput
	Xattr          XattrInput
	Class          ClassInput
	AllocHint      AllocHintInput
	Checksum       ChecksumInput
	Watch          WatchInput
	SparseRead     SparseReadInput
	MapExt         MapExtInput
	ListWatchers   ListWatchersInput
	ListSnaps      ListSnapsInput
	CopyFrom       CopyFromInput
	Data           []byte
	Version        uint64
	Snap           uint64 // ROLLBACK target snap id.
	Code           uint16
	Flags          uint32
	Offset, Length uint64
	// TruncateSize and TruncateSequence are the extent union's truncate
	// fields, admitted only on READ, WRITE and TRUNCATE (CephFS file data).
	TruncateSize     uint64
	TruncateSequence uint32
}

// Request.Hash is the caller's raw object hash. MOSDOp v6 transmits this in
// raw pg_t, not an independently chosen actual PG or erasure-code shard.
// The server maps it using the supplied pool and its OSDMap.
// ReadSnap zero (or NoSnap) reads the head; LIST_SNAPS sends SnapDir itself.
// SnapSeq/Snaps form the write SnapContext and OrderSnap sets ORDERSNAP.
type Request struct {
	ClientInc                      uint32
	MTimeSeconds, MTimeNanoseconds uint32
	MapEpoch                       uint32
	Locator                        Locator
	Object                         string
	Hash                           uint32
	Operations                     []Operation
	ReadSnap, SnapSeq              uint64
	Snaps                          []uint64
	OrderSnap                      bool
	// EnforceSnapContext uses the caller's SnapSeq/Snaps even in pool-snapshot mode.
	EnforceSnapContext bool
	// FullTry and FullForce set CEPH_OSD_FLAG_FULL_TRY/FULL_FORCE on a
	// writing vector; at most one. See FULL_CONTRACT.md.
	FullTry, FullForce bool
	// BalanceReads and LocalizeReads set CEPH_OSD_FLAG_BALANCE_READS and
	// LOCALIZE_READS so a non-primary replica may serve a read-only vector.
	BalanceReads, LocalizeReads bool
	// IgnoreRedirect sets CEPH_OSD_FLAG_IGNORE_REDIRECT: a manifest redirect
	// object is served locally instead of from its target. OrderReadsWrites
	// sets CEPH_OSD_FLAG_RWORDERED: reads are ordered with writes.
	IgnoreRedirect, OrderReadsWrites bool
	// SkipRWLocks sets CEPH_OSD_FLAG_SKIPRWLOCKS: the OSD takes no object
	// read/write lock. Admitted only on vectors that do not write.
	SkipRWLocks bool
	// IgnoreCache bypasses the selected pool's cache handling. It does not
	// bypass manifest redirects or change the caller-selected pool or target.
	IgnoreCache bool
	// IgnoreOverlay sets CEPH_OSD_FLAG_IGNORE_OVERLAY, which native cache
	// flush/try-flush/evict callers send; admitted only on those requests.
	IgnoreOverlay bool
	// RetryAttempt is the wire retry count. Nonzero sets CEPH_OSD_FLAG_RETRY
	// and marks a protocol resubmission under the original transaction ID.
	RetryAttempt int32
}

type Version struct {
	Version uint64
	Epoch   uint32
}

type OperationResult struct {
	// WriteSamePatternLength is the WRITESAME union's data_length echo.
	WriteSamePatternLength       uint64
	Xattr                        XattrResult
	CopyFrom                     CopyFromResult
	Class                        ClassResult
	AllocHint                    AllocHintInput
	Watch                        WatchInput
	Checksum                     ChecksumResult
	Snap                         uint64 // ROLLBACK union echo.
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

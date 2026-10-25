package cephmsgr

import (
	"context"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

type OSDOpcode uint16

const (
	OSDRead      OSDOpcode = OSDOpcode(osd.Read)
	OSDStat      OSDOpcode = OSDOpcode(osd.Stat)
	OSDWrite     OSDOpcode = OSDOpcode(osd.Write)
	OSDWriteFull OSDOpcode = OSDOpcode(osd.WriteFull)
	OSDWriteSame OSDOpcode = OSDOpcode(osd.WriteSame)
	OSDCopyFrom  OSDOpcode = OSDOpcode(osd.CopyFrom)
	OSDCopyFrom2 OSDOpcode = OSDOpcode(osd.CopyFrom2)
	// OSDSetRedirect turns the targeted object into a manifest redirect to the
	// head of Redirect's target (librados set_redirect without
	// WITH_REFERENCE): its data is truncated, later reads are served from
	// the target, and its user version becomes TargetVersion. OSDUnsetManifest
	// removes the manifest again. Both are standalone and flagless.
	OSDSetRedirect   OSDOpcode = OSDOpcode(osd.SetRedirect)
	OSDUnsetManifest OSDOpcode = OSDOpcode(osd.UnsetManifest)
	// OSDIsdirty reports the targeted head object's committed object_info dirty
	// flag as the result's Dirty bool (its per-op result is always 0); an
	// ordinary write sets that flag. OSDUndirty clears it and commits (per-op
	// result always 0, no output). Both are inputless, standalone and flagless,
	// and reach the handler on a plain replicated pool without a cache-tier
	// overlay.
	OSDIsdirty       OSDOpcode = OSDOpcode(osd.Isdirty)
	OSDUndirty       OSDOpcode = OSDOpcode(osd.Undirty)
	OSDAppend        OSDOpcode = OSDOpcode(osd.Append)
	OSDTruncate      OSDOpcode = OSDOpcode(osd.Truncate)
	OSDZero          OSDOpcode = OSDOpcode(osd.Zero)
	OSDCreate        OSDOpcode = OSDOpcode(osd.Create)
	OSDDelete        OSDOpcode = OSDOpcode(osd.Delete)
	OSDAssertVersion OSDOpcode = OSDOpcode(osd.AssertVersion)
	OSDCompareExtent OSDOpcode = OSDOpcode(osd.CompareExtent)
	// OSDCacheFlush, OSDCacheTryFlush and OSDCacheEvict write back or evict
	// the targeted cache-tier head; OSDCachePin and OSDCacheUnpin set or clear
	// its pin flag. Each is inputless, standalone and flagless, requires
	// EnableOSDCachePools, and takes exactly the native request flags:
	// flush IgnoreCache and IgnoreOverlay; try-flush and evict also
	// SkipRWLocks; pin and unpin optionally IgnoreCache only. The caller
	// targets the tier pool itself. Results are the OSD's signed codes (for
	// example EBUSY for a dirty evict, EPERM for a pinned object), and a lost
	// reply after transmission remains an unknown outcome.
	OSDCacheFlush    OSDOpcode = OSDOpcode(osd.CacheFlush)
	OSDCacheTryFlush OSDOpcode = OSDOpcode(osd.CacheTryFlush)
	OSDCacheEvict    OSDOpcode = OSDOpcode(osd.CacheEvict)
	OSDCachePin      OSDOpcode = OSDOpcode(osd.CachePin)
	OSDCacheUnpin    OSDOpcode = OSDOpcode(osd.CacheUnpin)
	// OSDSetChunk records Chunk's source extent in the targeted object's
	// chunked manifest after the OSD has its own client take a cls_cas
	// reference on the chunk target (librados set_chunk, as `rados
	// set-chunk` sends it). It is standalone, requires exactly the
	// OSDWithReference flag and Chunk, and takes no request flag, mtime,
	// snapshot context, resubmission or erasure-coded placement. Only
	// SourceOffset zero and a target other than the object itself are
	// admitted (see OSDChunkInput). The source keeps its bytes, user version
	// and mtime. Results are the OSD's signed codes (for example ENOENT for an
	// absent source or target, ERANGE beyond the source size, EOPNOTSUPP for
	// an overlapping chunk), and a lost reply after transmission remains an
	// unknown outcome that is never resent.
	OSDSetChunk OSDOpcode = OSDOpcode(osd.SetChunk)
)

// OSD reply completion flags are native OSD flags, not Messenger ACKs.
// ONDISK is a server completion report whose persistence depends on its store.
const (
	OSDReplyAck     = osd.FlagAck
	OSDReplyOnNVRAM = osd.FlagOnNVRAM
	OSDReplyOnDisk  = osd.FlagOnDisk
)

// OSDObject contains wire locator metadata prepared by the routing layer.
// Hash is the raw object-placement hash, not an actual PG number. The client
// performs no hashing or CRUSH calculation. The initial v6 encoding supports
// replicated-pool requests and no independently targeted erasure-code shard.
type OSDObject struct {
	Pool           int64
	Name           string
	Key, Namespace string
	Hash           uint32
}

type OSDOperationFlags uint32

const (
	OSDExclusiveCreate OSDOperationFlags = OSDOperationFlags(osd.OpExclusive)
	// OSDFailOK continues a compound vector after this operation's error. Its
	// negative per-op result is retained even if later mutations commit.
	OSDFailOK OSDOperationFlags = OSDOperationFlags(osd.OpFailOK)
	// OSDWithReference is CEPH_OSD_OP_FLAG_WITH_REFERENCE, a per-operation
	// flag that OSDSetChunk requires and every other opcode refuses.
	OSDWithReference OSDOperationFlags = OSDOperationFlags(osd.OpWithReference)
	// OSDFadvise* are cache hints (rados.h CEPH_OSD_OP_FLAG_FADVISE_*) that
	// READ, SPARSE_READ, CHECKSUM, CMPEXT, WRITE, WRITEFULL, WRITESAME and
	// APPEND pass to the OSD's object store; other opcodes refuse them before
	// submission. They change no result: the reply echoes them (the OSD may
	// add DONTNEED to a write) and only EXCL and FAILOK are compared.
	OSDFadviseRandom     OSDOperationFlags = OSDOperationFlags(osd.OpFadviseRandom)
	OSDFadviseSequential OSDOperationFlags = OSDOperationFlags(osd.OpFadviseSequential)
	OSDFadviseWillNeed   OSDOperationFlags = OSDOperationFlags(osd.OpFadviseWillNeed)
	OSDFadviseDontNeed   OSDOperationFlags = OSDOperationFlags(osd.OpFadviseDontNeed)
	OSDFadviseNoCache    OSDOperationFlags = OSDOperationFlags(osd.OpFadviseNoCache)
)

// OSDOperation is one ordered operation on the request's object. Write,
// WriteFull, Append and CompareExtent require Length == len(Data).
// WriteFull/Append have Offset 0.
// WriteSame repeats nonempty Data over Length bytes at Offset: Length must
// be a multiple of len(Data), and may be zero. Its cumulative logical length
// is bounded independently of the frame data. Zero-length effects and writes
// against an object with a truncate sequence follow the server; its reply
// echoes the requested logical extent, not the nested WRITE's clipped extent.
// An offset-zero positive WriteSame refuses every subsequent offset-zero
// positive Read/CompareExtent in the vector because that clipped size is unknown.
// OSDCopyFrom and OSDCopyFrom2 copy the source selected by CopyFrom; the OSD
// fetches it itself. A copy may stand alone or lead only SetXattr/RemoveXattr
// operations. Other source/snapshot/truncate limits are in OSDCopyFromInput.
// Truncate uses Offset as the new size. Create/Delete/Stat have zero extents.
// AssertVersion uses Version, with zero extents and no payload. Only Create
// accepts ExclusiveCreate. TruncateSize/TruncateSequence are the extent
// truncate fields CephFS data I/O sends on Read, Write and Truncate: a read
// past a newer truncation is clipped at TruncateSize, a write with an older
// sequence than the object's is trimmed at the object's size (the reply
// Length is then the bytes actually written), a write with a newer sequence
// truncates the object first, and a sequenced Truncate (Offset must equal
// TruncateSize) is a no-op unless its sequence is newer. A size requires a
// nonzero sequence. Even with zero fields the OSD trims a write that extends
// an object carrying a truncate sequence.
// Xattr is the active input union for Get/GetAll/Compare/Set/RemoveXattr;
// their extent and Version fields are zero. Data holds the SET/CMP bytes.
// Omap is the active input for the OMAP reads and mutations, with zero
// extent, Version, Data and Xattr. Its wire input length is derived locally.
// Class is the active input for OSDCall, with zero extent, Version, Xattr
// and Omap; Data holds the method input. AllocHint is the active input for
// OSDSetAllocHint, with every other field zero. Snapshot is the ROLLBACK
// target (1 through OSDMaxSnapshot) and ListSnapshots the OSDListSnapshots
// budget; both opcodes carry no other field and no flags. ListWatchers is the
// local output budget for head-only, standalone OSDListWatchers. MapExt selects
// a bounded standalone extent read at ReadSnapshot; Checksum selects a bounded read at ReadSnapshot using Offset/Length. Head
// checksum compounds admit only Read/Stat/AssertVersion/CompareExtent alongside
// checksums, without FAILOK, CALL or writing operations.
// Caller must not mutate the vector, Data or Omap inputs until Request returns.
type OSDOperation struct {
	Code           OSDOpcode
	Flags          OSDOperationFlags
	Offset, Length uint64
	Data           []byte
	Version        uint64
	Xattr          OSDXattrInput
	Omap           OSDOmapInput
	Class          OSDClassInput
	AllocHint      OSDAllocHint
	Snapshot       uint64
	Watch          OSDWatchInput
	SparseRead     OSDSparseReadInput
	MapExt         OSDMapExtInput
	Checksum       OSDChecksumInput
	ListWatchers   OSDListWatchersInput
	ListSnapshots  OSDListSnapshotsInput
	CopyFrom       OSDCopyFromInput
	Redirect       OSDRedirectInput
	Chunk          OSDChunkInput

	TruncateSize     uint64
	TruncateSequence uint32
}

// OSDCopyFromInput is the active input for OSDCopyFrom and OSDCopyFrom2. The
// source is the head of SourceName in SourcePool/SourceKey/SourceNamespace;
// the OSD computes the source PG and fetches it with its own connection, so
// the caller supplies no source placement. SourceSnapshot zero is the head;
// 1 through OSDMaxSnapshot names a snapshot the OSD resolves to a clone. The
// source must differ from the target. SourceVersion zero accepts any source version, otherwise the OSD
// refuses a different one (ERANGE). TruncateSequence selects COPY_FROM2's
// TRUNCATE_SEQ form: the destination gets TruncateSequence (nonzero) and
// TruncateSize instead of the source's values; it is refused on OSDCopyFrom.
// SourceFadvise carries FADVISE hints for the source read; cache-tier copy
// flags are unsupported.
// A reply with success reports server completion of the copy as ONDISK;
// a cancelled or failed local wait leaves the outcome unknown.
type OSDCopyFromInput struct {
	SourcePool                 int64
	SourceName                 string
	SourceKey, SourceNamespace string
	SourceSnapshot             uint64
	SourceVersion              uint64
	TruncateSequence           uint32
	TruncateSize               uint64
	// SourceFadvise is the FADVISE hint mask for the OSD's read of the
	// source (librados copy_from src_fadvise_flags); the reply echoes it.
	SourceFadvise OSDOperationFlags
}

// OSDRedirectInput is the active input for OSDSetRedirect: the target head
// named by pool, name, key and namespace (the OSD computes its PG; the target
// must differ from the redirected object), and the user version the
// redirected object reports afterwards.
type OSDRedirectInput struct {
	TargetPool                 int64
	TargetName                 string
	TargetKey, TargetNamespace string
	TargetVersion              uint64
}

// OSDChunkInput is the active input for OSDSetChunk: the source extent of the
// targeted object and the chunk target named by pool, name, key and namespace
// (the OSD computes its PG) at TargetOffset. The target must exist; the OSD
// takes a reference on it before recording the chunk.
//
// SourceOffset must be zero and SourceLength positive. The pinned OSD records
// a failed reference increment under both the chunk offset and offset zero,
// so a failure at a non-zero source offset leaves the request unanswered and
// the source object blocked on the OSD (source-derived, not observed); such
// requests are refused before transmission. A target equal to the targeted
// object is refused because the OSD's own reference request would wait on the
// object it has blocked.
type OSDChunkInput struct {
	SourceOffset, SourceLength uint64
	TargetPool                 int64
	TargetName                 string
	TargetKey, TargetNamespace string
	TargetOffset               uint64
}

// OSDTime is Tentacle's raw utime_t. Zero preserves the server's zero-mtime
// semantics; the client does not substitute its own clock. Nanoseconds < 1e9.
type OSDTime struct{ Seconds, Nanoseconds uint32 }

// OSDRequest is one ordered, atomic object vector on an explicitly targeted
// replicated OSD. Mutation replies request only ONDISK completion, not a
// separate preliminary ACK. There is no automatic epoch refresh, redirect,
// placement calculation, retry or mutation replay.
//
// ReadSnapshot selects the clone a read-only vector observes: zero (or
// OSDHeadSnapshot) is the head, and 1 through OSDMaxSnapshot name a snap id.
// The reply does not echo it. SnapContext and OrderSnapshots apply only to
// vectors that write, which always target the head. OrderSnapshots sets
// CEPH_OSD_FLAG_ORDERSNAP so an older SnapContext is refused with
// OSDOldSnapContext instead of writing without a clone.
type OSDRequest struct {
	ClientIncarnation uint32
	ModificationTime  OSDTime
	MapEpoch          uint32
	Object            OSDObject
	// Placement is mandatory for replicated PG profiles and absent for
	// OSDProfileObjectHash. The caller owns map application and target choice.
	Placement      *OSDPlacement
	Operations     []OSDOperation
	ReadSnapshot   uint64
	SnapContext    OSDSnapContext
	OrderSnapshots bool
	// EnforceSnapContext sends CEPH_OSD_FLAG_ENFORCE_SNAPC on a writing
	// vector: the server uses the supplied SnapContext even in pool-snapshot
	// mode. An empty context is permitted. The caller owns its selection and
	// must preserve it and this flag on an explicit resubmission.
	EnforceSnapContext bool
	// FullTry makes a writing vector fail with the server's signed Linux code
	// (-122 EDQUOT for a quota, -28 ENOSPC otherwise) when its pool is full
	// instead of being discarded unanswered; FullForce writes despite a full pool. At most one,
	// writing vectors only.
	FullTry, FullForce bool
	// BalanceReads and LocalizeReads let a non-primary replica serve a
	// read-only vector made of Read, Stat, GetXattr(s), the OMAP reads,
	// MapExt, Checksum or SparseRead. The connection may then target any
	// replica; the caller owns that choice and the PG's Primary field still
	// names the primary. Replicas may answer -EAGAIN (unreadable object or
	// unstable write), which is a known refusal the caller handles.
	BalanceReads, LocalizeReads bool
	// IgnoreRedirect (CEPH_OSD_FLAG_IGNORE_REDIRECT) makes the OSD serve a
	// manifest redirect object itself rather than its target.
	// OrderReadsWrites (CEPH_OSD_FLAG_RWORDERED, librados
	// ORDER_READS_WRITES) orders a read with the object's writes.
	IgnoreRedirect, OrderReadsWrites bool
	// SkipRWLocks (CEPH_OSD_FLAG_SKIPRWLOCKS, librados SKIPRWLOCKS) lets a read
	// proceed without the OSD's object lock; a writing vector refuses it.
	SkipRWLocks bool
	// IgnoreCache sends CEPH_OSD_FLAG_IGNORE_CACHE to the caller-selected
	// pool/primary, with EnableOSDCachePools selected before Dial. Initially
	// only standalone head STAT/READ/WRITEFULL/DELETE on replicated placement
	// are admitted, without request replay, snapshot, replica-read, ordering,
	// redirect, lock-bypass, per-op flags or extent-truncate combinations.
	// Cache promotion/proxy is bypassed; dirty tracking remains. This neither
	// selects the base pool nor guarantees absence of snapshot whiteouts.
	IgnoreCache bool
	// IgnoreOverlay sends CEPH_OSD_FLAG_IGNORE_OVERLAY. Native clients read it
	// when they choose a target; here the caller supplies the target, so it
	// is sent only with OSDCacheFlush, OSDCacheTryFlush and OSDCacheEvict,
	// which native callers submit with it. Every other request refuses it.
	IgnoreOverlay bool
	// Resubmit explicitly repeats an earlier request of this client under its
	// original identity (transaction ID and incarnation) so the OSD can answer
	// from its PG log instead of executing it again: pass the Submission an
	// earlier Request returned, including after an unknown outcome, with the
	// same ClientIncarnation, and a RetryAttempt of 1 or more that you raise
	// per resubmission. The caller must resend the same operations; the
	// client cannot check that. The server's duplicate detection covers only
	// requests still in the PG log and is never a replay guarantee; nothing
	// resubmits automatically.
	Resubmit     *OSDSubmission
	RetryAttempt int32
}

// OSDMaxRetryAttempt bounds RetryAttempt.
const OSDMaxRetryAttempt int32 = osd.MaxRetryAttempt

type OSDPG struct {
	Pool uint64
	// Seed is the actual PG for replicated-PG requests and ListPG; object-hash
	// profile replies instead echo the raw object-placement hash.
	Seed uint32
}

type OSDVersion struct {
	Epoch   uint32
	Version uint64
}

// OSDOperationResult preserves each operation's wire metadata and raw output.
// Read and CompareExtent may report a shorter Length at EOF. CompareExtent
// compares bytes beyond EOF with zero without returning a payload.
// Stat's bytes describe size and mtime; this layer does not convert them into a
// filesystem-style object API. The caller can interpret the wire data.
// Xattr keeps attribute lengths/comparison fields separate from extents.
// For OMAP operations, Length echoes the encoded input byte length (zero for
// GetOmapHeader and ClearOmap). Data retains the raw collection framing,
// including the page's more suffix, or the raw header; comparisons and
// mutations return none.
type OSDOperationResult struct {
	// WriteSamePatternLength preserves the WRITESAME data_length union field.
	WriteSamePatternLength       uint64
	Code                         OSDOpcode
	Flags                        uint32
	Offset, Length, TruncateSize uint64
	TruncateSequence             uint32
	PayloadLength                uint32
	Result                       int32
	Data                         []byte
	Xattr                        OSDXattrResult
	Class                        OSDClassResult
	AllocHint                    OSDAllocHint
	// CopyFromVersion and CopyFromSnapshot echo the COPY_FROM source version
	// and source snapid (OSDHeadSnapshot for the head).
	CopyFromVersion, CopyFromSnapshot uint64
	// Snapshot echoes the ROLLBACK target. Snapshots is decoded from Data
	// for a successful OSDListSnapshots, which also keeps the raw bytes.
	Snapshot  uint64
	Snapshots OSDSnapshotList
	// Watchers is decoded on a successful OSDListWatchers; Data keeps raw bytes.
	Watchers []OSDWatcher
	// Extents decodes successful MAPEXT output; Data preserves the raw map.
	Extents    []OSDExtent
	SparseRead OSDSparseReadResult
	Watch      OSDWatchInput
	// Checksum retains the union on all replies; successful output adds Values.
	Checksum OSDChecksumResult
	// Dirty decodes the ISDIRTY output bool on a successful read; Data keeps
	// the raw byte. It is meaningful only when Code is OSDIsdirty.
	Dirty bool
}

type OSDRedirect struct {
	Pool               int64
	Key, Namespace     string
	Hash               int64
	Object             string
	LegacyInstructions []byte
}

// OSDSubmission is local request metadata, retained when an admitted request
// ends with an error. It is not a server reply or proof of execution.
type OSDSubmission struct {
	Transaction, GlobalID       uint64
	ClientIncarnation, MapEpoch uint32
	Attempt                     uint32
	Placement                   *OSDPlacement
}

// OSDReply preserves the server's completion metadata and operation outputs.
// Flags are raw Ceph OSD flags, independent of Messenger ACKs. A read-only
// success does not establish any write durability guarantee. A mutation
// success, SET_CHUNK and successful cache operations require the server ONDISK
// flag. Cache no-op completion does not prove a state change; durability is
// storage/profile dependent (MemStore contents do not survive OSD restart).
type OSDReply struct {
	Submission                   OSDSubmission
	Transaction, GlobalID        uint64
	Object                       string
	PG                           OSDPG
	Flags                        uint64
	Code                         int32
	MapEpoch                     uint32
	RetryAttempt                 int32
	LegacyVersion, ReplayVersion OSDVersion
	UserVersion                  uint64
	Operations                   []OSDOperationResult
	Redirect                     OSDRedirect
	Trace                        [3]uint64
	RawFront, RawData            []byte
}

// ErrOSDRedirect accompanies a decoded redirect reply. The routing layer may
// inspect it; this client never follows a redirect automatically.
var ErrOSDRedirect = osd.ErrRedirect

// ErrUnsupportedOSDOperation rejects unknown opcodes, flags and unsupported
// compound combinations before object-request transmission. In Tentacle,
// offset-zero READ/CMPEXT after a possible full write can compare committed
// bytes with a staged digest; these combinations are conservatively rejected.
// A remote newer truncate sequence can shorten an offset-zero WRITE, so any
// later positive offset-zero READ/CMPEXT is refused after that WRITE, even when
// its requested length is smaller than the write payload.
// OMAP and xattr reads after a staged write of the same metadata kind or a
// DELETE, which may read committed rather than staged state, are rejected the
// same way.
var ErrUnsupportedOSDOperation = osd.ErrOperation

type OSDError struct{ Code int32 }

func (e *OSDError) Error() string { return fmt.Sprintf("ceph: OSD result code %d", e.Code) }

// Request transmits one explicitly targeted ordered object vector. ctx
// governs the full local wait, including MaxInFlight admission. Cancellation
// after transmission starts preserves OutcomeUnknownError. Replies expose the
// wire pg_t, epoch and versions for the caller's routing layer; no retry occurs.
func (o *OSDConnection) Request(ctx context.Context, request OSDRequest) (OSDReply, error) {
	return o.request(ctx, request, nil)
}

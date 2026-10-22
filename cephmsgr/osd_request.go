package cephmsgr

import (
	"context"
	"errors"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
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
// success requires the server ONDISK flag; durability is storage/profile
// dependent (in particular, MemStore contents do not survive OSD restart).
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

// Validate this capability before shared-slot admission, monitor readiness or
// TID allocation. Ordinary requests retain their existing validation path.
func (o *OSDConnection) validateIgnoreCache(request OSDRequest) error {
	if !request.IgnoreCache {
		return nil
	}
	if !o.client.options.EnableOSDCachePools {
		return fmt.Errorf("%w: IgnoreCache requires EnableOSDCachePools", ErrUnsupportedFeatures)
	}
	if request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure {
		return fmt.Errorf("%w: IgnoreCache refuses resubmission and erasure-coded placement", osd.ErrRequest)
	}
	input := osd.Request{IgnoreCache: true, ReadSnap: request.ReadSnapshot,
		SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext,
		BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect,
		OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, RetryAttempt: request.RetryAttempt}
	if len(request.Operations) == 1 {
		op := request.Operations[0]
		input.Operations = []osd.Operation{{Code: uint16(op.Code), Flags: uint32(op.Flags), TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence}}
	}
	if err := osd.ValidateIgnoreCache(input, o.features); err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			return fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return err
	}
	return nil
}

// Only the explicit new snapshot flag takes this early preflight. Ordinary
// requests keep their existing complete validation/encoding path. Operation
// codes and CALL modes suffice to classify snapshot applicability; the full
// opcode, payload and compound validators still run before transmission.
func (o *OSDConnection) validateEnforceSnapContext(request OSDRequest) error {
	if !request.EnforceSnapContext {
		return nil
	}
	if len(request.Operations) == 0 || len(request.Operations) > osd.MaxOperations {
		return osd.ErrRequest
	}
	input := osd.Request{EnforceSnapContext: true, ReadSnap: request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence,
		Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, Operations: make([]osd.Operation, len(request.Operations))}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Class: osd.ClassInput{Mode: uint8(op.Class.Mode)}}
	}
	return osd.ValidateEnforceSnapContext(input, o.client.options.MaxFrameSize)
}

// The compound checksum subset is head-only and caller-targeted at a
// replicated primary. This projection adds no policy to other vectors.
func (o *OSDConnection) validateChecksumCompound(request OSDRequest) error {
	if len(request.Operations) < 2 {
		return nil
	}
	compound := false
	for _, op := range request.Operations {
		compound = compound || op.Code == OSDChecksum
	}
	if !compound {
		return nil
	}
	if len(request.Operations) > osd.MaxOperations {
		return osd.ErrRequest
	}
	if request.Resubmit != nil || request.Placement != nil && request.Placement.Erasure {
		return osd.ErrRequest
	}
	input := osd.Request{ReadSnap: request.ReadSnapshot, BalanceReads: request.BalanceReads,
		LocalizeReads: request.LocalizeReads, RetryAttempt: request.RetryAttempt,
		Operations: make([]osd.Operation, len(request.Operations))}
	for i, op := range request.Operations {
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags)}
	}
	return osd.ValidateChecksumCompoundRequest(input)
}

// Watch operations reserve event ownership before transmission. This private
// path prevents raw callers from reusing a live stream's cookie on another object.
func (o *OSDConnection) request(ctx context.Context, request OSDRequest, owner *OSDWatchStream) (OSDReply, error) {
	var result OSDReply
	c := o.client
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if c.ctx.Err() != nil || o.ctx.Err() != nil {
		return result, ErrClosed
	}
	if err := o.validatePlacement(request); err != nil {
		return result, err
	}
	if err := o.validateChecksumCompound(request); err != nil {
		return result, err
	}
	if err := o.validateIgnoreCache(request); err != nil {
		return result, err
	}
	if err := o.validateEnforceSnapContext(request); err != nil {
		return result, err
	}
	request.Placement = cloneOSDPlacement(request.Placement)
	if len(request.Operations) == 0 || len(request.Operations) > osd.MaxOperations {
		return result, osd.ErrRequest
	}
	for _, op := range request.Operations {
		if osd.IsWatchOperation(uint16(op.Code)) {
			if owner == nil || owner.connection != o || owner.cookie != op.Watch.Cookie || owner.request.Object != request.Object || owner.request.ClientIncarnation != request.ClientIncarnation {
				return result, ErrUnsupportedOSDOperation
			}
		} else if owner != nil {
			return result, ErrUnsupportedOSDOperation
		}
	}
	select {
	case c.calls <- struct{}{}:
		defer func() { <-c.calls }()
	case <-ctx.Done():
		return result, ctx.Err()
	case <-o.ctx.Done():
		return result, ErrClosed
	}
	operation, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(o.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := operation.Err(); err != nil {
		return result, o.operationError(err)
	}
	if _, err := c.monitor(operation); err != nil {
		return result, o.operationError(err)
	}
	c.mu.Lock()
	s, closed, globalID := o.session, c.closed || o.closed, c.auth.GlobalID
	c.mu.Unlock()
	if closed {
		return result, ErrClosed
	}
	if globalID != o.globalID {
		return result, errors.New("ceph: OSD connection belongs to an earlier client identity")
	}
	var fixedTID uint64
	if request.Resubmit != nil || request.RetryAttempt != 0 {
		prior := request.Resubmit
		if prior == nil || owner != nil || request.RetryAttempt < 1 || request.RetryAttempt > OSDMaxRetryAttempt ||
			prior.Transaction == 0 || prior.Transaction > c.osdNext.Load() || prior.GlobalID != globalID || prior.ClientIncarnation != request.ClientIncarnation {
			return result, fmt.Errorf("%w: resubmission needs the earlier submission of this client identity and a positive retry attempt", osd.ErrRequest)
		}
		fixedTID = prior.Transaction
	}
	if err := osdOmapEntryBound(request.Operations, c.options.MaxFrameSize); err != nil {
		return result, err
	}
	input := osd.Request{
		ClientInc:        request.ClientIncarnation,
		MTimeSeconds:     request.ModificationTime.Seconds,
		MTimeNanoseconds: request.ModificationTime.Nanoseconds,
		MapEpoch:         request.MapEpoch, Object: request.Object.Name, Hash: request.Object.Hash,
		Locator:    osd.Locator{Pool: request.Object.Pool, Key: request.Object.Key, Namespace: request.Object.Namespace, Hash: -1},
		Operations: make([]osd.Operation, len(request.Operations)),
		ReadSnap:   request.ReadSnapshot, SnapSeq: request.SnapContext.Sequence, Snaps: request.SnapContext.Snapshots, OrderSnap: request.OrderSnapshots, EnforceSnapContext: request.EnforceSnapContext, FullTry: request.FullTry, FullForce: request.FullForce, BalanceReads: request.BalanceReads, LocalizeReads: request.LocalizeReads, IgnoreRedirect: request.IgnoreRedirect, OrderReadsWrites: request.OrderReadsWrites, SkipRWLocks: request.SkipRWLocks, IgnoreCache: request.IgnoreCache, RetryAttempt: request.RetryAttempt}
	for i, op := range request.Operations {
		if (op.Code == OSDSetRedirect) != (op.Redirect != OSDRedirectInput{}) || op.Code == OSDSetRedirect && op.CopyFrom != (OSDCopyFromInput{}) {
			return result, fmt.Errorf("%w: a redirect target belongs to OSDSetRedirect alone", osd.ErrRequest)
		}
		input.Operations[i] = osd.Operation{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence, Data: op.Data, Version: op.Version, Xattr: xattrWireInput(op.Xattr), Omap: omapWireInput(op.Omap), Class: classWireInput(op.Class),
			AllocHint:    osd.AllocHintInput{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: uint32(op.AllocHint.Flags)},
			Watch:        watchWireInput(op.Watch),
			SparseRead:   osd.SparseReadInput{MaxOutputBytes: op.SparseRead.MaxOutputBytes},
			MapExt:       osd.MapExtInput{MaxOutputBytes: op.MapExt.MaxOutputBytes},
			Checksum:     osd.ChecksumInput{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type, Seed: op.Checksum.Seed},
			ListWatchers: osd.ListWatchersInput{MaxOutputBytes: op.ListWatchers.MaxOutputBytes},
			CopyFrom:     copyFromWireInput(op),
			Snap:         op.Snapshot, ListSnaps: osd.ListSnapsInput{MaxOutputBytes: op.ListSnapshots.MaxOutputBytes}}
	}
	var m msgr.MessageData
	var err error
	if request.Placement != nil {
		p := request.Placement.PG
		if c.options.OSDProfile.requestV9() {
			m, err = osd.EncodeRequestV9(osd.V9Request{Request: input, PG: osd.PG{Pool: p.Pool, Seed: p.Seed}, Shard: request.Placement.wireShard()}, o.features, c.options.MaxFrameSize)
		} else {
			m, err = osd.EncodeRequestV8(osd.V8Request{Request: input, PG: osd.PG{Pool: p.Pool, Seed: p.Seed}, Shard: request.Placement.wireShard()}, o.features, c.options.MaxFrameSize)
		}
	} else {
		m, err = osd.EncodeRequest(input, o.features, c.options.MaxFrameSize)
	}
	if err != nil {
		if errors.Is(err, osd.ErrFeatures) {
			err = fmt.Errorf("%w: %w", ErrUnsupportedFeatures, err)
		}
		return result, err
	}
	var reply msgr.MessageData
	if fixedTID != 0 {
		reply, err = s.CallAt(operation, m, fixedTID)
	} else {
		reply, err = s.Call(operation, m)
	}
	submission := OSDSubmission{Transaction: reply.Transaction, GlobalID: o.globalID, ClientIncarnation: request.ClientIncarnation, MapEpoch: request.MapEpoch, Placement: request.Placement}
	result.Submission = submission
	if err != nil {
		return result, o.operationError(err)
	}
	decoded, err := o.decodeReply(reply)
	result = publicOSDReply(decoded)
	if osd.ChecksumCompound(input.Operations) {
		// Public conversion precedes identity/completion validation so raw
		// replies remain available on failure. Compound checksum values are
		// withheld until that entire vector validates, even if its reported
		// aggregate is positive. Standalone conversion remains unchanged.
		for i := range result.Operations {
			result.Operations[i].Checksum.Values = nil
		}
	}
	result.Submission = submission
	result.Transaction, result.GlobalID = reply.Transaction, o.globalID
	redirect := errors.Is(err, osd.ErrRedirect)
	if err == nil || redirect {
		if request.Placement != nil {
			input.Hash = request.Placement.PG.Seed
		}
		err = validateOSDReply(input, decoded, redirect)
	}
	if err != nil {
		cause := fmt.Errorf("%w: invalid OSD reply: %w", msgr.ErrFrame, err)
		s.Fail(cause)
		return result, &OutcomeUnknownError{Cause: cause}
	}
	if !redirect && osd.ChecksumCompound(input.Operations) {
		// Decode prefix values only after the complete compound validates,
		// including a valid signed error after an earlier checksum output.
		for i := 0; i < osd.ChecksumCompoundPrefix(decoded); i++ {
			if op := decoded.Operations[i]; op.Code == osd.Checksum {
				result.Operations[i].Checksum = publicChecksum(op.Checksum, op.Data)
			}
		}
	}
	if redirect {
		return result, ErrOSDRedirect
	}
	if result.Code < 0 {
		return result, &OSDError{Code: result.Code}
	}
	return result, nil
}

// Each encoded OMAP entry needs at least two length prefixes, each assertion
// two prefixes and an operator, and the whole request data must fit one frame.
// Refuse impossible maps, summed across the vector, before copying headers.
func osdOmapEntryBound(ops []OSDOperation, limit uint32) error {
	var minimum uint64
	for _, op := range ops {
		minimum += uint64(len(op.Omap.Entries))*8 + uint64(len(op.Omap.Assertions))*12
		if minimum > uint64(limit) {
			return ErrLimitExceeded
		}
	}
	return nil
}

func validateOSDReply(input osd.Request, reply osd.Reply, redirect bool) error {
	if reply.Object != input.Object || reply.PG.Pool != uint64(input.Locator.Pool) || reply.PG.Seed != input.Hash || reply.RetryAttempt != input.RetryAttempt || len(reply.Operations) != len(input.Operations) {
		return errors.New("OSD reply does not match request identity or operations")
	}
	flags, _, err := osd.ValidateOperations(input.Operations, ^uint32(0))
	if err != nil {
		return err
	}
	if !redirect && reply.Result >= 0 && flags&osd.FlagWrite != 0 && !reply.OnDisk() {
		return errors.New("mutation reply lacks requested ONDISK completion")
	}
	if !redirect && osd.ChecksumCompound(input.Operations) {
		return osd.ValidateChecksumCompoundReply(input.Operations, reply)
	}
	for i, requested := range input.Operations {
		op := reply.Operations[i]
		normalizedZero := requested.Code == osd.Zero && op.Code == osd.Truncate
		length := requested.Length
		if requested.Code == osd.AssertVersion {
			length = requested.Version
		}
		// The OSD trims a WRITE that extends an object past a newer
		// truncation and echoes the shortened extent (PrimaryLogPG.cc:6768-6777).
		shortExtent := requested.Code == osd.Read || requested.Code == osd.CompareExtent || requested.Code == osd.Write
		if op.Code != requested.Code && !normalizedZero || op.Flags&(osd.OpExclusive|osd.OpFailOK) != requested.Flags&(osd.OpExclusive|osd.OpFailOK) || uint64(len(op.Data)) != uint64(op.PayloadLength) {
			return errors.New("OSD reply operation differs from request or payload metadata")
		}
		// Tentacle stops a vector on an ordinary failed operation. Only
		// FAILOK can retain a negative per-op result in a successful vector.
		// A contradictory success is not an execution/durability receipt.
		// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L8205
		// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L9188
		if !redirect && reply.Result >= 0 && op.Result < 0 && requested.Flags&osd.OpFailOK == 0 {
			return errors.New("OSD successful vector contains a failed ordinary operation")
		}
		success := !redirect && reply.Result >= 0 && op.Result >= 0
		switch requested.Code {
		case osd.OmapGetKeys, osd.OmapGetValues, osd.OmapGetHeader, osd.OmapGetValuesByKeys, osd.OmapCompare,
			osd.OmapSetValues, osd.OmapSetHeader, osd.OmapClear, osd.OmapRemoveKeys, osd.OmapRemoveKeyRange:
			if err := osd.ValidateOmapReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.GetXattr, osd.GetXattrs, osd.CompareXattr, osd.SetXattr, osd.RemoveXattr:
			if err := osd.ValidateXattrReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.Call:
			if err := osd.ValidateClassReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.SetAllocHint:
			if err := osd.ValidateAllocHintReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.Rollback:
			if err := osd.ValidateRollbackReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.Watch, osd.Notify, osd.NotifyAck:
			if err := osd.ValidateWatchReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.SparseRead:
			if err := osd.ValidateSparseReadReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.MapExt:
			if err := osd.ValidateMapExtReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.Checksum:
			if err := osd.ValidateChecksumReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.ListWatchers:
			if err := osd.ValidateListWatchersReply(requested, op, success); err != nil {
				return err
			}
			continue
		case osd.WriteSame:
			if err := osd.ValidateWriteSameReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.CopyFrom, osd.CopyFrom2, osd.SetRedirect:
			if err := osd.ValidateCopyFromReply(requested, op); err != nil {
				return err
			}
			continue
		case osd.ListSnaps:
			if err := osd.ValidateListSnapsReply(requested, op, success); err != nil {
				return err
			}
			continue
		}
		if op.Offset != requested.Offset || op.Length > length || !shortExtent && op.Length != length || op.TruncateSize != requested.TruncateSize || op.TruncateSequence != requested.TruncateSequence || op.Xattr != (osd.XattrResult{}) {
			return errors.New("OSD reply extent differs from request")
		}
		switch requested.Code {
		case osd.Read:
			// A local read sets the extent length to the bytes returned; a read
			// the OSD proxies to a manifest object's redirect target echoes
			// the requested length instead (observed live, Tentacle 20.2.4).
			if uint64(op.PayloadLength) > requested.Length || success && uint64(op.PayloadLength) != op.Length && op.Length != requested.Length {
				return errors.New("OSD read reply exceeds requested length or has inconsistent output")
			}
		case osd.Stat:
			if op.PayloadLength > 16 || success && op.PayloadLength != 16 {
				return errors.New("OSD stat reply has invalid size and mtime output")
			}
		case osd.Isdirty:
			// ISDIRTY encodes one bool (1 byte) into its output; a successful
			// read returns exactly that, a failed one returns nothing.
			if op.PayloadLength > 1 || success && op.PayloadLength != 1 {
				return errors.New("OSD isdirty reply has invalid dirty-flag output")
			}
		default:
			if op.PayloadLength != 0 {
				return errors.New("OSD mutation/guard reply has unsupported output")
			}
		}
	}
	return nil
}

func (o *OSDConnection) operationError(err error) error {
	if o.ctx.Err() == nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return err
	}
	var unknown *OutcomeUnknownError
	if errors.As(err, &unknown) {
		return &OutcomeUnknownError{Cause: errors.Join(unknown.Cause, ErrClosed)}
	}
	return errors.Join(err, ErrClosed)
}

func publicOSDReply(r osd.Reply) OSDReply {
	result := OSDReply{Object: r.Object, PG: OSDPG{Pool: r.PG.Pool, Seed: r.PG.Seed}, Flags: r.Flags,
		Code: r.Result, MapEpoch: r.MapEpoch, RetryAttempt: r.RetryAttempt, UserVersion: r.UserVersion,
		LegacyVersion: OSDVersion{Epoch: r.LegacyVersion.Epoch, Version: r.LegacyVersion.Version},
		ReplayVersion: OSDVersion{Epoch: r.ReplayVersion.Epoch, Version: r.ReplayVersion.Version},
		Trace:         r.Trace, RawFront: r.RawFront, RawData: r.RawData,
		Redirect: OSDRedirect{Pool: r.Redirect.Locator.Pool, Key: r.Redirect.Locator.Key, Namespace: r.Redirect.Locator.Namespace,
			Hash: r.Redirect.Locator.Hash, Object: r.Redirect.Object, LegacyInstructions: r.Redirect.LegacyInstructions}}
	result.Operations = make([]OSDOperationResult, len(r.Operations))
	for i, op := range r.Operations {
		result.Operations[i] = OSDOperationResult{WriteSamePatternLength: op.WriteSamePatternLength, CopyFromVersion: op.CopyFrom.Version, CopyFromSnapshot: op.CopyFrom.Snap, Code: OSDOpcode(op.Code), Flags: op.Flags,
			Offset: op.Offset, Length: op.Length, TruncateSize: op.TruncateSize, TruncateSequence: op.TruncateSequence,
			PayloadLength: op.PayloadLength, Result: op.Result, Data: op.Data,
			Xattr:     OSDXattrResult{NameLength: op.Xattr.NameLength, ValueLength: op.Xattr.ValueLength, ComparisonOp: op.Xattr.ComparisonOp, ComparisonMode: op.Xattr.ComparisonMode},
			Class:     OSDClassResult{ClassLength: op.Class.ClassLength, MethodLength: op.Class.MethodLength, Argc: op.Class.Argc, InputLength: op.Class.InputLength, OutputLength: op.Class.OutputLength},
			AllocHint: OSDAllocHint{ExpectedObjectSize: op.AllocHint.ExpectedObjectSize, ExpectedWriteSize: op.AllocHint.ExpectedWriteSize, Flags: OSDAllocHintFlags(op.AllocHint.Flags)},
			Watch:     OSDWatchInput{Cookie: op.Watch.Cookie, Version: op.Watch.Version, NotifyID: op.Watch.NotifyID, Generation: op.Watch.Generation, TimeoutSeconds: op.Watch.TimeoutSeconds, ProtocolVersion: op.Watch.ProtocolVersion, Action: op.Watch.Action},
			Snapshot:  op.Snap, Checksum: OSDChecksumResult{ChunkSize: op.Checksum.ChunkSize, Type: op.Checksum.Type}}
		if op.Code == osd.SparseRead && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].SparseRead = publicSparseRead(op.Data)
		}
		if op.Code == osd.MapExt && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Extents = publicMapExt(op.Data)
		}
		if op.Code == osd.Checksum && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Checksum = publicChecksum(op.Checksum, op.Data)
		}
		if op.Code == osd.ListWatchers && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Watchers = publicWatchers(op.Data)
		}
		if op.Code == osd.ListSnaps && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() {
			result.Operations[i].Snapshots = publicSnapshotList(op.Data)
		}
		if op.Code == osd.Isdirty && r.Result >= 0 && op.Result >= 0 && r.Redirect.Empty() && len(op.Data) == 1 {
			result.Operations[i].Dirty = op.Data[0] != 0
		}
	}
	return result
}

func copyFromWireInput(op OSDOperation) osd.CopyFromInput {
	if op.Code == OSDSetRedirect {
		r := op.Redirect
		return osd.CopyFromInput{Object: r.TargetName, Locator: osd.Locator{Pool: r.TargetPool, Key: r.TargetKey, Namespace: r.TargetNamespace, Hash: -1}, Version: r.TargetVersion}
	}
	in := op.CopyFrom
	if in == (OSDCopyFromInput{}) {
		return osd.CopyFromInput{}
	}
	out := osd.CopyFromInput{Object: in.SourceName, Locator: osd.Locator{Pool: in.SourcePool, Key: in.SourceKey, Namespace: in.SourceNamespace, Hash: -1}, Snap: in.SourceSnapshot, Version: in.SourceVersion, SourceFadvise: uint32(in.SourceFadvise)}
	if in.TruncateSequence != 0 || in.TruncateSize != 0 {
		out.Flags = osd.CopyFromTruncateSequence
		out.TruncateSequence, out.TruncateSize = in.TruncateSequence, in.TruncateSize
	}
	return out
}

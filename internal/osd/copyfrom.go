package osd

import (
	"errors"
	"math"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// COPY_FROM and COPY_FROM2 are independently translated from Ceph 20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:307-308,
// 518-533 and 631-644, osdc/Objecter.h:1520-1544 and
// osd/PrimaryLogPG.cc:8106-8197. Those sources carry LGPL-2.1 notices; no
// native implementation is copied.
const (
	CopyFrom  uint16 = 0x221a
	CopyFrom2 uint16 = 0x222d

	// CopyFromTruncateSequence tells a COPY_FROM2 destination to use the
	// supplied truncate sequence and size instead of the source's (rados.h
	// CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ).
	CopyFromTruncateSequence uint8 = 32
	// CopyFromIgnoreOverlay, CopyFromIgnoreCache and CopyFromMapSnapClone
	// (rados.h CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY, IGNORE_CACHE and
	// MAP_SNAP_CLONE) only change how the destination OSD reads the source.
	// _copy_some turns each into the CEPH_OSD_FLAG_* bit of its own Objecter
	// read; nothing else in the copy path reads them
	// (ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc).
	//
	// IGNORE_OVERLAY keeps that Objecter from redirecting the read to the
	// source pool's read tier (Objecter::_calc_target,
	// ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc), so the base pool is read
	// even where a writeback tier holds newer data. IGNORE_CACHE makes the
	// source PG skip maybe_handle_cache: a cache-tier miss is -ENOENT instead
	// of a promotion. Neither changes a read of a pool without an overlay or a
	// cache mode.
	//
	// MAP_SNAP_CLONE makes find_object_context take the snap id as a clone id:
	// an id above the source snapset seq still selects the head, an id at or
	// below it must be one of the snapset's clones exactly (-ENOENT otherwise)
	// and the clone's own snapshot list and the OSDMap removed_snaps_queue are
	// not consulted, so a clone whose snapshots are removed but not yet
	// trimmed is still read (PrimaryLogPG.cc find_object_context).
	CopyFromIgnoreOverlay uint8 = 2
	CopyFromIgnoreCache   uint8 = 4
	CopyFromMapSnapClone  uint8 = 8

	// copyFromFlush and copyFromRWOrdered are the two defined flags that stay
	// refused. FLUSH makes the source read skip maybe_await_blocked_head and
	// the object's read/write lock when the source PG has a cache flush of
	// that object in progress, and answers -EINVAL otherwise (do_op); it is
	// the cache tier's own flush path, never a client copy. RWORDERED orders
	// the source read as a write: an exclusive lock, a wait for a degraded
	// source, and a silent drop while the OSD is failsafe full or the pool was
	// marked full after the read's map (do_op, get_rw_locks), which would leave
	// the copy unanswered. Its result on a quiescent source equals the
	// flagless copy, so no native observation separates it.
	copyFromFlush     uint8 = 1
	copyFromRWOrdered uint8 = 16

	// copyFromReadFlags are the admitted source-read flags of both opcodes.
	copyFromReadFlags = CopyFromIgnoreOverlay | CopyFromIgnoreCache | CopyFromMapSnapClone
)

// CopyFromInput is the COPY_FROM/COPY_FROM2 union and indata. The source is
// named by an explicit pool, key and namespace; the OSD maps it to its own PG.
// Snap zero (or NoSnap) selects the head and 1 through MaxSnap a snapshot the
// OSD resolves to the covering clone. Version zero disables the source version
// check. The admitted flags are COPY_FROM2's TRUNCATE_SEQ and, on both
// opcodes, the source-read flags IGNORE_OVERLAY, IGNORE_CACHE and
// MAP_SNAP_CLONE (see their constants); SourceFadvise admits the source-read
// FADVISE hints. MAP_SNAP_CLONE needs a snap id other than the head, where it
// changes nothing. Flagless COPY_FROM2 omits the unused 12-byte
// truncate tail emitted by the native Objecter builder, which PrimaryLogPG
// decodes only with TRUNCATE_SEQ set, so it is not byte-identical to the
// native request.
//
// The copy flags FLUSH and RWORDERED stay refused (see copyFromFlush), as do
// the undefined bits 64 and 128; the pinned handler's check of unknown bits
// applies to COPY_FROM2 only, and COPY_FROM passes every bit to the source
// read (ceph/ceph/blob/v20.2.4/src/include/rados.h,
// src/osd/PrimaryLogPG.cc). SnapDir and the (1, all ones) truncate pair the
// OSD rewrites to (0, 0) are refused. A nonzero version pins the source
// version, which the OSD checks like ASSERT_VER. A CopyFrom input on any
// opcode but COPY_FROM, COPY_FROM2 and SET_REDIRECT is refused, so no
// standalone branch silently discards a source.
//
// The OSD reads the source with its own Objecter at the requested snap id,
// so find_object_context resolves it like a client read: an id above the
// source snapset seq (MaxSnap, an id the pool never allocated, a snapshot
// removed after the source's last write, pending trim or trimmed) selects the
// head and its user version. At or below that seq, an id the covering clone
// does not list, a snapshot in the OSDMap removed_snaps_queue (pending trim)
// and one whose clone was trimmed are -ENOENT. SnapDir
// stays refused here; the pinned OSD would fail that read with -EINVAL. Every
// source failure, including the version assertion (1 below the source is
// -ERANGE, the largest value -EOVERFLOW), comes after the copy started:
// aggregate code, per-op -EINPROGRESS, destination unchanged. A source equal
// to the destination at its head is -EINVAL before the copy starts (aggregate
// and per-op); at any other snap id ValidateCopyFromRequest refuses it before
// transmission. Sources: ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc
// (do_osd_ops COPY_FROM, start_copy, _copy_some, find_object_context,
// do_op).
//
// Without TRUNCATE_SEQ the destination receives the source's truncate pair;
// with it the caller's pair is used unconditionally and the data is not
// truncated. The destination SnapContext and ORDERSNAP apply to the
// destination head like any write, and the source snapset is not copied.
// filter_snapc drops the context's removed snapshots that the PG still holds
// in its snap_trimq or purged_snaps, but not its seq: only removed snapshots
// clone nothing and still raise the snapset seq, and a removed newest snapshot
// over a live one clones the pre-copy head under the seq (the removed id)
// holding only the live snapshots. Once the MON stopped queueing a trimmed
// snapshot the PG drops it from purged_snaps (PG.cc on_active_advmap), and a
// context naming it then can clone the pre-copy head holding that snapshot,
// readable at that id right after the copy. Whether a later purged-snapshot
// scrub requeues and trims that clone is not verified
// (ceph/ceph/blob/v20.2.4/src/osd/PG.cc filter_snapc, PrimaryLogPG.cc
// execute_ctx and make_writeable). The
// destination gets the source's user version, which can be lower than its
// previous one. A copy that fails after the OSD started it reports its
// aggregate code with per-op -EINPROGRESS. The OSD omits a source's empty
// OMAP key even on success (PrimaryLogPG::do_copy_get starts its OMAP walk
// with UPPER_BOUND at the empty cursor); the client neither inspects nor
// restores source OMAP.
type CopyFromInput struct {
	Object           string
	Locator          Locator
	Snap             uint64
	Version          uint64
	Flags            uint8
	TruncateSequence uint32
	TruncateSize     uint64
	// SourceFadvise is the source read's FADVISE hint mask (COPY_FROM and
	// COPY_FROM2 src_fadvise_flags); SET_REDIRECT carries none.
	SourceFadvise uint32
}

// CopyFromResult preserves the echoed union (snapid, source version, copy
// flags and source fadvise flags).
type CopyFromResult struct {
	Snap, Version uint64
	Flags         uint8
	Fadvise       uint32
}

// SET_REDIRECT (rados.h 0x2227) and UNSET_MANIFEST (0x222a) are translated
// from the same pinned sources: osdc/Objecter.h set_redirect/unset_manifest
// and osd/PrimaryLogPG.cc do_osd_ops. SET_REDIRECT carries the COPY_FROM
// union (target snap and version) and indata (target object and locator),
// without WITH_REFERENCE; reference-counted redirects are not implemented.
// The OSD truncates the object, sets its user version to the target version
// and serves its reads (and proxies its writes) from the target in any pool.
// A proxied read echoes the requested extent length rather than the returned
// byte count, so the read validator accepts either. UNSET_MANIFEST clears the
// manifest and leaves a plain object; on a chunked object the OSD drops the
// chunk references after commit, without a completion to the caller. The
// redirected object must exist (-ENOENT) and must not target itself
// (-EINVAL); UNSET_MANIFEST on a plain object is -EOPNOTSUPP. Both are
// standalone and flagless.
const (
	SetRedirect   uint16 = 0x2227
	UnsetManifest uint16 = 0x222a
)

func usesCopyFromUnion(code uint16) bool {
	return code == CopyFrom || code == CopyFrom2 || code == SetRedirect
}

// validateRedirectOperation admits a flagless head redirect: the target
// object, locator (no hash) and version; COPY_FROM's flags do not apply.
func validateRedirectOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if op.CopyFrom.Flags != 0 || op.CopyFrom.SourceFadvise != 0 || op.CopyFrom.TruncateSequence != 0 || op.CopyFrom.TruncateSize != 0 {
		return 0, 0, ErrOperation
	}
	redirect := op
	redirect.Code = CopyFrom
	return validateCopyFromOperation(redirect, limit)
}

// ValidateCopyFromRequest refuses a COPY_FROM or COPY_FROM2 whose source is
// the destination object itself (same pool, name, namespace and locator key
// as hobject_t stores it) at a snap id other than the head. The pinned OSD
// answers the head form with -EINVAL before starting the copy (its source
// hobject equals the destination), so that form is sent. At any other snap id
// the two hobjects differ: start_copy blocks the destination head and its own
// Objecter reads the source snap from the same PG, where do_op's
// maybe_await_blocked_head parks that read until the head is unblocked, which
// only the copy's completion does. The request would then stay unanswered and
// every later operation on the object would wait behind it until an interval
// change (ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc do_osd_ops
// COPY_FROM, start_copy, _copy_some, do_op; no native test sends it). The destination hash is not compared: a
// request whose hash disagrees with its name is misrouted anyway.
func ValidateCopyFromRequest(r Request) error {
	for _, op := range r.Operations {
		if op.Code != CopyFrom && op.Code != CopyFrom2 {
			continue
		}
		in := op.CopyFrom
		if in.Snap != 0 && in.Snap != NoSnap && in.Locator.Pool == r.Locator.Pool && in.Locator.Namespace == r.Locator.Namespace &&
			in.Object == r.Object && hobjectKey(in.Locator.Key, in.Object) == hobjectKey(r.Locator.Key, r.Object) {
			return ErrRequest
		}
	}
	return nil
}

func copyFromTruncates(input CopyFromInput) bool {
	return input.Flags&CopyFromTruncateSequence != 0
}

func validateCopyFromOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Snap != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) {
		return 0, 0, ErrRequest
	}
	// FAILOK has no meaning for a standalone vector and EXCL is refused for
	// every opcode but CREATE.
	if op.Flags != 0 {
		return 0, 0, ErrOperation
	}
	input := op.CopyFrom
	if input.SourceFadvise&^OpFadviseMask != 0 {
		return 0, 0, ErrOperation
	}
	if input.Snap > MaxSnap && input.Snap != NoSnap || input.Object == "" || input.Locator.Pool < 0 || input.Locator.Hash != -1 || strings.IndexByte(input.Object, 0) >= 0 {
		return 0, 0, ErrRequest
	}
	switch {
	case input.Flags&^(CopyFromTruncateSequence|copyFromReadFlags) != 0:
		return 0, 0, ErrOperation
	case copyFromTruncates(input) && op.Code != CopyFrom2:
		return 0, 0, ErrOperation
	case !copyFromTruncates(input) && (input.TruncateSequence != 0 || input.TruncateSize != 0):
		return 0, 0, ErrRequest
	// find_object_context returns the head for CEPH_NOSNAP before it looks
	// at the flag, so MAP_SNAP_CLONE names no clone there.
	case input.Flags&CopyFromMapSnapClone != 0 && (input.Snap == 0 || input.Snap == NoSnap):
		return 0, 0, ErrRequest
	}
	// A size without a sequence has no effect on the OSD, and (1, all-ones)
	// is rewritten to (0, 0) there; both are refused like extent fields.
	if copyFromTruncates(input) && (input.TruncateSequence == 0 || input.TruncateSequence == 1 && input.TruncateSize == math.MaxUint64) {
		return 0, 0, ErrRequest
	}
	size, err := boundedBytes(limit, copyFromDataBytes(op))
	if err != nil {
		return 0, 0, err
	}
	return FlagWrite, size, nil
}

func copyFromLocatorBytes(loc Locator) uint64 {
	// struct header 6 + pool 8 + preferred 4 + key/namespace prefixes 8 + hash 8.
	return 6 + 8 + 4 + 4 + uint64(len(loc.Key)) + 4 + uint64(len(loc.Namespace)) + 8
}

func copyFromDataBytes(op Operation) uint64 {
	n := 4 + uint64(len(op.CopyFrom.Object)) + copyFromLocatorBytes(op.CopyFrom.Locator)
	if copyFromTruncates(op.CopyFrom) {
		n += 12
	}
	return n
}

func copyFromSnap(input CopyFromInput) uint64 {
	if input.Snap == 0 {
		return NoSnap
	}
	return input.Snap
}

func encodeCopyFromUnion(e *wire.Encoder, op Operation) {
	e.U64(copyFromSnap(op.CopyFrom))
	e.U64(op.CopyFrom.Version)
	e.U8(op.CopyFrom.Flags)
	e.U32(op.CopyFrom.SourceFadvise)
	e.Raw(make([]byte, 7))
}

func appendCopyFromData(data []byte, op Operation) []byte {
	e := wire.Encoder{}
	e.String(op.CopyFrom.Object)
	encodeLocator(&e, op.CopyFrom.Locator)
	if copyFromTruncates(op.CopyFrom) {
		e.U32(op.CopyFrom.TruncateSequence)
		e.U64(op.CopyFrom.TruncateSize)
	}
	return append(data, e.Data...)
}

func decodeCopyFromUnion(d *wire.Decoder) CopyFromResult {
	r := CopyFromResult{Snap: d.U64(), Version: d.U64(), Flags: d.U8(), Fadvise: d.U32()}
	for _, b := range d.Raw(7) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return r
}

// ValidateCopyFromReply requires the original union and no output on success
// or failure: the daemon keeps the request union and returns no payload.
func ValidateCopyFromReply(requested Operation, reply OperationResult) error {
	if !usesCopyFromUnion(requested.Code) {
		return ErrOperation
	}
	want := CopyFromResult{Snap: copyFromSnap(requested.CopyFrom), Version: requested.CopyFrom.Version, Flags: requested.CopyFrom.Flags, Fadvise: requested.CopyFrom.SourceFadvise}
	if reply.Code != requested.Code || reply.CopyFrom != want || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.PayloadLength != 0 || len(reply.Data) != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.AllocHint != (AllocHintInput{}) || reply.Checksum != (ChecksumResult{}) || reply.Snap != 0 || reply.WriteSamePatternLength != 0 {
		return errors.New("OSD copy-from reply differs from submitted operation")
	}
	return nil
}

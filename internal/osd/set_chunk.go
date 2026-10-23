package osd

import (
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// SET_CHUNK is independently translated from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h (opcode 0x8228,
// mode CACHE, and CEPH_OSD_OP_FLAG_WITH_REFERENCE 0x80), osdc/Objecter.h
// set_chunk (the indata), tools/rados/rados.cc set-chunk (the native request
// flags) and osd/PrimaryLogPG.cc do_osd_ops, inc_refcount_by_set and
// finish_set_manifest_refcount (the handler). Those sources carry LGPL-2.1
// notices; no native implementation is copied. See MANIFEST_CONTRACT.md.
//
// The packed union is zero and the indata is u64 src_offset, u64 src_length,
// object_locator_t of the target, object_t target name and u64 tgt_offset.
// WITH_REFERENCE is a packed per-operation flag, not a request flag. Native
// callers submit SET_CHUNK as a read operation: the request carries READ and
// no mtime. On success the OSD has its own Objecter increment the target's
// cls_cas reference before it records the chunk in the source's manifest, so a
// lost reply leaves an unknown outcome that is never replayed.
const (
	SetChunk uint16 = 0x8228

	// OpWithReference is CEPH_OSD_OP_FLAG_WITH_REFERENCE. SET_CHUNK requires
	// it; every other opcode refuses it.
	OpWithReference uint32 = 0x80
)

// SetChunkInput is the active input of SET_CHUNK: the source extent and the
// chunk target named by an explicit locator (no hash), object and offset.
//
// Only SourceOffset zero is admitted. finish_set_manifest_refcount records a
// failed reference increment under the chunk offset and under key 0; at any
// other offset one chunk then yields two results, the request never completes
// and the source object stays blocked on the OSD (source-derived, not
// observed). A target equal to the source is refused for the same reason: the
// OSD's own reference increment would wait on the source it has blocked.
type SetChunkInput struct {
	SourceOffset, SourceLength uint64
	Target                     Locator
	TargetObject               string
	TargetOffset               uint64
}

// validateSetChunkOperation admits one standalone SET_CHUNK with exactly the
// WITH_REFERENCE per-operation flag and an offset-zero, non-empty source
// extent. It contributes READ, as the native read submission does.
func validateSetChunkOperation(op Operation, total int, limit uint32) (uint64, uint64, error) {
	if total != 1 || op.Flags != OpWithReference {
		return 0, 0, ErrOperation
	}
	if op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Version != 0 ||
		op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) {
		return 0, 0, ErrRequest
	}
	in := op.SetChunk
	if in.SourceOffset != 0 {
		return 0, 0, ErrOperation
	}
	if in.SourceLength == 0 || in.TargetObject == "" || strings.IndexByte(in.TargetObject, 0) >= 0 || in.Target.Pool < 0 || in.Target.Hash != -1 {
		return 0, 0, ErrRequest
	}
	size, err := boundedBytes(limit, setChunkDataBytes(in))
	if err != nil {
		return 0, 0, err
	}
	return FlagRead, size, nil
}

func setChunkDataBytes(in SetChunkInput) uint64 {
	return 8 + 8 + copyFromLocatorBytes(in.Target) + 4 + uint64(len(in.TargetObject)) + 8
}

func appendSetChunkData(data []byte, in SetChunkInput) []byte {
	e := wire.Encoder{}
	e.U64(in.SourceOffset)
	e.U64(in.SourceLength)
	encodeLocator(&e, in.Target)
	e.String(in.TargetObject)
	e.U64(in.TargetOffset)
	return append(data, e.Data...)
}

// hobjectKey is the locator key as hobject_t stores it: a key equal to the
// object name is the same object as no key.
func hobjectKey(key, name string) string {
	if key == name {
		return ""
	}
	return key
}

// IsSetChunk reports a vector containing SET_CHUNK.
func IsSetChunk(ops []Operation) bool {
	for _, op := range ops {
		if op.Code == SetChunk {
			return true
		}
	}
	return false
}

// ValidateSetChunkRequest enforces the request-level contract of SET_CHUNK:
// a standalone head operation with exactly the native request (no snapshot
// context, replica-read, ordering, lock, redirect, cache, overlay, full or
// retry flag and no mtime) whose target is not the source object itself. The
// public caller additionally refuses explicit resubmission and erasure-coded
// placement.
func ValidateSetChunkRequest(r Request) error {
	if !IsSetChunk(r.Operations) {
		return nil
	}
	if len(r.Operations) != 1 {
		return ErrOperation
	}
	if r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.SnapSeq != 0 || len(r.Snaps) != 0 || r.OrderSnap || r.EnforceSnapContext ||
		r.BalanceReads || r.LocalizeReads || r.IgnoreRedirect || r.OrderReadsWrites || r.SkipRWLocks || r.IgnoreCache || r.IgnoreOverlay ||
		r.FullTry || r.FullForce || r.RetryAttempt != 0 || r.MTimeSeconds != 0 || r.MTimeNanoseconds != 0 {
		return ErrRequest
	}
	in := r.Operations[0].SetChunk
	if in.Target.Pool == r.Locator.Pool && in.Target.Namespace == r.Locator.Namespace && in.TargetObject == r.Object &&
		hobjectKey(in.Target.Key, in.TargetObject) == hobjectKey(r.Locator.Key, r.Object) {
		return ErrRequest
	}
	return nil
}

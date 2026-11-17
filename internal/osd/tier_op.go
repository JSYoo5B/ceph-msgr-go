package osd

// Manifest tier operations, independently translated from Ceph v20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h (opcode
// values and modes), osdc/Objecter.h (the inputless builders tier_promote,
// tier_flush and tier_evict), tools/rados/rados.cc tier-promote, tier-flush
// and tier-evict (the native request flags) and osd/PrimaryLogPG.cc
// do_osd_ops, start_copy, process_copy_chunk_manifest, finish_promote,
// start_flush, start_dedup and finish_set_dedup (the handlers). Those sources
// carry LGPL-2.1 notices; no native implementation is copied. See
// MANIFEST_CONTRACT.md.
//
// None carries input: the packed union and payload are zero. TIER_PROMOTE is
// a write-mode opcode that native callers submit as a write (WRITE, with the
// caller's mtime). TIER_FLUSH and TIER_EVICT are CACHE-mode opcodes that
// `rados tier-flush` and `rados tier-evict` submit as read operations (READ)
// with IGNORE_CACHE and IGNORE_OVERLAY and no mtime. Every handler refuses a
// tier pool (EINVAL) and an absent object (ENOENT). TIER_PROMOTE copies a
// redirect target, or every chunk target of a chunked manifest, into the
// object through the OSD's own client; a plain object is a no-op. TIER_EVICT
// marks every chunk missing and punches its local range, and refuses a plain
// object (EINVAL). TIER_FLUSH on a dirty or plain object runs the dedup flush
// configured on the pool (EINVAL without it); its reply is sent when the
// chunk references are taken, right after the OSD submits the transaction
// that records the new manifest and clears dirty. Successful replies require
// ONDISK without changing their wire mode; that bit identifies no state
// transition. A lost reply leaves an unknown outcome that is never replayed.
const (
	TierPromote uint16 = 0x2229
	TierFlush   uint16 = 0x822b
	TierEvict   uint16 = 0x822c
)

// IsTierOperation reports a manifest tier promote, flush or evict opcode.
func IsTierOperation(code uint16) bool {
	switch code {
	case TierPromote, TierFlush, TierEvict:
		return true
	}
	return false
}

// IsTierRequest reports a vector containing a manifest tier operation.
func IsTierRequest(ops []Operation) bool {
	for _, op := range ops {
		if IsTierOperation(op.Code) {
			return true
		}
	}
	return false
}

// validateTierOperation admits one standalone inputless tier operation with
// no per-op flags. TIER_PROMOTE contributes WRITE; flush and evict READ.
func validateTierOperation(op Operation, total int) (uint64, error) {
	if total != 1 {
		return 0, ErrOperation
	}
	if op.Flags != 0 || op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 ||
		op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Version != 0 || op.Snap != 0 ||
		op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) ||
		op.AllocHint != (AllocHintInput{}) || op.CopyFrom != (CopyFromInput{}) || op.SetChunk != (SetChunkInput{}) {
		return 0, ErrOperation
	}
	if op.Code == TierPromote {
		return FlagWrite, nil
	}
	return FlagRead, nil
}

// ValidateTierRequest enforces the request-level contract of a tier
// operation: a standalone head object without snapshot context, replica-read,
// ordering, lock, redirect, full or retry flags, and exactly the native
// request flags. No feature bit is required: the OSD checks OSD_CACHEPOOL only
// for pools in a cache-tier relation, and every tier handler refuses a tier
// pool. IGNORE_CACHE and IGNORE_OVERLAY are sent only to match the native
// flush and evict requests; on a pool without a cache mode neither changes how
// the OSD serves them. The public caller additionally refuses explicit
// resubmission and erasure-coded placement.
func ValidateTierRequest(r Request) error {
	if !IsTierRequest(r.Operations) {
		return nil
	}
	if len(r.Operations) != 1 {
		return ErrOperation
	}
	if r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.SnapSeq != 0 || len(r.Snaps) != 0 || r.OrderSnap || r.EnforceSnapContext ||
		r.BalanceReads || r.LocalizeReads || r.IgnoreRedirect || r.OrderReadsWrites || r.SkipRWLocks || r.FullTry || r.FullForce || r.RetryAttempt != 0 {
		return ErrRequest
	}
	switch r.Operations[0].Code {
	case TierPromote:
		if !r.IgnoreCache && !r.IgnoreOverlay {
			return nil
		}
	case TierFlush, TierEvict:
		if r.IgnoreCache && r.IgnoreOverlay && r.MTimeSeconds == 0 && r.MTimeNanoseconds == 0 {
			return nil
		}
	}
	return ErrRequest
}

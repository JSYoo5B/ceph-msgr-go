package osd

// Cache-tier operations, independently translated from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h (opcode and flag
// values), osdc/Objecter.h (the inputless builders cache_flush,
// cache_try_flush, cache_evict, cache_pin, cache_unpin), tools/rados/rados.cc
// and librados_util.cc translate_flags (the native request flags) and
// osd/PrimaryLogPG.cc do_osd_ops (the handlers).
//
// None carries input: the packed union and payload are zero. CACHE_FLUSH,
// CACHE_TRY_FLUSH and CACHE_EVICT are CACHE-mode opcodes that native callers
// submit as read operations (READ) with IGNORE_CACHE and IGNORE_OVERLAY;
// try-flush and evict add SKIPRWLOCKS. The flush handlers check the lock mode
// that flag selects: try-flush without it and flush with it return EINVAL.
// CACHE_PIN and CACHE_UNPIN are write-mode opcodes (WRITE). IGNORE_OVERLAY is
// read by the native Objecter when it chooses a target; the caller here
// already supplies the target, and the flag is sent only to match the native
// request. Every handler mutates the tier object (flush clears dirty after a
// write to the base pool, evict deletes the cached head, pin sets a flag), so
// a lost reply leaves an unknown outcome that is never replayed.
const (
	CacheFlush    uint16 = 0x821f
	CacheEvict    uint16 = 0x8220
	CacheTryFlush uint16 = 0x8221
	CachePin      uint16 = 0x2224
	CacheUnpin    uint16 = 0x2225

	// FlagIgnoreOverlay is CEPH_OSD_FLAG_IGNORE_OVERLAY.
	FlagIgnoreOverlay uint64 = 0x20000
)

// IsCacheOperation reports a cache-tier flush, evict, pin or unpin opcode.
func IsCacheOperation(code uint16) bool {
	switch code {
	case CacheFlush, CacheEvict, CacheTryFlush, CachePin, CacheUnpin:
		return true
	}
	return false
}

// validateCacheOperation admits one standalone inputless cache operation with
// no per-op flags. Flush, try-flush and evict contribute READ; pin and unpin
// WRITE.
func validateCacheOperation(op Operation, total int) (uint64, error) {
	if total != 1 {
		return 0, ErrOperation
	}
	if op.Flags != 0 || op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 ||
		op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Version != 0 || op.Snap != 0 ||
		op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) ||
		op.AllocHint != (AllocHintInput{}) || op.CopyFrom != (CopyFromInput{}) {
		return 0, ErrOperation
	}
	if op.Code == CachePin || op.Code == CacheUnpin {
		return FlagWrite, nil
	}
	return FlagRead, nil
}

// ValidateCacheRequest enforces the request-level contract of a cache
// operation: the negotiated cache-pool capability, a standalone head object
// without snapshot context, replica-read, ordering, redirect, full or retry
// flags, and exactly the native request flags. IgnoreOverlay is refused on
// every other request. The public caller additionally refuses explicit
// resubmission and erasure-coded placement.
func ValidateCacheRequest(r Request, features uint64) error {
	cache := false
	for _, op := range r.Operations {
		cache = cache || IsCacheOperation(op.Code)
	}
	if !cache {
		if r.IgnoreOverlay {
			return ErrRequest
		}
		return nil
	}
	if features&FeatureCachePool == 0 {
		return ErrFeatures
	}
	if len(r.Operations) != 1 {
		return ErrOperation
	}
	if r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.SnapSeq != 0 || len(r.Snaps) != 0 || r.OrderSnap || r.EnforceSnapContext ||
		r.BalanceReads || r.LocalizeReads || r.IgnoreRedirect || r.OrderReadsWrites || r.FullTry || r.FullForce || r.RetryAttempt != 0 {
		return ErrRequest
	}
	switch r.Operations[0].Code {
	case CacheFlush:
		if r.IgnoreCache && r.IgnoreOverlay && !r.SkipRWLocks {
			return nil
		}
	case CacheTryFlush, CacheEvict:
		if r.IgnoreCache && r.IgnoreOverlay && r.SkipRWLocks {
			return nil
		}
	case CachePin, CacheUnpin:
		if !r.IgnoreOverlay && !r.SkipRWLocks {
			return nil
		}
	}
	return ErrRequest
}

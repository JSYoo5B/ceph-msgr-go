package osd

// Pinned Ceph 20.2.4 include/ceph_features.h OSD_CACHEPOOL (incarnation 1)
// and include/rados.h CEPH_OSD_FLAG_IGNORE_CACHE. These are independent of
// request/reply encoding versions and of object-store fadvise hints.
const FeatureCachePool uint64 = 1 << 35
const FlagIgnoreCache uint64 = 0x8000

// ValidateIgnoreCache enforces the initial standalone head-object contract.
// False leaves every existing request policy unchanged. The public caller
// additionally checks its opt-in, explicit resubmission and EC placement.
func ValidateIgnoreCache(r Request, features uint64) error {
	if !r.IgnoreCache {
		return nil
	}
	if features&FeatureCachePool == 0 {
		return ErrFeatures
	}
	if len(r.Operations) != 1 || r.ReadSnap != 0 && r.ReadSnap != NoSnap || r.SnapSeq != 0 || len(r.Snaps) != 0 ||
		r.OrderSnap || r.EnforceSnapContext || r.BalanceReads || r.LocalizeReads || r.SkipRWLocks || r.IgnoreRedirect || r.OrderReadsWrites || r.RetryAttempt != 0 {
		return ErrRequest
	}
	op := r.Operations[0]
	if op.Flags != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 {
		return ErrRequest
	}
	switch op.Code {
	case Stat, Read, WriteFull, Delete:
		return nil
	default:
		return ErrOperation
	}
}

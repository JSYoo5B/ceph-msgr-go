package osd

// Pinned Ceph 20.2.4 include/ceph_features.h OSD_CACHEPOOL (incarnation 1)
// and include/rados.h CEPH_OSD_FLAG_IGNORE_CACHE. These are independent of
// request/reply encoding versions and of object-store fadvise hints.
// EnableOSDCachePools adds bit 35 to the supported and required features of
// MON and OSD sessions only; a peer without it fails the handshake with no
// weaker retry. The wire flag is 0x8000, not the librados operation flag 8
// the native client translates, and it is never set on a packed operation.
const FeatureCachePool uint64 = 1 << 35
const FlagIgnoreCache uint64 = 0x8000

// ValidateIgnoreCache enforces the initial standalone head-object contract:
// one STAT, READ, WRITEFULL or DELETE with no snapshot, replica-read, lock,
// redirect, ordering, per-op flag, truncate or retry input. The flag bypasses
// the tier's promotion and proxy path; it neither selects the base pool nor
// stops dirty tracking, and a DELETE may leave a snapshot whiteout rather than
// removing the object. False leaves every existing request policy unchanged.
// The public caller additionally checks its opt-in, explicit resubmission and
// EC placement. Live acceptance covers the legacy-v6 profile; FULL_TRY or
// FULL_FORCE with IgnoreCache keep their ordinary validation and are not
// verified live.
func ValidateIgnoreCache(r Request, features uint64) error {
	if !r.IgnoreCache {
		return nil
	}
	// A cache-tier operation carries its own request contract.
	if len(r.Operations) == 1 && IsCacheOperation(r.Operations[0].Code) {
		return ValidateCacheRequest(r, features)
	}
	// So does a manifest tier operation, which needs no cache-pool capability.
	if IsTierRequest(r.Operations) {
		return ValidateTierRequest(r)
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

package osd

// requestFullFlags validates and returns the full-pool request flags.
// Independently translated from Ceph 20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:479-480 and
// osd/PrimaryLogPG.cc:2138-2159,9014-9030 (LGPL-2.1; no code copied).
// Without either flag a write to a full pool is discarded or dropped by the
// OSD and never answered. FULL_TRY makes the OSD reply -EDQUOT or -ENOSPC;
// FULL_FORCE proceeds despite the flag. Only a writing vector may carry one.
func requestFullFlags(r Request, flags uint64) (uint64, error) {
	if !r.FullTry && !r.FullForce {
		return 0, nil
	}
	if r.FullTry && r.FullForce || flags&FlagWrite == 0 || IsWatchOperation(r.Operations[0].Code) {
		return 0, ErrRequest
	}
	if r.FullTry {
		return FlagFullTry, nil
	}
	return FlagFullForce, nil
}

// requestReplicaFlags validates and returns the replica-read request flags.
// The OSD (osd/PrimaryLogPG.cc:2058-2073, 2335-2358, osd/PG.cc:1940-1954)
// serves a vector on a non-primary only when one flag is set and the vector
// may read and neither writes nor uses the cache; otherwise it treats the
// request as misdirected and drops it. A replica may answer -EAGAIN when the
// object is unreadable or has an unstable write, which stays a known server
// refusal. Only vectors made of the plain reads below are admitted: watchers,
// PG listing, class calls, snapshot listing and writes are refused locally.
func requestReplicaFlags(r Request, flags uint64) (uint64, error) {
	if !r.BalanceReads && !r.LocalizeReads {
		return 0, nil
	}
	if flags&FlagWrite != 0 {
		return 0, ErrRequest
	}
	for _, op := range r.Operations {
		switch op.Code {
		case Read, Stat, GetXattr, GetXattrs, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, MapExt, Checksum, SparseRead:
		default:
			return 0, ErrOperation
		}
	}
	var out uint64
	if r.BalanceReads {
		out |= FlagBalanceReads
	}
	if r.LocalizeReads {
		out |= FlagLocalizeReads
	}
	return out, nil
}

// MaxRetryAttempt bounds an explicit resubmission count.
const MaxRetryAttempt = 1 << 16

// FlagRWOrdered and FlagIgnoreRedirect are rados.h CEPH_OSD_FLAG_RWORDERED and
// CEPH_OSD_FLAG_IGNORE_REDIRECT (osd/PrimaryLogPG.cc maybe_handle_manifest
// and the read/write ordering of do_op).
const (
	FlagRWOrdered      uint64 = 0x4000
	FlagSkipRWLocks    uint64 = 0x10000
	FlagIgnoreRedirect uint64 = 0x2000000
)

// requestOrderingFlags returns the redirect, ordering and lock flags.
// SKIPRWLOCKS (PrimaryLogPG do_op) is refused on a writing vector: without
// the object lock concurrent writes would not be serialized.
func requestOrderingFlags(r Request, flags uint64) (uint64, error) {
	var out uint64
	if r.SkipRWLocks {
		if flags&FlagWrite != 0 {
			return 0, ErrRequest
		}
		out |= FlagSkipRWLocks
	}
	if r.IgnoreRedirect {
		out |= FlagIgnoreRedirect
	}
	if r.OrderReadsWrites {
		out |= FlagRWOrdered
	}
	return out, nil
}

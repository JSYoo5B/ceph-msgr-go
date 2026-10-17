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

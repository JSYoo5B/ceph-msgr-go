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

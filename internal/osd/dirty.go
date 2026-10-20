package osd

// ISDIRTY and UNDIRTY operate on the committed object_info dirty flag, which an
// ordinary WRITE/WRITEFULL/TRUNCATE/OMAP mutation sets and the cache-tier flush
// path clears. They are independently translated from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h (the opcode values),
// osdc/Objecter.h (the inputless builders cache_evict/undirty and is_dirty) and
// osd/PrimaryLogPG.cc do_osd_ops. Both reach the handler on a plain replicated
// pool; neither requires a cache-tier overlay.
//
// Both ops carry no input: the packed extent union and payload are zero, like
// UNSET_MANIFEST. ISDIRTY's per-op result is always 0 and it reports the dirty
// state as one encoded bool (1 byte) in its reply output, not through the
// result code (PrimaryLogPG.cc:6283-6292). UNDIRTY's per-op result is always 0
// with no output; it sets ctx->undirty so make_writeable clears FLAG_DIRTY on
// commit (PrimaryLogPG.cc:6294-6305, 8650-8662). Any ordinary WRITE sets that
// flag, so a WRITE then ISDIRTY reports dirty and a following UNDIRTY clears it.
// Each is accepted only as a standalone, flagless head operation in this initial
// scope; compound ordering, snapshot interaction and the cache-tier agent paths
// that also emit them are deliberately left to a later contract. The cache-tier
// (CACHE_FLUSH/EVICT/PIN) and dedup-manifest (SET_CHUNK) opcodes are not
// implemented: on a plain replicated pool they return -EINVAL/-EOPNOTSUPP before
// any handler, so they are unreachable without a cache-tier or manifest setup.
const (
	Isdirty uint16 = 0x121d
	Undirty uint16 = 0x221c
)

// validateDirtyOperation admits a standalone inputless ISDIRTY or UNDIRTY. It
// mirrors the UNSET_MANIFEST shape: no flags, no extent fields, no data and no
// union variant. ISDIRTY contributes READ, UNDIRTY WRITE.
func validateDirtyOperation(op Operation, total int) (uint64, error) {
	if total != 1 {
		return 0, ErrOperation
	}
	if op.Flags != 0 || op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 ||
		op.TruncateSize != 0 || op.TruncateSequence != 0 || op.Version != 0 || op.Snap != 0 ||
		op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) ||
		op.AllocHint != (AllocHintInput{}) || op.CopyFrom != (CopyFromInput{}) {
		return 0, ErrOperation
	}
	if op.Code == Isdirty {
		return FlagRead, nil
	}
	return FlagWrite, nil
}

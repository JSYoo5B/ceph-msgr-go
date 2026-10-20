package osd

import (
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// copyFromVectorAllowed admits a leading COPY_FROM/COPY_FROM2 followed only by
// SETXATTR/RMXATTR, and SET_REDIRECT alone.
func copyFromVectorAllowed(ops []Operation) bool {
	if len(ops) == 0 || !usesCopyFromUnion(ops[0].Code) {
		return false
	}
	if ops[0].Code == SetRedirect {
		return len(ops) == 1
	}
	for _, later := range ops[1:] {
		if later.Code != SetXattr && later.Code != RemoveXattr {
			return false
		}
	}
	return true
}

// fadviseFlags is the cache-hint mask an opcode accepts: the extent reads and
// writes whose PrimaryLogPG paths pass op.flags to the object store.
func fadviseFlags(code uint16) uint32 {
	switch code {
	case Read, SparseRead, Checksum, CompareExtent, Write, WriteFull, WriteSame, Append:
		return OpFadviseMask
	}
	return 0
}

func SupportedOperation(code uint16) bool {
	switch code {
	case Read, Stat, Write, WriteFull, WriteSame, CopyFrom, CopyFrom2, SetRedirect, UnsetManifest, Isdirty, Undirty, Append, Truncate, Zero, Create, Delete, AssertVersion, CompareExtent, GetXattr, GetXattrs, CompareXattr, SetXattr, RemoveXattr, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, OmapCompare, OmapSetValues, OmapSetHeader, OmapClear, OmapRemoveKeys, OmapRemoveKeyRange, Call, SetAllocHint, Rollback, ListSnaps, ListWatchers, MapExt, Checksum, SparseRead, PGNLS, PGNLSFilter, ScrubList, Watch, Notify, NotifyAck:
		return true
	}
	return false
}

// ValidateOperations derives the mandatory request mode bits and bounds both
// input and expected output before allocation. Unknown opcodes/flags are never
// sent. Input remains caller-owned until EncodeRequest copies it.
func ValidateOperations(ops []Operation, limit uint32) (uint64, uint64, error) {
	if len(ops) == 0 || len(ops) > MaxOperations {
		return 0, 0, ErrRequest
	}
	if err := validateChecksumCompoundWork(ops, limit); err != nil {
		return 0, 0, err
	}
	var flags, total uint64
	var stagedDigestSize, logicalWriteBytes uint64
	// stagedAny marks a staged write that a later CALL may not follow: every
	// write except a read-mode CALL and a non-FAILOK CREATE.
	var stagedXattr, stagedOmap, stagedAny, afterCall bool
	for _, op := range ops {
		if !SupportedOperation(op.Code) || op.Code == PGNLS || op.Code == PGNLSFilter || op.Code == ScrubList || op.Flags&^(OpExclusive|OpFailOK|fadviseFlags(op.Code)) != 0 || op.Flags&OpExclusive != 0 && op.Code != Create {
			return 0, 0, ErrOperation
		}
		if len(op.Data) > math.MaxUint32 {
			return 0, 0, wire.ErrLimit
		}
		if op.Code != AssertVersion && op.Version != 0 {
			return 0, 0, ErrRequest
		}
		if err := validateTruncateFields(op); err != nil {
			return 0, 0, err
		}
		// Check inactive input before standalone operations can return early.
		// Only these opcodes encode the COPY_FROM union and source payload.
		if !usesCopyFromUnion(op.Code) && op.CopyFrom != (CopyFromInput{}) {
			return 0, 0, ErrRequest
		}
		if !IsWatchOperation(op.Code) && op.Watch != (WatchInput{}) {
			return 0, 0, ErrRequest
		}
		if IsWatchOperation(op.Code) {
			if len(ops) != 1 {
				return 0, 0, ErrOperation
			}
			if op.SparseRead != (SparseReadInput{}) {
				return 0, 0, ErrRequest
			}
			mode, inputBytes, err := ValidateWatchOperation(op, limit)
			if err != nil {
				return 0, 0, err
			}
			flags |= mode
			total += inputBytes
			continue
		}
		if op.Code != SparseRead && op.SparseRead != (SparseReadInput{}) {
			return 0, 0, ErrRequest
		}
		if op.Code == SparseRead {
			if len(ops) != 1 {
				return 0, 0, ErrOperation
			}
			if op.MapExt != (MapExtInput{}) || op.Checksum != (ChecksumInput{}) {
				return 0, 0, ErrRequest
			}
			if err := validateSparseReadOperation(op, limit); err != nil {
				return 0, 0, err
			}
			flags |= FlagRead
			continue
		}
		if op.Code != MapExt && op.MapExt != (MapExtInput{}) || op.Code != Checksum && op.Checksum != (ChecksumInput{}) {
			return 0, 0, ErrRequest
		}
		if op.Code == MapExt || op.Code == Checksum {
			if op.Code == MapExt && len(ops) != 1 {
				return 0, 0, ErrOperation
			}
			if op.ListWatchers != (ListWatchersInput{}) {
				return 0, 0, ErrRequest
			}
			if op.Code == MapExt {
				if err := validateMapExtOperation(op, limit); err != nil {
					return 0, 0, err
				}
			} else {
				if err := validateChecksumOperation(op, limit); err != nil {
					return 0, 0, err
				}
				total += checksumWidth(op.Checksum.Type)
			}
			flags |= FlagRead
			continue
		}
		if op.Code == ListWatchers {
			// Observe committed head registrations only; compound ordering and
			// snapshot watcher semantics are deliberately outside this subset.
			if len(ops) != 1 {
				return 0, 0, ErrOperation
			}
			if err := validateListWatchersOperation(op, limit); err != nil {
				return 0, 0, err
			}
			flags |= FlagRead
			continue
		}
		if op.ListWatchers != (ListWatchersInput{}) {
			return 0, 0, ErrRequest
		}
		if op.Code == Rollback || op.Code == ListSnaps {
			// ROLLBACK replaces or deletes the head through the committed
			// snapset, and LIST_SNAPS must be alone at SNAPDIR. Neither
			// is combined with other operations.
			if len(ops) != 1 {
				return 0, 0, ErrOperation
			}
			mode, err := validateSnapOperation(op, limit)
			if err != nil {
				return 0, 0, err
			}
			flags |= mode
			continue
		}
		if op.Snap != 0 || op.ListSnaps != (ListSnapsInput{}) {
			return 0, 0, ErrRequest
		}
		if op.Code == UnsetManifest {
			// Characterized only as a standalone, inputless operation.
			if len(ops) != 1 || op.Flags != 0 || op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.CopyFrom != (CopyFromInput{}) {
				return 0, 0, ErrOperation
			}
			flags |= FlagWrite
			continue
		}
		if op.Code == Isdirty || op.Code == Undirty {
			// Standalone inputless dirty-flag operations: ISDIRTY reads the
			// committed object_info dirty bit through its signed per-op result,
			// UNDIRTY clears it. Reachable on a plain replicated pool.
			mode, err := validateDirtyOperation(op, len(ops))
			if err != nil {
				return 0, 0, err
			}
			flags |= mode
			continue
		}
		if usesCopyFromUnion(op.Code) {
			// The daemon completes the copy through a restartable finisher
			// and the source may live in another PG; the whole vector runs
			// again in one transaction once the copy is staged. A copy must
			// lead its vector and may be followed only by xattr writes, the
			// characterized compound; SET_REDIRECT stays standalone.
			if !copyFromVectorAllowed(ops) {
				return 0, 0, ErrOperation
			}
			validate := validateCopyFromOperation
			if op.Code == SetRedirect {
				validate = validateRedirectOperation
			}
			mode, inputBytes, err := validate(op, limit)
			if err != nil {
				return 0, 0, err
			}
			if inputBytes > uint64(limit)-total {
				return 0, 0, wire.ErrLimit
			}
			flags |= mode
			total += inputBytes
			continue
		}
		if op.Code == Call {
			// A class method's helpers run as sub-operations of the same
			// transaction (pinned objclass.cc:105-339): stat and the read
			// length see the staged object state, a full-object read is
			// checked against the staged data digest while the bytes come
			// from committed storage, and the method may stage writes itself.
			// A write method followed by a full-reading method fails that
			// digest check; live Tentacle then repaired the primary copy in a
			// loop and never replied. A CALL may therefore follow guards,
			// reads, a non-FAILOK CREATE and read-mode CALLs (the caller's
			// declaration of a read-only method), the native librbd forms
			// create then rbd.set_id and lock.assert_locked then
			// rbd.object_map_update. A CALL after a write-mode CALL or any
			// other staged write, and an explicit read or guard after a
			// CALL, stay rejected.
			if stagedAny {
				return 0, 0, ErrOperation
			}
			mode, inputBytes, err := validateClassOperation(op, limit)
			if err != nil {
				return 0, 0, err
			}
			if inputBytes > uint64(limit)-total {
				return 0, 0, wire.ErrLimit
			}
			flags |= mode
			total += inputBytes
			afterCall, stagedXattr, stagedOmap = true, true, true
			// A FAILOK method may have staged writes before failing, whatever
			// mode the caller declared.
			stagedAny = mode == FlagWrite || op.Flags&OpFailOK != 0
			continue
		}
		if op.Class != (ClassInput{}) {
			return 0, 0, ErrRequest
		}
		if op.Code == SetAllocHint {
			if err := validateAllocHintOperation(op); err != nil {
				return 0, 0, err
			}
			stagedAny = true // a write; admitted after a CALL
			flags |= FlagWrite
			continue
		}
		if op.AllocHint != (AllocHintInput{}) {
			return 0, 0, ErrRequest
		}
		if afterCall && readModeOperation(op.Code) {
			return 0, 0, ErrOperation
		}
		stagedAny = stagedAny || writeOperation(op.Code) && (op.Code != Create || op.Flags&OpFailOK != 0)
		if IsOmapOperation(op.Code) {
			mode, inputBytes, err := validateOmapOperation(op, limit)
			if err != nil {
				return 0, 0, err
			}
			// OMAP writes stage transaction operations while reads consult
			// the backend. Keep reads after any staged OMAP write, including
			// FAILOK, unsupported until independently characterized.
			if stagedOmap && mode == FlagRead {
				return 0, 0, ErrOperation
			}
			stagedOmap = stagedOmap || mode == FlagWrite
			if inputBytes > uint64(limit)-total {
				return 0, 0, wire.ErrLimit
			}
			flags |= mode
			total += inputBytes
			continue
		}
		if !omapInputEmpty(op.Omap) {
			return 0, 0, ErrRequest
		}
		if IsXattrOperation(op.Code) {
			mode, inputBytes, err := validateXattrOperation(op, limit)
			if err != nil {
				return 0, 0, err
			}
			// The replicated native getter reads the committed backend rather
			// than the transaction's staged SET/RM attrs. Keep this ordering
			// unsupported until independently characterized, including FAILOK.
			if stagedXattr && (op.Code == GetXattr || op.Code == GetXattrs || op.Code == CompareXattr) {
				return 0, 0, ErrOperation
			}
			if op.Code == SetXattr || op.Code == RemoveXattr {
				stagedXattr = true
			}
			if inputBytes > uint64(limit)-total {
				return 0, 0, wire.ErrLimit
			}
			flags |= mode
			total += inputBytes
			continue
		}
		if op.Xattr != (XattrInput{}) {
			return 0, 0, ErrRequest
		}
		// Tentacle v20.2.4 can validate a staged full-write digest against
		// committed storage during a following full-object READ/CMPEXT.
		// APPEND may become a full write when the staged object is empty.
		// Keep every possible boundary across intervening FAILOK operations:
		// callers do not supply the original size or remote execution state.
		if (op.Code == Read || op.Code == CompareExtent) && stagedDigestSize > 0 && op.Offset == 0 && op.Length >= stagedDigestSize {
			return 0, 0, ErrOperation
		}
		switch op.Code {
		case Read:
			flags |= FlagRead
			if op.Length > uint64(limit) {
				return 0, 0, wire.ErrLimit
			}
			if op.Length == 0 || len(op.Data) != 0 || op.Offset > math.MaxUint64-op.Length {
				return 0, 0, ErrRequest
			}
		case Stat:
			flags |= FlagRead
			if op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 {
				return 0, 0, ErrRequest
			}
		case AssertVersion:
			flags |= FlagRead
			if op.Offset != 0 || op.Length != 0 || op.Version == 0 || len(op.Data) != 0 {
				return 0, 0, ErrRequest
			}
		case CompareExtent:
			flags |= FlagRead
			if op.Length == 0 || op.Length != uint64(len(op.Data)) || op.Offset > math.MaxUint64-op.Length {
				return 0, 0, ErrRequest
			}
		case WriteSame:
			// The daemon expands the pattern before invoking WRITE. Bound the
			// cumulative logical work independently of the wire input budget.
			if len(op.Data) == 0 || op.Length%uint64(len(op.Data)) != 0 || op.Offset > math.MaxUint64-op.Length {
				return 0, 0, ErrRequest
			}
			if op.Length > uint64(limit)-logicalWriteBytes {
				return 0, 0, wire.ErrLimit
			}
			logicalWriteBytes += op.Length
			flags |= FlagWrite
			// A newer remote truncate sequence can clip the nested WRITE to
			// any smaller whole-object size while the original union is echoed.
			// Refuse every following positive full-prefix READ/CMPEXT: callers
			// do not supply that committed size, even after a staged TRUNCATE.
			if op.Offset == 0 && op.Length > 0 {
				stagedDigestSize = 1
			}

		case Write, WriteFull, Append:
			// A newer remote truncate sequence can clip an offset-zero WRITE
			// below its requested length before setting a full-object digest
			// (pinned PrimaryLogPG.cc:6774-6782,6836-6840). The caller does not
			// supply that remote size: any positive prefix read may cover it.
			// WRITEFULL is not clipped; APPEND copies the current sequence.
			boundary := op.Length
			if op.Code == Write && op.Offset == 0 && boundary > 0 {
				boundary = 1
			}
			if (op.Code == WriteFull || op.Code == Append || op.Code == Write && op.Offset == 0) && boundary > 0 && (stagedDigestSize == 0 || boundary < stagedDigestSize) {
				stagedDigestSize = boundary
			}
			flags |= FlagWrite
			if op.Length != uint64(len(op.Data)) || op.Offset > math.MaxUint64-op.Length || op.Code != Write && op.Offset != 0 {
				return 0, 0, ErrRequest
			}
			if op.Length > uint64(limit)-logicalWriteBytes {
				return 0, 0, wire.ErrLimit
			}
			logicalWriteBytes += op.Length
		case Truncate:
			flags |= FlagWrite
			if op.Length != 0 || len(op.Data) != 0 {
				return 0, 0, ErrRequest
			}
		case Zero:
			flags |= FlagWrite
			if len(op.Data) != 0 || op.Offset > math.MaxUint64-op.Length {
				return 0, 0, ErrRequest
			}
		case Create, Delete:
			flags |= FlagWrite
			if op.Offset != 0 || op.Length != 0 || len(op.Data) != 0 {
				return 0, 0, ErrRequest
			}
		}
		// DELETE resets object state in the transaction, but a later metadata
		// read can still consult committed xattrs/OMAP (new_object keeps the
		// OMAP flag). Treat it as a staged write of both metadata kinds.
		if op.Code == Delete {
			stagedOmap, stagedXattr = true, true
		}
		if uint64(len(op.Data)) > uint64(limit)-total {
			return 0, 0, wire.ErrLimit
		}
		total += uint64(len(op.Data))
	}
	return flags, total, nil
}

func writeOperation(code uint16) bool {
	switch code {
	case Write, WriteFull, WriteSame, CopyFrom, CopyFrom2, SetRedirect, UnsetManifest, Undirty, Truncate, Zero, Delete, Append, Create, SetXattr, RemoveXattr, SetAllocHint, Rollback:
		return true
	}
	return isOmapMutation(code)
}

func readModeOperation(code uint16) bool {
	switch code {
	case Read, Stat, AssertVersion, CompareExtent, GetXattr, GetXattrs, CompareXattr, OmapCompare, Call, ListSnaps, ListWatchers, MapExt, Checksum, SparseRead, Isdirty, PGNLS, PGNLSFilter, ScrubList, Watch, Notify, NotifyAck:
		return true
	}
	return IsOmapOperation(code) && !isOmapMutation(code)
}

// validateTruncateFields admits the extent truncate fields only where native
// CephFS clients send them (Objecter read_trunc, write_trunc and trunc).
// A size without a sequence has no effect on the OSD and is refused, as is
// the (1, all-ones) pair the OSD silently rewrites to (0, 0)
// (PrimaryLogPG.cc:6088-6093). A sequenced TRUNCATE must name its own
// offset: the OSD asserts offset == truncate_size (PrimaryLogPG.cc:6987).
func validateTruncateFields(op Operation) error {
	if op.TruncateSize == 0 && op.TruncateSequence == 0 {
		return nil
	}
	switch op.Code {
	case Read, Write, Truncate:
	default:
		return ErrRequest
	}
	if op.TruncateSequence == 0 || op.TruncateSequence == 1 && op.TruncateSize == math.MaxUint64 {
		return ErrRequest
	}
	if op.Code == Truncate && op.Offset != op.TruncateSize {
		return ErrRequest
	}
	return nil
}

package osd

import (
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func SupportedOperation(code uint16) bool {
	switch code {
	case Read, Stat, Write, WriteFull, Append, Truncate, Zero, Create, Delete, AssertVersion, CompareExtent, GetXattr, GetXattrs, CompareXattr, SetXattr, RemoveXattr, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, OmapCompare, OmapSetValues, OmapSetHeader, OmapClear, OmapRemoveKeys, OmapRemoveKeyRange, Call, SetAllocHint, Rollback, ListSnaps, ListWatchers:
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
	var flags, total uint64
	var stagedDigestSize uint64
	var stagedXattr, stagedOmap, stagedAny, afterCall bool
	for _, op := range ops {
		if !SupportedOperation(op.Code) || op.Flags&^(OpExclusive|OpFailOK) != 0 || op.Flags&OpExclusive != 0 && op.Code != Create {
			return 0, 0, ErrOperation
		}
		if len(op.Data) > math.MaxUint32 {
			return 0, 0, wire.ErrLimit
		}
		if op.Code != AssertVersion && op.Version != 0 {
			return 0, 0, ErrRequest
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
		if op.Code == Call {
			// A class method reads committed or staged state inconsistently
			// (pinned objclass.cc:105-339) and may stage data, xattr and OMAP
			// writes itself. Refuse a CALL after any staged write, and any
			// read, guard or CALL after a CALL, until characterized.
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
			stagedAny, afterCall, stagedXattr, stagedOmap = true, true, true, true
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
		stagedAny = stagedAny || writeOperation(op.Code)
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
		case Write, WriteFull, Append:
			// A WRITE at zero can replace the whole object too; callers do
			// not provide its previous size. Retain the smallest possible
			// staged digest boundary across FAILOK operations.
			if (op.Code == WriteFull || op.Code == Append || op.Code == Write && op.Offset == 0) && op.Length > 0 && (stagedDigestSize == 0 || op.Length < stagedDigestSize) {
				stagedDigestSize = op.Length
			}
			flags |= FlagWrite
			if op.Length != uint64(len(op.Data)) || op.Offset > math.MaxUint64-op.Length || op.Code != Write && op.Offset != 0 {
				return 0, 0, ErrRequest
			}
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
	case Write, WriteFull, Truncate, Zero, Delete, Append, Create, SetXattr, RemoveXattr, SetAllocHint, Rollback:
		return true
	}
	return isOmapMutation(code)
}

func readModeOperation(code uint16) bool {
	switch code {
	case Read, Stat, AssertVersion, CompareExtent, GetXattr, GetXattrs, CompareXattr, OmapCompare, Call, ListSnaps:
		return true
	}
	return IsOmapOperation(code) && !isOmapMutation(code)
}

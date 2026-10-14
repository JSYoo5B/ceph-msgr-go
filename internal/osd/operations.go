package osd

import (
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func SupportedOperation(code uint16) bool {
	switch code {
	case Read, Stat, Write, WriteFull, Append, Truncate, Zero, Create, Delete, AssertVersion, CompareExtent, GetXattr, GetXattrs, CompareXattr, SetXattr, RemoveXattr, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, OmapCompare, OmapSetValues, OmapSetHeader, OmapClear, OmapRemoveKeys:
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
	var stagedXattr, stagedOmap bool
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

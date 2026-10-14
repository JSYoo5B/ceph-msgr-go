package osd

import (
	"errors"
	"math"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Current Tentacle comparison operands are caller-left. Only STRING mode is
// supported: its values are length-delimited bytes, not normalized text.
// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L503
const (
	XattrCompareEqual        uint8 = 1
	XattrCompareNotEqual     uint8 = 2
	XattrCompareGreater      uint8 = 3
	XattrCompareGreaterEqual uint8 = 4
	XattrCompareLess         uint8 = 5
	XattrCompareLessEqual    uint8 = 6
	XattrCompareString       uint8 = 1
)

// XattrInput is a request union variant; Data holds the SET/CMP operand.
// MaxOutputBytes is a positive local GET/GETALL budget, absent from the wire.
// The OSD cannot be asked to truncate its reply to this local budget.
type XattrInput struct {
	Name                         string
	ComparisonOp, ComparisonMode uint8
	MaxOutputBytes               uint32
}

// XattrResult preserves the raw reply union. NameLength does not return the
// submitted name. GET can normalize ValueLength to its output length.
type XattrResult struct {
	NameLength, ValueLength      uint32
	ComparisonOp, ComparisonMode uint8
}

func IsXattrOperation(code uint16) bool {
	switch code {
	case GetXattr, GetXattrs, CompareXattr, SetXattr, RemoveXattr:
		return true
	}
	return false
}

func validateXattrOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 {
		return 0, 0, ErrRequest
	}
	if uint64(len(op.Xattr.Name)) > math.MaxUint32 || uint64(len(op.Data)) > math.MaxUint32 {
		return 0, 0, wire.ErrLimit
	}
	inputBytes := uint64(len(op.Xattr.Name)) + uint64(len(op.Data))
	if inputBytes > math.MaxUint32 || inputBytes > uint64(limit) {
		return 0, 0, wire.ErrLimit
	}
	if op.Code != GetXattrs && (op.Xattr.Name == "" || strings.IndexByte(op.Xattr.Name, 0) >= 0) {
		// Empty names alias the server's internal OI_ATTR "_"; BlueStore
		// uses c_str() for attr names and can truncate an embedded NUL.
		// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L5977
		return 0, 0, ErrRequest
	}
	if op.Code != CompareXattr && (op.Xattr.ComparisonOp != 0 || op.Xattr.ComparisonMode != 0) {
		return 0, 0, ErrRequest
	}
	switch op.Code {
	case GetXattr, GetXattrs:
		if len(op.Data) != 0 || op.Code == GetXattrs && op.Xattr.Name != "" || op.Xattr.MaxOutputBytes == 0 {
			return 0, 0, ErrRequest
		}
		if op.Xattr.MaxOutputBytes > limit {
			return 0, 0, wire.ErrLimit
		}
		return FlagRead, inputBytes, nil
	case CompareXattr:
		if op.Xattr.MaxOutputBytes != 0 {
			return 0, 0, ErrRequest
		}
		if op.Xattr.ComparisonMode != XattrCompareString || op.Xattr.ComparisonOp < XattrCompareEqual || op.Xattr.ComparisonOp > XattrCompareLessEqual {
			return 0, 0, ErrOperation
		}
		return FlagRead, inputBytes, nil
	case SetXattr:
		if op.Xattr.MaxOutputBytes != 0 {
			return 0, 0, ErrRequest
		}
		return FlagWrite, inputBytes, nil
	case RemoveXattr:
		if len(op.Data) != 0 || op.Xattr.MaxOutputBytes != 0 {
			return 0, 0, ErrRequest
		}
		return FlagWrite, inputBytes, nil
	}
	return 0, 0, ErrOperation
}

// ValidateXattrReply compares only the xattr-specific union and output budget.
// The caller additionally validates identity, flags, aggregate/per-op codes,
// completion and payload splitting. success excludes an aborted/unexecuted op.
func ValidateXattrReply(requested Operation, reply OperationResult, success bool) error {
	if !IsXattrOperation(requested.Code) {
		return ErrOperation
	}
	if reply.Code != requested.Code || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr.NameLength != uint32(len(requested.Xattr.Name)) || reply.Xattr.ComparisonOp != requested.Xattr.ComparisonOp || reply.Xattr.ComparisonMode != requested.Xattr.ComparisonMode {
		return errors.New("OSD xattr reply union differs from submitted operation")
	}
	switch requested.Code {
	case GetXattr:
		if reply.PayloadLength > requested.Xattr.MaxOutputBytes || reply.Xattr.ValueLength > requested.Xattr.MaxOutputBytes {
			return wire.ErrLimit
		}
		if success && reply.Xattr.ValueLength != reply.PayloadLength {
			return errors.New("OSD xattr value length differs from successful output")
		}
	case GetXattrs:
		if reply.Xattr.ValueLength != 0 {
			return errors.New("OSD xattr list reply changed its reserved value length")
		}
		if reply.PayloadLength > requested.Xattr.MaxOutputBytes {
			return wire.ErrLimit
		}
		if success && reply.PayloadLength < 4 {
			return errors.New("OSD xattr list lacks its native map count")
		}
	default:
		if reply.Xattr.ValueLength != uint32(len(requested.Data)) || reply.PayloadLength != 0 {
			return errors.New("OSD xattr mutation/comparison reply changed its input or returned data")
		}
	}
	return nil
}

func encodeXattrUnion(e *wire.Encoder, op Operation) {
	e.U32(uint32(len(op.Xattr.Name)))
	e.U32(uint32(len(op.Data)))
	e.U8(op.Xattr.ComparisonOp)
	e.U8(op.Xattr.ComparisonMode)
	e.Raw(make([]byte, 18))
}

func decodeXattrUnion(d *wire.Decoder, code uint16) XattrResult {
	x := XattrResult{NameLength: d.U32(), ValueLength: d.U32(), ComparisonOp: d.U8(), ComparisonMode: d.U8()}
	for _, b := range d.Raw(18) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	if code == CompareXattr {
		if x.ComparisonMode != XattrCompareString || x.ComparisonOp < XattrCompareEqual || x.ComparisonOp > XattrCompareLessEqual {
			d.Fail(ErrOperation)
		}
	} else if x.ComparisonOp != 0 || x.ComparisonMode != 0 {
		d.Fail(msgr.ErrFrame)
	}
	return x
}

// Each addition is checked before it occurs; totals cannot wrap or allocate
// before their complete front/data/reply budgets have been bounded.
func boundedBytes(limit uint32, parts ...uint64) (uint64, error) {
	var total uint64
	for _, part := range parts {
		if part > uint64(limit)-total {
			return 0, wire.ErrLimit
		}
		total += part
	}
	return total, nil
}

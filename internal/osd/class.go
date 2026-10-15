package osd

import (
	"errors"
	"math"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// CALL is independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:600-605 (cls
// union), osdc/Objecter.h:227-242 (add_call) and osd/PrimaryLogPG.cc:6207-6257.
// Those sources carry LGPL-2.1 notices; no native implementation is copied.
const (
	Call uint16 = 0x1401

	// ClassRead and ClassWrite select only the request READ/WRITE flag and
	// whether a mutation mtime is admitted. The OSD classifies the method by
	// its registered flags, not by these request bits.
	ClassRead  uint8 = 1
	ClassWrite uint8 = 2

	maxClassName = math.MaxUint8
)

// ClassInput is the CALL union variant; Operation.Data holds the method input.
// MaxOutputBytes is a positive local receipt budget, absent from the wire.
type ClassInput struct {
	Class, Method  string
	Mode           uint8
	MaxOutputBytes uint32
}

// ClassResult preserves the echoed cls union and the OSD's output length
// (the reply's extent.length bytes). Argc is always zero in Tentacle.
type ClassResult struct {
	ClassLength, MethodLength, Argc uint8
	InputLength                     uint32
	OutputLength                    uint64
}

func validateClassOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) {
		return 0, 0, ErrRequest
	}
	// The builder assigns u8 lengths, which would silently truncate longer
	// names; the OSD opens the class through c_str(), so NUL is refused too.
	// https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/ClassHandler.cc#L97
	input := op.Class
	if input.Class == "" || input.Method == "" || len(input.Class) > maxClassName || len(input.Method) > maxClassName ||
		strings.IndexByte(input.Class, 0) >= 0 || strings.IndexByte(input.Method, 0) >= 0 {
		return 0, 0, ErrRequest
	}
	if input.Mode != ClassRead && input.Mode != ClassWrite || input.MaxOutputBytes == 0 {
		return 0, 0, ErrRequest
	}
	if input.MaxOutputBytes > limit || uint64(len(op.Data)) > math.MaxUint32 {
		return 0, 0, wire.ErrLimit
	}
	// A FAILOK method may leave staged writes after failing; keep it
	// unsupported until independently characterized.
	if op.Flags != 0 {
		return 0, 0, ErrOperation
	}
	size, err := boundedBytes(limit, uint64(len(input.Class)), uint64(len(input.Method)), uint64(len(op.Data)))
	if err != nil {
		return 0, 0, err
	}
	if input.Mode == ClassWrite {
		return FlagWrite, size, nil
	}
	return FlagRead, size, nil
}

func encodeClassUnion(e *wire.Encoder, op Operation) {
	e.U8(uint8(len(op.Class.Class)))
	e.U8(uint8(len(op.Class.Method)))
	e.U8(0) // argc is never set by the pinned builder.
	e.U32(uint32(len(op.Data)))
	e.Raw(make([]byte, 21))
}

func decodeClassUnion(d *wire.Decoder) ClassResult {
	c := ClassResult{ClassLength: d.U8(), MethodLength: d.U8(), Argc: d.U8(), InputLength: d.U32()}
	if d.U8() != 0 { // high byte of the overlapping extent.offset
		d.Fail(msgr.ErrFrame)
	}
	c.OutputLength = d.U64()
	if d.U64() != 0 || d.U32() != 0 || c.Argc != 0 {
		d.Fail(msgr.ErrFrame)
	}
	return c
}

// ValidateClassReply compares the echoed union, the OSD output length and the
// local output budget. A positive method result is preserved by the caller.
func ValidateClassReply(requested Operation, reply OperationResult) error {
	if requested.Code != Call {
		return ErrOperation
	}
	c := reply.Class
	if reply.Code != Call || c.ClassLength != uint8(len(requested.Class.Class)) || c.MethodLength != uint8(len(requested.Class.Method)) || c.InputLength != uint32(len(requested.Data)) || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr != (XattrResult{}) {
		return errors.New("OSD CALL reply union differs from submitted operation")
	}
	if reply.PayloadLength > requested.Class.MaxOutputBytes {
		return wire.ErrLimit
	}
	if uint64(len(reply.Data)) != uint64(reply.PayloadLength) {
		return errors.New("OSD CALL output length differs from reply payload")
	}
	// PrimaryLogPG.cc:6257 records the method output length; the RETURNVEC
	// bound (4287-4306) can later clear that output with -EOVERFLOW.
	if c.OutputLength != uint64(reply.PayloadLength) && !(reply.PayloadLength == 0 && reply.Result == -75) {
		return errors.New("OSD CALL output length differs from returned data")
	}
	return nil
}

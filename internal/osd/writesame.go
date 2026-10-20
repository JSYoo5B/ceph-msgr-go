package osd

import (
	"errors"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently translated from Ceph 20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:656-660,
// osdc/Objecter.h:182-188 and osd/PrimaryLogPG.cc:5051-5084,6895-6900.
// LGPL-2.1 sources; no native implementation is copied.
func encodeWriteSameUnion(e *wire.Encoder, op Operation) {
	e.U64(op.Offset)
	e.U64(op.Length)
	e.U64(uint64(len(op.Data)))
	e.U32(0)
}

func decodeWriteSameUnion(d *wire.Decoder) (offset, length, patternLength uint64) {
	offset, length, patternLength = d.U64(), d.U64(), d.U64()
	if d.U32() != 0 {
		d.Fail(msgr.ErrFrame)
	}
	return
}

// ValidateWriteSameReply requires the original union and no output on either
// success or failure. The nested WRITE may be clipped by a newer truncate
// sequence; its modified extent is not copied into the WRITESAME reply.
func ValidateWriteSameReply(requested Operation, reply OperationResult) error {
	if requested.Code != WriteSame {
		return ErrOperation
	}
	if reply.Code != WriteSame || reply.Offset != requested.Offset || reply.Length != requested.Length || reply.WriteSamePatternLength != uint64(len(requested.Data)) || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.PayloadLength != 0 || len(reply.Data) != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.AllocHint != (AllocHintInput{}) || reply.Checksum != (ChecksumResult{}) || reply.Snap != 0 {
		return errors.New("OSD writesame reply differs from submitted pattern or logical extent")
	}
	return nil
}

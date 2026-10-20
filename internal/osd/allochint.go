package osd

import (
	"errors"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// SETALLOCHINT is independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:558-568 and
// 651-655, osdc/Objecter.h:303-310 and 1631-1641, osd/PrimaryLogPG.cc:6717-6730.
// Those sources carry LGPL-2.1 notices; no native implementation is copied.
const (
	SetAllocHint uint16 = 0x2223

	// AllocHintFlags is the union of the pinned CEPH_OSD_ALLOC_HINT_FLAG_*
	// bits (SEQUENTIAL_WRITE through LOG).
	AllocHintFlags uint32 = 1<<11 - 1
)

// AllocHintInput is the SETALLOCHINT union variant. The hint is advisory: the
// OSD records it in object_info and passes it to the object store.
type AllocHintInput struct {
	ExpectedObjectSize, ExpectedWriteSize uint64
	Flags                                 uint32
}

func validateAllocHintOperation(op Operation) error {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) {
		return ErrRequest
	}
	if op.AllocHint.Flags&^AllocHintFlags != 0 {
		return ErrRequest
	}
	return nil
}

func encodeAllocHintUnion(e *wire.Encoder, hint AllocHintInput) {
	e.U64(hint.ExpectedObjectSize)
	e.U64(hint.ExpectedWriteSize)
	e.U32(hint.Flags)
	e.Raw(make([]byte, 8))
}

func decodeAllocHintUnion(d *wire.Decoder) AllocHintInput {
	hint := AllocHintInput{ExpectedObjectSize: d.U64(), ExpectedWriteSize: d.U64(), Flags: d.U32()}
	for _, b := range d.Raw(8) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return hint
}

// ValidateAllocHintReply requires the echoed union and no output.
func ValidateAllocHintReply(requested Operation, reply OperationResult) error {
	if requested.Code != SetAllocHint {
		return ErrOperation
	}
	if reply.Code != SetAllocHint || reply.AllocHint != requested.AllocHint || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.PayloadLength != 0 || len(reply.Data) != 0 {
		return errors.New("OSD allocation hint reply differs from submitted operation")
	}
	return nil
}

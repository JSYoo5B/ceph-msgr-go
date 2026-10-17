package osd

import (
	"errors"
	"math"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// COPY_FROM and COPY_FROM2 are independently translated from Ceph 20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:307-308,
// 518-533 and 631-644, osdc/Objecter.h:1520-1544 and
// osd/PrimaryLogPG.cc:8106-8197. Those sources carry LGPL-2.1 notices; no
// native implementation is copied.
const (
	CopyFrom  uint16 = 0x221a
	CopyFrom2 uint16 = 0x222d

	// CopyFromTruncateSequence is the only copy flag admitted: it tells a
	// COPY_FROM2 destination to use the supplied truncate sequence and size
	// instead of the source's (rados.h CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ).
	CopyFromTruncateSequence uint8 = 32
)

// CopyFromInput is the COPY_FROM/COPY_FROM2 union and indata. The source is a
// head object named by an explicit pool, key and namespace; the OSD maps it
// to its own PG. Version zero disables the source version check. Only the
// librados flags (zero) and COPY_FROM2's TRUNCATE_SEQ are admitted, source
// snapshots, fadvise hints and cache-tier flags are not.
type CopyFromInput struct {
	Object           string
	Locator          Locator
	Version          uint64
	Flags            uint8
	TruncateSequence uint32
	TruncateSize     uint64
}

// CopyFromResult preserves the echoed union (snapid, source version, copy
// flags and source fadvise flags).
type CopyFromResult struct {
	Snap, Version uint64
	Flags         uint8
	Fadvise       uint32
}

func copyFromTruncates(input CopyFromInput) bool {
	return input.Flags&CopyFromTruncateSequence != 0
}

func validateCopyFromOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Snap != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) {
		return 0, 0, ErrRequest
	}
	// FAILOK has no meaning for a standalone vector and EXCL is refused for
	// every opcode but CREATE.
	if op.Flags != 0 {
		return 0, 0, ErrOperation
	}
	input := op.CopyFrom
	if input.Object == "" || input.Locator.Pool < 0 || input.Locator.Hash != -1 || strings.IndexByte(input.Object, 0) >= 0 {
		return 0, 0, ErrRequest
	}
	switch {
	case input.Flags&^CopyFromTruncateSequence != 0:
		return 0, 0, ErrOperation
	case input.Flags != 0 && op.Code != CopyFrom2:
		return 0, 0, ErrOperation
	case !copyFromTruncates(input) && (input.TruncateSequence != 0 || input.TruncateSize != 0):
		return 0, 0, ErrRequest
	}
	// A size without a sequence has no effect on the OSD, and (1, all-ones)
	// is rewritten to (0, 0) there; both are refused like extent fields.
	if copyFromTruncates(input) && (input.TruncateSequence == 0 || input.TruncateSequence == 1 && input.TruncateSize == math.MaxUint64) {
		return 0, 0, ErrRequest
	}
	size, err := boundedBytes(limit, copyFromDataBytes(op))
	if err != nil {
		return 0, 0, err
	}
	return FlagWrite, size, nil
}

func copyFromLocatorBytes(loc Locator) uint64 {
	// struct header 6 + pool 8 + preferred 4 + key/namespace prefixes 8 + hash 8.
	return 6 + 8 + 4 + 4 + uint64(len(loc.Key)) + 4 + uint64(len(loc.Namespace)) + 8
}

func copyFromDataBytes(op Operation) uint64 {
	n := 4 + uint64(len(op.CopyFrom.Object)) + copyFromLocatorBytes(op.CopyFrom.Locator)
	if copyFromTruncates(op.CopyFrom) {
		n += 12
	}
	return n
}

func encodeCopyFromUnion(e *wire.Encoder, op Operation) {
	e.U64(NoSnap)
	e.U64(op.CopyFrom.Version)
	e.U8(op.CopyFrom.Flags)
	e.U32(0) // src_fadvise_flags
	e.Raw(make([]byte, 7))
}

func appendCopyFromData(data []byte, op Operation) []byte {
	e := wire.Encoder{}
	e.String(op.CopyFrom.Object)
	encodeLocator(&e, op.CopyFrom.Locator)
	if copyFromTruncates(op.CopyFrom) {
		e.U32(op.CopyFrom.TruncateSequence)
		e.U64(op.CopyFrom.TruncateSize)
	}
	return append(data, e.Data...)
}

func decodeCopyFromUnion(d *wire.Decoder) CopyFromResult {
	r := CopyFromResult{Snap: d.U64(), Version: d.U64(), Flags: d.U8(), Fadvise: d.U32()}
	for _, b := range d.Raw(7) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return r
}

// ValidateCopyFromReply requires the original union and no output on success
// or failure: the daemon keeps the request union and returns no payload.
func ValidateCopyFromReply(requested Operation, reply OperationResult) error {
	if requested.Code != CopyFrom && requested.Code != CopyFrom2 {
		return ErrOperation
	}
	want := CopyFromResult{Snap: NoSnap, Version: requested.CopyFrom.Version, Flags: requested.CopyFrom.Flags}
	if reply.Code != requested.Code || reply.CopyFrom != want || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.PayloadLength != 0 || len(reply.Data) != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.AllocHint != (AllocHintInput{}) || reply.Checksum != (ChecksumResult{}) || reply.Snap != 0 || reply.WriteSamePatternLength != 0 {
		return errors.New("OSD copy-from reply differs from submitted operation")
	}
	return nil
}

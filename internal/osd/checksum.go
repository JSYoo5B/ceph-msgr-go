package osd

import (
	"errors"
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:549-553,660-666,
// osdc/Objecter.h:623-650 and osd/PrimaryLogPG.cc:5533-5711. These sources
// carry LGPL-2.1 notices; no upstream implementation is copied.
const (
	Checksum         uint16 = 0x121f
	ChecksumXXHash32 uint8  = 0
	ChecksumXXHash64 uint8  = 1
	ChecksumCRC32C   uint8  = 2
)

// ChecksumInput selects a bounded checksum read. Seed is encoded little-endian
// in the request payload; ChunkSize and Type occupy the checksum union.
// ChunkSize zero requests one checksum over the requested extent.
type ChecksumInput struct {
	ChunkSize uint32
	Type      uint8
	Seed      uint64
}

// ChecksumResult is the union echoed by the OSD. The result's outer Length
// may be shortened at EOF. Raw data contains a count and little-endian digests.
type ChecksumResult struct {
	ChunkSize uint32
	Type      uint8
}

func validateChecksumOperation(op Operation, limit uint32) error {
	width := checksumWidth(op.Checksum.Type)
	if op.Flags&^OpFadviseMask != 0 || width == 0 {
		return ErrOperation
	}
	if width == 4 && op.Checksum.Seed > math.MaxUint32 {
		return ErrRequest
	}
	if op.Length == 0 || op.Offset > math.MaxUint64-op.Length || len(op.Data) != 0 || op.Version != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.Snap != 0 || op.ListSnaps != (ListSnapsInput{}) || op.ListWatchers != (ListWatchersInput{}) {
		return ErrRequest
	}
	if op.Checksum.ChunkSize != 0 && op.Length%uint64(op.Checksum.ChunkSize) != 0 {
		return ErrRequest
	}
	// Bound server work as well as receipt allocation. The unbounded whole
	// object shorthand (offset=length=0) is deliberately unavailable.
	if op.Length > uint64(limit) || checksumReplyBytes(op) > uint64(limit) || limit < 4 {
		return wire.ErrLimit
	}
	return nil
}

func checksumReplyBytes(op Operation) uint64 {
	count := uint64(1)
	if op.Checksum.ChunkSize != 0 {
		count = op.Length / uint64(op.Checksum.ChunkSize)
	}
	return 4 + checksumWidth(op.Checksum.Type)*count
}

func encodeChecksumUnion(e *wire.Encoder, op Operation) {
	e.U64(op.Offset)
	e.U64(op.Length)
	e.U32(op.Checksum.ChunkSize)
	e.U8(op.Checksum.Type)
	e.Raw(make([]byte, 7))
}

func decodeChecksumUnion(d *wire.Decoder) (uint64, uint64, ChecksumResult) {
	offset, length := d.U64(), d.U64()
	result := ChecksumResult{ChunkSize: d.U32(), Type: d.U8()}
	for _, b := range d.Raw(7) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return offset, length, result
}

func checksumWidth(algorithm uint8) uint64 {
	switch algorithm {
	case ChecksumXXHash32, ChecksumCRC32C:
		return 4
	case ChecksumXXHash64:
		return 8
	default:
		return 0
	}
}

func encodeChecksumData(input ChecksumInput) []byte {
	e := wire.Encoder{}
	if checksumWidth(input.Type) == 8 {
		e.U64(input.Seed)
	} else {
		e.U32(uint32(input.Seed))
	}
	return e.Data
}

// DecodeChecksum decodes the algorithm-sized count and values without allocating from
// an unchecked wire count. Empty data means the server skipped an EOF read.
func DecodeChecksum(algorithm uint8, data []byte) ([]uint64, error) {
	width := checksumWidth(algorithm)
	if width == 0 {
		return nil, ErrOperation
	}
	if len(data) == 0 {
		return nil, nil
	}
	if uint64(len(data)) > math.MaxUint32 {
		return nil, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	n := d.Count(uint32(width), math.MaxUint32)
	values := make([]uint64, n)
	for i := range values {
		if width == 8 {
			values[i] = d.U64()
		} else {
			values[i] = uint64(d.U32())
		}
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return values, nil
}

func ValidateChecksumReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != Checksum || checksumWidth(requested.Checksum.Type) == 0 {
		return ErrOperation
	}
	if reply.Code != Checksum || reply.Offset != requested.Offset || reply.Length > requested.Length || reply.Checksum.ChunkSize != requested.Checksum.ChunkSize || reply.Checksum.Type != requested.Checksum.Type || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || uint64(reply.PayloadLength) != uint64(len(reply.Data)) || uint64(reply.PayloadLength) > checksumReplyBytes(requested) {
		return errors.New("OSD checksum reply differs from submitted extent or budget")
	}
	if !success {
		if reply.PayloadLength != 0 {
			return errors.New("OSD failed checksum returned output")
		}
		return nil
	}
	// do_checksum returns immediately with the original union and no output
	// when offset >= size. The raw union/data distinguish that EOF case.
	if reply.PayloadLength == 0 {
		if reply.Length != requested.Length {
			return errors.New("OSD empty checksum changed requested length")
		}
		return nil
	}
	// Positive bounded requests never produce a zero-length counted result:
	// EOF exits before encoding a count, keeping the original request length.
	if reply.Length == 0 {
		return errors.New("OSD counted checksum has zero returned length")
	}
	values, err := DecodeChecksum(requested.Checksum.Type, reply.Data)
	if err != nil {
		return err
	}
	count := uint64(1)
	if requested.Checksum.ChunkSize != 0 {
		chunk := uint64(requested.Checksum.ChunkSize)
		if reply.Length%chunk != 0 {
			return errors.New("OSD checksum trimmed length is not chunk aligned")
		}
		count = reply.Length / chunk
	}
	if uint64(len(values)) != count {
		return errors.New("OSD checksum value count differs from returned extent")
	}
	return nil
}

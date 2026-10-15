package osd

import (
	"errors"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:549-553,660-666,
// osdc/Objecter.h:623-650 and osd/PrimaryLogPG.cc:5533-5711. These sources
// carry LGPL-2.1 notices; no upstream implementation is copied.
const (
	Checksum       uint16 = 0x121f
	ChecksumCRC32C uint8  = 2
)

// ChecksumInput selects a bounded CRC32C read. Seed is encoded little-endian
// in the request payload; ChunkSize and Type occupy the checksum union.
// ChunkSize zero requests one checksum over the requested extent.
type ChecksumInput struct {
	ChunkSize uint32
	Type      uint8
	Seed      uint32
}

// ChecksumResult is the union echoed by the OSD. The result's outer Length
// may be shortened at EOF. Raw data contains a count and little-endian CRCs.
type ChecksumResult struct {
	ChunkSize uint32
	Type      uint8
}

func validateChecksumOperation(op Operation, limit uint32) error {
	if op.Flags != 0 || op.Checksum.Type != ChecksumCRC32C {
		return ErrOperation
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
	return 4 + 4*count
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

func encodeChecksumData(seed uint32) []byte {
	e := wire.Encoder{}
	e.U32(seed)
	return e.Data
}

// DecodeChecksum decodes the CRC32C count and values without allocating from
// an unchecked wire count. Empty data means the server skipped an EOF read.
func DecodeChecksum(data []byte) ([]uint32, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if uint64(len(data)) > math.MaxUint32 {
		return nil, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	n := d.Count(4, math.MaxUint32)
	values := make([]uint32, n)
	for i := range values {
		values[i] = d.U32()
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return values, nil
}

func ValidateChecksumReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != Checksum {
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
	values, err := DecodeChecksum(reply.Data)
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

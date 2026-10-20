package osd

import (
	"errors"
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently implemented from LGPL-2.1 Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:257,
// osdc/Objecter.h:516-568,618-621 and osd/PrimaryLogPG.cc:5931-6024.
// No upstream implementation is copied.
const SparseRead uint16 = 0x1205

// SparseReadInput bounds the entire encoded extent map and byte buffer. The
// budget is local and includes the two four-byte counts.
type SparseReadInput struct{ MaxOutputBytes uint32 }

// SparseReadResult contains ordered logical extents and their concatenated
// bytes. Holes have no corresponding bytes; Data is not a dense object slice.
type SparseReadResult struct {
	Extents []Extent
	Data    []byte
}

func validateSparseReadOperation(op Operation, limit uint32) error {
	if op.Flags&^OpFadviseMask != 0 {
		return ErrOperation
	}
	if op.Length == 0 || op.Offset > math.MaxUint64-op.Length || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.Snap != 0 || op.ListSnaps != (ListSnapsInput{}) || op.ListWatchers != (ListWatchersInput{}) || op.MapExt != (MapExtInput{}) || op.Checksum != (ChecksumInput{}) {
		return ErrRequest
	}
	if op.Length > uint64(limit) || op.SparseRead.MaxOutputBytes < 8 || op.SparseRead.MaxOutputBytes > limit {
		return wire.ErrLimit
	}
	return nil
}

// DecodeSparseRead decodes a bounded map<uint64_t,uint64_t> followed by an
// encoded bufferlist. Every returned byte belongs to exactly one extent.
func DecodeSparseRead(data []byte) (SparseReadResult, error) {
	if uint64(len(data)) > math.MaxUint32 {
		return SparseReadResult{}, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	n := d.Count(16, math.MaxUint32)
	result := SparseReadResult{Extents: make([]Extent, n)}
	var end, total uint64
	for i := range result.Extents {
		e := Extent{Offset: d.U64(), Length: d.U64()}
		if e.Length == 0 || e.Offset > math.MaxUint64-e.Length || i > 0 && e.Offset < end || e.Length > math.MaxUint64-total {
			d.Fail(msgr.ErrFrame)
		}
		result.Extents[i] = e
		end = e.Offset + e.Length
		total += e.Length
	}
	result.Data = d.Bytes()
	if uint64(len(result.Data)) != total {
		d.Fail(msgr.ErrFrame)
	}
	if err := d.Done(); err != nil {
		return SparseReadResult{}, err
	}
	return result, nil
}

func ValidateSparseReadReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != SparseRead {
		return ErrOperation
	}
	if reply.Code != SparseRead || reply.Offset != requested.Offset || reply.Length > requested.Length || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || uint64(reply.PayloadLength) != uint64(len(reply.Data)) || reply.PayloadLength > requested.SparseRead.MaxOutputBytes {
		return errors.New("OSD sparse read reply differs from submitted extent or budget")
	}
	if !success {
		if reply.PayloadLength != 0 || reply.Length != requested.Length {
			return errors.New("OSD failed sparse read changed extent or returned output")
		}
		return nil
	}
	result, err := DecodeSparseRead(reply.Data)
	if err != nil {
		return err
	}
	if uint64(len(result.Data)) != reply.Length {
		return errors.New("OSD sparse read returned length differs from data")
	}
	for _, e := range result.Extents {
		if e.Offset < requested.Offset || e.Offset-requested.Offset > requested.Length || e.Length > requested.Length-(e.Offset-requested.Offset) {
			return errors.New("OSD sparse read range exceeds requested extent")
		}
	}
	return nil
}

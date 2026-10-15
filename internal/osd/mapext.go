package osd

import (
	"errors"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: include/rados.h:252,
// osdc/Objecter.h:614-617,3404-3422, osd/PrimaryLogPG.cc:6168-6196,
// os/memstore/MemStore.cc:335-368 and os/bluestore/BlueStore.cc:13167-13266.
// LGPL-2.1 sources; no upstream implementation is copied.
const MapExt uint16 = 0x1203

// MapExtInput is a local output budget; no budget is sent to the server.
type MapExtInput struct{ MaxOutputBytes uint32 }

func validateMapExtOperation(op Operation, limit uint32) error {
	if op.Flags != 0 {
		return ErrOperation
	}
	if op.Length == 0 || op.Offset > math.MaxUint64-op.Length || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.Snap != 0 || op.ListSnaps != (ListSnapsInput{}) || op.ListWatchers != (ListWatchersInput{}) || op.MapExt.MaxOutputBytes < 4 || op.MapExt.MaxOutputBytes > limit {
		return ErrRequest
	}
	return nil
}

// DecodeMapExt reads the ordered map<uint64_t,uint64_t> emitted by fiemap.
// Extents are logical allocated ranges, not physical locations or object data.
// Adjacent extents are legal; duplicate, overlapping or empty ranges are not.
func DecodeMapExt(data []byte) ([]Extent, error) {
	if uint64(len(data)) > math.MaxUint32 {
		return nil, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	n := d.Count(16, math.MaxUint32)
	extents := make([]Extent, n)
	var previousEnd uint64
	for i := range extents {
		e := Extent{Offset: d.U64(), Length: d.U64()}
		if e.Length == 0 || e.Offset > math.MaxUint64-e.Length || i > 0 && e.Offset < previousEnd {
			d.Fail(msgr.ErrFrame)
		}
		extents[i] = e
		previousEnd = e.Offset + e.Length
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return extents, nil
}

// ValidateMapExtReply retains the requested extent union, bounds output and
// verifies that every returned logical range lies inside the requested range.
func ValidateMapExtReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != MapExt {
		return ErrOperation
	}
	if reply.Code != MapExt || reply.Offset != requested.Offset || reply.Length != requested.Length || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.AllocHint != (AllocHintInput{}) || reply.Snap != 0 || uint64(len(reply.Data)) != uint64(reply.PayloadLength) || reply.PayloadLength > requested.MapExt.MaxOutputBytes {
		return errors.New("OSD extent map reply differs from request or budget")
	}
	if !success {
		if reply.PayloadLength != 0 {
			return errors.New("OSD failed extent map returned output")
		}
		return nil
	}
	extents, err := DecodeMapExt(reply.Data)
	if err != nil {
		return err
	}
	for _, e := range extents {
		if e.Offset < requested.Offset || e.Offset-requested.Offset > requested.Length || e.Length > requested.Length-(e.Offset-requested.Offset) {
			return errors.New("OSD extent map range exceeds requested extent")
		}
	}
	return nil
}

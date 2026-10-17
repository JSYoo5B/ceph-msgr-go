package osd

import (
	"errors"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Snapshot context, ROLLBACK and LIST_SNAPS are independently implemented
// from Ceph v20.2.4, commit 7f793731f1b39eb4f465e960113d2363c311b964:
// include/rados.h:38-40, 267, 290, 460, 498 and 610-612,
// messages/MOSDOp.h:330-332 and 542-544, common/snap_types.cc:99-117,
// osd/PrimaryLogPG.cc:2176-2183, 2321-2337, 4211-4229, 6571-6656 and
// 8373-8811, osd/osd_types.h:6786-6869. Those sources carry LGPL-2.1
// notices; no native implementation is copied.
const (
	ListSnaps uint16 = 0x120a
	Rollback  uint16 = 0x220e

	SnapDir       uint64 = ^uint64(0)
	MaxSnap       uint64 = ^uint64(0) - 2
	FlagOrderSnap uint64 = 0x40

	// FlagFullTry and FlagFullForce are rados.h CEPH_OSD_FLAG_FULL_TRY and
	// CEPH_OSD_FLAG_FULL_FORCE.
	FlagFullTry   uint64 = 0x800000
	FlagFullForce uint64 = 0x1000000

	// FlagBalanceReads and FlagLocalizeReads are rados.h
	// CEPH_OSD_FLAG_BALANCE_READS and CEPH_OSD_FLAG_LOCALIZE_READS.
	FlagBalanceReads  uint64 = 0x100
	FlagLocalizeReads uint64 = 0x2000

	// OldSnapContext is EOLDSNAPC from include/rados.h, not a host errno.
	OldSnapContext int32 = -85
)

// ListSnapsInput bounds the LIST_SNAPS output accepted from the OSD.
type ListSnapsInput struct{ MaxOutputBytes uint32 }

// SnapList is the decoded obj_list_snap_response_t. Clones keep the OSD's
// order; the head, when present and not a whiteout, is the last entry with ID
// NoSnap. Snaps are ascending as sent.
type SnapList struct {
	Seq    uint64
	Clones []Clone
}

type Clone struct {
	ID      uint64
	Snaps   []uint64
	Overlap []Extent
	Size    uint64
}

type Extent struct{ Offset, Length uint64 }

func validateSnapOperation(op Operation, limit uint32) (uint64, error) {
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || len(op.Data) != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) {
		return 0, ErrRequest
	}
	// FAILOK has no meaning for a single-operation vector and would hide
	// the only result; EXCL is refused for every opcode but CREATE.
	if op.Flags != 0 {
		return 0, ErrOperation
	}
	if op.Code == Rollback {
		// Snap 0 never names a clone, so the OSD would delete the head.
		if op.Snap == 0 || op.Snap > MaxSnap || op.ListSnaps != (ListSnapsInput{}) {
			return 0, ErrRequest
		}
		return FlagWrite, nil
	}
	if op.Snap != 0 || op.ListSnaps.MaxOutputBytes == 0 || op.ListSnaps.MaxOutputBytes > limit {
		return 0, ErrRequest
	}
	return FlagRead, nil
}

// requestSnapshot validates the request-level snapshot fields against the
// vector mode and returns the hobj.snap value to transmit.
func requestSnapshot(r Request, flags uint64) (uint64, error) {
	snap := r.ReadSnap
	if snap == 0 {
		snap = NoSnap
	}
	if snap == SnapDir {
		return 0, ErrRequest
	}
	if r.Operations[0].Code == ListWatchers && snap != NoSnap {
		return 0, ErrRequest
	}
	if IsWatchOperation(r.Operations[0].Code) && (snap != NoSnap || r.SnapSeq != 0 || len(r.Snaps) != 0 || r.OrderSnap) {
		return 0, ErrRequest
	}
	if r.Operations[0].Code == ListSnaps {
		if snap != NoSnap {
			return 0, ErrRequest
		}
		snap = SnapDir
	}
	if flags&FlagWrite != 0 {
		// The OSD refuses a writing vector at a snapshot with EINVAL.
		if snap != NoSnap {
			return 0, ErrRequest
		}
	} else if r.SnapSeq != 0 || len(r.Snaps) != 0 || r.OrderSnap {
		// Read vectors never take a SnapContext; the OSD would ignore it.
		return 0, ErrRequest
	}
	// SnapContext::is_valid, except that this client also refuses a
	// trailing zero snap id, which can never be allocated.
	if r.SnapSeq > MaxSnap {
		return 0, ErrRequest
	}
	for i, s := range r.Snaps {
		if s == 0 || s > r.SnapSeq || i > 0 && s >= r.Snaps[i-1] {
			return 0, ErrRequest
		}
	}
	return snap, nil
}

func encodeRollbackUnion(e *wire.Encoder, op Operation) {
	e.U64(op.Snap)
	e.Raw(make([]byte, 20))
}

func decodeRollbackUnion(d *wire.Decoder) uint64 {
	snap := d.U64()
	for _, b := range d.Raw(20) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return snap
}

// ValidateRollbackReply requires the echoed snap id and no output.
func ValidateRollbackReply(requested Operation, reply OperationResult) error {
	if requested.Code != Rollback {
		return ErrOperation
	}
	if reply.Code != Rollback || reply.Snap != requested.Snap || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.Xattr != (XattrResult{}) || reply.Class != (ClassResult{}) || reply.AllocHint != (AllocHintInput{}) || reply.PayloadLength != 0 || len(reply.Data) != 0 {
		return errors.New("OSD rollback reply differs from submitted operation")
	}
	return nil
}

// ValidateListSnapsReply requires an unchanged zero union, output within the
// local budget and, on success, exactly one well-formed listing.
func ValidateListSnapsReply(requested Operation, reply OperationResult, success bool) error {
	if requested.Code != ListSnaps {
		return ErrOperation
	}
	if reply.Code != ListSnaps || reply.Snap != 0 || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 || reply.PayloadLength > requested.ListSnaps.MaxOutputBytes {
		return errors.New("OSD snapshot listing reply differs from submitted operation or budget")
	}
	if !success {
		if reply.PayloadLength != 0 {
			return errors.New("OSD failed snapshot listing returned output")
		}
		return nil
	}
	_, err := DecodeSnapList(reply.Data)
	return err
}

// DecodeSnapList decodes obj_list_snap_response_t (version 2, compat 1) and
// its clone_info entries (version 1, compat 1). Version 1 lacks seq, which
// native decoding reports as NoSnap.
func DecodeSnapList(data []byte) (SnapList, error) {
	var list SnapList
	if len(data) > math.MaxUint32 {
		return list, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	version, body := d.Struct(2)
	// A clone_info envelope is at least 6 bytes plus cloneid, two empty
	// vectors and size.
	n := body.Count(6+8+4+4+8, math.MaxUint32)
	list.Clones = make([]Clone, n)
	for i := range list.Clones {
		cloneVersion, entry := body.Struct(1)
		c := Clone{ID: entry.U64()}
		c.Snaps = make([]uint64, entry.Count(8, math.MaxUint32))
		for j := range c.Snaps {
			c.Snaps[j] = entry.U64()
		}
		c.Overlap = make([]Extent, entry.Count(16, math.MaxUint32))
		for j := range c.Overlap {
			c.Overlap[j] = Extent{Offset: entry.U64(), Length: entry.U64()}
		}
		c.Size = entry.U64()
		if err := structDone(entry, cloneVersion, 1); err != nil {
			body.Fail(err)
		}
		// The OSD appends the head last, with no snaps or overlap.
		if c.ID == NoSnap && (i != n-1 || len(c.Snaps) != 0 || len(c.Overlap) != 0) || c.ID == SnapDir {
			body.Fail(errors.New("ceph osd: invalid snapshot listing head entry"))
		}
		list.Clones[i] = c
	}
	list.Seq = NoSnap
	if version >= 2 {
		list.Seq = body.U64()
	}
	if err := structDone(body, version, 2); err != nil {
		d.Fail(err)
	}
	if err := d.Done(); err != nil {
		return SnapList{}, err
	}
	return list, nil
}

// structDone admits appended fields only in a newer struct version.
func structDone(d *wire.Decoder, version, supported uint8) error {
	if version > supported {
		return d.Err()
	}
	return d.Done()
}

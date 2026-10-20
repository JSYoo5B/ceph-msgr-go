package osd

import (
	"bytes"
	"errors"
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// SCRUBLS lists the scrub errors a PG primary retained from its last scrub
// (librados get_inconsistent_objects/snapsets). Independently translated from
// Ceph 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964): include/rados.h,
// common/scrub_types.{h,cc} scrub_ls_arg_t/scrub_ls_result_t, osdc/Objecter.h
// scrub_ls and osd/PrimaryLogPG.cc do_scrub_ls. LGPL-2.1; no code copied.
const ScrubList uint16 = 0x1507

// ScrubListRequest is one SCRUBLS page. Interval zero accepts the PG's
// current interval; a nonzero interval the OSD no longer has is refused with
// -EAGAIN. StartAfter names the last object of the previous page. Snapsets
// selects inconsistent snapsets instead of objects.
type ScrubListRequest struct {
	ClientInc, MapEpoch uint32
	PG                  PG
	Erasure             bool
	Shard               uint8
	Interval            uint32
	Snapsets            bool
	StartAfter          ScrubObject
	MaxReturn           uint64
	MaxOutputBytes      uint32
}

type ScrubObject struct {
	Name, Namespace string
	Snap            uint64
}

// ScrubListPage keeps every value as the raw encoded inconsistent object or
// snapset wrapper; their interpretation belongs to the caller.
type ScrubListPage struct {
	Interval uint32
	Values   [][]byte
}

func scrubListArg(r ScrubListRequest) []byte {
	b := wire.Encoder{}
	b.U32(r.Interval)
	snapsets := uint32(0)
	if r.Snapsets {
		snapsets = 1
	}
	b.U32(snapsets)
	b.String(r.StartAfter.Name)
	b.String(r.StartAfter.Namespace)
	b.U64(r.StartAfter.Snap)
	b.U64(r.MaxReturn)
	e := wire.Encoder{}
	e.Struct(1, 1, b.Data)
	return e.Data
}

func (r ScrubListRequest) pgRequest() (PGListRequest, error) {
	if r.MaxReturn == 0 || r.MaxReturn > math.MaxUint32 {
		return PGListRequest{}, ErrRequest
	}
	return PGListRequest{ClientInc: r.ClientInc, MapEpoch: r.MapEpoch, PG: r.PG, MaxOutputBytes: r.MaxOutputBytes, Erasure: r.Erasure, Shard: r.Shard, scrub: scrubListArg(r)}, nil
}

// EncodeScrubListRequest builds SCRUBLS on the v6 object-hash envelope.
func EncodeScrubListRequest(r ScrubListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	p, err := r.pgRequest()
	if err != nil {
		return msgr.MessageData{}, err
	}
	return EncodePGListRequest(p, features, limit)
}

// EncodeScrubListRequestV8 builds SCRUBLS on the v8 actual-PG envelope.
func EncodeScrubListRequestV8(r ScrubListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	p, err := r.pgRequest()
	if err != nil {
		return msgr.MessageData{}, err
	}
	return EncodePGListRequestV8(p, features, limit)
}

// EncodeScrubListRequestV9 builds SCRUBLS on the untraced v9 envelope.
func EncodeScrubListRequestV9(r ScrubListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	p, err := r.pgRequest()
	if err != nil {
		return msgr.MessageData{}, err
	}
	return EncodePGListRequestV9(p, features, limit)
}

// DecodeScrubList reads scrub_ls_result_t, bounding the value count.
func DecodeScrubList(data []byte, max uint64) (ScrubListPage, error) {
	var page ScrubListPage
	if uint64(len(data)) > math.MaxUint32 {
		return page, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	v, b := d.Struct(1)
	page.Interval = b.U32()
	n := b.Count(4, math.MaxUint32)
	if uint64(n) > max {
		b.Fail(wire.ErrLimit)
	}
	if b.Err() != nil {
		return ScrubListPage{}, b.Err()
	}
	page.Values = make([][]byte, n)
	for i := range page.Values {
		page.Values[i] = bytes.Clone(b.Bytes())
	}
	if err := structDone(b, v, 1); err != nil {
		d.Fail(err)
	}
	if err := d.Done(); err != nil {
		return ScrubListPage{}, err
	}
	return page, nil
}

// ValidateScrubListReply checks the echoed SCRUBLS operation. A refusal
// (-ENOENT before any scrub, -EAGAIN for a stale interval) carries no output.
func ValidateScrubListReply(r ScrubListRequest, reply Reply, redirect bool) error {
	if reply.Object != "" || reply.PG != r.PG || reply.RetryAttempt != 0 || len(reply.Operations) != 1 {
		return errors.New("OSD scrub listing reply identity differs from request")
	}
	op := reply.Operations[0]
	if op.Code != ScrubList || op.Flags != 0 || op.Offset != 0 || op.Length != 0 || op.TruncateSize != 0 || op.TruncateSequence != 0 || uint64(len(op.Data)) != uint64(op.PayloadLength) || op.PayloadLength > r.MaxOutputBytes {
		return errors.New("OSD scrub listing reply union or budget differs from request")
	}
	if redirect || reply.Result < 0 {
		if op.PayloadLength != 0 {
			return errors.New("failed OSD scrub listing has output")
		}
		return nil
	}
	if reply.Result != 0 {
		return errors.New("invalid OSD scrub listing result")
	}
	_, err := DecodeScrubList(op.Data, r.MaxReturn)
	return err
}

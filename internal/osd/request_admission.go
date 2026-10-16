package osd

import (
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// RequestAdmissionTarget is the identity used by the outgoing backoff gate.
// PG is the actual caller-selected PG; Object.Hash remains the raw object hash.
// Object.Snap is always the head, including snapshot reads and PGNLS.
type RequestAdmissionTarget struct {
	PG     PG
	Shard  int8
	Object PGCursor
}

// DecodeRequestV8AdmissionTarget extracts routing identity from a trusted,
// locally encoded outgoing v8 request. This is not a general request decoder
// or operation validator. It checks the current envelopes and structural tail
// so truncated/unsupported input cannot silently bypass an installed gate.
// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964:
// MOSDOp.h:368-396,446-454; Objecter.h:1893-1905; LGPL-2.1, no code copied.
func DecodeRequestV8AdmissionTarget(m msgr.MessageData, limit uint32) (RequestAdmissionTarget, error) {
	var target RequestAdmissionTarget
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if _, err := boundedBytes(limit, 41, uint64(len(m.Front)), uint64(len(m.Middle)), uint64(len(m.Data))); err != nil {
		return target, err
	}
	if m.Type != RequestMessage || len(m.Middle) != 0 {
		return target, msgr.ErrFrame
	}
	if m.Version != 8 || m.CompatVersion != 3 {
		return target, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	v, spg := d.Struct(1)
	target.PG = decodePG(spg)
	target.Shard = int8(spg.U8())
	d.Fail(spg.Done())
	if v != 1 || len(m.Front) >= 2 && m.Front[1] != 1 {
		d.Fail(wire.ErrVersion)
	}
	hash, epoch, flags := d.U32(), d.U32(), uint64(d.U32())
	identityStart := len(m.Front) - d.Remaining()
	v, identity := d.Struct(2)
	identity.Raw(21) // entity type/u64 number, u64 TID, u32 incarnation.
	d.Fail(identity.Done())
	if v != 2 || identityStart+1 < len(m.Front) && m.Front[identityStart+1] != 2 {
		d.Fail(wire.ErrVersion)
	}
	d.Raw(24) // v8's fixed three-u64 trace, not v9 opentelemetry.
	d.Raw(12) // repeated incarnation and mtime seconds/nanoseconds.
	locatorStart := len(m.Front) - d.Remaining()
	loc := decodeLocator(d)
	if locatorStart+1 < len(m.Front) {
		compat := byte(3)
		if loc.Hash >= 0 {
			compat = 6
		}
		if m.Front[locatorStart+1] != compat {
			d.Fail(wire.ErrVersion)
		}
	}
	name := d.String()
	if target.PG.Pool > math.MaxInt64 || loc.Pool < 0 || uint64(loc.Pool) != target.PG.Pool || target.Shard < -1 || epoch == 0 || loc.Hash >= 0 && uint64(loc.Hash) != uint64(hash) || name == "" && flags&flagPGOp == 0 {
		d.Fail(ErrRequest)
	}
	// Skip fixed operation unions and the bounded snapshot-context tail; these
	// fields do not change admission identity. The actual read snap is not the
	// head hobject used by Objecter::get_hobj / PrimaryLogPG::do_op backoff.
	n := uint32(d.U16())
	if n == 0 || n > MaxOperations {
		d.Fail(ErrRequest)
	}
	d.Raw(n * opWireSize)
	d.U64() // read snapshot
	d.U64() // snapshot sequence
	snaps := d.Count(8, limit/8)
	d.Raw(uint32(snaps) * 8)
	d.U32() // retry attempt
	d.U64() // negotiated features
	if err := d.Done(); err != nil {
		return RequestAdmissionTarget{}, err
	}
	target.Object = PGCursor{Pool: loc.Pool, Hash: hash, Namespace: loc.Namespace, Key: loc.Key, Object: name, Snap: NoSnap}
	return target, nil
}

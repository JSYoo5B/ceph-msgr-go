package osd

import (
	"bytes"
	"cmp"
	"math/bits"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently implemented from Ceph v20.2.4, commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MOSDBackoff.h,
// osd/osd_types.h (spg_t), common/hobject.{h,cc}, include/rados.h and
// osdc/Objecter.cc:3835-3920. LGPL-2.1 sources; no implementation copied.
const (
	BackoffMessage  uint16 = 61
	BackoffBlock    uint8  = 1
	BackoffAckBlock uint8  = 2
	BackoffUnblock  uint8  = 3
)

// Backoff identifies a connection-local restriction, not an object operation
// result. PG.Seed is an actual mapped PG, not the raw placement hash. Shard
// preserves signed shard_id_t (-1 is NO_SHARD); preserving an EC identity does
// not establish EC object-request support. Begin/End use Ceph's hobject order:
// [Begin,End), or exactly one object when they compare equal.
// A BLOCK or UNBLOCK does not establish whether an already sent request ran.
type Backoff struct {
	PG         PG
	Shard      int8
	MapEpoch   uint32
	Op         uint8
	ID         uint64
	Begin, End PGCursor
	RawFront   []byte
}

// CompareBackoffObjects implements hobject_t's canonical bitwise ordering.
// Binary string comparisons follow unsigned byte ordering. A locator key equal
// to the object name and an empty locator key have identical effective keys.
func CompareBackoffObjects(a, b PGCursor) int {
	if n := cmp.Compare(boolByte(a.Max), boolByte(b.Max)); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Pool, b.Pool); n != 0 {
		return n
	}
	if n := cmp.Compare(bits.Reverse32(a.Hash), bits.Reverse32(b.Hash)); n != 0 {
		return n
	}
	if n := strings.Compare(a.Namespace, b.Namespace); n != 0 {
		return n
	}
	if a.Key != "" || b.Key != "" {
		ak, bk := a.Key, b.Key
		if ak == "" {
			ak = a.Object
		}
		if bk == "" {
			bk = b.Object
		}
		if n := strings.Compare(ak, bk); n != 0 {
			return n
		}
	}
	if n := strings.Compare(a.Object, b.Object); n != 0 {
		return n
	}
	return cmp.Compare(a.Snap, b.Snap)
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

// BackoffContains only compares object identities. A caller must separately
// match the authenticated session and actual PG/shard before applying a gate.
func BackoffContains(b Backoff, object PGCursor) bool {
	start := CompareBackoffObjects(object, b.Begin)
	if CompareBackoffObjects(b.Begin, b.End) == 0 {
		return start == 0
	}
	return start >= 0 && CompareBackoffObjects(object, b.End) < 0
}

func backoffCursorBytes(c PGCursor, limit uint32) ([]byte, error) {
	if _, err := boundedBytes(limit, 39, uint64(len(c.Key)), uint64(len(c.Object)), uint64(len(c.Namespace))); err != nil {
		return nil, err
	}
	if len(c.Raw) != 0 {
		if _, err := boundedBytes(limit, uint64(len(c.Raw))); err != nil {
			return nil, err
		}
		decoded, err := DecodePGCursor(c.Raw)
		if err != nil {
			return nil, err
		}
		if c.Raw[0] != 4 || c.Raw[1] != 3 {
			return nil, wire.ErrVersion
		}
		if noncanonicalBackoffMin(decoded) {
			return nil, ErrRequest
		}
		// Preserve the exact envelope only if its typed projection agrees. Raw
		// bytes cannot silently select a different gate than the caller sees.
		if decoded.Key != c.Key || decoded.Object != c.Object || decoded.Namespace != c.Namespace || decoded.Snap != c.Snap || decoded.Hash != c.Hash || decoded.Max != c.Max || decoded.Pool != c.Pool {
			return nil, ErrRequest
		}
		return c.Raw, nil
	}
	b := wire.Encoder{}
	b.String(c.Key)
	b.String(c.Object)
	b.U64(c.Snap)
	b.U32(c.Hash)
	b.U8(boolByte(c.Max))
	b.String(c.Namespace)
	b.U64(uint64(c.Pool))
	e := wire.Encoder{}
	e.Struct(4, 3, b.Data)
	if noncanonicalBackoffMin(c) {
		return nil, ErrRequest
	}
	if _, err := DecodePGCursor(e.Data); err != nil {
		return nil, err
	}
	return e.Data, nil
}

// The native decoder canonicalizes this older alias before comparison. Do not
// interpret its raw pool -1 as a distinct current Tentacle bound.
func noncanonicalBackoffMin(c PGCursor) bool {
	return c.Pool == -1 && c.Snap == 0 && c.Hash == 0 && !c.Max && c.Object == ""
}

// EncodeBackoff provides the current native envelope for development and
// protocol ACK production. It does not install a gate or advertise a feature.
func EncodeBackoff(b Backoff, limit uint32) (msgr.MessageData, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if b.Op < BackoffBlock || b.Op > BackoffUnblock {
		return msgr.MessageData{}, ErrOperation
	}
	begin, err := backoffCursorBytes(b.Begin, limit)
	if err != nil {
		return msgr.MessageData{}, err
	}
	end, err := backoffCursorBytes(b.End, limit)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if CompareBackoffObjects(b.Begin, b.End) > 0 {
		return msgr.MessageData{}, ErrRequest
	}
	if _, err := boundedBytes(limit, 41+24+13, uint64(len(begin)), uint64(len(end))); err != nil {
		return msgr.MessageData{}, err
	}
	e, pg := wire.Encoder{}, wire.Encoder{}
	encodePG(&pg, b.PG)
	pg.U8(uint8(b.Shard))
	e.Struct(1, 1, pg.Data)
	e.U32(b.MapEpoch)
	e.U8(b.Op)
	e.U64(b.ID)
	e.Raw(begin)
	e.Raw(end)
	return msgr.MessageData{Type: BackoffMessage, Version: 1, CompatVersion: 1, Priority: 127, Front: e.Data}, nil
}

// EncodeBackoffAck keeps the original block's epoch even if a newer OSDMap has
// arrived. The protocol receiver must install its send restriction before
// transmitting this acknowledgement on the same authenticated connection.
func EncodeBackoffAck(block Backoff, limit uint32) (msgr.MessageData, error) {
	if block.Op != BackoffBlock {
		return msgr.MessageData{}, ErrOperation
	}
	block.Op = BackoffAckBlock
	return EncodeBackoff(block, limit)
}

func decodeBackoffCursor(d *wire.Decoder, raw []byte) PGCursor {
	start := len(raw) - d.Remaining()
	c := decodePGCursor(d)
	end := len(raw) - d.Remaining()
	if d.Err() == nil {
		if raw[start] != 4 || raw[start+1] != 3 {
			d.Fail(wire.ErrVersion)
		}
		if noncanonicalBackoffMin(c) {
			d.Fail(ErrRequest)
		}
		c.Raw = raw[start:end]
	}
	return c
}

// DecodeBackoff owns RawFront and the two cursor Raw views. Current pinned
// envelopes are accepted exactly: future compatible hobject fields could
// change gate ordering and require separate source/interop verification.
func DecodeBackoff(m msgr.MessageData, limit uint32) (Backoff, error) {
	var b Backoff
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if _, err := boundedBytes(limit, 41, uint64(len(m.Front)), uint64(len(m.Middle)), uint64(len(m.Data))); err != nil {
		return b, err
	}
	if m.Type != BackoffMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return b, msgr.ErrFrame
	}
	if m.Version != 1 || m.CompatVersion != 1 {
		return b, wire.ErrVersion
	}
	b.RawFront = bytes.Clone(m.Front)
	d := wire.NewDecoderLimit(b.RawFront, limit)
	v, pg := d.Struct(1)
	b.PG = decodePG(pg)
	b.Shard = int8(pg.U8())
	if v != 1 || len(b.RawFront) >= 2 && b.RawFront[1] != 1 {
		pg.Fail(wire.ErrVersion)
	}
	d.Fail(pg.Done())
	b.MapEpoch = d.U32()
	b.Op = d.U8()
	b.ID = d.U64()
	b.Begin = decodeBackoffCursor(d, b.RawFront)
	b.End = decodeBackoffCursor(d, b.RawFront)
	if b.Op < BackoffBlock || b.Op > BackoffUnblock {
		d.Fail(ErrOperation)
	}
	if CompareBackoffObjects(b.Begin, b.End) > 0 {
		d.Fail(ErrRequest)
	}
	if err := d.Done(); err != nil {
		return b, err
	}
	return b, nil
}

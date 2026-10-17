package osd

import (
	"bytes"
	"errors"
	"math"
	"strings"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently implemented from pinned Ceph v20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964),
// include/rados.h, messages/MOSDOp.h, common/hobject.{h,cc}, osdc/Objecter.h
// and osd/PrimaryLogPG.cc:1261-1445. LGPL-2.1; no implementation copied.
const PGNLS uint16 = 0x1505

// PGNLSFilter is PGNLS with an OSD-side filter (Objecter add_pgls_filter;
// PrimaryLogPG::do_pg_op and get_pgls_filter).
const PGNLSFilter uint16 = 0x1506

// MaxPGListFilterName bounds a filter name: "plain" or "<class>.<filter>".
const MaxPGListFilterName = 256

// PGListFilter names an OSD-side PGNLS filter and its raw parameters. The
// OSD decodes Input with the filter's own init (for "plain": an attribute key
// and a value, each a length-prefixed string); entries the filter rejects are
// skipped but still advance the cursor.
type PGListFilter struct {
	Name  string
	Input []byte
}

const flagPGOp uint64 = 0x0400
const MinPGListBytes uint32 = 49

// PGCursor preserves the wire hobject_t. Raw is owned and may be resubmitted
// verbatim; Max is the global sentinel, not the end-of-this-PG indication.
type PGCursor struct {
	Key, Object, Namespace string
	Snap                   uint64
	Hash                   uint32
	Max                    bool
	Pool                   int64
	Raw                    []byte
}

type PGListEntry struct{ Namespace, Object, Key string }
type PGListPage struct {
	Cursor  PGCursor
	Entries []PGListEntry
}
type PGListRequest struct {
	ClientInc, MapEpoch        uint32
	PG                         PG
	Namespace                  string
	Cursor                     []byte // nil selects hobject_t(), the native minimum sentinel.
	Count                      uint64
	StartEpoch, MaxOutputBytes uint32
	// Erasure and Shard address an erasure-coded PG's shard in spg_t (v8/v9
	// only); a replicated PG uses NO_SHARD.
	Erasure bool
	Shard   uint8
	// Filter, when set, sends PGNLS_FILTER instead of PGNLS.
	Filter *PGListFilter
	// scrub, when set, makes the PG operation SCRUBLS with this encoded
	// scrub_ls_arg_t as indata (EncodeScrubListRequest*); Count, StartEpoch,
	// Cursor and Filter are then unused and zero.
	scrub []byte
}

// pgListPayload returns the opcode and the operation's indata: the cursor
// alone for PGNLS, or the fixed "pg"/"filter" names, the filter name, its raw
// input and then the cursor for PGNLS_FILTER.
func pgListPayload(r PGListRequest, cursor []byte) (uint16, []byte, error) {
	if r.scrub != nil {
		if r.Filter != nil || r.Count != 0 || r.StartEpoch != 0 || len(r.Cursor) != 0 {
			return 0, nil, ErrRequest
		}
		return ScrubList, r.scrub, nil
	}
	if r.Filter == nil {
		return PGNLS, cursor, nil
	}
	if r.Filter.Name == "" || len(r.Filter.Name) > MaxPGListFilterName || strings.IndexByte(r.Filter.Name, 0) >= 0 {
		return 0, nil, ErrRequest
	}
	e := wire.Encoder{}
	e.String("pg")
	e.String("filter")
	e.String(r.Filter.Name)
	e.Raw(r.Filter.Input)
	e.Raw(cursor)
	return PGNLSFilter, e.Data, nil
}

func initialPGCursor() []byte {
	b := wire.Encoder{}
	b.String("")
	b.String("")
	b.U64(0)
	b.U32(0)
	b.U8(0)
	b.String("")
	b.U64(1 << 63)
	e := wire.Encoder{}
	e.Struct(4, 3, b.Data)
	return e.Data
}

func decodePGCursor(d *wire.Decoder) PGCursor {
	v, b := d.Struct(4)
	c := PGCursor{Key: b.String(), Object: b.String(), Snap: b.U64(), Hash: b.U32()}
	max := b.U8()
	c.Max = max == 1
	c.Namespace = b.String()
	c.Pool = int64(b.U64())
	if v < 4 || max > 1 {
		b.Fail(wire.ErrVersion)
	}
	if c.Max && (c.Key != "" || c.Object != "" || c.Namespace != "" || c.Snap != 0 || c.Hash != 0 || c.Pool != math.MinInt64) {
		b.Fail(ErrRequest)
	}
	if err := structDone(b, v, 4); err != nil {
		d.Fail(err)
	}
	return c
}

// DecodePGCursor validates a current compatible hobject_t without imposing a
// pool or placement interpretation. The caller owns routing and PG boundaries.
func DecodePGCursor(data []byte) (PGCursor, error) {
	if uint64(len(data)) > math.MaxUint32 {
		return PGCursor{}, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	c := decodePGCursor(d)
	if err := d.Done(); err != nil {
		return PGCursor{}, err
	}
	c.Raw = bytes.Clone(data)
	return c, nil
}

func DecodePGList(data []byte, count uint64) (PGListPage, error) {
	var page PGListPage
	if uint64(len(data)) > math.MaxUint32 {
		return page, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(data, uint32(len(data)))
	v, b := d.Struct(1)
	// Retain the exact nested envelope, including compatible appended fields.
	start := b.Remaining()
	page.Cursor = decodePGCursor(b)
	used := start - b.Remaining()
	// Outer envelope is six bytes; current compatible PGNLS starts with cursor.
	if len(data) >= 6+int(used) {
		page.Cursor.Raw = bytes.Clone(data[6 : 6+int(used)])
	}
	n := b.Count(12, math.MaxUint32)
	if uint64(n) > count {
		b.Fail(wire.ErrLimit)
	}
	if b.Err() != nil {
		return page, b.Err()
	}
	page.Entries = make([]PGListEntry, n)
	for i := range page.Entries {
		page.Entries[i] = PGListEntry{Namespace: b.String(), Object: b.String(), Key: b.String()}
	}
	if err := structDone(b, v, 1); err != nil {
		d.Fail(err)
	}
	if err := d.Done(); err != nil {
		return PGListPage{}, err
	}
	return page, nil
}

// EncodePGListRequest uses one PGNLS, empty object, head snapshot and PGOP.
// PG.Seed must already be the actual folded PG seed selected by the caller.
// StartEpoch is preserved as protocol metadata; no epoch refresh occurs.
func EncodePGListRequest(r PGListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	if features&(featureObjectLocator|featurePGID64) != featureObjectLocator|featurePGID64 || features&(featureNewOSDOp|featureNewOSDReply) != 0 {
		return msgr.MessageData{}, ErrFeatures
	}
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if r.MapEpoch == 0 || r.PG.Pool > math.MaxInt64 || r.Count == 0 && r.scrub == nil || r.MaxOutputBytes < MinPGListBytes || r.MaxOutputBytes > limit {
		return msgr.MessageData{}, ErrRequest
	}
	cursor := r.Cursor
	if len(cursor) == 0 {
		cursor = initialPGCursor()
	}
	code, payload, err := pgListPayload(r, cursor)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := boundedBytes(limit, 227, uint64(len(r.Namespace)), uint64(len(payload))); err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := boundedBytes(limit, 232, uint64(r.MaxOutputBytes)); err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := DecodePGCursor(cursor); err != nil && r.scrub == nil {
		return msgr.MessageData{}, err
	}
	e := wire.Encoder{}
	e.U32(r.ClientInc)
	e.U32(r.MapEpoch)
	e.U32(uint32(FlagRead | FlagOnDisk | FlagReturnVec | flagPGOp))
	e.U32(0)
	e.U32(0)
	e.U64(0)
	e.U32(0)
	encodeLocator(&e, Locator{Pool: int64(r.PG.Pool), Namespace: r.Namespace, Hash: int64(r.PG.Seed)})
	encodePG(&e, r.PG)
	e.String("")
	e.U16(1)
	e.U16(code)
	e.U32(0)
	e.U64(r.Count) // pgls.count; zero for SCRUBLS
	e.U32(r.StartEpoch)
	e.U64(0)
	e.U64(0)
	e.U32(uint32(len(payload)))
	e.U64(NoSnap)
	e.U64(0)
	e.U32(0)
	e.U32(0)
	e.U64(features)
	id := wire.Encoder{}
	id.U8(0)
	id.U64(0)
	id.U64(0)
	id.U32(0)
	e.Struct(2, 2, id.Data)
	return msgr.MessageData{Type: RequestMessage, Version: 6, CompatVersion: 3, Priority: 127, Front: e.Data, Data: bytes.Clone(payload)}, nil
}

func ValidatePGListReply(r PGListRequest, reply Reply, redirect bool) error {
	if reply.Object != "" || reply.PG != r.PG || reply.RetryAttempt != 0 || len(reply.Operations) != 1 {
		return errors.New("OSD PG listing reply identity differs from request")
	}
	want := PGNLS
	if r.Filter != nil {
		want = PGNLSFilter
	}
	op := reply.Operations[0]
	if op.Code != want || op.Flags != 0 || op.Offset != r.Count || op.Length != uint64(r.StartEpoch) || op.TruncateSize != 0 || op.TruncateSequence != 0 || uint64(len(op.Data)) != uint64(op.PayloadLength) || op.PayloadLength > r.MaxOutputBytes {
		return errors.New("OSD PG listing reply union or budget differs from request")
	}
	if redirect || reply.Result < 0 {
		if op.PayloadLength != 0 {
			return errors.New("failed OSD PG listing has output")
		}
		return nil
	}
	// PrimaryLogPG::do_pg_op reports page completion on the aggregate result;
	// the individual operation rval remains zero, including an end page.
	if (reply.Result != 0 && reply.Result != 1) || op.Result != 0 {
		return errors.New("invalid OSD PG listing completion result")
	}
	page, err := DecodePGList(op.Data, r.Count)
	if err != nil {
		return err
	}
	if r.Namespace != "\x01" {
		for _, entry := range page.Entries {
			if entry.Namespace != r.Namespace {
				return errors.New("OSD PG listing entry differs from requested namespace")
			}
		}
	}
	return nil
}

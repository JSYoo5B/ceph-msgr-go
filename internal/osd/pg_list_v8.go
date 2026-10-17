package osd

import (
	"bytes"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// EncodePGListRequestV8 implements dedicated head-only PGNLS in MOSDOp v8.
// Independently written from Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: osdc/Objecter.h (pg_read),
// messages/MOSDOp.h (v8 and get_object_locator), and osd/osd_types.h.
// LGPL-2.1 sources; no implementation copied. Actual PG and the separate raw
// hash both use the caller's explicit PG.Seed for this PG operation. An empty
// object selects native object_locator_t's explicit-hash variant. This is not
// object placement, a pool scan, split handling or automatic resubmission.
// NEW_OSDOPREPLY_ENCODING changes only the final negotiated-feature u64 here;
// SERVER_SQUID remains unsupported because it would select a v9 request.
func EncodePGListRequestV8(r PGListRequest, features uint64, limit uint32) (msgr.MessageData, error) {
	if features&(featureObjectLocator|featurePGID64|featureNewOSDOp|featureResendOnSplitMask) != featureObjectLocator|featurePGID64|featureNewOSDOp|featureResendOnSplitMask || features&featureServerSquidMask == featureServerSquidMask {
		return msgr.MessageData{}, ErrFeatures
	}
	return encodeReplicatedPGList(r, features, limit, 8)
}

func encodeReplicatedPGList(r PGListRequest, features uint64, limit uint32, version uint16) (msgr.MessageData, error) {
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
	// v8 is 23 bytes larger than the corresponding v6 request: versioned spg
	// and raw hash, preserved reqid incarnation and fixed three-u64 trace.
	var traceBytes uint64
	if version == 9 {
		traceBytes = emptyOTELBytes
	}
	code, payload, err := pgListPayload(r, cursor)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := boundedBytes(limit, 250, traceBytes, uint64(len(r.Namespace)), uint64(len(payload))); err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := boundedBytes(limit, 232, uint64(r.MaxOutputBytes)); err != nil {
		return msgr.MessageData{}, err
	}
	if _, err := DecodePGCursor(cursor); err != nil && r.scrub == nil {
		return msgr.MessageData{}, err
	}
	e := wire.Encoder{}
	shard := int8(-1)
	if r.Erasure {
		if r.Shard > 127 {
			return msgr.MessageData{}, ErrRequest
		}
		shard = int8(r.Shard)
	}
	encodeSPG(&e, r.PG, shard)
	e.U32(r.PG.Seed)
	e.U32(r.MapEpoch)
	e.U32(uint32(FlagRead | FlagOnDisk | FlagReturnVec | flagPGOp))
	identity := wire.Encoder{}
	identity.U8(0)
	identity.U64(0)
	identity.U64(0)
	identity.U32(r.ClientInc)
	e.Struct(2, 2, identity.Data)
	e.U64(0)
	e.U64(0)
	e.U64(0) // Empty native trace is always present in v8.
	if version == 9 {
		encodeEmptyOTEL(&e)
	}
	e.U32(r.ClientInc)
	e.U32(0)
	e.U32(0) // Read-only mtime.
	encodeLocator(&e, Locator{Pool: int64(r.PG.Pool), Namespace: r.Namespace, Hash: int64(r.PG.Seed)})
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
	e.U32(0) // Attempt zero; no resend on split or unblock.
	e.U64(features)
	return msgr.MessageData{Type: RequestMessage, Version: version, CompatVersion: 3, Priority: 127, Front: e.Data, Data: bytes.Clone(payload)}, nil
}

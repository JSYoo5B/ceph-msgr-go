package osd

import (
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// These are full incarnation-aware masks from pinned Tentacle ceph_features.h.
// Their presence chooses the v8 encoder; defining this codec advertises nothing.
const featureResendOnSplitMask uint64 = 1<<21 | 1<<57
const featureServerSquidMask uint64 = 1<<49 | 1<<57

// V8Request separates the caller-selected actual PG and raw object hash.
// Shard -1 is native NO_SHARD for replicated requests. Nonnegative raw shard
// IDs are framing primitives only; this codec does not implement EC I/O.
// Public Request selects this encoder only for the replicated-PG profile.
type V8Request struct {
	Request
	PG    PG
	Shard int8
}

// EncodeRequestV8 independently implements the MOSDOp v8 layout from Ceph
// v20.2.4, 7f793731f1b39eb4f465e960113d2363c311b964: messages/MOSDOp.h,
// osd/osd_types.h (spg_t/osd_reqid_t), include/ceph_features.h and
// msg/Message.cc (empty three-u64 trace). LGPL-2.1; no native code copied.
// It requires NEW_OSDOP_ENCODING plus the complete RESEND_ON_SPLIT mask, and
// permits implemented NEW_OSDOPREPLY_ENCODING (bit 58) and refuses SERVER_SQUID
// (v9). Reply encoding does not change this request layout; negotiated features
// are retained in the final u64. It performs no routing,
// splitting, resend or feature advertisement. Session.Call supplies header TID;
// reqid keeps ClientInc so the daemon can derive the authenticated identity.
func EncodeRequestV8(r V8Request, features uint64, limit uint32) (msgr.MessageData, error) {
	if features&(featureObjectLocator|featurePGID64|featureNewOSDOp|featureResendOnSplitMask) != featureObjectLocator|featurePGID64|featureNewOSDOp|featureResendOnSplitMask || features&featureServerSquidMask == featureServerSquidMask {
		return msgr.MessageData{}, ErrFeatures
	}
	return encodeReplicatedRequest(replicatedRequest(r), features, limit, 8)
}

type replicatedRequest struct {
	Request
	PG    PG
	Shard int8
}

// The wire bodies differ only at the explicit empty OTEL envelope. The public
// entry points select their fixed version and validate its feature contract;
// this helper never selects a version from peer features or rewrites a frame.
func encodeReplicatedRequest(r replicatedRequest, features uint64, limit uint32, version uint16) (msgr.MessageData, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if r.PG.Pool > math.MaxInt64 || r.PG.Pool != uint64(r.Locator.Pool) || r.Shard < -1 || r.MapEpoch == 0 || r.Locator.Pool < 0 || r.Object == "" || r.Locator.Hash != -1 || len(r.Operations) == 0 || len(r.Operations) > MaxOperations {
		return msgr.MessageData{}, ErrRequest
	}
	// Include all fixed fields and length prefixes before allocating the front.
	const fixed = 23 + 41 + 4 + 4 + 4 + 8 + 12 + 6 + 8 + 4 + 4 + 4 + 8 + 17 + 4 + 2 + 8 + 8 + 4 + 4 + 8 + 6 + 21
	var traceBytes uint64
	if version == 9 {
		traceBytes = emptyOTELBytes
	}
	frontBytes, err := boundedBytes(limit, fixed, traceBytes, uint64(len(r.Locator.Key)), uint64(len(r.Locator.Namespace)), uint64(len(r.Object)), uint64(len(r.Operations))*opWireSize, uint64(len(r.Snaps))*8)
	if err != nil {
		return msgr.MessageData{}, err
	}
	flags, dataBytes, err := ValidateOperations(r.Operations, limit)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if r.MTimeNanoseconds >= 1_000_000_000 || flags&FlagWrite == 0 && (r.MTimeSeconds != 0 || r.MTimeNanoseconds != 0) {
		return msgr.MessageData{}, ErrRequest
	}
	snap, err := requestSnapshot(r.Request, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if r.OrderSnap {
		flags |= FlagOrderSnap
	}
	full, err := requestFullFlags(r.Request, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	flags |= full
	replica, err := requestReplicaFlags(r.Request, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	flags |= replica
	if _, err := boundedBytes(limit, frontBytes, dataBytes); err != nil {
		return msgr.MessageData{}, err
	}
	replyBudget, err := boundedBytes(limit, 190, uint64(len(r.Object)), uint64(len(r.Operations))*(opWireSize+4))
	if err != nil {
		return msgr.MessageData{}, err
	}
	for _, op := range r.Operations {
		var outputBytes uint64
		switch op.Code {
		case Read:
			outputBytes = op.Length
		case Stat:
			outputBytes = 16
		case GetXattr, GetXattrs:
			outputBytes = uint64(op.Xattr.MaxOutputBytes)
		case OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys:
			outputBytes = uint64(op.Omap.MaxOutputBytes)
		case Call:
			outputBytes = uint64(op.Class.MaxOutputBytes)
		case Notify:
			outputBytes = 8
		case SparseRead:
			outputBytes = uint64(op.SparseRead.MaxOutputBytes)
		case MapExt:
			outputBytes = uint64(op.MapExt.MaxOutputBytes)
		case Checksum:
			outputBytes = checksumReplyBytes(op)
		case ListWatchers:
			outputBytes = uint64(op.ListWatchers.MaxOutputBytes)
		case ListSnaps:
			outputBytes = uint64(op.ListSnaps.MaxOutputBytes)
		}
		replyBudget, err = boundedBytes(limit, replyBudget, outputBytes)
		if err != nil {
			return msgr.MessageData{}, err
		}
	}
	e := wire.Encoder{}
	encodeSPG(&e, r.PG, r.Shard)
	e.U32(r.Hash)
	e.U32(r.MapEpoch)
	e.U32(uint32(flags | FlagOnDisk | FlagReturnVec))
	identity := wire.Encoder{}
	identity.U8(0) // authenticated entity name is restored by the server.
	identity.U64(0)
	identity.U64(0) // header TID supplied by Session.Call.
	identity.U32(r.ClientInc)
	e.Struct(2, 2, identity.Data)
	e.U64(0)
	e.U64(0)
	e.U64(0) // empty native trace; always present in v8.
	if version == 9 {
		encodeEmptyOTEL(&e)
	}
	e.U32(r.ClientInc)
	e.U32(r.MTimeSeconds)
	e.U32(r.MTimeNanoseconds)
	encodeLocator(&e, r.Locator)
	e.String(r.Object)
	e.U16(uint16(len(r.Operations)))
	for _, op := range r.Operations {
		e.U16(op.Code)
		e.U32(op.Flags)
		payloadBytes := uint64(len(op.Data))
		if op.Code == Checksum {
			payloadBytes = checksumWidth(op.Checksum.Type)
		}
		if IsWatchOperation(op.Code) {
			payloadBytes = WatchDataBytes(op)
		}
		if IsOmapOperation(op.Code) {
			payloadBytes, _ = omapInputBytes(op, limit)
		}
		if IsWatchOperation(op.Code) {
			EncodeWatchUnion(&e, op)
		} else if IsXattrOperation(op.Code) {
			encodeXattrUnion(&e, op)
			payloadBytes += uint64(len(op.Xattr.Name))
		} else if op.Code == Call {
			encodeClassUnion(&e, op)
			payloadBytes += uint64(len(op.Class.Class)) + uint64(len(op.Class.Method))
		} else if op.Code == SetAllocHint {
			encodeAllocHintUnion(&e, op.AllocHint)
		} else if op.Code == Checksum {
			encodeChecksumUnion(&e, op)
		} else if op.Code == WriteSame {
			encodeWriteSameUnion(&e, op)
		} else if op.Code == CopyFrom || op.Code == CopyFrom2 {
			encodeCopyFromUnion(&e, op)
			payloadBytes += copyFromDataBytes(op)
		} else if op.Code == Rollback {
			encodeRollbackUnion(&e, op)
		} else {
			e.U64(op.Offset)
			if IsOmapOperation(op.Code) {
				e.U64(payloadBytes)
			} else if op.Code == AssertVersion {
				e.U64(op.Version)
			} else {
				e.U64(op.Length)
			}
			e.U64(op.TruncateSize)
			e.U32(op.TruncateSequence)
		}
		e.U32(uint32(payloadBytes))
	}
	e.U64(snap)
	e.U64(r.SnapSeq)
	e.U32(uint32(len(r.Snaps)))
	for _, s := range r.Snaps {
		e.U64(s)
	}
	e.U32(0) // first attempt, no automatic retry.
	e.U64(features)
	data := make([]byte, 0, int(dataBytes))
	for _, op := range r.Operations {
		if IsWatchOperation(op.Code) {
			data = AppendWatchData(data, op)
			continue
		}
		if op.Code == Checksum {
			data = append(data, encodeChecksumData(op.Checksum)...)
			continue
		}
		if IsOmapOperation(op.Code) {
			data = appendOmapInput(data, op)
			continue
		}
		if IsXattrOperation(op.Code) {
			data = append(data, op.Xattr.Name...)
		}
		if op.Code == Call {
			data = append(append(data, op.Class.Class...), op.Class.Method...)
		}
		if op.Code == CopyFrom || op.Code == CopyFrom2 {
			data = appendCopyFromData(data, op)
		}
		data = append(data, op.Data...)
	}
	return msgr.MessageData{Data: data, Type: RequestMessage, Version: version, CompatVersion: 3, Priority: 127, Front: e.Data}, nil
}

// spg_t is a versioned pg_t plus native signed one-byte shard ID.
func encodeSPG(e *wire.Encoder, pg PG, shard int8) {
	body := wire.Encoder{}
	encodePG(&body, pg)
	body.U8(uint8(shard))
	e.Struct(1, 1, body.Data)
}

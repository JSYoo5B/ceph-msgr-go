package osd

import (
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// EncodeRequest encodes the current Tentacle v6 branch selected by the
// negotiated OBJECTLOCATOR and PGID64 bits without NEW_OSDOP_ENCODING.
// It does not advertise or implement RESEND_ON_SPLIT or perform retries.
// Session.Call supplies the header TID; the default v6 reqid deliberately
// lets the server derive identity from the authenticated peer and that TID.
func EncodeRequest(r Request, features uint64, limit uint32) (msgr.MessageData, error) {
	if features&(featureObjectLocator|featurePGID64) != featureObjectLocator|featurePGID64 || features&featureNewOSDOp != 0 || features&featureNewOSDReply != 0 {
		return msgr.MessageData{}, ErrFeatures
	}
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if r.MapEpoch == 0 || r.Locator.Pool < 0 || r.Object == "" || r.Locator.Hash != -1 || len(r.Operations) == 0 || len(r.Operations) > MaxOperations {
		return msgr.MessageData{}, ErrRequest
	}
	// Include all fixed fields and length prefixes before allocating the front.
	const fixed = 41 + 4 + 4 + 4 + 8 + 12 + 6 + 8 + 4 + 4 + 4 + 8 + 17 + 4 + 2 + 8 + 8 + 4 + 4 + 8 + 6 + 21
	frontBytes, err := boundedBytes(limit, fixed, uint64(len(r.Locator.Key)), uint64(len(r.Locator.Namespace)), uint64(len(r.Object)), uint64(len(r.Operations))*opWireSize, uint64(len(r.Snaps))*8)
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
	snap, err := requestSnapshot(r, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	if r.OrderSnap {
		flags |= FlagOrderSnap
	}
	full, err := requestFullFlags(r, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	flags |= full
	replica, err := requestReplicaFlags(r, flags)
	if err != nil {
		return msgr.MessageData{}, err
	}
	flags |= replica
	if r.RetryAttempt < 0 || r.RetryAttempt > MaxRetryAttempt {
		return msgr.MessageData{}, ErrRequest
	}
	if r.RetryAttempt > 0 {
		flags |= FlagRetry
	}
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
	e.U32(r.ClientInc)
	e.U32(r.MapEpoch)
	e.U32(uint32(flags | FlagOnDisk | FlagReturnVec))
	e.U32(r.MTimeSeconds)
	e.U32(r.MTimeNanoseconds)
	e.U64(0) // zero reassert_version.version.
	e.U32(0) // zero reassert_version.epoch.
	encodeLocator(&e, r.Locator)
	encodePG(&e, PG{Pool: uint64(r.Locator.Pool), Seed: r.Hash})
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
	e.U32(uint32(r.RetryAttempt)) // zero unless the caller explicitly resubmits.
	e.U64(features)
	identity := wire.Encoder{}
	identity.U8(0) // default entity_name_t type.
	identity.U64(0)
	identity.U64(0) // derive TID from header, set by Session.Call.
	identity.U32(0) // server restores client_inc for this default reqid.
	e.Struct(2, 2, identity.Data)
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
	return msgr.MessageData{Data: data, Type: RequestMessage, Version: 6, CompatVersion: 3, Priority: 127, Front: e.Data}, nil
}

func encodePG(e *wire.Encoder, pg PG) {
	e.U8(1)
	e.U64(pg.Pool)
	e.U32(pg.Seed)
	e.U32(math.MaxUint32) // legacy preferred OSD, none.
}

func encodeLocator(e *wire.Encoder, loc Locator) {
	body := wire.Encoder{}
	body.U64(uint64(loc.Pool))
	body.U32(math.MaxUint32) // legacy preferred OSD, none.
	body.String(loc.Key)
	body.String(loc.Namespace)
	body.U64(uint64(loc.Hash))
	compat := uint8(3)
	if loc.Hash != -1 {
		compat = 6
	}
	e.Struct(6, compat, body.Data)
}

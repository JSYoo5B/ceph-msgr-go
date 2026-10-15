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
	frontBytes, err := boundedBytes(limit, fixed, uint64(len(r.Locator.Key)), uint64(len(r.Locator.Namespace)), uint64(len(r.Object)), uint64(len(r.Operations))*opWireSize)
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
		if IsOmapOperation(op.Code) {
			payloadBytes, _ = omapInputBytes(op, limit)
		}
		if IsXattrOperation(op.Code) {
			encodeXattrUnion(&e, op)
			payloadBytes += uint64(len(op.Xattr.Name))
		} else if op.Code == Call {
			encodeClassUnion(&e, op)
			payloadBytes += uint64(len(op.Class.Class)) + uint64(len(op.Class.Method))
		} else {
			e.U64(op.Offset)
			if IsOmapOperation(op.Code) {
				e.U64(payloadBytes)
			} else if op.Code == AssertVersion {
				e.U64(op.Version)
			} else {
				e.U64(op.Length)
			}
			e.U64(0) // truncate_size.
			e.U32(0) // truncate_seq.
		}
		e.U32(uint32(payloadBytes))
	}
	e.U64(NoSnap)
	e.U64(0) // snap sequence.
	e.U32(0) // empty snap context.
	e.U32(0) // first attempt, no automatic retry.
	e.U64(features)
	identity := wire.Encoder{}
	identity.U8(0) // default entity_name_t type.
	identity.U64(0)
	identity.U64(0) // derive TID from header, set by Session.Call.
	identity.U32(0) // server restores client_inc for this default reqid.
	e.Struct(2, 2, identity.Data)
	data := make([]byte, 0, int(dataBytes))
	for _, op := range r.Operations {
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

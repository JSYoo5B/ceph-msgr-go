package osd

import "github.com/JSYoo5B/ceph-msgr-go/internal/wire"

// The ceph_osd_op vector and its per-operation input data have one layout in
// every admitted MOSDOp version (v6, v8, v9). These helpers own that shared
// layout only. Envelope fields, identity, negotiated version/feature selection
// and admission stay at the version-specific entry points, which call these
// helpers at the position their own envelope defines.

// encodeOperations writes the u16 operation count followed by each
// ceph_osd_op: code, flags, the opcode-selected union and the input payload
// length. The payload length covers every input byte appendOperationData
// writes for the operation, including xattr names, class/method names and
// copy-from metadata that precede the caller data.
func encodeOperations(e *wire.Encoder, ops []Operation, limit uint32) {
	e.U16(uint16(len(ops)))
	for _, op := range ops {
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
		if op.Code == SetChunk {
			// The union stays zero; the indata carries the chunk.
			payloadBytes = setChunkDataBytes(op.SetChunk)
		}
		if IsWatchOperation(op.Code) {
			EncodeWatchUnion(e, op)
		} else if IsXattrOperation(op.Code) {
			encodeXattrUnion(e, op)
			payloadBytes += uint64(len(op.Xattr.Name))
		} else if op.Code == Call {
			encodeClassUnion(e, op)
			payloadBytes += uint64(len(op.Class.Class)) + uint64(len(op.Class.Method))
		} else if op.Code == SetAllocHint {
			encodeAllocHintUnion(e, op.AllocHint)
		} else if op.Code == Checksum {
			encodeChecksumUnion(e, op)
		} else if op.Code == WriteSame {
			encodeWriteSameUnion(e, op)
		} else if usesCopyFromUnion(op.Code) {
			encodeCopyFromUnion(e, op)
			payloadBytes += copyFromDataBytes(op)
		} else if op.Code == Rollback {
			encodeRollbackUnion(e, op)
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
}

// appendOperationData appends each operation's input bytes in vector order.
// Each operation contributes exactly the payload length encodeOperations
// wrote for it, so the daemon splits the shared data segment by those lengths.
func appendOperationData(data []byte, ops []Operation) []byte {
	for _, op := range ops {
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
		if op.Code == SetChunk {
			data = appendSetChunkData(data, op.SetChunk)
			continue
		}
		if IsXattrOperation(op.Code) {
			data = append(data, op.Xattr.Name...)
		}
		if op.Code == Call {
			data = append(append(data, op.Class.Class...), op.Class.Method...)
		}
		if usesCopyFromUnion(op.Code) {
			data = appendCopyFromData(data, op)
		}
		data = append(data, op.Data...)
	}
	return data
}

// operationReplyBudget adds each operation's bounded reply output to the
// caller-computed reply envelope budget and rejects a total above limit.
func operationReplyBudget(limit uint32, budget uint64, ops []Operation) (uint64, error) {
	for _, op := range ops {
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
		case Isdirty:
			// ISDIRTY returns one encoded bool (dirty state) as output.
			outputBytes = 1
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
		var err error
		budget, err = boundedBytes(limit, budget, outputBytes)
		if err != nil {
			return 0, err
		}
	}
	return budget, nil
}

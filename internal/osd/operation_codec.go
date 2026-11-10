package osd

import "github.com/JSYoo5B/ceph-msgr-go/internal/wire"

// The ceph_osd_op vector, its per-operation input data and the per-operation
// reply metadata have one layout in every admitted MOSDOp version (v6, v8, v9)
// and MOSDOpReply version (v6, v8). These helpers own that shared layout only.
// Envelope fields, identity, negotiated version/feature selection, admission
// and redirect rules stay at the version-specific entry points, which call
// these helpers at the position their own envelope defines.

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

// decodeOperations reads the reply's u32 operation count and each
// ceph_osd_op header and union. An empty vector or an unsupported opcode
// fails the decoder; the returned slice keeps every field decoded so far.
func decodeOperations(d *wire.Decoder) []OperationResult {
	n := d.Count(opWireSize+4, MaxOperations)
	if n == 0 && d.Err() == nil {
		d.Fail(ErrOperation)
	}
	ops := make([]OperationResult, n)
	for i := range ops {
		op := &ops[i]
		op.Code = d.U16()
		op.Flags = d.U32()
		if IsWatchOperation(op.Code) {
			op.Watch = DecodeWatchUnion(d, op.Code)
		} else if IsXattrOperation(op.Code) {
			op.Xattr = decodeXattrUnion(d, op.Code)
		} else if op.Code == Call {
			op.Class = decodeClassUnion(d)
		} else if op.Code == SetAllocHint {
			op.AllocHint = decodeAllocHintUnion(d)
		} else if op.Code == Checksum {
			op.Offset, op.Length, op.Checksum = decodeChecksumUnion(d)
		} else if usesCopyFromUnion(op.Code) {
			op.CopyFrom = decodeCopyFromUnion(d)
		} else if op.Code == WriteSame {
			op.Offset, op.Length, op.WriteSamePatternLength = decodeWriteSameUnion(d)
		} else if op.Code == Rollback {
			op.Snap = decodeRollbackUnion(d)
		} else {
			op.Offset, op.Length = d.U64(), d.U64()
			op.TruncateSize, op.TruncateSequence = d.U64(), d.U32()
		}
		op.PayloadLength = d.U32()
		if !SupportedOperation(op.Code) {
			d.Fail(ErrOperation)
		}
	}
	return ops
}

// decodeOperationResults reads one signed return code per decoded operation.
func decodeOperationResults(d *wire.Decoder, ops []OperationResult) {
	for i := range ops {
		ops[i].Result = int32(d.U32())
	}
}

// splitOperationData assigns the reply data segment to operations by their
// declared payload lengths and rejects short or unassigned trailing data.
// The caller wraps the error with its version-specific context.
func splitOperationData(raw []byte, limit uint32, ops []OperationResult) error {
	data := wire.NewDecoderLimit(raw, limit)
	for i := range ops {
		ops[i].Data = data.Raw(ops[i].PayloadLength)
	}
	return data.Done()
}

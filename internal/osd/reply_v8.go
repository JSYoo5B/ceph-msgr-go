package osd

import (
	"bytes"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// DecodeReplyV8 decodes the current Tentacle MOSDOpReply v8/compat2 branch.
// Independently translated from Ceph 20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964 messages/MOSDOpReply.h and
// msg/Message.cc (LGPL-2.1; no upstream implementation copied).
// The caller must require negotiated NEW_OSDOPREPLY_ENCODING before selecting
// this decoder. Implementing a codec alone does not authorize advertising it.
//
// This reply contains pg_t, not spg_t, reqid or client incarnation. Header TID
// is its transaction identity; request-side shard/identity metadata must remain
// available to the caller rather than being fabricated from the reply.
// Signed aggregate/per-operation results and completion flags remain raw.
func DecodeReplyV8(m msgr.MessageData, limit uint32) (Reply, error) {
	var r Reply
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if _, err := boundedBytes(limit, 41, uint64(len(m.Front)), uint64(len(m.Middle)), uint64(len(m.Data))); err != nil {
		return r, err
	}
	r.RawFront, r.RawData = bytes.Clone(m.Front), bytes.Clone(m.Data)
	if m.Type != ReplyMessage || len(m.Middle) != 0 {
		return r, msgr.ErrFrame
	}
	if m.Version != 8 || m.CompatVersion != 2 {
		return r, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(r.RawFront, limit)
	r.Object = d.String()
	r.PG = decodePG(d)
	r.Flags = d.U64()
	r.Result = int32(d.U32())
	r.LegacyVersion = decodeVersion(d)
	r.MapEpoch = d.U32()
	n := d.Count(opWireSize+4, MaxOperations)
	if n == 0 && d.Err() == nil {
		d.Fail(ErrOperation)
	}
	r.Operations = make([]OperationResult, n)
	for i := range r.Operations {
		op := &r.Operations[i]
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
	r.RetryAttempt = int32(d.U32())
	for i := range r.Operations {
		r.Operations[i].Result = int32(d.U32())
	}
	r.ReplayVersion = decodeVersion(d)
	r.UserVersion = d.U64()
	// v8 alone replaces v6's always-present redirect structure with a bool
	// and a conditional structure. The native empty locator uses pool -1.
	r.Redirect = Redirect{Locator: Locator{Pool: -1, Hash: -1}}
	redirect := d.Bool()
	if redirect {
		r.Redirect = decodeRedirect(d)
		// Current native encoding derives the flag from !redirect.empty().
		if r.Redirect.Empty() {
			d.Fail(msgr.ErrFrame)
		}
	}
	for i := range r.Trace {
		r.Trace[i] = d.U64()
	}
	if err := d.Done(); err != nil {
		return r, err
	}
	data := wire.NewDecoderLimit(r.RawData, limit)
	for i := range r.Operations {
		r.Operations[i].Data = data.Raw(r.Operations[i].PayloadLength)
	}
	if err := data.Done(); err != nil {
		return r, fmt.Errorf("ceph osd v8 reply operation data: %w", err)
	}
	if redirect {
		return r, ErrRedirect
	}
	return r, nil
}

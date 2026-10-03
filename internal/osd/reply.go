package osd

import (
	"bytes"
	"fmt"
	"math"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// DecodeReply reads the Tentacle v6 reply selected when PGID64 is negotiated
// without NEW_OSDOPREPLY_ENCODING. It retains raw bytes and per-operation
// result/data metadata. Returned slices are owned by Reply.
// A decoded redirect is returned with ErrRedirect; it is never followed.
func DecodeReply(m msgr.MessageData, limit uint32) (Reply, error) {
	var r Reply
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(41)+uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data)) > uint64(limit) {
		return r, wire.ErrLimit
	}
	r.RawFront, r.RawData = bytes.Clone(m.Front), bytes.Clone(m.Data)
	if m.Type != ReplyMessage || len(m.Middle) != 0 {
		return r, msgr.ErrFrame
	}
	if m.Version != 6 || m.CompatVersion != 2 {
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
		op.Offset, op.Length = d.U64(), d.U64()
		op.TruncateSize, op.TruncateSequence = d.U64(), d.U32()
		op.PayloadLength = d.U32()
		if op.Code != Stat && op.Code != Read {
			d.Fail(ErrOperation)
		}
	}
	r.RetryAttempt = int32(d.U32())
	for i := range r.Operations {
		r.Operations[i].Result = int32(d.U32())
	}
	r.ReplayVersion = decodeVersion(d)
	r.UserVersion = d.U64()
	r.Redirect = decodeRedirect(d)
	// MOSDOpReply::encode_payload always appends Message::encode_trace, even
	// after selecting header version 6. Native decoding tolerates this suffix.
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
		return r, fmt.Errorf("ceph osd reply operation data: %w", err)
	}
	if !r.Redirect.Empty() {
		return r, ErrRedirect
	}
	return r, nil
}

func decodePG(d *wire.Decoder) PG {
	version := d.U8()
	pg := PG{Pool: d.U64(), Seed: d.U32()}
	preferred := d.U32()
	if version != 1 {
		d.Fail(wire.ErrVersion)
	}
	if preferred != math.MaxUint32 {
		d.Fail(msgr.ErrFrame)
	}
	return pg
}

func decodeVersion(d *wire.Decoder) Version {
	return Version{Version: d.U64(), Epoch: d.U32()}
}

func decodeLocator(d *wire.Decoder) Locator {
	v, body := d.Struct(6)
	loc := Locator{Pool: int64(body.U64())}
	preferred := body.U32()
	loc.Key, loc.Namespace = body.String(), body.String()
	loc.Hash = int64(body.U64())
	if v != 6 {
		body.Fail(wire.ErrVersion)
	}
	if preferred != math.MaxUint32 || loc.Hash < -1 || loc.Hash >= 0 && loc.Key != "" {
		body.Fail(msgr.ErrFrame)
	}
	d.Fail(body.Done())
	return loc
}

func decodeRedirect(d *wire.Decoder) Redirect {
	v, body := d.Struct(1)
	r := Redirect{Locator: decodeLocator(body)}
	r.Object = body.String()
	r.LegacyInstructions = body.Bytes()
	if v != 1 {
		body.Fail(wire.ErrVersion)
	}
	d.Fail(body.Done())
	return r
}

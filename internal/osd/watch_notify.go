package osd

import (
	"bytes"
	"errors"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently translated from Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964,
// include/rados.h, osdc/Objecter.h, messages/MWatchNotify.h and osd/Watch.cc.
// Upstream sources are LGPL-2.1; no implementation is copied.
const (
	Watch                uint16 = 0x220f
	Notify               uint16 = 0x1206
	NotifyAck            uint16 = 0x1207
	WatchNotifyMessage   uint16 = 44
	WatchUnwatch         uint8  = 0
	WatchRegister        uint8  = 3
	WatchReconnect       uint8  = 5
	WatchPing            uint8  = 7
	WatchEventNotify     uint8  = 1
	WatchEventComplete   uint8  = 2
	WatchEventDisconnect uint8  = 3
)

// Data carries the notification or ACK blob; this structure holds wire metadata.
type WatchInput struct {
	Cookie, Version, NotifyID                   uint64
	Generation, TimeoutSeconds, ProtocolVersion uint32
	Action                                      uint8
}

func IsWatchOperation(code uint16) bool { return code == Watch || code == Notify || code == NotifyAck }

func ValidateWatchOperation(op Operation, limit uint32) (uint64, uint64, error) {
	if !IsWatchOperation(op.Code) {
		return 0, 0, ErrOperation
	}
	if op.Flags != 0 {
		return 0, 0, ErrOperation
	}
	if op.Offset != 0 || op.Length != 0 || op.Version != 0 || op.Snap != 0 || op.Xattr != (XattrInput{}) || !omapInputEmpty(op.Omap) || op.Class != (ClassInput{}) || op.AllocHint != (AllocHintInput{}) || op.ListSnaps != (ListSnapsInput{}) || op.ListWatchers != (ListWatchersInput{}) || op.MapExt != (MapExtInput{}) || op.Checksum != (ChecksumInput{}) {
		return 0, 0, ErrRequest
	}
	w := op.Watch
	var size uint64
	mode := uint64(FlagRead)
	switch op.Code {
	case Watch:
		if w.NotifyID != 0 || w.ProtocolVersion != 0 || w.Version != 0 || len(op.Data) != 0 {
			return 0, 0, ErrRequest
		}
		switch w.Action {
		case WatchRegister, WatchReconnect, WatchPing, WatchUnwatch:
		default:
			return 0, 0, ErrOperation
		}
		mode = FlagWrite
	case Notify:
		if w.Action != 0 || w.Generation != 0 || w.NotifyID != 0 || w.Version != 0 || w.ProtocolVersion != 1 {
			return 0, 0, ErrRequest
		}
		size = 12 + uint64(len(op.Data))
	case NotifyAck:
		if w.Action != 0 || w.Generation != 0 || w.TimeoutSeconds != 0 || w.ProtocolVersion != 0 || w.Version != 0 || w.NotifyID == 0 {
			return 0, 0, ErrRequest
		}
		size = 20 + uint64(len(op.Data))
	}
	if size > uint64(limit) {
		return 0, 0, wire.ErrLimit
	}
	return mode, size, nil
}

func EncodeWatchUnion(e *wire.Encoder, op Operation) {
	w := op.Watch
	switch op.Code {
	case Watch:
		e.U64(w.Cookie)
		e.U64(w.Version)
		e.U8(w.Action)
		e.U32(w.Generation)
		e.U32(w.TimeoutSeconds)
		e.Raw(make([]byte, 3))
	case Notify:
		e.U64(w.Cookie)
		e.Raw(make([]byte, 20))
	case NotifyAck:
		e.Raw(make([]byte, 28))
	}
}

func DecodeWatchUnion(d *wire.Decoder, code uint16) WatchInput {
	var w WatchInput
	n := uint32(28)
	if code == Watch {
		w.Cookie = d.U64()
		w.Version = d.U64()
		w.Action = d.U8()
		w.Generation = d.U32()
		w.TimeoutSeconds = d.U32()
		n = 3
	}
	if code == Notify {
		w.Cookie = d.U64()
		n = 20
	}
	for _, b := range d.Raw(n) {
		if b != 0 {
			d.Fail(msgr.ErrFrame)
		}
	}
	return w
}

func AppendWatchData(dst []byte, op Operation) []byte {
	e := wire.Encoder{Data: dst}
	switch op.Code {
	case Notify:
		e.U32(op.Watch.ProtocolVersion)
		e.U32(op.Watch.TimeoutSeconds)
		e.Bytes(op.Data)
	case NotifyAck:
		e.U64(op.Watch.NotifyID)
		e.U64(op.Watch.Cookie)
		e.Bytes(op.Data)
	}
	return e.Data
}

func WatchDataBytes(op Operation) uint64 {
	switch op.Code {
	case Notify:
		return 12 + uint64(len(op.Data))
	case NotifyAck:
		return 20 + uint64(len(op.Data))
	}
	return 0
}

func ValidateWatchReply(requested Operation, reply OperationResult, success bool) error {
	w := requested.Watch
	if requested.Code == Notify {
		w = WatchInput{Cookie: w.Cookie}
	}
	if requested.Code == NotifyAck {
		w = WatchInput{}
	}
	if reply.Code != requested.Code || reply.Watch != w || reply.Offset != 0 || reply.Length != 0 || reply.TruncateSize != 0 || reply.TruncateSequence != 0 {
		return errors.New("OSD watch operation reply differs from request")
	}
	want := uint32(0)
	if requested.Code == Notify && success {
		want = 8
	}
	if reply.PayloadLength != want || len(reply.Data) != int(want) {
		return errors.New("OSD watch operation reply has invalid output")
	}
	return nil
}

type NotifyIdentity struct{ GlobalID, Cookie uint64 }
type NotifyReply struct {
	NotifyIdentity
	Data []byte
}
type WatchEvent struct {
	Opcode                                      uint8
	Cookie, Version, NotifyID, NotifierGlobalID uint64
	ReturnCode                                  int32
	Payload, RawFront, RawData                  []byte
	Replies                                     []NotifyReply
	Missed                                      []NotifyIdentity
}

// DecodeWatchEvent accepts only the current Tentacle v3 event envelope and
// keeps front notification bytes separate from completion message data.
func DecodeWatchEvent(m msgr.MessageData, limit uint32) (WatchEvent, error) {
	var event WatchEvent
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if _, err := boundedBytes(limit, 41, uint64(len(m.Front)), uint64(len(m.Middle)), uint64(len(m.Data))); err != nil {
		return event, err
	}
	if m.Type != WatchNotifyMessage || len(m.Middle) != 0 {
		return event, msgr.ErrFrame
	}
	if m.Version != 3 || m.CompatVersion != 1 {
		return event, wire.ErrVersion
	}
	event.RawFront, event.RawData = bytes.Clone(m.Front), bytes.Clone(m.Data)
	d := wire.NewDecoderLimit(event.RawFront, limit)
	if d.U8() != 1 {
		d.Fail(wire.ErrVersion)
	}
	event.Opcode = d.U8()
	event.Cookie = d.U64()
	event.Version = d.U64()
	event.NotifyID = d.U64()
	event.Payload = d.Bytes()
	event.ReturnCode = int32(d.U32())
	event.NotifierGlobalID = d.U64()
	if err := d.Done(); err != nil {
		return event, err
	}
	switch event.Opcode {
	case WatchEventNotify, WatchEventDisconnect:
		if len(event.RawData) != 0 {
			return event, msgr.ErrFrame
		}
	case WatchEventComplete:
		if len(event.Payload) != 0 {
			return event, msgr.ErrFrame
		}
		data := wire.NewDecoderLimit(event.RawData, limit)
		n := data.Count(20, 65536)
		event.Replies = make([]NotifyReply, n)
		var previous NotifyIdentity
		for i := range event.Replies {
			id := NotifyIdentity{GlobalID: data.U64(), Cookie: data.U64()}
			if i > 0 && (id.GlobalID < previous.GlobalID || id.GlobalID == previous.GlobalID && id.Cookie <= previous.Cookie) {
				data.Fail(msgr.ErrFrame)
			}
			event.Replies[i] = NotifyReply{NotifyIdentity: id, Data: data.Bytes()}
			previous = id
		}
		n = data.Count(16, 65536)
		event.Missed = make([]NotifyIdentity, n)
		for i := range event.Missed {
			event.Missed[i] = NotifyIdentity{GlobalID: data.U64(), Cookie: data.U64()}
		}
		if err := data.Done(); err != nil {
			return event, err
		}
	default:
		return event, ErrOperation
	}
	return event, nil
}

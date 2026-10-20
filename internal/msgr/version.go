package msgr

import (
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MMonGetVersion and MMonGetVersionReply, independently translated from Ceph
// 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964:
// messages/MMonGetVersion.h, messages/MMonGetVersionReply.h and
// mon/Monitor.cc handle_get_version (LGPL-2.1; no code copied). The MON copies
// the request's header TID and payload handle into the reply.
const (
	MonGetVersionMessage      uint16 = 19
	MonGetVersionReplyMessage uint16 = 20
)

// MonGetVersion encodes one request for a map's committed version range.
func MonGetVersion(handle uint64, what string) MessageData {
	e := wire.Encoder{}
	e.U64(handle)
	e.String(what)
	return MessageData{Type: MonGetVersionMessage, Version: 1, Front: e.Data, Priority: 127}
}

// MonGetVersionReplyResult is the decoded reply: the echoed handle, the
// newest committed version and (header version 2) the oldest one.
type MonGetVersionReplyResult struct {
	Handle, Newest, Oldest uint64
}

// DecodeMonGetVersionReply decodes a v1 or v2 reply and refuses trailing bytes.
func DecodeMonGetVersionReply(m MessageData, limit uint32) (MonGetVersionReplyResult, error) {
	var r MonGetVersionReplyResult
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if m.Type != MonGetVersionReplyMessage || len(m.Middle) != 0 || len(m.Data) != 0 || m.Version == 0 || m.Version > 2 {
		return r, ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	r.Handle, r.Newest = d.U64(), d.U64()
	if m.Version >= 2 {
		r.Oldest = d.U64()
	}
	if err := d.Done(); err != nil {
		return MonGetVersionReplyResult{}, err
	}
	return r, nil
}

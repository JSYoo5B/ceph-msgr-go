package msgr

import (
	"bytes"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MPing, independently translated from Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MPing.h,
// mon/Monitor.cc handle_ping and mon/MonClient.h MonClientPinger (LGPL-2.1;
// no native code copied). Both directions use type 2 and TID zero.
const PingMessage uint16 = 2

// Ping requests one MON ping without a payload. Message::encode normalizes
// the native fresh empty request's default compat zero to version one.
func Ping() MessageData {
	return MessageData{Type: PingMessage, Version: 1, CompatVersion: 1, Priority: 127}
}

// PingReply decodes the MON's length-prefixed raw string, without interpreting
// its JSON. Monitor::handle_ping sets that payload before Message::encode, so
// the native reply retains compat zero (the empty-payload normalization does
// not run). Only that current producer envelope is accepted. The limit covers
// the 41-byte message header and all segments before the returned copy is made.
func PingReply(m MessageData, limit uint32) ([]byte, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if m.Type != PingMessage || m.Transaction != 0 || m.Version != 1 || m.CompatVersion != 0 || len(m.Middle) != 0 || len(m.Data) != 0 {
		return nil, ErrFrame
	}
	if uint64(41)+uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data)) > uint64(limit) {
		return nil, wire.ErrLimit
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	raw := d.Bytes()
	if err := d.Done(); err != nil {
		return nil, err
	}
	return bytes.Clone(raw), nil
}

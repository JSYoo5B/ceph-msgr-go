package msgr

import (
	"bytes"
	"fmt"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// In-session CephX messages of a daemon's MON session.
//
// MAuth (ceph/ceph/blob/v20.2.4/src/messages/MAuth.h) is a
// PaxosServiceMessage: the Paxos prefix (version, deprecated_session_mon,
// deprecated_session_mon_tid), protocol, auth_payload bufferlist and
// monmap_epoch, with the default 1/1 header. MAuthReply (MAuthReply.h) is
// protocol, signed result, global_id, result_bl and result_msg, also 1/1.
// The MON answers MAuth messages of one session in order and gives the reply
// no transaction ID; matching a reply to its request is the caller's.

// cephxProtocol is CEPH_AUTH_CEPHX.
const cephxProtocol uint32 = 2

// prioDefault is CEPH_MSG_PRIO_DEFAULT.
const prioDefault uint16 = 127

// AuthMessageRequest encodes an MAuth carrying a CephX auth_payload.
func AuthMessageRequest(payload []byte, monMapEpoch uint32) MessageData {
	e := wire.Encoder{}
	Paxos(&e)
	e.U32(cephxProtocol)
	e.Bytes(payload)
	e.U32(monMapEpoch)
	return MessageData{Type: AuthMessage, Version: 1, CompatVersion: 1, Priority: prioDefault, Front: e.Data}
}

// AuthReply is a decoded MAuthReply. Result is the wire result, not a host
// errno. ResultBL and Message own their memory.
type AuthReply struct {
	Protocol uint32
	Result   int32
	GlobalID uint64
	ResultBL []byte
	Message  string
}

// DecodeAuthReply decodes a front-only MAuthReply within limit, which
// includes the 41-byte Messenger header. Failure returns no partial reply.
func DecodeAuthReply(m MessageData, limit uint32) (AuthReply, error) {
	if m.Type != AuthReplyMessage {
		return AuthReply{}, ErrFrame
	}
	if m.CompatVersion > 1 {
		return AuthReply{}, fmt.Errorf("%w: auth reply compatibility version %d: %w", ErrFrame, m.CompatVersion, wire.ErrVersion)
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return AuthReply{}, wire.ErrLimit
	}
	if len(m.Middle) != 0 || len(m.Data) != 0 {
		return AuthReply{}, ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	r := AuthReply{Protocol: d.U32(), Result: int32(d.U32()), GlobalID: d.U64()}
	r.ResultBL = bytes.Clone(d.Bytes())
	r.Message = d.String()
	if err := d.Done(); err != nil {
		return AuthReply{}, fmt.Errorf("%w: invalid auth reply: %w", ErrFrame, err)
	}
	if r.Protocol != cephxProtocol {
		return AuthReply{}, fmt.Errorf("%w: auth reply protocol %d", ErrFrame, r.Protocol)
	}
	return r, nil
}

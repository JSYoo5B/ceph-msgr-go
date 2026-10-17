package mds

import (
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// MClientReclaim and MClientReclaimReply, independently translated from Ceph
// 20.2.4 commit 7f793731f1b39eb4f465e960113d2363c311b964: messages/
// MClientReclaim.h, messages/MClientReclaimReply.h, include/ceph_fs.h:161-162
// and 377, mds/Server.cc handle_client_reclaim/reclaim_session/
// finish_reclaim_session and client/Client.cc start_reclaim/finish_reclaim
// (LGPL-2.1; no code copied).
const (
	ClientReclaimMessage      uint16 = 27
	ClientReclaimReplyMessage uint16 = 28

	// ReclaimReset is CEPH_RECLAIM_RESET, the only flag the MDS honors.
	ReclaimReset uint32 = 1
	// ReclaimFinish is MClientReclaim::FLAG_FINISH and must travel alone.
	ReclaimFinish uint32 = 1 << 31

	// MaxReclaimUUID bounds the uuid string; native sends a printable UUID.
	MaxReclaimUUID = 256
)

// FeatureReclaimClient is CEPHFS_FEATURE_RECLAIM_CLIENT (cephfs_features.h:38),
// the MDS-side feature a client checks before sending a reclaim.
const FeatureReclaimClient = 10

// EncodeReclaim builds one MClientReclaim (header v1/compat 1). A RESET
// reclaim needs a nonempty uuid; the FINISH form carries an empty uuid and
// only that flag, as Client::finish_reclaim sends it.
func EncodeReclaim(uuid string, flags uint32) (msgr.MessageData, error) {
	switch flags {
	case ReclaimReset:
		if uuid == "" || len(uuid) > MaxReclaimUUID {
			return msgr.MessageData{}, ErrSession
		}
	case ReclaimFinish:
		if uuid != "" {
			return msgr.MessageData{}, ErrSession
		}
	default:
		return msgr.MessageData{}, ErrSession
	}
	e := wire.Encoder{}
	e.String(uuid)
	e.U32(flags)
	return msgr.MessageData{Type: ClientReclaimMessage, Priority: 127, Version: 1, CompatVersion: 1, Front: e.Data}, nil
}

// ReclaimReply is a decoded MClientReclaimReply: the signed result, the OSD
// map epoch the MDS had when the target was removed, and the target's
// addresses (empty when the MDS found no session with that uuid).
type ReclaimReply struct {
	Result    int32
	Epoch     uint32
	Addresses []msgr.Address
}

// DecodeReclaimReply decodes one server reply and refuses trailing bytes.
func DecodeReclaimReply(m msgr.MessageData, limit uint32) (ReclaimReply, error) {
	var r ReclaimReply
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return r, wire.ErrLimit
	}
	if m.Type != ClientReclaimReplyMessage || m.Version != 1 || m.CompatVersion != 1 || len(m.Middle) != 0 || len(m.Data) != 0 {
		return r, msgr.ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	r.Result = int32(d.U32())
	r.Epoch = d.U32()
	r.Addresses = msgr.DecodeAddresses(d)
	if err := d.Done(); err != nil {
		return ReclaimReply{}, err
	}
	return r, nil
}

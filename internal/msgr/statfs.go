package msgr

import (
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// MStatfs and MStatfsReply, independently translated from Ceph 20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MStatfs.h,
// messages/MStatfsReply.h, include/ceph_fs.h:218-227 and
// mon/MgrStatMonitor.cc preprocess_statfs (LGPL-2.1; no code copied).
const (
	StatfsMessage      uint16 = 13
	StatfsReplyMessage uint16 = 14
)

// Statfs encodes one MStatfs (header v2/compat 1). A nil dataPool asks for the
// cluster total, as librados cluster_stat does; otherwise the pool's figures,
// as libcephfs statfs does for a data pool. The header TID is assigned by
// Session.Call and echoed by the reply.
func Statfs(fsid [16]byte, dataPool *int64) MessageData {
	e := wire.Encoder{}
	Paxos(&e)
	e.Raw(fsid[:])
	if dataPool == nil {
		e.U8(0)
	} else {
		e.U8(1)
		e.U64(uint64(*dataPool))
	}
	return MessageData{Type: StatfsMessage, Version: 2, CompatVersion: 1, Front: e.Data, Priority: 127}
}

// StatfsReplyResult is the packed ceph_mon_statfs_reply.
type StatfsReplyResult struct {
	FSID                         [16]byte
	Version                      uint64
	KB, KBUsed, KBAvail, Objects uint64
}

// DecodeStatfsReply decodes a reply and refuses any size but the packed 56 bytes.
func DecodeStatfsReply(m MessageData, limit uint32) (StatfsReplyResult, error) {
	var r StatfsReplyResult
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if m.Type != StatfsReplyMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return r, ErrFrame
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	copy(r.FSID[:], d.Raw(16))
	r.Version = d.U64()
	r.KB, r.KBUsed, r.KBAvail, r.Objects = d.U64(), d.U64(), d.U64(), d.U64()
	if err := d.Done(); err != nil {
		return StatfsReplyResult{}, err
	}
	return r, nil
}

package msgr

import (
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MPoolOp and MPoolOpReply are independently translated from Ceph 20.2.4,
// commit 7f793731f1b39eb4f465e960113d2363c311b964: messages/MPoolOp.h,
// messages/MPoolOpReply.h, messages/PaxosServiceMessage.h, include/ceph_fs.h
// (POOL_OP_*) and mon/OSDMonitor.cc enforce_pool_op_caps/preprocess_pool_op/
// prepare_pool_op (LGPL-2.1; no code copied).
const (
	PoolOpReplyMessage uint16 = 48
	PoolOpMessage      uint16 = 49

	PoolOpCreateSnap          uint32 = 0x11
	PoolOpDeleteSnap          uint32 = 0x12
	PoolOpCreateUnmanagedSnap uint32 = 0x21
	PoolOpDeleteUnmanagedSnap uint32 = 0x22
)

// PoolOp encodes one MPoolOp (header v4/compat 2). The paxos version is zero
// and the deprecated session fields are -1/0, as for MMonCommand; the header
// TID is assigned by Session.Call. name is the pool snapshot name for the
// *Snap operations; snapid names the unmanaged snapshot to delete.
func PoolOp(fsid [16]byte, pool uint32, op uint32, snapid uint64, name string) MessageData {
	e := wire.Encoder{}
	Paxos(&e)
	e.Raw(fsid[:])
	e.U32(pool)
	e.U32(op)
	e.U64(0) // legacy auid
	e.U64(snapid)
	e.String(name)
	e.U8(0)  // pad for the v3->v4 encoding change
	e.U16(0) // crush_rule, meaningful only for pool creation
	return MessageData{Type: PoolOpMessage, Version: 4, CompatVersion: 2, Front: e.Data, Priority: 127}
}

// PoolOpReplyResult is a decoded MPoolOpReply.
type PoolOpReplyResult struct {
	Code  int32
	Epoch uint32
	Data  []byte
}

// DecodePoolOpReply decodes one reply and refuses trailing bytes.
func DecodePoolOpReply(m MessageData, limit uint32) (PoolOpReplyResult, error) {
	var r PoolOpReplyResult
	if m.Type != PoolOpReplyMessage || len(m.Middle) != 0 || len(m.Data) != 0 {
		return r, ErrFrame
	}
	if m.CompatVersion > 1 {
		return r, wire.ErrVersion
	}
	d := wire.NewDecoderLimit(m.Front, limit)
	d.U64()
	d.U16()
	d.U64()
	d.Raw(16) // fsid
	r.Code = int32(d.U32())
	r.Epoch = d.U32()
	if d.U8() != 0 {
		r.Data = append([]byte(nil), d.Bytes()...)
	}
	if err := d.Done(); err != nil {
		return PoolOpReplyResult{}, err
	}
	return r, nil
}

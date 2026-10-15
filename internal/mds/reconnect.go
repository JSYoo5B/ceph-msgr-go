package mds

import (
	"bytes"
	"slices"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientReconnect.h,
// mds/mdstypes.{h,cc} (cap_reconnect_t, snaprealm_reconnect_t) and
// include/ceph_fs.h (ceph_mds_cap_reconnect, ceph_mds_snaprealm_reconnect).
// LGPL-2.1 sources; no code copied.

const (
	ClientReconnectMessage uint16 = 23
	// FeatureMultiReconnect is the MDS feature that selects the current
	// MClientReconnect v5 (with the "more" flag) on native clients.
	FeatureMultiReconnect = 12
	reconnectVersion      = 5
	reconnectCompat       = 4
	capReconnectInfoBytes = 36
	MaxReconnectCaps      = 1 << 20
	MaxReconnectRealms    = 1 << 16
)

// CapReconnect is one cap_reconnect_t: the caller's capability on a head
// inode, its short path (base inode and relative path), the snap realm, the
// first pending cap snap and an opaque encoded file-lock blob.
type CapReconnect struct {
	Ino, CapID     uint64
	Wanted, Issued uint32
	SnapRealm      uint64
	PathBase       uint64
	Path           string
	SnapFollows    uint64
	Flock          []byte
}

// RealmReconnect is one snaprealm_reconnect_t.
type RealmReconnect struct {
	Ino, Seq, Parent uint64
}

// EncodeReconnect builds one MClientReconnect v5/compat4. The payload is in
// the data segment, as natively; caps are ordered by inode (std::map).
// Duplicate inodes are refused.
func EncodeReconnect(caps []CapReconnect, realms []RealmReconnect, more bool) (msgr.MessageData, error) {
	if len(caps) > MaxReconnectCaps || len(realms) > MaxReconnectRealms {
		return msgr.MessageData{}, wire.ErrLimit
	}
	ordered := slices.Clone(caps)
	slices.SortFunc(ordered, func(a, b CapReconnect) int {
		switch {
		case a.Ino < b.Ino:
			return -1
		case a.Ino > b.Ino:
			return 1
		}
		return 0
	})
	e := wire.Encoder{}
	e.U32(uint32(len(ordered)))
	for i, c := range ordered {
		if i > 0 && ordered[i-1].Ino == c.Ino {
			return msgr.MessageData{}, ErrRequest
		}
		e.U64(c.Ino)
		body := wire.Encoder{}
		body.String(c.Path)
		body.U64(c.CapID)
		body.U32(c.Wanted)
		body.U32(c.Issued)
		body.U64(c.SnapRealm)
		body.U64(c.PathBase)
		body.U32(uint32(len(c.Flock)))
		body.Raw(c.Flock)
		body.U64(c.SnapFollows)
		e.Struct(2, 1, body.Data)
	}
	e.U32(uint32(len(realms)))
	for _, r := range realms {
		body := wire.Encoder{}
		body.U64(r.Ino)
		body.U64(r.Seq)
		body.U64(r.Parent)
		e.Struct(1, 1, body.Data)
	}
	if more {
		e.U8(1)
	} else {
		e.U8(0)
	}
	return msgr.MessageData{Type: ClientReconnectMessage, Priority: 127, Version: reconnectVersion, CompatVersion: reconnectCompat, Data: e.Data}, nil
}

// DecodeReconnect decodes the exact v5 layout EncodeReconnect produces
// (vector comparison and synthetic MDS peers).
func DecodeReconnect(m msgr.MessageData, limit uint32) ([]CapReconnect, []RealmReconnect, bool, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return nil, nil, false, wire.ErrLimit
	}
	if m.Type != ClientReconnectMessage || len(m.Front) != 0 || len(m.Middle) != 0 {
		return nil, nil, false, msgr.ErrFrame
	}
	if m.Version != reconnectVersion || m.CompatVersion != reconnectCompat {
		return nil, nil, false, wire.ErrVersion
	}
	raw := bytes.Clone(m.Data)
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	n := d.Count(8+6+4+capReconnectInfoBytes+8, MaxReconnectCaps)
	var caps []CapReconnect
	if n != 0 {
		caps = make([]CapReconnect, n)
	}
	for i := range caps {
		c := CapReconnect{Ino: d.U64()}
		if i > 0 && c.Ino <= caps[i-1].Ino {
			d.Fail(ErrRequest) // std::map keys are unique and ordered
		}
		b := currentStruct(d, 2, 1)
		c.Path = b.String()
		c.CapID, c.Wanted, c.Issued, c.SnapRealm, c.PathBase = b.U64(), b.U32(), b.U32(), b.U64(), b.U64()
		c.Flock = cloneOrNil(b.Raw(b.U32()))
		c.SnapFollows = b.U64()
		d.Fail(b.Done())
		caps[i] = c
	}
	n = d.Count(6+24, MaxReconnectRealms)
	var realms []RealmReconnect
	if n != 0 {
		realms = make([]RealmReconnect, n)
	}
	for i := range realms {
		b := currentStruct(d, 1, 1)
		realms[i] = RealmReconnect{Ino: b.U64(), Seq: b.U64(), Parent: b.U64()}
		d.Fail(b.Done())
	}
	more := d.Bool()
	if err := d.Done(); err != nil {
		return nil, nil, false, err
	}
	return caps, realms, more, nil
}

package mds

import (
	"bytes"

	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientReply.h
// (LeaseStat, DirStat, InodeStat), mds/CInode.cc encode_inodestat,
// mds/Locker.cc encode_lease, mds/CDir.cc encode_dirstat, mds/Server.cc
// _finalize_readdir, common/snap_types.{h,cc}, include/frag.h,
// include/cephfs/types.h and include/ceph_fs.h. Only the REPLY_ENCODING
// (versioned) forms are implemented. LGPL-2.1 sources; no code copied.

const (
	MaxTraceFragSplits   uint32 = 1 << 16
	MaxTraceDistribution uint32 = 1 << 16
	MaxSnapMetadata      uint32 = 4096
	MaxReaddirEntries    uint32 = 1 << 20
	MaxSnapRealms        uint32 = 1 << 16
	MaxRealmSnaps        uint32 = 1 << 20
	inodeStatVersion            = 8
)

// ReplyCap is ceph_mds_reply_cap.
type ReplyCap struct {
	Caps, Wanted uint32
	CapID        uint64
	Seq, MSeq    uint32
	Realm        uint64
	Flags        uint8
}

// FileLayout is the legacy ceph_file_layout plus the pool namespace that the
// versioned InodeStat carries separately.
type FileLayout struct {
	StripeUnit, StripeCount, ObjectSize, CASHash, ObjectStripeUnit, Unused uint32
	Pool                                                                   int32
	PoolNamespace                                                          string
}

// FragSplit is one fragtree_t split: fragment Frag is split by Bits.
type FragSplit struct {
	Frag uint32
	Bits int32
}

// InodeStat is the REPLY_ENCODING InodeStat (struct v8/compat1). Xattrs keeps
// the raw xattr blob (an encoded map, or a zero count); OptionalMetadata keeps
// the raw optmetadata multiton envelope payload.
type InodeStat struct {
	Ino, SnapID                              uint64
	Rdev                                     uint32
	Version, XattrVersion                    uint64
	Cap                                      ReplyCap
	Layout                                   FileLayout
	CTime, MTime, ATime                      Time
	TimeWarpSeq                              uint32
	Size, MaxSize, TruncateSize              uint64
	TruncateSeq                              uint32
	Mode, UID, GID, NLink                    uint32
	Files, Subdirs, RBytes, RFiles, RSubdirs int64
	RCTime                                   Time
	FragTree                                 []FragSplit
	Symlink                                  string
	DirHash                                  uint8
	DirLayout                                [8]byte
	Xattrs                                   []byte
	InlineVersion                            uint64
	InlineData                               []byte
	QuotaMaxBytes, QuotaMaxFiles             int64
	BTime                                    Time
	ChangeAttr                               uint64
	DirPin                                   int32
	SnapBTime                                Time
	RSnaps                                   int64
	SnapMetadata                             map[string]string
	Fscrypt                                  bool
	FscryptAuth, FscryptFile                 []byte
	OptionalMetadata                         []byte
}

// DirStat is the versioned DirStat (v1/compat1).
type DirStat struct {
	Frag         uint32
	Auth         int32
	Distribution []int32
}

// LeaseStat is the versioned LeaseStat (v2/compat1).
type LeaseStat struct {
	Mask          uint16
	DurationMS    uint32
	Seq           uint32
	AlternateName []byte
}

// Trace is the decoded reply trace: the dentry's parent directory inode, its
// dirstat, the dentry name and lease (when IsDentry), then the target inode
// (when IsTarget).
type Trace struct {
	DirInode   *InodeStat
	Dir        *DirStat
	DentryName string
	Lease      *LeaseStat
	Target     *InodeStat
}

// DecodeTrace decodes a REPLY_ENCODING trace blob in the native client order.
// An empty trace is valid when neither record is present.
func DecodeTrace(raw []byte, isDentry, isTarget bool) (Trace, error) {
	var t Trace
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	if isDentry {
		dir := decodeInodeStat(d)
		stat := decodeDirStat(d)
		t.DirInode, t.Dir = &dir, &stat
		t.DentryName = d.String()
		lease := decodeLeaseStat(d)
		t.Lease = &lease
	}
	if isTarget {
		target := decodeInodeStat(d)
		t.Target = &target
	}
	if err := d.Done(); err != nil {
		return Trace{}, err
	}
	return t, nil
}

// ReaddirEntry is one entry of a readdir reply's extra blob.
type ReaddirEntry struct {
	Name  string
	Lease LeaseStat
	Inode InodeStat
}

// Readdir is the decoded readdir extra blob.
type Readdir struct {
	Dir     DirStat
	Flags   uint16
	Entries []ReaddirEntry
}

// DecodeReaddir decodes a readdir/snapdiff reply extra blob: DirStat, entry
// count, u16 flags, then name/LeaseStat/InodeStat per entry.
func DecodeReaddir(raw []byte) (Readdir, error) {
	var r Readdir
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	r.Dir = decodeDirStat(d)
	// Each entry has at least a name length, a lease and an inode envelope.
	n := d.Count(4+6+6, MaxReaddirEntries)
	r.Flags = d.U16()
	if n != 0 {
		r.Entries = make([]ReaddirEntry, n)
	}
	for i := range r.Entries {
		r.Entries[i].Name = d.String()
		r.Entries[i].Lease = decodeLeaseStat(d)
		r.Entries[i].Inode = decodeInodeStat(d)
	}
	if err := d.Done(); err != nil {
		return Readdir{}, err
	}
	return r, nil
}

func decodeInodeStat(d *wire.Decoder) InodeStat {
	var s InodeStat
	b := currentStruct(d, inodeStatVersion, 1)
	s.Ino, s.SnapID, s.Rdev, s.Version, s.XattrVersion = b.U64(), b.U64(), b.U32(), b.U64(), b.U64()
	s.Cap = ReplyCap{Caps: b.U32(), Wanted: b.U32(), CapID: b.U64(), Seq: b.U32(), MSeq: b.U32(), Realm: b.U64(), Flags: b.U8()}
	s.Layout = FileLayout{StripeUnit: b.U32(), StripeCount: b.U32(), ObjectSize: b.U32(), CASHash: b.U32(), ObjectStripeUnit: b.U32(), Unused: b.U32(), Pool: int32(b.U32())}
	s.CTime, s.MTime, s.ATime = decodeTime(b), decodeTime(b), decodeTime(b)
	s.TimeWarpSeq, s.Size, s.MaxSize, s.TruncateSize, s.TruncateSeq = b.U32(), b.U64(), b.U64(), b.U64(), b.U32()
	s.Mode, s.UID, s.GID, s.NLink = b.U32(), b.U32(), b.U32(), b.U32()
	s.Files, s.Subdirs = int64(b.U64()), int64(b.U64())
	s.RBytes, s.RFiles, s.RSubdirs = int64(b.U64()), int64(b.U64()), int64(b.U64())
	s.RCTime = decodeTime(b)
	n := b.Count(8, MaxTraceFragSplits)
	if n != 0 {
		s.FragTree = make([]FragSplit, n)
	}
	for i := range s.FragTree {
		s.FragTree[i] = FragSplit{Frag: b.U32(), Bits: int32(b.U32())}
	}
	s.Symlink = b.String()
	copy(s.DirLayout[:], b.Raw(8))
	s.DirHash = s.DirLayout[0]
	s.Xattrs = cloneOrNil(b.Bytes())
	s.InlineVersion = b.U64()
	s.InlineData = cloneOrNil(b.Bytes())
	quota := currentStruct(b, 1, 1)
	s.QuotaMaxBytes, s.QuotaMaxFiles = int64(quota.U64()), int64(quota.U64())
	b.Fail(quota.Done())
	s.Layout.PoolNamespace = b.String()
	s.BTime, s.ChangeAttr = decodeTime(b), b.U64()
	s.DirPin = int32(b.U32())
	s.SnapBTime = decodeTime(b)
	s.RSnaps = int64(b.U64())
	n = b.Count(8, MaxSnapMetadata)
	if n != 0 {
		s.SnapMetadata = make(map[string]string, n)
	}
	for range n {
		k, v := b.String(), b.String()
		if _, exists := s.SnapMetadata[k]; exists {
			b.Fail(ErrRequest)
		}
		s.SnapMetadata[k] = v
	}
	s.Fscrypt = b.Bool()
	s.FscryptAuth, s.FscryptFile = cloneOrNil(b.Bytes()), cloneOrNil(b.Bytes())
	// optmetadata_multiton: a v1/compat1 envelope around the option vector.
	// Its payload is preserved without interpreting option kinds.
	if b.U8() != 1 || b.U8() != 1 {
		b.Fail(wire.ErrVersion)
	}
	s.OptionalMetadata = cloneOrNil(b.Bytes())
	d.Fail(b.Done())
	return s
}

func decodeDirStat(d *wire.Decoder) DirStat {
	b := currentStruct(d, 1, 1)
	s := DirStat{Frag: b.U32(), Auth: int32(b.U32())}
	n := b.Count(4, MaxTraceDistribution)
	if n != 0 {
		s.Distribution = make([]int32, n)
	}
	for i := range s.Distribution {
		s.Distribution[i] = int32(b.U32())
		if i > 0 && s.Distribution[i] <= s.Distribution[i-1] {
			b.Fail(ErrRequest) // std::set is strictly ordered
		}
	}
	d.Fail(b.Done())
	return s
}

func decodeLeaseStat(d *wire.Decoder) LeaseStat {
	b := currentStruct(d, 2, 1)
	s := LeaseStat{Mask: b.U16(), DurationMS: b.U32(), Seq: b.U32(), AlternateName: cloneOrNil(b.Bytes())}
	d.Fail(b.Done())
	return s
}

// SnapRealm is SnapRealmInfoNew (v1/compat1): ceph_mds_snap_realm with its
// own and prior-parent snapshot lists, plus last_modified and change_attr.
type SnapRealm struct {
	Ino, Created, Parent, ParentSince, Seq uint64
	Snaps, PriorParentSnaps                []uint64
	LastModified                           Time
	ChangeAttr                             uint64
}

// DecodeSnapTrace decodes a NEW_SNAPREALM_INFO snap trace: a concatenation of
// SnapRealmInfoNew records, child realm first.
func DecodeSnapTrace(raw []byte) ([]SnapRealm, error) {
	var realms []SnapRealm
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	for d.Remaining() > 0 && d.Err() == nil {
		if uint32(len(realms)) >= MaxSnapRealms {
			d.Fail(wire.ErrLimit)
			break
		}
		b := currentStruct(d, 1, 1)
		r := SnapRealm{Ino: b.U64(), Created: b.U64(), Parent: b.U64(), ParentSince: b.U64(), Seq: b.U64()}
		snaps, prior := b.U32(), b.U32()
		if snaps > MaxRealmSnaps || prior > MaxRealmSnaps || (uint64(snaps)+uint64(prior))*8 > uint64(b.Remaining()) {
			b.Fail(wire.ErrLimit)
			snaps, prior = 0, 0
		}
		r.Snaps, r.PriorParentSnaps = readU64s(b, snaps), readU64s(b, prior)
		r.LastModified, r.ChangeAttr = decodeTime(b), b.U64()
		d.Fail(b.Done())
		realms = append(realms, r)
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return realms, nil
}

func readU64s(d *wire.Decoder, n uint32) []uint64 {
	if n == 0 {
		return nil
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = d.U64()
	}
	return out
}

// DecodeXattrs decodes an InodeStat xattr blob: a u32 count followed by
// name/value byte strings (map<string,bufferptr>). Names are unique.
func DecodeXattrs(raw []byte) (map[string][]byte, error) {
	if len(raw) == 0 {
		// The MDS sends an empty blob when xattrs were not requested.
		return nil, nil
	}
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	n := d.Count(8, 1<<20)
	out := make(map[string][]byte, n)
	for range n {
		k, v := d.String(), d.Bytes()
		if _, exists := out[k]; exists {
			d.Fail(ErrRequest)
		}
		out[k] = bytes.Clone(v)
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return out, nil
}

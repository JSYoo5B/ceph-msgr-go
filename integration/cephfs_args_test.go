package integration_test

import (
	"encoding/binary"
	"testing"
)

// Test-only consumer helpers that pack ceph_mds_request_args for live CephFS
// metadata acceptance. Request construction belongs to the consumer layer;
// the product sends the caller's packed union unchanged. Offsets follow the
// packed union in pinned include/ceph_fs.h (v20.2.4, LGPL-2.1, no code copied).

const cephfsArgsBytes = 56

// CephFS capability bits (CEPH_CAP_*), test-only.
const (
	cephCapPin         = 1
	cephCapAuthShared  = 1 << 2
	cephCapAuthExcl    = 2 << 2
	cephCapLinkShared  = 1 << 4
	cephCapXattrShared = 1 << 6
	cephCapFileShared  = 1 << 8
	cephCapFileExcl    = 2 << 8
	cephCapFileCache   = 4 << 8
	cephCapFileRD      = 8 << 8
	cephCapFileWR      = 16 << 8
	cephCapFileBuffer  = 32 << 8
	// CEPH_STAT_CAP_INODE_ALL-style getattr mask: pin, auth, link, xattr, file.
	cephStatAll = cephCapPin | cephCapAuthShared | cephCapLinkShared | cephCapXattrShared | cephCapFileShared

	cephOpenReadOnly  = 0
	cephOpenReadWrite = 2
	cephOpenCreate    = 0o100
	cephOpenExclusive = 0o200

	cephReaddirReplyBitflags = 1
)

func cephfsGetattrArgs(mask uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], mask)
	return a
}

func cephfsMkdirArgs(mode uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], mode)
	return a
}

// open/create: flags, mode, stripe_unit, stripe_count, object_size, pool,
// mask, old_size. Zero layout fields inherit the directory layout; pool -1
// keeps the default pool.
func cephfsOpenArgs(flags, mode, mask uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], flags)
	binary.LittleEndian.PutUint32(a[4:], mode)
	binary.LittleEndian.PutUint32(a[20:], ^uint32(0))
	binary.LittleEndian.PutUint32(a[24:], mask)
	return a
}

// readdir: frag, max_entries, max_bytes, flags (u16), offset_hash.
func cephfsReaddirArgs(frag, maxEntries, maxBytes uint32, flags uint16, offsetHash uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], frag)
	binary.LittleEndian.PutUint32(a[4:], maxEntries)
	binary.LittleEndian.PutUint32(a[8:], maxBytes)
	binary.LittleEndian.PutUint16(a[12:], flags)
	binary.LittleEndian.PutUint32(a[14:], offsetHash)
	return a
}

// setxattr: flags, osdmap_epoch. The name travels in path2 and the value in
// the request data segment.
func cephfsSetxattrArgs(flags, osdmapEpoch uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], flags)
	binary.LittleEndian.PutUint32(a[4:], osdmapEpoch)
	return a
}

func TestCephFSArgsLayout(t *testing.T) {
	a := cephfsReaddirArgs(1, 2, 3, 4, 5)
	if binary.LittleEndian.Uint16(a[12:]) != 4 || binary.LittleEndian.Uint32(a[14:]) != 5 {
		t.Fatal("packed readdir args")
	}
	o := cephfsOpenArgs(cephOpenCreate|cephOpenReadWrite, 0o644, cephCapFileRD)
	if binary.LittleEndian.Uint32(o[4:]) != 0o644 || int32(binary.LittleEndian.Uint32(o[20:])) != -1 || binary.LittleEndian.Uint32(o[24:]) != cephCapFileRD {
		t.Fatal("packed open args")
	}
}

package integration_test

import (
	"encoding/binary"
	"errors"
)

// Pinned include/ceph_fs.h filelock_change and ceph_filelock. Native
// Client::_do_filelock sets the owner high bit; _flock uses pid/start/length0.
// These builders are test-only consumer schema, not a filesystem API.
const (
	cephFlockShared      byte   = 1
	cephFlockExclusive   byte   = 2
	cephFlockUnlock      byte   = 4
	cephFlockGoOwner     uint64 = 0x80000000474f0001
	cephFlockNativeOwner uint64 = 0x8000000063657068
)

func cephfsFlockArgs(kind byte, owner uint64) (args [56]byte) {
	args[0], args[1] = 2, kind // CEPH_LOCK_FLOCK, requested lock type.
	binary.LittleEndian.PutUint64(args[2:10], owner)
	return args // wait=0, whole file, pid=0; unused union bytes stay zero.
}

type cephfsFilelock struct {
	Start, Length, Client, Owner, PID uint64
	Type                              byte
}

func cephfsDecodeFilelock(raw []byte) (cephfsFilelock, error) {
	if len(raw) != 41 {
		return cephfsFilelock{}, errors.New("ceph_filelock extra must be exactly 41 bytes")
	}
	return cephfsFilelock{Start: binary.LittleEndian.Uint64(raw[0:8]), Length: binary.LittleEndian.Uint64(raw[8:16]),
		Client: binary.LittleEndian.Uint64(raw[16:24]), Owner: binary.LittleEndian.Uint64(raw[24:32]),
		PID: binary.LittleEndian.Uint64(raw[32:40]), Type: raw[40]}, nil
}

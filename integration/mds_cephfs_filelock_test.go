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

// Lock rules of filelock_change (CEPH_LOCK_FCNTL and CEPH_LOCK_FLOCK).
const (
	cephLockRuleFcntl byte = 1
	cephLockRuleFlock byte = 2
)

// cephfsFilelockArgs is the packed filelock_change of one SETFILELOCK or
// GETFILELOCK: rule, type, then owner, pid, start and length as little-endian
// u64 and wait. wait stays 0 (nonblocking); unused union bytes stay zero.
func cephfsFilelockArgs(rule, kind byte, owner, pid, start, length uint64) (args [56]byte) {
	args[0], args[1] = rule, kind
	binary.LittleEndian.PutUint64(args[2:10], owner)
	binary.LittleEndian.PutUint64(args[10:18], pid)
	binary.LittleEndian.PutUint64(args[18:26], start)
	binary.LittleEndian.PutUint64(args[26:34], length)
	return args
}

// cephfsFlockArgs is a whole-file FLOCK request with pid 0.
func cephfsFlockArgs(kind byte, owner uint64) [56]byte {
	return cephfsFilelockArgs(cephLockRuleFlock, kind, owner, 0, 0, 0)
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

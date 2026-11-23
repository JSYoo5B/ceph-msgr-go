package main

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Held-lock recovery cells of the metadata fixture beyond the whole-file and
// fixed-range probes of mds_cephfs.go (issue #10): an FCNTL probe set over
// one owner's merged and split ranges, and a read of the current rank-0
// daemon's in-flight request of an owned Go session, which shows a blocked
// SETFILELOCK waiting on the MDS (Server::handle_client_file_setlock marks it
// "failed to add lock, waiting" and keeps it in flight). Native behavior
// relied upon: mds/Server.cc, mds/flock.cc and common/TrackedOp.cc at
// mdsFreshSource (LGPL-2.1 references; no code copied).

// capMDSHeldFcntlRanges admits one test alone on a fresh metadata fixture with
// the FCNTL probe of merged and split ranges (fcntl-probe-ranges).
var capMDSHeldFcntlRanges = defineExclusiveCapability(fixtureMDSCephFS, "mds-held-fcntl-ranges",
	"run one test alone on a fresh MDS CephFS metadata fixture with the merged and split FCNTL range probe",
	"MDS_FRESH=1 MDS_CEPHFS=1", nil)

// capMDSHeldFlockWait admits one test alone on a fresh metadata fixture with
// the in-flight read of a waiting lock request (lock-inflight).
var capMDSHeldFlockWait = defineExclusiveCapability(fixtureMDSCephFS, "mds-held-flock-wait",
	"run one test alone on a fresh MDS CephFS metadata fixture with the in-flight read of a waiting lock request",
	"MDS_FRESH=1 MDS_CEPHFS=1", nil)

// admitMDSCephFSHeldLocks refuses each held-lock cell action without its
// capability.
func admitMDSCephFSHeldLocks(action string, p profile) error {
	switch {
	case action == "fcntl-probe-ranges" && !p.admits(capMDSHeldFcntlRanges):
		return errors.New("MDS CephFS FCNTL range probe requires capability " + capMDSHeldFcntlRanges.name)
	case action == "lock-inflight" && !p.admits(capMDSHeldFlockWait):
		return errors.New("MDS CephFS in-flight lock read requires capability " + capMDSHeldFlockWait.name)
	}
	return nil
}

// mdsCephFSFcntlRangeProbes is the fixed probe set of fcntl-probe-ranges, for
// an exclusive range [0,32) merged from [0,16) and [16,32) of one owner and a
// shared range [64,128) of another owner split by an unlock of [80,96): bytes
// inside and at the end of the merged range, a shared request over it, the
// gap after it, the first split piece, the hole between the pieces, the
// second piece and a to-EOF request after it. Each probe overlaps at most one
// held range.
var mdsCephFSFcntlRangeProbes = []mdsCephFSFcntlProbe{{"w", 20, 1}, {"w", 31, 1}, {"r", 0, 32}, {"w", 32, 32}, {"w", 70, 1}, {"w", 85, 1}, {"w", 100, 1}, {"w", 128, 0}}

// mdsCephFSFcntlProbeSet returns the fixed probe set of an FCNTL probe action.
func mdsCephFSFcntlProbeSet(action string) []mdsCephFSFcntlProbe {
	switch action {
	case "fcntl-probe", "split-fcntl-probe":
		return mdsCephFSFcntlProbes
	case "fcntl-probe-ranges":
		return mdsCephFSFcntlRangeProbes
	}
	return nil
}

// mdsCephFSLockInFlight is one read of the current rank-0 daemon's op tracker
// for one request of an observed owned Go session: the tracked op, or none.
type mdsCephFSLockInFlight struct {
	GlobalID uint64               `json:"global_id"`
	TID      uint64               `json:"tid"`
	MDSGID   uint64               `json:"mds_gid"`
	InFlight bool                 `json:"in_flight"`
	Op       *mdsCephFSHistoricOp `json:"op,omitempty"`
}

// mdsCephFSLockInFlightRead reads dump_ops_in_flight of the current rank-0
// daemon (the action's proof binds it) and returns the one tracked client
// request gid:tid, if any. It changes nothing.
func (c *fixtureCluster) mdsCephFSLockInFlightRead(ctx context.Context, action mdsCephFSAction, result *mdsCephFSResult) error {
	x := c.mdsCephFS
	if !slices.Contains(slices.Collect(maps.Values(x.owned)), action.gid) {
		return errors.New("MDS CephFS in-flight lock read is admitted only for an observed owned Go session")
	}
	data, err := c.mdsSessionsDaemon(ctx, "dump_ops_in_flight", "--format", "json")
	if err != nil {
		return err
	}
	op, _, err := mdsCephFSFindBoundaryOp(data, action.gid, action.tid)
	if err != nil {
		return err
	}
	result.LockInFlight = &mdsCephFSLockInFlight{GlobalID: action.gid, TID: action.tid, MDSGID: x.current.GID, InFlight: op != nil, Op: op}
	return nil
}

// parseMDSCephFSGIDTID parses <GlobalID>:<TID> in canonical decimal.
func parseMDSCephFSGIDTID(arg string) (uint64, uint64, bool) {
	gidText, tidText, ok := strings.Cut(arg, ":")
	gid, gidErr := strconv.ParseUint(gidText, 10, 64)
	tid, tidErr := strconv.ParseUint(tidText, 10, 64)
	return gid, tid, ok && gidErr == nil && tidErr == nil && gid != 0 && tid != 0 && strconv.FormatUint(gid, 10) == gidText && strconv.FormatUint(tid, 10) == tidText
}

// mdsCephFSFcntlProbePython is the FCNTL probe driver shared by the metadata
// and completion native workers. It uses their globals identity, fsname,
// path, NATIVE_KEY and flock_state. Source: include/cephfs/libcephfs.h and
// client/Client.cc (_do_filelock) at mdsFreshSource; independent driver, no
// code copied.
const mdsCephFSFcntlProbePython = `
def fcntl_probe(extra):
    # One FCNTL probe run: for each closed request an F_GETLK, then a
    # nonblocking F_SETLK that is released again at once when it acquired.
    # The binding has no record-lock call: a second mount through ctypes
    # uses libcephfs ceph_getlk/ceph_setlk (include/cephfs/libcephfs.h).
    import ctypes
    lib = ctypes.CDLL('libcephfs.so.2')
    class FL(ctypes.Structure):
        _fields_ = [('l_type', ctypes.c_short), ('l_whence', ctypes.c_short), ('l_start', ctypes.c_int64),
                    ('l_len', ctypes.c_int64), ('l_pid', ctypes.c_int32)]
    vp = ctypes.c_void_p
    lib.ceph_create.argtypes = [ctypes.POINTER(vp), ctypes.c_char_p]
    lib.ceph_conf_read_file.argtypes = [vp, ctypes.c_char_p]
    lib.ceph_conf_set.argtypes = [vp, ctypes.c_char_p, ctypes.c_char_p]
    lib.ceph_select_filesystem.argtypes = [vp, ctypes.c_char_p]
    lib.ceph_mount.argtypes = [vp, ctypes.c_char_p]
    lib.ceph_get_instance_id.argtypes = [vp]
    lib.ceph_get_instance_id.restype = ctypes.c_uint64
    lib.ceph_open.argtypes = [vp, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint]
    lib.ceph_getlk.argtypes = [vp, ctypes.c_int, ctypes.POINTER(FL), ctypes.c_uint64]
    lib.ceph_setlk.argtypes = [vp, ctypes.c_int, ctypes.POINTER(FL), ctypes.c_uint64, ctypes.c_int]
    lib.ceph_close.argtypes = [vp, ctypes.c_int]
    lib.ceph_shutdown.argtypes = [vp]
    lib.ceph_shutdown.restype = None
    kinds = {'w': fcntl.F_WRLCK, 'r': fcntl.F_RDLCK}
    names = {fcntl.F_WRLCK: 'w', fcntl.F_RDLCK: 'r', fcntl.F_UNLCK: 'u'}
    requests = []
    for item in extra:
        kind, start, length = item.split(':')
        if kind not in kinds or not (0 <= int(start) < 4096 and 0 <= int(length) < 4096):
            raise ValueError('fcntl probe request outside its bound')
        requests.append((kind, int(start), int(length)))
    # libcephfs sets the owner's high bit itself (Client::_do_filelock).
    owner = 0x63657068
    def check(r, call):
        if r < 0:
            raise OSError(-r, call + ' failed')
    probe = {'owner': owner | (1 << 63), 'probes': [], 'before': flock_state()}
    cm = vp()
    check(lib.ceph_create(ctypes.byref(cm), identity.removeprefix('client.').encode()), 'ceph_create')
    started = time.monotonic_ns()
    try:
        check(lib.ceph_conf_read_file(cm, b'/tmp/ceph-msgr-test/ceph.conf'), 'ceph_conf_read_file')
        check(lib.ceph_conf_set(cm, b'key', NATIVE_KEY.encode()), 'ceph_conf_set')
        check(lib.ceph_select_filesystem(cm, fsname.encode()), 'ceph_select_filesystem')
        check(lib.ceph_mount(cm, b'/'), 'ceph_mount')
        probe['global_id'] = lib.ceph_get_instance_id(cm)
        fd = lib.ceph_open(cm, path.encode(), os.O_RDWR, 0)
        check(fd, 'ceph_open')
        try:
            for kind, start, length in requests:
                item = {'type': kind, 'start': start, 'len': length, 'errno': 0, 'acquired': False, 'unlocked': False}
                fl = FL(kinds[kind], os.SEEK_SET, start, length, 0)
                check(lib.ceph_getlk(cm, fd, ctypes.byref(fl), owner), 'ceph_getlk')
                if fl.l_type not in names:
                    raise ValueError('ceph_getlk returned an unknown lock type')
                item['getlk'] = {'type': names[fl.l_type], 'start': fl.l_start, 'len': fl.l_len, 'pid': fl.l_pid}
                fl = FL(kinds[kind], os.SEEK_SET, start, length, 0)
                r = lib.ceph_setlk(cm, fd, ctypes.byref(fl), owner, 0)
                if r == -11:
                    item['errno'] = 11
                else:
                    check(r, 'ceph_setlk')
                    item['acquired'] = True
                    fl = FL(fcntl.F_UNLCK, os.SEEK_SET, start, length, 0)
                    check(lib.ceph_setlk(cm, fd, ctypes.byref(fl), owner, 0), 'ceph_setlk unlock')
                    item['unlocked'] = True
                probe['probes'].append(item)
        finally:
            lib.ceph_close(cm, fd)
    finally:
        lib.ceph_shutdown(cm)
    probe['elapsed_ns'] = time.monotonic_ns() - started
    probe['after'] = flock_state()
    return probe
`

// saveMetadata (diagnostics.go) collects the public receipts the held-lock
// cells write into the control directory when they exist.
var _ = collectDiagnosticFiles(
	"mds-cephfs-dirtyflock-recovery-results.json",
	"mds-cephfs-fcntlranges-recovery-results.json",
	"mds-cephfs-heldfcntl-split-results.json",
	"mds-cephfs-heldflock-split-cut-results.json",
	"mds-cephfs-heldflock-split-dense-results.json",
	"mds-cephfs-intrflock-recovery-results.json",
	"mds-cephfs-noblob-recovery-results.json",
	"mds-cephfs-ranks-heldflock-results.json",
	"mds-cephfs-twoclient-recovery-results.json",
	"mds-cephfs-waitflock-recovery-results.json",
)

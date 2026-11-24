package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS consumer-request acceptance runs alone on a fresh completion
// fixture. It gives the Go test an independent native answer for two
// requests whose feature bits the library withholds and whose content is the
// consumer's: GETVXATTR (op_getvxattr, bit 17) and FILE_BLOCKDIFF (blockdiff,
// bit 23). At the pinned tag the MDS reads neither bit: the stock client
// refuses each request locally unless the MDS advertises it
// (Client::make_request feature_needed). It adds three closed native actions
// on one child of the owned directory, the blockdiff directory, and the MDS
// "p" flag (MAY_SET_VXATTR, which ceph.dir.layout needs) to the native
// identity of that fixture only. consumer-prepare sets a 64 KiB object layout
// on the empty directory, writes four objects of one file, takes the first
// snapshot, overwrites part of the second object, appends a fifth, and takes
// the second snapshot (one-shot, journaled). consumer-read reads the plan's
// vxattrs with getxattr, which the stock client answers through the MDS
// (GETVXATTR) for names outside its own vxattr table, and runs
// ceph_file_blockdiff between the two snapshots through libcephfs with the
// plan's page size, keeping each page as returned. consumer-remove removes
// both snapshots and the file (one-shot, journaled). Native behavior relied
// upon: mds/Server.cc (handle_client_getvxattr, handle_client_file_blockdiff,
// handle_file_blockdiff_finish, handle_set_vxattr ceph.dir.layout),
// mds/MDCache.cc (file_blockdiff, aggregate_snap_sets), mds/mdstypes.cc
// (BlockDiff::encode), client/Client.cc (_getxattr, _getvxattr,
// file_blockdiff), libcephfs.cc (ceph_file_blockdiff_init,
// ceph_file_blockdiff) and pybind/cephfs/cephfs.pyx at mdsFreshSource
// (LGPL-2.1 references; no code copied).

// capMDSConsumerRequests admits one test alone on a fresh completion fixture
// with the consumer-request controls.
var capMDSConsumerRequests = defineExclusiveCapability(fixtureMDSCephFSCompletion, "mds-consumer-requests",
	"run one test alone on a fresh MDS CephFS completion fixture with the GETVXATTR and FILE_BLOCKDIFF native oracle",
	"MDS_FRESH=1 MDS_CEPHFS_COMPLETION=1", nil)

var mdsCephFSConsumerActions = []string{"consumer-prepare", "consumer-read", "consumer-remove"}

// admitMDSCephFSConsumer refuses a consumer-request action without its
// capability.
func admitMDSCephFSConsumer(action string, p profile) error {
	if slices.Contains(mdsCephFSConsumerActions, action) && !p.admits(capMDSConsumerRequests) {
		return errors.New("MDS CephFS completion consumer-request action requires capability " + capMDSConsumerRequests.name)
	}
	return nil
}

// mdsCephFSConsumerExtent is one file byte range.
type mdsCephFSConsumerExtent struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// mdsCephFSConsumerVxattr names one vxattr read on the directory or the file.
type mdsCephFSConsumerVxattr struct {
	Target string `json:"target"` // dir or file
	Name   string `json:"name"`
}

// mdsCephFSConsumerPlan is the fixed namespace and data of one run. Both
// snapshots are of Dir; the file is written with ObjectSize objects: Objects
// of them before the first snapshot, then Overwrite and Append before the
// second. MaxObjects is the page size of both the native and the Go diff.
type mdsCephFSConsumerPlan struct {
	Dir        string                    `json:"dir"`
	File       string                    `json:"file"`
	Snapshots  []string                  `json:"snapshots"`
	ObjectSize uint64                    `json:"object_size"`
	Objects    uint64                    `json:"objects"`
	Overwrite  mdsCephFSConsumerExtent   `json:"overwrite"`
	Append     mdsCephFSConsumerExtent   `json:"append"`
	MaxObjects uint64                    `json:"max_objects"`
	Vxattrs    []mdsCephFSConsumerVxattr `json:"vxattrs"`
}

// mdsCephFSConsumerPlanFor is fixed. The overwrite lies inside the second
// object and the append adds a fifth, so a two-object page size needs three
// pages: the overwritten range, an empty page and the appended tail. The
// vxattrs are ones the stock client sends to the MDS, plus two the MDS
// answers with ENODATA: a charmap on a directory without one, and a
// directory vxattr on a file.
func mdsCephFSConsumerPlanFor(token string) mdsCephFSConsumerPlan {
	const object = 64 << 10
	return mdsCephFSConsumerPlan{Dir: "bdiff", File: "file", Snapshots: []string{"bdiff-a-" + token[:8], "bdiff-b-" + token[:8]}, ObjectSize: object, Objects: 4,
		Overwrite: mdsCephFSConsumerExtent{Offset: object + 8<<10, Length: 4 << 10}, Append: mdsCephFSConsumerExtent{Offset: 4 * object, Length: object}, MaxObjects: 2,
		Vxattrs: []mdsCephFSConsumerVxattr{
			{"dir", "ceph.dir.layout.json"}, {"dir", "ceph.dir.layout.pool_name"}, {"dir", "ceph.dir.layout.pool_id"}, {"dir", "ceph.dir.subvolume"},
			{"dir", "ceph.quiesce.block"}, {"dir", "ceph.dir.charmap"}, {"file", "ceph.file.layout.json"}, {"file", "ceph.file.layout.pool_name"},
			{"file", "ceph.dir.layout.json"},
		}}
}

// valid reports whether the plan names only owned paths and bounded values.
func (p mdsCephFSConsumerPlan) valid() bool {
	name := func(n string) bool { return n != "" && len(n) <= 64 && !strings.ContainsAny(n, "/. \x00") }
	ok := p.Dir == "bdiff" && p.File == "file" && len(p.Snapshots) == 2 && name(p.Snapshots[0]) && name(p.Snapshots[1]) && p.Snapshots[0] != p.Snapshots[1] &&
		p.ObjectSize == 64<<10 && p.Objects > 0 && p.Objects <= 8 && p.MaxObjects > 0 && p.MaxObjects <= 16 &&
		p.Overwrite.Length > 0 && p.Overwrite.Offset+p.Overwrite.Length <= p.Objects*p.ObjectSize && p.Append.Offset == p.Objects*p.ObjectSize && p.Append.Length > 0 &&
		p.Append.Length <= p.ObjectSize && len(p.Vxattrs) <= 16
	for _, v := range p.Vxattrs {
		ok = ok && (v.Target == "dir" || v.Target == "file") && strings.HasPrefix(v.Name, "ceph.") && len(v.Name) <= 64 && !strings.ContainsAny(v.Name, "/ \x00")
	}
	return ok
}

// consumerPath is the blockdiff directory, a direct child of the owned
// directory.
func (p mdsCephFSCompletionPlan) consumerPath() (string, error) {
	root, err := p.path("dir")
	if err != nil || !p.Consumer.valid() {
		return "", errors.Join(err, errors.New("MDS CephFS consumer-request directory is not owned"))
	}
	return root + "/" + p.Consumer.Dir, nil
}

// mdsCephFSConsumerValue is one native getxattr result: the value, or the
// errno (61, ENODATA) the stock client returned.
type mdsCephFSConsumerValue struct {
	Target   string `json:"target"`
	Name     string `json:"name"`
	Errno    int    `json:"errno"`
	ValueHex string `json:"value_hex"`
}

// mdsCephFSConsumerPage is one ceph_file_blockdiff call: its return value (1
// while more objects remain) and the extents it reported.
type mdsCephFSConsumerPage struct {
	Return int                      `json:"return"`
	Blocks []mdsCephFSConsumerBlock `json:"blocks"`
}

type mdsCephFSConsumerBlock [2]uint64

// mdsCephFSConsumerNative is the output of one consumer-request action.
type mdsCephFSConsumerNative struct {
	Action         string                   `json:"action"`
	Path           string                   `json:"path"`
	NativeGlobalID uint64                   `json:"native_global_id"`
	DiffGlobalID   uint64                   `json:"diff_global_id,omitempty"`
	Stat           *mdsCephFSStat           `json:"stat,omitempty"`
	FileStat       *mdsCephFSStat           `json:"file_stat,omitempty"`
	Snapshots      []mdsCephFSStat          `json:"snapshot_file_stats,omitempty"`
	Written        []mdsCephFSConsumerBlock `json:"written,omitempty"`
	Vxattrs        []mdsCephFSConsumerValue `json:"vxattrs,omitempty"`
	Pages          []mdsCephFSConsumerPage  `json:"pages,omitempty"`
}

// The consumer-request driver. It admits only the blockdiff child of the
// owned directory, its one file and the plan's two snapshots of it; every
// value comes from the plan argument, which the Go side checked. The diff runs
// on a second mount made through libcephfs itself, because the Python
// binding has no blockdiff call.
// Source: pybind/cephfs/cephfs.pyx and include/cephfs/libcephfs.h at
// mdsFreshSource; independent driver.
const mdsCephFSConsumerNativePython = `
import cephfs, ctypes, hashlib, json, os, sys
identity, fsname, directory, action, spec = sys.argv[1:6]
p = json.loads(spec)
def name_ok(n):
    return isinstance(n, str) and 0 < len(n) <= 64 and not any(c in n for c in '/. \x00')
if not directory.startswith('ceph-msgr-completion-') or any(c in directory for c in '/. ') or p['dir'] != 'bdiff' or p['file'] != 'file' or len(p['snapshots']) != 2 or not all(name_ok(s) for s in p['snapshots']):
    raise ValueError('path outside the owned directory')
base = '/' + directory + '/' + p['dir']
path = base + '/' + p['file']
snaps = [base + '/.snap/' + s for s in p['snapshots']]
MASK = 0x30f
def payload(label, n):
    out, block = b'', hashlib.sha256(('ceph-msgr-blockdiff:' + directory + ':' + label).encode()).digest()
    while len(out) < n:
        out += block
        block = hashlib.sha256(block).digest()
    return out[:n]
fs = cephfs.LibCephFS(conffile='/tmp/ceph-msgr-test/ceph.conf', auth_id=identity.removeprefix('client.'), conf={'key': NATIVE_KEY})
def stat(q):
    try:
        s = fs.statx(q, MASK, 0)
    except cephfs.ObjectNotFound as e:
        return {'exists': False, 'errno': abs(e.errno), 'ino': 0, 'mode': 0, 'uid': 0, 'gid': 0, 'nlink': 0, 'size': 0}
    return {'exists': True, 'errno': 0, 'ino': s['ino'], 'mode': s['mode'], 'uid': s['uid'], 'gid': s['gid'], 'nlink': s['nlink'], 'size': s['size']}
def write(fd, label, offset, n):
    data = payload(label, n)
    if fs.write(fd, data, offset) != n:
        raise ValueError('short native write')
    return [offset, n]
def diff():
    lib = ctypes.CDLL('libcephfs.so.2')
    class Info(ctypes.Structure):
        _fields_ = [('cmount', ctypes.c_void_p), ('blockp', ctypes.c_void_p)]
    class Block(ctypes.Structure):
        _fields_ = [('offset', ctypes.c_uint64), ('len', ctypes.c_uint64)]
    class Changed(ctypes.Structure):
        _fields_ = [('num_blocks', ctypes.c_uint64), ('b', ctypes.POINTER(Block))]
    lib.ceph_create.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p]
    for f in ('ceph_conf_read_file', 'ceph_select_filesystem', 'ceph_mount'):
        getattr(lib, f).argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.ceph_conf_set.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
    lib.ceph_get_instance_id.argtypes, lib.ceph_get_instance_id.restype = [ctypes.c_void_p], ctypes.c_uint64
    lib.ceph_unmount.argtypes = lib.ceph_release.argtypes = [ctypes.c_void_p]
    lib.ceph_file_blockdiff_init.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(Info)]
    lib.ceph_file_blockdiff.argtypes = [ctypes.POINTER(Info), ctypes.POINTER(Changed)]
    lib.ceph_free_file_blockdiff_buffer.argtypes = [ctypes.POINTER(Changed)]
    lib.ceph_free_file_blockdiff_buffer.restype = None
    lib.ceph_file_blockdiff_finish.argtypes = [ctypes.POINTER(Info)]
    def check(r, what):
        if r < 0:
            raise OSError(-r, 'libcephfs ' + what)
    cm = ctypes.c_void_p()
    check(lib.ceph_create(ctypes.byref(cm), identity.removeprefix('client.').encode()), 'create')
    pages, mounted = [], False
    try:
        check(lib.ceph_conf_read_file(cm, b'/tmp/ceph-msgr-test/ceph.conf'), 'conf')
        check(lib.ceph_conf_set(cm, b'key', NATIVE_KEY.encode()), 'key')
        check(lib.ceph_conf_set(cm, b'client_file_blockdiff_max_concurrent_object_scans', str(int(p['max_objects'])).encode()), 'page size')
        check(lib.ceph_select_filesystem(cm, fsname.encode()), 'filesystem')
        check(lib.ceph_mount(cm, b'/'), 'mount')
        mounted = True
        out['diff_global_id'] = lib.ceph_get_instance_id(cm)
        info = Info()
        check(lib.ceph_file_blockdiff_init(cm, base.encode(), p['file'].encode(), p['snapshots'][0].encode(), p['snapshots'][1].encode(), ctypes.byref(info)), 'blockdiff init')
        try:
            for _ in range(64):
                changed = Changed()
                r = lib.ceph_file_blockdiff(ctypes.byref(info), ctypes.byref(changed))
                check(r, 'blockdiff')
                blocks = [[changed.b[i].offset, changed.b[i].len] for i in range(changed.num_blocks)]
                lib.ceph_free_file_blockdiff_buffer(ctypes.byref(changed))
                pages.append({'return': r, 'blocks': blocks})
                if r == 0:
                    break
        finally:
            check(lib.ceph_file_blockdiff_finish(ctypes.byref(info)), 'blockdiff finish')
    finally:
        if mounted:
            lib.ceph_unmount(cm)
        lib.ceph_release(cm)
    return pages
out = {'action': action, 'path': base}
try:
    fs.mount(filesystem_name=fsname)
    out['native_global_id'] = fs.get_instance_id()
    osz = int(p['object_size'])
    if action == 'prepare':
        fs.setxattr(base, 'ceph.dir.layout', ('stripe_unit=%d stripe_count=1 object_size=%d' % (osz, osz)).encode(), 0)
        fd = fs.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            out['written'] = [write(fd, 'initial', 0, osz * int(p['objects']))]
            fs.fsync(fd, 0)
        finally:
            fs.close(fd)
        fs.mkdir(snaps[0], 0o755)
        fd = fs.open(path, os.O_WRONLY, 0)
        try:
            out['written'].append(write(fd, 'overwrite', int(p['overwrite']['offset']), int(p['overwrite']['length'])))
            out['written'].append(write(fd, 'append', int(p['append']['offset']), int(p['append']['length'])))
            fs.fsync(fd, 0)
        finally:
            fs.close(fd)
        fs.mkdir(snaps[1], 0o755)
        out['stat'], out['file_stat'] = stat(base), stat(path)
        out['snapshot_file_stats'] = [stat(s + '/' + p['file']) for s in snaps]
    elif action == 'read':
        out['vxattrs'] = []
        for v in p['vxattrs']:
            target = base if v['target'] == 'dir' else path
            try:
                value, errno = fs.getxattr(target, v['name'], 4096), 0
            except cephfs.NoData:
                value, errno = b'', 61
            out['vxattrs'].append({'target': v['target'], 'name': v['name'], 'errno': errno, 'value_hex': value.hex()})
        out['pages'] = diff()
        out['stat'], out['file_stat'] = stat(base), stat(path)
        out['snapshot_file_stats'] = [stat(s + '/' + p['file']) for s in snaps]
    elif action == 'remove':
        for s in reversed(snaps):
            fs.rmdir(s)
        fs.unlink(path)
        out['stat'], out['file_stat'] = stat(base), stat(path)
    else:
        raise ValueError('native CephFS consumer-request action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

// decodeMDSCephFSConsumerNative accepts exactly the shape of one action. The
// values are evidence for the test to compare; here each is present, bounded
// and of the right kind.
func decodeMDSCephFSConsumerNative(action, path string, plan mdsCephFSConsumerPlan, data []byte) (mdsCephFSConsumerNative, error) {
	var result mdsCephFSConsumerNative
	if len(data) == 0 || len(data) > mdsCephFSJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS consumer-request output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSConsumerNative{}, errors.New("native CephFS consumer-request output identity differs")
	}
	dir := result.Stat != nil && result.Stat.Exists && validMDSCephFSStat(*result.Stat) && result.Stat.Mode&0o170000 == 0o040000
	file := func(s *mdsCephFSStat) bool {
		return s != nil && s.Exists && validMDSCephFSStat(*s) && s.Mode&0o170000 == 0o100000
	}
	snapshots := len(result.Snapshots) == 2 && file(&result.Snapshots[0]) && file(&result.Snapshots[1])
	ok := false
	switch action {
	case "prepare":
		ok = dir && file(result.FileStat) && snapshots && len(result.Written) == 3 && result.Vxattrs == nil && result.Pages == nil && result.DiffGlobalID == 0
	case "read":
		ok = dir && file(result.FileStat) && snapshots && result.Written == nil && len(result.Vxattrs) == len(plan.Vxattrs) && len(result.Pages) > 0 &&
			len(result.Pages) <= 64 && result.DiffGlobalID != 0 && result.DiffGlobalID != result.NativeGlobalID
		for i, v := range result.Vxattrs {
			value, err := hex.DecodeString(v.ValueHex)
			ok = ok && v.Target == plan.Vxattrs[i].Target && v.Name == plan.Vxattrs[i].Name && err == nil && len(value) <= 4096 &&
				(v.Errno == 0 || v.Errno == 61 && v.ValueHex == "")
		}
		for i, page := range result.Pages {
			last := i == len(result.Pages)-1
			ok = ok && (page.Return == 1 && !last || page.Return == 0 && last) && len(page.Blocks) <= 64
		}
	case "remove":
		ok = dir && result.FileStat != nil && !result.FileStat.Exists && validMDSCephFSStat(*result.FileStat) && result.Snapshots == nil && result.Written == nil &&
			result.Vxattrs == nil && result.Pages == nil && result.DiffGlobalID == 0
	}
	if !ok {
		return mdsCephFSConsumerNative{}, fmt.Errorf("native CephFS consumer-request %s output shape differs", action)
	}
	return result, nil
}

// mdsCephFSConsumerAct runs one consumer-request action. consumer-prepare
// and consumer-remove are one-shot and journaled before the process starts
// (remove requires an applied prepare); an uncertain one is never replayed.
// A run that stops before consumer-remove leaves the snapshots and the file,
// which are discarded with the owned filesystem.
func (c *fixtureCluster) mdsCephFSConsumerAct(ctx context.Context, id int64, action mdsCephFSCompletionAction, result *mdsCephFSCompletionResult) error {
	s, x := c.mdsFresh, c.mdsCephFSCompletion
	if err := admitMDSCephFSConsumer(action.name, c.profile); err != nil {
		return err
	}
	if err := x.quiesced(x.current.GID); err != nil {
		return err
	}
	path, err := x.plan.consumerPath()
	if err != nil {
		return err
	}
	spec, err := json.Marshal(x.plan.Consumer)
	if err != nil {
		return err
	}
	driver, _ := strings.CutPrefix(action.name, "consumer-")
	var mutation *mdsCephFSMutation
	if action.name != "consumer-read" {
		kind := "native-" + action.name
		if x.mutated(kind) {
			return fmt.Errorf("MDS CephFS completion %s was already issued", kind)
		}
		if action.name == "consumer-remove" && !slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == "native-consumer-prepare" && m.State == "applied" }) {
			return errors.New("MDS CephFS consumer-request removal requires an applied preparation")
		}
		mutation = &mdsCephFSMutation{Kind: kind, RequestID: id, Target: x.plan.Consumer.Dir, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSCompletionJournal(); err != nil {
			return err
		}
	}
	argv := []string{"python3", "-c", mdsCephFSConsumerNativePython, "client.mds-cephfs-completion-native-" + s.token, s.name, x.plan.Directory, driver, string(spec)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 60 * time.Second, MaxStdout: mdsCephFSJSONLimit, MaxStderr: 16 << 10,
		Label: "owned native CephFS consumer request " + driver}, x.native)
	result.Process = &process
	var native mdsCephFSConsumerNative
	if err == nil && (!process.Complete() || process.ExitCode != 0) {
		err = fmt.Errorf("native CephFS consumer-request %s did not complete (exit %d, timed out %v)", driver, process.ExitCode, process.TimedOut)
	}
	if err == nil {
		native, err = decodeMDSCephFSConsumerNative(driver, path, x.plan.Consumer, bytes.TrimSpace(process.Stdout))
	}
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSCompletionJournal())
		}
		return err
	}
	result.Consumer = &native
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", native.NativeGlobalID
		return c.mdsCephFSCompletionJournal()
	}
	return nil
}

// saveMetadata (diagnostics.go) collects the public receipt the CephFS
// consumer-request test writes into the control directory.
var _ = collectDiagnosticFiles(
	"mds-cephfs-consumer-requests-results.json",
)

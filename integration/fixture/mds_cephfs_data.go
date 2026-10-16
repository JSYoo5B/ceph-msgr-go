package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The CephFS data acceptance drives file data I/O of an owned filesystem
// through the Go client's MDS session and its own OSD writes and reads, and
// compares every result with a native LibCephFS client on the same fixture.
// It owns one directory with a small striped directory layout, five files,
// one snapshot and two identities, both with OSD write grants on the owned
// data pool. It changes no daemon setting and restarts nothing. Native
// behavior relied upon: client/Client.cc and libcephfs (striped reads and
// writes, truncate, getxattr/setxattr/removexattr of the layout virtual
// xattrs), mds/Server.cc (handle_client_setvxattr ceph.dir.layout set and
// removal, handle_client_mksnap/rmsnap), osdc/Striper.cc (object naming and
// striping) and tools/rados (listsnaps) at mdsFreshSource (LGPL-2.1
// references; no code copied).

const (
	mdsCephFSDataJSONLimit   = 256 << 10
	mdsCephFSDataStripeUnit  = 65536
	mdsCephFSDataStripeCount = 2
	mdsCephFSDataObjectSize  = 131072
	mdsCephFSDataReadLimit   = 4 << 20
	mdsCephFSDataChunk       = 16384
	mdsCephFSDataMaxObjects  = 16
)

// mdsCephFSDataFiles are the closed file names directly below the owned
// directory.
var mdsCephFSDataFiles = []string{"go-file", "native-file", "trunc-file", "shared-file", "snap-file"}

// mdsCephFSDataTargets are every closed stat target: the directory, the files,
// the snapshot directory and the snapshot's view of snap-file.
var mdsCephFSDataTargets = append(append([]string{"dir"}, mdsCephFSDataFiles...), "snapshot", "snapshot-file")

// mdsCephFSDataPlan is the fixed data namespace of one run. Payload bytes are
// SHA-256 chains of the seeds (the block of the seed, then the block of the
// previous block), computed identically by the native driver and the test.
type mdsCephFSDataPlan struct {
	Directory      string            `json:"directory"`
	Snapshot       string            `json:"snapshot"`
	Files          []string          `json:"files"`
	StripeUnit     uint64            `json:"stripe_unit"`
	StripeCount    uint64            `json:"stripe_count"`
	ObjectSize     uint64            `json:"object_size"`
	Seeds          map[string]string `json:"seeds"`
	GoFileSize     uint64            `json:"go_file_size"`
	NativeExtents  [][2]uint64       `json:"native_extents"`
	NativeSize     uint64            `json:"native_size"`
	TruncWriteSize uint64            `json:"trunc_write_size"`
	TruncSize      uint64            `json:"trunc_size"`
	SharedSize     uint64            `json:"shared_size"`
	SnapV1Size     uint64            `json:"snap_v1_size"`
	SnapV2Offset   uint64            `json:"snap_v2_offset"`
	SnapV2Size     uint64            `json:"snap_v2_size"`
	ObjectCounts   map[string]int    `json:"object_counts"`
}

func mdsCephFSDataPlanFor(token string) mdsCephFSDataPlan {
	seeds := map[string]string{}
	for _, purpose := range []string{"go-file", "native-file", "trunc-file", "shared-file", "snap-file-v1", "snap-file-v2"} {
		seeds[purpose] = "ceph-msgr-cephfs-data-" + purpose + ":" + token
	}
	return mdsCephFSDataPlan{Directory: "ceph-msgr-data-" + token, Snapshot: "data-" + token[:8], Files: slices.Clone(mdsCephFSDataFiles),
		StripeUnit: mdsCephFSDataStripeUnit, StripeCount: mdsCephFSDataStripeCount, ObjectSize: mdsCephFSDataObjectSize, Seeds: seeds,
		// The minimum stripe unit is 64 KiB (file_layout_t::is_valid); a
		// period is 256 KiB over two 128 KiB objects. go-file is 2.34
		// periods: objects 0-5, the last stripe partial.
		GoFileSize: 614523,
		// Three extents with holes: in object set 0 (objects 0 and 1), set 2
		// (objects 4 and 5) and set 6 (object 12); sets 1 and 3-5 and the
		// other halves stay holes.
		NativeExtents: [][2]uint64{{0, 80000}, {600000, 120000}, {1600000, 20000}}, NativeSize: 1620000,
		TruncWriteSize: 1228800, TruncSize: 280000, SharedSize: 200000,
		SnapV1Size: 600000, SnapV2Offset: 400000, SnapV2Size: 320000,
		ObjectCounts: map[string]int{"go-file": 6, "native-file": 14, "trunc-file": 10, "shared-file": 2, "snap-file": 6}}
}

// layout is the native ceph.dir.layout value the plan sets on the directory.
func (p mdsCephFSDataPlan) layout(pool string) string {
	return fmt.Sprintf("stripe_unit=%d stripe_count=%d object_size=%d pool=%s", p.StripeUnit, p.StripeCount, p.ObjectSize, pool)
}

// path maps a closed target to its absolute path in the owned filesystem.
func (p mdsCephFSDataPlan) path(target string) (string, error) {
	if !strings.HasPrefix(p.Directory, "ceph-msgr-data-") || strings.ContainsAny(p.Directory, "/. \x00") || p.Snapshot == "" || strings.ContainsAny(p.Snapshot, "/. \x00") {
		return "", errors.New("MDS CephFS data directory is not owned")
	}
	root := "/" + p.Directory
	switch {
	case target == "dir":
		return root, nil
	case target == "snapshot":
		return root + "/.snap/" + p.Snapshot, nil
	case target == "snapshot-file":
		return root + "/.snap/" + p.Snapshot + "/snap-file", nil
	case slices.Contains(p.Files, target):
		return root + "/" + target, nil
	}
	return "", errors.New("MDS CephFS data target is not admitted")
}

func mdsCephFSDataGoCaps(fsName string) ceph.ClientCaps {
	// CephFS sessions and explicit OSD reads and writes of the owned data
	// pool; no layout or snapshot permission and no MGR.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rw fsname=" + fsName, OSD: "allow rw tag cephfs data=" + fsName}
}

func mdsCephFSDataNativeCaps(fsName string) ceph.ClientCaps {
	// The native oracle sets and removes the directory layout (`p`), creates
	// and removes one snapshot (`s`) and reads and writes file data.
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow rwps fsname=" + fsName, OSD: "allow rw tag cephfs data=" + fsName}
}

// One fixed native LibCephFS driver. It admits only the owned directory, its
// five files, the planned snapshot and the snapshot's view of snap-file.
// Payloads are regenerated from the plan seeds; nothing else reaches it.
// Object views use the python rados binding and, for clones, the rados CLI's
// listsnaps on the same identity. Sources: pybind/cephfs/cephfs.pyx,
// pybind/rados/rados.pyx and tools/rados/rados.cc at mdsFreshSource;
// independent driver, no code copied.
const mdsCephFSDataNativePython = `
import cephfs, hashlib, json, os, subprocess, sys, time
identity, keyring, fsname, directory, snapshot, pool, action, path = sys.argv[1:9]
extra = sys.argv[9:]
CONF = '/tmp/ceph-msgr-test/ceph.conf'
FILES = ('go-file', 'native-file', 'trunc-file', 'shared-file', 'snap-file')
CHUNK = 16384
LIMIT = 4 << 20
MASK = 0x13cf
if not directory.startswith('ceph-msgr-data-') or any(c in directory for c in '/. ') or not snapshot or any(c in snapshot for c in '/. '):
    raise ValueError('directory or snapshot is not owned')
root = '/' + directory
files = [root + '/' + f for f in FILES]
snapdir = root + '/.snap/' + snapshot
if path != root and path not in files and path != snapdir and path != snapdir + '/snap-file':
    raise ValueError('path outside the owned directory')
start = time.monotonic_ns()
def stream(seed, n):
    out = bytearray()
    block = hashlib.sha256(seed.encode()).digest()
    while len(out) < n:
        out += block
        block = hashlib.sha256(block).digest()
    return bytes(out[:n])
def stat(p):
    try:
        s = fs.statx(p, MASK, 0)
    except cephfs.ObjectNotFound as e:
        return {'exists': False, 'errno': abs(e.errno), 'ino': 0, 'mode': 0, 'uid': 0, 'gid': 0, 'nlink': 0, 'size': 0, 'mtime': 0, 'version': 0}
    return {'exists': True, 'errno': 0, 'ino': s['ino'], 'mode': s['mode'], 'uid': s['uid'], 'gid': s['gid'], 'nlink': s['nlink'], 'size': s['size'],
            'mtime': int(s['mtime'].timestamp()), 'version': s['version']}
def layout(p, name):
    try:
        return fs.getxattr(p, name).decode('utf-8'), False
    except cephfs.NoData:
        return '', True
def digest(data):
    return {'length': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
            'chunks': [hashlib.sha256(data[i:i + CHUNK]).hexdigest() for i in range(0, len(data), CHUNK)]}
def read_all(p, timings):
    fd = fs.open(p, os.O_RDONLY, 0)
    timings['opened'] = time.monotonic_ns() - start
    data = bytearray()
    try:
        while True:
            chunk = fs.read(fd, len(data), 1 << 16)
            if not chunk:
                break
            data += chunk
            if len(data) > LIMIT:
                raise ValueError('native read exceeds its bound')
        timings['read'] = time.monotonic_ns() - start
    finally:
        fs.close(fd)
    timings['closed'] = time.monotonic_ns() - start
    return bytes(data)
def write_at(p, pieces):
    fd = fs.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    written = 0
    try:
        for off, data in pieces:
            n = fs.write(fd, data, off)
            if n != len(data):
                raise IOError('short native write')
            written += n
        fs.fsync(fd, 0)
    finally:
        fs.close(fd)
    return written
def objects(ino, count, snaps):
    import rados
    if count < 1 or count > 16:
        raise ValueError('object count outside its bound')
    rid = identity.removeprefix('client.')
    cluster = rados.Rados(conffile=CONF, rados_id=rid, conf={'keyring': keyring})
    cluster.connect()
    result = []
    try:
        ioctx = cluster.open_ioctx(pool)
        try:
            for objno in range(count):
                name = '%x.%08x' % (ino, objno)
                entry = {'objno': objno, 'name': name, 'exists': False, 'size': 0}
                try:
                    size, _ = ioctx.stat(name)
                    entry['exists'], entry['size'] = True, size
                except rados.ObjectNotFound:
                    pass
                if snaps and entry['exists']:
                    proc = subprocess.run(['rados', '-c', CONF, '--id', rid, '--keyring', keyring, '-p', pool, '--format', 'json', 'listsnaps', name],
                                          capture_output=True, timeout=30)
                    if proc.returncode != 0:
                        raise IOError('rados listsnaps failed: ' + proc.stderr.decode('utf-8', 'replace')[-200:])
                    raw = json.loads(proc.stdout)
                    entry['snaps'] = {'seq': raw['seq'], 'clones': [{'head': c['id'] == 'head', 'id': 0 if c['id'] == 'head' else c['id'],
                        'snapshots': [s['id'] for s in c.get('snapshots', [])], 'size': c['size'],
                        'overlaps': [[o['start'], o['length']] for o in c.get('overlaps', [])]} for c in raw['clones']]}
                result.append(entry)
        finally:
            ioctx.close()
    finally:
        cluster.shutdown()
    return result
fs = cephfs.LibCephFS(conffile=CONF, auth_id=identity.removeprefix('client.'), conf={'keyring': keyring})
out = {'action': action, 'path': path}
try:
    fs.mount(filesystem_name=fsname)
    out['native_global_id'] = fs.get_instance_id()
    if action == 'stat' and not extra:
        out['stat'] = stat(path)
        if out['stat']['exists'] and not path.startswith(snapdir):
            out['layout'], out['layout_absent'] = layout(path, 'ceph.dir.layout' if path == root else 'ceph.file.layout')
    elif action == 'layout-set' and path == root and len(extra) == 4:
        fs.mkdir(path, 0o755)
        # One field at a time, each step a valid layout: the stripe unit
        # below the default object size first, the pool by its ID.
        for name, value in zip(('stripe_unit', 'object_size', 'stripe_count', 'pool_id'), extra):
            try:
                fs.setxattr(path, 'ceph.dir.layout.' + name, value.encode('utf-8'), 0)
            except cephfs.Error as e:
                raise ValueError('setxattr ceph.dir.layout.%s=%s: %s' % (name, value, e))
        out['layout'], out['layout_absent'] = layout(path, 'ceph.dir.layout')
        out['stat'] = stat(path)
    elif action == 'layout-clear' and path == root and not extra:
        fs.removexattr(path, 'ceph.dir.layout')
        out['layout'], out['layout_absent'] = layout(path, 'ceph.dir.layout')
        out['stat'] = stat(path)
    elif action in ('read', 'read-shared') and path != root and path != snapdir and not extra:
        if action == 'read-shared' and path != root + '/shared-file':
            raise ValueError('the concurrent reader admits only shared-file')
        timings = {'start': 0}
        out['read'] = digest(read_all(path, timings))
        out['stat'] = stat(path)
        if action == 'read-shared':
            out['timings_ns'] = timings
    elif action == 'write-native' and path == root + '/native-file' and len(extra) == 3:
        data = stream(extra[0], int(extra[1]))
        pieces = []
        for item in extra[2].split(','):
            off, n = (int(v) for v in item.split(':'))
            pieces.append((off, data[off:off + n]))
        out['written'] = write_at(path, pieces)
        out['stat'] = stat(path)
    elif action == 'write-trunc' and path == root + '/trunc-file' and len(extra) == 2:
        out['written'] = write_at(path, [(0, stream(extra[0], int(extra[1])))])
        out['stat'] = stat(path)
    elif action == 'truncate' and path == root + '/trunc-file' and len(extra) == 1:
        fs.truncate(path, int(extra[0]))
        out['stat'] = stat(path)
    elif action in ('mksnap', 'rmsnap') and path == snapdir and not extra:
        if action == 'mksnap':
            fs.mkdir(path, 0o755)
        else:
            fs.rmdir(path)
        out['stat'] = stat(path)
    elif action == 'list' and path == root and not extra:
        out['entries'] = []
        handle = fs.opendir(path)
        try:
            while True:
                entry = fs.readdir(handle)
                if entry is None:
                    break
                name = entry.d_name.decode('utf-8')
                if name in ('.', '..'):
                    continue
                out['entries'].append({'name': name, 'd_ino': entry.d_ino, 'stat': stat(path + '/' + name)})
        finally:
            fs.closedir(handle)
    elif action == 'objects' and path in files and len(extra) == 2 and extra[1] in ('plain', 'snaps'):
        out['stat'] = stat(path)
        out['objects'] = objects(out['stat']['ino'], int(extra[0]), extra[1] == 'snaps') if out['stat']['exists'] else []
    else:
        raise ValueError('native CephFS data action is not admitted')
finally:
    fs.shutdown()
print(json.dumps(out))
`

type mdsCephFSDataStat struct {
	Exists  bool   `json:"exists"`
	Errno   int    `json:"errno"`
	Ino     uint64 `json:"ino"`
	Mode    uint32 `json:"mode"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
	NLink   uint32 `json:"nlink"`
	Size    uint64 `json:"size"`
	MTime   int64  `json:"mtime"`
	Version uint64 `json:"version"`
}

type mdsCephFSDataDigest struct {
	Length uint64   `json:"length"`
	SHA256 string   `json:"sha256"`
	Chunks []string `json:"chunks"`
}

type mdsCephFSDataClone struct {
	Head      bool        `json:"head"`
	ID        uint64      `json:"id"`
	Snapshots []uint64    `json:"snapshots"`
	Size      uint64      `json:"size"`
	Overlaps  [][2]uint64 `json:"overlaps"`
}

type mdsCephFSDataSnaps struct {
	Seq    uint64               `json:"seq"`
	Clones []mdsCephFSDataClone `json:"clones"`
}

type mdsCephFSDataObject struct {
	ObjNo  int                 `json:"objno"`
	Name   string              `json:"name"`
	Exists bool                `json:"exists"`
	Size   uint64              `json:"size"`
	Snaps  *mdsCephFSDataSnaps `json:"snaps,omitempty"`
}

type mdsCephFSDataEntry struct {
	Name string            `json:"name"`
	DIno uint64            `json:"d_ino"`
	Stat mdsCephFSDataStat `json:"stat"`
}

type mdsCephFSDataNative struct {
	Action         string                `json:"action"`
	Path           string                `json:"path"`
	NativeGlobalID uint64                `json:"native_global_id"`
	Stat           *mdsCephFSDataStat    `json:"stat,omitempty"`
	Layout         string                `json:"layout,omitempty"`
	LayoutAbsent   bool                  `json:"layout_absent,omitempty"`
	Written        uint64                `json:"written,omitempty"`
	Read           *mdsCephFSDataDigest  `json:"read,omitempty"`
	TimingsNS      map[string]int64      `json:"timings_ns,omitempty"`
	Entries        []mdsCephFSDataEntry  `json:"entries,omitempty"`
	Objects        []mdsCephFSDataObject `json:"objects,omitempty"`
}

var mdsCephFSDataLayoutPattern = regexp.MustCompile(`^stripe_unit=([0-9]+) stripe_count=([0-9]+) object_size=([0-9]+) pool=([A-Za-z0-9_.-]+)$`)

func validMDSCephFSDataStat(s mdsCephFSDataStat) bool {
	if !s.Exists {
		return s.Errno == 2 && s.Ino == 0 && s.Size == 0
	}
	return s.Errno == 0 && s.Ino != 0 && s.Mode != 0
}

func validMDSCephFSDataDigest(d *mdsCephFSDataDigest) bool {
	if d == nil || d.Length > mdsCephFSDataReadLimit || len(d.Chunks) != int((d.Length+mdsCephFSDataChunk-1)/mdsCephFSDataChunk) {
		return false
	}
	for _, h := range append([]string{d.SHA256}, d.Chunks...) {
		if raw, err := hex.DecodeString(h); err != nil || len(raw) != 32 {
			return false
		}
	}
	return true
}

// decodeMDSCephFSDataNative accepts exactly the shape of one action. stat and
// objects bind the result to the plan's layout, pool and object names.
func decodeMDSCephFSDataNative(action, target, path, pool string, plan mdsCephFSDataPlan, data []byte) (mdsCephFSDataNative, error) {
	var result mdsCephFSDataNative
	if len(data) == 0 || len(data) > mdsCephFSDataJSONLimit || !json.Valid(data) {
		return result, errors.New("native CephFS data output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || result.Action != action || result.Path != path || result.NativeGlobalID == 0 {
		return mdsCephFSDataNative{}, errors.New("native CephFS data output identity differs")
	}
	stat := result.Stat != nil && validMDSCephFSDataStat(*result.Stat)
	layoutOK := func() bool {
		return result.Layout == plan.layout(pool) && !result.LayoutAbsent
	}
	ok := false
	switch action {
	case "stat":
		ok = stat && result.Read == nil && result.Objects == nil && result.Entries == nil && result.Written == 0
		switch {
		case !ok || !result.Stat.Exists || target == "snapshot" || target == "snapshot-file":
			ok = ok && result.Layout == "" && !result.LayoutAbsent
		case target == "dir":
			// The directory layout is set or removed by owned actions only.
			ok = result.LayoutAbsent && result.Layout == "" || layoutOK()
		default:
			ok = layoutOK()
		}
	case "layout-set":
		ok = stat && result.Stat.Exists && result.Stat.Mode&0o170000 == 0o040000 && layoutOK()
	case "layout-clear":
		ok = stat && result.Stat.Exists && result.LayoutAbsent && result.Layout == ""
	case "read", "read-shared":
		ok = stat && result.Stat.Exists && validMDSCephFSDataDigest(result.Read) && (action == "read") == (result.TimingsNS == nil)
		if action == "read-shared" {
			previous := int64(-1)
			for _, name := range []string{"start", "opened", "read", "closed"} {
				value, present := result.TimingsNS[name]
				ok = ok && present && value >= previous
				previous = value
			}
			ok = ok && len(result.TimingsNS) == 4
		}
	case "write-native":
		var want uint64
		for _, e := range plan.NativeExtents {
			want += e[1]
		}
		ok = stat && result.Written == want && result.Stat.Exists && result.Stat.Size == plan.NativeSize
	case "write-trunc":
		ok = stat && result.Written == plan.TruncWriteSize && result.Stat.Exists && result.Stat.Size == plan.TruncWriteSize
	case "truncate":
		ok = stat && result.Stat.Exists && result.Stat.Size == plan.TruncSize
	case "mksnap":
		ok = stat && result.Stat.Exists
	case "rmsnap":
		ok = stat && !result.Stat.Exists
	case "list":
		ok = len(result.Entries) <= len(plan.Files) && result.Stat == nil
		seen := map[string]bool{}
		for _, entry := range result.Entries {
			ok = ok && slices.Contains(plan.Files, entry.Name) && !seen[entry.Name] && entry.DIno != 0 && validMDSCephFSDataStat(entry.Stat) && entry.Stat.Exists
			seen[entry.Name] = true
		}
	case "objects":
		count := plan.ObjectCounts[target]
		ok = stat && count > 0 && (result.Stat.Exists && len(result.Objects) == count || !result.Stat.Exists && len(result.Objects) == 0)
		for i, o := range result.Objects {
			ok = ok && o.ObjNo == i && o.Name == fmt.Sprintf("%x.%08x", result.Stat.Ino, i) && (o.Exists || o.Size == 0) && (o.Snaps == nil || target == "snap-file" && o.Exists)
			if o.Snaps != nil {
				ok = ok && len(o.Snaps.Clones) <= 64
			}
		}
	}
	if !ok {
		return mdsCephFSDataNative{}, fmt.Errorf("native CephFS data %s output shape differs", action)
	}
	return result, nil
}

type mdsCephFSDataScope struct {
	out                    string
	plan                   mdsCephFSDataPlan
	goClient, native       *ceph.ClientConfig
	goIntent, nativeIntent bool
	baselineReady          bool
	baselineBlocklist      []mdsSessionsBlock
	lastRequestID          int64
	mutations              []*mdsCephFSMutation
	current                mdsFreshDaemon
	dataPool               int64
	dataPoolName           string
	// owned maps session addresses of the owned Go identity, observed
	// through observe, to their GlobalID.
	owned map[string]uint64
}

func (x *mdsCephFSDataScope) applied(kind string) bool {
	return slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == kind && m.State == "applied" })
}

func (x *mdsCephFSDataScope) mutated(kind string) bool {
	return slices.ContainsFunc(x.mutations, func(m *mdsCephFSMutation) bool { return m.Kind == kind })
}

type mdsCephFSDataAction struct {
	name, target string
	gid          uint64
}

// mdsCephFSDataMutations maps each one-shot native mutation to the mutation
// it requires first ("" when none).
var mdsCephFSDataMutations = map[string]string{
	"layout-set": "", "layout-clear": "layout-set", "write-native": "layout-set", "write-trunc": "layout-set", "truncate": "write-trunc",
	"snapshot-create": "layout-set", "snapshot-remove": "snapshot-create",
}

// parseMDSCephFSDataRequest admits <id> <token> <action> [argument]. Targets
// are closed plan names; GlobalIDs are canonical decimals.
func parseMDSCephFSDataRequest(fields []string, token string, previousID int64) (int64, mdsCephFSDataAction, error) {
	var action mdsCephFSDataAction
	if len(fields) < 3 || len(fields) > 4 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned MDS CephFS data request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, action, errors.New("invalid owned MDS CephFS data request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid MDS CephFS data request identity")
	}
	action.name = fields[2]
	arg, hasArg := "", len(fields) == 4
	if hasArg {
		arg = fields[3]
	}
	switch action.name {
	case "observe":
		gid, err := strconv.ParseUint(arg, 10, 64)
		if !hasArg || err != nil || gid == 0 || strconv.FormatUint(gid, 10) != arg {
			return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data observation requires one canonical GlobalID")
		}
		action.gid = gid
	case "stat":
		if !hasArg || !slices.Contains(mdsCephFSDataTargets, arg) {
			return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data stat target is not admitted")
		}
		action.target = arg
	case "read":
		if !hasArg || !slices.Contains(mdsCephFSDataFiles, arg) && arg != "snapshot-file" {
			return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data read target is not admitted")
		}
		action.target = arg
	case "objects":
		if !hasArg || !slices.Contains(mdsCephFSDataFiles, arg) {
			return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data object view admits only a planned file")
		}
		action.target = arg
	case "list", "layout-set", "layout-clear", "write-native", "write-trunc", "truncate", "read-shared", "snapshot-create", "snapshot-remove":
		if hasArg {
			return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data action takes no caller argument")
		}
		switch action.name {
		case "list", "layout-set", "layout-clear":
			action.target = "dir"
		case "write-native":
			action.target = "native-file"
		case "write-trunc", "truncate":
			action.target = "trunc-file"
		case "read-shared":
			action.target = "shared-file"
		default:
			action.target = "snapshot"
		}
	default:
		return 0, mdsCephFSDataAction{}, errors.New("MDS CephFS data action is not admitted")
	}
	return id, action, nil
}

type mdsCephFSDataResult struct {
	Source          string                    `json:"source_commit"`
	Token           string                    `json:"token"`
	RequestID       int64                     `json:"request_id"`
	Action          string                    `json:"action"`
	Target          string                    `json:"target,omitempty"`
	GlobalID        uint64                    `json:"global_id,omitempty"`
	FSID            string                    `json:"fsid"`
	FSCID           int64                     `json:"fscid"`
	FSName          string                    `json:"fs_name"`
	MDSGID          uint64                    `json:"mds_gid"`
	MDSEndpoint     string                    `json:"mds_endpoint"`
	MDSNonce        uint32                    `json:"mds_nonce"`
	MDSIncarnation  int32                     `json:"mds_incarnation"`
	Rank            int32                     `json:"rank"`
	IdentityChecked bool                      `json:"identity_checked"`
	Map             *mdsSessionsMap           `json:"map,omitempty"`
	DaemonEpoch     uint32                    `json:"daemon_mdsmap_epoch,omitempty"`
	Sessions        []mdsSessionsNative       `json:"sessions"`
	Blocklist       []mdsSessionsBlock        `json:"blocklist,omitempty"`
	Native          *mdsCephFSDataNative      `json:"native,omitempty"`
	Process         *ceph.NativeCommandResult `json:"native_process,omitempty"`
	Mutation        *mdsCephFSMutation        `json:"mutation,omitempty"`
	Complete        bool                      `json:"complete"`
	Error           string                    `json:"error,omitempty"`
	ElapsedMillis   int64                     `json:"elapsed_ms"`
	ObservedUTC     string                    `json:"observed_utc"`
}

func (c *fixtureCluster) mdsCephFSDataJournal() error {
	s, x := c.mdsFresh, c.mdsCephFSData
	identity, native := "", ""
	if x.goIntent {
		identity = "client.mds-cephfs-data-" + s.token
	}
	if x.nativeIntent {
		native = "client.mds-cephfs-data-native-" + s.token
	}
	return writeMDSFreshJSON(x.out, "mds-cephfs-data-owned.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name, "fscid": s.oracle.FSCID,
		"client_identity": identity, "native_identity": native, "plan": x.plan, "baseline_blocklist": x.baselineBlocklist, "mutations": x.mutations,
		"current_mds": x.current, "owned_session_addresses": x.owned,
	})
}

func (c *fixtureCluster) prepareMDSCephFSData(ctx context.Context, out string) error {
	p := c.profile
	if p.mdsCephFSData != "1" || p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSRanks == "1" || p.mdsCephFSCompletion == "1" ||
		p.osdProfile != "legacy-v6" || c.mdsFresh == nil || c.mdsFresh.fs == nil || c.mdsCephFSData != nil {
		return errors.New("invalid MDS CephFS data preparation scope")
	}
	s := c.mdsFresh
	if len(s.oracle.DataPools) != 1 {
		return errors.New("MDS CephFS data acceptance requires exactly one owned data pool")
	}
	x := &mdsCephFSDataScope{out: out, plan: mdsCephFSDataPlanFor(s.token), current: s.oracle.MDS, owned: map[string]uint64{}, dataPool: s.oracle.DataPools[0], dataPoolName: s.name + "-data"}
	c.mdsCephFSData = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	var err error
	if x.baselineBlocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	sessions, _, err := c.mdsSessionsList(ctx)
	if err != nil {
		return err
	}
	if len(sessions) != 0 {
		return errors.New("MDS CephFS data acceptance requires an owned filesystem without client sessions")
	}
	data, err := c.cluster.Ceph(ctx, "fs", "get", s.name, "--format", "json")
	if err != nil {
		return err
	}
	if allow, err := parseMDSCephFSAllowSnaps(data, s.oracle.FSCID, s.name); err != nil || !allow {
		return errors.Join(err, errors.New("MDS CephFS data acceptance requires a filesystem that already allows snapshots"))
	}
	x.baselineReady = true
	x.goIntent = true
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	if x.goClient, err = c.cluster.CreateClient(ctx, "mds-cephfs-data-"+s.token, mdsCephFSDataGoCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated MDS CephFS data identity: %w", err)
	}
	_, keyring, err := x.goClient.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-data.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	x.nativeIntent = true
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	if x.native, err = c.cluster.CreateClient(ctx, "mds-cephfs-data-native-"+s.token, mdsCephFSDataNativeCaps(s.name)); err != nil {
		return fmt.Errorf("create dedicated native CephFS data identity: %w", err)
	}
	_, nativeKeyring, err := x.native.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(nativeKeyring); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-cephfs-data-native.keyring"), nativeKeyring, 0600); err != nil {
		return err
	}
	c.profile.mdsCephFSDataIdentity = "client.mds-cephfs-data-" + s.token
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	goCaps, nativeCaps := mdsCephFSDataGoCaps(s.name), mdsCephFSDataNativeCaps(s.name)
	return writeMDSFreshJSON(out, "mds-cephfs-data-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name, "mds_gid": s.oracle.MDS.GID,
		"client_identity": c.profile.mdsCephFSDataIdentity, "native_identity": "client.mds-cephfs-data-native-" + s.token,
		"client_caps": map[string]string{"mon": goCaps.Mon, "mds": goCaps.MDS, "osd": goCaps.OSD}, "native_caps": map[string]string{"mon": nativeCaps.Mon, "mds": nativeCaps.MDS, "osd": nativeCaps.OSD},
		"plan": x.plan, "baseline_blocklist": x.baselineBlocklist, "data_pool": x.dataPool, "data_pool_name": x.dataPoolName, "dir_layout": x.plan.layout(x.dataPoolName),
	})
}

// mdsCephFSDataRunNative runs the fixed native driver once.
func (c *fixtureCluster) mdsCephFSDataRunNative(ctx context.Context, action, target string, extra ...string) (*ceph.NativeCommandResult, mdsCephFSDataNative, error) {
	s, x := c.mdsFresh, c.mdsCephFSData
	path, err := x.plan.path(target)
	if err != nil {
		return nil, mdsCephFSDataNative{}, err
	}
	timeout := 30 * time.Second
	switch action {
	case "write-native", "write-trunc", "truncate", "mksnap", "rmsnap", "objects", "layout-set", "layout-clear":
		timeout = 60 * time.Second
	case "read-shared":
		// The concurrent reader waits until the Go client flushes its
		// buffered data and acknowledges the revoke.
		timeout = 75 * time.Second
	}
	argv := append([]string{"python3", "-c", mdsCephFSDataNativePython, "client.mds-cephfs-data-native-" + s.token, "/out/mds-cephfs-data-native.keyring", s.name,
		x.plan.Directory, x.plan.Snapshot, x.dataPoolName, action, path}, extra...)
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: mdsCephFSDataJSONLimit, MaxStderr: 16 << 10,
		Label: "owned native CephFS data " + action})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, mdsCephFSDataNative{}, errors.Join(err, fmt.Errorf("native CephFS data %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeMDSCephFSDataNative(action, target, path, x.dataPoolName, x.plan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

func (c *fixtureCluster) mdsCephFSDataControl(ctx context.Context, out string, fields []string) (resultErr error) {
	s, x := c.mdsFresh, c.mdsCephFSData
	if c.profile.mdsCephFSData != "1" || c.profile.mdsFresh != "1" || s == nil || s.fs == nil || x == nil || s.out != out || x.out != out {
		return errors.New("invalid owned MDS CephFS data control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 110*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS CephFS data control requested after cleanup began")
	}
	id, action, err := parseMDSCephFSDataRequest(fields, s.token, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := mdsCephFSDataResult{Source: mdsFreshSource, Token: s.token, RequestID: id, Action: action.name, Target: action.target, GlobalID: action.gid, FSID: s.oracle.FSID,
		FSCID: s.oracle.FSCID, FSName: s.name, MDSGID: x.current.GID, MDSEndpoint: x.current.Endpoint, MDSNonce: x.current.Nonce, MDSIncarnation: x.current.Incarnation}
	defer func() {
		result.ElapsedMillis = time.Since(started).Milliseconds()
		result.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if resultErr != nil {
			result.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-cephfs-data-result.json", result))
	}()
	checkIdentity := func() error {
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		owned := x.current
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MDS.GID != owned.GID || current.MDS.Endpoint != owned.Endpoint || current.MDS.Nonce != owned.Nonce ||
			current.MDS.Incarnation != owned.Incarnation || current.MDS.Name != owned.Name || current.MDS.Rank != 0 || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
			return errors.New("MDS CephFS data native target changed during the owned action")
		}
		return nil
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	if err := c.mdsCephFSDataAct(ctx, id, action, &result); err != nil {
		return err
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	result.IdentityChecked = true
	current, err := c.mdsSessionsReadMap(ctx)
	if err != nil {
		return err
	}
	result.Map = &current
	data, err := c.mdsSessionsDaemon(ctx, "status", "--format", "json")
	if err != nil {
		return err
	}
	if result.DaemonEpoch, err = parseMDSSessionsDaemonEpoch(data); err != nil {
		return err
	}
	result.Complete = true
	return nil
}

// mdsCephFSDataExtra returns the plan values one native action receives.
func (x *mdsCephFSDataScope) extra(action, target string) []string {
	p := x.plan
	switch action {
	case "layout-set":
		return []string{strconv.FormatUint(p.StripeUnit, 10), strconv.FormatUint(p.ObjectSize, 10), strconv.FormatUint(p.StripeCount, 10), strconv.FormatInt(x.dataPool, 10)}
	case "write-native":
		var extents []string
		for _, e := range p.NativeExtents {
			extents = append(extents, fmt.Sprintf("%d:%d", e[0], e[1]))
		}
		return []string{p.Seeds["native-file"], strconv.FormatUint(p.NativeSize, 10), strings.Join(extents, ",")}
	case "write-trunc":
		return []string{p.Seeds["trunc-file"], strconv.FormatUint(p.TruncWriteSize, 10)}
	case "truncate":
		return []string{strconv.FormatUint(p.TruncSize, 10)}
	case "objects":
		mode := "plain"
		if target == "snap-file" {
			mode = "snaps"
		}
		return []string{strconv.Itoa(p.ObjectCounts[target]), mode}
	}
	return nil
}

func (c *fixtureCluster) mdsCephFSDataAct(ctx context.Context, id int64, action mdsCephFSDataAction, result *mdsCephFSDataResult) error {
	s, x := c.mdsFresh, c.mdsCephFSData
	if action.name == "observe" {
		sessions, _, err := c.mdsSessionsList(ctx)
		if err != nil {
			return err
		}
		result.Sessions = sessions
		for _, session := range sessions {
			if session.ID == action.gid && session.AuthID == "mds-cephfs-data-"+s.token && x.owned[session.Address] == 0 {
				// The test named this owned identity's GlobalID: its address
				// is owned evidence for cleanup of a later blocklist entry.
				x.owned[session.Address] = session.ID
				if err := c.mdsCephFSDataJournal(); err != nil {
					return err
				}
			}
		}
		result.Blocklist, err = c.mdsSessionsBlocklist(ctx)
		return err
	}
	native := action.name
	switch action.name {
	case "snapshot-create":
		native = "mksnap"
	case "snapshot-remove":
		native = "rmsnap"
	}
	var mutation *mdsCephFSMutation
	if requires, ok := mdsCephFSDataMutations[action.name]; ok {
		// One-shot: a mutation is never issued twice, whatever its outcome.
		if x.mutated(action.name) {
			return fmt.Errorf("MDS CephFS data %s was already issued", action.name)
		}
		if requires != "" && !x.applied(requires) {
			return fmt.Errorf("MDS CephFS data %s requires an applied %s", action.name, requires)
		}
		mutation = &mdsCephFSMutation{Kind: action.name, RequestID: id, Target: action.target, State: "intended"}
		x.mutations = append(x.mutations, mutation)
		result.Mutation = mutation
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
	}
	process, output, err := c.mdsCephFSDataRunNative(ctx, native, action.target, x.extra(action.name, action.target)...)
	result.Process = process
	if err != nil {
		if mutation != nil {
			mutation.State, mutation.Error = "uncertain", err.Error()
			err = errors.Join(err, c.mdsCephFSDataJournal())
		}
		return err
	}
	result.Native = &output
	if mutation != nil {
		mutation.State, mutation.NativeGlobalID = "applied", output.NativeGlobalID
		return c.mdsCephFSDataJournal()
	}
	return nil
}

type mdsCephFSDataCleanup struct {
	Source                string                  `json:"source_commit"`
	Token                 string                  `json:"token"`
	FSName                string                  `json:"fs_name"`
	Attempted             bool                    `json:"attempted"`
	Mutations             []*mdsCephFSMutation    `json:"mutations"`
	ResidualSessions      []mdsSessionsNative     `json:"residual_owned_sessions"`
	NamespaceChecked      bool                    `json:"namespace_checked"`
	NamespaceRestored     bool                    `json:"namespace_restored"`
	NamespaceNote         string                  `json:"namespace_note,omitempty"`
	Namespace             *mdsCephFSDataNative    `json:"namespace,omitempty"`
	LayoutRestored        bool                    `json:"layout_restored"`
	NewBlocklist          []mdsSessionsBlock      `json:"new_blocklist"`
	RemovedBlocklist      []mdsCephFSRemovedBlock `json:"removed_blocklist"`
	KeptBlocklist         []mdsSessionsBlock      `json:"kept_new_blocklist"`
	FinalBlocklist        []mdsSessionsBlock      `json:"final_blocklist,omitempty"`
	BlocklistRestored     bool                    `json:"blocklist_restored"`
	ClientIdentityRemoved bool                    `json:"client_identity_removed"`
	NativeIdentityRemoved bool                    `json:"native_identity_removed"`
	Completed             bool                    `json:"completed"`
	Errors                []string                `json:"errors,omitempty"`
}

// finishMDSCephFSData runs once, inside the cached MDS fresh cleanup and
// before the owned filesystem is removed. It reports residual owned
// sessions, reads the namespace back natively (the test removes the
// snapshot, the files, the directory layout and the directory itself),
// removes new blocklist entries attributable to an observed owned Go session
// address and deletes both identities. It never evicts; residual namespace
// entries are discarded by the verified filesystem removal.
func (c *fixtureCluster) finishMDSCephFSData(ctx context.Context) error {
	s, x := c.mdsFresh, c.mdsCephFSData
	if s == nil || x == nil {
		return nil
	}
	ctx, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	receipt := mdsCephFSDataCleanup{Source: mdsFreshSource, Token: s.token, FSName: s.name, Attempted: true, Mutations: x.mutations, ResidualSessions: []mdsSessionsNative{},
		NewBlocklist: []mdsSessionsBlock{}, RemovedBlocklist: []mdsCephFSRemovedBlock{}, KeptBlocklist: []mdsSessionsBlock{}}
	if err := writeMDSFreshJSON(x.out, "mds-cephfs-data-cleanup.json", receipt); err != nil {
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	goName, nativeName := "mds-cephfs-data-"+s.token, "mds-cephfs-data-native-"+s.token
	if x.baselineReady {
		sessions, _, err := c.mdsSessionsList(ctx)
		fail(err)
		goPresent := false
		for _, session := range sessions {
			if session.AuthID == goName || session.AuthID == nativeName {
				receipt.ResidualSessions = append(receipt.ResidualSessions, session)
				goPresent = goPresent || session.AuthID == goName
			}
		}
		switch {
		case err != nil:
			receipt.NamespaceNote = "native session listing failed; namespace readback skipped"
		case x.native == nil:
			receipt.NamespaceNote = "native identity was never created; no native namespace mutation was possible"
			receipt.NamespaceChecked, receipt.NamespaceRestored, receipt.LayoutRestored = true, len(x.mutations) == 0, len(x.mutations) == 0
		case goPresent:
			// A native mount could wait on capability revocations from a Go
			// session whose process is gone. The filesystem removal discards it.
			receipt.NamespaceNote = "Go session still present; namespace readback skipped and residual entries are discarded with the owned filesystem"
		default:
			_, native, err := c.mdsCephFSDataRunNative(ctx, "stat", "dir")
			fail(err)
			if err == nil {
				receipt.Namespace, receipt.NamespaceChecked, receipt.NamespaceRestored = &native, true, !native.Stat.Exists
				// The layout is gone with the directory, or was removed natively.
				receipt.LayoutRestored = !native.Stat.Exists || native.LayoutAbsent
				if !receipt.NamespaceRestored {
					receipt.NamespaceNote = "the owned directory (with any snapshot, file or layout in it) remained; it is discarded with the owned filesystem"
				}
			}
		}
		blocks, err := c.mdsSessionsBlocklist(ctx)
		fail(err)
		if err == nil {
			receipt.BlocklistRestored = true
			for _, entry := range blocks {
				if slices.ContainsFunc(x.baselineBlocklist, func(b mdsSessionsBlock) bool { return b.Address == entry.Address && b.Range == entry.Range }) {
					continue
				}
				receipt.NewBlocklist = append(receipt.NewBlocklist, entry)
				if entry.Range || x.owned[entry.Address] == 0 {
					receipt.KeptBlocklist = append(receipt.KeptBlocklist, entry)
					receipt.BlocklistRestored = false
					continue
				}
				if _, err := c.cluster.Ceph(ctx, "osd", "blocklist", "rm", entry.Address); err != nil {
					receipt.BlocklistRestored = false
					fail(err)
					continue
				}
				receipt.RemovedBlocklist = append(receipt.RemovedBlocklist, mdsCephFSRemovedBlock{Kind: "owned-session", Entry: entry})
			}
			if len(receipt.KeptBlocklist) != 0 {
				fail(errors.New("MDS CephFS data acceptance kept new blocklist entries it cannot attribute to an owned session"))
			}
			if len(receipt.RemovedBlocklist) != 0 {
				final, err := c.mdsSessionsBlocklist(ctx)
				fail(err)
				receipt.FinalBlocklist = final
				for _, removed := range receipt.RemovedBlocklist {
					if err != nil || mdsSessionsFindBlock(final, removed.Entry.Address) != nil {
						receipt.BlocklistRestored = false
						fail(errors.New("owned blocklist entry remained after removal"))
					}
				}
			}
		}
	} else {
		receipt.NamespaceChecked, receipt.NamespaceRestored, receipt.LayoutRestored, receipt.BlocklistRestored = true, true, true, true
		receipt.NamespaceNote = "preparation ended before any owned namespace mutation was possible"
	}
	remove := func(config *ceph.ClientConfig, intent bool, label string) bool {
		if config == nil {
			if intent {
				fail(fmt.Errorf("MDS CephFS data %s identity creation outcome is unknown", label))
				return false
			}
			return true
		}
		err := c.cluster.DeleteClient(ctx, config)
		fail(err)
		return err == nil
	}
	receipt.ClientIdentityRemoved = remove(x.goClient, x.goIntent, "client")
	receipt.NativeIdentityRemoved = remove(x.native, x.nativeIntent, "native oracle")
	for _, err := range errs {
		receipt.Errors = append(receipt.Errors, err.Error())
	}
	receipt.Completed = len(errs) == 0 && receipt.NamespaceChecked && receipt.BlocklistRestored && receipt.ClientIdentityRemoved && receipt.NativeIdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("MDS CephFS data cleanup did not verify every check"))
	}
	return errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "mds-cephfs-data-cleanup.json", receipt))
}

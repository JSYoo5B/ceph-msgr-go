package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The RBD clone acceptance reuses the RBD base scope (token, owned pool and
// both identities) and adds a parent image and its clone (layering, clone
// format 2) to the owned pool, created only on the test's request. The Go
// test then reads the clone header and its parent through the explicit OSD
// API, reads never-written clone blocks from the parent object at the parent
// snapshot, copies parent data up into the clone the way librbd's
// CopyupRequest does (one vector rbd.sparse_copyup, SETALLOCHINT and the
// write, or a separate copy-up under an empty SnapContext once the clone has
// a snapshot) and reads native copy-ups back. The closed control here only
// lets native librbd and librados create the images, write the clone, take a
// clone snapshot, flatten the clone and observe everything read-only after
// each step; the clone write and the flatten run in a separate native client
// whose Messenger requests to data objects (debug_ms 1) are kept as the
// independent record of the native op vectors, and its librbd copy-up
// extent maps (debug_rbd 20, CopyupRequest::convert_copyup_extent_map) are
// kept as the record of each copy-up's sparse parent map. Every native action is
// journaled in rbd-base-owned.json before it is issued and is never
// reissued; both images go with the owned pool at cleanup. Clone layout,
// class encodings and the copy-up construction stay in the test-only
// consumer (integration/rbd_clone_test.go). Native behavior relied upon:
// librbd/api/Image.cc and image/CloneRequest.cc (clone v2), image/
// AttachChildRequest.cc, image/RefreshRequest.cc (parent_get,
// parent_overlap_get, enable_sparse_copyup), io/ObjectRequest.cc,
// io/CopyupRequest.cc (convert_copyup_extent_map log line, debug_rbd 20),
// io/Utils.cc (read_parent), operation/FlattenRequest.cc,
// cls/rbd/cls_rbd.cc (copyup, sparse_copyup, get_parent, parent_get,
// parent_overlap_get, children_list, parent detach), msg/async/
// AsyncConnection.cc and msg/DispatchQueue.cc (debug_ms 1 lines),
// messages/MOSDOp.h and MOSDOpReply.h (print), pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource (LGPL-2.1 references; no code
// copied).

// rbdCloneActions is the closed, ordered action sequence of the clone
// control. Each action runs at most once and only after its predecessors.
var rbdCloneActions = []string{"setup", "observe-copyup", "write", "snapshot", "observe-snapshot", "flatten"}

// rbdCloneHasSnapshot reports the actions after which the clone holds its
// snapshot (and the observation adds the snapshot views).
func rbdCloneHasSnapshot(action string) bool {
	return action == "snapshot" || action == "observe-snapshot" || action == "flatten"
}

// rbdCloneTraced reports the actions whose native mutation runs in the
// traced client.
func rbdCloneTraced(action string) bool { return action == "write" || action == "flatten" }

// rbdCloneBlobViews names the binary views of every native clone action; the
// file of view v for action a is rbd-clone-<a>-<v>.bin. The two clone
// snapshot views exist only once the clone has its snapshot.
var rbdCloneBlobViews = map[string]string{
	"clone_head":          "clone-head",
	"clone_objects_head":  "clone-objects-head",
	"clone_snap":          "clone-snap",
	"clone_objects_snap":  "clone-objects-snap",
	"parent_head":         "parent-head",
	"parent_snap":         "parent-snap",
	"parent_objects_snap": "parent-objects-snap",
}

func rbdCloneBlobName(action, key string) string {
	return "rbd-clone-" + action + "-" + rbdCloneBlobViews[key] + ".bin"
}

func rbdCloneBlobKeys(action string) []string {
	keys := []string{"clone_head", "clone_objects_head", "parent_head", "parent_snap", "parent_objects_snap"}
	if rbdCloneHasSnapshot(action) {
		keys = append(keys, "clone_snap", "clone_objects_snap")
	}
	return keys
}

// The fixed native exec lists of every observation: the clone's id object
// and header (librbd's open and refresh reads, the legacy get_parent and the
// operation features), the clone header at the clone snapshot once it exists
// (librbd's per-snapshot refresh reads, get_parent and get_size), and the
// parent's id object and header at the parent snapshot.
var rbdCloneCalls = [][3]string{
	{"id", "rbd", "get_id"}, {"header", "rbd", "get_size"}, {"header", "rbd", "get_features"}, {"header", "rbd", "get_flags"},
	{"header", "rbd", "get_snapcontext"}, {"header", "lock", "get_info"}, {"header", "rbd", "parent_get"}, {"header", "rbd", "parent_overlap_get"},
	{"header", "rbd", "get_parent"}, {"header", "rbd", "get_object_prefix"}, {"header", "rbd", "op_features_get"},
}

var rbdCloneSnapshotCalls = []string{"snapshot_get", "parent_overlap_get", "get_flags", "get_protection_status", "get_parent", "get_size"}

var rbdCloneParentCalls = [][3]string{
	{"id", "rbd", "get_id"}, {"header", "rbd", "get_object_prefix"}, {"header", "rbd", "get_size"}, {"header", "rbd", "get_snapcontext"},
	{"header", "rbd", "snapshot_get"}, {"header", "rbd", "children_list"}, {"header", "rbd", "op_features_get"},
}

// rbdCloneTraceLimit and rbdCloneTraceWidth bound the kept native Messenger
// lines of one traced action; rbdCloneCopyupLimit bounds its kept librbd
// copy-up extent-map lines (same width).
const (
	rbdCloneTraceLimit  = 96
	rbdCloneTraceWidth  = 600
	rbdCloneCopyupLimit = 16
)

// rbdCloneCopyupPrefix starts every kept librbd copy-up line: the method
// name and the data object (CopyupRequest's dout prefix, without the
// request pointer).
const rbdCloneCopyupPrefix = "convert_copyup_extent_map: rbd_data."

// rbdClonePlan is the fixed recipe of the parent and the clone. The native
// driver writes the parent around its snapshot, clones it, writes
// NativeWrites to the clone, takes the clone snapshot and flattens the clone;
// GoCopyupWrites and GoSnapshotWrites are written by the Go test.
type rbdClonePlan struct {
	Pool             string          `json:"pool"`
	BaseImage        string          `json:"base_image"`
	Parent           string          `json:"parent"`
	Clone            string          `json:"clone"`
	ParentSnapshot   string          `json:"parent_snapshot"`
	CloneSnapshot    string          `json:"clone_snapshot"`
	Order            int             `json:"order"`
	Size             uint64          `json:"size"`
	Features         uint64          `json:"features"`
	CloneFormat      int             `json:"clone_format"`
	ParentPreWrites  []rbdBaseExtent `json:"parent_pre_snapshot_writes"`
	ParentPostWrites []rbdBaseExtent `json:"parent_post_snapshot_writes"`
	GoCopyupWrites   []rbdBaseExtent `json:"go_copyup_writes"`
	NativeWrites     []rbdBaseExtent `json:"native_writes"`
	GoSnapshotWrites []rbdBaseExtent `json:"go_snapshot_writes"`
}

// rbdClonePlanFor places a 384 KiB, six-object parent (64 KiB objects). At
// the parent snapshot: object 0 full; object 1 two extents with holes;
// object 2 its first half; object 3 two small extents with holes; object 4
// absent; object 5 its last 4 KiB. After the snapshot every parent object
// changes at the head (object 4 is created), so a clone read that used the
// parent head instead of the snapshot would differ everywhere. The clone
// (overlap 384 KiB) is written by Go in object 1 (a partial copy-up across a
// parent extent and a hole), natively in object 3 (librbd's own copy-up),
// and after the clone snapshot by Go in object 1 again (the OSD clones it)
// and in object 2 (a copy-up while the clone has a snapshot). Objects 0, 4
// and 5 stay unwritten in the clone until the flatten.
func rbdClonePlanFor(token string) rbdClonePlan {
	const k = 1 << 10
	psnap, csnap := "ceph-msgr-psnap", "ceph-msgr-csnap"
	if len(token) >= 8 {
		psnap += "-" + token[:8]
		csnap += "-" + token[:8]
	}
	return rbdClonePlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, Parent: "ceph-msgr-parent-" + token, Clone: "ceph-msgr-clone-" + token,
		ParentSnapshot: psnap, CloneSnapshot: csnap, Order: 16, Size: 384 * k,
		// RBD_FEATURE_LAYERING only and clone format 2 (child_attach on the
		// parent snapshot; no protection needed).
		Features: 1, CloneFormat: 2,
		ParentPreWrites:  []rbdBaseExtent{{0, 64 * k}, {72 * k, 16 * k}, {104 * k, 8 * k}, {128 * k, 32 * k}, {208 * k, 4 * k}, {232 * k, 8 * k}, {380 * k, 4 * k}},
		ParentPostWrites: []rbdBaseExtent{{0, 16 * k}, {80 * k, 16 * k}, {160 * k, 32 * k}, {192 * k, 4 * k}, {256 * k, 64 * k}, {376 * k, 8 * k}},
		GoCopyupWrites:   []rbdBaseExtent{{84 * k, 8 * k}},
		NativeWrites:     []rbdBaseExtent{{196 * k, 4 * k}},
		GoSnapshotWrites: []rbdBaseExtent{{108 * k, 8 * k}, {156 * k, 8 * k}}}
}

func (p rbdClonePlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.Parent == "ceph-msgr-parent-"+token &&
		p.Clone == "ceph-msgr-clone-"+token && p.ParentSnapshot == "ceph-msgr-psnap-"+token[:8] && p.CloneSnapshot == "ceph-msgr-csnap-"+token[:8] &&
		p.Order == 16 && p.Size == 384<<10 && p.Features == 1 && p.CloneFormat == 2 && len(p.ParentPreWrites) > 0 && len(p.ParentPostWrites) > 0 &&
		len(p.GoCopyupWrites) > 0 && len(p.NativeWrites) > 0 && len(p.GoSnapshotWrites) > 0
}

// The fixed native clone driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache and with error-level stderr only. The clone write and the flatten
// run in a second client that logs its Messenger traffic (debug_ms 1) and
// librbd (debug_rbd 20) to a private file; only the request and reply lines
// of data objects are kept (AsyncConnection::send_msg "-->" and
// DispatchQueue::pre_dispatch "<==" lines, cut at the message print), and
// librbd's copy-up extent maps (CopyupRequest::convert_copyup_extent_map,
// from the method name on). Read-only opens register no header
// watch. Sources: pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at
// rbdBaseSource; independent driver, no code copied.
const rbdCloneNativePython = `
import faulthandler, hashlib, json, os, struct, sys, time
faulthandler.enable(file=sys.stderr, all_threads=True)
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'observe-copyup', 'write', 'snapshot', 'observe-snapshot', 'flatten')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('parent') != 'ceph-msgr-parent-' + token or plan.get('clone') != 'ceph-msgr-clone-' + token
        or plan.get('parent_snapshot') != 'ceph-msgr-psnap-' + token[:8] or plan.get('clone_snapshot') != 'ceph-msgr-csnap-' + token[:8]
        or plan.get('features') != 1 or plan.get('clone_format') != 2 or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD clone driver scope is not owned')
OUT = '/out'
CONF = '/tmp/ceph-msgr-test/ceph.conf'
NOSNAP = 0xfffffffffffffffe
# cls_lock_get_info_op{name="rbd_lock"}: ENCODE_START(1, 1) and a string.
LOCK_INFO = b'\x01\x01' + struct.pack('<II', 12, 8) + b'rbd_lock'
TRACE_LIMIT, TRACE_WIDTH, COPYUP_LIMIT = 96, 600, 16
BASE = {'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'}
pool, parent, clone = plan['pool'], plan['parent'], plan['clone']
psnap, csnap = plan['parent_snapshot'], plan['clone_snapshot']
PREFIX = 'rbd-clone-%s-' % action

def phase(name):
    # The controller can read this record even if a native signal kills Python.
    # Diagnostics must not suppress cleanup or replace the action's exception.
    try:
        path = OUT + '/rbd-clone-' + action + '-phase.json'
        with open(path + '.tmp', 'w') as f:
            json.dump({'token': token, 'action': action, 'request_id': int(request_id), 'phase': name}, f)
            f.flush()
        os.replace(path + '.tmp', path)
    except OSError:
        pass

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def u64(value):
    return struct.pack('<Q', value)

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-clone:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
        counter += 1
    return bytes(data[:length])

def publish(filename, data):
    temporary = os.path.join(OUT, '.' + filename + '.tmp')
    with open(temporary, 'wb') as f:
        f.write(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, os.path.join(OUT, filename))
    return {'name': filename, 'length': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

def call(ioctx, oid, cls, method, data):
    entry = {'object': oid, 'class': cls, 'method': method, 'input_hex': data.hex()}
    try:
        length, output = ioctx.execute(oid, cls, method, data, 8192)
        output = bytes(output)
        if length != len(output):
            raise ValueError('native class output length differs')
        entry.update(code=0, output_hex=output.hex())
    except rados.Error as error:
        if not isinstance(error.errno, int) or error.errno <= 0:
            raise
        entry.update(code=-error.errno, output_hex='')
    return entry

def clone_calls(ioctx, image_id):
    header = 'rbd_header.' + image_id
    return [call(ioctx, 'rbd_id.' + clone, 'rbd', 'get_id', b''),
            call(ioctx, header, 'rbd', 'get_size', u64(NOSNAP)),
            call(ioctx, header, 'rbd', 'get_features', struct.pack('<QB', NOSNAP, 0)),
            call(ioctx, header, 'rbd', 'get_flags', u64(NOSNAP)),
            call(ioctx, header, 'rbd', 'get_snapcontext', b''),
            call(ioctx, header, 'lock', 'get_info', LOCK_INFO),
            call(ioctx, header, 'rbd', 'parent_get', b''),
            call(ioctx, header, 'rbd', 'parent_overlap_get', u64(NOSNAP)),
            call(ioctx, header, 'rbd', 'get_parent', u64(NOSNAP)),
            call(ioctx, header, 'rbd', 'get_object_prefix', b''),
            call(ioctx, header, 'rbd', 'op_features_get', b'')]

def clone_snapshot_calls(ioctx, image_id, snap_id):
    header = 'rbd_header.' + image_id
    return [call(ioctx, header, 'rbd', method, u64(snap_id))
            for method in ('snapshot_get', 'parent_overlap_get', 'get_flags', 'get_protection_status', 'get_parent', 'get_size')]

def parent_calls(ioctx, image_id, snap_id):
    header = 'rbd_header.' + image_id
    return [call(ioctx, 'rbd_id.' + parent, 'rbd', 'get_id', b''),
            call(ioctx, header, 'rbd', 'get_object_prefix', b''),
            call(ioctx, header, 'rbd', 'get_size', u64(snap_id)),
            call(ioctx, header, 'rbd', 'get_snapcontext', b''),
            call(ioctx, header, 'rbd', 'snapshot_get', u64(snap_id)),
            call(ioctx, header, 'rbd', 'children_list', u64(snap_id)),
            call(ioctx, header, 'rbd', 'op_features_get', b'')]

def view(image):
    stat = image.stat()
    result = {'image_id': text(image.id()),
              # image_info_t's fixed parent_name array reaches Python as bytes.
              'stat': {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix', 'parent_pool', 'parent_name')},
              'features': image.features(), 'op_features': image.op_features(), 'flags': image.flags(),
              'snapshots': [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]}
    try:
        spec = image.get_parent_image_spec()
        result['parent'] = {'pool_id': spec['pool_id'], 'pool_name': text(spec['pool_name']), 'pool_namespace': text(spec['pool_namespace']),
                            'image_id': text(spec['image_id']), 'image_name': text(spec['image_name']), 'trash': bool(spec['trash']),
                            'snap_id': spec['snap_id'], 'snap_name': text(spec['snap_name']), 'snap_namespace_type': spec['snap_namespace_type']}
    except rbd.ImageNotFound:
        result['parent'] = None
    try:
        result['overlap'] = image.overlap()
    except rbd.ImageNotFound:
        result['overlap'] = None
    return result

def object_views(ioctx, prefix, count, reads):
    objects, blobs = [], {label: bytearray() for label, _ in reads}
    try:
        for index in range(count):
            oid = '%s.%016x' % (prefix, index)
            entry = {'index': index, 'name': oid}
            for label, snap in reads:
                ioctx.set_read(snap)
                blob = blobs[label]
                try:
                    length, _ = ioctx.stat(oid)
                    data = ioctx.read(oid, length, 0) if length else b''
                except rados.ObjectNotFound:
                    entry[label] = {'exists': False, 'errno': 2, 'size': 0, 'offset': len(blob), 'sha256': ''}
                    continue
                if len(data) != length:
                    raise ValueError('native object read is shorter than its stat')
                entry[label] = {'exists': True, 'errno': 0, 'size': length, 'offset': len(blob), 'sha256': hashlib.sha256(data).hexdigest()}
                blob += data
            objects.append(entry)
    finally:
        ioctx.set_read(rados.LIBRADOS_SNAP_HEAD)
    return objects, {label: bytes(blob) for label, blob in blobs.items()}

def snap_id_of(snapshots, name):
    ids = [s['id'] for s in snapshots if s['name'] == name]
    return ids[0] if ids else None

def observe(ioctx, out):
    blobs = {}
    with rbd.Image(ioctx, clone, read_only=True) as image:
        out['clone_view'] = view(image)
        head = image.read(0, out['clone_view']['stat']['size'])
    clone_id = out['clone_view']['image_id']
    snap_id = snap_id_of(out['clone_view']['snapshots'], csnap)
    if snap_id is not None:
        with rbd.Image(ioctx, clone, snapshot=csnap, read_only=True) as image:
            out['clone_snapshot_view'] = view(image)
            blobs['clone_snap'] = publish(PREFIX + 'clone-snap.bin', image.read(0, out['clone_snapshot_view']['stat']['size']))
    with rbd.Image(ioctx, parent, read_only=True) as image:
        out['parent_view'] = view(image)
        blobs['parent_head'] = publish(PREFIX + 'parent-head.bin', image.read(0, out['parent_view']['stat']['size']))
    with rbd.Image(ioctx, parent, snapshot=psnap, read_only=True) as image:
        out['parent_snapshot_view'] = view(image)
        blobs['parent_snap'] = publish(PREFIX + 'parent-snap.bin', image.read(0, out['parent_snapshot_view']['stat']['size']))
        out['children'] = [{'pool': text(c['pool']), 'pool_namespace': text(c['pool_namespace']), 'image': text(c['image']), 'id': text(c['id']),
                            'trash': bool(c['trash'])} for c in image.list_children2()]
    parent_snap_id = snap_id_of(out['parent_view']['snapshots'], psnap)
    if parent_snap_id is None:
        raise ValueError('owned parent snapshot is missing')
    out['clone_calls'] = clone_calls(ioctx, clone_id)
    if snap_id is not None:
        out['clone_snapshot_calls'] = clone_snapshot_calls(ioctx, clone_id, snap_id)
    out['parent_calls'] = parent_calls(ioctx, out['parent_view']['image_id'], parent_snap_id)
    reads = [('head', rados.LIBRADOS_SNAP_HEAD)] + ([('snap', snap_id)] if snap_id is not None else [])
    stat = out['clone_view']['stat']
    out['clone_objects'], views = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'], reads)
    blobs['clone_head'] = publish(PREFIX + 'clone-head.bin', head)
    blobs['clone_objects_head'] = publish(PREFIX + 'clone-objects-head.bin', views['head'])
    if snap_id is not None:
        blobs['clone_objects_snap'] = publish(PREFIX + 'clone-objects-snap.bin', views['snap'])
    stat = out['parent_view']['stat']
    out['parent_objects'], views = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'], [('snap', parent_snap_id)])
    blobs['parent_objects_snap'] = publish(PREFIX + 'parent-objects-snap.bin', views['snap'])
    out['blobs'] = blobs

def write(image, phase, extent):
    data = pattern(phase, extent['offset'], extent['length'])
    started = time.monotonic_ns()
    written = image.write(data, extent['offset'])
    elapsed = time.monotonic_ns() - started
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest(), 'elapsed_ns': elapsed}

def traced(label, run):
    # A separate client logs its Messenger traffic (debug_ms 1) and librbd
    # (debug_rbd 20) to a private file; only request ("-->") and reply ("<==")
    # lines of data objects are kept, cut to the message print, and librbd's
    # copy-up extent maps from the method name on.
    path = '/tmp/ceph-msgr-rbd-clone-%s-%s.log' % (token, label)
    if os.path.exists(path):
        os.remove(path)
    phase('traced-client-create')
    traced_cluster = rados.Rados(conffile=CONF, rados_id=identity[len('client.'):], conf=dict(BASE, log_file=path, log_to_file='true', debug_ms='1', debug_rbd='20'))
    phase('traced-client-connect')
    traced_cluster.connect(timeout=30)
    try:
        phase('traced-ioctx-open')
        traced_ioctx = traced_cluster.open_ioctx(pool)
        try:
            result = run(traced_ioctx)
        finally:
            phase('traced-ioctx-close')
            traced_ioctx.close()
    finally:
        phase('traced-client-shutdown')
        traced_cluster.shutdown()
    phase('trace-extract')
    lines, copyups = [], []
    with open(path, errors='replace') as f:
        for line in f:
            marker = line.find('librbd::io::CopyupRequest: ')
            start = line.find('convert_copyup_extent_map: rbd_data.', marker) if marker >= 0 else -1
            if start >= 0:
                message = line[start:].rstrip('\n')
                if len(message) > TRACE_WIDTH or len(copyups) >= COPYUP_LIMIT:
                    raise ValueError('native copy-up trace exceeds its bound')
                copyups.append({'message': message})
                continue
            start = line.find('osd_op')
            if start < 0 or 'rbd_data.' not in line[start:]:
                continue
            direction = 'send' if ' --> ' in line[:start] else 'receive' if '<== ' in line[:start] else None
            if direction is None:
                continue
            message = line[start:].rstrip('\n')
            for cut in (' -- 0x', ' ==== '):
                end = message.find(cut)
                if end >= 0:
                    message = message[:end]
            if len(message) > TRACE_WIDTH or len(lines) >= TRACE_LIMIT:
                raise ValueError('native data-object trace exceeds its bound')
            lines.append({'direction': direction, 'message': message})
    os.remove(path)
    return result, lines, copyups

phase('client-create')
cluster = rados.Rados(conffile=CONF, rados_id=identity[len('client.'):], conf=BASE)
phase('client-connect')
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'parent': parent, 'clone': clone, 'native_global_id': cluster.get_instance_id()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            # The clone acceptance runs right after the base acceptance.
            listed = sorted(text(i) for i in rbd.RBD().list(ioctx))
            if listed != [plan['base_image']]:
                raise ValueError('owned RBD pool holds an unplanned image')
            rbd.RBD().create(ioctx, parent, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            writes = []
            with rbd.Image(ioctx, parent) as image:
                writes += [write(image, 'parent-pre', e) for e in plan['parent_pre_snapshot_writes']]
                image.flush()
                image.create_snap(psnap)
                writes += [write(image, 'parent-post', e) for e in plan['parent_post_snapshot_writes']]
                image.flush()
            rbd.RBD().clone(ioctx, parent, psnap, ioctx, clone, features=plan['features'], order=plan['order'], clone_format=plan['clone_format'])
            out['writes'] = writes
        elif action == 'write':
            def run(traced_ioctx):
                with rbd.Image(traced_ioctx, clone) as image:
                    writes = [write(image, 'native', e) for e in plan['native_writes']]
                    image.flush()
                return writes
            out['writes'], out['trace'], out['copyup_trace'] = traced('write', run)
        elif action == 'snapshot':
            with rbd.Image(ioctx, clone) as image:
                image.create_snap(csnap)
        elif action == 'flatten':
            def run(traced_ioctx):
                started = time.monotonic_ns()
                phase('flatten-image-open')
                with rbd.Image(traced_ioctx, clone) as image:
                    phase('flatten-call')
                    image.flatten()
                    phase('flatten-returned')
                phase('flatten-image-closed')
                return time.monotonic_ns() - started
            out['flatten_ns'], out['trace'], out['copyup_trace'] = traced('flatten', run)
        phase('observe')
        observe(ioctx, out)
    finally:
        phase('ioctx-close')
        ioctx.close()
finally:
    phase('client-shutdown')
    cluster.shutdown()
phase('complete')
print(json.dumps(out, sort_keys=True))
`

type rbdCloneParentSpec struct {
	PoolID            int64  `json:"pool_id"`
	PoolName          string `json:"pool_name"`
	PoolNamespace     string `json:"pool_namespace"`
	ImageID           string `json:"image_id"`
	ImageName         string `json:"image_name"`
	Trash             bool   `json:"trash"`
	SnapID            uint64 `json:"snap_id"`
	SnapName          string `json:"snap_name"`
	SnapNamespaceType int    `json:"snap_namespace_type"`
}

// rbdCloneImageView is librbd's view of one open image: stat, features,
// snapshots, the parent spec and overlap (null without a parent).
type rbdCloneImageView struct {
	ImageID    string              `json:"image_id"`
	Stat       *rbdBaseStat        `json:"stat"`
	Features   uint64              `json:"features"`
	OpFeatures uint64              `json:"op_features"`
	Flags      uint64              `json:"flags"`
	Snapshots  []rbdBaseSnapshot   `json:"snapshots"`
	Parent     *rbdCloneParentSpec `json:"parent"`
	Overlap    *uint64             `json:"overlap"`
}

type rbdCloneChild struct {
	Pool          string `json:"pool"`
	PoolNamespace string `json:"pool_namespace"`
	Image         string `json:"image"`
	ID            string `json:"id"`
	Trash         bool   `json:"trash"`
}

type rbdCloneObject struct {
	Index uint64             `json:"index"`
	Name  string             `json:"name"`
	Head  *rbdBaseObjectView `json:"head,omitempty"`
	Snap  *rbdBaseObjectView `json:"snap,omitempty"`
}

// rbdCloneTrace is one kept native Messenger line: a request sent to, or a
// reply received from, the OSD for a data object.
type rbdCloneTrace struct {
	Direction string `json:"direction"`
	Message   string `json:"message"`
}

// rbdCloneCopyupLine is one kept librbd copy-up extent-map line.
type rbdCloneCopyupLine struct {
	Message string `json:"message"`
}

// rbdCloneNative is one complete clone-driver output; unknown fields are
// refused.
type rbdCloneNative struct {
	Action             string                 `json:"action"`
	Token              string                 `json:"token"`
	RequestID          int64                  `json:"request_id"`
	Pool               string                 `json:"pool"`
	PoolID             int64                  `json:"pool_id"`
	Parent             string                 `json:"parent"`
	Clone              string                 `json:"clone"`
	NativeGlobalID     uint64                 `json:"native_global_id"`
	CloneView          *rbdCloneImageView     `json:"clone_view"`
	CloneSnapshotView  *rbdCloneImageView     `json:"clone_snapshot_view,omitempty"`
	ParentView         *rbdCloneImageView     `json:"parent_view"`
	ParentSnapshotView *rbdCloneImageView     `json:"parent_snapshot_view"`
	Children           []rbdCloneChild        `json:"children"`
	CloneCalls         []rbdWriteClassCall    `json:"clone_calls"`
	CloneSnapshotCalls []rbdWriteClassCall    `json:"clone_snapshot_calls,omitempty"`
	ParentCalls        []rbdWriteClassCall    `json:"parent_calls"`
	CloneObjects       []rbdCloneObject       `json:"clone_objects"`
	ParentObjects      []rbdCloneObject       `json:"parent_objects"`
	Blobs              map[string]rbdBaseBlob `json:"blobs"`
	Writes             []rbdLockWrite         `json:"writes,omitempty"`
	Trace              []rbdCloneTrace        `json:"trace,omitempty"`
	CopyupTrace        []rbdCloneCopyupLine   `json:"copyup_trace,omitempty"`
	FlattenNS          int64                  `json:"flatten_ns,omitempty"`
}

// rbdCloneFeatureOperations is RBD_FEATURE_OPERATIONS (include/rbd/
// features.h): clone format 2 sets operation features (clone-parent,
// clone-child) on both images, which enables this feature bit next to the
// requested layering.
const rbdCloneFeatureOperations = 1 << 8

// rbdCloneNoOverlap reports librbd's overlap() of an image without a parent:
// pybind raises ImageNotFound (null) or librbd reports 0.
func rbdCloneNoOverlap(overlap *uint64) bool { return overlap == nil || *overlap == 0 }

func rbdCloneIDValid(id string) bool {
	return id != "" && len(id) <= 64 && !strings.ContainsAny(id, "./ \x00")
}

// rbdCloneViewValid checks one librbd view's structure for the plan: the
// image ID, layout, size and features. Parent and snapshots are judged by the
// caller.
func rbdCloneViewValid(v *rbdCloneImageView, plan rbdClonePlan) bool {
	if v == nil || v.Stat == nil || !rbdCloneIDValid(v.ImageID) || len(v.Snapshots) > 4 {
		return false
	}
	s := v.Stat
	return s.Size == plan.Size && s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Objects == plan.Size>>plan.Order && s.Prefix == "rbd_data."+v.ImageID &&
		v.Features == plan.Features|rbdCloneFeatureOperations && len(s.ParentName) <= 64
}

func rbdCloneSnapshotsAre(v *rbdCloneImageView, name string, size uint64) bool {
	return len(v.Snapshots) == 1 && v.Snapshots[0].Name == name && v.Snapshots[0].ID != 0 && v.Snapshots[0].Size == size && v.Snapshots[0].Namespace == 0
}

func rbdCloneObjectsValid(objects []rbdCloneObject, prefix string, count uint64, head, snap bool, objectSize uint64) (map[string]uint64, bool) {
	offsets := map[string]uint64{"head": 0, "snap": 0}
	if uint64(len(objects)) != count {
		return offsets, false
	}
	ok := true
	for i, o := range objects {
		ok = ok && o.Index == uint64(i) && o.Name == fmt.Sprintf("%s.%016x", prefix, i) && (o.Head != nil) == head && (o.Snap != nil) == snap
		for label, view := range map[string]*rbdBaseObjectView{"head": o.Head, "snap": o.Snap} {
			if view == nil {
				continue
			}
			ok = ok && view.Offset == offsets[label] && view.Size <= objectSize &&
				(view.Exists && view.Errno == 0 && len(view.SHA256) == 64 || !view.Exists && view.Errno == 2 && view.Size == 0 && view.SHA256 == "")
			offsets[label] += view.Size
		}
	}
	return offsets, ok
}

func rbdCloneCallsValid(calls []rbdWriteClassCall, objects map[string]string, plan [][3]string) bool {
	if len(calls) != len(plan) {
		return false
	}
	for i := range calls {
		if !rbdLockCallValid(&calls[i], objects[plan[i][0]], plan[i][1], plan[i][2]) {
			return false
		}
	}
	return true
}

func rbdCloneTraceValid(trace []rbdCloneTrace) bool {
	if len(trace) == 0 || len(trace) > rbdCloneTraceLimit {
		return false
	}
	for _, line := range trace {
		if line.Direction != "send" && line.Direction != "receive" || len(line.Message) > rbdCloneTraceWidth || !strings.HasPrefix(line.Message, "osd_op") ||
			!strings.Contains(line.Message, "rbd_data.") {
			return false
		}
	}
	return true
}

func rbdCloneCopyupTraceValid(lines []rbdCloneCopyupLine) bool {
	if len(lines) > rbdCloneCopyupLimit {
		return false
	}
	for _, line := range lines {
		if len(line.Message) > rbdCloneTraceWidth || !strings.HasPrefix(line.Message, rbdCloneCopyupPrefix) {
			return false
		}
	}
	return true
}

// decodeRBDCloneNative accepts exactly the shape of one admitted action for
// the owned plan. It checks structure only: which parent, overlap, object or
// trace state each observation must show after the Go steps is the test's
// judgment, so a Go failure is reported by the test instead of stopping the
// control loop.
func decodeRBDCloneNative(action, token string, request int64, plan rbdClonePlan, data []byte) (rbdCloneNative, error) {
	var result rbdCloneNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD clone output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdCloneNative{}, fmt.Errorf("native RBD clone output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Parent != plan.Parent ||
		result.Clone != plan.Clone || result.PoolID < 0 || result.NativeGlobalID == 0 {
		return rbdCloneNative{}, errors.New("native RBD clone output identity differs")
	}
	snapshot := rbdCloneHasSnapshot(action)
	c, p, ps, cs := result.CloneView, result.ParentView, result.ParentSnapshotView, result.CloneSnapshotView
	ok := rbdCloneViewValid(c, plan) && rbdCloneViewValid(p, plan) && rbdCloneViewValid(ps, plan) && c.ImageID != p.ImageID && ps.ImageID == p.ImageID &&
		p.Parent == nil && rbdCloneNoOverlap(p.Overlap) && ps.Parent == nil && rbdCloneNoOverlap(ps.Overlap) && rbdCloneSnapshotsAre(p, plan.ParentSnapshot, plan.Size) &&
		rbdCloneSnapshotsAre(ps, plan.ParentSnapshot, plan.Size) && (action == "flatten" || c.Parent != nil && c.Overlap != nil) &&
		(c.Parent == nil || c.Parent.ImageID == p.ImageID && c.Parent.SnapID == p.Snapshots[0].ID && c.Parent.SnapName == plan.ParentSnapshot && c.Parent.ImageName == plan.Parent) &&
		len(result.Children) <= 4 && (cs != nil) == snapshot
	if snapshot {
		// The clone snapshot keeps its parent link, also after a flatten
		// without deep-flatten.
		ok = ok && rbdCloneSnapshotsAre(c, plan.CloneSnapshot, plan.Size) && rbdCloneViewValid(cs, plan) && cs.ImageID == c.ImageID && rbdCloneSnapshotsAre(cs, plan.CloneSnapshot, plan.Size) &&
			cs.Parent != nil && cs.Overlap != nil && cs.Parent.ImageID == p.ImageID && cs.Parent.SnapID == p.Snapshots[0].ID
	} else {
		ok = ok && len(c.Snapshots) == 0
	}
	if !ok {
		return rbdCloneNative{}, fmt.Errorf("native RBD clone %s image views differ", action)
	}
	cloneObjects := map[string]string{"id": "rbd_id." + plan.Clone, "header": "rbd_header." + c.ImageID}
	parentObjects := map[string]string{"id": "rbd_id." + plan.Parent, "header": "rbd_header." + p.ImageID}
	ok = rbdCloneCallsValid(result.CloneCalls, cloneObjects, rbdCloneCalls) && rbdCloneCallsValid(result.ParentCalls, parentObjects, rbdCloneParentCalls) &&
		(result.CloneSnapshotCalls != nil) == snapshot
	if snapshot {
		var methods [][3]string
		for _, method := range rbdCloneSnapshotCalls {
			methods = append(methods, [3]string{"header", "rbd", method})
		}
		ok = ok && rbdCloneCallsValid(result.CloneSnapshotCalls, cloneObjects, methods)
	}
	objectSize := uint64(1) << plan.Order
	cloneOffsets, cloneOK := rbdCloneObjectsValid(result.CloneObjects, c.Stat.Prefix, c.Stat.Objects, true, snapshot, objectSize)
	parentOffsets, parentOK := rbdCloneObjectsValid(result.ParentObjects, p.Stat.Prefix, p.Stat.Objects, false, true, objectSize)
	ok = ok && cloneOK && parentOK && len(result.Blobs) == len(rbdCloneBlobKeys(action))
	lengths := map[string]uint64{"clone_head": plan.Size, "clone_objects_head": cloneOffsets["head"], "clone_snap": plan.Size, "clone_objects_snap": cloneOffsets["snap"],
		"parent_head": plan.Size, "parent_snap": plan.Size, "parent_objects_snap": parentOffsets["snap"]}
	for _, key := range rbdCloneBlobKeys(action) {
		blob, present := result.Blobs[key]
		ok = ok && present && blob.Name == rbdCloneBlobName(action, key) && blob.Length <= rbdBaseBlobLimit && blob.Length == lengths[key] && len(blob.SHA256) == 64
	}
	ok = ok && (result.Trace != nil) == rbdCloneTraced(action) && (result.Trace == nil || rbdCloneTraceValid(result.Trace)) && (action == "flatten") == (result.FlattenNS > 0) && result.FlattenNS >= 0 &&
		(result.CopyupTrace == nil || rbdCloneTraced(action) && rbdCloneCopyupTraceValid(result.CopyupTrace))
	switch action {
	case "setup":
		ok = ok && len(result.Writes) == len(plan.ParentPreWrites)+len(plan.ParentPostWrites) &&
			rbdLockWritesValid(result.Writes[:min(len(result.Writes), len(plan.ParentPreWrites))], "parent-pre", plan.ParentPreWrites) &&
			rbdLockWritesValid(result.Writes[min(len(result.Writes), len(plan.ParentPreWrites)):], "parent-post", plan.ParentPostWrites)
	case "write":
		ok = ok && rbdLockWritesValid(result.Writes, "native", plan.NativeWrites)
	default:
		ok = ok && result.Writes == nil
	}
	if !ok {
		return rbdCloneNative{}, fmt.Errorf("native RBD clone %s output shape differs", action)
	}
	return result, nil
}

// checkRBDCloneBlobs verifies that each published binary view of one action
// matches its receipt.
func checkRBDCloneBlobs(out, action string, blobs map[string]rbdBaseBlob) error {
	for _, key := range rbdCloneBlobKeys(action) {
		blob := blobs[key]
		name := rbdCloneBlobName(action, key)
		if blob.Name != name {
			return fmt.Errorf("native RBD clone binary view %s is misnamed", name)
		}
		file, err := os.Open(filepath.Join(out, name))
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(file, rbdBaseBlobLimit+1))
		file.Close()
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		if len(data) > rbdBaseBlobLimit || uint64(len(data)) != blob.Length || hex.EncodeToString(sum[:]) != blob.SHA256 {
			return fmt.Errorf("native RBD clone binary view %s differs from its receipt", name)
		}
	}
	return nil
}

// rbdCloneNativeTimeout bounds one native clone action: image creation,
// writes, a snapshot or a six-object flatten, then the observation.
const rbdCloneNativeTimeout = 90 * time.Second

func (c *fixtureCluster) rbdCloneRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdCloneNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.clonePlan)
	if err != nil {
		return nil, rbdCloneNative{}, err
	}
	argv := []string{"python3", "-c", rbdCloneNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: rbdCloneNativeTimeout, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD clone " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdCloneNative{}, errors.Join(err, fmt.Errorf("native RBD clone %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDCloneNative(action, x.token, request, x.clonePlan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDCloneRequest admits <id> <ownership token> <action> where the
// action is the next one of rbdCloneActions (next is its index).
func parseRBDCloneRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD clone request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD clone request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD clone request identity")
	}
	if !slices.Contains(rbdCloneActions, fields[2]) {
		return 0, "", errors.New("RBD clone action is not admitted")
	}
	if next >= len(rbdCloneActions) || fields[2] != rbdCloneActions[next] {
		return 0, "", errors.New("RBD clone action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdCloneTarget names the owned state one clone action changes.
func rbdCloneTarget(plan rbdClonePlan, action string) string {
	switch action {
	case "setup":
		return plan.Parent + "@" + plan.ParentSnapshot + "," + plan.Clone
	case "write":
		return plan.Clone + ":native-write"
	case "snapshot":
		return plan.Clone + "@" + plan.CloneSnapshot
	case "flatten":
		return plan.Clone + ":flatten"
	}
	return plan.Clone
}

// rbdCloneControl runs one native action of the closed clone sequence. The
// setup, write, snapshot and flatten actions are journaled as mutations, the
// two observations as read-only actions.
func (c *fixtureCluster) rbdCloneControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) || !x.clonePlan.owned(x.token) {
		return errors.New("invalid owned RBD clone control scope")
	}
	ctx, stop := context.WithTimeout(ctx, rbdCloneNativeTimeout+20*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD clone control requested after cleanup began")
	}
	id, action, err := parseRBDCloneRequest(fields, x.token, x.cloneLastRequestID, x.cloneNext)
	if err != nil {
		return err
	}
	x.cloneLastRequestID = id
	x.cloneNext++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.clonePlan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-clone-"+action+".json", result))
	}()
	m, err := c.rbdBaseBeginAction("native-clone-"+action, rbdCloneTarget(x.clonePlan, action), id, strings.HasPrefix(action, "observe-"))
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdCloneRunNative(ctx, action, id)
	result["native_process"] = process
	result["phase_diagnostic"] = readRBDClonePhase(out, x.token, action, id)
	if err != nil && rbdCloneTraced(action) {
		// A separate read-only process survives a fatal signal in the driver.
		// Its failure must never replace the original process/mutation outcome.
		captureCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result["partial_trace"] = c.captureRBDCloneTrace(captureCtx, x.token, action)
		cancel()
	}
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD clone action used another pool")
	}
	if err == nil {
		err = checkRBDCloneBlobs(out, action, native.Blobs)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

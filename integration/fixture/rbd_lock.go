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

// The RBD exclusive-lock acceptance reuses the RBD base scope (token, owned
// pool and both identities) and adds a third native librbd image to the owned
// pool, created only on the test's request, with the exclusive-lock,
// object-map and fast-diff features. The Go test then takes part in librbd's
// managed-lock protocol on that image through the explicit OSD API: it reads
// the native owner's lock, asks for the lock with a RequestLock notify, takes
// it in librbd's format when the native owner releases it, updates the
// object map and writes a data object while it holds the lock, and gives the
// lock back when native librbd asks for it. The closed control here only lets
// native librbd create the image, hold it open read-write through the
// protocol (one long action with proceed signals), run `rbd object-map check`
// and observe the image read-only afterwards. Every native action is
// journaled in rbd-base-owned.json before it is issued and is never reissued;
// the image goes with the owned pool at cleanup. The lock protocol, payload
// encodings and object-map decoding stay in the test-only consumer
// (integration/rbd_lock_test.go). Native behavior relied upon:
// librbd/ImageWatcher.cc (handle_payload for AcquiredLock, ReleasedLock and
// RequestLock), librbd/image_watcher/NotifyLockOwner.cc,
// librbd/ExclusiveLock.cc, librbd/exclusive_lock/AutomaticPolicy.cc and
// PostAcquireRequest.cc and PreReleaseRequest.cc, librbd/managed_lock
// (AcquireRequest, BreakRequest, GetLockerRequest, ReleaseRequest),
// librbd/object_map (LockRequest, UnlockRequest, UpdateRequest,
// RefreshRequest, DiffRequest), librbd/internal.cc (is_exclusive_lock_owner,
// lock_get_owners, list_lockers), librbd/operation/ObjectMapIterate.cc,
// tools/rbd/action/ObjectMap.cc, cls/rbd/cls_rbd.cc (object_map_load,
// object_map_update), common/bit_vector.hpp, pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource (LGPL-2.1 references; no code
// copied).

// rbdLockActions is the closed, ordered action sequence of the lock control.
// Each action runs at most once and only after its predecessors.
var rbdLockActions = []string{"setup", "lock", "observe-released"}

// rbdLockPhases is the fixed phase sequence of the lock action.
var rbdLockPhases = []string{"owner", "released", "go-owner", "reacquired", "closed"}

// rbdLockBlobViews names the binary views every native lock action
// publishes; the file of view v for action a is rbd-lock-<a>-<v>.bin.
var rbdLockBlobViews = map[string]string{
	"image_head":   "image-head",
	"objects_head": "objects-head",
}

func rbdLockBlobName(action, key string) string {
	return "rbd-lock-" + action + "-" + rbdLockBlobViews[key] + ".bin"
}

// rbdLockClassCalls is the fixed native exec list of every observation:
// get_id on the id object, rbd and lock methods on the header, then the lock
// and the object map on the head object-map object.
var rbdLockClassCalls = [][3]string{
	{"id", "rbd", "get_id"}, {"header", "rbd", "get_size"}, {"header", "rbd", "get_object_prefix"}, {"header", "rbd", "get_features"},
	{"header", "rbd", "get_snapcontext"}, {"header", "lock", "get_info"}, {"map", "lock", "get_info"}, {"map", "rbd", "object_map_load"},
}

// RBD_FEATURE_LAYERING | EXCLUSIVE_LOCK | OBJECT_MAP | FAST_DIFF.
const rbdLockFeatures = 1 | 4 | 8 | 16

// rbdLockWrongCookie is the lock cookie of the negative compound object-map
// update: librbd locks the object map with an empty cookie, so an assertion
// with this cookie never matches.
const rbdLockWrongCookie = "ceph-msgr-wrong-cookie"

// rbdLockSetIDNames are the id objects and image ids of librbd's id-object
// creation form (create(exclusive) then rbd.set_id) for the native setup
// action and for the Go test. Neither names an image: the pool directory
// does not list them, and they go with the owned pool.
func rbdLockSetIDNames(token, who string) (object, id string) {
	prefix := map[string]string{"native": "f00d", "go": "beef"}[who]
	return "rbd_id.ceph-msgr-setid-" + who + "-" + token, prefix + token[:8]
}

// rbdLockPlan is the fixed recipe of the lock image. SetupWrites are written
// by the setup action, OwnerWrites by the lock action's first native write
// (which takes the exclusive lock), GoWrites by the Go test while it holds
// the lock, and ReacquireWrites by the native write that requests the lock
// back from Go.
type rbdLockPlan struct {
	Pool            string          `json:"pool"`
	BaseImage       string          `json:"base_image"`
	WriteImage      string          `json:"write_image"`
	Image           string          `json:"image"`
	Order           int             `json:"order"`
	Size            uint64          `json:"size"`
	Features        uint64          `json:"features"`
	SetupWrites     []rbdBaseExtent `json:"setup_writes"`
	OwnerWrites     []rbdBaseExtent `json:"owner_writes"`
	GoWrites        []rbdBaseExtent `json:"go_writes"`
	ReacquireWrites []rbdBaseExtent `json:"reacquire_writes"`
}

// rbdLockPlanFor places a 320 KiB, five-object image without snapshots:
// object 0 written whole at setup, object 1 by the native owner, object 2 by
// Go while it holds the lock, object 3 by the native write that requests the
// lock back; object 4 is never written, so the object map keeps one
// nonexistent entry.
func rbdLockPlanFor(token string) rbdLockPlan {
	const k = 1 << 10
	return rbdLockPlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, WriteImage: "ceph-msgr-write-" + token, Image: "ceph-msgr-lock-" + token,
		Order: 16, Size: 320 * k, Features: rbdLockFeatures,
		SetupWrites:     []rbdBaseExtent{{0, 64 * k}},
		OwnerWrites:     []rbdBaseExtent{{64 * k, 16 * k}},
		GoWrites:        []rbdBaseExtent{{136 * k, 8 * k}},
		ReacquireWrites: []rbdBaseExtent{{200 * k, 8 * k}}}
}

func (p rbdLockPlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.WriteImage == "ceph-msgr-write-"+token &&
		p.Image == "ceph-msgr-lock-"+token && p.Order == 16 && p.Size == 320<<10 && p.Features == rbdLockFeatures &&
		len(p.SetupWrites) > 0 && len(p.OwnerWrites) > 0 && len(p.GoWrites) > 0 && len(p.ReacquireWrites) > 0
}

// The fixed native lock driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache and with error-level stderr only, with its default exclusive-lock
// policy (rbd_auto_exclusive_lock_until_manual_request: the automatic policy
// releases the lock to a requester). A read-write open registers librbd's
// header watch and takes the lock on the first write; read-only opens
// register no watch, take no lock and refresh the header, so lockers and
// flags of each phase come from a fresh read-only open (the read-write
// instance caches them). Sources: pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource; independent driver, no code
// copied.
const rbdLockNativePython = `
import hashlib, json, os, struct, subprocess, sys, time
import rados, rbd
identity, keyring, action, token, request_id, plan_json = sys.argv[1:7]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'lock', 'observe-released')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('write_image') != 'ceph-msgr-write-' + token or plan.get('image') != 'ceph-msgr-lock-' + token
        or plan.get('features') != 29 or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD lock driver scope is not owned')
OUT = '/out'
CONF = '/tmp/ceph-msgr-test/ceph.conf'
NOSNAP = 0xfffffffffffffffe
# cls_lock_get_info_op{name="rbd_lock"}: ENCODE_START(1, 1) and a string.
LOCK_INFO = b'\x01\x01' + struct.pack('<II', 12, 8) + b'rbd_lock'
RETURNVEC = 1024  # LIBRADOS_OPERATION_RETURNVEC
WRONG_COOKIE = 'ceph-msgr-wrong-cookie'
SET_ID = {'native': ('rbd_id.ceph-msgr-setid-native-' + token, 'f00d' + token[:8]),
          'go': ('rbd_id.ceph-msgr-setid-go-' + token, 'beef' + token[:8])}
pool, name = plan['pool'], plan['image']
PREFIX = 'rbd-lock-%s-' % action

def lstr(value):
    data = value.encode()
    return struct.pack('<I', len(data)) + data

# cls_lock_assert_op: ENCODE_START(1, 1), name, u8 type (1 exclusive),
# cookie and tag; cls_rbd object_map_update: start, end, new state and an
# absent boost::optional current state.
def assert_locked_input(cookie):
    body = lstr('rbd_lock') + b'\x01' + lstr(cookie) + lstr('')
    return b'\x01\x01' + struct.pack('<I', len(body)) + body

def map_update_input(start, end, state):
    return struct.pack('<QQB', start, end, state) + b'\x00'

def write_vector(ioctx, oid, build):
    # One librados write op with RETURNVEC and the current time; python
    # rados exposes only the aggregate (no per-op prval).
    with rados.WriteOpCtx() as op:
        build(op)
        try:
            ioctx.operate_write_op(op, oid, mtime=int(time.time()), flags=RETURNVEC)
        except rados.Error as error:
            if not isinstance(error.errno, int) or error.errno <= 0:
                raise
            return -error.errno
    return 0

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-lock:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
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

def raw_object(ioctx, oid):
    try:
        length, _ = ioctx.stat(oid)
        data = ioctx.read(oid, length, 0) if length else b''
    except rados.ObjectNotFound:
        return {'object': oid, 'exists': False, 'hex': ''}
    if len(data) != length or length > 4096:
        raise ValueError('object map object read differs from its stat or exceeds its bound')
    return {'object': oid, 'exists': True, 'hex': data.hex()}

def versioned(ioctx, oid):
    entry = raw_object(ioctx, oid)
    entry['version'] = ioctx.get_last_version() if entry['exists'] else 0
    return entry

def set_id_compound(ioctx, who):
    # librbd CreateRequest::create_id_object: create(exclusive) then
    # rbd.set_id(id) in one write; sent twice, the second must find the object.
    oid, image_id = SET_ID[who]
    data = lstr(image_id)
    def build(op):
        op.new(rados.LIBRADOS_CREATE_EXCLUSIVE)
        op.execute('rbd', 'set_id', data)
    first = write_vector(ioctx, oid, build)
    first_view = versioned(ioctx, oid)
    second = write_vector(ioctx, oid, build)
    return {'object': oid, 'image_id': image_id, 'input_hex': data.hex(), 'first': first, 'first_view': first_view,
            'second': second, 'second_view': versioned(ioctx, oid), 'get_id': call(ioctx, oid, 'rbd', 'get_id', b'')}

def negative_compound(ioctx, oid):
    # librbd's object-map update vector (UpdateRequest) asserting a cookie
    # that is not the instance's object-map lock cookie (empty).
    inputs = [assert_locked_input(WRONG_COOKIE), map_update_input(4, 5, 1)]
    before = raw_object(ioctx, oid)
    def build(op):
        op.execute('lock', 'assert_locked', inputs[0])
        op.execute('rbd', 'object_map_update', inputs[1])
    code = write_vector(ioctx, oid, build)
    return {'object': oid, 'inputs_hex': [i.hex() for i in inputs], 'code': code, 'map_before': before, 'map_after': raw_object(ioctx, oid)}

def class_calls(ioctx, image_id):
    header, omap = 'rbd_header.' + image_id, 'rbd_object_map.' + image_id
    return [call(ioctx, 'rbd_id.' + name, 'rbd', 'get_id', b''),
            call(ioctx, header, 'rbd', 'get_size', struct.pack('<Q', NOSNAP)),
            call(ioctx, header, 'rbd', 'get_object_prefix', b''),
            call(ioctx, header, 'rbd', 'get_features', struct.pack('<QB', NOSNAP, 0)),
            call(ioctx, header, 'rbd', 'get_snapcontext', b''),
            call(ioctx, header, 'lock', 'get_info', LOCK_INFO),
            call(ioctx, omap, 'lock', 'get_info', LOCK_INFO),
            call(ioctx, omap, 'rbd', 'object_map_load', b'')]

def lock_view(ioctx, image_id):
    header, omap = 'rbd_header.' + image_id, 'rbd_object_map.' + image_id
    return {'header_lock': call(ioctx, header, 'lock', 'get_info', LOCK_INFO), 'map_lock': call(ioctx, omap, 'lock', 'get_info', LOCK_INFO),
            'map_raw': raw_object(ioctx, omap)}

def watchers(image):
    return [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]

def lockers(image):
    # librbd reports an image without lockers as an empty list.
    value = image.list_lockers()
    if not value:
        return {'empty': True, 'tag': '', 'exclusive': False, 'lockers': []}
    return {'empty': False, 'tag': text(value['tag']), 'exclusive': bool(value['exclusive']),
            'lockers': [{'client': text(c), 'cookie': text(k), 'addr': text(a)} for c, k, a in value['lockers']]}

def diff(image, size):
    # Whole-object diff from the beginning of time: with fast-diff valid,
    # librbd answers it from the on-disk head object map (object_map_load).
    extents = []
    def collect(offset, length, exists):
        extents.append({'offset': offset, 'length': length, 'exists': bool(exists)})
        if len(extents) > 64:
            raise ValueError('owned RBD image diff has too many extents')
        return 0
    image.diff_iterate(0, size, None, collect, include_parent=False, whole_object=True)
    return extents

def fresh(ioctx):
    # A read-only open registers no watch, takes no lock and refreshes the
    # header; the read-write instance keeps lockers and flags cached.
    with rbd.Image(ioctx, name, read_only=True) as image:
        size = image.size()
        return {'lockers': lockers(image), 'flags': image.flags(), 'size': size, 'diff': diff(image, size)}

def object_views(ioctx, prefix, count):
    objects, blob = [], bytearray()
    for index in range(count):
        oid = '%s.%016x' % (prefix, index)
        entry = {'index': index, 'name': oid}
        try:
            length, _ = ioctx.stat(oid)
            data = ioctx.read(oid, length, 0) if length else b''
        except rados.ObjectNotFound:
            entry['head'] = {'exists': False, 'errno': 2, 'size': 0, 'offset': len(blob), 'sha256': ''}
            objects.append(entry)
            continue
        if len(data) != length:
            raise ValueError('native object read is shorter than its stat')
        entry['head'] = {'exists': True, 'errno': 0, 'size': length, 'offset': len(blob), 'sha256': hashlib.sha256(data).hexdigest()}
        blob += data
        objects.append(entry)
    return objects, bytes(blob)

def observe(ioctx, out):
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        out['image_id'] = text(image.id())
        # image_info_t's fixed parent_name array reaches Python as bytes.
        out['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix', 'parent_pool', 'parent_name')}
        out['features'], out['op_features'], out['flags'] = image.features(), image.op_features(), image.flags()
        out['stripe_unit'], out['stripe_count'], out['data_pool_id'] = image.stripe_unit(), image.stripe_count(), image.data_pool_id()
        out['snapshots'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]
        out['lockers'], out['watchers'] = lockers(image), watchers(image)
        out['diff'] = diff(image, stat['size'])
        head = image.read(0, stat['size'])
    out['header_object'], out['id_object'], out['map_object'] = 'rbd_header.' + out['image_id'], 'rbd_id.' + name, 'rbd_object_map.' + out['image_id']
    out['class_calls'] = class_calls(ioctx, out['image_id'])
    out['map_raw'] = raw_object(ioctx, out['map_object'])
    objects, view = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'])
    out['objects'] = objects
    out['blobs'] = {'image_head': publish(PREFIX + 'image-head.bin', head), 'objects_head': publish(PREFIX + 'objects-head.bin', view)}

def write(image, phase, extent):
    data = pattern(phase, extent['offset'], extent['length'])
    started = time.monotonic_ns()
    written = image.write(data, extent['offset'])
    elapsed = time.monotonic_ns() - started
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest(), 'elapsed_ns': elapsed}

def read_go(image):
    # librbd reads need no exclusive lock: the native instance reads the Go
    # extents whoever holds the lock.
    return [{'offset': e['offset'], 'length': e['length'], 'sha256': hashlib.sha256(image.read(e['offset'], e['length'])).hexdigest()} for e in plan['go_writes']]

def wait_proceed(step):
    path, expected = os.path.join(OUT, 'rbd-lock-proceed-%d' % step), '%s %s %d' % (token, request_id, step)
    started, deadline = time.monotonic_ns(), time.monotonic() + 30
    while True:
        try:
            with open(path) as f:
                value = f.read(256).strip()
        except FileNotFoundError:
            value = None
        if value is not None:
            if value != expected:
                raise ValueError('proceed signal belongs to another request')
            os.remove(path)
            return time.monotonic_ns() - started
        if time.monotonic() > deadline:
            raise TimeoutError('proceed signal %d missing' % step)
        time.sleep(0.02)

def object_map_check():
    # rbd object-map check (librbd Image::check_object_map, C++ only) takes
    # the exclusive lock, compares every object with the head object map and
    # invalidates the map on an object the map does not account for.
    argv = ['rbd', '--conf', CONF, '--id', identity[len('client.'):], '--keyring', keyring, '--rbd_cache=false', '--log_to_stderr=false', '--err_to_stderr=true',
            'object-map', 'check', '--no-progress', pool + '/' + name]
    started = time.monotonic_ns()
    done = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
    elapsed = time.monotonic_ns() - started
    if len(done.stdout) > 4096 or len(done.stderr) > 4096:
        raise ValueError('rbd object-map check output exceeds its bound')
    return {'argv': argv[7:], 'returncode': done.returncode, 'stdout': text(done.stdout), 'stderr': text(done.stderr), 'elapsed_ns': elapsed}

cluster = rados.Rados(conffile=CONF, rados_id=identity[len('client.'):],
                      conf={'keyring': keyring, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'image': name, 'native_global_id': cluster.get_instance_id()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            listed = sorted(text(i) for i in rbd.RBD().list(ioctx))
            if plan['base_image'] not in listed or any(i not in (plan['base_image'], plan['write_image']) for i in listed):
                raise ValueError('owned RBD pool holds an unplanned image')
            rbd.RBD().create(ioctx, name, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            with rbd.Image(ioctx, name) as image:
                out['writes'] = [write(image, 'setup', e) for e in plan['setup_writes']]
                image.flush()
            out['set_id'] = set_id_compound(ioctx, 'native')
        elif action == 'lock':
            state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'phases': []}
            image = rbd.Image(ioctx, name)
            try:
                image_id = text(image.id())
                def phase(label, **extra):
                    owners = [{'mode': o['mode'], 'owner': text(o['owner'])} for o in image.lock_get_owners()]
                    entry = dict(phase=label, is_owner=image.is_exclusive_lock_owner(), owners=owners, watchers=watchers(image), **extra)
                    entry.update(fresh(ioctx))
                    entry.update(lock_view(ioctx, image_id))
                    state['phases'].append(entry)
                    state['phase'] = label
                    publish('rbd-lock-state.json', json.dumps(state, sort_keys=True).encode())
                # The first write takes the exclusive lock (automatic policy).
                owner = [write(image, 'owner', e) for e in plan['owner_writes']]
                image.flush()
                phase('owner', writes=owner, negative_compound=negative_compound(ioctx, 'rbd_object_map.' + image_id))
                phase('released', wait_ns=wait_proceed(1))
                phase('go-owner', wait_ns=wait_proceed(2), go_extents=read_go(image))
                waited = wait_proceed(3)
                started = time.monotonic_ns()
                reacquire = [write(image, 'reacquire', e) for e in plan['reacquire_writes']]
                image.flush()
                phase('reacquired', wait_ns=waited, writes=reacquire, write_ns=time.monotonic_ns() - started, go_extents=read_go(image))
                waited = wait_proceed(4)
            except BaseException:
                image.close()
                raise
            started = time.monotonic_ns()
            image.close()
            closed = dict(phase='closed', wait_ns=waited, close_ns=time.monotonic_ns() - started, **lock_view(ioctx, image_id))
            closed.update(fresh(ioctx))
            state['phases'].append(closed)
            state['phase'] = 'closed'
            publish('rbd-lock-state.json', json.dumps(state, sort_keys=True).encode())
            out['phases'] = state['phases']
            out['check'] = object_map_check()
        elif action == 'observe-released':
            oid = SET_ID['go'][0]
            out['set_id_go'] = {'object': oid, 'view': versioned(ioctx, oid), 'get_id': call(ioctx, oid, 'rbd', 'get_id', b'')}
        observe(ioctx, out)
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
print(json.dumps(out, sort_keys=True))
`

type rbdLockWrite struct {
	Phase     string `json:"phase"`
	Offset    uint64 `json:"offset"`
	Length    uint64 `json:"length"`
	Written   uint64 `json:"written"`
	SHA256    string `json:"sha256"`
	ElapsedNS int64  `json:"elapsed_ns"`
}

type rbdLockRawObject struct {
	Object string `json:"object"`
	Exists bool   `json:"exists"`
	Hex    string `json:"hex"`
}

type rbdLockDiffExtent struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Exists bool   `json:"exists"`
}

type rbdLockOwner struct {
	Mode  int    `json:"mode"`
	Owner string `json:"owner"`
}

type rbdLockExtentSum struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type rbdLockObjectView struct {
	Index uint64            `json:"index"`
	Name  string            `json:"name"`
	Head  rbdBaseObjectView `json:"head"`
}

// rbdLockPhase is one published phase of the lock action. The closed phase
// carries no read-write image view (owner, owners, watchers).
type rbdLockPhase struct {
	Phase      string              `json:"phase"`
	IsOwner    bool                `json:"is_owner"`
	Owners     []rbdLockOwner      `json:"owners"`
	Watchers   []rbdBaseWatcher    `json:"watchers"`
	Lockers    *rbdWriteLockers    `json:"lockers"`
	Flags      uint64              `json:"flags"`
	Size       uint64              `json:"size"`
	Diff       []rbdLockDiffExtent `json:"diff"`
	HeaderLock *rbdWriteClassCall  `json:"header_lock"`
	MapLock    *rbdWriteClassCall  `json:"map_lock"`
	MapRaw     *rbdLockRawObject   `json:"map_raw"`
	WaitNS     int64               `json:"wait_ns,omitempty"`
	Writes     []rbdLockWrite      `json:"writes,omitempty"`
	WriteNS    int64               `json:"write_ns,omitempty"`
	GoExtents  []rbdLockExtentSum  `json:"go_extents,omitempty"`
	CloseNS    int64               `json:"close_ns,omitempty"`
	Negative   *rbdLockCompound    `json:"negative_compound,omitempty"`
}

// rbdLockVersioned is a raw object read with the object's user version.
type rbdLockVersioned struct {
	Object  string `json:"object"`
	Exists  bool   `json:"exists"`
	Hex     string `json:"hex"`
	Version uint64 `json:"version"`
}

// rbdLockSetID is the native setup's librbd-form id-object creation, sent
// twice, with the object after each and a native get_id.
type rbdLockSetID struct {
	Object     string             `json:"object"`
	ImageID    string             `json:"image_id"`
	InputHex   string             `json:"input_hex"`
	First      int                `json:"first"`
	FirstView  *rbdLockVersioned  `json:"first_view"`
	Second     int                `json:"second"`
	SecondView *rbdLockVersioned  `json:"second_view"`
	GetID      *rbdWriteClassCall `json:"get_id"`
}

// rbdLockSetIDGo is the read-only native observation of the Go id object.
type rbdLockSetIDGo struct {
	Object string             `json:"object"`
	View   *rbdLockVersioned  `json:"view"`
	GetID  *rbdWriteClassCall `json:"get_id"`
}

// rbdLockCompound is the owner phase's native object-map update vector with
// a non-matching lock cookie and the object map around it.
type rbdLockCompound struct {
	Object    string            `json:"object"`
	InputsHex []string          `json:"inputs_hex"`
	Code      int               `json:"code"`
	MapBefore *rbdLockRawObject `json:"map_before"`
	MapAfter  *rbdLockRawObject `json:"map_after"`
}

type rbdLockCheck struct {
	Argv       []string `json:"argv"`
	ReturnCode int      `json:"returncode"`
	Stdout     string   `json:"stdout"`
	Stderr     string   `json:"stderr"`
	ElapsedNS  int64    `json:"elapsed_ns"`
}

// rbdLockNative is one complete lock-driver output; unknown fields are
// refused.
type rbdLockNative struct {
	Action         string                 `json:"action"`
	Token          string                 `json:"token"`
	RequestID      int64                  `json:"request_id"`
	Pool           string                 `json:"pool"`
	PoolID         int64                  `json:"pool_id"`
	Image          string                 `json:"image"`
	NativeGlobalID uint64                 `json:"native_global_id"`
	ImageID        string                 `json:"image_id"`
	HeaderObject   string                 `json:"header_object"`
	IDObject       string                 `json:"id_object"`
	MapObject      string                 `json:"map_object"`
	Stat           *rbdBaseStat           `json:"stat"`
	Features       uint64                 `json:"features"`
	OpFeatures     uint64                 `json:"op_features"`
	Flags          uint64                 `json:"flags"`
	StripeUnit     uint64                 `json:"stripe_unit"`
	StripeCount    uint64                 `json:"stripe_count"`
	DataPoolID     int64                  `json:"data_pool_id"`
	Snapshots      []rbdBaseSnapshot      `json:"snapshots"`
	Lockers        *rbdWriteLockers       `json:"lockers"`
	Watchers       []rbdBaseWatcher       `json:"watchers"`
	Diff           []rbdLockDiffExtent    `json:"diff"`
	ClassCalls     []rbdWriteClassCall    `json:"class_calls"`
	MapRaw         *rbdLockRawObject      `json:"map_raw"`
	Writes         []rbdLockWrite         `json:"writes,omitempty"`
	Objects        []rbdLockObjectView    `json:"objects"`
	Blobs          map[string]rbdBaseBlob `json:"blobs"`
	Phases         []rbdLockPhase         `json:"phases,omitempty"`
	Check          *rbdLockCheck          `json:"check,omitempty"`
	SetID          *rbdLockSetID          `json:"set_id,omitempty"`
	SetIDGo        *rbdLockSetIDGo        `json:"set_id_go,omitempty"`
}

func rbdLockCallValid(call *rbdWriteClassCall, object, class, method string) bool {
	if call == nil {
		return false
	}
	_, inErr := hex.DecodeString(call.InputHex)
	_, outErr := hex.DecodeString(call.OutputHex)
	return call.Object == object && call.Class == class && call.Method == method && inErr == nil && outErr == nil && call.Code <= 0 && len(call.OutputHex) <= 16384
}

func rbdLockRawValid(raw *rbdLockRawObject, object string) bool {
	if raw == nil {
		return false
	}
	_, err := hex.DecodeString(raw.Hex)
	return raw.Object == object && err == nil && len(raw.Hex) <= 8192 && raw.Exists == (raw.Hex != "")
}

func rbdLockVersionedValid(v *rbdLockVersioned, object string) bool {
	return v != nil && rbdLockRawValid(&rbdLockRawObject{Object: v.Object, Exists: v.Exists, Hex: v.Hex}, object) && v.Exists == (v.Version != 0)
}

func rbdLockSetIDValid(s *rbdLockSetID, token string) bool {
	if s == nil {
		return false
	}
	object, id := rbdLockSetIDNames(token, "native")
	_, err := hex.DecodeString(s.InputHex)
	return s.Object == object && s.ImageID == id && err == nil && s.First <= 0 && s.Second <= 0 &&
		rbdLockVersionedValid(s.FirstView, object) && rbdLockVersionedValid(s.SecondView, object) && rbdLockCallValid(s.GetID, object, "rbd", "get_id")
}

func rbdLockSetIDGoValid(s *rbdLockSetIDGo, token string) bool {
	object, _ := rbdLockSetIDNames(token, "go")
	return s != nil && s.Object == object && rbdLockVersionedValid(s.View, object) && rbdLockCallValid(s.GetID, object, "rbd", "get_id")
}

func rbdLockCompoundValid(c *rbdLockCompound, object string) bool {
	if c == nil || c.Object != object || len(c.InputsHex) != 2 || c.Code > 0 {
		return false
	}
	for _, input := range c.InputsHex {
		if _, err := hex.DecodeString(input); err != nil || len(input) > 512 {
			return false
		}
	}
	return rbdLockRawValid(c.MapBefore, object) && rbdLockRawValid(c.MapAfter, object)
}

func rbdLockLockersValid(l *rbdWriteLockers) bool {
	return l != nil && len(l.Lockers) <= 16 && l.Empty == (len(l.Lockers) == 0) && (!l.Empty || l.Tag == "" && !l.Exclusive)
}

func rbdLockDiffValid(extents []rbdLockDiffExtent, size uint64) bool {
	end := uint64(0)
	for _, e := range extents {
		if e.Length == 0 || e.Offset < end || e.Offset+e.Length > size {
			return false
		}
		end = e.Offset + e.Length
	}
	return len(extents) <= 64
}

func rbdLockWritesValid(writes []rbdLockWrite, phase string, plan []rbdBaseExtent) bool {
	if len(writes) != len(plan) {
		return false
	}
	for i, w := range writes {
		if w.Phase != phase || w.Offset != plan[i].Offset || w.Length != plan[i].Length || w.Written != plan[i].Length || len(w.SHA256) != 64 || w.ElapsedNS < 0 {
			return false
		}
	}
	return true
}

func rbdLockExtentSumsValid(sums []rbdLockExtentSum, plan []rbdBaseExtent) bool {
	if len(sums) != len(plan) {
		return false
	}
	for i, s := range sums {
		if s.Offset != plan[i].Offset || s.Length != plan[i].Length || len(s.SHA256) != 64 {
			return false
		}
	}
	return true
}

// decodeRBDLockNative accepts exactly the shape of one admitted action for
// the owned plan. It checks structure only: which owner, lock or object-map
// state each phase must show after the Go steps is the test's judgment, so a
// Go failure is reported by the test instead of stopping the control loop.
func decodeRBDLockNative(action, token string, request int64, plan rbdLockPlan, data []byte) (rbdLockNative, error) {
	var result rbdLockNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD lock output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdLockNative{}, fmt.Errorf("native RBD lock output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Image != plan.Image ||
		result.PoolID < 0 || result.NativeGlobalID == 0 {
		return rbdLockNative{}, errors.New("native RBD lock output identity differs")
	}
	s := result.Stat
	ok := s != nil && result.ImageID != "" && !strings.ContainsAny(result.ImageID, "./ \x00") && result.HeaderObject == "rbd_header."+result.ImageID &&
		result.IDObject == "rbd_id."+plan.Image && result.MapObject == "rbd_object_map."+result.ImageID &&
		s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Prefix == "rbd_data."+result.ImageID && s.ParentPool == ^uint64(0) && s.ParentName == "" &&
		s.Size == plan.Size && s.Objects == plan.Size>>plan.Order && result.Features == plan.Features && result.StripeUnit == s.ObjectSize && result.StripeCount == 1 &&
		len(result.Snapshots) == 0 && rbdLockLockersValid(result.Lockers) && rbdWriteWatchersValid(result.Watchers) && rbdLockDiffValid(result.Diff, plan.Size) &&
		len(result.ClassCalls) == len(rbdLockClassCalls) && rbdLockRawValid(result.MapRaw, result.MapObject) && len(result.Objects) == int(s.Objects) &&
		len(result.Blobs) == len(rbdLockBlobViews)
	objects := map[string]string{"id": result.IDObject, "header": result.HeaderObject, "map": result.MapObject}
	for i := range result.ClassCalls {
		c := rbdLockClassCalls[i]
		ok = ok && rbdLockCallValid(&result.ClassCalls[i], objects[c[0]], c[1], c[2])
	}
	offset := uint64(0)
	for i, o := range result.Objects {
		view := o.Head
		ok = ok && o.Index == uint64(i) && o.Name == fmt.Sprintf("%s.%016x", s.Prefix, i) && view.Offset == offset && view.Size <= s.ObjectSize &&
			(view.Exists && view.Errno == 0 && len(view.SHA256) == 64 || !view.Exists && view.Errno == 2 && view.Size == 0 && view.SHA256 == "")
		offset += view.Size
	}
	for key := range rbdLockBlobViews {
		blob, present := result.Blobs[key]
		ok = ok && present && blob.Name == rbdLockBlobName(action, key) && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
	}
	ok = ok && result.Blobs["image_head"].Length == s.Size && result.Blobs["objects_head"].Length == offset
	switch action {
	case "setup":
		ok = ok && rbdLockWritesValid(result.Writes, "setup", plan.SetupWrites) && result.Phases == nil && result.Check == nil && len(result.Watchers) == 0 &&
			rbdLockSetIDValid(result.SetID, token) && result.SetIDGo == nil
	case "observe-released":
		ok = ok && result.Writes == nil && result.Phases == nil && result.Check == nil && result.SetID == nil && rbdLockSetIDGoValid(result.SetIDGo, token)
	case "lock":
		c := result.Check
		ok = ok && result.Writes == nil && result.SetID == nil && result.SetIDGo == nil && len(result.Phases) == len(rbdLockPhases) && c != nil && c.ElapsedNS > 0 && len(c.Stdout) <= 4096 && len(c.Stderr) <= 4096 &&
			slices.Equal(c.Argv, []string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "object-map", "check", "--no-progress", plan.Pool + "/" + plan.Image})
		for i, label := range rbdLockPhases {
			if i >= len(result.Phases) {
				break
			}
			p := result.Phases[i]
			closed := label == "closed"
			ok = ok && p.Phase == label && rbdLockLockersValid(p.Lockers) && p.Size == plan.Size && rbdLockDiffValid(p.Diff, plan.Size) &&
				rbdLockCallValid(p.HeaderLock, result.HeaderObject, "lock", "get_info") && rbdLockCallValid(p.MapLock, result.MapObject, "lock", "get_info") &&
				rbdLockRawValid(p.MapRaw, result.MapObject) && rbdWriteWatchersValid(p.Watchers) && len(p.Owners) <= 1 &&
				(i == 0) == (p.WaitNS == 0) && p.WaitNS >= 0 && closed == (p.CloseNS > 0) && (!closed || !p.IsOwner && p.Owners == nil && p.Watchers == nil)
			ok = ok && (label == "owner") == (p.Negative != nil)
			switch label {
			case "owner":
				ok = ok && rbdLockWritesValid(p.Writes, "owner", plan.OwnerWrites) && p.WriteNS == 0 && p.GoExtents == nil && rbdLockCompoundValid(p.Negative, result.MapObject)
			case "reacquired":
				ok = ok && rbdLockWritesValid(p.Writes, "reacquire", plan.ReacquireWrites) && p.WriteNS > 0 && rbdLockExtentSumsValid(p.GoExtents, plan.GoWrites)
			case "go-owner":
				ok = ok && p.Writes == nil && p.WriteNS == 0 && rbdLockExtentSumsValid(p.GoExtents, plan.GoWrites)
			default:
				ok = ok && p.Writes == nil && p.WriteNS == 0 && p.GoExtents == nil
			}
		}
	default:
		ok = false
	}
	if !ok {
		return rbdLockNative{}, fmt.Errorf("native RBD lock %s output shape differs", action)
	}
	return result, nil
}

// checkRBDLockBlobs verifies that each published binary view of one action
// matches its receipt.
func checkRBDLockBlobs(out, action string, blobs map[string]rbdBaseBlob) error {
	for key := range rbdLockBlobViews {
		blob := blobs[key]
		name := rbdLockBlobName(action, key)
		if blob.Name != name {
			return fmt.Errorf("native RBD lock binary view %s is misnamed", name)
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
			return fmt.Errorf("native RBD lock binary view %s differs from its receipt", name)
		}
	}
	return nil
}

// rbdLockNativeTimeout bounds one native lock action: the lock action waits
// for four Go steps of at most 30 s each, then runs the object-map check
// (at most 60 s) and the observation.
func rbdLockNativeTimeout(action string) time.Duration {
	if action == "lock" {
		return 220 * time.Second
	}
	return 60 * time.Second
}

func (c *fixtureCluster) rbdLockRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdLockNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.lockPlan)
	if err != nil {
		return nil, rbdLockNative{}, err
	}
	argv := []string{"python3", "-c", rbdLockNativePython, "client.rbd-base-native-" + x.token, "/out/rbd-base-native.keyring", action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: rbdLockNativeTimeout(action), MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD lock " + action})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdLockNative{}, errors.Join(err, fmt.Errorf("native RBD lock %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDLockNative(action, x.token, request, x.lockPlan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDLockRequest admits <id> <ownership token> <action> where the action
// is the next one of rbdLockActions (next is its index).
func parseRBDLockRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD lock request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD lock request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD lock request identity")
	}
	if !slices.Contains(rbdLockActions, fields[2]) {
		return 0, "", errors.New("RBD lock action is not admitted")
	}
	if next >= len(rbdLockActions) || fields[2] != rbdLockActions[next] {
		return 0, "", errors.New("RBD lock action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdLockControl runs one native action of the closed lock sequence. The
// setup and lock actions are journaled as mutations (image creation and
// writes; a header watch, lock traffic, writes and the object-map check),
// the final observation as a read-only action.
func (c *fixtureCluster) rbdLockControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) || !x.lockPlan.owned(x.token) {
		return errors.New("invalid owned RBD lock control scope")
	}
	ctx, stop := context.WithTimeout(ctx, rbdLockNativeTimeout("lock")+20*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD lock control requested after cleanup began")
	}
	id, action, err := parseRBDLockRequest(fields, x.token, x.lockLastRequestID, x.lockNext)
	if err != nil {
		return err
	}
	x.lockLastRequestID = id
	x.lockNext++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.lockPlan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-lock-"+action+".json", result))
	}()
	target := x.lockPlan.Image
	if action == "lock" {
		target += ":header-watch,exclusive-lock,object-map-check"
	}
	m, err := c.rbdBaseBeginAction("native-lock-"+action, target, id, action == "observe-released")
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdLockRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD lock action used another pool")
	}
	if err == nil {
		err = checkRBDLockBlobs(out, action, native.Blobs)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

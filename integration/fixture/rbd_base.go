package main

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// The RBD base acceptance owns one pool on the single-OSD legacy-v6 fixture
// and one native librbd image inside it. Native librbd prepares the image
// (format 2, layering only, 64 KiB objects): data across several objects with
// sparse holes, one user snapshot and different data after it. The Go test
// then reads the image's header through rbd object-class CALLs and its data
// objects through the explicit OSD API, and compares both with native librbd
// and librados observations. One closed control action keeps the image open
// natively (librbd's header watch) while the test lists the header watchers,
// registers its own watch and acknowledges one librbd header-update notify of
// a native resize (and leaves a second one unacknowledged). Every native
// mutation is journaled before it is issued and is never reissued; cleanup
// removes the owned pool (with its image and CRUSH rule) and both identities.
// The image layout, header decoding and object naming stay in the test-only
// consumer (integration/rbd_base_test.go). Native behavior relied upon:
// cls/rbd/cls_rbd.cc, librbd/Operations.cc (C_NotifyUpdate),
// librbd/ImageWatcher.cc, librbd/watcher/Notifier.cc,
// librbd/WatchNotifyTypes.cc, pybind/rbd/rbd.pyx and pybind/rados/rados.pyx
// at mdsFreshSource (LGPL-2.1 references; no code copied).

const rbdBaseSource = mdsFreshSource
const rbdBaseJSONLimit = 256 << 10

// rbdBaseBlobLimit bounds each native binary view: the 512 KiB image or its
// eight objects concatenated.
const rbdBaseBlobLimit = 1 << 20

// Native binary views published by the setup action into the control
// directory. The Go test compares its reads with these bytes.
var rbdBaseBlobNames = map[string]string{
	"image_head":   "rbd-base-image-head.bin",
	"image_snap":   "rbd-base-image-snap.bin",
	"objects_head": "rbd-base-objects-head.bin",
	"objects_snap": "rbd-base-objects-snap.bin",
}

type rbdBaseExtent struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// rbdBasePlan is the fixed image recipe of one run. Every name is derived from
// the ownership token; the native driver re-checks them.
type rbdBasePlan struct {
	Pool        string          `json:"pool"`
	Image       string          `json:"image"`
	Snapshot    string          `json:"snapshot"`
	Order       int             `json:"order"`
	Size        uint64          `json:"size"`
	Features    uint64          `json:"features"`
	PreWrites   []rbdBaseExtent `json:"pre_snapshot_writes"`
	PostWrites  []rbdBaseExtent `json:"post_snapshot_writes"`
	ResizeAck   uint64          `json:"resize_acknowledged"`
	ResizeNoAck uint64          `json:"resize_unacknowledged"`
}

// rbdBasePlanFor places writes on 64 KiB objects 0 to 4 of an eight-object
// image: object 0 full and object 1 half before the snapshot; object 2 with an
// in-object hole between two 8 KiB writes; object 3 written only after the
// snapshot; object 4 with a leading hole; objects 5 to 7 never written. After
// the snapshot the second half of object 0 and the start of object 1 change.
func rbdBasePlanFor(token string) rbdBasePlan {
	const k = 1 << 10
	snapshot := "ceph-msgr-snap"
	if len(token) >= 8 {
		snapshot += "-" + token[:8]
	}
	return rbdBasePlan{Pool: "ceph-msgr-rbd-" + token, Image: "ceph-msgr-image-" + token, Snapshot: snapshot, Order: 16, Size: 512 * k,
		// RBD_FEATURE_LAYERING only: no exclusive lock, object map or journal,
		// so header notifies come only from the explicit native resizes.
		Features:    1,
		PreWrites:   []rbdBaseExtent{{0, 96 * k}, {128 * k, 8 * k}, {168 * k, 8 * k}, {272 * k, 4 * k}},
		PostWrites:  []rbdBaseExtent{{32 * k, 40 * k}, {192 * k, 4 * k}},
		ResizeAck:   640 * k,
		ResizeNoAck: 768 * k}
}

func (p rbdBasePlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.Image == "ceph-msgr-image-"+token && p.Snapshot == "ceph-msgr-snap-"+token[:8] &&
		p.Order == 16 && p.Size == 512<<10 && p.Features == 1 && p.ResizeAck > p.Size && p.ResizeNoAck > p.ResizeAck
}

// rbdBaseCaps is RBD's standard capability profile bound to the owned pool,
// for both the Go client and the native librbd oracle.
func rbdBaseCaps(pool string) ceph.ClientCaps {
	return ceph.ClientCaps{Mon: "profile rbd", OSD: "profile rbd pool=" + pool}
}

type rbdBaseMutation struct {
	Kind      string `json:"kind"`
	RequestID int64  `json:"request_id,omitempty"`
	Target    string `json:"target"`
	// ReadOnly marks a journaled native observation of the RBD write
	// acceptance: one-shot like a mutation, but it changes nothing.
	ReadOnly bool   `json:"read_only,omitempty"`
	State    string `json:"state"`
	Error    string `json:"error,omitempty"`
}

type rbdBaseScope struct {
	mu               sync.Mutex
	out, token       string
	plan             rbdBasePlan
	goClient, native *ceph.ClientConfig
	goIntent         bool
	nativeIntent     bool
	poolIntent       bool
	poolID           int64
	lastRequestID    int64
	mutations        []*rbdBaseMutation
	setup            *rbdBaseNative
	cleanupAttempted bool
	cleanupErr       error
	// The RBD write acceptance's second image in the same owned pool
	// (rbd_write.go): its plan, the last control request and the index of
	// the next admitted action.
	writePlan          rbdWritePlan
	writeLastRequestID int64
	writeNext          int
	// The RBD exclusive-lock acceptance's third image in the same owned
	// pool (rbd_lock.go): its plan, the last control request and the index
	// of the next admitted action.
	lockPlan          rbdLockPlan
	lockLastRequestID int64
	lockNext          int
	// The RBD clone acceptance's parent and clone images in the same owned
	// pool (rbd_clone.go): their plan, the last control request and the
	// index of the next admitted action.
	clonePlan          rbdClonePlan
	cloneLastRequestID int64
	cloneNext          int
	// The RBD journaling acceptance's image in the same owned pool
	// (rbd_journal.go): its plan, the last control request and the index of
	// the next admitted action.
	journalPlan          rbdJournalPlan
	journalLastRequestID int64
	journalNext          int
}

func (x *rbdBaseScope) mutated(kind string) bool {
	return slices.ContainsFunc(x.mutations, func(m *rbdBaseMutation) bool { return m.Kind == kind })
}

// begin journals one owned mutation before it is issued. A kind is issued at
// most once, whatever its outcome.
func (c *fixtureCluster) rbdBaseBegin(kind, target string, request int64) (*rbdBaseMutation, error) {
	return c.rbdBaseBeginAction(kind, target, request, false)
}

// rbdBaseBeginAction journals one native action; readOnly marks an
// observation that changes no owned state.
func (c *fixtureCluster) rbdBaseBeginAction(kind, target string, request int64, readOnly bool) (*rbdBaseMutation, error) {
	x := c.rbdBase
	if x.mutated(kind) {
		return nil, fmt.Errorf("RBD base %s was already issued", kind)
	}
	m := &rbdBaseMutation{Kind: kind, RequestID: request, Target: target, ReadOnly: readOnly, State: "intended"}
	x.mutations = append(x.mutations, m)
	return m, c.rbdBaseJournal()
}

func (c *fixtureCluster) rbdBaseFinish(m *rbdBaseMutation, err error) error {
	if err != nil {
		m.State, m.Error = "uncertain", err.Error()
		return errors.Join(err, c.rbdBaseJournal())
	}
	m.State = "applied"
	return c.rbdBaseJournal()
}

func (c *fixtureCluster) rbdBaseJournal() error {
	x := c.rbdBase
	identity, native := "", ""
	if x.goIntent {
		identity = "client.rbd-base-" + x.token
	}
	if x.nativeIntent {
		native = "client.rbd-base-native-" + x.token
	}
	return writeMDSFreshJSON(x.out, "rbd-base-owned.json", map[string]any{
		"source_commit": rbdBaseSource, "token": x.token, "fsid": c.profile.fsid, "plan": x.plan, "write_plan": x.writePlan, "lock_plan": x.lockPlan, "clone_plan": x.clonePlan, "journal_plan": x.journalPlan, "pool_intended": x.poolIntent, "pool_id": x.poolID,
		"client_identity": identity, "native_identity": native, "mutations": x.mutations,
	})
}

// The fixed native librbd driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache, and only error-level log lines reach stderr (the receipt keeps
// them: librbd reports a timed-out header notify there). Sources:
// pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at rbdBaseSource; independent
// driver, no code copied.
const rbdBaseNativePython = `
import hashlib, json, os, struct, sys, time
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('image') != 'ceph-msgr-image-' + token
        or plan.get('snapshot') != 'ceph-msgr-snap-' + token[:8] or action not in ('setup', 'watch') or not request_id.isdecimal()):
    raise ValueError('native RBD driver scope is not owned')
OUT = '/out'
NOSNAP = 0xfffffffffffffffe
pool, name, snap_name, size = plan['pool'], plan['image'], plan['snapshot'], plan['size']

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
        counter += 1
    return bytes(data[:length])

def publish(filename, data):
    temporary = os.path.join(OUT, '.' + filename + '.tmp')
    with open(temporary, 'wb') as f:
        f.write(data)
    os.chmod(temporary, 0o644)
    os.replace(temporary, os.path.join(OUT, filename))
    return {'name': filename, 'length': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

def call(ioctx, oid, method, data):
    entry = {'object': oid, 'method': method, 'input_hex': data.hex()}
    try:
        length, output = ioctx.execute(oid, 'rbd', method, data, 4096)
        output = bytes(output)
        if length != len(output):
            raise ValueError('native class output length differs')
        entry.update(code=0, output_hex=output.hex())
    except rados.Error as error:
        if not isinstance(error.errno, int) or error.errno <= 0:
            raise
        entry.update(code=-error.errno, output_hex='')
    return entry

def header_calls(ioctx, image_id, snap_id):
    header = 'rbd_header.' + image_id
    return [call(ioctx, 'rbd_id.' + name, 'get_id', b''),
            call(ioctx, header, 'get_size', struct.pack('<Q', NOSNAP)),
            call(ioctx, header, 'get_size', struct.pack('<Q', snap_id)),
            call(ioctx, header, 'get_object_prefix', b''),
            call(ioctx, header, 'get_features', struct.pack('<QB', NOSNAP, 0)),
            call(ioctx, header, 'get_snapcontext', b''),
            call(ioctx, header, 'get_snapshot_name', struct.pack('<Q', snap_id)),
            call(ioctx, header, 'snapshot_get', struct.pack('<Q', snap_id))]

def object_views(ioctx, prefix, count, snap_id):
    objects, blobs = [], {'head': bytearray(), 'snap': bytearray()}
    try:
        for index in range(count):
            oid = '%s.%016x' % (prefix, index)
            entry = {'index': index, 'name': oid}
            for label, snap in (('head', rados.LIBRADOS_SNAP_HEAD), ('snap', snap_id)):
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
    return objects, blobs

def observe(ioctx, out, data):
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        out['image_id'] = text(image.id())
        # image_info_t's fixed parent_name array reaches Python as bytes.
        out['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix', 'parent_pool', 'parent_name')}
        out['features'], out['op_features'], out['flags'] = image.features(), image.op_features(), image.flags()
        out['stripe_unit'], out['stripe_count'], out['data_pool_id'] = image.stripe_unit(), image.stripe_count(), image.data_pool_id()
        out['snapshots'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]
        snap_id = image.snap_get_id(snap_name)
        head = image.read(0, stat['size']) if data else None
    out['header_object'], out['id_object'] = 'rbd_header.' + out['image_id'], 'rbd_id.' + name
    out['class_calls'] = header_calls(ioctx, out['image_id'], snap_id)
    if not data:
        return
    with rbd.Image(ioctx, name, snapshot=snap_name, read_only=True) as image:
        snap = image.read(0, image.size())
    objects, views = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'], snap_id)
    out['objects'] = objects
    out['blobs'] = {'image_head': publish('rbd-base-image-head.bin', head), 'image_snap': publish('rbd-base-image-snap.bin', snap),
                    'objects_head': publish('rbd-base-objects-head.bin', bytes(views['head'])), 'objects_snap': publish('rbd-base-objects-snap.bin', bytes(views['snap']))}
    listed = sorted(text(o.key) for o in ioctx.list_objects())
    if len(listed) > 64:
        raise ValueError('owned RBD pool lists too many objects')
    out['pool_objects'] = listed

def write(image, phase, extent):
    data = pattern(phase, extent['offset'], extent['length'])
    written = image.write(data, extent['offset'])
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest()}

def wait_proceed(step):
    path, expected = os.path.join(OUT, 'rbd-base-watch-proceed-%d' % step), '%s %s %d' % (token, request_id, step)
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

cluster = rados.Rados(conffile='/tmp/ceph-msgr-test/ceph.conf', rados_id=identity[len('client.'):],
                      conf={'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'image': name, 'native_global_id': cluster.get_instance_id()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            if list(rbd.RBD().list(ioctx)):
                raise ValueError('owned RBD pool already holds an image')
            rbd.RBD().create(ioctx, name, size, order=plan['order'], old_format=False, features=plan['features'])
            writes = []
            with rbd.Image(ioctx, name) as image:
                writes += [write(image, 'pre', e) for e in plan['pre_snapshot_writes']]
                image.flush()
                image.create_snap(snap_name)
                writes += [write(image, 'post', e) for e in plan['post_snapshot_writes']]
                image.flush()
            out['writes'] = writes
            observe(ioctx, out, True)
        else:
            state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'phases': []}
            # A read-write open registers librbd's header watch.
            with rbd.Image(ioctx, name) as image:
                def phase(label, **extra):
                    watchers = [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]
                    state['phases'].append(dict(phase=label, size=image.size(), watchers=watchers, **extra))
                    state['phase'] = label
                    publish('rbd-base-watch-state.json', json.dumps(state, sort_keys=True).encode())
                phase('opened')
                waited = wait_proceed(1)
                started = time.monotonic_ns()
                image.resize(plan['resize_acknowledged'])
                phase('resized-acknowledged', resize_ns=time.monotonic_ns() - started, wait_ns=waited)
                waited = wait_proceed(2)
                started = time.monotonic_ns()
                image.resize(plan['resize_unacknowledged'])
                phase('resized-unacknowledged', resize_ns=time.monotonic_ns() - started, wait_ns=waited)
            state['phase'] = 'closed'
            publish('rbd-base-watch-state.json', json.dumps(state, sort_keys=True).encode())
            out['phases'] = state['phases']
            observe(ioctx, out, False)
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
print(json.dumps(out, sort_keys=True))
`

type rbdBaseStat struct {
	Size       uint64 `json:"size"`
	ObjectSize uint64 `json:"obj_size"`
	Objects    uint64 `json:"num_objs"`
	Order      int    `json:"order"`
	Prefix     string `json:"block_name_prefix"`
	// image_info_t's parent pool reaches Python as an unsigned value; no
	// parent is (uint64)-1.
	ParentPool uint64 `json:"parent_pool"`
	ParentName string `json:"parent_name"`
}

type rbdBaseSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Size      uint64 `json:"size"`
	Namespace int    `json:"namespace"`
}

type rbdBaseClassCall struct {
	Object    string `json:"object"`
	Method    string `json:"method"`
	InputHex  string `json:"input_hex"`
	Code      int32  `json:"code"`
	OutputHex string `json:"output_hex"`
}

type rbdBaseObjectView struct {
	Exists bool   `json:"exists"`
	Errno  int    `json:"errno"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

type rbdBaseObject struct {
	Index uint64            `json:"index"`
	Name  string            `json:"name"`
	Head  rbdBaseObjectView `json:"head"`
	Snap  rbdBaseObjectView `json:"snap"`
}

type rbdBaseBlob struct {
	Name   string `json:"name"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type rbdBaseWrite struct {
	Phase   string `json:"phase"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Written uint64 `json:"written"`
	SHA256  string `json:"sha256"`
}

type rbdBaseWatcher struct {
	Address string `json:"addr"`
	ID      uint64 `json:"id"`
	Cookie  uint64 `json:"cookie"`
}

type rbdBaseWatchPhase struct {
	Phase    string           `json:"phase"`
	Size     uint64           `json:"size"`
	Watchers []rbdBaseWatcher `json:"watchers"`
	ResizeNS int64            `json:"resize_ns,omitempty"`
	WaitNS   int64            `json:"wait_ns,omitempty"`
}

// rbdBaseNative is one complete driver output; unknown fields are refused.
type rbdBaseNative struct {
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
	Stat           *rbdBaseStat           `json:"stat"`
	Features       uint64                 `json:"features"`
	OpFeatures     uint64                 `json:"op_features"`
	Flags          uint64                 `json:"flags"`
	StripeUnit     uint64                 `json:"stripe_unit"`
	StripeCount    uint64                 `json:"stripe_count"`
	DataPoolID     int64                  `json:"data_pool_id"`
	Snapshots      []rbdBaseSnapshot      `json:"snapshots"`
	ClassCalls     []rbdBaseClassCall     `json:"class_calls"`
	Writes         []rbdBaseWrite         `json:"writes,omitempty"`
	Objects        []rbdBaseObject        `json:"objects,omitempty"`
	Blobs          map[string]rbdBaseBlob `json:"blobs,omitempty"`
	PoolObjects    []string               `json:"pool_objects,omitempty"`
	Phases         []rbdBaseWatchPhase    `json:"phases,omitempty"`
}

var rbdBaseClassMethods = []string{"get_id", "get_size", "get_size", "get_object_prefix", "get_features", "get_snapcontext", "get_snapshot_name", "snapshot_get"}

// decodeRBDBaseNative accepts exactly the shape of one admitted action for the
// owned plan. Binary views are checked separately against their files.
func decodeRBDBaseNative(action, token string, request int64, plan rbdBasePlan, data []byte) (rbdBaseNative, error) {
	var result rbdBaseNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdBaseNative{}, fmt.Errorf("native RBD output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Image != plan.Image ||
		result.PoolID < 0 || result.NativeGlobalID == 0 {
		return rbdBaseNative{}, errors.New("native RBD output identity differs")
	}
	s := result.Stat
	ok := s != nil && result.ImageID != "" && !strings.ContainsAny(result.ImageID, "./ \x00") && result.HeaderObject == "rbd_header."+result.ImageID && result.IDObject == "rbd_id."+plan.Image &&
		s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Prefix == "rbd_data."+result.ImageID && s.ParentPool == ^uint64(0) && s.ParentName == "" &&
		result.Features == plan.Features && result.StripeUnit == s.ObjectSize && result.StripeCount == 1 && len(result.Snapshots) == 1 && result.Snapshots[0].Name == plan.Snapshot &&
		result.Snapshots[0].ID != 0 && result.Snapshots[0].Size == plan.Size && result.Snapshots[0].Namespace == 0 && len(result.ClassCalls) == len(rbdBaseClassMethods)
	for i, call := range result.ClassCalls {
		_, inErr := hex.DecodeString(call.InputHex)
		_, outErr := hex.DecodeString(call.OutputHex)
		ok = ok && call.Method == rbdBaseClassMethods[i] && inErr == nil && outErr == nil && call.Code <= 0 && len(call.OutputHex) <= 8192 && (i == 0) == (call.Object == result.IDObject) && (i == 0 || call.Object == result.HeaderObject)
	}
	switch action {
	case "setup":
		ok = ok && s.Size == plan.Size && s.Objects == plan.Size>>plan.Order && len(result.Writes) == len(plan.PreWrites)+len(plan.PostWrites) && len(result.Objects) == int(s.Objects) &&
			len(result.Blobs) == len(rbdBaseBlobNames) && len(result.PoolObjects) > 0 && len(result.PoolObjects) <= 64 && result.Phases == nil
		for i, w := range result.Writes {
			want, phase := rbdBaseExtent{}, "pre"
			if i < len(plan.PreWrites) {
				want = plan.PreWrites[i]
			} else {
				want, phase = plan.PostWrites[i-len(plan.PreWrites)], "post"
			}
			ok = ok && w.Phase == phase && w.Offset == want.Offset && w.Length == want.Length && w.Written == want.Length && len(w.SHA256) == 64
		}
		var offsets = map[string]uint64{"head": 0, "snap": 0}
		for i, o := range result.Objects {
			ok = ok && o.Index == uint64(i) && o.Name == fmt.Sprintf("%s.%016x", s.Prefix, i)
			for label, view := range map[string]rbdBaseObjectView{"head": o.Head, "snap": o.Snap} {
				ok = ok && view.Offset == offsets[label] && view.Size <= s.ObjectSize && (view.Exists && view.Errno == 0 && len(view.SHA256) == 64 || !view.Exists && view.Errno == 2 && view.Size == 0 && view.SHA256 == "")
				offsets[label] += view.Size
			}
		}
		for key, name := range rbdBaseBlobNames {
			blob, present := result.Blobs[key]
			ok = ok && present && blob.Name == name && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
		}
		ok = ok && result.Blobs["image_head"].Length == plan.Size && result.Blobs["image_snap"].Length == plan.Size &&
			result.Blobs["objects_head"].Length == offsets["head"] && result.Blobs["objects_snap"].Length == offsets["snap"]
	case "watch":
		ok = ok && s.Size == plan.ResizeNoAck && result.Writes == nil && result.Objects == nil && result.Blobs == nil && result.PoolObjects == nil && len(result.Phases) == 3
		for i, label := range []string{"opened", "resized-acknowledged", "resized-unacknowledged"} {
			if i >= len(result.Phases) {
				break
			}
			p := result.Phases[i]
			size := []uint64{plan.Size, plan.ResizeAck, plan.ResizeNoAck}[i]
			ok = ok && p.Phase == label && p.Size == size && len(p.Watchers) <= 16 && (i == 0) == (p.ResizeNS == 0) && p.ResizeNS >= 0 && p.WaitNS >= 0
		}
	default:
		ok = false
	}
	if !ok {
		return rbdBaseNative{}, fmt.Errorf("native RBD %s output shape differs", action)
	}
	return result, nil
}

// checkRBDBaseBlobs verifies that each published binary view matches its
// receipt, so the Go test compares against exactly what the driver read.
func checkRBDBaseBlobs(out string, blobs map[string]rbdBaseBlob) error {
	for key, name := range rbdBaseBlobNames {
		blob := blobs[key]
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
			return fmt.Errorf("native RBD binary view %s differs from its receipt", name)
		}
	}
	return nil
}

func (c *fixtureCluster) rbdBaseRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdBaseNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.plan)
	if err != nil {
		return nil, rbdBaseNative{}, err
	}
	timeout := 60 * time.Second
	if action == "watch" {
		// Two Go handshakes of at most 30 s each, one unacknowledged notify
		// (librbd's 5 s timeout) and the observation that follows.
		timeout = 100 * time.Second
	}
	argv := []string{"python3", "-c", rbdBaseNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdBaseNative{}, errors.Join(err, fmt.Errorf("native RBD %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDBaseNative(action, x.token, request, x.plan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

type rbdBasePoolPolicy struct {
	ID        int64  `json:"pool"`
	Name      string `json:"pool_name"`
	Flags     uint64 `json:"flags"`
	FlagNames string `json:"flags_names"`
}

// prepareRBDBase runs after the native oracle worker is ready (its
// configuration copy and the base OSD fixture exist). Every mutation is
// journaled first; an error leaves cleanup to remove whatever was intended.
func (c *fixtureCluster) prepareRBDBase(ctx context.Context, out string) error {
	p := c.profile
	if p.rbdBase != "1" || p.osd != "1" || p.osdProfile != "legacy-v6" || p.osdReplicas != "1" || p.mdsFresh != "0" || c.rbdBase != nil {
		return errors.New("invalid RBD base preparation scope")
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return err
	}
	token := hex.EncodeToString(raw[:])
	x := &rbdBaseScope{out: out, token: token, plan: rbdBasePlanFor(token), writePlan: rbdWritePlanFor(token), lockPlan: rbdLockPlanFor(token), clonePlan: rbdClonePlanFor(token), journalPlan: rbdJournalPlanFor(token), poolID: -1}
	if !x.plan.owned(token) || !x.writePlan.owned(token) || !x.lockPlan.owned(token) || !x.clonePlan.owned(token) || !x.journalPlan.owned(token) {
		return errors.New("RBD base plan is not bound to its token")
	}
	c.rbdBase = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	x.poolIntent = true
	m, err := c.rbdBaseBegin("pool-create", x.plan.Pool, 0)
	if err != nil {
		return err
	}
	// Eight PGs, no HASHPSPOOL: the legacy-v6 client sends the raw object
	// hash and the OSD folds it with the legacy additive seed.
	_, err = c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: x.plan.Pool, PGNum: 8, Replicas: 1, MinSize: 1, Application: "rbd", NoHashPSPool: true})
	if err := c.rbdBaseFinish(m, err); err != nil {
		return fmt.Errorf("create owned RBD pool: %w", err)
	}
	m, err = c.rbdBaseBegin("pool-init", x.plan.Pool, 0)
	if err != nil {
		return err
	}
	if err := c.rbdBaseFinish(m, rbd.InitPool(ctx, c.cluster, x.plan.Pool)); err != nil {
		return fmt.Errorf("initialize owned RBD pool: %w", err)
	}
	if err := c.cluster.WaitForClean(ctx); err != nil {
		return fmt.Errorf("owned RBD pool readiness: %w", err)
	}
	policy, err := c.poolPolicy(ctx)
	if err != nil {
		return err
	}
	var owned *rbdBasePoolPolicy
	for _, pool := range policy.Pools {
		// The legacy-v6 client admits no HASHPSPOOL bit on any pool.
		if pool.Flags&1 != 0 || strings.Contains(","+pool.FlagNames+",", ",hashpspool,") {
			return fmt.Errorf("RBD base fixture pool %s requires HASHPSPOOL", pool.Name)
		}
		if pool.Name == x.plan.Pool {
			owned = &rbdBasePoolPolicy{ID: pool.ID, Name: pool.Name, Flags: pool.Flags, FlagNames: pool.FlagNames}
		}
	}
	if owned == nil {
		return errors.New("owned RBD pool missing from the native OSDMap")
	}
	x.poolID = owned.ID
	x.goIntent = true
	if err := c.rbdBaseJournal(); err != nil {
		return err
	}
	x.goClient, err = c.cluster.CreateClient(ctx, "rbd-base-"+token, rbdBaseCaps(x.plan.Pool))
	if err != nil {
		return fmt.Errorf("create dedicated RBD base identity: %w", err)
	}
	_, keyring, err := x.goClient.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "rbd-base.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	x.nativeIntent = true
	if err := c.rbdBaseJournal(); err != nil {
		return err
	}
	x.native, err = c.cluster.CreateClient(ctx, "rbd-base-native-"+token, rbdBaseCaps(x.plan.Pool))
	if err != nil {
		return fmt.Errorf("create dedicated native RBD oracle identity: %w", err)
	}
	_, nativeKeyring, err := x.native.ConnectionConfig()
	if err != nil {
		return err
	}
	if _, err := mdsSessionsKeyringSecret(nativeKeyring); err != nil {
		return err
	}
	// The native-only key stays in memory and reaches each worker on its
	// standard input (runKeyedNative); no host keyring file is written.
	c.profile.rbdBaseIdentity = "client.rbd-base-" + token
	m, err = c.rbdBaseBegin("native-setup", x.plan.Image, 0)
	if err != nil {
		return err
	}
	started := time.Now()
	process, native, err := c.rbdBaseRunNative(ctx, "setup", 0)
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD setup used another pool")
	}
	if err == nil {
		err = checkRBDBaseBlobs(out, native.Blobs)
	}
	receipt := map[string]any{"source_commit": rbdBaseSource, "token": token, "action": "setup", "elapsed_ms": time.Since(started).Milliseconds(), "native_process": process, "complete": err == nil}
	if err != nil {
		receipt["error"] = err.Error()
	} else {
		receipt["native"] = native
	}
	err = errors.Join(c.rbdBaseFinish(m, err), writeMDSFreshJSON(out, "rbd-base-setup.json", receipt))
	if err != nil {
		return fmt.Errorf("native RBD image setup: %w", err)
	}
	x.setup = &native
	caps := rbdBaseCaps(x.plan.Pool)
	return writeMDSFreshJSON(out, "rbd-base-oracle.json", map[string]any{
		"source_commit": rbdBaseSource, "token": token, "fsid": p.fsid, "plan": x.plan, "pool": owned,
		"client_identity": c.profile.rbdBaseIdentity, "native_identity": "client.rbd-base-native-" + token,
		"caps": map[string]string{"mon": caps.Mon, "osd": caps.OSD}, "native": native,
	})
}

// The request is <id> <ownership token> <action>. Only "watch" exists: the
// driver keeps the image open, waits for the test's two proceed signals and
// resizes the image after each.
func parseRBDBaseRequest(fields []string, token string, previousID int64) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD base request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD base request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD base request identity")
	}
	if fields[2] != "watch" {
		return 0, "", errors.New("RBD base action is not admitted")
	}
	return id, fields[2], nil
}

func (c *fixtureCluster) rbdBaseControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) {
		return errors.New("invalid owned RBD base control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD base control requested after cleanup began")
	}
	id, action, err := parseRBDBaseRequest(fields, x.token, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-base-result.json", result))
	}()
	m, err := c.rbdBaseBegin("native-resize", fmt.Sprintf("%s:%d,%d", x.plan.Image, x.plan.ResizeAck, x.plan.ResizeNoAck), id)
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdBaseRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD watch used another pool")
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

type rbdBaseCleanup struct {
	Source                string             `json:"source_commit"`
	Token                 string             `json:"token"`
	Pool                  string             `json:"pool"`
	PoolID                int64              `json:"pool_id"`
	Attempted             bool               `json:"attempted"`
	Mutations             []*rbdBaseMutation `json:"mutations"`
	PoolRemoved           bool               `json:"pool_removed"`
	PoolAbsentNatively    bool               `json:"pool_absent_natively"`
	ClientIdentityRemoved bool               `json:"client_identity_removed"`
	NativeIdentityRemoved bool               `json:"native_identity_removed"`
	Completed             bool               `json:"completed"`
	Errors                []string           `json:"errors,omitempty"`
}

// finishRBDBase runs once. It removes only owned state: the pool (its images,
// snapshots, lock attributes, object maps and CRUSH rule go with it; any
// lingering watch is discarded) and
// the two identities, then proves the pool's absence from a native OSDMap.
func (c *fixtureCluster) finishRBDBase(ctx context.Context) error {
	if c == nil || c.rbdBase == nil {
		return nil
	}
	x := c.rbdBase
	ctx, stop := context.WithTimeout(ctx, 100*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return x.cleanupErr
	}
	x.cleanupAttempted = true
	receipt := rbdBaseCleanup{Source: rbdBaseSource, Token: x.token, Pool: x.plan.Pool, PoolID: x.poolID, Attempted: true, Mutations: x.mutations}
	if err := writeMDSFreshJSON(x.out, "rbd-base-cleanup.json", receipt); err != nil {
		x.cleanupErr = err
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if x.poolIntent {
		err := c.cluster.RemovePool(ctx, x.plan.Pool)
		receipt.PoolRemoved = err == nil
		fail(err)
		data, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		var dump struct {
			FSID  string `json:"fsid"`
			Pools []struct {
				ID   int64  `json:"pool"`
				Name string `json:"pool_name"`
			} `json:"pools"`
		}
		if err == nil && (len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.FSID != c.profile.fsid || dump.Pools == nil) {
			err = errors.New("RBD base cleanup native OSDMap unavailable")
		}
		fail(err)
		if err == nil {
			receipt.PoolAbsentNatively = !slices.ContainsFunc(dump.Pools, func(p struct {
				ID   int64  `json:"pool"`
				Name string `json:"pool_name"`
			}) bool {
				return p.Name == x.plan.Pool || x.poolID >= 0 && p.ID == x.poolID
			})
			if !receipt.PoolAbsentNatively {
				fail(errors.New("owned RBD pool remained after removal"))
			}
		}
	} else {
		receipt.PoolRemoved, receipt.PoolAbsentNatively = true, true
	}
	remove := func(config *ceph.ClientConfig, intent bool, label string) bool {
		if config == nil {
			if intent {
				fail(fmt.Errorf("RBD base %s identity creation outcome is unknown", label))
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
	receipt.Completed = len(errs) == 0 && receipt.PoolRemoved && receipt.PoolAbsentNatively && receipt.ClientIdentityRemoved && receipt.NativeIdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("RBD base cleanup did not verify every check"))
	}
	x.cleanupErr = errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "rbd-base-cleanup.json", receipt))
	return x.cleanupErr
}

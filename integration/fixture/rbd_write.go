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

// The RBD write acceptance reuses the RBD base scope (token, owned pool and
// both identities) and adds one more native librbd image to the owned pool,
// created only on the test's request. The Go test then writes to that image
// through the explicit OSD API: data objects under the image SnapContext,
// rbd class header methods, the lock class and a header notify. The closed
// control here only lets native librbd and librados create the image and
// observe it afterwards: read-only opens (no header watch) after each Go step,
// and one read-write open whose header watch receives the Go notify. Every
// native action is journaled in rbd-base-owned.json before it is issued and is
// never reissued; the image goes with the owned pool at cleanup. Image layout,
// class encodings and lock decoding stay in the test-only consumer
// (integration/rbd_write_test.go). Native behavior relied upon:
// cls/rbd/cls_rbd.cc (set_size, metadata_set/get/list), cls/lock/cls_lock.cc
// and cls_lock_ops.h, librbd/ImageWatcher.cc (HeaderUpdatePayload),
// librbd/ImageState.cc (refresh_if_required), librbd/internal.cc
// (list_lockers), librbd/io/ObjectRequest.cc, pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource (LGPL-2.1 references; no code
// copied).

// rbdWriteActions is the closed, ordered action sequence of the write
// control. Each action runs at most once and only after its predecessors.
var rbdWriteActions = []string{"setup", "observe-written", "observe-resized", "observe-locked", "observe-unlocked", "watch"}

// rbdWriteBlobViews names the binary views every native write action
// publishes; the file of view v for action a is rbd-write-<a>-<v>.bin.
var rbdWriteBlobViews = map[string]string{
	"image_head":   "image-head",
	"image_snap":   "image-snap",
	"objects_head": "objects-head",
	"objects_snap": "objects-snap",
}

func rbdWriteBlobName(action, key string) string {
	return "rbd-write-" + action + "-" + rbdWriteBlobViews[key] + ".bin"
}

// rbdWriteClassCalls is the fixed native exec list of every observation:
// get_id on the id object, then rbd and lock methods on the header.
var rbdWriteClassCalls = [][2]string{
	{"rbd", "get_id"}, {"rbd", "get_size"}, {"rbd", "get_size"}, {"rbd", "get_object_prefix"}, {"rbd", "get_features"},
	{"rbd", "get_snapcontext"}, {"rbd", "metadata_list"}, {"lock", "get_info"},
}

// rbdWritePlan is the fixed recipe of the write image. GoWrites, GoResize,
// GoResizeWatched and the metadata pair are performed by the Go test; the
// native driver only creates the image (pre- and post-snapshot writes).
type rbdWritePlan struct {
	Pool            string          `json:"pool"`
	BaseImage       string          `json:"base_image"`
	Image           string          `json:"image"`
	Snapshot        string          `json:"snapshot"`
	Order           int             `json:"order"`
	Size            uint64          `json:"size"`
	Features        uint64          `json:"features"`
	PreWrites       []rbdBaseExtent `json:"pre_snapshot_writes"`
	PostWrites      []rbdBaseExtent `json:"post_snapshot_writes"`
	GoWrites        []rbdBaseExtent `json:"go_writes"`
	GoResize        uint64          `json:"go_resize"`
	GoResizeWatched uint64          `json:"go_resize_watched"`
	MetadataKey     string          `json:"metadata_key"`
	MetadataValue   string          `json:"metadata_value"`
}

// rbdWritePlanFor places a 256 KiB, four-object image: object 0 written fully
// and object 1 partly before the snapshot, object 1 rewritten after it (so it
// already has a clone), objects 2 and 3 absent. The Go writes then hit object
// 0 (no clone yet: the OSD must clone it), object 1 (already cloned), object 2
// (created) and one image extent across the boundary of objects 2 and 3.
func rbdWritePlanFor(token string) rbdWritePlan {
	const k = 1 << 10
	snapshot, key, value := "ceph-msgr-wsnap", "ceph-msgr-write", "go-written"
	if len(token) >= 8 {
		snapshot += "-" + token[:8]
		key += "." + token[:8]
		value += "-" + token[:8]
	}
	return rbdWritePlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, Image: "ceph-msgr-write-" + token, Snapshot: snapshot,
		Order: 16, Size: 256 * k,
		// RBD_FEATURE_LAYERING only, as the base image: no exclusive lock,
		// object map or journal, so the Go writes need no lock ownership.
		Features:   1,
		PreWrites:  []rbdBaseExtent{{0, 64 * k}, {64 * k, 16 * k}},
		PostWrites: []rbdBaseExtent{{72 * k, 8 * k}},
		GoWrites:   []rbdBaseExtent{{16 * k, 16 * k}, {64 * k, 4 * k}, {136 * k, 4 * k}, {188 * k, 8 * k}},
		GoResize:   320 * k, GoResizeWatched: 384 * k, MetadataKey: key, MetadataValue: value}
}

func (p rbdWritePlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.Image == "ceph-msgr-write-"+token &&
		p.Snapshot == "ceph-msgr-wsnap-"+token[:8] && p.Order == 16 && p.Size == 256<<10 && p.Features == 1 && p.GoResize > p.Size && p.GoResizeWatched > p.GoResize &&
		p.MetadataKey == "ceph-msgr-write."+token[:8] && p.MetadataValue == "go-written-"+token[:8] && len(p.GoWrites) > 0
}

// sizes is the closed set of image sizes a native observation may report.
func (p rbdWritePlan) sizes() []uint64 { return []uint64{p.Size, p.GoResize, p.GoResizeWatched} }

// The fixed native write driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache and with error-level stderr only. Read-only opens register no
// header watch (librbd/image/OpenRequest.cc); the watch action's read-write
// open does. Sources: pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at
// rbdBaseSource; independent driver, no code copied.
const rbdWriteNativePython = `
import hashlib, json, os, struct, sys, time
import rados, rbd
identity, keyring, action, token, request_id, plan_json = sys.argv[1:7]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'observe-written', 'observe-resized', 'observe-locked', 'observe-unlocked', 'watch')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('image') != 'ceph-msgr-write-' + token or plan.get('snapshot') != 'ceph-msgr-wsnap-' + token[:8]
        or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD write driver scope is not owned')
OUT = '/out'
NOSNAP = 0xfffffffffffffffe
# cls_lock_get_info_op{name="rbd_lock"}: ENCODE_START(1, 1) and a string.
LOCK_INFO = b'\x01\x01' + struct.pack('<II', 12, 8) + b'rbd_lock'
pool, name, snap_name = plan['pool'], plan['image'], plan['snapshot']
PREFIX = 'rbd-write-%s-' % action

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-write:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
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

def class_calls(ioctx, image_id, snap_id):
    header = 'rbd_header.' + image_id
    return [call(ioctx, 'rbd_id.' + name, 'rbd', 'get_id', b''),
            call(ioctx, header, 'rbd', 'get_size', struct.pack('<Q', NOSNAP)),
            call(ioctx, header, 'rbd', 'get_size', struct.pack('<Q', snap_id)),
            call(ioctx, header, 'rbd', 'get_object_prefix', b''),
            call(ioctx, header, 'rbd', 'get_features', struct.pack('<QB', NOSNAP, 0)),
            call(ioctx, header, 'rbd', 'get_snapcontext', b''),
            call(ioctx, header, 'rbd', 'metadata_list', struct.pack('<IQ', 0, 64)),
            call(ioctx, header, 'lock', 'get_info', LOCK_INFO)]

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

def watchers(image):
    return [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]

def lockers(image):
    # librbd reports an image without lockers as an empty list.
    value = image.list_lockers()
    if not value:
        return {'empty': True, 'tag': '', 'exclusive': False, 'lockers': []}
    return {'empty': False, 'tag': text(value['tag']), 'exclusive': bool(value['exclusive']),
            'lockers': [{'client': text(c), 'cookie': text(k), 'addr': text(a)} for c, k, a in value['lockers']]}

def metadata(image):
    entries = []
    for key, value in image.metadata_list():
        entries.append({'key': text(key), 'value': text(value)})
        if len(entries) > 16:
            raise ValueError('owned RBD image holds too much metadata')
    return entries

def observe(ioctx, out):
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        out['image_id'] = text(image.id())
        # image_info_t's fixed parent_name array reaches Python as bytes.
        out['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix', 'parent_pool', 'parent_name')}
        out['features'], out['op_features'], out['flags'] = image.features(), image.op_features(), image.flags()
        out['stripe_unit'], out['stripe_count'], out['data_pool_id'] = image.stripe_unit(), image.stripe_count(), image.data_pool_id()
        out['snapshots'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]
        out['metadata'], out['lockers'], out['watchers'] = metadata(image), lockers(image), watchers(image)
        snap_id = image.snap_get_id(snap_name)
        head = image.read(0, stat['size'])
    out['header_object'], out['id_object'] = 'rbd_header.' + out['image_id'], 'rbd_id.' + name
    out['class_calls'] = class_calls(ioctx, out['image_id'], snap_id)
    with rbd.Image(ioctx, name, snapshot=snap_name, read_only=True) as image:
        snap = image.read(0, image.size())
    objects, views = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'], snap_id)
    out['objects'] = objects
    out['blobs'] = {'image_head': publish(PREFIX + 'image-head.bin', head), 'image_snap': publish(PREFIX + 'image-snap.bin', snap),
                    'objects_head': publish(PREFIX + 'objects-head.bin', bytes(views['head'])), 'objects_snap': publish(PREFIX + 'objects-snap.bin', bytes(views['snap']))}

def write(image, phase, extent):
    data = pattern(phase, extent['offset'], extent['length'])
    written = image.write(data, extent['offset'])
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest()}

def wait_proceed(step):
    path, expected = os.path.join(OUT, 'rbd-write-watch-proceed-%d' % step), '%s %s %d' % (token, request_id, step)
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
                      conf={'keyring': keyring, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'image': name, 'native_global_id': cluster.get_instance_id()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            # The exclusive-lock and clone acceptances' images may already
            # exist.
            listed = sorted(text(i) for i in rbd.RBD().list(ioctx))
            planned = (plan['base_image'], 'ceph-msgr-lock-' + token, 'ceph-msgr-parent-' + token, 'ceph-msgr-clone-' + token)
            if plan['base_image'] not in listed or any(i not in planned for i in listed):
                raise ValueError('owned RBD pool holds an unplanned image')
            rbd.RBD().create(ioctx, name, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            writes = []
            with rbd.Image(ioctx, name) as image:
                writes += [write(image, 'pre', e) for e in plan['pre_snapshot_writes']]
                image.flush()
                image.create_snap(snap_name)
                writes += [write(image, 'post', e) for e in plan['post_snapshot_writes']]
                image.flush()
            out['writes'] = writes
        elif action == 'watch':
            state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'phases': []}
            # A read-write open registers librbd's header watch. size() and
            # stat() refresh the image only after a header update notify.
            with rbd.Image(ioctx, name) as image:
                header = 'rbd_header.' + text(image.id())
                def phase(label, **extra):
                    size = image.size()
                    stat_size = image.stat()['size']
                    state['phases'].append(dict(phase=label, size=size, stat_size=stat_size, watchers=watchers(image), **extra))
                    state['phase'] = label
                    publish('rbd-write-watch-state.json', json.dumps(state, sort_keys=True).encode())
                phase('opened')
                waited = wait_proceed(1)
                on_disk = call(ioctx, header, 'rbd', 'get_size', struct.pack('<Q', NOSNAP))
                phase('stale', wait_ns=waited, header_size=on_disk)
                waited = wait_proceed(2)
                phase('refreshed', wait_ns=waited)
            state['phase'] = 'closed'
            publish('rbd-write-watch-state.json', json.dumps(state, sort_keys=True).encode())
            out['phases'] = state['phases']
        observe(ioctx, out)
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
print(json.dumps(out, sort_keys=True))
`

type rbdWriteClassCall struct {
	Object    string `json:"object"`
	Class     string `json:"class"`
	Method    string `json:"method"`
	InputHex  string `json:"input_hex"`
	Code      int32  `json:"code"`
	OutputHex string `json:"output_hex"`
}

type rbdWriteLocker struct {
	Client  string `json:"client"`
	Cookie  string `json:"cookie"`
	Address string `json:"addr"`
}

type rbdWriteLockers struct {
	Empty     bool             `json:"empty"`
	Tag       string           `json:"tag"`
	Exclusive bool             `json:"exclusive"`
	Lockers   []rbdWriteLocker `json:"lockers"`
}

type rbdWriteMetadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type rbdWritePhase struct {
	Phase      string             `json:"phase"`
	Size       uint64             `json:"size"`
	StatSize   uint64             `json:"stat_size"`
	Watchers   []rbdBaseWatcher   `json:"watchers"`
	WaitNS     int64              `json:"wait_ns,omitempty"`
	HeaderSize *rbdWriteClassCall `json:"header_size,omitempty"`
}

// rbdWriteNative is one complete write-driver output; unknown fields are
// refused.
type rbdWriteNative struct {
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
	Metadata       []rbdWriteMetadata     `json:"metadata"`
	Lockers        *rbdWriteLockers       `json:"lockers"`
	Watchers       []rbdBaseWatcher       `json:"watchers"`
	ClassCalls     []rbdWriteClassCall    `json:"class_calls"`
	Writes         []rbdBaseWrite         `json:"writes,omitempty"`
	Objects        []rbdBaseObject        `json:"objects"`
	Blobs          map[string]rbdBaseBlob `json:"blobs"`
	Phases         []rbdWritePhase        `json:"phases,omitempty"`
}

func rbdWriteWatchersValid(watchers []rbdBaseWatcher) bool {
	return len(watchers) <= 16 && !slices.ContainsFunc(watchers, func(w rbdBaseWatcher) bool { return w.Address == "" || w.ID == 0 || w.Cookie == 0 })
}

// decodeRBDWriteNative accepts exactly the shape of one admitted action for
// the owned plan. It checks structure only: which size or lock state each
// observation must show after the Go steps is the test's judgment, so a Go
// failure is reported by the test instead of stopping the control loop.
func decodeRBDWriteNative(action, token string, request int64, plan rbdWritePlan, data []byte) (rbdWriteNative, error) {
	var result rbdWriteNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD write output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdWriteNative{}, fmt.Errorf("native RBD write output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Image != plan.Image ||
		result.PoolID < 0 || result.NativeGlobalID == 0 {
		return rbdWriteNative{}, errors.New("native RBD write output identity differs")
	}
	s := result.Stat
	ok := s != nil && result.ImageID != "" && !strings.ContainsAny(result.ImageID, "./ \x00") && result.HeaderObject == "rbd_header."+result.ImageID && result.IDObject == "rbd_id."+plan.Image &&
		s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Prefix == "rbd_data."+result.ImageID && s.ParentPool == ^uint64(0) && s.ParentName == "" &&
		slices.Contains(plan.sizes(), s.Size) && s.Objects == (s.Size+s.ObjectSize-1)/s.ObjectSize &&
		result.Features == plan.Features && result.StripeUnit == s.ObjectSize && result.StripeCount == 1 && len(result.Snapshots) == 1 && result.Snapshots[0].Name == plan.Snapshot &&
		result.Snapshots[0].ID != 0 && result.Snapshots[0].Size == plan.Size && result.Snapshots[0].Namespace == 0 && len(result.ClassCalls) == len(rbdWriteClassCalls) &&
		len(result.Metadata) <= 16 && result.Lockers != nil && len(result.Lockers.Lockers) <= 16 && result.Lockers.Empty == (len(result.Lockers.Lockers) == 0) &&
		(!result.Lockers.Empty || result.Lockers.Tag == "" && !result.Lockers.Exclusive) && rbdWriteWatchersValid(result.Watchers) && len(result.Objects) == int(s.Objects) &&
		len(result.Blobs) == len(rbdWriteBlobViews)
	for i, call := range result.ClassCalls {
		_, inErr := hex.DecodeString(call.InputHex)
		_, outErr := hex.DecodeString(call.OutputHex)
		ok = ok && call.Class == rbdWriteClassCalls[i][0] && call.Method == rbdWriteClassCalls[i][1] && inErr == nil && outErr == nil && call.Code <= 0 && len(call.OutputHex) <= 16384 &&
			(i == 0) == (call.Object == result.IDObject) && (i == 0 || call.Object == result.HeaderObject)
	}
	offsets := map[string]uint64{"head": 0, "snap": 0}
	for i, o := range result.Objects {
		ok = ok && o.Index == uint64(i) && o.Name == fmt.Sprintf("%s.%016x", s.Prefix, i)
		for label, view := range map[string]rbdBaseObjectView{"head": o.Head, "snap": o.Snap} {
			ok = ok && view.Offset == offsets[label] && view.Size <= s.ObjectSize && (view.Exists && view.Errno == 0 && len(view.SHA256) == 64 || !view.Exists && view.Errno == 2 && view.Size == 0 && view.SHA256 == "")
			offsets[label] += view.Size
		}
	}
	for key := range rbdWriteBlobViews {
		blob, present := result.Blobs[key]
		ok = ok && present && blob.Name == rbdWriteBlobName(action, key) && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
	}
	ok = ok && result.Blobs["image_head"].Length == s.Size && result.Blobs["image_snap"].Length == plan.Size &&
		result.Blobs["objects_head"].Length == offsets["head"] && result.Blobs["objects_snap"].Length == offsets["snap"]
	switch action {
	case "setup":
		ok = ok && s.Size == plan.Size && len(result.Writes) == len(plan.PreWrites)+len(plan.PostWrites) && result.Phases == nil && len(result.Metadata) == 0 && result.Lockers.Empty && len(result.Watchers) == 0
		for i, w := range result.Writes {
			want, phase := rbdBaseExtent{}, "pre"
			if i < len(plan.PreWrites) {
				want = plan.PreWrites[i]
			} else {
				want, phase = plan.PostWrites[i-len(plan.PreWrites)], "post"
			}
			ok = ok && w.Phase == phase && w.Offset == want.Offset && w.Length == want.Length && w.Written == want.Length && len(w.SHA256) == 64
		}
	case "observe-written", "observe-resized", "observe-locked", "observe-unlocked":
		ok = ok && result.Writes == nil && result.Phases == nil
	case "watch":
		ok = ok && result.Writes == nil && len(result.Phases) == 3
		for i, label := range []string{"opened", "stale", "refreshed"} {
			if i >= len(result.Phases) {
				break
			}
			p := result.Phases[i]
			ok = ok && p.Phase == label && slices.Contains(plan.sizes(), p.Size) && slices.Contains(plan.sizes(), p.StatSize) && rbdWriteWatchersValid(p.Watchers) &&
				(i == 0) == (p.WaitNS == 0) && p.WaitNS >= 0 && (i == 1) == (p.HeaderSize != nil)
			if h := p.HeaderSize; h != nil {
				_, outErr := hex.DecodeString(h.OutputHex)
				ok = ok && h.Object == result.HeaderObject && h.Class == "rbd" && h.Method == "get_size" && h.InputHex == "feffffffffffffff" && h.Code <= 0 && outErr == nil && len(h.OutputHex) <= 64
			}
		}
	default:
		ok = false
	}
	if !ok {
		return rbdWriteNative{}, fmt.Errorf("native RBD write %s output shape differs", action)
	}
	return result, nil
}

// checkRBDWriteBlobs verifies that each published binary view of one action
// matches its receipt.
func checkRBDWriteBlobs(out, action string, blobs map[string]rbdBaseBlob) error {
	for key := range rbdWriteBlobViews {
		blob := blobs[key]
		name := rbdWriteBlobName(action, key)
		if blob.Name != name {
			return fmt.Errorf("native RBD write binary view %s is misnamed", name)
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
			return fmt.Errorf("native RBD write binary view %s differs from its receipt", name)
		}
	}
	return nil
}

func (c *fixtureCluster) rbdWriteRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdWriteNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.writePlan)
	if err != nil {
		return nil, rbdWriteNative{}, err
	}
	timeout := 60 * time.Second
	if action == "watch" {
		// Two Go steps of at most 30 s each and the observation that follows.
		timeout = 100 * time.Second
	}
	argv := []string{"python3", "-c", rbdWriteNativePython, "client.rbd-base-native-" + x.token, "/out/rbd-base-native.keyring", action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: timeout, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD write " + action})
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdWriteNative{}, errors.Join(err, fmt.Errorf("native RBD write %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDWriteNative(action, x.token, request, x.writePlan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDWriteRequest admits <id> <ownership token> <action> where the
// action is the next one of rbdWriteActions (next is its index).
func parseRBDWriteRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD write request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD write request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD write request identity")
	}
	if !slices.Contains(rbdWriteActions, fields[2]) {
		return 0, "", errors.New("RBD write action is not admitted")
	}
	if next >= len(rbdWriteActions) || fields[2] != rbdWriteActions[next] {
		return 0, "", errors.New("RBD write action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdWriteControl runs one native action of the closed write sequence. The
// setup and watch actions are journaled as mutations (image creation; a
// header watch registration), the observations as read-only actions.
func (c *fixtureCluster) rbdWriteControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) || !x.writePlan.owned(x.token) {
		return errors.New("invalid owned RBD write control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD write control requested after cleanup began")
	}
	id, action, err := parseRBDWriteRequest(fields, x.token, x.writeLastRequestID, x.writeNext)
	if err != nil {
		return err
	}
	x.writeLastRequestID = id
	x.writeNext++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.writePlan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-write-"+action+".json", result))
	}()
	target := x.writePlan.Image
	if action == "watch" {
		target += ":header-watch"
	}
	m, err := c.rbdBaseBeginAction("native-write-"+action, target, id, strings.HasPrefix(action, "observe-"))
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdWriteRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD write action used another pool")
	}
	if err == nil {
		err = checkRBDWriteBlobs(out, action, native.Blobs)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

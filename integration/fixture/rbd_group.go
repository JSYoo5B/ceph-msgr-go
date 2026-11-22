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

// The RBD group-snapshot acceptance reuses the RBD base scope (token, owned
// pool and both identities) and adds, on the test's request only, two native
// librbd images in one native RBD group with one native group snapshot. Every
// mutation stays with native librbd: the Go test only reads the group header
// and the member image headers through rbd object-class CALLs and the data
// objects through explicit OSD reads, and compares them with native librbd
// listings and native librados reads published by this control. The closed
// sequence is setup (images, contents, group, membership, group snapshot),
// modify (new data on both live heads after the snapshot) and cleanup (the
// group snapshot, the membership, the group and both images removed natively,
// each removal followed by a native absence read). Every native action is
// journaled in rbd-base-owned.json before it is issued and is never reissued;
// whatever an uncertain action leaves goes with the owned pool at fixture
// cleanup. Group-record and snapshot decoding stay in the test-only consumer
// (integration/rbd_group_test.go). Native behavior relied upon, at
// rbdBaseSource: librbd/api/Group.cc (group creation, membership, snapshot
// creation and removal, the ".group.<pool>_<group id>_<snapshot id>" member
// snapshot name), cls/rbd/cls_rbd.cc (group_snap_list, snapshot_get,
// dir_get_id), cls/rbd/cls_rbd_types.cc, pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx (LGPL-2.1 references; no code copied).

// capRBDGroupSnapshot admits the group-snapshot acceptance. Its test runs
// alone on a fresh RBD fixture, so the owned pool holds only the base image
// besides the group's two images.
var capRBDGroupSnapshot = defineExclusiveCapability(fixtureRBD, "rbd-group-snapshot",
	"run one test alone on a fresh RBD fixture with the native group-snapshot sequence",
	"RBD=1", nil)

// rbdGroupActions is the closed, ordered action sequence of the group
// control. Each action runs at most once and only after its predecessors.
var rbdGroupActions = []string{"setup", "modify", "cleanup"}

// rbdGroupBlobViews names the binary views each image of an observation
// publishes; the file of view v of image k for action a is
// rbd-group-<a>-<k>-<v>.bin.
var rbdGroupBlobViews = map[string]string{
	"image_head":   "image-head",
	"image_snap":   "image-snap",
	"objects_head": "objects-head",
	"objects_snap": "objects-snap",
}

func rbdGroupBlobName(action, image, key string) string {
	return "rbd-group-" + action + "-" + image + "-" + rbdGroupBlobViews[key] + ".bin"
}

// rbdGroupImagePlan is one member image: writes before the group snapshot
// (setup) and after it (modify).
type rbdGroupImagePlan struct {
	Key        string          `json:"key"`
	Name       string          `json:"name"`
	PreWrites  []rbdBaseExtent `json:"pre_snapshot_writes"`
	PostWrites []rbdBaseExtent `json:"post_snapshot_writes"`
}

// rbdGroupPlan is the fixed recipe of the group acceptance. ListPage is the
// page size of every group_snap_list call, the driver's and the Go test's.
type rbdGroupPlan struct {
	Pool          string              `json:"pool"`
	BaseImage     string              `json:"base_image"`
	Group         string              `json:"group"`
	GroupSnapshot string              `json:"group_snapshot"`
	Order         int                 `json:"order"`
	Size          uint64              `json:"size"`
	Features      uint64              `json:"features"`
	ListPage      uint64              `json:"list_page"`
	Images        []rbdGroupImagePlan `json:"images"`
}

// rbdGroupPlanFor places two three-object images with different contents.
// Image a: object 0 and part of object 1 before the snapshot; after it, part
// of object 0 rewritten and object 2 created. Image b: parts of objects 0 and
// 2 before; after it, object 1 created and part of object 2 rewritten. Each
// image's snapshot view therefore differs from its later head in an existing
// object and in an object created after the snapshot.
func rbdGroupPlanFor(token string) rbdGroupPlan {
	const k = 1 << 10
	snapshot := "ceph-msgr-gsnap"
	if len(token) >= 8 {
		snapshot += "-" + token[:8]
	}
	return rbdGroupPlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, Group: "ceph-msgr-group-" + token, GroupSnapshot: snapshot,
		// RBD_FEATURE_LAYERING only: no exclusive lock to acquire while
		// librbd quiesces and snapshots the members.
		Order: 16, Size: 192 * k, Features: 1, ListPage: 1,
		Images: []rbdGroupImagePlan{
			{Key: "a", Name: "ceph-msgr-gimage-a-" + token, PreWrites: []rbdBaseExtent{{0, 48 * k}, {80 * k, 16 * k}}, PostWrites: []rbdBaseExtent{{16 * k, 16 * k}, {128 * k, 8 * k}}},
			{Key: "b", Name: "ceph-msgr-gimage-b-" + token, PreWrites: []rbdBaseExtent{{8 * k, 24 * k}, {136 * k, 32 * k}}, PostWrites: []rbdBaseExtent{{64 * k, 12 * k}, {144 * k, 8 * k}}},
		}}
}

func (p rbdGroupPlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.Group == "ceph-msgr-group-"+token &&
		p.GroupSnapshot == "ceph-msgr-gsnap-"+token[:8] && p.Order == 16 && p.Size == 192<<10 && p.Features == 1 && p.ListPage == 1 && len(p.Images) == 2 &&
		p.Images[0].Key == "a" && p.Images[0].Name == "ceph-msgr-gimage-a-"+token && p.Images[1].Key == "b" && p.Images[1].Name == "ceph-msgr-gimage-b-"+token &&
		!slices.ContainsFunc(p.Images, func(i rbdGroupImagePlan) bool { return len(i.PreWrites) == 0 || len(i.PostWrites) == 0 })
}

// A group member carries RBD_FEATURE_OPERATIONS with the
// RBD_OPERATION_FEATURE_GROUP op feature, which cls_rbd's image_group_add
// sets (ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd.cc,
// ceph/ceph/blob/v20.2.4/src/include/rbd/features.h).
const (
	rbdFeatureOperations     = 1 << 8
	rbdOperationFeatureGroup = 1 << 2
)

// rbdGroupState is the group control's part of the RBD base scope: its plan,
// the last control request and the index of the next admitted action.
type rbdGroupState struct {
	plan          rbdGroupPlan
	lastRequestID int64
	next          int
}

// The fixed native group driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache and with error-level stderr only. Every native class call is a
// librados exec of a read method, so it changes nothing. Sources:
// pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at rbdBaseSource; independent
// driver, no code copied.
const rbdGroupNativePython = `
import hashlib, json, os, struct, sys
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'modify', 'cleanup')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('group') != 'ceph-msgr-group-' + token or plan.get('group_snapshot') != 'ceph-msgr-gsnap-' + token[:8]
        or [i.get('key') for i in plan.get('images', [])] != ['a', 'b']
        or [i.get('name') for i in plan['images']] != ['ceph-msgr-gimage-a-' + token, 'ceph-msgr-gimage-b-' + token]
        or plan.get('list_page') != 1 or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD group driver scope is not owned')
OUT = '/out'
pool, group_name, snap_name, page = plan['pool'], plan['group'], plan['group_snapshot'], plan['list_page']
# group_snap_list input: cls::rbd::GroupSnapshot (ENCODE_START(1, 1), empty id
# and name, state INCOMPLETE, no member snapshots) and the page size.
START = b'\x01\x01' + struct.pack('<I', 13) + struct.pack('<IIBI', 0, 0, 0, 0)

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(key, phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-group:%s:%s:%s:%d:%d' % (token, key, phase, offset, counter)).encode()).digest()
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

def errno_of(ioctx, oid):
    try:
        ioctx.stat(oid)
        return 0
    except rados.ObjectNotFound:
        return 2

def group_snap(s):
    return {'id': text(s['id']), 'name': text(s['name']), 'state': s['state'], 'namespace_type': s['namespace_type'],
            'image_snap_name': text(s['image_snap_name']),
            'image_snaps': [{'pool_id': i['pool_id'], 'snap_id': i['snap_id'], 'image_name': text(i['image_name'])} for i in s['image_snaps']]}

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

def observe_group(ioctx, out):
    with rbd.Group(ioctx, group_name) as group:
        group_id = text(group.id())
        members = [{'name': text(m['name']), 'pool': m['pool'], 'state': m['state']} for m in group.list_images()]
        snaps = [group_snap(s) for s in group.list_snaps()]
        info = group_snap(group.get_snap_info(snap_name))
    header = 'rbd_group_header.' + group_id
    first = call(ioctx, header, 'rbd', 'group_snap_list', START + struct.pack('<Q', page))
    calls = [first]
    output = bytes.fromhex(first['output_hex'])
    # The next page starts after the first page's only record: the output is
    # a u32 count and that one encoded GroupSnapshot.
    if first['code'] == 0 and len(output) > 4 and struct.unpack('<I', output[:4])[0] == 1:
        calls.append(call(ioctx, header, 'rbd', 'group_snap_list', output[4:] + struct.pack('<Q', page)))
    out['group'] = {'name': group_name, 'id': group_id, 'header_object': header, 'members': members, 'snapshots': snaps, 'snapshot_info': info,
                    'directory_call': call(ioctx, 'rbd_group_directory', 'rbd', 'dir_get_id', struct.pack('<I', len(group_name)) + group_name.encode()),
                    'list_calls': calls}

def observe_image(ioctx, entry):
    name = entry['name']
    view = {'key': entry['key'], 'name': name}
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        view['image_id'] = text(image.id())
        view['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix', 'parent_pool', 'parent_name')}
        view['features'], view['op_features'] = image.features(), image.op_features()
        snaps = []
        for s in image.list_snaps():
            snap = {'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']}
            if s['namespace'] == rbd.RBD_SNAP_NAMESPACE_TYPE_GROUP:
                g = image.snap_get_group_namespace(s['id'])
                snap['group'] = {'pool': g['pool'], 'name': text(g['name']), 'snap_name': text(g['snap_name'])}
            snaps.append(snap)
        view['snapshots'] = snaps
        head = image.read(0, stat['size'])
    view['header_object'], view['id_object'] = 'rbd_header.' + view['image_id'], 'rbd_id.' + name
    view['snapshot_calls'] = [call(ioctx, view['header_object'], 'rbd', 'snapshot_get', struct.pack('<Q', s['id'])) for s in snaps]
    if len(snaps) != 1:
        raise ValueError('owned group member holds other snapshots')
    snap_id = snaps[0]['id']
    with rbd.Image(ioctx, name, read_only=True) as image:
        image.set_snap_by_id(snap_id)
        snap = image.read(0, image.size())
    objects, views = object_views(ioctx, stat['block_name_prefix'], stat['num_objs'], snap_id)
    view['objects'] = objects
    prefix = 'rbd-group-%s-%s-' % (action, entry['key'])
    view['blobs'] = {'image_head': publish(prefix + 'image-head.bin', head), 'image_snap': publish(prefix + 'image-snap.bin', snap),
                     'objects_head': publish(prefix + 'objects-head.bin', bytes(views['head'])), 'objects_snap': publish(prefix + 'objects-snap.bin', bytes(views['snap']))}
    return view

def write(image, key, phase, extent):
    data = pattern(key, phase, extent['offset'], extent['length'])
    written = image.write(data, extent['offset'])
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'image': key, 'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest()}

def listed(ioctx, out):
    out['images_listed'] = sorted(text(i) for i in rbd.RBD().list(ioctx))
    out['groups_listed'] = sorted(text(g) for g in rbd.RBD().group_list(ioctx))

cluster = rados.Rados(conffile='/tmp/ceph-msgr-test/ceph.conf', rados_id=identity[len('client.'):],
                      conf={'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'native_global_id': cluster.get_instance_id()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            # The acceptance runs alone on its fixture: only the base image
            # and no group may exist before it.
            listed(ioctx, out)
            if out['images_listed'] != [plan['base_image']] or out['groups_listed']:
                raise ValueError('owned RBD pool holds an unplanned image or group')
            writes = []
            for entry in plan['images']:
                rbd.RBD().create(ioctx, entry['name'], plan['size'], order=plan['order'], old_format=False, features=plan['features'])
                with rbd.Image(ioctx, entry['name']) as image:
                    writes += [write(image, entry['key'], 'pre', e) for e in entry['pre_snapshot_writes']]
                    image.flush()
            rbd.RBD().group_create(ioctx, group_name)
            with rbd.Group(ioctx, group_name) as group:
                for entry in plan['images']:
                    group.add_image(ioctx, entry['name'])
                group.create_snap(snap_name)
            out['writes'] = writes
        elif action == 'modify':
            writes = []
            for entry in plan['images']:
                with rbd.Image(ioctx, entry['name']) as image:
                    writes += [write(image, entry['key'], 'post', e) for e in entry['post_snapshot_writes']]
                    image.flush()
            out['writes'] = writes
        if action in ('setup', 'modify'):
            listed(ioctx, out)
            observe_group(ioctx, out)
            out['images'] = [observe_image(ioctx, entry) for entry in plan['images']]
        else:
            # Identities first, then each removal with its own native
            # absence read before the next one.
            with rbd.Group(ioctx, group_name) as group:
                group_id = text(group.id())
            removal = {'group_id': group_id, 'group_header': 'rbd_group_header.' + group_id, 'images': []}
            for entry in plan['images']:
                with rbd.Image(ioctx, entry['name'], read_only=True) as image:
                    stat = image.stat()
                    removal['images'].append({'key': entry['key'], 'name': entry['name'], 'image_id': text(image.id()),
                                              'prefix': text(stat['block_name_prefix']), 'objects': stat['num_objs']})
            with rbd.Group(ioctx, group_name) as group:
                group.remove_snap(snap_name)
                removal['group_snapshots_after'] = [group_snap(s) for s in group.list_snaps()]
                for image in removal['images']:
                    with rbd.Image(ioctx, image['name'], read_only=True) as opened:
                        image['snapshots_after'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in opened.list_snaps()]
                for entry in plan['images']:
                    group.remove_image(ioctx, entry['name'])
                removal['members_after'] = [{'name': text(m['name']), 'pool': m['pool'], 'state': m['state']} for m in group.list_images()]
            rbd.RBD().group_remove(ioctx, group_name)
            removal['group_header_errno'] = errno_of(ioctx, removal['group_header'])
            removal['directory_call'] = call(ioctx, 'rbd_group_directory', 'rbd', 'dir_get_id', struct.pack('<I', len(group_name)) + group_name.encode())
            for image in removal['images']:
                rbd.RBD().remove(ioctx, image['name'])
                image['header_errno'] = errno_of(ioctx, 'rbd_header.' + image['image_id'])
                image['id_errno'] = errno_of(ioctx, 'rbd_id.' + image['name'])
                image['data_errno'] = [errno_of(ioctx, '%s.%016x' % (image['prefix'], i)) for i in range(image['objects'])]
            listed(ioctx, out)
            out['removed'] = removal
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
print(json.dumps(out, sort_keys=True))
`

type rbdGroupMember struct {
	Name  string `json:"name"`
	Pool  int64  `json:"pool"`
	State int    `json:"state"`
}

type rbdGroupImageSnap struct {
	PoolID    int64  `json:"pool_id"`
	SnapID    uint64 `json:"snap_id"`
	ImageName string `json:"image_name"`
}

// rbdGroupSnap is one group snapshot as librbd lists it.
type rbdGroupSnap struct {
	ID            string              `json:"id"`
	Name          string              `json:"name"`
	State         int                 `json:"state"`
	NamespaceType int                 `json:"namespace_type"`
	ImageSnapName string              `json:"image_snap_name"`
	ImageSnaps    []rbdGroupImageSnap `json:"image_snaps"`
}

type rbdGroupView struct {
	Name          string              `json:"name"`
	ID            string              `json:"id"`
	HeaderObject  string              `json:"header_object"`
	Members       []rbdGroupMember    `json:"members"`
	Snapshots     []rbdGroupSnap      `json:"snapshots"`
	SnapshotInfo  *rbdGroupSnap       `json:"snapshot_info"`
	DirectoryCall *rbdWriteClassCall  `json:"directory_call"`
	ListCalls     []rbdWriteClassCall `json:"list_calls"`
}

type rbdGroupNamespace struct {
	Pool     int64  `json:"pool"`
	Name     string `json:"name"`
	SnapName string `json:"snap_name"`
}

type rbdGroupSnapshot struct {
	ID        uint64             `json:"id"`
	Name      string             `json:"name"`
	Size      uint64             `json:"size"`
	Namespace int                `json:"namespace"`
	Group     *rbdGroupNamespace `json:"group,omitempty"`
}

type rbdGroupImageView struct {
	Key           string                 `json:"key"`
	Name          string                 `json:"name"`
	ImageID       string                 `json:"image_id"`
	HeaderObject  string                 `json:"header_object"`
	IDObject      string                 `json:"id_object"`
	Stat          *rbdBaseStat           `json:"stat"`
	Features      uint64                 `json:"features"`
	OpFeatures    uint64                 `json:"op_features"`
	Snapshots     []rbdGroupSnapshot     `json:"snapshots"`
	SnapshotCalls []rbdWriteClassCall    `json:"snapshot_calls"`
	Objects       []rbdBaseObject        `json:"objects"`
	Blobs         map[string]rbdBaseBlob `json:"blobs"`
}

type rbdGroupWrite struct {
	Image   string `json:"image"`
	Phase   string `json:"phase"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Written uint64 `json:"written"`
	SHA256  string `json:"sha256"`
}

type rbdGroupRemovedImage struct {
	Key            string             `json:"key"`
	Name           string             `json:"name"`
	ImageID        string             `json:"image_id"`
	Prefix         string             `json:"prefix"`
	Objects        uint64             `json:"objects"`
	SnapshotsAfter []rbdGroupSnapshot `json:"snapshots_after"`
	HeaderErrno    int                `json:"header_errno"`
	IDErrno        int                `json:"id_errno"`
	DataErrno      []int              `json:"data_errno"`
}

// rbdGroupRemoval is the cleanup action's record: the identities read
// before the removals and the native absence read after each one.
type rbdGroupRemoval struct {
	GroupID             string                 `json:"group_id"`
	GroupHeader         string                 `json:"group_header"`
	Images              []rbdGroupRemovedImage `json:"images"`
	GroupSnapshotsAfter []rbdGroupSnap         `json:"group_snapshots_after"`
	MembersAfter        []rbdGroupMember       `json:"members_after"`
	GroupHeaderErrno    int                    `json:"group_header_errno"`
	DirectoryCall       *rbdWriteClassCall     `json:"directory_call"`
}

// rbdGroupNative is one complete group-driver output; unknown fields are
// refused.
type rbdGroupNative struct {
	Action         string              `json:"action"`
	Token          string              `json:"token"`
	RequestID      int64               `json:"request_id"`
	Pool           string              `json:"pool"`
	PoolID         int64               `json:"pool_id"`
	NativeGlobalID uint64              `json:"native_global_id"`
	ImagesListed   []string            `json:"images_listed"`
	GroupsListed   []string            `json:"groups_listed"`
	Writes         []rbdGroupWrite     `json:"writes,omitempty"`
	Group          *rbdGroupView       `json:"group,omitempty"`
	Images         []rbdGroupImageView `json:"images,omitempty"`
	Removed        *rbdGroupRemoval    `json:"removed,omitempty"`
}

func rbdGroupIDValid(id string) bool {
	return id != "" && len(id) <= 64 && !strings.ContainsAny(id, "./ \x00")
}

func rbdGroupCallValid(call *rbdWriteClassCall, object, method string) bool {
	if call == nil {
		return false
	}
	_, inErr := hex.DecodeString(call.InputHex)
	_, outErr := hex.DecodeString(call.OutputHex)
	return call.Object == object && call.Class == "rbd" && call.Method == method && inErr == nil && outErr == nil && call.Code <= 0 && len(call.OutputHex) <= 16384
}

func rbdGroupSnapValid(s *rbdGroupSnap) bool {
	return s != nil && len(s.ImageSnaps) <= 16 && len(s.Name) <= 256 && len(s.ImageSnapName) <= 512 && len(s.ID) <= 64
}

// rbdGroupImageViewValid checks the structure of one member observation;
// which snapshot and contents it must show is the test's judgment.
func rbdGroupImageViewValid(action string, v rbdGroupImageView, want rbdGroupImagePlan, plan rbdGroupPlan) bool {
	s := v.Stat
	ok := v.Key == want.Key && v.Name == want.Name && rbdGroupIDValid(v.ImageID) && v.HeaderObject == "rbd_header."+v.ImageID && v.IDObject == "rbd_id."+want.Name &&
		s != nil && s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Size == plan.Size && s.Objects == plan.Size>>plan.Order &&
		s.Prefix == "rbd_data."+v.ImageID && s.ParentPool == ^uint64(0) && s.ParentName == "" &&
		v.Features == plan.Features|rbdFeatureOperations && v.OpFeatures == rbdOperationFeatureGroup &&
		len(v.Snapshots) == 1 && len(v.SnapshotCalls) == 1 && len(v.Objects) == int(s.Objects) && len(v.Blobs) == len(rbdGroupBlobViews)
	for i, snap := range v.Snapshots {
		ok = ok && snap.ID != 0 && len(snap.Name) <= 512 && rbdGroupCallValid(&v.SnapshotCalls[i], v.HeaderObject, "snapshot_get") &&
			(snap.Namespace == 1) == (snap.Group != nil)
	}
	offsets := map[string]uint64{"head": 0, "snap": 0}
	for i, o := range v.Objects {
		ok = ok && o.Index == uint64(i) && o.Name == fmt.Sprintf("%s.%016x", s.Prefix, i)
		for label, view := range map[string]rbdBaseObjectView{"head": o.Head, "snap": o.Snap} {
			ok = ok && view.Offset == offsets[label] && view.Size <= s.ObjectSize && (view.Exists && view.Errno == 0 && len(view.SHA256) == 64 || !view.Exists && view.Errno == 2 && view.Size == 0 && view.SHA256 == "")
			offsets[label] += view.Size
		}
	}
	for key := range rbdGroupBlobViews {
		blob, present := v.Blobs[key]
		ok = ok && present && blob.Name == rbdGroupBlobName(action, want.Key, key) && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
	}
	return ok && v.Blobs["image_head"].Length == plan.Size && v.Blobs["image_snap"].Length == plan.Size &&
		v.Blobs["objects_head"].Length == offsets["head"] && v.Blobs["objects_snap"].Length == offsets["snap"]
}

// decodeRBDGroupNative accepts exactly the shape of one admitted action for
// the owned plan. It checks structure only; the group record, the member
// snapshots and the absence results are the test's judgment, so a mismatch
// is reported by the test instead of stopping the control loop.
func decodeRBDGroupNative(action, token string, request int64, plan rbdGroupPlan, data []byte) (rbdGroupNative, error) {
	var result rbdGroupNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD group output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdGroupNative{}, fmt.Errorf("native RBD group output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.PoolID < 0 || result.NativeGlobalID == 0 {
		return rbdGroupNative{}, errors.New("native RBD group output identity differs")
	}
	ok := len(result.ImagesListed) <= 16 && len(result.GroupsListed) <= 16
	switch action {
	case "setup", "modify":
		g := result.Group
		ok = ok && result.Removed == nil && len(result.Images) == len(plan.Images) && g != nil && g.Name == plan.Group && rbdGroupIDValid(g.ID) &&
			g.HeaderObject == "rbd_group_header."+g.ID && len(g.Members) <= 16 && len(g.Snapshots) <= 16 && rbdGroupSnapValid(g.SnapshotInfo) &&
			rbdGroupCallValid(g.DirectoryCall, "rbd_group_directory", "dir_get_id") && len(g.ListCalls) >= 1 && len(g.ListCalls) <= 2
		if g != nil {
			for i := range g.Snapshots {
				ok = ok && rbdGroupSnapValid(&g.Snapshots[i])
			}
			for i := range g.ListCalls {
				ok = ok && rbdGroupCallValid(&g.ListCalls[i], g.HeaderObject, "group_snap_list")
			}
		}
		for i, v := range result.Images {
			if i < len(plan.Images) {
				ok = ok && rbdGroupImageViewValid(action, v, plan.Images[i], plan)
			}
		}
		var want []rbdGroupWrite
		phase := map[string]string{"setup": "pre", "modify": "post"}[action]
		for _, image := range plan.Images {
			extents := image.PreWrites
			if action == "modify" {
				extents = image.PostWrites
			}
			for _, e := range extents {
				want = append(want, rbdGroupWrite{Image: image.Key, Phase: phase, Offset: e.Offset, Length: e.Length, Written: e.Length})
			}
		}
		ok = ok && len(result.Writes) == len(want)
		for i, w := range result.Writes {
			if i < len(want) {
				sum := w.SHA256
				w.SHA256 = ""
				ok = ok && w == want[i] && len(sum) == 64
			}
		}
	case "cleanup":
		r := result.Removed
		ok = ok && result.Group == nil && result.Images == nil && result.Writes == nil && r != nil && rbdGroupIDValid(r.GroupID) &&
			r.GroupHeader == "rbd_group_header."+r.GroupID && len(r.Images) == len(plan.Images) && len(r.GroupSnapshotsAfter) <= 16 && len(r.MembersAfter) <= 16 &&
			(r.GroupHeaderErrno == 0 || r.GroupHeaderErrno == 2) && rbdGroupCallValid(r.DirectoryCall, "rbd_group_directory", "dir_get_id")
		if r != nil {
			for i, image := range r.Images {
				if i >= len(plan.Images) {
					break
				}
				ok = ok && image.Key == plan.Images[i].Key && image.Name == plan.Images[i].Name && rbdGroupIDValid(image.ImageID) && image.Prefix == "rbd_data."+image.ImageID &&
					image.Objects == plan.Size>>plan.Order && len(image.SnapshotsAfter) <= 16 && (image.HeaderErrno == 0 || image.HeaderErrno == 2) &&
					(image.IDErrno == 0 || image.IDErrno == 2) && len(image.DataErrno) == int(image.Objects) &&
					!slices.ContainsFunc(image.DataErrno, func(e int) bool { return e != 0 && e != 2 })
			}
		}
	default:
		ok = false
	}
	if !ok {
		return rbdGroupNative{}, fmt.Errorf("native RBD group %s output shape differs", action)
	}
	return result, nil
}

// checkRBDGroupBlobs verifies that each published binary view of one action
// matches its receipt.
func checkRBDGroupBlobs(out, action string, images []rbdGroupImageView) error {
	for _, image := range images {
		for key := range rbdGroupBlobViews {
			blob := image.Blobs[key]
			name := rbdGroupBlobName(action, image.Key, key)
			if blob.Name != name {
				return fmt.Errorf("native RBD group binary view %s is misnamed", name)
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
				return fmt.Errorf("native RBD group binary view %s differs from its receipt", name)
			}
		}
	}
	return nil
}

func (c *fixtureCluster) rbdGroupRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdGroupNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.group.plan)
	if err != nil {
		return nil, rbdGroupNative{}, err
	}
	argv := []string{"python3", "-c", rbdGroupNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 90 * time.Second, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD group " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdGroupNative{}, errors.Join(err, fmt.Errorf("native RBD group %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDGroupNative(action, x.token, request, x.group.plan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDGroupRequest admits <id> <ownership token> <action> where the
// action is the next one of rbdGroupActions (next is its index).
func parseRBDGroupRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD group request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD group request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD group request identity")
	}
	if !slices.Contains(rbdGroupActions, fields[2]) {
		return 0, "", errors.New("RBD group action is not admitted")
	}
	if next >= len(rbdGroupActions) || fields[2] != rbdGroupActions[next] {
		return 0, "", errors.New("RBD group action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdGroupTarget names what one action changes in the mutation journal.
func rbdGroupTarget(plan rbdGroupPlan, action string) string {
	images := plan.Images[0].Name + "," + plan.Images[1].Name
	switch action {
	case "setup":
		return images + ":" + plan.Group + "@" + plan.GroupSnapshot
	case "modify":
		return images + ":head"
	}
	return plan.Group + "@" + plan.GroupSnapshot + ":" + images + ":removed"
}

// rbdGroupControl runs one native action of the closed group sequence, only
// on a profile that admits capRBDGroupSnapshot. Every action is journaled as
// a mutation.
func (c *fixtureCluster) rbdGroupControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || !c.profile.admits(capRBDGroupSnapshot) || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) {
		return errors.New("invalid owned RBD group control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD group control requested after cleanup began")
	}
	if x.group.plan.Pool == "" {
		x.group.plan = rbdGroupPlanFor(x.token)
	}
	if !x.group.plan.owned(x.token) {
		return errors.New("RBD group plan is not bound to its token")
	}
	id, action, err := parseRBDGroupRequest(fields, x.token, x.group.lastRequestID, x.group.next)
	if err != nil {
		return err
	}
	x.group.lastRequestID = id
	x.group.next++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.group.plan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-group-"+action+".json", result))
	}()
	m, err := c.rbdBaseBegin("native-group-"+action, rbdGroupTarget(x.group.plan, action), id)
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdGroupRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD group action used another pool")
	}
	if err == nil {
		err = checkRBDGroupBlobs(out, action, native.Images)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

var _ = defineRequestControl("rbd-group", controlAfterRanked, (*fixtureCluster).rbdGroupControl)

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's control when they exist.
var _ = collectDiagnosticFiles(
	"rbd-group-cleanup.json",
	"rbd-group-modify.json",
	"rbd-group-results.json",
	"rbd-group-setup.json",
)

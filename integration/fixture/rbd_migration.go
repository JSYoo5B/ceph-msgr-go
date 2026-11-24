package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The RBD migration acceptance reuses the RBD base scope (token, owned pool,
// both identities and the second owned pool of the group acceptance) and
// leaves every mutation to native librbd. The Go test reads the image
// directories, the trash, the image and group headers and the data objects
// with raw CLASS calls and object reads, holds a header watch where librbd
// lists watchers, and compares what it saw with native librados exec of the
// same inputs, native librbd listings and native librados reads published by
// this control. The closed sequence is
//
//   - setup: the source image s (layering, exclusive-lock, object-map and
//     fast-diff) with writes before and after one user snapshot, a group in
//     the first pool holding s, and the abort source image a with its own
//     writes;
//   - watched: a migration prepare of s while the Go client watches s's
//     header, which librbd refuses with EBUSY before any mutation;
//   - group-snapshot: a group snapshot of the group, so s holds a
//     group-namespace snapshot, then a migration prepare of s, which librbd
//     refuses with EBUSY, then the removal of the group snapshot;
//   - prepare: s migrated to the destination d in the second owned pool;
//   - execute: the migration executed while the Go client watches d's
//     header and acknowledges every notification;
//   - commit: the migration committed;
//   - abort-prepare: a migrated to the abort destination b in the second
//     owned pool;
//   - abort-watched: a migration abort while the Go client watches b's
//     header, which librbd refuses with EBUSY;
//   - abort: the migration aborted;
//   - cleanup: d removed from the group, the group, d and a removed, each
//     removal followed by a native absence read.
//
// Every native action is journaled in rbd-base-owned.json before it is
// issued and is never reissued; whatever an uncertain action leaves goes
// with the owned pools at fixture cleanup. Migration-record, trash and group
// decoding stay in the test-only consumer (integration/rbd_migration_test.go).
// Native behavior relied upon, at rbdBaseSource: librbd/api/Migration.cc
// (prepare, execute, commit and abort; the watcher refusals; the refusal of
// non-user snapshots; the source moved to the trash with source MIGRATION;
// the group membership moved to the destination and back; set_state and
// notify_update), librbd/deep_copy/SnapshotCopyRequest.cc (the destination's
// snapshots), cls/rbd/cls_rbd.cc (migration_get, set_migration and the
// MIGRATING feature, trash_get, image_group_get, group_image_list,
// get_features, get_snapcontext, snapshot_get, dir_get_id),
// cls/rbd/cls_rbd_types.cc, pybind/rbd/rbd.pyx and pybind/rados/rados.pyx
// (LGPL-2.1 references; no code copied).

// capRBDMigration admits the migration acceptance. Its test runs alone on a
// fresh RBD fixture, so the owned pools hold only the base image besides the
// migration's images.
var capRBDMigration = defineExclusiveCapability(fixtureRBD, "rbd-migration",
	"run one test alone on a fresh RBD fixture with a second owned pool and the native image migration sequence",
	"RBD=1", nil)

// rbdMigrationActions is the closed, ordered action sequence of the
// migration control. Each action runs at most once and only after its
// predecessors. rbdMigrationRefusing are the actions whose native call
// librbd must refuse.
var (
	rbdMigrationActions  = []string{"setup", "watched", "group-snapshot", "prepare", "execute", "commit", "abort-prepare", "abort-watched", "abort", "cleanup"}
	rbdMigrationRefusing = []string{"watched", "group-snapshot", "abort-watched"}
)

// rbdMigrationKeys are the image keys: the source s and its destination d,
// the abort source a and its destination b.
var rbdMigrationKeys = []string{"s", "d", "a", "b"}

// rbdMigrationPlan is the fixed recipe of the migration acceptance. Pool
// holds the group, the base image and both sources; SecondPool holds both
// destinations.
type rbdMigrationPlan struct {
	Pool             string          `json:"pool"`
	SecondPool       string          `json:"second_pool"`
	BaseImage        string          `json:"base_image"`
	Group            string          `json:"group"`
	GroupSnapshot    string          `json:"group_snapshot"`
	Snapshot         string          `json:"snapshot"`
	Source           string          `json:"source"`
	Destination      string          `json:"destination"`
	AbortSource      string          `json:"abort_source"`
	AbortDestination string          `json:"abort_destination"`
	Order            int             `json:"order"`
	Size             uint64          `json:"size"`
	Features         uint64          `json:"features"`
	ListPage         uint64          `json:"list_page"`
	PreWrites        []rbdBaseExtent `json:"pre_snapshot_writes"`
	PostWrites       []rbdBaseExtent `json:"post_snapshot_writes"`
	AbortWrites      []rbdBaseExtent `json:"abort_writes"`
}

// rbdMigrationPlanFor places two three-object images. The source s has
// object 0 and part of object 1 before its snapshot and, after it, part of
// object 0 rewritten and object 2 created; the abort source a has parts of
// objects 0 and 2.
func rbdMigrationPlanFor(token string) rbdMigrationPlan {
	const k = 1 << 10
	short := ""
	if len(token) >= 8 {
		short = "-" + token[:8]
	}
	return rbdMigrationPlan{Pool: "ceph-msgr-rbd-" + token, SecondPool: rbdGroupSecondPoolName(token), BaseImage: "ceph-msgr-image-" + token,
		Group: "ceph-msgr-mgroup-" + token, GroupSnapshot: "ceph-msgr-mgsnap" + short, Snapshot: "ceph-msgr-msnap" + short,
		Source: "ceph-msgr-msrc-" + token, Destination: "ceph-msgr-mdst-" + token, AbortSource: "ceph-msgr-masrc-" + token, AbortDestination: "ceph-msgr-madst-" + token,
		Order: 16, Size: 192 * k, Features: rbdGroupFeatures, ListPage: 16,
		PreWrites: []rbdBaseExtent{{0, 48 * k}, {80 * k, 16 * k}}, PostWrites: []rbdBaseExtent{{16 * k, 16 * k}, {128 * k, 8 * k}},
		AbortWrites: []rbdBaseExtent{{8 * k, 24 * k}, {136 * k, 32 * k}}}
}

func (p rbdMigrationPlan) owned(token string) bool {
	if len(token) != 32 {
		return false
	}
	want := rbdMigrationPlanFor(token)
	return p.Pool == want.Pool && p.SecondPool == want.SecondPool && p.BaseImage == want.BaseImage && p.Group == want.Group && p.GroupSnapshot == want.GroupSnapshot &&
		p.Snapshot == want.Snapshot && p.Source == want.Source && p.Destination == want.Destination && p.AbortSource == want.AbortSource &&
		p.AbortDestination == want.AbortDestination && p.Order == want.Order && p.Size == want.Size && p.Features == want.Features && p.ListPage == want.ListPage &&
		slices.Equal(p.PreWrites, want.PreWrites) && slices.Equal(p.PostWrites, want.PostWrites) && slices.Equal(p.AbortWrites, want.AbortWrites)
}

// image returns the name and pool of the image with key k.
func (p rbdMigrationPlan) image(k string) (name, pool string) {
	switch k {
	case "s":
		return p.Source, p.Pool
	case "d":
		return p.Destination, p.SecondPool
	case "a":
		return p.AbortSource, p.Pool
	case "b":
		return p.AbortDestination, p.SecondPool
	}
	return "", ""
}

// rbdMigrationState is the migration control's part of the RBD base scope:
// its plan, the last control request, the index of the next admitted action
// and the image IDs native outputs have reported so far (passed to later
// actions, so the driver can name the headers of images that left their
// directory).
type rbdMigrationState struct {
	plan          rbdMigrationPlan
	lastRequestID int64
	next          int
	ids           map[string]string
}

// The fixed native migration driver. The fixture passes only the token-bound
// plan, a closed action and the image IDs earlier actions reported; the
// driver re-checks every name. librbd runs without its cache and with
// error-level stderr only. Every native class call is a librados exec of a
// read method. An image's ID is its directory entry's, else its trash
// entry's, else the one passed in. Sources: pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource; independent driver, no code
// copied.
const rbdMigrationNativePython = `
import hashlib, json, os, struct, sys, time
import rados, rbd
identity, action, token, request_id, plan_json, ids_json = sys.argv[1:7]
plan, known = json.loads(plan_json), json.loads(ids_json)
ACTIONS = ('setup', 'watched', 'group-snapshot', 'prepare', 'execute', 'commit', 'abort-prepare', 'abort-watched', 'abort', 'cleanup')
short = '-' + token[:8]
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('second_pool') != 'ceph-msgr-rbd2-' + token
        or plan.get('base_image') != 'ceph-msgr-image-' + token or plan.get('group') != 'ceph-msgr-mgroup-' + token
        or plan.get('group_snapshot') != 'ceph-msgr-mgsnap' + short or plan.get('snapshot') != 'ceph-msgr-msnap' + short
        or plan.get('source') != 'ceph-msgr-msrc-' + token or plan.get('destination') != 'ceph-msgr-mdst-' + token
        or plan.get('abort_source') != 'ceph-msgr-masrc-' + token or plan.get('abort_destination') != 'ceph-msgr-madst-' + token
        or plan.get('list_page') != 16 or action not in ACTIONS or not request_id.isdecimal()
        or any(k not in ('s', 'd', 'a', 'b') or not isinstance(v, str) or not v or len(v) > 64 for k, v in known.items())):
    raise ValueError('native RBD migration driver scope is not owned')
OUT = '/out'
pool, second = plan['pool'], plan['second_pool']
IMAGES = {'s': (plan['source'], pool), 'd': (plan['destination'], second), 'a': (plan['abort_source'], pool), 'b': (plan['abort_destination'], second)}
CEPH_NOSNAP = 0xfffffffffffffffe
# group_image_list input: an empty cls::rbd::GroupImageSpec (ENCODE_START(1,
# 1), empty image ID, pool -1) and the page size.
GROUP_START = b'\x01\x01' + struct.pack('<I', 12) + struct.pack('<Iq', 0, -1)

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(key, phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-migration:%s:%s:%s:%d:%d' % (token, key, phase, offset, counter)).encode()).digest()
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

def error_number(error):
    number = getattr(error, 'errno', None)
    if not isinstance(number, int) or number == 0:
        raise error
    return abs(number)

def lstr(value):
    data = value.encode()
    return struct.pack('<I', len(data)) + data

def write(image, key, phase, extent):
    data = pattern(key, phase, extent['offset'], extent['length'])
    written = image.write(data, extent['offset'])
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'image': key, 'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest()}

TRASH_SOURCES = ('USER', 'MIRRORING', 'MIGRATION', 'REMOVING', 'USER_PARENT')

def trash_records(ioctx):
    # The rbd_trash omap: an id_<image id> key per entry, its value a
    # cls::rbd::TrashImageSpec (ENCODE_START, the u8 source, the name). The
    # binding's trash_list iterator names only the USER and MIRRORING sources
    # and fails on a MIGRATION entry (pybind/rbd/rbd.pyx TrashIterator).
    with rados.ReadOpCtx() as op:
        values, _ = ioctx.get_omap_vals(op, '', 'id_', 64)
        try:
            ioctx.operate_read_op(op, 'rbd_trash')
        except rados.ObjectNotFound:
            return []
        records = []
        for key, value in values:
            source, n = value[6], struct.unpack_from('<I', value, 7)[0]
            records.append((key[len('id_'):], text(value[11:11 + n]), TRASH_SOURCES[source] if source < len(TRASH_SOURCES) else str(source)))
    return records

def trash_entries(ioctx):
    return sorted('%s/%s/%s' % r for r in trash_records(ioctx))

def listed(out):
    out['images_listed'] = sorted('%s/%s' % (p, text(i)) for p in (pool, second) for i in rbd.RBD().list(ioctxs[p]))
    out['groups_listed'] = sorted(text(g) for p in (pool, second) for g in rbd.RBD().group_list(ioctxs[p]))
    out['trash_listed'] = sorted('%s/%s' % (p, e) for p in (pool, second) for e in trash_entries(ioctxs[p]))

def image_id(key):
    # The directory entry's ID, else the trash entry's, else the one an
    # earlier action reported.
    name, p = IMAGES[key]
    ioctx = ioctxs[p]
    entry = call(ioctx, 'rbd_directory', 'rbd', 'dir_get_id', lstr(name))
    if entry['code'] == 0:
        output = bytes.fromhex(entry['output_hex'])
        return text(output[4:4 + struct.unpack_from('<I', output, 0)[0]]), 'directory', entry
    for iid, trashed, _ in trash_records(ioctx):
        if trashed == name:
            return iid, 'trash', entry
    return known.get(key, ''), ('known' if key in known else ''), entry

def data_view(ioctx, key, label, snap, prefix, count):
    objects, blob = [], bytearray()
    ioctx.set_read(snap)
    try:
        for index in range(count):
            oid = '%s.%016x' % (prefix, index)
            try:
                length, _ = ioctx.stat(oid)
                data = ioctx.read(oid, length, 0) if length else b''
            except rados.ObjectNotFound:
                objects.append({'index': index, 'exists': False, 'errno': 2, 'size': 0, 'offset': len(blob), 'sha256': ''})
                continue
            if len(data) != length:
                raise ValueError('native object read is shorter than its stat')
            objects.append({'index': index, 'exists': True, 'errno': 0, 'size': length, 'offset': len(blob), 'sha256': hashlib.sha256(data).hexdigest()})
            blob += data
    finally:
        ioctx.set_read(rados.LIBRADOS_SNAP_HEAD)
    return {'label': label, 'snap_id': 0 if snap == rados.LIBRADOS_SNAP_HEAD else snap, 'objects': objects,
            'object_bytes': publish('rbd-migration-%s-%s-%s-objects.bin' % (action, key, label), bytes(blob))}

def snap_ids(entry):
    # get_snapcontext output: the u64 snap_seq, then the snapshot IDs.
    if entry['code'] != 0:
        return []
    output = bytes.fromhex(entry['output_hex'])
    count = struct.unpack_from('<I', output, 8)[0]
    return list(struct.unpack_from('<%dQ' % count, output, 12))

def observe_image(key):
    name, p = IMAGES[key]
    ioctx = ioctxs[p]
    view = {'key': key, 'name': name, 'pool': p, 'pool_id': ioctx.get_pool_id()}
    view['image_id'], view['id_source'], view['directory_call'] = image_id(key)
    view['trash'], view['status'], view['reads'] = None, None, None
    if not view['image_id']:
        return view
    iid = view['image_id']
    header = 'rbd_header.' + iid
    view['header_object'], view['header_errno'] = header, errno_of(ioctx, header)
    view['trash_call'] = call(ioctx, 'rbd_trash', 'rbd', 'trash_get', lstr(iid))
    try:
        t = rbd.RBD().trash_get(ioctx, iid)
        view['trash'] = {'id': text(t['id']), 'name': text(t['name']), 'source': text(t['source'])}
    except rbd.Error as error:
        view['trash_errno'] = error_number(error)
    view['calls'] = {}
    if view['header_errno'] == 0:
        view['calls'] = {'migration_get': call(ioctx, header, 'rbd', 'migration_get', b''),
                         'get_features': call(ioctx, header, 'rbd', 'get_features', struct.pack('<Q', CEPH_NOSNAP)),
                         'get_snapcontext': call(ioctx, header, 'rbd', 'get_snapcontext', b''),
                         'image_group_get': call(ioctx, header, 'rbd', 'image_group_get', b'')}
    if view['calls'].get('migration_get', {'code': -2})['code'] == 0:
        # librbd's migration status, only for an image with a migration
        # record: without one librbd logs its failed lookups as errors.
        try:
            info = rbd.RBD().migration_status(ioctx, name)
            view['status'] = {k: (text(v) if isinstance(v, (str, bytes)) else v) for k, v in info.items()}
        except rbd.Error as error:
            view['status_errno'] = error_number(error)
    snaps = snap_ids(view['calls'].get('get_snapcontext', {'code': -2}))
    view['snapshot_calls'] = [call(ioctx, header, 'rbd', 'snapshot_get', struct.pack('<Q', s)) for s in snaps]
    prefix, count = 'rbd_data.' + iid, plan['size'] >> plan['order']
    view['views'] = [data_view(ioctx, key, 'head', rados.LIBRADOS_SNAP_HEAD, prefix, count)] + [data_view(ioctx, key, 's%d' % s, s, prefix, count) for s in snaps]
    if view['directory_call']['code'] == 0:
        # What a native librbd reader of the image sees, head and snapshots.
        reads = {}
        try:
            with rbd.Image(ioctx, name, read_only=True) as image:
                reads['features'], reads['op_features'] = image.features(), image.op_features()
                reads['snapshots'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]
                reads['head'] = publish('rbd-migration-%s-%s-head-image.bin' % (action, key), image.read(0, image.size()))
            reads['snapshot_images'] = []
            for s in reads['snapshots']:
                with rbd.Image(ioctx, name, read_only=True) as image:
                    image.set_snap_by_id(s['id'])
                    reads['snapshot_images'].append(publish('rbd-migration-%s-%s-s%d-image.bin' % (action, key, s['id']), image.read(0, image.size())))
            reads['errno'] = 0
        except rbd.Error as error:
            reads = {'errno': error_number(error)}
        view['reads'] = reads
    return view

def observe_group(out):
    ioctx = ioctxs[pool]
    with rbd.Group(ioctx, plan['group']) as group:
        gid = text(group.id())
        members = [{'name': text(m['name']), 'pool': m['pool'], 'state': m['state']} for m in group.list_images()]
        snaps = [text(s['name']) for s in group.list_snaps()]
    header = 'rbd_group_header.' + gid
    out['group'] = {'name': plan['group'], 'id': gid, 'header_object': header, 'members': members, 'snapshots': snaps,
                    'directory_call': call(ioctx, 'rbd_group_directory', 'rbd', 'dir_get_id', lstr(plan['group'])),
                    'image_list_call': call(ioctx, header, 'rbd', 'group_image_list', GROUP_START + struct.pack('<Q', plan['list_page']))}

def refuse(out, fn, *args):
    # One native call librbd must refuse; its errno, never retried.
    started = time.monotonic_ns()
    try:
        fn(*args)
        out['refusal'] = {'errno': 0}
    except rbd.Error as error:
        out['refusal'] = {'errno': error_number(error)}
    out['refusal']['elapsed_ns'] = time.monotonic_ns() - started

cluster = rados.Rados(conffile='/tmp/ceph-msgr-test/ceph.conf', rados_id=identity[len('client.'):],
                      conf={'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'native_global_id': cluster.get_instance_id()}
ioctxs = {}
try:
    for p in (pool, second):
        ioctxs[p] = cluster.open_ioctx(p)
    out['pool_id'], out['second_pool_id'] = ioctxs[pool].get_pool_id(), ioctxs[second].get_pool_id()
    src_io, dst_io = ioctxs[pool], ioctxs[second]
    if action == 'setup':
        # The acceptance runs alone on its fixture: only the base image and
        # no group or trash entry may exist before it.
        listed(out)
        if out['images_listed'] != [pool + '/' + plan['base_image']] or out['groups_listed'] or out['trash_listed']:
            raise ValueError('owned RBD pools hold an unplanned image, group or trash entry')
        writes = []
        for key, extents in (('s', None), ('a', plan['abort_writes'])):
            name = IMAGES[key][0]
            rbd.RBD().create(src_io, name, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            with rbd.Image(src_io, name) as image:
                if key == 's':
                    writes += [write(image, key, 'pre', e) for e in plan['pre_snapshot_writes']]
                    image.flush()
                    image.create_snap(plan['snapshot'])
                    writes += [write(image, key, 'post', e) for e in plan['post_snapshot_writes']]
                else:
                    writes += [write(image, key, 'pre', e) for e in extents]
                image.flush()
        rbd.RBD().group_create(src_io, plan['group'])
        with rbd.Group(src_io, plan['group']) as group:
            group.add_image(src_io, plan['source'])
        out['writes'] = writes
    elif action == 'watched':
        refuse(out, rbd.RBD().migration_prepare, src_io, plan['source'], dst_io, plan['destination'])
    elif action == 'group-snapshot':
        with rbd.Group(src_io, plan['group']) as group:
            group.create_snap(plan['group_snapshot'])
        with rbd.Image(src_io, plan['source'], read_only=True) as image:
            out['source_snapshots_during'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in image.list_snaps()]
        refuse(out, rbd.RBD().migration_prepare, src_io, plan['source'], dst_io, plan['destination'])
        with rbd.Group(src_io, plan['group']) as group:
            group.remove_snap(plan['group_snapshot'])
    elif action == 'prepare':
        rbd.RBD().migration_prepare(src_io, plan['source'], dst_io, plan['destination'])
    elif action == 'execute':
        rbd.RBD().migration_execute(src_io, plan['source'])
    elif action == 'commit':
        rbd.RBD().migration_commit(src_io, plan['source'])
    elif action == 'abort-prepare':
        rbd.RBD().migration_prepare(src_io, plan['abort_source'], dst_io, plan['abort_destination'])
    elif action == 'abort-watched':
        refuse(out, rbd.RBD().migration_abort, src_io, plan['abort_source'])
    elif action == 'abort':
        rbd.RBD().migration_abort(src_io, plan['abort_source'])
    if action != 'cleanup':
        listed(out)
        observe_group(out)
        out['images'] = [observe_image(key) for key in ('s', 'd', 'a', 'b')]
    else:
        # Identities first, then each removal with its own native absence
        # read before the next one.
        with rbd.Group(src_io, plan['group']) as group:
            gid = text(group.id())
        removal = {'group_id': gid, 'group_header': 'rbd_group_header.' + gid, 'images': []}
        for key in ('d', 'a'):
            name, p = IMAGES[key]
            with rbd.Image(ioctxs[p], name, read_only=True) as image:
                removal['images'].append({'key': key, 'name': name, 'pool': p, 'image_id': text(image.id()), 'objects': plan['size'] >> plan['order']})
        with rbd.Group(src_io, plan['group']) as group:
            group.remove_image(dst_io, plan['destination'])
            removal['members_after'] = [{'name': text(m['name']), 'pool': m['pool'], 'state': m['state']} for m in group.list_images()]
        rbd.RBD().group_remove(src_io, plan['group'])
        removal['group_header_errno'] = errno_of(src_io, removal['group_header'])
        removal['directory_call'] = call(src_io, 'rbd_group_directory', 'rbd', 'dir_get_id', lstr(plan['group']))
        for image in removal['images']:
            io = ioctxs[image['pool']]
            with rbd.Image(io, image['name']) as opened:
                for s in list(opened.list_snaps()):
                    opened.remove_snap_by_id(s['id'])
            rbd.RBD().remove(io, image['name'])
            iid = image['image_id']
            image['header_errno'] = errno_of(io, 'rbd_header.' + iid)
            image['id_errno'] = errno_of(io, 'rbd_id.' + image['name'])
            image['object_map_errno'] = errno_of(io, 'rbd_object_map.' + iid)
            image['data_errno'] = [errno_of(io, 'rbd_data.%s.%016x' % (iid, i)) for i in range(image['objects'])]
        listed(out)
        out['removed'] = removal
finally:
    for io in ioctxs.values():
        io.close()
    cluster.shutdown()
print(json.dumps(out, sort_keys=True))
`

type rbdMigrationWrite struct {
	Image   string `json:"image"`
	Phase   string `json:"phase"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Written uint64 `json:"written"`
	SHA256  string `json:"sha256"`
}

// rbdMigrationDataView is one image's data objects at the head or at one
// snapshot, as native librados reads them.
type rbdMigrationDataView struct {
	Label       string               `json:"label"`
	SnapID      uint64               `json:"snap_id"`
	Objects     []rbdGroupObjectView `json:"objects"`
	ObjectBytes rbdBaseBlob          `json:"object_bytes"`
}

// rbdMigrationReads is what a native librbd reader of an image found, when
// its name was in its directory: features, snapshots and the bytes at the
// head and at each snapshot, or the errno of a refused open.
type rbdMigrationReads struct {
	Errno          int                `json:"errno"`
	Features       uint64             `json:"features,omitempty"`
	OpFeatures     uint64             `json:"op_features,omitempty"`
	Snapshots      []rbdGroupSnapshot `json:"snapshots,omitempty"`
	Head           *rbdBaseBlob       `json:"head,omitempty"`
	SnapshotImages []rbdBaseBlob      `json:"snapshot_images,omitempty"`
}

type rbdMigrationTrash struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Source string `json:"source"`
}

// rbdMigrationStatus is librbd's migration status as the Python binding
// returns it. The binding fills dest_pool_id from the source pool ID
// (pybind/rbd/rbd.pyx migration_status at rbdBaseSource); the test reads
// the destination pool from the migration records instead.
type rbdMigrationStatus struct {
	SourcePoolID        int64  `json:"source_pool_id"`
	SourcePoolNamespace string `json:"source_pool_namespace"`
	SourceImageName     string `json:"source_image_name"`
	SourceImageID       string `json:"source_image_id"`
	DestPoolID          int64  `json:"dest_pool_id"`
	DestPoolNamespace   string `json:"dest_pool_namespace"`
	DestImageName       string `json:"dest_image_name"`
	DestImageID         string `json:"dest_image_id"`
	State               int    `json:"state"`
	StateDescription    string `json:"state_description"`
}

// rbdMigrationImageView is one image observation.
type rbdMigrationImageView struct {
	Key           string                       `json:"key"`
	Name          string                       `json:"name"`
	Pool          string                       `json:"pool"`
	PoolID        int64                        `json:"pool_id"`
	ImageID       string                       `json:"image_id"`
	IDSource      string                       `json:"id_source"`
	DirectoryCall *rbdWriteClassCall           `json:"directory_call"`
	Status        *rbdMigrationStatus          `json:"status"`
	StatusErrno   int                          `json:"status_errno,omitempty"`
	HeaderObject  string                       `json:"header_object,omitempty"`
	HeaderErrno   int                          `json:"header_errno,omitempty"`
	TrashCall     *rbdWriteClassCall           `json:"trash_call,omitempty"`
	Trash         *rbdMigrationTrash           `json:"trash"`
	TrashErrno    int                          `json:"trash_errno,omitempty"`
	Calls         map[string]rbdWriteClassCall `json:"calls,omitempty"`
	SnapshotCalls []rbdWriteClassCall          `json:"snapshot_calls,omitempty"`
	Views         []rbdMigrationDataView       `json:"views,omitempty"`
	Reads         *rbdMigrationReads           `json:"reads"`
}

type rbdMigrationGroupView struct {
	Name          string             `json:"name"`
	ID            string             `json:"id"`
	HeaderObject  string             `json:"header_object"`
	Members       []rbdGroupMember   `json:"members"`
	Snapshots     []string           `json:"snapshots"`
	DirectoryCall *rbdWriteClassCall `json:"directory_call"`
	ImageListCall *rbdWriteClassCall `json:"image_list_call"`
}

type rbdMigrationRefusal struct {
	Errno     int   `json:"errno"`
	ElapsedNS int64 `json:"elapsed_ns"`
}

type rbdMigrationRemovedImage struct {
	Key            string `json:"key"`
	Name           string `json:"name"`
	Pool           string `json:"pool"`
	ImageID        string `json:"image_id"`
	Objects        uint64 `json:"objects"`
	HeaderErrno    int    `json:"header_errno"`
	IDErrno        int    `json:"id_errno"`
	ObjectMapErrno int    `json:"object_map_errno"`
	DataErrno      []int  `json:"data_errno"`
}

type rbdMigrationRemoval struct {
	GroupID          string                     `json:"group_id"`
	GroupHeader      string                     `json:"group_header"`
	Images           []rbdMigrationRemovedImage `json:"images"`
	MembersAfter     []rbdGroupMember           `json:"members_after"`
	GroupHeaderErrno int                        `json:"group_header_errno"`
	DirectoryCall    *rbdWriteClassCall         `json:"directory_call"`
}

// rbdMigrationNative is one complete migration-driver output; unknown
// fields are refused.
type rbdMigrationNative struct {
	Action                string                  `json:"action"`
	Token                 string                  `json:"token"`
	RequestID             int64                   `json:"request_id"`
	Pool                  string                  `json:"pool"`
	PoolID                int64                   `json:"pool_id"`
	SecondPoolID          int64                   `json:"second_pool_id"`
	NativeGlobalID        uint64                  `json:"native_global_id"`
	ImagesListed          []string                `json:"images_listed"`
	GroupsListed          []string                `json:"groups_listed"`
	TrashListed           []string                `json:"trash_listed"`
	Writes                []rbdMigrationWrite     `json:"writes,omitempty"`
	Refusal               *rbdMigrationRefusal    `json:"refusal,omitempty"`
	SourceSnapshotsDuring []rbdGroupSnapshot      `json:"source_snapshots_during,omitempty"`
	Group                 *rbdMigrationGroupView  `json:"group,omitempty"`
	Images                []rbdMigrationImageView `json:"images,omitempty"`
	Removed               *rbdMigrationRemoval    `json:"removed,omitempty"`
}

// rbdMigrationHeaderMethods are the header reads of every observation of an
// existing header.
var rbdMigrationHeaderMethods = []string{"get_features", "get_snapcontext", "image_group_get", "migration_get"}

func rbdMigrationBlobValid(blob *rbdBaseBlob, name string, length uint64) bool {
	return blob != nil && blob.Name == name && blob.Length == length && len(blob.SHA256) == 64
}

// rbdMigrationImageViewValid checks the structure of one image
// observation; which records, entries and contents it must show is the
// test's judgment.
func rbdMigrationImageViewValid(action string, v rbdMigrationImageView, key string, plan rbdMigrationPlan) bool {
	name, pool := plan.image(key)
	ok := v.Key == key && v.Name == name && v.Pool == pool && v.PoolID >= 0 && rbdGroupCallValid(v.DirectoryCall, "rbd_directory", "rbd", "dir_get_id") &&
		slices.Contains([]string{"directory", "trash", "known", ""}, v.IDSource) && (v.IDSource == "") == (v.ImageID == "") &&
		(v.Status == nil || v.StatusErrno == 0) && v.StatusErrno >= 0 && v.StatusErrno < 4096
	if !ok {
		return false
	}
	if v.ImageID == "" {
		return v.HeaderObject == "" && v.TrashCall == nil && v.Trash == nil && v.TrashErrno == 0 && len(v.Calls) == 0 && len(v.SnapshotCalls) == 0 && len(v.Views) == 0 && v.Reads == nil &&
			v.DirectoryCall.Code == -2
	}
	ok = rbdGroupIDValid(v.ImageID) && v.HeaderObject == "rbd_header."+v.ImageID && (v.HeaderErrno == 0 || v.HeaderErrno == 2) &&
		rbdGroupCallValid(v.TrashCall, "rbd_trash", "rbd", "trash_get") && (v.Trash != nil) != (v.TrashErrno > 0) && v.TrashErrno < 4096 &&
		len(v.SnapshotCalls) <= 4 && len(v.Views) == len(v.SnapshotCalls)+1 && (v.Reads != nil) == (v.DirectoryCall.Code == 0)
	if v.HeaderErrno == 0 {
		ok = ok && slices.Equal(slices.Sorted(maps.Keys(v.Calls)), rbdMigrationHeaderMethods)
		for method, call := range v.Calls {
			ok = ok && rbdGroupCallValid(&call, v.HeaderObject, "rbd", method)
		}
	} else {
		ok = ok && len(v.Calls) == 0 && len(v.SnapshotCalls) == 0
	}
	for i := range v.SnapshotCalls {
		ok = ok && rbdGroupCallValid(&v.SnapshotCalls[i], v.HeaderObject, "rbd", "snapshot_get")
	}
	objects := plan.Size >> plan.Order
	for i, view := range v.Views {
		var offset uint64
		ok = ok && len(view.Objects) == int(objects) && (i == 0) == (view.Label == "head") && (i == 0) == (view.SnapID == 0) && (i == 0 || view.Label == fmt.Sprintf("s%d", view.SnapID))
		for j, o := range view.Objects {
			ok = ok && o.Index == uint64(j) && o.Offset == offset && o.Size <= 1<<plan.Order &&
				(o.Exists && o.Errno == 0 && len(o.SHA256) == 64 || !o.Exists && o.Errno == 2 && o.Size == 0 && o.SHA256 == "")
			offset += o.Size
		}
		ok = ok && rbdMigrationBlobValid(&view.ObjectBytes, "rbd-migration-"+action+"-"+key+"-"+view.Label+"-objects.bin", offset) && offset <= rbdBaseBlobLimit
	}
	if r := v.Reads; r != nil {
		if r.Errno != 0 {
			ok = ok && r.Errno > 0 && r.Errno < 4096 && r.Head == nil && r.Snapshots == nil && r.SnapshotImages == nil
		} else {
			ok = ok && rbdMigrationBlobValid(r.Head, "rbd-migration-"+action+"-"+key+"-head-image.bin", plan.Size) && len(r.Snapshots) <= 4 && len(r.SnapshotImages) == len(r.Snapshots)
			for i, s := range r.Snapshots {
				if i < len(r.SnapshotImages) {
					ok = ok && s.ID != 0 && len(s.Name) <= 512 && rbdMigrationBlobValid(&r.SnapshotImages[i], fmt.Sprintf("rbd-migration-%s-%s-s%d-image.bin", action, key, s.ID), plan.Size)
				}
			}
		}
	}
	return ok
}

// decodeRBDMigrationNative accepts exactly the shape of one admitted
// action for the owned plan. It checks structure only; the records, entries,
// refusals and contents are the test's judgment, so a mismatch is reported
// by the test instead of stopping the control loop.
func decodeRBDMigrationNative(action, token string, request int64, plan rbdMigrationPlan, data []byte) (rbdMigrationNative, error) {
	var result rbdMigrationNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD migration output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdMigrationNative{}, fmt.Errorf("native RBD migration output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.PoolID < 0 || result.SecondPoolID < 0 ||
		result.PoolID == result.SecondPoolID || result.NativeGlobalID == 0 {
		return rbdMigrationNative{}, errors.New("native RBD migration output identity differs")
	}
	ok := len(result.ImagesListed) <= 16 && len(result.GroupsListed) <= 16 && len(result.TrashListed) <= 16 &&
		slices.Contains(rbdMigrationRefusing, action) == (result.Refusal != nil) && (result.Refusal == nil || result.Refusal.Errno >= 0 && result.Refusal.Errno < 4096 && result.Refusal.ElapsedNS > 0) &&
		(action == "group-snapshot") == (result.SourceSnapshotsDuring != nil) && len(result.SourceSnapshotsDuring) <= 4
	if action == "setup" {
		var want []rbdMigrationWrite
		for _, w := range []struct {
			key, phase string
			extents    []rbdBaseExtent
		}{{"s", "pre", plan.PreWrites}, {"s", "post", plan.PostWrites}, {"a", "pre", plan.AbortWrites}} {
			for _, e := range w.extents {
				want = append(want, rbdMigrationWrite{Image: w.key, Phase: w.phase, Offset: e.Offset, Length: e.Length, Written: e.Length})
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
	} else {
		ok = ok && result.Writes == nil
	}
	if action != "cleanup" {
		g := result.Group
		ok = ok && result.Removed == nil && g != nil && g.Name == plan.Group && rbdGroupIDValid(g.ID) && g.HeaderObject == "rbd_group_header."+g.ID && len(g.Members) <= 4 &&
			len(g.Snapshots) <= 4 && rbdGroupCallValid(g.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id") && rbdGroupCallValid(g.ImageListCall, g.HeaderObject, "rbd", "group_image_list") &&
			len(result.Images) == len(rbdMigrationKeys)
		for i, v := range result.Images {
			if i < len(rbdMigrationKeys) {
				ok = ok && rbdMigrationImageViewValid(action, v, rbdMigrationKeys[i], plan)
			}
		}
	} else {
		r := result.Removed
		ok = ok && result.Group == nil && result.Images == nil && r != nil && rbdGroupIDValid(r.GroupID) && r.GroupHeader == "rbd_group_header."+r.GroupID && len(r.Images) == 2 &&
			len(r.MembersAfter) <= 4 && (r.GroupHeaderErrno == 0 || r.GroupHeaderErrno == 2) && rbdGroupCallValid(r.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id")
		if r != nil {
			for i, image := range r.Images {
				if i >= 2 {
					break
				}
				key := []string{"d", "a"}[i]
				name, pool := plan.image(key)
				errnoOK := func(e int) bool { return e == 0 || e == 2 }
				ok = ok && image.Key == key && image.Name == name && image.Pool == pool && rbdGroupIDValid(image.ImageID) && image.Objects == plan.Size>>plan.Order &&
					errnoOK(image.HeaderErrno) && errnoOK(image.IDErrno) && errnoOK(image.ObjectMapErrno) && len(image.DataErrno) == int(image.Objects) &&
					!slices.ContainsFunc(image.DataErrno, func(e int) bool { return !errnoOK(e) })
			}
		}
	}
	if !ok {
		return rbdMigrationNative{}, fmt.Errorf("native RBD migration %s output shape differs", action)
	}
	return result, nil
}

// checkRBDMigrationBlobs verifies every published binary view of one
// action.
func checkRBDMigrationBlobs(out string, images []rbdMigrationImageView) error {
	for _, image := range images {
		blobs := []rbdBaseBlob{}
		for _, view := range image.Views {
			blobs = append(blobs, view.ObjectBytes)
		}
		if r := image.Reads; r != nil && r.Head != nil {
			blobs = append(append(blobs, *r.Head), r.SnapshotImages...)
		}
		for _, blob := range blobs {
			if err := checkRBDGroupBlob(out, blob); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *fixtureCluster) rbdMigrationRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdMigrationNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.migration.plan)
	if err != nil {
		return nil, rbdMigrationNative{}, err
	}
	ids, err := json.Marshal(x.migration.ids)
	if err != nil {
		return nil, rbdMigrationNative{}, err
	}
	argv := []string{"python3", "-c", rbdMigrationNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan), string(ids)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: 90 * time.Second, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD migration " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdMigrationNative{}, errors.Join(err, fmt.Errorf("native RBD migration %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDMigrationNative(action, x.token, request, x.migration.plan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDMigrationRequest admits <id> <ownership token> <action> where the
// action is the next one of rbdMigrationActions (next is its index).
func parseRBDMigrationRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD migration request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD migration request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD migration request identity")
	}
	if !slices.Contains(rbdMigrationActions, fields[2]) {
		return 0, "", errors.New("RBD migration action is not admitted")
	}
	if next >= len(rbdMigrationActions) || fields[2] != rbdMigrationActions[next] {
		return 0, "", errors.New("RBD migration action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdMigrationTarget names what one action changes in the mutation journal.
func rbdMigrationTarget(plan rbdMigrationPlan, action string) string {
	src, dst := plan.Pool+"/"+plan.Source, plan.SecondPool+"/"+plan.Destination
	asrc, adst := plan.Pool+"/"+plan.AbortSource, plan.SecondPool+"/"+plan.AbortDestination
	switch action {
	case "setup":
		return src + "@" + plan.Snapshot + "," + asrc + "," + plan.Pool + "/" + plan.Group
	case "watched":
		return src + "->" + dst + ":refused"
	case "group-snapshot":
		return plan.Pool + "/" + plan.Group + "@" + plan.GroupSnapshot + "," + src + "->" + dst + ":refused"
	case "prepare", "execute", "commit":
		return src + "->" + dst + ":" + action
	case "abort-prepare":
		return asrc + "->" + adst + ":prepare"
	case "abort-watched":
		return asrc + "->" + adst + ":abort-refused"
	case "abort":
		return asrc + "->" + adst + ":abort"
	}
	return plan.Pool + "/" + plan.Group + "," + dst + "," + asrc + ":removed"
}

// rbdMigrationControl runs one native action of the closed migration
// sequence, only on a profile that admits capRBDMigration and after the
// second owned pool was created. Every action is journaled as a mutation;
// the refused actions too, since librbd may refuse after it changed
// something.
func (c *fixtureCluster) rbdMigrationControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || !c.profile.admits(capRBDMigration) || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) ||
		!x.group.secondPoolIntent || x.group.secondPool == nil || x.group.secondPoolName != rbdGroupSecondPoolName(x.token) {
		return errors.New("invalid owned RBD migration control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 120*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD migration control requested after cleanup began")
	}
	if x.migration.plan.Pool == "" {
		x.migration.plan, x.migration.ids = rbdMigrationPlanFor(x.token), map[string]string{}
	}
	if !x.migration.plan.owned(x.token) {
		return errors.New("RBD migration plan is not bound to its token")
	}
	id, action, err := parseRBDMigrationRequest(fields, x.token, x.migration.lastRequestID, x.migration.next)
	if err != nil {
		return err
	}
	x.migration.lastRequestID = id
	x.migration.next++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.migration.plan, "known_ids": maps.Clone(x.migration.ids)}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-migration-"+action+".json", result))
	}()
	m, err := c.rbdBaseBegin("native-migration-"+action, rbdMigrationTarget(x.migration.plan, action), id)
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdMigrationRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && (native.PoolID != x.poolID || native.SecondPoolID != x.group.secondPool.ID) {
		err = errors.New("native RBD migration action used another pool")
	}
	if err == nil {
		err = checkRBDMigrationBlobs(out, native.Images)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	// The IDs later actions pass on: an image keeps the ID it first had.
	for _, image := range native.Images {
		if image.ImageID != "" && x.migration.ids[image.Key] == "" {
			x.migration.ids[image.Key] = image.ImageID
		}
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

var _ = defineRequestControl("rbd-migration", controlAfterRanked, (*fixtureCluster).rbdMigrationControl)

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's control when they exist.
var _ = collectDiagnosticFiles(
	"rbd-migration-abort-prepare.json",
	"rbd-migration-abort-watched.json",
	"rbd-migration-abort.json",
	"rbd-migration-cleanup.json",
	"rbd-migration-commit.json",
	"rbd-migration-execute.json",
	"rbd-migration-group-snapshot.json",
	"rbd-migration-prepare.json",
	"rbd-migration-results.json",
	"rbd-migration-setup.json",
	"rbd-migration-watched.json",
)

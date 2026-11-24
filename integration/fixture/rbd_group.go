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
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// The RBD group-snapshot acceptance reuses the RBD base scope (token, owned
// pool and both identities), adds a second owned pool and, on the test's
// request only, two native librbd images (exclusive-lock, object-map and
// fast-diff members, one in each pool) in one native RBD group held in the first
// pool. Every mutation stays with native librbd: the Go test reads the group
// header, the member image headers and the data objects, holds one member
// header watch and answers its notifications, and compares what it saw with
// native librbd listings and native librados reads published by this
// control. The closed sequence is
//
//   - setup: images, contents, group, membership and the first group
//     snapshot;
//   - modify: new data on both live heads;
//   - refused: a second group snapshot whose quiesce the Go watcher refuses,
//     so native librbd compensates;
//   - accepted: a second group snapshot whose quiesce the Go watcher accepts;
//   - timed-out: a group snapshot created with plan.QuiesceAttempts quiesce
//     attempts, none of which the Go watcher answers, so every attempt times
//     out at the OSD and native librbd compensates;
//   - retried: a third group snapshot with the same attempt count, whose
//     first quiesce the Go watcher leaves to time out, whose retry it accepts
//     and whose unquiesce it never answers;
//   - over-limit: a group snapshot creation after native librbd set member
//     a's snapshot limit to its current snapshot count, so cls_rbd refuses
//     member a's snapshot with EDQUOT while member b's is created and native
//     librbd removes member b's snapshot and the record; the limit is then
//     removed natively;
//   - lock-refused: a group snapshot creation while the Go test holds member
//     b's exclusive lock in librbd's managed-lock form on its member header
//     watch and answers native librbd's lock request with -EROFS, so the
//     member lock acquisition fails and native librbd compensates;
//   - complete-refused: a group snapshot creation during whose quiesce the
//     Go test replaces the INCOMPLETE record with one of its own under the
//     same name, so cls_rbd refuses the COMPLETE record with EEXIST after
//     both member snapshots exist and native librbd removes them;
//   - rollback: the group rolled back to the first group snapshot;
//   - cleanup: the three group snapshots, the membership, the group and both
//     images removed natively, each removal followed by a native absence
//     read.
//
// During every creation the native driver creates the group snapshot
// on a second thread while its main thread independently reads the group
// header and the member headers through librados exec of read methods, tries
// a rollback to the snapshot being created, and publishes what it saw before
// the Go watcher answers. Every native action is journaled in
// rbd-base-owned.json before it is issued and is never reissued; whatever an
// uncertain action leaves goes with the owned pools at fixture cleanup.
// Group-record, notification and snapshot decoding stay in the test-only
// consumer (integration/rbd_group_test.go). Native behavior relied upon, at
// rbdBaseSource: librbd/api/Group.cc (membership, snapshot creation with its
// INCOMPLETE record, quiesce, exclusive locks, COMPLETE record, unquiesce and
// both compensations; rollback, snapshot removal, the
// ".group.<pool>_<group id>_<snapshot id>" member snapshot name),
// librbd/ImageWatcher.cc (quiesce notification, its responses and its
// retry after a timeout), librbd/watcher/Notifier.cc (the 5 s notify
// timeout), common/options/rbd.yaml.in (rbd_quiesce_notification_attempts),
// cls/rbd/cls_rbd.cc (group_snap_list, snapshot_get, snapshot_add and its
// snapshot limit, snapshot_get_limit, get_snapcontext, dir_get_id),
// cls/lock/cls_lock.cc (get_info), cls/rbd/cls_rbd_types.cc,
// pybind/rbd/rbd.pyx and pybind/rados/rados.pyx (LGPL-2.1 references; no
// code copied).

// capRBDGroupSnapshot admits the group-snapshot acceptance. Its test runs
// alone on a fresh RBD fixture, so the owned pools hold only the base image
// besides the group's two images.
var capRBDGroupSnapshot = defineExclusiveCapability(fixtureRBD, "rbd-group-snapshot",
	"run one test alone on a fresh RBD fixture with a second owned pool and the native group-snapshot sequence",
	"RBD=1", nil)

// rbdGroupActions is the closed, ordered action sequence of the group
// control. Each action runs at most once and only after its predecessors.
var rbdGroupActions = []string{"setup", "modify", "refused", "accepted", "timed-out", "retried", "over-limit", "lock-refused", "complete-refused", "rollback", "cleanup"}

// rbdGroupObserved are the actions whose output carries a group and image
// observation; rbdGroupCreating are those that create a group snapshot under
// a Go-answered quiesce.
var (
	rbdGroupObserved = []string{"setup", "modify", "refused", "accepted", "timed-out", "retried", "over-limit", "lock-refused", "complete-refused", "rollback"}
	rbdGroupCreating = []string{"refused", "accepted", "timed-out", "retried", "over-limit", "lock-refused", "complete-refused"}
)

// rbdGroupCreationPhases are the native observations each creation publishes
// while the creating thread waits on the Go watcher: the INCOMPLETE record
// (with the rollback attempt), the COMPLETE record of an accepted creation,
// member b's snapshot while the over-limit creation compensates, member a's
// lock while the lock-refused creation compensates, and both member
// snapshots while the complete-refused creation compensates.
var rbdGroupCreationPhases = map[string][]string{
	"refused":          {"incomplete-observed"},
	"accepted":         {"incomplete-observed", "complete-observed"},
	"timed-out":        {"incomplete-observed"},
	"retried":          {"incomplete-observed", "complete-observed"},
	"over-limit":       {"incomplete-observed", "member-snapshot-observed"},
	"lock-refused":     {"incomplete-observed", "member-lock-observed"},
	"complete-refused": {"incomplete-observed", "member-snapshots-observed"},
}

// rbdGroupQuiesceAttempts is the rbd_quiesce_notification_attempts the
// native creating client sets for the timed-out and retried creations; the
// others keep librbd's default, rbdGroupDefaultQuiesceAttempts
// (ceph/ceph/blob/v20.2.4/src/common/options/rbd.yaml.in).
const (
	rbdGroupQuiesceAttempts        = 2
	rbdGroupDefaultQuiesceAttempts = 10
)

// createdSnapshot names the group snapshot a creating action creates.
func (p rbdGroupPlan) createdSnapshot(action string) string {
	return map[string]string{"refused": p.RefusedSnapshot, "accepted": p.SecondSnapshot, "timed-out": p.TimedOutSnapshot, "retried": p.RetriedSnapshot,
		"over-limit": p.OverLimitSnapshot, "lock-refused": p.LockRefusedSnapshot, "complete-refused": p.CompleteRefusedSnapshot}[action]
}

// rbdGroupQuiesceRefusal is the result the Go watcher answers the refused
// creation's quiesce with: -EBUSY in a ResponseMessage.
const rbdGroupQuiesceRefusal = -16

// rbdGroupBlobName names one published binary view: the image bytes or the
// concatenated objects of image k at view label v (head, or s<snapshot id>)
// for action a.
func rbdGroupBlobName(action, image, label, kind string) string {
	return "rbd-group-" + action + "-" + image + "-" + label + "-" + kind + ".bin"
}

// rbdGroupImagePlan is one member image: its owned pool and its writes before
// the first group snapshot (setup) and after it (modify).
type rbdGroupImagePlan struct {
	Key        string          `json:"key"`
	Name       string          `json:"name"`
	Pool       string          `json:"pool"`
	PreWrites  []rbdBaseExtent `json:"pre_snapshot_writes"`
	PostWrites []rbdBaseExtent `json:"post_snapshot_writes"`
}

// rbdGroupPlan is the fixed recipe of the group acceptance. Pool holds the
// group, the base image and image a; SecondPool holds image b. ListPage is
// the page size of the paged group_snap_list calls, PollPage that of the
// single-call listings during creation. QuiesceRefusal is the Go watcher's
// answer to the refused creation's quiesce; QuiesceAttempts is the native
// creating client's rbd_quiesce_notification_attempts for the timed-out and
// retried creations. The over-limit creation limits member a (Images[0]).
type rbdGroupPlan struct {
	Pool              string `json:"pool"`
	SecondPool        string `json:"second_pool"`
	BaseImage         string `json:"base_image"`
	Group             string `json:"group"`
	GroupSnapshot     string `json:"group_snapshot"`
	RefusedSnapshot   string `json:"refused_snapshot"`
	SecondSnapshot    string `json:"second_snapshot"`
	TimedOutSnapshot  string `json:"timed_out_snapshot"`
	RetriedSnapshot   string `json:"retried_snapshot"`
	OverLimitSnapshot string `json:"over_limit_snapshot"`
	// LockRefusedSnapshot and CompleteRefusedSnapshot are the names the
	// lock-refused and complete-refused creations use.
	LockRefusedSnapshot     string              `json:"lock_refused_snapshot"`
	CompleteRefusedSnapshot string              `json:"complete_refused_snapshot"`
	Order                   int                 `json:"order"`
	Size                    uint64              `json:"size"`
	Features                uint64              `json:"features"`
	ListPage                uint64              `json:"list_page"`
	PollPage                uint64              `json:"poll_page"`
	QuiesceRefusal          int                 `json:"quiesce_refusal"`
	QuiesceAttempts         int                 `json:"quiesce_attempts"`
	Images                  []rbdGroupImagePlan `json:"images"`
}

// rbdGroupFeatures is layering, exclusive-lock, object-map and fast-diff
// (librbd enables fast-diff with object-map unless told otherwise): librbd
// acquires each member's exclusive lock after the quiesce and before the
// member snapshots (ceph/ceph/blob/v20.2.4/src/include/rbd/features.h).
const rbdGroupFeatures = 1 | 4 | 8 | 16

// rbdGroupPlanFor places two three-object images with different contents.
// Image a: object 0 and part of object 1 before the first snapshot; after it,
// part of object 0 rewritten and object 2 created. Image b: parts of objects
// 0 and 2 before; after it, object 1 created and part of object 2 rewritten.
// Each image's first snapshot view therefore differs from its later head in
// an existing object and in an object created after the snapshot.
func rbdGroupPlanFor(token string) rbdGroupPlan {
	const k = 1 << 10
	short := ""
	if len(token) >= 8 {
		short = "-" + token[:8]
	}
	return rbdGroupPlan{Pool: "ceph-msgr-rbd-" + token, SecondPool: rbdGroupSecondPoolName(token), BaseImage: "ceph-msgr-image-" + token, Group: "ceph-msgr-group-" + token,
		GroupSnapshot: "ceph-msgr-gsnap" + short, RefusedSnapshot: "ceph-msgr-gsnap-refused" + short, SecondSnapshot: "ceph-msgr-gsnap-second" + short,
		TimedOutSnapshot: "ceph-msgr-gsnap-timedout" + short, RetriedSnapshot: "ceph-msgr-gsnap-retried" + short, OverLimitSnapshot: "ceph-msgr-gsnap-overlimit" + short,
		LockRefusedSnapshot: "ceph-msgr-gsnap-lockrefused" + short, CompleteRefusedSnapshot: "ceph-msgr-gsnap-completerefused" + short,
		Order: 16, Size: 192 * k, Features: rbdGroupFeatures, ListPage: 1, PollPage: 16, QuiesceRefusal: rbdGroupQuiesceRefusal, QuiesceAttempts: rbdGroupQuiesceAttempts,
		Images: []rbdGroupImagePlan{
			{Key: "a", Name: "ceph-msgr-gimage-a-" + token, Pool: "ceph-msgr-rbd-" + token, PreWrites: []rbdBaseExtent{{0, 48 * k}, {80 * k, 16 * k}}, PostWrites: []rbdBaseExtent{{16 * k, 16 * k}, {128 * k, 8 * k}}},
			{Key: "b", Name: "ceph-msgr-gimage-b-" + token, Pool: rbdGroupSecondPoolName(token), PreWrites: []rbdBaseExtent{{8 * k, 24 * k}, {136 * k, 32 * k}}, PostWrites: []rbdBaseExtent{{64 * k, 12 * k}, {144 * k, 8 * k}}},
		}}
}

func rbdGroupSecondPoolName(token string) string { return "ceph-msgr-rbd2-" + token }

func (p rbdGroupPlan) owned(token string) bool {
	if len(token) != 32 {
		return false
	}
	short := "-" + token[:8]
	return p.Pool == "ceph-msgr-rbd-"+token && p.SecondPool == rbdGroupSecondPoolName(token) && p.BaseImage == "ceph-msgr-image-"+token && p.Group == "ceph-msgr-group-"+token &&
		p.GroupSnapshot == "ceph-msgr-gsnap"+short && p.RefusedSnapshot == "ceph-msgr-gsnap-refused"+short && p.SecondSnapshot == "ceph-msgr-gsnap-second"+short &&
		p.TimedOutSnapshot == "ceph-msgr-gsnap-timedout"+short && p.RetriedSnapshot == "ceph-msgr-gsnap-retried"+short && p.OverLimitSnapshot == "ceph-msgr-gsnap-overlimit"+short &&
		p.LockRefusedSnapshot == "ceph-msgr-gsnap-lockrefused"+short && p.CompleteRefusedSnapshot == "ceph-msgr-gsnap-completerefused"+short &&
		p.Order == 16 && p.Size == 192<<10 && p.Features == rbdGroupFeatures && p.ListPage == 1 && p.PollPage == 16 && p.QuiesceRefusal == rbdGroupQuiesceRefusal &&
		p.QuiesceAttempts == rbdGroupQuiesceAttempts && len(p.Images) == 2 &&
		p.Images[0].Key == "a" && p.Images[0].Name == "ceph-msgr-gimage-a-"+token && p.Images[0].Pool == p.Pool &&
		p.Images[1].Key == "b" && p.Images[1].Name == "ceph-msgr-gimage-b-"+token && p.Images[1].Pool == p.SecondPool &&
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
// the last control request, the index of the next admitted action and the
// second owned pool (its name is journaled before it is created).
type rbdGroupState struct {
	plan             rbdGroupPlan
	lastRequestID    int64
	next             int
	secondPoolIntent bool
	secondPoolName   string
	secondPoolHandle *ceph.Pool
	secondPool       *rbdBasePoolPolicy
}

// prepareRBDGroupPool creates and initializes the group acceptance's second
// owned pool with the given policy (the first pool's, or the erasure-hold
// sequence's erasure-coded one, which is not initialized), before the
// identities whose capabilities name both pools. The intent is journaled
// first; a pool handle returned with an error still identifies it for
// cleanup.
func (c *fixtureCluster) prepareRBDGroupPool(ctx context.Context, config ceph.PoolConfig) error {
	x := c.rbdBase
	if x == nil || x.group.secondPoolIntent || x.goIntent || x.nativeIntent {
		return errors.New("invalid RBD group pool preparation scope")
	}
	x.group.secondPoolIntent, x.group.secondPoolName = true, rbdGroupSecondPoolName(x.token)
	config.Name = x.group.secondPoolName
	m, err := c.rbdBaseBegin("group-pool-create", config.Name, 0)
	if err != nil {
		return err
	}
	x.group.secondPoolHandle, err = c.cluster.CreatePool(ctx, config)
	if err := c.rbdBaseFinish(m, err); err != nil {
		return fmt.Errorf("create the second owned RBD pool: %w", err)
	}
	if config.ErasureCode != nil {
		// An erasure-coded pool holds no RBD metadata, only data objects of
		// images in the first pool, so `rbd pool init` does not apply; the
		// pool carries the rbd application from its creation.
		return nil
	}
	m, err = c.rbdBaseBegin("group-pool-init", config.Name, 0)
	if err != nil {
		return err
	}
	if err := c.rbdBaseFinish(m, rbd.InitPool(ctx, c.cluster, config.Name)); err != nil {
		return fmt.Errorf("initialize the second owned RBD pool: %w", err)
	}
	return nil
}

// removeRBDGroupPool removes the second owned pool through its handle (or
// the pool of the planned name, which then must exist) and proves its
// absence from a native OSDMap.
func (c *fixtureCluster) removeRBDGroupPool(ctx context.Context, receipt *rbdBaseCleanup) error {
	x := c.rbdBase
	receipt.SecondPool = x.group.secondPoolName
	pool := x.group.secondPoolHandle
	var err error
	if pool == nil {
		pool, err = c.cluster.Pool(ctx, x.group.secondPoolName)
	}
	if err == nil {
		err = pool.Remove(ctx)
	}
	receipt.SecondPoolRemoved = err == nil
	data, readErr := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	var dump struct {
		FSID  string `json:"fsid"`
		Pools []struct {
			ID   int64  `json:"pool"`
			Name string `json:"pool_name"`
		} `json:"pools"`
	}
	if readErr == nil && (len(data) > mdsFreshJSONLimit || json.Unmarshal(data, &dump) != nil || dump.FSID != c.profile.fsid || dump.Pools == nil) {
		readErr = errors.New("RBD group cleanup native OSDMap unavailable")
	}
	if readErr == nil {
		receipt.SecondPoolAbsentNatively = true
		for _, p := range dump.Pools {
			if p.Name == x.group.secondPoolName || x.group.secondPool != nil && p.ID == x.group.secondPool.ID {
				receipt.SecondPoolAbsentNatively = false
				readErr = errors.New("second owned RBD pool remained after removal")
			}
		}
	}
	return errors.Join(err, readErr)
}

// The fixed native group driver. The fixture passes only the token-bound plan
// and a closed action; the driver re-checks every name. librbd runs without
// its cache and with error-level stderr only. Every native class call is a
// librados exec of a read method, so it changes nothing. During every
// creation the group snapshot is created on a second thread with its own
// ioctx; the main thread only reads, tries the rollback that librbd must
// refuse for an INCOMPLETE snapshot, and publishes rbd-group-<action>-state.json
// before the Go watcher answers the quiesce. Before the timed-out and retried
// creations the process sets rbd_quiesce_notification_attempts to the plan's
// count; around the over-limit creation it sets member a's snapshot limit
// through librbd and removes it afterwards. Sources: pybind/rbd/rbd.pyx and
// pybind/rados/rados.pyx at rbdBaseSource; independent driver, no code
// copied.
const rbdGroupNativePython = `
import hashlib, json, os, struct, sys, threading, time
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'modify', 'refused', 'accepted', 'timed-out', 'retried', 'over-limit', 'lock-refused', 'complete-refused', 'rollback', 'cleanup')
short = '-' + token[:8]
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('second_pool') != 'ceph-msgr-rbd2-' + token
        or plan.get('base_image') != 'ceph-msgr-image-' + token or plan.get('group') != 'ceph-msgr-group-' + token
        or plan.get('group_snapshot') != 'ceph-msgr-gsnap' + short or plan.get('refused_snapshot') != 'ceph-msgr-gsnap-refused' + short
        or plan.get('second_snapshot') != 'ceph-msgr-gsnap-second' + short
        or plan.get('timed_out_snapshot') != 'ceph-msgr-gsnap-timedout' + short
        or plan.get('retried_snapshot') != 'ceph-msgr-gsnap-retried' + short
        or plan.get('over_limit_snapshot') != 'ceph-msgr-gsnap-overlimit' + short
        or plan.get('lock_refused_snapshot') != 'ceph-msgr-gsnap-lockrefused' + short
        or plan.get('complete_refused_snapshot') != 'ceph-msgr-gsnap-completerefused' + short
        or plan.get('quiesce_attempts') != 2
        or [i.get('key') for i in plan.get('images', [])] != ['a', 'b']
        or [i.get('name') for i in plan['images']] != ['ceph-msgr-gimage-a-' + token, 'ceph-msgr-gimage-b-' + token]
        or [i.get('pool') for i in plan['images']] != [plan['pool'], plan['second_pool']]
        or plan.get('list_page') != 1 or plan.get('poll_page') != 16 or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD group driver scope is not owned')
OUT = '/out'
pool, group_name, page, poll_page = plan['pool'], plan['group'], plan['list_page'], plan['poll_page']
CREATED = {'refused': plan['refused_snapshot'], 'accepted': plan['second_snapshot'], 'timed-out': plan['timed_out_snapshot'],
           'retried': plan['retried_snapshot'], 'over-limit': plan['over_limit_snapshot'], 'lock-refused': plan['lock_refused_snapshot'],
           'complete-refused': plan['complete_refused_snapshot']}
# The creations that retry a timed-out quiesce with the plan's attempt count.
RETRYING = ('timed-out', 'retried')
# group_snap_list input: cls::rbd::GroupSnapshot (ENCODE_START(1, 1), empty id
# and name, state INCOMPLETE, no member snapshots) and the page size.
START = b'\x01\x01' + struct.pack('<I', 13) + struct.pack('<IIBI', 0, 0, 0, 0)
# cls_lock_get_info_op: ENCODE_START(1, 1) and the lock name.
LOCK_INFO = b'\x01\x01' + struct.pack('<I', 12) + struct.pack('<I', 8) + b'rbd_lock'

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

def error_number(error):
    number = getattr(error, 'errno', None)
    if not isinstance(number, int) or number == 0:
        raise error
    return abs(number)

def group_records(output):
    # The (id, name, state) of each GroupSnapshot in a group_snap_list output.
    count, pos, records = struct.unpack_from('<I', output, 0)[0], 4, []
    for _ in range(count):
        version, compat, length = struct.unpack_from('<BBI', output, pos)
        body, pos = output[pos + 6:pos + 6 + length], pos + 6 + length
        n = struct.unpack_from('<I', body, 0)[0]
        snap_id, p = body[4:4 + n].decode(), 4 + n
        n = struct.unpack_from('<I', body, p)[0]
        name, p = body[p + 4:p + 4 + n].decode(), p + 4 + n
        records.append({'id': snap_id, 'name': name, 'state': body[p]})
    if pos != len(output):
        raise ValueError('group_snap_list output has trailing bytes')
    return records

def group_snap(s):
    return {'id': text(s['id']), 'name': text(s['name']), 'state': s['state'], 'namespace_type': s['namespace_type'],
            'image_snap_name': text(s['image_snap_name']),
            'image_snaps': [{'pool_id': i['pool_id'], 'snap_id': i['snap_id'], 'image_name': text(i['image_name'])} for i in s['image_snaps']]}

def members_of(group):
    return [{'name': text(m['name']), 'pool': m['pool'], 'state': m['state']} for m in group.list_images()]

def data_view(ioctx, key, label, snap, image_bytes, prefix, count):
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
            'image': publish('rbd-group-%s-%s-%s-image.bin' % (action, key, label), image_bytes),
            'object_bytes': publish('rbd-group-%s-%s-%s-objects.bin' % (action, key, label), bytes(blob))}

def observe_group(out):
    ioctx = ioctxs[pool]
    with rbd.Group(ioctx, group_name) as group:
        group_id = text(group.id())
        members = members_of(group)
        snaps = [group_snap(s) for s in group.list_snaps()]
        infos = [group_snap(group.get_snap_info(s['name'])) for s in snaps]
    header = 'rbd_group_header.' + group_id
    # Page by page from the empty start record: each next page starts after
    # the previous page's only record, until an empty page.
    calls, start = [], START
    while len(calls) < 8:
        entry = call(ioctx, header, 'rbd', 'group_snap_list', start + struct.pack('<Q', page))
        calls.append(entry)
        output = bytes.fromhex(entry['output_hex'])
        if entry['code'] != 0 or len(output) <= 4 or struct.unpack('<I', output[:4])[0] != 1:
            break
        start = output[4:]
    out['group'] = {'name': group_name, 'id': group_id, 'header_object': header, 'members': members, 'snapshots': snaps, 'snapshot_infos': infos,
                    'directory_call': call(ioctx, 'rbd_group_directory', 'rbd', 'dir_get_id', struct.pack('<I', len(group_name)) + group_name.encode()),
                    'list_calls': calls}

def observe_image(entry):
    ioctx, name = ioctxs[entry['pool']], entry['name']
    view = {'key': entry['key'], 'name': name, 'pool': entry['pool'], 'pool_id': ioctx.get_pool_id()}
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
    header = 'rbd_header.' + view['image_id']
    view['header_object'], view['id_object'] = header, 'rbd_id.' + name
    view['snapshot_calls'] = [call(ioctx, header, 'rbd', 'snapshot_get', struct.pack('<Q', s['id'])) for s in snaps]
    view['snapcontext_call'] = call(ioctx, header, 'rbd', 'get_snapcontext', b'')
    view['lock_call'] = call(ioctx, header, 'lock', 'get_info', LOCK_INFO)
    prefix, count = stat['block_name_prefix'], stat['num_objs']
    views = [data_view(ioctx, entry['key'], 'head', rados.LIBRADOS_SNAP_HEAD, head, prefix, count)]
    for s in snaps:
        with rbd.Image(ioctx, name, read_only=True) as image:
            image.set_snap_by_id(s['id'])
            data = image.read(0, image.size())
        views.append(data_view(ioctx, entry['key'], 's%d' % s['id'], s['id'], data, prefix, count))
    view['views'] = views
    return view

def write(image, key, phase, extent):
    data = pattern(key, phase, extent['offset'], extent['length'])
    written = image.write(data, extent['offset'])
    if written != len(data):
        raise ValueError('native RBD write was short')
    return {'image': key, 'phase': phase, 'offset': extent['offset'], 'length': extent['length'], 'written': written, 'sha256': hashlib.sha256(data).hexdigest()}

def listed(out):
    out['images_listed'] = sorted('%s/%s' % (p, text(i)) for p in (pool, plan['second_pool']) for i in rbd.RBD().list(ioctxs[p]))
    out['groups_listed'] = sorted(text(g) for p in (pool, plan['second_pool']) for g in rbd.RBD().group_list(ioctxs[p]))

def create_under_quiesce(out):
    # Snapshot creation on a second thread; this thread reads and publishes.
    name, ioctx = CREATED[action], ioctxs[pool]
    with rbd.Group(ioctx, group_name) as group:
        group_id = text(group.id())
    header = 'rbd_group_header.' + group_id
    image_ids = {}
    for entry in plan['images']:
        with rbd.Image(ioctxs[entry['pool']], entry['name'], read_only=True) as image:
            image_ids[entry['key']] = text(image.id())
    creation = {'snapshot': name, 'group_id': group_id, 'image_ids': image_ids, 'phases': []}
    headers = {entry['key']: (ioctxs[entry['pool']], 'rbd_header.' + image_ids[entry['key']]) for entry in plan['images']}
    if action in RETRYING:
        cluster.conf_set('rbd_quiesce_notification_attempts', str(plan['quiesce_attempts']))
    creation['quiesce_attempts'] = int(cluster.conf_get('rbd_quiesce_notification_attempts'))
    limited = plan['images'][0]
    if action == 'over-limit':
        # cls_rbd's snapshot_add refuses a snapshot once the image holds
        # snap_limit snapshots: member a's next snapshot gets EDQUOT.
        with rbd.Image(ioctxs[limited['pool']], limited['name']) as image:
            count = len(list(image.list_snaps()))
            image.set_snap_limit(count)
            limit = image.get_snap_limit()
        creation['snap_limit'] = {'image': limited['key'], 'snapshots': count, 'limit': limit,
                                  'set_call': call(*headers[limited['key']], 'rbd', 'snapshot_get_limit', b'')}
    def snapshot_ids(key):
        # get_snapcontext output: the u64 snap_seq and the snapshot IDs.
        entry = call(*headers[key], 'rbd', 'get_snapcontext', b'')
        if entry['code'] != 0:
            raise ValueError('native get_snapcontext failed')
        output = bytes.fromhex(entry['output_hex'])
        count = struct.unpack_from('<I', output, 8)[0]
        return list(struct.unpack_from('<%dQ' % count, output, 12))
    member_before = len(snapshot_ids('b'))
    members_before = {key: len(snapshot_ids(key)) for key in headers}
    result = {}
    creator_ioctx = cluster.open_ioctx(pool)
    def create():
        started = time.monotonic_ns()
        try:
            with rbd.Group(creator_ioctx, group_name) as group:
                group.create_snap(name)
            result['errno'] = 0
        except Exception as error:
            try:
                result['errno'] = error_number(error)
            except Exception as other:
                result['error'] = repr(other)
        finally:
            result['elapsed_ns'] = time.monotonic_ns() - started
    started = time.monotonic_ns()
    worker = threading.Thread(target=create, daemon=True)
    state = {'token': token, 'request_id': int(request_id), 'native_global_id': out['native_global_id'], 'snapshot': name, 'group_id': group_id,
             'image_ids': image_ids, 'phase': 'started', 'phases': creation['phases']}
    def listing():
        return call(ioctx, header, 'rbd', 'group_snap_list', START + struct.pack('<Q', poll_page))
    def wait_record(want):
        deadline = time.monotonic() + 30
        while True:
            entry = listing()
            if entry['code'] == 0 and any(r['name'] == name and r['state'] == want for r in group_records(bytes.fromhex(entry['output_hex']))):
                return
            if not worker.is_alive():
                raise ValueError('group snapshot creation returned before its record reached state %d' % want)
            if time.monotonic() > deadline:
                raise TimeoutError('group snapshot record did not reach state %d' % want)
            time.sleep(0.005)
    def wait_member_snapshot():
        # Member b's group-namespace snapshot exists before the over-limit
        # creation's unquiesce, which the Go watcher holds.
        deadline = time.monotonic() + 30
        while len(snapshot_ids('b')) <= member_before:
            if not worker.is_alive():
                raise ValueError('group snapshot creation returned before member b held its snapshot')
            if time.monotonic() > deadline:
                raise TimeoutError('member b did not hold its group snapshot')
            time.sleep(0.005)
    def wait_member_snapshots():
        # Both members hold their group snapshot before the complete-refused
        # creation's unquiesce, which the Go watcher holds.
        deadline = time.monotonic() + 30
        while any(len(snapshot_ids(key)) <= members_before[key] for key in headers):
            if not worker.is_alive():
                raise ValueError('group snapshot creation returned before both members held their snapshot')
            if time.monotonic() > deadline:
                raise TimeoutError('the members did not hold their group snapshots')
            time.sleep(0.005)
    def wait_member_lock(key):
        # lock.get_info output: ENCODE_START's six bytes, then the u32 count
        # of lockers. Member a's lock is the creating client's before the
        # lock-refused creation's unquiesce, which the Go watcher holds.
        deadline = time.monotonic() + 30
        while True:
            entry = call(*headers[key], 'lock', 'get_info', LOCK_INFO)
            output = bytes.fromhex(entry['output_hex'])
            if entry['code'] == 0 and len(output) < 10:
                raise ValueError('native lock.get_info output is too short')
            if entry['code'] == 0 and struct.unpack_from('<I', output, 6)[0] == 1:
                return
            if not worker.is_alive():
                raise ValueError('group snapshot creation returned before member %s was locked' % key)
            if time.monotonic() > deadline:
                raise TimeoutError('member %s was not locked' % key)
            time.sleep(0.005)
    def observe(label, rollback):
        phase = {'phase': label, 'after_ns': time.monotonic_ns() - started, 'list_call': listing(), 'images': []}
        for entry in plan['images']:
            io, header_object = headers[entry['key']]
            phase['images'].append({'key': entry['key'], 'snapcontext_call': call(io, header_object, 'rbd', 'get_snapcontext', b''),
                                    'lock_call': call(io, header_object, 'lock', 'get_info', LOCK_INFO),
                                    'limit_call': call(io, header_object, 'rbd', 'snapshot_get_limit', b'')})
        if label == 'member-snapshot-observed':
            phase['member_snapshot_call'] = call(*headers['b'], 'rbd', 'snapshot_get', struct.pack('<Q', max(snapshot_ids('b'))))
        if rollback:
            try:
                with rbd.Group(ioctx, group_name) as group:
                    group.rollback_to_snap(name)
                phase['rollback_errno'] = 0
            except Exception as error:
                phase['rollback_errno'] = error_number(error)
        creation['phases'].append(phase)
        state['phase'] = label
        publish('rbd-group-%s-state.json' % action, json.dumps(state, sort_keys=True).encode())
    worker.start()
    try:
        # INCOMPLETE (0) is written before the quiesce, COMPLETE (1) after the
        # member snapshots and before the unquiesce; the Go watcher holds its
        # answer to each until this thread has published.
        wait_record(0)
        observe('incomplete-observed', True)
        if action in ('accepted', 'retried'):
            wait_record(1)
            observe('complete-observed', False)
        elif action == 'over-limit':
            wait_member_snapshot()
            observe('member-snapshot-observed', False)
        elif action == 'lock-refused':
            wait_member_lock(plan['images'][0]['key'])
            observe('member-lock-observed', False)
        elif action == 'complete-refused':
            wait_member_snapshots()
            observe('member-snapshots-observed', False)
    finally:
        worker.join(60)
        if worker.is_alive():
            raise TimeoutError('group snapshot creation did not return')
        creator_ioctx.close()
    if action == 'over-limit':
        with rbd.Image(ioctxs[limited['pool']], limited['name']) as image:
            image.remove_snap_limit()
            creation['snap_limit']['removed_limit'] = image.get_snap_limit()
        creation['snap_limit']['removed_call'] = call(*headers[limited['key']], 'rbd', 'snapshot_get_limit', b'')
    if 'error' in result:
        raise ValueError('group snapshot creation raised without an errno: ' + result['error'])
    creation['errno'], creation['elapsed_ns'] = result['errno'], result['elapsed_ns']
    out['creation'] = creation

cluster = rados.Rados(conffile='/tmp/ceph-msgr-test/ceph.conf', rados_id=identity[len('client.'):],
                      conf={'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'native_global_id': cluster.get_instance_id()}
ioctxs = {}
try:
    for p in (pool, plan['second_pool']):
        ioctxs[p] = cluster.open_ioctx(p)
    out['pool_id'], out['second_pool_id'] = ioctxs[pool].get_pool_id(), ioctxs[plan['second_pool']].get_pool_id()
    group_ioctx = ioctxs[pool]
    if action == 'setup':
        # The acceptance runs alone on its fixture: only the base image and
        # no group may exist before it.
        listed(out)
        if out['images_listed'] != [pool + '/' + plan['base_image']] or out['groups_listed']:
            raise ValueError('owned RBD pools hold an unplanned image or group')
        writes = []
        for entry in plan['images']:
            rbd.RBD().create(ioctxs[entry['pool']], entry['name'], plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            with rbd.Image(ioctxs[entry['pool']], entry['name']) as image:
                writes += [write(image, entry['key'], 'pre', e) for e in entry['pre_snapshot_writes']]
                image.flush()
        rbd.RBD().group_create(group_ioctx, group_name)
        with rbd.Group(group_ioctx, group_name) as group:
            for entry in plan['images']:
                group.add_image(ioctxs[entry['pool']], entry['name'])
            group.create_snap(plan['group_snapshot'])
        out['writes'] = writes
    elif action == 'modify':
        writes = []
        for entry in plan['images']:
            with rbd.Image(ioctxs[entry['pool']], entry['name']) as image:
                writes += [write(image, entry['key'], 'post', e) for e in entry['post_snapshot_writes']]
                image.flush()
        out['writes'] = writes
    elif action in CREATED:
        create_under_quiesce(out)
    elif action == 'rollback':
        with rbd.Group(group_ioctx, group_name) as group:
            group.rollback_to_snap(plan['group_snapshot'])
        out['rolled_back_to'] = plan['group_snapshot']
    if action != 'cleanup':
        listed(out)
        observe_group(out)
        out['images'] = [observe_image(entry) for entry in plan['images']]
    else:
        # Identities first, then each removal with its own native absence
        # read before the next one.
        with rbd.Group(group_ioctx, group_name) as group:
            group_id = text(group.id())
        removal = {'group_id': group_id, 'group_header': 'rbd_group_header.' + group_id, 'images': [], 'group_snapshots_after': []}
        for entry in plan['images']:
            with rbd.Image(ioctxs[entry['pool']], entry['name'], read_only=True) as image:
                stat = image.stat()
                removal['images'].append({'key': entry['key'], 'name': entry['name'], 'pool': entry['pool'], 'image_id': text(image.id()),
                                          'prefix': text(stat['block_name_prefix']), 'objects': stat['num_objs']})
        with rbd.Group(group_ioctx, group_name) as group:
            for snap_name in (plan['retried_snapshot'], plan['second_snapshot'], plan['group_snapshot']):
                group.remove_snap(snap_name)
                removal['group_snapshots_after'].append([group_snap(s) for s in group.list_snaps()])
            for image in removal['images']:
                with rbd.Image(ioctxs[image['pool']], image['name'], read_only=True) as opened:
                    image['snapshots_after'] = [{'id': s['id'], 'name': text(s['name']), 'size': s['size'], 'namespace': s['namespace']} for s in opened.list_snaps()]
            for entry in plan['images']:
                group.remove_image(ioctxs[entry['pool']], entry['name'])
            removal['members_after'] = members_of(group)
        rbd.RBD().group_remove(group_ioctx, group_name)
        removal['group_header_errno'] = errno_of(group_ioctx, removal['group_header'])
        removal['directory_call'] = call(group_ioctx, 'rbd_group_directory', 'rbd', 'dir_get_id', struct.pack('<I', len(group_name)) + group_name.encode())
        for image in removal['images']:
            io = ioctxs[image['pool']]
            rbd.RBD().remove(io, image['name'])
            image['header_errno'] = errno_of(io, 'rbd_header.' + image['image_id'])
            image['id_errno'] = errno_of(io, 'rbd_id.' + image['name'])
            image['object_map_errno'] = errno_of(io, 'rbd_object_map.' + image['image_id'])
            image['data_errno'] = [errno_of(io, '%s.%016x' % (image['prefix'], i)) for i in range(image['objects'])]
        listed(out)
        out['removed'] = removal
finally:
    for io in ioctxs.values():
        io.close()
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
	SnapshotInfos []rbdGroupSnap      `json:"snapshot_infos"`
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

type rbdGroupObjectView struct {
	Index  uint64 `json:"index"`
	Exists bool   `json:"exists"`
	Errno  int    `json:"errno"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

// rbdGroupDataView is one image's bytes and data objects at the head or at
// one snapshot.
type rbdGroupDataView struct {
	Label       string               `json:"label"`
	SnapID      uint64               `json:"snap_id"`
	Objects     []rbdGroupObjectView `json:"objects"`
	Image       rbdBaseBlob          `json:"image"`
	ObjectBytes rbdBaseBlob          `json:"object_bytes"`
}

type rbdGroupImageView struct {
	Key             string              `json:"key"`
	Name            string              `json:"name"`
	Pool            string              `json:"pool"`
	PoolID          int64               `json:"pool_id"`
	ImageID         string              `json:"image_id"`
	HeaderObject    string              `json:"header_object"`
	IDObject        string              `json:"id_object"`
	Stat            *rbdBaseStat        `json:"stat"`
	Features        uint64              `json:"features"`
	OpFeatures      uint64              `json:"op_features"`
	Snapshots       []rbdGroupSnapshot  `json:"snapshots"`
	SnapshotCalls   []rbdWriteClassCall `json:"snapshot_calls"`
	SnapContextCall *rbdWriteClassCall  `json:"snapcontext_call"`
	LockCall        *rbdWriteClassCall  `json:"lock_call"`
	Views           []rbdGroupDataView  `json:"views"`
}

type rbdGroupWrite struct {
	Image   string `json:"image"`
	Phase   string `json:"phase"`
	Offset  uint64 `json:"offset"`
	Length  uint64 `json:"length"`
	Written uint64 `json:"written"`
	SHA256  string `json:"sha256"`
}

// rbdGroupCreationImage is one member header's state during a creation.
type rbdGroupCreationImage struct {
	Key             string             `json:"key"`
	SnapContextCall *rbdWriteClassCall `json:"snapcontext_call"`
	LockCall        *rbdWriteClassCall `json:"lock_call"`
	LimitCall       *rbdWriteClassCall `json:"limit_call"`
}

// rbdGroupSnapLimit is the over-limit creation's native snapshot limit on
// one member: its snapshot count when librbd set the limit to it, the limit
// librbd read back, and the native snapshot_get_limit after the set and after
// the removal.
type rbdGroupSnapLimit struct {
	Image        string             `json:"image"`
	Snapshots    uint64             `json:"snapshots"`
	Limit        uint64             `json:"limit"`
	SetCall      *rbdWriteClassCall `json:"set_call"`
	RemovedLimit uint64             `json:"removed_limit"`
	RemovedCall  *rbdWriteClassCall `json:"removed_call"`
}

// rbdGroupCreationPhase is one independent native observation while the
// creating thread waited on the Go watcher's answer.
type rbdGroupCreationPhase struct {
	Phase         string                  `json:"phase"`
	AfterNS       int64                   `json:"after_ns"`
	ListCall      *rbdWriteClassCall      `json:"list_call"`
	Images        []rbdGroupCreationImage `json:"images"`
	RollbackErrno *int                    `json:"rollback_errno,omitempty"`
	// MemberSnapshotCall is the over-limit creation's snapshot_get of
	// member b's newest snapshot while librbd compensates.
	MemberSnapshotCall *rbdWriteClassCall `json:"member_snapshot_call,omitempty"`
}

// rbdGroupCreation is one creation: the snapshot name, the creating
// client's quiesce attempt count, the creating call's errno (0 for success),
// the observations and, for over-limit, the member snapshot limit.
type rbdGroupCreation struct {
	Snapshot        string                  `json:"snapshot"`
	GroupID         string                  `json:"group_id"`
	ImageIDs        map[string]string       `json:"image_ids"`
	QuiesceAttempts int                     `json:"quiesce_attempts"`
	Phases          []rbdGroupCreationPhase `json:"phases"`
	SnapLimit       *rbdGroupSnapLimit      `json:"snap_limit,omitempty"`
	Errno           int                     `json:"errno"`
	ElapsedNS       int64                   `json:"elapsed_ns"`
}

type rbdGroupRemovedImage struct {
	Key            string             `json:"key"`
	Name           string             `json:"name"`
	Pool           string             `json:"pool"`
	ImageID        string             `json:"image_id"`
	Prefix         string             `json:"prefix"`
	Objects        uint64             `json:"objects"`
	SnapshotsAfter []rbdGroupSnapshot `json:"snapshots_after"`
	HeaderErrno    int                `json:"header_errno"`
	IDErrno        int                `json:"id_errno"`
	ObjectMapErrno int                `json:"object_map_errno"`
	DataErrno      []int              `json:"data_errno"`
}

// rbdGroupRemoval is the cleanup action's record: the identities read
// before the removals and the native absence read after each one.
type rbdGroupRemoval struct {
	GroupID             string                 `json:"group_id"`
	GroupHeader         string                 `json:"group_header"`
	Images              []rbdGroupRemovedImage `json:"images"`
	GroupSnapshotsAfter [][]rbdGroupSnap       `json:"group_snapshots_after"`
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
	SecondPoolID   int64               `json:"second_pool_id"`
	NativeGlobalID uint64              `json:"native_global_id"`
	ImagesListed   []string            `json:"images_listed"`
	GroupsListed   []string            `json:"groups_listed"`
	Writes         []rbdGroupWrite     `json:"writes,omitempty"`
	Creation       *rbdGroupCreation   `json:"creation,omitempty"`
	RolledBackTo   string              `json:"rolled_back_to,omitempty"`
	Group          *rbdGroupView       `json:"group,omitempty"`
	Images         []rbdGroupImageView `json:"images,omitempty"`
	Removed        *rbdGroupRemoval    `json:"removed,omitempty"`
}

func rbdGroupIDValid(id string) bool {
	return id != "" && len(id) <= 64 && !strings.ContainsAny(id, "./ \x00")
}

func rbdGroupCallValid(call *rbdWriteClassCall, object, class, method string) bool {
	if call == nil {
		return false
	}
	_, inErr := hex.DecodeString(call.InputHex)
	_, outErr := hex.DecodeString(call.OutputHex)
	return call.Object == object && call.Class == class && call.Method == method && inErr == nil && outErr == nil && call.Code <= 0 && len(call.OutputHex) <= 16384
}

func rbdGroupSnapValid(s *rbdGroupSnap) bool {
	return s != nil && len(s.ImageSnaps) <= 16 && len(s.Name) <= 256 && len(s.ImageSnapName) <= 512 && len(s.ID) <= 64
}

// rbdGroupDataViewValid checks one view's object layout and blob names.
func rbdGroupDataViewValid(action, key string, v rbdGroupDataView, s *rbdBaseStat) bool {
	ok := len(v.Objects) == int(s.Objects) && (v.Label == "head") == (v.SnapID == 0) && (v.Label == "head" || v.Label == fmt.Sprintf("s%d", v.SnapID))
	var offset uint64
	for i, o := range v.Objects {
		ok = ok && o.Index == uint64(i) && o.Offset == offset && o.Size <= s.ObjectSize &&
			(o.Exists && o.Errno == 0 && len(o.SHA256) == 64 || !o.Exists && o.Errno == 2 && o.Size == 0 && o.SHA256 == "")
		offset += o.Size
	}
	return ok && v.Image.Name == rbdGroupBlobName(action, key, v.Label, "image") && v.Image.Length == s.Size && len(v.Image.SHA256) == 64 &&
		v.ObjectBytes.Name == rbdGroupBlobName(action, key, v.Label, "objects") && v.ObjectBytes.Length == offset && offset <= rbdBaseBlobLimit && len(v.ObjectBytes.SHA256) == 64
}

// rbdGroupImageViewValid checks the structure of one member observation;
// which snapshots and contents it must show is the test's judgment.
func rbdGroupImageViewValid(action string, v rbdGroupImageView, want rbdGroupImagePlan, plan rbdGroupPlan) bool {
	s := v.Stat
	ok := v.Key == want.Key && v.Name == want.Name && v.Pool == want.Pool && v.PoolID >= 0 && rbdGroupIDValid(v.ImageID) && v.HeaderObject == "rbd_header."+v.ImageID && v.IDObject == "rbd_id."+want.Name &&
		s != nil && s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Size == plan.Size && s.Objects == plan.Size>>plan.Order &&
		s.Prefix == "rbd_data."+v.ImageID && s.ParentPool == ^uint64(0) && s.ParentName == "" &&
		v.Features == plan.Features|rbdFeatureOperations && v.OpFeatures == rbdOperationFeatureGroup &&
		len(v.Snapshots) <= 4 && len(v.SnapshotCalls) == len(v.Snapshots) && len(v.Views) == len(v.Snapshots)+1 &&
		rbdGroupCallValid(v.SnapContextCall, v.HeaderObject, "rbd", "get_snapcontext") && rbdGroupCallValid(v.LockCall, v.HeaderObject, "lock", "get_info")
	if !ok {
		return false
	}
	for i, snap := range v.Snapshots {
		ok = ok && snap.ID != 0 && len(snap.Name) <= 512 && rbdGroupCallValid(&v.SnapshotCalls[i], v.HeaderObject, "rbd", "snapshot_get") &&
			(snap.Namespace == 1) == (snap.Group != nil) && v.Views[i+1].SnapID == snap.ID
	}
	for _, view := range v.Views {
		ok = ok && rbdGroupDataViewValid(action, want.Key, view, s)
	}
	return ok && v.Views[0].Label == "head"
}

// rbdGroupCreationValid checks the structure of a creation record: the
// action's observations (rbdGroupCreationPhases), each with both member
// headers, the rollback attempt on the first only, the attempt count the
// action runs with and, for over-limit only, member a's snapshot limit.
func rbdGroupCreationValid(action string, c *rbdGroupCreation, plan rbdGroupPlan, header string) bool {
	if c == nil {
		return false
	}
	phases := rbdGroupCreationPhases[action]
	attempts := rbdGroupDefaultQuiesceAttempts
	if action == "timed-out" || action == "retried" {
		attempts = plan.QuiesceAttempts
	}
	ok := c.Snapshot == plan.createdSnapshot(action) && c.Snapshot != "" && rbdGroupIDValid(c.GroupID) && header == "rbd_group_header."+c.GroupID && len(c.ImageIDs) == len(plan.Images) &&
		c.QuiesceAttempts == attempts && len(c.Phases) == len(phases) && c.Errno >= 0 && c.Errno < 4096 && c.ElapsedNS > 0
	for _, image := range plan.Images {
		ok = ok && rbdGroupIDValid(c.ImageIDs[image.Key])
	}
	if l := c.SnapLimit; (action == "over-limit") != (l != nil) {
		return false
	} else if l != nil {
		limited := "rbd_header." + c.ImageIDs[plan.Images[0].Key]
		ok = ok && l.Image == plan.Images[0].Key && l.Snapshots <= 4 && rbdGroupCallValid(l.SetCall, limited, "rbd", "snapshot_get_limit") &&
			rbdGroupCallValid(l.RemovedCall, limited, "rbd", "snapshot_get_limit")
	}
	for i, p := range c.Phases {
		if i >= len(phases) || !ok {
			return false
		}
		ok = p.Phase == phases[i] && p.AfterNS > 0 && rbdGroupCallValid(p.ListCall, header, "rbd", "group_snap_list") && len(p.Images) == len(plan.Images) &&
			(i == 0) == (p.RollbackErrno != nil) && (p.Phase == "member-snapshot-observed") == (p.MemberSnapshotCall != nil) &&
			(p.MemberSnapshotCall == nil || rbdGroupCallValid(p.MemberSnapshotCall, "rbd_header."+c.ImageIDs[plan.Images[1].Key], "rbd", "snapshot_get"))
		for j, image := range p.Images {
			if j >= len(plan.Images) {
				return false
			}
			object := "rbd_header." + c.ImageIDs[plan.Images[j].Key]
			ok = ok && image.Key == plan.Images[j].Key && rbdGroupCallValid(image.SnapContextCall, object, "rbd", "get_snapcontext") && rbdGroupCallValid(image.LockCall, object, "lock", "get_info") &&
				rbdGroupCallValid(image.LimitCall, object, "rbd", "snapshot_get_limit")
		}
	}
	return ok
}

// decodeRBDGroupNative accepts exactly the shape of one admitted action for
// the owned plan. It checks structure only; the group records, the member
// snapshots, the creation outcome and the absence results are the test's
// judgment, so a mismatch is reported by the test instead of stopping the
// control loop.
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
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.PoolID < 0 || result.SecondPoolID < 0 ||
		result.PoolID == result.SecondPoolID || result.NativeGlobalID == 0 {
		return rbdGroupNative{}, errors.New("native RBD group output identity differs")
	}
	ok := len(result.ImagesListed) <= 16 && len(result.GroupsListed) <= 16
	observed, creating := slices.Contains(rbdGroupObserved, action), slices.Contains(rbdGroupCreating, action)
	if observed {
		g := result.Group
		ok = ok && result.Removed == nil && len(result.Images) == len(plan.Images) && g != nil && g.Name == plan.Group && rbdGroupIDValid(g.ID) &&
			g.HeaderObject == "rbd_group_header."+g.ID && len(g.Members) <= 16 && len(g.Snapshots) <= 4 && len(g.SnapshotInfos) == len(g.Snapshots) &&
			rbdGroupCallValid(g.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id") && len(g.ListCalls) >= 1 && len(g.ListCalls) <= len(g.Snapshots)+1
		if g != nil {
			for i := range g.Snapshots {
				ok = ok && rbdGroupSnapValid(&g.Snapshots[i]) && rbdGroupSnapValid(&g.SnapshotInfos[i])
			}
			for i := range g.ListCalls {
				ok = ok && rbdGroupCallValid(&g.ListCalls[i], g.HeaderObject, "rbd", "group_snap_list")
			}
			ok = ok && (creating == (result.Creation != nil)) && (!creating || rbdGroupCreationValid(action, result.Creation, plan, g.HeaderObject))
		}
		for i, v := range result.Images {
			if i < len(plan.Images) {
				ok = ok && rbdGroupImageViewValid(action, v, plan.Images[i], plan)
			}
		}
		ok = ok && (action == "rollback") == (result.RolledBackTo != "") && (result.RolledBackTo == "" || result.RolledBackTo == plan.GroupSnapshot)
		var want []rbdGroupWrite
		phase := map[string]string{"setup": "pre", "modify": "post"}[action]
		for _, image := range plan.Images {
			extents := map[string][]rbdBaseExtent{"setup": image.PreWrites, "modify": image.PostWrites}[action]
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
	} else if action == "cleanup" {
		r := result.Removed
		ok = ok && result.Group == nil && result.Images == nil && result.Writes == nil && result.Creation == nil && result.RolledBackTo == "" && r != nil && rbdGroupIDValid(r.GroupID) &&
			r.GroupHeader == "rbd_group_header."+r.GroupID && len(r.Images) == len(plan.Images) && len(r.GroupSnapshotsAfter) == 3 && len(r.MembersAfter) <= 16 &&
			(r.GroupHeaderErrno == 0 || r.GroupHeaderErrno == 2) && rbdGroupCallValid(r.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id")
		if r != nil {
			for _, after := range r.GroupSnapshotsAfter {
				ok = ok && len(after) <= 4
			}
			for i, image := range r.Images {
				if i >= len(plan.Images) {
					break
				}
				errnoOK := func(e int) bool { return e == 0 || e == 2 }
				ok = ok && image.Key == plan.Images[i].Key && image.Name == plan.Images[i].Name && image.Pool == plan.Images[i].Pool && rbdGroupIDValid(image.ImageID) &&
					image.Prefix == "rbd_data."+image.ImageID && image.Objects == plan.Size>>plan.Order && len(image.SnapshotsAfter) <= 4 &&
					errnoOK(image.HeaderErrno) && errnoOK(image.IDErrno) && errnoOK(image.ObjectMapErrno) && len(image.DataErrno) == int(image.Objects) &&
					!slices.ContainsFunc(image.DataErrno, func(e int) bool { return !errnoOK(e) })
			}
		}
	} else {
		ok = false
	}
	if !ok {
		return rbdGroupNative{}, fmt.Errorf("native RBD group %s output shape differs", action)
	}
	return result, nil
}

// checkRBDGroupBlob verifies that one published binary view matches its
// receipt.
func checkRBDGroupBlob(out string, blob rbdBaseBlob) error {
	file, err := os.Open(filepath.Join(out, filepath.Base(blob.Name)))
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
		return fmt.Errorf("native RBD group binary view %s differs from its receipt", blob.Name)
	}
	return nil
}

// checkRBDGroupBlobs verifies every published binary view of one action.
func checkRBDGroupBlobs(out string, images []rbdGroupImageView) error {
	for _, image := range images {
		for _, view := range image.Views {
			for _, blob := range []rbdBaseBlob{view.Image, view.ObjectBytes} {
				if err := checkRBDGroupBlob(out, blob); err != nil {
					return err
				}
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
	images := plan.Images[0].Pool + "/" + plan.Images[0].Name + "," + plan.Images[1].Pool + "/" + plan.Images[1].Name
	switch action {
	case "setup":
		return images + ":" + plan.Group + "@" + plan.GroupSnapshot
	case "modify":
		return images + ":head"
	case "refused":
		return images + ":" + plan.Group + "@" + plan.RefusedSnapshot
	case "accepted":
		return images + ":" + plan.Group + "@" + plan.SecondSnapshot
	case "timed-out":
		return images + ":" + plan.Group + "@" + plan.TimedOutSnapshot
	case "retried":
		return images + ":" + plan.Group + "@" + plan.RetriedSnapshot
	case "over-limit":
		return images + ":" + plan.Group + "@" + plan.OverLimitSnapshot + "," + plan.Images[0].Pool + "/" + plan.Images[0].Name + ":snap_limit"
	case "lock-refused":
		return images + ":" + plan.Group + "@" + plan.LockRefusedSnapshot
	case "complete-refused":
		return images + ":" + plan.Group + "@" + plan.CompleteRefusedSnapshot
	case "rollback":
		return images + ":head<-" + plan.Group + "@" + plan.GroupSnapshot
	}
	return plan.Group + "@" + plan.RetriedSnapshot + "," + plan.SecondSnapshot + "," + plan.GroupSnapshot + ":" + images + ":removed"
}

// rbdGroupControl runs one native action of the closed group sequence, only
// on a profile that admits capRBDGroupSnapshot and after the second owned
// pool was created. Every action is journaled as a mutation.
func (c *fixtureCluster) rbdGroupControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || !c.profile.admits(capRBDGroupSnapshot) || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) ||
		!x.group.secondPoolIntent || x.group.secondPool == nil || x.group.secondPoolName != rbdGroupSecondPoolName(x.token) {
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
	if err == nil && (native.PoolID != x.poolID || native.SecondPoolID != x.group.secondPool.ID) {
		err = errors.New("native RBD group action used another pool")
	}
	if err == nil {
		err = checkRBDGroupBlobs(out, native.Images)
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
	"rbd-group-accepted-state.json",
	"rbd-group-accepted.json",
	"rbd-group-cleanup.json",
	"rbd-group-complete-refused-state.json",
	"rbd-group-complete-refused.json",
	"rbd-group-lock-refused-state.json",
	"rbd-group-lock-refused.json",
	"rbd-group-modify.json",
	"rbd-group-over-limit-state.json",
	"rbd-group-over-limit.json",
	"rbd-group-refused-state.json",
	"rbd-group-refused.json",
	"rbd-group-results.json",
	"rbd-group-retried-state.json",
	"rbd-group-retried.json",
	"rbd-group-rollback.json",
	"rbd-group-setup.json",
	"rbd-group-timed-out-state.json",
	"rbd-group-timed-out.json",
)

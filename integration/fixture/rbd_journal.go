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

// The RBD journaling acceptance reuses the RBD base scope (token, owned pool
// and both identities) and adds one more native librbd image to the owned
// pool, created only on the test's request, with the exclusive-lock and
// journaling features. The Go test reads the image's journal header and
// journal data objects through the explicit OSD API, registers its own
// journal client and commits positions as a mirror-like consumer would,
// watches the journal header while native librbd writes, and appends journal
// entries in librbd's format under the image's exclusive lock. The closed
// control here only lets native librbd create the image (with one native
// journal client registered at an empty commit position, so that `rbd
// journal inspect` replays every entry), write and discard through librbd,
// observe the journal with librados `exec`, librados reads and the `rbd
// journal info`, `status` and `inspect` commands, and finally replay the
// journal by acquiring the exclusive lock. Every native action is journaled
// in rbd-base-owned.json before it is issued and is never reissued; the image
// goes with the owned pool at cleanup. The journal encodings, decodings and
// the append protocol stay in the test-only consumer
// (integration/rbd_journal_test.go). Native behavior relied upon:
// cls/journal/cls_journal.cc, cls/journal/cls_journal_client.cc,
// cls/journal/cls_journal_types.cc, journal/Entry.cc,
// journal/JournalMetadata.cc, journal/JournalRecorder.cc,
// journal/ObjectRecorder.cc, journal/Journaler.cc, librbd/Journal.cc,
// librbd/journal/CreateRequest.cc, librbd/journal/Types.cc,
// tools/rbd/action/Journal.cc, pybind/rbd/rbd.pyx and pybind/rados/rados.pyx
// at rbdBaseSource (LGPL-2.1 references; no code copied).

// rbdJournalActions is the closed, ordered action sequence of the journal
// control. Each action runs at most once and only after its predecessors.
var rbdJournalActions = []string{"setup", "observe-registered", "write", "observe-committed", "inspect", "replay"}

// rbdJournalInspected names the actions that run `rbd journal inspect`.
func rbdJournalInspected(action string) bool {
	return action == "setup" || action == "inspect" || action == "replay"
}

// rbdJournalReadOnly names the observations that change no owned state.
// `rbd journal inspect` registers and unregisters its own journal client, so
// the inspect action is journaled as a mutation.
func rbdJournalReadOnly(action string) bool { return strings.HasPrefix(action, "observe-") }

// rbdJournalBlobViews names the binary views every native journal action
// publishes; the file of view v for action a is rbd-journal-<a>-<v>.bin.
var rbdJournalBlobViews = map[string]string{
	"image_head":      "image-head",
	"journal_objects": "journal-objects",
}

func rbdJournalBlobName(action, key string) string {
	return "rbd-journal-" + action + "-" + rbdJournalBlobViews[key] + ".bin"
}

// rbdJournalObjectCount is the number of journal data objects every
// observation reads: the first two object sets of the default splay width
// (four). The plan never fills object set 0, so set 1 must stay absent.
const rbdJournalObjectCount = 8

// rbdJournalObjectLimit bounds one journal data object read.
const rbdJournalObjectLimit = 256 << 10

// rbdJournalCalls is the fixed native exec list of every observation on
// `journal.<image id>`: the immutable and mutable metadata, the client list,
// the three clients by id, the next tag tid and the image client's tag list.
var rbdJournalCalls = []string{
	"get_order", "get_splay_width", "get_pool_id", "get_minimum_set", "get_active_set", "client_list",
	"get_client", "get_client", "get_client", "get_next_tag_tid", "tag_list",
}

// RBD_FEATURE_LAYERING | EXCLUSIVE_LOCK | JOURNALING.
const rbdJournalFeatures = 1 | 4 | 64

type rbdJournalEvent struct {
	Kind   string `json:"kind"`
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// rbdJournalPlan is the fixed recipe of the journaling image. SetupEvents
// are librbd writes and discards of the setup action, WriteEvents those of
// the write action, GoWrites the AioWrite events the Go test appends.
type rbdJournalPlan struct {
	Pool         string            `json:"pool"`
	BaseImage    string            `json:"base_image"`
	Image        string            `json:"image"`
	Order        int               `json:"order"`
	Size         uint64            `json:"size"`
	Features     uint64            `json:"features"`
	OracleClient string            `json:"oracle_client"`
	GoClient     string            `json:"go_client"`
	SetupEvents  []rbdJournalEvent `json:"setup_events"`
	WriteEvents  []rbdJournalEvent `json:"write_events"`
	GoWrites     []rbdBaseExtent   `json:"go_writes"`
}

// rbdJournalPlanFor places a 256 KiB, four-object image: setup writes
// objects 0 to 3 and discards object 3 whole, the write action writes
// objects 0 and 1 and discards object 2 whole, and Go appends writes to
// objects 0 and 1 that only a journal replay applies. Every write is at most
// 8 KiB, below librbd's 16 KiB journal payload limit, so each is one
// AioWrite event; every discard covers a whole 64 KiB object, so librbd's
// 64 KiB discard granularity keeps it.
func rbdJournalPlanFor(token string) rbdJournalPlan {
	const k = 1 << 10
	return rbdJournalPlan{Pool: "ceph-msgr-rbd-" + token, BaseImage: "ceph-msgr-image-" + token, Image: "ceph-msgr-journal-" + token,
		Order: 16, Size: 256 * k, Features: rbdJournalFeatures,
		OracleClient: "ceph-msgr-oracle-" + token[:8], GoClient: "ceph-msgr-mirror-" + token[:8],
		SetupEvents: []rbdJournalEvent{{"write", 0, 8 * k}, {"write", 72 * k, 8 * k}, {"write", 136 * k, 4 * k}, {"write", 196 * k, 8 * k}, {"discard", 192 * k, 64 * k}},
		WriteEvents: []rbdJournalEvent{{"write", 16 * k, 8 * k}, {"write", 80 * k, 4 * k}, {"discard", 128 * k, 64 * k}},
		GoWrites:    []rbdBaseExtent{{40 * k, 4 * k}, {100 * k, 4 * k}}}
}

func (p rbdJournalPlan) owned(token string) bool {
	return len(token) == 32 && p.Pool == "ceph-msgr-rbd-"+token && p.BaseImage == "ceph-msgr-image-"+token && p.Image == "ceph-msgr-journal-"+token &&
		p.Order == 16 && p.Size == 256<<10 && p.Features == rbdJournalFeatures && p.OracleClient == "ceph-msgr-oracle-"+token[:8] &&
		p.GoClient == "ceph-msgr-mirror-"+token[:8] && len(p.SetupEvents) > 0 && len(p.WriteEvents) > 0 && len(p.GoWrites) > 0
}

// The fixed native journal driver. The fixture passes only the token-bound
// plan and a closed action; the driver re-checks every name. librbd runs
// without its cache and with error-level stderr only. A read-write open
// takes the exclusive lock on the first write (or lock_acquire), which opens
// the journal, replays it and allocates a new local tag; closing flushes the
// commit position. Read-only opens take no lock and open no journal.
// Sources: pybind/rbd/rbd.pyx and pybind/rados/rados.pyx at rbdBaseSource;
// independent driver, no code copied.
const rbdJournalNativePython = `
import hashlib, json, os, struct, subprocess, sys, time
import rados, rbd
identity, action, token, request_id, plan_json = sys.argv[1:6]
plan = json.loads(plan_json)
ACTIONS = ('setup', 'observe-registered', 'write', 'observe-committed', 'inspect', 'replay')
if (len(token) != 32 or any(c not in '0123456789abcdef' for c in token) or identity != 'client.rbd-base-native-' + token
        or plan.get('pool') != 'ceph-msgr-rbd-' + token or plan.get('base_image') != 'ceph-msgr-image-' + token
        or plan.get('image') != 'ceph-msgr-journal-' + token or plan.get('features') != 69
        or plan.get('oracle_client') != 'ceph-msgr-oracle-' + token[:8] or plan.get('go_client') != 'ceph-msgr-mirror-' + token[:8]
        or action not in ACTIONS or not request_id.isdecimal()):
    raise ValueError('native RBD journal driver scope is not owned')
OUT = '/out'
CONF = '/tmp/ceph-msgr-test/ceph.conf'
OBJECTS = 8
OBJECT_LIMIT = 256 * 1024
pool, name = plan['pool'], plan['image']
PREFIX = 'rbd-journal-%s-' % action

def lstr(value):
    data = value.encode() if isinstance(value, str) else value
    return struct.pack('<I', len(data)) + data

def text(value):
    return value.decode('utf-8') if isinstance(value, bytes) else value

def pattern(phase, offset, length):
    data, counter = bytearray(), 0
    while len(data) < length:
        data += hashlib.sha256(('ceph-msgr-rbd-journal:%s:%s:%d:%d' % (token, phase, offset, counter)).encode()).digest()
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
        length, output = ioctx.execute(oid, cls, method, data, 65536)
        output = bytes(output)
        if length != len(output):
            raise ValueError('native class output length differs')
        entry.update(code=0, output_hex=output.hex())
    except rados.Error as error:
        if not isinstance(error.errno, int) or error.errno <= 0:
            raise
        entry.update(code=-error.errno, output_hex='')
    return entry

def journal_calls(ioctx, oid):
    # cls_journal_client.cc inputs: client_list(start_after, max_return),
    # get_client(id), tag_list(start_after_tag_tid, max_return, client_id,
    # optional tag class 0).
    clients = ('', plan['go_client'], plan['oracle_client'])
    calls = [call(ioctx, oid, 'journal', m, b'') for m in ('get_order', 'get_splay_width', 'get_pool_id', 'get_minimum_set', 'get_active_set')]
    calls.append(call(ioctx, oid, 'journal', 'client_list', lstr('') + struct.pack('<Q', 256)))
    calls += [call(ioctx, oid, 'journal', 'get_client', lstr(c)) for c in clients]
    calls.append(call(ioctx, oid, 'journal', 'get_next_tag_tid', b''))
    calls.append(call(ioctx, oid, 'journal', 'tag_list', struct.pack('<QQ', 0, 256) + lstr('') + b'\x01' + struct.pack('<Q', 0)))
    return calls

def cli(args, parse):
    argv = ['rbd', '--conf', CONF, '--id', identity[len('client.'):], '--keyring', native_keyring_file(identity), '--rbd_cache=false', '--log_to_stderr=false', '--err_to_stderr=true'] + args
    started = time.monotonic_ns()
    done = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
    elapsed = time.monotonic_ns() - started
    if len(done.stdout) > 65536 or len(done.stderr) > 4096:
        raise ValueError('rbd journal output exceeds its bound')
    out = {'argv': argv[7:], 'returncode': done.returncode, 'stdout': text(done.stdout), 'stderr': text(done.stderr), 'elapsed_ns': elapsed}
    if done.returncode == 0:
        out['parsed'] = parse(text(done.stdout))
    return out

def parse_inspect(stdout):
    # tools/rbd/action/Journal.cc JournalInspector --verbose: per entry a line
    # "Entry: tag_id=<t>, commit_tid=<c>" and a pretty JSON event_entry
    # object, then "Summary:" and "  <n> entries inspected, <e> errors".
    entries, pending, body, summary = [], None, [], None
    lines = stdout.split('\n')
    for index, line in enumerate(lines):
        if pending is not None:
            body.append(line)
            try:
                event = json.loads('\n'.join(body))
            except ValueError:
                continue
            entries.append(dict(pending, event=event))
            pending, body = None, []
            continue
        if line.startswith('Entry: tag_id='):
            tag, commit = line[len('Entry: tag_id='):].split(', commit_tid=')
            pending = {'tag_id': int(tag), 'commit_tid': int(commit)}
        elif line == 'Summary:':
            words = lines[index + 1].split()
            summary = {'total': int(words[0]), 'errors': int(words[3])}
            break
        elif line.strip():
            raise ValueError('unexpected rbd journal inspect line')
    if pending is not None or summary is None or len(entries) > 64:
        raise ValueError('rbd journal inspect output is incomplete')
    return {'entries': entries, 'summary': summary}

def journal_objects(ioctx, prefix):
    objects, blob = [], bytearray()
    for number in range(OBJECTS):
        oid = prefix + str(number)
        entry = {'number': number, 'object': oid}
        try:
            length, _ = ioctx.stat(oid)
            data = ioctx.read(oid, length, 0) if length else b''
        except rados.ObjectNotFound:
            entry.update(exists=False, size=0, offset=len(blob), sha256='')
            objects.append(entry)
            continue
        if len(data) != length or length > OBJECT_LIMIT:
            raise ValueError('journal object read differs from its stat or exceeds its bound')
        entry.update(exists=True, size=length, offset=len(blob), sha256=hashlib.sha256(data).hexdigest())
        blob += data
        objects.append(entry)
    return objects, bytes(blob)

def lockers(image):
    value = image.list_lockers()
    if not value:
        return {'empty': True, 'tag': '', 'exclusive': False, 'lockers': []}
    return {'empty': False, 'tag': text(value['tag']), 'exclusive': bool(value['exclusive']),
            'lockers': [{'client': text(c), 'cookie': text(k), 'addr': text(a)} for c, k, a in value['lockers']]}

def observe(ioctx, out):
    with rbd.Image(ioctx, name, read_only=True) as image:
        stat = image.stat()
        image_id = text(image.id())
        out['image_id'] = image_id
        out['stat'] = {k: text(stat[k]) for k in ('size', 'obj_size', 'num_objs', 'order', 'block_name_prefix')}
        out['features'], out['flags'] = image.features(), image.flags()
        out['lockers'] = lockers(image)
        out['watchers'] = [{'addr': text(w['addr']), 'id': w['id'], 'cookie': w['cookie']} for w in image.watchers_list()]
        head = image.read(0, stat['size'])
    header = 'journal.' + image_id
    prefix = 'journal_data.%d.%s.' % (ioctx.get_pool_id(), image_id)
    out['header_object'], out['journal_header'], out['journal_prefix'] = 'rbd_header.' + image_id, header, prefix
    out['class_calls'] = journal_calls(ioctx, header)
    objects, view = journal_objects(ioctx, prefix)
    out['journal_objects'] = objects
    spec = pool + '/' + image_id
    out['info'] = cli(['journal', 'info', '--format', 'json', spec], json.loads)
    out['status'] = cli(['journal', 'status', '--format', 'json', spec], json.loads)
    if action in ('setup', 'inspect', 'replay'):
        out['inspect'] = cli(['journal', 'inspect', '--verbose', spec], parse_inspect)
    out['blobs'] = {'image_head': publish(PREFIX + 'image-head.bin', head), 'journal_objects': publish(PREFIX + 'journal-objects.bin', view)}

def apply(image, phase, event):
    started = time.time_ns()
    if event['kind'] == 'write':
        data = pattern(phase, event['offset'], event['length'])
        if image.write(data, event['offset']) != len(data):
            raise ValueError('native RBD write was short')
        digest = hashlib.sha256(data).hexdigest()
    elif event['kind'] == 'discard':
        image.discard(event['offset'], event['length'])
        digest = ''
    else:
        raise ValueError('unplanned native RBD journal event')
    return {'phase': phase, 'kind': event['kind'], 'offset': event['offset'], 'length': event['length'], 'sha256': digest,
            'started_ns': started, 'finished_ns': time.time_ns()}

cluster = rados.Rados(conffile=CONF, rados_id=identity[len('client.'):],
                      conf={'key': NATIVE_KEY, 'rbd_cache': 'false', 'log_to_stderr': 'false', 'err_to_stderr': 'true'})
cluster.connect(timeout=30)
out = {'action': action, 'token': token, 'request_id': int(request_id), 'pool': pool, 'image': name, 'native_global_id': cluster.get_instance_id(),
       'started_ns': time.time_ns()}
try:
    ioctx = cluster.open_ioctx(pool)
    try:
        out['pool_id'] = ioctx.get_pool_id()
        if action == 'setup':
            # The clone acceptance's parent and clone may precede this image.
            listed = sorted(text(i) for i in rbd.RBD().list(ioctx))
            planned = (plan['base_image'], 'ceph-msgr-parent-' + token, 'ceph-msgr-clone-' + token)
            if plan['base_image'] not in listed or any(i not in planned for i in listed):
                raise ValueError('owned RBD pool holds an unplanned image')
            rbd.RBD().create(ioctx, name, plan['size'], order=plan['order'], old_format=False, features=plan['features'])
            with rbd.Image(ioctx, name, read_only=True) as image:
                image_id = text(image.id())
            # A native journal client at an empty commit position, as
            # cls_journal client_register records it, keeps every entry
            # replayable for rbd journal inspect.
            out['oracle_register'] = call(ioctx, 'journal.' + image_id, 'journal', 'client_register', lstr(plan['oracle_client']) + lstr(b''))
            if out['oracle_register']['code'] != 0:
                raise ValueError('native oracle journal client registration failed')
        if action in ('setup', 'write'):
            phase = 'setup' if action == 'setup' else 'write'
            image = rbd.Image(ioctx, name)
            try:
                out['events'] = [apply(image, phase, e) for e in plan[phase + '_events']]
            finally:
                started = time.monotonic_ns()
                image.close()
                out['close_ns'] = time.monotonic_ns() - started
        elif action == 'replay':
            # Acquiring the exclusive lock opens the journal, which replays
            # every entry after the image client's commit position.
            image = rbd.Image(ioctx, name)
            try:
                started = time.monotonic_ns()
                image.lock_acquire(rbd.RBD_LOCK_MODE_EXCLUSIVE)
                out['acquire_ns'] = time.monotonic_ns() - started
                out['owner'] = image.is_exclusive_lock_owner()
            finally:
                started = time.monotonic_ns()
                image.close()
                out['close_ns'] = time.monotonic_ns() - started
        observe(ioctx, out)
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
out['finished_ns'] = time.time_ns()
print(json.dumps(out, sort_keys=True))
`

type rbdJournalApplied struct {
	Phase      string `json:"phase"`
	Kind       string `json:"kind"`
	Offset     uint64 `json:"offset"`
	Length     uint64 `json:"length"`
	SHA256     string `json:"sha256"`
	StartedNS  int64  `json:"started_ns"`
	FinishedNS int64  `json:"finished_ns"`
}

type rbdJournalObject struct {
	Number uint64 `json:"number"`
	Object string `json:"object"`
	Exists bool   `json:"exists"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

type rbdJournalStat struct {
	Size       uint64 `json:"size"`
	ObjectSize uint64 `json:"obj_size"`
	Objects    uint64 `json:"num_objs"`
	Order      int    `json:"order"`
	Prefix     string `json:"block_name_prefix"`
}

// rbdJournalCLI is one `rbd journal` command with its parsed output (the
// JSON of info and status, the entries and summary of inspect).
type rbdJournalCLI struct {
	Argv       []string        `json:"argv"`
	ReturnCode int             `json:"returncode"`
	Stdout     string          `json:"stdout"`
	Stderr     string          `json:"stderr"`
	ElapsedNS  int64           `json:"elapsed_ns"`
	Parsed     json.RawMessage `json:"parsed,omitempty"`
}

// rbdJournalNative is one complete journal-driver output; unknown fields are
// refused.
type rbdJournalNative struct {
	Action         string                 `json:"action"`
	Token          string                 `json:"token"`
	RequestID      int64                  `json:"request_id"`
	Pool           string                 `json:"pool"`
	PoolID         int64                  `json:"pool_id"`
	Image          string                 `json:"image"`
	NativeGlobalID uint64                 `json:"native_global_id"`
	StartedNS      int64                  `json:"started_ns"`
	FinishedNS     int64                  `json:"finished_ns"`
	ImageID        string                 `json:"image_id"`
	HeaderObject   string                 `json:"header_object"`
	JournalHeader  string                 `json:"journal_header"`
	JournalPrefix  string                 `json:"journal_prefix"`
	Stat           *rbdJournalStat        `json:"stat"`
	Features       uint64                 `json:"features"`
	Flags          uint64                 `json:"flags"`
	Lockers        *rbdWriteLockers       `json:"lockers"`
	Watchers       []rbdBaseWatcher       `json:"watchers"`
	ClassCalls     []rbdWriteClassCall    `json:"class_calls"`
	JournalObjects []rbdJournalObject     `json:"journal_objects"`
	Info           *rbdJournalCLI         `json:"info"`
	Status         *rbdJournalCLI         `json:"status"`
	Inspect        *rbdJournalCLI         `json:"inspect,omitempty"`
	Blobs          map[string]rbdBaseBlob `json:"blobs"`
	OracleRegister *rbdWriteClassCall     `json:"oracle_register,omitempty"`
	Events         []rbdJournalApplied    `json:"events,omitempty"`
	CloseNS        int64                  `json:"close_ns,omitempty"`
	AcquireNS      int64                  `json:"acquire_ns,omitempty"`
	Owner          *bool                  `json:"owner,omitempty"`
}

func rbdJournalCLIValid(c *rbdJournalCLI, args ...string) bool {
	if c == nil {
		return false
	}
	want := append([]string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "journal"}, args...)
	return slices.Equal(c.Argv, want) && c.ElapsedNS > 0 && len(c.Stdout) <= 65536 && len(c.Stderr) <= 4096 && (c.ReturnCode == 0) == (len(c.Parsed) != 0)
}

func rbdJournalEventsValid(applied []rbdJournalApplied, phase string, plan []rbdJournalEvent) bool {
	if len(applied) != len(plan) {
		return false
	}
	for i, a := range applied {
		e := plan[i]
		if a.Phase != phase || a.Kind != e.Kind || a.Offset != e.Offset || a.Length != e.Length || (e.Kind == "write") != (len(a.SHA256) == 64) ||
			e.Kind == "discard" && a.SHA256 != "" || a.StartedNS <= 0 || a.FinishedNS < a.StartedNS {
			return false
		}
	}
	return true
}

// decodeRBDJournalNative accepts exactly the shape of one admitted action
// for the owned plan. It checks structure only: which clients, positions,
// tags and entries each observation must show after the Go steps is the
// test's judgment, so a Go failure is reported by the test instead of
// stopping the control loop.
func decodeRBDJournalNative(action, token string, request int64, plan rbdJournalPlan, data []byte) (rbdJournalNative, error) {
	var result rbdJournalNative
	if len(data) == 0 || len(data) > rbdBaseJSONLimit || !json.Valid(data) {
		return result, errors.New("native RBD journal output exceeds its bound or is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return rbdJournalNative{}, fmt.Errorf("native RBD journal output decoding: %w", err)
	}
	if result.Action != action || result.Token != token || result.RequestID != request || result.Pool != plan.Pool || result.Image != plan.Image ||
		result.PoolID < 0 || result.NativeGlobalID == 0 || result.StartedNS <= 0 || result.FinishedNS < result.StartedNS {
		return rbdJournalNative{}, errors.New("native RBD journal output identity differs")
	}
	s := result.Stat
	prefix := fmt.Sprintf("journal_data.%d.%s.", result.PoolID, result.ImageID)
	ok := s != nil && result.ImageID != "" && !strings.ContainsAny(result.ImageID, "./ \x00") && result.HeaderObject == "rbd_header."+result.ImageID &&
		result.JournalHeader == "journal."+result.ImageID && result.JournalPrefix == prefix &&
		s.Order == plan.Order && s.ObjectSize == 1<<plan.Order && s.Prefix == "rbd_data."+result.ImageID && s.Size == plan.Size && s.Objects == plan.Size>>plan.Order &&
		result.Features == plan.Features && rbdLockLockersValid(result.Lockers) && rbdWriteWatchersValid(result.Watchers) &&
		len(result.ClassCalls) == len(rbdJournalCalls) && len(result.JournalObjects) == rbdJournalObjectCount && len(result.Blobs) == len(rbdJournalBlobViews)
	for i, method := range rbdJournalCalls {
		if i < len(result.ClassCalls) {
			ok = ok && rbdLockCallValid(&result.ClassCalls[i], result.JournalHeader, "journal", method) && len(result.ClassCalls[i].OutputHex) <= 131072
		}
	}
	offset := uint64(0)
	for i, o := range result.JournalObjects {
		ok = ok && o.Number == uint64(i) && o.Object == prefix+strconv.Itoa(i) && o.Offset == offset && o.Size <= rbdJournalObjectLimit &&
			(o.Exists && len(o.SHA256) == 64 || !o.Exists && o.Size == 0 && o.SHA256 == "")
		offset += o.Size
	}
	for key := range rbdJournalBlobViews {
		blob, present := result.Blobs[key]
		ok = ok && present && blob.Name == rbdJournalBlobName(action, key) && blob.Length <= rbdBaseBlobLimit && len(blob.SHA256) == 64
	}
	spec := plan.Pool + "/" + result.ImageID
	ok = ok && result.Blobs["image_head"].Length == s.Size && result.Blobs["journal_objects"].Length == offset &&
		rbdJournalCLIValid(result.Info, "info", "--format", "json", spec) && rbdJournalCLIValid(result.Status, "status", "--format", "json", spec)
	if rbdJournalInspected(action) {
		ok = ok && rbdJournalCLIValid(result.Inspect, "inspect", "--verbose", spec)
	} else {
		ok = ok && result.Inspect == nil
	}
	ok = ok && (action == "setup") == (result.OracleRegister != nil) && (action == "replay") == (result.Owner != nil) && (action == "replay") == (result.AcquireNS > 0)
	if result.OracleRegister != nil {
		ok = ok && rbdLockCallValid(result.OracleRegister, result.JournalHeader, "journal", "client_register") && result.OracleRegister.Code == 0
	}
	switch action {
	case "setup":
		ok = ok && rbdJournalEventsValid(result.Events, "setup", plan.SetupEvents) && result.CloseNS > 0
	case "write":
		ok = ok && rbdJournalEventsValid(result.Events, "write", plan.WriteEvents) && result.CloseNS > 0
	case "replay":
		ok = ok && result.Events == nil && result.CloseNS > 0
	default:
		ok = ok && result.Events == nil && result.CloseNS == 0
	}
	if !ok {
		return rbdJournalNative{}, fmt.Errorf("native RBD journal %s output shape differs", action)
	}
	return result, nil
}

// checkRBDJournalBlobs verifies that each published binary view of one
// action matches its receipt.
func checkRBDJournalBlobs(out, action string, blobs map[string]rbdBaseBlob) error {
	for key := range rbdJournalBlobViews {
		blob := blobs[key]
		name := rbdJournalBlobName(action, key)
		if blob.Name != name {
			return fmt.Errorf("native RBD journal binary view %s is misnamed", name)
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
			return fmt.Errorf("native RBD journal binary view %s differs from its receipt", name)
		}
	}
	return nil
}

// rbdJournalNativeTimeout bounds one native journal action: image creation
// or I/O, then the observation with up to three `rbd journal` commands of at
// most 60 s each.
const rbdJournalNativeTimeout = 240 * time.Second

func (c *fixtureCluster) rbdJournalRunNative(ctx context.Context, action string, request int64) (*ceph.NativeCommandResult, rbdJournalNative, error) {
	x := c.rbdBase
	plan, err := json.Marshal(x.journalPlan)
	if err != nil {
		return nil, rbdJournalNative{}, err
	}
	argv := []string{"python3", "-c", rbdJournalNativePython, "client.rbd-base-native-" + x.token, action, x.token, strconv.FormatInt(request, 10), string(plan)}
	process, err := c.runKeyedNative(ctx, c.Container, ceph.NativeCommand{Argv: argv, Timeout: rbdJournalNativeTimeout, MaxStdout: rbdBaseJSONLimit, MaxStderr: 16 << 10, Label: "owned native RBD journal " + action}, x.native)
	if err != nil || !process.Complete() || process.ExitCode != 0 {
		return &process, rbdJournalNative{}, errors.Join(err, fmt.Errorf("native RBD journal %s did not complete (exit %d, timed out %v)", action, process.ExitCode, process.TimedOut))
	}
	native, err := decodeRBDJournalNative(action, x.token, request, x.journalPlan, bytes.TrimSpace(process.Stdout))
	return &process, native, err
}

// parseRBDJournalRequest admits <id> <ownership token> <action> where the
// action is the next one of rbdJournalActions (next is its index).
func parseRBDJournalRequest(fields []string, token string, previousID int64, next int) (int64, string, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, "", errors.New("invalid owned RBD journal request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, "", errors.New("invalid owned RBD journal request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, "", errors.New("invalid RBD journal request identity")
	}
	if !slices.Contains(rbdJournalActions, fields[2]) {
		return 0, "", errors.New("RBD journal action is not admitted")
	}
	if next >= len(rbdJournalActions) || fields[2] != rbdJournalActions[next] {
		return 0, "", errors.New("RBD journal action is out of order or was already issued")
	}
	return id, fields[2], nil
}

// rbdJournalTarget names the owned state one journal action changes.
func rbdJournalTarget(plan rbdJournalPlan, action string) string {
	switch action {
	case "setup":
		return plan.Image + ",journal-client:" + plan.OracleClient
	case "write":
		return plan.Image + ":native-write"
	case "inspect":
		return plan.Image + ":journal-inspect"
	case "replay":
		return plan.Image + ":exclusive-lock,journal-replay"
	}
	return plan.Image
}

// rbdJournalControl runs one native action of the closed journal sequence.
// The setup, write, inspect and replay actions are journaled as mutations,
// the two observations as read-only actions.
func (c *fixtureCluster) rbdJournalControl(ctx context.Context, out string, fields []string) (resultErr error) {
	x := c.rbdBase
	if c.profile.rbdBase != "1" || x == nil || x.out != out || x.setup == nil || x.native == nil || !x.plan.owned(x.token) || !x.journalPlan.owned(x.token) {
		return errors.New("invalid owned RBD journal control scope")
	}
	ctx, stop := context.WithTimeout(ctx, rbdJournalNativeTimeout+20*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &x.mu); err != nil {
		return err
	}
	defer x.mu.Unlock()
	if x.cleanupAttempted {
		return errors.New("RBD journal control requested after cleanup began")
	}
	id, action, err := parseRBDJournalRequest(fields, x.token, x.journalLastRequestID, x.journalNext)
	if err != nil {
		return err
	}
	x.journalLastRequestID = id
	x.journalNext++
	started := time.Now()
	result := map[string]any{"source_commit": rbdBaseSource, "token": x.token, "request_id": id, "action": action, "plan": x.journalPlan}
	defer func() {
		result["elapsed_ms"] = time.Since(started).Milliseconds()
		result["observed_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
		result["complete"] = resultErr == nil
		if resultErr != nil {
			result["error"] = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "rbd-journal-"+action+".json", result))
	}()
	m, err := c.rbdBaseBeginAction("native-journal-"+action, rbdJournalTarget(x.journalPlan, action), id, rbdJournalReadOnly(action))
	result["mutation"] = m
	if err != nil {
		return err
	}
	process, native, err := c.rbdJournalRunNative(ctx, action, id)
	result["native_process"] = process
	if err == nil && native.PoolID != x.poolID {
		err = errors.New("native RBD journal action used another pool")
	}
	if err == nil {
		err = checkRBDJournalBlobs(out, action, native.Blobs)
	}
	if err != nil {
		return c.rbdBaseFinish(m, err)
	}
	result["native"] = native
	return c.rbdBaseFinish(m, nil)
}

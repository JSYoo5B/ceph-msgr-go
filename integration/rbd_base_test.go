package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDBaseIntegration checks that the explicit OSD API carries the
// Messenger I/O that RBD needs, against an image prepared by native librbd on
// a disposable Tentacle fixture: rbd object-class CALLs on the image's id and
// header objects, data-object reads (and SPARSE_READ) at the head and at a
// self-managed snapshot, LIST_SNAPS, LIST_WATCHERS of the header with librbd's
// own watch, and a Go watch that receives and acknowledges a librbd header
// update notify. See integration/fixture/RBD_BASE_FIXTURE.md and
// RBD_BASE_VALIDATION.md.
//
// Everything RBD-specific below is test-only consumer logic: the class input
// encodings, the decoding of their outputs, data-object naming and the
// image-to-object mapping. The product carries raw class names, inputs and
// outputs and raw object bytes only. Encodings restate the pinned
// cls/rbd/cls_rbd.cc, cls/rbd/cls_rbd_types.cc, librbd/WatchNotifyTypes.cc and
// include/rbd_types.h (LGPL-2.1 references; no code copied).

const (
	rbdNoSnap uint64 = 0xfffffffffffffffe // CEPH_NOSNAP
	// librbd watcher::Notifier::NOTIFY_TIMEOUT in milliseconds.
	rbdNotifyTimeout = 5 * time.Second
	// CEPH_ENTITY_TYPE_CLIENT in a watch_item_t name.
	rbdEntityClient = 8
)

type nativeRBDExtent struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

type nativeRBDPlan struct {
	Pool        string            `json:"pool"`
	Image       string            `json:"image"`
	Snapshot    string            `json:"snapshot"`
	Order       int               `json:"order"`
	Size        uint64            `json:"size"`
	Features    uint64            `json:"features"`
	PreWrites   []nativeRBDExtent `json:"pre_snapshot_writes"`
	PostWrites  []nativeRBDExtent `json:"post_snapshot_writes"`
	ResizeAck   uint64            `json:"resize_acknowledged"`
	ResizeNoAck uint64            `json:"resize_unacknowledged"`
}

type nativeRBDClassCall struct {
	Object    string `json:"object"`
	Method    string `json:"method"`
	InputHex  string `json:"input_hex"`
	Code      int32  `json:"code"`
	OutputHex string `json:"output_hex"`
}

type nativeRBDObjectView struct {
	Exists bool   `json:"exists"`
	Errno  int    `json:"errno"`
	Size   uint64 `json:"size"`
	Offset uint64 `json:"offset"`
	SHA256 string `json:"sha256"`
}

type nativeRBDObject struct {
	Index uint64              `json:"index"`
	Name  string              `json:"name"`
	Head  nativeRBDObjectView `json:"head"`
	Snap  nativeRBDObjectView `json:"snap"`
}

type nativeRBDBlob struct {
	Name   string `json:"name"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256"`
}

type nativeRBDWatcher struct {
	Address string `json:"addr"`
	ID      uint64 `json:"id"`
	Cookie  uint64 `json:"cookie"`
}

type nativeRBDPhase struct {
	Phase    string             `json:"phase"`
	Size     uint64             `json:"size"`
	Watchers []nativeRBDWatcher `json:"watchers"`
	ResizeNS int64              `json:"resize_ns"`
	WaitNS   int64              `json:"wait_ns"`
}

type nativeRBD struct {
	Action         string `json:"action"`
	Token          string `json:"token"`
	RequestID      int64  `json:"request_id"`
	Pool           string `json:"pool"`
	PoolID         int64  `json:"pool_id"`
	Image          string `json:"image"`
	NativeGlobalID uint64 `json:"native_global_id"`
	ImageID        string `json:"image_id"`
	HeaderObject   string `json:"header_object"`
	IDObject       string `json:"id_object"`
	Stat           struct {
		Size       uint64 `json:"size"`
		ObjectSize uint64 `json:"obj_size"`
		Objects    uint64 `json:"num_objs"`
		Order      int    `json:"order"`
		Prefix     string `json:"block_name_prefix"`
	} `json:"stat"`
	Features    uint64 `json:"features"`
	StripeUnit  uint64 `json:"stripe_unit"`
	StripeCount uint64 `json:"stripe_count"`
	Snapshots   []struct {
		ID        uint64 `json:"id"`
		Name      string `json:"name"`
		Size      uint64 `json:"size"`
		Namespace int    `json:"namespace"`
	} `json:"snapshots"`
	ClassCalls  []nativeRBDClassCall     `json:"class_calls"`
	Objects     []nativeRBDObject        `json:"objects"`
	Blobs       map[string]nativeRBDBlob `json:"blobs"`
	PoolObjects []string                 `json:"pool_objects"`
	Phases      []nativeRBDPhase         `json:"phases"`
}

type nativeRBDOracle struct {
	Source string        `json:"source_commit"`
	Token  string        `json:"token"`
	FSID   string        `json:"fsid"`
	Plan   nativeRBDPlan `json:"plan"`
	Pool   struct {
		ID        int64  `json:"pool"`
		Name      string `json:"pool_name"`
		Flags     uint64 `json:"flags"`
		FlagNames string `json:"flags_names"`
	} `json:"pool"`
	ClientIdentity string            `json:"client_identity"`
	NativeIdentity string            `json:"native_identity"`
	Caps           map[string]string `json:"caps"`
	Native         nativeRBD         `json:"native"`
}

type nativeRBDWatchResult struct {
	Source    string     `json:"source_commit"`
	Token     string     `json:"token"`
	RequestID int64      `json:"request_id"`
	Action    string     `json:"action"`
	Native    *nativeRBD `json:"native"`
	Process   *struct {
		Stdout, Stderr []byte
		ExitCode       int
		TimedOut       bool
	} `json:"native_process"`
	Mutation *struct {
		Kind      string `json:"kind"`
		RequestID int64  `json:"request_id"`
		State     string `json:"state"`
	} `json:"mutation"`
	Complete  bool   `json:"complete"`
	Error     string `json:"error"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

type nativeRBDWatchState struct {
	Token          string           `json:"token"`
	RequestID      int64            `json:"request_id"`
	NativeGlobalID uint64           `json:"native_global_id"`
	Phase          string           `json:"phase"`
	Phases         []nativeRBDPhase `json:"phases"`
}

// rbdRoute is caller-side placement of one object, read from the MON's
// `osd map` (the CephFS recovery acceptance's test-only helper).
type rbdRoute struct {
	Object  string
	Pool    int64
	Hash    uint32
	Epoch   uint32
	RawPGID string
	PGID    string
	Primary int32
	Address string
}

type rbdOperationEvidence struct {
	Code        uint16
	Result      int32
	Length      uint64
	DataLength  int
	DataSHA256  string
	DataHex     string `json:",omitempty"`
	ClassOutput uint64 `json:",omitempty"`
	Extents     []cephmsgr.OSDExtent
	Snapshots   *cephmsgr.OSDSnapshotList `json:",omitempty"`
	Watchers    []cephmsgr.OSDWatcher     `json:",omitempty"`
}

type rbdRequestEvidence struct {
	Label        string
	Route        rbdRoute
	ReadSnapshot uint64 `json:",omitempty"`
	Submission   cephmsgr.OSDSubmission
	Transaction  uint64
	GlobalID     uint64
	MapEpoch     uint32
	PG           cephmsgr.OSDPG
	Code         int32
	Flags        uint64
	UserVersion  uint64
	Operations   []rbdOperationEvidence
	Error        string `json:",omitempty"`
}

type rbdWatchEventEvidence struct {
	Opcode            uint8
	Cookie            uint64
	NotifyID          uint64
	NotifierGlobalID  uint64
	ReturnCode        int32
	GlobalID          uint64
	SessionGeneration uint64
	PayloadHex        string
	RawFrontHex       string
	RawDataHex        string
	Acknowledged      bool
}

type rbdBaseEvidence struct {
	Token        string
	Pool         string
	PoolID       int64
	GoIdentity   string
	GoGlobalID   uint64
	ImageID      string
	SnapshotID   uint64
	Requests     []rbdRequestEvidence
	ClassOutputs map[string]string
	Events       []rbdWatchEventEvidence
	WatchState   *nativeRBDWatchState
	WatchResult  *nativeRBDWatchResult
	Timings      map[string]string
}

// Test-only RBD class encodings.

func rbdLE64(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

type rbdDecoder struct {
	data []byte
	err  error
}

func (d *rbdDecoder) take(n int) []byte {
	if d.err != nil || n < 0 || len(d.data) < n {
		d.err = errors.New("truncated RBD class output")
		return nil
	}
	b := d.data[:n]
	d.data = d.data[n:]
	return b
}
func (d *rbdDecoder) u8() uint8 {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}
func (d *rbdDecoder) u32() uint32 {
	if b := d.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}
func (d *rbdDecoder) u64() uint64 {
	if b := d.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func (d *rbdDecoder) str() string {
	n := d.u32()
	if n > 4096 {
		d.err = errors.New("RBD class string exceeds its bound")
		return ""
	}
	return string(d.take(int(n)))
}

// header reads an ENCODE_START envelope and returns its version and a
// decoder over exactly its payload.
func (d *rbdDecoder) header(compat uint8) (uint8, *rbdDecoder) {
	v, c, n := d.u8(), d.u8(), d.u32()
	body := d.take(int(n))
	if d.err == nil && c != compat {
		d.err = fmt.Errorf("RBD class envelope compat %d differs", c)
	}
	return v, &rbdDecoder{data: body, err: d.err}
}
func (d *rbdDecoder) done() error {
	if d.err == nil && len(d.data) != 0 {
		return fmt.Errorf("%d trailing RBD class output bytes", len(d.data))
	}
	return d.err
}

type rbdSnapshotInfo struct {
	ID, ImageSize             uint64
	NamespaceType, ChildCount uint32
	Name                      string
	Seconds, Nanoseconds      uint32
}

// decodeRBDSnapshotInfo decodes cls::rbd::SnapshotInfo (v1): id, the
// SnapshotNamespace envelope with its u32 type (0 = user, no fields), name,
// image size, utime_t timestamp and u32 child count.
func decodeRBDSnapshotInfo(data []byte) (rbdSnapshotInfo, error) {
	var info rbdSnapshotInfo
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	info.ID = body.u64()
	_, ns := body.header(1)
	info.NamespaceType = ns.u32()
	if err := ns.done(); err != nil {
		return info, err
	}
	info.Name = body.str()
	info.ImageSize = body.u64()
	info.Seconds, info.Nanoseconds = body.u32(), body.u32()
	info.ChildCount = body.u32()
	return info, errors.Join(body.done(), d.done())
}

// librbd NotifyMessage(HeaderUpdatePayload): ENCODE_START(7, 1), u32
// NOTIFY_OP_HEADER_UPDATE (3) and an empty payload.
var rbdHeaderUpdateNotify = []byte{7, 1, 4, 0, 0, 0, 3, 0, 0, 0}

// librbd ResponseMessage(0): ENCODE_START(1, 1) and an int32 result, the
// acknowledgement a librbd watcher sends for a header update.
var rbdResponseAck = []byte{1, 1, 4, 0, 0, 0, 0, 0, 0, 0}

// rbdObjectExtents maps an image extent to (object number, offset, length)
// for the default striping (stripe unit = object size, count 1).
func rbdObjectExtents(offset, length uint64, order int) [][3]uint64 {
	var result [][3]uint64
	size := uint64(1) << order
	for length > 0 {
		object, inner := offset>>order, offset&(size-1)
		n := min(length, size-inner)
		result = append(result, [3]uint64{object, inner, n})
		offset, length = offset+n, length-n
	}
	return result
}

// rbdObjectWrites lists the in-object ranges a plan's extents touch.
func rbdObjectWrites(extents []nativeRBDExtent, order int) map[uint64][]cephmsgr.OSDExtent {
	result := map[uint64][]cephmsgr.OSDExtent{}
	for _, e := range extents {
		for _, piece := range rbdObjectExtents(e.Offset, e.Length, order) {
			result[piece[0]] = append(result[piece[0]], cephmsgr.OSDExtent{Offset: piece[1], Length: piece[2]})
		}
	}
	return result
}

// rbdSubtract returns [0, size) minus the given ranges, merged and ordered.
func rbdSubtract(size uint64, ranges []cephmsgr.OSDExtent) []cephmsgr.OSDExtent {
	covered := make([]bool, size)
	for _, r := range ranges {
		for i := r.Offset; i < r.Offset+r.Length && i < size; i++ {
			covered[i] = true
		}
	}
	var result []cephmsgr.OSDExtent
	for i := uint64(0); i < size; {
		if covered[i] {
			i++
			continue
		}
		start := i
		for i < size && !covered[i] {
			i++
		}
		result = append(result, cephmsgr.OSDExtent{Offset: start, Length: i - start})
	}
	return result
}

func TestRBDBaseTestOnlyLayoutAndDecoders(t *testing.T) {
	if got := rbdObjectExtents(32<<10, 40<<10, 16); !reflect.DeepEqual(got, [][3]uint64{{0, 32 << 10, 32 << 10}, {1, 0, 8 << 10}}) {
		t.Fatal("image extent mapping", got)
	}
	if got := rbdSubtract(32<<10, []cephmsgr.OSDExtent{{Offset: 0, Length: 8 << 10}}); !reflect.DeepEqual(got, []cephmsgr.OSDExtent{{Offset: 8 << 10, Length: 24 << 10}}) {
		t.Fatal("overlap subtraction", got)
	}
	info := []byte{1, 1, 0, 0, 0, 0}
	body := rbdLE64(4)
	body = append(body, 1, 1, 4, 0, 0, 0, 0, 0, 0, 0)
	body = binary.LittleEndian.AppendUint32(body, 4)
	body = append(body, "snap"...)
	body = append(append(body, rbdLE64(512<<10)...), 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(info[2:], uint32(len(body)))
	decoded, err := decodeRBDSnapshotInfo(append(info, body...))
	if err != nil || decoded.ID != 4 || decoded.Name != "snap" || decoded.ImageSize != 512<<10 || decoded.Seconds != 1 || decoded.Nanoseconds != 2 || decoded.NamespaceType != 0 {
		t.Fatal("SnapshotInfo decoding", decoded, err)
	}
	for cut := 0; cut < len(info)+len(body); cut++ {
		if _, err := decodeRBDSnapshotInfo(append(info, body...)[:cut]); err == nil {
			t.Fatal("truncated SnapshotInfo admitted", cut)
		}
	}
	if _, err := decodeRBDSnapshotInfo(append(append(info, body...), 0)); err == nil {
		t.Fatal("trailing SnapshotInfo byte admitted")
	}
}

func readRBDBlob(t *testing.T, dir string, blob nativeRBDBlob) []byte {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, filepath.Base(blob.Name)))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	sum := sha256.Sum256(data)
	if err != nil || uint64(len(data)) != blob.Length || hex.EncodeToString(sum[:]) != blob.SHA256 {
		t.Fatal("native RBD binary view differs from its receipt", blob.Name, err)
	}
	return data
}

// waitRBDWatchState polls the driver's published state until it holds the
// named phase. Later phases keep earlier ones, so a transient phase is not
// missed; after the control action ended the file is read once more.
func waitRBDWatchState(ctx context.Context, path, phase string, finished <-chan struct{}, finishedErr *error) (nativeRBDWatchState, error) {
	read := func() (nativeRBDWatchState, bool) {
		var state nativeRBDWatchState
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &state) != nil {
			return state, false
		}
		return state, slices.ContainsFunc(state.Phases, func(p nativeRBDPhase) bool { return p.Phase == phase })
	}
	for {
		if state, ok := read(); ok {
			return state, nil
		}
		select {
		case <-finished:
			if state, ok := read(); ok {
				return state, nil
			}
			return nativeRBDWatchState{}, fmt.Errorf("native RBD watch action ended before %s: %v", phase, *finishedErr)
		case <-ctx.Done():
			return nativeRBDWatchState{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestCephRBDBaseIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD base fixture (CEPH_MSGR_TEST_RBD=1)")
	}
	var oracle nativeRBDOracle
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	plan, native := oracle.Plan, oracle.Native
	token := oracle.Token
	identity := os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || plan.Pool != "ceph-msgr-rbd-"+token || plan.Image != "ceph-msgr-image-"+token ||
		oracle.Pool.Name != plan.Pool || oracle.Pool.Flags&1 != 0 || native.PoolID != oracle.Pool.ID || native.Action != "setup" || native.Token != token ||
		identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" || oracle.Caps["osd"] != "profile rbd pool="+plan.Pool ||
		len(native.Snapshots) != 1 || len(native.ClassCalls) != 8 || plan.Order != 16 || plan.Features != 1 {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity)
	}
	snapshot := native.Snapshots[0]
	views := map[string][]byte{}
	for key, blob := range native.Blobs {
		views[key] = readRBDBlob(t, dir, blob)
	}
	if len(views) != 4 || uint64(len(views["image_head"])) != plan.Size || uint64(len(views["image_snap"])) != plan.Size {
		t.Fatal("native RBD binary views missing")
	}
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_RBD_BASE_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	want, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != want {
		t.Fatal("RBD base credential does not match the selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	options.Identity, options.EnableOSD = identity, true
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	started := time.Now()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate the RBD-profile Go client", err)
	}
	defer c.Close()
	if c.Snapshot().OSDTicket.Expires.IsZero() {
		t.Fatal("OSD service ticket missing")
	}
	evidence := rbdBaseEvidence{Token: token, Pool: plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, GoGlobalID: c.Snapshot().GlobalID, ClassOutputs: map[string]string{}, Timings: map[string]string{}}
	defer func() {
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-base-results.json"), data)
		}
		if err != nil {
			t.Error("RBD base evidence", err)
		}
	}()
	addresses := osdDumpAddresses(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	route := func(t *testing.T, object string) (rbdRoute, *cephmsgr.OSDConnection) {
		t.Helper()
		p, err := recoveryObjectPlacement(ctx, c, plan.Pool, object)
		if err != nil {
			t.Fatal(err)
		}
		r := rbdRoute{Object: object, Pool: p.PoolID, Hash: p.Hash, Epoch: p.Epoch, RawPGID: p.RawPGID, PGID: p.PGID, Primary: p.Primary, Address: addresses[p.Primary]}
		if r.Pool != oracle.Pool.ID || r.Primary < 0 || r.Address == "" || p.Acting[0] != p.Primary {
			t.Fatal("caller-side RBD object placement differs", r)
		}
		if connections[r.Primary] == nil {
			o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: r.Primary, Address: r.Address})
			if err != nil {
				t.Fatal("explicit OSD connection", err)
			}
			connections[r.Primary] = o
		}
		return r, connections[r.Primary]
	}
	request := func(r rbdRoute, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		return cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}, Operations: ops}
	}
	record := func(label string, r rbdRoute, req cephmsgr.OSDRequest, reply cephmsgr.OSDReply, err error) {
		e := rbdRequestEvidence{Label: label, Route: r, ReadSnapshot: req.ReadSnapshot, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion}
		for _, op := range reply.Operations {
			sum := sha256.Sum256(op.Data)
			o := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]), Extents: op.SparseRead.Extents, Watchers: op.Watchers}
			if len(op.Data) <= 512 {
				o.DataHex = hex.EncodeToString(op.Data)
			}
			if op.Code == cephmsgr.OSDCall {
				o.ClassOutput = op.Class.OutputLength
			}
			if op.Code == cephmsgr.OSDListSnapshots {
				list := op.Snapshots
				o.Snapshots = &list
			}
			e.Operations = append(e.Operations, o)
		}
		if err != nil {
			e.Error = err.Error()
		}
		evidence.Requests = append(evidence.Requests, e)
	}
	// checkReply asserts request identity, the raw-hash PG echo and the epoch.
	checkReply := func(t *testing.T, r rbdRoute, reply cephmsgr.OSDReply, code int32) {
		t.Helper()
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != c.Snapshot().GlobalID || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 {
			t.Fatalf("OSD reply identity differs: code=%d tid=%d gid=%d pg=%+v epoch=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG, reply.MapEpoch)
		}
	}
	call := func(t *testing.T, label, object, method string, input []byte) ([]byte, cephmsgr.OSDReply) {
		t.Helper()
		r, o := route(t, object)
		op := cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: "rbd", Method: method, Mode: cephmsgr.OSDCallRead, MaxOutputBytes: 4096}}
		req := request(r, op)
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatalf("Go CALL rbd.%s: %v", method, err)
		}
		checkReply(t, r, reply, 0)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != 3 || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) || got.Class.OutputLength != uint64(len(got.Data)) {
			t.Fatalf("CALL rbd.%s per-op result or union echo differs: %+v", method, got.Class)
		}
		evidence.ClassOutputs[label] = hex.EncodeToString(got.Data)
		return got.Data, reply
	}
	// sameAsNative compares the Go input and raw output with the native
	// librados exec of the same method on the same object.
	sameAsNative := func(t *testing.T, calls []nativeRBDClassCall, index int, object, method string, input, output []byte) {
		t.Helper()
		n := calls[index]
		if n.Object != object || n.Method != method || n.InputHex != hex.EncodeToString(input) || n.Code != 0 || n.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("Go CALL rbd.%s differs from native librados exec: native=%+v go_input=%x go_output=%x", method, n, input, output)
		}
	}

	var imageID, prefix string
	var snapID uint64
	order := 0
	t.Run("header_class_calls", func(t *testing.T) {
		idObject := "rbd_id." + plan.Image
		out, _ := call(t, "get_id", idObject, "get_id", nil)
		sameAsNative(t, native.ClassCalls, 0, idObject, "get_id", nil, out)
		d := &rbdDecoder{data: out}
		imageID = d.str()
		if err := d.done(); err != nil || imageID != native.ImageID {
			t.Fatal("rbd_id get_id differs from native librbd image id", imageID, native.ImageID, err)
		}
		header := "rbd_header." + imageID
		evidence.ImageID = imageID
		out, _ = call(t, "get_snapcontext", header, "get_snapcontext", nil)
		sameAsNative(t, native.ClassCalls, 5, header, "get_snapcontext", nil, out)
		d = &rbdDecoder{data: out}
		seq, count := d.u64(), d.u32()
		var snaps []uint64
		for i := uint32(0); i < count && i < 64; i++ {
			snaps = append(snaps, d.u64())
		}
		if err := d.done(); err != nil || len(snaps) != 1 || seq != snaps[0] || snaps[0] != snapshot.ID {
			t.Fatal("get_snapcontext differs from native librbd snapshot list", seq, snaps, snapshot, err)
		}
		snapID = snaps[0]
		evidence.SnapshotID = snapID
		out, _ = call(t, "get_size_head", header, "get_size", rbdLE64(rbdNoSnap))
		sameAsNative(t, native.ClassCalls, 1, header, "get_size", rbdLE64(rbdNoSnap), out)
		d = &rbdDecoder{data: out}
		headOrder, headSize := d.u8(), d.u64()
		if err := d.done(); err != nil || int(headOrder) != native.Stat.Order || headSize != native.Stat.Size || headSize != plan.Size {
			t.Fatal("get_size(head) differs from native librbd stat", headOrder, headSize, err)
		}
		order = int(headOrder)
		out, _ = call(t, "get_size_snapshot", header, "get_size", rbdLE64(snapID))
		sameAsNative(t, native.ClassCalls, 2, header, "get_size", rbdLE64(snapID), out)
		d = &rbdDecoder{data: out}
		snapOrder, snapSize := d.u8(), d.u64()
		if err := d.done(); err != nil || snapOrder != headOrder || snapSize != snapshot.Size {
			t.Fatal("get_size(snapshot) differs from native librbd snapshot size", snapOrder, snapSize, err)
		}
		out, _ = call(t, "get_object_prefix", header, "get_object_prefix", nil)
		sameAsNative(t, native.ClassCalls, 3, header, "get_object_prefix", nil, out)
		d = &rbdDecoder{data: out}
		prefix = d.str()
		if err := d.done(); err != nil || prefix != native.Stat.Prefix || prefix != "rbd_data."+imageID {
			t.Fatal("get_object_prefix differs from native librbd block name prefix", prefix, err)
		}
		input := append(rbdLE64(rbdNoSnap), 0) // snap id and read_only=false
		out, _ = call(t, "get_features", header, "get_features", input)
		sameAsNative(t, native.ClassCalls, 4, header, "get_features", input, out)
		d = &rbdDecoder{data: out}
		features, incompatible := d.u64(), d.u64()
		if err := d.done(); err != nil || features != native.Features || features != plan.Features || incompatible&^features != 0 {
			t.Fatal("get_features differs from native librbd features", features, incompatible, err)
		}
		out, _ = call(t, "get_snapshot_name", header, "get_snapshot_name", rbdLE64(snapID))
		sameAsNative(t, native.ClassCalls, 6, header, "get_snapshot_name", rbdLE64(snapID), out)
		d = &rbdDecoder{data: out}
		if name := d.str(); d.done() != nil || name != snapshot.Name || name != plan.Snapshot {
			t.Fatal("get_snapshot_name differs from native librbd snapshot name", name)
		}
		out, _ = call(t, "snapshot_get", header, "snapshot_get", rbdLE64(snapID))
		sameAsNative(t, native.ClassCalls, 7, header, "snapshot_get", rbdLE64(snapID), out)
		info, err := decodeRBDSnapshotInfo(out)
		if err != nil || info.ID != snapID || info.NamespaceType != uint32(snapshot.Namespace) || info.Name != snapshot.Name || info.ImageSize != snapshot.Size || info.ChildCount != 0 || info.Seconds == 0 {
			t.Fatal("snapshot_get differs from native librbd snapshot", info, err)
		}
		t.Logf("image id %s, prefix %s, order %d, size %d, features %#x (incompatible %#x), snapshot %d %q size %d at %d.%09d", imageID, prefix, order, headSize, features, incompatible, snapID, info.Name, info.ImageSize, info.Seconds, info.Nanoseconds)
	})
	if t.Failed() {
		return
	}
	objectSize := uint64(1) << order
	objects := plan.Size / objectSize
	preWrites, postWrites := rbdObjectWrites(plan.PreWrites, order), rbdObjectWrites(plan.PostWrites, order)
	names := make([]string, objects)
	for i := range names {
		names[i] = fmt.Sprintf("%s.%016x", prefix, i)
		if uint64(len(native.Objects)) != objects || native.Objects[i].Name != names[i] {
			t.Fatal("Go data object name differs from native librbd layout", names[i])
		}
	}
	nativeBytes := func(view nativeRBDObjectView, blob []byte) []byte {
		return blob[view.Offset : view.Offset+view.Size]
	}
	readObjects := func(t *testing.T, snapshotRead bool) {
		label, snap, image, blob := "head", uint64(0), views["image_head"], views["objects_head"]
		if snapshotRead {
			label, snap, image, blob = "snapshot", snapID, views["image_snap"], views["objects_snap"]
		}
		for i, name := range names {
			view := native.Objects[i].Head
			if snapshotRead {
				view = native.Objects[i].Snap
			}
			r, o := route(t, name)
			req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
			req.ReadSnapshot = snap
			reply, err := o.Request(ctx, req)
			record(fmt.Sprintf("read_%s_%d", label, i), r, req, reply, err)
			extent := image[uint64(i)*objectSize : uint64(i+1)*objectSize]
			if !view.Exists {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != -2 {
					t.Fatalf("%s object %d: native librados reports ENOENT, Go %v", label, i, err)
				}
				checkReply(t, r, reply, -2)
				if !bytes.Equal(extent, make([]byte, objectSize)) {
					t.Fatalf("%s object %d is absent but native librbd reads non-zero bytes", label, i)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s object %d: %v", label, i, err)
			}
			checkReply(t, r, reply, 0)
			stat, data := reply.Operations[0], reply.Operations[1].Data
			if stat.Result != 0 || len(stat.Data) != 16 || binary.LittleEndian.Uint64(stat.Data) != view.Size || uint64(len(data)) != view.Size || reply.Operations[1].Result != 0 {
				t.Fatalf("%s object %d stat/read size differs from native librados: native %d", label, i, view.Size)
			}
			if !bytes.Equal(data, nativeBytes(view, blob)) {
				t.Fatalf("%s object %d bytes differ from native librados", label, i)
			}
			padded := append(bytes.Clone(data), make([]byte, objectSize-uint64(len(data)))...)
			if !bytes.Equal(padded, extent) {
				t.Fatalf("%s object %d bytes differ from native librbd image extent", label, i)
			}
		}
	}
	t.Run("data_objects_at_head", func(t *testing.T) {
		readObjects(t, false)
		present := 0
		for i, o := range native.Objects {
			if o.Head.Exists {
				present++
				if !slices.Contains(native.PoolObjects, names[i]) {
					t.Fatal("native pool listing lacks a data object", names[i])
				}
			}
			if o.Head.Exists != (len(preWrites[uint64(i)])+len(postWrites[uint64(i)]) != 0) {
				t.Fatal("object existence differs from the written image extents", i)
			}
		}
		t.Logf("%d of %d data objects exist at the head; Go reads equal native librados objects and librbd image extents", present, objects)
	})
	t.Run("sparse_read", func(t *testing.T) {
		bluestore := os.Getenv("CEPH_MSGR_TEST_OSD_STORE") == "bluestore"
		for i, name := range names {
			view := native.Objects[i].Head
			if !view.Exists {
				continue
			}
			written := append(append([]cephmsgr.OSDExtent{}, preWrites[uint64(i)]...), postWrites[uint64(i)]...)
			holes := rbdSubtract(view.Size, written)
			object := nativeBytes(view, views["objects_head"])
			r, o := route(t, name)
			// The whole object range, then each hole of the image extents alone.
			spans := append([]cephmsgr.OSDExtent{{Offset: 0, Length: objectSize}}, holes...)
			for j, span := range spans {
				op := cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: span.Offset, Length: span.Length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 1 << 20}}
				req := request(r, op)
				reply, err := o.Request(ctx, req)
				record(fmt.Sprintf("sparse_read_%d_%d", i, j), r, req, reply, err)
				if err != nil {
					t.Fatalf("object %d SPARSE_READ %+v: %v", i, span, err)
				}
				checkReply(t, r, reply, 0)
				got := reply.Operations[0].SparseRead
				end := min(span.Offset+span.Length, view.Size)
				expanded, cursor, total := make([]byte, end-span.Offset), span.Offset, uint64(0)
				for _, e := range got.Extents {
					if e.Offset < cursor || e.Length == 0 || e.Offset+e.Length > end || total+e.Length > uint64(len(got.Data)) {
						t.Fatalf("object %d sparse extents are not ordered, positive and inside %+v: %+v", i, span, got.Extents)
					}
					copy(expanded[e.Offset-span.Offset:], got.Data[total:total+e.Length])
					cursor, total = e.Offset+e.Length, total+e.Length
				}
				if total != uint64(len(got.Data)) || !bytes.Equal(expanded, object[span.Offset:end]) {
					t.Fatalf("object %d SPARSE_READ %+v expansion differs from native librados bytes", i, span)
				}
				if bluestore {
					// BlueStore reports logical extents: the 4 KiB-aligned holes
					// of the image extents are omitted, every write is covered.
					want := []cephmsgr.OSDExtent(nil)
					for _, w := range rbdSubtract(view.Size, holes) {
						if lo, hi := max(w.Offset, span.Offset), min(w.Offset+w.Length, end); lo < hi {
							want = append(want, cephmsgr.OSDExtent{Offset: lo, Length: hi - lo})
						}
					}
					if len(got.Extents)+len(want) != 0 && !reflect.DeepEqual(got.Extents, want) {
						t.Fatalf("object %d BlueStore extents %+v in %+v differ from the written ranges %+v", i, got.Extents, span, want)
					}
				} else if !reflect.DeepEqual(got.Extents, []cephmsgr.OSDExtent{{Offset: span.Offset, Length: end - span.Offset}}) {
					// MemStore::fiemap reports the whole requested range up to EOF.
					t.Fatalf("object %d MemStore extents in %+v differ: %+v", i, span, got.Extents)
				}
				t.Logf("object %d range %d+%d: %d sparse extents %+v, %d bytes (holes %+v)", i, span.Offset, span.Length, len(got.Extents), got.Extents, len(got.Data), holes)
			}
		}
	})
	t.Run("snapshot_reads", func(t *testing.T) {
		readObjects(t, true)
		for i, name := range names {
			head, snap := native.Objects[i].Head, native.Objects[i].Snap
			if !head.Exists {
				continue
			}
			r, o := route(t, name)
			req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
			reply, err := o.Request(ctx, req)
			record(fmt.Sprintf("list_snaps_%d", i), r, req, reply, err)
			if err != nil {
				t.Fatalf("object %d LIST_SNAPS: %v", i, err)
			}
			list := reply.Operations[0].Snapshots
			changed := len(postWrites[uint64(i)]) != 0
			last := len(list.Clones) - 1
			if last < 0 || list.Clones[last].ID != cephmsgr.OSDHeadSnapshot || list.Clones[last].Size != head.Size {
				t.Fatalf("object %d LIST_SNAPS head entry differs from native head size %d: %+v", i, head.Size, list)
			}
			switch {
			case changed && snap.Exists:
				// Written before and after the snapshot: one clone for it,
				// sharing the bytes the post-snapshot writes left unchanged.
				overlap := rbdSubtract(snap.Size, postWrites[uint64(i)])
				clone := list.Clones[0]
				if list.Sequence != snapID || len(list.Clones) != 2 || clone.ID != snapID || !reflect.DeepEqual(clone.Snapshots, []uint64{snapID}) || clone.Size != snap.Size || !reflect.DeepEqual(clone.Overlap, overlap) {
					t.Fatalf("object %d LIST_SNAPS clone differs (want overlap %+v, size %d): %+v", i, overlap, snap.Size, list)
				}
			case changed:
				// Created after the snapshot under its SnapContext: no clone.
				if list.Sequence != snapID || len(list.Clones) != 1 {
					t.Fatalf("object %d created after the snapshot lists %+v", i, list)
				}
			default:
				// Untouched since the snapshot: the head serves the snapshot read.
				if list.Sequence != 0 || len(list.Clones) != 1 || !bytes.Equal(nativeBytes(snap, views["objects_snap"]), nativeBytes(head, views["objects_head"])) {
					t.Fatalf("object %d unchanged since the snapshot lists %+v", i, list)
				}
			}
		}
	})
	if t.Failed() {
		return
	}
	t.Run("header_watch_notify", func(t *testing.T) {
		header := "rbd_header." + imageID
		r, o := route(t, header)
		listWatchers := func(t *testing.T, label string) []cephmsgr.OSDWatcher {
			t.Helper()
			req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
			reply, err := o.Request(ctx, req)
			record(label, r, req, reply, err)
			if err != nil {
				t.Fatal(label, err)
			}
			checkReply(t, r, reply, 0)
			return reply.Operations[0].Watchers
		}
		if watchers := listWatchers(t, "list_watchers_closed"); len(watchers) != 0 {
			t.Fatal("header has watchers while no client has the image open", watchers)
		}
		watchRequest := request(r)
		watchRequest.Operations = nil
		w, reply, err := o.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 60})
		record("watch_register", r, watchRequest, reply, err)
		if err != nil {
			t.Fatal("Go header watch", err)
		}
		watching := true
		defer func() {
			if watching {
				cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				if _, err := w.Unwatch(cleanup, r.Epoch); err != nil {
					t.Error("Go header unwatch cleanup", err)
				}
			}
		}()
		gid := c.Snapshot().GlobalID
		if watchers := listWatchers(t, "list_watchers_go"); len(watchers) != 1 || watchers[0].Cookie != w.Cookie() || uint64(watchers[0].NameNumber) != gid || watchers[0].NameType != rbdEntityClient || watchers[0].TimeoutSeconds != 60 {
			t.Fatal("header watcher listing lacks the Go registration", watchers)
		}
		statePath := filepath.Join(dir, "rbd-base-watch-state.json")
		for _, name := range []string{statePath, filepath.Join(dir, "rbd-base-watch-proceed-1"), filepath.Join(dir, "rbd-base-watch-proceed-2"), filepath.Join(dir, "rbd-base-result.json")} {
			if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		var controlErr error
		controlDone := make(chan struct{})
		go func() {
			controlErr = testcluster.RequestControl(ctx, dir, "rbd-base", "rbd-base-completed", token, "watch")
			close(controlDone)
		}()
		defer func() {
			// After a failure the driver gives up at its own 30 s proceed
			// deadline; do not leave the control request behind.
			select {
			case <-controlDone:
			case <-time.After(45 * time.Second):
			}
		}()
		opened, err := waitRBDWatchState(ctx, statePath, "opened", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		evidence.WatchState = &opened
		if opened.Token != token || opened.NativeGlobalID == 0 || opened.NativeGlobalID == gid || opened.RequestID <= 0 {
			t.Fatal("native RBD watch scope differs", opened)
		}
		nativeWatchers := opened.Phases[0].Watchers
		listed := listWatchers(t, "list_watchers_librbd_open")
		if len(listed) != 2 || len(nativeWatchers) != 2 {
			t.Fatal("header watchers with the image open natively", listed, nativeWatchers)
		}
		var librbdCookie uint64
		for _, got := range listed {
			if got.NameType != rbdEntityClient || !got.Address.Endpoint.IsValid() || got.Address.Nonce == 0 {
				t.Fatal("header watcher identity or address is incomplete", got)
			}
			switch uint64(got.NameNumber) {
			case gid:
				if got.Cookie != w.Cookie() {
					t.Fatal("Go header watch cookie changed", got)
				}
			case opened.NativeGlobalID:
				librbdCookie = got.Cookie
			default:
				t.Fatal("unexpected header watcher", got)
			}
			matched := slices.ContainsFunc(nativeWatchers, func(n nativeRBDWatcher) bool {
				return n.ID == uint64(got.NameNumber) && n.Cookie == got.Cookie && strings.HasSuffix(n.Address, fmt.Sprintf("%s/%d", got.Address.Endpoint, got.Address.Nonce))
			})
			if !matched {
				t.Fatal("Go LIST_WATCHERS entry absent from native librbd watchers_list", got, nativeWatchers)
			}
		}
		if librbdCookie == 0 {
			t.Fatal("librbd header watch missing from the Go listing")
		}
		proceed := func(step int) {
			t.Helper()
			signal := fmt.Sprintf("%s %d %d", token, opened.RequestID, step)
			if err := testcluster.WritePublicEvidence(filepath.Join(dir, fmt.Sprintf("rbd-base-watch-proceed-%d", step)), []byte(signal)); err != nil {
				t.Fatal(err)
			}
		}
		next := func(t *testing.T, label string) cephmsgr.OSDWatchEvent {
			t.Helper()
			wait, stop := context.WithTimeout(ctx, 20*time.Second)
			defer stop()
			event, err := w.Next(wait)
			if err != nil {
				t.Fatal(label, err)
			}
			if event.Opcode != cephmsgr.OSDWatchNotification || event.Cookie != w.Cookie() || event.NotifyID == 0 || event.NotifierGlobalID != opened.NativeGlobalID ||
				event.GlobalID != gid || event.Source.ID != r.Primary || !bytes.Equal(event.Payload, rbdHeaderUpdateNotify) {
				t.Fatalf("%s: librbd header update notify differs: opcode=%d cookie=%d notifier=%d payload=%x", label, event.Opcode, event.Cookie, event.NotifierGlobalID, event.Payload)
			}
			return event
		}
		addEvent := func(event cephmsgr.OSDWatchEvent, acknowledged bool) {
			evidence.Events = append(evidence.Events, rbdWatchEventEvidence{Opcode: event.Opcode, Cookie: event.Cookie, NotifyID: event.NotifyID, NotifierGlobalID: event.NotifierGlobalID,
				ReturnCode: event.ReturnCode, GlobalID: event.GlobalID, SessionGeneration: event.SessionGeneration, PayloadHex: hex.EncodeToString(event.Payload),
				RawFrontHex: hex.EncodeToString(event.RawFront), RawDataHex: hex.EncodeToString(event.RawData), Acknowledged: acknowledged})
		}
		ping, err := w.Ping(ctx, r.Epoch)
		record("watch_ping_1", r, watchRequest, ping, err)
		if err != nil {
			t.Fatal("Go header watch ping", err)
		}
		proceed(1)
		first := next(t, "acknowledged resize")
		ack, err := w.Acknowledge(ctx, r.Epoch, first, rbdResponseAck)
		addEvent(first, err == nil)
		record("notify_ack", r, watchRequest, ack, err)
		if err != nil {
			t.Fatal("Go acknowledgement of the librbd notify", err)
		}
		checkReply(t, r, ack, 0)
		resized, err := waitRBDWatchState(ctx, statePath, "resized-acknowledged", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		acknowledgedResize := time.Duration(resized.Phases[1].ResizeNS)
		if resized.Phases[1].Size != plan.ResizeAck || acknowledgedResize <= 0 || acknowledgedResize >= rbdNotifyTimeout || len(resized.Phases[1].Watchers) != 2 {
			t.Fatal("acknowledged native resize did not complete before librbd's notify timeout", resized.Phases[1])
		}
		ping, err = w.Ping(ctx, r.Epoch)
		record("watch_ping_2", r, watchRequest, ping, err)
		if err != nil {
			t.Fatal("Go header watch ping", err)
		}
		proceed(2)
		second := next(t, "unacknowledged resize")
		addEvent(second, false)
		if second.NotifyID == first.NotifyID {
			t.Fatal("second librbd notify reused the first notify ID")
		}
		unacknowledged, err := waitRBDWatchState(ctx, statePath, "resized-unacknowledged", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		unacknowledgedResize := time.Duration(unacknowledged.Phases[2].ResizeNS)
		// The OSD's timer and librbd's clock differ by at most milliseconds;
		// the acknowledged resize above took far less than the timeout.
		if unacknowledged.Phases[2].Size != plan.ResizeNoAck || unacknowledgedResize < rbdNotifyTimeout-100*time.Millisecond {
			t.Fatal("native resize finished before librbd's notify timeout without the Go acknowledgement", unacknowledged.Phases[2])
		}
		evidence.WatchState = &unacknowledged
		evidence.Timings["resize_acknowledged"] = acknowledgedResize.String()
		evidence.Timings["resize_unacknowledged"] = unacknowledgedResize.String()
		select {
		case <-controlDone:
			err = controlErr
		case <-time.After(30 * time.Second):
			err = errors.New("native RBD watch action did not complete")
		}
		if err != nil {
			t.Fatal("native RBD watch action", err)
		}
		var result nativeRBDWatchResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-result.json"), &result); err != nil {
			t.Fatal(err)
		}
		evidence.WatchResult = &result
		final := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.RequestID != opened.RequestID || result.Action != "watch" || !result.Complete || result.Error != "" ||
			result.Mutation == nil || result.Mutation.Kind != "native-resize" || result.Mutation.State != "applied" || final == nil || final.NativeGlobalID != opened.NativeGlobalID ||
			final.Stat.Size != plan.ResizeNoAck || len(final.Snapshots) != 1 || final.Snapshots[0] != snapshot || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut {
			t.Fatal("native RBD watch receipt differs", result.Error)
		}
		// librbd's C_NotifyUpdate logs a timed-out header notify at error
		// level and still completes the operation: only the second resize.
		if timedOut := strings.Count(string(result.Process.Stderr), "update notification timed-out"); timedOut != 1 {
			t.Fatalf("native librbd reported %d timed-out header notifies, want 1: %q", timedOut, result.Process.Stderr)
		}
		if watchers := listWatchers(t, "list_watchers_librbd_closed"); len(watchers) != 1 || watchers[0].Cookie != w.Cookie() || uint64(watchers[0].NameNumber) != gid {
			t.Fatal("librbd close left header watchers other than Go", watchers)
		}
		out, _ := call(t, "get_size_head_resized", header, "get_size", rbdLE64(rbdNoSnap))
		sameAsNative(t, final.ClassCalls, 1, header, "get_size", rbdLE64(rbdNoSnap), out)
		if d := (&rbdDecoder{data: out}); d.u8() != uint8(order) || d.u64() != plan.ResizeNoAck || d.done() != nil {
			t.Fatal("get_size(head) after the native resizes differs", out)
		}
		out, _ = call(t, "get_size_snapshot_resized", header, "get_size", rbdLE64(snapID))
		sameAsNative(t, final.ClassCalls, 2, header, "get_size", rbdLE64(snapID), out)
		if d := (&rbdDecoder{data: out}); d.u8() != uint8(order) || d.u64() != snapshot.Size || d.done() != nil {
			t.Fatal("get_size(snapshot) changed with the head resize", out)
		}
		unwatch, err := w.Unwatch(ctx, r.Epoch)
		record("watch_unwatch", r, watchRequest, unwatch, err)
		if err != nil {
			t.Fatal("Go header unwatch", err)
		}
		watching = false
		if watchers := listWatchers(t, "list_watchers_unwatched"); len(watchers) != 0 {
			t.Fatal("Go unwatch left header watchers", watchers)
		}
		t.Logf("librbd watch cookie %d (global ID %d) listed with Go cookie %d; acknowledged resize %v, unacknowledged resize %v; notify IDs %d and %d", librbdCookie, opened.NativeGlobalID, w.Cookie(), acknowledgedResize, unacknowledgedResize, first.NotifyID, second.NotifyID)
	})
}

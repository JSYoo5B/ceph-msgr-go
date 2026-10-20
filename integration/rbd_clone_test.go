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
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDCloneIntegration checks that the explicit OSD API carries the
// Messenger I/O of a librbd clone (layering, clone format 2) against a parent
// and clone created by native librbd in the RBD base fixture's owned pool:
// the clone header's parent spec and overlap read with librbd's compound
// class calls, never-written clone blocks read from the parent object at the
// parent snapshot, a copy-up write in librbd's form (the guarded write that
// fails with ENOENT, the parent read, then rbd.sparse_copyup with the write
// in one vector, or a separate copy-up under an empty SnapContext once the
// clone has a snapshot), the read-back of a native librbd copy-up, the OSD
// clone state after a clone snapshot, and reads after a native flatten.
// Native librbd and librados observe every step independently, and the
// native clone write and flatten keep their Messenger request lines. See
// integration/fixture/RBD_BASE_FIXTURE.md and RBD_CLONE_VALIDATION.md.
//
// Everything RBD-specific below is test-only consumer logic: the striping
// map, the parent fallback with overlap pruning, the copy-up construction,
// class encodings and decodings, and the Messenger line parser. The product
// carries raw class names, inputs and outputs, raw object bytes, the read
// snapshot and the caller's SnapContext only. Encodings restate the pinned
// cls/rbd/cls_rbd.cc, cls/rbd/cls_rbd_client.cc, cls/rbd/cls_rbd_types.cc,
// include/encoding.h (std::optional), librbd/io/ObjectRequest.cc,
// librbd/io/CopyupRequest.cc, librbd/io/Utils.cc, librbd/image/
// RefreshRequest.cc, messages/MOSDOp.h, messages/MOSDOpReply.h and
// osd/osd_types.cc (LGPL-2.1 references; no code copied).

const (
	// RBD_OPERATION_FEATURE_CLONE_PARENT and CLONE_CHILD
	// (include/rbd/features.h): clone v2 sets them on the parent and the
	// child.
	rbdOpFeatureCloneParent = 1
	rbdOpFeatureCloneChild  = 2
	// RBD_FEATURE_OPERATIONS: set with any operation feature.
	rbdFeatureOperations = 1 << 8
	// rbd_sparse_read_threshold_bytes default: a whole 64 KiB object read
	// is a SPARSE_READ (librbd/io/ObjectRequest.cc read_object).
	rbdSparseReadThreshold = 64 << 10
)

type nativeRBDClonePlan struct {
	Pool             string            `json:"pool"`
	BaseImage        string            `json:"base_image"`
	Parent           string            `json:"parent"`
	Clone            string            `json:"clone"`
	ParentSnapshot   string            `json:"parent_snapshot"`
	CloneSnapshot    string            `json:"clone_snapshot"`
	Order            int               `json:"order"`
	Size             uint64            `json:"size"`
	Features         uint64            `json:"features"`
	CloneFormat      int               `json:"clone_format"`
	ParentPreWrites  []nativeRBDExtent `json:"parent_pre_snapshot_writes"`
	ParentPostWrites []nativeRBDExtent `json:"parent_post_snapshot_writes"`
	GoCopyupWrites   []nativeRBDExtent `json:"go_copyup_writes"`
	NativeWrites     []nativeRBDExtent `json:"native_writes"`
	GoSnapshotWrites []nativeRBDExtent `json:"go_snapshot_writes"`
}

type nativeRBDCloneSpec struct {
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

type nativeRBDCloneView struct {
	ImageID string `json:"image_id"`
	Stat    struct {
		Size       uint64 `json:"size"`
		ObjectSize uint64 `json:"obj_size"`
		Objects    uint64 `json:"num_objs"`
		Order      int    `json:"order"`
		Prefix     string `json:"block_name_prefix"`
		ParentPool uint64 `json:"parent_pool"`
		ParentName string `json:"parent_name"`
	} `json:"stat"`
	Features   uint64 `json:"features"`
	OpFeatures uint64 `json:"op_features"`
	Flags      uint64 `json:"flags"`
	Snapshots  []struct {
		ID        uint64 `json:"id"`
		Name      string `json:"name"`
		Size      uint64 `json:"size"`
		Namespace int    `json:"namespace"`
	} `json:"snapshots"`
	Parent  *nativeRBDCloneSpec `json:"parent"`
	Overlap *uint64             `json:"overlap"`
}

type nativeRBDCloneObject struct {
	Index uint64               `json:"index"`
	Name  string               `json:"name"`
	Head  *nativeRBDObjectView `json:"head"`
	Snap  *nativeRBDObjectView `json:"snap"`
}

type nativeRBDCloneChild struct {
	Pool          string `json:"pool"`
	PoolNamespace string `json:"pool_namespace"`
	Image         string `json:"image"`
	ID            string `json:"id"`
	Trash         bool   `json:"trash"`
}

type nativeRBDCloneTrace struct {
	Direction string `json:"direction"`
	Message   string `json:"message"`
}

// nativeRBDCopyupTrace is one librbd copy-up extent-map line (debug_rbd 20,
// CopyupRequest::convert_copyup_extent_map, from the method name on).
type nativeRBDCopyupTrace struct {
	Message string `json:"message"`
}

type nativeRBDCloneWrite struct {
	Phase     string `json:"phase"`
	Offset    uint64 `json:"offset"`
	Length    uint64 `json:"length"`
	Written   uint64 `json:"written"`
	SHA256    string `json:"sha256"`
	ElapsedNS int64  `json:"elapsed_ns"`
}

type nativeRBDClone struct {
	Action             string                    `json:"action"`
	Token              string                    `json:"token"`
	RequestID          int64                     `json:"request_id"`
	Pool               string                    `json:"pool"`
	PoolID             int64                     `json:"pool_id"`
	Parent             string                    `json:"parent"`
	Clone              string                    `json:"clone"`
	NativeGlobalID     uint64                    `json:"native_global_id"`
	CloneView          *nativeRBDCloneView       `json:"clone_view"`
	CloneSnapshotView  *nativeRBDCloneView       `json:"clone_snapshot_view"`
	ParentView         *nativeRBDCloneView       `json:"parent_view"`
	ParentSnapshotView *nativeRBDCloneView       `json:"parent_snapshot_view"`
	Children           []nativeRBDCloneChild     `json:"children"`
	CloneCalls         []nativeRBDWriteClassCall `json:"clone_calls"`
	CloneSnapshotCalls []nativeRBDWriteClassCall `json:"clone_snapshot_calls"`
	ParentCalls        []nativeRBDWriteClassCall `json:"parent_calls"`
	CloneObjects       []nativeRBDCloneObject    `json:"clone_objects"`
	ParentObjects      []nativeRBDCloneObject    `json:"parent_objects"`
	Blobs              map[string]nativeRBDBlob  `json:"blobs"`
	Writes             []nativeRBDCloneWrite     `json:"writes"`
	Trace              []nativeRBDCloneTrace     `json:"trace"`
	CopyupTrace        []nativeRBDCopyupTrace    `json:"copyup_trace"`
	FlattenNS          int64                     `json:"flatten_ns"`
}

type nativeRBDCloneResult struct {
	Source    string             `json:"source_commit"`
	Token     string             `json:"token"`
	RequestID int64              `json:"request_id"`
	Action    string             `json:"action"`
	Plan      nativeRBDClonePlan `json:"plan"`
	Native    *nativeRBDClone    `json:"native"`
	Process   *struct {
		Stdout, Stderr []byte
		ExitCode       int
		TimedOut       bool
	} `json:"native_process"`
	Mutation *struct {
		Kind      string `json:"kind"`
		RequestID int64  `json:"request_id"`
		ReadOnly  bool   `json:"read_only"`
		State     string `json:"state"`
	} `json:"mutation"`
	Complete  bool   `json:"complete"`
	Error     string `json:"error"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

// Test-only clone encodings and decodings.

// rbdParentSpec is cls::rbd::ParentImageSpec (parent_get): ENCODE_START(1,
// 1), int64 pool, pool namespace, image id and snap id.
type rbdParentSpec struct {
	PoolID    int64
	Namespace string
	ImageID   string
	SnapID    uint64
}

func decodeRBDParentSpec(data []byte) (rbdParentSpec, error) {
	var spec rbdParentSpec
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	spec.PoolID = int64(body.u64())
	spec.Namespace = body.str()
	spec.ImageID = body.str()
	spec.SnapID = body.u64()
	return spec, errors.Join(body.done(), d.done())
}

// decodeRBDOptionalU64 decodes std::optional<uint64_t> (parent_overlap_get):
// a u8 presence flag, then the value when present.
func decodeRBDOptionalU64(data []byte) (*uint64, error) {
	d := &rbdDecoder{data: data}
	switch present := d.u8(); {
	case d.err != nil:
		return nil, d.err
	case present == 0:
		return nil, d.done()
	case present == 1:
		value := d.u64()
		return &value, d.done()
	default:
		return nil, fmt.Errorf("std::optional presence byte %d", present)
	}
}

// rbdLegacyParent is the legacy get_parent output: int64 pool, image id,
// snap id and the overlap (0 without one).
type rbdLegacyParent struct {
	PoolID  int64
	ImageID string
	SnapID  uint64
	Overlap uint64
}

func decodeRBDLegacyParent(data []byte) (rbdLegacyParent, error) {
	d := &rbdDecoder{data: data}
	p := rbdLegacyParent{PoolID: int64(d.u64()), ImageID: d.str(), SnapID: d.u64(), Overlap: d.u64()}
	return p, d.done()
}

// rbdChildSpec is cls::rbd::ChildImageSpec: ENCODE_START(2, 1), int64 pool,
// image id and pool namespace; children_list returns a set of them.
type rbdChildSpec struct {
	PoolID    int64
	ImageID   string
	Namespace string
}

func decodeRBDChildren(data []byte) ([]rbdChildSpec, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	if n > 16 {
		return nil, errors.New("children list exceeds its bound")
	}
	var children []rbdChildSpec
	for i := uint32(0); i < n && d.err == nil; i++ {
		_, body := d.header(1)
		children = append(children, rbdChildSpec{PoolID: int64(body.u64()), ImageID: body.str(), Namespace: body.str()})
		d.adopt(body)
	}
	return children, d.done()
}

func decodeRBDSnapContext(data []byte) (cephmsgr.OSDSnapContext, error) {
	var snapc cephmsgr.OSDSnapContext
	d := &rbdDecoder{data: data}
	snapc.Sequence = d.u64()
	n := d.u32()
	if n > 64 {
		return snapc, errors.New("snap context exceeds its bound")
	}
	for i := uint32(0); i < n && d.err == nil; i++ {
		snapc.Snapshots = append(snapc.Snapshots, d.u64())
	}
	return snapc, d.done()
}

// rbdSparseCopyupInput is cls_rbd sparse_copyup's input as
// cls_rbd_client.cc encodes it: the extent map (u32 count, then u64 offset
// and length pairs) and the data bufferlist (u32 length, then the bytes).
func rbdSparseCopyupInput(extents []cephmsgr.OSDExtent, data []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(extents)))
	for _, e := range extents {
		b = binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(b, e.Offset), e.Length)
	}
	return append(binary.LittleEndian.AppendUint32(b, uint32(len(data))), data...)
}

// rbdMergeExtents orders extents and joins overlapping or adjacent ones.
func rbdMergeExtents(extents []cephmsgr.OSDExtent) []cephmsgr.OSDExtent {
	sorted := slices.Clone(extents)
	slices.SortFunc(sorted, func(a, b cephmsgr.OSDExtent) int { return int(a.Offset) - int(b.Offset) })
	var merged []cephmsgr.OSDExtent
	for _, e := range sorted {
		if e.Length == 0 {
			continue
		}
		if n := len(merged); n > 0 && merged[n-1].Offset+merged[n-1].Length >= e.Offset {
			merged[n-1].Length = max(merged[n-1].Offset+merged[n-1].Length, e.Offset+e.Length) - merged[n-1].Offset
			continue
		}
		merged = append(merged, e)
	}
	return merged
}

// rbdCopyupData restates CopyupRequest::prepare_copyup_data for one object:
// the parent's sparse extents (object offsets) with their bytes, minus the
// pending write's extents for a partial copy-up (exclude is nil for a full
// copy-up, used once the clone has a snapshot).
func rbdCopyupData(extents []cephmsgr.OSDExtent, data []byte, exclude []cephmsgr.OSDExtent) ([]cephmsgr.OSDExtent, []byte) {
	exclude = rbdMergeExtents(exclude)
	var pieces []cephmsgr.OSDExtent
	var bytesOut []byte
	consumed := uint64(0)
	for _, e := range extents {
		cursor, end := e.Offset, e.Offset+e.Length
		emit := func(lo, hi uint64) {
			if lo < hi {
				pieces = append(pieces, cephmsgr.OSDExtent{Offset: lo, Length: hi - lo})
				bytesOut = append(bytesOut, data[consumed+lo-e.Offset:consumed+hi-e.Offset]...)
			}
		}
		for _, x := range exclude {
			if x.Offset+x.Length <= cursor || x.Offset >= end {
				continue
			}
			emit(cursor, max(cursor, x.Offset))
			cursor = max(cursor, min(end, x.Offset+x.Length))
		}
		emit(cursor, end)
		consumed += e.Length
	}
	return pieces, bytesOut
}

// rbdExpandSparse places sparse-read bytes into a zero-filled buffer of the
// given length starting at base.
func rbdExpandSparse(extents []cephmsgr.OSDExtent, data []byte, base, length uint64) ([]byte, error) {
	out, consumed, cursor := make([]byte, length), uint64(0), base
	for _, e := range extents {
		if e.Offset < cursor || e.Length == 0 || e.Offset+e.Length > base+length || consumed+e.Length > uint64(len(data)) {
			return nil, fmt.Errorf("sparse extents are not ordered, positive and inside the range: %+v", extents)
		}
		copy(out[e.Offset-base:], data[consumed:consumed+e.Length])
		consumed, cursor = consumed+e.Length, e.Offset+e.Length
	}
	if consumed != uint64(len(data)) {
		return nil, errors.New("sparse data length differs from its extents")
	}
	return out, nil
}

func rbdMustExpand(t *testing.T, extents []cephmsgr.OSDExtent, data []byte, length uint64) []byte {
	t.Helper()
	out, err := rbdExpandSparse(extents, data, 0, length)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// rbdClonePattern restates the native clone driver's token-bound SHA-256
// stream.
func rbdClonePattern(token, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-clone:%s:%s:%d:%d", token, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

// rbdTraceMessage is one parsed native Messenger line: an MOSDOp sent
// (messages/MOSDOp.h print: reqid, pgid, hobject, ops, snapc, flags, epoch)
// or an MOSDOpReply received (tid, object, ops, versions, result).
type rbdTraceMessage struct {
	Send        bool
	Tid         uint64
	Object      string
	Snap        string `json:",omitempty"`
	Ops         string
	SnapContext string `json:",omitempty"`
	Flags       string `json:",omitempty"`
	Result      int32
	UserVersion uint64 `json:",omitempty"`
	Raw         string
}

var (
	rbdTraceRequest = regexp.MustCompile(`^osd_op\((\S+) (\S+) -?\d+:[0-9a-f]{8}:[^:\s]*:[^:\s]*:([^:\s]+):(head|snapdir|[0-9a-f]+) \[(.*)\] snapc ([0-9a-f]+=\[[0-9a-f,]*\]) (\S+) e(\d+)\)$`)
	rbdTraceReply   = regexp.MustCompile(`^osd_op_reply\((\d+) (\S+) \[(.*)\] v(\S+) uv(\d+) (?:ondisk|onnvram|ack) = (-?\d+)(?: \(.*\))?\)$`)
)

func parseRBDTrace(lines []nativeRBDCloneTrace) ([]rbdTraceMessage, error) {
	var messages []rbdTraceMessage
	for _, line := range lines {
		m := rbdTraceMessage{Send: line.Direction == "send", Raw: line.Message}
		if m.Send {
			g := rbdTraceRequest.FindStringSubmatch(line.Message)
			if g == nil {
				return nil, fmt.Errorf("unparsed native request line %q", line.Message)
			}
			tid, err := strconv.ParseUint(g[1][strings.LastIndexByte(g[1], ':')+1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("native request id %q", g[1])
			}
			m.Tid, m.Object, m.Snap, m.Ops, m.SnapContext, m.Flags = tid, g[3], g[4], g[5], g[6], g[7]
		} else {
			g := rbdTraceReply.FindStringSubmatch(line.Message)
			if g == nil {
				return nil, fmt.Errorf("unparsed native reply line %q", line.Message)
			}
			tid, _ := strconv.ParseUint(g[1], 10, 64)
			uv, _ := strconv.ParseUint(g[5], 10, 64)
			result, err := strconv.ParseInt(g[6], 10, 32)
			if err != nil {
				return nil, err
			}
			m.Tid, m.Object, m.Ops, m.UserVersion, m.Result = tid, g[2], g[3], uv, int32(result)
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// rbdCopyupLine is "convert_copyup_extent_map: <object> image_extents=[..],
// object_extents=[..]" (include/types.h prints a vector of pairs as a flat
// comma list): the parent extents librbd's copy-up read returned, mapped to
// the copied-up object's offsets, before the pending write is subtracted.
var rbdCopyupLine = regexp.MustCompile(`^convert_copyup_extent_map: (rbd_data\.[0-9a-f]+\.[0-9a-f]{16}) image_extents=\[([0-9,]*)\], object_extents=\[([0-9,]*)\]$`)

func parseRBDCopyupTrace(lines []nativeRBDCopyupTrace) (map[string][]cephmsgr.OSDExtent, error) {
	maps := map[string][]cephmsgr.OSDExtent{}
	for _, line := range lines {
		g := rbdCopyupLine.FindStringSubmatch(line.Message)
		if g == nil {
			return nil, fmt.Errorf("unparsed librbd copy-up line %q", line.Message)
		}
		if _, seen := maps[g[1]]; seen {
			return nil, fmt.Errorf("two librbd copy-up maps for %s", g[1])
		}
		var extents []cephmsgr.OSDExtent
		var image []uint64
		for i, list := range []string{g[2], g[3]} {
			var values []uint64
			for _, field := range strings.Split(list, ",") {
				if field == "" {
					continue
				}
				v, err := strconv.ParseUint(field, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("librbd copy-up extent %q", field)
				}
				values = append(values, v)
			}
			if len(values)%2 != 0 {
				return nil, fmt.Errorf("librbd copy-up extent list %q is not pairs", list)
			}
			if i == 0 {
				image = values
				continue
			}
			if len(values) != len(image) {
				return nil, fmt.Errorf("librbd copy-up image and object extents differ in count: %q", line.Message)
			}
			for j := 0; j < len(values); j += 2 {
				if values[j+1] != image[j+1] {
					return nil, fmt.Errorf("librbd copy-up image and object extent lengths differ: %q", line.Message)
				}
				extents = append(extents, cephmsgr.OSDExtent{Offset: values[j], Length: values[j+1]})
			}
		}
		maps[g[1]] = extents
	}
	return maps, nil
}

// rbdTraceOutLength is the out=<n>b of a reply op print (the per-op output
// length), or -1 without one.
var rbdTraceOut = regexp.MustCompile(` out=(\d+)b`)

func rbdTraceOutLength(ops string) int {
	g := rbdTraceOut.FindStringSubmatch(ops)
	if g == nil {
		return -1
	}
	n, err := strconv.Atoi(g[1])
	if err != nil {
		return -1
	}
	return n
}

// rbdTraceExchange is one native request with its reply.
type rbdTraceExchange struct {
	Request, Reply rbdTraceMessage
	ReplyIndex     int
	RequestIndex   int
}

// rbdTraceExchanges pairs every request with the one later reply of the same
// tid and object; an unanswered request, a duplicate or an orphan reply is an
// error.
func rbdTraceExchanges(messages []rbdTraceMessage) ([]rbdTraceExchange, error) {
	var exchanges []rbdTraceExchange
	answered := map[int]bool{}
	for i, m := range messages {
		if !m.Send {
			continue
		}
		found := -1
		for j := i + 1; j < len(messages); j++ {
			if r := messages[j]; !r.Send && r.Tid == m.Tid && r.Object == m.Object {
				found = j
				break
			}
		}
		if found < 0 || answered[found] {
			return nil, fmt.Errorf("native request %q has no reply", m.Raw)
		}
		answered[found] = true
		exchanges = append(exchanges, rbdTraceExchange{Request: m, Reply: messages[found], RequestIndex: i, ReplyIndex: found})
	}
	if len(answered) != len(messages)-len(exchanges) {
		return nil, errors.New("native trace holds a reply without its request")
	}
	return exchanges, nil
}

// rbdCopyupEvidence records one Go copy-up: the parent read, the copy-up
// extents and input, and the resulting object.
type rbdCopyupEvidence struct {
	Label            string
	Object           string
	ObjectNumber     uint64
	Write            cephmsgr.OSDExtent
	Full             bool
	ParentObject     string
	ParentSnapshot   uint64
	ParentExtents    []cephmsgr.OSDExtent
	CopyupExtents    []cephmsgr.OSDExtent
	CopyupDataLength int
	InputLength      int
	InputSHA256      string
	GuardCode        int32
	GuardResults     []int32
	CopyupResults    []int32
	WriteResults     []int32 `json:",omitempty"`
	UserVersion      uint64
	ObjectSize       uint64
	SparseExtents    []cephmsgr.OSDExtent
}

type rbdNoopCopyupEvidence struct {
	Label                     string
	Method                    string
	InputLength               int
	Result                    int32
	UserVersionBefore         uint64
	UserVersionAfter          uint64
	ReplayVersion             cephmsgr.OSDVersion
	ObjectUnchanged           bool
	ReplyFlags                uint64
	NativeCopyupInputExpected int `json:",omitempty"`
}

type rbdCloneEvidence struct {
	Token, Pool                             string
	PoolID                                  int64
	GoIdentity                              string
	GoGlobalID                              uint64
	Store                                   string
	RequireOSDRelease, RequireMinCompat     string
	MinCompatClient                         string
	ParentID, CloneID                       string
	ParentPrefix, ClonePrefix               string
	ParentSnapshotID, CloneSnapshotID       uint64
	ParentSpec                              rbdParentSpec
	HeadOverlap                             *uint64
	SnapshotOverlap                         *uint64
	FlattenedHeadOverlap                    *uint64
	FlattenedLegacyParent                   *rbdLegacyParent
	Children                                []rbdChildSpec
	Requests                                []rbdWriteRequestEvidence
	ClassOutputs                            map[string]string
	Copyups                                 []rbdCopyupEvidence
	NoopCopyups                             []rbdNoopCopyupEvidence
	ListSnaps                               map[string]map[string]any
	ParentExtents                           map[string][]cephmsgr.OSDExtent
	NativeTraces                            map[string][]rbdTraceMessage
	NativeCopyupInputLength                 int
	NativeCopyupExtentMaps                  map[string][]cephmsgr.OSDExtent
	NativeParentSparseReadOutputs           map[string]int
	GoParentSparseReadOutputs               map[string]int
	CopyupInputsEqualNative                 map[string]string
	NativeFlattenCopyupInputs               map[string]int
	NativeActions                           []rbdNativeActionEvidence
	ImageSHA256                             map[string]string
	Timings                                 map[string]string
	ParentHeadDiffersFromSnapshotPerObject  []bool
	NativeCopyupSparseExtents               []cephmsgr.OSDExtent
	CloneObjectSizesAfterFlatten            map[string]uint64
	FlattenReadsWithoutParent, FlattenedAll bool
}

func TestRBDCloneTestOnlyEncodings(t *testing.T) {
	spec := rbdEnvelope(append(append(append(binary.LittleEndian.AppendUint64(nil, 3), rbdString("")...), rbdString("abc")...), rbdLE64(5)...))
	if got, err := decodeRBDParentSpec(spec); err != nil || got != (rbdParentSpec{PoolID: 3, ImageID: "abc", SnapID: 5}) {
		t.Fatal("ParentImageSpec decoding", got, err)
	}
	empty := rbdEnvelope(append(append(append(rbdLE64(^uint64(0)), rbdString("")...), rbdString("")...), rbdLE64(rbdNoSnap)...))
	if got, err := decodeRBDParentSpec(empty); err != nil || got.PoolID != -1 || got.ImageID != "" || got.SnapID != rbdNoSnap {
		t.Fatal("absent ParentImageSpec decoding", got, err)
	}
	for cut := 0; cut < len(spec); cut++ {
		if _, err := decodeRBDParentSpec(spec[:cut]); err == nil {
			t.Fatal("truncated ParentImageSpec admitted", cut)
		}
	}
	if _, err := decodeRBDParentSpec(append(bytes.Clone(spec), 0)); err == nil {
		t.Fatal("trailing ParentImageSpec byte admitted")
	}
	if v, err := decodeRBDOptionalU64([]byte{0}); err != nil || v != nil {
		t.Fatal("absent overlap", v, err)
	}
	if v, err := decodeRBDOptionalU64(append([]byte{1}, rbdLE64(384<<10)...)); err != nil || v == nil || *v != 384<<10 {
		t.Fatal("present overlap", v, err)
	}
	for _, bad := range [][]byte{nil, {1}, {2, 0, 0, 0, 0, 0, 0, 0, 0}, {0, 0}, append([]byte{1}, rbdLE64(1)[:7]...)} {
		if _, err := decodeRBDOptionalU64(bad); err == nil {
			t.Fatal("malformed std::optional admitted", bad)
		}
	}
	legacy := append(append(append(rbdLE64(3), rbdString("abc")...), rbdLE64(5)...), rbdLE64(7)...)
	if got, err := decodeRBDLegacyParent(legacy); err != nil || got != (rbdLegacyParent{PoolID: 3, ImageID: "abc", SnapID: 5, Overlap: 7}) {
		t.Fatal("legacy get_parent decoding", got, err)
	}
	child := append([]byte{2, 1}, binary.LittleEndian.AppendUint32(nil, 8+4+3+4)...)
	child = append(append(append(child, rbdLE64(3)...), rbdString("def")...), rbdString("")...)
	children := append(binary.LittleEndian.AppendUint32(nil, 1), child...)
	if got, err := decodeRBDChildren(children); err != nil || !reflect.DeepEqual(got, []rbdChildSpec{{PoolID: 3, ImageID: "def"}}) {
		t.Fatal("children_list decoding", got, err)
	}
	for cut := 0; cut < len(children); cut++ {
		if _, err := decodeRBDChildren(children[:cut]); err == nil {
			t.Fatal("truncated children list admitted", cut)
		}
	}
	if got, err := decodeRBDSnapContext(append(append(rbdLE64(9), 1, 0, 0, 0), rbdLE64(9)...)); err != nil || got.Sequence != 9 || !reflect.DeepEqual(got.Snapshots, []uint64{9}) {
		t.Fatal("snap context decoding", got, err)
	}
	// sparse_copyup input: one extent 4096~4 and the four bytes.
	if got := hex.EncodeToString(rbdSparseCopyupInput([]cephmsgr.OSDExtent{{Offset: 4096, Length: 4}}, []byte("abcd"))); got != "01000000"+"0010000000000000"+"0400000000000000"+"04000000"+"61626364" {
		t.Fatal("sparse_copyup input encoding", got)
	}
	// A partial copy-up keeps the parent extents outside the write.
	k := uint64(1 << 10)
	parent := []cephmsgr.OSDExtent{{Offset: 8 * k, Length: 16 * k}, {Offset: 40 * k, Length: 8 * k}}
	data := make([]byte, 24*k)
	for i := range data {
		data[i] = byte(i / 1024)
	}
	extents, copied := rbdCopyupData(parent, data, []cephmsgr.OSDExtent{{Offset: 20 * k, Length: 8 * k}})
	if !reflect.DeepEqual(extents, []cephmsgr.OSDExtent{{Offset: 8 * k, Length: 12 * k}, {Offset: 40 * k, Length: 8 * k}}) || len(copied) != int(20*k) ||
		!bytes.Equal(copied[:12*k], data[:12*k]) || !bytes.Equal(copied[12*k:], data[16*k:]) {
		t.Fatal("partial copy-up extents", extents, len(copied))
	}
	if full, fullData := rbdCopyupData(parent, data, nil); !reflect.DeepEqual(full, parent) || !bytes.Equal(fullData, data) {
		t.Fatal("full copy-up extents", full)
	}
	if got := rbdMergeExtents([]cephmsgr.OSDExtent{{Offset: 20 * k, Length: 8 * k}, {Offset: 8 * k, Length: 12 * k}, {Offset: 40 * k, Length: 8 * k}, {Offset: 9 * k}}); !reflect.DeepEqual(got,
		[]cephmsgr.OSDExtent{{Offset: 8 * k, Length: 20 * k}, {Offset: 40 * k, Length: 8 * k}}) {
		t.Fatal("extent merge", got)
	}
	if got, err := rbdExpandSparse([]cephmsgr.OSDExtent{{Offset: 2, Length: 2}}, []byte("xy"), 0, 6); err != nil || string(got) != "\x00\x00xy\x00\x00" {
		t.Fatal("sparse expansion", got, err)
	}
	if _, err := rbdExpandSparse([]cephmsgr.OSDExtent{{Offset: 2, Length: 2}}, []byte("xyz"), 0, 6); err == nil {
		t.Fatal("sparse data longer than its extents admitted")
	}
	// Messenger lines as messages/MOSDOp.h and MOSDOpReply.h print them.
	lines := []nativeRBDCloneTrace{
		{"send", "osd_op(unknown.0.0:5 3.6 3:6c2e5d4f:::rbd_data.10a2b3c4d5e6.0000000000000003:head [stat,set-alloc-hint object_size 65536 write_size 65536,write 4096~4096 in=4096b] snapc 0=[] ondisk+write+known_if_redirected+supports_pool_eio e24)"},
		{"receive", "osd_op_reply(5 rbd_data.10a2b3c4d5e6.0000000000000003 [stat,set-alloc-hint object_size 65536 write_size 65536,write 4096~4096] v0'0 uv0 ondisk = -2 ((2) No such file or directory))"},
		{"send", "osd_op(client.4321.0:6 3.1 3:83a1b2c3:::rbd_data.f00f00f00f00.0000000000000003:1c [sparse-read 0~65536] snapc 0=[] ondisk+read+known_if_redirected+supports_pool_eio e24)"},
		{"receive", "osd_op_reply(6 rbd_data.f00f00f00f00.0000000000000003 [sparse-read 0~65536 out=12312b] v0'0 uv12 ondisk = 0)"},
	}
	messages, err := parseRBDTrace(lines)
	if err != nil || len(messages) != 4 || messages[0].Tid != 5 || messages[0].Object != "rbd_data.10a2b3c4d5e6.0000000000000003" || messages[0].Snap != "head" ||
		messages[0].SnapContext != "0=[]" || !strings.HasPrefix(messages[0].Ops, "stat,") || messages[1].Result != -2 || messages[2].Snap != "1c" || messages[2].Tid != 6 ||
		messages[3].Result != 0 || messages[3].UserVersion != 12 {
		t.Fatal("Messenger line parsing", messages, err)
	}
	exchanges, err := rbdTraceExchanges(messages)
	if err != nil || len(exchanges) != 2 || exchanges[1].Reply.Ops != "sparse-read 0~65536 out=12312b" {
		t.Fatal("request and reply pairing", exchanges, err)
	}
	if _, err := rbdTraceExchanges(messages[:3]); err == nil {
		t.Fatal("unanswered native request admitted")
	}
	if _, err := parseRBDTrace([]nativeRBDCloneTrace{{"send", "osd_op(unknown.0.0:5 3.6 (undecoded) ondisk+write e24)"}}); err == nil {
		t.Fatal("undecoded request line admitted")
	}
	if n := rbdTraceOutLength(exchanges[1].Reply.Ops); n != 12312 || rbdTraceOutLength("sparse-read 0~65536") != -1 {
		t.Fatal("reply output length parsing", n)
	}
	// librbd copy-up maps (include/types.h prints pairs as a flat list).
	maps, err := parseRBDCopyupTrace([]nativeRBDCopyupTrace{
		{"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000003 image_extents=[212992,4096,237568,8192], object_extents=[16384,4096,40960,8192]"},
		{"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000004 image_extents=[], object_extents=[]"},
	})
	if err != nil || !reflect.DeepEqual(maps["rbd_data.10a2b3c4d5e6.0000000000000003"], []cephmsgr.OSDExtent{{Offset: 16 * k, Length: 4 * k}, {Offset: 40 * k, Length: 8 * k}}) ||
		len(maps["rbd_data.10a2b3c4d5e6.0000000000000004"]) != 0 || len(maps) != 2 {
		t.Fatal("librbd copy-up map parsing", maps, err)
	}
	for _, bad := range []string{
		"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000003 image_extents=[1,2,3], object_extents=[1,2,3]",
		"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000003 image_extents=[1,2], object_extents=[1,3]",
		"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000003 image_extents=[1,2], object_extents=[]",
		"prepare_copyup_data: rbd_data.10a2b3c4d5e6.0000000000000003 processing full copy-up",
	} {
		if _, err := parseRBDCopyupTrace([]nativeRBDCopyupTrace{{bad}}); err == nil {
			t.Fatal("malformed librbd copy-up line admitted", bad)
		}
	}
	twice := nativeRBDCopyupTrace{"convert_copyup_extent_map: rbd_data.10a2b3c4d5e6.0000000000000003 image_extents=[], object_extents=[]"}
	if _, err := parseRBDCopyupTrace([]nativeRBDCopyupTrace{twice, twice}); err == nil {
		t.Fatal("two copy-up maps of one object admitted")
	}
}

func TestCephRBDCloneIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD fixture (CEPH_MSGR_TEST_RBD=1)")
	}
	var oracle nativeRBDOracle
	if err := readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle); err != nil {
		t.Fatal(err)
	}
	token, identity := oracle.Token, os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.Plan.Pool != "ceph-msgr-rbd-"+token || oracle.Pool.Name != oracle.Plan.Pool ||
		oracle.Pool.Flags&1 != 0 || identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" || oracle.Caps["osd"] != "profile rbd pool="+oracle.Plan.Pool {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity)
	}
	store := os.Getenv("CEPH_MSGR_TEST_OSD_STORE")
	if store == "" {
		store = "memstore"
	}
	bluestore := store == "bluestore"
	options := integrationOptions(t)
	encoded, err := os.ReadFile(os.Getenv("CEPH_MSGR_RBD_BASE_KEY_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	private, err := cephx.ParseKey(strings.TrimSpace(string(encoded)))
	wantType, profileErr := mdsFreshExpectedKeyType(os.Getenv("CEPH_MSGR_TEST_KEY_TYPE"))
	if err != nil || profileErr != nil || private.Type() != wantType {
		t.Fatal("RBD credential does not match the selected key profile")
	}
	if options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(encoded))); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	options.Identity, options.EnableOSD = identity, true
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	gid := c.Snapshot().GlobalID
	evidence := rbdCloneEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, GoGlobalID: gid, Store: store, ClassOutputs: map[string]string{},
		ListSnaps: map[string]map[string]any{}, ParentExtents: map[string][]cephmsgr.OSDExtent{}, NativeTraces: map[string][]rbdTraceMessage{}, ImageSHA256: map[string]string{},
		Timings: map[string]string{}, NativeFlattenCopyupInputs: map[string]int{}, CloneObjectSizesAfterFlatten: map[string]uint64{}, NativeCopyupExtentMaps: map[string][]cephmsgr.OSDExtent{},
		NativeParentSparseReadOutputs: map[string]int{}, GoParentSparseReadOutputs: map[string]int{}, CopyupInputsEqualNative: map[string]string{}}
	defer func() {
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-clone-results.json"), data)
		}
		if err != nil {
			t.Error("RBD clone evidence", err)
		}
	}()

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDClonePlan
	native := func(t *testing.T, action string) *nativeRBDClone {
		t.Helper()
		receipt := filepath.Join(dir, "rbd-clone-"+action+".json")
		if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		actionStarted := time.Now()
		if err := testcluster.RequestControl(ctx, dir, "rbd-clone", "rbd-clone-completed", token, action); err != nil {
			t.Fatal("native RBD clone", action, err)
		}
		var result nativeRBDCloneResult
		if err := readMDSFreshJSON(receipt, &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-clone-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != strings.HasPrefix(action, "observe-") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != oracle.Pool.ID ||
			(plan.Clone != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD clone receipt differs", action, result.Error)
		}
		if plan.Clone == "" {
			plan = result.Plan
		}
		if len(result.Process.Stderr) != 0 {
			t.Fatalf("native RBD clone %s wrote to stderr: %s", action, result.Process.Stderr)
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		evidence.Timings["native_"+action] = time.Since(actionStarted).String()
		return n
	}
	blobs := func(t *testing.T, n *nativeRBDClone) map[string][]byte {
		t.Helper()
		views := map[string][]byte{}
		for key, blob := range n.Blobs {
			views[key] = readRBDBlob(t, dir, blob)
		}
		for _, key := range []string{"clone_head", "parent_head", "parent_snap"} {
			if uint64(len(views[key])) != plan.Size {
				t.Fatal("native RBD clone binary view missing", n.Action, key)
			}
		}
		for key, data := range views {
			sum := sha256.Sum256(data)
			evidence.ImageSHA256[n.Action+"/"+key] = hex.EncodeToString(sum[:])
		}
		return views
	}

	setup := native(t, "setup")
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.BaseImage != oracle.Plan.Image || plan.Parent != "ceph-msgr-parent-"+token || plan.Clone != "ceph-msgr-clone-"+token ||
		plan.ParentSnapshot != "ceph-msgr-psnap-"+token[:8] || plan.CloneSnapshot != "ceph-msgr-csnap-"+token[:8] || plan.Order != 16 || plan.Size != 384*k ||
		plan.Features != 1 || plan.CloneFormat != 2 || len(plan.GoCopyupWrites) != 1 || len(plan.NativeWrites) != 1 || len(plan.GoSnapshotWrites) != 2 {
		t.Fatal("RBD clone plan differs", plan)
	}
	order := plan.Order
	objectSize := uint64(1) << order
	objects := plan.Size / objectSize
	// The test-only patterns equal the native driver's writes.
	for i, w := range setup.Writes {
		extent, phase := nativeRBDExtent{}, "parent-pre"
		if i < len(plan.ParentPreWrites) {
			extent = plan.ParentPreWrites[i]
		} else {
			extent, phase = plan.ParentPostWrites[i-len(plan.ParentPreWrites)], "parent-post"
		}
		sum := sha256.Sum256(rbdClonePattern(token, phase, w.Offset, w.Length))
		if w.Phase != phase || w.Offset != extent.Offset || w.Length != extent.Length || hex.EncodeToString(sum[:]) != w.SHA256 {
			t.Fatal("test-only RBD clone pattern differs from the native driver", w)
		}
	}
	apply := func(image []byte, phase string, extents []nativeRBDExtent) []byte {
		image = bytes.Clone(image)
		for _, e := range extents {
			copy(image[e.Offset:], rbdClonePattern(token, phase, e.Offset, e.Length))
		}
		return image
	}
	// Test-only images: the parent at its snapshot and head, and the clone
	// after each step.
	parentSnap := apply(make([]byte, plan.Size), "parent-pre", plan.ParentPreWrites)
	parentHead := apply(parentSnap, "parent-post", plan.ParentPostWrites)
	cloneSetup := parentSnap
	cloneCopiedUp := apply(cloneSetup, "go", plan.GoCopyupWrites)
	cloneNativeWritten := apply(cloneCopiedUp, "native", plan.NativeWrites)
	cloneFinal := apply(cloneNativeWritten, "go", plan.GoSnapshotWrites)
	setupViews := blobs(t, setup)
	if !bytes.Equal(setupViews["parent_snap"], parentSnap) || !bytes.Equal(setupViews["parent_head"], parentHead) || !bytes.Equal(setupViews["clone_head"], cloneSetup) {
		t.Fatal("native librbd parent or clone reads differ from the planned images")
	}
	cv, pv := setup.CloneView, setup.ParentView
	if len(pv.Snapshots) != 1 || cv.Parent == nil || cv.Overlap == nil || *cv.Overlap != plan.Size || cv.Parent.PoolID != oracle.Pool.ID || cv.Parent.PoolName != plan.Pool ||
		cv.Parent.PoolNamespace != "" || cv.Parent.ImageID != pv.ImageID || cv.Parent.ImageName != plan.Parent || cv.Parent.SnapID != pv.Snapshots[0].ID ||
		cv.Parent.SnapName != plan.ParentSnapshot || cv.Parent.Trash || cv.OpFeatures != rbdOpFeatureCloneChild || pv.OpFeatures != rbdOpFeatureCloneParent ||
		len(setup.Children) != 1 || setup.Children[0] != (nativeRBDCloneChild{Pool: plan.Pool, Image: plan.Clone, ID: cv.ImageID}) {
		t.Fatal("native librbd clone v2 parent link differs", cv.Parent, cv.Overlap, cv.OpFeatures, pv.OpFeatures, setup.Children)
	}
	for _, o := range setup.CloneObjects {
		if o.Head.Exists {
			t.Fatal("native clone holds a data object before any write", o.Name)
		}
	}
	cloneID, parentID, parentSnapID := cv.ImageID, pv.ImageID, pv.Snapshots[0].ID
	evidence.CloneID, evidence.ParentID, evidence.ParentSnapshotID = cloneID, parentID, parentSnapID
	cloneHeader, parentHeader := "rbd_header."+cloneID, "rbd_header."+parentID
	cloneNames, parentNames := make([]string, objects), make([]string, objects)
	for i := range objects {
		cloneNames[i] = fmt.Sprintf("rbd_data.%s.%016x", cloneID, i)
		parentNames[i] = fmt.Sprintf("rbd_data.%s.%016x", parentID, i)
		if setup.CloneObjects[i].Name != cloneNames[i] || setup.ParentObjects[i].Name != parentNames[i] {
			t.Fatal("Go data object names differ from native librbd layout", cloneNames[i], parentNames[i])
		}
	}

	// The cluster's release gates: librbd enables sparse copy-up when
	// require_osd_release is octopus or later (image/RefreshRequest.cc).
	dump, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("native osd dump", err)
	}
	var releases struct {
		RequireOSDRelease      string `json:"require_osd_release"`
		RequireMinCompatClient string `json:"require_min_compat_client"`
		MinCompatClient        string `json:"min_compat_client"`
	}
	if err := json.Unmarshal(dump.Data, &releases); err != nil || releases.RequireOSDRelease != "tentacle" {
		t.Fatal("osd dump release gates", releases, err)
	}
	evidence.RequireOSDRelease, evidence.RequireMinCompat, evidence.MinCompatClient = releases.RequireOSDRelease, releases.RequireMinCompatClient, releases.MinCompatClient

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
		e := rbdWriteRequestEvidence{Label: label, Route: r, ReadSnapshot: req.ReadSnapshot, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion, LegacyVersion: reply.LegacyVersion, ReplayVersion: reply.ReplayVersion}
		if req.SnapContext.Sequence != 0 || len(req.SnapContext.Snapshots) != 0 {
			snapc := req.SnapContext
			e.SnapContext = &snapc
		}
		if req.ModificationTime != (cephmsgr.OSDTime{}) {
			mtime := req.ModificationTime
			e.ModificationTime = &mtime
		}
		for _, op := range req.Operations {
			sum := sha256.Sum256(op.Data)
			in := rbdOperationInputEvidence{Code: uint16(op.Code), Flags: uint32(op.Flags), Offset: op.Offset, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]),
				Class: op.Class.Class, Method: op.Class.Method, Mode: uint8(op.Class.Mode)}
			if len(op.Data) <= 512 {
				in.DataHex = hex.EncodeToString(op.Data)
			} else if op.Code == cephmsgr.OSDCall {
				// The copy-up input's extent map and data length prefix.
				in.DataHex = hex.EncodeToString(op.Data[:min(len(op.Data), 4+16*int(binary.LittleEndian.Uint32(op.Data))+4)])
			}
			if op.Code == cephmsgr.OSDSetAllocHint {
				hint := op.AllocHint
				in.AllocHint = &hint
			}
			e.Inputs = append(e.Inputs, in)
		}
		for _, op := range reply.Operations {
			sum := sha256.Sum256(op.Data)
			o := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]), Extents: op.SparseRead.Extents}
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
	// checkReply asserts request identity, the raw-hash PG echo and the epoch;
	// a successful mutation also carries ONDISK and a user version.
	checkReply := func(t *testing.T, r rbdRoute, reply cephmsgr.OSDReply, code int32, mutation bool) {
		t.Helper()
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != gid || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 ||
			mutation && code == 0 && (reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || reply.UserVersion == 0) {
			t.Fatalf("OSD reply identity differs: code=%d tid=%d gid=%d pg=%+v epoch=%d flags=%#x version=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG, reply.MapEpoch, reply.Flags, reply.UserVersion)
		}
	}
	enoent := func(err error) bool {
		var server *cephmsgr.OSDError
		return errors.As(err, &server) && server.Code == -2
	}
	now := func() cephmsgr.OSDTime {
		t := time.Now()
		return cephmsgr.OSDTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
	}
	results := func(reply cephmsgr.OSDReply) []int32 {
		var codes []int32
		for _, op := range reply.Operations {
			codes = append(codes, op.Result)
		}
		return codes
	}

	type callSpec struct {
		class, method string
		mode          cephmsgr.OSDCallMode
		input         []byte
	}
	read := func(class, method string, input []byte) callSpec {
		return callSpec{class: class, method: method, mode: cephmsgr.OSDCallRead, input: input}
	}
	// calls sends the CALLs as one vector, as librados does for an
	// ObjectReadOperation of several cls_client *_start calls; each per-op
	// output is returned separately.
	calls := func(t *testing.T, label, object string, specs ...callSpec) [][]byte {
		t.Helper()
		r, o := route(t, object)
		var ops []cephmsgr.OSDOperation
		write := false
		for _, s := range specs {
			ops = append(ops, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: s.input, Class: cephmsgr.OSDClassInput{Class: s.class, Method: s.method, Mode: s.mode, MaxOutputBytes: 4096}})
			write = write || s.mode == cephmsgr.OSDCallWrite
		}
		req := request(r, ops...)
		if write {
			req.ModificationTime = now()
		}
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatalf("Go CALL vector %s: %v", label, err)
		}
		checkReply(t, r, reply, 0, write)
		var outputs [][]byte
		for i, got := range reply.Operations {
			s := specs[i]
			if got.Result != 0 || got.Class.ClassLength != uint8(len(s.class)) || got.Class.MethodLength != uint8(len(s.method)) || got.Class.InputLength != uint32(len(s.input)) ||
				got.Class.OutputLength != uint64(len(got.Data)) {
				t.Fatalf("CALL %s.%s per-op result or union echo differs in %s: %+v", s.class, s.method, label, got.Class)
			}
			evidence.ClassOutputs[fmt.Sprintf("%s/%d.%s", label, i, s.method)] = hex.EncodeToString(got.Data)
			outputs = append(outputs, got.Data)
		}
		return outputs
	}
	// sameAsNative compares a Go input and raw output with one native
	// librados exec of the same method on the same object.
	sameAsNative := func(t *testing.T, action string, e nativeRBDWriteClassCall, object, class, method string, input, output []byte) {
		t.Helper()
		if e.Object != object || e.Class != class || e.Method != method || e.InputHex != hex.EncodeToString(input) || e.Code != 0 || e.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("Go CALL %s.%s differs from native librados exec (%s): native=%+v go_input=%x go_output=%x", class, method, action, e, input, output)
		}
	}
	// sparseRead sends one SPARSE_READ alone (the product admits it only
	// alone), at the head (snap 0) or at a read snapshot, as librbd does for
	// whole-object reads of the clone, of the clone at a clone snapshot and
	// of the parent at the parent snapshot. It reports ENOENT. The raw
	// output length is kept by label for the native trace comparison.
	sparseOutputs := map[string]int{}
	sparseRead := func(t *testing.T, label, object string, snap, offset, length uint64) (cephmsgr.OSDSparseReadResult, bool) {
		t.Helper()
		r, o := route(t, object)
		op := cephmsgr.OSDOperation{Code: cephmsgr.OSDSparseRead, Offset: offset, Length: length, SparseRead: cephmsgr.OSDSparseReadInput{MaxOutputBytes: 1 << 20}}
		req := request(r, op)
		req.ReadSnapshot = snap
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if enoent(err) {
			checkReply(t, r, reply, -2, false)
			return cephmsgr.OSDSparseReadResult{}, false
		}
		if err != nil {
			t.Fatalf("%s read: %v", label, err)
		}
		checkReply(t, r, reply, 0, false)
		got := reply.Operations[0]
		if got.Result != 0 || got.Offset != offset || got.Length != uint64(len(got.SparseRead.Data)) || len(got.Data) != 8+16*len(got.SparseRead.Extents)+len(got.SparseRead.Data) {
			t.Fatalf("%s SPARSE_READ echo or encoding differs: %+v", label, got.SparseRead.Extents)
		}
		sparseOutputs[label] = len(got.Data)
		return got.SparseRead, true
	}
	// objectState is a librados-style STAT and READ of a whole object.
	objectState := func(t *testing.T, label, object string, snap uint64) ([]byte, bool) {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
		req.ReadSnapshot = snap
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if enoent(err) {
			checkReply(t, r, reply, -2, false)
			return nil, false
		}
		if err != nil {
			t.Fatalf("%s STAT+READ: %v", label, err)
		}
		checkReply(t, r, reply, 0, false)
		data := reply.Operations[1].Data
		if len(reply.Operations[0].Data) != 16 || binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(data)) {
			t.Fatalf("%s stat size differs from the read length", label)
		}
		return data, true
	}
	listSnaps := func(t *testing.T, label, object string) (cephmsgr.OSDSnapshotList, bool) {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if evidence.ListSnaps[label] == nil {
			evidence.ListSnaps[label] = map[string]any{}
		}
		if enoent(err) {
			checkReply(t, r, reply, -2, false)
			evidence.ListSnaps[label][object] = "ENOENT"
			return cephmsgr.OSDSnapshotList{}, false
		}
		if err != nil {
			t.Fatalf("%s LIST_SNAPS: %v", label, err)
		}
		checkReply(t, r, reply, 0, false)
		evidence.ListSnaps[label][object] = reply.Operations[0].Snapshots
		return reply.Operations[0].Snapshots, true
	}
	// storeParentExtents is the sparse map a SPARSE_READ of a whole parent
	// object at the parent snapshot returns on the active store: BlueStore
	// reports the written ranges (merged), MemStore the whole object up to
	// its end; nil when the object did not exist at the snapshot. The bytes
	// come from the test-only parent image, checked equal to native librbd's.
	storeParentExtents := func(number uint64) ([]cephmsgr.OSDExtent, []byte) {
		written := rbdMergeExtents(rbdObjectWrites(plan.ParentPreWrites, order)[number])
		if len(written) == 0 {
			return nil, nil
		}
		extents := written
		if !bluestore {
			last := written[len(written)-1]
			extents = []cephmsgr.OSDExtent{{Offset: 0, Length: last.Offset + last.Length}}
		}
		var data []byte
		for _, e := range extents {
			lo := number*objectSize + e.Offset
			data = append(data, parentSnap[lo:lo+e.Length]...)
		}
		return extents, data
	}
	// parentRead reads the parent object extent a clone object maps to, at
	// the parent snapshot, pruned by the overlap (librbd io::util::read_parent
	// and ObjectRequest::compute_parent_extents with the default striping:
	// the same object number and offsets), with librbd's SPARSE_READ at the
	// parent snapshot. It returns the object-relative sparse extents and
	// bytes, nil when the overlap leaves nothing or the parent object is
	// absent at the snapshot. A whole-object read must return the store's
	// map of the parent object at the snapshot.
	parentRead := func(t *testing.T, label string, number uint64, overlap *uint64) ([]cephmsgr.OSDExtent, []byte) {
		t.Helper()
		start := number * objectSize
		if overlap == nil || *overlap <= start {
			return nil, nil
		}
		length := min(objectSize, *overlap-start)
		got, exists := sparseRead(t, label, parentNames[number], parentSnapID, 0, length)
		wantExtents, wantData := storeParentExtents(number)
		if length == objectSize && (exists != (wantExtents != nil) || !reflect.DeepEqual(got.Extents, wantExtents) || !bytes.Equal(got.Data, wantData)) {
			t.Fatalf("%s: parent object %d SPARSE_READ at snapshot %d differs from the %s map %+v: got %+v (exists %v)", label, number, parentSnapID, store, wantExtents, got.Extents, exists)
		}
		if !exists {
			return nil, nil
		}
		evidence.ParentExtents[label] = got.Extents
		return got.Extents, got.Data
	}
	// readImage reads the clone as librbd's ObjectReadRequest does for
	// whole-object reads: a SPARSE_READ of each clone object (64 KiB reaches
	// the sparse-read threshold) at the head or the clone snapshot, and on
	// ENOENT the parent extent at the parent snapshot.
	readImage := func(t *testing.T, label string, snap uint64, overlap *uint64) []byte {
		t.Helper()
		image := make([]byte, plan.Size)
		for i := range objects {
			got, exists := sparseRead(t, fmt.Sprintf("%s_clone_%d", label, i), cloneNames[i], snap, 0, objectSize)
			if !exists {
				extents, data := parentRead(t, fmt.Sprintf("%s_parent_%d", label, i), i, overlap)
				got = cephmsgr.OSDSparseReadResult{Extents: extents, Data: data}
			}
			expanded, err := rbdExpandSparse(got.Extents, got.Data, 0, objectSize)
			if err != nil {
				t.Fatalf("%s object %d: %v", label, i, err)
			}
			copy(image[i*objectSize:], expanded)
		}
		sum := sha256.Sum256(image)
		evidence.ImageSHA256["go/"+label] = hex.EncodeToString(sum[:])
		return image
	}
	// compareObjects checks Go STAT+READ of every clone object (head, and the
	// clone snapshot when given) against the native librados observation.
	compareObjects := func(t *testing.T, label string, n *nativeRBDClone, views map[string][]byte, snap uint64) {
		t.Helper()
		for i, o := range n.CloneObjects {
			kinds := []string{"head"}
			if snap != 0 {
				kinds = append(kinds, "snap")
			}
			for _, kind := range kinds {
				view, blob, readSnap := o.Head, views["clone_objects_head"], uint64(0)
				if kind == "snap" {
					view, blob, readSnap = o.Snap, views["clone_objects_snap"], snap
				}
				data, exists := objectState(t, fmt.Sprintf("%s_%s_%d", label, kind, i), cloneNames[i], readSnap)
				if view == nil || exists != view.Exists || exists && !bytes.Equal(data, blob[view.Offset:view.Offset+view.Size]) {
					t.Fatalf("%s: native librados %s object %d differs from the Go read (native %+v, Go exists %v size %d)", label, kind, i, view, exists, len(data))
				}
			}
		}
	}
	// guardedWrite is librbd's first attempt at a write to a clone object
	// with a parent overlap (AbstractObjectWriteRequest::write_object):
	// assert_exists (STAT), the allocation hint (FAILOK, as librados sets it)
	// and the write, under the clone's SnapContext.
	allocHint := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}}
	guardedWrite := func(t *testing.T, label string, number uint64, w cephmsgr.OSDExtent, data []byte, snapc cephmsgr.OSDSnapContext) (cephmsgr.OSDReply, error) {
		t.Helper()
		r, o := route(t, cloneNames[number])
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, allocHint, cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: w.Offset, Length: w.Length, Data: data})
		req.SnapContext, req.ModificationTime = snapc, now()
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err == nil {
			checkReply(t, r, reply, 0, true)
		} else if enoent(err) {
			checkReply(t, r, reply, -2, false)
		}
		return reply, err
	}
	sparseExtents := func(t *testing.T, label string, number uint64) []cephmsgr.OSDExtent {
		t.Helper()
		got, exists := sparseRead(t, label, cloneNames[number], 0, 0, objectSize)
		if !exists {
			t.Fatalf("%s: clone object %d is absent", label, number)
		}
		return got.Extents
	}
	// expectedSparse is the object's covered range after a copy-up of the
	// given parent extents and the given writes: BlueStore reports logical
	// extents (holes stay holes), MemStore the whole range up to EOF.
	expectedSparse := func(size uint64, parts ...[]cephmsgr.OSDExtent) []cephmsgr.OSDExtent {
		if !bluestore {
			return []cephmsgr.OSDExtent{{Offset: 0, Length: size}}
		}
		return rbdMergeExtents(slices.Concat(parts...))
	}
	objectExtent := func(t *testing.T, e nativeRBDExtent) (uint64, cephmsgr.OSDExtent) {
		t.Helper()
		pieces := rbdObjectExtents(e.Offset, e.Length, order)
		if len(pieces) != 1 {
			t.Fatal("planned write crosses an object boundary", e)
		}
		return pieces[0][0], cephmsgr.OSDExtent{Offset: pieces[0][1], Length: pieces[0][2]}
	}
	models := map[uint64]*rbdCloneModel{}
	for i := range objects {
		models[i] = &rbdCloneModel{}
	}
	checkListSnaps := func(t *testing.T, label string, snapID uint64) {
		t.Helper()
		for i := range objects {
			got, exists := listSnaps(t, label, cloneNames[i])
			m := models[i]
			if exists != m.Exists || exists && !sameSnapshotList(got, m.listing(snapID)) {
				t.Fatalf("%s object %d LIST_SNAPS differs from the clone model: got %+v (exists %v) want %+v", label, i, got, exists, m.listing(snapID))
			}
		}
	}
	nativeTrace := func(t *testing.T, n *nativeRBDClone) []rbdTraceExchange {
		t.Helper()
		messages, err := parseRBDTrace(n.Trace)
		if err == nil {
			evidence.NativeTraces[n.Action] = messages
		}
		exchanges, pairErr := rbdTraceExchanges(messages)
		if err != nil || pairErr != nil {
			t.Fatal("native Messenger trace", n.Action, err, pairErr)
		}
		return exchanges
	}
	describe := func(exchanges []rbdTraceExchange) string {
		var b strings.Builder
		for _, x := range exchanges {
			fmt.Fprintf(&b, "\n  %s@%s [%s] snapc %s -> %d", x.Request.Object, x.Request.Snap, x.Request.Ops, x.Request.SnapContext, x.Reply.Result)
		}
		return b.String()
	}
	const hintOps = "set-alloc-hint object_size 65536 write_size 65536"
	var headOverlap *uint64
	t.Run("clone_header_and_parent_reads", func(t *testing.T) {
		// librbd's open: get_id, the initial get_object_prefix, then the
		// refresh vectors of mutable metadata and of the parent
		// (image/RefreshRequest.cc send_v2_get_mutable_metadata and
		// send_v2_get_parent), each one compound read.
		idObject := "rbd_id." + plan.Clone
		id := calls(t, "clone_get_id", idObject, read("rbd", "get_id", nil))[0]
		sameAsNative(t, "setup", setup.CloneCalls[0], idObject, "rbd", "get_id", nil, id)
		if d := (&rbdDecoder{data: id}); d.str() != cloneID || d.done() != nil {
			t.Fatal("rbd_id get_id differs from the native clone id", id)
		}
		prefix := calls(t, "clone_get_object_prefix", cloneHeader, read("rbd", "get_object_prefix", nil))[0]
		sameAsNative(t, "setup", setup.CloneCalls[9], cloneHeader, "rbd", "get_object_prefix", nil, prefix)
		if d := (&rbdDecoder{data: prefix}); d.str() != "rbd_data."+cloneID || d.done() != nil {
			t.Fatal("clone object prefix differs", prefix)
		}
		evidence.ClonePrefix = "rbd_data." + cloneID
		mutableInputs := [][]byte{rbdLE64(rbdNoSnap), append(rbdLE64(rbdNoSnap), 0), rbdLE64(rbdNoSnap), nil, rbdLockGetInfoInput()}
		mutable := calls(t, "clone_mutable_metadata", cloneHeader, read("rbd", "get_size", mutableInputs[0]), read("rbd", "get_features", mutableInputs[1]),
			read("rbd", "get_flags", mutableInputs[2]), read("rbd", "get_snapcontext", nil), read("lock", "get_info", mutableInputs[4]))
		for i, method := range []string{"get_size", "get_features", "get_flags", "get_snapcontext", "get_info"} {
			class := "rbd"
			if method == "get_info" {
				class = "lock"
			}
			sameAsNative(t, "setup", setup.CloneCalls[1+i], cloneHeader, class, method, mutableInputs[i], mutable[i])
		}
		d := &rbdDecoder{data: mutable[0]}
		if headOrder, size := d.u8(), d.u64(); d.done() != nil || int(headOrder) != order || size != plan.Size {
			t.Fatal("clone get_size differs", headOrder, size)
		}
		d = &rbdDecoder{data: mutable[1]}
		// Clone format 2 enables RBD_FEATURE_OPERATIONS with the operation
		// features; for a read-write open both bits are incompatible
		// (RBD_FEATURES_RW_INCOMPATIBLE).
		if features, incompatible := d.u64(), d.u64(); d.done() != nil || features != plan.Features|rbdFeatureOperations || incompatible != features || features != cv.Features {
			t.Fatal("clone get_features differs", features, incompatible)
		}
		if d = (&rbdDecoder{data: mutable[2]}); d.u64() != 0 || d.done() != nil || cv.Flags != 0 {
			t.Fatal("clone flags differ", mutable[2])
		}
		if snapc, err := decodeRBDSnapContext(mutable[3]); err != nil || snapc.Sequence != 0 || len(snapc.Snapshots) != 0 {
			t.Fatal("new clone SnapContext differs", snapc, err)
		}
		if info, err := decodeRBDLockInfo(mutable[4]); err != nil || len(info.Lockers) != 0 {
			t.Fatal("clone header is locked", info, err)
		}
		parent := calls(t, "clone_parent", cloneHeader, read("rbd", "parent_get", nil), read("rbd", "parent_overlap_get", rbdLE64(rbdNoSnap)))
		sameAsNative(t, "setup", setup.CloneCalls[6], cloneHeader, "rbd", "parent_get", nil, parent[0])
		sameAsNative(t, "setup", setup.CloneCalls[7], cloneHeader, "rbd", "parent_overlap_get", rbdLE64(rbdNoSnap), parent[1])
		spec, err := decodeRBDParentSpec(parent[0])
		if err != nil || spec != (rbdParentSpec{PoolID: oracle.Pool.ID, ImageID: parentID, SnapID: parentSnapID}) {
			t.Fatal("parent_get differs from the native librbd parent spec", spec, err)
		}
		headOverlap, err = decodeRBDOptionalU64(parent[1])
		if err != nil || headOverlap == nil || *headOverlap != *cv.Overlap {
			t.Fatal("parent_overlap_get(head) differs from native librbd overlap()", headOverlap, err)
		}
		evidence.ParentSpec, evidence.HeadOverlap = spec, headOverlap
		legacy := calls(t, "clone_get_parent", cloneHeader, read("rbd", "get_parent", rbdLE64(rbdNoSnap)))[0]
		sameAsNative(t, "setup", setup.CloneCalls[8], cloneHeader, "rbd", "get_parent", rbdLE64(rbdNoSnap), legacy)
		if got, err := decodeRBDLegacyParent(legacy); err != nil || got != (rbdLegacyParent{PoolID: spec.PoolID, ImageID: spec.ImageID, SnapID: spec.SnapID, Overlap: *headOverlap}) {
			t.Fatal("legacy get_parent differs from parent_get", got, err)
		}
		opFeatures := calls(t, "clone_op_features", cloneHeader, read("rbd", "op_features_get", nil))[0]
		sameAsNative(t, "setup", setup.CloneCalls[10], cloneHeader, "rbd", "op_features_get", nil, opFeatures)
		if d = (&rbdDecoder{data: opFeatures}); d.u64() != rbdOpFeatureCloneChild || d.done() != nil {
			t.Fatal("clone op features differ", opFeatures)
		}
		// The parent: its id object names the spec's image id; the header at
		// the parent snapshot gives the layout and the clone v2 child link.
		pid := calls(t, "parent_get_id", "rbd_id."+plan.Parent, read("rbd", "get_id", nil))[0]
		sameAsNative(t, "setup", setup.ParentCalls[0], "rbd_id."+plan.Parent, "rbd", "get_id", nil, pid)
		if d = (&rbdDecoder{data: pid}); d.str() != spec.ImageID || d.done() != nil {
			t.Fatal("parent id object differs from the parent spec", pid)
		}
		parentInputs := [][]byte{nil, rbdLE64(parentSnapID), nil, rbdLE64(parentSnapID), rbdLE64(parentSnapID), nil}
		parentMethods := []string{"get_object_prefix", "get_size", "get_snapcontext", "snapshot_get", "children_list", "op_features_get"}
		var specs []callSpec
		for i, method := range parentMethods {
			specs = append(specs, read("rbd", method, parentInputs[i]))
		}
		out := calls(t, "parent_header", parentHeader, specs...)
		for i, method := range parentMethods {
			sameAsNative(t, "setup", setup.ParentCalls[1+i], parentHeader, "rbd", method, parentInputs[i], out[i])
		}
		if d = (&rbdDecoder{data: out[0]}); d.str() != "rbd_data."+parentID || d.done() != nil {
			t.Fatal("parent object prefix differs", out[0])
		}
		evidence.ParentPrefix = "rbd_data." + parentID
		if d = (&rbdDecoder{data: out[1]}); d.u8() != uint8(order) || d.u64() != plan.Size || d.done() != nil {
			t.Fatal("parent get_size at its snapshot differs", out[1])
		}
		if snapc, err := decodeRBDSnapContext(out[2]); err != nil || snapc.Sequence != parentSnapID || !reflect.DeepEqual(snapc.Snapshots, []uint64{parentSnapID}) {
			t.Fatal("parent SnapContext differs", snapc, err)
		}
		info, err := decodeRBDSnapshotInfo(out[3])
		if err != nil || info.ID != parentSnapID || info.Name != plan.ParentSnapshot || info.ImageSize != plan.Size || info.NamespaceType != 0 || info.ChildCount != 1 {
			t.Fatal("parent snapshot_get differs (clone v2 counts one child)", info, err)
		}
		children, err := decodeRBDChildren(out[4])
		if err != nil || !reflect.DeepEqual(children, []rbdChildSpec{{PoolID: oracle.Pool.ID, ImageID: cloneID}}) {
			t.Fatal("parent children_list differs from the clone", children, err)
		}
		evidence.Children = children
		if d = (&rbdDecoder{data: out[5]}); d.u64() != rbdOpFeatureCloneParent || d.done() != nil {
			t.Fatal("parent op features differ", out[5])
		}
		// The parent objects at the parent snapshot equal native librados.
		parentViews := setupViews["parent_objects_snap"]
		for i, o := range setup.ParentObjects {
			data, exists := objectState(t, fmt.Sprintf("parent_object_snap_%d", i), parentNames[i], parentSnapID)
			if exists != o.Snap.Exists || exists && !bytes.Equal(data, parentViews[o.Snap.Offset:o.Snap.Offset+o.Snap.Size]) {
				t.Fatalf("parent object %d at the snapshot differs from native librados", i)
			}
		}
		// Never-written clone blocks: the clone object is absent, so each
		// object's bytes come from the parent object at the parent snapshot.
		// Every object of the parent head differs from the snapshot, so a
		// head read would be detected.
		for i := range objects {
			lo, hi := i*objectSize, (i+1)*objectSize
			differs := !bytes.Equal(parentHead[lo:hi], parentSnap[lo:hi])
			evidence.ParentHeadDiffersFromSnapshotPerObject = append(evidence.ParentHeadDiffersFromSnapshotPerObject, differs)
			if !differs {
				t.Fatal("planned parent head equals its snapshot in object", i)
			}
		}
		image := readImage(t, "setup_head", 0, headOverlap)
		if !bytes.Equal(image, cloneSetup) || !bytes.Equal(image, setupViews["clone_head"]) {
			t.Fatal("Go clone read through the parent differs from native librbd")
		}
		// A small read below the sparse-read threshold is a plain READ, on
		// the clone object and then on the parent object.
		small := cephmsgr.OSDExtent{Offset: objectSize - 4*k, Length: 4 * k}
		last := objects - 1
		r, o := route(t, cloneNames[last])
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: small.Offset, Length: small.Length})
		reply, err := o.Request(ctx, req)
		record("small_read_clone", r, req, reply, err)
		if !enoent(err) {
			t.Fatal("never-written clone object read did not fail with ENOENT", err)
		}
		checkReply(t, r, reply, -2, false)
		r, o = route(t, parentNames[last])
		req = request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: small.Offset, Length: small.Length})
		req.ReadSnapshot = parentSnapID
		reply, err = o.Request(ctx, req)
		record("small_read_parent", r, req, reply, err)
		if err != nil {
			t.Fatal("parent snapshot read", err)
		}
		checkReply(t, r, reply, 0, false)
		lo := last*objectSize + small.Offset
		if !bytes.Equal(reply.Operations[0].Data, setupViews["clone_head"][lo:lo+small.Length]) || bytes.Equal(reply.Operations[0].Data, parentHead[lo:lo+small.Length]) {
			t.Fatal("small parent read differs from the native clone extent")
		}
		t.Logf("clone %s -> parent pool %d image %s snap %d, overlap %d; parent snapshot child count %d; sparse-copyup gate require_osd_release=%s (min compat client %s/%s)",
			cloneID, spec.PoolID, spec.ImageID, spec.SnapID, *headOverlap, info.ChildCount, releases.RequireOSDRelease, releases.RequireMinCompatClient, releases.MinCompatClient)
	})
	if t.Failed() {
		return
	}

	// copyup sends librbd's copy-up for one clone object: the guarded write
	// fails with ENOENT, the parent extent is read at the parent snapshot,
	// and the copy-up follows. Without clone snapshots it is one vector
	// [rbd.sparse_copyup, SETALLOCHINT, WRITE] under the clone SnapContext
	// carrying only the parent extents outside the write; with snapshots the
	// full parent data goes in [rbd.sparse_copyup, SETALLOCHINT] under an
	// empty SnapContext, then [WRITE] under the clone SnapContext
	// (io/CopyupRequest.cc copyup and prepare_copyup_data).
	copyup := func(t *testing.T, label string, e nativeRBDExtent, snapc cephmsgr.OSDSnapContext, overlap *uint64) rbdCopyupEvidence {
		t.Helper()
		number, w := objectExtent(t, e)
		data := rbdClonePattern(token, "go", e.Offset, e.Length)
		full := len(snapc.Snapshots) != 0
		ev := rbdCopyupEvidence{Label: label, Object: cloneNames[number], ObjectNumber: number, Write: w, Full: full, ParentObject: parentNames[number], ParentSnapshot: parentSnapID}
		reply, err := guardedWrite(t, label+"_guarded", number, w, data, snapc)
		ev.GuardCode, ev.GuardResults = reply.Code, results(reply)
		if !enoent(err) || len(reply.Operations) != 3 || reply.Operations[0].Result != -2 {
			t.Fatalf("%s: guarded write to the unwritten clone object did not stop at assert_exists with ENOENT: %v %+v", label, err, ev.GuardResults)
		}
		extents, parentData := parentRead(t, label+"_parent", number, overlap)
		ev.ParentExtents = extents
		var exclude []cephmsgr.OSDExtent
		if !full {
			exclude = []cephmsgr.OSDExtent{w}
		}
		pieces, copied := rbdCopyupData(extents, parentData, exclude)
		input := rbdSparseCopyupInput(pieces, copied)
		sum := sha256.Sum256(input)
		ev.CopyupExtents, ev.CopyupDataLength, ev.InputLength, ev.InputSHA256 = pieces, len(copied), len(input), hex.EncodeToString(sum[:])
		r, o := route(t, cloneNames[number])
		call := cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: "rbd", Method: "sparse_copyup", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}}
		writeOp := cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: w.Offset, Length: w.Length, Data: data}
		if !full {
			req := request(r, call, allocHint, writeOp)
			req.SnapContext, req.ModificationTime = snapc, now()
			reply, err := o.Request(ctx, req)
			record(label+"_copyup_write", r, req, reply, err)
			if err != nil {
				t.Fatalf("%s: copy-up vector: %v", label, err)
			}
			checkReply(t, r, reply, 0, true)
			ev.CopyupResults, ev.UserVersion = results(reply), reply.UserVersion
			got := reply.Operations
			if len(got) != 3 || got[0].Result != 0 || got[0].Class.InputLength != uint32(len(input)) || got[0].Class.OutputLength != 0 || len(got[0].Data) != 0 ||
				got[1].Result != 0 || got[1].AllocHint != allocHint.AllocHint || got[2].Result != 0 || got[2].Length != w.Length {
				t.Fatalf("%s: copy-up vector per-op results differ: %+v", label, ev.CopyupResults)
			}
		} else {
			req := request(r, call, allocHint)
			req.ModificationTime = now()
			reply, err := o.Request(ctx, req)
			record(label+"_copyup", r, req, reply, err)
			if err != nil {
				t.Fatalf("%s: copy-up under an empty SnapContext: %v", label, err)
			}
			checkReply(t, r, reply, 0, true)
			ev.CopyupResults = results(reply)
			if got := reply.Operations; len(got) != 2 || got[0].Result != 0 || got[0].Class.InputLength != uint32(len(input)) || len(got[0].Data) != 0 || got[1].Result != 0 {
				t.Fatalf("%s: copy-up per-op results differ: %+v", label, ev.CopyupResults)
			}
			req = request(r, writeOp)
			req.SnapContext, req.ModificationTime = snapc, now()
			reply, err = o.Request(ctx, req)
			record(label+"_write", r, req, reply, err)
			if err != nil {
				t.Fatalf("%s: write after the copy-up: %v", label, err)
			}
			checkReply(t, r, reply, 0, true)
			ev.WriteResults, ev.UserVersion = results(reply), reply.UserVersion
			if reply.Operations[0].Result != 0 || reply.Operations[0].Length != w.Length {
				t.Fatalf("%s: write per-op result differs: %+v", label, ev.WriteResults)
			}
		}
		// The OSD clone model: the copy-up writes the extents at the object's
		// first sequence (an empty SnapContext for the full form), then the
		// write under the clone SnapContext.
		copySeq := snapc.Sequence
		if full {
			copySeq = 0
		}
		for _, p := range pieces {
			models[number].write(p.Offset, p.Length, copySeq)
		}
		models[number].write(w.Offset, w.Length, snapc.Sequence)
		ev.ObjectSize = models[number].Size
		evidence.Copyups = append(evidence.Copyups, ev)
		return ev
	}

	// sameNativeCopyup checks Go's copy-up input for one object against the
	// copy-up native librbd sent for the same object in a traced action:
	// librbd's logged parent extent map (convert_copyup_extent_map) equals
	// Go's SPARSE_READ map of the parent object at the parent snapshot, the
	// parent sparse-read reply's output length equals Go's raw output, and
	// the input rebuilt from librbd's map and the bytes cls_rbd wrote (native
	// librados's read of the copied-up object) equals Go's input byte for
	// byte; its length is the native CALL's in=<n>b minus the class and
	// method names. exclude is the pending write of a partial copy-up.
	sameNativeCopyup := func(t *testing.T, n *nativeRBDClone, views map[string][]byte, exchanges []rbdTraceExchange, number uint64, parentLabel string, goExtents []cephmsgr.OSDExtent, goData []byte, exclude []cephmsgr.OSDExtent) []byte {
		t.Helper()
		maps, err := parseRBDCopyupTrace(n.CopyupTrace)
		if err != nil {
			t.Fatal("native librbd copy-up trace", n.Action, err)
		}
		logged, ok := maps[cloneNames[number]]
		evidence.NativeCopyupExtentMaps[n.Action+"/"+cloneNames[number]] = logged
		if !ok || !reflect.DeepEqual(logged, goExtents) {
			t.Fatalf("%s: librbd's copy-up parent map of object %d %+v (logged %v) differs from Go's SPARSE_READ at the parent snapshot %+v", n.Action, number, logged, ok, goExtents)
		}
		nativeOut := -1
		for _, x := range exchanges {
			if x.Request.Object == parentNames[number] && strings.HasPrefix(x.Request.Ops, "sparse-read ") {
				nativeOut = rbdTraceOutLength(x.Reply.Ops)
			}
		}
		evidence.NativeParentSparseReadOutputs[n.Action+"/"+parentNames[number]] = nativeOut
		evidence.GoParentSparseReadOutputs[n.Action+"/"+parentNames[number]] = sparseOutputs[parentLabel]
		if nativeOut < 0 || nativeOut != sparseOutputs[parentLabel] {
			t.Fatalf("%s: native parent sparse-read output %d differs from Go's raw SPARSE_READ output %d", n.Action, nativeOut, sparseOutputs[parentLabel])
		}
		pieces, copied := rbdCopyupData(goExtents, goData, exclude)
		input := rbdSparseCopyupInput(pieces, copied)
		total := uint64(0)
		for _, e := range logged {
			total += e.Length
		}
		nativePieces, _ := rbdCopyupData(logged, make([]byte, total), exclude)
		view := n.CloneObjects[number].Head
		if view == nil || !view.Exists {
			t.Fatalf("%s: native librados does not see copied-up object %d", n.Action, number)
		}
		object := views["clone_objects_head"][view.Offset : view.Offset+view.Size]
		var written []byte
		for _, p := range nativePieces {
			if p.Offset+p.Length > uint64(len(object)) {
				t.Fatalf("%s: copy-up extent %+v lies past native object %d (%d bytes)", n.Action, p, number, len(object))
			}
			written = append(written, object[p.Offset:p.Offset+p.Length]...)
		}
		nativeInput := rbdSparseCopyupInput(nativePieces, written)
		sum := sha256.Sum256(input)
		evidence.CopyupInputsEqualNative[n.Action+"/"+cloneNames[number]] = fmt.Sprintf("%d bytes sha256 %x", len(input), sum)
		if !bytes.Equal(input, nativeInput) {
			t.Fatalf("%s: Go's copy-up input for object %d (%d bytes) differs from librbd's (%d bytes, extents %+v)", n.Action, number, len(input), len(nativeInput), nativePieces)
		}
		return input
	}

	t.Run("go_copyup_write", func(t *testing.T) {
		e := plan.GoCopyupWrites[0]
		ev := copyup(t, "go_copyup", e, cephmsgr.OSDSnapContext{}, headOverlap)
		number := ev.ObjectNumber
		lo := number * objectSize
		data, exists := objectState(t, "go_copyup_readback", cloneNames[number], 0)
		if !exists || uint64(len(data)) != ev.ObjectSize || !bytes.Equal(data, cloneCopiedUp[lo:lo+ev.ObjectSize]) {
			t.Fatalf("copied-up object differs from the parent bytes with the Go write (size %d, want %d)", len(data), ev.ObjectSize)
		}
		got := rbdMergeExtents(sparseExtents(t, "go_copyup_sparse", number))
		want := expectedSparse(ev.ObjectSize, ev.CopyupExtents, []cephmsgr.OSDExtent{ev.Write})
		evidence.Copyups[len(evidence.Copyups)-1].SparseExtents = got
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("copied-up object's sparse extents %+v differ from the copy-up extents and the write %+v", got, want)
		}
		checkListSnaps(t, "after_go_copyup", 0)
		// A second copy-up on the existing object is a no-op: cls_rbd's
		// sparse_copyup and copyup return 0 without writing when the object
		// exists (check_exists / cls_cxx_stat), whatever data they carry.
		marker := rbdClonePattern(token, "marker", lo, objectSize)
		r, o := route(t, cloneNames[number])
		for _, form := range []struct {
			method string
			input  []byte
		}{
			{"sparse_copyup", rbdSparseCopyupInput([]cephmsgr.OSDExtent{{Offset: 0, Length: objectSize}}, marker)},
			{"copyup", marker},
		} {
			req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: form.input, Class: cephmsgr.OSDClassInput{Class: "rbd", Method: form.method, Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}})
			req.ModificationTime = now()
			reply, err := o.Request(ctx, req)
			record("second_"+form.method, r, req, reply, err)
			if err != nil {
				t.Fatalf("second %s on the existing object: %v", form.method, err)
			}
			checkReply(t, r, reply, 0, true)
			after, _ := objectState(t, "second_"+form.method+"_readback", cloneNames[number], 0)
			noop := rbdNoopCopyupEvidence{Label: "second_" + form.method, Method: form.method, InputLength: len(form.input), Result: reply.Operations[0].Result,
				UserVersionBefore: ev.UserVersion, UserVersionAfter: reply.UserVersion, ReplayVersion: reply.ReplayVersion, ObjectUnchanged: bytes.Equal(after, data), ReplyFlags: reply.Flags}
			evidence.NoopCopyups = append(evidence.NoopCopyups, noop)
			if reply.Operations[0].Result != 0 || len(reply.Operations[0].Data) != 0 || !noop.ObjectUnchanged || reply.UserVersion != ev.UserVersion {
				t.Fatalf("second %s was not a no-op: %+v", form.method, noop)
			}
		}
		checkListSnaps(t, "after_second_copyups", 0)
		// Native librbd reads the Go bytes and the parent bytes around them;
		// the parent is unchanged.
		observed := native(t, "observe-copyup")
		views := blobs(t, observed)
		if !bytes.Equal(views["clone_head"], cloneCopiedUp) || !bytes.Equal(views["parent_head"], parentHead) || !bytes.Equal(views["parent_snap"], parentSnap) ||
			!bytes.Equal(views["parent_objects_snap"], setupViews["parent_objects_snap"]) {
			t.Fatal("native librbd reads after the Go copy-up differ (clone, or a changed parent)")
		}
		compareObjects(t, "observe_copyup", observed, views, 0)
		for i, call := range observed.CloneCalls {
			if call != setup.CloneCalls[i] {
				t.Fatal("clone header changed with the copy-up", call)
			}
		}
		if image := readImage(t, "copyup_head", 0, headOverlap); !bytes.Equal(image, cloneCopiedUp) {
			t.Fatal("Go clone read after the copy-up differs")
		}
		t.Logf("object %d copy-up: parent extents %+v -> copy-up extents %+v (%d bytes, input %d), write %+v; results guard %v, vector %v; object size %d, sparse %+v",
			number, ev.ParentExtents, ev.CopyupExtents, ev.CopyupDataLength, ev.InputLength, ev.Write, ev.GuardResults, ev.CopyupResults, ev.ObjectSize, got)
	})
	if t.Failed() {
		return
	}

	t.Run("native_copyup_write", func(t *testing.T) {
		written := native(t, "write")
		e := plan.NativeWrites[0]
		sum := sha256.Sum256(rbdClonePattern(token, "native", e.Offset, e.Length))
		if len(written.Writes) != 1 || written.Writes[0].SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatal("native clone write differs from the test-only pattern", written.Writes)
		}
		number, w := objectExtent(t, e)
		// The copy-up input librbd builds for this write (partial: no clone
		// snapshot yet) from its SPARSE_READ of the parent object at the
		// parent snapshot, rebuilt by Go from its own SPARSE_READ there.
		extents, parentData := parentRead(t, "native_copyup_parent", number, headOverlap)
		pieces, _ := rbdCopyupData(extents, parentData, []cephmsgr.OSDExtent{w})
		exchanges := nativeTrace(t, written)
		input := sameNativeCopyup(t, written, blobs(t, written), exchanges, number, "native_copyup_parent", extents, parentData, []cephmsgr.OSDExtent{w})
		evidence.NativeCopyupInputLength = len(input)
		writeOps := fmt.Sprintf("write %d~%d in=%db", w.Offset, w.Length, w.Length)
		want := []struct {
			object, snap, ops, snapc string
			result                   int32
		}{
			{cloneNames[number], "head", "stat," + hintOps + "," + writeOps, "0=[]", -2},
			{parentNames[number], strconv.FormatUint(parentSnapID, 16), fmt.Sprintf("sparse-read 0~%d", objectSize), "", 0},
			// The CALL's indata is the class name, method name and input.
			{cloneNames[number], "head", fmt.Sprintf("call rbd.sparse_copyup in=%db,%s,%s", 3+len("sparse_copyup")+len(input), hintOps, writeOps), "0=[]", 0},
		}
		ok := len(exchanges) == len(want)
		for i := range want {
			if !ok {
				break
			}
			x := exchanges[i]
			ok = x.Request.Object == want[i].object && x.Request.Snap == want[i].snap && x.Request.Ops == want[i].ops && (want[i].snapc == "" || x.Request.SnapContext == want[i].snapc) &&
				x.Reply.Result == want[i].result && (i == 0 || exchanges[i-1].ReplyIndex < x.RequestIndex)
		}
		if !ok {
			t.Fatalf("native librbd copy-up vectors differ from Go's form (input %d bytes):%s", len(input), describe(exchanges))
		}
		lo := number * objectSize
		for _, p := range pieces {
			models[number].write(p.Offset, p.Length, 0)
		}
		models[number].write(w.Offset, w.Length, 0)
		size := models[number].Size
		data, exists := objectState(t, "native_copyup_readback", cloneNames[number], 0)
		if !exists || uint64(len(data)) != size || !bytes.Equal(data, cloneNativeWritten[lo:lo+size]) {
			t.Fatalf("native copy-up object differs from the parent bytes with the native write (size %d, want %d)", len(data), size)
		}
		got := rbdMergeExtents(sparseExtents(t, "native_copyup_sparse", number))
		evidence.NativeCopyupSparseExtents = got
		// BlueStore keeps the parent's holes: librbd used sparse_copyup
		// (a thick copyup would write the zeros between the extents).
		if wantSparse := expectedSparse(size, pieces, []cephmsgr.OSDExtent{w}); !reflect.DeepEqual(got, wantSparse) {
			t.Fatalf("native copy-up object's sparse extents %+v differ from %+v", got, wantSparse)
		}
		checkListSnaps(t, "after_native_copyup", 0)
		views := blobs(t, written)
		if !bytes.Equal(views["clone_head"], cloneNativeWritten) || !bytes.Equal(views["parent_head"], parentHead) || !bytes.Equal(views["parent_snap"], parentSnap) {
			t.Fatal("native librbd reads after the native write differ")
		}
		compareObjects(t, "observe_write", written, views, 0)
		if image := readImage(t, "native_write_head", 0, headOverlap); !bytes.Equal(image, cloneNativeWritten) {
			t.Fatal("Go clone read after the native copy-up differs")
		}
		t.Logf("native copy-up of object %d: %d request/reply pairs:%s; sparse %+v", number, len(exchanges), describe(exchanges), got)
	})
	if t.Failed() {
		return
	}

	var snapc cephmsgr.OSDSnapContext
	var snapshotOverlap *uint64
	t.Run("clone_snapshot_copyups", func(t *testing.T) {
		snapped := native(t, "snapshot")
		if len(snapped.CloneView.Snapshots) != 1 || snapped.CloneSnapshotView == nil || snapped.CloneSnapshotView.Parent == nil || snapped.CloneSnapshotView.Overlap == nil {
			t.Fatal("native clone snapshot missing", snapped.CloneView.Snapshots)
		}
		snapID := snapped.CloneView.Snapshots[0].ID
		evidence.CloneSnapshotID = snapID
		// librbd's refresh after the snapshot: the mutable metadata vector
		// carries the new SnapContext, and one vector per snapshot reads it
		// (send_v2_get_snapshots: snapshot_get, parent_overlap_get,
		// get_flags, get_protection_status).
		mutableInputs := [][]byte{rbdLE64(rbdNoSnap), append(rbdLE64(rbdNoSnap), 0), rbdLE64(rbdNoSnap), nil, rbdLockGetInfoInput()}
		mutable := calls(t, "snapshot_mutable_metadata", cloneHeader, read("rbd", "get_size", mutableInputs[0]), read("rbd", "get_features", mutableInputs[1]),
			read("rbd", "get_flags", mutableInputs[2]), read("rbd", "get_snapcontext", nil), read("lock", "get_info", mutableInputs[4]))
		sameAsNative(t, "snapshot", snapped.CloneCalls[4], cloneHeader, "rbd", "get_snapcontext", nil, mutable[3])
		var err error
		if snapc, err = decodeRBDSnapContext(mutable[3]); err != nil || snapc.Sequence != snapID || !reflect.DeepEqual(snapc.Snapshots, []uint64{snapID}) || snapID <= parentSnapID {
			t.Fatal("clone SnapContext after the snapshot differs", snapc, err)
		}
		snapInputs := rbdLE64(snapID)
		perSnapshot := calls(t, "clone_snapshot", cloneHeader, read("rbd", "snapshot_get", snapInputs), read("rbd", "parent_overlap_get", snapInputs),
			read("rbd", "get_flags", snapInputs), read("rbd", "get_protection_status", snapInputs))
		for i, method := range []string{"snapshot_get", "parent_overlap_get", "get_flags", "get_protection_status"} {
			sameAsNative(t, "snapshot", snapped.CloneSnapshotCalls[i], cloneHeader, "rbd", method, snapInputs, perSnapshot[i])
		}
		info, err := decodeRBDSnapshotInfo(perSnapshot[0])
		if err != nil || info.ID != snapID || info.Name != plan.CloneSnapshot || info.ImageSize != plan.Size || info.ChildCount != 0 {
			t.Fatal("clone snapshot_get differs", info, err)
		}
		snapshotOverlap, err = decodeRBDOptionalU64(perSnapshot[1])
		if err != nil || snapshotOverlap == nil || *snapshotOverlap != *snapped.CloneSnapshotView.Overlap || *snapshotOverlap != plan.Size {
			t.Fatal("parent_overlap_get(snapshot) differs from native librbd", snapshotOverlap, err)
		}
		evidence.SnapshotOverlap = snapshotOverlap
		if d := (&rbdDecoder{data: perSnapshot[2]}); d.u64() != 0 || d.done() != nil {
			t.Fatal("clone snapshot flags differ", perSnapshot[2])
		}
		if !bytes.Equal(perSnapshot[3], []byte{0}) {
			t.Fatal("clone snapshot protection differs (unprotected expected)", perSnapshot[3])
		}
		legacy := calls(t, "clone_snapshot_get_parent", cloneHeader, read("rbd", "get_parent", snapInputs))[0]
		sameAsNative(t, "snapshot", snapped.CloneSnapshotCalls[4], cloneHeader, "rbd", "get_parent", snapInputs, legacy)
		if got, err := decodeRBDLegacyParent(legacy); err != nil || got != (rbdLegacyParent{PoolID: oracle.Pool.ID, ImageID: parentID, SnapID: parentSnapID, Overlap: plan.Size}) {
			t.Fatal("legacy get_parent(snapshot) differs", got, err)
		}
		snappedViews := blobs(t, snapped)
		if !bytes.Equal(snappedViews["clone_snap"], cloneNativeWritten) || !bytes.Equal(snappedViews["clone_head"], cloneNativeWritten) {
			t.Fatal("native clone snapshot does not read the clone at the snapshot")
		}
		// The snapshot touched no data object: LIST_SNAPS unchanged.
		checkListSnaps(t, "after_clone_snapshot", snapID)
		// Go writes under the clone SnapContext: object 1, already copied
		// up, is written in place and the OSD clones it for the snapshot;
		// object 2, never written, takes the full copy-up form.
		number, w := objectExtent(t, plan.GoSnapshotWrites[0])
		if !models[number].Exists || models[number].Sequence != 0 {
			t.Fatal("planned in-place write does not target the copied-up object", number)
		}
		data := rbdClonePattern(token, "go", plan.GoSnapshotWrites[0].Offset, plan.GoSnapshotWrites[0].Length)
		reply, err := guardedWrite(t, "snapshot_inplace_write", number, w, data, snapc)
		if err != nil || len(reply.Operations) != 3 || reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 || reply.Operations[2].Result != 0 {
			t.Fatal("guarded write to the copied-up object", err, results(reply))
		}
		models[number].write(w.Offset, w.Length, snapc.Sequence)
		deep := copyup(t, "snapshot_copyup", plan.GoSnapshotWrites[1], snapc, headOverlap)
		if !deep.Full || len(deep.CopyupExtents) == 0 || !reflect.DeepEqual(deep.CopyupExtents, deep.ParentExtents) {
			t.Fatal("copy-up with a clone snapshot did not carry the full parent extents", deep.CopyupExtents, deep.ParentExtents)
		}
		checkListSnaps(t, "after_snapshot_writes", snapID)
		// Go reads at the head and at the clone snapshot (absent objects at
		// the snapshot fall back to the parent with the snapshot's overlap).
		if image := readImage(t, "snapshot_writes_head", 0, headOverlap); !bytes.Equal(image, cloneFinal) {
			t.Fatal("Go clone head read after the snapshot writes differs")
		}
		if image := readImage(t, "snapshot_writes_snap", snapID, snapshotOverlap); !bytes.Equal(image, cloneNativeWritten) {
			t.Fatal("Go clone snapshot read differs from the clone at the snapshot")
		}
		observed := native(t, "observe-snapshot")
		views := blobs(t, observed)
		if !bytes.Equal(views["clone_head"], cloneFinal) || !bytes.Equal(views["clone_snap"], cloneNativeWritten) || !bytes.Equal(views["parent_head"], parentHead) ||
			!bytes.Equal(views["parent_snap"], parentSnap) {
			t.Fatal("native librbd reads after the Go snapshot writes differ")
		}
		compareObjects(t, "observe_snapshot", observed, views, snapID)
		t.Logf("clone snapshot %d (SnapContext %d %v, overlap %d); full copy-up of object %d: extents %+v input %d; LIST_SNAPS %v",
			snapID, snapc.Sequence, snapc.Snapshots, *snapshotOverlap, deep.ObjectNumber, deep.CopyupExtents, deep.InputLength, evidence.ListSnaps["after_snapshot_writes"])
	})
	if t.Failed() {
		return
	}

	t.Run("flatten", func(t *testing.T) {
		flattened := native(t, "flatten")
		snapID := snapc.Sequence
		views := blobs(t, flattened)
		// Without deep-flatten and with a snapshot that still needs the
		// parent, librbd detaches only the head (cls_rbd parent detach keeps
		// the spec and drops the head overlap); the child link and the
		// snapshot's overlap stay.
		if flattened.CloneView.Parent != nil || flattened.CloneView.Overlap != nil && *flattened.CloneView.Overlap != 0 || flattened.CloneSnapshotView.Parent == nil || flattened.CloneSnapshotView.Overlap == nil ||
			*flattened.CloneSnapshotView.Overlap != plan.Size || len(flattened.Children) != 1 {
			t.Fatal("native flatten parent state differs", flattened.CloneView.Parent, flattened.CloneView.Overlap, flattened.CloneSnapshotView.Parent, flattened.Children)
		}
		parent := calls(t, "flattened_parent", cloneHeader, read("rbd", "parent_get", nil), read("rbd", "parent_overlap_get", rbdLE64(rbdNoSnap)))
		sameAsNative(t, "flatten", flattened.CloneCalls[6], cloneHeader, "rbd", "parent_get", nil, parent[0])
		sameAsNative(t, "flatten", flattened.CloneCalls[7], cloneHeader, "rbd", "parent_overlap_get", rbdLE64(rbdNoSnap), parent[1])
		spec, err := decodeRBDParentSpec(parent[0])
		if err != nil || spec != evidence.ParentSpec {
			t.Fatal("parent_get after the flatten differs", spec, err)
		}
		overlap, err := decodeRBDOptionalU64(parent[1])
		if err != nil || overlap != nil {
			t.Fatal("head parent overlap remains after the flatten", overlap, err)
		}
		evidence.FlattenedHeadOverlap = overlap
		legacy := calls(t, "flattened_get_parent", cloneHeader, read("rbd", "get_parent", rbdLE64(rbdNoSnap)))[0]
		sameAsNative(t, "flatten", flattened.CloneCalls[8], cloneHeader, "rbd", "get_parent", rbdLE64(rbdNoSnap), legacy)
		got, err := decodeRBDLegacyParent(legacy)
		if err != nil || got.ImageID != parentID || got.Overlap != 0 {
			t.Fatal("legacy get_parent after the flatten differs", got, err)
		}
		evidence.FlattenedLegacyParent = &got
		snapOverlap := calls(t, "flattened_snapshot_overlap", cloneHeader, read("rbd", "parent_overlap_get", rbdLE64(snapID)))[0]
		sameAsNative(t, "flatten", flattened.CloneSnapshotCalls[1], cloneHeader, "rbd", "parent_overlap_get", rbdLE64(snapID), snapOverlap)
		if o, err := decodeRBDOptionalU64(snapOverlap); err != nil || o == nil || *o != plan.Size {
			t.Fatal("snapshot parent overlap after the flatten differs", o, err)
		}
		// The native flatten's own vectors: each object's guarded empty
		// write under the clone SnapContext; for objects without a clone
		// object and with parent data, the parent read, then the full
		// copy-up under an empty SnapContext and the empty write under the
		// clone SnapContext (the copy-up form Go used for object 2).
		exchanges := nativeTrace(t, flattened)
		snapcText := fmt.Sprintf("%x=[%x]", snapID, snapID)
		guard := "stat," + hintOps + ",write 0~0"
		existed, parentAbsent := map[string]bool{}, map[string]bool{}
		expected := map[string]int{}
		for i := range objects {
			existed[cloneNames[i]] = models[i].Exists
			expected[cloneNames[i]+" "+guard+" "+snapcText] = 1
		}
		for i := range objects {
			if models[i].Exists {
				continue
			}
			expected[parentNames[i]+" sparse-read 0~65536"] = 1
			// librbd's flatten writes the head, so the parent extent is
			// pruned by the head overlap it had before the detach.
			label := fmt.Sprintf("flatten_parent_%d", i)
			extents, parentData := parentRead(t, label, i, headOverlap)
			if extents == nil {
				parentAbsent[parentNames[i]] = true
				maps, err := parseRBDCopyupTrace(flattened.CopyupTrace)
				if logged, ok := maps[cloneNames[i]]; err != nil || ok && len(logged) != 0 {
					t.Fatal("librbd logged parent data for an object absent at the parent snapshot", i, logged, err)
				}
				continue
			}
			pieces, _ := rbdCopyupData(extents, parentData, nil)
			input := sameNativeCopyup(t, flattened, views, exchanges, i, label, extents, parentData, nil)
			evidence.NativeFlattenCopyupInputs[cloneNames[i]] = len(input)
			expected[cloneNames[i]+" "+fmt.Sprintf("call rbd.sparse_copyup in=%db,%s", 3+len("sparse_copyup")+len(input), hintOps)+" 0=[]"] = 1
			expected[cloneNames[i]+" write 0~0 "+snapcText] = 1
			for _, p := range pieces {
				models[i].write(p.Offset, p.Length, 0)
			}
		}
		seen := map[string]int{}
		for _, x := range exchanges {
			key, wantResult := x.Request.Object+" "+x.Request.Ops+" "+x.Request.SnapContext, int32(0)
			switch {
			case strings.HasPrefix(x.Request.Ops, "sparse-read"):
				key = x.Request.Object + " " + x.Request.Ops
				if x.Request.Snap != strconv.FormatUint(parentSnapID, 16) {
					t.Fatal("flatten parent read not at the parent snapshot", x.Request.Raw)
				}
				if parentAbsent[x.Request.Object] {
					wantResult = -2
				}
			case strings.HasPrefix(x.Request.Ops, "stat,") && !existed[x.Request.Object]:
				wantResult = -2
			}
			seen[key]++
			if x.Reply.Result != wantResult {
				t.Fatalf("native flatten request %q returned %d, want %d:%s", x.Request.Raw, x.Reply.Result, wantResult, describe(exchanges))
			}
		}
		if !reflect.DeepEqual(seen, expected) {
			t.Fatalf("native flatten vectors differ:%s\nseen %v\nwant %v", describe(exchanges), seen, expected)
		}
		// The empty write under the clone SnapContext makes the OSD clone
		// every object still at sequence 0.
		for i := range objects {
			if models[i].Exists {
				models[i].write(0, 0, snapc.Sequence)
			}
		}
		checkListSnaps(t, "after_flatten", snapID)
		// The head reads without the parent; the snapshot still reads the
		// parent where the clone has no object at it.
		image := readImage(t, "flatten_head", 0, nil)
		if !bytes.Equal(image, cloneFinal) || !bytes.Equal(views["clone_head"], cloneFinal) {
			t.Fatal("Go or native clone head read after the flatten differs")
		}
		evidence.FlattenReadsWithoutParent = true
		if image := readImage(t, "flatten_snap", snapID, snapshotOverlap); !bytes.Equal(image, cloneNativeWritten) || !bytes.Equal(views["clone_snap"], cloneNativeWritten) {
			t.Fatal("Go or native clone snapshot read after the flatten differs")
		}
		if !bytes.Equal(views["parent_head"], parentHead) || !bytes.Equal(views["parent_snap"], parentSnap) {
			t.Fatal("flatten changed the parent")
		}
		compareObjects(t, "observe_flatten", flattened, views, snapID)
		flattenedAll := true
		for i, o := range flattened.CloneObjects {
			evidence.CloneObjectSizesAfterFlatten[cloneNames[i]] = o.Head.Size
			lo := uint64(i) * objectSize
			if !o.Head.Exists && !bytes.Equal(cloneFinal[lo:lo+objectSize], make([]byte, objectSize)) {
				flattenedAll = false
			}
		}
		evidence.FlattenedAll = flattenedAll
		if !flattenedAll {
			t.Fatal("flatten left a non-zero object without a clone object")
		}
		t.Logf("flatten in %.1f ms: %d native request/reply pairs:%s", float64(flattened.FlattenNS)/1e6, len(exchanges), describe(exchanges))
	})
}

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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only RBD group encodings, restating the pinned
// ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd_types.cc (GroupSnapshot,
// ImageSnapshotSpec, SnapshotInfo, SnapshotNamespace and
// GroupSnapshotNamespace) and ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd.cc
// (group_snap_list, snapshot_get, dir_get_id); LGPL-2.1 references, no code
// copied. The product carries raw class names, inputs and outputs only.

const (
	rbdGroupSnapStateComplete     = 1 // cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE
	rbdSnapshotNamespaceTypeGroup = 1 // cls::rbd::SNAPSHOT_NAMESPACE_TYPE_GROUP
)

type rbdGroupImageSnapSpec struct {
	Pool    int64
	ImageID string
	SnapID  uint64
}

// rbdGroupSnapRecord is one cls::rbd::GroupSnapshot of the group header.
type rbdGroupSnapRecord struct {
	ID    string
	Name  string
	State uint8
	Snaps []rbdGroupImageSnapSpec
}

func rbdGroupString(b []byte, s string) []byte {
	return append(binary.LittleEndian.AppendUint32(b, uint32(len(s))), s...)
}

func rbdGroupEnvelope(b []byte, body []byte) []byte {
	return append(binary.LittleEndian.AppendUint32(append(b, 1, 1), uint32(len(body))), body...)
}

// encodeRBDGroupSnapRecord is GroupSnapshot::encode: ENCODE_START(1, 1), id,
// name, the state as a u8 and the member specs, each ENCODE_START(1, 1) with
// an s64 pool, the image id and a u64 snapshot id.
func encodeRBDGroupSnapRecord(r rbdGroupSnapRecord) []byte {
	body := rbdGroupString(rbdGroupString(nil, r.ID), r.Name)
	body = binary.LittleEndian.AppendUint32(append(body, r.State), uint32(len(r.Snaps)))
	for _, s := range r.Snaps {
		spec := binary.LittleEndian.AppendUint64(nil, uint64(s.Pool))
		spec = binary.LittleEndian.AppendUint64(rbdGroupString(spec, s.ImageID), s.SnapID)
		body = rbdGroupEnvelope(body, spec)
	}
	return rbdGroupEnvelope(nil, body)
}

// rbdGroupSnapListInput is group_snap_list's input: the record to list after
// (the empty record starts at the beginning) and the page size.
func rbdGroupSnapListInput(start rbdGroupSnapRecord, page uint64) []byte {
	return binary.LittleEndian.AppendUint64(encodeRBDGroupSnapRecord(start), page)
}

func decodeRBDGroupSnapRecord(d *rbdDecoder) rbdGroupSnapRecord {
	var r rbdGroupSnapRecord
	v, body := d.header(1)
	if v != 1 && d.err == nil {
		d.err = fmt.Errorf("GroupSnapshot version %d differs", v)
	}
	r.ID, r.Name, r.State = body.str(), body.str(), body.u8()
	n := body.u32()
	if n > 16 {
		body.err = errors.Join(body.err, errors.New("GroupSnapshot lists too many member snapshots"))
	}
	for i := uint32(0); i < n && body.err == nil; i++ {
		sv, spec := body.header(1)
		s := rbdGroupImageSnapSpec{Pool: int64(spec.u64()), ImageID: spec.str(), SnapID: spec.u64()}
		if err := spec.done(); err != nil || sv != 1 {
			body.err = errors.Join(err, fmt.Errorf("ImageSnapshotSpec version %d", sv))
		}
		r.Snaps = append(r.Snaps, s)
	}
	if err := body.done(); err != nil && d.err == nil {
		d.err = err
	}
	return r
}

// decodeRBDGroupSnapList decodes group_snap_list's output, a u32 count and
// that many records.
func decodeRBDGroupSnapList(data []byte) ([]rbdGroupSnapRecord, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	if n > 16 {
		return nil, errors.New("group_snap_list output lists too many records")
	}
	records := []rbdGroupSnapRecord{}
	for i := uint32(0); i < n && d.err == nil; i++ {
		records = append(records, decodeRBDGroupSnapRecord(d))
	}
	return records, d.done()
}

// rbdGroupSnapshotInfo is snapshot_get's cls::rbd::SnapshotInfo with its
// namespace, which must be a GroupSnapshotNamespace.
type rbdGroupSnapshotInfo struct {
	ID              uint64
	NamespaceType   uint32
	GroupPool       int64
	GroupID         string
	GroupSnapshotID string
	Name            string
	ImageSize       uint64
	Seconds         uint32
	Nanoseconds     uint32
	ChildCount      uint32
}

func decodeRBDGroupSnapshotInfo(data []byte) (rbdGroupSnapshotInfo, error) {
	var info rbdGroupSnapshotInfo
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	info.ID = body.u64()
	_, ns := body.header(1)
	info.NamespaceType = ns.u32()
	if info.NamespaceType != rbdSnapshotNamespaceTypeGroup {
		return info, fmt.Errorf("snapshot namespace type %d is not a group namespace", info.NamespaceType)
	}
	info.GroupPool, info.GroupID, info.GroupSnapshotID = int64(ns.u64()), ns.str(), ns.str()
	if err := ns.done(); err != nil {
		return info, err
	}
	info.Name = body.str()
	info.ImageSize = body.u64()
	info.Seconds, info.Nanoseconds = body.u32(), body.u32()
	info.ChildCount = body.u32()
	return info, errors.Join(body.done(), d.done())
}

// rbdGroupImageSnapName is librbd's member snapshot name for a group
// snapshot (ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc
// calc_ind_image_snap_name): the group pool in hexadecimal, the group ID and
// the group snapshot ID.
func rbdGroupImageSnapName(pool int64, groupID, snapID string) string {
	return fmt.Sprintf(".group.%x_%s_%s", pool, groupID, snapID)
}

// rbdGroupPattern restates the native driver's deterministic contents of one
// member image's write.
func rbdGroupPattern(token, image, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-group:%s:%s:%s:%d:%d", token, image, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

func TestRBDGroupTestOnlyEncodings(t *testing.T) {
	record := rbdGroupSnapRecord{ID: "s1", Name: "snap", State: rbdGroupSnapStateComplete,
		Snaps: []rbdGroupImageSnapSpec{{Pool: 3, ImageID: "i0", SnapID: 4}, {Pool: 3, ImageID: "i1", SnapID: 5}}}
	encoded := encodeRBDGroupSnapRecord(record)
	want := "01014b000000" + "020000007331" + "04000000736e6170" + "01" + "02000000" +
		"010116000000" + "0300000000000000" + "020000006930" + "0400000000000000" +
		"010116000000" + "0300000000000000" + "020000006931" + "0500000000000000"
	if hex.EncodeToString(encoded) != want {
		t.Fatalf("GroupSnapshot encoding %x, want %s", encoded, want)
	}
	if got := hex.EncodeToString(rbdGroupSnapListInput(rbdGroupSnapRecord{}, 1)); got != "01010d00000000000000000000000000000000"+"0100000000000000" {
		t.Fatal("empty group_snap_list start record differs", got)
	}
	list := append(binary.LittleEndian.AppendUint32(nil, 1), encoded...)
	decoded, err := decodeRBDGroupSnapList(list)
	if err != nil || len(decoded) != 1 || !reflect.DeepEqual(decoded[0], record) {
		t.Fatal("group_snap_list decoding", decoded, err)
	}
	if empty, err := decodeRBDGroupSnapList([]byte{0, 0, 0, 0}); err != nil || len(empty) != 0 {
		t.Fatal("empty group_snap_list page", empty, err)
	}
	for cut := 0; cut < len(list); cut++ {
		if _, err := decodeRBDGroupSnapList(list[:cut]); err == nil {
			t.Fatal("truncated group_snap_list output admitted", cut)
		}
	}
	if _, err := decodeRBDGroupSnapList(append(bytes.Clone(list), 0)); err == nil {
		t.Fatal("trailing group_snap_list byte admitted")
	}
	versioned := bytes.Clone(list)
	versioned[4] = 2
	if _, err := decodeRBDGroupSnapList(versioned); err == nil {
		t.Fatal("GroupSnapshot of another version admitted")
	}

	ns := binary.LittleEndian.AppendUint32(nil, rbdSnapshotNamespaceTypeGroup)
	ns = rbdGroupString(rbdGroupString(binary.LittleEndian.AppendUint64(ns, 3), "g1"), "s1")
	body := rbdGroupEnvelope(binary.LittleEndian.AppendUint64(nil, 4), ns)
	body = binary.LittleEndian.AppendUint64(rbdGroupString(body, ".group.3_g1_s1"), 192<<10)
	body = binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(body, 1), 2), 0)
	info := rbdGroupEnvelope(nil, body)
	got, err := decodeRBDGroupSnapshotInfo(info)
	if err != nil || got != (rbdGroupSnapshotInfo{ID: 4, NamespaceType: 1, GroupPool: 3, GroupID: "g1", GroupSnapshotID: "s1", Name: ".group.3_g1_s1", ImageSize: 192 << 10, Seconds: 1, Nanoseconds: 2}) {
		t.Fatal("group SnapshotInfo decoding", got, err)
	}
	for cut := 0; cut < len(info); cut++ {
		if _, err := decodeRBDGroupSnapshotInfo(info[:cut]); err == nil {
			t.Fatal("truncated group SnapshotInfo admitted", cut)
		}
	}
	user := bytes.Clone(info)
	user[6+8+6] = 0 // namespace type USER
	if _, err := decodeRBDGroupSnapshotInfo(user); err == nil {
		t.Fatal("user-namespace SnapshotInfo admitted as a group member snapshot")
	}
	if name := rbdGroupImageSnapName(26, "g1", "s1"); name != ".group.1a_g1_s1" {
		t.Fatal("member snapshot name differs", name)
	}
	if p := rbdGroupPattern(strings.Repeat("ab", 16), "a", "pre", 0, 40); len(p) != 40 || bytes.Equal(p, rbdGroupPattern(strings.Repeat("ab", 16), "b", "pre", 0, 40)) {
		t.Fatal("member image patterns are not distinct")
	}
}

type nativeRBDGroupCall struct {
	Object    string `json:"object"`
	Class     string `json:"class"`
	Method    string `json:"method"`
	InputHex  string `json:"input_hex"`
	Code      int32  `json:"code"`
	OutputHex string `json:"output_hex"`
}

type nativeRBDGroupImagePlan struct {
	Key        string            `json:"key"`
	Name       string            `json:"name"`
	PreWrites  []nativeRBDExtent `json:"pre_snapshot_writes"`
	PostWrites []nativeRBDExtent `json:"post_snapshot_writes"`
}

type nativeRBDGroupPlan struct {
	Pool          string                    `json:"pool"`
	BaseImage     string                    `json:"base_image"`
	Group         string                    `json:"group"`
	GroupSnapshot string                    `json:"group_snapshot"`
	Order         int                       `json:"order"`
	Size          uint64                    `json:"size"`
	Features      uint64                    `json:"features"`
	ListPage      uint64                    `json:"list_page"`
	Images        []nativeRBDGroupImagePlan `json:"images"`
}

type nativeRBDGroupMember struct {
	Name  string `json:"name"`
	Pool  int64  `json:"pool"`
	State int    `json:"state"`
}

type nativeRBDGroupSnap struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	State         int    `json:"state"`
	NamespaceType int    `json:"namespace_type"`
	ImageSnapName string `json:"image_snap_name"`
	ImageSnaps    []struct {
		PoolID    int64  `json:"pool_id"`
		SnapID    uint64 `json:"snap_id"`
		ImageName string `json:"image_name"`
	} `json:"image_snaps"`
}

type nativeRBDGroupImageSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Size      uint64 `json:"size"`
	Namespace int    `json:"namespace"`
	Group     *struct {
		Pool     int64  `json:"pool"`
		Name     string `json:"name"`
		SnapName string `json:"snap_name"`
	} `json:"group"`
}

type nativeRBDGroupImage struct {
	Key          string `json:"key"`
	Name         string `json:"name"`
	ImageID      string `json:"image_id"`
	HeaderObject string `json:"header_object"`
	IDObject     string `json:"id_object"`
	Stat         struct {
		Size       uint64 `json:"size"`
		ObjectSize uint64 `json:"obj_size"`
		Objects    uint64 `json:"num_objs"`
		Order      int    `json:"order"`
		Prefix     string `json:"block_name_prefix"`
	} `json:"stat"`
	Features      uint64                        `json:"features"`
	Snapshots     []nativeRBDGroupImageSnapshot `json:"snapshots"`
	SnapshotCalls []nativeRBDGroupCall          `json:"snapshot_calls"`
	Objects       []nativeRBDObject             `json:"objects"`
	Blobs         map[string]nativeRBDBlob      `json:"blobs"`
}

type nativeRBDGroup struct {
	Action         string   `json:"action"`
	RequestID      int64    `json:"request_id"`
	PoolID         int64    `json:"pool_id"`
	NativeGlobalID uint64   `json:"native_global_id"`
	ImagesListed   []string `json:"images_listed"`
	GroupsListed   []string `json:"groups_listed"`
	Writes         []struct {
		Image   string `json:"image"`
		Phase   string `json:"phase"`
		Offset  uint64 `json:"offset"`
		Length  uint64 `json:"length"`
		Written uint64 `json:"written"`
		SHA256  string `json:"sha256"`
	} `json:"writes"`
	Group *struct {
		Name          string                 `json:"name"`
		ID            string                 `json:"id"`
		HeaderObject  string                 `json:"header_object"`
		Members       []nativeRBDGroupMember `json:"members"`
		Snapshots     []nativeRBDGroupSnap   `json:"snapshots"`
		SnapshotInfo  nativeRBDGroupSnap     `json:"snapshot_info"`
		DirectoryCall nativeRBDGroupCall     `json:"directory_call"`
		ListCalls     []nativeRBDGroupCall   `json:"list_calls"`
	} `json:"group"`
	Images  []nativeRBDGroupImage `json:"images"`
	Removed *struct {
		GroupID     string `json:"group_id"`
		GroupHeader string `json:"group_header"`
		Images      []struct {
			Key            string                        `json:"key"`
			Name           string                        `json:"name"`
			ImageID        string                        `json:"image_id"`
			Prefix         string                        `json:"prefix"`
			Objects        uint64                        `json:"objects"`
			SnapshotsAfter []nativeRBDGroupImageSnapshot `json:"snapshots_after"`
			HeaderErrno    int                           `json:"header_errno"`
			IDErrno        int                           `json:"id_errno"`
			DataErrno      []int                         `json:"data_errno"`
		} `json:"images"`
		GroupSnapshotsAfter []nativeRBDGroupSnap   `json:"group_snapshots_after"`
		MembersAfter        []nativeRBDGroupMember `json:"members_after"`
		GroupHeaderErrno    int                    `json:"group_header_errno"`
		DirectoryCall       nativeRBDGroupCall     `json:"directory_call"`
	} `json:"removed"`
}

type nativeRBDGroupResult struct {
	Source    string             `json:"source_commit"`
	Token     string             `json:"token"`
	RequestID int64              `json:"request_id"`
	Action    string             `json:"action"`
	Plan      nativeRBDGroupPlan `json:"plan"`
	Native    *nativeRBDGroup    `json:"native"`
	Process   *struct {
		Stdout, Stderr []byte
		ExitCode       int
		TimedOut       bool
	} `json:"native_process"`
	Mutation *struct {
		Kind      string `json:"kind"`
		RequestID int64  `json:"request_id"`
		Target    string `json:"target"`
		ReadOnly  bool   `json:"read_only"`
		State     string `json:"state"`
	} `json:"mutation"`
	Complete  bool   `json:"complete"`
	Error     string `json:"error"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

type rbdGroupRequestEvidence struct {
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
	InputHex     string `json:",omitempty"`
	Operations   []rbdOperationEvidence
	Error        string `json:",omitempty"`
}

type rbdGroupNativeActionEvidence struct {
	Action         string
	RequestID      int64
	NativeGlobalID uint64
	ElapsedMS      int64
}

type rbdGroupEvidence struct {
	Token         string
	Pool          string
	PoolID        int64
	GoIdentity    string
	GoGlobalID    uint64
	GroupID       string
	GroupSnapshot string
	Members       []rbdGroupImageSnapSpec
	Requests      []rbdGroupRequestEvidence
	ClassOutputs  map[string]string
	NativeActions []rbdGroupNativeActionEvidence
	Timings       map[string]string
}

// TestCephRBDGroupSnapshotIntegration reads a native RBD group snapshot
// through the explicit OSD API. Native librbd creates two layering images
// with different contents in the fixture's RBD pool, a group holding both
// and one group snapshot; every mutation stays native. The Go client then
// resolves the group ID with dir_get_id on rbd_group_directory, sends
// read-only rbd.group_snap_list to the group header with the empty start
// record and a page size of one, and lists the next page starting after the
// returned record, re-encoded by the test. For every member tuple of that
// record it sends rbd.snapshot_get for the tuple's snapshot ID to the
// tuple's image header, routed in the tuple's pool. The complete group record
// (ID, name, COMPLETE state, member pools, image IDs and snapshot IDs) and
// every member snapshot's GroupSnapshotNamespace (group pool, group ID,
// group snapshot ID), name, size and child count are compared with native
// librbd's group listing, group snapshot info, image snapshot listing and
// group namespace, and the raw class inputs and outputs with native librados
// exec of the same methods. Native librbd then writes new data to both live
// heads; the group record and member snapshots must be byte-identical, and
// Go reads of every data object at the head and at each member snapshot must
// equal native librados objects, native librbd image reads and the expected
// contents, the snapshot view keeping only the data written before the group
// snapshot. Finally native librbd removes the group snapshot, the membership,
// the group and both images, each followed by a native absence read, and the
// Go reads of the old group header, directory entry and image headers must
// answer ENOENT. Native librbd writes INCOMPLETE before quiescing and
// snapshotting the members and COMPLETE only afterwards
// (ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc), so the record read here
// is the finished one; an INCOMPLETE record, its compensation, mirrored
// group snapshots, migration and group snapshots with exclusive-lock or
// journaling members are not covered.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdgroupsnapshot key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-group-snapshot
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdgroupsnapshot key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-group-snapshot
//scenario:covers RBD underlying I/O
//fixture:isolated creates and removes a group, a group snapshot and two images in the fixture's RBD pool natively
func TestCephRBDGroupSnapshotIntegration(t *testing.T) {
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
	gid := c.Snapshot().GlobalID
	poolID := oracle.Pool.ID
	evidence := rbdGroupEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: poolID, GoIdentity: identity, GoGlobalID: gid, ClassOutputs: map[string]string{}, Timings: map[string]string{}}
	defer func() {
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-group-results.json"), data)
		}
		if err != nil {
			t.Error("RBD group evidence", err)
		}
	}()

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDGroupPlan
	native := func(t *testing.T, action string) *nativeRBDGroup {
		t.Helper()
		if err := os.Remove(filepath.Join(dir, "rbd-group-"+action+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := testcluster.RequestControl(ctx, dir, "rbd-group", "rbd-group-completed", token, action); err != nil {
			t.Fatal("native RBD group", action, err)
		}
		var result nativeRBDGroupResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-group-"+action+".json"), &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-group-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly || result.Mutation.RequestID != result.RequestID ||
			result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil || n.Action != action || n.RequestID != result.RequestID ||
			n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != poolID || plan.Pool != "" && !reflect.DeepEqual(result.Plan, plan) {
			t.Fatal("native RBD group receipt differs", action, result.Error)
		}
		// librbd logs failed group, snapshot and removal steps at error level.
		if len(result.Process.Stderr) != 0 {
			t.Fatalf("native RBD group %s logged errors: %q", action, result.Process.Stderr)
		}
		if plan.Pool == "" {
			plan = result.Plan
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdGroupNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS})
		return n
	}
	setup := native(t, "setup")
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.BaseImage != oracle.Plan.Image || plan.Group != "ceph-msgr-group-"+token || plan.GroupSnapshot != "ceph-msgr-gsnap-"+token[:8] ||
		plan.Order != 16 || plan.Size != 192*k || plan.Features != 1 || plan.ListPage != 1 || len(plan.Images) != 2 ||
		plan.Images[0].Key != "a" || plan.Images[0].Name != "ceph-msgr-gimage-a-"+token || plan.Images[1].Key != "b" || plan.Images[1].Name != "ceph-msgr-gimage-b-"+token {
		t.Fatal("RBD group plan differs", plan)
	}
	evidence.GroupSnapshot = plan.GroupSnapshot
	objectSize := uint64(1) << plan.Order
	objectCount := plan.Size >> plan.Order
	// expected builds an image's bytes from its writes of the given phases.
	expected := func(image nativeRBDGroupImagePlan, phases ...string) []byte {
		view := make([]byte, plan.Size)
		for _, phase := range phases {
			extents := image.PreWrites
			if phase == "post" {
				extents = image.PostWrites
			}
			for _, e := range extents {
				copy(view[e.Offset:], rbdGroupPattern(token, image.Key, phase, e.Offset, e.Length))
			}
		}
		return view
	}
	checkWrites := func(t *testing.T, n *nativeRBDGroup, phase string) {
		t.Helper()
		i := 0
		for _, image := range plan.Images {
			extents := image.PreWrites
			if phase == "post" {
				extents = image.PostWrites
			}
			for _, e := range extents {
				sum := sha256.Sum256(rbdGroupPattern(token, image.Key, phase, e.Offset, e.Length))
				if i >= len(n.Writes) || n.Writes[i].Image != image.Key || n.Writes[i].Phase != phase || n.Writes[i].Offset != e.Offset || n.Writes[i].Length != e.Length ||
					n.Writes[i].Written != e.Length || n.Writes[i].SHA256 != hex.EncodeToString(sum[:]) {
					t.Fatal("test-only RBD group pattern differs from the native driver", phase, i)
				}
				i++
			}
		}
		if i != len(n.Writes) {
			t.Fatal("native RBD group writes differ from the plan", phase)
		}
	}
	checkWrites(t, setup, "pre")
	wantListed := []string{plan.BaseImage, plan.Images[0].Name, plan.Images[1].Name}
	slices.Sort(wantListed)
	if !slices.Equal(setup.ImagesListed, wantListed) || !slices.Equal(setup.GroupsListed, []string{plan.Group}) || setup.Group == nil || len(setup.Images) != 2 {
		t.Fatal("native RBD group setup listing differs", setup.ImagesListed, setup.GroupsListed)
	}
	group := setup.Group
	evidence.GroupID = group.ID
	imageIDs := map[string]string{}
	for i, image := range setup.Images {
		if image.Key != plan.Images[i].Key || image.Name != plan.Images[i].Name || image.Stat.Size != plan.Size || image.Stat.Order != plan.Order || image.Stat.Objects != objectCount ||
			image.Stat.Prefix != "rbd_data."+image.ImageID || image.Features != plan.Features || image.HeaderObject != "rbd_header."+image.ImageID {
			t.Fatal("native RBD group member image differs", image.Key, image.Stat)
		}
		imageIDs[image.Name] = image.ImageID
	}
	if len(imageIDs) != 2 || imageIDs[plan.Images[0].Name] == imageIDs[plan.Images[1].Name] {
		t.Fatal("native RBD group member image IDs are not distinct", imageIDs)
	}
	members := slices.Clone(group.Members)
	slices.SortFunc(members, func(a, b nativeRBDGroupMember) int { return strings.Compare(a.Name, b.Name) })
	if !reflect.DeepEqual(members, []nativeRBDGroupMember{{plan.Images[0].Name, poolID, 0}, {plan.Images[1].Name, poolID, 0}}) {
		t.Fatal("native RBD group membership differs (want both images attached)", group.Members)
	}
	if len(group.Snapshots) != 1 || !reflect.DeepEqual(group.Snapshots[0], group.SnapshotInfo) {
		t.Fatal("native RBD group snapshot listing and info differ", group.Snapshots, group.SnapshotInfo)
	}
	nativeSnap := group.Snapshots[0]
	// librbd's state COMPLETE (1) and the user group-snapshot namespace (0).
	if nativeSnap.Name != plan.GroupSnapshot || nativeSnap.State != 1 || nativeSnap.NamespaceType != 0 || nativeSnap.ID == "" || len(nativeSnap.ImageSnaps) != 2 ||
		nativeSnap.ImageSnapName != rbdGroupImageSnapName(poolID, group.ID, nativeSnap.ID) {
		t.Fatal("native RBD group snapshot differs", nativeSnap)
	}

	addresses := osdDumpAddresses(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	// route places one object of the owned pool; pool is the ID the caller
	// read for this object (a member tuple's pool), which must be that pool.
	route := func(t *testing.T, pool int64, object string) (rbdRoute, *cephmsgr.OSDConnection) {
		t.Helper()
		if pool != poolID {
			t.Fatal("RBD group object is outside the owned pool", pool, object)
		}
		p, err := recoveryObjectPlacement(ctx, c, plan.Pool, object)
		if err != nil {
			t.Fatal(err)
		}
		r := rbdRoute{Object: object, Pool: p.PoolID, Hash: p.Hash, Epoch: p.Epoch, RawPGID: p.RawPGID, PGID: p.PGID, Primary: p.Primary, Address: addresses[p.Primary]}
		if r.Pool != pool || r.Primary < 0 || r.Address == "" || p.Acting[0] != p.Primary {
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
		e := rbdGroupRequestEvidence{Label: label, Route: r, ReadSnapshot: req.ReadSnapshot, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion}
		if len(req.Operations) == 1 && req.Operations[0].Code == cephmsgr.OSDCall {
			e.InputHex = hex.EncodeToString(req.Operations[0].Data)
		}
		for _, op := range reply.Operations {
			sum := sha256.Sum256(op.Data)
			o := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:])}
			if len(op.Data) <= 512 {
				o.DataHex = hex.EncodeToString(op.Data)
			}
			if op.Code == cephmsgr.OSDCall {
				o.ClassOutput = op.Class.OutputLength
			}
			e.Operations = append(e.Operations, o)
		}
		if err != nil {
			e.Error = err.Error()
		}
		evidence.Requests = append(evidence.Requests, e)
	}
	checkReply := func(t *testing.T, r rbdRoute, reply cephmsgr.OSDReply, code int32) {
		t.Helper()
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != gid || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 {
			t.Fatalf("OSD reply identity differs: code=%d tid=%d gid=%d pg=%+v epoch=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG, reply.MapEpoch)
		}
	}
	// call sends one read-mode rbd CALL alone in its request and requires
	// the given result; it returns the class output of a success.
	call := func(t *testing.T, label string, pool int64, object, method string, input []byte, code int32) []byte {
		t.Helper()
		r, o := route(t, pool, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: "rbd", Method: method, Mode: cephmsgr.OSDCallRead, MaxOutputBytes: 8192}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if code != 0 {
			var server *cephmsgr.OSDError
			if !errors.As(err, &server) || server.Code != code {
				t.Fatalf("Go CALL rbd.%s (%s): want server result %d, got %v", method, label, code, err)
			}
			checkReply(t, r, reply, code)
			return nil
		}
		if err != nil {
			t.Fatalf("Go CALL rbd.%s (%s): %v", method, label, err)
		}
		checkReply(t, r, reply, 0)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != 3 || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) || got.Class.OutputLength != uint64(len(got.Data)) {
			t.Fatalf("CALL rbd.%s per-op result or union echo differs: %+v", method, got.Class)
		}
		evidence.ClassOutputs[label] = hex.EncodeToString(got.Data)
		return got.Data
	}
	sameCall := func(t *testing.T, label string, n nativeRBDGroupCall, object, method string, input, output []byte) {
		t.Helper()
		if n.Object != object || n.Class != "rbd" || n.Method != method || n.InputHex != hex.EncodeToString(input) || n.Code != 0 || n.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("%s: Go CALL rbd.%s differs from native librados exec: native=%+v go_input=%x go_output=%x", label, method, n, input, output)
		}
	}

	// observeGroup reads the group directory, both group_snap_list pages and
	// each member snapshot, compares them with the native observation n and
	// returns the group record with every class output.
	observeGroup := func(t *testing.T, phase string, n *nativeRBDGroup) (rbdGroupSnapRecord, map[string][]byte) {
		t.Helper()
		outputs := map[string][]byte{}
		g := n.Group
		nameInput := rbdGroupString(nil, plan.Group)
		out := call(t, phase+"_dir_get_id", poolID, "rbd_group_directory", "dir_get_id", nameInput, 0)
		sameCall(t, phase+" dir_get_id", g.DirectoryCall, "rbd_group_directory", "dir_get_id", nameInput, out)
		d := &rbdDecoder{data: out}
		groupID := d.str()
		if err := d.done(); err != nil || groupID != g.ID || groupID != group.ID {
			t.Fatal("Go group directory lookup differs from native librbd's group ID", groupID, g.ID, err)
		}
		outputs["dir_get_id"] = out
		header := "rbd_group_header." + groupID
		if g.HeaderObject != header || len(g.ListCalls) != 2 {
			t.Fatal("native group header observation differs", g.HeaderObject, len(g.ListCalls))
		}
		first := rbdGroupSnapListInput(rbdGroupSnapRecord{}, plan.ListPage)
		out = call(t, phase+"_group_snap_list_1", poolID, header, "group_snap_list", first, 0)
		sameCall(t, phase+" group_snap_list page 1", g.ListCalls[0], header, "group_snap_list", first, out)
		outputs["group_snap_list_1"] = out
		records, err := decodeRBDGroupSnapList(out)
		if err != nil || len(records) != 1 {
			t.Fatal("group_snap_list page 1 must hold exactly the one group snapshot", records, err)
		}
		rec := records[0]
		// The next page starts after the returned record, re-encoded here:
		// the bytes must equal the server's own encoding of that record.
		if !bytes.Equal(encodeRBDGroupSnapRecord(rec), out[4:]) {
			t.Fatal("test-only GroupSnapshot re-encoding differs from the OSD class output")
		}
		next := rbdGroupSnapListInput(rec, plan.ListPage)
		out = call(t, phase+"_group_snap_list_2", poolID, header, "group_snap_list", next, 0)
		sameCall(t, phase+" group_snap_list page 2", g.ListCalls[1], header, "group_snap_list", next, out)
		outputs["group_snap_list_2"] = out
		if after, err := decodeRBDGroupSnapList(out); err != nil || len(after) != 0 {
			t.Fatal("group_snap_list after the only record must be empty", after, err)
		}
		// The complete record against native librbd's listing.
		ns := g.Snapshots[0]
		if rec.ID != ns.ID || rec.Name != ns.Name || rec.Name != plan.GroupSnapshot || rec.State != rbdGroupSnapStateComplete || int(rec.State) != ns.State || len(rec.Snaps) != len(ns.ImageSnaps) || len(rec.Snaps) != 2 {
			t.Fatal("Go group record differs from native librbd's group snapshot", rec, ns)
		}
		seen := map[string]bool{}
		for i, spec := range rec.Snaps {
			want := ns.ImageSnaps[i]
			if spec.Pool != want.PoolID || spec.Pool != poolID || spec.SnapID != want.SnapID || spec.ImageID != imageIDs[want.ImageName] || spec.ImageID == "" || seen[spec.ImageID] {
				t.Fatal("Go group member tuple differs from native librbd", i, spec, want)
			}
			seen[spec.ImageID] = true
		}
		// Each member snapshot on its own image header.
		for _, spec := range rec.Snaps {
			var image *nativeRBDGroupImage
			for i := range n.Images {
				if n.Images[i].ImageID == spec.ImageID {
					image = &n.Images[i]
				}
			}
			if image == nil || len(image.Snapshots) != 1 || len(image.SnapshotCalls) != 1 {
				t.Fatal("native observation lacks the member image", spec)
			}
			header := "rbd_header." + spec.ImageID
			input := rbdLE64(spec.SnapID)
			label := phase + "_snapshot_get_" + image.Key
			out := call(t, label, spec.Pool, header, "snapshot_get", input, 0)
			sameCall(t, phase+" snapshot_get "+image.Key, image.SnapshotCalls[0], header, "snapshot_get", input, out)
			outputs["snapshot_get_"+image.Key] = out
			info, err := decodeRBDGroupSnapshotInfo(out)
			snap := image.Snapshots[0]
			if err != nil || info.ID != spec.SnapID || info.GroupPool != poolID || info.GroupID != groupID || info.GroupSnapshotID != rec.ID ||
				info.Name != ns.ImageSnapName || info.ImageSize != plan.Size || info.ChildCount != 0 || info.Seconds == 0 {
				t.Fatal("Go member snapshot differs from the group record", image.Key, info, err)
			}
			if snap.ID != info.ID || snap.Name != info.Name || snap.Size != info.ImageSize || snap.Namespace != rbdSnapshotNamespaceTypeGroup || snap.Group == nil ||
				snap.Group.Pool != info.GroupPool || snap.Group.Name != plan.Group || snap.Group.SnapName != rec.Name {
				t.Fatal("Go member snapshot differs from native librbd's snapshot and group namespace", image.Key, info, snap)
			}
		}
		return rec, outputs
	}

	var setupRecord rbdGroupSnapRecord
	var setupOutputs map[string][]byte
	t.Run("group_snapshot_record", func(t *testing.T) {
		setupRecord, setupOutputs = observeGroup(t, "setup", setup)
		evidence.Members = setupRecord.Snaps
		// Before the heads change, each member snapshot reads as its head
		// and as the pre-snapshot contents.
		for i, image := range setup.Images {
			head, snap := readRBDBlob(t, dir, image.Blobs["image_head"]), readRBDBlob(t, dir, image.Blobs["image_snap"])
			if !bytes.Equal(head, expected(plan.Images[i], "pre")) || !bytes.Equal(snap, head) {
				t.Fatal("native member image views before the head writes differ from the plan", image.Key)
			}
		}
		t.Logf("group %s snapshot %s: COMPLETE with %d member snapshots, each in its group namespace", group.ID, setupRecord.ID, len(setupRecord.Snaps))
	})
	if t.Failed() {
		return
	}

	modify := native(t, "modify")
	checkWrites(t, modify, "post")
	if !slices.Equal(modify.ImagesListed, wantListed) || !slices.Equal(modify.GroupsListed, []string{plan.Group}) || modify.Group == nil || len(modify.Images) != 2 {
		t.Fatal("native RBD group listing after the head writes differs")
	}
	for i, image := range modify.Images {
		if image.ImageID != setup.Images[i].ImageID || !reflect.DeepEqual(image.Snapshots, setup.Images[i].Snapshots) || !reflect.DeepEqual(image.SnapshotCalls, setup.Images[i].SnapshotCalls) {
			t.Fatal("native member snapshot changed with its head", image.Key)
		}
	}
	if !reflect.DeepEqual(modify.Group.Snapshots, group.Snapshots) || !reflect.DeepEqual(modify.Group.ListCalls, group.ListCalls) {
		t.Fatal("native group snapshot changed with the member heads")
	}
	t.Run("record_after_head_writes", func(t *testing.T) {
		rec, outputs := observeGroup(t, "modify", modify)
		if !reflect.DeepEqual(rec, setupRecord) || !reflect.DeepEqual(outputs, setupOutputs) {
			t.Fatal("group record or member snapshots changed with the member heads")
		}
	})
	t.Run("snapshot_and_head_bytes", func(t *testing.T) {
		for i, image := range modify.Images {
			want := plan.Images[i]
			snapID := setupRecord.Snaps[slices.IndexFunc(setupRecord.Snaps, func(s rbdGroupImageSnapSpec) bool { return s.ImageID == image.ImageID })].SnapID
			views := map[string][]byte{}
			for key, blob := range image.Blobs {
				views[key] = readRBDBlob(t, dir, blob)
			}
			if !bytes.Equal(views["image_head"], expected(want, "pre", "post")) || !bytes.Equal(views["image_snap"], expected(want, "pre")) ||
				bytes.Equal(views["image_head"], views["image_snap"]) {
				t.Fatal("native member image views after the head writes differ from the plan", image.Key)
			}
			if len(image.Objects) != int(objectCount) {
				t.Fatal("native member object views missing", image.Key)
			}
			for _, snapshot := range []bool{false, true} {
				label, read, imageView, blob := "head", uint64(0), views["image_head"], views["objects_head"]
				if snapshot {
					label, read, imageView, blob = "snap", snapID, views["image_snap"], views["objects_snap"]
				}
				for j, object := range image.Objects {
					view := object.Head
					if snapshot {
						view = object.Snap
					}
					name := fmt.Sprintf("%s.%016x", image.Stat.Prefix, j)
					if object.Name != name {
						t.Fatal("Go data object name differs from native librbd layout", name)
					}
					r, o := route(t, poolID, name)
					req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
					req.ReadSnapshot = read
					reply, err := o.Request(ctx, req)
					record(fmt.Sprintf("read_%s_%s_%d", image.Key, label, j), r, req, reply, err)
					extent := imageView[uint64(j)*objectSize : uint64(j+1)*objectSize]
					if !view.Exists {
						var server *cephmsgr.OSDError
						if !errors.As(err, &server) || server.Code != -2 {
							t.Fatalf("%s %s object %d: native librados reports ENOENT, Go %v", image.Key, label, j, err)
						}
						checkReply(t, r, reply, -2)
						if !bytes.Equal(extent, make([]byte, objectSize)) {
							t.Fatalf("%s %s object %d is absent but native librbd reads non-zero bytes", image.Key, label, j)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%s %s object %d: %v", image.Key, label, j, err)
					}
					checkReply(t, r, reply, 0)
					stat, data := reply.Operations[0], reply.Operations[1].Data
					if stat.Result != 0 || len(stat.Data) != 16 || binary.LittleEndian.Uint64(stat.Data) != view.Size || uint64(len(data)) != view.Size || reply.Operations[1].Result != 0 {
						t.Fatalf("%s %s object %d stat/read size differs from native librados: native %d", image.Key, label, j, view.Size)
					}
					if !bytes.Equal(data, blob[view.Offset:view.Offset+view.Size]) {
						t.Fatalf("%s %s object %d bytes differ from native librados", image.Key, label, j)
					}
					if !bytes.Equal(append(bytes.Clone(data), make([]byte, objectSize-uint64(len(data)))...), extent) {
						t.Fatalf("%s %s object %d bytes differ from native librbd image extent", image.Key, label, j)
					}
				}
			}
		}
		t.Log("Go head and member-snapshot reads equal native librados objects, native librbd reads and the planned contents")
	})
	if t.Failed() {
		return
	}

	cleanup := native(t, "cleanup")
	t.Run("native_removal", func(t *testing.T) {
		r := cleanup.Removed
		if r == nil || r.GroupID != group.ID || r.GroupHeader != "rbd_group_header."+group.ID || len(r.GroupSnapshotsAfter) != 0 || len(r.MembersAfter) != 0 ||
			r.GroupHeaderErrno != 2 || r.DirectoryCall.Code != -2 || r.DirectoryCall.InputHex != hex.EncodeToString(rbdGroupString(nil, plan.Group)) || len(r.Images) != 2 {
			t.Fatal("native group snapshot, membership or group removal is not proven", r)
		}
		for i, image := range r.Images {
			if image.Key != plan.Images[i].Key || image.ImageID != setup.Images[i].ImageID || image.Prefix != setup.Images[i].Stat.Prefix || len(image.SnapshotsAfter) != 0 ||
				image.HeaderErrno != 2 || image.IDErrno != 2 || len(image.DataErrno) != int(objectCount) || slices.ContainsFunc(image.DataErrno, func(e int) bool { return e != 2 }) {
				t.Fatal("native member snapshot or image removal is not proven", image.Key, image)
			}
		}
		if !slices.Equal(cleanup.ImagesListed, []string{plan.BaseImage}) || len(cleanup.GroupsListed) != 0 {
			t.Fatal("native listing after removal still holds owned group state", cleanup.ImagesListed, cleanup.GroupsListed)
		}
	})
	t.Run("go_reads_after_removal", func(t *testing.T) {
		call(t, "cleanup_dir_get_id", poolID, "rbd_group_directory", "dir_get_id", rbdGroupString(nil, plan.Group), -2)
		call(t, "cleanup_group_snap_list", poolID, "rbd_group_header."+group.ID, "group_snap_list", rbdGroupSnapListInput(rbdGroupSnapRecord{}, plan.ListPage), -2)
		for _, spec := range setupRecord.Snaps {
			call(t, "cleanup_snapshot_get_"+spec.ImageID, spec.Pool, "rbd_header."+spec.ImageID, "snapshot_get", rbdLE64(spec.SnapID), -2)
		}
	})
}

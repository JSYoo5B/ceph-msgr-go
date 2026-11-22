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
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
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
	// cls_rbd's image_group_add sets RBD_OPERATION_FEATURE_GROUP, and with
	// it RBD_FEATURE_OPERATIONS (rbd_clone_test.go), on every group member
	// (ceph/ceph/blob/v20.2.4/src/include/rbd/features.h).
	rbdOperationFeatureGroup = 1 << 2
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

// rbdAsyncRequestID is librbd's AsyncRequestId: the requester's ClientId and
// its u64 request ID, without an envelope.
type rbdAsyncRequestID struct {
	Client  rbdClientID
	Request uint64
}

// decodeRBDQuiesceNotify restates NotifyMessage::decode for QuiescePayload
// and UnquiescePayload (ceph/ceph/blob/v20.2.4/src/librbd/WatchNotifyTypes.cc):
// ENCODE_START(7, 1), the u32 op and the AsyncRequestId.
func decodeRBDQuiesceNotify(data []byte) (uint32, rbdAsyncRequestID, error) {
	var id rbdAsyncRequestID
	d := &rbdDecoder{data: data}
	version, body := d.header(1)
	op := body.u32()
	if d.err == nil && version != 7 {
		d.err = fmt.Errorf("NotifyMessage version %d differs", version)
	}
	if d.err == nil && op != rbdNotifyQuiesce && op != rbdNotifyUnquiesce {
		d.err = fmt.Errorf("notify op %d is not a quiesce request", op)
	}
	id.Client = rbdClientID{GID: body.u64(), Handle: body.u64()}
	id.Request = body.u64()
	d.adopt(body)
	return op, id, d.done()
}

func TestRBDGroupQuiesceEncodings(t *testing.T) {
	id := rbdAsyncRequestID{Client: rbdClientID{GID: 4100, Handle: 93}, Request: 2}
	payload := binary.LittleEndian.AppendUint64(id.Client.encode(), id.Request)
	for _, op := range []uint32{rbdNotifyQuiesce, rbdNotifyUnquiesce} {
		message := rbdNotifyMessage(op, payload)
		if len(message) != 6+4+24 || message[0] != 7 || message[1] != 1 {
			t.Fatalf("quiesce NotifyMessage envelope %x", message)
		}
		got, decoded, err := decodeRBDQuiesceNotify(message)
		if err != nil || got != op || decoded != id {
			t.Fatal("quiesce NotifyMessage decoding", got, decoded, err)
		}
		for cut := 0; cut < len(message); cut++ {
			if _, _, err := decodeRBDQuiesceNotify(message[:cut]); err == nil {
				t.Fatal("truncated quiesce notification admitted", cut)
			}
		}
		older := bytes.Clone(message)
		older[0] = 6
		if _, _, err := decodeRBDQuiesceNotify(older); err == nil {
			t.Fatal("quiesce notification of another version admitted")
		}
	}
	if _, _, err := decodeRBDQuiesceNotify(rbdLockNotify(rbdNotifyAcquiredLock, id.Client)); err == nil {
		t.Fatal("AcquiredLock admitted as a quiesce request")
	}
	// The answers: librbd decodes a ResponseMessage and takes any result
	// other than -EOPNOTSUPP (ImageWatcher::notify_quiesce).
	if r, err := decodeRBDResponseMessage(rbdResponseMessage(rbdGroupQuiesceRefusal)); err != nil || r != -16 {
		t.Fatal("quiesce refusal ResponseMessage", r, err)
	}
	incomplete := rbdGroupSnapRecord{ID: "s2", Name: "snap", State: rbdGroupSnapStateIncomplete, Snaps: []rbdGroupImageSnapSpec{{Pool: 3, ImageID: "i0"}, {Pool: 4, ImageID: "i1"}}}
	list := append(binary.LittleEndian.AppendUint32(nil, 1), encodeRBDGroupSnapRecord(incomplete)...)
	if records, err := decodeRBDGroupSnapList(list); err != nil || len(records) != 1 || !reflect.DeepEqual(records[0], incomplete) {
		t.Fatal("INCOMPLETE group record decoding", records, err)
	}
}

const (
	rbdNotifyQuiesce   = 18 // librbd NOTIFY_OP_QUIESCE
	rbdNotifyUnquiesce = 19 // librbd NOTIFY_OP_UNQUIESCE
	// rbdGroupQuiesceRefusal is the Go watcher's answer to the refused
	// creation's quiesce: -EBUSY (the fixture plan's quiesce_refusal).
	rbdGroupQuiesceRefusal      = -16
	rbdGroupSnapStateIncomplete = 0 // cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE
	// The timed-out creation's result: every quiesce attempt ended with the
	// OSD's ETIMEDOUT (ceph/ceph/blob/v20.2.4/src/librbd/ImageWatcher.cc
	// notify_quiesce).
	rbdGroupQuiesceTimedOut = 110
	// The over-limit creation's result. cls_rbd's snapshot_add refuses
	// member a's snapshot with EDQUOT once the image holds snap_limit
	// snapshots (ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd.cc), but the
	// group path creates member snapshots with
	// SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE, and there SnapshotCreateRequest
	// completes its finisher with the snapshot ID release's result instead
	// of the saved EDQUOT (send_notify_unquiesce returns
	// create_context_finisher(m_ret_val), which the state callback adapter
	// completes with the handler's own result;
	// ceph/ceph/blob/v20.2.4/src/librbd/operation/SnapshotCreateRequest.cc,
	// ceph/ceph/blob/v20.2.4/src/librbd/Utils.h). Group::snap_create then
	// finds no member snapshot in the group namespace and returns ENOENT
	// (ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc).
	rbdGroupMemberSnapshotMissing = 2
	// rbd_quiesce_notification_attempts: the plan's count for the timed-out
	// and retried creations, and librbd's default for the others
	// (ceph/ceph/blob/v20.2.4/src/common/options/rbd.yaml.in).
	rbdGroupQuiesceAttempts        = 2
	rbdGroupDefaultQuiesceAttempts = 10
	// rbdGroupNoSnapLimit is snapshot_get_limit's answer for an image
	// without a limit.
	rbdGroupNoSnapLimit = ^uint64(0)
)

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
	Pool       string            `json:"pool"`
	PreWrites  []nativeRBDExtent `json:"pre_snapshot_writes"`
	PostWrites []nativeRBDExtent `json:"post_snapshot_writes"`
}

type nativeRBDGroupPlan struct {
	Pool              string                    `json:"pool"`
	SecondPool        string                    `json:"second_pool"`
	BaseImage         string                    `json:"base_image"`
	Group             string                    `json:"group"`
	GroupSnapshot     string                    `json:"group_snapshot"`
	RefusedSnapshot   string                    `json:"refused_snapshot"`
	SecondSnapshot    string                    `json:"second_snapshot"`
	TimedOutSnapshot  string                    `json:"timed_out_snapshot"`
	RetriedSnapshot   string                    `json:"retried_snapshot"`
	OverLimitSnapshot string                    `json:"over_limit_snapshot"`
	Order             int                       `json:"order"`
	Size              uint64                    `json:"size"`
	Features          uint64                    `json:"features"`
	ListPage          uint64                    `json:"list_page"`
	PollPage          uint64                    `json:"poll_page"`
	QuiesceRefusal    int32                     `json:"quiesce_refusal"`
	QuiesceAttempts   int                       `json:"quiesce_attempts"`
	Images            []nativeRBDGroupImagePlan `json:"images"`
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

type nativeRBDGroupDataView struct {
	Label   string `json:"label"`
	SnapID  uint64 `json:"snap_id"`
	Objects []struct {
		Index  uint64 `json:"index"`
		Exists bool   `json:"exists"`
		Errno  int    `json:"errno"`
		Size   uint64 `json:"size"`
		Offset uint64 `json:"offset"`
		SHA256 string `json:"sha256"`
	} `json:"objects"`
	Image       nativeRBDBlob `json:"image"`
	ObjectBytes nativeRBDBlob `json:"object_bytes"`
}

type nativeRBDGroupImage struct {
	Key          string `json:"key"`
	Name         string `json:"name"`
	Pool         string `json:"pool"`
	PoolID       int64  `json:"pool_id"`
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
	Features        uint64                        `json:"features"`
	OpFeatures      uint64                        `json:"op_features"`
	Snapshots       []nativeRBDGroupImageSnapshot `json:"snapshots"`
	SnapshotCalls   []nativeRBDGroupCall          `json:"snapshot_calls"`
	SnapContextCall nativeRBDGroupCall            `json:"snapcontext_call"`
	LockCall        nativeRBDGroupCall            `json:"lock_call"`
	Views           []nativeRBDGroupDataView      `json:"views"`
}

type nativeRBDGroupCreationPhase struct {
	Phase    string             `json:"phase"`
	AfterNS  int64              `json:"after_ns"`
	ListCall nativeRBDGroupCall `json:"list_call"`
	Images   []struct {
		Key             string             `json:"key"`
		SnapContextCall nativeRBDGroupCall `json:"snapcontext_call"`
		LockCall        nativeRBDGroupCall `json:"lock_call"`
		LimitCall       nativeRBDGroupCall `json:"limit_call"`
	} `json:"images"`
	RollbackErrno *int `json:"rollback_errno"`
	// MemberSnapshotCall is the over-limit creation's snapshot_get of
	// member b's newest snapshot while librbd compensates.
	MemberSnapshotCall *nativeRBDGroupCall `json:"member_snapshot_call"`
}

type nativeRBDGroupCreation struct {
	Snapshot        string                        `json:"snapshot"`
	GroupID         string                        `json:"group_id"`
	ImageIDs        map[string]string             `json:"image_ids"`
	QuiesceAttempts int                           `json:"quiesce_attempts"`
	Phases          []nativeRBDGroupCreationPhase `json:"phases"`
	// SnapLimit is the over-limit creation's native snapshot limit on
	// member a: librbd set it to the member's snapshot count before the
	// creation and removed it afterwards.
	SnapLimit *struct {
		Image        string             `json:"image"`
		Snapshots    uint64             `json:"snapshots"`
		Limit        uint64             `json:"limit"`
		SetCall      nativeRBDGroupCall `json:"set_call"`
		RemovedLimit uint64             `json:"removed_limit"`
		RemovedCall  nativeRBDGroupCall `json:"removed_call"`
	} `json:"snap_limit"`
	Errno     int   `json:"errno"`
	ElapsedNS int64 `json:"elapsed_ns"`
}

// nativeRBDGroupState is the native driver's published state during a
// creation (rbd-group-<action>-state.json).
type nativeRBDGroupState struct {
	Token          string                        `json:"token"`
	RequestID      int64                         `json:"request_id"`
	NativeGlobalID uint64                        `json:"native_global_id"`
	Snapshot       string                        `json:"snapshot"`
	GroupID        string                        `json:"group_id"`
	ImageIDs       map[string]string             `json:"image_ids"`
	Phase          string                        `json:"phase"`
	Phases         []nativeRBDGroupCreationPhase `json:"phases"`
}

type nativeRBDGroupView struct {
	Name          string                 `json:"name"`
	ID            string                 `json:"id"`
	HeaderObject  string                 `json:"header_object"`
	Members       []nativeRBDGroupMember `json:"members"`
	Snapshots     []nativeRBDGroupSnap   `json:"snapshots"`
	SnapshotInfos []nativeRBDGroupSnap   `json:"snapshot_infos"`
	DirectoryCall nativeRBDGroupCall     `json:"directory_call"`
	ListCalls     []nativeRBDGroupCall   `json:"list_calls"`
}

type nativeRBDGroup struct {
	Action         string   `json:"action"`
	RequestID      int64    `json:"request_id"`
	PoolID         int64    `json:"pool_id"`
	SecondPoolID   int64    `json:"second_pool_id"`
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
	Creation     *nativeRBDGroupCreation `json:"creation"`
	RolledBackTo string                  `json:"rolled_back_to"`
	Group        *nativeRBDGroupView     `json:"group"`
	Images       []nativeRBDGroupImage   `json:"images"`
	Removed      *struct {
		GroupID     string `json:"group_id"`
		GroupHeader string `json:"group_header"`
		Images      []struct {
			Key            string                        `json:"key"`
			Name           string                        `json:"name"`
			Pool           string                        `json:"pool"`
			ImageID        string                        `json:"image_id"`
			Prefix         string                        `json:"prefix"`
			Objects        uint64                        `json:"objects"`
			SnapshotsAfter []nativeRBDGroupImageSnapshot `json:"snapshots_after"`
			HeaderErrno    int                           `json:"header_errno"`
			IDErrno        int                           `json:"id_errno"`
			ObjectMapErrno int                           `json:"object_map_errno"`
			DataErrno      []int                         `json:"data_errno"`
		} `json:"images"`
		GroupSnapshotsAfter [][]nativeRBDGroupSnap `json:"group_snapshots_after"`
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

// nativeRBDGroupOracle is the group acceptance's part of the RBD base
// oracle: its second owned pool.
type nativeRBDGroupOracle struct {
	SecondPool *struct {
		ID        int64  `json:"pool"`
		Name      string `json:"pool_name"`
		Flags     uint64 `json:"flags"`
		FlagNames string `json:"flags_names"`
	} `json:"group_second_pool"`
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
	StderrLines    int
}

// rbdGroupNotification is one notification on the Go member header watch
// and the Go answer to it.
type rbdGroupNotification struct {
	Action           string
	Index            int
	AfterMS          float64
	NotifyID         uint64
	NotifierGlobalID uint64
	Cookie           uint64
	PayloadHex       string
	Op               uint32
	Request          *rbdAsyncRequestID `json:",omitempty"`
	// Held marks a quiesce or unquiesce request whose answer waited for the
	// test's reads; HeldMS is that wait. An omitted answer
	// (AckDisposition omitted) was never sent.
	Held           bool
	HeldMS         float64
	AckDisposition string
	AckHex         string
	AckError       string `json:",omitempty"`
	Unexpected     bool
}

// rbdGroupHoldEvidence is what the Go test read while it held one answer.
type rbdGroupHoldEvidence struct {
	Action, Phase    string
	NotifyID         uint64
	Request          rbdAsyncRequestID
	ControlRunning   [2]bool // before the first read and after the last
	GroupSnapListHex string
	SnapContexts     map[string]string
	LockInfos        map[string]string
	SnapLimits       map[string]string
	NativePhase      string `json:",omitempty"`
	HeldMS           float64
	AnswerHex        string
	Omitted          bool
}

type rbdGroupEvidence struct {
	Token          string
	Pool           string
	PoolID         int64
	SecondPool     string
	SecondPoolID   int64
	GoIdentity     string
	GoGlobalID     uint64
	GoWatchCookie  uint64
	GroupID        string
	Snapshots      map[string]string
	Records        map[string]rbdGroupSnapRecord
	Requests       []rbdGroupRequestEvidence
	ClassOutputs   map[string]string
	Notifications  []rbdGroupNotification
	Holds          []rbdGroupHoldEvidence
	Pings          int
	PingErrors     []string
	CreationErrnos map[string]int
	NativeActions  []rbdGroupNativeActionEvidence
	Timings        map[string]string
}

// rbdGroupHeld is one quiesce or unquiesce request the watch responder hands
// to the test, which answers it on Answer (nil is an empty acknowledgement)
// or closes Answer to leave the request unanswered.
type rbdGroupHeld struct {
	Event    cephmsgr.OSDWatchEvent
	Op       uint32
	Request  rbdAsyncRequestID
	Received time.Time
	Answer   chan []byte
}

// rbdGroupResponder answers the Go member header watch's notifications in
// librbd's ImageWatcher form: lock and header-update notifications get an
// empty acknowledgement at once; quiesce and unquiesce requests go to the
// test, which reads the cluster before it answers (a quiesce with a
// ResponseMessage, an unquiesce with an empty acknowledgement) or leaves the
// request unanswered, so the OSD times the notify out for the notifier. It
// pings the watch while idle.
//
// A notification is recorded only after its acknowledgement returned, which
// can be after the notifier's notify completed: the OSD completes the notify
// once it has the acknowledgement, independently of when the Go watcher
// receives the OSD reply. A verdict on the recorded notifications therefore
// settles first (settle).
type rbdGroupResponder struct {
	mu         sync.Mutex
	action     string
	allowed    []uint32
	events     []rbdGroupNotification
	ledger     rbdWatchLedger
	changed    chan struct{} // closed and replaced on every recording
	failure    error
	pings      int
	pingErrors []string
	held       chan rbdGroupHeld
}

func newRBDGroupResponder() *rbdGroupResponder {
	return &rbdGroupResponder{held: make(chan rbdGroupHeld), changed: make(chan struct{})}
}

func (r *rbdGroupResponder) fail(err error) {
	r.mu.Lock()
	if r.failure == nil {
		r.failure = err
	}
	r.mu.Unlock()
}

// begin starts one creation cell: notifications are attributed to action and
// must have one of the allowed ops.
func (r *rbdGroupResponder) begin(action string, allowed ...uint32) {
	r.mu.Lock()
	r.action, r.allowed = action, allowed
	r.mu.Unlock()
}

// receive opens the record of one notification: it is in flight until
// record appends it.
func (r *rbdGroupResponder) receive(received, started time.Time, event cephmsgr.OSDWatchEvent) (rbdGroupNotification, []uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ledger.receive(event.NotifyID)
	n := rbdGroupNotification{Action: r.action, AfterMS: float64(received.Sub(started).Microseconds()) / 1000, NotifyID: event.NotifyID,
		NotifierGlobalID: event.NotifierGlobalID, Cookie: event.Cookie, PayloadHex: hex.EncodeToString(event.Payload)}
	return n, slices.Clone(r.allowed)
}

// record appends a received notification with its acknowledgement.
func (r *rbdGroupResponder) record(n rbdGroupNotification) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n.Index = len(r.events)
	r.events = append(r.events, n)
	r.ledger.record(n.NotifyID)
	close(r.changed)
	r.changed = make(chan struct{})
}

// settle waits, bounded by ctx, until every notification in ids is recorded
// and no received notification is still unrecorded, and returns the
// recorded notifications (settleRBDWatch).
func (r *rbdGroupResponder) settle(ctx context.Context, ids ...uint64) ([]rbdGroupNotification, error) {
	return settleRBDWatch(ctx, &r.mu, &r.ledger, func() ([]rbdGroupNotification, <-chan struct{}, error) {
		return slices.Clone(r.events), r.changed, r.failure
	}, ids)
}

func (r *rbdGroupResponder) handle(ctx context.Context, w *cephmsgr.OSDWatch, epoch uint32, started time.Time, event cephmsgr.OSDWatchEvent) {
	received := time.Now()
	n, allowed := r.receive(received, started, event)
	defer func() { r.record(n) }()
	if event.Opcode != cephmsgr.OSDWatchNotification || event.Cookie != w.Cookie() {
		n.Unexpected = true
		r.fail(fmt.Errorf("unexpected member header watch event opcode %d cookie %d", event.Opcode, event.Cookie))
		return
	}
	var ack []byte
	op, request, err := decodeRBDQuiesceNotify(event.Payload)
	if err == nil {
		n.Op, n.Request, n.Held = op, &request, true
		h := rbdGroupHeld{Event: event, Op: op, Request: request, Received: received, Answer: make(chan []byte, 1)}
		select {
		case r.held <- h:
		case <-ctx.Done():
			return
		}
		var answered bool
		select {
		case ack, answered = <-h.Answer:
		case <-ctx.Done():
			// The test ended without an answer: no acknowledgement.
			n.AckDisposition = rbdAckOmitted
			return
		}
		n.HeldMS = float64(time.Since(received).Microseconds()) / 1000
		if !answered {
			// The test left the request unanswered.
			n.AckDisposition = rbdAckOmitted
			if !slices.Contains(allowed, n.Op) {
				n.Unexpected = true
				r.fail(fmt.Errorf("unexpected member header notification op %d from client.%d during %s", n.Op, event.NotifierGlobalID, n.Action))
			}
			return
		}
	} else {
		m, err := decodeRBDNotifyMessage(event.Payload)
		n.Op = m.Op
		if err != nil || !m.Known {
			n.Op = ^uint32(0)
		}
	}
	if !slices.Contains(allowed, n.Op) {
		n.Unexpected = true
		r.fail(fmt.Errorf("unexpected member header notification op %d from client.%d during %s", n.Op, event.NotifierGlobalID, n.Action))
	}
	n.AckHex = hex.EncodeToString(ack)
	_, submitErr := w.Acknowledge(ctx, epoch, event, ack)
	n.AckDisposition = rbdAckDispositionOf(false, ack, submitErr)
	if submitErr != nil {
		n.AckError = submitErr.Error()
		r.fail(fmt.Errorf("acknowledge member header notification %d: %w", event.NotifyID, submitErr))
	}
}

// rbdGroupExpectedAnswer is one quiesce or unquiesce request the test held
// and the Go watcher's answer to it: an acknowledgement carrying Ack (empty
// for an unquiesce), or none at all (Omitted).
type rbdGroupExpectedAnswer struct {
	NotifyID uint64
	Op       uint32
	Ack      []byte
	Omitted  bool
}

// settleRBDGroupCreation waits for the member header notifications of one
// native creation to settle, keyed by the exact notify IDs of the held
// requests, and judges them: the quiesce attempts and the final unquiesce
// are exactly the held requests, in order, each answered as held (or never
// answered), and every other notification is acknowledged empty once. A
// creation that locked its members (locked) sends one AcquiredLock after
// the last quiesce and before the unquiesce and one ReleasedLock after the
// unquiesce, both from the creating client, with a HeaderUpdate between
// them; one that removed a member snapshot after the unquiesce
// (compensated) sends a HeaderUpdate after the unquiesce too. A creation
// that took no lock sends nothing but the held requests.
func settleRBDGroupCreation(ctx context.Context, r *rbdGroupResponder, action string, held []rbdGroupExpectedAnswer, nativeGID uint64, locked, compensated bool) ([]rbdGroupNotification, error) {
	if len(held) < 2 || held[len(held)-1].Op != rbdNotifyUnquiesce || slices.ContainsFunc(held[:len(held)-1], func(h rbdGroupExpectedAnswer) bool { return h.Op != rbdNotifyQuiesce }) {
		return nil, fmt.Errorf("%s: held requests are not quiesce attempts followed by one unquiesce", action)
	}
	ids := make([]uint64, 0, len(held))
	for _, h := range held {
		ids = append(ids, h.NotifyID)
	}
	settleCtx, cancel := context.WithTimeout(ctx, rbdNotifySettleTimeout)
	defer cancel()
	events, err := r.settle(settleCtx, ids...)
	if err != nil {
		return events, err
	}
	var own []rbdGroupNotification
	for _, e := range events {
		if e.Action == action {
			own = append(own, e)
		}
	}
	seen := map[uint64]bool{}
	var requests []rbdGroupNotification
	count := map[uint32]int{}
	for _, e := range own {
		if seen[e.NotifyID] {
			return events, fmt.Errorf("%s: member header notification %d recorded twice", action, e.NotifyID)
		}
		seen[e.NotifyID] = true
		count[e.Op]++
		if e.Op == rbdNotifyQuiesce || e.Op == rbdNotifyUnquiesce {
			requests = append(requests, e)
			continue
		}
		if e.Unexpected || e.AckDisposition != rbdAckSubmittedEmpty || e.AckHex != "" || e.AckError != "" || e.NotifierGlobalID != nativeGID {
			return events, fmt.Errorf("%s: member header notification differs: %+v", action, e)
		}
	}
	// Every quiesce attempt (a retry follows only a timeout) and the
	// unquiesce are the exact notifications the test held.
	if len(requests) != len(held) {
		return events, fmt.Errorf("%s: member header quiesce requests differ: %+v", action, requests)
	}
	for i, want := range held {
		e := requests[i]
		disposition := rbdAckDispositionOf(want.Omitted, want.Ack, nil)
		if e.NotifyID != want.NotifyID || e.Op != want.Op || e.AckDisposition != disposition || e.AckHex != hex.EncodeToString(want.Ack) || want.Omitted && len(want.Ack) != 0 ||
			e.AckError != "" || e.Unexpected || e.NotifierGlobalID != nativeGID {
			return events, fmt.Errorf("%s: member header quiesce request %d differs: %+v", action, i, e)
		}
	}
	if !locked {
		if len(own) != len(held) {
			return events, fmt.Errorf("%s: a creation without member locks sent more than its quiesce requests: %+v", action, own)
		}
		return events, nil
	}
	position := func(op uint32) int {
		return slices.IndexFunc(own, func(e rbdGroupNotification) bool { return e.Op == op })
	}
	at := func(id uint64) int {
		return slices.IndexFunc(own, func(e rbdGroupNotification) bool { return e.NotifyID == id })
	}
	lastQuiesce, unquiesce := at(held[len(held)-2].NotifyID), at(held[len(held)-1].NotifyID)
	acquired, header, released := position(rbdNotifyAcquiredLock), position(rbdNotifyHeaderUpdate), position(rbdNotifyReleasedLock)
	if count[rbdNotifyAcquiredLock] != 1 || count[rbdNotifyReleasedLock] != 1 || count[rbdNotifyHeaderUpdate] == 0 || position(rbdNotifyQuiesce) != 0 ||
		acquired < lastQuiesce || header < acquired || unquiesce < acquired || released < unquiesce || released < header {
		return events, fmt.Errorf("%s: member header lock notifications differ: %+v", action, own)
	}
	if compensated && !slices.ContainsFunc(own[unquiesce+1:], func(e rbdGroupNotification) bool { return e.Op == rbdNotifyHeaderUpdate }) {
		return events, fmt.Errorf("%s: no header update followed the unquiesce of a compensated creation: %+v", action, own)
	}
	return events, nil
}

// waitRBDGroupState polls the group driver's published creation state until
// it holds the named phase, or the control action ended.
func waitRBDGroupState(ctx context.Context, path, phase string, finished <-chan struct{}, finishedErr *error) (nativeRBDGroupState, nativeRBDGroupCreationPhase, error) {
	read := func() (nativeRBDGroupState, nativeRBDGroupCreationPhase, bool) {
		var state nativeRBDGroupState
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &state) != nil {
			return state, nativeRBDGroupCreationPhase{}, false
		}
		i := slices.IndexFunc(state.Phases, func(p nativeRBDGroupCreationPhase) bool { return p.Phase == phase })
		if i < 0 {
			return state, nativeRBDGroupCreationPhase{}, false
		}
		return state, state.Phases[i], true
	}
	for {
		if state, p, ok := read(); ok {
			return state, p, nil
		}
		select {
		case <-finished:
			if state, p, ok := read(); ok {
				return state, p, nil
			}
			return nativeRBDGroupState{}, nativeRBDGroupCreationPhase{}, fmt.Errorf("native RBD group action ended before %s: %v", phase, *finishedErr)
		case <-ctx.Done():
			return nativeRBDGroupState{}, nativeRBDGroupCreationPhase{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestRBDGroupCreationSettlement checks the member header notification
// verdict against a Go watcher that records the final acknowledgements only
// after the native creation returned: the verdict waits for the exact held
// notify IDs and every notification in flight, admits the refused, accepted,
// timed-out, retried and over-limit shapes, and still refuses an extra or
// missing quiesce attempt, a duplicate, a wrong or omitted answer, a missing
// lock or compensation notification and a notification that never settles.
func TestRBDGroupCreationSettlement(t *testing.T) {
	const native = 4100
	type step struct {
		id       uint64
		op       uint32
		ack      []byte
		omit     bool // the watcher never answered
		late     bool // recorded only after the native creation returned
		unseen   bool // not yet received when the native creation returned
		never    bool // received but never recorded
		notifier uint64
	}
	accepted := func() []step {
		return []step{{id: 11, op: rbdNotifyQuiesce, ack: rbdResponseMessage(0)}, {id: 12, op: rbdNotifyAcquiredLock}, {id: 13, op: rbdNotifyHeaderUpdate},
			{id: 14, op: rbdNotifyUnquiesce}, {id: 15, op: rbdNotifyReleasedLock}}
	}
	refused := func() []step {
		return []step{{id: 11, op: rbdNotifyQuiesce, ack: rbdResponseMessage(rbdGroupQuiesceRefusal)}, {id: 14, op: rbdNotifyUnquiesce}}
	}
	timedOut := func() []step {
		return []step{{id: 10, op: rbdNotifyQuiesce, omit: true}, {id: 11, op: rbdNotifyQuiesce, omit: true}, {id: 14, op: rbdNotifyUnquiesce}}
	}
	retried := func() []step {
		return []step{{id: 10, op: rbdNotifyQuiesce, omit: true}, {id: 11, op: rbdNotifyQuiesce, ack: rbdResponseMessage(0)}, {id: 12, op: rbdNotifyAcquiredLock},
			{id: 13, op: rbdNotifyHeaderUpdate}, {id: 14, op: rbdNotifyUnquiesce, omit: true}, {id: 15, op: rbdNotifyReleasedLock}}
	}
	overLimit := func() []step {
		return []step{{id: 11, op: rbdNotifyQuiesce, ack: rbdResponseMessage(0)}, {id: 12, op: rbdNotifyAcquiredLock}, {id: 13, op: rbdNotifyHeaderUpdate},
			{id: 14, op: rbdNotifyUnquiesce}, {id: 16, op: rbdNotifyHeaderUpdate}, {id: 17, op: rbdNotifyReleasedLock}}
	}
	// expectOf is the held list of an intended watcher run.
	expectOf := func(steps []step) []rbdGroupExpectedAnswer {
		var held []rbdGroupExpectedAnswer
		for _, s := range steps {
			if s.op == rbdNotifyQuiesce || s.op == rbdNotifyUnquiesce {
				held = append(held, rbdGroupExpectedAnswer{NotifyID: s.id, Op: s.op, Ack: s.ack, Omitted: s.omit})
			}
		}
		return held
	}
	late := func(steps []step, from int, unseen bool) []step {
		for i := from; i < len(steps); i++ {
			steps[i].late, steps[i].unseen = true, unseen
		}
		return steps
	}
	with := func(steps []step, f func([]step) []step) []step { return f(steps) }
	cases := []struct {
		name                string
		steps               []step
		expect              []rbdGroupExpectedAnswer // nil: expectOf(steps)
		locked, compensated bool
		ok                  bool
	}{
		{name: "accepted recorded before return", steps: accepted(), locked: true, ok: true},
		{name: "accepted unquiesce and release recorded after return", steps: late(accepted(), 3, false), locked: true, ok: true},
		// The unquiesce key holds the verdict until it is received; the
		// release librbd waited for is already in flight.
		{name: "accepted unquiesce received after return", steps: with(late(accepted(), 3, false), func(s []step) []step { s[3].unseen = true; return s }), locked: true, ok: true},
		{name: "refused unquiesce recorded after return", steps: late(refused(), 1, false), ok: true},
		{name: "refused unquiesce received after return", steps: late(refused(), 1, true), ok: true},
		{name: "timed out", steps: timedOut(), ok: true},
		{name: "timed out attempts recorded after return", steps: late(timedOut(), 1, false), ok: true},
		{name: "retried", steps: retried(), locked: true, ok: true},
		{name: "retried unanswered unquiesce recorded after return", steps: late(retried(), 4, false), locked: true, ok: true},
		{name: "over limit", steps: overLimit(), locked: true, compensated: true, ok: true},
		{name: "unquiesce never recorded", steps: with(refused(), func(s []step) []step { s[1].never = true; return s })},
		{name: "unquiesce never received", steps: refused()[:1], expect: expectOf(refused())},
		{name: "lock notification never recorded", steps: with(accepted(), func(s []step) []step { s[4].never = true; return s }), locked: true},
		{name: "unexpected quiesce attempt", steps: append([]step{{id: 10, op: rbdNotifyQuiesce, ack: rbdResponseMessage(rbdGroupQuiesceRefusal)}}, refused()...), expect: expectOf(refused())},
		{name: "duplicate notify ID", steps: append(accepted(), step{id: 15, op: rbdNotifyReleasedLock}), locked: true},
		{name: "quiesce answered otherwise", steps: with(accepted(), func(s []step) []step { s[0].ack = rbdResponseMessage(-95); return s }), expect: expectOf(accepted()), locked: true},
		{name: "unquiesce answered with a response", steps: with(refused(), func(s []step) []step { s[1].ack = rbdResponseMessage(0); return s }), expect: expectOf(refused())},
		{name: "accepted without its release", steps: accepted()[:4], locked: true},
		{name: "accepted with a second acquire", steps: append(accepted(), step{id: 16, op: rbdNotifyAcquiredLock}), locked: true},
		{name: "release before the unquiesce", steps: with(accepted(), func(s []step) []step { s[3], s[4] = s[4], s[3]; return s }), locked: true},
		{name: "refused with a lock notification", steps: append(refused(), step{id: 15, op: rbdNotifyReleasedLock})},
		{name: "lock notification from another client", steps: with(accepted(), func(s []step) []step { s[1].notifier = native + 1; return s }), locked: true},
		{name: "timed out with an answered attempt", steps: with(timedOut(), func(s []step) []step { s[0].omit, s[0].ack = false, rbdResponseMessage(0); return s }), expect: expectOf(timedOut())},
		{name: "timed out without the retry", steps: timedOut()[1:], expect: expectOf(timedOut())},
		{name: "timed out unquiesce unanswered", steps: with(timedOut(), func(s []step) []step { s[2].omit = true; return s }), expect: expectOf(timedOut())},
		{name: "retried lock before the retry", steps: with(retried(), func(s []step) []step { s[1], s[2] = s[2], s[1]; return s }), locked: true},
		{name: "retried unquiesce acknowledged", steps: with(retried(), func(s []step) []step { s[4].omit = false; return s }), expect: expectOf(retried()), locked: true},
		{name: "over limit without the removal's header update", steps: with(overLimit(), func(s []step) []step { return append(s[:4], s[5]) }), locked: true, compensated: true},
		{name: "held list without its unquiesce", steps: refused(), expect: expectOf(refused())[:1]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRBDGroupResponder()
			r.begin("cell", rbdNotifyQuiesce, rbdNotifyUnquiesce, rbdNotifyAcquiredLock, rbdNotifyReleasedLock, rbdNotifyHeaderUpdate)
			started := time.Now()
			deliver := func(s step) rbdGroupNotification {
				notifier := s.notifier
				if notifier == 0 {
					notifier = native
				}
				n, _ := r.receive(time.Now(), started, cephmsgr.OSDWatchEvent{Opcode: cephmsgr.OSDWatchNotification, NotifyID: s.id, NotifierGlobalID: notifier, Cookie: 1})
				n.Op, n.AckHex, n.AckDisposition = s.op, hex.EncodeToString(s.ack), rbdAckDispositionOf(s.omit, s.ack, nil)
				return n
			}
			// later holds, in delivery order, what the watcher records
			// after the native creation returned.
			var later []func()
			for _, s := range tc.steps {
				switch {
				case s.unseen:
					later = append(later, func() { r.record(deliver(s)) })
				case s.never:
					deliver(s)
				case s.late:
					n := deliver(s)
					later = append(later, func() { r.record(n) })
				default:
					r.record(deliver(s))
				}
			}
			// The native creation has returned here.
			go func() {
				time.Sleep(50 * time.Millisecond)
				for _, f := range later {
					f()
				}
			}()
			expect := tc.expect
			if expect == nil {
				expect = expectOf(tc.steps)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			events, err := settleRBDGroupCreation(ctx, r, "cell", expect, native, tc.locked, tc.compensated)
			if tc.ok && (err != nil || len(events) != len(tc.steps)) {
				t.Fatal("settled creation refused", err, events)
			}
			if !tc.ok && err == nil {
				t.Fatalf("creation verdict admitted %+v", events)
			}
		})
	}
}

// rbdGroupStderrAllowed reports whether every non-empty native stderr line
// is one of librbd's expected error-level reports for the action and each
// expected report appears.
func rbdGroupStderrAllowed(stderr []byte, expected []string) bool {
	seen := map[string]bool{}
	for _, line := range strings.Split(string(stderr), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		i := slices.IndexFunc(expected, func(s string) bool { return strings.Contains(line, s) })
		if i < 0 {
			return false
		}
		seen[expected[i]] = true
	}
	return len(seen) == len(expected)
}

func TestRBDGroupStderrAllowed(t *testing.T) {
	quiesce := []string{"failed to notify quiesce", "group snapshot is not complete"}
	good := "2026 7f -1 librbd::ImageWatcher: 0x1 notify_quiesce: failed to notify quiesce: (16) Device or resource busy\n2026 7f -1 librbd::api::Group: snap_rollback: group snapshot is not complete\n"
	if !rbdGroupStderrAllowed([]byte(good), quiesce) || !rbdGroupStderrAllowed(nil, nil) {
		t.Fatal("expected librbd reports refused")
	}
	for _, bad := range []string{"", "failed to notify quiesce\n", good + "error while cleaning up group snapshot\n"} {
		if rbdGroupStderrAllowed([]byte(bad), quiesce) {
			t.Fatalf("native stderr admitted: %q", bad)
		}
	}
	if rbdGroupStderrAllowed([]byte("lderr\n"), nil) {
		t.Fatal("native stderr admitted for an action without expected reports")
	}
}

// TestCephRBDGroupSnapshotIntegration follows a native RBD group through its
// snapshot lifecycle with the explicit OSD API. Native librbd creates two
// layering, exclusive-lock, object-map and fast-diff images with different contents, a
// in the fixture's RBD pool and b in a second owned pool, a group in the
// first pool holding both, and the first group snapshot; every mutation
// stays native. After each native step the Go client resolves the group ID
// with dir_get_id on rbd_group_directory, pages through rbd.group_snap_list
// on the group header with a page size of one (each next page starting after
// the previous record, re-encoded by the test), and sends rbd.snapshot_get
// for every member tuple to that tuple's image header in the tuple's pool,
// rbd.get_snapcontext and lock.get_info to both member headers, and stat and
// read to every data object at the head and at every member snapshot. Every
// raw class input and output is compared with native librados exec of the
// same input, every record and member snapshot with native librbd's
// listings and group namespaces, and every data object with native librados
// reads, native librbd image reads and the planned contents.
//
// Five native group snapshot creations then run under a Go watch on member
// b's header. librbd writes the INCOMPLETE record, quiesces every member,
// acquires the exclusive locks, creates the member snapshots, writes the
// COMPLETE record and unquiesces; on a failed quiesce it unquiesces and
// removes the record, and on a failed member snapshot it unquiesces, removes
// the member snapshots that exist and removes the record
// (ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc snap_create,
// ImageWatcher.cc notify_quiesce). Each quiesce attempt is one notify with
// librbd's 5 s timeout (ceph/ceph/blob/v20.2.4/src/librbd/watcher/Notifier.cc);
// librbd retries a timed-out quiesce with the same request ID up to
// rbd_quiesce_notification_attempts times, and ignores the unquiesce's
// result. While the Go watcher holds each quiesce and unquiesce request,
// the Go test reads the group header and both member headers
// (group_snap_list, get_snapcontext, lock.get_info, snapshot_get_limit) and,
// at the requests the native driver observes, the driver's second thread
// reads the same objects with the same inputs and the outputs must be
// byte-identical; at the first quiesce it also tries a rollback to the
// snapshot being created, which librbd refuses with EINVAL for an
// INCOMPLETE record. Every hold is judged before the Go watcher answers.
//
//   - refused: the quiesce is answered with ResponseMessage(-EBUSY). At the
//     quiesce and at the unquiesce the record is INCOMPLETE with zero member
//     snapshot IDs and no member snapshot or lock exists; the creation
//     returns EBUSY and afterwards the record is gone.
//   - accepted: the quiesce is answered with ResponseMessage(0). At the
//     quiesce the record is INCOMPLETE without member snapshots or locks; at
//     the unquiesce it is COMPLETE with both member snapshots and native
//     librbd holds both exclusive locks, member b's cookie handle being the
//     quiesce requester's; after the creation the locks are released.
//   - timed-out: the creating client sets rbd_quiesce_notification_attempts
//     to plan.QuiesceAttempts (2) and the Go watcher answers no quiesce
//     attempt. Each attempt reaches the watch as a new notification with the
//     first one's request ID; the record stays INCOMPLETE without member
//     snapshots or locks through every attempt and the unquiesce, the
//     creation returns ETIMEDOUT with librbd's "failed to notify quiesce:
//     (110)" report, and afterwards the record is gone.
//   - retried: with the same attempt count, the first attempt is left to
//     time out and the retry is answered with ResponseMessage(0); librbd then
//     locks, snapshots both members and writes COMPLETE. The unquiesce is
//     never answered: the record is COMPLETE with both locks held while the
//     Go watcher holds it, and after that notify times out the creation
//     returns success and the locks are released.
//   - over-limit: native librbd sets member a's snapshot limit to its
//     snapshot count. The quiesce is answered with ResponseMessage(0);
//     cls_rbd's snapshot_add refuses member a's snapshot with EDQUOT while
//     member b's is created. At the unquiesce the record is still INCOMPLETE
//     with zero member snapshot IDs, member a has no new snapshot, member
//     b's new snapshot names the INCOMPLETE record in its group namespace
//     (Go and native snapshot_get are byte-identical) and both locks are
//     held. A header update follows the unquiesce on member b, and
//     afterwards the record and member b's snapshot are gone (Go
//     snapshot_get answers ENOENT). The creation returns ENOENT, not the
//     class's EDQUOT: on the group path librbd's per-image snapshot request
//     completes with success after the refusal, and snap_create then finds
//     no member snapshot (rbdGroupMemberSnapshotMissing). Native librbd
//     removes the limit, and Go and native snapshot_get_limit then answer no
//     limit.
//
// After each creation the Go reads of the group and both members are
// compared with native librbd again, and every member's data objects with
// the planned contents. The member header notifications of each creation
// are judged once settled: the held requests in order, each answered as
// held or never answered, and for the locking creations one AcquiredLock
// after the last quiesce, a HeaderUpdate and one ReleasedLock after the
// unquiesce. Native librbd then rolls the group back to the first snapshot;
// the heads read as the first snapshot while the second and retried
// snapshots keep the later contents. Finally native librbd removes the three
// group snapshots, the membership, the group and both images, each followed
// by a native absence read, and the Go reads of the old directory entry,
// group header and member headers answer ENOENT.
//
// Not covered: a lock acquisition failure; a failure of the COMPLETE
// group_snap_set after the member snapshots, which shares the over-limit
// compensation path in librbd but has no bounded fault here; journaling
// members; members in another namespace (librbd refuses them before any
// wire request); mirrored group snapshots and migration.
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdgroupsnapshot key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-group-snapshot
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdgroupsnapshot key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-group-snapshot
//scenario:covers RBD underlying I/O
//fixture:isolated adds a second owned pool; natively snapshots, limits, rolls back and removes a group of two images
func TestCephRBDGroupSnapshotIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if os.Getenv("CEPH_MSGR_TEST_RBD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || dir == "" {
		t.Skip("requires the opt-in disposable RBD fixture (CEPH_MSGR_TEST_RBD=1)")
	}
	var oracle nativeRBDOracle
	var groupOracle nativeRBDGroupOracle
	if err := errors.Join(readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &oracle), readMDSFreshJSON(filepath.Join(dir, "rbd-base-oracle.json"), &groupOracle)); err != nil {
		t.Fatal(err)
	}
	token, identity := oracle.Token, os.Getenv("CEPH_MSGR_RBD_BASE_IDENTITY")
	second := groupOracle.SecondPool
	if second == nil || second.Name != "ceph-msgr-rbd2-"+token || second.ID == oracle.Pool.ID || second.Flags&1 != 0 {
		t.Fatal("native RBD oracle lacks the second owned pool", second)
	}
	if oracle.Source != mdsFreshSource || len(token) != 32 || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") || oracle.Plan.Pool != "ceph-msgr-rbd-"+token || oracle.Pool.Name != oracle.Plan.Pool ||
		oracle.Pool.Flags&1 != 0 || identity != "client.rbd-base-"+token || oracle.ClientIdentity != identity || oracle.Caps["mon"] != "profile rbd" ||
		oracle.Caps["osd"] != "profile rbd pool="+oracle.Plan.Pool+", profile rbd pool="+second.Name {
		t.Fatal("native RBD oracle scope differs", oracle.Source, oracle.Pool, identity, oracle.Caps)
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
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate the RBD-profile Go client", err)
	}
	defer c.Close()
	gid := c.Snapshot().GlobalID
	poolID, secondID := oracle.Pool.ID, second.ID
	poolNames := map[int64]string{poolID: oracle.Plan.Pool, secondID: second.Name}
	evidence := rbdGroupEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: poolID, SecondPool: second.Name, SecondPoolID: secondID, GoIdentity: identity, GoGlobalID: gid,
		Snapshots: map[string]string{}, Records: map[string]rbdGroupSnapRecord{}, ClassOutputs: map[string]string{}, CreationErrnos: map[string]int{}, Timings: map[string]string{}}
	var responder *rbdGroupResponder
	defer func() {
		if responder != nil {
			responder.mu.Lock()
			evidence.Notifications, evidence.Pings, evidence.PingErrors = slices.Clone(responder.events), responder.pings, slices.Clone(responder.pingErrors)
			responder.mu.Unlock()
		}
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
	// fixture and answered with its own receipt. stderr lists librbd's
	// expected error-level reports for the action.
	var plan nativeRBDGroupPlan
	receipt := func(t *testing.T, action string, stderr []string) *nativeRBDGroup {
		t.Helper()
		var result nativeRBDGroupResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-group-"+action+".json"), &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-group-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly || result.Mutation.RequestID != result.RequestID ||
			result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil || n.Action != action || n.RequestID != result.RequestID ||
			n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != poolID || n.SecondPoolID != secondID || plan.Pool != "" && !reflect.DeepEqual(result.Plan, plan) {
			t.Fatal("native RBD group receipt differs", action, result.Error)
		}
		if !rbdGroupStderrAllowed(result.Process.Stderr, stderr) {
			t.Fatalf("native RBD group %s logged unexpected errors: %q", action, result.Process.Stderr)
		}
		if plan.Pool == "" {
			plan = result.Plan
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdGroupNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID,
			ElapsedMS: result.ElapsedMS, StderrLines: strings.Count(string(result.Process.Stderr), "\n")})
		return n
	}
	request := func(t *testing.T, action string) {
		t.Helper()
		for _, name := range []string{"rbd-group-" + action + ".json", "rbd-group-" + action + "-state.json"} {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
	native := func(t *testing.T, action string) *nativeRBDGroup {
		t.Helper()
		request(t, action)
		if err := testcluster.RequestControl(ctx, dir, "rbd-group", "rbd-group-completed", token, action); err != nil {
			t.Fatal("native RBD group", action, err)
		}
		return receipt(t, action, nil)
	}
	setup := native(t, "setup")
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.SecondPool != second.Name || plan.BaseImage != oracle.Plan.Image || plan.Group != "ceph-msgr-group-"+token ||
		plan.GroupSnapshot != "ceph-msgr-gsnap-"+token[:8] || plan.RefusedSnapshot != "ceph-msgr-gsnap-refused-"+token[:8] || plan.SecondSnapshot != "ceph-msgr-gsnap-second-"+token[:8] ||
		plan.TimedOutSnapshot != "ceph-msgr-gsnap-timedout-"+token[:8] || plan.RetriedSnapshot != "ceph-msgr-gsnap-retried-"+token[:8] || plan.OverLimitSnapshot != "ceph-msgr-gsnap-overlimit-"+token[:8] ||
		plan.Order != 16 || plan.Size != 192*k || plan.Features != 1|4|8|16 || plan.ListPage != 1 || plan.PollPage != 16 || plan.QuiesceRefusal != rbdGroupQuiesceRefusal ||
		plan.QuiesceAttempts != rbdGroupQuiesceAttempts || len(plan.Images) != 2 ||
		plan.Images[0].Key != "a" || plan.Images[0].Name != "ceph-msgr-gimage-a-"+token || plan.Images[0].Pool != plan.Pool ||
		plan.Images[1].Key != "b" || plan.Images[1].Name != "ceph-msgr-gimage-b-"+token || plan.Images[1].Pool != plan.SecondPool {
		t.Fatal("RBD group plan differs", plan)
	}
	objectSize := uint64(1) << plan.Order
	objectCount := plan.Size >> plan.Order
	imagePool := map[string]int64{"a": poolID, "b": secondID}
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
	wantListed := []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.Images[0].Name, plan.SecondPool + "/" + plan.Images[1].Name}
	slices.Sort(wantListed)
	if !slices.Equal(setup.ImagesListed, wantListed) || !slices.Equal(setup.GroupsListed, []string{plan.Group}) || setup.Group == nil || len(setup.Images) != 2 {
		t.Fatal("native RBD group setup listing differs", setup.ImagesListed, setup.GroupsListed)
	}
	group := setup.Group
	evidence.GroupID = group.ID
	imageIDs := map[string]string{}
	imageKeys := map[string]string{}
	for i, image := range setup.Images {
		if image.Key != plan.Images[i].Key || image.Name != plan.Images[i].Name || image.Pool != plan.Images[i].Pool || image.PoolID != imagePool[image.Key] ||
			image.Stat.Size != plan.Size || image.Stat.Order != plan.Order || image.Stat.Objects != objectCount || image.Stat.Prefix != "rbd_data."+image.ImageID ||
			image.Features != plan.Features|rbdFeatureOperations || image.OpFeatures != rbdOperationFeatureGroup || image.HeaderObject != "rbd_header."+image.ImageID {
			t.Fatal("native RBD group member image differs", image.Key, image.Stat)
		}
		imageIDs[image.Key], imageKeys[image.ImageID] = image.ImageID, image.Key
	}
	if len(imageKeys) != 2 {
		t.Fatal("native RBD group member image IDs are not distinct", imageIDs)
	}
	members := slices.Clone(group.Members)
	slices.SortFunc(members, func(a, b nativeRBDGroupMember) int { return strings.Compare(a.Name, b.Name) })
	if !reflect.DeepEqual(members, []nativeRBDGroupMember{{plan.Images[0].Name, poolID, 0}, {plan.Images[1].Name, secondID, 0}}) {
		t.Fatal("native RBD group membership differs (want both images attached, b in the second pool)", group.Members)
	}

	addresses := osdDumpAddresses(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	// route places one object of an owned pool; pool is the ID the caller
	// read for this object (a member tuple's pool), which must be owned.
	route := func(t *testing.T, pool int64, object string) (rbdRoute, *cephmsgr.OSDConnection) {
		t.Helper()
		name, owned := poolNames[pool]
		if !owned {
			t.Fatal("RBD group object is outside the owned pools", pool, object)
		}
		p, err := recoveryObjectPlacement(ctx, c, name, object)
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
	osdRequest := func(r rbdRoute, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		return cephmsgr.OSDRequest{MapEpoch: r.Epoch, Object: cephmsgr.OSDObject{Pool: r.Pool, Name: r.Object, Hash: r.Hash}, Operations: ops}
	}
	var evidenceMu sync.Mutex
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
		evidenceMu.Lock()
		evidence.Requests = append(evidence.Requests, e)
		evidenceMu.Unlock()
	}
	checkReply := func(t *testing.T, r rbdRoute, reply cephmsgr.OSDReply, code int32) {
		t.Helper()
		if reply.Code != code || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.GlobalID != gid || reply.Submission.MapEpoch != r.Epoch ||
			reply.Object != r.Object || reply.PG.Pool != uint64(r.Pool) || reply.PG.Seed != r.Hash || reply.MapEpoch < r.Epoch || reply.RetryAttempt != 0 {
			t.Fatalf("OSD reply identity differs: code=%d tid=%d gid=%d pg=%+v epoch=%d", reply.Code, reply.Transaction, reply.GlobalID, reply.PG, reply.MapEpoch)
		}
	}
	// call sends one read-mode CALL alone in its request and requires the
	// given result; it returns the class output of a success.
	call := func(t *testing.T, label string, pool int64, object, class, method string, input []byte, code int32) []byte {
		t.Helper()
		r, o := route(t, pool, object)
		req := osdRequest(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: cephmsgr.OSDCallRead, MaxOutputBytes: 8192}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if code != 0 {
			var server *cephmsgr.OSDError
			if !errors.As(err, &server) || server.Code != code {
				t.Fatalf("Go CALL %s.%s (%s): want server result %d, got %v", class, method, label, code, err)
			}
			checkReply(t, r, reply, code)
			return nil
		}
		if err != nil {
			t.Fatalf("Go CALL %s.%s (%s): %v", class, method, label, err)
		}
		checkReply(t, r, reply, 0)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != uint8(len(class)) || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) || got.Class.OutputLength != uint64(len(got.Data)) {
			t.Fatalf("CALL %s.%s per-op result or union echo differs: %+v", class, method, got.Class)
		}
		evidenceMu.Lock()
		evidence.ClassOutputs[label] = hex.EncodeToString(got.Data)
		evidenceMu.Unlock()
		return got.Data
	}
	sameCall := func(t *testing.T, label string, n nativeRBDGroupCall, object, class, method string, input, output []byte) {
		t.Helper()
		if n.Object != object || n.Class != class || n.Method != method || n.InputHex != hex.EncodeToString(input) || n.Code != 0 || n.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("%s: Go CALL %s.%s differs from native librados exec: native=%+v go_input=%x go_output=%x", label, class, method, n, input, output)
		}
	}
	header := "rbd_group_header." + group.ID
	pollInput := rbdGroupSnapListInput(rbdGroupSnapRecord{}, plan.PollPage)
	memberHeader := func(key string) string { return "rbd_header." + imageIDs[key] }
	// listRecords decodes a single-call group_snap_list output by name.
	listRecords := func(t *testing.T, out []byte) map[string]rbdGroupSnapRecord {
		t.Helper()
		records, err := decodeRBDGroupSnapList(out)
		byName := map[string]rbdGroupSnapRecord{}
		for _, r := range records {
			byName[r.Name] = r
		}
		if err != nil || len(byName) != len(records) {
			t.Fatal("group_snap_list output", records, err)
		}
		return byName
	}
	// memberSpecs checks a record's member tuples against the two members,
	// in either order, and returns them by image key.
	memberSpecs := func(t *testing.T, r rbdGroupSnapRecord) map[string]rbdGroupImageSnapSpec {
		t.Helper()
		specs := map[string]rbdGroupImageSnapSpec{}
		for _, s := range r.Snaps {
			key := imageKeys[s.ImageID]
			if key == "" || s.Pool != imagePool[key] {
				t.Fatal("group record member tuple names another image or pool", r.Name, s)
			}
			specs[key] = s
		}
		if len(specs) != 2 || len(r.Snaps) != 2 {
			t.Fatal("group record does not hold both members once", r)
		}
		return specs
	}
	snapContext := func(t *testing.T, out []byte) []uint64 {
		t.Helper()
		snapc, err := decodeRBDSnapContext(out)
		if err != nil {
			t.Fatal("get_snapcontext output", err)
		}
		return snapc.Snapshots
	}
	lockInfo := func(t *testing.T, out []byte) rbdLockInfo {
		t.Helper()
		info, err := decodeRBDLockInfo(out)
		if err != nil {
			t.Fatal("lock.get_info output", err)
		}
		return info
	}

	// observeGroup reads the group directory, every group_snap_list page,
	// each member snapshot and both member headers, compares them with the
	// native observation n and returns the group records by name. want lists
	// the group snapshot names the group must hold.
	observeGroup := func(t *testing.T, phase string, n *nativeRBDGroup, want []string) map[string]rbdGroupSnapRecord {
		t.Helper()
		g := n.Group
		if g == nil || g.ID != group.ID || g.HeaderObject != header || len(g.Snapshots) != len(want) || len(g.ListCalls) != len(want)+1 || !reflect.DeepEqual(g.Snapshots, g.SnapshotInfos) {
			t.Fatal("native group observation differs", phase, g)
		}
		nameInput := rbdGroupString(nil, plan.Group)
		out := call(t, phase+"_dir_get_id", poolID, "rbd_group_directory", "rbd", "dir_get_id", nameInput, 0)
		sameCall(t, phase+" dir_get_id", g.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id", nameInput, out)
		d := &rbdDecoder{data: out}
		if groupID := d.str(); d.done() != nil || groupID != group.ID {
			t.Fatal("Go group directory lookup differs from native librbd's group ID", groupID)
		}
		records := map[string]rbdGroupSnapRecord{}
		start := rbdGroupSnapRecord{}
		for i := range g.ListCalls {
			input := rbdGroupSnapListInput(start, plan.ListPage)
			out := call(t, fmt.Sprintf("%s_group_snap_list_%d", phase, i+1), poolID, header, "rbd", "group_snap_list", input, 0)
			sameCall(t, fmt.Sprintf("%s group_snap_list page %d", phase, i+1), g.ListCalls[i], header, "rbd", "group_snap_list", input, out)
			page, err := decodeRBDGroupSnapList(out)
			if i == len(g.ListCalls)-1 {
				if err != nil || len(page) != 0 {
					t.Fatal("group_snap_list after the last record must be empty", page, err)
				}
				break
			}
			if err != nil || len(page) != 1 {
				t.Fatal("group_snap_list page must hold exactly one record", i, page, err)
			}
			// The next page starts after the returned record, re-encoded
			// here: the bytes must equal the server's own encoding.
			if !bytes.Equal(encodeRBDGroupSnapRecord(page[0]), out[4:]) {
				t.Fatal("test-only GroupSnapshot re-encoding differs from the OSD class output")
			}
			start, records[page[0].Name] = page[0], page[0]
		}
		if len(records) != len(want) {
			t.Fatal("group_snap_list pages hold a repeated record", records)
		}
		snapshotIDs := map[string][]uint64{}
		for _, ns := range g.Snapshots {
			rec, ok := records[ns.Name]
			if !ok || !slices.Contains(want, ns.Name) || rec.ID != ns.ID || rec.State != rbdGroupSnapStateComplete || ns.State != 1 || ns.NamespaceType != 0 ||
				len(ns.ImageSnaps) != 2 || ns.ImageSnapName != rbdGroupImageSnapName(poolID, group.ID, rec.ID) {
				t.Fatal("Go group record differs from native librbd's group snapshot", phase, rec, ns)
			}
			specs := memberSpecs(t, rec)
			for _, nativeSnap := range ns.ImageSnaps {
				key := slices.IndexFunc(plan.Images, func(i nativeRBDGroupImagePlan) bool { return i.Name == nativeSnap.ImageName })
				if key < 0 {
					t.Fatal("native group snapshot names another image", nativeSnap)
				}
				spec := specs[plan.Images[key].Key]
				if spec.Pool != nativeSnap.PoolID || spec.SnapID != nativeSnap.SnapID || spec.SnapID == 0 {
					t.Fatal("Go group member tuple differs from native librbd", phase, spec, nativeSnap)
				}
			}
			// Each member snapshot on its own image header, in its pool;
			// the namespace names the group pool for both.
			for _, image := range n.Images {
				spec := specs[image.Key]
				j := slices.IndexFunc(image.Snapshots, func(s nativeRBDGroupImageSnapshot) bool { return s.ID == spec.SnapID })
				if j < 0 {
					t.Fatal("native observation lacks the member snapshot", image.Key, spec)
				}
				input := rbdLE64(spec.SnapID)
				label := phase + "_snapshot_get_" + image.Key + "_" + ns.Name
				out := call(t, label, spec.Pool, image.HeaderObject, "rbd", "snapshot_get", input, 0)
				sameCall(t, label, image.SnapshotCalls[j], image.HeaderObject, "rbd", "snapshot_get", input, out)
				info, err := decodeRBDGroupSnapshotInfo(out)
				snap := image.Snapshots[j]
				if err != nil || info.ID != spec.SnapID || info.GroupPool != poolID || info.GroupID != group.ID || info.GroupSnapshotID != rec.ID ||
					info.Name != ns.ImageSnapName || info.ImageSize != plan.Size || info.ChildCount != 0 || info.Seconds == 0 {
					t.Fatal("Go member snapshot differs from the group record", image.Key, info, err)
				}
				if snap.Name != info.Name || snap.Size != info.ImageSize || snap.Namespace != rbdSnapshotNamespaceTypeGroup || snap.Group == nil ||
					snap.Group.Pool != poolID || snap.Group.Name != plan.Group || snap.Group.SnapName != rec.Name {
					t.Fatal("Go member snapshot differs from native librbd's snapshot and group namespace", image.Key, info, snap)
				}
				snapshotIDs[image.Key] = append(snapshotIDs[image.Key], spec.SnapID)
			}
		}
		for i, image := range n.Images {
			if image.Key != plan.Images[i].Key || image.ImageID != imageIDs[image.Key] || image.PoolID != imagePool[image.Key] || len(image.Snapshots) != len(want) {
				t.Fatal("native member observation differs", phase, image.Key, image.Snapshots)
			}
			out := call(t, phase+"_get_snapcontext_"+image.Key, image.PoolID, image.HeaderObject, "rbd", "get_snapcontext", nil, 0)
			sameCall(t, phase+" get_snapcontext "+image.Key, image.SnapContextCall, image.HeaderObject, "rbd", "get_snapcontext", nil, out)
			got := snapContext(t, out)
			wantIDs := slices.Clone(snapshotIDs[image.Key])
			slices.Sort(wantIDs)
			slices.Reverse(wantIDs)
			if !slices.Equal(got, wantIDs) {
				t.Fatal("member snap context differs from the group records", image.Key, got, wantIDs)
			}
			out = call(t, phase+"_lock_get_info_"+image.Key, image.PoolID, image.HeaderObject, "lock", "get_info", rbdLockGetInfoInput(), 0)
			sameCall(t, phase+" lock.get_info "+image.Key, image.LockCall, image.HeaderObject, "lock", "get_info", rbdLockGetInfoInput(), out)
			if info := lockInfo(t, out); len(info.Lockers) != 0 {
				t.Fatal("a member exclusive lock is held after the native action returned", phase, image.Key, info)
			}
		}
		return records
	}

	// checkData reads every data object of both members at the head and at
	// every snapshot and compares it with the native views; want gives each
	// view's planned phases by label ("head" or a group snapshot name).
	checkData := func(t *testing.T, phase string, n *nativeRBDGroup, records map[string]rbdGroupSnapRecord, want map[string][]string) {
		t.Helper()
		for i, image := range n.Images {
			labels := map[string]string{"head": "head"}
			for name, rec := range records {
				labels[fmt.Sprint("s", memberSpecs(t, rec)[image.Key].SnapID)] = name
			}
			if len(image.Views) != len(labels) {
				t.Fatal("native member views differ from the group records", image.Key, len(image.Views))
			}
			for _, view := range image.Views {
				name, ok := labels[view.Label]
				if !ok || len(view.Objects) != int(objectCount) {
					t.Fatal("native member view is not a planned one", image.Key, view.Label)
				}
				imageBytes, blob := readRBDBlob(t, dir, view.Image), readRBDBlob(t, dir, view.ObjectBytes)
				if !bytes.Equal(imageBytes, expected(plan.Images[i], want[name]...)) {
					t.Fatalf("%s: native member %s %s (%s) differs from the planned %v contents", phase, image.Key, view.Label, name, want[name])
				}
				for j, object := range view.Objects {
					objectName := fmt.Sprintf("%s.%016x", image.Stat.Prefix, j)
					r, o := route(t, image.PoolID, objectName)
					req := osdRequest(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
					req.ReadSnapshot = view.SnapID
					reply, err := o.Request(ctx, req)
					record(fmt.Sprintf("%s_read_%s_%s_%d", phase, image.Key, view.Label, j), r, req, reply, err)
					extent := imageBytes[uint64(j)*objectSize : uint64(j+1)*objectSize]
					if object.Index != uint64(j) {
						t.Fatal("native object view order differs", image.Key, view.Label, j)
					}
					if !object.Exists {
						var server *cephmsgr.OSDError
						if !errors.As(err, &server) || server.Code != -2 {
							t.Fatalf("%s %s object %d: native librados reports ENOENT, Go %v", image.Key, view.Label, j, err)
						}
						checkReply(t, r, reply, -2)
						if !bytes.Equal(extent, make([]byte, objectSize)) {
							t.Fatalf("%s %s object %d is absent but native librbd reads non-zero bytes", image.Key, view.Label, j)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%s %s object %d: %v", image.Key, view.Label, j, err)
					}
					checkReply(t, r, reply, 0)
					stat, data := reply.Operations[0], reply.Operations[1].Data
					if stat.Result != 0 || len(stat.Data) != 16 || binary.LittleEndian.Uint64(stat.Data) != object.Size || uint64(len(data)) != object.Size || reply.Operations[1].Result != 0 {
						t.Fatalf("%s %s object %d stat/read size differs from native librados: native %d", image.Key, view.Label, j, object.Size)
					}
					if !bytes.Equal(data, blob[object.Offset:object.Offset+object.Size]) {
						t.Fatalf("%s %s object %d bytes differ from native librados", image.Key, view.Label, j)
					}
					if !bytes.Equal(append(bytes.Clone(data), make([]byte, objectSize-uint64(len(data)))...), extent) {
						t.Fatalf("%s %s object %d bytes differ from native librbd image extent", image.Key, view.Label, j)
					}
				}
			}
		}
	}

	var first rbdGroupSnapRecord
	t.Run("group_snapshot_record", func(t *testing.T) {
		records := observeGroup(t, "setup", setup, []string{plan.GroupSnapshot})
		first = records[plan.GroupSnapshot]
		evidence.Records["first"], evidence.Snapshots["first"] = first, first.ID
		checkData(t, "setup", setup, records, map[string][]string{"head": {"pre"}, plan.GroupSnapshot: {"pre"}})
		t.Logf("group %s snapshot %s: COMPLETE with members in pools %d and %d, each snapshot in the group namespace of pool %d", group.ID, first.ID, poolID, secondID, poolID)
	})
	if t.Failed() {
		return
	}

	modify := native(t, "modify")
	checkWrites(t, modify, "post")
	if !slices.Equal(modify.ImagesListed, wantListed) || !slices.Equal(modify.GroupsListed, []string{plan.Group}) || len(modify.Images) != 2 {
		t.Fatal("native RBD group listing after the head writes differs")
	}
	t.Run("record_after_head_writes", func(t *testing.T) {
		records := observeGroup(t, "modify", modify, []string{plan.GroupSnapshot})
		if !reflect.DeepEqual(records[plan.GroupSnapshot], first) || !reflect.DeepEqual(modify.Group.ListCalls, group.ListCalls) {
			t.Fatal("group record changed with the member heads")
		}
		for i, image := range modify.Images {
			if !reflect.DeepEqual(image.Snapshots, setup.Images[i].Snapshots) || !reflect.DeepEqual(image.SnapshotCalls, setup.Images[i].SnapshotCalls) {
				t.Fatal("native member snapshot changed with its head", image.Key)
			}
		}
		checkData(t, "modify", modify, records, map[string][]string{"head": {"pre", "post"}, plan.GroupSnapshot: {"pre"}})
	})
	if t.Failed() {
		return
	}

	// The Go watch on member b's header, in the second pool, for both
	// creations.
	wr, wo := route(t, secondID, memberHeader("b"))
	watchRequest := osdRequest(wr)
	watchRequest.Operations = nil
	w, reply, err := wo.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	record("member_watch_register", wr, watchRequest, reply, err)
	if err != nil {
		t.Fatal("Go member header watch", err)
	}
	evidence.GoWatchCookie = w.Cookie()
	responder = newRBDGroupResponder()
	respondCtx, stopResponding := context.WithCancel(ctx)
	responderDone := make(chan struct{})
	responderStarted := time.Now()
	go func() {
		defer close(responderDone)
		lastPing := time.Now()
		for {
			if time.Since(lastPing) >= rbdLockPingInterval {
				_, err := w.Ping(respondCtx, wr.Epoch)
				if respondCtx.Err() != nil {
					return
				}
				lastPing = time.Now()
				responder.mu.Lock()
				responder.pings++
				if err != nil {
					responder.pingErrors = append(responder.pingErrors, err.Error())
				}
				responder.mu.Unlock()
			}
			wait, stop := context.WithDeadline(respondCtx, lastPing.Add(rbdLockPingInterval))
			event, err := w.Next(wait)
			stop()
			if respondCtx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if err != nil {
				responder.fail(fmt.Errorf("member header watch ended: %w", err))
				return
			}
			responder.handle(respondCtx, w, wr.Epoch, responderStarted, event)
		}
	}()
	watching := true
	unwatch := func(t *testing.T) {
		stopResponding()
		<-responderDone
		// A notification after the last settled creation is judged too.
		responder.mu.Lock()
		failure := responder.failure
		responder.mu.Unlock()
		if failure != nil {
			t.Error("member header watch responder", failure)
		}
		if watching {
			watching = false
			req := osdRequest(wr)
			req.Operations = nil
			reply, err := w.Unwatch(ctx, wr.Epoch)
			record("member_watch_unwatch", wr, req, reply, err)
			if err != nil {
				t.Error("Go member header unwatch", err)
			}
		}
	}
	defer func() {
		if watching {
			unwatch(t)
		}
	}()

	// creation runs one native creation under the Go watch. Each step is
	// one quiesce or unquiesce request on member b's header, in arrival
	// order: while it holds the request the test reads the group header and
	// both member headers, waits for the named native observation, compares
	// the two and judges the hold, then answers the request or never
	// answers it (omit), which leaves it to time out at the OSD for the
	// notifier. A failed judgment still answers the request.
	type holdView struct {
		list    []byte
		records map[string]rbdGroupSnapRecord
		snapc   map[string][]uint64
		locks   map[string]rbdLockInfo
		limits  map[string]uint64
	}
	type holdStep struct {
		op     uint32
		phase  string // the evidence label
		native string // the native observation to compare with, or ""
		answer []byte
		omit   bool
		check  func(t *testing.T, v holdView, h rbdGroupHeld, state nativeRBDGroupState, p nativeRBDGroupCreationPhase)
	}
	type creationCell struct {
		action, snapshot string
		attempts         int // the creating client's quiesce attempts
		limitA           uint64
		steps            []holdStep
		allowed          []uint32
		stderr           []string
		// locked: the creation acquires both member locks; compensated: it
		// removes a member snapshot after the unquiesce.
		locked, compensated bool
	}
	creation := func(t *testing.T, cell creationCell) *nativeRBDGroup {
		t.Helper()
		action := cell.action
		responder.begin(action, cell.allowed...)
		request(t, action)
		finished := make(chan struct{})
		var controlErr error
		go func() {
			defer close(finished)
			controlErr = testcluster.RequestControl(ctx, dir, "rbd-group", "rbd-group-completed", token, action)
		}()
		defer func() { <-finished }()
		running := func() bool {
			select {
			case <-finished:
				return false
			default:
				return true
			}
		}
		statePath := filepath.Join(dir, "rbd-group-"+action+"-state.json")
		waitHeld := func(t *testing.T, op uint32) rbdGroupHeld {
			t.Helper()
			select {
			case h := <-responder.held:
				if h.Op != op {
					h.Answer <- nil
					t.Fatalf("%s: member header received op %d before op %d", action, h.Op, op)
				}
				return h
			case <-finished:
				t.Fatalf("%s: native creation returned before op %d reached the Go watch: %v", action, op, controlErr)
			case <-time.After(60 * time.Second):
				t.Fatalf("%s: op %d did not reach the Go watch", action, op)
			}
			return rbdGroupHeld{}
		}
		// read takes the hold's view: the group records and both member
		// headers' snap contexts, lock records and snapshot limits.
		read := func(t *testing.T, phase string) holdView {
			t.Helper()
			v := holdView{snapc: map[string][]uint64{}, locks: map[string]rbdLockInfo{}, limits: map[string]uint64{}}
			v.list = call(t, action+"_"+phase+"_group_snap_list", poolID, header, "rbd", "group_snap_list", pollInput, 0)
			v.records = listRecords(t, v.list)
			for _, key := range []string{"a", "b"} {
				v.snapc[key] = snapContext(t, call(t, action+"_"+phase+"_get_snapcontext_"+key, imagePool[key], memberHeader(key), "rbd", "get_snapcontext", nil, 0))
				v.locks[key] = lockInfo(t, call(t, action+"_"+phase+"_lock_get_info_"+key, imagePool[key], memberHeader(key), "lock", "get_info", rbdLockGetInfoInput(), 0))
				out := call(t, action+"_"+phase+"_snapshot_get_limit_"+key, imagePool[key], memberHeader(key), "rbd", "snapshot_get_limit", nil, 0)
				if len(out) != 8 {
					t.Fatal("snapshot_get_limit output", key, out)
				}
				v.limits[key] = binary.LittleEndian.Uint64(out)
			}
			return v
		}
		// sameAsNative compares the hold's reads with the native thread's
		// observation of the same objects with the same inputs.
		sameAsNative := func(t *testing.T, label string, v holdView, p nativeRBDGroupCreationPhase) {
			t.Helper()
			sameCall(t, label+" group_snap_list", p.ListCall, header, "rbd", "group_snap_list", pollInput, v.list)
			if len(p.Images) != 2 {
				t.Fatal("native creation observation lacks a member", label)
			}
			for i, image := range p.Images {
				if image.Key != plan.Images[i].Key {
					t.Fatal("native creation observation member order differs", label, image.Key)
				}
				object := memberHeader(image.Key)
				outputs := func(method string) []byte {
					out, err := hex.DecodeString(evidence.ClassOutputs[action+"_"+label+"_"+method+"_"+image.Key])
					if err != nil {
						t.Fatal(err)
					}
					return out
				}
				sameCall(t, label+" get_snapcontext "+image.Key, image.SnapContextCall, object, "rbd", "get_snapcontext", nil, outputs("get_snapcontext"))
				sameCall(t, label+" lock.get_info "+image.Key, image.LockCall, object, "lock", "get_info", rbdLockGetInfoInput(), outputs("lock_get_info"))
				sameCall(t, label+" snapshot_get_limit "+image.Key, image.LimitCall, object, "rbd", "snapshot_get_limit", nil, outputs("snapshot_get_limit"))
			}
		}
		release := func(h rbdGroupHeld, s holdStep) {
			if s.omit {
				close(h.Answer)
			} else {
				h.Answer <- s.answer
			}
		}
		answerHeld := func(h rbdGroupHeld, s holdStep, v holdView, nativePhase string, runningBefore bool) {
			hold := rbdGroupHoldEvidence{Action: action, Phase: s.phase, NotifyID: h.Event.NotifyID, Request: h.Request, ControlRunning: [2]bool{runningBefore, running()},
				GroupSnapListHex: hex.EncodeToString(v.list), SnapContexts: map[string]string{}, LockInfos: map[string]string{}, SnapLimits: map[string]string{}, NativePhase: nativePhase,
				HeldMS: float64(time.Since(h.Received).Microseconds()) / 1000, AnswerHex: hex.EncodeToString(s.answer), Omitted: s.omit}
			for _, key := range []string{"a", "b"} {
				hold.SnapContexts[key] = fmt.Sprint(v.snapc[key])
				hold.LockInfos[key] = evidence.ClassOutputs[action+"_"+s.phase+"_lock_get_info_"+key]
				hold.SnapLimits[key] = fmt.Sprint(v.limits[key])
			}
			evidence.Holds = append(evidence.Holds, hold)
			release(h, s)
		}

		var initial rbdGroupHeld
		var state nativeRBDGroupState
		var held []rbdGroupExpectedAnswer
		var phases []string
		for i, s := range cell.steps {
			h := waitHeld(t, s.op)
			before := running()
			answered := false
			func() {
				defer func() {
					if !answered {
						release(h, s)
					}
				}()
				v := read(t, s.phase)
				stepState, p := state, nativeRBDGroupCreationPhase{}
				if s.native != "" {
					var err error
					if stepState, p, err = waitRBDGroupState(ctx, statePath, s.native, finished, &controlErr); err != nil {
						t.Fatal(err)
					}
					phases = append(phases, s.native)
				}
				if i == 0 {
					state, initial = stepState, h
					if s.native != "incomplete-observed" || state.Token != token || state.Snapshot != cell.snapshot || state.GroupID != group.ID || state.NativeGlobalID == 0 ||
						state.NativeGlobalID == gid || !reflect.DeepEqual(state.ImageIDs, imageIDs) || h.Request.Client.GID != state.NativeGlobalID ||
						h.Event.NotifierGlobalID != state.NativeGlobalID || h.Request.Client.Handle == 0 {
						t.Fatal("quiesce request does not come from the native creating client", action, state.NativeGlobalID, h.Request, h.Event.NotifierGlobalID)
					}
				} else if h.Request != initial.Request || h.Event.NotifierGlobalID != state.NativeGlobalID ||
					slices.ContainsFunc(held, func(e rbdGroupExpectedAnswer) bool { return e.NotifyID == h.Event.NotifyID }) {
					// A retried quiesce and the unquiesce carry the first
					// quiesce's request ID in a new notification.
					t.Fatal("request differs from its first quiesce", action, s.phase, h.Request, initial.Request, h.Event.NotifyID)
				}
				if s.native != "" {
					sameAsNative(t, s.phase, v, p)
				}
				if s.native == "incomplete-observed" && (p.RollbackErrno == nil || *p.RollbackErrno != 22) {
					t.Fatal("native librbd did not refuse a rollback to the INCOMPLETE group snapshot with EINVAL", action, p.RollbackErrno)
				}
				if v.limits["a"] != cell.limitA || v.limits["b"] != rbdGroupNoSnapLimit {
					t.Fatal("member snapshot limits differ during the creation", action, s.phase, v.limits)
				}
				s.check(t, v, h, stepState, p)
				// An answer must reach the OSD before its notify times
				// out; an omitted one never does.
				if !before || !running() || !s.omit && time.Since(h.Received) >= rbdNotifyTimeout {
					t.Fatalf("%s: the %s hold is unproven (control running %v, held %v)", action, s.phase, before, time.Since(h.Received))
				}
				answered = true
				answerHeld(h, s, v, p.Phase, before)
			}()
			held = append(held, rbdGroupExpectedAnswer{NotifyID: h.Event.NotifyID, Op: s.op, Ack: s.answer, Omitted: s.omit})
		}
		select {
		case <-finished:
		case <-time.After(90 * time.Second):
			t.Fatal("native creation did not return after its held requests", action)
		}
		if controlErr != nil {
			t.Fatal("native RBD group", action, controlErr)
		}
		n := receipt(t, action, cell.stderr)
		// The native return does not order the Go watcher's recording of
		// the acknowledgements librbd waited for.
		if _, err := settleRBDGroupCreation(ctx, responder, action, held, state.NativeGlobalID, cell.locked, cell.compensated); err != nil {
			t.Fatal(err)
		}
		cr := n.Creation
		final, _, err := waitRBDGroupState(ctx, statePath, phases[len(phases)-1], finished, &controlErr)
		if err != nil || cr == nil || cr.Snapshot != cell.snapshot || cr.GroupID != group.ID || !reflect.DeepEqual(cr.ImageIDs, imageIDs) || final.Phase != phases[len(phases)-1] ||
			!reflect.DeepEqual(cr.Phases, final.Phases) || len(cr.Phases) != len(phases) {
			t.Fatal("native creation record differs from its published state", action, cr, err)
		}
		for i, p := range cr.Phases {
			if p.Phase != phases[i] {
				t.Fatal("native creation observations differ from the held steps", action, i, p.Phase)
			}
		}
		if cr.QuiesceAttempts != cell.attempts {
			t.Fatal("native creating client's quiesce attempts differ", action, cr.QuiesceAttempts, cell.attempts)
		}
		evidence.CreationErrnos[action] = cr.Errno
		return n
	}

	// snapIDs is each member's snapshot IDs from the given group records,
	// newest first (get_snapcontext's order).
	snapIDs := func(t *testing.T, records []rbdGroupSnapRecord) map[string][]uint64 {
		t.Helper()
		ids := map[string][]uint64{}
		for _, rec := range records {
			for key, spec := range memberSpecs(t, rec) {
				ids[key] = append(ids[key], spec.SnapID)
			}
		}
		for key := range ids {
			slices.Sort(ids[key])
			slices.Reverse(ids[key])
		}
		return ids
	}
	// pendingAt checks a hold at which the created record is INCOMPLETE,
	// with zero member snapshot IDs, beside the prior records. The record
	// must stay the same through the creation: *record keeps it.
	pendingAt := func(t *testing.T, cell, snapshot string, v holdView, prior []rbdGroupSnapRecord, record *rbdGroupSnapRecord) {
		t.Helper()
		created, ok := v.records[snapshot]
		if len(v.records) != len(prior)+1 || !ok || created.State != rbdGroupSnapStateIncomplete || created.ID == "" {
			t.Fatal("the group must hold the prior records and the INCOMPLETE record", cell, v.records)
		}
		for _, rec := range prior {
			if !reflect.DeepEqual(v.records[rec.Name], rec) || rec.ID == created.ID {
				t.Fatal("a prior group record changed during the creation", cell, rec.Name)
			}
		}
		for key, spec := range memberSpecs(t, created) {
			if spec.SnapID != 0 {
				t.Fatal("INCOMPLETE record names a member snapshot", cell, key, spec)
			}
		}
		if record.ID != "" && !reflect.DeepEqual(created, *record) {
			t.Fatal("INCOMPLETE record changed between holds", cell, created, *record)
		}
		*record = created
	}
	// untouchedAt checks that no member holds a snapshot beyond the prior
	// records or a lock.
	untouchedAt := func(t *testing.T, cell string, v holdView, prior []rbdGroupSnapRecord) {
		t.Helper()
		want := snapIDs(t, prior)
		for _, key := range []string{"a", "b"} {
			if !slices.Equal(v.snapc[key], want[key]) || len(v.locks[key].Lockers) != 0 {
				t.Fatal("a member holds a new snapshot or a lock", cell, key, v.snapc[key], v.locks[key])
			}
		}
	}
	// lockedAt checks that native librbd's creating client holds both
	// member exclusive locks: an "auto <handle>" cookie, exclusive with the
	// internal tag. The quiesce came from member b's image watcher, so its
	// ClientId handle is member b's lock cookie.
	lockedAt := func(t *testing.T, cell string, v holdView, h rbdGroupHeld, state nativeRBDGroupState) {
		t.Helper()
		for _, key := range []string{"a", "b"} {
			info := v.locks[key]
			if len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != rbdLockTag {
				t.Fatal("a member exclusive lock is not held", cell, key, info)
			}
			l := info.Lockers[0]
			handle, cookie := rbdLockHandle(l.Cookie)
			if l.NameType != rbdEntityClient || uint64(l.NameNumber) != state.NativeGlobalID || !cookie || l.Address.Endpoint.Addr().IsUnspecified() {
				t.Fatal("member exclusive lock is not native librbd's", cell, key, l)
			}
			if key == "b" && handle != h.Request.Client.Handle {
				t.Fatal("member b's lock cookie differs from its quiesce requester", cell, l.Cookie, h.Request)
			}
		}
	}
	// incomplete is the check of a hold at which the creation has taken no
	// member snapshot or lock.
	incomplete := func(cell, snapshot string, prior []rbdGroupSnapRecord, record *rbdGroupSnapRecord) func(*testing.T, holdView, rbdGroupHeld, nativeRBDGroupState, nativeRBDGroupCreationPhase) {
		return func(t *testing.T, v holdView, _ rbdGroupHeld, _ nativeRBDGroupState, _ nativeRBDGroupCreationPhase) {
			pendingAt(t, cell, snapshot, v, prior, record)
			untouchedAt(t, cell, v, prior)
		}
	}
	// complete is the check of an accepted creation's unquiesce: the same
	// record, now COMPLETE, a new snapshot on both members and both locks.
	complete := func(cell, snapshot string, prior []rbdGroupSnapRecord, record *rbdGroupSnapRecord) func(*testing.T, holdView, rbdGroupHeld, nativeRBDGroupState, nativeRBDGroupCreationPhase) {
		return func(t *testing.T, v holdView, h rbdGroupHeld, state nativeRBDGroupState, _ nativeRBDGroupCreationPhase) {
			rec, ok := v.records[snapshot]
			if len(v.records) != len(prior)+1 || !ok || rec.State != rbdGroupSnapStateComplete || rec.ID != record.ID {
				t.Fatal("at the accepted unquiesce the created record must be COMPLETE", cell, v.records)
			}
			for _, p := range prior {
				if !reflect.DeepEqual(v.records[p.Name], p) {
					t.Fatal("a prior group record changed during the creation", cell, p.Name)
				}
			}
			*record = rec
			specs, before := memberSpecs(t, rec), snapIDs(t, prior)
			for _, key := range []string{"a", "b"} {
				want := append([]uint64{specs[key].SnapID}, before[key]...)
				if specs[key].SnapID <= slices.Max(before[key]) || !slices.Equal(v.snapc[key], want) {
					t.Fatal("at the accepted unquiesce a member lacks its new snapshot", cell, key, v.snapc[key], want)
				}
			}
			lockedAt(t, cell, v, h, state)
		}
	}
	// after checks the Go and native views once a creation returned: the
	// group holds exactly want and the member snapshots of want.
	after := func(t *testing.T, cell string, n *nativeRBDGroup, want []rbdGroupSnapRecord) map[string]rbdGroupSnapRecord {
		t.Helper()
		var names []string
		for _, rec := range want {
			names = append(names, rec.Name)
		}
		records := observeGroup(t, cell, n, names)
		for _, rec := range want {
			if !reflect.DeepEqual(records[rec.Name], rec) {
				t.Fatal("a group record differs after the creation", cell, rec.Name)
			}
		}
		out := call(t, cell+"_after_group_snap_list", poolID, header, "rbd", "group_snap_list", pollInput, 0)
		if listed := listRecords(t, out); len(listed) != len(want) || !reflect.DeepEqual(listed, records) {
			t.Fatal("group records after the creation differ", cell, listed)
		}
		return records
	}
	headPhases := map[string][]string{"head": {"pre", "post"}, plan.GroupSnapshot: {"pre"}}
	views := func(extra ...string) map[string][]string {
		want := maps.Clone(headPhases)
		for _, name := range extra {
			want[name] = []string{"pre", "post"}
		}
		return want
	}
	quiesceOps := []uint32{rbdNotifyQuiesce, rbdNotifyUnquiesce}
	lockOps := []uint32{rbdNotifyQuiesce, rbdNotifyUnquiesce, rbdNotifyAcquiredLock, rbdNotifyReleasedLock, rbdNotifyHeaderUpdate}
	refusal, acceptance := rbdResponseMessage(rbdGroupQuiesceRefusal), rbdResponseMessage(0)
	notComplete := "group snapshot is not complete"

	// The refused creation: Go answers the quiesce with -EBUSY. librbd
	// unquiesces before it removes the INCOMPLETE record, takes no lock and
	// creates no member snapshot.
	var refusedRecord rbdGroupSnapRecord
	t.Run("refused_quiesce_compensation", func(t *testing.T) {
		prior := []rbdGroupSnapRecord{first}
		cell := "refused"
		n := creation(t, creationCell{action: cell, snapshot: plan.RefusedSnapshot, attempts: rbdGroupDefaultQuiesceAttempts, limitA: rbdGroupNoSnapLimit,
			allowed: quiesceOps, stderr: []string{"failed to notify quiesce", notComplete}, steps: []holdStep{
				{op: rbdNotifyQuiesce, phase: "quiesce", native: "incomplete-observed", answer: refusal, check: incomplete(cell, plan.RefusedSnapshot, prior, &refusedRecord)},
				// Still INCOMPLETE at the unquiesce: the compensation
				// removes the record only after the unquiesce returns.
				{op: rbdNotifyUnquiesce, phase: "unquiesce", check: incomplete(cell, plan.RefusedSnapshot, prior, &refusedRecord)},
			}})
		evidence.Records["refused"], evidence.Snapshots["refused"] = refusedRecord, refusedRecord.ID
		if n.Creation.Errno != -rbdGroupQuiesceRefusal {
			t.Fatal("refused group snapshot creation must return the quiesce answer's EBUSY", n.Creation.Errno)
		}
		records := after(t, cell, n, prior)
		for i, image := range n.Images {
			if !reflect.DeepEqual(image.Snapshots, modify.Images[i].Snapshots) {
				t.Fatal("refused creation left a member snapshot", image.Key, image.Snapshots)
			}
		}
		checkData(t, cell, n, records, headPhases)
		t.Logf("refused group snapshot %s: INCOMPLETE at quiesce and unquiesce, EBUSY returned, record removed", refusedRecord.ID)
	})
	if t.Failed() {
		return
	}

	// The accepted creation: Go answers the quiesce with 0. librbd acquires
	// both exclusive locks, snapshots both members and writes COMPLETE
	// before the unquiesce, and releases the locks when it closes.
	var secondRecord rbdGroupSnapRecord
	var accepted *nativeRBDGroup
	t.Run("accepted_quiesce_ordering", func(t *testing.T) {
		prior := []rbdGroupSnapRecord{first}
		cell := "accepted"
		n := creation(t, creationCell{action: cell, snapshot: plan.SecondSnapshot, attempts: rbdGroupDefaultQuiesceAttempts, limitA: rbdGroupNoSnapLimit,
			allowed: lockOps, stderr: []string{notComplete}, locked: true, steps: []holdStep{
				{op: rbdNotifyQuiesce, phase: "quiesce", native: "incomplete-observed", answer: acceptance, check: incomplete(cell, plan.SecondSnapshot, prior, &secondRecord)},
				{op: rbdNotifyUnquiesce, phase: "unquiesce", native: "complete-observed", check: complete(cell, plan.SecondSnapshot, prior, &secondRecord)},
			}})
		if n.Creation.Errno != 0 {
			t.Fatal("accepted group snapshot creation failed", n.Creation.Errno)
		}
		evidence.Records["second"], evidence.Snapshots["second"] = secondRecord, secondRecord.ID
		records := after(t, cell, n, []rbdGroupSnapRecord{first, secondRecord})
		checkData(t, cell, n, records, views(plan.SecondSnapshot))
		accepted = n
		t.Logf("accepted group snapshot %s: INCOMPLETE at the quiesce, COMPLETE with both locks held at the unquiesce, locks released after", secondRecord.ID)
	})
	if t.Failed() {
		return
	}

	// The timed-out creation: the creating client makes plan.QuiesceAttempts
	// quiesce attempts and Go answers none. Each attempt's notify times out
	// at the OSD after librbd's 5 s notify timeout, librbd retries with the
	// same request ID while attempts remain, then fails the quiesce with
	// ETIMEDOUT, unquiesces and removes the INCOMPLETE record without a lock
	// or member snapshot (ceph/ceph/blob/v20.2.4/src/librbd/ImageWatcher.cc
	// notify_quiesce, ceph/ceph/blob/v20.2.4/src/librbd/watcher/Notifier.cc,
	// ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc snap_create).
	t.Run("timed_out_quiesce_compensation", func(t *testing.T) {
		prior := []rbdGroupSnapRecord{first, secondRecord}
		cell := "timed-out"
		var record rbdGroupSnapRecord
		steps := []holdStep{{op: rbdNotifyQuiesce, phase: "quiesce", native: "incomplete-observed", omit: true, check: incomplete(cell, plan.TimedOutSnapshot, prior, &record)}}
		for i := 2; i <= plan.QuiesceAttempts; i++ {
			steps = append(steps, holdStep{op: rbdNotifyQuiesce, phase: fmt.Sprint("quiesce_attempt_", i), omit: true, check: incomplete(cell, plan.TimedOutSnapshot, prior, &record)})
		}
		steps = append(steps, holdStep{op: rbdNotifyUnquiesce, phase: "unquiesce", check: incomplete(cell, plan.TimedOutSnapshot, prior, &record)})
		n := creation(t, creationCell{action: cell, snapshot: plan.TimedOutSnapshot, attempts: plan.QuiesceAttempts, limitA: rbdGroupNoSnapLimit,
			allowed: quiesceOps, stderr: []string{"failed to notify quiesce: (110)", notComplete}, steps: steps})
		evidence.Records["timed-out"], evidence.Snapshots["timed-out"] = record, record.ID
		if n.Creation.Errno != rbdGroupQuiesceTimedOut {
			t.Fatal("timed-out group snapshot creation must return ETIMEDOUT", n.Creation.Errno)
		}
		records := after(t, cell, n, prior)
		for i, image := range n.Images {
			if !reflect.DeepEqual(image.Snapshots, accepted.Images[i].Snapshots) {
				t.Fatal("timed-out creation left a member snapshot", image.Key, image.Snapshots)
			}
		}
		checkData(t, cell, n, records, views(plan.SecondSnapshot))
		t.Logf("timed-out group snapshot %s: %d unanswered quiesce attempts, ETIMEDOUT returned, record removed", record.ID, plan.QuiesceAttempts)
	})
	if t.Failed() {
		return
	}

	// The retried creation: Go leaves the first quiesce attempt to time out
	// and accepts the retry, which carries the same request ID; librbd then
	// locks, snapshots both members and writes COMPLETE. Go never answers
	// the unquiesce: librbd ignores the unquiesce result, so after that
	// notify times out the creation returns success and releases the locks.
	var retriedRecord rbdGroupSnapRecord
	var retried *nativeRBDGroup
	t.Run("retried_quiesce_unanswered_unquiesce", func(t *testing.T) {
		prior := []rbdGroupSnapRecord{first, secondRecord}
		cell := "retried"
		steps := []holdStep{{op: rbdNotifyQuiesce, phase: "quiesce", native: "incomplete-observed", omit: true, check: incomplete(cell, plan.RetriedSnapshot, prior, &retriedRecord)}}
		for i := 2; i < plan.QuiesceAttempts; i++ {
			steps = append(steps, holdStep{op: rbdNotifyQuiesce, phase: fmt.Sprint("quiesce_attempt_", i), omit: true, check: incomplete(cell, plan.RetriedSnapshot, prior, &retriedRecord)})
		}
		steps = append(steps,
			holdStep{op: rbdNotifyQuiesce, phase: fmt.Sprint("quiesce_attempt_", plan.QuiesceAttempts), answer: acceptance, check: incomplete(cell, plan.RetriedSnapshot, prior, &retriedRecord)},
			holdStep{op: rbdNotifyUnquiesce, phase: "unquiesce", native: "complete-observed", omit: true, check: complete(cell, plan.RetriedSnapshot, prior, &retriedRecord)})
		n := creation(t, creationCell{action: cell, snapshot: plan.RetriedSnapshot, attempts: plan.QuiesceAttempts, limitA: rbdGroupNoSnapLimit,
			allowed: lockOps, stderr: []string{notComplete}, locked: true, steps: steps})
		if n.Creation.Errno != 0 {
			t.Fatal("retried group snapshot creation failed", n.Creation.Errno)
		}
		evidence.Records["retried"], evidence.Snapshots["retried"] = retriedRecord, retriedRecord.ID
		records := after(t, cell, n, []rbdGroupSnapRecord{first, secondRecord, retriedRecord})
		checkData(t, cell, n, records, views(plan.SecondSnapshot, plan.RetriedSnapshot))
		retried = n
		t.Logf("retried group snapshot %s: first quiesce timed out, retry accepted, COMPLETE at the unanswered unquiesce, success returned", retriedRecord.ID)
	})
	if t.Failed() {
		return
	}

	// The over-limit creation: native librbd first sets member a's snapshot
	// limit to its snapshot count. Go accepts the quiesce; librbd locks both
	// members and creates both member snapshots, cls_rbd refuses member a's
	// with EDQUOT while member b's is created, and librbd unquiesces,
	// removes member b's snapshot and the INCOMPLETE record and returns
	// ENOENT (rbdGroupMemberSnapshotMissing: the per-image request reports
	// success without a snapshot; ceph/ceph/blob/v20.2.4/src/librbd/api/Group.cc
	// snap_create remove_image_snaps, ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd.cc
	// snapshot_add). Native librbd removes the limit afterwards.
	t.Run("over_limit_member_snapshot_compensation", func(t *testing.T) {
		prior := []rbdGroupSnapRecord{first, secondRecord, retriedRecord}
		cell := "over-limit"
		var record rbdGroupSnapRecord
		var compensated uint64
		before := snapIDs(t, prior)
		limit := uint64(len(before["a"]))
		n := creation(t, creationCell{action: cell, snapshot: plan.OverLimitSnapshot, attempts: rbdGroupDefaultQuiesceAttempts, limitA: limit,
			allowed: lockOps, stderr: []string{notComplete}, locked: true, compensated: true, steps: []holdStep{
				{op: rbdNotifyQuiesce, phase: "quiesce", native: "incomplete-observed", answer: acceptance, check: incomplete(cell, plan.OverLimitSnapshot, prior, &record)},
				{op: rbdNotifyUnquiesce, phase: "unquiesce", native: "member-snapshot-observed", check: func(t *testing.T, v holdView, h rbdGroupHeld, state nativeRBDGroupState, p nativeRBDGroupCreationPhase) {
					// The record never became COMPLETE: it still has zero
					// member snapshot IDs, member a has no new snapshot and
					// member b's group snapshot exists. All of it is read
					// before the unquiesce answer lets librbd remove it.
					pendingAt(t, cell, plan.OverLimitSnapshot, v, prior, &record)
					if !slices.Equal(v.snapc["a"], before["a"]) || len(v.snapc["b"]) != len(before["b"])+1 || !slices.Equal(v.snapc["b"][1:], before["b"]) ||
						v.snapc["b"][0] <= slices.Max(before["b"]) {
						t.Fatal("at the compensating unquiesce only member b holds a new snapshot", v.snapc)
					}
					compensated = v.snapc["b"][0]
					label := cell + "_unquiesce_snapshot_get_b"
					out := call(t, label, secondID, memberHeader("b"), "rbd", "snapshot_get", rbdLE64(compensated), 0)
					if p.MemberSnapshotCall == nil {
						t.Fatal("native observation lacks member b's snapshot")
					}
					sameCall(t, label, *p.MemberSnapshotCall, memberHeader("b"), "rbd", "snapshot_get", rbdLE64(compensated), out)
					info, err := decodeRBDGroupSnapshotInfo(out)
					if err != nil || info.ID != compensated || info.GroupPool != poolID || info.GroupID != group.ID || info.GroupSnapshotID != record.ID ||
						info.Name != rbdGroupImageSnapName(poolID, group.ID, record.ID) || info.ImageSize != plan.Size || info.ChildCount != 0 {
						t.Fatal("member b's snapshot is not the INCOMPLETE record's member snapshot", info, err)
					}
					lockedAt(t, cell, v, h, state)
				}},
			}})
		evidence.Records["over-limit"], evidence.Snapshots["over-limit"] = record, record.ID
		if n.Creation.Errno != rbdGroupMemberSnapshotMissing {
			t.Fatal("over-limit group snapshot creation must return librbd's ENOENT for the missing member snapshot", n.Creation.Errno)
		}
		l := n.Creation.SnapLimit
		limited := memberHeader("a")
		if l == nil || l.Image != "a" || l.Snapshots != limit || l.Limit != limit || l.RemovedLimit != rbdGroupNoSnapLimit {
			t.Fatal("native snapshot limit on member a differs", l)
		}
		sameCall(t, "over-limit limit set", l.SetCall, limited, "rbd", "snapshot_get_limit", nil, rbdLE64(limit))
		out := call(t, cell+"_after_snapshot_get_limit_a", poolID, limited, "rbd", "snapshot_get_limit", nil, 0)
		sameCall(t, "over-limit limit removed", l.RemovedCall, limited, "rbd", "snapshot_get_limit", nil, out)
		if !bytes.Equal(out, rbdLE64(rbdGroupNoSnapLimit)) {
			t.Fatal("member a's snapshot limit remained after the creation", out)
		}
		records := after(t, cell, n, prior)
		for i, image := range n.Images {
			if !reflect.DeepEqual(image.Snapshots, retried.Images[i].Snapshots) {
				t.Fatal("over-limit creation left a member snapshot", image.Key, image.Snapshots)
			}
		}
		call(t, cell+"_after_snapshot_get_b", secondID, memberHeader("b"), "rbd", "snapshot_get", rbdLE64(compensated), -2)
		checkData(t, cell, n, records, views(plan.SecondSnapshot, plan.RetriedSnapshot))
		t.Logf("over-limit group snapshot %s: member a refused, member b's snapshot %d existed at the unquiesce, ENOENT returned, snapshot and record removed", record.ID, compensated)
	})
	unwatch(t)
	if t.Failed() {
		return
	}

	rollback := native(t, "rollback")
	t.Run("rollback_to_first", func(t *testing.T) {
		if rollback.RolledBackTo != plan.GroupSnapshot {
			t.Fatal("native rollback target differs", rollback.RolledBackTo)
		}
		records := after(t, "rollback", rollback, []rbdGroupSnapRecord{first, secondRecord, retriedRecord})
		want := views(plan.SecondSnapshot, plan.RetriedSnapshot)
		want["head"] = []string{"pre"}
		checkData(t, "rollback", rollback, records, want)
		t.Log("after the group rollback both heads read as the first snapshot; the later snapshots keep the later contents")
	})
	if t.Failed() {
		return
	}

	cleanup := native(t, "cleanup")
	t.Run("native_removal", func(t *testing.T) {
		r := cleanup.Removed
		names := func(after []nativeRBDGroupSnap) []string {
			var got []string
			for _, s := range after {
				got = append(got, s.Name)
			}
			slices.Sort(got)
			return got
		}
		firstAndSecond := []string{plan.GroupSnapshot, plan.SecondSnapshot}
		slices.Sort(firstAndSecond)
		if r == nil || r.GroupID != group.ID || r.GroupHeader != header || len(r.GroupSnapshotsAfter) != 3 || !slices.Equal(names(r.GroupSnapshotsAfter[0]), firstAndSecond) ||
			!slices.Equal(names(r.GroupSnapshotsAfter[1]), []string{plan.GroupSnapshot}) || r.GroupSnapshotsAfter[1][0].ID != first.ID || len(r.GroupSnapshotsAfter[2]) != 0 || len(r.MembersAfter) != 0 ||
			r.GroupHeaderErrno != 2 || r.DirectoryCall.Code != -2 || r.DirectoryCall.InputHex != hex.EncodeToString(rbdGroupString(nil, plan.Group)) || len(r.Images) != 2 {
			t.Fatal("native group snapshot, membership or group removal is not proven", r)
		}
		for i, image := range r.Images {
			if image.Key != plan.Images[i].Key || image.Pool != plan.Images[i].Pool || image.ImageID != imageIDs[image.Key] || image.Prefix != "rbd_data."+image.ImageID ||
				len(image.SnapshotsAfter) != 0 || image.HeaderErrno != 2 || image.IDErrno != 2 || image.ObjectMapErrno != 2 || len(image.DataErrno) != int(objectCount) ||
				slices.ContainsFunc(image.DataErrno, func(e int) bool { return e != 2 }) {
				t.Fatal("native member snapshot or image removal is not proven", image.Key, image)
			}
		}
		if !slices.Equal(cleanup.ImagesListed, []string{plan.Pool + "/" + plan.BaseImage}) || len(cleanup.GroupsListed) != 0 {
			t.Fatal("native listing after removal still holds owned group state", cleanup.ImagesListed, cleanup.GroupsListed)
		}
	})
	t.Run("go_reads_after_removal", func(t *testing.T) {
		call(t, "cleanup_dir_get_id", poolID, "rbd_group_directory", "rbd", "dir_get_id", rbdGroupString(nil, plan.Group), -2)
		call(t, "cleanup_group_snap_list", poolID, header, "rbd", "group_snap_list", rbdGroupSnapListInput(rbdGroupSnapRecord{}, plan.ListPage), -2)
		for _, rec := range []rbdGroupSnapRecord{first, secondRecord, retriedRecord} {
			for key, spec := range memberSpecs(t, rec) {
				call(t, "cleanup_snapshot_get_"+key+"_"+rec.ID, spec.Pool, "rbd_header."+spec.ImageID, "rbd", "snapshot_get", rbdLE64(spec.SnapID), -2)
			}
		}
	})
}

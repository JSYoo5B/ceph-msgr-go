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
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Test-only RBD migration encodings, restating the pinned
// ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd_types.cc (MigrationSpec,
// TrashImageSpec, GroupSpec, GroupImageSpec and GroupImageStatus) and
// ceph/ceph/blob/v20.2.4/src/cls/rbd/cls_rbd.cc (migration_get, trash_get,
// image_group_get, group_image_list, get_features); LGPL-2.1 references, no
// code copied. The product carries raw class names, inputs and outputs only.

const (
	rbdMigrationHeaderSource      = 1 // cls::rbd::MIGRATION_HEADER_TYPE_SRC
	rbdMigrationHeaderDestination = 2 // cls::rbd::MIGRATION_HEADER_TYPE_DST
	rbdMigrationStatePrepared     = 2 // cls::rbd::MIGRATION_STATE_PREPARED
	rbdMigrationStateExecuted     = 4 // cls::rbd::MIGRATION_STATE_EXECUTED
	rbdTrashSourceMigration       = 2 // cls::rbd::TRASH_IMAGE_SOURCE_MIGRATION
	// RBD_FEATURE_MIGRATING (ceph/ceph/blob/v20.2.4/src/include/rbd/features.h),
	// which cls_rbd's set_migration sets on the header and migration_remove
	// clears.
	rbdFeatureMigrating = 1 << 9
	// rbdMigrationEBUSY is the errno librbd returns for each refusal.
	rbdMigrationEBUSY = 16
)

// rbdMigrationSpec is one cls::rbd::MigrationSpec of an image header.
type rbdMigrationSpec struct {
	Version          uint8
	HeaderType       uint8
	PoolID           int64
	PoolNamespace    string
	ImageName        string
	ImageID          string
	SnapSeqs         map[uint64]uint64
	Overlap          uint64
	Flatten          bool
	Mirroring        bool
	State            uint8
	StateDescription string
	MirrorImageMode  uint8
	SourceSpec       string
}

// decodeRBDMigrationSpec decodes migration_get's output: ENCODE_START(3,
// compat 1 without a source spec), the header type, the s64 pool, the
// namespace, image name and ID, the snapshot sequence map, the u64 overlap,
// the flatten and mirroring bools, the state and its description, the u8
// mirror image mode and the source spec.
func decodeRBDMigrationSpec(data []byte) (rbdMigrationSpec, error) {
	var s rbdMigrationSpec
	d := &rbdDecoder{data: data}
	v, body := d.header(1)
	s.Version = v
	if v != 3 && d.err == nil {
		return s, fmt.Errorf("MigrationSpec version %d differs", v)
	}
	s.HeaderType, s.PoolID, s.PoolNamespace, s.ImageName, s.ImageID = body.u8(), int64(body.u64()), body.str(), body.str(), body.str()
	n := body.u32()
	if n > 16 {
		return s, errors.New("MigrationSpec lists too many snapshot sequences")
	}
	s.SnapSeqs = map[uint64]uint64{}
	for i := uint32(0); i < n && body.err == nil; i++ {
		from := body.u64()
		s.SnapSeqs[from] = body.u64()
	}
	s.Overlap = body.u64()
	s.Flatten, s.Mirroring = body.u8() != 0, body.u8() != 0
	s.State, s.StateDescription, s.MirrorImageMode, s.SourceSpec = body.u8(), body.str(), body.u8(), body.str()
	return s, errors.Join(body.done(), d.done())
}

// encodeRBDMigrationSpec is MigrationSpec::encode with an empty source spec.
func encodeRBDMigrationSpec(s rbdMigrationSpec) []byte {
	body := []byte{s.HeaderType}
	body = binary.LittleEndian.AppendUint64(body, uint64(s.PoolID))
	body = rbdGroupString(rbdGroupString(rbdGroupString(body, s.PoolNamespace), s.ImageName), s.ImageID)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(s.SnapSeqs)))
	keys := make([]uint64, 0, len(s.SnapSeqs))
	for k := range s.SnapSeqs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		body = binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(body, k), s.SnapSeqs[k])
	}
	body = binary.LittleEndian.AppendUint64(body, s.Overlap)
	flag := func(b bool) byte {
		if b {
			return 1
		}
		return 0
	}
	body = append(body, flag(s.Flatten), flag(s.Mirroring), s.State)
	body = append(rbdGroupString(body, s.StateDescription), s.MirrorImageMode)
	body = rbdGroupString(body, s.SourceSpec)
	return append(binary.LittleEndian.AppendUint32([]byte{3, 1}, uint32(len(body))), body...)
}

// rbdTrashSpec is one cls::rbd::TrashImageSpec of rbd_trash.
type rbdTrashSpec struct {
	Source                          uint8
	Name                            string
	DeletionSeconds, DeferEndSecond uint32
	State                           uint8
}

// decodeRBDTrashSpec decodes trash_get's output: ENCODE_START(2, 1), the u8
// source, the name, two utime_t and the u8 state.
func decodeRBDTrashSpec(data []byte) (rbdTrashSpec, error) {
	var s rbdTrashSpec
	d := &rbdDecoder{data: data}
	v, body := d.header(1)
	if v != 2 && d.err == nil {
		return s, fmt.Errorf("TrashImageSpec version %d differs", v)
	}
	s.Source, s.Name = body.u8(), body.str()
	s.DeletionSeconds, _ = body.u32(), body.u32()
	s.DeferEndSecond, _ = body.u32(), body.u32()
	s.State = body.u8()
	return s, errors.Join(body.done(), d.done())
}

// rbdGroupRef is image_group_get's cls::rbd::GroupSpec: ENCODE_START(1, 1),
// the s64 group pool (-1 for none) and the group ID.
type rbdGroupRef struct {
	Pool int64
	ID   string
}

func decodeRBDGroupRef(data []byte) (rbdGroupRef, error) {
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	r := rbdGroupRef{Pool: int64(body.u64()), ID: body.str()}
	return r, errors.Join(body.done(), d.done())
}

// rbdGroupImage is one cls::rbd::GroupImageStatus of group_image_list.
type rbdGroupImage struct {
	ImageID string
	Pool    int64
	State   uint8
}

// rbdGroupImageListInput is group_image_list's input: the GroupImageSpec to
// list after (empty, pool -1, starts at the beginning) and the page size.
func rbdGroupImageListInput(page uint64) []byte {
	spec := binary.LittleEndian.AppendUint64(rbdGroupString(nil, ""), ^uint64(0))
	return binary.LittleEndian.AppendUint64(rbdGroupEnvelope(nil, spec), page)
}

func decodeRBDGroupImageList(data []byte) ([]rbdGroupImage, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	if n > 16 {
		return nil, errors.New("group_image_list lists too many images")
	}
	images := []rbdGroupImage{}
	for i := uint32(0); i < n && d.err == nil; i++ {
		_, status := d.header(1)
		_, spec := status.header(1)
		image := rbdGroupImage{ImageID: spec.str(), Pool: int64(spec.u64())}
		if err := spec.done(); err != nil {
			return nil, err
		}
		image.State = status.u8()
		if err := status.done(); err != nil {
			return nil, err
		}
		images = append(images, image)
	}
	return images, d.done()
}

// rbdMigrationPattern restates the native driver's deterministic contents of
// one write.
func rbdMigrationPattern(token, image, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-migration:%s:%s:%s:%d:%d", token, image, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

func TestRBDMigrationTestOnlyEncodings(t *testing.T) {
	spec := rbdMigrationSpec{Version: 3, HeaderType: rbdMigrationHeaderDestination, PoolID: 3, ImageName: "src", ImageID: "1a2b", SnapSeqs: map[uint64]uint64{4: 3},
		Overlap: 196608, State: rbdMigrationStatePrepared}
	raw := encodeRBDMigrationSpec(spec)
	got, err := decodeRBDMigrationSpec(raw)
	if err != nil || !reflect.DeepEqual(got, spec) {
		t.Fatal("test-only MigrationSpec round trip differs", got, err)
	}
	// The header type, pool, the snapshot sequence pair, the state and the
	// empty source spec sit at fixed offsets of this encoding.
	if raw[0] != 3 || raw[1] != 1 || raw[6] != 2 || binary.LittleEndian.Uint64(raw[7:]) != 3 || raw[len(raw)-4-1-4-1] != rbdMigrationStatePrepared {
		t.Fatalf("test-only MigrationSpec layout differs: %x", raw)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"truncated":      func(b []byte) []byte { return b[:len(b)-1] },
		"trailing byte":  func(b []byte) []byte { return append(b, 0) },
		"version 2":      func(b []byte) []byte { b[0] = 2; return b },
		"compat 3":       func(b []byte) []byte { b[1] = 3; return b },
		"too many seqs":  func(b []byte) []byte { binary.LittleEndian.PutUint32(b[6+1+8+4+0+4+3+4+4:], 17); return b },
		"short envelope": func(b []byte) []byte { binary.LittleEndian.PutUint32(b[2:], uint32(len(b)-7)); return b },
	} {
		if _, err := decodeRBDMigrationSpec(mutate(slices.Clone(raw))); err == nil {
			t.Fatal("MigrationSpec decoded:", name)
		}
	}
	trash := append(rbdGroupString([]byte{rbdTrashSourceMigration}, "src"), make([]byte, 16)...)
	trash = append(binary.LittleEndian.AppendUint32([]byte{2, 1}, uint32(len(trash)+1)), append(trash, 0)...)
	if ts, err := decodeRBDTrashSpec(trash); err != nil || ts.Source != rbdTrashSourceMigration || ts.Name != "src" || ts.State != 0 {
		t.Fatal("test-only TrashImageSpec differs", ts, err)
	}
	if _, err := decodeRBDTrashSpec(trash[:len(trash)-1]); err == nil {
		t.Fatal("truncated TrashImageSpec decoded")
	}
	ref := rbdGroupEnvelope(nil, rbdGroupString(binary.LittleEndian.AppendUint64(nil, 3), "g1"))
	if r, err := decodeRBDGroupRef(ref); err != nil || r != (rbdGroupRef{3, "g1"}) {
		t.Fatal("test-only GroupSpec differs", r, err)
	}
	none := rbdGroupEnvelope(nil, rbdGroupString(binary.LittleEndian.AppendUint64(nil, ^uint64(0)), ""))
	if r, err := decodeRBDGroupRef(none); err != nil || r != (rbdGroupRef{-1, ""}) {
		t.Fatal("test-only empty GroupSpec differs", r, err)
	}
	// group_image_list's input is the client's empty start spec (pool -1)
	// and the page size; its output one status per member.
	if input := rbdGroupImageListInput(16); hex.EncodeToString(input) != "01010c00000000000000ffffffffffffffff1000000000000000" {
		t.Fatalf("test-only group_image_list input differs: %x", input)
	}
	status := rbdGroupEnvelope(nil, append(rbdGroupEnvelope(nil, binary.LittleEndian.AppendUint64(rbdGroupString(nil, "1a2b"), 4)), 0))
	list := append(binary.LittleEndian.AppendUint32(nil, 1), status...)
	if images, err := decodeRBDGroupImageList(list); err != nil || !reflect.DeepEqual(images, []rbdGroupImage{{"1a2b", 4, 0}}) {
		t.Fatal("test-only group_image_list output differs", images, err)
	}
	if _, err := decodeRBDGroupImageList(append(list, 0)); err == nil {
		t.Fatal("group_image_list output with a trailing byte decoded")
	}
	if p := rbdMigrationPattern("t", "s", "pre", 0, 40); len(p) != 40 || bytes.Equal(p, rbdMigrationPattern("t", "s", "post", 0, 40)) {
		t.Fatal("test-only migration pattern is not phase-specific")
	}
}

// Mirrors of the fixture's migration-driver output
// (integration/fixture/rbd_migration.go).
type nativeRBDMigrationPlan struct {
	Pool             string            `json:"pool"`
	SecondPool       string            `json:"second_pool"`
	BaseImage        string            `json:"base_image"`
	Group            string            `json:"group"`
	GroupSnapshot    string            `json:"group_snapshot"`
	Snapshot         string            `json:"snapshot"`
	Source           string            `json:"source"`
	Destination      string            `json:"destination"`
	AbortSource      string            `json:"abort_source"`
	AbortDestination string            `json:"abort_destination"`
	Order            int               `json:"order"`
	Size             uint64            `json:"size"`
	Features         uint64            `json:"features"`
	ListPage         uint64            `json:"list_page"`
	PreWrites        []nativeRBDExtent `json:"pre_snapshot_writes"`
	PostWrites       []nativeRBDExtent `json:"post_snapshot_writes"`
	AbortWrites      []nativeRBDExtent `json:"abort_writes"`
}

type nativeRBDMigrationDataView struct {
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
	ObjectBytes nativeRBDBlob `json:"object_bytes"`
}

type nativeRBDMigrationSnapshot struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Size      uint64 `json:"size"`
	Namespace int    `json:"namespace"`
}

type nativeRBDMigrationImage struct {
	Key           string             `json:"key"`
	Name          string             `json:"name"`
	Pool          string             `json:"pool"`
	PoolID        int64              `json:"pool_id"`
	ImageID       string             `json:"image_id"`
	IDSource      string             `json:"id_source"`
	DirectoryCall nativeRBDGroupCall `json:"directory_call"`
	Status        *struct {
		SourcePoolID     int64  `json:"source_pool_id"`
		SourceImageName  string `json:"source_image_name"`
		SourceImageID    string `json:"source_image_id"`
		DestPoolID       int64  `json:"dest_pool_id"`
		DestImageName    string `json:"dest_image_name"`
		DestImageID      string `json:"dest_image_id"`
		State            int    `json:"state"`
		StateDescription string `json:"state_description"`
	} `json:"status"`
	StatusErrno   int                                `json:"status_errno"`
	HeaderObject  string                             `json:"header_object"`
	HeaderErrno   int                                `json:"header_errno"`
	TrashCall     *nativeRBDGroupCall                `json:"trash_call"`
	Trash         *struct{ ID, Name, Source string } `json:"trash"`
	TrashErrno    int                                `json:"trash_errno"`
	Calls         map[string]nativeRBDGroupCall      `json:"calls"`
	SnapshotCalls []nativeRBDGroupCall               `json:"snapshot_calls"`
	Views         []nativeRBDMigrationDataView       `json:"views"`
	Reads         *struct {
		Errno          int                          `json:"errno"`
		Features       uint64                       `json:"features"`
		OpFeatures     uint64                       `json:"op_features"`
		Snapshots      []nativeRBDMigrationSnapshot `json:"snapshots"`
		Head           *nativeRBDBlob               `json:"head"`
		SnapshotImages []nativeRBDBlob              `json:"snapshot_images"`
	} `json:"reads"`
}

type nativeRBDMigration struct {
	Action         string   `json:"action"`
	RequestID      int64    `json:"request_id"`
	PoolID         int64    `json:"pool_id"`
	SecondPoolID   int64    `json:"second_pool_id"`
	NativeGlobalID uint64   `json:"native_global_id"`
	ImagesListed   []string `json:"images_listed"`
	GroupsListed   []string `json:"groups_listed"`
	TrashListed    []string `json:"trash_listed"`
	Writes         []struct {
		Image   string `json:"image"`
		Phase   string `json:"phase"`
		Offset  uint64 `json:"offset"`
		Length  uint64 `json:"length"`
		Written uint64 `json:"written"`
		SHA256  string `json:"sha256"`
	} `json:"writes"`
	Refusal *struct {
		Errno     int   `json:"errno"`
		ElapsedNS int64 `json:"elapsed_ns"`
	} `json:"refusal"`
	SourceSnapshotsDuring []nativeRBDMigrationSnapshot `json:"source_snapshots_during"`
	Group                 *struct {
		Name          string                 `json:"name"`
		ID            string                 `json:"id"`
		HeaderObject  string                 `json:"header_object"`
		Members       []nativeRBDGroupMember `json:"members"`
		Snapshots     []string               `json:"snapshots"`
		DirectoryCall nativeRBDGroupCall     `json:"directory_call"`
		ImageListCall nativeRBDGroupCall     `json:"image_list_call"`
	} `json:"group"`
	Images  []nativeRBDMigrationImage `json:"images"`
	Removed *struct {
		GroupID     string `json:"group_id"`
		GroupHeader string `json:"group_header"`
		Images      []struct {
			Key            string `json:"key"`
			Name           string `json:"name"`
			Pool           string `json:"pool"`
			ImageID        string `json:"image_id"`
			Objects        uint64 `json:"objects"`
			HeaderErrno    int    `json:"header_errno"`
			IDErrno        int    `json:"id_errno"`
			ObjectMapErrno int    `json:"object_map_errno"`
			DataErrno      []int  `json:"data_errno"`
		} `json:"images"`
		MembersAfter     []nativeRBDGroupMember `json:"members_after"`
		GroupHeaderErrno int                    `json:"group_header_errno"`
		DirectoryCall    nativeRBDGroupCall     `json:"directory_call"`
	} `json:"removed"`
}

type nativeRBDMigrationResult struct {
	Source    string                 `json:"source_commit"`
	Token     string                 `json:"token"`
	RequestID int64                  `json:"request_id"`
	Action    string                 `json:"action"`
	Plan      nativeRBDMigrationPlan `json:"plan"`
	KnownIDs  map[string]string      `json:"known_ids"`
	Native    *nativeRBDMigration    `json:"native"`
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

// rbdMigrationImageState is what the Go reads of one image established in
// one observation.
type rbdMigrationImageState struct {
	Key, ImageID string
	InDirectory  bool
	HeaderExists bool
	Migration    *rbdMigrationSpec `json:",omitempty"`
	Trash        *rbdTrashSpec     `json:",omitempty"`
	Features     uint64
	Group        rbdGroupRef
	Snapshots    map[uint64]string
}

type rbdMigrationEvidence struct {
	Token          string
	Pool           string
	PoolID         int64
	SecondPool     string
	SecondPoolID   int64
	GoIdentity     string
	GoGlobalID     uint64
	GroupID        string
	ImageIDs       map[string]string
	States         map[string]map[string]rbdMigrationImageState
	Refusals       map[string]int
	Requests       []rbdGroupRequestEvidence
	ClassOutputs   map[string]string
	Watches        map[string][]rbdGroupNotification
	WatchCookies   map[string]uint64
	Pings          map[string]int
	NativeActions  []rbdGroupNativeActionEvidence
	BindingDestIDs map[string]int64
	Timings        map[string]string
}

// TestCephRBDMigrationIntegration follows native RBD image migration through
// prepare, execute and commit, and through prepare and abort, with the
// explicit OSD API; every mutation stays with native librbd. Native librbd
// creates the source s (layering, exclusive-lock, object-map and fast-diff)
// in the fixture's RBD pool with writes before and after one user snapshot,
// a group in that pool holding s, and the abort source a. After each native
// step the Go client sends, in the pool of each image, rbd.dir_get_id for
// its name to rbd_directory, rbd.trash_get for its ID to rbd_trash, and to
// its header rbd.migration_get, rbd.get_features, rbd.get_snapcontext,
// rbd.image_group_get and rbd.snapshot_get for every snapshot; it sends
// rbd.dir_get_id to rbd_group_directory and rbd.group_image_list to the
// group header, and stat and read to every data object at the head and at
// every snapshot. Every raw class input, result code and output is compared
// with native librados exec of the same input, the decoded records with
// native librbd's migration status, trash entry, group membership and
// snapshots, and every data object with native librados reads; native librbd
// image reads are compared with the planned contents.
//
// The sequence and its claims, at ceph/ceph/blob/v20.2.4/src/librbd/api/Migration.cc:
//
//   - watched: while the Go client watches s's header, native prepare of s
//     to the destination d in the second owned pool returns EBUSY ("image
//     has watchers - not migrating"): prepare lists the source's watchers
//     before any mutation. No notification reaches the Go watch and every
//     Go read equals the setup's.
//   - group-snapshot: native librbd snapshots the group, so s holds a
//     group-namespace snapshot besides its user snapshot, and prepare
//     returns EBUSY ("image has non-user type snapshots"); after the group
//     snapshot is removed every Go read equals the setup's.
//   - prepare: s leaves rbd_directory for rbd_trash with source MIGRATION,
//     its header holds a source MigrationSpec (type SRC naming d's pool, name
//     and ID, state PREPARED) and the MIGRATING feature, and its group
//     reference moved to d. d's header holds a destination MigrationSpec
//     (type DST naming s's pool, name and ID, the overlap of the image size,
//     a snapshot sequence pair mapping s's snapshot to d's copied snapshot of
//     the same name, state PREPARED), the MIGRATING feature and the group
//     reference; the group lists d instead of s. No data object of d exists
//     yet, while native librbd reads d's head and snapshot as s's: the
//     migration source is read by the consumer, not copied by the OSD.
//   - execute: while the Go client watches d's header and acknowledges every
//     notification, native execute copies the data; the watch receives one
//     AcquiredLock, at least one HeaderUpdate and one ReleasedLock from the
//     native client, in that order. d's state is EXECUTED and its data
//     objects at the head and at its snapshot hold s's contents.
//   - commit: d's MigrationSpec and MIGRATING feature are gone; s's header,
//     trash entry and data objects are gone (ENOENT to Go and native reads).
//   - abort-prepare, abort-watched and abort: a is prepared to b; while the Go
//     client watches b's header native abort returns EBUSY ("image has
//     watchers - cannot abort migration") with no notification on the watch
//     and every Go read unchanged; after the unwatched abort a is back in
//     rbd_directory with its contents, no MigrationSpec, no MIGRATING feature
//     and no trash entry, and b's header and data objects are gone.
//
// Finally native librbd removes d from the group, the group, d (after its
// snapshot) and a, each followed by a native absence read, and the Go reads
// of the group directory entry, the group header and both headers answer
// ENOENT.
//
// Transport and consumer stay apart here: the Go client carries the class
// calls, watch registrations and acknowledgements and the object reads; the
// refusals while a watcher is listed, the refusal of a group-namespace
// snapshot, reading through to the source while d has no objects, copying,
// the group membership move and the trash bookkeeping are librbd's choices,
// observed through those reads. Not covered: migration of a clone, of a
// journaling or mirrored image (prepare disables the source's mirroring and
// commit enables the destination's), of a namespaced image, import-only
// (source-spec) migration and an interrupted execute.
//
// Two defects of the Python binding at v20.2.4 shape the native driver, not
// the claims: migration_status fills dest_pool_id from the source pool ID,
// so the destination pool is read from the migration records (the evidence
// keeps the binding's value), and the trash_list iterator names only the USER
// and MIRRORING sources and fails on a MIGRATION entry, so the driver lists
// rbd_trash's omap through librados (pybind/rbd/rbd.pyx migration_status and
// TrashIterator).
//
//scenario:row rbd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmigration key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-migration
//scenario:row rbd.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-rbdmigration key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 rbd=1 capabilities=rbd-migration
//scenario:covers RBD underlying I/O
//fixture:isolated adds a second owned pool; natively migrates, commits, aborts and removes images and a group
func TestCephRBDMigrationIntegration(t *testing.T) {
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
	poolOf := map[string]int64{oracle.Plan.Pool: poolID, second.Name: secondID}
	evidence := rbdMigrationEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: poolID, SecondPool: second.Name, SecondPoolID: secondID, GoIdentity: identity, GoGlobalID: gid,
		ImageIDs: map[string]string{}, States: map[string]map[string]rbdMigrationImageState{}, Refusals: map[string]int{}, ClassOutputs: map[string]string{},
		Watches: map[string][]rbdGroupNotification{}, WatchCookies: map[string]uint64{}, Pings: map[string]int{}, BindingDestIDs: map[string]int64{}, Timings: map[string]string{}}
	var evidenceMu sync.Mutex
	defer func() {
		evidenceMu.Lock()
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		evidenceMu.Unlock()
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-migration-results.json"), data)
		}
		if err != nil {
			t.Error("RBD migration evidence", err)
		}
	}()

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt. stderr lists librbd's
	// expected error-level reports for the action.
	var plan nativeRBDMigrationPlan
	native := func(t *testing.T, action string, stderr ...string) *nativeRBDMigration {
		t.Helper()
		receipt := filepath.Join(dir, "rbd-migration-"+action+".json")
		if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		begun := time.Now()
		if err := testcluster.RequestControl(ctx, dir, "rbd-migration", "rbd-migration-completed", token, action); err != nil {
			t.Fatal("native RBD migration", action, err)
		}
		evidence.Timings["native_"+action] = time.Since(begun).String()
		var result nativeRBDMigrationResult
		if err := readMDSFreshJSON(receipt, &result); err != nil {
			t.Fatal(err)
		}
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-migration-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly || result.Mutation.RequestID != result.RequestID ||
			result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil || n.Action != action || n.RequestID != result.RequestID ||
			n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != poolID || n.SecondPoolID != secondID || plan.Pool != "" && !reflect.DeepEqual(result.Plan, plan) {
			t.Fatal("native RBD migration receipt differs", action, result.Error)
		}
		if !rbdGroupStderrAllowed(result.Process.Stderr, stderr) {
			t.Fatalf("native RBD migration %s logged unexpected errors: %q", action, result.Process.Stderr)
		}
		if plan.Pool == "" {
			plan = result.Plan
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdGroupNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID,
			ElapsedMS: result.ElapsedMS, StderrLines: strings.Count(string(result.Process.Stderr), "\n")})
		return n
	}
	setup := native(t, "setup")
	if plan.Pool != oracle.Plan.Pool || plan.SecondPool != second.Name || plan.BaseImage != oracle.Plan.Image || plan.Group != "ceph-msgr-mgroup-"+token ||
		plan.Snapshot != "ceph-msgr-msnap-"+token[:8] || plan.GroupSnapshot != "ceph-msgr-mgsnap-"+token[:8] || plan.Source != "ceph-msgr-msrc-"+token ||
		plan.Destination != "ceph-msgr-mdst-"+token || plan.AbortSource != "ceph-msgr-masrc-"+token || plan.AbortDestination != "ceph-msgr-madst-"+token ||
		plan.Order != 16 || plan.Size != 192<<10 || plan.Features != 1|4|8|16 || plan.ListPage != 16 || len(plan.PreWrites) == 0 || len(plan.PostWrites) == 0 || len(plan.AbortWrites) == 0 {
		t.Fatal("RBD migration plan differs", plan)
	}
	objectSize := uint64(1) << plan.Order
	objectCount := plan.Size >> plan.Order
	images := map[string]struct{ name, pool string }{"s": {plan.Source, plan.Pool}, "d": {plan.Destination, plan.SecondPool}, "a": {plan.AbortSource, plan.Pool}, "b": {plan.AbortDestination, plan.SecondPool}}
	keys := []string{"s", "d", "a", "b"}
	// expected builds an image's bytes from planned writes.
	expected := func(key string, phases ...string) []byte {
		view := make([]byte, plan.Size)
		for _, phase := range phases {
			extents := map[string][]nativeRBDExtent{"s/pre": plan.PreWrites, "s/post": plan.PostWrites, "a/pre": plan.AbortWrites}[key+"/"+phase]
			for _, e := range extents {
				copy(view[e.Offset:], rbdMigrationPattern(token, key, phase, e.Offset, e.Length))
			}
		}
		return view
	}
	i := 0
	for _, w := range []struct {
		key, phase string
		extents    []nativeRBDExtent
	}{{"s", "pre", plan.PreWrites}, {"s", "post", plan.PostWrites}, {"a", "pre", plan.AbortWrites}} {
		for _, e := range w.extents {
			sum := sha256.Sum256(rbdMigrationPattern(token, w.key, w.phase, e.Offset, e.Length))
			if i >= len(setup.Writes) || setup.Writes[i].Image != w.key || setup.Writes[i].Phase != w.phase || setup.Writes[i].Offset != e.Offset || setup.Writes[i].Written != e.Length ||
				setup.Writes[i].SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("test-only RBD migration pattern differs from the native driver", i)
			}
			i++
		}
	}
	if i != len(setup.Writes) {
		t.Fatal("native RBD migration writes differ from the plan")
	}

	addresses := osdDumpAddresses(t, ctx, c)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	route := func(t *testing.T, pool int64, object string) (rbdRoute, *cephmsgr.OSDConnection) {
		t.Helper()
		name, owned := poolNames[pool]
		if !owned {
			t.Fatal("RBD migration object is outside the owned pools", pool, object)
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
	// native result code n.Code; on success the output must equal native
	// librados exec's for the same input, which it returns.
	call := func(t *testing.T, label string, pool int64, n nativeRBDGroupCall, object, class, method string, input []byte) []byte {
		t.Helper()
		if n.Object != object || n.Class != class || n.Method != method || n.InputHex != hex.EncodeToString(input) {
			t.Fatalf("%s: native exec of %s.%s names another object or input: %+v (want %s, %x)", label, class, method, n, object, input)
		}
		r, o := route(t, pool, object)
		req := osdRequest(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: cephmsgr.OSDCallRead, MaxOutputBytes: 8192}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if n.Code != 0 {
			var server *cephmsgr.OSDError
			if !errors.As(err, &server) || server.Code != n.Code || n.OutputHex != "" {
				t.Fatalf("%s: Go CALL %s.%s: native result %d, Go %v", label, class, method, n.Code, err)
			}
			checkReply(t, r, reply, n.Code)
			return nil
		}
		if err != nil {
			t.Fatalf("%s: Go CALL %s.%s: %v (native answered 0)", label, class, method, err)
		}
		checkReply(t, r, reply, 0)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != uint8(len(class)) || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) || got.Class.OutputLength != uint64(len(got.Data)) {
			t.Fatalf("CALL %s.%s per-op result or union echo differs: %+v", class, method, got.Class)
		}
		if hex.EncodeToString(got.Data) != n.OutputHex {
			t.Fatalf("%s: Go CALL %s.%s output differs from native librados exec: native=%s go=%x", label, class, method, n.OutputHex, got.Data)
		}
		evidenceMu.Lock()
		evidence.ClassOutputs[label] = n.OutputHex
		evidenceMu.Unlock()
		return got.Data
	}
	// absent sends one read-mode CALL to an object Go expects gone and
	// requires ENOENT.
	absent := func(t *testing.T, label string, pool int64, object, class, method string, input []byte) {
		t.Helper()
		call(t, label, pool, nativeRBDGroupCall{Object: object, Class: class, Method: method, InputHex: hex.EncodeToString(input), Code: -2}, object, class, method, input)
	}

	// observe reads everything of one native observation and returns, by
	// key, what the Go reads established, with the group's member list.
	type observation struct {
		states  map[string]rbdMigrationImageState
		members []rbdGroupImage
	}
	var groupID string
	observe := func(t *testing.T, phase string, n *nativeRBDMigration) observation {
		t.Helper()
		g := n.Group
		if g == nil || g.Name != plan.Group || len(n.Images) != len(keys) || groupID != "" && g.ID != groupID {
			t.Fatal("native RBD migration observation differs", phase, g)
		}
		groupID = g.ID
		obs := observation{states: map[string]rbdMigrationImageState{}}
		out := call(t, phase+"_group_dir_get_id", poolID, g.DirectoryCall, "rbd_group_directory", "rbd", "dir_get_id", rbdGroupString(nil, plan.Group))
		if d := (&rbdDecoder{data: out}); d.str() != g.ID || d.done() != nil {
			t.Fatal("Go group directory lookup differs from native librbd's group ID", phase)
		}
		out = call(t, phase+"_group_image_list", poolID, g.ImageListCall, g.HeaderObject, "rbd", "group_image_list", rbdGroupImageListInput(plan.ListPage))
		members, err := decodeRBDGroupImageList(out)
		if err != nil || len(members) != len(g.Members) {
			t.Fatal("group_image_list output differs from native librbd's members", phase, members, g.Members, err)
		}
		obs.members = members
		ids := map[string]string{}
		for i, v := range n.Images {
			key := keys[i]
			want := images[key]
			pool := poolOf[want.pool]
			if v.Key != key || v.Name != want.name || v.Pool != want.pool || v.PoolID != pool {
				t.Fatal("native RBD migration image view differs", phase, key, v.Name)
			}
			st := rbdMigrationImageState{Key: key, ImageID: v.ImageID, Snapshots: map[uint64]string{}}
			label := phase + "_" + key
			out := call(t, label+"_dir_get_id", pool, v.DirectoryCall, "rbd_directory", "rbd", "dir_get_id", rbdGroupString(nil, want.name))
			if out != nil {
				d := &rbdDecoder{data: out}
				if id := d.str(); d.done() != nil || id != v.ImageID || v.IDSource != "directory" {
					t.Fatal("Go directory entry differs from the native image ID", phase, key, id, v.ImageID)
				}
				st.InDirectory = true
			}
			if v.ImageID == "" {
				obs.states[key] = st
				continue
			}
			ids[key] = v.ImageID
			if v.TrashCall == nil {
				t.Fatal("native observation lacks the trash read", phase, key)
			}
			idInput := rbdGroupString(nil, v.ImageID)
			if out := call(t, label+"_trash_get", pool, *v.TrashCall, "rbd_trash", "rbd", "trash_get", idInput); out != nil {
				ts, err := decodeRBDTrashSpec(out)
				if err != nil || v.Trash == nil || ts.Name != v.Trash.Name || ts.Name != want.name || v.Trash.ID != v.ImageID || []string{"USER", "MIRRORING", "MIGRATION", "REMOVING"}[min(int(ts.Source), 3)] != v.Trash.Source {
					t.Fatal("Go trash entry differs from native librbd's", phase, key, ts, v.Trash, err)
				}
				st.Trash = &ts
			} else if v.Trash != nil || v.TrashErrno != 2 {
				t.Fatal("native librbd lists a trash entry the class does not", phase, key, v.TrashErrno)
			}
			header := "rbd_header." + v.ImageID
			if v.HeaderObject != header {
				t.Fatal("native header object differs", phase, key, v.HeaderObject)
			}
			if v.HeaderErrno == 2 {
				absent(t, label+"_migration_get_absent", pool, header, "rbd", "migration_get", nil)
			} else {
				st.HeaderExists = true
				inputs := map[string][]byte{"migration_get": nil, "get_features": rbdLE64(rbdNoSnap), "get_snapcontext": nil, "image_group_get": nil}
				outputs := map[string][]byte{}
				for _, method := range []string{"migration_get", "get_features", "get_snapcontext", "image_group_get"} {
					nc, ok := v.Calls[method]
					if !ok {
						t.Fatal("native observation lacks a header read", phase, key, method)
					}
					outputs[method] = call(t, label+"_"+method, pool, nc, header, "rbd", method, inputs[method])
				}
				if out := outputs["migration_get"]; out != nil {
					spec, err := decodeRBDMigrationSpec(out)
					if err != nil || !bytes.Equal(encodeRBDMigrationSpec(spec), out) {
						t.Fatal("Go migration record decoding differs from the OSD class output", phase, key, err)
					}
					st.Migration = &spec
				}
				d := &rbdDecoder{data: outputs["get_features"]}
				features, incompatible := d.u64(), d.u64()
				if d.done() != nil || incompatible != features || features&0xff != plan.Features || (features&rbdFeatureMigrating != 0) != (st.Migration != nil) {
					t.Fatalf("%s %s: features %#x (incompatible %#x) do not match the plan and the migration record", phase, key, features, incompatible)
				}
				st.Features = features
				if st.Group, err = decodeRBDGroupRef(outputs["image_group_get"]); err != nil {
					t.Fatal("image_group_get output", phase, key, err)
				}
				snapc, err := decodeRBDSnapContext(outputs["get_snapcontext"])
				if err != nil || len(snapc.Snapshots) != len(v.SnapshotCalls) {
					t.Fatal("get_snapcontext output differs from the native snapshot reads", phase, key, err)
				}
				for j, id := range snapc.Snapshots {
					out := call(t, fmt.Sprintf("%s_snapshot_get_%d", label, id), pool, v.SnapshotCalls[j], header, "rbd", "snapshot_get", rbdLE64(id))
					info, err := decodeRBDSnapshotInfo(out)
					if err != nil || info.ID != id || info.ImageSize != plan.Size || info.NamespaceType != 0 {
						t.Fatal("Go snapshot differs: not a user snapshot of the image's size", phase, key, info, err)
					}
					st.Snapshots[id] = info.Name
				}
				if r := v.Reads; r != nil && r.Errno == 0 {
					if r.Features != features&^rbdFeatureMigrating && r.Features != features || len(r.Snapshots) != len(st.Snapshots) {
						t.Fatalf("%s %s: native librbd features %#x or snapshots differ from the Go header reads %#x", phase, key, r.Features, features)
					}
					for _, s := range r.Snapshots {
						if st.Snapshots[s.ID] != s.Name || s.Namespace != 0 || s.Size != plan.Size {
							t.Fatal("native librbd snapshot differs from the Go header reads", phase, key, s)
						}
					}
				}
			}
			// The data objects at the head and at every snapshot.
			if len(v.Views) != len(st.Snapshots)+1 {
				t.Fatal("native object views differ from the snapshots", phase, key)
			}
			for _, view := range v.Views {
				if view.Label != "head" && st.Snapshots[view.SnapID] == "" || len(view.Objects) != int(objectCount) {
					t.Fatal("native object view is not of a Go-read snapshot", phase, key, view.Label)
				}
				blob := readRBDBlob(t, dir, view.ObjectBytes)
				for j, object := range view.Objects {
					objectName := fmt.Sprintf("rbd_data.%s.%016x", v.ImageID, j)
					r, o := route(t, pool, objectName)
					req := osdRequest(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
					req.ReadSnapshot = view.SnapID
					reply, err := o.Request(ctx, req)
					record(fmt.Sprintf("%s_read_%s_%d", label, view.Label, j), r, req, reply, err)
					if !object.Exists {
						var server *cephmsgr.OSDError
						if !errors.As(err, &server) || server.Code != -2 {
							t.Fatalf("%s %s %s object %d: native librados reports ENOENT, Go %v", phase, key, view.Label, j, err)
						}
						checkReply(t, r, reply, -2)
						continue
					}
					if err != nil {
						t.Fatalf("%s %s %s object %d: %v", phase, key, view.Label, j, err)
					}
					checkReply(t, r, reply, 0)
					stat, data := reply.Operations[0], reply.Operations[1].Data
					if stat.Result != 0 || len(stat.Data) != 16 || binary.LittleEndian.Uint64(stat.Data) != object.Size || uint64(len(data)) != object.Size || reply.Operations[1].Result != 0 {
						t.Fatalf("%s %s %s object %d stat/read size differs from native librados: native %d", phase, key, view.Label, j, object.Size)
					}
					if !bytes.Equal(data, blob[object.Offset:object.Offset+object.Size]) {
						t.Fatalf("%s %s %s object %d bytes differ from native librados", phase, key, view.Label, j)
					}
				}
			}
			obs.states[key] = st
		}
		evidenceMu.Lock()
		evidence.States[phase] = obs.states
		for k, id := range ids {
			if evidence.ImageIDs[k] == "" {
				evidence.ImageIDs[k] = id
			}
		}
		evidenceMu.Unlock()
		return obs
	}
	// objects returns the bytes of the object views of an image at a label:
	// each object padded to the object size, absent objects as nil.
	objectsOf := func(t *testing.T, v nativeRBDMigrationImage, label string) [][]byte {
		t.Helper()
		for _, view := range v.Views {
			if view.Label != label {
				continue
			}
			blob := readRBDBlob(t, dir, view.ObjectBytes)
			out := make([][]byte, len(view.Objects))
			for j, o := range view.Objects {
				if o.Exists {
					out[j] = append(bytes.Clone(blob[o.Offset:o.Offset+o.Size]), make([]byte, objectSize-o.Size)...)
				}
			}
			return out
		}
		t.Fatal("native object view missing", v.Key, label)
		return nil
	}
	// contents requires an image's objects at a label to hold want; an
	// object absent from the view must be all zeros in want.
	contents := func(t *testing.T, phase string, v nativeRBDMigrationImage, label string, want []byte) {
		t.Helper()
		for j, data := range objectsOf(t, v, label) {
			extent := want[uint64(j)*objectSize : uint64(j+1)*objectSize]
			if data == nil && !bytes.Equal(extent, make([]byte, objectSize)) || data != nil && !bytes.Equal(data, extent) {
				t.Fatalf("%s: %s %s object %d differs from the planned contents (present %v)", phase, v.Key, label, j, data != nil)
			}
		}
	}
	// noObjects requires every object of an image at every view to be
	// absent.
	noObjects := func(t *testing.T, phase string, v nativeRBDMigrationImage) {
		t.Helper()
		for _, view := range v.Views {
			for j, o := range view.Objects {
				if o.Exists {
					t.Fatalf("%s: %s %s object %d exists", phase, v.Key, view.Label, j)
				}
			}
		}
	}
	// reads requires native librbd's head and snapshot reads of an image to
	// hold the planned contents; snap names the one user snapshot.
	reads := func(t *testing.T, phase string, v nativeRBDMigrationImage, head []byte, snap []byte) {
		t.Helper()
		r := v.Reads
		if r == nil || r.Errno != 0 || r.Head == nil || !bytes.Equal(readRBDBlob(t, dir, *r.Head), head) {
			t.Fatalf("%s: native librbd head read of %s differs from the planned contents", phase, v.Key)
		}
		if (snap == nil) != (len(r.Snapshots) == 0) || len(r.Snapshots) > 1 {
			t.Fatal("native librbd snapshots differ", phase, v.Key, r.Snapshots)
		}
		if snap != nil && (r.Snapshots[0].Name != plan.Snapshot || !bytes.Equal(readRBDBlob(t, dir, r.SnapshotImages[0]), snap)) {
			t.Fatalf("%s: native librbd snapshot read of %s differs from the planned contents", phase, v.Key)
		}
	}
	snapshotLabel := func(t *testing.T, st rbdMigrationImageState) string {
		t.Helper()
		if len(st.Snapshots) != 1 {
			t.Fatal("image does not hold exactly its one user snapshot", st.Key, st.Snapshots)
		}
		for id, name := range st.Snapshots {
			if name != plan.Snapshot {
				t.Fatal("image snapshot is not the planned one", st.Key, name)
			}
			return fmt.Sprint("s", id)
		}
		return ""
	}
	view := func(n *nativeRBDMigration, key string) nativeRBDMigrationImage {
		return n.Images[slices.Index(keys, key)]
	}
	listing := func(t *testing.T, phase string, n *nativeRBDMigration, wantImages, wantTrash []string) {
		t.Helper()
		slices.Sort(wantImages)
		slices.Sort(wantTrash)
		trash := []string{}
		for _, e := range n.TrashListed {
			pool, rest, _ := strings.Cut(e, "/")
			_, rest, _ = strings.Cut(rest, "/")
			trash = append(trash, pool+"/"+rest)
		}
		if !slices.Equal(n.ImagesListed, wantImages) || !slices.Equal(n.GroupsListed, []string{plan.Group}) || !slices.Equal(trash, wantTrash) {
			t.Fatal("native RBD migration listing differs", phase, n.ImagesListed, n.GroupsListed, n.TrashListed)
		}
	}

	// setupState judges the state before any migration: s and a in their
	// directory with their contents, s in the group, nothing migrating.
	var sID, aID string
	setupState := func(t *testing.T, phase string, n *nativeRBDMigration) observation {
		t.Helper()
		listing(t, phase, n, []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.Source, plan.Pool + "/" + plan.AbortSource}, nil)
		obs := observe(t, phase, n)
		s, d, a, b := obs.states["s"], obs.states["d"], obs.states["a"], obs.states["b"]
		if !s.InDirectory || !s.HeaderExists || s.Migration != nil || s.Trash != nil || s.Group != (rbdGroupRef{poolID, groupID}) ||
			!a.InDirectory || !a.HeaderExists || a.Migration != nil || a.Trash != nil || a.Group != (rbdGroupRef{-1, ""}) || len(a.Snapshots) != 0 ||
			d.ImageID != "" || d.InDirectory || b.ImageID != "" || b.InDirectory {
			t.Fatal("RBD images before the migration differ", phase, s, a, d, b)
		}
		if s.Features != plan.Features|rbdFeatureOperations || a.Features != plan.Features {
			t.Fatalf("%s: source features %#x or abort source features %#x differ", phase, s.Features, a.Features)
		}
		if len(obs.members) != 1 || obs.members[0] != (rbdGroupImage{s.ImageID, poolID, 0}) {
			t.Fatal("the group does not hold exactly the source, attached", phase, obs.members)
		}
		label := snapshotLabel(t, s)
		contents(t, phase, view(n, "s"), "head", expected("s", "pre", "post"))
		contents(t, phase, view(n, "s"), label, expected("s", "pre"))
		contents(t, phase, view(n, "a"), "head", expected("a", "pre"))
		reads(t, phase, view(n, "s"), expected("s", "pre", "post"), expected("s", "pre"))
		reads(t, phase, view(n, "a"), expected("a", "pre"), nil)
		for _, key := range keys {
			if v := view(n, key); v.Status != nil || v.StatusErrno != 0 {
				t.Fatal("native librbd was asked for a migration status before any migration", phase, key)
			}
		}
		sID, aID = s.ImageID, a.ImageID
		return obs
	}
	var before observation
	t.Run("images_before_migration", func(t *testing.T) {
		before = setupState(t, "setup", setup)
		evidence.GroupID = groupID
	})
	if t.Failed() {
		return
	}

	// watch holds a Go watch on one header for one native action and
	// acknowledges every notification empty, librbd's ImageWatcher form
	// for lock and header-update notifications; allowed are the ops the
	// action may send. It returns the settled notifications.
	watch := func(t *testing.T, label string, pool int64, header string, allowed []uint32, run func()) []rbdGroupNotification {
		t.Helper()
		wr, wo := route(t, pool, header)
		req := osdRequest(wr)
		req.Operations = nil
		w, reply, err := wo.WatchObject(ctx, req, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		record(label+"_watch_register", wr, req, reply, err)
		if err != nil {
			t.Fatal("Go header watch", label, err)
		}
		evidence.WatchCookies[label] = w.Cookie()
		responder := newRBDGroupResponder()
		responder.begin(label, allowed...)
		respondCtx, stop := context.WithCancel(ctx)
		begun := time.Now()
		arrivals := make(chan rbdGroupArrival, 64)
		var responding sync.WaitGroup
		responding.Add(3)
		go func() {
			defer responding.Done()
			for {
				select {
				case <-respondCtx.Done():
					return
				case <-time.After(rbdLockPingInterval):
				}
				_, err := w.Ping(respondCtx, wr.Epoch)
				if respondCtx.Err() != nil {
					return
				}
				responder.mu.Lock()
				responder.pings++
				if err != nil {
					responder.pingErrors = append(responder.pingErrors, err.Error())
				}
				responder.mu.Unlock()
			}
		}()
		go func() {
			defer responding.Done()
			defer close(arrivals)
			for {
				event, err := w.Next(respondCtx)
				if respondCtx.Err() != nil {
					return
				}
				if err != nil {
					responder.fail(fmt.Errorf("header watch ended: %w", err))
					return
				}
				select {
				case arrivals <- responder.receive(time.Now(), begun, event):
				default:
					responder.fail(fmt.Errorf("header watch queued more than %d notifications", cap(arrivals)))
					return
				}
			}
		}()
		go func() {
			defer responding.Done()
			for a := range arrivals {
				if respondCtx.Err() != nil {
					return
				}
				responder.handle(respondCtx, w, wr.Epoch, a)
			}
		}()
		run()
		settled, settleErr := responder.settle(ctx)
		stop()
		responding.Wait()
		responder.mu.Lock()
		failure, events, pings, pingErrors := responder.failure, slices.Clone(responder.events), responder.pings, slices.Clone(responder.pingErrors)
		responder.mu.Unlock()
		evidenceMu.Lock()
		evidence.Watches[label], evidence.Pings[label] = events, pings
		evidenceMu.Unlock()
		reply, err = w.Unwatch(ctx, wr.Epoch)
		record(label+"_watch_unwatch", wr, req, reply, err)
		if err != nil || failure != nil || settleErr != nil || len(pingErrors) != 0 {
			t.Fatal("Go header watch", label, err, failure, settleErr, pingErrors)
		}
		return settled
	}
	refusal := func(t *testing.T, action string, n *nativeRBDMigration) {
		t.Helper()
		if n.Refusal == nil || n.Refusal.Errno != rbdMigrationEBUSY {
			t.Fatal("native librbd did not refuse with EBUSY", action, n.Refusal)
		}
		evidence.Refusals[action] = n.Refusal.Errno
	}
	unchanged := func(t *testing.T, phase string, got, want observation) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: Go reads after the refused action differ from those before it:\n got %+v\nwant %+v", phase, got, want)
		}
	}

	t.Run("prepare_refused_while_watched", func(t *testing.T) {
		var n *nativeRBDMigration
		events := watch(t, "watched", poolID, "rbd_header."+sID, nil, func() { n = native(t, "watched", "image has watchers - not migrating") })
		if len(events) != 0 {
			t.Fatal("a notification reached the watch of the refused prepare", events)
		}
		refusal(t, "watched", n)
		unchanged(t, "watched", setupState(t, "watched", n), before)
	})
	if t.Failed() {
		return
	}
	t.Run("prepare_refused_with_group_snapshot", func(t *testing.T) {
		n := native(t, "group-snapshot", "image has non-user type snapshots that are not supported by migration")
		refusal(t, "group-snapshot", n)
		user, group := 0, 0
		for _, s := range n.SourceSnapshotsDuring {
			switch s.Namespace {
			case 0:
				user++
			case rbdSnapshotNamespaceTypeGroup:
				group++
			}
		}
		if user != 1 || group != 1 || len(n.SourceSnapshotsDuring) != 2 {
			t.Fatal("the source did not hold its user and its group snapshot at the refusal", n.SourceSnapshotsDuring)
		}
		if len(n.Group.Snapshots) != 0 {
			t.Fatal("the group snapshot remained", n.Group.Snapshots)
		}
		unchanged(t, "group-snapshot", setupState(t, "group-snapshot", n), before)
	})
	if t.Failed() {
		return
	}

	// preparedState judges a prepared migration of src to dst.
	preparedState := func(t *testing.T, phase string, n *nativeRBDMigration, src, dst string, grouped bool) observation {
		t.Helper()
		obs := observe(t, phase, n)
		s, d := obs.states[src], obs.states[dst]
		srcPool, dstPool := poolOf[images[src].pool], poolOf[images[dst].pool]
		if s.InDirectory || !s.HeaderExists || s.Trash == nil || s.Trash.Source != rbdTrashSourceMigration || s.Trash.Name != images[src].name || s.Trash.State != 0 ||
			!d.InDirectory || !d.HeaderExists || d.Trash != nil || d.ImageID == "" || d.ImageID == s.ImageID {
			t.Fatal("prepared images differ: the source is not in the trash for migration or the destination not in its directory", phase, s, d)
		}
		sm, dm := s.Migration, d.Migration
		if sm == nil || dm == nil {
			t.Fatal("a prepared image lacks its migration record", phase)
		}
		// The source record names the destination, the destination record
		// the source; only the destination maps snapshots and an overlap.
		if sm.HeaderType != rbdMigrationHeaderSource || sm.PoolID != dstPool || sm.ImageName != images[dst].name || sm.ImageID != d.ImageID || len(sm.SnapSeqs) != 0 || sm.Overlap != 0 ||
			dm.HeaderType != rbdMigrationHeaderDestination || dm.PoolID != srcPool || dm.ImageName != images[src].name || dm.ImageID != s.ImageID || dm.Overlap != plan.Size ||
			sm.PoolNamespace != "" || dm.PoolNamespace != "" || sm.SourceSpec != "" || dm.SourceSpec != "" || sm.Flatten || dm.Flatten || sm.Mirroring || dm.Mirroring ||
			sm.State != rbdMigrationStatePrepared || dm.State != rbdMigrationStatePrepared || sm.StateDescription != "" || dm.StateDescription != "" {
			t.Fatalf("%s: migration records differ:\nsource %+v\ndestination %+v", phase, sm, dm)
		}
		if len(dm.SnapSeqs) != len(s.Snapshots) || len(d.Snapshots) != len(s.Snapshots) {
			t.Fatal("the destination does not map every source snapshot", phase, dm.SnapSeqs, s.Snapshots, d.Snapshots)
		}
		for from, to := range dm.SnapSeqs {
			if s.Snapshots[from] == "" || d.Snapshots[to] != s.Snapshots[from] {
				t.Fatal("a snapshot sequence pair does not map a source snapshot to the destination's of the same name", phase, from, to)
			}
		}
		ref := rbdGroupRef{-1, ""}
		if grouped {
			ref = rbdGroupRef{poolID, groupID}
		}
		if s.Group != (rbdGroupRef{-1, ""}) || d.Group != ref {
			t.Fatal("the group reference did not move to the destination", phase, s.Group, d.Group)
		}
		for _, key := range []string{src, dst} {
			v := view(n, key)
			if v.Status == nil || v.Status.SourceImageName != images[src].name || v.Status.SourceImageID != s.ImageID || v.Status.SourcePoolID != srcPool ||
				v.Status.DestImageName != images[dst].name || v.Status.DestImageID != d.ImageID || v.Status.State != rbdMigrationStatePrepared {
				t.Fatal("native librbd's migration status differs from the records", phase, key, v.Status)
			}
			evidence.BindingDestIDs[phase+"_"+key] = v.Status.DestPoolID
		}
		noObjects(t, phase, view(n, dst))
		return obs
	}

	var prepared observation
	t.Run("prepare", func(t *testing.T) {
		n := native(t, "prepare")
		listing(t, "prepare", n, []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.AbortSource, plan.SecondPool + "/" + plan.Destination},
			[]string{plan.Pool + "/" + plan.Source + "/MIGRATION"})
		prepared = preparedState(t, "prepare", n, "s", "d", true)
		s, d := prepared.states["s"], prepared.states["d"]
		if s.ImageID != sID || len(prepared.members) != 1 || prepared.members[0] != (rbdGroupImage{d.ImageID, secondID, 0}) {
			t.Fatal("the group does not hold exactly the destination, attached", prepared.members)
		}
		// The source's objects are untouched; native librbd reads the
		// destination through to them.
		contents(t, "prepare", view(n, "s"), "head", expected("s", "pre", "post"))
		contents(t, "prepare", view(n, "s"), snapshotLabel(t, s), expected("s", "pre"))
		reads(t, "prepare", view(n, "d"), expected("s", "pre", "post"), expected("s", "pre"))
		if !reflect.DeepEqual(prepared.states["a"], before.states["a"]) {
			t.Fatal("the abort source changed with the prepare")
		}
		t.Logf("prepared %s (%s) -> %s (%s) in pool %d; snapshot sequences %v", plan.Source, s.ImageID, plan.Destination, d.ImageID, secondID, d.Migration.SnapSeqs)
	})
	if t.Failed() {
		return
	}

	t.Run("execute_under_watch", func(t *testing.T) {
		var n *nativeRBDMigration
		d := prepared.states["d"]
		events := watch(t, "execute", secondID, "rbd_header."+d.ImageID, []uint32{rbdNotifyAcquiredLock, rbdNotifyHeaderUpdate, rbdNotifyReleasedLock}, func() { n = native(t, "execute") })
		var ops []uint32
		for _, e := range events {
			if e.NotifierGlobalID != n.NativeGlobalID || e.AckDisposition != rbdAckSubmittedEmpty || e.Unexpected {
				t.Fatal("a notification did not come from the executing client or was not acknowledged empty", e)
			}
			ops = append(ops, e.Op)
		}
		count := map[uint32]int{}
		for _, op := range ops {
			count[op]++
		}
		if len(ops) < 3 || ops[0] != rbdNotifyAcquiredLock || ops[len(ops)-1] != rbdNotifyReleasedLock || count[rbdNotifyAcquiredLock] != 1 || count[rbdNotifyReleasedLock] != 1 ||
			count[rbdNotifyHeaderUpdate] != len(ops)-2 {
			t.Fatal("the execute's notifications are not one AcquiredLock, header updates and one ReleasedLock", ops)
		}
		listing(t, "execute", n, []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.AbortSource, plan.SecondPool + "/" + plan.Destination},
			[]string{plan.Pool + "/" + plan.Source + "/MIGRATION"})
		obs := observe(t, "execute", n)
		s, dd := obs.states["s"], obs.states["d"]
		if dd.Migration == nil || dd.Migration.State != rbdMigrationStateExecuted || s.Migration == nil || s.Migration.State != rbdMigrationStateExecuted ||
			dd.Migration.StateDescription != "" || !reflect.DeepEqual(dd.Snapshots, d.Snapshots) {
			t.Fatal("the executed migration records differ", s.Migration, dd.Migration)
		}
		contents(t, "execute", view(n, "d"), "head", expected("s", "pre", "post"))
		contents(t, "execute", view(n, "d"), snapshotLabel(t, dd), expected("s", "pre"))
		contents(t, "execute", view(n, "s"), "head", expected("s", "pre", "post"))
		reads(t, "execute", view(n, "d"), expected("s", "pre", "post"), expected("s", "pre"))
		present := 0
		for _, data := range objectsOf(t, view(n, "d"), "head") {
			if data != nil {
				present++
			}
		}
		if present == 0 {
			t.Fatal("the execute copied no data object")
		}
		prepared = obs
	})
	if t.Failed() {
		return
	}

	t.Run("commit", func(t *testing.T) {
		n := native(t, "commit")
		listing(t, "commit", n, []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.AbortSource, plan.SecondPool + "/" + plan.Destination}, nil)
		obs := observe(t, "commit", n)
		s, d := obs.states["s"], obs.states["d"]
		if s.ImageID != sID || view(n, "s").IDSource != "known" || s.InDirectory || s.HeaderExists || s.Trash != nil ||
			!d.InDirectory || !d.HeaderExists || d.Migration != nil || d.Trash != nil || d.Group != (rbdGroupRef{poolID, groupID}) || d.Features != plan.Features|rbdFeatureOperations {
			t.Fatal("committed images differ: the source remains or the destination still migrates", s, d)
		}
		noObjects(t, "commit", view(n, "s"))
		contents(t, "commit", view(n, "d"), "head", expected("s", "pre", "post"))
		contents(t, "commit", view(n, "d"), snapshotLabel(t, d), expected("s", "pre"))
		reads(t, "commit", view(n, "d"), expected("s", "pre", "post"), expected("s", "pre"))
		if view(n, "d").Status != nil || view(n, "s").Status != nil {
			t.Fatal("native librbd still reports a migration after the commit")
		}
		if len(obs.members) != 1 || obs.members[0] != (rbdGroupImage{d.ImageID, secondID, 0}) {
			t.Fatal("the group does not hold exactly the destination", obs.members)
		}
	})
	if t.Failed() {
		return
	}

	var abortPrepared observation
	t.Run("abort_prepare", func(t *testing.T) {
		n := native(t, "abort-prepare")
		listing(t, "abort-prepare", n, []string{plan.Pool + "/" + plan.BaseImage, plan.SecondPool + "/" + plan.Destination, plan.SecondPool + "/" + plan.AbortDestination},
			[]string{plan.Pool + "/" + plan.AbortSource + "/MIGRATION"})
		abortPrepared = preparedState(t, "abort-prepare", n, "a", "b", false)
		if abortPrepared.states["a"].ImageID != aID {
			t.Fatal("the abort source changed its ID")
		}
		contents(t, "abort-prepare", view(n, "a"), "head", expected("a", "pre"))
		reads(t, "abort-prepare", view(n, "b"), expected("a", "pre"), nil)
	})
	if t.Failed() {
		return
	}
	t.Run("abort_refused_while_watched", func(t *testing.T) {
		var n *nativeRBDMigration
		b := abortPrepared.states["b"]
		events := watch(t, "abort-watched", secondID, "rbd_header."+b.ImageID, nil, func() { n = native(t, "abort-watched", "image has watchers - cannot abort migration") })
		if len(events) != 0 {
			t.Fatal("a notification reached the watch of the refused abort", events)
		}
		refusal(t, "abort-watched", n)
		unchanged(t, "abort-watched", observe(t, "abort-watched", n), abortPrepared)
	})
	if t.Failed() {
		return
	}
	t.Run("abort", func(t *testing.T) {
		n := native(t, "abort")
		listing(t, "abort", n, []string{plan.Pool + "/" + plan.BaseImage, plan.Pool + "/" + plan.AbortSource, plan.SecondPool + "/" + plan.Destination}, nil)
		obs := observe(t, "abort", n)
		a, b := obs.states["a"], obs.states["b"]
		if a.ImageID != aID || !a.InDirectory || !a.HeaderExists || a.Migration != nil || a.Trash != nil || a.Features != plan.Features || a.Group != (rbdGroupRef{-1, ""}) ||
			b.ImageID != abortPrepared.states["b"].ImageID || view(n, "b").IDSource != "known" || b.InDirectory || b.HeaderExists || b.Trash != nil {
			t.Fatal("aborted images differ: the source is not restored or the destination remains", a, b)
		}
		contents(t, "abort", view(n, "a"), "head", expected("a", "pre"))
		reads(t, "abort", view(n, "a"), expected("a", "pre"), nil)
		noObjects(t, "abort", view(n, "b"))
	})
	if t.Failed() {
		return
	}

	t.Run("cleanup", func(t *testing.T) {
		n := native(t, "cleanup")
		r := n.Removed
		if r == nil || r.GroupID != groupID || r.GroupHeaderErrno != 2 || r.DirectoryCall.Code != -2 || len(r.MembersAfter) != 0 || len(r.Images) != 2 {
			t.Fatal("native RBD migration cleanup differs", r)
		}
		listing := []string{plan.Pool + "/" + plan.BaseImage}
		if !slices.Equal(n.ImagesListed, listing) || len(n.GroupsListed) != 0 || len(n.TrashListed) != 0 {
			t.Fatal("native listing after the cleanup differs", n.ImagesListed, n.GroupsListed, n.TrashListed)
		}
		absent(t, "cleanup_group_dir_get_id", poolID, "rbd_group_directory", "rbd", "dir_get_id", rbdGroupString(nil, plan.Group))
		absent(t, "cleanup_group_image_list", poolID, "rbd_group_header."+groupID, "rbd", "group_image_list", rbdGroupImageListInput(plan.ListPage))
		for _, image := range r.Images {
			pool := poolOf[image.Pool]
			if image.HeaderErrno != 2 || image.IDErrno != 2 || image.ObjectMapErrno != 2 || slices.ContainsFunc(image.DataErrno, func(e int) bool { return e != 2 }) ||
				image.ImageID != evidence.ImageIDs[image.Key] {
				t.Fatal("native absence after an image removal differs", image)
			}
			absent(t, "cleanup_"+image.Key+"_migration_get", pool, "rbd_header."+image.ImageID, "rbd", "migration_get", nil)
			absent(t, "cleanup_"+image.Key+"_dir_get_id", pool, "rbd_directory", "rbd", "dir_get_id", rbdGroupString(nil, image.Name))
		}
	})
}

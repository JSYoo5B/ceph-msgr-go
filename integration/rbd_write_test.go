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
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDWriteIntegration checks that the explicit OSD API carries the
// Messenger I/O of an RBD writer, against a second image created by native
// librbd in the RBD base fixture's owned pool: data-object writes under the
// image's self-managed SnapContext (the OSD clones what the snapshot still
// needs), rbd class header mutations (set_size, metadata_set), a lock-class
// exclusive lock in librbd's managed-lock format, and a header-update notify
// that librbd acknowledges and acts on. Native librbd and librados observe
// every step independently. See integration/fixture/RBD_BASE_FIXTURE.md and
// RBD_WRITE_VALIDATION.md.
//
// Everything RBD-specific below is test-only consumer logic: the striping
// map, the SnapContext taken from the header, class and lock encodings and
// decodings, and the clone model. The product carries raw class names,
// inputs and outputs, raw object bytes and the caller's SnapContext only.
// Encodings restate the pinned cls/rbd/cls_rbd.cc, cls/lock/cls_lock_ops.h,
// cls/lock/cls_lock_types.h, cls/lock/cls_lock.cc, msg/msg_types.h,
// librbd/managed_lock/Utils.cc, librbd/io/ObjectRequest.cc and
// librbd/WatchNotifyTypes.cc (LGPL-2.1 references; no code copied).

const (
	// RBD_LOCK_NAME (include/rbd_types.h) and the managed-lock tag and
	// cookie prefix (librbd/managed_lock/Utils.cc).
	rbdLockName         = "rbd_lock"
	rbdLockTag          = "internal"
	rbdLockCookiePrefix = "auto"
	// ClsLockType::EXCLUSIVE and entity_addr_t::TYPE_LEGACY, which
	// cls_lock stores for every locker address.
	rbdLockExclusive  = 1
	rbdAddrTypeLegacy = 1
)

type nativeRBDWritePlan struct {
	Pool            string            `json:"pool"`
	BaseImage       string            `json:"base_image"`
	Image           string            `json:"image"`
	Snapshot        string            `json:"snapshot"`
	Order           int               `json:"order"`
	Size            uint64            `json:"size"`
	Features        uint64            `json:"features"`
	PreWrites       []nativeRBDExtent `json:"pre_snapshot_writes"`
	PostWrites      []nativeRBDExtent `json:"post_snapshot_writes"`
	GoWrites        []nativeRBDExtent `json:"go_writes"`
	GoResize        uint64            `json:"go_resize"`
	GoResizeWatched uint64            `json:"go_resize_watched"`
	MetadataKey     string            `json:"metadata_key"`
	MetadataValue   string            `json:"metadata_value"`
}

type nativeRBDWriteClassCall struct {
	Object    string `json:"object"`
	Class     string `json:"class"`
	Method    string `json:"method"`
	InputHex  string `json:"input_hex"`
	Code      int32  `json:"code"`
	OutputHex string `json:"output_hex"`
}

type nativeRBDWriteLockers struct {
	Empty     bool   `json:"empty"`
	Tag       string `json:"tag"`
	Exclusive bool   `json:"exclusive"`
	Lockers   []struct {
		Client  string `json:"client"`
		Cookie  string `json:"cookie"`
		Address string `json:"addr"`
	} `json:"lockers"`
}

type nativeRBDWritePhase struct {
	Phase      string                   `json:"phase"`
	Size       uint64                   `json:"size"`
	StatSize   uint64                   `json:"stat_size"`
	Watchers   []nativeRBDWatcher       `json:"watchers"`
	WaitNS     int64                    `json:"wait_ns"`
	HeaderSize *nativeRBDWriteClassCall `json:"header_size"`
}

type nativeRBDWrite struct {
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
	Features  uint64 `json:"features"`
	Snapshots []struct {
		ID        uint64 `json:"id"`
		Name      string `json:"name"`
		Size      uint64 `json:"size"`
		Namespace int    `json:"namespace"`
	} `json:"snapshots"`
	Metadata []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"metadata"`
	Lockers    nativeRBDWriteLockers     `json:"lockers"`
	Watchers   []nativeRBDWatcher        `json:"watchers"`
	ClassCalls []nativeRBDWriteClassCall `json:"class_calls"`
	Writes     []struct {
		Phase  string `json:"phase"`
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
		SHA256 string `json:"sha256"`
	} `json:"writes"`
	Objects []nativeRBDObject        `json:"objects"`
	Blobs   map[string]nativeRBDBlob `json:"blobs"`
	Phases  []nativeRBDWritePhase    `json:"phases"`
}

type nativeRBDWriteResult struct {
	Source    string             `json:"source_commit"`
	Token     string             `json:"token"`
	RequestID int64              `json:"request_id"`
	Action    string             `json:"action"`
	Plan      nativeRBDWritePlan `json:"plan"`
	Native    *nativeRBDWrite    `json:"native"`
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

type nativeRBDWriteState struct {
	Token          string                `json:"token"`
	RequestID      int64                 `json:"request_id"`
	NativeGlobalID uint64                `json:"native_global_id"`
	Phase          string                `json:"phase"`
	Phases         []nativeRBDWritePhase `json:"phases"`
}

type rbdOperationInputEvidence struct {
	Code           uint16
	Flags          uint32 `json:",omitempty"`
	Offset, Length uint64
	DataLength     int
	DataSHA256     string
	DataHex        string                 `json:",omitempty"`
	Class          string                 `json:",omitempty"`
	Method         string                 `json:",omitempty"`
	Mode           uint8                  `json:",omitempty"`
	AllocHint      *cephmsgr.OSDAllocHint `json:",omitempty"`
}

type rbdWriteRequestEvidence struct {
	Label                        string
	Route                        rbdRoute
	ReadSnapshot                 uint64                   `json:",omitempty"`
	SnapContext                  *cephmsgr.OSDSnapContext `json:",omitempty"`
	ModificationTime             *cephmsgr.OSDTime        `json:",omitempty"`
	Inputs                       []rbdOperationInputEvidence
	Submission                   cephmsgr.OSDSubmission
	Transaction                  uint64
	GlobalID                     uint64
	MapEpoch                     uint32
	PG                           cephmsgr.OSDPG
	Code                         int32
	Flags                        uint64
	UserVersion                  uint64
	LegacyVersion, ReplayVersion cephmsgr.OSDVersion
	Operations                   []rbdOperationEvidence
	Error                        string `json:",omitempty"`
}

type rbdGoWriteEvidence struct {
	ImageOffset, ImageLength uint64
	Object                   string
	ObjectNumber             uint64
	ObjectOffset, Length     uint64
	SHA256                   string
	Transaction, UserVersion uint64
}

type rbdNotifyReplyEvidence struct {
	GlobalID, Cookie uint64
	DataHex          string
}

type rbdNotifyEvidence struct {
	TimeoutSeconds   uint32
	PayloadHex       string
	Cookie           uint64
	NotifyID         uint64
	ReplyDataHex     string
	Opcode           uint8
	CompletionID     uint64
	ReturnCode       int32
	Replies          []rbdNotifyReplyEvidence
	Missed           []cephmsgr.OSDNotifyIdentity
	RawFrontHex      string
	RawDataHex       string
	CompletedAfterMS float64
}

type rbdLockEvidence struct {
	WatchCookie                          uint64
	Cookie                               string
	LockInputHex, UnlockInputHex         string
	InfoBefore, InfoLocked, InfoUnlocked string
	Locked                               *rbdLockInfo
	Unlocked                             *rbdLockInfo
	GoWatcher                            *cephmsgr.OSDWatcher
	NativeLockers                        *nativeRBDWriteLockers
}

type rbdNativeActionEvidence struct {
	Action         string
	RequestID      int64
	NativeGlobalID uint64
	ElapsedMS      int64
	StderrBytes    int
}

type rbdWriteEvidence struct {
	Token, Pool       string
	PoolID            int64
	GoIdentity        string
	GoGlobalID        uint64
	Image, ImageID    string
	SnapshotID        uint64
	SnapContext       cephmsgr.OSDSnapContext
	Requests          []rbdWriteRequestEvidence
	ClassOutputs      map[string]string
	Writes            []rbdGoWriteEvidence
	ListSnapsBefore   map[string]any
	ListSnapsAfter    map[string]any
	Lock              *rbdLockEvidence
	Notify            *rbdNotifyEvidence
	NativeActions     []rbdNativeActionEvidence
	WatchState        *nativeRBDWriteState
	Timings           map[string]string
	ExpectedHeadSHA   string
	ExpectedSnapSHA   string
	NativeWrittenHead string
}

// Test-only class encodings.

func rbdString(s string) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(s))), s...)
}

// rbdEnvelope is ENCODE_START(1, 1) around payload.
func rbdEnvelope(payload []byte) []byte {
	return append(binary.LittleEndian.AppendUint32([]byte{1, 1}, uint32(len(payload))), payload...)
}

// rbdLockCookie is librbd's managed-lock cookie for a watch handle.
func rbdLockCookie(handle uint64) string { return fmt.Sprintf("%s %d", rbdLockCookiePrefix, handle) }

// rbdLockHandle restates librbd's decode_lock_cookie: the prefix, a space and
// a decimal handle.
func rbdLockHandle(cookie string) (uint64, bool) {
	prefix, value, ok := strings.Cut(cookie, " ")
	handle, err := strconv.ParseUint(value, 10, 64)
	return handle, ok && prefix == rbdLockCookiePrefix && err == nil && handle != 0
}

// rbdLockLockInput is cls_lock_lock_op as librbd's AcquireRequest sends it:
// name, u8 type, cookie, tag, empty description, zero utime_t duration and
// zero flags.
func rbdLockLockInput(cookie string) []byte {
	p := append(rbdString(rbdLockName), rbdLockExclusive)
	p = append(append(append(p, rbdString(cookie)...), rbdString(rbdLockTag)...), rbdString("")...)
	return rbdEnvelope(append(p, make([]byte, 9)...))
}

func rbdLockUnlockInput(cookie string) []byte {
	return rbdEnvelope(append(rbdString(rbdLockName), rbdString(cookie)...))
}

func rbdLockGetInfoInput() []byte { return rbdEnvelope(rbdString(rbdLockName)) }

// rbdMetadataSetInput is a one-entry map<string, bufferlist>.
func rbdMetadataSetInput(key, value string) []byte {
	return append(append(binary.LittleEndian.AppendUint32(nil, 1), rbdString(key)...), rbdString(value)...)
}

// rbdMetadataListInput is start_after "" and max_return 64.
func rbdMetadataListInput() []byte { return append(rbdString(""), rbdLE64(64)...) }

func (d *rbdDecoder) adopt(sub *rbdDecoder) {
	if err := sub.done(); err != nil && d.err == nil {
		d.err = err
	}
}

func decodeRBDMetadata(data []byte) (map[string]string, error) {
	d := &rbdDecoder{data: data}
	n := d.u32()
	result := map[string]string{}
	for i := uint32(0); i < n && d.err == nil; i++ {
		if i >= 64 {
			return nil, errors.New("RBD metadata map exceeds its bound")
		}
		key := d.str()
		result[key] = d.str()
	}
	return result, d.done()
}

type rbdLockAddress struct {
	Type, Nonce uint32
	Endpoint    netip.AddrPort
	RawHex      string
}

type rbdLocker struct {
	NameType          uint8
	NameNumber        int64
	Cookie            string
	ExpirationSeconds uint32
	ExpirationNanos   uint32
	Address           rbdLockAddress
	Description       string
}

type rbdLockInfo struct {
	Lockers []rbdLocker
	Type    uint8
	Tag     string
}

// entityAddr decodes a features-encoded entity_addr_t with the MSG_ADDR2
// marker: type, nonce and a Linux sockaddr (u16 family, then the network
// order port and address).
func (d *rbdDecoder) entityAddr() rbdLockAddress {
	var a rbdLockAddress
	if marker := d.u8(); d.err == nil && marker != 1 {
		d.err = fmt.Errorf("entity_addr_t marker %d", marker)
		return a
	}
	_, body := d.header(1)
	a.Type, a.Nonce = body.u32(), body.u32()
	n := body.u32()
	raw := body.take(int(n))
	a.RawHex = hex.EncodeToString(raw)
	if body.err == nil {
		var ip netip.Addr
		family := uint16(0)
		if len(raw) >= 2 {
			family = binary.LittleEndian.Uint16(raw)
		}
		switch {
		case family == 2 && len(raw) == 16:
			ip = netip.AddrFrom4([4]byte(raw[4:8]))
		case family == 10 && len(raw) == 28:
			ip = netip.AddrFrom16([16]byte(raw[8:24]))
		default:
			body.err = fmt.Errorf("unsupported sockaddr family %d length %d", family, len(raw))
		}
		if body.err == nil {
			a.Endpoint = netip.AddrPortFrom(ip, binary.BigEndian.Uint16(raw[2:4]))
		}
	}
	d.adopt(body)
	return a
}

// decodeRBDLockInfo decodes cls_lock_get_info_reply: the locker map
// (locker_id_t: entity_name_t and cookie; locker_info_t: utime_t expiration,
// entity_addr_t and description), the u8 lock type and the tag.
func decodeRBDLockInfo(data []byte) (rbdLockInfo, error) {
	var info rbdLockInfo
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	n := body.u32()
	if n > 16 {
		return info, errors.New("lock info locker count exceeds its bound")
	}
	for i := uint32(0); i < n && body.err == nil; i++ {
		var l rbdLocker
		_, id := body.header(1)
		l.NameType, l.NameNumber = id.u8(), int64(id.u64())
		l.Cookie = id.str()
		body.adopt(id)
		_, entry := body.header(1)
		l.ExpirationSeconds, l.ExpirationNanos = entry.u32(), entry.u32()
		l.Address = entry.entityAddr()
		l.Description = entry.str()
		body.adopt(entry)
		info.Lockers = append(info.Lockers, l)
	}
	info.Type = body.u8()
	info.Tag = body.str()
	d.adopt(body)
	return info, d.done()
}

// rbdWritePattern restates the native driver's token-bound SHA-256 stream.
func rbdWritePattern(token, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-write:%s:%s:%d:%d", token, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

// rbdCloneModel predicts one data object's snapset under self-managed
// snapshots: a write whose SnapContext is newer than the object's sequence
// clones an existing head (overlap: the clone minus later head writes) and
// advances the sequence; a write that creates the object only sets the
// sequence.
type rbdCloneModel struct {
	Exists     bool
	Size       uint64
	Sequence   uint64
	Clone      bool
	CloneSize  uint64
	AfterClone []cephmsgr.OSDExtent
}

func (m *rbdCloneModel) write(offset, length, snapSequence uint64) {
	if snapSequence > m.Sequence {
		if m.Exists {
			m.Clone, m.CloneSize, m.AfterClone = true, m.Size, nil
		}
		m.Sequence = snapSequence
	}
	if m.Clone {
		m.AfterClone = append(m.AfterClone, cephmsgr.OSDExtent{Offset: offset, Length: length})
	}
	m.Exists, m.Size = true, max(m.Size, offset+length)
}

// listing is the OSDSnapshotList the model predicts for snapshot snapID.
func (m rbdCloneModel) listing(snapID uint64) cephmsgr.OSDSnapshotList {
	list := cephmsgr.OSDSnapshotList{Sequence: m.Sequence}
	if m.Clone {
		list.Clones = append(list.Clones, cephmsgr.OSDClone{ID: snapID, Snapshots: []uint64{snapID}, Size: m.CloneSize, Overlap: rbdSubtract(m.CloneSize, m.AfterClone)})
	}
	return cephmsgr.OSDSnapshotList{Sequence: list.Sequence, Clones: append(list.Clones, cephmsgr.OSDClone{ID: cephmsgr.OSDHeadSnapshot, Size: m.Size})}
}

func sameSnapshotList(got, want cephmsgr.OSDSnapshotList) bool {
	if got.Sequence != want.Sequence || len(got.Clones) != len(want.Clones) {
		return false
	}
	for i := range want.Clones {
		g, w := got.Clones[i], want.Clones[i]
		if g.ID != w.ID || g.Size != w.Size || len(g.Snapshots) != len(w.Snapshots) || len(g.Overlap) != len(w.Overlap) ||
			len(w.Snapshots) != 0 && !reflect.DeepEqual(g.Snapshots, w.Snapshots) || len(w.Overlap) != 0 && !reflect.DeepEqual(g.Overlap, w.Overlap) {
			return false
		}
	}
	return true
}

func TestRBDWriteTestOnlyEncodings(t *testing.T) {
	lock := rbdLockLockInput("auto 38")
	want := "0101" + "31000000" + "08000000" + hex.EncodeToString([]byte("rbd_lock")) + "01" + "07000000" + hex.EncodeToString([]byte("auto 38")) +
		"08000000" + hex.EncodeToString([]byte("internal")) + "00000000" + "0000000000000000" + "00"
	if hex.EncodeToString(lock) != want {
		t.Fatalf("cls_lock_lock_op encoding differs: %x", lock)
	}
	if got := hex.EncodeToString(rbdLockUnlockInput("auto 38")); got != "0101"+"17000000"+"08000000"+hex.EncodeToString([]byte("rbd_lock"))+"07000000"+hex.EncodeToString([]byte("auto 38")) {
		t.Fatal("cls_lock_unlock_op encoding differs", got)
	}
	if got := hex.EncodeToString(rbdLockGetInfoInput()); got != "01010c00000008000000"+hex.EncodeToString([]byte("rbd_lock")) {
		t.Fatal("cls_lock_get_info_op encoding differs", got)
	}
	if handle, ok := rbdLockHandle(rbdLockCookie(281473231959072)); !ok || handle != 281473231959072 {
		t.Fatal("managed-lock cookie round trip", handle, ok)
	}
	for _, cookie := range []string{"auto", "auto 0", "auto x", "manual 7", "auto  7", ""} {
		if _, ok := rbdLockHandle(cookie); ok {
			t.Fatal("foreign lock cookie decoded", cookie)
		}
	}
	if got := hex.EncodeToString(rbdMetadataSetInput("k", "vv")); got != "01000000"+"01000000"+"6b"+"02000000"+"7676" {
		t.Fatal("metadata map encoding differs", got)
	}
	if meta, err := decodeRBDMetadata(rbdMetadataSetInput("k", "vv")); err != nil || !reflect.DeepEqual(meta, map[string]string{"k": "vv"}) {
		t.Fatal("metadata map decoding", meta, err)
	}
	addr := func(family uint16, sockaddr []byte) []byte {
		raw := append(binary.LittleEndian.AppendUint16(nil, family), sockaddr...)
		body := binary.LittleEndian.AppendUint32(nil, rbdAddrTypeLegacy)
		body = binary.LittleEndian.AppendUint32(body, 3974648288)
		body = append(binary.LittleEndian.AppendUint32(body, uint32(len(raw))), raw...)
		return append([]byte{1}, rbdEnvelope(body)...)
	}
	v4 := append([]byte{0x1f, 0x90, 172, 19, 0, 9}, make([]byte, 8)...)
	v6 := append(append([]byte{0, 0, 0, 0, 0, 0}, netip.MustParseAddr("fd62:93fe:2c9e::9").AsSlice()...), 0, 0, 0, 0)
	for _, tc := range []struct {
		family   uint16
		sockaddr []byte
		endpoint string
	}{{2, v4, "172.19.0.9:8080"}, {10, v6, "[fd62:93fe:2c9e::9]:0"}} {
		locker := append(rbdEnvelope(append(append([]byte{8}, rbdLE64(4743)...), rbdString("auto 38")...)),
			rbdEnvelope(append(append(make([]byte, 8), addr(tc.family, tc.sockaddr)...), rbdString("")...))...)
		reply := rbdEnvelope(append(append(append(binary.LittleEndian.AppendUint32(nil, 1), locker...), rbdLockExclusive), rbdString(rbdLockTag)...))
		info, err := decodeRBDLockInfo(reply)
		if err != nil || len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != rbdLockTag || info.Lockers[0].NameType != rbdEntityClient || info.Lockers[0].NameNumber != 4743 ||
			info.Lockers[0].Cookie != "auto 38" || info.Lockers[0].Address.Type != rbdAddrTypeLegacy || info.Lockers[0].Address.Nonce != 3974648288 || info.Lockers[0].Address.Endpoint.String() != tc.endpoint {
			t.Fatal("cls_lock_get_info_reply decoding", info, err)
		}
		for cut := 0; cut < len(reply); cut++ {
			if _, err := decodeRBDLockInfo(reply[:cut]); err == nil {
				t.Fatal("truncated lock info admitted", cut)
			}
		}
		if _, err := decodeRBDLockInfo(append(bytes.Clone(reply), 0)); err == nil {
			t.Fatal("trailing lock info byte admitted")
		}
	}
	// The clone model: a pre-snapshot object cloned by the first write under
	// the newer context, then shrinking overlap; an object created after it.
	var m rbdCloneModel
	m.write(0, 64<<10, 0)
	m.write(16<<10, 16<<10, 4)
	m.write(0, 4<<10, 4)
	if got := m.listing(4); !sameSnapshotList(got, cephmsgr.OSDSnapshotList{Sequence: 4, Clones: []cephmsgr.OSDClone{
		{ID: 4, Snapshots: []uint64{4}, Size: 64 << 10, Overlap: []cephmsgr.OSDExtent{{Offset: 4 << 10, Length: 12 << 10}, {Offset: 32 << 10, Length: 32 << 10}}},
		{ID: cephmsgr.OSDHeadSnapshot, Size: 64 << 10}}}) {
		t.Fatal("clone model differs", got)
	}
	var created rbdCloneModel
	created.write(8<<10, 4<<10, 4)
	if got := created.listing(4); !sameSnapshotList(got, cephmsgr.OSDSnapshotList{Sequence: 4, Clones: []cephmsgr.OSDClone{{ID: cephmsgr.OSDHeadSnapshot, Size: 12 << 10}}}) {
		t.Fatal("created-object model differs", got)
	}
}

// waitRBDWriteState polls the write driver's published state until it holds
// the named phase (earlier phases are kept), or the control action ended.
func waitRBDWriteState(ctx context.Context, path, phase string, finished <-chan struct{}, finishedErr *error) (nativeRBDWriteState, nativeRBDWritePhase, error) {
	read := func() (nativeRBDWriteState, nativeRBDWritePhase, bool) {
		var state nativeRBDWriteState
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &state) != nil {
			return state, nativeRBDWritePhase{}, false
		}
		i := slices.IndexFunc(state.Phases, func(p nativeRBDWritePhase) bool { return p.Phase == phase })
		if i < 0 {
			return state, nativeRBDWritePhase{}, false
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
			return nativeRBDWriteState{}, nativeRBDWritePhase{}, fmt.Errorf("native RBD write watch action ended before %s: %v", phase, *finishedErr)
		case <-ctx.Done():
			return nativeRBDWriteState{}, nativeRBDWritePhase{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestCephRBDWriteIntegration(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	evidence := rbdWriteEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, GoGlobalID: gid, ClassOutputs: map[string]string{}, Timings: map[string]string{},
		ListSnapsBefore: map[string]any{}, ListSnapsAfter: map[string]any{}}
	defer func() {
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-write-results.json"), data)
		}
		if err != nil {
			t.Error("RBD write evidence", err)
		}
	}()

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDWritePlan
	native := func(t *testing.T, action string) nativeRBDWriteResult {
		t.Helper()
		receipt := filepath.Join(dir, "rbd-write-"+action+".json")
		if err := os.Remove(receipt); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := testcluster.RequestControl(ctx, dir, "rbd-write", "rbd-write-completed", token, action); err != nil {
			t.Fatal("native RBD write", action, err)
		}
		var result nativeRBDWriteResult
		if err := readMDSFreshJSON(receipt, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	checkNative := func(t *testing.T, action string, result nativeRBDWriteResult) *nativeRBDWrite {
		t.Helper()
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-write-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != strings.HasPrefix(action, "observe-") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != oracle.Pool.ID ||
			(plan.Image != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD write receipt differs", action, result.Error)
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		return n
	}
	setupResult := native(t, "setup")
	plan = setupResult.Plan
	setup := checkNative(t, "setup", setupResult)
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.BaseImage != oracle.Plan.Image || plan.Image != "ceph-msgr-write-"+token || plan.Snapshot != "ceph-msgr-wsnap-"+token[:8] ||
		plan.Order != 16 || plan.Size != 256*k || plan.Features != 1 || plan.GoResize != 320*k || plan.GoResizeWatched != 384*k || len(plan.GoWrites) != 4 ||
		plan.MetadataKey != "ceph-msgr-write."+token[:8] || plan.MetadataValue != "go-written-"+token[:8] {
		t.Fatal("RBD write plan differs", plan)
	}
	// The test-only data pattern equals the native driver's writes.
	for i, w := range setup.Writes {
		extent := append(append([]nativeRBDExtent{}, plan.PreWrites...), plan.PostWrites...)[i]
		sum := sha256.Sum256(rbdWritePattern(token, w.Phase, w.Offset, w.Length))
		if w.Offset != extent.Offset || w.Length != extent.Length || hex.EncodeToString(sum[:]) != w.SHA256 {
			t.Fatal("test-only RBD write pattern differs from the native driver", w)
		}
	}
	if len(setup.Snapshots) != 1 || setup.Snapshots[0].Name != plan.Snapshot || setup.Snapshots[0].Size != plan.Size || setup.Stat.Size != plan.Size || setup.Features != plan.Features ||
		len(setup.ClassCalls) != 8 || !setup.Lockers.Empty || len(setup.Metadata) != 0 || len(setup.Watchers) != 0 {
		t.Fatal("native RBD write image setup differs", setup.Stat, setup.Snapshots)
	}
	blobs := func(n *nativeRBDWrite) map[string][]byte {
		views := map[string][]byte{}
		for key, blob := range n.Blobs {
			views[key] = readRBDBlob(t, dir, blob)
		}
		if len(views) != 4 || uint64(len(views["image_head"])) != n.Stat.Size || uint64(len(views["image_snap"])) != plan.Size {
			t.Fatal("native RBD write binary views missing", n.Action)
		}
		return views
	}
	setupViews := blobs(setup)
	snapshot := setup.Snapshots[0]
	evidence.Image, evidence.SnapshotID = plan.Image, snapshot.ID

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
			}
			if op.Code == cephmsgr.OSDSetAllocHint {
				hint := op.AllocHint
				in.AllocHint = &hint
			}
			e.Inputs = append(e.Inputs, in)
		}
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
	now := func() cephmsgr.OSDTime {
		t := time.Now()
		return cephmsgr.OSDTime{Seconds: uint32(t.Unix()), Nanoseconds: uint32(t.Nanosecond())}
	}
	// call sends one CALL alone in its request. Write-mode methods carry a
	// modification time as librados would, and no SnapContext: librbd sends
	// header operations through its metadata context without one.
	call := func(t *testing.T, label, object, class, method string, mode cephmsgr.OSDCallMode, input []byte) []byte {
		t.Helper()
		r, o := route(t, object)
		op := cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 4096}}
		req := request(r, op)
		if mode == cephmsgr.OSDCallWrite {
			req.ModificationTime = now()
		}
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatalf("Go CALL %s.%s: %v", class, method, err)
		}
		checkReply(t, r, reply, 0, mode == cephmsgr.OSDCallWrite)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != uint8(len(class)) || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) || got.Class.OutputLength != uint64(len(got.Data)) ||
			mode == cephmsgr.OSDCallWrite && len(got.Data) != 0 {
			t.Fatalf("CALL %s.%s per-op result or union echo differs: %+v", class, method, got.Class)
		}
		evidence.ClassOutputs[label] = hex.EncodeToString(got.Data)
		return got.Data
	}
	// sameAsNative compares a Go input and raw output with one native
	// librados exec of the same method on the same object.
	sameAsNative := func(t *testing.T, n *nativeRBDWrite, index int, object, class, method string, input, output []byte) {
		t.Helper()
		e := n.ClassCalls[index]
		if e.Object != object || e.Class != class || e.Method != method || e.InputHex != hex.EncodeToString(input) || e.Code != 0 || e.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("Go CALL %s.%s differs from native librados exec (%s): native=%+v go_input=%x go_output=%x", class, method, n.Action, e, input, output)
		}
	}
	listWatchers := func(t *testing.T, label string, r rbdRoute, o *cephmsgr.OSDConnection) []cephmsgr.OSDWatcher {
		t.Helper()
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatal(label, err)
		}
		checkReply(t, r, reply, 0, false)
		return reply.Operations[0].Watchers
	}

	var imageID, prefix, header string
	var snapc cephmsgr.OSDSnapContext
	order := 0
	models := map[uint64]*rbdCloneModel{}
	var expectedHead []byte
	var objectNames []string
	t.Run("snapcontext_data_writes", func(t *testing.T) {
		idObject := "rbd_id." + plan.Image
		out := call(t, "get_id", idObject, "rbd", "get_id", cephmsgr.OSDCallRead, nil)
		sameAsNative(t, setup, 0, idObject, "rbd", "get_id", nil, out)
		d := &rbdDecoder{data: out}
		imageID = d.str()
		if err := d.done(); err != nil || imageID != setup.ImageID {
			t.Fatal("rbd_id get_id differs from native librbd image id", imageID, err)
		}
		header, evidence.ImageID = "rbd_header."+imageID, imageID
		// The write SnapContext comes from the header, as librbd builds it.
		out = call(t, "get_snapcontext", header, "rbd", "get_snapcontext", cephmsgr.OSDCallRead, nil)
		sameAsNative(t, setup, 5, header, "rbd", "get_snapcontext", nil, out)
		d = &rbdDecoder{data: out}
		snapc.Sequence = d.u64()
		for i, n := uint32(0), d.u32(); i < n && i < 64; i++ {
			snapc.Snapshots = append(snapc.Snapshots, d.u64())
		}
		if err := d.done(); err != nil || len(snapc.Snapshots) != 1 || snapc.Sequence != snapc.Snapshots[0] || snapc.Sequence != snapshot.ID {
			t.Fatal("get_snapcontext differs from native librbd snapshot list", snapc, snapshot, err)
		}
		evidence.SnapContext = snapc
		out = call(t, "get_object_prefix", header, "rbd", "get_object_prefix", cephmsgr.OSDCallRead, nil)
		sameAsNative(t, setup, 3, header, "rbd", "get_object_prefix", nil, out)
		d = &rbdDecoder{data: out}
		if prefix = d.str(); d.done() != nil || prefix != setup.Stat.Prefix || prefix != "rbd_data."+imageID {
			t.Fatal("get_object_prefix differs from native librbd block name prefix", prefix)
		}
		out = call(t, "get_size_head", header, "rbd", "get_size", cephmsgr.OSDCallRead, rbdLE64(rbdNoSnap))
		sameAsNative(t, setup, 1, header, "rbd", "get_size", rbdLE64(rbdNoSnap), out)
		d = &rbdDecoder{data: out}
		headOrder, headSize := d.u8(), d.u64()
		if err := d.done(); err != nil || int(headOrder) != setup.Stat.Order || headSize != plan.Size {
			t.Fatal("get_size(head) differs from native librbd stat", headOrder, headSize, err)
		}
		order = int(headOrder)
		objectSize := uint64(1) << order
		for i := range setup.Objects {
			name := fmt.Sprintf("%s.%016x", prefix, i)
			if setup.Objects[i].Name != name {
				t.Fatal("Go data object name differs from native librbd layout", name)
			}
			objectNames = append(objectNames, name)
		}
		// The clone model replays the native writes: before the snapshot
		// without a SnapContext, after it under the image's.
		for i := range objectNames {
			models[uint64(i)] = &rbdCloneModel{}
		}
		for _, e := range plan.PreWrites {
			for _, piece := range rbdObjectExtents(e.Offset, e.Length, order) {
				models[piece[0]].write(piece[1], piece[2], 0)
			}
		}
		for _, e := range plan.PostWrites {
			for _, piece := range rbdObjectExtents(e.Offset, e.Length, order) {
				models[piece[0]].write(piece[1], piece[2], snapc.Sequence)
			}
		}
		listSnaps := func(t *testing.T, label string, i int, into map[string]any) {
			t.Helper()
			r, o := route(t, objectNames[i])
			req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListSnapshots, ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}})
			reply, err := o.Request(ctx, req)
			record(fmt.Sprintf("%s_%d", label, i), r, req, reply, err)
			m := models[uint64(i)]
			if !m.Exists {
				var server *cephmsgr.OSDError
				if !errors.As(err, &server) || server.Code != -2 {
					t.Fatalf("%s object %d: absent object LIST_SNAPS %v, want ENOENT", label, i, err)
				}
				checkReply(t, r, reply, -2, false)
				into[objectNames[i]] = "ENOENT"
				return
			}
			if err != nil {
				t.Fatalf("%s object %d LIST_SNAPS: %v", label, i, err)
			}
			checkReply(t, r, reply, 0, false)
			got, want := reply.Operations[0].Snapshots, m.listing(snapc.Sequence)
			into[objectNames[i]] = got
			if !sameSnapshotList(got, want) {
				t.Fatalf("%s object %d LIST_SNAPS differs from the clone model: got %+v want %+v", label, i, got, want)
			}
		}
		for i := range objectNames {
			listSnaps(t, "list_snaps_before", i, evidence.ListSnapsBefore)
		}
		// The planned categories exist: a pre-snapshot object without a clone,
		// an object already cloned by the native post-snapshot write, and
		// absent objects.
		if m0, m1 := models[0], models[1]; !m0.Exists || m0.Clone || m0.Sequence != 0 || !m1.Clone || m1.Sequence != snapc.Sequence || models[2].Exists || models[3].Exists {
			t.Fatal("native image does not hold the planned clone states", *m0, *m1)
		}
		// No librbd instance holds the image while Go writes (the setup
		// closed it; read-only observations register no watch).
		if hr, ho := route(t, header); len(listWatchers(t, "list_watchers_before_writes", hr, ho)) != 0 {
			t.Fatal("the image header is watched while Go writes its data objects")
		}
		// Go writes the image extents as librbd's ObjectWriteRequest does
		// for an image without parent or object map: SETALLOCHINT (object
		// size, FAILOK as librados adds it) and WRITE, with the image
		// SnapContext and no ORDERSNAP.
		expectedHead = bytes.Clone(setupViews["image_head"])
		for _, e := range plan.GoWrites {
			data := rbdWritePattern(token, "go", e.Offset, e.Length)
			copy(expectedHead[e.Offset:], data)
			consumed := uint64(0)
			for _, piece := range rbdObjectExtents(e.Offset, e.Length, order) {
				number, offset, length := piece[0], piece[1], piece[2]
				chunk := data[consumed : consumed+length]
				consumed += length
				r, o := route(t, objectNames[number])
				req := request(r,
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: length, Data: chunk})
				req.SnapContext, req.ModificationTime = cephmsgr.OSDSnapContext{Sequence: snapc.Sequence, Snapshots: slices.Clone(snapc.Snapshots)}, now()
				reply, err := o.Request(ctx, req)
				label := fmt.Sprintf("go_write_%d_%d", number, offset)
				record(label, r, req, reply, err)
				if err != nil {
					t.Fatalf("Go RBD data write %s: %v", label, err)
				}
				checkReply(t, r, reply, 0, true)
				if reply.Operations[0].Result != 0 || reply.Operations[0].AllocHint != req.Operations[0].AllocHint || reply.Operations[1].Result != 0 || reply.Operations[1].Length != length {
					t.Fatalf("Go RBD data write %s per-op results differ", label)
				}
				models[number].write(offset, length, snapc.Sequence)
				sum := sha256.Sum256(chunk)
				evidence.Writes = append(evidence.Writes, rbdGoWriteEvidence{ImageOffset: e.Offset, ImageLength: e.Length, Object: objectNames[number], ObjectNumber: number,
					ObjectOffset: offset, Length: length, SHA256: hex.EncodeToString(sum[:]), Transaction: reply.Transaction, UserVersion: reply.UserVersion})
			}
		}
		for i := range objectNames {
			listSnaps(t, "list_snaps_after", i, evidence.ListSnapsAfter)
		}
		// Copy-on-write happened only where the snapshot needed it: object
		// 0 gained a clone, object 1 kept its clone, objects 2 and 3 were
		// created under the SnapContext without one.
		if m0, m1, m2, m3 := models[0], models[1], models[2], models[3]; !m0.Clone || !m1.Clone || m2.Clone || m3.Clone || !m2.Exists || !m3.Exists || m0.Sequence != snapc.Sequence || m2.Sequence != snapc.Sequence {
			t.Fatal("Go writes did not reach the planned clone states")
		}
		sum := sha256.Sum256(expectedHead)
		evidence.ExpectedHeadSHA = hex.EncodeToString(sum[:])
		sum = sha256.Sum256(setupViews["image_snap"])
		evidence.ExpectedSnapSHA = hex.EncodeToString(sum[:])
		// Go reads of the head and of the snapshot.
		goObjects := map[string][][]byte{}
		for _, label := range []string{"head", "snapshot"} {
			for i, name := range objectNames {
				r, o := route(t, name)
				req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
				image := expectedHead
				if label == "snapshot" {
					req.ReadSnapshot, image = snapc.Sequence, setupViews["image_snap"]
				}
				reply, err := o.Request(ctx, req)
				record(fmt.Sprintf("read_%s_%d", label, i), r, req, reply, err)
				m := models[uint64(i)]
				exists, size := m.Exists, m.Size
				if label == "snapshot" {
					exists, size = m.Clone || m.Exists && m.Sequence < snapc.Sequence, m.CloneSize
					if !m.Clone {
						size = m.Size
					}
				}
				if !exists {
					var server *cephmsgr.OSDError
					if !errors.As(err, &server) || server.Code != -2 {
						t.Fatalf("%s object %d: model says absent, Go %v", label, i, err)
					}
					checkReply(t, r, reply, -2, false)
					goObjects[label] = append(goObjects[label], nil)
					continue
				}
				if err != nil {
					t.Fatalf("%s object %d: %v", label, i, err)
				}
				checkReply(t, r, reply, 0, false)
				data := reply.Operations[1].Data
				extent := image[uint64(i)*objectSize : uint64(i+1)*objectSize]
				if binary.LittleEndian.Uint64(reply.Operations[0].Data) != size || uint64(len(data)) != size || !bytes.Equal(data, extent[:size]) || !bytes.Equal(extent[size:], make([]byte, objectSize-size)) {
					t.Fatalf("%s object %d Go read differs from the expected image extent (size %d)", label, i, size)
				}
				goObjects[label] = append(goObjects[label], data)
			}
		}
		// Native librbd and librados observe the result independently.
		written := checkNative(t, "observe-written", native(t, "observe-written"))
		views := blobs(written)
		sum = sha256.Sum256(views["image_head"])
		evidence.NativeWrittenHead = hex.EncodeToString(sum[:])
		if !bytes.Equal(views["image_head"], expectedHead) {
			t.Fatal("native librbd head read lacks the Go writes")
		}
		if !bytes.Equal(views["image_snap"], setupViews["image_snap"]) {
			t.Fatal("native librbd snapshot read changed after the Go writes")
		}
		for i, o := range written.Objects {
			for _, label := range []string{"head", "snapshot"} {
				view, blob := o.Head, views["objects_head"]
				if label == "snapshot" {
					view, blob = o.Snap, views["objects_snap"]
				}
				goData := goObjects[label][i]
				if view.Exists != (goData != nil) || view.Exists && !bytes.Equal(blob[view.Offset:view.Offset+view.Size], goData) {
					t.Fatalf("native librados %s object %d differs from the Go read", label, i)
				}
			}
		}
		if written.Stat.Size != plan.Size || !written.Lockers.Empty || len(written.Metadata) != 0 || len(written.Watchers) != 0 || written.ClassCalls[5].OutputHex != setup.ClassCalls[5].OutputHex {
			t.Fatal("native observation after the Go writes differs", written.Stat, written.ClassCalls[5])
		}
		t.Logf("SnapContext seq %d snaps %v; %d Go object writes; LIST_SNAPS after %v", snapc.Sequence, snapc.Snapshots, len(evidence.Writes), evidence.ListSnapsAfter)
	})
	if t.Failed() {
		return
	}
	var resized *nativeRBDWrite
	t.Run("rbd_class_writes", func(t *testing.T) {
		// librbd's ResizeRequest grows a layering-only image with a single
		// rbd.set_size on the header (no object-map or trim step), and
		// MetadataSetRequest sends one rbd.metadata_set.
		call(t, "set_size_grow", header, "rbd", "set_size", cephmsgr.OSDCallWrite, rbdLE64(plan.GoResize))
		call(t, "metadata_set", header, "rbd", "metadata_set", cephmsgr.OSDCallWrite, rbdMetadataSetInput(plan.MetadataKey, plan.MetadataValue))
		headSize := call(t, "get_size_head_grown", header, "rbd", "get_size", cephmsgr.OSDCallRead, rbdLE64(rbdNoSnap))
		if d := (&rbdDecoder{data: headSize}); d.u8() != uint8(order) || d.u64() != plan.GoResize || d.done() != nil {
			t.Fatal("get_size(head) after Go set_size differs", headSize)
		}
		snapSize := call(t, "get_size_snapshot_grown", header, "rbd", "get_size", cephmsgr.OSDCallRead, rbdLE64(snapc.Sequence))
		if d := (&rbdDecoder{data: snapSize}); d.u8() != uint8(order) || d.u64() != plan.Size || d.done() != nil {
			t.Fatal("get_size(snapshot) changed with the head resize", snapSize)
		}
		value := call(t, "metadata_get", header, "rbd", "metadata_get", cephmsgr.OSDCallRead, rbdString(plan.MetadataKey))
		if d := (&rbdDecoder{data: value}); d.str() != plan.MetadataValue || d.done() != nil {
			t.Fatal("metadata_get differs from the Go metadata_set", value)
		}
		list := call(t, "metadata_list", header, "rbd", "metadata_list", cephmsgr.OSDCallRead, rbdMetadataListInput())
		if meta, err := decodeRBDMetadata(list); err != nil || !reflect.DeepEqual(meta, map[string]string{plan.MetadataKey: plan.MetadataValue}) {
			t.Fatal("metadata_list differs from the Go metadata_set", meta, err)
		}
		resized = checkNative(t, "observe-resized", native(t, "observe-resized"))
		views := blobs(resized)
		sameAsNative(t, resized, 1, header, "rbd", "get_size", rbdLE64(rbdNoSnap), headSize)
		sameAsNative(t, resized, 2, header, "rbd", "get_size", rbdLE64(snapc.Sequence), snapSize)
		sameAsNative(t, resized, 6, header, "rbd", "metadata_list", rbdMetadataListInput(), list)
		if resized.Stat.Size != plan.GoResize || resized.Stat.Objects != plan.GoResize>>order || len(resized.Snapshots) != 1 || resized.Snapshots[0] != snapshot ||
			len(resized.Metadata) != 1 || resized.Metadata[0].Key != plan.MetadataKey || resized.Metadata[0].Value != plan.MetadataValue || !resized.Lockers.Empty {
			t.Fatal("native librbd does not reflect the Go header writes", resized.Stat, resized.Metadata)
		}
		// The grown range reads as zeros; the old range and the snapshot are
		// unchanged.
		if !bytes.Equal(views["image_head"][:plan.Size], expectedHead) || !bytes.Equal(views["image_head"][plan.Size:], make([]byte, plan.GoResize-plan.Size)) ||
			!bytes.Equal(views["image_snap"], setupViews["image_snap"]) || resized.Objects[len(resized.Objects)-1].Head.Exists {
			t.Fatal("native librbd reads of the grown image differ")
		}
		t.Logf("set_size %d and metadata %s=%s visible to native librbd (stat size %d, %d objects)", plan.GoResize, plan.MetadataKey, plan.MetadataValue, resized.Stat.Size, resized.Stat.Objects)
	})
	if t.Failed() {
		return
	}
	t.Run("header_lock", func(t *testing.T) {
		r, o := route(t, header)
		if watchers := listWatchers(t, "list_watchers_before_lock", r, o); len(watchers) != 0 {
			t.Fatal("header has watchers while no client has the image open", watchers)
		}
		// librbd's ManagedLock takes the lock with its header watch handle in
		// the cookie; the Go watch handle is its cookie.
		watchRequest := request(r)
		watchRequest.Operations = nil
		w, reply, err := o.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		record("lock_watch_register", r, watchRequest, reply, err)
		if err != nil {
			t.Fatal("Go header watch", err)
		}
		watching, lockSent, unlocked := true, false, false
		cookie := rbdLockCookie(w.Cookie())
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if lockSent && !unlocked {
				// A compensating unlock, never a repeated lock.
				req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: rbdLockUnlockInput(cookie), Class: cephmsgr.OSDClassInput{Class: "lock", Method: "unlock", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}})
				reply, err := o.Request(cleanup, req)
				record("lock_unlock_cleanup", r, req, reply, err)
				t.Log("cleanup unlock after a failure:", err)
			}
			if watching {
				if _, err := w.Unwatch(cleanup, r.Epoch); err != nil {
					t.Error("Go header unwatch cleanup", err)
				}
			}
		}()
		watchers := listWatchers(t, "list_watchers_go_lock_watch", r, o)
		if len(watchers) != 1 || watchers[0].Cookie != w.Cookie() || uint64(watchers[0].NameNumber) != gid || watchers[0].NameType != rbdEntityClient || !watchers[0].Address.Endpoint.Addr().IsValid() {
			t.Fatal("header watcher listing lacks the Go registration", watchers)
		}
		goWatcher := watchers[0]
		lockEvidence := &rbdLockEvidence{WatchCookie: w.Cookie(), Cookie: cookie, LockInputHex: hex.EncodeToString(rbdLockLockInput(cookie)), UnlockInputHex: hex.EncodeToString(rbdLockUnlockInput(cookie)), GoWatcher: &goWatcher}
		evidence.Lock = lockEvidence
		before := call(t, "lock_get_info_before", header, "lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput())
		lockEvidence.InfoBefore = hex.EncodeToString(before)
		sameAsNative(t, resized, 7, header, "lock", "get_info", rbdLockGetInfoInput(), before)
		if info, err := decodeRBDLockInfo(before); err != nil || len(info.Lockers) != 0 || info.Type != 0 || info.Tag != "" {
			t.Fatal("header holds a lock before the Go lock", info, err)
		}
		lockSent = true
		call(t, "lock_lock", header, "lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(cookie))
		locked := call(t, "lock_get_info_locked", header, "lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput())
		lockEvidence.InfoLocked = hex.EncodeToString(locked)
		info, err := decodeRBDLockInfo(locked)
		if err != nil || len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != rbdLockTag {
			t.Fatal("lock.get_info lacks the Go exclusive lock", info, err)
		}
		lockedInfo := info
		lockEvidence.Locked = &lockedInfo
		l := info.Lockers[0]
		// cls_lock records the locker's entity and its address as
		// TYPE_LEGACY; endpoint and nonce are the watch connection's.
		if l.NameType != rbdEntityClient || uint64(l.NameNumber) != gid || l.Cookie != cookie || l.ExpirationSeconds != 0 || l.ExpirationNanos != 0 || l.Description != "" ||
			l.Address.Type != rbdAddrTypeLegacy || l.Address.Nonce != goWatcher.Address.Nonce || l.Address.Endpoint != goWatcher.Address.Endpoint {
			t.Fatal("Go locker differs from the Go watch registration", l, goWatcher)
		}
		// librbd's managed-lock rules (GetLockerRequest, BreakRequest): the
		// internal tag, an exclusive type, an "auto <handle>" cookie, a
		// non-blank address, and a live owner is a watcher with the same
		// address and cookie == handle.
		handle, ok := rbdLockHandle(l.Cookie)
		if !ok || handle != w.Cookie() || l.Address.Endpoint.Addr().IsUnspecified() {
			t.Fatal("Go lock is not in librbd's managed-lock format", l)
		}
		lockedNative := checkNative(t, "observe-locked", native(t, "observe-locked"))
		sameAsNative(t, lockedNative, 7, header, "lock", "get_info", rbdLockGetInfoInput(), locked)
		lockers := lockedNative.Lockers
		lockEvidence.NativeLockers = &lockers
		legacy := fmt.Sprintf("%s/%d", l.Address.Endpoint, l.Address.Nonce)
		if lockers.Empty || !lockers.Exclusive || lockers.Tag != rbdLockTag || len(lockers.Lockers) != 1 || lockers.Lockers[0].Client != fmt.Sprintf("client.%d", gid) ||
			lockers.Lockers[0].Cookie != cookie || lockers.Lockers[0].Address != legacy {
			t.Fatal("native librbd list_lockers differs from the Go lock", lockers, legacy)
		}
		alive := slices.ContainsFunc(lockedNative.Watchers, func(n nativeRBDWatcher) bool {
			return n.Address == lockers.Lockers[0].Address && n.Cookie == handle && n.ID == gid
		})
		if len(lockedNative.Watchers) != 1 || !alive {
			t.Fatal("native librbd watchers_list does not show the Go lock owner alive", lockedNative.Watchers, lockers)
		}
		call(t, "lock_unlock", header, "lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie))
		unlocked = true
		after := call(t, "lock_get_info_unlocked", header, "lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput())
		lockEvidence.InfoUnlocked = hex.EncodeToString(after)
		unlockedInfo, err := decodeRBDLockInfo(after)
		if err != nil || len(unlockedInfo.Lockers) != 0 {
			t.Fatal("lock.get_info after the Go unlock still lists lockers", unlockedInfo, err)
		}
		lockEvidence.Unlocked = &unlockedInfo
		unlockedNative := checkNative(t, "observe-unlocked", native(t, "observe-unlocked"))
		sameAsNative(t, unlockedNative, 7, header, "lock", "get_info", rbdLockGetInfoInput(), after)
		if !unlockedNative.Lockers.Empty || len(unlockedNative.Lockers.Lockers) != 0 {
			t.Fatal("native librbd still lists lockers after the Go unlock", unlockedNative.Lockers)
		}
		unwatch, err := w.Unwatch(ctx, r.Epoch)
		record("lock_watch_unwatch", r, watchRequest, unwatch, err)
		if err != nil {
			t.Fatal("Go header unwatch", err)
		}
		watching = false
		if watchers := listWatchers(t, "list_watchers_after_lock", r, o); len(watchers) != 0 {
			t.Fatal("Go unwatch left header watchers", watchers)
		}
		t.Logf("lock cookie %q by client.%d at %s (watch type %d); get_info after unlock: type %d tag %q", cookie, gid, legacy, goWatcher.Address.Type, unlockedInfo.Type, unlockedInfo.Tag)
	})
	if t.Failed() {
		return
	}
	t.Run("notify_librbd_refresh", func(t *testing.T) {
		r, o := route(t, header)
		statePath := filepath.Join(dir, "rbd-write-watch-state.json")
		for _, name := range []string{statePath, filepath.Join(dir, "rbd-write-watch-proceed-1"), filepath.Join(dir, "rbd-write-watch-proceed-2"), filepath.Join(dir, "rbd-write-watch.json")} {
			if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
		var controlErr error
		controlDone := make(chan struct{})
		go func() {
			controlErr = testcluster.RequestControl(ctx, dir, "rbd-write", "rbd-write-completed", token, "watch")
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
		state, opened, err := waitRBDWriteState(ctx, statePath, "opened", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		evidence.WatchState = &state
		if state.Token != token || state.NativeGlobalID == 0 || state.NativeGlobalID == gid || state.RequestID <= 0 || opened.Size != plan.GoResize || opened.StatSize != plan.GoResize || len(opened.Watchers) != 1 {
			t.Fatal("native RBD write watch scope differs", state)
		}
		librbd := opened.Watchers[0]
		listed := listWatchers(t, "list_watchers_librbd_open", r, o)
		if len(listed) != 1 || uint64(listed[0].NameNumber) != state.NativeGlobalID || librbd.ID != state.NativeGlobalID || listed[0].Cookie != librbd.Cookie ||
			!strings.HasSuffix(librbd.Address, fmt.Sprintf("%s/%d", listed[0].Address.Endpoint, listed[0].Address.Nonce)) {
			t.Fatal("Go LIST_WATCHERS differs from librbd's header watch", listed, opened.Watchers)
		}
		proceed := func(step int) {
			t.Helper()
			signal := fmt.Sprintf("%s %d %d", token, state.RequestID, step)
			if err := testcluster.WritePublicEvidence(filepath.Join(dir, fmt.Sprintf("rbd-write-watch-proceed-%d", step)), []byte(signal)); err != nil {
				t.Fatal(err)
			}
		}
		// A second Go resize while librbd holds the image open: the header
		// changes, but librbd refreshes only on a header-update notify.
		call(t, "set_size_grow_watched", header, "rbd", "set_size", cephmsgr.OSDCallWrite, rbdLE64(plan.GoResizeWatched))
		headSize := call(t, "get_size_head_watched", header, "rbd", "get_size", cephmsgr.OSDCallRead, rbdLE64(rbdNoSnap))
		if d := (&rbdDecoder{data: headSize}); d.u8() != uint8(order) || d.u64() != plan.GoResizeWatched || d.done() != nil {
			t.Fatal("get_size(head) after the watched Go set_size differs", headSize)
		}
		proceed(1)
		state, stale, err := waitRBDWriteState(ctx, statePath, "stale", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		if stale.Size != plan.GoResize || stale.StatSize != plan.GoResize || stale.HeaderSize == nil || stale.HeaderSize.Code != 0 || stale.HeaderSize.OutputHex != hex.EncodeToString(headSize) {
			t.Fatal("librbd did not keep its cached size while the header changed", stale)
		}
		// Go notifies as librbd's ImageWatcher::notify_header_update does:
		// NotifyMessage(HeaderUpdatePayload) with librbd's 5 s timeout.
		notifyRequest := request(r)
		notifyRequest.Operations = nil
		notifyStarted := time.Now()
		n, reply, err := o.NotifyObject(ctx, notifyRequest, rbdHeaderUpdateNotify, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
		record("notify_header_update", r, notifyRequest, reply, err)
		if err != nil {
			t.Fatal("Go header-update notify", err)
		}
		defer n.Close()
		checkReply(t, r, reply, 0, false)
		notifyID := binary.LittleEndian.Uint64(reply.Operations[0].Data)
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		completion, err := n.Next(wait)
		if err != nil {
			t.Fatal("Go notify completion", err)
		}
		completed := time.Since(notifyStarted)
		notify := &rbdNotifyEvidence{TimeoutSeconds: 5, PayloadHex: hex.EncodeToString(rbdHeaderUpdateNotify), Cookie: n.Cookie(), NotifyID: notifyID, ReplyDataHex: hex.EncodeToString(reply.Operations[0].Data),
			Opcode: completion.Opcode, CompletionID: completion.NotifyID, ReturnCode: completion.ReturnCode, Missed: completion.Missed,
			RawFrontHex: hex.EncodeToString(completion.RawFront), RawDataHex: hex.EncodeToString(completion.RawData), CompletedAfterMS: float64(completed.Microseconds()) / 1000}
		for _, reply := range completion.Replies {
			notify.Replies = append(notify.Replies, rbdNotifyReplyEvidence{GlobalID: reply.GlobalID, Cookie: reply.Cookie, DataHex: hex.EncodeToString(reply.Data)})
		}
		evidence.Notify = notify
		// librbd's ImageWatcher acknowledges a HeaderUpdate with
		// ResponseMessage(0) after marking the image for refresh.
		if completion.Opcode != cephmsgr.OSDWatchCompletion || completion.NotifyID != notifyID || completion.Cookie != n.Cookie() || completion.ReturnCode != 0 || len(completion.Missed) != 0 ||
			len(completion.Replies) != 1 || completion.Replies[0].GlobalID != state.NativeGlobalID || completion.Replies[0].Cookie != librbd.Cookie || !bytes.Equal(completion.Replies[0].Data, rbdResponseAck) ||
			completed >= rbdNotifyTimeout {
			t.Fatalf("librbd acknowledgement of the Go notify differs: %+v", notify)
		}
		if _, err := n.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDNotifyComplete) {
			t.Fatal("notify stream did not end after its completion", err)
		}
		proceed(2)
		state, refreshed, err := waitRBDWriteState(ctx, statePath, "refreshed", controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		evidence.WatchState = &state
		if refreshed.Size != plan.GoResizeWatched || refreshed.StatSize != plan.GoResizeWatched || len(refreshed.Watchers) != 1 {
			t.Fatal("librbd did not refresh after acknowledging the Go notify", refreshed)
		}
		select {
		case <-controlDone:
			err = controlErr
		case <-time.After(30 * time.Second):
			err = errors.New("native RBD write watch action did not complete")
		}
		if err != nil {
			t.Fatal("native RBD write watch action", err)
		}
		var result nativeRBDWriteResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-write-watch.json"), &result); err != nil {
			t.Fatal(err)
		}
		final := checkNative(t, "watch", result)
		sameAsNative(t, final, 1, header, "rbd", "get_size", rbdLE64(rbdNoSnap), headSize)
		if final.NativeGlobalID != state.NativeGlobalID || final.Stat.Size != plan.GoResizeWatched || len(final.Snapshots) != 1 || final.Snapshots[0] != snapshot || len(final.Phases) != 3 ||
			strings.Contains(string(result.Process.Stderr), "update notification timed-out") {
			t.Fatal("native RBD write watch receipt differs", final.Stat, result.Process.Stderr)
		}
		if watchers := listWatchers(t, "list_watchers_librbd_closed", r, o); len(watchers) != 0 {
			t.Fatal("librbd close left header watchers", watchers)
		}
		evidence.Timings["notify_completion"] = completed.String()
		t.Logf("notify ID %d acknowledged by librbd client.%d cookie %d with %x after %v; librbd size %d -> %d without reopening", notifyID, state.NativeGlobalID, librbd.Cookie, completion.Replies[0].Data, completed, stale.Size, refreshed.Size)
	})
}

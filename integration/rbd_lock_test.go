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
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephRBDExclusiveLockIntegration checks that the explicit OSD API
// carries librbd's exclusive-lock protocol, against a third image created by
// native librbd in the RBD base fixture's owned pool with the exclusive-lock,
// object-map and fast-diff features. Native librbd holds the image open
// read-write and owns the lock; Go reads the owner from lock.get_info and
// LIST_WATCHERS, asks for the lock with a RequestLock notify on its own header
// watch, takes the lock in librbd's managed-lock format once native librbd
// releases it, announces itself with AcquiredLock, updates the object map and
// writes a data object as librbd's writer would, and releases the lock
// (object-map unlock, header unlock, ReleasedLock) when native librbd's next
// write asks for it. Native librbd, librados and `rbd object-map check`
// observe every step independently. See
// integration/fixture/RBD_BASE_FIXTURE.md and RBD_LOCK_VALIDATION.md.
//
// Everything RBD-specific below is test-only consumer logic: the watch-notify
// payloads, the lock-owner rules, the lock and object-map class encodings,
// the BitVector decoding and the image layout. The product carries raw class
// names, inputs and outputs, raw notify payloads and acknowledgements and raw
// object bytes only. Encodings restate the pinned
// librbd/WatchNotifyTypes.{h,cc}, librbd/watcher/Types.cc,
// librbd/ImageWatcher.cc, librbd/image_watcher/NotifyLockOwner.cc,
// librbd/ExclusiveLock.cc, librbd/managed_lock/{AcquireRequest,BreakRequest,
// GetLockerRequest,ReleaseRequest}.cc, librbd/object_map/{LockRequest,
// UnlockRequest,UpdateRequest,RefreshRequest}.cc, librbd/io/ObjectRequest.cc,
// cls/lock/cls_lock_ops.h, cls/lock/cls_lock.cc, cls/rbd/cls_rbd.cc,
// cls/rbd/cls_rbd_client.cc, common/bit_vector.hpp and include/rbd/
// object_map_types.h (LGPL-2.1 references; no code copied).

const (
	// librbd watch_notify::NotifyOp.
	rbdNotifyAcquiredLock = 0
	rbdNotifyReleasedLock = 1
	rbdNotifyRequestLock  = 2
	rbdNotifyHeaderUpdate = 3
	// include/rbd/object_map_types.h.
	rbdObjectNonexistent = 0
	rbdObjectExists      = 1
	// RBD_FEATURE_LAYERING | EXCLUSIVE_LOCK | OBJECT_MAP | FAST_DIFF.
	rbdLockImageFeatures = 1 | 4 | 8 | 16
	// librbd's client_notify_timeout default: a lock owner that acknowledged
	// a RequestLock but released nothing is asked again after it.
	rbdRequestLockRetry = 10 * time.Second
	// RBD_LOCK_MODE_EXCLUSIVE as lock_get_owners reports it.
	rbdLockModeExclusive = 0
	// The Go header watch pings at least this often (its timeout is 30 s).
	rbdLockPingInterval = 5 * time.Second
)

type nativeRBDLockPlan struct {
	Pool            string            `json:"pool"`
	BaseImage       string            `json:"base_image"`
	WriteImage      string            `json:"write_image"`
	Image           string            `json:"image"`
	Order           int               `json:"order"`
	Size            uint64            `json:"size"`
	Features        uint64            `json:"features"`
	SetupWrites     []nativeRBDExtent `json:"setup_writes"`
	OwnerWrites     []nativeRBDExtent `json:"owner_writes"`
	GoWrites        []nativeRBDExtent `json:"go_writes"`
	ReacquireWrites []nativeRBDExtent `json:"reacquire_writes"`
}

type nativeRBDLockWrite struct {
	Phase     string `json:"phase"`
	Offset    uint64 `json:"offset"`
	Length    uint64 `json:"length"`
	Written   uint64 `json:"written"`
	SHA256    string `json:"sha256"`
	ElapsedNS int64  `json:"elapsed_ns"`
}

type nativeRBDLockRaw struct {
	Object string `json:"object"`
	Exists bool   `json:"exists"`
	Hex    string `json:"hex"`
}

type nativeRBDLockDiff struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Exists bool   `json:"exists"`
}

type nativeRBDLockOwner struct {
	Mode  int    `json:"mode"`
	Owner string `json:"owner"`
}

type nativeRBDLockPhase struct {
	Phase      string                  `json:"phase"`
	IsOwner    bool                    `json:"is_owner"`
	Owners     []nativeRBDLockOwner    `json:"owners"`
	Watchers   []nativeRBDWatcher      `json:"watchers"`
	Lockers    nativeRBDWriteLockers   `json:"lockers"`
	Flags      uint64                  `json:"flags"`
	Size       uint64                  `json:"size"`
	Diff       []nativeRBDLockDiff     `json:"diff"`
	HeaderLock nativeRBDWriteClassCall `json:"header_lock"`
	MapLock    nativeRBDWriteClassCall `json:"map_lock"`
	MapRaw     nativeRBDLockRaw        `json:"map_raw"`
	WaitNS     int64                   `json:"wait_ns"`
	Writes     []nativeRBDLockWrite    `json:"writes"`
	WriteNS    int64                   `json:"write_ns"`
	GoExtents  []struct {
		Offset uint64 `json:"offset"`
		Length uint64 `json:"length"`
		SHA256 string `json:"sha256"`
	} `json:"go_extents"`
	CloseNS int64 `json:"close_ns"`
}

type nativeRBDLockCheck struct {
	Argv       []string `json:"argv"`
	ReturnCode int      `json:"returncode"`
	Stdout     string   `json:"stdout"`
	Stderr     string   `json:"stderr"`
	ElapsedNS  int64    `json:"elapsed_ns"`
}

type nativeRBDLock struct {
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
	MapObject      string `json:"map_object"`
	Stat           struct {
		Size       uint64 `json:"size"`
		ObjectSize uint64 `json:"obj_size"`
		Objects    uint64 `json:"num_objs"`
		Order      int    `json:"order"`
		Prefix     string `json:"block_name_prefix"`
	} `json:"stat"`
	Features   uint64                    `json:"features"`
	Flags      uint64                    `json:"flags"`
	Snapshots  []json.RawMessage         `json:"snapshots"`
	Lockers    nativeRBDWriteLockers     `json:"lockers"`
	Watchers   []nativeRBDWatcher        `json:"watchers"`
	Diff       []nativeRBDLockDiff       `json:"diff"`
	ClassCalls []nativeRBDWriteClassCall `json:"class_calls"`
	MapRaw     nativeRBDLockRaw          `json:"map_raw"`
	Writes     []nativeRBDLockWrite      `json:"writes"`
	Objects    []struct {
		Index uint64              `json:"index"`
		Name  string              `json:"name"`
		Head  nativeRBDObjectView `json:"head"`
	} `json:"objects"`
	Blobs  map[string]nativeRBDBlob `json:"blobs"`
	Phases []nativeRBDLockPhase     `json:"phases"`
	Check  *nativeRBDLockCheck      `json:"check"`
}

type nativeRBDLockResult struct {
	Source    string            `json:"source_commit"`
	Token     string            `json:"token"`
	RequestID int64             `json:"request_id"`
	Action    string            `json:"action"`
	Plan      nativeRBDLockPlan `json:"plan"`
	Native    *nativeRBDLock    `json:"native"`
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

type nativeRBDLockState struct {
	Token          string               `json:"token"`
	RequestID      int64                `json:"request_id"`
	NativeGlobalID uint64               `json:"native_global_id"`
	Phase          string               `json:"phase"`
	Phases         []nativeRBDLockPhase `json:"phases"`
}

// Test-only watch-notify encodings.

// rbdClientID is librbd's watcher::ClientId: the client's global ID and its
// header watch handle (the watch cookie), both u64 without an envelope.
type rbdClientID struct{ GID, Handle uint64 }

func (c rbdClientID) encode() []byte {
	return binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, c.GID), c.Handle)
}

// rbdNotifyMessage is NotifyMessage: ENCODE_START(7, 1), u32 notify op and
// the payload.
func rbdNotifyMessage(op uint32, payload []byte) []byte {
	return rbdEnvelopeV(7, append(binary.LittleEndian.AppendUint32(nil, op), payload...))
}

// rbdEnvelopeV is ENCODE_START(version, 1) around payload.
func rbdEnvelopeV(version uint8, payload []byte) []byte {
	return append(binary.LittleEndian.AppendUint32([]byte{version, 1}, uint32(len(payload))), payload...)
}

// rbdLockNotify is AcquiredLockPayload or ReleasedLockPayload: the ClientId.
func rbdLockNotify(op uint32, id rbdClientID) []byte { return rbdNotifyMessage(op, id.encode()) }

// rbdRequestLockNotify is RequestLockPayload: the ClientId and a bool force.
func rbdRequestLockNotify(id rbdClientID, force bool) []byte {
	flag := byte(0)
	if force {
		flag = 1
	}
	return rbdNotifyMessage(rbdNotifyRequestLock, append(id.encode(), flag))
}

// rbdResponseMessage is ResponseMessage: ENCODE_START(1, 1) and an int32.
func rbdResponseMessage(result int32) []byte {
	return rbdEnvelope(binary.LittleEndian.AppendUint32(nil, uint32(result)))
}

type rbdNotifyDecoded struct {
	Version uint8
	Op      uint32
	Client  rbdClientID
	Force   bool
	Known   bool
}

// decodeRBDNotifyMessage restates NotifyMessage::decode for the lock and
// header-update payloads (DECODE_START(1): compat 1). An empty notification
// is librbd's legacy header update. Payload fields are present from version
// 2 (ClientId) and 3 (force); other notify ops are reported as unknown.
func decodeRBDNotifyMessage(data []byte) (rbdNotifyDecoded, error) {
	if len(data) == 0 {
		return rbdNotifyDecoded{Op: rbdNotifyHeaderUpdate, Known: true}, nil
	}
	var m rbdNotifyDecoded
	d := &rbdDecoder{data: data}
	version, body := d.header(1)
	m.Version, m.Op = version, body.u32()
	switch m.Op {
	case rbdNotifyAcquiredLock, rbdNotifyReleasedLock, rbdNotifyRequestLock:
		if version >= 2 {
			m.Client = rbdClientID{GID: body.u64(), Handle: body.u64()}
		}
		if m.Op == rbdNotifyRequestLock && version >= 3 {
			switch body.u8() {
			case 0:
			case 1:
				m.Force = true
			default:
				body.err = errors.New("RequestLock force is not a bool")
			}
		}
		m.Known = true
	case rbdNotifyHeaderUpdate:
		m.Known = true
	default:
		body.data = nil
	}
	d.adopt(body)
	return m, d.done()
}

func decodeRBDResponseMessage(data []byte) (int32, error) {
	d := &rbdDecoder{data: data}
	_, body := d.header(1)
	result := int32(body.u32())
	d.adopt(body)
	return result, d.done()
}

// Test-only lock and object-map class encodings.

// rbdLockLockInputFor is cls_lock_lock_op for the exclusive rbd_lock with
// the given cookie and tag, an empty description, zero duration and flags:
// librbd's managed lock uses "auto <handle>" and "internal" on the header,
// its object-map LockRequest an empty cookie and tag on the object map.
func rbdLockLockInputFor(cookie, tag string) []byte {
	p := append(rbdString(rbdLockName), rbdLockExclusive)
	p = append(append(append(p, rbdString(cookie)...), rbdString(tag)...), rbdString("")...)
	return rbdEnvelope(append(p, make([]byte, 9)...))
}

// rbdLockAssertInput is cls_lock_assert_op: name, u8 type, cookie and tag.
func rbdLockAssertInput(cookie, tag string) []byte {
	p := append(rbdString(rbdLockName), rbdLockExclusive)
	return rbdEnvelope(append(append(p, rbdString(cookie)...), rbdString(tag)...))
}

// rbdObjectMapUpdateInput is cls_rbd object_map_update's input: start and
// end object numbers, the new state and a boost::optional current state
// (a u8 presence flag, then the value).
func rbdObjectMapUpdateInput(start, end uint64, state uint8, current *uint8) []byte {
	p := append(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, start), end), state)
	if current == nil {
		return append(p, 0)
	}
	return append(p, 1, *current)
}

var rbdCastagnoli = crc32.MakeTable(crc32.Castagnoli)

// rbdCRC32C is ceph_crc32c(0, data): the reflected Castagnoli CRC without the
// initial and final inversion.
func rbdCRC32C(data []byte) uint32 { return ^crc32.Update(^uint32(0), rbdCastagnoli, data) }

type rbdObjectMap struct {
	Size      uint64
	States    []uint8
	CRC       bool
	HeaderCRC uint32
	DataCRCs  []uint32
}

// decodeRBDObjectMap restates BitVector<2>::decode: a bufferlist-wrapped
// header (ENCODE_START(1, 1) with the u64 element count), the data bytes (four
// two-bit elements per byte, the first in the high bits) and a
// bufferlist-wrapped footer that is empty without CRCs or holds the header
// CRC, the block count and one CRC per 4 KiB data block. CRCs are verified as
// BitVector does.
func decodeRBDObjectMap(raw []byte) (rbdObjectMap, error) {
	var m rbdObjectMap
	d := &rbdDecoder{data: raw}
	headerBL := d.take(int(d.u32()))
	h := &rbdDecoder{data: headerBL}
	_, body := h.header(1)
	m.Size = body.u64()
	h.adopt(body)
	if err := h.done(); err != nil || d.err != nil {
		return m, errors.Join(err, d.err)
	}
	if m.Size > 1<<16 {
		return m, errors.New("object map exceeds its bound")
	}
	data := d.take(int((m.Size + 3) / 4))
	footer := &rbdDecoder{data: d.take(int(d.u32()))}
	if err := d.done(); err != nil {
		return m, err
	}
	for i := uint64(0); i < m.Size; i++ {
		m.States = append(m.States, data[i/4]>>((3-i%4)*2)&3)
	}
	if len(footer.data) == 0 {
		return m, nil
	}
	m.CRC, m.HeaderCRC = true, footer.u32()
	n := footer.u32()
	for i := uint32(0); i < n && i < 64 && footer.err == nil; i++ {
		m.DataCRCs = append(m.DataCRCs, footer.u32())
	}
	if err := footer.done(); err != nil {
		return m, err
	}
	if m.HeaderCRC != rbdCRC32C(headerBL) {
		return m, errors.New("object map header CRC differs")
	}
	if len(m.DataCRCs) != (len(data)+4095)/4096 {
		return m, errors.New("object map data CRC count differs")
	}
	for i, crc := range m.DataCRCs {
		if crc != rbdCRC32C(data[i*4096:min(len(data), (i+1)*4096)]) {
			return m, fmt.Errorf("object map data block %d CRC differs", i)
		}
	}
	return m, nil
}

// rbdLockPattern restates the native lock driver's token-bound data stream.
func rbdLockPattern(token, phase string, offset, length uint64) []byte {
	var data []byte
	for counter := 0; uint64(len(data)) < length; counter++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "ceph-msgr-rbd-lock:%s:%s:%d:%d", token, phase, offset, counter))
		data = append(data, sum[:]...)
	}
	return data[:length]
}

// rbdDiffObjects lists the objects a whole-object diff reports as data.
func rbdDiffObjects(extents []nativeRBDLockDiff, order int) ([]uint64, bool) {
	var objects []uint64
	for _, e := range extents {
		if !e.Exists || e.Offset%(1<<order) != 0 {
			return nil, false
		}
		for o := e.Offset >> order; o<<order < e.Offset+e.Length; o++ {
			objects = append(objects, o)
		}
	}
	return objects, true
}

func TestRBDLockTestOnlyEncodings(t *testing.T) {
	// ceph_crc32c known values from the pinned src/test/common/test_crc32c.cc.
	if rbdCRC32C([]byte("foo bar baz")) != 4119623852 || rbdCRC32C([]byte("whiz bang boom")) != 2360230088 {
		t.Fatal("ceph_crc32c restatement differs")
	}
	gid, handle := uint64(4764), uint64(0x55d48bcef190)
	id := rbdClientID{gid, handle}
	ident := hex.EncodeToString(id.encode())
	if ident != "9c12000000000000"+"90f1ce8bd4550000" {
		t.Fatal("ClientId encoding differs", ident)
	}
	for _, tc := range []struct {
		payload []byte
		want    string
		op      uint32
		force   bool
	}{
		{rbdLockNotify(rbdNotifyAcquiredLock, id), "0701" + "14000000" + "00000000" + ident, rbdNotifyAcquiredLock, false},
		{rbdLockNotify(rbdNotifyReleasedLock, id), "0701" + "14000000" + "01000000" + ident, rbdNotifyReleasedLock, false},
		{rbdRequestLockNotify(id, false), "0701" + "15000000" + "02000000" + ident + "00", rbdNotifyRequestLock, false},
		{rbdRequestLockNotify(id, true), "0701" + "15000000" + "02000000" + ident + "01", rbdNotifyRequestLock, true},
	} {
		if hex.EncodeToString(tc.payload) != tc.want {
			t.Fatalf("NotifyMessage encoding differs: %x", tc.payload)
		}
		m, err := decodeRBDNotifyMessage(tc.payload)
		if err != nil || !m.Known || m.Version != 7 || m.Op != tc.op || m.Client != id || m.Force != tc.force {
			t.Fatal("NotifyMessage decoding differs", m, err)
		}
		for cut := 1; cut < len(tc.payload); cut++ {
			if _, err := decodeRBDNotifyMessage(tc.payload[:cut]); err == nil {
				t.Fatal("truncated NotifyMessage admitted", cut)
			}
		}
		if _, err := decodeRBDNotifyMessage(append(bytes.Clone(tc.payload), 0)); err == nil {
			t.Fatal("trailing NotifyMessage byte admitted")
		}
	}
	if m, err := decodeRBDNotifyMessage(rbdHeaderUpdateNotify); err != nil || m.Op != rbdNotifyHeaderUpdate || !m.Known {
		t.Fatal("HeaderUpdate decoding differs", m, err)
	}
	if m, err := decodeRBDNotifyMessage(nil); err != nil || m.Op != rbdNotifyHeaderUpdate || m.Version != 0 {
		t.Fatal("legacy empty notification is librbd's header update", m, err)
	}
	// Version 1 lock payloads carried no ClientId; an unknown op keeps its
	// number; compat 2 is refused like DECODE_START(1).
	if m, err := decodeRBDNotifyMessage(rbdEnvelopeV(1, []byte{0, 0, 0, 0})); err != nil || m.Client != (rbdClientID{}) || m.Op != rbdNotifyAcquiredLock {
		t.Fatal("version 1 AcquiredLock decoding differs", m, err)
	}
	if m, err := decodeRBDNotifyMessage(rbdNotifyMessage(20, []byte{9, 9})); err != nil || m.Known || m.Op != 20 {
		t.Fatal("unknown notify op decoding differs", m, err)
	}
	if _, err := decodeRBDNotifyMessage([]byte{7, 2, 4, 0, 0, 0, 0, 0, 0, 0}); err == nil {
		t.Fatal("NotifyMessage with compat 2 admitted")
	}
	if _, err := decodeRBDNotifyMessage(append(rbdNotifyMessage(rbdNotifyRequestLock, id.encode()), 2)); err == nil {
		t.Fatal("non-bool force admitted")
	}
	if !bytes.Equal(rbdResponseMessage(0), rbdResponseAck) || hex.EncodeToString(rbdResponseMessage(-30)) != "010104000000e2ffffff" {
		t.Fatal("ResponseMessage encoding differs")
	}
	if r, err := decodeRBDResponseMessage(rbdResponseMessage(-16)); err != nil || r != -16 {
		t.Fatal("ResponseMessage decoding differs", r, err)
	}
	if _, err := decodeRBDResponseMessage(rbdResponseAck[:9]); err == nil {
		t.Fatal("truncated ResponseMessage admitted")
	}
	name := "08000000" + hex.EncodeToString([]byte(rbdLockName))
	if got := hex.EncodeToString(rbdLockLockInputFor("", "")); got != "0101"+"22000000"+name+"01"+"00000000"+"00000000"+"00000000"+"0000000000000000"+"00" {
		t.Fatal("object-map cls_lock_lock_op encoding differs", got)
	}
	if !bytes.Equal(rbdLockLockInputFor("auto 38", rbdLockTag), rbdLockLockInput("auto 38")) {
		t.Fatal("managed-lock cls_lock_lock_op differs from the write acceptance's")
	}
	if got := hex.EncodeToString(rbdLockAssertInput("", "")); got != "0101"+"15000000"+name+"01"+"00000000"+"00000000" {
		t.Fatal("cls_lock_assert_op encoding differs", got)
	}
	exists := uint8(rbdObjectExists)
	if got := hex.EncodeToString(rbdObjectMapUpdateInput(2, 3, rbdObjectExists, nil)); got != "0200000000000000"+"0300000000000000"+"01"+"00" {
		t.Fatal("object_map_update input differs", got)
	}
	if got := hex.EncodeToString(rbdObjectMapUpdateInput(2, 3, rbdObjectNonexistent, &exists)); got != "0200000000000000"+"0300000000000000"+"00"+"0101" {
		t.Fatal("object_map_update input with a current state differs", got)
	}
	// BitVector<2> of five objects [1 1 1 0 0] with and without CRCs.
	headerBL := rbdEnvelope(rbdLE64(5))
	data := []byte{0x54, 0x00}
	footer := binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, rbdCRC32C(headerBL)), 1), rbdCRC32C(data))
	raw := append(append(append(binary.LittleEndian.AppendUint32(nil, uint32(len(headerBL))), headerBL...), data...), append(binary.LittleEndian.AppendUint32(nil, uint32(len(footer))), footer...)...)
	m, err := decodeRBDObjectMap(raw)
	if err != nil || m.Size != 5 || !reflect.DeepEqual(m.States, []uint8{1, 1, 1, 0, 0}) || !m.CRC || len(m.DataCRCs) != 1 {
		t.Fatal("object map decoding differs", m, err)
	}
	loaded := append(append(append(binary.LittleEndian.AppendUint32(nil, uint32(len(headerBL))), headerBL...), data...), 0, 0, 0, 0)
	if m, err := decodeRBDObjectMap(loaded); err != nil || m.CRC || !reflect.DeepEqual(m.States, []uint8{1, 1, 1, 0, 0}) {
		t.Fatal("CRC-less object map decoding differs", m, err)
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, err := decodeRBDObjectMap(raw[:cut]); err == nil {
			t.Fatal("truncated object map admitted", cut)
		}
	}
	for _, i := range []int{6, 18, 19, len(raw) - 1} {
		damaged := bytes.Clone(raw)
		damaged[i] ^= 0x40
		if _, err := decodeRBDObjectMap(damaged); err == nil {
			t.Fatal("object map with a damaged byte admitted", i)
		}
	}
	if _, err := decodeRBDObjectMap(append(bytes.Clone(raw), 0)); err == nil {
		t.Fatal("trailing object map byte admitted")
	}
	if objects, ok := rbdDiffObjects([]nativeRBDLockDiff{{0, 128 << 10, true}, {192 << 10, 64 << 10, true}}, 16); !ok || !reflect.DeepEqual(objects, []uint64{0, 1, 3}) {
		t.Fatal("whole-object diff mapping differs", objects)
	}
	if _, ok := rbdDiffObjects([]nativeRBDLockDiff{{0, 64 << 10, false}}, 16); ok {
		t.Fatal("hole extent admitted in a diff from the beginning of time")
	}
}

type rbdLockNotification struct {
	Index            int
	AfterMS          float64
	NotifyID         uint64
	NotifierGlobalID uint64
	Cookie           uint64
	PayloadHex       string
	Version          uint8
	Op               uint32
	Client           rbdClientID
	Force            bool
	Own              bool
	AnsweredAsOwner  bool
	Unexpected       bool
	AckHex           string
	AckError         string `json:",omitempty"`
	DecodeError      string `json:",omitempty"`
	RawFrontHex      string
	RawDataHex       string
}

// rbdLockResponder answers the Go header watch's notifications as librbd's
// ImageWatcher would (handle_payload): AcquiredLock and ReleasedLock with an
// empty acknowledgement; RequestLock with ResponseMessage(0) while Go owns
// the lock (its automatic-policy answer, after which Go releases) and an
// empty acknowledgement otherwise or for its own request; HeaderUpdate with
// ResponseMessage(0). It pings the watch while idle.
type rbdLockResponder struct {
	mu         sync.Mutex
	events     []rbdLockNotification
	changed    chan struct{}
	owning     bool
	failure    error
	pings      int
	pingErrors []string
}

func (r *rbdLockResponder) setOwning(owning bool) {
	r.mu.Lock()
	r.owning = owning
	r.mu.Unlock()
}

func (r *rbdLockResponder) fail(err error) {
	r.mu.Lock()
	if r.failure == nil {
		r.failure = err
	}
	r.mu.Unlock()
}

func (r *rbdLockResponder) snapshot() ([]rbdLockNotification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events), r.failure
}

func (r *rbdLockResponder) handle(ctx context.Context, w *cephmsgr.OSDWatch, epoch uint32, self rbdClientID, started time.Time, event cephmsgr.OSDWatchEvent) {
	n := rbdLockNotification{AfterMS: float64(time.Since(started).Microseconds()) / 1000, NotifyID: event.NotifyID, NotifierGlobalID: event.NotifierGlobalID, Cookie: event.Cookie,
		PayloadHex: hex.EncodeToString(event.Payload), RawFrontHex: hex.EncodeToString(event.RawFront), RawDataHex: hex.EncodeToString(event.RawData)}
	defer func() {
		r.mu.Lock()
		n.Index = len(r.events)
		r.events = append(r.events, n)
		close(r.changed)
		r.changed = make(chan struct{})
		r.mu.Unlock()
	}()
	if event.Opcode != cephmsgr.OSDWatchNotification || event.Cookie != w.Cookie() {
		n.Unexpected = true
		r.fail(fmt.Errorf("unexpected header watch event opcode %d cookie %d", event.Opcode, event.Cookie))
		return
	}
	m, err := decodeRBDNotifyMessage(event.Payload)
	if err != nil {
		// librbd logs an undecodable notification and does not acknowledge it.
		n.DecodeError, n.Unexpected = err.Error(), true
		r.fail(fmt.Errorf("undecodable header notification: %w", err))
		return
	}
	n.Version, n.Op, n.Client, n.Force = m.Version, m.Op, m.Client, m.Force
	n.Own = m.Client == self
	r.mu.Lock()
	owning := r.owning
	r.mu.Unlock()
	var ack []byte
	switch {
	case !m.Known:
		n.Unexpected = true
		if owning {
			ack = rbdResponseMessage(-95) // UnknownPayload: -EOPNOTSUPP from a lock owner
		}
	case m.Op == rbdNotifyRequestLock && !n.Own && owning:
		ack, n.AnsweredAsOwner = rbdResponseMessage(0), true
	case m.Op == rbdNotifyHeaderUpdate:
		ack, n.Unexpected = rbdResponseMessage(0), true
	}
	n.AckHex = hex.EncodeToString(ack)
	if _, err := w.Acknowledge(ctx, epoch, event, ack); err != nil {
		n.AckError = err.Error()
		r.fail(fmt.Errorf("acknowledge header notification %d: %w", event.NotifyID, err))
	}
	if n.Unexpected {
		r.fail(fmt.Errorf("unexpected header notification op %d from client.%d", m.Op, event.NotifierGlobalID))
	}
}

type rbdLockGoNotify struct {
	Label            string
	PayloadHex       string
	Cookie, NotifyID uint64
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

type rbdLockObjectMapEvidence struct {
	Label  string
	RawHex string
	CRC    bool
	States []int // one entry per object (a []uint8 would marshal as base64)
}

type rbdLockStepEvidence struct {
	Label string
	Value any
}

type rbdLockTestEvidence struct {
	Token, Pool           string
	PoolID                int64
	GoIdentity            string
	GoGlobalID            uint64
	GoWatchCookie         uint64
	GoCookie              string
	Image, ImageID        string
	NativeGlobalID        uint64
	NativeHandle          uint64
	Requests              []rbdWriteRequestEvidence
	ClassOutputs          map[string]string
	LockInfo              map[string]string
	ObjectMaps            []rbdLockObjectMapEvidence
	GoNotifies            []*rbdLockGoNotify
	Notifications         []rbdLockNotification
	Pings                 int
	PingErrors            []string
	Steps                 []rbdLockStepEvidence
	NativeActions         []rbdNativeActionEvidence
	LockState             *nativeRBDLockState
	Check                 *nativeRBDLockCheck
	CompoundObjectMapCall string
	Timings               map[string]string
	ExpectedHeadSHA       string
}

// waitRBDLockState polls the lock driver's published state until it holds
// the named phase, or the control action ended.
func waitRBDLockState(ctx context.Context, path, phase string, finished <-chan struct{}, finishedErr *error) (nativeRBDLockState, nativeRBDLockPhase, error) {
	read := func() (nativeRBDLockState, nativeRBDLockPhase, bool) {
		var state nativeRBDLockState
		data, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(data, &state) != nil {
			return state, nativeRBDLockPhase{}, false
		}
		i := slices.IndexFunc(state.Phases, func(p nativeRBDLockPhase) bool { return p.Phase == phase })
		if i < 0 {
			return state, nativeRBDLockPhase{}, false
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
			return nativeRBDLockState{}, nativeRBDLockPhase{}, fmt.Errorf("native RBD lock action ended before %s: %v", phase, *finishedErr)
		case <-ctx.Done():
			return nativeRBDLockState{}, nativeRBDLockPhase{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestCephRBDExclusiveLockIntegration(t *testing.T) {
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
	if c.Snapshot().OSDTicket.Expires.IsZero() {
		t.Fatal("OSD service ticket missing")
	}
	gid := c.Snapshot().GlobalID
	evidence := rbdLockTestEvidence{Token: token, Pool: oracle.Plan.Pool, PoolID: oracle.Pool.ID, GoIdentity: identity, GoGlobalID: gid, ClassOutputs: map[string]string{},
		LockInfo: map[string]string{}, Timings: map[string]string{}}
	var responder *rbdLockResponder
	defer func() {
		if responder != nil {
			evidence.Notifications, _ = responder.snapshot()
			responder.mu.Lock()
			evidence.Pings, evidence.PingErrors = responder.pings, slices.Clone(responder.pingErrors)
			responder.mu.Unlock()
		}
		evidence.Timings["total"] = time.Since(started).String()
		data, err := json.Marshal(evidence)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-lock-results.json"), data)
		}
		if err != nil {
			t.Error("RBD lock evidence", err)
		}
	}()
	step := func(label string, value any) {
		evidence.Steps = append(evidence.Steps, rbdLockStepEvidence{Label: label, Value: value})
	}

	// Native control: one closed action at a time, each journaled by the
	// fixture and answered with its own receipt.
	var plan nativeRBDLockPlan
	readNative := func(t *testing.T, action string) nativeRBDLockResult {
		t.Helper()
		var result nativeRBDLockResult
		if err := readMDSFreshJSON(filepath.Join(dir, "rbd-lock-"+action+".json"), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	native := func(t *testing.T, action string) nativeRBDLockResult {
		t.Helper()
		if err := os.Remove(filepath.Join(dir, "rbd-lock-"+action+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if err := testcluster.RequestControl(ctx, dir, "rbd-lock", "rbd-lock-completed", token, action); err != nil {
			t.Fatal("native RBD lock", action, err)
		}
		return readNative(t, action)
	}
	checkNative := func(t *testing.T, action string, result nativeRBDLockResult) *nativeRBDLock {
		t.Helper()
		n := result.Native
		if result.Source != mdsFreshSource || result.Token != token || result.Action != action || !result.Complete || result.Error != "" || result.Mutation == nil ||
			result.Mutation.Kind != "native-lock-"+action || result.Mutation.State != "applied" || result.Mutation.ReadOnly != (action == "observe-released") ||
			result.Mutation.RequestID != result.RequestID || result.Process == nil || result.Process.ExitCode != 0 || result.Process.TimedOut || n == nil ||
			n.Action != action || n.RequestID != result.RequestID || n.NativeGlobalID == 0 || n.NativeGlobalID == gid || n.PoolID != oracle.Pool.ID ||
			(plan.Image != "" && !reflect.DeepEqual(result.Plan, plan)) {
			t.Fatal("native RBD lock receipt differs", action, result.Error)
		}
		// librbd logs failed lock, object-map and notify steps at error level.
		if len(result.Process.Stderr) != 0 {
			t.Fatalf("native RBD lock %s logged errors: %q", action, result.Process.Stderr)
		}
		evidence.NativeActions = append(evidence.NativeActions, rbdNativeActionEvidence{Action: action, RequestID: result.RequestID, NativeGlobalID: n.NativeGlobalID, ElapsedMS: result.ElapsedMS, StderrBytes: len(result.Process.Stderr)})
		return n
	}
	setupResult := native(t, "setup")
	plan = setupResult.Plan
	setup := checkNative(t, "setup", setupResult)
	k := uint64(1) << 10
	if plan.Pool != oracle.Plan.Pool || plan.BaseImage != oracle.Plan.Image || plan.WriteImage != "ceph-msgr-write-"+token || plan.Image != "ceph-msgr-lock-"+token ||
		plan.Order != 16 || plan.Size != 320*k || plan.Features != rbdLockImageFeatures ||
		!reflect.DeepEqual(plan.SetupWrites, []nativeRBDExtent{{0, 64 * k}}) || !reflect.DeepEqual(plan.OwnerWrites, []nativeRBDExtent{{64 * k, 16 * k}}) ||
		!reflect.DeepEqual(plan.GoWrites, []nativeRBDExtent{{136 * k, 8 * k}}) || !reflect.DeepEqual(plan.ReacquireWrites, []nativeRBDExtent{{200 * k, 8 * k}}) {
		t.Fatal("RBD lock plan differs", plan)
	}
	order := plan.Order
	objectSize := uint64(1) << order
	objectCount := plan.Size >> order
	// The expected head image grows with every writer.
	expectedHead := make([]byte, plan.Size)
	apply := func(phase string, extents []nativeRBDExtent) {
		for _, e := range extents {
			copy(expectedHead[e.Offset:], rbdLockPattern(token, phase, e.Offset, e.Length))
		}
	}
	checkWrites := func(t *testing.T, label string, writes []nativeRBDLockWrite, phase string, extents []nativeRBDExtent) {
		t.Helper()
		if len(writes) != len(extents) {
			t.Fatal("native writes differ from the plan", label)
		}
		for i, w := range writes {
			sum := sha256.Sum256(rbdLockPattern(token, phase, w.Offset, w.Length))
			if w.Phase != phase || w.Offset != extents[i].Offset || w.Length != extents[i].Length || w.Written != w.Length || w.SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("test-only RBD lock pattern differs from the native driver", label, w)
			}
		}
	}
	checkWrites(t, "setup", setup.Writes, "setup", plan.SetupWrites)
	apply("setup", plan.SetupWrites)
	diffObjects := func(t *testing.T, label string, extents []nativeRBDLockDiff, want ...uint64) {
		t.Helper()
		got, ok := rbdDiffObjects(extents, order)
		if !ok || !slices.Equal(got, want) {
			t.Fatalf("%s: native fast-diff objects %v, want %v (%+v)", label, got, want, extents)
		}
	}
	if setup.Stat.Size != plan.Size || setup.Stat.Objects != objectCount || setup.Features != plan.Features || setup.Flags != 0 || len(setup.Snapshots) != 0 ||
		!setup.Lockers.Empty || len(setup.Watchers) != 0 {
		t.Fatal("native RBD lock image setup differs", setup.Stat, setup.Features, setup.Flags, setup.Lockers)
	}
	diffObjects(t, "setup", setup.Diff, 0)
	views := func(t *testing.T, n *nativeRBDLock) map[string][]byte {
		t.Helper()
		result := map[string][]byte{}
		for key, blob := range n.Blobs {
			result[key] = readRBDBlob(t, dir, blob)
		}
		if len(result) != 2 || uint64(len(result["image_head"])) != plan.Size {
			t.Fatal("native RBD lock binary views missing", n.Action)
		}
		return result
	}
	if !bytes.Equal(views(t, setup)["image_head"], expectedHead) {
		t.Fatal("native librbd setup read differs from the test-only pattern")
	}
	evidence.Image = plan.Image

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
	var recordMu sync.Mutex
	record := func(label string, r rbdRoute, req cephmsgr.OSDRequest, reply cephmsgr.OSDReply, err error) {
		e := rbdWriteRequestEvidence{Label: label, Route: r, Submission: reply.Submission, Transaction: reply.Transaction, GlobalID: reply.GlobalID,
			MapEpoch: reply.MapEpoch, PG: reply.PG, Code: reply.Code, Flags: reply.Flags, UserVersion: reply.UserVersion, LegacyVersion: reply.LegacyVersion, ReplayVersion: reply.ReplayVersion}
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
			o := rbdOperationEvidence{Code: uint16(op.Code), Result: op.Result, Length: op.Length, DataLength: len(op.Data), DataSHA256: hex.EncodeToString(sum[:]), Watchers: op.Watchers}
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
		recordMu.Lock()
		evidence.Requests = append(evidence.Requests, e)
		recordMu.Unlock()
	}
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
	// call sends one CALL alone in its request (write mode with a
	// modification time and no SnapContext, as librados sends librbd's
	// metadata operations) and requires result 0.
	call := func(t *testing.T, label, object, class, method string, mode cephmsgr.OSDCallMode, input []byte) []byte {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 4096}})
		if mode == cephmsgr.OSDCallWrite {
			req.ModificationTime = now()
		}
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatalf("Go CALL %s.%s (%s): %v", class, method, label, err)
		}
		checkReply(t, r, reply, 0, mode == cephmsgr.OSDCallWrite)
		got := reply.Operations[0]
		if got.Result != 0 || got.Class.ClassLength != uint8(len(class)) || got.Class.MethodLength != uint8(len(method)) || got.Class.InputLength != uint32(len(input)) ||
			got.Class.OutputLength != uint64(len(got.Data)) || mode == cephmsgr.OSDCallWrite && len(got.Data) != 0 {
			t.Fatalf("CALL %s.%s per-op result or union echo differs: %+v", class, method, got.Class)
		}
		evidence.ClassOutputs[label] = hex.EncodeToString(got.Data)
		return got.Data
	}
	sameHex := func(t *testing.T, label string, native nativeRBDWriteClassCall, object, class, method string, input, output []byte) {
		t.Helper()
		if native.Object != object || native.Class != class || native.Method != method || native.InputHex != hex.EncodeToString(input) || native.Code != 0 || native.OutputHex != hex.EncodeToString(output) {
			t.Fatalf("%s: Go CALL %s.%s differs from native librados exec: native=%+v go_input=%x go_output=%x", label, class, method, native, input, output)
		}
	}
	listWatchers := func(t *testing.T, label, object string) []cephmsgr.OSDWatcher {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		if err != nil {
			t.Fatal(label, err)
		}
		checkReply(t, r, reply, 0, false)
		return reply.Operations[0].Watchers
	}
	readRaw := func(t *testing.T, label, object string) []byte {
		t.Helper()
		r, o := route(t, object)
		req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: objectSize})
		reply, err := o.Request(ctx, req)
		record(label, r, req, reply, err)
		var server *cephmsgr.OSDError
		if errors.As(err, &server) && server.Code == -2 {
			checkReply(t, r, reply, -2, false)
			return nil
		}
		if err != nil {
			t.Fatal(label, err)
		}
		checkReply(t, r, reply, 0, false)
		data := reply.Operations[1].Data
		if binary.LittleEndian.Uint64(reply.Operations[0].Data) != uint64(len(data)) {
			t.Fatal(label, "STAT size differs from the READ length")
		}
		return data
	}
	legacy := func(a rbdLockAddress) string { return fmt.Sprintf("%s/%d", a.Endpoint, a.Nonce) }
	watcherLegacy := func(w cephmsgr.OSDWatcher) string { return fmt.Sprintf("%s/%d", w.Address.Endpoint, w.Address.Nonce) }
	lockInfo := func(t *testing.T, label, object string) ([]byte, rbdLockInfo) {
		t.Helper()
		out := call(t, label, object, "lock", "get_info", cephmsgr.OSDCallRead, rbdLockGetInfoInput())
		info, err := decodeRBDLockInfo(out)
		if err != nil {
			t.Fatal(label, "cls_lock_get_info_reply decoding", err)
		}
		evidence.LockInfo[label] = hex.EncodeToString(out)
		return out, info
	}
	objectMap := func(t *testing.T, label string, raw []byte, crc bool, want ...uint8) rbdObjectMap {
		t.Helper()
		m, err := decodeRBDObjectMap(raw)
		states := make([]int, len(m.States))
		for i, s := range m.States {
			states[i] = int(s)
		}
		evidence.ObjectMaps = append(evidence.ObjectMaps, rbdLockObjectMapEvidence{Label: label, RawHex: hex.EncodeToString(raw), CRC: m.CRC, States: states})
		if err != nil || m.Size != objectCount || m.CRC != crc || !slices.Equal(m.States, want) {
			t.Fatalf("%s: object map %v (crc %v, size %d, %v), want %v", label, m.States, m.CRC, m.Size, err, want)
		}
		return m
	}

	// The image identity as librbd's open reads it, equal to the native
	// librados exec of the setup observation.
	idObject := "rbd_id." + plan.Image
	out := call(t, "get_id", idObject, "rbd", "get_id", cephmsgr.OSDCallRead, nil)
	sameHex(t, "setup", setup.ClassCalls[0], idObject, "rbd", "get_id", nil, out)
	d := &rbdDecoder{data: out}
	imageID := d.str()
	if err := d.done(); err != nil || imageID != setup.ImageID {
		t.Fatal("rbd_id get_id differs from the native image id", imageID, err)
	}
	header, mapObject := "rbd_header."+imageID, "rbd_object_map."+imageID
	evidence.ImageID = imageID
	if setup.HeaderObject != header || setup.MapObject != mapObject {
		t.Fatal("native header or object-map object names differ", setup.HeaderObject, setup.MapObject)
	}
	out = call(t, "get_size", header, "rbd", "get_size", cephmsgr.OSDCallRead, rbdLE64(rbdNoSnap))
	sameHex(t, "setup", setup.ClassCalls[1], header, "rbd", "get_size", rbdLE64(rbdNoSnap), out)
	if d := (&rbdDecoder{data: out}); d.u8() != uint8(order) || d.u64() != plan.Size || d.done() != nil {
		t.Fatal("get_size(head) differs from the plan", out)
	}
	out = call(t, "get_object_prefix", header, "rbd", "get_object_prefix", cephmsgr.OSDCallRead, nil)
	sameHex(t, "setup", setup.ClassCalls[2], header, "rbd", "get_object_prefix", nil, out)
	prefix := (&rbdDecoder{data: out}).str()
	if prefix != "rbd_data."+imageID || prefix != setup.Stat.Prefix {
		t.Fatal("get_object_prefix differs from the native block name prefix", prefix)
	}
	featuresInput := append(rbdLE64(rbdNoSnap), 0)
	out = call(t, "get_features", header, "rbd", "get_features", cephmsgr.OSDCallRead, featuresInput)
	sameHex(t, "setup", setup.ClassCalls[3], header, "rbd", "get_features", featuresInput, out)
	// The exclusive-lock, object-map and fast-diff bits are all
	// RBD_FEATURES_RW_INCOMPATIBLE: a writer must implement them.
	if d := (&rbdDecoder{data: out}); d.u64() != rbdLockImageFeatures || d.u64() != rbdLockImageFeatures || d.done() != nil {
		t.Fatal("get_features differs from exclusive-lock, object-map and fast-diff", out)
	}
	out = call(t, "get_snapcontext", header, "rbd", "get_snapcontext", cephmsgr.OSDCallRead, nil)
	sameHex(t, "setup", setup.ClassCalls[4], header, "rbd", "get_snapcontext", nil, out)
	if d := (&rbdDecoder{data: out}); d.u64() != 0 || d.u32() != 0 || d.done() != nil {
		t.Fatal("image without snapshots has a SnapContext", out)
	}
	before, info := lockInfo(t, "lock_get_info_setup", header)
	sameHex(t, "setup", setup.ClassCalls[5], header, "lock", "get_info", rbdLockGetInfoInput(), before)
	if len(info.Lockers) != 0 {
		t.Fatal("closed image header is locked", info)
	}
	mapBefore, info := lockInfo(t, "map_lock_get_info_setup", mapObject)
	sameHex(t, "setup", setup.ClassCalls[6], mapObject, "lock", "get_info", rbdLockGetInfoInput(), mapBefore)
	if len(info.Lockers) != 0 {
		t.Fatal("closed image object map is locked", info)
	}
	raw := readRaw(t, "object_map_read_setup", mapObject)
	if hex.EncodeToString(raw) != setup.MapRaw.Hex || !setup.MapRaw.Exists {
		t.Fatal("Go object-map object read differs from native librados")
	}
	objectMap(t, "setup_raw", raw, true, 1, 0, 0, 0, 0)
	out = call(t, "object_map_load_setup", mapObject, "rbd", "object_map_load", cephmsgr.OSDCallRead, nil)
	sameHex(t, "setup", setup.ClassCalls[7], mapObject, "rbd", "object_map_load", nil, out)
	objectMap(t, "setup_load", out, false, 1, 0, 0, 0, 0)

	// The native lock action runs in the background through four Go steps.
	statePath := filepath.Join(dir, "rbd-lock-state.json")
	for i := 0; i <= 4; i++ {
		name := statePath
		if i > 0 {
			name = filepath.Join(dir, fmt.Sprintf("rbd-lock-proceed-%d", i))
		}
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(dir, "rbd-lock-lock.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var controlErr error
	controlDone := make(chan struct{})
	go func() {
		controlErr = testcluster.RequestControl(ctx, dir, "rbd-lock", "rbd-lock-completed", token, "lock")
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
	state, owner, err := waitRBDLockState(ctx, statePath, "owner", controlDone, &controlErr)
	if err != nil {
		t.Fatal(err)
	}
	evidence.LockState = &state
	nativeGID := state.NativeGlobalID
	evidence.NativeGlobalID = nativeGID
	if state.Token != token || nativeGID == 0 || nativeGID == gid || state.RequestID <= 0 {
		t.Fatal("native RBD lock action scope differs", state)
	}
	proceed := func(t *testing.T, step int) {
		t.Helper()
		signal := fmt.Sprintf("%s %d %d", token, state.RequestID, step)
		if err := testcluster.WritePublicEvidence(filepath.Join(dir, fmt.Sprintf("rbd-lock-proceed-%d", step)), []byte(signal)); err != nil {
			t.Fatal(err)
		}
	}
	waitPhase := func(t *testing.T, phase string) nativeRBDLockPhase {
		t.Helper()
		s, p, err := waitRBDLockState(ctx, statePath, phase, controlDone, &controlErr)
		if err != nil {
			t.Fatal(err)
		}
		state = s
		evidence.LockState = &state
		return p
	}

	var nativeOwner rbdClientID
	var nativeLegacy string
	t.Run("lock_owner_observation", func(t *testing.T) {
		// Native librbd's first write took the lock (automatic policy).
		checkWrites(t, "owner", owner.Writes, "owner", plan.OwnerWrites)
		apply("owner", plan.OwnerWrites)
		if !owner.IsOwner || len(owner.Watchers) != 1 || owner.Watchers[0].ID != nativeGID || owner.Flags != 0 || owner.Size != plan.Size {
			t.Fatal("native librbd does not own the lock after its write", owner.IsOwner, owner.Watchers, owner.Flags)
		}
		librbd := owner.Watchers[0]
		nativeOwner, nativeLegacy = rbdClientID{GID: nativeGID, Handle: librbd.Cookie}, librbd.Address
		evidence.NativeHandle = librbd.Cookie
		// Go reads the owner as librbd's GetLockerRequest and BreakRequest do.
		out, info := lockInfo(t, "lock_get_info_native_owner", header)
		sameHex(t, "owner", owner.HeaderLock, header, "lock", "get_info", rbdLockGetInfoInput(), out)
		if len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != rbdLockTag {
			t.Fatal("header lock is not librbd's exclusive managed lock", info)
		}
		l := info.Lockers[0]
		handle, ok := rbdLockHandle(l.Cookie)
		if !ok || handle != librbd.Cookie || l.NameType != rbdEntityClient || uint64(l.NameNumber) != nativeGID || l.Address.Type != rbdAddrTypeLegacy ||
			legacy(l.Address) != librbd.Address || l.Address.Endpoint.Addr().IsUnspecified() || l.ExpirationSeconds != 0 || l.ExpirationNanos != 0 || l.Description != "" {
			t.Fatal("native locker differs from librbd's managed-lock owner", l, librbd)
		}
		watchers := listWatchers(t, "list_watchers_native_owner", header)
		if len(watchers) != 1 || uint64(watchers[0].NameNumber) != nativeGID || watchers[0].NameType != rbdEntityClient || watchers[0].Cookie != handle || watcherLegacy(watchers[0]) != legacy(l.Address) {
			t.Fatal("LIST_WATCHERS does not show the lock owner alive", watchers, l)
		}
		if len(owner.Owners) != 1 || owner.Owners[0].Mode != rbdLockModeExclusive || owner.Owners[0].Owner != nativeLegacy || owner.Lockers.Empty || !owner.Lockers.Exclusive || owner.Lockers.Tag != rbdLockTag ||
			len(owner.Lockers.Lockers) != 1 || owner.Lockers.Lockers[0].Client != fmt.Sprintf("client.%d", nativeGID) || owner.Lockers.Lockers[0].Cookie != l.Cookie || owner.Lockers.Lockers[0].Address != nativeLegacy {
			t.Fatal("native lock_get_owners or list_lockers differ from the decoded owner", owner.Owners, owner.Lockers)
		}
		// librbd's PostAcquireRequest also locked the object map (empty
		// cookie and tag) and its write marked object 1.
		out, info = lockInfo(t, "map_lock_get_info_native_owner", mapObject)
		sameHex(t, "owner", owner.MapLock, mapObject, "lock", "get_info", rbdLockGetInfoInput(), out)
		if len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != "" || info.Lockers[0].Cookie != "" || uint64(info.Lockers[0].NameNumber) != nativeGID ||
			legacy(info.Lockers[0].Address) != nativeLegacy {
			t.Fatal("object map is not locked by the native owner", info)
		}
		raw := readRaw(t, "object_map_read_native_owner", mapObject)
		if hex.EncodeToString(raw) != owner.MapRaw.Hex {
			t.Fatal("Go object-map read differs from the native owner phase")
		}
		objectMap(t, "native_owner_raw", raw, true, 1, 1, 0, 0, 0)
		diffObjects(t, "owner", owner.Diff, 0, 1)
		step("native_owner", map[string]any{"client_id": nativeOwner, "cookie": l.Cookie, "address": nativeLegacy, "owners": owner.Owners})
		t.Logf("native owner client.%d handle %d at %s (cookie %q)", nativeGID, librbd.Cookie, nativeLegacy, l.Cookie)
	})
	if t.Failed() {
		return
	}

	// Go's header watch: its handle is the ClientId handle and the lock
	// cookie, and librbd's BreakRequest finds Go alive through it.
	hr, ho := route(t, header)
	mr, mo := route(t, mapObject)
	watchRequest := request(hr)
	watchRequest.Operations = nil
	w, reply, err := ho.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
	record("lock_watch_register", hr, watchRequest, reply, err)
	if err != nil {
		t.Fatal("Go header watch", err)
	}
	goID := rbdClientID{GID: gid, Handle: w.Cookie()}
	cookie := rbdLockCookie(w.Cookie())
	evidence.GoWatchCookie, evidence.GoCookie = w.Cookie(), cookie
	responder = &rbdLockResponder{changed: make(chan struct{})}
	respondCtx, stopResponding := context.WithCancel(ctx)
	responderDone := make(chan struct{})
	responderStarted := time.Now()
	go func() {
		defer close(responderDone)
		// The OSD expires a watch without pings for its timeout; notify
		// acknowledgements do not renew it.
		lastPing := time.Now()
		for {
			if time.Since(lastPing) >= rbdLockPingInterval {
				_, err := w.Ping(respondCtx, hr.Epoch)
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
				responder.fail(fmt.Errorf("header watch ended: %w", err))
				return
			}
			responder.handle(respondCtx, w, hr.Epoch, goID, responderStarted, event)
		}
	}()
	var watching, headerLockSent, headerUnlocked, mapLockSent, mapUnlocked bool = true, false, false, false, false
	stopResponder := func() {
		stopResponding()
		<-responderDone
	}
	defer func() {
		// Compensation after a failure: give back what Go may hold (never a
		// repeated lock), tell librbd, then remove the watch.
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		released := false
		for _, u := range []struct {
			sent, done *bool
			route      rbdRoute
			connection *cephmsgr.OSDConnection
			cookie     string
		}{{&mapLockSent, &mapUnlocked, mr, mo, ""}, {&headerLockSent, &headerUnlocked, hr, ho, cookie}} {
			if *u.sent && !*u.done {
				r, o := u.route, u.connection
				req := request(r, cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: rbdLockUnlockInput(u.cookie), Class: cephmsgr.OSDClassInput{Class: "lock", Method: "unlock", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}})
				req.ModificationTime = now()
				reply, err := o.Request(cleanup, req)
				record("compensating_unlock_"+r.Object, r, req, reply, err)
				t.Log("compensating unlock after a failure:", r.Object, err)
				*u.done, released = err == nil, true
			}
		}
		if released {
			req := request(hr)
			req.Operations = nil
			if n, reply, err := ho.NotifyObject(cleanup, req, rbdLockNotify(rbdNotifyReleasedLock, goID), cephmsgr.OSDWatchOptions{TimeoutSeconds: 5}); err == nil {
				record("compensating_released_lock", hr, req, reply, err)
				_, _ = n.Next(cleanup)
				n.Close()
			}
		}
		stopResponder()
		if watching {
			if _, err := w.Unwatch(cleanup, hr.Epoch); err != nil {
				t.Error("Go header unwatch cleanup", err)
			}
		}
	}()
	waitNotification := func(t *testing.T, from int, label string, match func(rbdLockNotification) bool) rbdLockNotification {
		t.Helper()
		deadline := time.After(20 * time.Second)
		for {
			responder.mu.Lock()
			for _, n := range responder.events[min(from, len(responder.events)):] {
				if match(n) {
					responder.mu.Unlock()
					return n
				}
			}
			changed, failure := responder.changed, responder.failure
			responder.mu.Unlock()
			if failure != nil {
				t.Fatal(label, "header watch responder:", failure)
			}
			select {
			case <-changed:
			case <-deadline:
				t.Fatal(label, "notification not received")
			case <-ctx.Done():
				t.Fatal(label, ctx.Err())
			}
		}
	}
	notifications := func() int {
		responder.mu.Lock()
		defer responder.mu.Unlock()
		return len(responder.events)
	}
	// goNotify sends one librbd notification from Go with librbd's 5 s
	// notify timeout and returns its completion: the native watch and Go's
	// own watch must both acknowledge.
	goNotify := func(t *testing.T, label string, payload []byte) *rbdLockGoNotify {
		t.Helper()
		req := request(hr)
		req.Operations = nil
		notifyStarted := time.Now()
		n, reply, err := ho.NotifyObject(ctx, req, payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 5})
		record(label, hr, req, reply, err)
		if err != nil {
			t.Fatal("Go notify", label, err)
		}
		defer n.Close()
		checkReply(t, hr, reply, 0, false)
		notifyID := binary.LittleEndian.Uint64(reply.Operations[0].Data)
		wait, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		completion, err := n.Next(wait)
		if err != nil {
			t.Fatal("Go notify completion", label, err)
		}
		e := &rbdLockGoNotify{Label: label, PayloadHex: hex.EncodeToString(payload), Cookie: n.Cookie(), NotifyID: notifyID, ReplyDataHex: hex.EncodeToString(reply.Operations[0].Data),
			Opcode: completion.Opcode, CompletionID: completion.NotifyID, ReturnCode: completion.ReturnCode, Missed: completion.Missed,
			RawFrontHex: hex.EncodeToString(completion.RawFront), RawDataHex: hex.EncodeToString(completion.RawData), CompletedAfterMS: float64(time.Since(notifyStarted).Microseconds()) / 1000}
		for _, reply := range completion.Replies {
			e.Replies = append(e.Replies, rbdNotifyReplyEvidence{GlobalID: reply.GlobalID, Cookie: reply.Cookie, DataHex: hex.EncodeToString(reply.Data)})
		}
		evidence.GoNotifies = append(evidence.GoNotifies, e)
		if completion.Opcode != cephmsgr.OSDWatchCompletion || completion.NotifyID != notifyID || completion.Cookie != n.Cookie() || completion.ReturnCode != 0 || len(completion.Missed) != 0 ||
			len(completion.Replies) != 2 || time.Since(notifyStarted) >= rbdNotifyTimeout {
			t.Fatalf("%s completion differs: %+v", label, e)
		}
		if _, err := n.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDNotifyComplete) {
			t.Fatal("notify stream did not end after its completion", label, err)
		}
		return e
	}
	replyFrom := func(t *testing.T, e *rbdLockGoNotify, id rbdClientID) []byte {
		t.Helper()
		i := slices.IndexFunc(e.Replies, func(r rbdNotifyReplyEvidence) bool { return r.GlobalID == id.GID && r.Cookie == id.Handle })
		if i < 0 {
			t.Fatalf("%s: no acknowledgement from client.%d handle %d: %+v", e.Label, id.GID, id.Handle, e.Replies)
		}
		data, _ := hex.DecodeString(e.Replies[i].DataHex)
		return data
	}
	// Every Go notification reaches Go's own watch too; librbd acknowledges
	// its own notifications empty, and so does the Go responder.
	ownNotification := func(t *testing.T, from int, label string, op uint32) {
		t.Helper()
		n := waitNotification(t, from, label, func(n rbdLockNotification) bool { return n.Op == op && n.Own && n.NotifierGlobalID == gid })
		if n.Client != goID || n.AckHex != "" || n.AckError != "" {
			t.Fatal(label, "own notification acknowledgement differs", n)
		}
	}

	var goLocked, goMap []byte
	t.Run("request_lock_release", func(t *testing.T) {
		watchers := listWatchers(t, "list_watchers_go_watch", header)
		if len(watchers) != 2 || !slices.ContainsFunc(watchers, func(x cephmsgr.OSDWatcher) bool { return uint64(x.NameNumber) == gid && x.Cookie == w.Cookie() }) ||
			!slices.ContainsFunc(watchers, func(x cephmsgr.OSDWatcher) bool {
				return uint64(x.NameNumber) == nativeGID && x.Cookie == nativeOwner.Handle
			}) {
			t.Fatal("header watchers lack the native owner or the Go watch", watchers)
		}
		mark := notifications()
		requested := time.Now()
		e := goNotify(t, "notify_request_lock", rbdRequestLockNotify(goID, false))
		// NotifyLockOwner takes the single non-empty acknowledgement as the
		// lock owner's answer.
		answer := replyFrom(t, e, nativeOwner)
		result, err := decodeRBDResponseMessage(answer)
		if err != nil || len(replyFrom(t, e, goID)) != 0 {
			t.Fatal("RequestLock acknowledgements differ", e.Replies, err)
		}
		step("native_lock_policy_answer", map[string]any{"response_hex": hex.EncodeToString(answer), "result": result})
		if result != 0 {
			t.Fatalf("native librbd answered RequestLock with %d, not the automatic policy's release (0)", result)
		}
		ownNotification(t, mark, "own RequestLock", rbdNotifyRequestLock)
		released := waitNotification(t, mark, "native ReleasedLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyReleasedLock && n.NotifierGlobalID == nativeGID
		})
		evidence.Timings["request_to_released"] = time.Duration(released.AfterMS*float64(time.Millisecond) - float64(requested.Sub(responderStarted))).String()
		if released.Client != nativeOwner || released.Version != 7 || released.AckHex != "" || released.AckError != "" {
			t.Fatal("native ReleasedLock differs from the owner's ClientId", released)
		}
		// librbd closed the object map (its unlock) and unlocked the header
		// before announcing the release.
		out, info := lockInfo(t, "lock_get_info_native_released", header)
		if len(info.Lockers) != 0 {
			t.Fatal("native librbd announced a release but still holds the header lock", info)
		}
		mapOut, mapInfo := lockInfo(t, "map_lock_get_info_native_released", mapObject)
		if len(mapInfo.Lockers) != 0 {
			t.Fatal("native librbd released the lock but still holds the object map", mapInfo)
		}
		proceed(t, 1)
		p := waitPhase(t, "released")
		sameHex(t, "released", p.HeaderLock, header, "lock", "get_info", rbdLockGetInfoInput(), out)
		sameHex(t, "released", p.MapLock, mapObject, "lock", "get_info", rbdLockGetInfoInput(), mapOut)
		if p.IsOwner || len(p.Owners) != 0 || !p.Lockers.Empty || len(p.Watchers) != 2 || p.Flags != 0 {
			t.Fatal("native librbd still reports itself as the owner after releasing", p.IsOwner, p.Owners, p.Lockers)
		}
		raw, _ := hex.DecodeString(p.MapRaw.Hex)
		objectMap(t, "native_released_raw", raw, true, 1, 1, 0, 0, 0)
		t.Logf("librbd answered RequestLock with %x and released: ReleasedLock %s", answer, released.PayloadHex)
	})
	if t.Failed() {
		return
	}

	goData := map[uint64][]byte{}
	t.Run("go_owner_object_map", func(t *testing.T) {
		// librbd's AcquireRequest form of the header lock.
		headerLockSent = true
		call(t, "lock_lock_go", header, "lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInput(cookie))
		responder.setOwning(true)
		var info rbdLockInfo
		goLocked, info = lockInfo(t, "lock_get_info_go_owner", header)
		goWatcher := listWatchers(t, "list_watchers_go_owner", header)
		i := slices.IndexFunc(goWatcher, func(x cephmsgr.OSDWatcher) bool { return uint64(x.NameNumber) == gid && x.Cookie == w.Cookie() })
		if len(info.Lockers) != 1 || info.Type != rbdLockExclusive || info.Tag != rbdLockTag || i < 0 {
			t.Fatal("header lock lacks the Go managed lock", info)
		}
		l := info.Lockers[0]
		goLegacy := legacy(l.Address)
		if uint64(l.NameNumber) != gid || l.Cookie != cookie || l.Address.Type != rbdAddrTypeLegacy || goLegacy != watcherLegacy(goWatcher[i]) {
			t.Fatal("Go locker differs from its watch", l, goWatcher[i])
		}
		// librbd's PostAcquireRequest opens the object map: object_map
		// LockRequest (exclusive, empty cookie and tag), then the load.
		mapLockSent = true
		call(t, "map_lock_lock_go", mapObject, "lock", "lock", cephmsgr.OSDCallWrite, rbdLockLockInputFor("", ""))
		_, mapInfo := lockInfo(t, "map_lock_get_info_go_owner", mapObject)
		if len(mapInfo.Lockers) != 1 || uint64(mapInfo.Lockers[0].NameNumber) != gid || mapInfo.Lockers[0].Cookie != "" || mapInfo.Tag != "" {
			t.Fatal("object map lock lacks the Go lock", mapInfo)
		}
		out := call(t, "object_map_load_go_owner", mapObject, "rbd", "object_map_load", cephmsgr.OSDCallRead, nil)
		objectMap(t, "go_owner_load", out, false, 1, 1, 0, 0, 0)
		mark := notifications()
		e := goNotify(t, "notify_acquired_lock", rbdLockNotify(rbdNotifyAcquiredLock, goID))
		if len(replyFrom(t, e, nativeOwner)) != 0 || len(replyFrom(t, e, goID)) != 0 {
			t.Fatal("AcquiredLock acknowledgements are not empty", e.Replies)
		}
		ownNotification(t, mark, "own AcquiredLock", rbdNotifyAcquiredLock)

		// librbd's pre-write object-map update for an absent object:
		// UpdateRequest sends [lock.assert_locked(rbd_lock, exclusive, "", ""),
		// rbd.object_map_update(n, n+1, OBJECT_EXISTS, none)] as one write.
		number := plan.GoWrites[0].Offset >> order
		update := rbdObjectMapUpdateInput(number, number+1, rbdObjectExists, nil)
		compound := request(mr,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: rbdLockAssertInput("", ""), Class: cephmsgr.OSDClassInput{Class: "lock", Method: "assert_locked", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: update, Class: cephmsgr.OSDClassInput{Class: "rbd", Method: "object_map_update", Mode: cephmsgr.OSDCallWrite, MaxOutputBytes: 64}})
		compound.ModificationTime = now()
		reply, err := mo.Request(ctx, compound)
		record("object_map_update_librbd_form", mr, compound, reply, err)
		switch {
		case errors.Is(err, cephmsgr.ErrUnsupportedOSDOperation) && reply.Transaction == 0:
			// The product refuses a CALL after a CALL locally (nothing is
			// sent). The guard goes alone first; it is not atomic with the
			// update, which the held object-map lock covers here.
			evidence.CompoundObjectMapCall = "refused locally: " + err.Error()
			if out := call(t, "map_assert_locked_go", mapObject, "lock", "assert_locked", cephmsgr.OSDCallRead, rbdLockAssertInput("", "")); len(out) != 0 {
				t.Fatal("assert_locked returned output", out)
			}
			call(t, "object_map_update_go", mapObject, "rbd", "object_map_update", cephmsgr.OSDCallWrite, update)
		case err == nil:
			evidence.CompoundObjectMapCall = "sent"
			checkReply(t, mr, reply, 0, true)
			if len(reply.Operations) != 2 || reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 {
				t.Fatal("librbd-form object-map update per-op results differ", reply.Operations)
			}
		default:
			t.Fatal("librbd-form object-map update", err)
		}
		goMap = readRaw(t, "object_map_read_go_updated", mapObject)
		objectMap(t, "go_updated_raw", goMap, true, 1, 1, 1, 0, 0)
		out = call(t, "object_map_load_go_updated", mapObject, "rbd", "object_map_load", cephmsgr.OSDCallRead, nil)
		objectMap(t, "go_updated_load", out, false, 1, 1, 1, 0, 0)

		// librbd's ObjectWriteRequest for an object the map says is absent,
		// in an image without snapshots or parent: SETALLOCHINT (object
		// size, FAILOK) and WRITE, no SnapContext.
		for _, ext := range plan.GoWrites {
			data := rbdLockPattern(token, "go", ext.Offset, ext.Length)
			for _, piece := range rbdObjectExtents(ext.Offset, ext.Length, order) {
				number, offset, length := piece[0], piece[1], piece[2]
				object := fmt.Sprintf("%s.%016x", prefix, number)
				r, o := route(t, object)
				req := request(r,
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: cephmsgr.OSDFailOK, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: objectSize}},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: offset, Length: length, Data: data[:length]})
				req.ModificationTime = now()
				reply, err := o.Request(ctx, req)
				record(fmt.Sprintf("go_write_%d_%d", number, offset), r, req, reply, err)
				if err != nil {
					t.Fatal("Go RBD data write", err)
				}
				checkReply(t, r, reply, 0, true)
				if reply.Operations[0].Result != 0 || reply.Operations[1].Result != 0 || reply.Operations[1].Length != length {
					t.Fatal("Go RBD data write per-op results differ", reply.Operations)
				}
				goData[number] = append(make([]byte, offset), data[:length]...)
				if got := readRaw(t, fmt.Sprintf("go_read_%d", number), object); !bytes.Equal(got, goData[number]) {
					t.Fatal("Go read of its data object differs")
				}
				data = data[length:]
			}
		}
		apply("go", plan.GoWrites)
		proceed(t, 2)
		p := waitPhase(t, "go-owner")
		sameHex(t, "go-owner", p.HeaderLock, header, "lock", "get_info", rbdLockGetInfoInput(), goLocked)
		if p.IsOwner || len(p.Owners) != 1 || p.Owners[0].Mode != rbdLockModeExclusive || p.Owners[0].Owner != goLegacy || p.Lockers.Empty || !p.Lockers.Exclusive || p.Lockers.Tag != rbdLockTag || len(p.Lockers.Lockers) != 1 ||
			p.Lockers.Lockers[0].Client != fmt.Sprintf("client.%d", gid) || p.Lockers.Lockers[0].Cookie != cookie || p.Lockers.Lockers[0].Address != goLegacy || len(p.Watchers) != 2 || p.Flags != 0 {
			t.Fatal("native librbd does not see Go as the lock owner", p.IsOwner, p.Owners, p.Lockers)
		}
		if !slices.ContainsFunc(p.Watchers, func(x nativeRBDWatcher) bool { return x.ID == gid && x.Cookie == w.Cookie() && x.Address == goLegacy }) {
			t.Fatal("native watchers_list does not show the Go owner alive", p.Watchers, goLegacy)
		}
		if p.MapRaw.Hex != hex.EncodeToString(goMap) {
			t.Fatal("native librados object-map read differs from Go's after the Go update")
		}
		mapNative, err := decodeRBDLockInfo(mustHex(t, p.MapLock.OutputHex))
		if err != nil || len(mapNative.Lockers) != 1 || uint64(mapNative.Lockers[0].NameNumber) != gid {
			t.Fatal("native exec does not show the Go object-map lock", mapNative, err)
		}
		diffObjects(t, "go-owner", p.Diff, 0, 1, 2)
		for i, ext := range plan.GoWrites {
			sum := sha256.Sum256(rbdLockPattern(token, "go", ext.Offset, ext.Length))
			if p.GoExtents[i].SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("native librbd read of the Go extent differs while Go holds the lock")
			}
		}
		step("go_owner", map[string]any{"client_id": goID, "cookie": cookie, "address": goLegacy, "native_owners": p.Owners, "compound_object_map_update": evidence.CompoundObjectMapCall})
		t.Logf("Go holds the lock as %q at %s; object map %x; compound update: %s", cookie, goLegacy, goMap, evidence.CompoundObjectMapCall)
	})
	if t.Failed() {
		return
	}

	t.Run("native_requests_lock_back", func(t *testing.T) {
		mark := notifications()
		proceed(t, 3)
		// librbd's write finds the lock held by a live owner (BreakRequest:
		// -EAGAIN), waits for it and asks for it.
		requested := waitNotification(t, mark, "native RequestLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyRequestLock && n.NotifierGlobalID == nativeGID
		})
		answeredAt := time.Now()
		if requested.Client != nativeOwner || requested.Force || requested.Version != 7 || !requested.AnsweredAsOwner || requested.AckHex != hex.EncodeToString(rbdResponseMessage(0)) || requested.AckError != "" {
			t.Fatal("native RequestLock or the Go owner's answer differs", requested)
		}
		// Release as librbd's PreReleaseRequest and ReleaseRequest do: close
		// the object map (unlock), unlock the header, announce ReleasedLock.
		responder.setOwning(false)
		call(t, "map_lock_unlock_go", mapObject, "lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(""))
		mapUnlocked = true
		call(t, "lock_unlock_go", header, "lock", "unlock", cephmsgr.OSDCallWrite, rbdLockUnlockInput(cookie))
		headerUnlocked = true
		e := goNotify(t, "notify_released_lock", rbdLockNotify(rbdNotifyReleasedLock, goID))
		if len(replyFrom(t, e, nativeOwner)) != 0 || len(replyFrom(t, e, goID)) != 0 {
			t.Fatal("ReleasedLock acknowledgements are not empty", e.Replies)
		}
		evidence.Timings["request_to_go_release"] = time.Since(answeredAt).String()
		ownNotification(t, mark, "own ReleasedLock", rbdNotifyReleasedLock)
		acquired := waitNotification(t, mark, "native AcquiredLock", func(n rbdLockNotification) bool {
			return n.Op == rbdNotifyAcquiredLock && n.NotifierGlobalID == nativeGID
		})
		if acquired.Client != nativeOwner || acquired.AckHex != "" || acquired.AckError != "" {
			t.Fatal("native AcquiredLock differs", acquired)
		}
		p := waitPhase(t, "reacquired")
		checkWrites(t, "reacquire", p.Writes, "reacquire", plan.ReacquireWrites)
		apply("reacquire", plan.ReacquireWrites)
		// The write waited for Go's release, not for librbd's
		// client_notify_timeout retry.
		evidence.Timings["native_reacquire_write"] = time.Duration(p.WriteNS).String()
		if time.Duration(p.WriteNS) >= rbdNotifyTimeout || time.Duration(p.WriteNS) >= rbdRequestLockRetry {
			t.Fatal("native write did not complete promptly after the Go release", time.Duration(p.WriteNS))
		}
		events, _ := responder.snapshot()
		if n := len(slices.DeleteFunc(slices.Clone(events[mark:]), func(n rbdLockNotification) bool {
			return n.Op != rbdNotifyRequestLock || n.NotifierGlobalID != nativeGID
		})); n != 1 {
			t.Fatal("native librbd asked for the lock more than once", n)
		}
		out, info := lockInfo(t, "lock_get_info_native_reacquired", header)
		sameHex(t, "reacquired", p.HeaderLock, header, "lock", "get_info", rbdLockGetInfoInput(), out)
		if len(info.Lockers) != 1 || uint64(info.Lockers[0].NameNumber) != nativeGID || info.Lockers[0].Cookie != rbdLockCookie(nativeOwner.Handle) {
			t.Fatal("native librbd does not hold the lock again", info)
		}
		mapOut, mapInfo := lockInfo(t, "map_lock_get_info_native_reacquired", mapObject)
		sameHex(t, "reacquired", p.MapLock, mapObject, "lock", "get_info", rbdLockGetInfoInput(), mapOut)
		if len(mapInfo.Lockers) != 1 || uint64(mapInfo.Lockers[0].NameNumber) != nativeGID {
			t.Fatal("native librbd did not lock the object map again", mapInfo)
		}
		if !p.IsOwner || len(p.Owners) != 1 || p.Owners[0].Mode != rbdLockModeExclusive || p.Owners[0].Owner != nativeLegacy || p.Lockers.Empty || len(p.Lockers.Lockers) != 1 || p.Lockers.Lockers[0].Cookie != rbdLockCookie(nativeOwner.Handle) || p.Flags != 0 {
			t.Fatal("native librbd does not report the reacquired lock", p.IsOwner, p.Owners, p.Lockers)
		}
		raw := readRaw(t, "object_map_read_native_reacquired", mapObject)
		if hex.EncodeToString(raw) != p.MapRaw.Hex {
			t.Fatal("Go object-map read differs from native librados after the reacquire")
		}
		objectMap(t, "native_reacquired_raw", raw, true, 1, 1, 1, 1, 0)
		diffObjects(t, "reacquired", p.Diff, 0, 1, 2, 3)
		// librbd reads through its object map while it owns the lock: an
		// object the loaded map called absent would read as zeros.
		for i, ext := range plan.GoWrites {
			sum := sha256.Sum256(rbdLockPattern(token, "go", ext.Offset, ext.Length))
			if p.GoExtents[i].SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("native lock owner reads the Go extent differently (stale object map?)")
			}
		}
		step("native_reacquired", map[string]any{"request_lock": requested, "acquired_lock": acquired, "write_ns": p.WriteNS})
		t.Logf("librbd asked with %s, Go released, librbd reacquired with %s; its write took %v", requested.PayloadHex, acquired.PayloadHex, time.Duration(p.WriteNS))
	})
	if t.Failed() {
		return
	}

	var final *nativeRBDLock
	t.Run("object_map_check", func(t *testing.T) {
		mark := notifications()
		proceed(t, 4)
		select {
		case <-controlDone:
		case <-time.After(90 * time.Second):
			t.Fatal("native RBD lock action did not complete")
		}
		if controlErr != nil {
			t.Fatal("native RBD lock action", controlErr)
		}
		result := readNative(t, "lock")
		final = checkNative(t, "lock", result)
		if final.NativeGlobalID != nativeGID || len(final.Phases) != 5 {
			t.Fatal("native RBD lock receipt differs", final.NativeGlobalID, len(final.Phases))
		}
		closed, check := final.Phases[4], final.Check
		evidence.Check = check
		// The close released the lock: ReleasedLock from the native owner;
		// the check then took and gave back the lock in its own client.
		events, failure := responder.snapshot()
		if failure != nil {
			t.Fatal("header watch responder", failure)
		}
		after := events[mark:]
		if len(after) != 3 || after[0].Op != rbdNotifyReleasedLock || after[0].Client != nativeOwner || after[1].Op != rbdNotifyAcquiredLock || after[2].Op != rbdNotifyReleasedLock ||
			after[1].Client != after[2].Client || after[1].Client.GID == gid || after[1].Client.GID == nativeGID || after[1].NotifierGlobalID != after[1].Client.GID ||
			slices.ContainsFunc(after, func(n rbdLockNotification) bool { return n.AckHex != "" || n.AckError != "" }) {
			t.Fatalf("notifications of the native close and the object-map check differ: %+v", after)
		}
		if !closed.Lockers.Empty || closed.Flags != 0 {
			t.Fatal("closed image is still locked or flagged", closed.Lockers, closed.Flags)
		}
		for _, l := range []nativeRBDWriteClassCall{closed.HeaderLock, closed.MapLock} {
			if info, err := decodeRBDLockInfo(mustHex(t, l.OutputHex)); err != nil || len(info.Lockers) != 0 {
				t.Fatal("closed image keeps a locker", l.Object, info, err)
			}
		}
		// rbd object-map check found nothing to invalidate.
		if check.ReturnCode != 0 || check.Stdout != "" || check.Stderr != "" || final.Flags != 0 {
			t.Fatalf("rbd object-map check differs: %+v flags %d", check, final.Flags)
		}
		if final.MapRaw.Hex != closed.MapRaw.Hex {
			t.Fatal("the object-map check changed the object map")
		}
		raw := readRaw(t, "object_map_read_final", mapObject)
		if hex.EncodeToString(raw) != final.MapRaw.Hex {
			t.Fatal("Go object-map read differs from the final native read")
		}
		objectMap(t, "final_raw", raw, true, 1, 1, 1, 1, 0)
		out := call(t, "object_map_load_final", mapObject, "rbd", "object_map_load", cephmsgr.OSDCallRead, nil)
		sameHex(t, "final", final.ClassCalls[7], mapObject, "rbd", "object_map_load", nil, out)
		objectMap(t, "final_load", out, false, 1, 1, 1, 1, 0)
		diffObjects(t, "closed", closed.Diff, 0, 1, 2, 3)
		diffObjects(t, "final", final.Diff, 0, 1, 2, 3)
		if !final.Lockers.Empty || len(final.Watchers) != 1 || final.Watchers[0].ID != gid || final.Watchers[0].Cookie != w.Cookie() {
			t.Fatal("final native observation differs (lockers, only the Go watch)", final.Lockers, final.Watchers)
		}
		// Native librbd and librados read every writer's bytes; Go reads the
		// same objects.
		v := views(t, final)
		sum := sha256.Sum256(expectedHead)
		evidence.ExpectedHeadSHA = hex.EncodeToString(sum[:])
		if !bytes.Equal(v["image_head"], expectedHead) {
			t.Fatal("native librbd head read differs from the setup, owner, Go and reacquire writes")
		}
		for i, o := range final.Objects {
			got := readRaw(t, fmt.Sprintf("final_read_%d", i), o.Name)
			if o.Head.Exists != (got != nil) || o.Head.Exists && !bytes.Equal(v["objects_head"][o.Head.Offset:o.Head.Offset+o.Head.Size], got) {
				t.Fatalf("native librados object %d differs from the Go read", i)
			}
			if want := uint64(i) < 4; o.Head.Exists != want {
				t.Fatalf("object %d existence %v, want %v", i, o.Head.Exists, want)
			}
		}
		if got := v["objects_head"][final.Objects[2].Head.Offset : final.Objects[2].Head.Offset+final.Objects[2].Head.Size]; !bytes.Equal(got, goData[2]) {
			t.Fatal("native librados reads object 2 differently from the Go write")
		}
		evidence.Timings["object_map_check"] = time.Duration(check.ElapsedNS).String()
		evidence.Timings["native_close"] = time.Duration(closed.CloseNS).String()
		step("object_map_check", map[string]any{"check": check, "check_client": after[1].Client, "flags": final.Flags, "diff": final.Diff})
		t.Logf("rbd object-map check: exit %d in %v as client.%d; object map %s", check.ReturnCode, time.Duration(check.ElapsedNS), after[1].Client.GID, final.MapRaw.Hex)
	})
	if t.Failed() {
		return
	}

	t.Run("cleanup_state", func(t *testing.T) {
		stopResponder()
		events, failure := responder.snapshot()
		if failure != nil {
			t.Fatal("header watch responder", failure)
		}
		drain, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		if event, err := w.Next(drain); err == nil {
			t.Fatal("unacknowledged header notification left", event.NotifyID, hex.EncodeToString(event.Payload))
		}
		stop()
		unwatch, err := w.Unwatch(ctx, hr.Epoch)
		record("lock_watch_unwatch", hr, watchRequest, unwatch, err)
		if err != nil {
			t.Fatal("Go header unwatch", err)
		}
		watching = false
		if watchers := listWatchers(t, "list_watchers_released", header); len(watchers) != 0 {
			t.Fatal("header watchers left", watchers)
		}
		headerOut, info := lockInfo(t, "lock_get_info_released", header)
		mapOut, mapInfo := lockInfo(t, "map_lock_get_info_released", mapObject)
		if len(info.Lockers) != 0 || len(mapInfo.Lockers) != 0 {
			t.Fatal("locks left after the protocol", info, mapInfo)
		}
		released := checkNative(t, "observe-released", native(t, "observe-released"))
		sameHex(t, "observe-released", released.ClassCalls[5], header, "lock", "get_info", rbdLockGetInfoInput(), headerOut)
		sameHex(t, "observe-released", released.ClassCalls[6], mapObject, "lock", "get_info", rbdLockGetInfoInput(), mapOut)
		if !released.Lockers.Empty || len(released.Watchers) != 0 || released.Flags != 0 || released.MapRaw.Hex != final.MapRaw.Hex {
			t.Fatal("native observation after the Go cleanup differs", released.Lockers, released.Watchers, released.Flags)
		}
		if !bytes.Equal(views(t, released)["image_head"], expectedHead) {
			t.Fatal("native head read changed after the Go cleanup")
		}
		ops := map[uint32]int{}
		for _, n := range events {
			ops[n.Op]++
		}
		responder.mu.Lock()
		pingErrors := slices.Clone(responder.pingErrors)
		responder.mu.Unlock()
		if len(pingErrors) != 0 {
			t.Fatal("Go header watch pings failed", pingErrors)
		}
		step("cleanup", map[string]any{"notifications_by_op": ops, "notifications": len(events)})
		t.Logf("%d header notifications acknowledged (%v by op); no Go watch or lock left", len(events), ops)
	})
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	data, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

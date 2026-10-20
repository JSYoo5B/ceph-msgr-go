package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Fresh native Tentacle MClientReconnect vectors produced by
// tools/mds_reconnect_native_encoder.cc; see
// mds-reconnect-native-provenance.json. Every vector is a canonical native
// Message::encode of a fresh object filled with add_cap/add_snaprealm/
// mark_more at the declared HEAD_VERSION 5/COMPAT_VERSION 4, with the whole
// payload in the data segment and an empty front. The receipts are what
// separate default-constructed native objects decoded from that segment.

type nativeFilelockReceipt struct {
	Start  uint64 `json:"start"`
	Length uint64 `json:"length"`
	Client uint64 `json:"client"`
	Owner  uint64 `json:"owner"`
	PID    uint64 `json:"pid"`
	Type   uint8  `json:"type"`
}

type nativeCapReconnectReceipt struct {
	Ino         uint64                  `json:"ino"`
	Path        string                  `json:"path"`
	CapID       uint64                  `json:"cap_id"`
	Wanted      uint32                  `json:"wanted"`
	Issued      uint32                  `json:"issued"`
	SnapRealm   uint64                  `json:"snaprealm"`
	PathBase    uint64                  `json:"pathbase"`
	FlockLen    uint32                  `json:"flock_len"`
	Flock       string                  `json:"flockbl_hex"`
	SnapFollows uint64                  `json:"snap_follows"`
	FcntlLocks  []nativeFilelockReceipt `json:"fcntl_locks"`
	FlockLocks  []nativeFilelockReceipt `json:"flock_locks"`
}

type nativeRealmReconnectReceipt struct {
	Ino    uint64 `json:"ino"`
	Seq    uint64 `json:"seq"`
	Parent uint64 `json:"parent"`
}

type nativeReconnectReceipt struct {
	Type    uint16                        `json:"message_type"`
	Version uint16                        `json:"version"`
	Compat  uint16                        `json:"compat_version"`
	TID     uint64                        `json:"tid"`
	Caps    []nativeCapReconnectReceipt   `json:"caps"`
	Realms  []nativeRealmReconnectReceipt `json:"realms"`
	More    bool                          `json:"more"`
}

type nativeReconnectProof struct {
	Commit     string `json:"ceph_commit"`
	Wrapper    string `json:"wrapper_sha256"`
	Driver     string `json:"build_driver_sha256"`
	Launcher   string `json:"launcher_sha256"`
	Terminated bool   `json:"builder_termination_completed"`
	Canonical  bool   `json:"canonical_message_encode_verified"`
	Decoder    bool   `json:"native_payload_decoder_verified"`
	Fixtures   map[string]struct {
		Front   string          `json:"front_sha256"`
		Data    string          `json:"data_sha256"`
		Header  string          `json:"header_sha256"`
		Decoded json.RawMessage `json:"decoded_reconnect"`
	} `json:"fixtures"`
}

func readNativeReconnectProof(t *testing.T) nativeReconnectProof {
	t.Helper()
	data, err := os.ReadFile("testdata/mds-reconnect-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof nativeReconnectProof
	if err = json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func nativeReconnectMessage(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	header, front, data := nativeMapBytes(t, name, "header"), nativeMapBytes(t, name, "front"), nativeMapBytes(t, name, "data")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, nil, data}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sequence != 1 || m.Transaction != 0 || m.AckSequence != 3 || m.Priority != 127 || len(m.Front) != 0 || len(m.Middle) != 0 ||
		len(m.Data) == 0 || !bytes.Equal(m.Data, data) || !bytes.Equal(header, m.Frame().Segments[0]) {
		t.Fatal("native offline reconnect header lost", m)
	}
	if m.Type != ClientReconnectMessage || m.Version != 5 || m.CompatVersion != 4 {
		t.Fatal("native canonical reconnect version/compat differs", m)
	}
	return m
}

// The flock blob inputs, as Client::_encode_filelocks writes the held
// fcntl and flock locks of client 4100: a byte-range shared fcntl lock and a
// whole-file exclusive flock.
var nativeReconnectLocks = struct{ fcntl, flock []nativeFilelockReceipt }{
	fcntl: []nativeFilelockReceipt{{Start: 4096, Length: 8192, Client: 4100, Owner: 0x8888, PID: 4242, Type: 1}},
	flock: []nativeFilelockReceipt{{Start: 0, Length: 0, Client: 4100, Owner: 0x9999, PID: 4243, Type: 2}},
}

// nativeFilelockBlob writes the Client::_encode_filelocks layout from the
// test inputs: a u32 count and packed 41-byte ceph_filelock records for
// fcntl locks, then the same for flock locks.
func nativeFilelockBlob(fcntl, flock []nativeFilelockReceipt) []byte {
	var b []byte
	for _, locks := range [][]nativeFilelockReceipt{fcntl, flock} {
		b = binary.LittleEndian.AppendUint32(b, uint32(len(locks)))
		for _, l := range locks {
			for _, v := range []uint64{l.Start, l.Length, l.Client, l.Owner, l.PID} {
				b = binary.LittleEndian.AppendUint64(b, v)
			}
			b = append(b, l.Type)
		}
	}
	return b
}

// The reconnect inputs are written here as Client::send_reconnect fills
// them (caps in insertion order, which is not inode order), without
// consulting the native bytes or receipts. Cap masks: PIN 0x1, AUTH_SHARED
// 0x4, LINK_SHARED 0x10, XATTR_SHARED 0x40, FILE_SHARED 0x100, FILE_CACHE
// 0x400, FILE_RD 0x800, FILE_WR 0x1000, FILE_BUFFER 0x2000.
var nativeReconnectInputs = []struct {
	name   string
	caps   []CapReconnect
	realms []RealmReconnect
	more   bool
}{
	{name: "mds-reconnect-native-reconnect-two-caps",
		caps: []CapReconnect{
			{Ino: 0x10000000005, CapID: 0x7002, Wanted: 0x3c00, Issued: 0x3d55, SnapRealm: 0x10000000004, PathBase: 0x10000000004,
				Path: "data.bin", SnapFollows: 9, Flock: nativeFilelockBlob(nativeReconnectLocks.fcntl, nativeReconnectLocks.flock)},
			{Ino: 0x10000000003, CapID: 0x7001, Wanted: 0x100, Issued: 0x115, SnapRealm: 1, PathBase: 1, Path: "dir"},
		},
		realms: []RealmReconnect{{Ino: 0x10000000004, Seq: 9, Parent: 1}, {Ino: 1, Seq: 3}}},
	{name: "mds-reconnect-native-reconnect-more",
		caps: []CapReconnect{{Ino: 0x10000000006, CapID: 0x7003, Wanted: 0xc00, Issued: 0xd01, SnapRealm: 1, PathBase: 0x10000000003, Path: "notes.txt"}},
		more: true},
	{name: "mds-reconnect-native-reconnect-empty",
		realms: []RealmReconnect{{Ino: 1, Seq: 3}}},
}

func reconnectFromReceipt(t *testing.T, r *nativeReconnectReceipt) ([]CapReconnect, []RealmReconnect) {
	t.Helper()
	var caps []CapReconnect
	for _, c := range r.Caps {
		flock := nilIfEmpty(mustHex(t, c.Flock))
		if int(c.FlockLen) != len(flock) {
			t.Fatal("native flock_len differs from the flock blob", c.FlockLen, len(flock))
		}
		caps = append(caps, CapReconnect{Ino: c.Ino, CapID: c.CapID, Wanted: c.Wanted, Issued: c.Issued, SnapRealm: c.SnapRealm,
			PathBase: c.PathBase, Path: c.Path, SnapFollows: c.SnapFollows, Flock: flock})
	}
	var realms []RealmReconnect
	for _, r := range r.Realms {
		realms = append(realms, RealmReconnect{Ino: r.Ino, Seq: r.Seq, Parent: r.Parent})
	}
	return caps, realms
}

// Go must reproduce the native data segment, empty front and 41-byte header
// byte-for-byte, and decode the native segment to exactly what the native
// decoder reported.
func TestEncodeReconnectMatchesFreshNative(t *testing.T) {
	proof := readNativeReconnectProof(t)
	for _, tc := range nativeReconnectInputs {
		t.Run(tc.name, func(t *testing.T) {
			native := nativeReconnectMessage(t, tc.name)
			got, err := EncodeReconnect(tc.caps, tc.realms, tc.more)
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion ||
				got.Priority != native.Priority || got.Transaction != native.Transaction || len(got.Front) != 0 || len(got.Middle) != 0 {
				t.Fatalf("envelope = %+v, native %+v", got, native)
			}
			if !bytes.Equal(got.Front, native.Front) {
				t.Fatalf("front\n got %x\nnative %x", got.Front, native.Front)
			}
			if !bytes.Equal(got.Data, native.Data) {
				t.Fatalf("data\n got %x\nnative %x", got.Data, native.Data)
			}
			got.Sequence, got.AckSequence = 1, 3
			if header := nativeMapBytes(t, tc.name, "header"); !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatalf("header\n got %x\nnative %x", got.Frame().Segments[0], header)
			}

			var r nativeReconnectReceipt
			strictNativeReceipt(t, tc.name, proof.Fixtures[tc.name].Decoded, &r)
			if r.Type != ClientReconnectMessage || r.Version != 5 || r.Compat != 4 || r.TID != 0 || r.More != tc.more {
				t.Fatal("native reconnect receipt envelope", r.Type, r.Version, r.Compat, r.TID, r.More)
			}
			caps, realms, more, err := DecodeReconnect(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			wantCaps, wantRealms := reconnectFromReceipt(t, &r)
			if !reflect.DeepEqual(caps, wantCaps) || !reflect.DeepEqual(realms, wantRealms) || more != r.More {
				t.Fatalf("Go decode differs from native decode\n got %+v %+v %v\nnative %+v %+v %v", caps, realms, more, wantCaps, wantRealms, r.More)
			}
			// The native map orders caps by inode; the inputs were not.
			for i := 1; i < len(r.Caps); i++ {
				if r.Caps[i-1].Ino >= r.Caps[i].Ino {
					t.Fatal("native caps are not in inode order", r.Caps)
				}
			}
			if len(tc.caps) > 1 && reflect.DeepEqual(caps, tc.caps) {
				t.Fatal("vector does not exercise inode reordering")
			}
			if !reflect.DeepEqual(realms, tc.realms) {
				t.Fatalf("realms %+v, inputs %+v", realms, tc.realms)
			}
			// Re-encoding the decoded view reproduces the native data.
			again, err := EncodeReconnect(caps, realms, more)
			if err != nil || !bytes.Equal(again.Data, native.Data) {
				t.Fatalf("re-encoded data differs: %v", err)
			}
		})
	}
}

// The opaque flock blob carried by the Go codec is exactly what the native
// MDS reads with Server::recover_filelocks.
func TestNativeReconnectFlockBlobLayout(t *testing.T) {
	proof := readNativeReconnectProof(t)
	name := "mds-reconnect-native-reconnect-two-caps"
	var r nativeReconnectReceipt
	strictNativeReceipt(t, name, proof.Fixtures[name].Decoded, &r)
	locked := 0
	for _, c := range r.Caps {
		if c.FlockLen == 0 {
			if len(c.FcntlLocks) != 0 || len(c.FlockLocks) != 0 || c.Flock != "" {
				t.Fatal("native locks without a flock blob", c)
			}
			continue
		}
		locked++
		if !reflect.DeepEqual(c.FcntlLocks, nativeReconnectLocks.fcntl) || !reflect.DeepEqual(c.FlockLocks, nativeReconnectLocks.flock) ||
			!bytes.Equal(mustHex(t, c.Flock), nativeFilelockBlob(nativeReconnectLocks.fcntl, nativeReconnectLocks.flock)) {
			t.Fatalf("native filelocks %+v / %+v differ from the inputs", c.FcntlLocks, c.FlockLocks)
		}
		if c.SnapFollows == 0 {
			t.Fatal("locked cap lost its pending cap snap", c)
		}
	}
	if locked != 1 {
		t.Fatal("two-caps vector must carry exactly one flock blob", locked)
	}
	if r.Caps[0].CapID == r.Caps[1].CapID || r.Caps[0].Wanted == r.Caps[1].Wanted || r.Caps[0].Issued == r.Caps[1].Issued ||
		r.Caps[0].SnapRealm == r.Caps[1].SnapRealm || r.Caps[0].PathBase == r.Caps[1].PathBase || r.Caps[0].Path == r.Caps[1].Path {
		t.Fatal("two-caps vector fields are not distinctive", r.Caps)
	}
}

func TestNativeReconnectProvenanceMatchesCurrentSourcesAndSegments(t *testing.T) {
	proof := readNativeReconnectProof(t)
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Terminated || !proof.Decoder || !proof.Canonical || len(proof.Fixtures) != len(nativeReconnectInputs) {
		t.Fatal("native reconnect proof lacks pinned source, cleanup or decoder")
	}
	for _, producer := range []struct{ hash, file string }{{proof.Wrapper, "mds_reconnect_native_encoder.cc"}, {proof.Driver, "mds_reconnect_native_encoder.py"}, {proof.Launcher, "mds_reconnect_native_encoder.py"}} {
		raw, err := os.ReadFile("../../tools/" + producer.file)
		if err != nil {
			t.Fatal(err)
		}
		if producer.hash != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("native reconnect producer hash differs", producer.file)
		}
	}
	for _, tc := range nativeReconnectInputs {
		fixture, ok := proof.Fixtures[tc.name]
		if !ok || len(fixture.Decoded) == 0 {
			t.Fatal("native reconnect vector or decoder receipt missing", tc.name)
		}
		for segment, digest := range map[string]string{"header": fixture.Header, "front": fixture.Front, "data": fixture.Data} {
			if digest != fmt.Sprintf("%x", sha256.Sum256(nativeMapBytes(t, tc.name, segment))) {
				t.Fatal("native reconnect segment differs", tc.name, segment)
			}
		}
		if len(nativeMapBytes(t, tc.name, "front")) != 0 {
			t.Fatal("native reconnect front is not empty", tc.name)
		}
	}
}

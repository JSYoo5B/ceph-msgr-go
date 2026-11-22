package maps

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

type mgrDaemonVector struct {
	Type    uint16         `json:"type"`
	Version uint16         `json:"version"`
	Compat  uint16         `json:"compat"`
	Front   string         `json:"front_hex"`
	Middle  string         `json:"middle_hex"`
	Data    string         `json:"data_hex"`
	Header  string         `json:"header_hex"`
	Decoded map[string]any `json:"decoded"`
}

// mgrDaemonVectors loads testdata/mgr-daemon-native-provenance.json: MGR
// daemon-role MON control messages produced by fresh native Ceph v20.2.4
// (commit 7f793731f1b39eb4f465e960113d2363c311b964) objects through the
// canonical Message::encode, each checked by a separate native decoder, and
// two beacon fronts the native decoder refused. Regenerate on a Linux amd64
// Docker host with copies of tools/mgr_daemon_native_encoder.{py,cc} and
// tools/mon_ping_native_encoder.{py,cc} in a writable directory mounted at
// /tools and an empty /work:
//
//	docker run --rm -v <tools>:/tools -v <work>:/work --entrypoint python3 \
//	  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
//	  /tools/mgr_daemon_native_encoder.py --work /work --wrapper /tools/mgr_daemon_native_encoder.cc
//
// The last JSON line of its output is the provenance file. Headers are an
// offline MessageFrame projection (sequence 1, ACK 3, default priority), not
// a ProtocolV2 capture. All keys in it are synthetic constants.
func mgrDaemonVectors(t *testing.T) map[string]mgrDaemonVector {
	t.Helper()
	var proof struct {
		Commit       string                     `json:"ceph_commit"`
		Image        string                     `json:"image"`
		Architecture string                     `json:"build_architecture"`
		Wrapper      string                     `json:"wrapper_sha256"`
		Driver       string                     `json:"build_driver_sha256"`
		Bootstrap    map[string]string          `json:"bootstrap_sources_sha256"`
		Canonical    bool                       `json:"canonical_message_encode_verified"`
		Decoder      bool                       `json:"native_payload_decoder_verified"`
		Loader       bool                       `json:"matching_native_loader_verified"`
		Fixtures     map[string]mgrDaemonVector `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/mgr-daemon-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" ||
		proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" ||
		proof.Architecture != "x86_64" || !proof.Canonical || !proof.Decoder || !proof.Loader || len(proof.Fixtures) != 14 {
		t.Fatal("pinned native provenance missing")
	}
	producers := map[string]string{"mgr_daemon_native_encoder.cc": proof.Wrapper, "mgr_daemon_native_encoder.py": proof.Driver}
	for _, path := range []string{"mon_ping_native_encoder.py", "mon_ping_native_encoder.cc"} {
		producers[path] = proof.Bootstrap[path]
	}
	for path, want := range producers {
		b, err := os.ReadFile(filepath.Join("..", "..", "tools", path))
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != want {
			t.Fatal("producer differs from retained proof", path)
		}
	}
	return proof.Fixtures
}

func unhexVector(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// nativeMessage checks the envelope of a vector separately from its front:
// the header's type, version and compat, and empty middle and data segments.
func nativeMessage(t *testing.T, v mgrDaemonVector, typ, version, compat uint16) msgr.MessageData {
	t.Helper()
	header := unhexVector(t, v.Header)
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header}})
	if err != nil || m.Type != typ || m.Version != version || m.CompatVersion != compat || v.Type != typ {
		t.Fatal("native envelope", m, err)
	}
	if v.Middle != "" || v.Data != "" {
		t.Fatal("native vector has middle or data segments")
	}
	m.Front = unhexVector(t, v.Front)
	return m
}

func vectorAddress(t *testing.T, kind uint32, endpoint string) msgr.Address {
	t.Helper()
	return msgr.Address{Type: kind, Nonce: 1234, Endpoint: netip.MustParseAddrPort(endpoint)}
}

func vectorModules() []ModuleInfo {
	return []ModuleInfo{{Name: "zeta", CanRun: true}, {Name: "alpha", ErrorString: "missing dep", Options: map[string]ModuleOption{"opt": {
		Name: "opt", Type: 4, Level: 2, Flags: 1, DefaultValue: "on", Min: "0", Max: "9",
		EnumAllowed: []string{"on", "off"}, Description: "d", LongDescription: "long",
		Tags: []string{"t2", "t1"}, SeeAlso: []string{"x"},
	}}}}
}

var vectorFSID = [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}

// TestMgrBeaconMatchesNativeEncoder requires byte equality of the Go
// MMgrBeacon front with the native encoder's, for a standby beacon and a
// populated active one (unsorted sets and maps, a module with every option
// collection, two command descriptors, three addresses of two types and
// families, and clients with a repeated name and an empty vector).
func TestMgrBeaconMatchesNativeEncoder(t *testing.T) {
	vectors := mgrDaemonVectors(t)
	standby, err := EncodeMgrBeacon(MgrBeacon{GlobalID: 4242, Name: "x", FSID: vectorFSID}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sixOne := vectorAddress(t, 2, "10.0.0.6:0")
	sixOne.Nonce = 77
	sixTwo := sixOne
	sixTwo.Nonce = 78
	active, err := EncodeMgrBeacon(MgrBeacon{
		GlobalID: 4242, Name: "x", FSID: vectorFSID, Available: true,
		ServerAddresses: []msgr.Address{vectorAddress(t, 2, "10.0.0.5:6800"), vectorAddress(t, 1, "10.0.0.5:6801"), vectorAddress(t, 2, "[fd00::5]:6802")},
		Commands: []MgrCommand{
			{Command: "gotest status name=x,type=CephString,req=false", Help: "show", Module: "mgr", Permissions: "r", Flags: 24},
			{Command: "gotest reset", Module: "mgr", Permissions: "rw"},
		},
		Metadata: map[string]string{"binary": "\x00\xff", "addr": "10.0.0.5"},
		Services: map[string]string{"dashboard": "https://10.0.0.5:8443/"},
		Modules:  vectorModules(), Features: 0x3f,
		Clients: []MgrClient{{Name: "rbd_support"}, {Name: "libcephsqlite", Addresses: []msgr.Address{sixOne}}, {Name: "libcephsqlite", Addresses: []msgr.Address{sixTwo}}},
	}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]msgr.MessageData{"beacon-standby": standby, "beacon-active": active} {
		native := nativeMessage(t, vectors[name], msgr.MgrBeaconMessage, 11, 8)
		if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion || got.Priority != 196 || got.Middle != nil || got.Data != nil {
			t.Fatal("Go beacon envelope differs", name, got.Version, got.CompatVersion, got.Priority)
		}
		if !bytes.Equal(got.Front, native.Front) {
			t.Fatalf("%s front differs from the native encoder:\n go     %x\n native %x", name, got.Front, native.Front)
		}
	}
	// The native decoder refused a front one byte short and one whose client
	// name count differs from its address count. The Go encoder derives both
	// lists from one slice, so it cannot produce the second.
	for _, name := range []string{"beacon-truncated", "beacon-count-mismatch"} {
		if vectors[name].Decoded["native_refused"] != true {
			t.Fatal("native refusal receipt missing", name)
		}
	}
	mismatch := unhexVector(t, vectors["beacon-count-mismatch"].Front)
	if bytes.Equal(mismatch, active.Front) || !bytes.HasPrefix(active.Front, unhexVector(t, vectors["beacon-truncated"].Front)) {
		t.Fatal("refused fronts are not the documented mutations")
	}
}

func TestMgrMapDecodesNativeEncodings(t *testing.T) {
	vectors := mgrDaemonVectors(t)
	populated := nativeMessage(t, vectors["mgrmap-populated"], msgr.MgrMapMessage, 1, 1)
	got, err := DecodeMgr(populated.Front)
	if err != nil {
		t.Fatal(err)
	}
	sixOne := vectorAddress(t, 2, "10.0.0.6:0")
	sixOne.Nonce = 77
	activeAddresses := []msgr.Address{vectorAddress(t, 2, "10.0.0.5:6800"), vectorAddress(t, 1, "10.0.0.5:6801")}
	modules := vectorModules()
	// Sets decode sorted, as the native encoder wrote them.
	modules[1].Options["opt"] = ModuleOption{Name: "opt", Type: 4, Level: 2, Flags: 1, DefaultValue: "on", Min: "0", Max: "9", EnumAllowed: []string{"off", "on"}, Description: "d", LongDescription: "long", Tags: []string{"t1", "t2"}, SeeAlso: []string{"x"}}
	want := Mgr{
		Epoch: 12, GlobalID: 4242, Available: true, Name: "x",
		Addresses: activeAddresses[:1], ActiveAddresses: activeAddresses,
		Standbys:       []Standby{{Name: "y", GlobalID: 4300, Modules: []ModuleInfo{{Name: "zeta", CanRun: true}}, Features: 0x3f}, {Name: "z", GlobalID: 4400}},
		EnabledModules: []string{"balancer", "pg_autoscaler"}, Services: map[string]string{"dashboard": "https://10.0.0.5:8443/"},
		AvailableModules: modules, AlwaysOnModules: map[uint32][]string{20: {"balancer", "crash"}},
		ForceDisabledModules: []string{"telemetry"}, ActiveChange: [2]uint32{1700000000, 5}, ActiveFeatures: 0x3f,
		LastFailureOSDEpoch: 9, Clients: []MgrClient{{Name: "libcephsqlite", Addresses: []msgr.Address{sixOne}}}, Flags: 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("populated MgrMap differs from the native decoder's fields:\n got  %+v\n want %+v", got, want)
	}
	empty, err := DecodeMgr(nativeMessage(t, vectors["mgrmap-empty"], msgr.MgrMapMessage, 1, 1).Front)
	if err != nil || empty.Epoch != 1 || empty.ActiveAddresses != nil && len(empty.ActiveAddresses) != 0 || empty.Available || empty.Standbys != nil || empty.Clients != nil {
		t.Fatal("empty MgrMap", empty, err)
	}
	null, err := DecodeMgr(nativeMessage(t, vectors["mgrmap-null"], msgr.MgrMapMessage, 1, 1).Front)
	if err != nil || null.Epoch != NullMgrEpoch || null.GlobalID != 0 || null.Available || len(null.ActiveAddresses) != 0 {
		t.Fatal("native null MgrMap", null, err)
	}
	// The pre-Nautilus feature variant is the v5 legacy encoding; only the
	// modern 14/6 shape is supported.
	if _, err := DecodeMgr(nativeMessage(t, vectors["mgrmap-legacy"], msgr.MgrMapMessage, 1, 1).Front); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("legacy MgrMap accepted", err)
	}
	for n := range len(populated.Front) {
		if _, err := DecodeMgr(populated.Front[:n]); err == nil {
			t.Fatal("truncated native MgrMap accepted", n)
		}
	}
	// clients_names count is followed by one name, flags and the one-entry
	// force_disabled set.
	front := bytes.Clone(populated.Front)
	at := len(front) - (4 + 4 + len("libcephsqlite") + 8 + 4 + 4 + len("telemetry"))
	if binary.LittleEndian.Uint32(front[at:]) != 1 {
		t.Fatal("client name count offset")
	}
	binary.LittleEndian.PutUint32(front[at:], 2)
	if _, err := DecodeMgr(front); err == nil {
		t.Fatal("client name/address count mismatch accepted")
	}
	front = bytes.Clone(populated.Front)
	binary.LittleEndian.PutUint32(front[2:], binary.LittleEndian.Uint32(front[2:])+1)
	if _, err := DecodeMgr(front); err == nil {
		t.Fatal("overlong MgrMap envelope accepted")
	}
}

func TestMgrDigestAndAuthMessagesMatchNativeEncodings(t *testing.T) {
	vectors := mgrDaemonVectors(t)
	for name, want := range map[string]msgr.Digest{
		"digest-populated": {MonStatus: []byte(`{"epoch":3}`), Health: []byte(`{"status":"HEALTH_OK"}`)},
		"digest-empty":     {MonStatus: []byte{}, Health: []byte{}},
	} {
		got, err := msgr.DecodeDigest(nativeMessage(t, vectors[name], msgr.MgrDigestMessage, 1, 1), wire.DefaultLimit)
		if err != nil || !bytes.Equal(got.MonStatus, want.MonStatus) || !bytes.Equal(got.Health, want.Health) {
			t.Fatal("native digest", name, got, err)
		}
	}
	request := msgr.AuthMessageRequest(cephx.RotatingRequest(), 3)
	native := nativeMessage(t, vectors["auth-rotating-request"], msgr.AuthMessage, 1, 1)
	if request.Version != 1 || request.CompatVersion != 1 || !bytes.Equal(request.Front, native.Front) {
		t.Fatalf("MAuth rotating request differs from native:\n go     %x\n native %x", request.Front, native.Front)
	}
	for _, name := range []string{"auth-rotating-reply", "auth-rotating-reply-aes"} {
		v := vectors[name]
		reply, err := msgr.DecodeAuthReply(nativeMessage(t, v, msgr.AuthReplyMessage, 1, 1), wire.DefaultLimit)
		if err != nil || reply.Result != 0 || reply.GlobalID != 4242 || reply.Message != "" {
			t.Fatal("native auth reply", reply, err)
		}
		principal := cephx.DecodeKey(wire.NewDecoder(unhexVector(t, v.Decoded["principal_key_hex"].(string))))
		ring, err := cephx.DecodeRotatingReply(reply.ResultBL, principal)
		if err != nil || ring.MaxVersion != 7 || len(ring.Keys) != 3 {
			t.Fatal("native rotating secrets", name, err)
		}
		keys := v.Decoded["keys"].([]any)
		for i, key := range ring.Keys {
			native := keys[i].(map[string]any)
			expires := native["expires"].([]any)
			material := cephx.DecodeKey(wire.NewDecoder(unhexVector(t, native["key_hex"].(string))))
			if float64(key.ID) != native["id"] || !key.Expires.Equal(time.Unix(int64(expires[0].(float64)), int64(expires[1].(float64)))) || !reflect.DeepEqual(key.Key, material) {
				t.Fatal("rotating key differs from the native decoder", name, i)
			}
		}
		if _, ok := ring.Lookup(6); !ok {
			t.Fatal("native set lookup")
		}
		other := cephx.DecodeKey(wire.NewDecoder(unhexVector(t, vectors["auth-rotating-reply"].Decoded["keys"].([]any)[0].(map[string]any)["key_hex"].(string))))
		if _, err := cephx.DecodeRotatingReply(reply.ResultBL, other); err == nil {
			t.Fatal("rotating set decrypted with another key")
		}
		for n := range len(reply.ResultBL) {
			if _, err := cephx.DecodeRotatingReply(reply.ResultBL[:n], principal); err == nil {
				t.Fatal("truncated rotating reply accepted", n)
			}
		}
	}
	refused, err := msgr.DecodeAuthReply(nativeMessage(t, vectors["auth-reply-refused"], msgr.AuthReplyMessage, 1, 1), wire.DefaultLimit)
	if err != nil || refused.Result != -13 || refused.Message != "denied" || len(refused.ResultBL) != 0 {
		t.Fatal("native refused reply", refused, err)
	}
	front := unhexVector(t, vectors["auth-reply-refused"].Front)
	for n := range len(front) {
		if _, err := msgr.DecodeAuthReply(msgr.MessageData{Type: msgr.AuthReplyMessage, Version: 1, CompatVersion: 1, Front: front[:n]}, wire.DefaultLimit); err == nil {
			t.Fatal("truncated auth reply accepted", n)
		}
	}
	if _, err := msgr.DecodeAuthReply(msgr.MessageData{Type: msgr.AuthReplyMessage, Version: 2, CompatVersion: 2, Front: front}, wire.DefaultLimit); !errors.Is(err, wire.ErrVersion) {
		t.Fatal("future auth reply compat accepted", err)
	}
	if _, err := msgr.DecodeAuthReply(msgr.MessageData{Type: msgr.AuthReplyMessage, Version: 1, CompatVersion: 1, Front: front}, uint32(len(front)+40)); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("auth reply limit ignores the header", err)
	}
}

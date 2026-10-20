package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Fresh native Tentacle MClientSession vectors produced by
// tools/mds_session_native_encoder.cc; see mds-session-native-provenance.json.

type nativeSessionReceipt struct {
	Type               uint16            `json:"message_type"`
	Version            uint16            `json:"version"`
	Op                 uint32            `json:"op"`
	Sequence           uint64            `json:"seq"`
	MaxCaps            uint32            `json:"max_caps"`
	MaxLeases          uint32            `json:"max_leases"`
	Metadata           map[string]string `json:"metadata"`
	SupportedFeatures  string            `json:"supported_features_hex"`
	MetricSpec         string            `json:"metric_spec_hex"`
	Flags              uint32            `json:"flags"`
	OldestClientTID    uint64            `json:"oldest_client_tid"`
	SourceOldestClient uint64            `json:"source_oldest_client_tid"`
	CapAuths           []struct {
		UID        int64    `json:"uid"`
		GIDs       []uint32 `json:"gids"`
		Path       string   `json:"path"`
		FSName     string   `json:"fs_name"`
		RootSquash bool     `json:"root_squash"`
		Readable   bool     `json:"readable"`
		Writeable  bool     `json:"writeable"`
	} `json:"cap_auths"`
}

type nativeSessionProof struct {
	Commit     string `json:"ceph_commit"`
	Wrapper    string `json:"wrapper_sha256"`
	Driver     string `json:"build_driver_sha256"`
	Launcher   string `json:"launcher_sha256"`
	Terminated bool   `json:"builder_termination_completed"`
	Canonical  bool   `json:"canonical_message_encode_verified"`
	Decoder    bool   `json:"native_payload_decoder_verified"`
	Fixtures   map[string]struct {
		Front   string               `json:"front_sha256"`
		Data    string               `json:"data_sha256"`
		Header  string               `json:"header_sha256"`
		Decoded nativeSessionReceipt `json:"decoded_session"`
	} `json:"fixtures"`
}

func readNativeSessionProof(t *testing.T) nativeSessionProof {
	t.Helper()
	data, err := os.ReadFile("testdata/mds-session-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof nativeSessionProof
	if err = json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func nativeSessionMessage(t *testing.T, name string, version uint16) msgr.MessageData {
	t.Helper()
	header := nativeMapBytes(t, name, "header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, nativeMapBytes(t, name, "front"), nil, nativeMapBytes(t, name, "data")}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sequence != 1 || m.Transaction != 0 || m.AckSequence != 3 || m.Priority != 127 || len(m.Middle) != 0 || len(m.Data) != 0 || !bytes.Equal(header, m.Frame().Segments[0]) {
		t.Fatal("native offline session header lost", m)
	}
	if m.Type != ClientSessionMessage || m.Version != version || m.CompatVersion != 1 {
		t.Fatal("native canonical session version/compat differs", m)
	}
	return m
}

// nativeBitsetImage re-adds the u32 byte length that DecodeClientSession
// strips, giving the exact feature_bitset_t::encode image.
func nativeBitsetImage(raw []byte) []byte {
	return append(binary.LittleEndian.AppendUint32(nil, uint32(len(raw))), raw...)
}

func TestEncodeClientSessionMatchesFreshNative(t *testing.T) {
	proof := readNativeSessionProof(t)
	for _, tc := range []struct {
		name    string
		version uint16
		encode  func() (msgr.MessageData, error)
	}{
		{"request-open", 7, func() (msgr.MessageData, error) {
			return EncodeRequestOpen(map[string]string{"entity_id": "admin", "root": "/"}, ClientFeatures), nil
		}},
		{"request-renewcaps", 1, func() (msgr.MessageData, error) { return EncodeSessionControl(SessionRequestRenewCaps, 5) }},
		{"request-close", 1, func() (msgr.MessageData, error) { return EncodeSessionControl(SessionRequestClose, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-session-native-" + tc.name
			native := nativeSessionMessage(t, name, tc.version)
			got, err := tc.encode()
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion || got.Priority != native.Priority || len(got.Middle) != 0 || len(got.Data) != 0 {
				t.Fatalf("envelope = %+v, native %+v", got, native)
			}
			if !bytes.Equal(got.Front, native.Front) {
				t.Fatalf("front\n got %x\nnative %x", got.Front, native.Front)
			}
			got.Sequence, got.AckSequence = 1, 3
			if header := nativeMapBytes(t, name, "header"); !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatalf("header\n got %x\nnative %x", got.Frame().Segments[0], header)
			}
			// The native decoder receipt binds what actually crossed the wire:
			// renewcaps set oldest_client_tid 9 at the source, but version 1
			// does not transmit it.
			receipt := proof.Fixtures[name].Decoded
			decoded, err := DecodeClientSession(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Op != receipt.Op || decoded.Sequence != receipt.Sequence || decoded.OldestClientTID != receipt.OldestClientTID || decoded.OldestClientTID != NoOldestClientTID {
				t.Fatalf("decoded = %+v, native receipt %+v", decoded, receipt)
			}
			if tc.name == "request-renewcaps" && receipt.SourceOldestClient != 9 {
				t.Fatal("renewcaps source oldest_client_tid input lost")
			}
			if tc.version == 7 {
				if !bytes.Equal(decoded.SupportedFeatures, ClientFeatures) || !reflect.DeepEqual(decoded.Metadata, receipt.Metadata) ||
					hex.EncodeToString(nativeBitsetImage(decoded.SupportedFeatures)) != receipt.SupportedFeatures {
					t.Fatalf("request-open extension = %+v, native receipt %+v", decoded, receipt)
				}
			}
		})
	}
}

func TestDecodeFreshNativeServerSessions(t *testing.T) {
	proof := readNativeSessionProof(t)
	all := []byte{0xff, 0xff, 0xff, 0, 0, 0, 0, 0}  // CEPHFS_FEATURES_ALL: bits 0-23
	metrics := []byte{0xff, 0xff, 0, 0, 0, 0, 0, 0} // CEPHFS_METRIC_FEATURES_ALL: bits 0-15
	for _, tc := range []struct {
		name string
		want ClientSession
	}{
		{"open", ClientSession{Version: 7, Op: SessionOpen, Metadata: map[string]string{}, SupportedFeatures: all, MetricFlags: metrics,
			CapAuths: []CapAuth{{UID: -1, GIDs: []uint32{1000, 1001}, Path: "volumes/a", FilesystemName: "cephfs", RootSquash: true, Readable: true}}, OldestClientTID: NoOldestClientTID}},
		{"reject", ClientSession{Version: 7, Op: SessionReject, Metadata: map[string]string{"error_string": "blocklisted (blacklisted)"},
			Flags: SessionFlagBlocklisted, CapAuths: []CapAuth{}, OldestClientTID: NoOldestClientTID}},
		{"renewcaps", ClientSession{Version: 1, Op: SessionRenewCaps, Sequence: 5, OldestClientTID: NoOldestClientTID}},
		{"recall-state", ClientSession{Version: 1, Op: SessionRecallState, MaxCaps: 100, MaxLeases: 7, OldestClientTID: NoOldestClientTID}},
		{"stale", ClientSession{Version: 1, Op: SessionStale, OldestClientTID: NoOldestClientTID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "mds-session-native-" + tc.name
			m := nativeSessionMessage(t, name, tc.want.Version)
			got, err := DecodeClientSession(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.SupportedFeatures, tc.want.SupportedFeatures) || !bytes.Equal(got.MetricFlags, tc.want.MetricFlags) {
				t.Fatalf("bitsets = %x / %x", got.SupportedFeatures, got.MetricFlags)
			}
			cmp, want := got, tc.want
			cmp.SupportedFeatures, cmp.MetricFlags, want.SupportedFeatures, want.MetricFlags = nil, nil, nil, nil
			if !reflect.DeepEqual(cmp, want) || got.Stamp != (Time{}) {
				t.Fatalf("decoded\n got %+v\nwant %+v", got, tc.want)
			}
			// Bind every decoded field to the independent native decoder receipt.
			receipt := proof.Fixtures[name].Decoded
			if receipt.Type != ClientSessionMessage || receipt.Version != got.Version || receipt.Op != got.Op || receipt.Sequence != got.Sequence ||
				receipt.MaxCaps != got.MaxCaps || receipt.MaxLeases != got.MaxLeases || receipt.Flags != got.Flags || receipt.OldestClientTID != got.OldestClientTID ||
				len(receipt.Metadata) != len(got.Metadata) || len(receipt.CapAuths) != len(got.CapAuths) {
				t.Fatalf("native receipt %+v differs from %+v", receipt, got)
			}
			for k, v := range receipt.Metadata {
				if got.Metadata[k] != v {
					t.Fatal("native metadata differs", k)
				}
			}
			for i, auth := range receipt.CapAuths {
				if !reflect.DeepEqual(got.CapAuths[i], CapAuth{UID: auth.UID, GIDs: auth.GIDs, Path: auth.Path, FilesystemName: auth.FSName, RootSquash: auth.RootSquash, Readable: auth.Readable, Writeable: auth.Writeable}) {
					t.Fatal("native cap auth differs", got.CapAuths[i], auth)
				}
			}
			// metric_spec_t is ENCODE_START(1, 1) around its feature_bitset_t.
			features, flags := nativeBitsetImage(got.SupportedFeatures), nativeBitsetImage(got.MetricFlags)
			metric := append(binary.LittleEndian.AppendUint32([]byte{1, 1}, uint32(len(flags))), flags...)
			if hex.EncodeToString(features) != receipt.SupportedFeatures || hex.EncodeToString(metric) != receipt.MetricSpec {
				t.Fatalf("native bitset images %s / %s differ from %x / %x", receipt.SupportedFeatures, receipt.MetricSpec, features, metric)
			}
		})
	}
}

func TestNativeSessionProvenanceMatchesCurrentSourcesAndSegments(t *testing.T) {
	proof := readNativeSessionProof(t)
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Terminated || !proof.Decoder || !proof.Canonical || len(proof.Fixtures) != 8 {
		t.Fatal("native session proof lacks pinned source, cleanup or decoder")
	}
	for _, producer := range []struct{ hash, file string }{{proof.Wrapper, "mds_session_native_encoder.cc"}, {proof.Driver, "mds_session_native_encoder.py"}, {proof.Launcher, "mds_session_native_encoder.py"}} {
		raw, err := os.ReadFile("../../tools/" + producer.file)
		if err != nil {
			t.Fatal(err)
		}
		if producer.hash != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("native session producer hash differs", producer.file)
		}
	}
	for name, fixture := range proof.Fixtures {
		for segment, digest := range map[string]string{"header": fixture.Header, "front": fixture.Front, "data": fixture.Data} {
			if digest != fmt.Sprintf("%x", sha256.Sum256(nativeMapBytes(t, name, segment))) {
				t.Fatal("native session segment differs", name, segment)
			}
		}
	}
}

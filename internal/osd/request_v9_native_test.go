package osd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

const requestV9NativeFeatures uint64 = 3387274269135242038

// This suite intentionally requires retained fresh native artifacts. Before
// the authorized native build it can be compiled with -run '^$'; missing
// native vectors must never be manufactured by Go or silently accepted.
func requestV9NativeSegment(t *testing.T, name, segment string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "request-v9-native", name+"."+segment+".hex"))
	if err != nil {
		t.Fatal("requires separately built pinned native v9 evidence", err)
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestRequestV9MatchesFreshNativeCanonicalEncoder(t *testing.T) {
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum", "pgnls"} {
		t.Run(kind, func(t *testing.T) {
			var got msgr.MessageData
			var err error
			if kind == "pgnls" {
				got, err = EncodePGListRequestV9(pgListNativeRequest(), requestV9NativeFeatures, wire.DefaultLimit)
			} else {
				input := nativeV8Request(kind)
				input.Object = "v9-codec"
				got, err = EncodeRequestV9(V9Request{Request: input.Request, PG: input.PG, Shard: input.Shard}, requestV9NativeFeatures, wire.DefaultLimit)
			}
			if err != nil {
				t.Fatal(err)
			}
			name := "request-v9-native-" + kind
			header, front, data := requestV9NativeSegment(t, name, "header"), requestV9NativeSegment(t, name, "front"), requestV9NativeSegment(t, name, "data")
			native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, nil, data}})
			if err != nil {
				t.Fatal(err)
			}
			got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
			if native.Type != RequestMessage || native.Version != 9 || native.CompatVersion != 3 || native.Priority != 127 || native.Sequence != 1 || native.Transaction != 77 || native.AckSequence != 3 || !bytes.Equal(got.Front, front) || !bytes.Equal(got.Data, data) || !bytes.Equal(got.Frame().Segments[0], header) {
				t.Fatal("v9 header/front/data differ from canonical fresh native Message::encode", kind)
			}
			if len(front) < 98 || !bytes.Equal(front[63:87], make([]byte, 24)) || !bytes.Equal(front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
				t.Fatal("native trace is not the admitted empty legacy/OTEL prefix")
			}
			target, err := DecodeRequestV9AdmissionTarget(native, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			hash := uint32(0x11223344)
			object := "v9-codec"
			key := "route-key"
			if kind == "pgnls" {
				hash, object, key = 0x44, "", ""
			}
			if target.PG != (PG{Pool: 7, Seed: 0x44}) || target.Shard != -1 || target.Object.Hash != hash || target.Object.Object != object || target.Object.Key != key || target.Object.Namespace != "tenant" || target.Object.Snap != NoSnap {
				t.Fatal("native actual PG/raw hash/head admission identity changed", target)
			}
		})
	}
}

func TestRequestV9RetainedNativeProofIsCurrentAndClosed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "request-v9-native", "provenance.json"))
	if err != nil {
		t.Fatal("requires actual closed native v9 build proof", err)
	}
	var proof struct {
		Commit    string `json:"ceph_commit"`
		Image     string `json:"image"`
		Wrapper   string `json:"wrapper_sha256"`
		Driver    string `json:"build_driver_sha256"`
		Config    string `json:"generated_config_sha256"`
		Library   string `json:"native_library_sha256"`
		Compiler  string `json:"compiler_binary_sha256"`
		Canonical bool   `json:"native_canonical_message_encode_verified"`
		Decode    bool   `json:"native_payload_decoder_verified"`
		Cleanup   bool   `json:"builder_termination_completed"`
		SDK       struct {
			Option     string `json:"cmake_option"`
			HaveJaeger bool   `json:"have_jaeger"`
			Config     string `json:"generated_config_sha256"`
			Cache      string `json:"cmake_cache_sha256"`
		} `json:"sdk_tracing"`
		Selection struct {
			Canonical bool   `json:"canonical_message_encode"`
			With      uint16 `json:"with_server_squid_version"`
			Without   uint16 `json:"without_server_squid_version"`
			Features  uint64 `json:"features"`
			Empty     string `json:"empty_otel_hex"`
		} `json:"feature_selection_control"`
		Fixtures map[string]struct {
			HeaderHash string `json:"header_sha256"`
			FrontHash  string `json:"front_sha256"`
			DataHash   string `json:"data_sha256"`
			Routing    struct {
				Valid    bool   `json:"otel_valid"`
				Final    bool   `json:"finish_decode_performed"`
				Second   bool   `json:"second_finish_decode_needed"`
				Empty    string `json:"empty_otel_hex"`
				Features uint64 `json:"features"`
			} `json:"decoded_routing"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" || !proof.Canonical || !proof.Decode || !proof.Cleanup || len(proof.Fixtures) != 5 || proof.SDK.HaveJaeger || proof.SDK.Option != "WITH_JAEGER=OFF" || proof.SDK.Config != proof.Config || len(proof.Config) != 64 || len(proof.Library) != 64 || len(proof.Compiler) != 64 || len(proof.SDK.Cache) != 64 {
		t.Fatal("native v9 proof lacks closed SDK/source/compile/library/cleanup evidence")
	}
	if !proof.Selection.Canonical || proof.Selection.With != 9 || proof.Selection.Without != 8 || proof.Selection.Features != requestV9NativeFeatures || proof.Selection.Empty != "01010100000000" {
		t.Fatal("native canonical feature-selection control missing")
	}
	for file, want := range map[string]string{"osd_request_v9_native_encoder.cc": proof.Wrapper, "osd_request_v9_native_encoder.py": proof.Driver} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "tools", file))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != want {
			t.Fatal("native v9 source differs from actual build", file)
		}
	}
	for _, kind := range []string{"read", "mutation", "snapshot", "checksum", "pgnls"} {
		name := "request-v9-native-" + kind
		fixture, ok := proof.Fixtures[name]
		if !ok {
			t.Fatal("native five-vector set changed", name)
		}
		for segment, want := range map[string]string{"header": fixture.HeaderHash, "front": fixture.FrontHash, "data": fixture.DataHash} {
			if fmt.Sprintf("%x", sha256.Sum256(requestV9NativeSegment(t, name, segment))) != want {
				t.Fatal("native retained segment hash differs", name, segment)
			}
		}
		if fixture.Routing.Valid || !fixture.Routing.Final || fixture.Routing.Second || fixture.Routing.Empty != "01010100000000" || fixture.Routing.Features != requestV9NativeFeatures {
			t.Fatal("fresh native partial/final invalid OTEL receipt differs", name)
		}
	}
}

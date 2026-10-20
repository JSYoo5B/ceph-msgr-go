package osd

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// These are explicit native codec inputs, not a live negotiation or placement
// result. Bit35 is identical for normal/IgnoreCache pairs in each wire branch.
var ignoreCacheNativeFeatures = map[uint16]uint64{
	6: 3026423347916342070 | 1<<35,
	8: 3386711319181820726 | 1<<35,
	9: 3387274269135242038 | 1<<35,
}

var ignoreCacheNativeKinds = []string{"stat", "read", "writefull", "delete"}

type ignoreCacheNativeFixture struct {
	Version    uint16 `json:"version"`
	Kind       string `json:"kind"`
	Ignore     bool   `json:"ignore_cache"`
	Header     string `json:"header_hex"`
	Front      string `json:"front_hex"`
	Middle     string `json:"middle_hex"`
	Data       string `json:"data_hex"`
	HeaderHash string `json:"header_sha256"`
	FrontHash  string `json:"front_sha256"`
	MiddleHash string `json:"middle_sha256"`
	DataHash   string `json:"data_sha256"`
	Decoded    struct {
		Features    uint64   `json:"features"`
		Flags       uint64   `json:"flags"`
		Pool        uint64   `json:"pool"`
		Seed        uint32   `json:"seed"`
		Shard       int8     `json:"shard"`
		RawHash     uint32   `json:"raw_hash"`
		ClientInc   uint32   `json:"client_inc"`
		ReqInc      uint32   `json:"reqid_inc"`
		ReqTID      uint64   `json:"reqid_tid"`
		Epoch       uint32   `json:"epoch"`
		LocatorHash int64    `json:"locator_hash"`
		Object      string   `json:"object"`
		Key         string   `json:"key"`
		Namespace   string   `json:"namespace"`
		Snap        uint64   `json:"snap"`
		SnapSeq     uint64   `json:"snap_seq"`
		Snaps       []uint64 `json:"snaps"`
		Retry       int32    `json:"retry_attempt"`
		Seconds     uint32   `json:"mtime_seconds"`
		Nanoseconds uint32   `json:"mtime_nanoseconds"`
		OTELValid   bool     `json:"otel_valid"`
		Final       bool     `json:"finish_decode_performed"`
		Second      bool     `json:"second_finish_decode_needed"`
		Operation   struct {
			Code       uint16 `json:"code"`
			Flags      uint32 `json:"flags"`
			Offset     uint64 `json:"offset"`
			Length     uint64 `json:"length"`
			PayloadLen uint32 `json:"payload_len"`
			Union      string `json:"union_hex"`
			Data       string `json:"indata_hex"`
		} `json:"operation"`
	} `json:"decoded"`
}

type ignoreCacheNativeProof struct {
	Commit        string `json:"ceph_commit"`
	Image         string `json:"image"`
	Architecture  string `json:"build_architecture"`
	Archive       string `json:"source_archive_sha256"`
	Config        string `json:"generated_config_sha256"`
	Cache         string `json:"cmake_cache_sha256"`
	Binary        string `json:"binary_sha256"`
	Library       string `json:"native_library_sha256"`
	Compiler      string `json:"compiler_binary_sha256"`
	Wrapper       string `json:"wrapper_sha256"`
	Driver        string `json:"build_driver_sha256"`
	SharedDriver  string `json:"shared_build_driver_sha256"`
	SharedWrapper string `json:"shared_wrapper_sha256"`
	Canonical     bool   `json:"canonical_message_encode_verified"`
	Decoder       bool   `json:"native_payload_decoder_verified"`
	Header        bool   `json:"native_message_header_verified"`
	BuildLog      string `json:"build_log_sha256"`
	Execution     struct {
		Start           string `json:"start"`
		End             string `json:"end"`
		Name            string `json:"name"`
		Profile         string `json:"profile"`
		ExitCode        *int   `json:"exit_code"`
		ContainerExists *bool  `json:"container_exists_after_cleanup"`
	} `json:"execution"`
	Sources    map[string]string `json:"source_files_sha256"`
	Submodules map[string]struct {
		Commit  string `json:"commit"`
		Archive string `json:"archive_sha256"`
	} `json:"pinned_submodules"`
	SDK struct {
		Option     string `json:"cmake_option"`
		HaveJaeger bool   `json:"have_jaeger"`
	} `json:"sdk_tracing"`
	Fixtures map[string]ignoreCacheNativeFixture `json:"fixtures"`
}

func loadIgnoreCacheNativeProof(t *testing.T) ignoreCacheNativeProof {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "ignore-cache-native-provenance.json"))
	if err != nil {
		t.Fatal("requires separately generated pinned native 24-vector proof; no Go-generated substitute", err)
	}
	var p ignoreCacheNativeProof
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func ignoreCacheNativeRequest(kind string, ignore bool) Request {
	r := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "cache-codec", Hash: 0x11223344, IgnoreCache: ignore}
	switch kind {
	case "stat":
		r.Operations = []Operation{{Code: Stat}}
	case "read":
		r.Operations = []Operation{{Code: Read, Offset: 2, Length: 4}}
	case "writefull":
		r.Operations = []Operation{{Code: WriteFull, Length: 4, Data: []byte{0, 0xff, 'A', '\n'}}}
	case "delete":
		r.Operations = []Operation{{Code: Delete}}
	default:
		panic("outside closed IgnoreCache native operations")
	}
	if kind == "writefull" || kind == "delete" {
		r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 5
	}
	return r
}

func ignoreCacheNativeBytes(t *testing.T, encoded, hash string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != hash {
		t.Fatal("retained native segment hash differs")
	}
	return raw
}

func TestIgnoreCacheMatches24FreshNativeRequests(t *testing.T) {
	proof := loadIgnoreCacheNativeProof(t)
	if len(proof.Fixtures) != 24 {
		t.Fatal("native set must be exactly 3 versions x 4 standalone operations x 2 flags")
	}
	for version, features := range ignoreCacheNativeFeatures {
		for _, kind := range ignoreCacheNativeKinds {
			var normal []byte
			for _, ignore := range []bool{false, true} {
				mode := "normal"
				if ignore {
					mode = "ignore"
				}
				name := fmt.Sprintf("ignore-cache-native-v%d-%s-%s", version, kind, mode)
				t.Run(name, func(t *testing.T) {
					fixture, ok := proof.Fixtures[name]
					if !ok || fixture.Version != version || fixture.Kind != kind || fixture.Ignore != ignore {
						t.Fatal("native fixture identity differs")
					}
					r := ignoreCacheNativeRequest(kind, ignore)
					var got msgr.MessageData
					var err error
					switch version {
					case 6:
						got, err = EncodeRequest(r, features, wire.DefaultLimit)
					case 8:
						got, err = EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, features, wire.DefaultLimit)
					case 9:
						got, err = EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, features, wire.DefaultLimit)
					}
					if err != nil {
						t.Fatal(err)
					}
					header := ignoreCacheNativeBytes(t, fixture.Header, fixture.HeaderHash)
					front := ignoreCacheNativeBytes(t, fixture.Front, fixture.FrontHash)
					middle := ignoreCacheNativeBytes(t, fixture.Middle, fixture.MiddleHash)
					data := ignoreCacheNativeBytes(t, fixture.Data, fixture.DataHash)
					native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, middle, data}})
					if err != nil {
						t.Fatal(err)
					}
					got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
					if native.Type != RequestMessage || native.Version != version || native.CompatVersion != 3 || native.Sequence != 1 || native.Transaction != 77 || native.AckSequence != 3 || native.Priority != 127 || len(middle) != 0 || !bytes.Equal(got.Front, front) || !bytes.Equal(got.Middle, middle) || !bytes.Equal(got.Data, data) || !bytes.Equal(got.Frame().Segments[0], header) {
						t.Fatal("Go header/front/middle/data differ from fresh native canonical Message::encode")
					}
					d := fixture.Decoded
					flags := FlagOnDisk | FlagReturnVec | FlagRead
					if kind == "writefull" || kind == "delete" {
						flags = FlagOnDisk | FlagReturnVec | FlagWrite
					}
					if ignore {
						flags |= 0x8000
					}
					seed := uint32(0x44)
					if version == 6 {
						seed = r.Hash
					}
					if d.Features != features || d.Flags != flags || d.Pool != 7 || d.Seed != seed || d.Shard != -1 || d.RawHash != r.Hash || d.ClientInc != 1 || d.ReqInc != 1 || d.ReqTID != 77 || d.Epoch != 11 || d.LocatorHash != -1 || d.Object != r.Object || d.Key != r.Locator.Key || d.Namespace != r.Locator.Namespace || d.Snap != NoSnap || d.SnapSeq != 0 || len(d.Snaps) != 0 || d.Retry != 0 || d.Seconds != r.MTimeSeconds || d.Nanoseconds != r.MTimeNanoseconds || d.OTELValid || d.Final != (version >= 8) || d.Second {
						t.Fatal("native decoder request/identity/trace receipt differs", d)
					}
					op := r.Operations[0]
					length := op.Length
					if kind == "writefull" {
						length = uint64(len(op.Data))
					}
					packed := make([]byte, 28)
					binary.LittleEndian.PutUint64(packed[:8], op.Offset)
					binary.LittleEndian.PutUint64(packed[8:16], length)
					if d.Operation.Code != op.Code || d.Operation.Flags != 0 || d.Operation.Offset != op.Offset || d.Operation.Length != length || d.Operation.PayloadLen != uint32(len(op.Data)) || d.Operation.Union != hex.EncodeToString(packed) || d.Operation.Data != hex.EncodeToString(op.Data) {
						t.Fatal("native packed single operation receipt differs", d.Operation)
					}
					offset := 32
					if version == 6 {
						offset = 8
					}
					if binary.LittleEndian.Uint32(front[offset:offset+4]) != uint32(flags) {
						t.Fatal("native flag bytes contradict receipt")
					}
					if version >= 8 && !bytes.Equal(front[63:87], make([]byte, 24)) || version == 9 && !bytes.Equal(front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
						t.Fatal("native trace is not empty")
					}
					if !ignore {
						normal = bytes.Clone(front)
					} else {
						cleared := bytes.Clone(front)
						copy(cleared[offset:offset+4], normal[offset:offset+4])
						if !bytes.Equal(normal, cleared) {
							t.Fatal("normal/IgnoreCache differ beyond request flag0x8000")
						}
					}
				})
			}
		}
	}
}

func TestIgnoreCacheNativeProofSourceAndSDKClosure(t *testing.T) {
	p := loadIgnoreCacheNativeProof(t)
	if p.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || p.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" || p.Architecture != "x86_64" || !p.Canonical || !p.Decoder || !p.Header || p.SDK.Option != "WITH_JAEGER=OFF" || p.SDK.HaveJaeger || len(p.Fixtures) != 24 {
		t.Fatal("native source/SDK/canonical proof is incomplete")
	}
	start, startErr := time.Parse(time.RFC3339Nano, p.Execution.Start)
	end, endErr := time.Parse(time.RFC3339Nano, p.Execution.End)
	if startErr != nil || endErr != nil || !end.After(start) || strings.TrimSpace(p.Execution.Name) == "" || p.Execution.Profile != "fresh-amd64-native-offline-pinned-20.2.4" || p.Execution.ExitCode == nil || *p.Execution.ExitCode != 0 || p.Execution.ContainerExists == nil || *p.Execution.ContainerExists {
		t.Fatal("native execution/owned-container cleanup proof is incomplete")
	}
	for _, hash := range []string{p.Archive, p.Config, p.Cache, p.Binary, p.Library, p.Compiler, p.BuildLog} {
		raw, err := hex.DecodeString(hash)
		if err != nil || len(raw) != 32 {
			t.Fatal("missing actual native build digest")
		}
	}
	for file, want := range map[string]string{"osd_ignore_cache_native_encoder.cc": p.Wrapper, "osd_ignore_cache_native_encoder.py": p.Driver, "mon_ping_native_encoder.py": p.SharedDriver, "mon_ping_native_encoder.cc": p.SharedWrapper} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "tools", file))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != want {
			t.Fatal("native producer/build source differs from retained proof", file)
		}
	}
	paths := []string{"src/messages/MOSDOp.h", "src/osdc/Objecter.h", "src/include/rados.h", "src/include/ceph_features.h", "src/msg/Message.h", "src/msg/Message.cc", "src/msg/async/frames_v2.h", "src/common/tracer.h"}
	if len(p.Sources) != len(paths) || len(p.Submodules) < 8 {
		t.Fatal("native source/submodule provenance set is incomplete")
	}
	for _, path := range paths {
		raw, err := hex.DecodeString(p.Sources[path])
		if err != nil || len(raw) != 32 {
			t.Fatal("missing unchanged native codec digest", path)
		}
	}
	for path, module := range p.Submodules {
		commit, err := hex.DecodeString(module.Commit)
		archive, archiveErr := hex.DecodeString(module.Archive)
		if err != nil || archiveErr != nil || len(commit) != 20 || len(archive) != 32 {
			t.Fatal("missing pinned submodule evidence", path)
		}
	}
}

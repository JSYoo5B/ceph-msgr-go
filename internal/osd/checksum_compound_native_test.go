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

// Offline native request encoding/decoding only. These layouts do not prove
// server execution, checksum values, EOF, or a terminal prefix/suffix outcome.
var checksumCompoundNativeFeatures = map[uint16]uint64{
	6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038,
}

var checksumCompoundNativeShapes = []string{
	"mixed", "compare-boundary", "eof-layout", "assert-boundary", "checksum-boundary",
}

type checksumCompoundNativeOperation struct {
	Code       uint16 `json:"code"`
	Flags      uint32 `json:"flags"`
	Offset     uint64 `json:"offset"`
	Length     uint64 `json:"length"`
	PayloadLen uint32 `json:"payload_len"`
	Union      string `json:"union_hex"`
	Input      string `json:"indata_hex"`
	Output     string `json:"outdata_hex"`
	Result     int32  `json:"result"`
}

type checksumCompoundNativeDecoded struct {
	Features    uint64                            `json:"features"`
	Flags       uint64                            `json:"flags"`
	Pool        uint64                            `json:"pool"`
	Seed        uint32                            `json:"seed"`
	Shard       int8                              `json:"shard"`
	RawHash     uint32                            `json:"raw_hash"`
	ClientInc   uint32                            `json:"client_inc"`
	ReqInc      uint32                            `json:"reqid_inc"`
	ReqTID      uint64                            `json:"reqid_tid"`
	Epoch       uint32                            `json:"epoch"`
	LocatorHash int64                             `json:"locator_hash"`
	Object      string                            `json:"object"`
	Key         string                            `json:"key"`
	Namespace   string                            `json:"namespace"`
	Snap        uint64                            `json:"snap"`
	SnapSeq     uint64                            `json:"snap_seq"`
	Snaps       []uint64                          `json:"snaps"`
	Retry       int32                             `json:"retry_attempt"`
	Seconds     uint32                            `json:"mtime_seconds"`
	Nanoseconds uint32                            `json:"mtime_nanoseconds"`
	OTELValid   bool                              `json:"otel_valid"`
	Final       bool                              `json:"finish_decode_performed"`
	Second      bool                              `json:"second_finish_decode_needed"`
	Operations  []checksumCompoundNativeOperation `json:"operations"`
}

type checksumCompoundNativeFixture struct {
	ignoreCacheNativeFixture
	Shape   string                        `json:"shape"`
	Decoded checksumCompoundNativeDecoded `json:"decoded"`
}

type checksumCompoundNativeProof struct {
	ignoreCacheNativeProof
	HelperDriver string                                   `json:"helper_build_driver_sha256"`
	Loader       string                                   `json:"loader_audit"`
	Fixtures     map[string]checksumCompoundNativeFixture `json:"fixtures"`
}

func loadChecksumCompoundNativeProof(t *testing.T) checksumCompoundNativeProof {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "checksum-compound-native-provenance.json"))
	if err != nil {
		t.Fatal("requires actual fresh pinned native 15-request evidence; no skip or Go-generated substitute", err)
	}
	var proof checksumCompoundNativeProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func checksumCompoundNativeRequest(shape string) Request {
	c := func(algorithm uint8, offset, length uint64, chunk uint32, seed uint64) Operation {
		return Operation{Code: Checksum, Offset: offset, Length: length, Checksum: ChecksumInput{Type: algorithm, ChunkSize: chunk, Seed: seed}}
	}
	r := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "checksum-compound-codec", Hash: 0x11223344}
	switch shape {
	case "mixed":
		r.Operations = []Operation{c(ChecksumCRC32C, 0, 8, 4, 0x1234), {Code: Stat}, {Code: Read, Offset: 4, Length: 8}, c(ChecksumXXHash64, 0, 16, 0, 0xfedcba9876543210)}
	case "compare-boundary":
		r.Operations = []Operation{c(ChecksumXXHash32, 0, 8, 0, 0xffffffff), {Code: CompareExtent, Length: 4, Data: []byte{0, 0xff, 'A', '\n'}}, c(ChecksumXXHash64, 8, 8, 4, 0x8000000000000001), {Code: Stat}}
	case "eof-layout":
		r.Operations = []Operation{c(ChecksumCRC32C, 16, 8, 4, 1), {Code: Read, Length: 8}, {Code: Stat}}
	case "assert-boundary":
		r.Operations = []Operation{c(ChecksumCRC32C, 0, 8, 4, 1), {Code: AssertVersion, Version: 9}, c(ChecksumXXHash64, 0, 8, 0, 2), {Code: Read, Length: 4}}
	case "checksum-boundary":
		r.Operations = []Operation{{Code: Read, Length: 4}, c(ChecksumCRC32C, 12, 8, 8, 3), {Code: Stat}, c(ChecksumXXHash32, 0, 8, 0, 4)}
	default:
		panic("outside closed native compound request inventory")
	}
	return r
}

// Form explicit packed expectations without calling the product union/data
// encoders. ASSERT_VER has an unused u64 before its version (rados.h).
func checksumCompoundNativePacked(op Operation) (union, input []byte) {
	union = make([]byte, 28)
	switch op.Code {
	case Checksum:
		binary.LittleEndian.PutUint64(union, op.Offset)
		binary.LittleEndian.PutUint64(union[8:], op.Length)
		binary.LittleEndian.PutUint32(union[16:], op.Checksum.ChunkSize)
		union[20] = op.Checksum.Type
		input = make([]byte, 4)
		if op.Checksum.Type == ChecksumXXHash64 {
			input = make([]byte, 8)
			binary.LittleEndian.PutUint64(input, op.Checksum.Seed)
		} else {
			binary.LittleEndian.PutUint32(input, uint32(op.Checksum.Seed))
		}
	case AssertVersion:
		binary.LittleEndian.PutUint64(union[8:], op.Version)
	case CompareExtent, Read:
		binary.LittleEndian.PutUint64(union, op.Offset)
		binary.LittleEndian.PutUint64(union[8:], op.Length)
		input = bytes.Clone(op.Data)
	case Stat:
	default:
		panic("outside closed native compound operation inventory")
	}
	return union, input
}

func TestChecksumCompoundMatches15FreshNativeRequests(t *testing.T) {
	proof := loadChecksumCompoundNativeProof(t)
	if len(proof.Fixtures) != 15 {
		t.Fatal("requires exactly 3 versions x 5 compound request layouts")
	}
	for version, features := range checksumCompoundNativeFeatures {
		for _, shape := range checksumCompoundNativeShapes {
			name := fmt.Sprintf("checksum-compound-native-v%d-%s", version, shape)
			t.Run(name, func(t *testing.T) {
				f, ok := proof.Fixtures[name]
				if !ok || f.Version != version || f.Shape != shape {
					t.Fatal("native compound request inventory/identity differs")
				}
				r := checksumCompoundNativeRequest(shape)
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
				header := ignoreCacheNativeBytes(t, f.Header, f.HeaderHash)
				front := ignoreCacheNativeBytes(t, f.Front, f.FrontHash)
				middle := ignoreCacheNativeBytes(t, f.Middle, f.MiddleHash)
				data := ignoreCacheNativeBytes(t, f.Data, f.DataHash)
				native, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, middle, data}})
				if err != nil {
					t.Fatal(err)
				}
				got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
				if native.Type != RequestMessage || native.Version != version || native.CompatVersion != 3 || native.Sequence != 1 || native.Transaction != 77 || native.AckSequence != 3 || native.Priority != 127 || len(middle) != 0 || !bytes.Equal(got.Front, front) || !bytes.Equal(got.Middle, middle) || !bytes.Equal(got.Data, data) || !bytes.Equal(got.Frame().Segments[0], header) {
					t.Fatal("Go header/front/middle/data differ from canonical fresh native request")
				}
				d := f.Decoded
				flags := FlagOnDisk | FlagReturnVec | FlagRead
				seed := uint32(0x44)
				if version == 6 {
					seed = r.Hash
				}
				if d.Features != features || features&(1<<35) != 0 || d.Flags != flags || d.Pool != 7 || d.Seed != seed || d.Shard != -1 || d.RawHash != r.Hash || d.ClientInc != 1 || d.ReqInc != 1 || d.ReqTID != 77 || d.Epoch != 11 || d.LocatorHash != -1 || d.Object != r.Object || d.Key != r.Locator.Key || d.Namespace != r.Locator.Namespace || d.Snap != NoSnap || d.SnapSeq != 0 || len(d.Snaps) != 0 || d.Retry != 0 || d.Seconds != 0 || d.Nanoseconds != 0 || d.OTELValid || d.Final != (version >= 8) || d.Second || len(d.Operations) != len(r.Operations) {
					t.Fatal("native fresh decoder changed compound identity/head/flags/mtime/context/trace", d)
				}
				var expectedData, packedOps []byte
				packedOps = binary.LittleEndian.AppendUint16(packedOps, uint16(len(r.Operations)))
				for i, op := range r.Operations {
					u, input := checksumCompoundNativePacked(op)
					n := d.Operations[i]
					if n.Code != op.Code || n.Flags != 0 || n.Offset != binary.LittleEndian.Uint64(u) || n.Length != binary.LittleEndian.Uint64(u[8:]) || n.PayloadLen != uint32(len(input)) || n.Union != hex.EncodeToString(u) || n.Input != hex.EncodeToString(input) || n.Output != "" || n.Result != 0 {
						t.Fatalf("native op %d packed union/seed/input differs: %+v", i, n)
					}
					expectedData = append(expectedData, input...)
					packedOps = binary.LittleEndian.AppendUint16(packedOps, op.Code)
					packedOps = binary.LittleEndian.AppendUint32(packedOps, 0)
					packedOps = append(packedOps, u...)
					packedOps = binary.LittleEndian.AppendUint32(packedOps, uint32(len(input)))
				}
				if !bytes.Equal(data, expectedData) || bytes.Count(front, packedOps) != 1 {
					t.Fatal("native wire op order/union/payload lengths or seed/CMPEXT concatenation differs")
				}
				offset := 32
				if version == 6 {
					offset = 8
				}
				if len(front) < offset+4 || binary.LittleEndian.Uint32(front[offset:]) != uint32(flags) {
					t.Fatal("native flag bytes disagree with fresh decoder")
				}
				if version >= 8 {
					if len(front) < 94 || !bytes.Equal(front[63:87], make([]byte, 24)) || binary.LittleEndian.Uint64(front[len(front)-8:]) != features || version == 9 && !bytes.Equal(front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
						t.Fatal("native default features/empty legacy or OTEL trace differ")
					}
					var target RequestAdmissionTarget
					if version == 8 {
						target, err = DecodeRequestV8AdmissionTarget(native, wire.DefaultLimit)
					} else {
						target, err = DecodeRequestV9AdmissionTarget(native, wire.DefaultLimit)
					}
					if err != nil || target.PG != (PG{Pool: 7, Seed: 0x44}) || target.Shard != -1 || target.Object.Hash != r.Hash || target.Object.Object != r.Object || target.Object.Snap != NoSnap || target.Object.Namespace != r.Locator.Namespace || target.Object.Key != r.Locator.Key {
						t.Fatal("compound operations changed modern/backoff admission identity", target, err)
					}
				}
			})
		}
	}
}

func TestChecksumCompoundNativeProofSourceAndSDKClosure(t *testing.T) {
	p := loadChecksumCompoundNativeProof(t)
	if p.Archive != "f2144fc4375532342657dc531e256f04aab51ad35e46beec6d6b940eeccbbc1d" || p.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || p.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" || p.Architecture != "x86_64" || !p.Canonical || !p.Decoder || !p.Header || p.SDK.Option != "WITH_JAEGER=OFF" || p.SDK.HaveJaeger || len(p.Fixtures) != 15 {
		t.Fatal("native source/SDK/canonical proof is incomplete")
	}
	if !strings.Contains(p.Loader, "libceph-common.so.2 => /native-cache/work/build/lib/libceph-common.so.2") {
		t.Fatal("native loader does not bind the owned amd64 ceph-common")
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
	for file, want := range map[string]string{"osd_checksum_compound_native_encoder.cc": p.Wrapper, "osd_checksum_compound_native_encoder.py": p.Driver, "mon_ping_native_encoder.py": p.SharedDriver, "mon_ping_native_encoder.cc": p.SharedWrapper, "osd_ignore_cache_native_encoder.py": p.HelperDriver} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "tools", file))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != want {
			t.Fatal("native producer/shared build source differs from proof", file)
		}
	}
	paths := []string{"src/messages/MOSDOp.h", "src/osdc/Objecter.h", "src/include/rados.h", "src/include/ceph_features.h", "src/msg/Message.h", "src/msg/Message.cc", "src/msg/async/frames_v2.h", "src/common/tracer.h"}
	if len(p.Sources) != len(paths) || len(p.Submodules) < 8 {
		t.Fatal("native source/submodule provenance closure is incomplete")
	}
	for _, path := range paths {
		raw, err := hex.DecodeString(p.Sources[path])
		if err != nil || len(raw) != 32 {
			t.Fatal("missing native codec digest", path)
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

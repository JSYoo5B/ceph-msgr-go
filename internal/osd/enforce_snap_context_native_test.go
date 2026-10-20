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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Shared test-only receipt fields and digest readers belong to the same
// retained native SDK family. Existing IgnoreCache proof/source is unchanged.
type enforceSnapContextNativeFixture struct {
	ignoreCacheNativeFixture
	Context string `json:"context"`
	Enforce bool   `json:"enforce_snap_context"`
}
type enforceSnapContextNativeProof struct {
	ignoreCacheNativeProof
	HelperDriver string                                     `json:"helper_build_driver_sha256"`
	Fixtures     map[string]enforceSnapContextNativeFixture `json:"fixtures"`
}

var enforceSnapContextNativeFeatures = map[uint16]uint64{
	6: 3026423347916342070, 8: 3386711319181820726, 9: 3387274269135242038,
}

func loadEnforceSnapContextNativeProof(t *testing.T) enforceSnapContextNativeProof {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "enforce-snap-context-native-provenance.json"))
	if err != nil {
		t.Fatal("requires actual fresh pinned native 12-vector evidence; no skip or Go-generated substitute", err)
	}
	var proof enforceSnapContextNativeProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	return proof
}

func TestEnforceSnapContextMatches12FreshNativeRequests(t *testing.T) {
	proof := loadEnforceSnapContextNativeProof(t)
	if len(proof.Fixtures) != 12 {
		t.Fatal("requires exactly 3 versions x 2 contexts x 2 flags")
	}
	for version, features := range enforceSnapContextNativeFeatures {
		for _, context := range []string{"empty", "nonempty"} {
			var normal []byte
			for _, enforce := range []bool{false, true} {
				mode := "normal"
				if enforce {
					mode = "enforce"
				}
				name := fmt.Sprintf("enforce-snap-context-native-v%d-%s-%s", version, context, mode)
				t.Run(name, func(t *testing.T) {
					f, ok := proof.Fixtures[name]
					if !ok || f.Version != version || f.Context != context || f.Enforce != enforce {
						t.Fatal("native vector identity differs")
					}
					r := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "enforce-snapc-codec", Hash: 0x11223344,
						MTimeSeconds: 1700000000, MTimeNanoseconds: 5, OrderSnap: true, EnforceSnapContext: enforce, Operations: []Operation{{Code: WriteFull, Length: 4, Data: []byte{0, 0xff, 'A', '\n'}}}}
					if context == "nonempty" {
						r.SnapSeq, r.Snaps = 7, []uint64{7, 5}
					}
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
						t.Fatal("Go envelope/front/middle/data differ from canonical fresh native Message::encode")
					}
					d := f.Decoded
					flags := FlagOnDisk | FlagReturnVec | FlagWrite | uint64(0x40)
					if enforce {
						flags |= 0x100000
					}
					seed := uint32(0x44)
					if version == 6 {
						seed = r.Hash
					}
					if d.Features != features || features&(1<<35) != 0 || d.Flags != flags || d.Pool != 7 || d.Seed != seed || d.Shard != -1 || d.RawHash != r.Hash || d.ClientInc != 1 || d.ReqInc != 1 || d.ReqTID != 77 || d.Epoch != 11 || d.LocatorHash != -1 || d.Object != r.Object || d.Key != r.Locator.Key || d.Namespace != r.Locator.Namespace || d.Snap != NoSnap || d.SnapSeq != r.SnapSeq || !slices.Equal(d.Snaps, r.Snaps) || d.Retry != 0 || d.Seconds != r.MTimeSeconds || d.Nanoseconds != r.MTimeNanoseconds || d.OTELValid || d.Final != (version >= 8) || d.Second {
						t.Fatal("fresh native decoder changed identity/ORDERSNAP/ENFORCE_SNAPC/SnapContext/mtime", d)
					}
					packed := make([]byte, 28)
					binary.LittleEndian.PutUint64(packed[8:16], 4)
					if d.Operation.Code != WriteFull || d.Operation.Flags != 0 || d.Operation.Offset != 0 || d.Operation.Length != 4 || d.Operation.PayloadLen != 4 || d.Operation.Union != hex.EncodeToString(packed) || d.Operation.Data != "00ff410a" {
						t.Fatal("native packed WRITEFULL receipt differs")
					}
					offset := 32
					if version == 6 {
						offset = 8
					}
					if binary.LittleEndian.Uint32(front[offset:offset+4]) != uint32(flags) {
						t.Fatal("native flag bytes contradict fresh decoder receipt")
					}
					if version >= 8 && !bytes.Equal(front[63:87], make([]byte, 24)) || version == 9 && !bytes.Equal(front[87:94], []byte{1, 1, 1, 0, 0, 0, 0}) {
						t.Fatal("trace must remain empty")
					}
					if version >= 8 {
						var target RequestAdmissionTarget
						if version == 8 {
							target, err = DecodeRequestV8AdmissionTarget(native, wire.DefaultLimit)
						} else {
							target, err = DecodeRequestV9AdmissionTarget(native, wire.DefaultLimit)
						}
						if err != nil || target.PG != (PG{Pool: 7, Seed: 0x44}) || target.Shard != -1 || target.Object.Hash != r.Hash || target.Object.Object != r.Object || target.Object.Snap != NoSnap || target.Object.Namespace != r.Locator.Namespace || target.Object.Key != r.Locator.Key {
							t.Fatal("ENFORCE_SNAPC changed backoff admission identity", target, err)
						}
					}
					if !enforce {
						normal = bytes.Clone(front)
					} else {
						cleared := bytes.Clone(front)
						copy(cleared[offset:offset+4], normal[offset:offset+4])
						if !bytes.Equal(normal, cleared) {
							t.Fatal("false/true differ beyond request flag0x100000")
						}
					}
				})
			}
		}
	}
}

func TestEnforceSnapContextNativeProofSourceAndSDKClosure(t *testing.T) {
	p := loadEnforceSnapContextNativeProof(t)
	if p.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || p.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" || p.Architecture != "x86_64" || !p.Canonical || !p.Decoder || !p.Header || p.SDK.Option != "WITH_JAEGER=OFF" || p.SDK.HaveJaeger || len(p.Fixtures) != 12 {
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
	for file, want := range map[string]string{"osd_enforce_snap_context_native_encoder.cc": p.Wrapper, "osd_enforce_snap_context_native_encoder.py": p.Driver, "mon_ping_native_encoder.py": p.SharedDriver, "mon_ping_native_encoder.cc": p.SharedWrapper, "osd_ignore_cache_native_encoder.py": p.HelperDriver} {
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

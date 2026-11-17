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
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Offline native request encoding/decoding only. These vectors do not prove
// manifest, chunk, dedup or dirty state, or any server execution. Each Go
// request with the matching native request flags must equal its native
// header, front, middle and data.
var tierOpNativeShapes = []struct {
	name  string
	code  uint16
	flags uint64
	write bool
}{
	{"promote", TierPromote, FlagWrite, true},
	{"flush", TierFlush, FlagRead | FlagIgnoreCache | FlagIgnoreOverlay, false},
	{"evict", TierEvict, FlagRead | FlagIgnoreCache | FlagIgnoreOverlay, false},
}

type tierOpNativeProof struct {
	ignoreCacheNativeProof
	Loader   string                          `json:"loader_audit"`
	Fixtures map[string]cacheOpNativeFixture `json:"fixtures"`
}

func TestTierOperationsRetainFreshNativeRequests(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "tier-op-native-provenance.json"))
	if err != nil {
		t.Fatal("requires actual fresh pinned native tier-operation evidence; no skip or Go-generated substitute", err)
	}
	var proof tierOpNativeProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Fixtures) != 9 || proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || proof.Architecture != "x86_64" ||
		!proof.Canonical || !proof.Decoder || !proof.Header || proof.Execution.ExitCode == nil || *proof.Execution.ExitCode != 0 ||
		proof.Execution.ContainerExists == nil || *proof.Execution.ContainerExists {
		t.Fatal("requires exactly 3 versions x 3 tier-operation shapes from one successful, cleaned-up pinned amd64 build")
	}
	// The proof binds the committed wrapper and driver.
	for name, want := range map[string]string{"osd_tier_op_native_encoder.cc": proof.Wrapper, "osd_tier_op_native_encoder.py": proof.Driver} {
		source, err := os.ReadFile(filepath.Join("..", "..", "tools", name))
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(source); hex.EncodeToString(sum[:]) != want {
			t.Fatalf("native proof does not bind the committed %s", name)
		}
	}
	for _, version := range []uint16{6, 8, 9} {
		for _, shape := range tierOpNativeShapes {
			name := fmt.Sprintf("tier-op-native-v%d-%s", version, shape.name)
			t.Run(name, func(t *testing.T) {
				f, ok := proof.Fixtures[name]
				if !ok || f.Version != version || f.Shape != shape.name {
					t.Fatal("native tier-operation inventory/identity differs")
				}
				header := ignoreCacheNativeBytes(t, f.Header, f.HeaderHash)
				front := ignoreCacheNativeBytes(t, f.Front, f.FrontHash)
				middle := ignoreCacheNativeBytes(t, f.Middle, f.MiddleHash)
				data := ignoreCacheNativeBytes(t, f.Data, f.DataHash)
				message, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, middle, data}})
				if err != nil || message.Type != 42 || uint16(message.Version) != version || len(middle) != 0 || len(data) != 0 {
					t.Fatal("native tier request envelope differs", err)
				}
				features := tierOpFeatures[version]
				if f.Decoded.Features != features || features&FeatureCachePool != 0 {
					t.Fatal("native vector must carry the default features without the cache-pool capability")
				}
				flags := FlagOnDisk | FlagReturnVec | shape.flags
				offset := 8
				if version != 6 {
					offset = 32
				}
				if f.Decoded.Flags != flags || uint64(binary.LittleEndian.Uint32(front[offset:])) != flags {
					t.Fatalf("native request flags %#x, want %#x", f.Decoded.Flags, flags)
				}
				if shape.write != (f.Decoded.MTime != 0 || f.Decoded.MTimeNanos != 0) {
					t.Fatal("native mutation mtime presence differs")
				}
				ops := f.Decoded.Operations
				if len(ops) != 1 || ops[0].Code != shape.code || ops[0].Flags != 0 || ops[0].PayloadLen != 0 || ops[0].Union != fmt.Sprintf("%056x", 0) || ops[0].Input != "" {
					t.Fatal("native tier operation union differs")
				}
				packed := binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint16(nil, 1), shape.code)
				packed = append(packed, make([]byte, 4+28+4)...)
				if bytes.Count(front, packed) != 1 {
					t.Fatal("native wire op count/order/packed union differs")
				}
				r := tierOpUnitRequest(shape.code)
				var got msgr.MessageData
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
				got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
				if !bytes.Equal(got.Frame().Segments[0], header) || !bytes.Equal(got.Front, front) || !bytes.Equal(got.Middle, middle) || !bytes.Equal(got.Data, data) {
					t.Fatal("Go header/front/middle/data differ from canonical fresh native request")
				}
			})
		}
	}
}

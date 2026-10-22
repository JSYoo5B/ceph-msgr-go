package osd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Offline native request encoding/decoding only. These vectors do not prove
// tier state, dirty or pin flags, lock modes or any server execution. Each Go
// request with the matching IgnoreCache/IgnoreOverlay/SkipRWLocks inputs must
// equal its native header, front, middle and data.
var cacheOpNativeShapes = []struct {
	name                            string
	code                            uint16
	flags                           uint64
	write                           bool
	ignoreCache, overlay, skipLocks bool
}{
	{"flush", 0x821f, 0x10 | 0x8000 | 0x20000, false, true, true, false},
	{"try-flush", 0x8221, 0x10 | 0x8000 | 0x20000 | 0x10000, false, true, true, true},
	{"evict", 0x8220, 0x10 | 0x8000 | 0x20000 | 0x10000, false, true, true, true},
	{"pin", 0x2224, 0x20, true, false, false, false},
	{"pin-ignore-cache", 0x2224, 0x20 | 0x8000, true, true, false, false},
	{"unpin", 0x2225, 0x20, true, false, false, false},
	{"unpin-ignore-cache", 0x2225, 0x20 | 0x8000, true, true, false, false},
}

func cacheOpNativeRequest(code uint16, write, ignoreCache, overlay, skipLocks bool) Request {
	r := Request{ClientInc: 1, MapEpoch: 11, Locator: Locator{Pool: 7, Key: "route-key", Namespace: "tenant", Hash: -1}, Object: "cache-op-codec", Hash: 0x11223344,
		Operations: []Operation{{Code: code}}, IgnoreCache: ignoreCache, IgnoreOverlay: overlay, SkipRWLocks: skipLocks}
	if write {
		r.MTimeSeconds, r.MTimeNanoseconds = 1700000000, 123456789
	}
	return r
}

type cacheOpNativeFixture struct {
	ignoreCacheNativeFixture
	Shape   string `json:"shape"`
	Decoded struct {
		Features   uint64 `json:"features"`
		Flags      uint64 `json:"flags"`
		MTime      uint32 `json:"mtime_seconds"`
		MTimeNanos uint32 `json:"mtime_nanoseconds"`
		Operations []struct {
			Code       uint16 `json:"code"`
			Flags      uint32 `json:"flags"`
			PayloadLen uint32 `json:"payload_len"`
			Union      string `json:"union_hex"`
			Input      string `json:"indata_hex"`
		} `json:"operations"`
	} `json:"decoded"`
}

type cacheOpNativeProof struct {
	ignoreCacheNativeProof
	Loader   string                          `json:"loader_audit"`
	Fixtures map[string]cacheOpNativeFixture `json:"fixtures"`
}

func TestCacheOperationsRetain21FreshNativeRequests(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "cache-op-native-provenance.json"))
	if err != nil {
		t.Fatal("requires actual fresh pinned native 21-request evidence; no skip or Go-generated substitute", err)
	}
	var proof cacheOpNativeProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Fixtures) != 21 || proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || proof.Architecture != "x86_64" {
		t.Fatal("requires exactly 3 versions x 7 cache-operation shapes from the pinned amd64 SDK")
	}
	for _, version := range []uint16{6, 8, 9} {
		for _, shape := range cacheOpNativeShapes {
			name := fmt.Sprintf("cache-op-native-v%d-%s", version, shape.name)
			t.Run(name, func(t *testing.T) {
				f, ok := proof.Fixtures[name]
				if !ok || f.Version != version || f.Shape != shape.name {
					t.Fatal("native cache-operation inventory/identity differs")
				}
				header := ignoreCacheNativeBytes(t, f.Header, f.HeaderHash)
				front := ignoreCacheNativeBytes(t, f.Front, f.FrontHash)
				middle := ignoreCacheNativeBytes(t, f.Middle, f.MiddleHash)
				data := ignoreCacheNativeBytes(t, f.Data, f.DataHash)
				message, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, middle, data}})
				if err != nil || message.Type != 42 || uint16(message.Version) != version || len(middle) != 0 || len(data) != 0 {
					t.Fatal("native cache request envelope differs", err)
				}
				flags := FlagOnDisk | FlagReturnVec | shape.flags
				offset := 8
				if version != 6 {
					offset = 32
				}
				if f.Decoded.Features != ignoreCacheNativeFeatures[version] {
					t.Fatal("native vector must carry the negotiated cache-pool capability")
				}
				if f.Decoded.Flags != flags || uint64(binary.LittleEndian.Uint32(front[offset:])) != flags {
					t.Fatalf("native request flags %#x, want %#x", f.Decoded.Flags, flags)
				}
				if shape.write != (f.Decoded.MTime != 0 || f.Decoded.MTimeNanos != 0) {
					t.Fatal("native mutation mtime presence differs")
				}
				ops := f.Decoded.Operations
				if len(ops) != 1 || ops[0].Code != shape.code || ops[0].Flags != 0 || ops[0].PayloadLen != 0 || ops[0].Union != fmt.Sprintf("%056x", 0) || ops[0].Input != "" {
					t.Fatal("native cache operation union differs")
				}
				packed := binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint16(nil, 1), shape.code)
				packed = append(packed, make([]byte, 4+28+4)...)
				if bytes.Count(front, packed) != 1 {
					t.Fatal("native wire op count/order/packed union differs")
				}
				r := cacheOpNativeRequest(shape.code, shape.write, shape.ignoreCache, shape.overlay, shape.skipLocks)
				features := ignoreCacheNativeFeatures[version]
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
				// Both sides encode the negotiated cache-pool bit 35 in the front.
				got.Sequence, got.Transaction, got.AckSequence = 1, 77, 3
				if !bytes.Equal(got.Frame().Segments[0], header) || !bytes.Equal(got.Front, front) || !bytes.Equal(got.Middle, middle) || !bytes.Equal(got.Data, data) {
					t.Fatal("Go header/front/middle/data differ from canonical fresh native request")
				}
			})
		}
	}
}

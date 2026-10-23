package osd

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Offline native request encoding/decoding only. These vectors do not prove
// manifest state, chunk references, target existence or any server result.
// Each admitted Go request must equal its native header, front, middle and
// data; the Go encoder refuses the two retained native shapes it does not
// admit.
var setChunkNativeShapes = []struct {
	name     string
	input    SetChunkInput
	flags    uint32
	admitted bool
}{
	{"chunk", SetChunkInput{SourceLength: 8, Target: Locator{Pool: 9, Hash: -1}, TargetObject: "set-chunk-target"}, OpWithReference, true},
	{"chunk-located", SetChunkInput{SourceLength: 4096, Target: Locator{Pool: 7, Key: "chunk-key", Namespace: "tenant", Hash: -1}, TargetObject: "set-chunk-target-2", TargetOffset: 65536}, OpWithReference, true},
	{"nonzero-source", SetChunkInput{SourceOffset: 16, SourceLength: 8, Target: Locator{Pool: 9, Hash: -1}, TargetObject: "set-chunk-target", TargetOffset: 8}, OpWithReference, false},
	{"without-reference", SetChunkInput{SourceLength: 8, Target: Locator{Pool: 9, Hash: -1}, TargetObject: "set-chunk-target"}, 0, false},
}

type setChunkNativeFixture struct {
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
		SetChunk struct {
			SourceOffset uint64 `json:"src_offset"`
			SourceLength uint64 `json:"src_length"`
			Pool         int64  `json:"tgt_pool"`
			Key          string `json:"tgt_key"`
			Namespace    string `json:"tgt_namespace"`
			Hash         int64  `json:"tgt_hash"`
			Object       string `json:"tgt_oid"`
			Offset       uint64 `json:"tgt_offset"`
		} `json:"set_chunk"`
	} `json:"decoded"`
}

type setChunkNativeProof struct {
	ignoreCacheNativeProof
	Loader   string                           `json:"loader_audit"`
	Fixtures map[string]setChunkNativeFixture `json:"fixtures"`
}

func setChunkNativeRequest(shape int) Request {
	r := setChunkUnitRequest()
	r.Operations[0].Flags = setChunkNativeShapes[shape].flags
	r.Operations[0].SetChunk = setChunkNativeShapes[shape].input
	return r
}

func TestSetChunkRetainsFreshNativeRequests(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "set-chunk-native-provenance.json"))
	if err != nil {
		t.Fatal("requires actual fresh pinned native 12-request evidence; no skip or Go-generated substitute", err)
	}
	var proof setChunkNativeProof
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Fixtures) != 12 || proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || proof.Architecture != "x86_64" {
		t.Fatal("requires exactly 3 versions x 4 SET_CHUNK shapes from the pinned amd64 SDK")
	}
	for _, version := range []uint16{6, 8, 9} {
		for i, shape := range setChunkNativeShapes {
			name := fmt.Sprintf("set-chunk-native-v%d-%s", version, shape.name)
			t.Run(name, func(t *testing.T) {
				f, ok := proof.Fixtures[name]
				if !ok || f.Version != version || f.Shape != shape.name {
					t.Fatal("native SET_CHUNK inventory/identity differs")
				}
				header := ignoreCacheNativeBytes(t, f.Header, f.HeaderHash)
				front := ignoreCacheNativeBytes(t, f.Front, f.FrontHash)
				middle := ignoreCacheNativeBytes(t, f.Middle, f.MiddleHash)
				data := ignoreCacheNativeBytes(t, f.Data, f.DataHash)
				message, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, front, middle, data}})
				if err != nil || message.Type != 42 || uint16(message.Version) != version || len(middle) != 0 {
					t.Fatal("native SET_CHUNK envelope differs", err)
				}
				features := setChunkFeatures[version]
				flags := FlagOnDisk | FlagReturnVec | FlagRead
				offset := 8
				if version != 6 {
					offset = 32
				}
				if f.Decoded.Features != features || features&FeatureCachePool != 0 {
					t.Fatal("native vector features differ from the default plain-pool set")
				}
				if f.Decoded.Flags != flags || uint64(binary.LittleEndian.Uint32(front[offset:])) != flags || f.Decoded.MTime != 0 || f.Decoded.MTimeNanos != 0 {
					t.Fatalf("native request flags %#x or mtime differ, want %#x and none", f.Decoded.Flags, flags)
				}
				// The independent model of the Objecter indata, compared with
				// both the native data segment and the native decoded fields.
				input := appendSetChunkData(nil, shape.input)
				ops := f.Decoded.Operations
				if len(ops) != 1 || ops[0].Code != SetChunk || ops[0].Flags != shape.flags || ops[0].PayloadLen != uint32(len(input)) ||
					ops[0].Union != fmt.Sprintf("%056x", 0) || ops[0].Input != hex.EncodeToString(input) || !bytes.Equal(data, input) {
					t.Fatal("native SET_CHUNK operation or indata differs from the model")
				}
				d := f.Decoded.SetChunk
				if d.SourceOffset != shape.input.SourceOffset || d.SourceLength != shape.input.SourceLength || d.Pool != shape.input.Target.Pool ||
					d.Key != shape.input.Target.Key || d.Namespace != shape.input.Target.Namespace || d.Hash != -1 || d.Object != shape.input.TargetObject || d.Offset != shape.input.TargetOffset {
					t.Fatalf("native decoded SET_CHUNK fields differ: %+v", d)
				}
				packed := binary.LittleEndian.AppendUint16(binary.LittleEndian.AppendUint16(nil, 1), SetChunk)
				packed = binary.LittleEndian.AppendUint32(packed, shape.flags)
				packed = append(packed, make([]byte, 28)...)
				packed = binary.LittleEndian.AppendUint32(packed, uint32(len(input)))
				if bytes.Count(front, packed) != 1 {
					t.Fatal("native wire op count/order/packed union differs")
				}
				r := setChunkNativeRequest(i)
				var got msgr.MessageData
				switch version {
				case 6:
					got, err = EncodeRequest(r, features, wire.DefaultLimit)
				case 8:
					got, err = EncodeRequestV8(V8Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, features, wire.DefaultLimit)
				case 9:
					got, err = EncodeRequestV9(V9Request{Request: r, PG: PG{Pool: 7, Seed: 0x44}, Shard: -1}, features, wire.DefaultLimit)
				}
				if !shape.admitted {
					if !errors.Is(err, ErrOperation) {
						t.Fatal("Go encoded a refused native SET_CHUNK shape", err)
					}
					return
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

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

const (
	setChunkSource       = "set-chunk-source"
	setChunkAbsentSource = "set-chunk-absent-source"
	setChunkTarget       = "set-chunk-target"
	setChunkAbsentTarget = "set-chunk-absent-target"
	setChunkWithRef      = 0x80
)

var (
	setChunkSourceData = []byte("S\x00\xff\x80set-chunk-source-bytes-01234")
	setChunkTargetData = []byte("T\x00\xffset-chunk-target-bytes")
)

// setChunkExpected restates the pinned PrimaryLogPG SET_CHUNK branches for
// the closed sequence independently of the oracle's own gates.
var setChunkExpected = []struct {
	label, object, target string
	offset, length        uint64
	targetOffset          uint64
	flags, code           int
}{
	{"absent-source", setChunkAbsentSource, setChunkTarget, 0, 8, 0, setChunkWithRef, -2},
	{"zero-length", setChunkSource, setChunkTarget, 0, 0, 0, setChunkWithRef, -22},
	{"out-of-range", setChunkSource, setChunkTarget, 28, 8, 0, setChunkWithRef, -34},
	{"without-reference", setChunkSource, setChunkTarget, 0, 8, 0, 0, -95},
	{"absent-target", setChunkSource, setChunkAbsentTarget, 0, 8, 0, setChunkWithRef, -2},
	{"chunk-a", setChunkSource, setChunkTarget, 8, 8, 0, setChunkWithRef, 0},
	{"overlap-same", setChunkSource, setChunkTarget, 8, 8, 0, setChunkWithRef, -95},
	{"overlap-partial", setChunkSource, setChunkTarget, 12, 8, 0, setChunkWithRef, -95},
	{"chunk-b", setChunkSource, setChunkTarget, 16, 8, 8, setChunkWithRef, 0},
}

type setChunkRefs struct {
	Code       int    `json:"code"`
	ObjectCode *int   `json:"object_code"`
	Raw        []byte `json:"raw_b64"`
}

type setChunkObservation struct {
	Source       nativeXattrSnapshot `json:"set-chunk-source"`
	AbsentSource nativeXattrSnapshot `json:"set-chunk-absent-source"`
	Target       nativeXattrSnapshot `json:"set-chunk-target"`
	AbsentTarget nativeXattrSnapshot `json:"set-chunk-absent-target"`
	Refs         setChunkRefs        `json:"chunk_refs"`
	References   struct {
		Code int `json:"code"`
	} `json:"references_chunk"`
}

type setChunkReceipt struct {
	Schema       int    `json:"schema_version"`
	Source       string `json:"source_commit"`
	Checkpoint   string `json:"checkpoint"`
	NativeOnly   bool   `json:"native_only"`
	ProductClaim bool   `json:"product_support_claimed"`
	Complete     bool   `json:"complete"`
	Sequence     bool   `json:"sequence_complete"`
	Stopped      string `json:"stopped"`
	Error        string `json:"error"`
	Input        struct {
		Source int64 `json:"source_pool_id"`
		Chunk  int64 `json:"chunk_pool_id"`
	} `json:"input"`
	Identity struct {
		FSID  string `json:"fsid"`
		Epoch uint32 `json:"epoch"`
	} `json:"identity"`
	NativeIdentity struct {
		Name     string `json:"name"`
		GlobalID uint64 `json:"global_id"`
	} `json:"native_identity"`
	NativeAPI struct {
		Symbols       map[string]string `json:"cxx_symbols"`
		RequestFlags  int               `json:"request_flags"`
		WithReference int               `json:"with_reference_op_flag"`
	} `json:"native_api"`
	Steps []struct {
		Label        string `json:"label"`
		Kind         string `json:"kind"`
		State        string `json:"state"`
		Object       string `json:"object"`
		Target       string `json:"target_object"`
		Offset       uint64 `json:"src_offset"`
		Length       uint64 `json:"src_length"`
		TargetOffset uint64 `json:"tgt_offset"`
		Flags        int    `json:"op_flags"`
		RequestFlags int    `json:"request_flags"`
		Result       struct {
			Code    int    `json:"code"`
			Version uint64 `json:"version"`
		} `json:"result"`
	} `json:"steps"`
	Observations map[string]setChunkObservation `json:"observations"`
	Release      struct {
		Polls []struct {
			Code int `json:"code"`
		} `json:"polls"`
		Removed    bool `json:"target_removed_by_reference_release"`
		SourceCode int  `json:"source_code_after_delete"`
	} `json:"release"`
	Cleanup struct {
		Attempted bool `json:"attempted"`
		Complete  bool `json:"complete"`
	} `json:"cleanup"`
}

// setChunkRefsEncoding is the exact chunk_refs_t (by_object) the pinned
// cls_cas stores for count references from the head of one source object.
func setChunkRefsEncoding(count int, name string, hash uint32, pool int64) []byte {
	envelope := func(version, compat byte, body []byte) []byte {
		out := []byte{version, compat}
		out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
		return append(out, body...)
	}
	text := func(s string) []byte {
		return append(binary.LittleEndian.AppendUint32(nil, uint32(len(s))), s...)
	}
	var object []byte
	object = append(object, text("")...)
	object = append(object, text(name)...)
	object = binary.LittleEndian.AppendUint64(object, 1<<64-2) // CEPH_NOSNAP: the head
	object = binary.LittleEndian.AppendUint32(object, hash)
	object = append(object, 0)
	object = append(object, text("")...)
	object = binary.LittleEndian.AppendUint64(object, uint64(pool))
	set := binary.LittleEndian.AppendUint32(nil, uint32(count))
	for range count {
		set = append(set, envelope(4, 3, object)...)
	}
	return envelope(1, 1, append([]byte{1}, envelope(1, 1, set)...))
}

// TestCephOSDSetChunkNativeIntegration is the native-only SET_CHUNK manifest
// checkpoint: a closed librados C++ oracle seeds a source in one owned plain
// replicated pool and a chunk target in another, issues the source-derived
// refusals and two disjoint chunks once each, and observes source/target bytes,
// the target's cls_cas chunk_refs attribute and the source's own reference
// count before and after; deleting the source releases both references and
// the target. No product client sends SET_CHUNK.
func TestCephOSDSetChunkNativeIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	client, _, base, _ := fixture(t)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stop()
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1, 2)
	// Independent native placement supplies the source's raw object hash,
	// which the target's reference must name.
	route := poolPlace(t, ctx, 1, setChunkSource, "")
	if route.Pool != pools.ids[1] {
		t.Fatal("native placement pool differs", route.Pool, pools.ids[1])
	}
	input, _ := json.Marshal(map[string]int64{"source_pool_id": pools.ids[1], "chunk_pool_id": pools.ids[2]})
	if err := testcluster.WritePublicEvidence(filepath.Join(directory, "osd-set-chunk-input.json"), input); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, directory, "verify", "osd-set-chunk", "checkpoint"); err != nil {
		t.Fatal("native SET_CHUNK checkpoint", err)
	}
	raw, err := os.ReadFile(filepath.Join(directory, "osd-set-chunk-result.json"))
	var r setChunkReceipt
	if err != nil || len(raw) > 512<<10 || json.Unmarshal(raw, &r) != nil {
		t.Fatal("bounded native SET_CHUNK receipt", err)
	}
	summary := map[string]any{"source_commit": mdsFreshSource, "receipt_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "receipt_bytes": len(raw),
		"complete": r.Complete, "stopped": r.Stopped, "source_hash": route.Hash, "pools": pools.ids}
	defer func() {
		encoded, err := json.MarshalIndent(summary, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-set-chunk-results.json"), append(encoded, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	}()
	if r.Schema != 1 || r.Source != mdsFreshSource || r.Checkpoint != "set-chunk-native-v1" || !r.NativeOnly || r.ProductClaim ||
		r.Input.Source != pools.ids[1] || r.Input.Chunk != pools.ids[2] || r.Identity.FSID != client.Snapshot().FSID || r.Identity.Epoch == 0 ||
		r.NativeIdentity.Name != "client.test" || r.NativeIdentity.GlobalID == 0 || r.NativeAPI.RequestFlags != 0 || r.NativeAPI.WithReference != setChunkWithRef ||
		r.NativeAPI.Symbols["set_chunk"] == "" {
		t.Fatalf("native SET_CHUNK receipt provenance differs: %+v", r)
	}
	if !r.Complete || !r.Sequence || r.Stopped != "" || r.Error != "" || !r.Cleanup.Attempted || !r.Cleanup.Complete {
		t.Fatalf("native SET_CHUNK checkpoint incomplete: stopped=%q error=%q cleanup=%+v", r.Stopped, r.Error, r.Cleanup)
	}
	var issued int
	codes := map[string]int{}
	for _, step := range r.Steps {
		if step.State != "completed" {
			t.Fatal("native step not completed", step.Label, step.State)
		}
		if step.Kind != "set-chunk" {
			continue
		}
		want := setChunkExpected[issued]
		if step.Label != want.label || step.Object != want.object || step.Target != want.target || step.Offset != want.offset ||
			step.Length != want.length || step.TargetOffset != want.targetOffset || step.Flags != want.flags || step.RequestFlags != 0 ||
			step.Result.Code != want.code {
			t.Fatalf("native SET_CHUNK step %d differs: %+v want %+v", issued, step, want)
		}
		codes[step.Label] = step.Result.Code
		issued++
	}
	if issued != len(setChunkExpected) {
		t.Fatal("native SET_CHUNK step inventory differs", issued)
	}
	summary["set_chunk_codes"] = codes
	observe := func(label string) setChunkObservation {
		o, ok := r.Observations[label]
		if !ok {
			t.Fatal("missing native observation", label)
		}
		if o.AbsentSource.Code != -2 || o.AbsentTarget.Code != -2 {
			t.Fatal("absent control object exists", label)
		}
		return o
	}
	seeded, refused, first, overlapped, second := observe("seeded"), observe("after-refusals"), observe("after-chunk-a"), observe("after-overlaps"), observe("after-chunk-b")
	if seeded.Source.Code != 0 || !bytes.Equal(seeded.Source.Data, setChunkSourceData) || seeded.Target.Code != 0 || !bytes.Equal(seeded.Target.Data, setChunkTargetData) ||
		seeded.Refs.Code != -61 || seeded.References.Code != -67 {
		t.Fatalf("seeded native state differs: %+v", seeded)
	}
	unchanged := func(a, b nativeXattrSnapshot) bool {
		return a.Code == b.Code && bytes.Equal(a.Data, b.Data) && a.Version == b.Version && a.Seconds == b.Seconds && a.Nanos == b.Nanos
	}
	if !unchanged(refused.Source, seeded.Source) || !unchanged(refused.Target, seeded.Target) || refused.Refs.Code != -61 || refused.References.Code != -67 {
		t.Fatal("refused SET_CHUNK changed native state")
	}
	one := setChunkRefsEncoding(1, setChunkSource, route.Hash, pools.ids[1])
	two := setChunkRefsEncoding(2, setChunkSource, route.Hash, pools.ids[1])
	// SET_CHUNK leaves the source's bytes and user version alone (it is not a
	// user-visible modification); the reference increment is a cls write on
	// the target, which advances the target's user version only.
	if !setChunkSourceKept(seeded.Source, first.Source) ||
		!bytes.Equal(first.Target.Data, setChunkTargetData) || first.Target.Version <= seeded.Target.Version ||
		first.Refs.Code != len(one) || !bytes.Equal(first.Refs.Raw, one) || first.References.Code != 1 {
		t.Fatalf("first chunk native state differs: source v%d->v%d target v%d->v%d refs=%d %x references=%d",
			seeded.Source.Version, first.Source.Version, seeded.Target.Version, first.Target.Version, first.Refs.Code, first.Refs.Raw, first.References.Code)
	}
	if !unchanged(overlapped.Source, first.Source) || !unchanged(overlapped.Target, first.Target) || !bytes.Equal(overlapped.Refs.Raw, one) || overlapped.References.Code != 1 {
		t.Fatal("overlapping SET_CHUNK changed native state")
	}
	if !setChunkSourceKept(seeded.Source, second.Source) ||
		!bytes.Equal(second.Target.Data, setChunkTargetData) || second.Target.Version <= first.Target.Version ||
		!bytes.Equal(second.Refs.Raw, two) || second.References.Code != 2 {
		t.Fatalf("second chunk native state differs: target v%d refs %x references=%d", second.Target.Version, second.Refs.Raw, second.References.Code)
	}
	if !r.Release.Removed || len(r.Release.Polls) == 0 || r.Release.Polls[len(r.Release.Polls)-1].Code != -2 || r.Release.SourceCode != -2 {
		t.Fatalf("releasing the source did not remove the target: %+v", r.Release)
	}
	summary["versions"] = map[string]any{
		"source":       []uint64{seeded.Source.Version, first.Source.Version, second.Source.Version},
		"source_mtime": [][2]int64{{seeded.Source.Seconds, seeded.Source.Nanos}, {first.Source.Seconds, first.Source.Nanos}, {second.Source.Seconds, second.Source.Nanos}},
		"target":       []uint64{seeded.Target.Version, first.Target.Version, second.Target.Version},
	}
	summary["release_polls"] = len(r.Release.Polls)
	t.Logf("native SET_CHUNK: codes %v; source v%d mtime %d.%09d -> %d.%09d; target v%d -> v%d -> v%d; refs 1 then 2 by_object from pool %d hash %#x; target removed after %d release polls",
		codes, seeded.Source.Version, seeded.Source.Seconds, seeded.Source.Nanos, second.Source.Seconds, second.Source.Nanos,
		seeded.Target.Version, first.Target.Version, second.Target.Version, pools.ids[1], route.Hash, len(r.Release.Polls))
}

// setChunkSourceKept reports whether a successful SET_CHUNK left the seeded
// source unchanged: bytes, user version and mtime. SET_CHUNK is not a
// user-visible modification of the source.
func setChunkSourceKept(seeded, got nativeXattrSnapshot) bool {
	return got.Code == 0 && bytes.Equal(got.Data, setChunkSourceData) && bytes.Equal(got.Data, seeded.Data) &&
		got.Version == seeded.Version && got.Seconds == seeded.Seconds && got.Nanos == seeded.Nanos
}

func TestSetChunkSourceKeptRequiresMTime(t *testing.T) {
	seeded := nativeXattrSnapshot{Data: setChunkSourceData, Version: 3, Seconds: 1700000000, Nanos: 5}
	if !setChunkSourceKept(seeded, seeded) {
		t.Fatal("unchanged source refused")
	}
	for name, mutate := range map[string]func(*nativeXattrSnapshot){
		"seconds": func(s *nativeXattrSnapshot) { s.Seconds++ },
		"nanos":   func(s *nativeXattrSnapshot) { s.Nanos++ },
		"version": func(s *nativeXattrSnapshot) { s.Version++ },
		"bytes":   func(s *nativeXattrSnapshot) { s.Data = []byte("other") },
		"code":    func(s *nativeXattrSnapshot) { s.Code = -2 },
	} {
		got := seeded
		mutate(&got)
		if setChunkSourceKept(seeded, got) {
			t.Error("changed source accepted:", name)
		}
	}
}

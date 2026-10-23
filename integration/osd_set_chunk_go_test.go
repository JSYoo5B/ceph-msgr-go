package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Go-originated SET_CHUNK on two owned plain replicated pools. The product
// client sends every SET_CHUNK and the source deletions; the native owned-pool
// oracle seeds, places and observes the objects independently (STAT2, READ and
// the GETXATTRS inventory, which includes the target's cls_cas chunk_refs
// attribute). These consumer helpers are test-only and nothing is exported.

const (
	setChunkGoSourceA      = "go-set-chunk-source-a"
	setChunkGoSourceB      = "go-set-chunk-source-b"
	setChunkGoAbsentSource = "go-set-chunk-absent-source"
	setChunkGoTarget       = "go-set-chunk-target"
	setChunkGoAbsentTarget = "go-set-chunk-absent-target"
	setChunkGoReleasePolls = 20
)

var (
	setChunkGoSourceData = []byte("A\x00\xff\x80go-set-chunk-source-a-0123")
	setChunkGoOtherData  = []byte("B\x00\xff\x80go-set-chunk-source-b-4567")
	setChunkGoTargetData = []byte("T\x00\xffgo-set-chunk-target-b")
)

type setChunkGoRef struct {
	name string
	hash uint32
}

// setChunkGoRefs is the exact chunk_refs_t (by_object) the pinned cls_cas
// stores for references from the heads of the given sources in one pool and
// the default namespace. The multiset is ordered as hobject_t compares within
// one pool and namespace: by bit-reversed hash, then by name.
func setChunkGoRefs(pool int64, refs ...setChunkGoRef) []byte {
	sorted := slices.Clone(refs)
	slices.SortFunc(sorted, func(a, b setChunkGoRef) int {
		if x, y := bits.Reverse32(a.hash), bits.Reverse32(b.hash); x != y {
			if x < y {
				return -1
			}
			return 1
		}
		return bytes.Compare([]byte(a.name), []byte(b.name))
	})
	envelope := func(version, compat byte, body []byte) []byte {
		out := binary.LittleEndian.AppendUint32([]byte{version, compat}, uint32(len(body)))
		return append(out, body...)
	}
	text := func(s string) []byte { return append(binary.LittleEndian.AppendUint32(nil, uint32(len(s))), s...) }
	set := binary.LittleEndian.AppendUint32(nil, uint32(len(sorted)))
	for _, ref := range sorted {
		object := append(text(""), text(ref.name)...)
		object = binary.LittleEndian.AppendUint64(object, 1<<64-2) // CEPH_NOSNAP: the head
		object = binary.LittleEndian.AppendUint32(object, ref.hash)
		object = append(object, 0)
		object = append(object, text("")...)
		object = binary.LittleEndian.AppendUint64(object, uint64(pool))
		set = append(set, envelope(4, 3, object)...)
	}
	return envelope(1, 1, append([]byte{1}, envelope(1, 1, set)...))
}

// setChunkGoRefsAttr returns the target's chunk_refs value from the native
// GETXATTRS inventory and whether it is present.
func setChunkGoRefsAttr(s nativeXattrSnapshot) ([]byte, bool) {
	for _, a := range s.Attributes {
		if string(a.Name) == "chunk_refs" {
			return a.Value, true
		}
	}
	return nil, false
}

// setChunkGoSame reports an unchanged native object: code, bytes, user
// version, mtime and xattr inventory.
func setChunkGoSame(a, b nativeXattrSnapshot) bool {
	return a.Code == b.Code && bytes.Equal(a.Data, b.Data) && a.Version == b.Version && a.Seconds == b.Seconds && a.Nanos == b.Nanos &&
		fmt.Sprint(xattrInventory(a)) == fmt.Sprint(xattrInventory(b))
}

// setChunkGoTargetHolds reports the seeded target bytes and exactly the given
// chunk_refs value (nil: no attribute), with a user version after floor.
func setChunkGoTargetHolds(s nativeXattrSnapshot, refs []byte, floor uint64) bool {
	got, ok := setChunkGoRefsAttr(s)
	return s.Code == 0 && bytes.Equal(s.Data, setChunkGoTargetData) && s.Version > floor && ok == (refs != nil) && bytes.Equal(got, refs)
}

type setChunkGoRecord struct {
	Label   string              `json:"label"`
	Request cephmsgr.OSDRequest `json:"request"`
	Reply   cephmsgr.OSDReply   `json:"reply"`
	Error   string              `json:"error,omitempty"`
	Unknown bool                `json:"unknown_outcome"`
}

func TestCephOSDSetChunkGoIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	client, _, base, _ := fixture(t)
	ctx, stop := context.WithTimeout(context.Background(), 6*time.Minute)
	defer stop()
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1, 2)
	records := []setChunkGoRecord{}
	receipt := map[string]any{"source_commit": mdsFreshSource, "pools": pools.ids, "identity": client.Snapshot()}
	defer func() {
		receipt["requests"] = records
		encoded, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-set-chunk-go-results.json"), append(encoded, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	}()

	seededA := poolSeed(t, ctx, 1, setChunkGoSourceA, "", setChunkGoSourceData, nil)
	seededB := poolSeed(t, ctx, 1, setChunkGoSourceB, "", setChunkGoOtherData, nil)
	seededTarget := poolSeed(t, ctx, 2, setChunkGoTarget, "", setChunkGoTargetData, nil)
	routes := map[string]osdFixtureOracle{}
	for _, name := range []string{setChunkGoSourceA, setChunkGoSourceB, setChunkGoAbsentSource} {
		routes[name] = poolPlace(t, ctx, 1, name, "")
		if routes[name].Pool != pools.ids[1] {
			t.Fatal("native placement pool differs", name, routes[name].Pool)
		}
	}
	receipt["routes"] = routes
	if _, ok := setChunkGoRefsAttr(seededTarget); ok || poolObserve(t, ctx, 1, setChunkGoAbsentSource, "", "").Code != -2 || poolObserve(t, ctx, 2, setChunkGoAbsentTarget, "", "").Code != -2 {
		t.Fatal("seeded target already referenced or absent controls exist")
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, c := range connections {
			c.Close()
		}
	}()
	seed := func(route osdFixtureOracle) uint32 {
		if isReplicatedPGFixtureProfile() {
			return route.PG
		}
		return route.Hash
	}
	chunk := func(length uint64, target string, targetOffset uint64) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetChunk, Flags: cephmsgr.OSDWithReference,
			Chunk: cephmsgr.OSDChunkInput{SourceLength: length, TargetPool: pools.ids[2], TargetName: target, TargetOffset: targetOffset}}
	}
	// send issues one Go request exactly once and checks its signed result,
	// identity and placement echo. The per-op result must be zero or the
	// aggregate unless perOp names the expected value. An unknown outcome
	// stops the test; it is never resent.
	send := func(t *testing.T, label, source string, op cephmsgr.OSDOperation, want int32, perOp ...int32) cephmsgr.OSDReply {
		t.Helper()
		route := routes[source]
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, client, ctx, route)
		}
		request := pools.request(route, op)
		if op.Code == cephmsgr.OSDDelete {
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001300}
		}
		reply, err := connections[route.ID].Request(ctx, request)
		entry := setChunkGoRecord{Label: label, Request: request, Reply: reply}
		var unknown *cephmsgr.OutcomeUnknownError
		entry.Unknown = errors.As(err, &unknown)
		if err != nil {
			entry.Error = err.Error()
		}
		records = append(records, entry)
		if entry.Unknown {
			t.Fatal("Go SET_CHUNK outcome unknown; not resent", label, err)
		}
		if reply.Submission.Transaction == 0 || reply.Submission.GlobalID != client.Snapshot().GlobalID || reply.Object != source || reply.PG.Pool != uint64(route.Pool) ||
			reply.PG.Seed != seed(route) || reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || len(reply.RawFront) == 0 || len(reply.Operations) != 1 {
			t.Fatal("Go reply lost identity, placement or its operation", label, reply.Code, err)
		}
		if reply.Code < 0 {
			var remote *cephmsgr.OSDError
			if !errors.As(err, &remote) || remote.Code != reply.Code {
				t.Fatal("signed server refusal lost its raw error", label, reply.Code, err)
			}
		} else if err != nil {
			t.Fatal("successful reply returned error", label, err)
		}
		allowed := []int32{0, reply.Code}
		if len(perOp) != 0 {
			allowed = perOp
		}
		if result := reply.Operations[0].Result; !slices.Contains(allowed, result) || reply.Operations[0].PayloadLength != 0 {
			t.Fatal("per-op result or output differs", label, result, reply.Code, allowed)
		}
		if reply.Code != want {
			t.Fatalf("%s returned %d, want %d", label, reply.Code, want)
		}
		return reply
	}

	var chunked, overlapped, both nativeXattrSnapshot
	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"hazard_shapes_are_refused_locally", func(t *testing.T) {
			// A non-zero source offset could leave the request unanswered and the
			// source blocked if the target reference failed; a self target would
			// wait on the blocked source. Neither reaches the OSD.
			route := routes[setChunkGoSourceA]
			if connections[route.ID] == nil {
				connections[route.ID] = mutationConnection(t, client, ctx, route)
			}
			offset := chunk(8, setChunkGoTarget, 0)
			offset.Chunk.SourceOffset = 8
			self := chunk(8, setChunkGoSourceA, 0)
			self.Chunk.TargetPool = pools.ids[1]
			unreferenced := chunk(8, setChunkGoTarget, 0)
			unreferenced.Flags = 0
			for label, op := range map[string]cephmsgr.OSDOperation{"nonzero-source-offset": offset, "self-target": self, "without-reference": unreferenced} {
				reply, err := connections[route.ID].Request(ctx, pools.request(route, op))
				if err == nil || reply.Submission.Transaction != 0 {
					t.Fatal("hazardous SET_CHUNK was transmitted", label, err)
				}
			}
		}},
		{"server_refusals_leave_state", func(t *testing.T) {
			send(t, "absent-source", setChunkGoAbsentSource, chunk(8, setChunkGoTarget, 0), -2)
			send(t, "out-of-range", setChunkGoSourceA, chunk(uint64(len(setChunkGoSourceData))+8, setChunkGoTarget, 0), -34)
			// Offset zero keeps the absent-target failure on the documented path.
			// The failed reference increment is answered by RefCountCallback's
			// reply_op_error: the aggregate is the increment's ENOENT while the
			// per-op result keeps the handler's -EINPROGRESS (observed live).
			send(t, "absent-target", setChunkGoSourceA, chunk(8, setChunkGoAbsentTarget, 0), -2, -115)
			source, target := poolObserve(t, ctx, 1, setChunkGoSourceA, "", ""), poolObserve(t, ctx, 2, setChunkGoTarget, "", "")
			if !setChunkGoSame(source, seededA) || !setChunkGoSame(target, seededTarget) ||
				poolObserve(t, ctx, 1, setChunkGoAbsentSource, "", "").Code != -2 || poolObserve(t, ctx, 2, setChunkGoAbsentTarget, "", "").Code != -2 {
				t.Fatal("refused Go SET_CHUNK changed native state", source, target)
			}
		}},
		{"chunk_references_the_target", func(t *testing.T) {
			send(t, "chunk-a", setChunkGoSourceA, chunk(8, setChunkGoTarget, 0), 0)
			source, target := poolObserve(t, ctx, 1, setChunkGoSourceA, "", ""), poolObserve(t, ctx, 2, setChunkGoTarget, "", "")
			one := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoSourceA, routes[setChunkGoSourceA].Hash})
			// The source is not user-visibly modified: bytes, version, mtime.
			if !setChunkGoSame(source, seededA) || !setChunkGoTargetHolds(target, one, seededTarget.Version) {
				got, _ := setChunkGoRefsAttr(target)
				t.Fatalf("Go chunk native state differs: source v%d->v%d mtime %d.%09d->%d.%09d target v%d->v%d refs %x want %x",
					seededA.Version, source.Version, seededA.Seconds, seededA.Nanos, source.Seconds, source.Nanos, seededTarget.Version, target.Version, got, one)
			}
			chunked = target
		}},
		{"overlapping_chunks_are_refused", func(t *testing.T) {
			send(t, "overlap-same", setChunkGoSourceA, chunk(8, setChunkGoTarget, 0), -95)
			send(t, "overlap-partial", setChunkGoSourceA, chunk(4, setChunkGoTarget, 16), -95)
			source, target := poolObserve(t, ctx, 1, setChunkGoSourceA, "", ""), poolObserve(t, ctx, 2, setChunkGoTarget, "", "")
			if !setChunkGoSame(source, seededA) || !setChunkGoSame(target, chunked) {
				t.Fatal("overlapping Go SET_CHUNK changed native state", source, target)
			}
			overlapped = target
		}},
		{"second_source_adds_a_reference", func(t *testing.T) {
			send(t, "chunk-b", setChunkGoSourceB, chunk(16, setChunkGoTarget, 8), 0)
			source, target := poolObserve(t, ctx, 1, setChunkGoSourceB, "", ""), poolObserve(t, ctx, 2, setChunkGoTarget, "", "")
			two := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoSourceA, routes[setChunkGoSourceA].Hash}, setChunkGoRef{setChunkGoSourceB, routes[setChunkGoSourceB].Hash})
			if !setChunkGoSame(source, seededB) || !setChunkGoTargetHolds(target, two, overlapped.Version) || !setChunkGoSame(poolObserve(t, ctx, 1, setChunkGoSourceA, "", ""), seededA) {
				got, _ := setChunkGoRefsAttr(target)
				t.Fatalf("second Go chunk native state differs: target v%d refs %x want %x", target.Version, got, two)
			}
			both = target
		}},
		{"deleting_sources_releases_references", func(t *testing.T) {
			// Deletion drops the source's references after commit; cls_cas
			// removes the target with its last reference. Polls are native reads.
			poll := func(label string, done func(nativeXattrSnapshot) bool) (nativeXattrSnapshot, int) {
				var s nativeXattrSnapshot
				for i := 1; i <= setChunkGoReleasePolls; i++ {
					if s = poolObserve(t, ctx, 2, setChunkGoTarget, "", ""); done(s) {
						return s, i
					}
					time.Sleep(500 * time.Millisecond)
				}
				t.Fatalf("%s: target state after %d polls: code %d version %d", label, setChunkGoReleasePolls, s.Code, s.Version)
				return s, 0
			}
			send(t, "delete-source-a", setChunkGoSourceA, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
			one := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoSourceB, routes[setChunkGoSourceB].Hash})
			_, first := poll("release-a", func(s nativeXattrSnapshot) bool { return setChunkGoTargetHolds(s, one, both.Version) })
			if poolObserve(t, ctx, 1, setChunkGoSourceA, "", "").Code != -2 {
				t.Fatal("deleted Go source remains")
			}
			send(t, "delete-source-b", setChunkGoSourceB, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
			_, second := poll("release-b", func(s nativeXattrSnapshot) bool { return s.Code == -2 })
			if poolObserve(t, ctx, 1, setChunkGoSourceB, "", "").Code != -2 {
				t.Fatal("deleted Go source remains")
			}
			receipt["release_polls"] = []int{first, second}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			return
		}
	}
	summary := map[string]int32{}
	for _, r := range records {
		summary[r.Label] = r.Reply.Code
		t.Logf("go %s tid=%d epoch=%d flags=%#x code=%d op=%d", r.Label, r.Reply.Submission.Transaction, r.Reply.MapEpoch, r.Reply.Flags, r.Reply.Code, r.Reply.Operations[0].Result)
	}
	receipt["result_codes"] = summary
	receipt["target_versions"] = []uint64{seededTarget.Version, chunked.Version, both.Version}
	t.Logf("Go SET_CHUNK: source A v%d mtime %d.%09d kept; target v%d -> v%d -> v%d; release polls %v",
		seededA.Version, seededA.Seconds, seededA.Nanos, seededTarget.Version, chunked.Version, both.Version, receipt["release_polls"])
}

func TestSetChunkGoRefsOrderAndTargetGate(t *testing.T) {
	a, b := setChunkGoRef{"a", 0x00000001}, setChunkGoRef{"b", 0x80000000}
	// Bit-reversed 0x80000000 (1) sorts before bit-reversed 0x00000001.
	if !bytes.Equal(setChunkGoRefs(3, a, b), setChunkGoRefs(3, b, a)) {
		t.Fatal("reference multiset depends on argument order")
	}
	two := setChunkGoRefs(3, a, b)
	if first, second := bytes.Index(two, []byte("\x01\x00\x00\x00b")), bytes.Index(two, []byte("\x01\x00\x00\x00a")); first < 0 || second < first {
		t.Fatal("hobject order not by bit-reversed hash")
	}
	one := setChunkGoRefs(3, a)
	if !bytes.Equal(one, setChunkRefsEncoding(1, "a", 1, 3)) {
		t.Fatal("single reference differs from the native checkpoint encoding")
	}
	target := nativeXattrSnapshot{Data: setChunkGoTargetData, Version: 5, Attributes: []nativeXattrEntry{{Name: []byte("chunk_refs"), Value: one}}}
	if !setChunkGoTargetHolds(target, one, 4) {
		t.Fatal("referenced target refused")
	}
	for name, c := range map[string]struct {
		s     nativeXattrSnapshot
		refs  []byte
		floor uint64
	}{
		"stale version":  {target, one, 5},
		"wrong refs":     {target, two, 4},
		"missing refs":   {nativeXattrSnapshot{Data: setChunkGoTargetData, Version: 5}, one, 4},
		"refs not gone":  {target, nil, 4},
		"changed bytes":  {nativeXattrSnapshot{Data: []byte("x"), Version: 5, Attributes: target.Attributes}, one, 4},
		"absent target":  {nativeXattrSnapshot{Code: -2}, nil, 0},
		"failed observe": {nativeXattrSnapshot{Code: -5, Data: setChunkGoTargetData, Version: 9}, nil, 0},
	} {
		if setChunkGoTargetHolds(c.s, c.refs, c.floor) {
			t.Error("contradictory target observation accepted:", name)
		}
	}
	seeded := nativeXattrSnapshot{Data: setChunkGoSourceData, Version: 3, Seconds: 1700000000, Nanos: 5}
	for name, mutate := range map[string]func(*nativeXattrSnapshot){
		"seconds": func(s *nativeXattrSnapshot) { s.Seconds++ },
		"nanos":   func(s *nativeXattrSnapshot) { s.Nanos++ },
		"version": func(s *nativeXattrSnapshot) { s.Version++ },
		"bytes":   func(s *nativeXattrSnapshot) { s.Data = []byte("other") },
		"xattr":   func(s *nativeXattrSnapshot) { s.Attributes = []nativeXattrEntry{{Name: []byte("k")}} },
	} {
		got := seeded
		mutate(&got)
		if setChunkGoSame(seeded, got) {
			t.Error("changed source accepted:", name)
		}
	}
}

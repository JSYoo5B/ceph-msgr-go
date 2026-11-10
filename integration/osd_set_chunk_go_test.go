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
// attribute). A second sequence on fresh objects releases one reference with
// a standalone UNSET_MANIFEST while its source stays alive. These consumer
// helpers are test-only and nothing is exported.

const (
	setChunkGoSourceA      = "go-set-chunk-source-a"
	setChunkGoSourceB      = "go-set-chunk-source-b"
	setChunkGoAbsentSource = "go-set-chunk-absent-source"
	setChunkGoTarget       = "go-set-chunk-target"
	setChunkGoAbsentTarget = "go-set-chunk-absent-target"
	// The UNSET_MANIFEST sequence: a kept source, its peer and their target.
	setChunkGoKeptSource   = "go-set-chunk-kept-source"
	setChunkGoPeerSource   = "go-set-chunk-peer-source"
	setChunkGoSharedTarget = "go-set-chunk-shared-target"
	setChunkGoReleasePolls = 20
	setChunkGoUnsetSeconds = 1700001400
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

// setChunkGoSourceKept reports a source head that survived UNSET_MANIFEST
// with its seeded bytes and xattr inventory. The handler marks the source
// modified, so the user version and mtime may move; the version must not go
// back. A deleted or emptied source fails.
func setChunkGoSourceKept(seeded, after nativeXattrSnapshot) bool {
	return seeded.Code == 0 && after.Code == 0 && len(seeded.Data) != 0 && bytes.Equal(after.Data, seeded.Data) && after.Version >= seeded.Version &&
		fmt.Sprint(xattrInventory(after)) == fmt.Sprint(xattrInventory(seeded))
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
	seededKept := poolSeed(t, ctx, 1, setChunkGoKeptSource, "", setChunkGoSourceData, nil)
	seededPeer := poolSeed(t, ctx, 1, setChunkGoPeerSource, "", setChunkGoOtherData, nil)
	seededShared := poolSeed(t, ctx, 2, setChunkGoSharedTarget, "", setChunkGoTargetData, nil)
	routes := map[string]osdFixtureOracle{}
	for _, name := range []string{setChunkGoSourceA, setChunkGoSourceB, setChunkGoAbsentSource, setChunkGoKeptSource, setChunkGoPeerSource} {
		routes[name] = poolPlace(t, ctx, 1, name, "")
		if routes[name].Pool != pools.ids[1] {
			t.Fatal("native placement pool differs", name, routes[name].Pool)
		}
	}
	receipt["routes"] = routes
	if _, ok := setChunkGoRefsAttr(seededShared); ok {
		t.Fatal("seeded shared target already referenced")
	}
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
		switch op.Code {
		case cephmsgr.OSDDelete:
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700001300}
		case cephmsgr.OSDUnsetManifest:
			request.ModificationTime = cephmsgr.OSDTime{Seconds: setChunkGoUnsetSeconds}
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
			t.Fatal("Go request outcome unknown; not resent", label, err)
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

	// poll reads only the target, natively, until done or the bound. lost
	// (optional) names an observation that can no longer become done. Either
	// way the last observation is kept in the receipt before the test stops.
	poll := func(t *testing.T, label, target string, done, lost func(nativeXattrSnapshot) bool) (nativeXattrSnapshot, int) {
		t.Helper()
		var s nativeXattrSnapshot
		for i := 1; i <= setChunkGoReleasePolls; i++ {
			if s = poolObserve(t, ctx, 2, target, "", ""); done(s) {
				return s, i
			}
			if lost != nil && lost(s) {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		refs, _ := setChunkGoRefsAttr(s)
		receipt["last_target_observation"] = map[string]any{"label": label, "target": target, "code": s.Code, "version": s.Version, "data": s.Data, "chunk_refs": refs}
		t.Fatalf("%s: target %s never reached the expected state within %d polls: code %d version %d refs %x", label, target, setChunkGoReleasePolls, s.Code, s.Version, refs)
		return s, 0
	}

	var chunked, overlapped, both nativeXattrSnapshot
	var shared, kept nativeXattrSnapshot
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
			send(t, "delete-source-a", setChunkGoSourceA, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
			one := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoSourceB, routes[setChunkGoSourceB].Hash})
			_, first := poll(t, "release-a", setChunkGoTarget, func(s nativeXattrSnapshot) bool { return setChunkGoTargetHolds(s, one, both.Version) }, nil)
			if poolObserve(t, ctx, 1, setChunkGoSourceA, "", "").Code != -2 {
				t.Fatal("deleted Go source remains")
			}
			send(t, "delete-source-b", setChunkGoSourceB, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
			_, second := poll(t, "release-b", setChunkGoTarget, func(s nativeXattrSnapshot) bool { return s.Code == -2 }, nil)
			if poolObserve(t, ctx, 1, setChunkGoSourceB, "", "").Code != -2 {
				t.Fatal("deleted Go source remains")
			}
			receipt["release_polls"] = []int{first, second}
		}},
		{"two_fresh_sources_share_a_target", func(t *testing.T) {
			send(t, "chunk-kept", setChunkGoKeptSource, chunk(8, setChunkGoSharedTarget, 0), 0)
			send(t, "chunk-peer", setChunkGoPeerSource, chunk(16, setChunkGoSharedTarget, 8), 0)
			two := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoKeptSource, routes[setChunkGoKeptSource].Hash}, setChunkGoRef{setChunkGoPeerSource, routes[setChunkGoPeerSource].Hash})
			shared = poolObserve(t, ctx, 2, setChunkGoSharedTarget, "", "")
			if !setChunkGoTargetHolds(shared, two, seededShared.Version) || !setChunkGoSame(poolObserve(t, ctx, 1, setChunkGoKeptSource, "", ""), seededKept) ||
				!setChunkGoSame(poolObserve(t, ctx, 1, setChunkGoPeerSource, "", ""), seededPeer) {
				got, _ := setChunkGoRefsAttr(shared)
				t.Fatalf("two-source native state differs before UNSET_MANIFEST: target v%d refs %x want %x", shared.Version, got, two)
			}
		}},
		{"unset_manifest_keeps_the_source_and_releases_its_reference", func(t *testing.T) {
			// Sent once. The source reply does not wait for the target: the
			// OSD drops the reference after the source commit, without a
			// completion back to this caller.
			reply := send(t, "unset-manifest-kept", setChunkGoKeptSource, cephmsgr.OSDOperation{Code: cephmsgr.OSDUnsetManifest}, 0)
			kept = poolObserve(t, ctx, 1, setChunkGoKeptSource, "", "")
			peer := poolObserve(t, ctx, 1, setChunkGoPeerSource, "", "")
			// Both heads keep their seeded bytes. The kept source's version and
			// mtime are recorded as observed; SET_CHUNK's unchanged-version
			// expectation does not apply to UNSET_MANIFEST.
			if !setChunkGoSourceKept(seededKept, kept) || !setChunkGoSame(peer, seededPeer) {
				t.Fatalf("sources after UNSET_MANIFEST: kept code %d %q v%d (seeded v%d), peer code %d %q v%d (seeded v%d)",
					kept.Code, kept.Data, kept.Version, seededKept.Version, peer.Code, peer.Data, peer.Version, seededPeer.Version)
			}
			receipt["kept_source"] = map[string]any{"seeded_version": seededKept.Version, "seeded_seconds": seededKept.Seconds, "seeded_nanos": seededKept.Nanos,
				"version": kept.Version, "seconds": kept.Seconds, "nanos": kept.Nanos, "reply_user_version": reply.UserVersion}
			// A surviving source alone would also pass a no-op, so the target
			// must lose exactly that reference: unchanged bytes, a newer
			// version and the by_object encoding of the peer alone. An absent
			// target would mean the peer's reference was lost; it ends the poll.
			one := setChunkGoRefs(pools.ids[1], setChunkGoRef{setChunkGoPeerSource, routes[setChunkGoPeerSource].Hash})
			released, polls := poll(t, "unset-release-kept", setChunkGoSharedTarget,
				func(s nativeXattrSnapshot) bool { return setChunkGoTargetHolds(s, one, shared.Version) },
				func(s nativeXattrSnapshot) bool { return s.Code == -2 })
			receipt["unset_release_polls"] = polls
			// The source is plain again: the pinned handler refuses a second
			// UNSET_MANIFEST on an object without a manifest, and that refusal
			// changes neither the source nor the target.
			send(t, "unset-manifest-again", setChunkGoKeptSource, cephmsgr.OSDOperation{Code: cephmsgr.OSDUnsetManifest}, -95)
			if !setChunkGoSame(poolObserve(t, ctx, 1, setChunkGoKeptSource, "", ""), kept) || !setChunkGoSame(poolObserve(t, ctx, 2, setChunkGoSharedTarget, "", ""), released) {
				t.Fatal("refused second UNSET_MANIFEST changed native state")
			}
			t.Logf("UNSET_MANIFEST kept source: v%d mtime %d.%09d -> v%d mtime %d.%09d (reply user version %d); shared target v%d -> v%d after %d poll(s)",
				seededKept.Version, seededKept.Seconds, seededKept.Nanos, kept.Version, kept.Seconds, kept.Nanos, reply.UserVersion, shared.Version, released.Version, polls)
			shared = released
		}},
		{"deleting_the_peer_removes_the_target_and_keeps_the_source", func(t *testing.T) {
			send(t, "delete-peer-source", setChunkGoPeerSource, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
			_, polls := poll(t, "release-peer", setChunkGoSharedTarget, func(s nativeXattrSnapshot) bool { return s.Code == -2 }, nil)
			receipt["peer_release_polls"] = polls
			if poolObserve(t, ctx, 1, setChunkGoPeerSource, "", "").Code != -2 || !setChunkGoSame(poolObserve(t, ctx, 1, setChunkGoKeptSource, "", ""), kept) {
				t.Fatal("peer deletion left the peer or changed the kept source")
			}
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
	receipt["shared_target_versions"] = []uint64{seededShared.Version, shared.Version}
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
	// After UNSET_MANIFEST the version and mtime may move; deletion, emptied
	// or changed bytes, a version going back and a changed inventory may not.
	modified := seeded
	modified.Version, modified.Seconds, modified.Nanos = 4, setChunkGoUnsetSeconds, 0
	if !setChunkGoSourceKept(seeded, seeded) || !setChunkGoSourceKept(seeded, modified) {
		t.Fatal("kept source refused")
	}
	for name, after := range map[string]nativeXattrSnapshot{
		"deleted":        {Code: -2},
		"emptied":        {Version: 4},
		"changed bytes":  {Data: setChunkGoOtherData, Version: 4},
		"older version":  {Data: setChunkGoSourceData, Version: 2},
		"new xattr":      {Data: setChunkGoSourceData, Version: 4, Attributes: []nativeXattrEntry{{Name: []byte("k")}}},
		"failed observe": {Code: -5, Data: setChunkGoSourceData, Version: 4},
	} {
		if setChunkGoSourceKept(seeded, after) {
			t.Error("lost or changed source accepted after UNSET_MANIFEST:", name)
		}
	}
	if setChunkGoSourceKept(nativeXattrSnapshot{Code: -2}, nativeXattrSnapshot{Code: -2}) || setChunkGoSourceKept(nativeXattrSnapshot{}, nativeXattrSnapshot{}) {
		t.Error("absent or empty seed accepted as a kept source")
	}
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

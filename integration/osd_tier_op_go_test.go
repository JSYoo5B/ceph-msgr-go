package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Go-originated TIER_PROMOTE, TIER_FLUSH and TIER_EVICT on two owned plain
// replicated pools: slot 1 holds the manifest objects, slot 2 their chunk and
// redirect targets and, once slot 1 carries its dedup options, the
// fingerprint-named dedup chunks. Every tier operation the Go client sends is
// paired with the same operation issued once by a closed native librados
// twin on a separate object seeded the same way, and both results must equal
// the source-derived signed code. The native observer judges both objects
// without promoting them (IGNORE_REDIRECT local reads, is_dirty, proxied
// reads of closed extents and the slot-2 targets' bytes and chunk_refs).
// Setup operations (SET_CHUNK, SET_REDIRECT, UNDIRTY, DELETE) are already
// admitted Go operations; the probe of a cleared manifest is a signed
// UNSET_MANIFEST refusal. These consumer helpers are test-only and nothing is
// exported.

const (
	tierGoAbsent = "tier-absent"
	tierGoPolls  = 20
	tierGoMTime  = 1700002000
)

// tierGoTwin is one Go-addressed object and its native twin in slot 1, each
// with its own seed and, where one is needed, its own slot-2 target.
type tierGoTwin struct {
	name, target string
	seed, data   []byte
}

var (
	tierGoPlain = [2]tierGoTwin{
		{name: "tier-plain-go", seed: []byte("P\x00\xffgo-tier-plain-0123456789abcdef")},
		{name: "tier-plain-native", seed: []byte("Q\x00\xffnative-tier-plain-0123456789")},
	}
	tierGoChunk = [2]tierGoTwin{
		{name: "tier-chunk-go", target: "tier-chunk-target-go", seed: []byte("C\x00\xffgo-tier-chunk-source-01234567"), data: []byte("T\x00\xffgo-tier-chunk-target-89abcdef")},
		{name: "tier-chunk-native", target: "tier-chunk-target-native", seed: []byte("D\x00\xffnative-tier-chunk-source-0123"), data: []byte("U\x00\xffnative-tier-chunk-target-4567")},
	}
	tierGoRedirect = [2]tierGoTwin{
		{name: "tier-redirect-go", target: "tier-redirect-target-go", seed: []byte("R\x00\xffgo-tier-redirect-source"), data: []byte("X\x00\xffgo-tier-redirect-target-bytes")},
		{name: "tier-redirect-native", target: "tier-redirect-target-native", seed: []byte("S\x00\xffnative-tier-redirect-src"), data: []byte("Y\x00\xffnative-tier-redirect-target-b")},
	}
	// Three fixed 16-byte CDC chunks (16, 16 and 8 bytes), distinct across
	// both twins so that no fingerprint chunk is shared.
	tierGoDedup = [2]tierGoTwin{
		{name: "tier-dedup-go", seed: []byte("go-dedup-chunk-0go-dedup-chunk-1go-tail2")},
		{name: "tier-dedup-native", seed: []byte("nat-dedup-chunk0nat-dedup-chunk1nat-tail")},
	}
)

// tierGoLocal is the observer's non-promoting view of one slot-1 object.
type tierGoLocal struct {
	Code    int32  `json:"code"`
	Version uint64 `json:"version"`
	Size    uint64 `json:"size"`
	Seconds int64  `json:"seconds"`
	Nanos   int64  `json:"nanos"`
	Data    []byte `json:"data"`
	Dirty   struct {
		Code  int32 `json:"code"`
		Prval int32 `json:"isdirty_prval"`
		Dirty *bool `json:"dirty"`
	} `json:"dirty"`
	Proxied *struct {
		Extent []uint64 `json:"extent"`
		Code   int32    `json:"code"`
		Prval  int32    `json:"read_prval"`
		Data   []byte   `json:"data"`
	} `json:"proxied"`
}

// dirty reports a valid is_dirty observation and its value.
func (l tierGoLocal) dirty() (bool, bool) {
	if l.Code != 0 || l.Dirty.Code != 0 || l.Dirty.Prval != 0 || l.Dirty.Dirty == nil {
		return false, false
	}
	return *l.Dirty.Dirty, true
}

// tierGoHolds reports an existing object with exactly data, the given dirty
// state and, when version is nonzero, that user version.
func tierGoHolds(l tierGoLocal, data []byte, dirty bool, version uint64) bool {
	d, ok := l.dirty()
	return ok && d == dirty && l.Size == uint64(len(data)) && bytes.Equal(l.Data, data) && (version == 0 || l.Version == version)
}

// tierGoSame reports an unchanged object: bytes, user version, mtime and dirty.
func tierGoSame(a, b tierGoLocal) bool {
	da, oka := a.dirty()
	db, okb := b.dirty()
	return a.Code == b.Code && oka == okb && da == db && bytes.Equal(a.Data, b.Data) && a.Size == b.Size && a.Version == b.Version && a.Seconds == b.Seconds && a.Nanos == b.Nanos
}

// tierGoProxied reports a proxied extent read that returned exactly data.
func tierGoProxied(l tierGoLocal, data []byte) bool {
	return l.Proxied != nil && l.Proxied.Code >= 0 && l.Proxied.Prval >= 0 && bytes.Equal(l.Proxied.Data, data)
}

// tierGoEvicted is the local view after TIER_EVICT: every chunk range punched
// to zeros, the rest kept, the size, user version and mtime unchanged.
func tierGoEvicted(seed []byte, chunks int) []byte {
	out := slices.Clone(seed)
	clear(out[:chunks])
	return out
}

// tierGoFingerprints are the dedup chunk object names and bytes of one seed:
// sha256 hex of each fixed 16-byte chunk (PrimaryLogPG get_fpoid_from_chunk).
func tierGoFingerprints(seed []byte) ([]string, [][]byte) {
	var names []string
	var chunks [][]byte
	for i := 0; i < len(seed); i += 16 {
		chunk := seed[i:min(i+16, len(seed))]
		sum := sha256.Sum256(chunk)
		names, chunks = append(names, hex.EncodeToString(sum[:])), append(chunks, chunk)
	}
	return names, chunks
}

type tierGoRecord struct {
	Label        string              `json:"label"`
	Request      cephmsgr.OSDRequest `json:"request"`
	Reply        cephmsgr.OSDReply   `json:"reply"`
	Error        string              `json:"error,omitempty"`
	Unknown      bool                `json:"unknown_outcome"`
	NativeObject string              `json:"native_object,omitempty"`
	NativeCode   *int32              `json:"native_code,omitempty"`
}

// tierGoOracle runs one closed call of the native tier observer and returns
// its result.
func tierGoOracle(t *testing.T, ctx context.Context, input map[string]any) json.RawMessage {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > 4096 {
		t.Fatal("bounded tier observer input", err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-tier-op-go-input.json"), raw); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-tier-op-go-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-tier-op", "call"); err != nil {
		t.Fatal("native tier observer", input["action"], err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-tier-op-go-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("tier observer result exceeds bound", err)
	}
	var result struct {
		Schema     int             `json:"schema_version"`
		Commit     string          `json:"source_commit"`
		Checkpoint string          `json:"checkpoint"`
		Input      map[string]any  `json:"input"`
		Result     json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Schema != 1 || result.Commit != mdsFreshSource || result.Checkpoint != "tier-op-go-observer-v1" || result.Input["action"] != input["action"] {
		t.Fatal("tier observer result provenance differs", result.Schema, result.Commit, result.Checkpoint)
	}
	return result.Result
}

func tierGoObserve(t *testing.T, ctx context.Context, name string, extent ...uint64) tierGoLocal {
	t.Helper()
	input := map[string]any{"action": "observe", "object": name}
	if len(extent) == 2 {
		input["extent"] = extent
	}
	var l tierGoLocal
	if err := json.Unmarshal(tierGoOracle(t, ctx, input), &l); err != nil {
		t.Fatal(err)
	}
	if l.Code != 0 && l.Code != -2 {
		t.Fatal("native local observation failed", name, l.Code)
	}
	return l
}

//fixture:isolated owned-pool cleanup leaves central config mon/mon_allow_pool_delete=true set
func TestCephOSDTierOpGoIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	client, _, base, _ := fixture(t)
	ctx, stop := context.WithTimeout(context.Background(), 8*time.Minute)
	defer stop()
	directory := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	pools := createOwnedPools(t, ctx, base, 1, 2)
	records := []tierGoRecord{}
	observations := map[string]tierGoLocal{}
	receipt := map[string]any{"source_commit": mdsFreshSource, "pools": pools.ids, "identity": client.Snapshot()}
	defer func() {
		receipt["requests"] = records
		receipt["observations"] = observations
		encoded, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(directory, "osd-tier-op-go-results.json"), append(encoded, '\n'))
		}
		if err != nil {
			t.Error(err)
		}
	}()
	var bound struct {
		Source int64 `json:"source_pool_id"`
		Chunk  int64 `json:"chunk_pool_id"`
	}
	if err := json.Unmarshal(tierGoOracle(t, ctx, map[string]any{"action": "bind", "source_pool_id": pools.ids[1], "chunk_pool_id": pools.ids[2]}), &bound); err != nil ||
		bound.Source != pools.ids[1] || bound.Chunk != pools.ids[2] {
		t.Fatal("native tier observer bound other pools", bound, err)
	}
	routes := map[string]osdFixtureOracle{}
	for _, set := range [][2]tierGoTwin{tierGoPlain, tierGoChunk, tierGoRedirect, tierGoDedup} {
		for _, twin := range set {
			routes[twin.name] = poolPlace(t, ctx, 1, twin.name, "")
		}
	}
	routes[tierGoAbsent] = poolPlace(t, ctx, 1, tierGoAbsent, "")
	for name, route := range routes {
		if route.Pool != pools.ids[1] {
			t.Fatal("native placement pool differs", name, route.Pool)
		}
	}
	receipt["routes"] = routes
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, c := range connections {
			c.Close()
		}
	}()
	seedOf := func(route osdFixtureOracle) uint32 {
		if isReplicatedPGFixtureProfile() {
			return route.PG
		}
		return route.Hash
	}
	observe := func(t *testing.T, label, name string, extent ...uint64) tierGoLocal {
		t.Helper()
		l := tierGoObserve(t, ctx, name, extent...)
		observations[label+"/"+name] = l
		return l
	}
	// send issues one Go request exactly once and checks its signed result,
	// identity and placement echo. The per-op result must be zero or the
	// aggregate unless perOp names the expected value. An unknown outcome
	// stops the test; it is never resent.
	send := func(t *testing.T, label, object string, op cephmsgr.OSDOperation, want int32, perOp ...int32) cephmsgr.OSDReply {
		t.Helper()
		route := routes[object]
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, client, ctx, route)
		}
		request := pools.request(route, op)
		switch op.Code {
		case cephmsgr.OSDTierFlush, cephmsgr.OSDTierEvict:
			// The native rados tier-flush/tier-evict request flags.
			request.IgnoreCache, request.IgnoreOverlay = true, true
		case cephmsgr.OSDTierPromote, cephmsgr.OSDDelete, cephmsgr.OSDUndirty:
			request.ModificationTime = cephmsgr.OSDTime{Seconds: tierGoMTime}
		}
		reply, err := connections[route.ID].Request(ctx, request)
		entry := tierGoRecord{Label: label, Request: request, Reply: reply}
		var unknown *cephmsgr.OutcomeUnknownError
		entry.Unknown = errors.As(err, &unknown)
		if err != nil {
			entry.Error = err.Error()
		}
		records = append(records, entry)
		if entry.Unknown {
			t.Fatal("Go request outcome unknown; not resent", label, err)
		}
		if reply.Submission.Transaction == 0 || reply.Submission.GlobalID != client.Snapshot().GlobalID || reply.Object != object || reply.PG.Pool != uint64(route.Pool) ||
			reply.PG.Seed != seedOf(route) || reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || len(reply.RawFront) == 0 || len(reply.Operations) != 1 {
			t.Fatal("Go reply lost identity, placement or its operation", label, reply.Code, err)
		}
		if reply.Code < 0 {
			var remote *cephmsgr.OSDError
			if !errors.As(err, &remote) || remote.Code != reply.Code {
				t.Fatal("signed server refusal lost its raw error", label, reply.Code, err)
			}
		} else if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("successful reply returned an error or lacks ONDISK", label, reply.Flags, err)
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
	operation := map[cephmsgr.OSDOpcode]string{cephmsgr.OSDTierPromote: "tier-promote", cephmsgr.OSDTierFlush: "tier-flush", cephmsgr.OSDTierEvict: "tier-evict"}
	// pair sends one Go tier operation to the Go object and has the native
	// twin issue the same operation once on its own object. Both must return
	// the source-derived signed code.
	pair := func(t *testing.T, label string, code cephmsgr.OSDOpcode, set [2]tierGoTwin, want int32, perOp ...int32) {
		t.Helper()
		send(t, label, set[0].name, cephmsgr.OSDOperation{Code: code}, want, perOp...)
		var native struct {
			Code    int32  `json:"code"`
			Version uint64 `json:"version"`
		}
		if err := json.Unmarshal(tierGoOracle(t, ctx, map[string]any{"action": "native", "label": label, "operation": operation[code], "object": set[1].name}), &native); err != nil {
			t.Fatal(err)
		}
		records[len(records)-1].NativeObject, records[len(records)-1].NativeCode = set[1].name, &native.Code
		if native.Code != want {
			t.Fatalf("native twin %s returned %d, want %d (Go returned %d)", label, native.Code, want, records[len(records)-1].Reply.Code)
		}
	}
	chunkRefs := func(source string) []byte {
		return setChunkGoRefs(pools.ids[1], setChunkGoRef{source, routes[source].Hash})
	}
	// poll reads natively until done or the bound; the last observation is
	// kept before the test stops.
	poll := func(t *testing.T, label string, done func() (bool, any)) int {
		t.Helper()
		var last any
		for i := 1; i <= tierGoPolls; i++ {
			ok, state := done()
			if ok {
				return i
			}
			last = state
			time.Sleep(500 * time.Millisecond)
		}
		receipt["last_poll_observation"] = map[string]any{"label": label, "state": last}
		t.Fatalf("%s never reached the expected state within %d polls: %v", label, tierGoPolls, last)
		return 0
	}

	var plain, chunked, redirected, deduped [2]tierGoLocal
	var chunkTargets [2]nativeXattrSnapshot
	steps := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"absent_objects_are_refused", func(t *testing.T) {
			if observe(t, "absent", tierGoAbsent).Code != -2 {
				t.Fatal("absent control exists")
			}
			absent := [2]tierGoTwin{{name: tierGoAbsent}, {name: tierGoAbsent}}
			pair(t, "absent-promote", cephmsgr.OSDTierPromote, absent, -2)
			pair(t, "absent-flush", cephmsgr.OSDTierFlush, absent, -2)
			pair(t, "absent-evict", cephmsgr.OSDTierEvict, absent, -2)
			if observe(t, "absent-after", tierGoAbsent).Code != -2 {
				t.Fatal("refused tier operations created the absent object")
			}
		}},
		{"plain_objects_refuse_evict_and_flush_and_promote_is_a_no_op", func(t *testing.T) {
			for i, twin := range tierGoPlain {
				poolSeed(t, ctx, 1, twin.name, "", twin.seed, nil)
				plain[i] = observe(t, "plain-seeded", twin.name)
				if !tierGoHolds(plain[i], twin.seed, true, 0) {
					t.Fatal("seeded plain object differs or is not dirty", twin.name)
				}
			}
			// A plain object has no manifest; the source pool has no
			// fingerprint algorithm or dedup tier yet.
			pair(t, "plain-evict", cephmsgr.OSDTierEvict, tierGoPlain, -22)
			pair(t, "plain-flush", cephmsgr.OSDTierFlush, tierGoPlain, -22)
			pair(t, "plain-promote", cephmsgr.OSDTierPromote, tierGoPlain, 0)
			for i, twin := range tierGoPlain {
				if after := observe(t, "plain-after", twin.name); !tierGoSame(after, plain[i]) {
					t.Fatal("refused or no-op tier operation changed the plain object", twin.name, after, plain[i])
				}
			}
		}},
		{"chunked_object_evicts_and_promotes_from_its_chunk_target", func(t *testing.T) {
			for i, twin := range tierGoChunk {
				poolSeed(t, ctx, 1, twin.name, "", twin.seed, nil)
				poolSeed(t, ctx, 2, twin.target, "", twin.data, nil)
				send(t, "chunk-set-"+twin.name, twin.name, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetChunk, Flags: cephmsgr.OSDWithReference,
					Chunk: cephmsgr.OSDChunkInput{SourceLength: 16, TargetPool: pools.ids[2], TargetName: twin.target}}, 0)
				chunked[i] = observe(t, "chunk-set", twin.name)
				chunkTargets[i] = poolObserve(t, ctx, 2, twin.target, "", "")
				if refs, ok := setChunkGoRefsAttr(chunkTargets[i]); !tierGoHolds(chunked[i], twin.seed, true, 0) || !ok || !bytes.Equal(refs, chunkRefs(twin.name)) || !bytes.Equal(chunkTargets[i].Data, twin.data) {
					t.Fatal("chunked source or its target reference differs", twin.name, refs)
				}
			}
			// A dirty chunked manifest takes the dedup flush, which the
			// source pool has not configured yet.
			pair(t, "chunk-dirty-flush", cephmsgr.OSDTierFlush, tierGoChunk, -22)
			for i, twin := range tierGoChunk {
				if after := observe(t, "chunk-dirty-flush", twin.name); !tierGoSame(after, chunked[i]) {
					t.Fatal("refused dirty flush changed the chunked object", twin.name)
				}
				send(t, "chunk-undirty-"+twin.name, twin.name, cephmsgr.OSDOperation{Code: cephmsgr.OSDUndirty}, 0)
				chunked[i] = observe(t, "chunk-undirtied", twin.name)
				if !tierGoHolds(chunked[i], twin.seed, false, 0) {
					t.Fatal("UNDIRTY did not leave a clean chunked object", twin.name)
				}
			}
			// A clean chunked manifest is already flushed.
			pair(t, "chunk-clean-flush", cephmsgr.OSDTierFlush, tierGoChunk, 0)
			for i, twin := range tierGoChunk {
				if after := observe(t, "chunk-clean-flush", twin.name); !tierGoSame(after, chunked[i]) {
					t.Fatal("clean flush changed the chunked object", twin.name)
				}
			}
			pair(t, "chunk-evict", cephmsgr.OSDTierEvict, tierGoChunk, 0)
			for i, twin := range tierGoChunk {
				// The chunk range is punched locally, the rest stays, and the
				// cache-mode operation changes neither user version nor mtime.
				// A proxied read of exactly that range is served from the chunk
				// target, which shows the chunk is marked missing.
				after := observe(t, "chunk-evicted", twin.name, 0, 16)
				if !tierGoHolds(after, tierGoEvicted(twin.seed, 16), false, chunked[i].Version) || after.Seconds != chunked[i].Seconds || after.Nanos != chunked[i].Nanos ||
					!tierGoProxied(after, twin.data[:16]) {
					t.Fatalf("evicted chunked object differs: %s local %q v%d (was v%d) proxied %+v", twin.name, after.Data, after.Version, chunked[i].Version, after.Proxied)
				}
				if target := poolObserve(t, ctx, 2, twin.target, "", ""); !setChunkGoSame(target, chunkTargets[i]) {
					t.Fatal("evict changed the chunk target or its reference", twin.target)
				}
			}
			pair(t, "chunk-promote", cephmsgr.OSDTierPromote, tierGoChunk, 0)
			var promoted [2]tierGoLocal
			for i, twin := range tierGoChunk {
				// Promote copies the chunk target's bytes into the chunk range;
				// they differ from the seeded source bytes there.
				promoted[i] = observe(t, "chunk-promoted", twin.name)
				want := append(slices.Clone(twin.data[:16]), twin.seed[16:]...)
				if d, ok := promoted[i].dirty(); !ok || !bytes.Equal(promoted[i].Data, want) || promoted[i].Size != uint64(len(twin.seed)) || promoted[i].Version < chunked[i].Version {
					t.Fatalf("promoted chunked object differs: %s %q dirty %v/%v v%d", twin.name, promoted[i].Data, d, ok, promoted[i].Version)
				}
				if target := poolObserve(t, ctx, 2, twin.target, "", ""); !setChunkGoSame(target, chunkTargets[i]) {
					t.Fatal("promote changed the chunk target or its reference", twin.target)
				}
			}
			// The Go and native twins changed in the same way.
			gd, _ := promoted[0].dirty()
			nd, _ := promoted[1].dirty()
			if gd != nd || (promoted[0].Version == chunked[0].Version) != (promoted[1].Version == chunked[1].Version) ||
				(promoted[0].Seconds == chunked[0].Seconds) != (promoted[1].Seconds == chunked[1].Seconds) {
				t.Fatal("Go and native promote changed dirty, version or mtime differently", promoted, chunked)
			}
			receipt["chunk_promote"] = map[string]any{"go": map[string]any{"before": chunked[0], "after": promoted[0]}, "native": map[string]any{"before": chunked[1], "after": promoted[1]}}
		}},
		{"redirect_promote_copies_the_target_and_clears_the_manifest", func(t *testing.T) {
			for i, twin := range tierGoRedirect {
				poolSeed(t, ctx, 1, twin.name, "", twin.seed, nil)
				target := poolSeed(t, ctx, 2, twin.target, "", twin.data, nil)
				send(t, "redirect-set-"+twin.name, twin.name, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetRedirect,
					Redirect: cephmsgr.OSDRedirectInput{TargetPool: pools.ids[2], TargetName: twin.target, TargetVersion: target.Version}}, 0)
				redirected[i] = observe(t, "redirect-set", twin.name)
				if redirected[i].Code != 0 || redirected[i].Size != 0 || redirected[i].Version != target.Version {
					t.Fatal("redirected object is not an empty local manifest at the target version", twin.name, redirected[i])
				}
			}
			// A redirect manifest has no chunks: evict succeeds without a
			// transaction and changes nothing.
			pair(t, "redirect-evict", cephmsgr.OSDTierEvict, tierGoRedirect, 0)
			for i, twin := range tierGoRedirect {
				if after := observe(t, "redirect-evict", twin.name); !tierGoSame(after, redirected[i]) {
					t.Fatal("evict changed the redirected object", twin.name)
				}
			}
			pair(t, "redirect-promote", cephmsgr.OSDTierPromote, tierGoRedirect, 0)
			for i, twin := range tierGoRedirect {
				after := observe(t, "redirect-promoted", twin.name)
				target := poolObserve(t, ctx, 2, twin.target, "", "")
				if d, ok := after.dirty(); !ok || after.Code != 0 || !bytes.Equal(after.Data, twin.data) || after.Version != target.Version || !bytes.Equal(target.Data, twin.data) {
					t.Fatalf("promoted redirect differs: %s %q v%d (target v%d) dirty %v/%v", twin.name, after.Data, after.Version, target.Version, d, ok)
				}
				// The manifest is gone: UNSET_MANIFEST is refused on a plain object.
				send(t, "redirect-unset-"+twin.name, twin.name, cephmsgr.OSDOperation{Code: cephmsgr.OSDUnsetManifest}, -95)
				redirected[i] = after
			}
		}},
		{"dedup_flush_creates_fingerprint_chunks_then_evict_and_promote_restore", func(t *testing.T) {
			var configured struct {
				Configured bool           `json:"configured"`
				Options    map[string]any `json:"options"`
			}
			if err := json.Unmarshal(tierGoOracle(t, ctx, map[string]any{"action": "configure-dedup"}), &configured); err != nil || !configured.Configured {
				t.Fatal("native dedup configuration of the source pool failed", configured.Options, err)
			}
			receipt["dedup_options"] = configured.Options
			for i, twin := range tierGoDedup {
				poolSeed(t, ctx, 1, twin.name, "", twin.seed, nil)
				deduped[i] = observe(t, "dedup-seeded", twin.name)
				if !tierGoHolds(deduped[i], twin.seed, true, 0) {
					t.Fatal("seeded dedup source differs or is not dirty", twin.name)
				}
				names, _ := tierGoFingerprints(twin.seed)
				for _, name := range names {
					if poolObserve(t, ctx, 2, name, "", "").Code != -2 {
						t.Fatal("dedup chunk already exists", name)
					}
				}
			}
			// The dirty plain object is chunked into the dedup pool. The reply
			// (reply_op_error from finish_set_dedup) follows the chunk
			// references, right after the clean transaction is submitted, so
			// the observation polls; it keeps the handler's per-op -EAGAIN
			// beside the aggregate 0.
			pair(t, "dedup-flush", cephmsgr.OSDTierFlush, tierGoDedup, 0, -11)
			polls := map[string]int{}
			for i, twin := range tierGoDedup {
				names, chunks := tierGoFingerprints(twin.seed)
				polls[twin.name] = poll(t, "dedup-flushed-"+twin.name, func() (bool, any) {
					local := tierGoObserve(t, ctx, twin.name)
					state := map[string]any{"local": local}
					if !tierGoHolds(local, twin.seed, false, deduped[i].Version) {
						return false, state
					}
					for j, name := range names {
						chunk := poolObserve(t, ctx, 2, name, "", "")
						refs, ok := setChunkGoRefsAttr(chunk)
						state[name] = map[string]any{"code": chunk.Code, "refs": refs}
						if chunk.Code != 0 || !bytes.Equal(chunk.Data, chunks[j]) || !ok || !bytes.Equal(refs, chunkRefs(twin.name)) {
							return false, state
						}
					}
					return true, state
				})
				deduped[i] = observe(t, "dedup-flushed", twin.name)
			}
			receipt["dedup_flush_polls"] = polls
			// The flushed manifest is clean: a second flush changes nothing.
			pair(t, "dedup-clean-flush", cephmsgr.OSDTierFlush, tierGoDedup, 0)
			for i, twin := range tierGoDedup {
				if after := observe(t, "dedup-clean-flush", twin.name); !tierGoSame(after, deduped[i]) {
					t.Fatal("clean flush changed the dedup source", twin.name)
				}
			}
			pair(t, "dedup-evict", cephmsgr.OSDTierEvict, tierGoDedup, 0)
			for i, twin := range tierGoDedup {
				_, chunks := tierGoFingerprints(twin.seed)
				after := observe(t, "dedup-evicted", twin.name, 0, 16)
				if !tierGoHolds(after, make([]byte, len(twin.seed)), false, deduped[i].Version) || after.Seconds != deduped[i].Seconds || after.Nanos != deduped[i].Nanos ||
					!tierGoProxied(after, chunks[0]) {
					t.Fatalf("evicted dedup source differs: %s %q v%d proxied %+v", twin.name, after.Data, after.Version, after.Proxied)
				}
			}
			pair(t, "dedup-promote", cephmsgr.OSDTierPromote, tierGoDedup, 0)
			for _, twin := range tierGoDedup {
				after := observe(t, "dedup-promoted", twin.name)
				if after.Code != 0 || !bytes.Equal(after.Data, twin.seed) {
					t.Fatalf("promoted dedup source differs: %s %q", twin.name, after.Data)
				}
			}
			// Deleting each source releases its chunk references; cls_cas
			// removes each dedup chunk with its last reference.
			for _, twin := range tierGoDedup {
				send(t, "dedup-delete-"+twin.name, twin.name, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}, 0)
				names, _ := tierGoFingerprints(twin.seed)
				polls["release-"+twin.name] = poll(t, "dedup-release-"+twin.name, func() (bool, any) {
					codes := map[string]int{}
					for _, name := range names {
						codes[name] = poolObserve(t, ctx, 2, name, "", "").Code
						if codes[name] != -2 {
							return false, codes
						}
					}
					return true, codes
				})
				if tierGoObserve(t, ctx, twin.name).Code != -2 {
					t.Fatal("deleted dedup source remains", twin.name)
				}
			}
		}},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) {
			return
		}
	}
	summary := map[string][2]int32{}
	for _, r := range records {
		native := int32(1)
		if r.NativeCode != nil {
			native = *r.NativeCode
		}
		summary[r.Label] = [2]int32{r.Reply.Code, native}
		t.Logf("go %s tid=%d epoch=%d flags=%#x code=%d op=%d native=%s", r.Label, r.Reply.Submission.Transaction, r.Reply.MapEpoch, r.Reply.Flags, r.Reply.Code, r.Reply.Operations[0].Result, tierGoNative(r))
	}
	receipt["result_codes"] = summary
}

func tierGoNative(r tierGoRecord) string {
	if r.NativeCode == nil {
		return "-"
	}
	return fmt.Sprintf("%s:%d", r.NativeObject, *r.NativeCode)
}

func TestTierGoObservationGates(t *testing.T) {
	yes, no := true, false
	local := func(data []byte, dirty *bool, version uint64) tierGoLocal {
		l := tierGoLocal{Version: version, Size: uint64(len(data)), Data: data, Seconds: 1700000500, Nanos: 7}
		l.Dirty.Dirty = dirty
		return l
	}
	seed := []byte("0123456789abcdef-tail")
	clean := local(seed, &no, 4)
	if !tierGoHolds(clean, seed, false, 4) || !tierGoHolds(clean, seed, false, 0) || !tierGoSame(clean, clean) {
		t.Fatal("matching observation refused")
	}
	for name, l := range map[string]tierGoLocal{
		"dirty":           local(seed, &yes, 4),
		"no dirty answer": local(seed, nil, 4),
		"other bytes":     local([]byte("0123456789abcdef-TAIL"), &no, 4),
		"short":           local(seed[:4], &no, 4),
		"other version":   local(seed, &no, 5),
		"absent":          {Code: -2},
	} {
		if tierGoHolds(l, seed, false, 4) {
			t.Error("contradictory observation accepted:", name)
		}
	}
	failed := clean
	failed.Dirty.Prval = -5
	if tierGoHolds(failed, seed, false, 4) {
		t.Error("failed is_dirty accepted")
	}
	for name, mutate := range map[string]func(*tierGoLocal){
		"seconds": func(l *tierGoLocal) { l.Seconds++ },
		"nanos":   func(l *tierGoLocal) { l.Nanos++ },
		"version": func(l *tierGoLocal) { l.Version++ },
		"bytes":   func(l *tierGoLocal) { l.Data = []byte("x") },
		"dirty":   func(l *tierGoLocal) { l.Dirty.Dirty = &yes },
	} {
		changed := clean
		mutate(&changed)
		if tierGoSame(clean, changed) {
			t.Error("changed object accepted as unchanged:", name)
		}
	}
	if got := tierGoEvicted(seed, 16); !bytes.Equal(got[:16], make([]byte, 16)) || !bytes.Equal(got[16:], seed[16:]) || bytes.Equal(seed[:16], got[:16]) {
		t.Fatal("evicted view differs", got)
	}
	proxied := clean
	if tierGoProxied(proxied, seed[:16]) {
		t.Fatal("missing proxied read accepted")
	}
	proxied.Proxied = &struct {
		Extent []uint64 `json:"extent"`
		Code   int32    `json:"code"`
		Prval  int32    `json:"read_prval"`
		Data   []byte   `json:"data"`
	}{Extent: []uint64{0, 16}, Data: seed[:16]}
	if !tierGoProxied(proxied, seed[:16]) || tierGoProxied(proxied, make([]byte, 16)) {
		t.Fatal("proxied read gate differs")
	}
	proxied.Proxied.Code = -2
	if tierGoProxied(proxied, seed[:16]) {
		t.Fatal("failed proxied read accepted")
	}
	names, chunks := tierGoFingerprints(tierGoDedup[0].seed)
	if len(names) != 3 || len(chunks[2]) != 8 || names[0] == names[1] {
		t.Fatal("fixed 16-byte chunking differs", names)
	}
	// sha256("go-dedup-chunk-0"), as get_fpoid_from_chunk names it.
	sum := sha256.Sum256([]byte("go-dedup-chunk-0"))
	if names[0] != hex.EncodeToString(sum[:]) || len(names[0]) != 64 {
		t.Fatal("fingerprint name differs", names[0])
	}
	all := map[string]bool{}
	for _, twin := range tierGoDedup {
		if len(twin.seed) != 40 {
			t.Fatal("dedup seed is not three fixed chunks", twin.name)
		}
		n, _ := tierGoFingerprints(twin.seed)
		for _, name := range n {
			if all[name] {
				t.Fatal("dedup chunks are shared between twins", name)
			}
			all[name] = true
		}
	}
	for _, set := range [][2]tierGoTwin{tierGoChunk} {
		for _, twin := range set {
			if len(twin.seed) < 17 || len(twin.data) < 16 || bytes.Equal(twin.seed[:16], twin.data[:16]) {
				t.Fatal("chunk target bytes must differ from the source range they replace", twin.name)
			}
		}
	}
}

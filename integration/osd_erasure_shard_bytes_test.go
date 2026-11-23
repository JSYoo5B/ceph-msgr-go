package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Shard-local bytes of one object of an optimized isa pool. The fixture
// copies the store of every OSD of the object's up vector, one paused
// container at a time, and reads the object's bytes in that shard's own
// collection of the private copy with the pinned tool's `--op list` and
// `get-bytes` (integration/fixture/shard_bytes.go); the owned-pool oracle
// brackets the capture with two native placement records and judges it
// (tools/osd_pool_mode_shardbytes_oracle.py).
//
// The expected content of each shard is computed twice, independently of
// any Ceph code: by the oracle and here. The pinned isa plugin without a
// chunk mapping holds raw chunk i on shard i. A stripe of stripe_width bytes
// is k data chunks; the parity chunks are ISA-L's Vandermonde rows of
// gf_gen_rs_matrix in GF(2^8) with the polynomial 0x11d, the first of them
// the XOR, which m=1 uses directly (src/erasure-code/isa/ErasureCodeIsa.cc
// :288-300, :630-650; ISA-L erasure_code/ec_base.c:73-90). A shard holds its
// chunk of each stripe in order. Only objects of whole stripes are written.
//
// What the bytes establish: the content a shard's own store held for the
// object's head at capture time, under a bracketed placement, with the
// shard identity taken from the up position, the collection and the listed
// shard_id together, never from a request's wire numbering. They do not
// establish which chunks a primary read or decoded for any logical read.
// These consumer helpers are test-only and nothing is exported.

// gfMul multiplies in GF(2^8) with the polynomial 0x11d.
func gfMul(a, b byte) byte {
	var product byte
	for b != 0 {
		if b&1 != 0 {
			product ^= a
		}
		carry := a&0x80 != 0
		a <<= 1
		if carry {
			a ^= 0x1d
		}
		b >>= 1
	}
	return product
}

// erasureParityRows are the parity rows of ISA-L gf_gen_rs_matrix(a, k+m, k):
// row r holds (2^r)^j for data chunk j.
func erasureParityRows(k, m int) [][]byte {
	rows, gen := make([][]byte, 0, m), byte(1)
	for range m {
		row, p := make([]byte, k), byte(1)
		for j := range k {
			row[j] = p
			p = gfMul(p, gen)
		}
		rows = append(rows, row)
		gen = gfMul(gen, 2)
	}
	return rows
}

// erasureShardContents is what each of the k+m shards holds for an object
// of whole stripes: its chunk of every stripe, in order.
func erasureShardContents(data []byte, k, m, stripeWidth int) ([][]byte, error) {
	if k < 1 || m < 1 || stripeWidth <= 0 || stripeWidth%k != 0 || len(data) == 0 || len(data)%stripeWidth != 0 {
		return nil, fmt.Errorf("%d bytes are not whole stripes of %d for k=%d m=%d", len(data), stripeWidth, k, m)
	}
	chunk, rows := stripeWidth/k, erasureParityRows(k, m)
	shards := make([][]byte, k+m)
	for start := 0; start < len(data); start += stripeWidth {
		for j := range k {
			shards[j] = append(shards[j], data[start+j*chunk:start+(j+1)*chunk]...)
		}
		for r, row := range rows {
			parity := make([]byte, chunk)
			for j := range k {
				for x, b := range data[start+j*chunk : start+(j+1)*chunk] {
					parity[x] ^= gfMul(row[j], b)
				}
			}
			shards[k+r] = append(shards[k+r], parity...)
		}
	}
	return shards, nil
}

// stripedContent is stripes stripes of k chunks of pseudo-random bytes
// that depend on the seed, the stripe and the chunk.
func stripedContent(seed byte, stripes, k, chunk int) []byte {
	data := make([]byte, stripes*k*chunk)
	x := uint32(seed)*2654435761 + 1
	for i := range data {
		x = x*1664525 + 1013904223
		data[i] = byte(x >> 24)
	}
	return data
}

// shardBytesShard is one position's part of the oracle's receipt.
type shardBytesShard struct {
	Index        int      `json:"index"`
	OSD          int32    `json:"osd"`
	SPG          string   `json:"spg"`
	Absent       bool     `json:"absent"`
	Whoami       string   `json:"whoami"`
	PausedMillis int64    `json:"paused_ms"`
	ArchiveBytes int64    `json:"store_archive_bytes"`
	Complete     bool     `json:"complete"`
	Failures     []string `json:"failures"`
	Length       int      `json:"length"`
	SHA256       string   `json:"sha256"`
	Raw          string   `json:"raw"`
	ShardID      *int     `json:"shard_id"`
	Matches      []string `json:"matches"`
}

// shardBytesSnapshot is the receipt over every position of the up vector,
// bracketed by the native placement before the first copy and after the
// last read.
type shardBytesSnapshot struct {
	Complete           bool                `json:"complete"`
	Failures           []string            `json:"failures"`
	Before             osdFixtureOracle    `json:"before"`
	After              osdFixtureOracle    `json:"after"`
	PGID               string              `json:"pgid"`
	Up                 []int32             `json:"up"`
	K                  int                 `json:"k"`
	M                  int                 `json:"m"`
	StripeWidth        int                 `json:"stripe_width"`
	AbsenceEstablished bool                `json:"absence_established"`
	ChunksUsedByReads  string              `json:"chunks_used_by_reads"`
	Expected           map[string][]string `json:"expected"`
	Native             struct {
		Version uint64 `json:"version"`
		Length  int    `json:"length"`
		SHA256  string `json:"sha256"`
	} `json:"native"`
	Shards []shardBytesShard `json:"shards"`
}

// captureShardBytes brackets and judges one shard-local byte capture of an
// object of the owned pool in slot 1. references are logical contents,
// natively read earlier, that a shard may still hold; "native" is the
// object as the oracle reads it in the second bracket.
func captureShardBytes(t *testing.T, ctx context.Context, object string, route osdFixtureOracle, references map[string][]byte) shardBytesSnapshot {
	t.Helper()
	first := nativePool(t, ctx, "shardbytes", map[string]any{"phase": "before", "slot": 1, "object": object}).Placement
	if !samePlacement(first, route) || first.Epoch < route.Epoch {
		t.Fatalf("placement moved before the shard-bytes capture: %+v vs %+v", first, route)
	}
	if err := testcluster.RequestControl(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), "capture-osd-shard-bytes", "osd-shard-bytes"); err != nil {
		t.Fatal("shard-bytes capture:", err)
	}
	var refs []map[string]string
	for _, label := range slices.Sorted(maps.Keys(references)) {
		refs = append(refs, map[string]string{"label": label, "data": base64.StdEncoding.EncodeToString(references[label])})
	}
	var result struct {
		ShardBytes shardBytesSnapshot `json:"shard_bytes"`
	}
	nativePoolResult(t, ctx, "shardbytes", map[string]any{"phase": "after", "slot": 1, "object": object, "references": refs}, &result)
	s := result.ShardBytes
	for _, shard := range s.Shards {
		listed := "none"
		if shard.ShardID != nil {
			listed = strconv.Itoa(*shard.ShardID)
		}
		t.Logf("shard copy %s of osd.%d (absent %t, whoami %q, paused %d ms, %d archive bytes): complete=%t %q, %d bytes sha256 %s, listed shard %s, matches %v",
			shard.SPG, shard.OSD, shard.Absent, shard.Whoami, shard.PausedMillis, shard.ArchiveBytes, shard.Complete, shard.Failures, shard.Length, shard.SHA256, listed, shard.Matches)
	}
	t.Logf("shard-bytes brackets: epoch %d pg %s up %v acting %v primary osd.%d shard %d pg_temp %v; k=%d m=%d stripe_width %d; native object %d bytes at version %d",
		s.Before.Epoch, s.PGID, s.Before.Up, s.Before.Acting, s.Before.PrimaryID, s.Before.Shard, s.Before.PGTemp, s.K, s.M, s.StripeWidth, s.Native.Length, s.Native.Version)
	return s
}

// checkShardBytes requires a complete receipt whose brackets are identical
// and are route's placement, with one entry for every up position: an
// absent one exactly where route's up vector is empty, and otherwise the
// store copy of that up member, listed as that shard, whose bytes are the
// content want names for that position. contents maps each label to its
// logical bytes; the expected chunks are computed here, independently of
// the oracle's computation, and the oracle must report the same match.
func checkShardBytes(s shardBytesSnapshot, route osdFixtureOracle, width erasureWidth, stripeWidth int, contents map[string][]byte, want []string) error {
	if !s.Complete || len(s.Failures) != 0 || s.AbsenceEstablished || s.ChunksUsedByReads != "not established" {
		return fmt.Errorf("shard-bytes receipt is incomplete or overclaims: %q", s.Failures)
	}
	if !reflect.DeepEqual(s.Before, s.After) {
		return fmt.Errorf("placement changed between the brackets: before %+v after %+v", s.Before, s.After)
	}
	if !samePlacement(s.Before, route) || s.Before.Epoch < route.Epoch || s.PGID != route.PGID || !slices.Equal(s.Up, route.Up) {
		return fmt.Errorf("brackets %+v differ from the placement %+v", s.Before, route)
	}
	if s.K != width.k || s.M != width.m || s.StripeWidth != stripeWidth || len(s.Shards) != len(route.Up) || len(want) != len(route.Up) {
		return fmt.Errorf("receipt is k=%d m=%d stripe_width %d with %d shards; want %s, %d, %d positions", s.K, s.M, s.StripeWidth, len(s.Shards), width, stripeWidth, len(route.Up))
	}
	expected := map[string][][]byte{}
	for label, data := range contents {
		shards, err := erasureShardContents(data, width.k, width.m, stripeWidth)
		if err != nil {
			return err
		}
		expected[label] = shards
	}
	for i, shard := range s.Shards {
		if shard.Index != i || shard.SPG != route.PGID+"s"+strconv.Itoa(i) || shard.OSD != route.Up[i] {
			return fmt.Errorf("position %d is attributed to index %d, %s, osd.%d; the up vector names osd.%d", i, shard.Index, shard.SPG, shard.OSD, route.Up[i])
		}
		if want[i] == "" {
			if route.Up[i] != crushItemNone || !shard.Absent || !shard.Complete || shard.Raw != "" {
				return fmt.Errorf("position %d: up member %d, absent %t, %d bytes; want an empty position", i, route.Up[i], shard.Absent, shard.Length)
			}
			continue
		}
		if shard.Absent || !shard.Complete || shard.ShardID == nil || *shard.ShardID != i || shard.Whoami != strconv.Itoa(int(route.Up[i])) {
			return fmt.Errorf("position %d is not an attributed copy of shard %d of osd.%d: %+v", i, i, route.Up[i], shard)
		}
		raw, err := base64.StdEncoding.DecodeString(shard.Raw)
		digest := sha256.Sum256(raw)
		if err != nil || len(raw) != shard.Length || hex.EncodeToString(digest[:]) != shard.SHA256 {
			return fmt.Errorf("position %d raw bytes differ from their receipt: %v, %d of %d bytes", i, err, len(raw), shard.Length)
		}
		chunks, ok := expected[want[i]]
		if !ok {
			return fmt.Errorf("position %d wants the unknown content %q", i, want[i])
		}
		if !bytes.Equal(raw, chunks[i]) {
			return fmt.Errorf("shard %d (osd.%d) holds %d bytes sha256 %s, not shard %d of %q (%d bytes); the oracle matched %v",
				i, route.Up[i], len(raw), shard.SHA256, i, want[i], len(chunks[i]), shard.Matches)
		}
		// The oracle's own computation must agree on every label.
		for label, shards := range expected {
			if slices.Contains(shard.Matches, label) != bytes.Equal(raw, shards[i]) {
				return fmt.Errorf("shard %d: the oracle's matches %v disagree with this computation on %q", i, shard.Matches, label)
			}
		}
	}
	return nil
}

// stripeWidth is the pool's native stripe_width.
func (r *ecEpochRun) stripeWidth() int {
	r.t.Helper()
	if r.facts.chunk <= 0 {
		r.t.Fatal("the pool has no native chunk size")
	}
	return r.facts.chunk * r.width.k
}

// shardBytes captures and checks the object's shard bytes on route (checkShardBytes).
func (r *ecEpochRun) shardBytes(label, object string, route osdFixtureOracle, contents map[string][]byte, want []string) shardBytesSnapshot {
	r.t.Helper()
	references := map[string][]byte{}
	for name, data := range contents {
		if name != "native" {
			references[name] = data
		}
	}
	s := captureShardBytes(r.t, r.ctx, object, route, references)
	if err := checkShardBytes(s, route, r.width, r.stripeWidth(), contents, want); err != nil {
		r.t.Fatalf("shard bytes %s: %v", label, err)
	}
	r.t.Logf("shard bytes %s: %v", label, want)
	return s
}

// readContent sends one Go READ of the whole object exactly once through
// route's primary and requires data at the user version.
func (r *ecEpochRun) readContent(label string, route osdFixtureOracle, data []byte, version uint64) {
	r.t.Helper()
	o := mutationConnection(r.t, r.c, r.ctx, route)
	defer o.Close()
	reply, err := o.Request(r.ctx, r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(data))}))
	if err != nil || reply.Code != 0 || len(reply.Operations) != 1 || reply.Operations[0].Result != 0 || !bytes.Equal(reply.Operations[0].Data, data) || reply.UserVersion != version {
		r.t.Fatalf("%s: err=%v code=%d version %d (want %d)", label, err, reply.Code, reply.UserVersion, version)
	}
}

// requireContent reads the object natively in route's interval and requires
// data at the user version.
func (r *ecEpochRun) requireContent(label, object string, route osdFixtureOracle, data []byte, version uint64) nativeXattrSnapshot {
	r.t.Helper()
	n := r.observeIn(label, object, route)
	if n.Code != 0 || !bytes.Equal(n.Data, data) || n.Version != version {
		r.t.Fatalf("%s: native code %d, %d bytes at version %d; want %d bytes at version %d", label, n.Code, len(n.Data), n.Version, len(data), version)
	}
	return n
}

// distinctChunks requires every shard of a and of b to differ, so that a
// shard's bytes tell the two contents apart.
func distinctChunks(a, b []byte, width erasureWidth, stripeWidth int) error {
	x, err := erasureShardContents(a, width.k, width.m, stripeWidth)
	if err != nil {
		return err
	}
	y, err := erasureShardContents(b, width.k, width.m, stripeWidth)
	if err != nil {
		return err
	}
	for i := range x {
		if bytes.Equal(x[i], y[i]) {
			return fmt.Errorf("shard %d is the same for both contents", i)
		}
	}
	return nil
}

// positions names one content for every up position, except the empty ones.
func positions(size int, label string, overrides map[int]string) []string {
	want := slices.Repeat([]string{label}, size)
	for i, l := range overrides {
		want[i] = l
	}
	return want
}

// erasureShardBytesRecoveryScenario: one Go WRITEFULL of two stripes, with
// different bytes in every chunk, to a fresh head of an owned optimized isa
// pool, then two outages, each with one Go WRITEFULL of new content while
// the OSD is away. Every shard's own bytes are captured and compared with
// the expected chunks five times:
//
//  1. Clean: every shard holds its chunk of the first content.
//  2. The OSD of the last nonprimary data shard is killed and the PG is
//     active without it under the unchanged primary. After the second
//     content is written through that primary, every present shard holds its
//     chunk of it and the position is empty.
//  3. The OSD returns and every PG is clean again in a newer interval. The
//     returned shard, absent during the write, now holds its chunk of the
//     second content: recovered bytes, which differ from what it held
//     before the outage.
//  4. The primary's OSD is killed and the PG is active under the first parity
//     shard with a primary-first pg_temp. The third content is written
//     through it; the request carries the replacement's actual shard with
//     primary-first numbering, and the capture names shards by their actual
//     positions. Every present shard holds its chunk of the third content.
//  5. The primary returns and leads again in a clean PG: its shard 0 holds
//     its chunk of the third content, recovered.
//
// Each write is answered ONDISK once and read back natively and through the
// Go client in its interval, which must hold around the write. Not
// established: which chunks any read used, partial stripes, appends, other
// plugins or techniques, chunk mappings, and a capture while a shard is being
// recovered.
func erasureShardBytesRecoveryScenario(t *testing.T, width erasureWidth) {
	if os.Getenv("CEPH_MSGR_NATIVE_SHARD_HISTORY") != "1" {
		t.Skip("requires the fixture-selected exclusive shard-history capture marker")
	}
	r := startECEpochRun(t, width, 1500*time.Second)
	const object = "ec-shard-bytes"
	stripe, size := r.stripeWidth(), r.facts.size
	contents := [3][]byte{}
	for i := range contents {
		contents[i] = stripedContent(byte(i+1), 2, width.k, r.facts.chunk)
		if i > 0 {
			if err := distinctChunks(contents[i-1], contents[i], width, stripe); err != nil {
				t.Fatal(err)
			}
		}
	}
	route, since := r.healthy(object)
	first := r.write("first full write", route, contents[0], 1700013000, cephmsgr.OSDSnapContext{})
	r.heldThrough("the first full write", object, route)
	r.requireContent("native read of the first write", object, route, contents[0], first.UserVersion)
	r.shardBytes("after the first write", object, route, map[string][]byte{"native": contents[0]}, positions(size, "native", nil))

	// The last nonprimary data shard's OSD away.
	victim := r.facts.nonprimary[len(r.facts.nonprimary)-1]
	outage := r.kill(route.Acting[victim])
	absent := r.waitState("active without the data shard", object, func(o pgIntervalObservation) error { return o.activeWithout(route, []int{victim}, since) })
	away := *absent.After
	second := r.write("full write without the data shard", away, contents[1], 1700013001, cephmsgr.OSDSnapContext{})
	r.heldThrough("the write without the data shard", object, away)
	if second.UserVersion <= first.UserVersion || second.ReplayVersion.Epoch < absent.Query.SameIntervalSince {
		t.Fatalf("the write without the data shard is at %s, user version %d; the interval started at epoch %d", replayVersion(second), second.UserVersion, absent.Query.SameIntervalSince)
	}
	r.requireContent("native read without the data shard", object, away, contents[1], second.UserVersion)
	r.shardBytes("without the data shard", object, away, map[string][]byte{"native": contents[1], "first": contents[0]},
		positions(size, "native", map[int]string{victim: ""}))
	r.start(outage)
	r.clean()
	var back osdFixtureOracle
	seen := r.waitState("clean again with the data shard", object, func(o pgIntervalObservation) error {
		var err error
		back, err = o.cleanAgainOn(route, absent.Query.SameIntervalSince)
		return err
	})
	r.requireContent("native read after the data shard's recovery", object, back, contents[1], second.UserVersion)
	r.readContent("Go read after the data shard's recovery", back, contents[1], second.UserVersion)
	r.shardBytes("after the data shard's recovery", object, back, map[string][]byte{"native": contents[1], "first": contents[0]}, positions(size, "native", nil))
	t.Logf("shard %d (osd.%d) held the first content's chunk before its outage and holds the second content's chunk after its recovery", victim, route.Acting[victim])

	// The primary's OSD away.
	primaryOutage := r.kill(back.ID)
	replacement, replacedSince := r.awaitReplacement(object, back, seen.Query.SameIntervalSince)
	request := r.request(replacement, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull})
	if request.Placement == nil || request.Placement.PrimaryFirst == nil || int(request.Placement.Shard) != int(replacement.Shard) {
		t.Fatalf("the request through the replacement carries %+v; want actual shard %d with primary-first numbering", request.Placement, replacement.Shard)
	}
	t.Logf("the replacement osd.%d leads on actual shard %d; the request carries that shard with primary-first numbering %+v", replacement.ID, replacement.Shard, *request.Placement.PrimaryFirst)
	third := r.write("full write through the replacement primary", replacement, contents[2], 1700013002, cephmsgr.OSDSnapContext{})
	r.heldThrough("the write through the replacement primary", object, replacement)
	if third.UserVersion <= second.UserVersion {
		t.Fatalf("the write through the replacement has user version %d after %d", third.UserVersion, second.UserVersion)
	}
	r.requireContent("native read under the replacement", object, replacement, contents[2], third.UserVersion)
	r.shardBytes("under the replacement", object, replacement, map[string][]byte{"native": contents[2], "second": contents[1]},
		positions(size, "native", map[int]string{int(back.Shard): ""}))
	returned, _ := r.primaryBack(object, back, primaryOutage, replacedSince)
	r.requireContent("native read after the primary's recovery", object, returned, contents[2], third.UserVersion)
	r.readContent("Go read after the primary's recovery", returned, contents[2], third.UserVersion)
	r.shardBytes("after the primary's recovery", object, returned, map[string][]byte{"native": contents[2], "second": contents[1]}, positions(size, "native", nil))
	t.Logf("shard %d (osd.%d) held the second content's chunk before its outage and holds the third content's chunk after its recovery", back.Shard, back.ID)
}

// TestCephOSDErasureOptimizedK2M2ShardBytesRecoveryIntegration is erasureShardBytesRecoveryScenario on optimized isa
// k=2,m=2 (four OSDs; nonprimary shard {1}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2shardbytesrecovery key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a data-shard OSD and the primary OSD (restarts verified osd-ready clean) and pauses each up OSD for a store copy
func TestCephOSDErasureOptimizedK2M2ShardBytesRecoveryIntegration(t *testing.T) {
	erasureShardBytesRecoveryScenario(t, erasureK2M2)
}

// TestCephOSDErasureOptimizedK3M1ShardBytesRecoveryIntegration is erasureShardBytesRecoveryScenario on optimized isa
// k=3,m=1 (four OSDs; nonprimary shards {1,2}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1shardbytesrecovery key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a data-shard OSD and the primary OSD (restarts verified osd-ready clean) and pauses each up OSD for a store copy
func TestCephOSDErasureOptimizedK3M1ShardBytesRecoveryIntegration(t *testing.T) {
	erasureShardBytesRecoveryScenario(t, erasureK3M1)
}

// TestCephOSDErasureOptimizedK4M2ShardBytesRecoveryIntegration is erasureShardBytesRecoveryScenario on optimized isa
// k=4,m=2 (six OSDs; nonprimary shards {1,2,3}).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2shardbytesrecovery key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated kills a data-shard OSD and the primary OSD (restarts verified osd-ready clean) and pauses each up OSD for a store copy
func TestCephOSDErasureOptimizedK4M2ShardBytesRecoveryIntegration(t *testing.T) {
	erasureShardBytesRecoveryScenario(t, erasureK4M2)
}

func TestErasureShardContents(t *testing.T) {
	if gfMul(0x80, 2) != 0x1d || gfMul(2, 2) != 4 || gfMul(1, 0xa7) != 0xa7 || gfMul(0, 0x53) != 0 {
		t.Fatal("GF(2^8)/0x11d multiplication")
	}
	// Every nonzero element has exactly one inverse.
	for a := 1; a < 256; a++ {
		n := 0
		for b := 1; b < 256; b++ {
			if gfMul(byte(a), byte(b)) == 1 {
				n++
			}
		}
		if n != 1 {
			t.Fatal("element without one inverse", a, n)
		}
	}
	if rows := erasureParityRows(4, 2); !reflect.DeepEqual(rows, [][]byte{{1, 1, 1, 1}, {1, 2, 4, 8}}) {
		t.Fatal("Vandermonde rows", rows)
	}
	// The same vector as the oracle's test: k=2, m=2, two stripes of 4 bytes.
	shards, err := erasureShardContents([]byte{1, 2, 3, 4, 0x80, 6, 7, 0x80}, 2, 2, 4)
	if err != nil || !reflect.DeepEqual(shards, [][]byte{{1, 2, 0x80, 6}, {3, 4, 7, 0x80}, {1 ^ 3, 2 ^ 4, 0x80 ^ 7, 6 ^ 0x80}, {1 ^ 6, 2 ^ 8, 0x80 ^ 14, 6 ^ 0x1d}}) {
		t.Fatal("shard contents", shards, err)
	}
	if xor, err := erasureShardContents([]byte{0, 1, 2, 3, 4, 5}, 3, 1, 6); err != nil || !bytes.Equal(xor[3], []byte{0 ^ 2 ^ 4, 1 ^ 3 ^ 5}) {
		t.Fatal("k=3 m=1 parity", xor, err)
	}
	for _, c := range []struct{ n, k, width int }{{6, 2, 4}, {0, 2, 4}, {8, 3, 8}} {
		if _, err := erasureShardContents(make([]byte, c.n), c.k, 2, c.width); err == nil {
			t.Error("unmodelled layout accepted", c)
		}
	}
	for _, w := range []erasureWidth{erasureK2M2, erasureK3M1, erasureK4M2} {
		for seed := byte(1); seed < 3; seed++ {
			a, b := stripedContent(seed, 2, w.k, 4096), stripedContent(seed+1, 2, w.k, 4096)
			if err := distinctChunks(a, b, w, w.k*4096); err != nil {
				t.Fatal(w, seed, err)
			}
		}
	}
	a := stripedContent(1, 2, 4, 4096)
	if err := distinctChunks(a, a, erasureK4M2, 4*4096); err == nil {
		t.Fatal("identical contents accepted as distinct")
	}
}

// shardBytesReceipt builds one synthetic complete receipt for content on
// placement route; no daemon produced it.
func shardBytesReceipt(route osdFixtureOracle, data []byte, width erasureWidth, stripeWidth int) shardBytesSnapshot {
	s := shardBytesSnapshot{Complete: true, Failures: []string{}, Before: route, After: route, PGID: route.PGID, Up: route.Up, K: width.k, M: width.m,
		StripeWidth: stripeWidth, ChunksUsedByReads: "not established"}
	shards, _ := erasureShardContents(data, width.k, width.m, stripeWidth)
	for i, osd := range route.Up {
		shard := shardBytesShard{Index: i, OSD: osd, SPG: route.PGID + "s" + strconv.Itoa(i), Complete: true}
		if osd == crushItemNone {
			shard.Absent = true
		} else {
			digest := sha256.Sum256(shards[i])
			id := i
			shard.Whoami, shard.ShardID, shard.Length, shard.SHA256 = strconv.Itoa(int(osd)), &id, len(shards[i]), hex.EncodeToString(digest[:])
			shard.Raw, shard.Matches = base64.StdEncoding.EncodeToString(shards[i]), []string{"native"}
		}
		s.Shards = append(s.Shards, shard)
	}
	return s
}

func TestCheckShardBytes(t *testing.T) {
	route := osdFixtureOracle{PGID: "4.2", Up: []int32{1, 3, 2, 0}, Acting: []int32{1, 3, 2, 0}, ID: 1, PrimaryID: 1, Epoch: 30, PoolType: 3}
	data, other := stripedContent(1, 2, 2, 8), stripedContent(2, 2, 2, 8)
	contents := map[string][]byte{"native": data, "first": other}
	want := positions(4, "native", nil)
	if err := checkShardBytes(shardBytesReceipt(route, data, erasureK2M2, 16), route, erasureK2M2, 16, contents, want); err != nil {
		t.Fatal(err)
	}
	holed := route
	holed.Up, holed.Acting = []int32{1, crushItemNone, 2, 0}, []int32{1, crushItemNone, 2, 0}
	if err := checkShardBytes(shardBytesReceipt(holed, data, erasureK2M2, 16), holed, erasureK2M2, 16, contents, positions(4, "native", map[int]string{1: ""})); err != nil {
		t.Fatal("an empty position:", err)
	}
	stale := shardBytesReceipt(route, data, erasureK2M2, 16)
	staleShards, _ := erasureShardContents(other, 2, 2, 16)
	stale.Shards[1].Raw, stale.Shards[1].Matches = base64.StdEncoding.EncodeToString(staleShards[1]), []string{"first"}
	digest := sha256.Sum256(staleShards[1])
	stale.Shards[1].SHA256, stale.Shards[1].Length = hex.EncodeToString(digest[:]), len(staleShards[1])
	if err := checkShardBytes(stale, route, erasureK2M2, 16, contents, positions(4, "native", map[int]string{1: "first"})); err != nil {
		t.Fatal("a stale shard:", err)
	}
	for name, c := range map[string]struct {
		change func(*shardBytesSnapshot)
		want   []string
	}{
		"incomplete":           {func(s *shardBytesSnapshot) { s.Complete = false }, want},
		"overclaim":            {func(s *shardBytesSnapshot) { s.ChunksUsedByReads = "established" }, want},
		"moved bracket":        {func(s *shardBytesSnapshot) { s.After.Epoch = 31 }, want},
		"another placement":    {func(s *shardBytesSnapshot) { s.Before.Up = []int32{1, 3, 0, 2}; s.After = s.Before }, want},
		"another width":        {func(s *shardBytesSnapshot) { s.K = 3 }, want},
		"another OSD's copy":   {func(s *shardBytesSnapshot) { s.Shards[2].Whoami = "3" }, want},
		"listed as another":    {func(s *shardBytesSnapshot) { id := 0; s.Shards[2].ShardID = &id }, want},
		"swapped positions":    {func(s *shardBytesSnapshot) { s.Shards[2], s.Shards[3] = s.Shards[3], s.Shards[2] }, want},
		"stale bytes":          {func(s *shardBytesSnapshot) { *s = stale }, want},
		"a disagreeing oracle": {func(s *shardBytesSnapshot) { s.Shards[0].Matches = []string{"native", "first"} }, want},
		"an absent wanted":     {func(s *shardBytesSnapshot) {}, positions(4, "native", map[int]string{1: ""})},
		"damaged raw":          {func(s *shardBytesSnapshot) { s.Shards[0].Raw = s.Shards[1].Raw }, want},
	} {
		s := shardBytesReceipt(route, data, erasureK2M2, 16)
		c.change(&s)
		if err := checkShardBytes(s, route, erasureK2M2, 16, contents, c.want); err == nil {
			t.Error("accepted", name)
		}
	}
	if err := checkShardBytes(shardBytesReceipt(route, data, erasureK2M2, 16), route, erasureK2M2, 16, contents, positions(4, "second", nil)); err == nil {
		t.Error("an unknown content accepted")
	}
}

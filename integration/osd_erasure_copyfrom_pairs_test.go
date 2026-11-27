package integration_test

import (
	"fmt"
	"testing"
)

// erasureCopyPair is one pair of owned optimized isa widths for EC-to-EC
// COPY_FROM: the source pool in slot 1, the destination pool in slot 2 and
// the OSD fixture topology that hosts both.
type erasureCopyPair struct {
	source, dest erasureWidth
	topology     string
}

func (p erasureCopyPair) String() string { return fmt.Sprintf("%s onto %s", p.source, p.dest) }

var (
	// erasurePairK2M2ToK3M1 copies from a stripe of two data chunks onto
	// one of three, whose rounded copy-get max (36864) is no multiple of
	// the source stripe.
	erasurePairK2M2ToK3M1 = erasureCopyPair{source: erasureK2M2, dest: erasureK3M1, topology: "4"}
	// erasurePairK3M1ToK2M2 is the reverse: every copy-get is the 32 KiB
	// limit, a multiple of the destination stripe, and the second chunk
	// starts inside a stripe of the source (12288 bytes).
	erasurePairK3M1ToK2M2 = erasureCopyPair{source: erasureK3M1, dest: erasureK2M2, topology: "4"}
	// erasurePairK4M2ToK3M1 puts a source of six shards on every OSD of the
	// six-OSD topology and a destination of four shards on four of them.
	erasurePairK4M2ToK3M1 = erasureCopyPair{source: erasureK4M2, dest: erasureK3M1, topology: "6"}
)

// The baseline cells of TestCephOSDErasureOptimizedCopyFromErasureToErasureIntegration
// (osd_erasure_copyfrom_snapshots_test.go) from an owned optimized isa
// k=3,m=1 source onto an owned optimized isa k=2,m=2 destination on the
// four-OSD topology. The destination stripe (8192 bytes) leaves the lowered
// 32 KiB limit as it is, so every copy-get asserts a max of 32768, and the
// second copy-get starts at 32768, inside the source's third stripe of 12288
// bytes: the source primary reads that extent from its three data shards
// (do_copy_get on an erasure-coded source, src/osd/PrimaryLogPG.cc:9318-9320
// and 9364-9378), whatever the alignment. Head data, a head written between
// the chunks (-ERANGE) and a snapshot with its head written between them,
// each unversioned and pinned, for both opcodes.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1tok2m2copyfromerasuretoerasure key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates owned snapshots
func TestCephOSDErasureOptimizedK3M1ToK2M2CopyFromErasureToErasureIntegration(t *testing.T) {
	runCopyErasureToErasure(t, erasurePairK3M1ToK2M2, "osd-erasure-copyfrom-k3m1-to-k2m2-results.json")
}

// The loss cells of TestCephOSDErasureOptimizedCopyFromErasureToErasureLossIntegration
// (osd_erasure_copyfrom_pair_loss_test.go) on the k=3,m=1 to k=2,m=2 pair:
// destination primary, shared non-primary shard and source primary lost
// between two chunks of a head copy (the source primary also pinned), and
// the destination primary lost while the copy's final transaction is held
// behind the destination PG's EC write queue (also pinned), and a
// destination replica lost there. Four OSDs hold both PGs. The
// source's min_size is three of four shards and the destination's three of
// four, so every single loss leaves both PGs active. The held final
// transaction needs a source whose three data shards avoid the frozen
// destination shard, which leaves its parity shard there; when none of the
// source pool's eight PGs has that shape for a destination PG, the
// destination is renamed onto another PG (heldCommitPlacement).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk3m1tok2m2copyfromerasuretoerasureloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width
//scenario:covers CLIENT to OSD
//fixture:isolated freezes and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, restores owned pools and profiles
func TestCephOSDErasureOptimizedK3M1ToK2M2CopyFromErasureToErasureLossIntegration(t *testing.T) {
	runCopyErasurePairLoss(t, erasurePairK3M1ToK2M2, "osd-erasure-copyfrom-k3m1-to-k2m2-loss-results.json", copyErasurePairHeadLossCells(), copyErasurePairLossHooks{})
}

// The baseline cells on the six-OSD topology from an owned optimized isa
// k=4,m=2 source, which holds a shard on every OSD, onto an owned optimized
// isa k=3,m=1 destination on four of them. Every copy-get asserts the
// destination's rounded max of 36864 bytes, which spans more than two
// stripes of the source (16384 bytes).
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2tok3m1copyfromerasuretoerasure key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated pauses OSDs, sets pause, lowers a copy chunk at runtime, allocates owned snapshots
func TestCephOSDErasureOptimizedK4M2ToK3M1CopyFromErasureToErasureIntegration(t *testing.T) {
	runCopyErasureToErasure(t, erasurePairK4M2ToK3M1, "osd-erasure-copyfrom-k4m2-to-k3m1-results.json")
}

// The loss cells on the k=4,m=2 to k=3,m=1 pair. The source's min_size is
// five of six shards and the destination's three of four. A lost source
// primary must also hold a destination shard, so the source is named until
// its primary is one of the destination PG's four OSDs. The held final
// transaction needs the frozen destination shard on one of the source's two
// parity shards; with the destination primary, the frozen OSD and the
// source primary excluded, a destination replica is left to lose in it.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-6.osd-profile-replicated-pg-v8.t-osderasureoptimizedk4m2tok3m1copyfromerasuretoerasureloss key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=6 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width-six
//scenario:covers CLIENT to OSD
//fixture:isolated freezes and SIGKILLs OSDs, sets pause, lowers a copy chunk at runtime, restores owned pools and profiles
func TestCephOSDErasureOptimizedK4M2ToK3M1CopyFromErasureToErasureLossIntegration(t *testing.T) {
	runCopyErasurePairLoss(t, erasurePairK4M2ToK3M1, "osd-erasure-copyfrom-k4m2-to-k3m1-loss-results.json", copyErasurePairHeadLossCells(), copyErasurePairLossHooks{})
}

func TestErasureCopyPairs(t *testing.T) {
	for pair, want := range map[erasureCopyPair]struct {
		topology string
		max      int
	}{
		erasurePairK2M2ToK3M1: {"4", 36864},
		erasurePairK3M1ToK2M2: {"4", 32768},
		erasurePairK4M2ToK3M1: {"6", 36864},
	} {
		// Both widths fit the topology that hosts the pair.
		if pair.topology != want.topology || pair.source.size() > 6 || pair.topology == "4" && (pair.source.size() > 4 || pair.dest.size() > 4) {
			t.Errorf("%s: topology %q", pair, pair.topology)
		}
		if got := erasureCopyGetMax(erasureFacts{chunk: 4096}, pair.dest); got != want.max {
			t.Errorf("%s: copy-get max %d, want %d", pair, got, want.max)
		}
	}
	if erasurePairK2M2ToK3M1.String() != "k=2,m=2 onto k=3,m=1" {
		t.Fatal(erasurePairK2M2ToK3M1.String())
	}
}

func TestHeldCommitSource(t *testing.T) {
	src := osdFixtureOracle{ID: 0, PrimaryID: 0, Acting: []int32{0, 1, 2, 3}}
	for _, c := range []struct {
		name            string
		route           osdFixtureOracle
		primary, frozen int32
		k               int
		accept          bool
	}{
		{"replicated source", osdFixtureOracle{ID: 0, PrimaryID: 0, Acting: []int32{0, 1, 2}}, 1, 2, 0, true},
		{"replicated source on the destination primary", osdFixtureOracle{ID: 1, PrimaryID: 1, Acting: []int32{1, 0, 2}}, 1, 2, 0, false},
		{"replicated source on the frozen OSD", osdFixtureOracle{ID: 2, PrimaryID: 2, Acting: []int32{2, 1, 0}}, 1, 2, 0, false},
		{"frozen parity shard of k=2", src, 1, 3, 2, true},
		{"frozen data shard of k=2", src, 3, 1, 2, false},
		{"frozen parity shard of k=3", src, 1, 3, 3, true},
		{"frozen data shard of k=3", src, 1, 2, 3, false},
		{"not the primary", osdFixtureOracle{ID: 0, PrimaryID: 1, Acting: []int32{0, 1, 2, 3}}, 2, 3, 2, false},
		{"acting set without parity", osdFixtureOracle{ID: 0, PrimaryID: 0, Acting: []int32{0, 1}}, 2, 3, 2, false},
	} {
		if got := heldCommitSource(c.route, c.primary, c.frozen, c.k); got != c.accept {
			t.Errorf("%s: accepted %t", c.name, got)
		}
	}
}

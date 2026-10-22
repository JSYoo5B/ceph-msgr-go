package cephmsgr

import (
	"errors"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/osd"
)

func TestOSDErasurePlacementSendsTheShard(t *testing.T) {
	_, o, peer, ctx, _ := replyV8ProfileOpen(t)
	send := func(p OSDPlacement) byte {
		t.Helper()
		input := backoffTransportRequest()
		input.Placement = &p
		input.Operations = []OSDOperation{{Code: OSDStat}}
		go o.Request(ctx, input)
		m := peer.next(t)
		// spg_t opens the v8 front: 6-byte struct header, pg_t (17), then shard.
		return m.Front[23]
	}
	base := *backoffTransportRequest().Placement
	base.Primary = o.target.ID
	if got := send(base); got != 0xff {
		t.Fatal("replicated placement did not send NO_SHARD", got)
	}
	erasure := base
	erasure.Erasure, erasure.Shard = true, 2
	if got := send(erasure); got != 2 {
		t.Fatal("erasure placement did not send its shard", got)
	}
	// k=2,m=1 optimized with pg_temp: actual shard 2 travels as 1, which the
	// OSD maps back to 2 for a client without SERVER_TENTACLE.
	erasure.PrimaryFirst = &OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 1}
	if got := send(erasure); got != 1 {
		t.Fatal("primary-first placement did not send the pre-Tentacle shard", got)
	}
	// Submissions keep the actual shard and their own copy of the facts.
	if erasure.Shard != 2 {
		t.Fatal("caller placement was modified")
	}
}

// The OSDMap::pgtemp_primaryfirst example of a 4+2 optimized pool (nonprimary
// shards 1-3, pg_temp {NONE, 9, 10, 6, 7, 8}) and the k=2,m=1 fixture pool;
// every mapping is a permutation that keeps shard 0.
func TestOSDPrimaryFirstShard(t *testing.T) {
	for _, tc := range []struct {
		f    OSDPrimaryFirst
		want []uint8
	}{
		{OSDPrimaryFirst{PoolSize: 6, NonprimaryShards: 3}, []uint8{0, 3, 4, 5, 1, 2}},
		{OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 1}, []uint8{0, 2, 1}},
		{OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 0}, []uint8{0, 1, 2}},
	} {
		for shard, want := range tc.want {
			if got := primaryFirstShard(uint8(shard), &tc.f); got != want {
				t.Fatalf("%+v shard %d: got %d want %d", tc.f, shard, got, want)
			}
		}
	}
	if primaryFirstShard(5, nil) != 5 {
		t.Fatal("no primary-first facts changed the shard")
	}
}

// Every locally accepted numeric domain of primaryFirstShard (PoolSize
// 2..128, NonprimaryShards 0..PoolSize-1, every actual shard) against an
// independent reference: the wire order [0, eligible suffix, nonprimary
// prefix] of OSDMap::pgtemp_primaryfirst with contiguous nonprimary shards
// 1..NonprimaryShards, where an actual shard's wire shard is its position.
// It does not reuse the production arithmetic. Synthetic facts only: this is
// not a claim that each combination is a real EC configuration.
func TestOSDPrimaryFirstNumericDomain(t *testing.T) {
	pairs, mappings := 0, 0
	for size := 2; size <= 128; size++ {
		for nonprimary := 0; nonprimary < size; nonprimary++ {
			order := []int{0}
			for shard := nonprimary + 1; shard < size; shard++ {
				order = append(order, shard)
			}
			for shard := 1; shard <= nonprimary; shard++ {
				order = append(order, shard)
			}
			facts := &OSDPrimaryFirst{PoolSize: uint8(size), NonprimaryShards: uint8(nonprimary)}
			seen := make([]bool, size)
			for position, actual := range order {
				if !validShardPlacement(true, uint8(actual), facts) {
					t.Fatalf("%+v: in-range shard %d refused", facts, actual)
				}
				wire := primaryFirstShard(uint8(actual), facts)
				if int(wire) != position || wire > 127 || int8(wire) < 0 || seen[wire] {
					t.Fatalf("%+v: actual shard %d -> wire %d, want position %d", facts, actual, wire, position)
				}
				seen[wire] = true
				if order[wire] != actual {
					t.Fatalf("%+v: wire %d does not invert to actual %d", facts, wire, actual)
				}
				mappings++
			}
			if primaryFirstShard(0, facts) != 0 {
				t.Fatalf("%+v moved shard 0", facts)
			}
			if validShardPlacement(true, uint8(size), facts) {
				t.Fatalf("%+v: out-of-pool shard %d accepted", facts, size)
			}
			// The caller's placement keeps its actual shard (no in-place edit).
			placement := OSDPlacement{Erasure: true, Shard: uint8(size - 1), PrimaryFirst: facts}
			wire := placement.wireShard()
			if placement.Shard != uint8(size-1) || int(wire) != slices.Index(order, size-1) {
				t.Fatal("wire numbering modified the actual shard or differs from the reference", wire)
			}
			pairs++
		}
		if validShardPlacement(true, 0, &OSDPrimaryFirst{PoolSize: uint8(size), NonprimaryShards: uint8(size)}) {
			t.Fatalf("nonprimary count equal to pool size %d accepted", size)
		}
	}
	for _, size := range []int{0, 1, 129, 255} {
		if validShardPlacement(true, 0, &OSDPrimaryFirst{PoolSize: uint8(size)}) {
			t.Fatalf("pool size %d outside 2..128 accepted", size)
		}
	}
	if pairs != 8255 || mappings != 707263 {
		t.Fatalf("enumerated %d fact pairs and %d mappings", pairs, mappings)
	}
}

func TestOSDErasurePlacementRefusesMeaninglessShards(t *testing.T) {
	_, o, _, ctx, _ := replyV8ProfileOpen(t)
	for name, edit := range map[string]func(*OSDPlacement){
		"shard without erasure": func(p *OSDPlacement) { p.Shard = 1 },
		"shard above int8":      func(p *OSDPlacement) { p.Erasure, p.Shard = true, 128 },
		"primary-first without erasure": func(p *OSDPlacement) {
			p.PrimaryFirst = &OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 1}
		},
		"shard beyond the pool": func(p *OSDPlacement) {
			p.Erasure, p.Shard, p.PrimaryFirst = true, 3, &OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 1}
		},
		"all shards nonprimary": func(p *OSDPlacement) {
			p.Erasure, p.PrimaryFirst = true, &OSDPrimaryFirst{PoolSize: 3, NonprimaryShards: 3}
		},
	} {
		input := backoffTransportRequest()
		input.Placement.Primary = o.target.ID
		edit(input.Placement)
		input.Operations = []OSDOperation{{Code: OSDStat}}
		if _, err := o.Request(ctx, input); !errors.Is(err, osd.ErrRequest) {
			t.Fatal("meaningless shard accepted", name, err)
		}
	}
}

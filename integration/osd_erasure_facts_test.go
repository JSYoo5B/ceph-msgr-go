package integration_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type factsPool = map[string]any

type factsEdit func(factsPool, map[string]string, *poolReceipt, *[]map[string]any)

// buildErasureFacts returns a synthetic k=2,m=2 osd dump, profile and oracle
// receipt in the pinned MON's JSON shapes.
func buildErasureFacts(edit factsEdit) ([]byte, []byte, poolReceipt) {
	p := factsPool{"pool": 7, "type": 3, "size": 4, "min_size": 3, "erasure_code_profile": "ceph-msgr-owned-ec-k2m2",
		"flags_names": "hashpspool,ec_overwrites", "crush_rule": 2, "nonprimary_shards": "{}", "stripe_width": 8192}
	profile := map[string]string{"k": "2", "m": "2", "plugin": "isa", "crush-failure-domain": "osd", "technique": "reed_sol_van"}
	yes := true
	receipt := poolReceipt{PoolID: 7, Size: 4, MinSize: 3, ErasureCodeProfile: "ceph-msgr-owned-ec-k2m2", ECOverwrites: &yes,
		ECProfile: map[string]string{"k": "2", "m": "2", "plugin": "isa", "crush-failure-domain": "osd"},
		CrushRule: json.RawMessage(`{"rule_id":2,"type":3,"steps":[{"op":"take","item_name":"default"},{"op":"choose_indep","num":0,"type":"osd"},{"op":"emit"}]}`)}
	osds := []map[string]any{{"osd": 0, "up": 1, "in": 1}, {"osd": 1, "up": 1, "in": 1}, {"osd": 2, "up": 1, "in": 1}, {"osd": 3, "up": 1, "in": 1}}
	if edit != nil {
		edit(p, profile, &receipt, &osds)
	}
	dump, _ := json.Marshal(map[string]any{"epoch": 51, "pools": []factsPool{{"pool": 1, "type": 1}, p}, "osds": osds})
	codes, _ := json.Marshal(profile)
	return dump, codes, receipt
}

// Synthetic pool/map facts: the derivation must follow the native values and
// reject a k=2,m=1 assumption on a k=2,m=2 pool. This is not native evidence.
func TestErasureFactsFollowTheNativeWidth(t *testing.T) {
	dump, codes, receipt := buildErasureFacts(nil)
	facts, err := erasureFactsFrom(dump, codes, receipt, erasureK2M2, false)
	if err != nil {
		t.Fatal(err)
	}
	if facts.size != 4 || facts.minSize != 3 || len(facts.nonprimary) != 0 || !slices.Equal(facts.owned, []int32{0, 1, 2, 3}) || facts.epoch != 51 || facts.chunk != 4096 {
		t.Fatalf("facts %+v", facts)
	}
	if erasureK2M1.size() != 3 || erasureK2M1.minSize() != 2 || erasureK2M2.size() != 4 || erasureK2M2.minSize() != 3 {
		t.Fatal("width arithmetic")
	}
	for name, edit := range map[string]factsEdit{
		"k=2,m=1 size": func(p factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) { p["size"], r.Size = 3, 3 },
		"lowered min_size": func(p factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) {
			p["min_size"], r.MinSize = 2, 2
		},
		"oracle min_size": func(_ factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) { r.MinSize = 4 },
		"replicated pool": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) { p["type"] = 1 },
		"shared profile": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) {
			p["erasure_code_profile"] = "ceph-msgr-owned-ec"
		},
		"no overwrites flag": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) {
			p["flags_names"] = "hashpspool"
		},
		"no overwrites read": func(_ factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) { r.ECOverwrites = nil },
		"profile m": func(_ factsPool, c map[string]string, r *poolReceipt, _ *[]map[string]any) {
			c["m"], r.ECProfile["m"] = "1", "1"
		},
		"plugin disagrees": func(_ factsPool, c map[string]string, _ *poolReceipt, _ *[]map[string]any) { c["plugin"] = "jerasure" },
		"host domain": func(_ factsPool, c map[string]string, r *poolReceipt, _ *[]map[string]any) {
			c["crush-failure-domain"], r.ECProfile["crush-failure-domain"] = "host", "host"
		},
		"other rule": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) { p["crush_rule"] = 0 },
		"replicated rule": func(_ factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) {
			r.CrushRule = json.RawMessage(`{"rule_id":2,"type":1,"steps":[{"op":"choose_firstn","num":0,"type":"osd"}]}`)
		},
		"unexpected nonprimary": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) {
			p["nonprimary_shards"] = "{1}"
		},
		"three up OSDs":    func(_ factsPool, _ map[string]string, _ *poolReceipt, o *[]map[string]any) { (*o)[3]["up"] = 0 },
		"missing pool":     func(_ factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) { r.PoolID = 9 },
		"no stripe width":  func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) { delete(p, "stripe_width") },
		"odd stripe width": func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) { p["stripe_width"] = 4097 },
	} {
		dump, codes, receipt := buildErasureFacts(edit)
		if _, err := erasureFactsFrom(dump, codes, receipt, erasureK2M2, false); err == nil {
			t.Fatal("facts accepted:", name)
		}
	}
	// An optimized pool names nonprimary shards inside the pool, never shard 0.
	dump, codes, receipt = buildErasureFacts(func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) {
		p["nonprimary_shards"] = "{1}"
	})
	if f, err := erasureFactsFrom(dump, codes, receipt, erasureK2M2, true); err != nil || !slices.Equal(f.nonprimary, []int{1}) {
		t.Fatal("optimized facts", f, err)
	}
	if !slices.Equal(erasureK2M1.nonprimaryShards(true), []int{1}) || len(erasureK2M2.nonprimaryShards(false)) != 0 {
		t.Fatal("pinned nonprimary derivation")
	}
	for _, shards := range []string{"{}", "{0}", "{2}", "{1,2}", "{4}", "{1,1}"} {
		dump, codes, receipt = buildErasureFacts(func(p factsPool, _ map[string]string, _ *poolReceipt, _ *[]map[string]any) {
			p["nonprimary_shards"] = shards
		})
		if _, err := erasureFactsFrom(dump, codes, receipt, erasureK2M2, true); err == nil {
			t.Fatal("optimized nonprimary shards accepted:", shards)
		}
	}
}

func TestErasurePlacementControlUsesThePoolSize(t *testing.T) {
	facts := erasureFacts{pool: 7, size: 4, minSize: 3, owned: []int32{0, 1, 2, 3}, epoch: 51}
	good := osdFixtureOracle{Pool: 7, PoolType: 3, Replicas: 4, MinSize: 3, Epoch: 52, Acting: []int32{2, 0, 3, 1}, Up: []int32{2, 0, 3, 1}, ID: 1, PrimaryID: 1, Shard: 3}
	if shard, err := facts.placementControl(good); err != nil || shard != 0 {
		t.Fatal("control shard wraps at the pool size", shard, err)
	}
	good.ID, good.PrimaryID, good.Shard = 2, 2, 0
	if shard, err := facts.placementControl(good); err != nil || shard != 1 {
		t.Fatal(shard, err)
	}
	for name, edit := range map[string]func(*osdFixtureOracle){
		"three-member acting": func(r *osdFixtureOracle) { r.Acting = []int32{2, 0, 3} },
		"empty position":      func(r *osdFixtureOracle) { r.Acting = []int32{2, crushItemNone, 3, 1} },
		"foreign OSD":         func(r *osdFixtureOracle) { r.Acting = []int32{2, 0, 3, 9} },
		"duplicate OSD":       func(r *osdFixtureOracle) { r.Up = []int32{2, 0, 3, 3} },
		"wrong shard":         func(r *osdFixtureOracle) { r.Shard = 1 },
		"shard beyond pool":   func(r *osdFixtureOracle) { r.Shard = 4 },
		"replica target":      func(r *osdFixtureOracle) { r.ID = 0 },
		"pool size 3":         func(r *osdFixtureOracle) { r.Replicas = 3 },
		"lowered min_size":    func(r *osdFixtureOracle) { r.MinSize = 2 },
		"older epoch":         func(r *osdFixtureOracle) { r.Epoch = 50 },
		"other pool":          func(r *osdFixtureOracle) { r.Pool = 1 },
		"nonprimary count":    func(r *osdFixtureOracle) { r.Nonprimary = 1 },
		"replicated pool":     func(r *osdFixtureOracle) { r.PoolType = 1 },
	} {
		route := good
		route.Acting, route.Up = slices.Clone(good.Acting), slices.Clone(good.Up)
		edit(&route)
		if _, err := facts.placementControl(route); err == nil {
			t.Fatal("placement accepted:", name)
		}
	}
	// The k=2,m=1 control shard remains (shard+1)%3.
	optimized := erasureFacts{pool: 7, size: 3, minSize: 2, owned: []int32{0, 1, 2}, epoch: 1, nonprimary: []int{1}}
	route := osdFixtureOracle{Pool: 7, PoolType: 3, Replicas: 3, MinSize: 2, Epoch: 1, Acting: []int32{0, 1, 2}, Up: []int32{0, 1, 2}, ID: 1, PrimaryID: 1, Shard: 1, Nonprimary: 1}
	if _, err := optimized.placementControl(route); err == nil {
		t.Fatal("primary on a nonprimary shard accepted")
	}
	route.ID, route.PrimaryID, route.Shard = 2, 2, 2
	if shard, err := optimized.placementControl(route); err != nil || shard != 0 {
		t.Fatal("k=2,m=1 control shard", shard, err)
	}
}

// The wider widths derive size, min_size and nonprimary shards from k and m
// alone, and their facts follow the native k=3,m=1 and k=4,m=2 shapes. The
// synthetic dumps are the pinned MON's JSON shapes, not native evidence.
func TestErasureFactsOfTheWiderWidths(t *testing.T) {
	for _, c := range []struct {
		width           erasureWidth
		size, min       int
		nonprimary      []int
		owned           int
		wrongNonprimary []string
	}{
		{erasureK3M1, 4, 3, []int{1, 2}, 4, []string{"{1}", "{2}", "{1,2,3}", "{}"}},
		{erasureK4M2, 6, 5, []int{1, 2, 3}, 6, []string{"{1,2}", "{1,2,3,4}", "{2,3,4}", "{}"}},
	} {
		if c.width.size() != c.size || c.width.minSize() != c.min || !slices.Equal(c.width.nonprimaryShards(true), c.nonprimary) ||
			len(c.width.nonprimaryShards(false)) != 0 {
			t.Fatal("width arithmetic", c.width)
		}
		shape := func(nonprimary string) factsEdit {
			return func(p factsPool, codes map[string]string, r *poolReceipt, osds *[]map[string]any) {
				k, m := strconv.Itoa(c.width.k), strconv.Itoa(c.width.m)
				p["size"], p["min_size"], p["erasure_code_profile"], p["nonprimary_shards"] = c.size, c.min, c.width.profile, nonprimary
				p["stripe_width"] = 4096 * c.width.k
				codes["k"], codes["m"] = k, m
				r.Size, r.MinSize, r.ErasureCodeProfile, r.ECProfile["k"], r.ECProfile["m"] = c.size, c.min, c.width.profile, k, m
				*osds = nil
				for id := range c.owned {
					*osds = append(*osds, map[string]any{"osd": id, "up": 1, "in": 1})
				}
			}
		}
		text := "{" + strings.Trim(strings.Join(strings.Fields(fmt.Sprint(c.nonprimary)), ","), "[]") + "}"
		dump, codes, receipt := buildErasureFacts(shape(text))
		facts, err := erasureFactsFrom(dump, codes, receipt, c.width, true)
		if err != nil || facts.size != c.size || facts.minSize != c.min || !slices.Equal(facts.nonprimary, c.nonprimary) || len(facts.owned) != c.owned || facts.chunk != 4096 {
			t.Fatal(c.width, facts, err)
		}
		for _, wrong := range c.wrongNonprimary {
			dump, codes, receipt := buildErasureFacts(shape(wrong))
			if _, err := erasureFactsFrom(dump, codes, receipt, c.width, true); err == nil {
				t.Fatal(c.width, "nonprimary shards accepted:", wrong)
			}
		}
		// One OSD fewer than k+m cannot host the width.
		dump, codes, receipt = buildErasureFacts(func(p factsPool, codes map[string]string, r *poolReceipt, osds *[]map[string]any) {
			shape(text)(p, codes, r, osds)
			(*osds)[c.owned-1]["in"] = 0
		})
		if _, err := erasureFactsFrom(dump, codes, receipt, c.width, true); err == nil {
			t.Fatal(c.width, "facts accepted with too few OSDs")
		}
		// The k=2,m=2 width does not accept this pool.
		dump, codes, receipt = buildErasureFacts(shape(text))
		if _, err := erasureFactsFrom(dump, codes, receipt, erasureK2M2, true); err == nil {
			t.Fatal(c.width, "facts accepted as k=2,m=2")
		}
	}
}

// The failover lead and the k=3,m=1 read-participation arithmetic, on
// synthetic facts.
func TestErasureLeadAndReadParticipation(t *testing.T) {
	for _, c := range []struct {
		width erasureWidth
		lead  int
	}{{erasureK2M1, 2}, {erasureK2M2, 2}, {erasureK3M1, 3}, {erasureK4M2, 4}} {
		facts := erasureFacts{size: c.width.size(), nonprimary: c.width.nonprimaryShards(true), chunk: 4096}
		if got := erasureLeadAfterLoss(facts, 0); got != c.lead {
			t.Fatal(c.width, "lead after the loss of shard 0", got, "want", c.lead)
		}
		if got := erasureLeadAfterLoss(facts, c.lead); got != 0 {
			t.Fatal(c.width, "lead after the loss of a parity shard", got)
		}
		plain := erasureFacts{size: c.width.size(), chunk: 4096}
		if got := erasureLeadAfterLoss(plain, 0); got != 1 {
			t.Fatal(c.width, "a pool without nonprimary shards leads on shard 1", got)
		}
	}
	k3m1 := erasureFacts{size: 4, chunk: 4096}
	if !erasureReadNeedsEveryShard(k3m1, erasureK3M1, 12000) {
		t.Fatal("a 12000-byte k=3,m=1 object with one shard down needs every remaining shard")
	}
	for name, c := range map[string]struct {
		facts erasureFacts
		width erasureWidth
		size  int
	}{
		"two data chunks only": {k3m1, erasureK3M1, 8192},
		"past one stripe":      {k3m1, erasureK3M1, 12289},
		"unknown chunk":        {erasureFacts{size: 4}, erasureK3M1, 12000},
		"more than k left":     {erasureFacts{size: 4, chunk: 4096}, erasureK2M2, 8000},
		"k=4,m=2":              {erasureFacts{size: 6, chunk: 4096}, erasureK4M2, 16000},
	} {
		if erasureReadNeedsEveryShard(c.facts, c.width, c.size) {
			t.Fatal("read participation claimed:", name)
		}
	}
}

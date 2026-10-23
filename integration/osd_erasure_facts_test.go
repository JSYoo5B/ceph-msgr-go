package integration_test

import (
	"encoding/json"
	"slices"
	"testing"
)

type factsPool = map[string]any

type factsEdit func(factsPool, map[string]string, *poolReceipt, *[]map[string]any)

// buildErasureFacts returns a synthetic k=2,m=2 osd dump, profile and oracle
// receipt in the pinned MON's JSON shapes.
func buildErasureFacts(edit factsEdit) ([]byte, []byte, poolReceipt) {
	p := factsPool{"pool": 7, "type": 3, "size": 4, "min_size": 3, "erasure_code_profile": "ceph-msgr-owned-ec-k2m2",
		"flags_names": "hashpspool,ec_overwrites", "crush_rule": 2, "nonprimary_shards": "{}"}
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
	if facts.size != 4 || facts.minSize != 3 || len(facts.nonprimary) != 0 || !slices.Equal(facts.owned, []int32{0, 1, 2, 3}) || facts.epoch != 51 {
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
		"three up OSDs": func(_ factsPool, _ map[string]string, _ *poolReceipt, o *[]map[string]any) { (*o)[3]["up"] = 0 },
		"missing pool":  func(_ factsPool, _ map[string]string, r *poolReceipt, _ *[]map[string]any) { r.PoolID = 9 },
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

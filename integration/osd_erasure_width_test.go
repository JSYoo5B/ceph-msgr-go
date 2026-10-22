package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// erasureWidth is one closed erasure-code pool width of the owned-pool
// oracle (tools/osd_pool_oracle.py EC_WIDTHS) and the OSD fixture topology
// (CEPH_MSGR_TEST_OSD_REPLICAS) that hosts it.
type erasureWidth struct {
	name, profile, topology string
	k, m                    int
}

var (
	erasureK2M1 = erasureWidth{name: "k2m1", profile: "ceph-msgr-owned-ec", topology: "3", k: 2, m: 1}
	erasureK2M2 = erasureWidth{name: "k2m2", profile: "ceph-msgr-owned-ec-k2m2", topology: "4", k: 2, m: 2}
)

func (w erasureWidth) String() string { return fmt.Sprintf("k=%d,m=%d", w.k, w.m) }

func (w erasureWidth) size() int { return w.k + w.m }

// minSize is the MON-derived EC min_size of pinned OSDMonitor::prepare_pool_size
// (k + min(1, m-1)). The tests read the actual value and never lower it.
func (w erasureWidth) minSize() int { return w.k + min(1, w.m-1) }

// erasureFacts are the authenticated native pool and map facts a scenario
// derives its expectations from.
type erasureFacts struct {
	pool          int64
	size, minSize int
	// nonprimary are the pool's nonprimary_shards: shards that may not be
	// the primary. Empty unless allow_ec_optimizations is set.
	nonprimary []int
	// owned are the up and in OSDs of the map.
	owned []int32
	epoch uint32
}

// crushItemNone is CRUSH_ITEM_NONE, an empty acting-set position.
const crushItemNone = 0x7fffffff

var digits = regexp.MustCompile(`[0-9]+`)

// erasurePoolMapFacts compares the owned-pool oracle's receipt with the Go
// client's own authenticated MON view of the same pool, profile and rule.
func erasurePoolMapFacts(t *testing.T, ctx context.Context, c *cephmsgr.Client, created poolReceipt, width erasureWidth, optimized bool) erasureFacts {
	t.Helper()
	dump, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd dump","format":"json"}`)})
	if err != nil {
		t.Fatal("authenticated osd dump", err)
	}
	profile, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"osd erasure-code-profile get","name":"` + width.profile + `","format":"json"}`)})
	if err != nil {
		t.Fatal("authenticated erasure-code profile", err)
	}
	facts, err := erasureFactsFrom(dump.Data, profile.Data, created, width, optimized)
	if err != nil {
		t.Fatal(err)
	}
	return facts
}

type erasureMapPool struct {
	ID         int64           `json:"pool"`
	Type       int             `json:"type"`
	Size       int             `json:"size"`
	MinSize    int             `json:"min_size"`
	Profile    string          `json:"erasure_code_profile"`
	FlagNames  string          `json:"flags_names"`
	CrushRule  int             `json:"crush_rule"`
	Nonprimary json.RawMessage `json:"nonprimary_shards"`
}

type erasureRuleStep struct {
	Op   string `json:"op"`
	Type string `json:"type"`
}

func erasureFactsFrom(dump, profile []byte, created poolReceipt, width erasureWidth, optimized bool) (erasureFacts, error) {
	var osdMap struct {
		Epoch uint32           `json:"epoch"`
		Pools []erasureMapPool `json:"pools"`
		OSDs  []struct {
			ID int32 `json:"osd"`
			Up int   `json:"up"`
			In int   `json:"in"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(dump, &osdMap); err != nil || osdMap.Epoch == 0 {
		return erasureFacts{}, fmt.Errorf("authenticated osd dump: %v", err)
	}
	facts := erasureFacts{pool: created.PoolID, size: width.size(), minSize: width.minSize(), epoch: osdMap.Epoch}
	index := slices.IndexFunc(osdMap.Pools, func(p erasureMapPool) bool {
		return p.ID == created.PoolID
	})
	if index < 0 {
		return erasureFacts{}, errors.New("owned EC pool is absent from the authenticated map")
	}
	pool := osdMap.Pools[index]
	if pool.Type != 3 || pool.Size != facts.size || created.Size != facts.size || pool.MinSize != facts.minSize || created.MinSize != facts.minSize {
		return erasureFacts{}, fmt.Errorf("%s pool type/size/min_size %d/%d/%d (oracle %d/%d), expected 3/%d/%d",
			width, pool.Type, pool.Size, pool.MinSize, created.Size, created.MinSize, facts.size, facts.minSize)
	}
	if pool.Profile != width.profile || created.ErasureCodeProfile != width.profile {
		return erasureFacts{}, fmt.Errorf("EC pool profile %q (oracle %q), expected %q", pool.Profile, created.ErasureCodeProfile, width.profile)
	}
	if !slices.Contains(strings.Split(pool.FlagNames, ","), "ec_overwrites") || created.ECOverwrites == nil || !*created.ECOverwrites {
		return erasureFacts{}, fmt.Errorf("EC pool overwrite flag missing: %q", pool.FlagNames)
	}
	var codes map[string]string
	if err := json.Unmarshal(profile, &codes); err != nil {
		return erasureFacts{}, fmt.Errorf("authenticated erasure-code profile: %w", err)
	}
	for _, key := range []string{"k", "m", "plugin", "crush-failure-domain"} {
		if codes[key] == "" || codes[key] != created.ECProfile[key] {
			return erasureFacts{}, fmt.Errorf("erasure-code profile %s %q differs from the oracle's %q", key, codes[key], created.ECProfile[key])
		}
	}
	if codes["k"] != strconv.Itoa(width.k) || codes["m"] != strconv.Itoa(width.m) || codes["crush-failure-domain"] != "osd" {
		return erasureFacts{}, fmt.Errorf("erasure-code profile %v is not %s with an OSD failure domain", codes, width)
	}
	var rule struct {
		ID    *int              `json:"rule_id"`
		Type  int               `json:"type"`
		Steps []erasureRuleStep `json:"steps"`
	}
	if err := json.Unmarshal(created.CrushRule, &rule); err != nil || rule.ID == nil || *rule.ID != pool.CrushRule || rule.Type != 3 ||
		!slices.ContainsFunc(rule.Steps, func(s erasureRuleStep) bool {
			return (s.Op == "choose_indep" || s.Op == "chooseleaf_indep") && s.Type == "osd"
		}) {
		return erasureFacts{}, fmt.Errorf("pool rule %d is not an OSD-domain erasure rule: %s", pool.CrushRule, created.CrushRule)
	}
	for _, text := range digits.FindAllString(string(pool.Nonprimary), -1) {
		shard, err := strconv.Atoi(text)
		if err != nil || shard < 1 || shard >= facts.size || slices.Contains(facts.nonprimary, shard) {
			return erasureFacts{}, fmt.Errorf("invalid nonprimary_shards %s", pool.Nonprimary)
		}
		facts.nonprimary = append(facts.nonprimary, shard)
	}
	slices.Sort(facts.nonprimary)
	if optimized != (len(facts.nonprimary) > 0) {
		return erasureFacts{}, fmt.Errorf("nonprimary_shards %s disagree with optimized=%v", pool.Nonprimary, optimized)
	}
	for _, osd := range osdMap.OSDs {
		if osd.Up == 1 && osd.In == 1 {
			facts.owned = append(facts.owned, osd.ID)
		}
	}
	if len(facts.owned) < facts.size {
		return erasureFacts{}, fmt.Errorf("%d up and in OSDs cannot host %s", len(facts.owned), width)
	}
	return facts, nil
}

// checkPlacement verifies a native object placement against the pool facts
// before any Go I/O and returns a control shard: another valid shard that
// the primary does not hold.
func (f erasureFacts) checkPlacement(t *testing.T, route osdFixtureOracle) uint8 {
	t.Helper()
	shard, err := f.placementControl(route)
	if err != nil {
		t.Fatalf("native EC placement %+v: %v", route, err)
	}
	return shard
}

func (f erasureFacts) placementControl(route osdFixtureOracle) (uint8, error) {
	if route.Pool != f.pool || route.PoolType != 3 || route.Replicas != f.size || route.MinSize != f.minSize || route.Epoch < f.epoch {
		return 0, errors.New("pool identity, size, min_size or epoch differs from the pool facts")
	}
	for name, set := range map[string][]int32{"acting": route.Acting, "up": route.Up} {
		seen := map[int32]bool{}
		for _, osd := range set {
			if osd == crushItemNone || seen[osd] || !slices.Contains(f.owned, osd) {
				return 0, fmt.Errorf("%s set is not %d distinct up and in OSDs", name, f.size)
			}
			seen[osd] = true
		}
		if len(set) != f.size {
			return 0, fmt.Errorf("%s set has %d members, pool size is %d", name, len(set), f.size)
		}
	}
	shard := int(route.Shard)
	if route.ID != route.PrimaryID || shard < 0 || shard >= f.size || route.Acting[shard] != route.ID {
		return 0, errors.New("target is not the acting primary on its own shard")
	}
	if slices.Contains(f.nonprimary, shard) || route.Nonprimary != len(f.nonprimary) {
		return 0, errors.New("primary holds a nonprimary shard or the nonprimary count differs")
	}
	return uint8((shard + 1) % f.size), nil
}

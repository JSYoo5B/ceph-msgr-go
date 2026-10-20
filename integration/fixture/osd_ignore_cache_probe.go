package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// This command is native-only. It never constructs a product Client or changes
// product feature advertisement. Its explicit entry point isolates tier pools
// (which make MON require OSD_CACHEPOOL) from all ordinary integration suites.
func validateIgnoreCacheProbeProfile(p profile) error {
	if p.osdCachePools != "0" || p.image != profileDefaultImage || p.osd != "1" || p.osdReplicas != "1" || p.osdStore != "bluestore" || p.osdProfile != "legacy-v6" ||
		p.mdsFresh != "0" || p.mdsCommands != "0" || p.mdsSessions != "0" || p.mdsCephFS != "0" || p.mdsCephFSRanks != "0" || p.mdsCephFSCompletion != "0" || p.mdsCephFSData != "0" || p.rbdBase != "0" || p.monPing != "0" || p.monFreeze != "" || p.testRun != "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$" || p.osdSoak != "" || p.stressDuration != "" ||
		p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.nativeMgrPreparation != "0" || p.modeRejection != "" ||
		p.sparseReadOracle != "" || p.mapExtOracle != "" || p.u64XattrOracle != "" || p.writeSameOracle != "" || p.race != "0" || p.mappedIPv6 != "0" {
		return errors.New("native IgnoreCache probe requires pinned isolated legacy-v6 single BlueStore OSD, no MDS/RBD or other test selector/fault profile")
	}
	return nil
}

type ignoreCacheProbePlan struct {
	SourceCommit string            `json:"source_commit"`
	FSID         string            `json:"fsid"`
	Token        string            `json:"token"`
	Base         string            `json:"base"`
	Tier         string            `json:"tier"`
	Namespace    string            `json:"namespace"`
	Clients      map[string]string `json:"clients"`
}

type ignoreCacheProbeStep struct {
	Action string `json:"action"`
	State  string `json:"state"`
	Error  string `json:"error,omitempty"`
}
type ignoreCacheProbeReceipt struct {
	Schema         int                    `json:"schema_version"`
	NativeOnly     bool                   `json:"native_only"`
	ProductSupport bool                   `json:"product_support_claimed"`
	Complete       bool                   `json:"complete"`
	Plan           ignoreCacheProbePlan   `json:"plan"`
	Image          string                 `json:"image"`
	ImageID        string                 `json:"image_id"`
	Architecture   string                 `json:"architecture"`
	Version        string                 `json:"daemon_version"`
	Sources        map[string]string      `json:"source_sha256"`
	Configuration  map[string]string      `json:"configuration"`
	Steps          []ignoreCacheProbeStep `json:"steps"`
	Error          string                 `json:"error,omitempty"`
}

func writeIgnoreCacheJSON(out, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > 256<<10 {
		return errors.New("cache probe receipt exceeds bound")
	}
	temporary := filepath.Join(out, name+".tmp")
	if err = os.WriteFile(temporary, append(data, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, name))
}

// Validate raw pool metadata independently of the C-API object's read results.
func validateIgnoreCacheTier(data []byte, base, tier string, attached bool) error {
	var dump struct {
		Epoch uint32 `json:"epoch"`
		Pools []struct {
			ID        int64             `json:"pool"`
			Name      string            `json:"pool_name"`
			Type      int               `json:"type"`
			Size      int               `json:"size"`
			MinSize   int               `json:"min_size"`
			TierOf    int64             `json:"tier_of"`
			Tiers     []int64           `json:"tiers"`
			ReadTier  int64             `json:"read_tier"`
			WriteTier int64             `json:"write_tier"`
			CacheMode string            `json:"cache_mode"`
			HitSet    map[string]any    `json:"hit_set_params"`
			Snaps     []json.RawMessage `json:"pool_snaps"`
		} `json:"pools"`
	}
	if json.Unmarshal(data, &dump) != nil || dump.Epoch == 0 {
		return errors.New("invalid native OSDMap dump")
	}
	b, t := -1, -1
	for i, p := range dump.Pools {
		if p.Name == base {
			if b >= 0 {
				return errors.New("ambiguous base pool")
			}
			b = i
		}
		if p.Name == tier {
			if t >= 0 {
				return errors.New("ambiguous tier pool")
			}
			t = i
		}
	}
	if b < 0 || t < 0 {
		return errors.New("owned cache pools missing")
	}
	bp, tp := dump.Pools[b], dump.Pools[t]
	if bp.ID < 0 || tp.ID < 0 || bp.ID == tp.ID || bp.TierOf != -1 || bp.CacheMode != "none" || bp.Type != 1 || tp.Type != 1 || bp.Size != 1 || tp.Size != 1 || bp.MinSize != 1 || tp.MinSize != 1 || len(bp.Snaps) != 0 || len(tp.Snaps) != 0 || bp.ReadTier != -1 || bp.WriteTier != -1 || tp.ReadTier != -1 || tp.WriteTier != -1 {
		return errors.New("cache pools differ from head replicated no-overlay shape")
	}
	if attached {
		if len(bp.Tiers) != 1 || bp.Tiers[0] != tp.ID || tp.TierOf != bp.ID || tp.CacheMode != "writeback" || len(tp.Tiers) != 0 {
			return errors.New("native cache relationship differs")
		}
		if kind := tp.HitSet["type"]; kind != "none" {
			return errors.New("cache promotion proof requires no hit set")
		}
	} else if len(bp.Tiers) != 0 || len(tp.Tiers) != 0 || tp.TierOf != -1 || tp.CacheMode != "none" {
		return errors.New("native cache relationship remains attached")
	}
	return nil
}

func validateIgnoreCacheNative(data []byte, plan ignoreCacheProbePlan) error {
	var result struct {
		Complete bool                 `json:"complete"`
		Source   string               `json:"source_commit"`
		Plan     ignoreCacheProbePlan `json:"plan"`
		Steps    []struct {
			State string `json:"state"`
		} `json:"steps"`
		Placements []struct {
			Pool      string `json:"pool_name"`
			Object    string `json:"object"`
			Namespace string `json:"namespace"`
			Address   string `json:"address"`
			Epoch     uint32 `json:"epoch"`
			Primary   int32  `json:"primary_id"`
			Shard     int8   `json:"pg_shard"`
		} `json:"placements"`
	}
	if len(data) > 256<<10 || json.Unmarshal(data, &result) != nil || !result.Complete || result.Source != nativeEncoderCommit || !reflect.DeepEqual(result.Plan, plan) || len(result.Steps) != 66 || len(result.Placements) != 10 {
		return errors.New("native cache proof incomplete or foreign")
	}
	for _, step := range result.Steps {
		if step.State != "completed" {
			return errors.New("native cache operation remains uncertain")
		}
	}
	objects := map[string]bool{"miss": true, "write-normal": true, "write-ignore": true, "delete-normal": true, "delete-ignore": true}
	seen := map[string]bool{}
	for _, placement := range result.Placements {
		key := placement.Pool + "/" + placement.Object
		if (placement.Pool != plan.Base && placement.Pool != plan.Tier) || !objects[placement.Object] || placement.Namespace != plan.Namespace || placement.Epoch == 0 || placement.Primary != 0 || placement.Shard != -1 || !strings.HasPrefix(placement.Address, "v2:") || seen[key] {
			return errors.New("native cache placement proof differs")
		}
		seen[key] = true
	}
	return nil
}

func ignoreCacheProbeCommand(ctx context.Context, args []string) (result error) {
	flags := flag.NewFlagSet("probe-osd-ignore-cache", flag.ContinueOnError)
	project := flags.String("project", "../..", "product checkout")
	output := flags.String("output", "", "new private evidence directory")
	diagnostics := flags.String("diagnostics", "", "public probe receipts only")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *output == "" {
		return errors.New("probe-osd-ignore-cache requires -output and no positional arguments")
	}
	if os.Getenv("CEPH_MSGR_TEST_RUN") != "" {
		return errors.New("native cache probe does not accept an integration test selector")
	}
	p, err := loadProfile(os.Getenv)
	if err != nil {
		return err
	}
	if err = validateIgnoreCacheProbeProfile(p); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	out, err := filepath.Abs(*output)
	if err != nil {
		return err
	}
	if _, err = os.Stat(filepath.Join(root, "SPEC.md")); err != nil {
		return err
	}
	if err = os.Mkdir(out, 0700); err != nil {
		return fmt.Errorf("probe output must be new: %w", err)
	}
	var token [6]byte
	if _, err = rand.Read(token[:]); err != nil {
		return err
	}
	id := hex.EncodeToString(token[:])
	plan := ignoreCacheProbePlan{SourceCommit: nativeEncoderCommit, Token: id, Base: "ceph-msgr-cache-base-" + id, Tier: "ceph-msgr-cache-tier-" + id, Namespace: "cache-probe", Clients: map[string]string{}}
	for _, role := range []string{"rw", "ro", "base"} {
		plan.Clients[role] = "client.cache-" + role + "-" + id
	}
	receipt := ignoreCacheProbeReceipt{Schema: 1, NativeOnly: true, Plan: plan, Image: p.image, Sources: map[string]string{}, Configuration: map[string]string{"key_type": p.keyType, "service_cipher": p.serviceCipher, "connection_mode": p.connectionMode, "ip_family": p.ipFamily, "osd_store": p.osdStore, "auth_ticket_ttl": "12s", "service_ticket_ttl": "12s"}}
	for _, name := range []string{"integration/fixture/osd_ignore_cache_probe.go", "integration/fixture/osd_cache_pools.go", "tools/osd_ignore_cache_oracle.py", "tools/osd_mutation_oracle.py", "tools/osd_oracle.py"} {
		raw, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			return e
		}
		receipt.Sources[name] = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	save := func() error { return writeIgnoreCacheJSON(out, "osd-ignore-cache-probe.json", receipt) }
	defer func() {
		if result != nil {
			receipt.Error = result.Error()
			receipt.Complete = false
		}
		result = errors.Join(result, save())
		if *diagnostics != "" {
			result = errors.Join(result, saveIgnoreCacheEvidence(out, *diagnostics))
		}
	}()
	if err = stageAssets(root, out, p); err != nil {
		return err
	}
	c, err := startCluster(ctx, p, out)
	defer func() {
		if c != nil {
			cleanup, stop := context.WithTimeout(context.Background(), p.cleanupBudget())
			defer stop()
			result = errors.Join(result, c.Terminate(cleanup))
		}
	}()
	if ownerErr := recordOwnership(c, out); ownerErr != nil {
		return errors.Join(err, ownerErr)
	}
	if err != nil {
		return err
	}
	plan.FSID = c.profile.fsid
	receipt.Plan = plan
	imageID, arch, err := inspectImageMetadata(ctx, c)
	if err != nil {
		return err
	}
	receipt.ImageID, receipt.Architecture = imageID, arch
	receipt.Version, err = nativeVersion(ctx, c)
	if err != nil {
		return err
	}
	if !strings.Contains(receipt.Version, nativeEncoderCommit) {
		return errors.New("cache probe requires exact pinned daemon")
	}
	if err = writeIgnoreCacheJSON(out, "osd-ignore-cache-plan.json", plan); err != nil {
		return err
	}
	if err = save(); err != nil {
		return err
	}
	mutate := func(action string, operation func() error) error {
		receipt.Steps = append(receipt.Steps, ignoreCacheProbeStep{Action: action, State: "intended"})
		i := len(receipt.Steps) - 1
		if e := save(); e != nil {
			return e
		}
		e := operation()
		if e != nil {
			receipt.Steps[i].State = "unknown-or-failed"
			receipt.Steps[i].Error = e.Error()
		} else {
			receipt.Steps[i].State = "completed"
		}
		return errors.Join(e, save())
	}
	resources, err := prepareIgnoreCacheResources(ctx, c, out, plan, mutate, false)
	if err != nil {
		return err
	}
	if err = mutate("native-fixed-matrix", func() error {
		return c.runOracleCommandWithTimeout(ctx, out, []string{"python3", "/out/osd_ignore_cache_oracle.py", "/tmp/ceph-msgr-test", "/out"}, "osd-ignore-cache-command.json", "native-only IgnoreCache proof", 3*time.Minute)
	}); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(out, "osd-ignore-cache-native.json"))
	if err != nil {
		return err
	}
	if err = validateIgnoreCacheNative(raw, plan); err != nil {
		return err
	}
	if err = finishIgnoreCacheResources(ctx, c, out, plan, resources, mutate); err != nil {
		return err
	}
	receipt.Complete = true
	return save()
}

func saveIgnoreCacheEvidence(out, diagnostics string) error {
	if err := os.MkdirAll(diagnostics, 0700); err != nil {
		return err
	}
	for _, name := range ignoreCacheProbeEvidence {
		raw, err := os.ReadFile(filepath.Join(out, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if len(raw) > 4<<20 {
			return errors.New("cache probe evidence exceeds bound")
		}
		if err = os.WriteFile(filepath.Join(diagnostics, name), raw, 0600); err != nil {
			return err
		}
	}
	return nil
}

var ignoreCacheProbeEvidence = []string{"osd-ignore-cache-probe.json", "osd-ignore-cache-plan.json", "osd-ignore-cache-attached.json", "osd-ignore-cache-detached.json", "osd-ignore-cache-native.json", "osd-ignore-cache-command.json"}

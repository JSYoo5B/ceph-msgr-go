package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func (c *fixtureCluster) prepareOSD(ctx context.Context, out string) error {
	topology := c.profile.osdTopology()
	var started []int
	for _, daemon := range c.cluster.OSDs() {
		started = append(started, daemon.ID)
	}
	// The CRUSH root lists exactly the daemons this fixture owns, never a
	// count inferred from the profile name alone.
	owned, err := ownedOSDInventory(started, topology)
	if err != nil {
		return err
	}
	path := filepath.Join(out, "legacy.crush.txt")
	source, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	text, err := legacyCRUSHForInventory(string(source), owned)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		return err
	}
	if err := c.exec(ctx, "crushtool", "-c", "/out/legacy.crush.txt", "-o", "/out/legacy.crush"); err != nil {
		return err
	}
	// Input blobs for the control container travel through the public copy API.
	if err := c.cluster.ControlContainer().CopyFileToContainer(ctx, filepath.Join(out, "legacy.crush"), "/tmp/legacy.crush", 0600); err != nil {
		return err
	}
	if _, err := c.cluster.Ceph(ctx, "osd", "setcrushmap", "-i", "/tmp/legacy.crush"); err != nil {
		return err
	}
	installed, err := c.cluster.Ceph(ctx, "osd", "crush", "dump", "--format", "json")
	if err != nil {
		return err
	}
	if err := verifyCRUSHInventory(installed, owned); err != nil {
		return err
	}
	if _, err := c.cluster.Ceph(ctx, "balancer", "off"); err != nil {
		return err
	}
	poolPGs, noHashPSPool := 1, true
	if c.profile.osdPGV8() {
		poolPGs, noHashPSPool = 8, false
	}
	_, err = c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: "ceph-msgr-osd-fixture", PGNum: poolPGs, Replicas: topology.poolSize, MinSize: topology.poolMinSize, NoHashPSPool: noHashPSPool})
	if err != nil {
		return fmt.Errorf("create legacy pool: %w", err)
	}
	// The global bootstrap default reaches automatic .mgr creation before
	// startup. Never repair pool flags here: verify the recipe's actual result
	// across every native pool and reject unsupported admission requirements.
	validator := validateLegacyPoolPolicy
	if c.profile.osdPGV8() {
		validator = validateReplicatedPGPoolPolicy
	}
	policy, err := waitFixturePoolPolicy(ctx, c.profile.mgrCount == "2", c.poolPolicy, waitLegacyPoolObservation, validator)
	if err != nil {
		return err
	}
	evidence, err := json.MarshalIndent(map[string]any{
		"bootstrap":                        map[string]any{"section": "global", "osd_pool_default_flag_hashpspool": !noHashPSPool, "api": "WithConfigFile"},
		"explicit_data_pool_no_hashpspool": noHashPSPool,
		"osd_profile":                      c.profile.osdProfile, "data_pool_pg_num": poolPGs,
		"verified": policy,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "pool-policy.json"), evidence, 0600)
}

var errLegacyPoolsNotReady = errors.New("legacy fixture pools are not ready")

func validateLegacyPoolPolicy(policy poolPolicy, managers bool) error {
	return validateFixturePoolPolicy(policy, managers, false)
}

func validateReplicatedPGPoolPolicy(policy poolPolicy, managers bool) error {
	return validateFixturePoolPolicy(policy, managers, true)
}

func validateFixturePoolPolicy(policy poolPolicy, managers, hashPSPool bool) error {
	if policy.Epoch == 0 || policy.FSID == "" {
		return errors.New("legacy pool policy has no actual map identity")
	}
	dataPool, managerPool := false, false
	ids, names := make(map[int64]bool), make(map[string]bool)
	for _, pool := range policy.Pools {
		if pool.ID < 0 || pool.Name == "" || strings.ContainsRune(pool.Name, 0) || ids[pool.ID] || names[pool.Name] {
			return errors.New("legacy pool policy has invalid or duplicate pool identity")
		}
		ids[pool.ID], names[pool.Name] = true, true
		// Tentacle pg_pool_t::FLAG_HASHPSPOOL is bit 0. Check native numeric
		// flags as well as display names so a missing label cannot admit it.
		numericHash := pool.Flags&1 != 0
		namedHash := strings.Contains(","+pool.FlagNames+",", ",hashpspool,")
		if !hashPSPool && (numericHash || namedHash) {
			return fmt.Errorf("legacy fixture still requires HASHPSPOOL in pool %s", pool.Name)
		}
		if hashPSPool && (!numericHash || !namedHash) {
			return fmt.Errorf("replicated-pg-v8 fixture missing HASHPSPOOL in pool %s", pool.Name)
		}
		dataPool = dataPool || pool.Name == "ceph-msgr-osd-fixture"
		managerPool = managerPool || pool.Name == ".mgr"
	}
	if !dataPool {
		return fmt.Errorf("%w: missing data pool", errLegacyPoolsNotReady)
	}
	if managers && !managerPool {
		return fmt.Errorf("%w: missing automatic .mgr pool", errLegacyPoolsNotReady)
	}
	return nil
}

// MGR availability does not imply that its background module has created .mgr
// yet. Poll only the read-only map observation, never pool creation or repair.
func waitLegacyPoolPolicy(ctx context.Context, managers bool,
	fetch func(context.Context) (poolPolicy, error), wait func(context.Context, time.Duration) error,
) (poolPolicy, error) {
	return waitFixturePoolPolicy(ctx, managers, fetch, wait, validateLegacyPoolPolicy)
}

func waitFixturePoolPolicy(ctx context.Context, managers bool,
	fetch func(context.Context) (poolPolicy, error), wait func(context.Context, time.Duration) error,
	validate func(poolPolicy, bool) error,
) (poolPolicy, error) {
	ctx, cancel := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return poolPolicy{}, err
		}
		policy, err := fetch(ctx)
		if err != nil {
			return poolPolicy{}, err
		}
		if err := ctx.Err(); err != nil {
			return poolPolicy{}, err
		}
		err = validate(policy, managers)
		if err == nil {
			return policy, nil
		}
		if !errors.Is(err, errLegacyPoolsNotReady) {
			return poolPolicy{}, err
		}
		if waitErr := wait(ctx, 250*time.Millisecond); waitErr != nil {
			return poolPolicy{}, errors.Join(err, waitErr)
		}
	}
}

func waitLegacyPoolObservation(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type poolPolicy struct {
	Epoch uint32            `json:"epoch"`
	FSID  string            `json:"fsid"`
	Pools []poolPolicyEntry `json:"pools"`
}

type poolPolicyEntry struct {
	ID        int64  `json:"pool"`
	Name      string `json:"pool_name"`
	Flags     uint64 `json:"flags"`
	FlagNames string `json:"flags_names"`
}

// Mandatory wire fields use pointers: absent/null numeric flags are not zero
// flags, and an incomplete JSON observation must not become readiness polling.
func decodeLegacyPoolPolicy(raw []byte, expectedFSID string) (poolPolicy, error) {
	var result poolPolicy
	if len(raw) > 1<<20 || expectedFSID == "" {
		return result, errors.New("legacy pool policy lacks bounded fixture identity")
	}
	var decoded struct {
		Epoch *uint32 `json:"epoch"`
		FSID  *string `json:"fsid"`
		Pools *[]struct {
			ID        *int64  `json:"pool"`
			Name      *string `json:"pool_name"`
			Flags     *uint64 `json:"flags"`
			FlagNames string  `json:"flags_names"`
		} `json:"pools"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return result, fmt.Errorf("decode legacy pool policy: %w", err)
	}
	if decoded.Epoch == nil || *decoded.Epoch == 0 || decoded.FSID == nil || *decoded.FSID != expectedFSID || decoded.Pools == nil {
		return result, errors.New("legacy pool policy has missing or mismatched map identity")
	}
	result.Epoch, result.FSID = *decoded.Epoch, *decoded.FSID
	ids, names := make(map[int64]bool), make(map[string]bool)
	for _, pool := range *decoded.Pools {
		if pool.ID == nil || pool.Name == nil || pool.Flags == nil || *pool.ID < 0 || *pool.Name == "" || strings.ContainsRune(*pool.Name, 0) || ids[*pool.ID] || names[*pool.Name] {
			return poolPolicy{}, errors.New("legacy pool policy has missing flags or invalid/duplicate pool identity")
		}
		ids[*pool.ID], names[*pool.Name] = true, true
		result.Pools = append(result.Pools, poolPolicyEntry{ID: *pool.ID, Name: *pool.Name, Flags: *pool.Flags, FlagNames: pool.FlagNames})
	}
	return result, nil
}

func (c *fixtureCluster) poolPolicy(ctx context.Context) (poolPolicy, error) {
	// The public supervisor bounds the command and its raw output before the
	// host JSON decoder allocates. An incomplete observation is never admitted.
	observation, err := c.cluster.RunNativeCommand(ctx, c.cluster.ControlContainer(), ceph.NativeCommand{
		Argv:    []string{"ceph", "--connect-timeout", "5", "osd", "dump", "--format", "json"},
		Timeout: 20 * time.Second, MaxStdout: 1 << 20, MaxStderr: 16 << 10, Label: "legacy pool policy observation",
	})
	var result poolPolicy
	if err != nil {
		return result, err
	}
	if !observation.Complete() || observation.ExitCode != 0 {
		return result, fmt.Errorf("legacy pool policy observation exit=%d complete=%t", observation.ExitCode, observation.Complete())
	}
	return decodeLegacyPoolPolicy(observation.Stdout, c.profile.fsid)
}

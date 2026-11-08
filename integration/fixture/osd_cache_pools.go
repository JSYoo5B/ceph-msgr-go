package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

type cachePoolMutate func(string, func() error) error

type cachePoolResources struct {
	Pools           []*ceph.Pool
	Clients         map[string]*ceph.ClientConfig
	FinishAttempted bool
	FinishError     error
	Cleanup         map[string]any
}

func cachePoolClientCaps(pool, permission, namespace string, live bool) ceph.ClientCaps {
	caps := ceph.ClientCaps{Mon: "allow r", OSD: "allow " + permission + " pool=" + pool + " namespace=" + namespace}
	if live {
		caps.MGR = "allow r"
	}
	return caps
}

// Shared disposable-pool lifecycle. The native-only probe keeps its own
// receipt and fixed 66-operation oracle; live Go acceptance selects a separate
// profile, object namespace inventory and oracle without importing the product.
func prepareIgnoreCacheResources(ctx context.Context, c *fixtureCluster, out string, plan ignoreCacheProbePlan, mutate cachePoolMutate, live bool) (*cachePoolResources, error) {
	resources := &cachePoolResources{Clients: map[string]*ceph.ClientConfig{}}
	var err error
	cli := func(action string, argv ...string) error {
		return mutate(action, func() error { _, e := c.cluster.Ceph(ctx, argv...); return e })
	}
	for _, name := range []string{plan.Base, plan.Tier} {
		name := name
		if err = mutate("create-pool-"+name, func() error {
			pool, e := c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, PGNum: 1, Replicas: 1, MinSize: 1, Application: "rados", NoHashPSPool: true})
			if pool != nil {
				resources.Pools = append(resources.Pools, pool)
			}
			return e
		}); err != nil {
			return resources, err
		}
	}
	if err = observe(c.cluster).WaitForClean(ctx); err != nil {
		return resources, err
	}
	for _, role := range []string{"rw", "ro", "base"} {
		role := role
		pool, permission := plan.Tier, "rw"
		if role == "ro" {
			permission = "r"
		}
		if role == "base" {
			pool = plan.Base
		}
		if err = mutate("create-client-"+role, func() error {
			client, e := c.cluster.CreateClient(ctx, plan.Clients[role], cachePoolClientCaps(pool, permission, plan.Namespace, live))
			if e != nil {
				return e
			}
			resources.Clients[role] = client
			keyring, e := connectionKeyring(client.ConnectionConfig())
			if e != nil {
				return e
			}
			return os.WriteFile(filepath.Join(out, "osd-ignore-cache-"+role+".keyring"), keyring, 0600)
		}); err != nil {
			return resources, err
		}
	}
	if err = cli("attach-tier", "osd", "tier", "add", plan.Base, plan.Tier); err != nil {
		return resources, err
	}
	if err = cli("writeback-mode", "osd", "tier", "cache-mode", plan.Tier, "writeback"); err != nil {
		return resources, err
	}
	dump, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return resources, err
	}
	if err = validateIgnoreCacheTier(dump, plan.Base, plan.Tier, true); err != nil {
		return resources, err
	}
	if err = os.WriteFile(filepath.Join(out, "osd-ignore-cache-attached.json"), dump, 0644); err != nil {
		return resources, err
	}
	return resources, nil
}

func finishIgnoreCacheResources(ctx context.Context, c *fixtureCluster, out string, plan ignoreCacheProbePlan, resources *cachePoolResources, mutate cachePoolMutate) (result error) {
	if resources.FinishAttempted {
		return resources.FinishError
	}
	resources.FinishAttempted = true
	defer func() { resources.FinishError = result }()
	var err error
	cli := func(action string, argv ...string) error {
		return mutate(action, func() error { _, e := c.cluster.Ceph(ctx, argv...); return e })
	}
	if err = cli("detach-tier", "osd", "tier", "remove", plan.Base, plan.Tier); err != nil {
		return err
	}
	dump, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return err
	}
	if err = validateIgnoreCacheTier(dump, plan.Base, plan.Tier, false); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "osd-ignore-cache-detached.json"), dump, 0644); err != nil {
		return err
	}
	baseline, err := withOwnedPoolRemoval(ctx, clusterPoolRemovalConfig(c.cluster), mutate, func() error {
		for _, pool := range resources.Pools {
			if err := cli("remove-pool-"+pool.Name, "osd", "pool", "rm", pool.Name, pool.Name, "--yes-i-really-really-mean-it"); err != nil {
				return err
			}
			if err := cli("remove-rule-"+pool.CRUSHRule, "osd", "crush", "rule", "rm", pool.CRUSHRule); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, role := range []string{"rw", "ro", "base"} {
		role := role
		if err = mutate("remove-client-"+role, func() error { return resources.Clients[role].Delete(ctx) }); err != nil {
			return err
		}
		_, e := c.cluster.Ceph(ctx, "auth", "get", plan.Clients[role])
		if !errors.Is(e, fs.ErrNotExist) {
			return errors.New("owned native client removal not confirmed")
		}
	}
	listing, err := c.cluster.Ceph(ctx, "osd", "pool", "ls", "--format", "json")
	if err != nil {
		return err
	}
	var names []string
	if json.Unmarshal(listing, &names) != nil {
		return errors.New("invalid final pool inventory")
	}
	for _, name := range names {
		if name == plan.Base || name == plan.Tier {
			return errors.New("owned cache pool remains")
		}
	}
	rulesRaw, err := c.cluster.Ceph(ctx, "osd", "crush", "rule", "ls", "--format", "json")
	if err != nil {
		return err
	}
	var rules []string
	if json.Unmarshal(rulesRaw, &rules) != nil {
		return errors.New("invalid final CRUSH rule inventory")
	}
	for _, rule := range rules {
		for _, pool := range resources.Pools {
			if rule == pool.CRUSHRule {
				return errors.New("owned cache CRUSH rule remains")
			}
		}
	}
	config, err := c.cluster.Ceph(ctx, "config", "dump", "--format", "json")
	if err != nil {
		return err
	}
	if err = poolDeleteOverrideAbsent(config, "invalid restored config inventory", "pool deletion override remains"); err != nil {
		return err
	}
	resources.Cleanup = map[string]any{"owned_pools_absent": true, "owned_rules_absent": true, "owned_clients_absent": true, "pool_delete_override_absent": true, "pool_delete_baseline": baseline, "remaining_pool_names": names, "remaining_rule_names": rules}
	return nil
}

// centralConfigRestorer is the restorable handle of one central
// configuration entry (override.ConfigHandle in production).
type centralConfigRestorer interface{ Restore(context.Context) error }

// poolRemovalConfig is the central configuration access of the owned
// pool-removal window. observe runs the independent read-only CLI checks;
// allow changes, preserves and later restores the central entry.
type poolRemovalConfig struct {
	observe func(context.Context, ...string) ([]byte, error)
	allow   func(context.Context, ceph.ConfigSetting) (centralConfigRestorer, error)
}

// poolDeleteSetting is the only central entry the pool-removal window owns.
var poolDeleteSetting = ceph.ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"}

// clusterPoolRemovalConfig delegates the change/preserve/restore mechanics of
// the central entry to override.SetConfig. Its handle records the prior
// absence, refuses to restore over an outside edit and confirms the restored
// entry by readback. The pre and post checks stay independent CLI reads.
func clusterPoolRemovalConfig(cluster *ceph.Container) poolRemovalConfig {
	return poolRemovalConfig{
		observe: cluster.Ceph,
		allow: func(ctx context.Context, setting ceph.ConfigSetting) (centralConfigRestorer, error) {
			handle, err := override.SetConfig(ctx, cluster, setting)
			if handle == nil {
				return nil, err
			}
			return handle, err
		},
	}
}

// poolDeleteOverrideAbsent checks one native `config dump` for any
// mon_allow_pool_delete entry, in every section and mask.
func poolDeleteOverrideAbsent(config []byte, invalid, present string) error {
	var entries []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(config, &entries) != nil {
		return errors.New(invalid)
	}
	for _, entry := range entries {
		if entry.Name == poolDeleteSetting.Name {
			return errors.New(present)
		}
	}
	return nil
}

// withOwnedPoolRemoval allows pool deletion only around remove. The
// disposable fixture has no central deletion override: that baseline is
// checked independently before the change, preserved by the handle, and
// checked independently again after the restore. Daemon files and unrelated
// configuration sections are never changed.
//
// Each step runs once through mutate. A refused, failed or uncertain change
// ends the window without removing anything and without retrying the change;
// an uncertain entry is retained with the disposable cluster, whose
// termination discards the configuration database. A failed or uncertain
// removal leaves the entry for the same reason, and a failed restore is
// returned without trusting any later readback.
func withOwnedPoolRemoval(ctx context.Context, central poolRemovalConfig, mutate cachePoolMutate, remove func() error) (string, error) {
	get := func() (string, error) {
		setting, err := central.observe(ctx, "config", "get", "mon", poolDeleteSetting.Name)
		return strings.TrimSpace(string(setting)), err
	}
	setting, err := get()
	if err != nil {
		return "", err
	}
	if setting != "false" {
		return "", errors.New("unexpected pool deletion baseline")
	}
	config, err := central.observe(ctx, "config", "dump", "--format", "json")
	if err != nil {
		return "", err
	}
	if err = poolDeleteOverrideAbsent(config, "invalid native config inventory", "unexpected central pool deletion override"); err != nil {
		return "", err
	}
	var handle centralConfigRestorer
	if err = mutate("enable-owned-pool-removal", func() error {
		var e error
		handle, e = central.allow(ctx, poolDeleteSetting)
		if e == nil && handle == nil {
			e = errors.New("pool deletion override returned no handle")
		}
		return e
	}); err != nil {
		return "", err
	}
	if err = remove(); err != nil {
		return "", err
	}
	if err = mutate("restore-pool-removal-baseline", func() error { return handle.Restore(ctx) }); err != nil {
		return "", err
	}
	setting, err = get()
	if err != nil || setting != "false" {
		return "", errors.Join(err, errors.New("pool deletion baseline not restored"))
	}
	return setting, nil
}

type osdCachePoolScope struct {
	Out             string
	Plan            ignoreCacheProbePlan
	Resources       *cachePoolResources
	Steps           []ignoreCacheProbeStep
	LastID          int64
	TierOps         bool
	Finished        bool
	FinishAttempted bool
	FinishError     error
}

func (x *osdCachePoolScope) save() error {
	errorText := ""
	if x.FinishError != nil {
		errorText = x.FinishError.Error()
	}
	var cleanup map[string]any
	if x.Resources != nil {
		cleanup = x.Resources.Cleanup
	}
	return writeIgnoreCacheJSON(x.Out, "osd-cache-pools-fixture.json", map[string]any{
		"source_commit": nativeEncoderCommit, "plan": x.Plan, "live_go_fixture": true,
		"steps": x.Steps, "finished": x.Finished, "finish_attempted": x.FinishAttempted, "tier_ops_selected": x.TierOps,
		"cleanup_error": errorText,
		"cleanup":       cleanup,
	})
}
func (x *osdCachePoolScope) mutate(action string, fn func() error) error {
	x.Steps = append(x.Steps, ignoreCacheProbeStep{Action: action, State: "intended"})
	i := len(x.Steps) - 1
	if err := x.save(); err != nil {
		return err
	}
	err := fn()
	x.Steps[i].State = "completed"
	if err != nil {
		x.Steps[i].State = "unknown-or-failed"
		x.Steps[i].Error = err.Error()
	}
	return errors.Join(err, x.save())
}

func (c *fixtureCluster) prepareOSDCachePools(ctx context.Context, out string) error {
	if c.profile.osdCachePools != "1" || c.cachePools != nil {
		return errors.New("invalid live cache-pool preparation scope")
	}
	var token [6]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	id := hex.EncodeToString(token[:])
	x := &osdCachePoolScope{Out: out, Plan: ignoreCacheProbePlan{SourceCommit: nativeEncoderCommit, FSID: c.profile.fsid, Token: id, Base: "ceph-msgr-cache-base-" + id, Tier: "ceph-msgr-cache-tier-" + id, Namespace: "cache-probe", Clients: map[string]string{}}}
	c.cachePools = x
	for _, role := range []string{"rw", "ro", "base"} {
		x.Plan.Clients[role] = "client.cache-" + role + "-" + id
	}
	if err := writeIgnoreCacheJSON(out, "osd-ignore-cache-plan.json", x.Plan); err != nil {
		return err
	}
	resources, err := prepareIgnoreCacheResources(ctx, c, out, x.Plan, x.mutate, true)
	x.Resources = resources
	if err != nil {
		return err
	}
	// Text keys are fixture-private consumer inputs, never public diagnostics.
	for _, role := range []string{"rw", "ro", "base"} {
		keyring, err := connectionKeyring(resources.Clients[role].ConnectionConfig())
		if err != nil {
			return err
		}
		found := false
		for _, line := range strings.Split(string(keyring), "\n") {
			k, v, ok := strings.Cut(line, "=")
			if ok && strings.TrimSpace(k) == "key" {
				if err := os.WriteFile(filepath.Join(out, "osd-cache-pools-"+role+".key"), []byte(strings.TrimSpace(v)+"\n"), 0600); err != nil {
					return err
				}
				found = true
			}
		}
		if !found {
			return errors.New("owned cache client key missing")
		}
	}
	if err := c.cachePoolNative(ctx, "prepare", ""); err != nil {
		return err
	}
	return x.save()
}

func (c *fixtureCluster) cachePoolNative(ctx context.Context, action, which string) error {
	x := c.cachePools
	if x == nil {
		return errors.New("cache-pool scope missing")
	}
	argv := []string{"python3", "/out/osd_ignore_cache_integration_oracle.py", "/tmp/ceph-msgr-test", "/out", action}
	if which != "" {
		argv = append(argv, which)
	}
	run := func() error {
		return c.runOracleCommandWithTimeout(ctx, x.Out, argv, "osd-cache-pools-command.json", "closed live Go cache-pool oracle", 3*time.Minute)
	}
	if action == "observe" {
		return run()
	}
	return x.mutate("oracle-"+action+"-"+which, run)
}

func cachePoolCase(which string) bool {
	switch which {
	case "stat-normal", "stat-ignore", "read-normal", "read-ignore", "writefull-normal", "writefull-ignore", "delete-normal", "delete-ignore":
		return true
	}
	return false
}

func (c *fixtureCluster) osdCachePoolsControl(ctx context.Context, out string, fields []string) error {
	x := c.cachePools
	if c.profile.osdCachePools != "1" || x == nil || out != x.Out || len(fields) < 3 || fields[1] != x.Plan.Token || x.FinishAttempted {
		return errors.New("foreign or finished cache-pool control")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= x.LastID {
		return errors.New("stale cache-pool control identity")
	}
	action, which := fields[2], ""
	if action == cacheOpGoAction {
		phase, label, ok := cacheOpGoControl(fields)
		if !ok {
			return errors.New("unsupported Go cache-op observer control")
		}
		x.LastID = id
		return c.cacheOpGoNative(ctx, phase, label)
	}
	if action == "finish" || action == cacheTierOpsAction {
		if len(fields) != 3 {
			return errors.New(action + " takes no case")
		}
	} else {
		if len(fields) != 4 || !cachePoolCase(fields[3]) || (action != "native" && action != "observe") {
			return errors.New("unsupported cache-pool control action")
		}
		which = fields[3]
	}
	x.LastID = id
	if action == "finish" {
		return c.finishOSDCachePools(ctx)
	}
	if action == cacheTierOpsAction {
		return c.cacheTierOpsNative(ctx)
	}
	return c.cachePoolNative(ctx, action, which)
}

func (c *fixtureCluster) finishOSDCachePools(ctx context.Context) (result error) {
	x := c.cachePools
	if x == nil {
		return nil
	}
	if x.FinishAttempted {
		return x.FinishError
	}
	x.FinishAttempted = true
	defer func() {
		x.FinishError = result
		result = errors.Join(result, x.save())
		x.FinishError = result
	}()
	if x.Resources == nil {
		return errors.New("cache-pool setup did not establish owned resources")
	}
	// Finish is selected once. An uncertain native operation or cleanup is
	// retained, never rerun by a later Terminate call.
	if err := c.cachePoolNative(ctx, "finish", ""); err != nil {
		return err
	}
	if err := finishIgnoreCacheResources(ctx, c, x.Out, x.Plan, x.Resources, x.mutate); err != nil {
		return err
	}
	x.Finished = true
	return nil
}

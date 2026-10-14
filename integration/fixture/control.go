package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func (c *fixtureCluster) daemon(kind, name string) (testcontainers.Container, error) {
	if kind == "mon" {
		for _, d := range c.cluster.Monitors() {
			if d.DaemonName == name {
				return d.Container, nil
			}
		}
	}
	if kind == "mgr" {
		for _, d := range c.cluster.Managers() {
			if d.DaemonName == name {
				return d.Container, nil
			}
		}
	}
	return nil, errors.New("fixture daemon not found")
}
func publishAck(out, name string) error {
	f, err := os.CreateTemp(out, "ack-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(out, name))
}
func (c *fixtureCluster) controlLoop(ctx context.Context, out string) {
	defer close(c.done)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.controls(ctx, out); err != nil {
				c.mu.Lock()
				c.controlErr = err
				c.mu.Unlock()
				_ = os.WriteFile(filepath.Join(out, "provider.error"), []byte(err.Error()+"\n"), 0600)
				return
			}
		}
	}
}
func (c *fixtureCluster) request(out, name string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(out, name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return nil, errors.New("empty fixture request")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 {
		return nil, errors.New("invalid fixture request identity")
	}
	if err = os.Remove(filepath.Join(out, name)); err != nil {
		return nil, err
	}
	return fields, nil
}
func (c *fixtureCluster) controls(ctx context.Context, out string) error {
	if fileExists(filepath.Join(out, "native-oracles.error")) {
		return errors.New("native oracle worker failed; inspect native-oracles.log")
	}
	for _, kind := range []string{"mon", "mgr"} {
		for _, action := range []string{"pause", "resume", "stop", "start"} {
			fields, err := c.request(out, action+"-"+kind)
			if err != nil {
				return err
			}
			if fields == nil {
				continue
			}
			if len(fields) != 2 {
				return errors.New("invalid daemon control request")
			}
			name := fields[1]
			daemon, err := c.daemon(kind, name)
			if err != nil {
				return err
			}
			key := kind + "." + name
			switch action {
			case "pause":
				if c.pauses[key] != nil {
					return errors.New("daemon already paused")
				}
				c.pauses[key], err = c.cluster.PauseContainer(ctx, daemon)
			case "resume":
				if c.pauses[key] == nil {
					return errors.New("daemon is not paused")
				}
				err = c.pauses[key].Resume(ctx)
				if err == nil {
					delete(c.pauses, key)
				}
			case "stop":
				err = daemon.Stop(ctx, nil)
			case "start":
				err = daemon.Start(ctx)
			}
			if err != nil {
				return err
			}
			if err = publishAck(out, kind+"-"+action+"."+fields[0]); err != nil {
				return err
			}
		}
	}
	fields, err := c.request(out, "restart-mon")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 {
			return errors.New("invalid restart request")
		}
		d, err := c.daemon("mon", fields[1])
		if err != nil {
			return err
		}
		if err = d.Stop(ctx, nil); err != nil {
			return err
		}
		if err = d.Start(ctx); err != nil {
			return err
		}
		if err = c.waitReady(ctx, false); err != nil {
			return err
		}
		if err = publishAck(out, "mon-restarted."+fields[0]); err != nil {
			return err
		}
	}
	for _, action := range []string{"stop", "start"} {
		fields, err = c.request(out, action+"-mons")
		if err != nil {
			return err
		}
		if fields == nil {
			continue
		}
		if len(fields) != 1 {
			return errors.New("invalid quorum request")
		}
		// MON a's Start waits for CLI readiness: restore a quorum with b/c first.
		for _, name := range []string{"b", "c", "a"} {
			d, e := c.daemon("mon", name)
			if e != nil {
				return e
			}
			if action == "stop" {
				e = d.Stop(ctx, nil)
			} else {
				e = d.Start(ctx)
			}
			if e != nil {
				return e
			}
		}
		if action == "start" {
			if err = c.waitReady(ctx, false); err != nil {
				return err
			}
		}
		ack := map[string]string{"stop": "mons-stopped.", "start": "mons-started."}[action]
		if err = publishAck(out, ack+fields[0]); err != nil {
			return err
		}
	}
	if fileExists(filepath.Join(out, "stop-mon-a")) && !fileExists(filepath.Join(out, "mon-a-stopped")) {
		d, e := c.daemon("mon", "a")
		if e != nil {
			return e
		}
		if e = d.Stop(ctx, nil); e != nil {
			return e
		}
		if e = publishAck(out, "mon-a-stopped"); e != nil {
			return e
		}
	}
	if fileExists(filepath.Join(out, "start-mgrs")) && !fileExists(filepath.Join(out, "mgrs-started")) {
		for _, name := range []string{"a", "b"} {
			if _, err = c.cluster.AddManager(ctx, name); err != nil {
				return err
			}
		}
		if err = c.waitReady(ctx, true); err != nil {
			return err
		}
		if err = publishAck(out, "mgrs-started"); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "secure-mgr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.modeRejection != "mgr" {
			return errors.New("invalid policy request")
		}
		if _, err = c.cluster.TemporaryMessengerPolicy(ctx, ceph.MessengerPolicy{Managers: []ceph.MessengerConnectionMode{ceph.MessengerSecure}}); err != nil {
			return err
		}
		for _, d := range c.cluster.Managers() {
			if err = d.Stop(ctx, nil); err != nil {
				return err
			}
			if err = d.Start(ctx); err != nil {
				return err
			}
		}
		if err = c.waitReady(ctx, true); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-secure."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-epoch")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "advance" || c.profile.osd != "1" {
			return errors.New("invalid OSD epoch request")
		}
		dump, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(out, "osd-map-epoch.json"), dump, 0600); err != nil {
			return err
		}
		var expected struct {
			Epoch uint32 `json:"epoch"`
			FSID  string `json:"fsid"`
		}
		if err = json.Unmarshal(dump, &expected); err != nil {
			return err
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			data, e := c.cluster.AdminSocket(ctx, "osd.0", "status")
			if e != nil {
				return e
			}
			var applied struct {
				FSID   string `json:"cluster_fsid"`
				Newest string `json:"newest_map"`
				Lower  uint32 `json:"cluster_osdmap_trim_lower_bound"`
			}
			if e = json.Unmarshal(data, &applied); e != nil {
				return e
			}
			epoch, _ := strconv.ParseUint(applied.Newest, 10, 32)
			if applied.FSID == expected.FSID && epoch >= uint64(expected.Epoch) && applied.Lower > 1 {
				if err = os.WriteFile(filepath.Join(out, "osd-map-applied.json"), data, 0600); err != nil {
					return err
				}
				break
			}
			if time.Now().After(deadline) {
				return errors.New("OSD did not apply/trim requested epoch")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if err = publishAck(out, "osd-epoch-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}
func (c *fixtureCluster) prepareOSD(ctx context.Context, out string) error {
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
	if _, err := c.cluster.Ceph(ctx, "balancer", "off"); err != nil {
		return err
	}
	_, err := c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: "ceph-msgr-osd-fixture", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true})
	if err != nil {
		return fmt.Errorf("create legacy pool: %w", err)
	}
	// MGR modules create .mgr independently of CreatePool(NoHashPSPool).
	// Its flag contributes to MON's required features even for a command-only
	// client. Change only this known internal pool, then verify every pool.
	before, err := c.poolPolicy(ctx)
	if err != nil {
		return err
	}
	for _, pool := range before.Pools {
		if pool.Name == ".mgr" && strings.Contains(","+pool.FlagNames+",", ",hashpspool,") {
			if _, err = c.cluster.Ceph(ctx, "osd", "pool", "set", ".mgr", "hashpspool", "false", "--yes-i-really-mean-it"); err != nil {
				return err
			}
		}
	}
	after, err := c.poolPolicy(ctx)
	if err != nil {
		return err
	}
	for _, pool := range after.Pools {
		if strings.Contains(","+pool.FlagNames+",", ",hashpspool,") {
			return fmt.Errorf("legacy fixture still requires HASHPSPOOL in pool %s", pool.Name)
		}
	}
	evidence, err := json.MarshalIndent(map[string]any{"before": before, "after": after}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "pool-policy.json"), evidence, 0600)
}

type poolPolicy struct {
	Epoch uint32 `json:"epoch"`
	FSID  string `json:"fsid"`
	Pools []struct {
		Name      string `json:"pool_name"`
		Flags     uint64 `json:"flags"`
		FlagNames string `json:"flags_names"`
	} `json:"pools"`
}

func (c *fixtureCluster) poolPolicy(ctx context.Context) (poolPolicy, error) {
	data, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	var result poolPolicy
	if err == nil {
		err = json.Unmarshal(data, &result)
	}
	return result, err
}

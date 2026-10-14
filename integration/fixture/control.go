package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	if kind == "osd" {
		for _, d := range c.cluster.OSDs() {
			if strconv.Itoa(d.ID) == name {
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
	for _, kind := range []string{"mon", "mgr", "osd"} {
		for _, action := range []string{"pause", "resume", "stop", "start", "kill"} {
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
			case "kill":
				if kind != "osd" {
					return errors.New("forced crash is restricted to disposable OSDs")
				}
				zero := time.Duration(0)
				err = daemon.Stop(ctx, &zero)
				if err == nil {
					state, stateErr := daemon.State(ctx)
					if stateErr != nil {
						return stateErr
					}
					if state == nil || state.Running || state.ExitCode != 137 || state.OOMKilled {
						return errors.New("OSD forced crash did not produce a non-OOM SIGKILL receipt")
					}
					data, encodeErr := json.Marshal(map[string]any{"daemon": key, "id": daemon.GetContainerID(), "exit_code": state.ExitCode, "oom_killed": state.OOMKilled})
					if encodeErr != nil {
						return encodeErr
					}
					if err = os.WriteFile(filepath.Join(out, "osd-crash-state.json"), data, 0644); err != nil {
						return err
					}
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
	fields, err := c.request(out, "verify-osd-ready")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "clean" || c.profile.osd != "1" {
			return errors.New("invalid OSD clean readiness request")
		}
		readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := c.cluster.WaitForClean(readyCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("restore native OSD fixture readiness: %w", err)
		}
		if err := publishAck(out, "osd-ready-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-compound")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "snapshot" || c.profile.osd != "1" {
			return errors.New("invalid compound oracle request")
		}
		if err = c.exec(ctx, "python3", "/out/osd_compound_oracle.py", "/tmp/ceph-msgr-test", "/out"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-compound-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-mgr-auth")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.nativeMgrPreparation != "1" {
			return errors.New("native MGR rejection observation is restricted to its opt-in acceptance fixture")
		}
		daemon, err := c.daemon("mgr", fields[1])
		if err != nil {
			return err
		}
		status, err := c.cluster.ManagerStatus(ctx)
		if err != nil {
			return err
		}
		rejection, err := nativeManagerRejection(ctx, daemon)
		if err != nil {
			return err
		}
		present := status.ActiveName == fields[1]
		for _, standby := range status.Standbys {
			present = present || standby.Name == fields[1]
		}
		data, err := json.Marshal(map[string]any{"daemon": "mgr." + fields[1], "missing": !present, "native_rejection": rejection, "status": status})
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(out, "manager-native-auth.json"), data, 0644); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-auth-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "prepare-mgr-failover")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "ready" || c.profile.mgrCount != "2" || c.profile.authEpoch == "1" || c.profile.idleSessions == "1" || c.profile.shortTickets == "1" || c.profile.expireTickets == "1" || c.profile.modeRejection != "" {
			return errors.New("invalid ordinary MGR failover preparation request")
		}
		if err = c.prepareManagerFailover(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-failover-prepare."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-mutation")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "compare" || c.profile.osd != "1" {
			return errors.New("invalid mutation oracle request")
		}
		if err = c.exec(ctx, "python3", "/out/osd_mutation_oracle.py", "/tmp/ceph-msgr-test", "/out"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mutation-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-xattr-native")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "matrix" || c.profile.osd != "1" {
			return errors.New("invalid native xattr oracle request")
		}
		if err = c.runXattrOracle(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "osd-xattr-native-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "restart-mon")
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
		if len(fields) != 3 || fields[1] != "advance" || c.profile.osd != "1" {
			return errors.New("invalid OSD epoch request")
		}
		selected, err := strconv.ParseInt(fields[2], 10, 32)
		if err != nil || selected < 0 {
			return errors.New("invalid OSD epoch target")
		}
		if _, err = c.daemon("osd", fields[2]); err != nil {
			return err
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
		if expected.Epoch == 0 || expected.FSID == "" || expected.FSID != c.profile.fsid {
			return errors.New("invalid native OSD epoch identity")
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			data, e := c.cluster.AdminSocket(ctx, "osd."+fields[2], "status")
			if e != nil {
				return e
			}
			ready, e := osdAppliedEpoch(data, int32(selected), expected.FSID, expected.Epoch)
			if e != nil {
				return e
			}
			if ready {
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

func (c *fixtureCluster) runXattrOracle(ctx context.Context, out string) error {
	result, runErr := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
		Argv:    []string{"python3", "/out/osd_xattr_native.py", "/tmp/ceph-msgr-test", "/out"},
		Timeout: 80 * time.Second, MaxStdout: 64 << 10, MaxStderr: 16 << 10,
		Label: "fixed native xattr matrix",
	})
	// This fixed oracle contains only disposable object data and ordinary
	// native API errors. Preserve its bounded first-failure traceback rather
	// than discarding it with the generic control command's output.
	data, err := json.Marshal(result)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, "osd-xattr-native-command.json"), data, 0644)
	}
	if runErr != nil || err != nil {
		return errors.Join(runErr, err)
	}
	if !result.Complete() || result.ExitCode != 0 {
		return fmt.Errorf("native xattr matrix exit=%d complete=%t; see osd-xattr-native-command.json", result.ExitCode, result.Complete())
	}
	return nil
}

func osdAppliedEpoch(data []byte, selected int32, fsid string, expected uint32) (bool, error) {
	var applied struct {
		Who    *int32 `json:"whoami"`
		FSID   string `json:"cluster_fsid"`
		Newest string `json:"newest_map"`
		Lower  uint32 `json:"cluster_osdmap_trim_lower_bound"`
	}
	if err := json.Unmarshal(data, &applied); err != nil {
		return false, err
	}
	if applied.Who == nil || *applied.Who != selected || fsid == "" || applied.FSID != fsid {
		return false, errors.New("native OSD applied-map receipt belongs to a different daemon or cluster")
	}
	newest, err := strconv.ParseUint(applied.Newest, 10, 32)
	if err != nil || newest == 0 || uint64(applied.Lower) > newest {
		return false, errors.New("invalid native OSD applied-map epoch/trim receipt")
	}
	return newest >= uint64(expected) && applied.Lower > 1, nil
}

// managerAuthRejection recognizes an explicit native MonClient refusal in the
// latest ceph-mgr process only. Old refusals from an earlier respawn cannot
// authorize another restart. No Go subject's authentication is retried here.
// Pinned MonClient.cc:1968-1998 and CephxServiceHandler.cc:253-260 explain the
// failure after the MON no longer retains the native daemon's AUTH secret.
func managerAuthRejection(log string) string {
	started := strings.LastIndex(log, " ceph version ")
	if started < 0 {
		return ""
	}
	for _, line := range strings.Split(log[started:], "\n") {
		if strings.Contains(line, "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied") {
			return line
		}
	}
	return ""
}

func nativeManagerRejection(ctx context.Context, daemon testcontainers.Container) (string, error) {
	logs, err := daemon.Logs(ctx)
	if err != nil {
		return "", err
	}
	data, readErr := io.ReadAll(io.LimitReader(logs, (8<<20)+1))
	closeErr := logs.Close()
	if readErr != nil || closeErr != nil {
		return "", errors.Join(readErr, closeErr)
	}
	if len(data) > 8<<20 {
		return "", errors.New("native MGR preparation log exceeds bound")
	}
	return managerAuthRejection(string(data)), nil
}

func managerFailoverReady(status ceph.ManagerStatus, names map[string]bool) bool {
	return len(names) == 2 && status.Available && status.ActiveGID != 0 && names[status.ActiveName] &&
		len(status.Standbys) == 1 && names[status.Standbys[0].Name] && status.Standbys[0].Name != status.ActiveName && status.Standbys[0].GID != 0
}

func (c *fixtureCluster) prepareManagerFailover(ctx context.Context, out string) error {
	preparation, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	names := make(map[string]bool)
	for _, d := range c.cluster.Managers() {
		names[d.DaemonName] = true
	}
	if len(names) != 2 {
		return errors.New("ordinary MGR failover requires two owned daemons")
	}
	restarted := make(map[string]bool)
	for {
		status, err := c.cluster.ManagerStatus(preparation)
		if err != nil {
			return err
		}
		if managerFailoverReady(status, names) {
			return nil
		}
		present := map[string]bool{status.ActiveName: true}
		for _, standby := range status.Standbys {
			present[standby.Name] = true
		}
		for _, d := range c.cluster.Managers() {
			if present[d.DaemonName] || restarted[d.DaemonName] {
				continue
			}
			rejection, err := nativeManagerRejection(preparation, d)
			if err != nil {
				return err
			}
			if rejection == "" {
				continue
			}
			// This restores a missing disposable fixture daemon before an
			// independent failover test. Preserve the first native refusal and
			// current map identity before the once-only public container restart.
			// The measured Go mutation/authentication has not been submitted.
			receipt, err := json.Marshal(map[string]any{"observed_at": time.Now().UTC(), "daemon": "mgr." + d.DaemonName, "native_rejection": rejection,
				"active_name": status.ActiveName, "active_gid": status.ActiveGID, "standbys": status.Standbys, "action": "restore missing native daemon before independent failover"})
			if err != nil {
				return err
			}
			f, err := os.OpenFile(filepath.Join(out, "manager-preparation.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			_, writeErr := f.Write(append(receipt, '\n'))
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				return errors.Join(writeErr, closeErr)
			}
			restarted[d.DaemonName] = true
			if err := d.Stop(preparation, nil); err != nil {
				return err
			}
			if err := d.Start(preparation); err != nil {
				return err
			}
		}
		select {
		case <-preparation.Done():
			return fmt.Errorf("native MGR failover fixture never restored active and standby: %w", preparation.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
func (c *fixtureCluster) prepareOSD(ctx context.Context, out string) error {
	count, _ := strconv.Atoi(c.profile.osdReplicas)
	minimum := 1
	if count == 3 {
		minimum = 2
		path := filepath.Join(out, "legacy.crush.txt")
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := strings.Replace(string(source), "device 0 osd.0", "device 0 osd.0\ndevice 1 osd.1\ndevice 2 osd.2", 1)
		text = strings.Replace(text, "item osd.0 weight 1.00000", "item osd.0 weight 1.00000\n    item osd.1 weight 1.00000\n    item osd.2 weight 1.00000", 1)
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			return err
		}
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
	if _, err := c.cluster.Ceph(ctx, "balancer", "off"); err != nil {
		return err
	}
	_, err := c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: "ceph-msgr-osd-fixture", PGNum: 1, Replicas: count, MinSize: minimum, NoHashPSPool: true})
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

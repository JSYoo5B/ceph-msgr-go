package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
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
				if ctx.Err() == nil {
					err = c.classifyControlFailure(out, err)
				}
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
	if fields, err := c.request(out, "observe-named-mgr"); err != nil {
		return err
	} else if fields != nil {
		if err := c.namedManagerOracle(ctx, out, fields); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-commands-native"); err != nil {
		return err
	} else if fields != nil {
		if err := c.nativeMDSCommands(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-commands-native-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-sessions"); err != nil {
		return err
	} else if fields != nil {
		if err := c.mdsSessionsControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-sessions-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-cephfs"); err != nil {
		return err
	} else if fields != nil {
		if err := c.mdsCephFSControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-cephfs-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-cephfs-ranks"); err != nil {
		return err
	} else if fields != nil {
		if err := c.mdsCephFSRanksControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-cephfs-ranks-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-cephfs-completion"); err != nil {
		return err
	} else if fields != nil {
		if err := c.mdsCephFSCompletionControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-cephfs-completion-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-cephfs-data"); err != nil {
		return err
	} else if fields != nil {
		if err := c.mdsCephFSDataControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-cephfs-data-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-base"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdBaseControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-base-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-write"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdWriteControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-write-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-lock"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdLockControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-lock-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-clone"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdCloneControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-clone-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "rbd-journal"); err != nil {
		return err
	} else if fields != nil {
		if err := c.rbdJournalControl(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "rbd-journal-completed."+fields[0]); err != nil {
			return err
		}
	}
	if fields, err := c.request(out, "mds-fresh-observe"); err != nil {
		return err
	} else if fields != nil {
		if err := c.observeMDSFresh(ctx, out, fields); err != nil {
			return err
		}
		if err := publishAck(out, "mds-fresh-observed."+fields[0]); err != nil {
			return err
		}
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
				// Docker stop sends SIGTERM first even with a zero timeout,
				// and an OSD with osd_fast_shutdown may then exit 0 before the
				// SIGKILL. Send SIGKILL itself so the 137 receipt holds.
				var state *container.State
				state, err = forceKill(ctx, dockerKill(daemon.GetContainerID()), daemon.State, forcedKillTimeout)
				if err == nil {
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
	fields, err := c.request(out, "wait-monitor-election")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 {
			return errors.New("invalid settled MON election request")
		}
		if err := c.settleMonitorElection(ctx, out, fields[0], fields[1]); err != nil {
			return err
		}
		if err := publishAck(out, "monitor-election-settled."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-ready")
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
		rejection, _, err := nativeManagerRejection(ctx, daemon)
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
	fields, err = c.request(out, "verify-mgr-keyring")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || !shortTicketGateLabels[fields[1]] || c.profile.shortTickets != "1" {
			return errors.New("native MGR keyring gate is restricted to the short-ticket fixture")
		}
		if err = c.verifyShortTicketGate(ctx, out, fields[1]); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-keyring-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "collect-auth-bundle")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 {
			return errors.New("invalid auth first-failure bundle request")
		}
		if err = c.collectAuthBundleControl(ctx, out, fields[0], fields[1]); err != nil {
			return err
		}
		if err = publishAck(out, "auth-bundle-collect."+fields[0]); err != nil {
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
		if err = c.runMutationCommand(ctx, out); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mutation-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "limit-osd-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid scoped xattr limit request")
		}
		var scope *xattrLimitsScope
		switch fields[1] {
		case "apply":
			if c.xattrLimits != nil {
				return errors.New("xattr limit scope already owned")
			}
			c.xattrLimits, err = beginXattrLimits(ctx, c.cluster)
			scope = c.xattrLimits
		case "restore":
			if c.xattrLimits == nil {
				return errors.New("xattr limit scope is not owned")
			}
			scope = c.xattrLimits
			err = scope.Restore(ctx)
			if err == nil {
				c.xattrLimits = nil
			}
		default:
			return errors.New("invalid xattr limit action")
		}
		if scope != nil {
			data, encodeErr := json.Marshal(scope.Receipt())
			if encodeErr == nil {
				encodeErr = os.WriteFile(filepath.Join(out, "osd-xattr-limits.json"), data, 0600)
			}
			err = errors.Join(err, encodeErr)
		}
		if err != nil {
			return fmt.Errorf("scoped native xattr limits: %w", err)
		}
		if err = publishAck(out, "osd-xattr-limit."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-omap")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid native OMAP oracle request")
		}
		switch fields[1] {
		case "prepare", "observe", "compare", "cleanup":
		default:
			return errors.New("invalid native OMAP oracle action")
		}
		args := []string{"python3", "/out/osd_omap_native.py", "--" + fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-omap-command.json", "native OMAP oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-omap-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-omap-write")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" {
			return errors.New("invalid native OMAP write oracle request")
		}
		switch fields[1] {
		case "prepare", "observe", "assert", "exec", "compare", "cleanup",
			"snapcreate", "snapwrite", "snapread", "snaprollback", "listsnaps":
		default:
			return errors.New("invalid native OMAP write oracle action")
		}
		args := []string{"python3", "/out/osd_omap_write_native.py", "--" + fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-omap-write-command.json", "native OMAP write oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-omap-write-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-backoff")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || !c.profile.osdPGV8() || c.profile.osdReplicas != "1" ||
			!slices.Contains([]string{"start", "observe", "finish"}, fields[1]) {
			return errors.New("invalid owned single-OSD backoff oracle request")
		}
		args := []string{"python3", "/out/osd_backoff_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-backoff-command.json", "native backoff PG-state oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-backoff-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-object-backoff")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || !c.profile.osdPGV8() || c.profile.osdReplicas != "3" || c.profile.osdStore != "bluestore" ||
			!slices.Contains(objectBackoffActions, fields[1]) {
			return errors.New("invalid owned three-replica object backoff oracle request")
		}
		args := []string{"python3", "/out/osd_object_backoff_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommandWithTimeout(ctx, out, args, "osd-object-backoff-command.json", "native object backoff recipe", objectBackoffOracleTimeout); err != nil {
			return err
		}
		if err = publishAck(out, "osd-object-backoff-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-upmap")
	if err != nil {
		return err
	}
	if fields != nil {
		if err := c.upmapControl(ctx, out, fields); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-mds-maps")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.mdsFresh == "1" || c.profile.osd != "1" || !c.profile.osdPGV8() || c.profile.osdReplicas != "1" ||
			!slices.Contains([]string{"start", "observe", "finish"}, fields[1]) {
			return errors.New("invalid owned single-OSD MDS maps oracle request")
		}
		args := []string{"python3", "/out/mds_maps_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "mds-maps-command.json", "native MON MDS maps oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "mds-maps-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-split")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || !c.profile.osdPGV8() || c.profile.osdReplicas != "1" ||
			!slices.Contains([]string{"start", "observe", "finish"}, fields[1]) {
			return errors.New("invalid owned single-OSD split oracle request")
		}
		args := []string{"python3", "/out/osd_split_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-split-command.json", "native split PG-state oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-split-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-watch-notify")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || (fields[1] != "start" && fields[1] != "observe" && fields[1] != "finish") {
			return errors.New("invalid native watch notify oracle request")
		}
		args := []string{"python3", "/out/osd_watch_notify_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-watch-notify-command.json", "native watch notify oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-watch-notify-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-sparse-read")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native sparse-read oracle request")
		}
		args := []string{"python3", "/out/osd_sparse_read_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-sparse-read-command.json", "native sparse-read oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-sparse-read-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-pg-list")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native pg-list oracle request")
		}
		args := []string{"python3", "/out/osd_pg_list_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-pg-list-command.json", "native pg-list oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-pg-list-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-u64-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || c.profile.u64XattrOracle == "" || fields[1] != "compare" {
			return errors.New("invalid native U64 xattr oracle request")
		}
		args := []string{"python3", "/out/osd_u64_xattr_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-u64-xattr-command.json", "native U64 xattr oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-u64-xattr-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-writesame")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || c.profile.writeSameOracle == "" || fields[1] != "compare" {
			return errors.New("invalid native WRITESAME oracle request")
		}
		args := []string{"python3", "/out/osd_writesame_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-writesame-command.json", "native WRITESAME oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-writesame-verify."+fields[0]); err != nil {
			return err
		}
	}

	fields, err = c.request(out, "verify-osd-checksum")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "compare" {
			return errors.New("invalid native checksum oracle request")
		}
		args := []string{"python3", "/out/osd_checksum_oracle.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-checksum-command.json", "native checksum oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-checksum-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-mapext-range")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native exact-range extent map oracle request")
		}
		args := []string{"python3", "/out/osd_mapext_range_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-mapext-range-command.json", "native exact-range extent map oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mapext-range-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-mapext")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "observe" {
			return errors.New("invalid native extent map oracle request")
		}
		args := []string{"python3", "/out/osd_mapext_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-mapext-command.json", "native extent map oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-mapext-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-watchers")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || (fields[1] != "start" && fields[1] != "observe" && fields[1] != "stop") {
			return errors.New("invalid native watcher oracle request")
		}
		args := []string{"python3", "/out/osd_watchers_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-watchers-command.json", "native watcher oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-watchers-verify."+fields[0]); err != nil {
			return err
		}
	}
	fields, err = c.request(out, "verify-osd-command")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || fields[1] != "run" {
			return errors.New("invalid native OSD command oracle request")
		}
		args := []string{"python3", "/out/osd_command_native.py", "/tmp/ceph-msgr-test", "/out"}
		if err = c.runOracleCommand(ctx, out, args, "osd-command-command.json", "native OSD command oracle"); err != nil {
			return err
		}
		if err = publishAck(out, "osd-command-verify."+fields[0]); err != nil {
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
	fields, err = c.request(out, "verify-osd-xattr")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || fields[1] != "compare" || c.profile.osd != "1" {
			return errors.New("invalid xattr comparison oracle request")
		}
		if err = c.runXattrCommand(ctx, out, true); err != nil {
			return err
		}
		if err = publishAck(out, "osd-xattr-verify."+fields[0]); err != nil {
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
		// Start waits only for the admin socket, and a pre-stop registration
		// can outlive its process; acknowledge only an accepted MGR
		// transition receipt with new GIDs and beacon sessions.
		if err = c.restartManagersWithReceipt(ctx, out, fields[0]); err != nil {
			return err
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
	return c.runXattrCommand(ctx, out, false)
}

func (c *fixtureCluster) runXattrCommand(ctx context.Context, out string, compare bool) error {
	args := []string{"python3", "/out/osd_xattr_native.py"}
	receipt := "osd-xattr-native-command.json"
	if compare {
		args = append(args, "--compare")
		receipt = "osd-xattr-command.json"
	}
	args = append(args, "/tmp/ceph-msgr-test", "/out")
	return c.runOracleCommand(ctx, out, args, receipt, "native xattr oracle")
}

func (c *fixtureCluster) runMutationCommand(ctx context.Context, out string) error {
	return c.runOracleCommand(ctx, out,
		[]string{"python3", "/out/osd_mutation_oracle.py", "/tmp/ceph-msgr-test", "/out"},
		"osd-mutation-command.json", "native mutation oracle")
}

// The object-backoff DOWN phase must outlast its configured heartbeat grace,
// including native query time and the monitor/PG transition. Other oracles
// retain their existing 80-second command bound.
const objectBackoffOracleTimeout = 150 * time.Second

func (c *fixtureCluster) runOracleCommand(ctx context.Context, out string, args []string, receipt, label string) error {
	return c.runOracleCommandWithTimeout(ctx, out, args, receipt, label, 80*time.Second)
}

func (c *fixtureCluster) runOracleCommandWithTimeout(ctx context.Context, out string, args []string, receipt, label string, timeout time.Duration) error {
	result, runErr := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
		Argv:    args,
		Timeout: timeout, MaxStdout: 64 << 10, MaxStderr: 16 << 10,
		Label: label,
	})
	// This oracle contains only disposable object data and ordinary
	// native API errors. Preserve its bounded first-failure traceback rather
	// than discarding it with the generic control command's output.
	data, err := json.Marshal(result)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, receipt), data, 0644)
	}
	if runErr != nil || err != nil {
		return errors.Join(runErr, err)
	}
	if !result.Complete() || result.ExitCode != 0 {
		return fmt.Errorf("%s exit=%d complete=%t; see %s", label, result.ExitCode, result.Complete(), receipt)
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
// It also returns the latest process-start line, which bounds MON attribution.
func managerAuthRejection(log string) (rejection, start string) {
	started := strings.LastIndex(log, " ceph version ")
	if started < 0 {
		return "", ""
	}
	lineStart := strings.LastIndexByte(log[:started], '\n') + 1
	lines := strings.Split(log[lineStart:], "\n")
	for _, line := range lines {
		if strings.Contains(line, "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied") {
			rejection = line
		}
	}
	return rejection, lines[0]
}

func nativeManagerRejection(ctx context.Context, daemon testcontainers.Container) (rejection, start string, err error) {
	logs, err := daemon.Logs(ctx)
	if err != nil {
		return "", "", err
	}
	data, readErr := io.ReadAll(io.LimitReader(logs, (8<<20)+1))
	closeErr := logs.Close()
	if readErr != nil || closeErr != nil {
		return "", "", errors.Join(readErr, closeErr)
	}
	if len(data) > 8<<20 {
		return "", "", errors.New("native MGR preparation log exceeds bound")
	}
	rejection, start = managerAuthRejection(string(data))
	return rejection, start, nil
}

// Pinned src/log/LogClock.h:139-164 prints coarse millisecond or fine
// microsecond local time with a numeric zone. Docker daemons on one host share
// the kernel clock, so MON and MGR entries are comparable instants.
var cephLogEntry = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.(?:\d{3}|\d{6})[+-]\d{4}) [0-9a-f]+ +-?\d+ (.*)$`)

func cephLogTime(stamp string) (time.Time, error) {
	layout := "2006-01-02T15:04:05.000-0700"
	if len(stamp) == len("2006-01-02T15:04:05.000000-0700") {
		layout = "2006-01-02T15:04:05.000000-0700"
	}
	return time.Parse(layout, stamp)
}

// cephLogEntryTime parses the leading timestamp of one native log entry.
func cephLogEntryTime(line string) (time.Time, string, error) {
	entry := cephLogEntry.FindStringSubmatch(strings.TrimSuffix(line, "\r"))
	if entry == nil {
		return time.Time{}, "", errors.New("line is not a timestamped native Ceph log entry")
	}
	stamp, err := cephLogTime(entry[1])
	return stamp, entry[2], err
}

// monitorReclaimLogLimit bounds candidate refusal evidence from each MON.
// Unrelated debug history is streamed without being retained by the filter.
// objectBackoffActions is the closed native recipe action set of
// tools/osd_object_backoff_oracle.py.
var objectBackoffActions = []string{"prepare", "config", "wait-osd", "put", "wait-degraded", "native-start", "native-finish", "native-stat", "ops", "ack-history", "restore"}

const monitorReclaimLogLimit = 32 << 20

// The three refusals in pinned CephxServiceHandler.cc:111-113, 126-128 and
// 141-143 share this dout prefix (line 30) and name the global_id whose
// reclaim the MON refused with -EACCES.
var monitorReclaimSuffixes = []string{" without presenting ticket", " using bad ticket", " using mismatching ticket"}

type countedReader struct {
	r io.Reader
	n int64
}

func (c *countedReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// scanMonitorReclaimRefusals returns, sorted and unique, the global IDs one MON
// log refused to reclaim for mgr.<name> at or after since. A matching line that
// is not an exact native entry is an explicit error rather than silent absence.
func scanMonitorReclaimRefusals(r io.Reader, name string, since time.Time, limit int) ([]uint64, error) {
	if name == "" || limit <= 0 || since.IsZero() {
		return nil, errors.New("invalid native MON reclaim attribution scope")
	}
	prefix := "cephx server mgr." + name + ":  attempt to reclaim global_id "
	counted := &countedReader{r: io.LimitReader(r, int64(limit)+1)}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 0, 64<<10), limit+1)
	found := make(map[uint64]bool)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, prefix) {
			continue
		}
		stamp, message, err := cephLogEntryTime(line)
		if err != nil || !strings.HasPrefix(message, prefix) {
			return nil, fmt.Errorf("malformed native MON reclaim refusal for mgr.%s: %q", name, line)
		}
		if stamp.Before(since) {
			continue
		}
		value, suffix, _ := strings.Cut(message[len(prefix):], " ")
		suffix = " " + suffix
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil || id == 0 || value[0] == '0' || !slices.Contains(monitorReclaimSuffixes, suffix) {
			return nil, fmt.Errorf("malformed native MON reclaim global_id for mgr.%s: %q", name, line)
		}
		found[id] = true
	}
	if counted.n > int64(limit) {
		return nil, fmt.Errorf("native MON log exceeds %d-byte reclaim attribution bound", limit)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(found)), nil
}

type monitorLogSource struct {
	name string
	logs func(context.Context) (io.ReadCloser, error)
}

// monitorReclaimRefusals unions every owned MON's attribution. A missing MON
// set is an error: no observation must never read as "nothing refused".
func monitorReclaimRefusals(ctx context.Context, monitors []monitorLogSource, name string, since time.Time) ([]uint64, error) {
	if len(monitors) == 0 {
		return nil, errors.New("no owned MON logs for native MGR refusal attribution")
	}
	found := make(map[uint64]bool)
	for _, mon := range monitors {
		logs, err := mon.logs(ctx)
		if err != nil {
			return nil, fmt.Errorf("read mon.%s log for native MGR refusal attribution: %w", mon.name, err)
		}
		stopClose := context.AfterFunc(ctx, func() { _ = logs.Close() })
		ids, scanErr := scanMonitorReclaimRefusals(newMonitorReclaimLogReader(ctx, logs, name), name, since, monitorReclaimLogLimit)
		stopClose()
		if err := errors.Join(scanErr, logs.Close()); err != nil {
			return nil, fmt.Errorf("mon.%s native MGR refusal attribution: %w", mon.name, err)
		}
		for _, id := range ids {
			found[id] = true
		}
	}
	return slices.Sorted(maps.Keys(found)), nil
}

func managerFailoverReady(status ceph.ManagerStatus, names map[string]bool) bool {
	return len(names) == 2 && status.Available && status.ActiveGID != 0 && names[status.ActiveName] &&
		len(status.Standbys) == 1 && names[status.Standbys[0].Name] && status.Standbys[0].Name != status.ActiveName &&
		status.Standbys[0].GID != 0 && status.Standbys[0].GID != status.ActiveGID
}

// nativeManagerRefusal reads one owned MGR's latest-process MonClient -13 line
// and attributes it to MonClient global IDs using MON evidence from the same
// process only. With scanAlways the MONs are read even without a -13 line.
func (c *fixtureCluster) nativeManagerRefusal(ctx context.Context, name string, daemon testcontainers.Container, scanAlways bool) (managerRefusal, error) {
	line, start, err := nativeManagerRejection(ctx, daemon)
	if err != nil || line == "" && !scanAlways {
		return managerRefusal{Line: line}, err
	}
	started, _, err := cephLogEntryTime(start)
	if err != nil {
		return managerRefusal{}, fmt.Errorf("native mgr.%s process start %q: %w", name, start, err)
	}
	var monitors []monitorLogSource
	for _, mon := range c.cluster.Monitors() {
		monitors = append(monitors, monitorLogSource{name: mon.DaemonName, logs: mon.Logs})
	}
	refused, err := monitorReclaimRefusals(ctx, monitors, name, started)
	if err != nil {
		return managerRefusal{}, err
	}
	return managerRefusal{Line: line, Refused: refused}, nil
}

// nativeManagerMapTimeout bounds one native `mgr dump`. A native CLI whose
// MGR peer crashed mid-activation can hang; one call must not consume a whole
// preparation budget silently.
const nativeManagerMapTimeout = 15 * time.Second

func (c *fixtureCluster) observeManagerMap(ctx context.Context) (managerPreparationMap, error) {
	call, cancel := context.WithTimeout(ctx, nativeManagerMapTimeout)
	defer cancel()
	data, err := c.cluster.Ceph(call, "mgr", "dump", "--format", "json")
	var state managerPreparationMap
	if err == nil {
		err = json.Unmarshal(data, &state)
	} else if call.Err() != nil && ctx.Err() == nil {
		err = fmt.Errorf("native mgr dump exceeded %v: %w", nativeManagerMapTimeout, err)
	}
	return state, err
}

func (c *fixtureCluster) ownedManagers() (map[string]testcontainers.Container, []string, error) {
	daemons := make(map[string]testcontainers.Container)
	var names []string
	for _, d := range c.cluster.Managers() {
		if _, duplicate := daemons[d.DaemonName]; duplicate {
			return nil, nil, errors.New("duplicate owned MGR preparation identity")
		}
		daemons[d.DaemonName] = d.Container
		names = append(names, d.DaemonName)
	}
	return daemons, names, nil
}

func (c *fixtureCluster) prepareManagerFailover(ctx context.Context, out string) error {
	daemons, names, err := c.ownedManagers()
	if err != nil {
		return err
	}
	backend := managerPreparationBackend{
		observe: c.observeManagerMap,
		rejection: func(call context.Context, name string) (managerRefusal, error) {
			return c.nativeManagerRefusal(call, name, daemons[name], false)
		},
		stop:  func(call context.Context, name string) error { return daemons[name].Stop(call, nil) },
		start: func(call context.Context, name string) error { return daemons[name].Start(call) },
		record: func(receipt managerPreparationReceipt) error {
			data, err := json.Marshal(receipt)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(filepath.Join(out, "manager-preparation.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return err
			}
			_, writeErr := f.Write(append(data, '\n'))
			return errors.Join(writeErr, f.Close())
		},
	}
	return prepareManagerFailoverWithBackend(ctx, names, backend, 45*time.Second)
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
	poolPGs, noHashPSPool := 1, true
	if c.profile.osdPGV8() {
		poolPGs, noHashPSPool = 8, false
	}
	_, err := c.cluster.CreatePool(ctx, ceph.PoolConfig{Name: "ceph-msgr-osd-fixture", PGNum: poolPGs, Replicas: count, MinSize: minimum, NoHashPSPool: noHashPSPool})
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

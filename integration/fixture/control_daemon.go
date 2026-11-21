package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

func (c *fixtureCluster) daemon(ctx context.Context, kind, name string) (testcontainers.Container, error) {
	if kind == "mon" {
		monitors, err := c.cluster.Monitors(ctx)
		if err != nil {
			return nil, err
		}
		for _, d := range monitors {
			if d.DaemonName == name {
				return d.Container, nil
			}
		}
	}
	if kind == "osd" {
		osds, err := c.cluster.OSDs(ctx)
		if err != nil {
			return nil, err
		}
		for _, d := range osds {
			if strconv.Itoa(d.ID) == name {
				return d.Container, nil
			}
		}
	}
	if kind == "mgr" {
		managers, err := c.cluster.Managers(ctx)
		if err != nil {
			return nil, err
		}
		for _, d := range managers {
			if d.DaemonName == name {
				return d.Container, nil
			}
		}
	}
	if kind == "mds" {
		for _, fs := range cephfs.Filesystems(c.cluster) {
			for _, d := range fs.MDSs() {
				if d != nil && d.Container != nil && d.ID == name {
					return d.Container, nil
				}
			}
		}
	}
	return nil, errors.New("fixture daemon not found")
}

// daemonControlKinds are the daemon kinds a pause, resume, stop, start or
// kill request may name.
var daemonControlKinds = []string{"mon", "mgr", "osd", "mds"}

// daemonControlAdmitted is the closed admission of the daemon controls. MON,
// MGR and OSD keep every action, with kill restricted further below. An MDS
// may only be stopped, and only in the public MDS command acceptance, whose
// last step sends commands to its stopped daemon; no profile restarts,
// pauses or kills an MDS through this control.
func (p profile) daemonControlAdmitted(kind, action string) bool {
	if kind != "mds" {
		return kind == "mon" || kind == "mgr" || kind == "osd"
	}
	return action == "stop" && p.mdsCommands == "1" && p.mdsFresh == "1" && !p.admits(capMDSAppliedCommand)
}

// daemonActionScope selects the daemon control requests one poll serves.
type daemonActionScope int

const (
	// allDaemonActions serves every daemon control request.
	allDaemonActions daemonActionScope = iota
	// pauseEndingActions serves only what ends a pause: resume requests and
	// pause recoveries, with the original pause request a recovery settles.
	// The control loop serves these while an owned-pool oracle request runs
	// (controls), so that a lease ends at its deadline even while a native
	// read of the paused cluster is outstanding.
	pauseEndingActions
)

func (c *fixtureCluster) controlDaemonRequests(ctx context.Context, out string) error {
	return c.daemonRequests(ctx, out, allDaemonActions)
}

// daemonRequests serves the daemon control requests of scope.
//
// Every publication has its own envelope (pendingRequests), so a poll
// serves every pending request of an action in publication order. Pause
// recovery is per OSD and idempotent: a poll first claims every pending
// recover-paused-osd, then runs the pending pause and resume requests (the
// original requests a recovery settles, so none of them is left to freeze
// the daemon later), and then resumes each recovered OSD that is still
// paused; a second recovery of one OSD finds it resumed and is only
// acknowledged. Two leases that ended together are therefore both
// recovered. In pauseEndingActions scope only resume requests and the
// pause requests of a recovered OSD run, and every other request stays
// pending.
func (c *fixtureCluster) daemonRequests(ctx context.Context, out string, scope daemonActionScope) error {
	pending, err := pendingRequests(out, "recover-paused-osd")
	if err != nil {
		return err
	}
	var recoveries [][]string
	recovered := map[string]bool{}
	for _, r := range pending {
		if len(r.fields) != 2 {
			return errors.New("invalid OSD pause recovery request")
		}
		fields, err := claim(out, r)
		if err != nil {
			return err
		}
		recoveries = append(recoveries, fields)
		recovered[fields[1]] = true
	}
	for _, kind := range daemonControlKinds {
		for _, action := range []string{"pause", "resume", "stop", "start", "kill"} {
			if scope == pauseEndingActions && action != "resume" && (action != "pause" || kind != "osd" || len(recovered) == 0) {
				continue
			}
			pending, err := pendingRequests(out, action+"-"+kind)
			if err != nil {
				return err
			}
			for _, r := range pending {
				if len(r.fields) != 2 {
					return errors.New("invalid daemon control request")
				}
				if scope == pauseEndingActions && action == "pause" && !recovered[r.fields[1]] {
					continue
				}
				fields, err := claim(out, r)
				if err != nil {
					return err
				}
				if err := c.daemonAction(ctx, out, kind, action, fields); err != nil {
					return err
				}
			}
		}
	}
	for _, recovery := range recoveries {
		key := "osd." + recovery[1]
		if lease := c.pauses[key]; lease != nil {
			if err := lease.Resume(ctx); err != nil {
				return err
			}
			delete(c.pauses, key)
		}
		if err := publishAck(out, "osd-pause-recovered."+recovery[0]); err != nil {
			return err
		}
	}
	return nil
}

// daemonAction runs one claimed daemon control request and acknowledges it.
func (c *fixtureCluster) daemonAction(ctx context.Context, out, kind, action string, fields []string) error {
	if !c.profile.daemonControlAdmitted(kind, action) {
		return errors.New("MDS daemon control is restricted to stopping the owned daemon of the public MDS command acceptance")
	}
	name := fields[1]
	daemon, err := c.daemon(ctx, kind, name)
	if err != nil {
		return err
	}
	key := kind + "." + name
	switch action {
	case "pause":
		if c.pauses[key] != nil {
			return errors.New("daemon already paused")
		}
		c.pauses[key], err = fault.Pause(ctx, c.cluster, daemon)
	case "resume":
		if c.pauses[key] == nil {
			return errors.New("daemon is not paused")
		}
		err = c.pauses[key].Resume(ctx)
		if err == nil {
			delete(c.pauses, key)
		}
	case "kill":
		if !c.profile.forcedKillAdmitted(kind) {
			return errors.New("forced crash is restricted to disposable OSDs and the selected MON forced-kill cells")
		}
		// Docker stop sends SIGTERM first even with a zero timeout,
		// and an OSD with osd_fast_shutdown may then exit 0 before the
		// SIGKILL; a MON would shut down in order. Send SIGKILL itself
		// so the 137 receipt holds.
		var state *container.State
		state, err = forceKill(ctx, dockerKill(daemon.GetContainerID()), daemon.State, forcedKillTimeout)
		if err == nil {
			data, encodeErr := json.Marshal(map[string]any{"daemon": key, "id": daemon.GetContainerID(), "exit_code": state.ExitCode, "oom_killed": state.OOMKilled})
			if encodeErr != nil {
				return encodeErr
			}
			if err = os.WriteFile(filepath.Join(out, kind+"-crash-state.json"), data, 0644); err != nil {
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
	if err != nil {
		return err
	}
	return publishAck(out, kind+"-"+action+"."+fields[0])
}

var _ = defineControl("daemon", frozenControlRank(18), (*fixtureCluster).controlDaemonRequests)

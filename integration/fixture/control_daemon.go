package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
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

// daemonControlKinds are the daemon kinds a stop, start or kill request, or
// a pause lease (daemon_lease.go), may name.
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

// controlDaemonRequests serves the stop, start and kill requests of the
// daemons: per kind, every pending request of the three actions, in
// request-ID order. A pause is a lease and has its own loop
// (daemon_lease.go).
func (c *fixtureCluster) controlDaemonRequests(ctx context.Context, out string) error {
	for _, kind := range daemonControlKinds {
		// The pending requests of the three actions of a kind run in
		// request-ID order, the order of their publication: a start that a
		// cleanup published after a kill or stop whose wait the test
		// abandoned runs after it, never before.
		type pendingAction struct {
			action string
			r      pendingRequest
		}
		var pending []pendingAction
		for _, action := range []string{"stop", "start", "kill"} {
			requests, err := pendingRequests(out, action+"-"+kind)
			if err != nil {
				return err
			}
			for _, r := range requests {
				pending = append(pending, pendingAction{action, r})
			}
		}
		slices.SortFunc(pending, func(a, b pendingAction) int { return cmp.Compare(a.r.id, b.r.id) })
		for _, p := range pending {
			action := p.action
			fields, err := claim(out, p.r)
			if err != nil {
				return err
			}
			if len(fields) != 2 {
				return errors.New("invalid daemon control request")
			}
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
			if err = publishAck(out, kind+"-"+action+"."+fields[0]); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = defineControl("daemon", frozenControlRank(18), (*fixtureCluster).controlDaemonRequests)

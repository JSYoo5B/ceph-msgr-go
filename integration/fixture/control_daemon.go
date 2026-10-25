package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"

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

func (c *fixtureCluster) controlDaemonRequests(ctx context.Context, out string) error {
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
	return nil
}

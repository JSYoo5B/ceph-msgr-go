package main

import (
	"context"
	"errors"
	"path/filepath"
)

func (c *fixtureCluster) controlMonitorElectionRequest(ctx context.Context, out string) error {
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
	return nil
}

func (c *fixtureCluster) controlMonitorRestartRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

func (c *fixtureCluster) controlMonitorQuorumRequests(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

func (c *fixtureCluster) controlLegacyMonitorStopRequest(ctx context.Context, out string) error {
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
	return nil
}

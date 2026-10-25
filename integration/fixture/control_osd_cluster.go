package main

import (
	"context"
	"errors"
	"slices"
	"time"
)

// The object-backoff DOWN phase must outlast its configured heartbeat grace,
// including native query time and the monitor/PG transition. Other oracles
// retain their existing 80-second command bound.
const objectBackoffOracleTimeout = 150 * time.Second

// objectBackoffActions is the closed native recipe action set of
// tools/osd_object_backoff_oracle.py.
var objectBackoffActions = []string{"prepare", "config", "wait-osd", "put", "wait-degraded", "native-start", "native-finish", "native-stat", "ops", "ack-history", "restore"}

func (c *fixtureCluster) controlOSDBackoffRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

func (c *fixtureCluster) controlOSDObjectBackoffRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

func (c *fixtureCluster) controlOSDUpmapRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-upmap")
	if err != nil {
		return err
	}
	if fields != nil {
		if err := c.upmapControl(ctx, out, fields); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlOSDSplitRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

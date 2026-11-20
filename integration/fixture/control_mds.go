package main

import (
	"context"
	"errors"
	"slices"
)

func (c *fixtureCluster) controlMDSFreshRequest(ctx context.Context, out string) error {
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
	return nil
}

var _ = defineControl("mds-fresh", frozenControlRank(17), (*fixtureCluster).controlMDSFreshRequest)

func (c *fixtureCluster) controlMDSMapsRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
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
	return nil
}

var _ = defineControl("mds-maps", frozenControlRank(34), (*fixtureCluster).controlMDSMapsRequest)

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"mds-maps-command.json",
	"mds-maps-commands.json",
	"mds-maps-go.json",
	"mds-maps-input.json",
	"mds-maps-owned.json",
	"mds-maps-result.json",
)

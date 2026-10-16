package main

import (
	"context"
	"errors"
	"slices"
	"time"
)

var upmapActions = []string{"prepare", "items", "restore-items", "full", "restore-full", "observe", "finish"}

const upmapOracleTimeout = 150 * time.Second

// The original three-replica fixture remains unchanged. The closed native
// recipe owns a separate two-replica pool on its three BlueStore daemons.
func (c *fixtureCluster) upmapControl(ctx context.Context, out string, fields []string) error {
	if len(fields) != 2 || c.profile.osd != "1" || !c.profile.osdPGV8() ||
		c.profile.osdReplicas != "3" || c.profile.osdStore != "bluestore" || c.profile.mdsFresh == "1" ||
		!slices.Contains(upmapActions, fields[1]) {
		return errors.New("invalid owned three-OSD upmap oracle request")
	}
	args := []string{"python3", "/out/osd_upmap_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
	if err := c.runOracleCommandWithTimeout(ctx, out, args, "osd-upmap-command.json", "native owned upmap recipe", upmapOracleTimeout); err != nil {
		return err
	}
	return publishAck(out, "osd-upmap-verify."+fields[0])
}

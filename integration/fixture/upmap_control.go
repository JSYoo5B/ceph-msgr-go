package main

import (
	"context"
	"errors"
	"slices"
	"time"
)

var upmapActions = []string{"prepare", "items", "restore-items", "full", "restore-full", "observe", "finish"}

const upmapOracleTimeout = 150 * time.Second

// capOSDLongTickets gives the placement-only upmap recipe, which can spend
// tens of seconds in native controls, the long AUTH and service ticket
// validity (provider.go fixtureTicketTTLs) on the explicit-PG three-OSD
// BlueStore fixture. Dedicated short or fractional ticket profiles keep
// their own validity and do not support it.
var capOSDLongTickets = defineCapability("osd-long-tickets",
	"give one test on the explicit-PG three-OSD BlueStore fixture 120 s AUTH and service tickets",
	"OSD=1 OSD_PROFILE=replicated-pg-v8 OSD_REPLICAS=3 OSD_STORE=bluestore MDS_FRESH=0 SHORT_TICKETS=0", nil)

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

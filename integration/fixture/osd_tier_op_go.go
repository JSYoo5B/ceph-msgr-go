package main

import (
	"context"
	"errors"
	"time"
)

// The Go manifest tier-operation acceptance runs a closed librados observer
// against the two owned plain replicated pools of the owned-pool oracle
// (slot 1 holds the manifest objects, slot 2 their chunk and redirect
// targets). Each call reads its own closed input; the oracle refuses a
// repeated native label and any call after one left without a result, so
// nothing is reissued.
const tierOpGoAction = "call"

func (c *fixtureCluster) controlOSDTierOpRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "verify-osd-tier-op")
	if err != nil || fields == nil {
		return err
	}
	if len(fields) != 2 || c.profile.osd != "1" || fields[1] != tierOpGoAction {
		return errors.New("invalid native tier-operation observer request")
	}
	args := []string{"python3", "/out/osd_tier_op_go_oracle.py", "/tmp/ceph-msgr-test", "/out"}
	if err := c.runOracleCommandWithTimeout(ctx, out, args, "osd-tier-op-go-command.json", "closed native tier-operation observer", 2*time.Minute); err != nil {
		return err
	}
	return publishAck(out, "osd-tier-op-verify."+fields[0])
}

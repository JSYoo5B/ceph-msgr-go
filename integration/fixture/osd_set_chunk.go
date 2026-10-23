package main

import (
	"context"
	"errors"
	"time"
)

// The native-only SET_CHUNK checkpoint runs a closed librados oracle against
// the two owned plain replicated pools of the owned-pool oracle (slot 1 is the
// manifest source, slot 2 the chunk target). It uses no product client and
// adds no cache-tier or dedup pool configuration.
const setChunkNativeAction = "checkpoint"

// The checkpoint is selected at most once per fixture. A stopped or uncertain
// run is retained in its receipt, never reissued; the oracle itself also
// refuses to overwrite an existing receipt.
func (c *fixtureCluster) setChunkNative(ctx context.Context, out string) error {
	if c.setChunkSelected {
		return errors.New("native SET_CHUNK checkpoint was already selected")
	}
	c.setChunkSelected = true
	args := []string{"python3", "/out/osd_set_chunk_oracle.py", "/tmp/ceph-msgr-test", "/out"}
	return c.runOracleCommandWithTimeout(ctx, out, args, "osd-set-chunk-command.json", "closed native SET_CHUNK checkpoint", 3*time.Minute)
}

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func publishAck(out, name string) error {
	f, err := os.CreateTemp(out, "ack-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(out, name))
}

func (c *fixtureCluster) controlLoop(ctx context.Context, out string) {
	defer close(c.done)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.controls(ctx, out); err != nil {
				if ctx.Err() == nil {
					err = c.classifyControlFailure(out, err)
				}
				c.mu.Lock()
				c.controlErr = err
				c.mu.Unlock()
				_ = os.WriteFile(filepath.Join(out, "provider.error"), []byte(err.Error()+"\n"), 0600)
				return
			}
		}
	}
}

func (c *fixtureCluster) request(out, name string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(out, name))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return nil, errors.New("empty fixture request")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 {
		return nil, errors.New("invalid fixture request identity")
	}
	if err = os.Remove(filepath.Join(out, name)); err != nil {
		return nil, err
	}
	return fields, nil
}

// controls preserves the polling order across role-specific requests. Stop on
// the first failure; earlier mutations and acknowledgements are not replayed.
func (c *fixtureCluster) controls(ctx context.Context, out string) error {
	if err := c.controlCopyFromErrorRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlCachePoolsRequest(ctx, out); err != nil {
		return err
	}
	if fileExists(filepath.Join(out, "native-oracles.error")) {
		return errors.New("native oracle worker failed; inspect native-oracles.log")
	}
	if err := c.controlNamedManagerRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlMDSRequests(ctx, out); err != nil {
		return err
	}
	if err := c.controlRBDRequests(ctx, out); err != nil {
		return err
	}
	if err := c.controlMDSFreshRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlDaemonRequests(ctx, out); err != nil {
		return err
	}
	if err := c.controlMonitorElectionRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDReadyRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDCompoundRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerAuthRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerKeyringRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlAuthBundleRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerFailoverRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDMutationRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDXattrLimitRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDOMAPRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDOMAPWriteRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDBackoffRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDObjectBackoffRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDUpmapRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlMDSMapsRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDSplitRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDWatchNotifyRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDSparseReadRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDPGListRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDU64XattrRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDWriteSameRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDChecksumCompoundRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDEnforceSnapContextRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDSetChunkRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerServiceConfigRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOwnedPoolRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDChecksumRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDMapExtRangeRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDMapExtRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDWatchersRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDCommandRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDXattrNativeRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDXattrRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlMonitorRestartRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlMonitorQuorumRequests(ctx, out); err != nil {
		return err
	}
	if err := c.controlLegacyMonitorStopRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerStartRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlManagerSecurityRequest(ctx, out); err != nil {
		return err
	}
	if err := c.controlOSDEpochRequest(ctx, out); err != nil {
		return err
	}
	return nil
}

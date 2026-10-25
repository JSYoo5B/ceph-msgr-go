package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func (c *fixtureCluster) runOracleCommand(ctx context.Context, out string, args []string, receipt, label string) error {
	return c.runOracleCommandWithTimeout(ctx, out, args, receipt, label, 80*time.Second)
}

func (c *fixtureCluster) runOracleCommandWithTimeout(ctx context.Context, out string, args []string, receipt, label string, timeout time.Duration) error {
	result, runErr := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
		Argv:    args,
		Timeout: timeout, MaxStdout: 64 << 10, MaxStderr: 16 << 10,
		Label: label,
	})
	// This oracle contains only disposable object data and ordinary
	// native API errors. Preserve its bounded first-failure traceback rather
	// than discarding it with the generic control command's output.
	data, err := json.Marshal(result)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, receipt), data, 0644)
	}
	if runErr != nil || err != nil {
		return errors.Join(runErr, err)
	}
	if !result.Complete() || result.ExitCode != 0 {
		return fmt.Errorf("%s exit=%d complete=%t; see %s", label, result.ExitCode, result.Complete(), receipt)
	}
	return nil
}

// nativeOwnedPoolModes are the requests the native owned-pool oracle accepts.
var nativeOwnedPoolModes = map[string]bool{"create": true, "seed": true, "observe": true, "place": true, "snapshot": true, "selfmanaged": true, "quota": true, "poolinfo": true, "clusterstat": true, "versions": true, "mgrstate": true, "service": true, "servicewatch": true, "servicecompare": true, "servicemetadata": true, "fullratios": true, "osdflags": true, "inconsistent": true, "cleanup": true}

func (c *fixtureCluster) controlOwnedPoolRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-pool")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.osd != "1" || !nativeOwnedPoolModes[fields[1]] {
			return errors.New("invalid native owned-pool oracle request")
		}
		if (fields[1] == "servicemetadata" || fields[1] == "servicecompare" || fields[1] == "mgrstate") && c.profile.mgrService != "1" {
			return errors.New("native metadata observation requires the exclusive MGR service profile")
		}
		args := []string{"python3", "/out/osd_pool_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		budget := 80 * time.Second
		if fields[1] == "servicewatch" {
			// A bounded 90 s watch may wait through the native 60 s beacon
			// grace and then require a stable absence interval.
			budget = 120 * time.Second
		}
		if fields[1] == "servicecompare" {
			budget = 240 * time.Second
		}
		if err = c.runOracleCommandWithTimeout(ctx, out, args, "osd-pool-command.json", "native owned-pool oracle", budget); err != nil {
			return err
		}
		if err = publishAck(out, "osd-pool-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

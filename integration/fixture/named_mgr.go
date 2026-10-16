package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type namedManagerOracleReceipt struct {
	Phase   string                `json:"phase"`
	Before  managerPreparationMap `json:"before"`
	After   managerPreparationMap `json:"after"`
	Version json.RawMessage       `json:"version"`
}

func namedManagerOracleReady(current managerPreparationMap, previous *namedManagerOracleReceipt) bool {
	if !current.Available || current.ActiveGID == 0 || (current.ActiveName != "a" && current.ActiveName != "b") {
		return false
	}
	return previous == nil || (current.Epoch > previous.After.Epoch && current.ActiveGID != previous.After.ActiveGID && current.ActiveName != previous.After.ActiveName)
}

// Fresh native CLI observations remain test-only. No command arguments or
// filesystem paths are supplied by the consumer; only two fixed phases exist.
func (c *fixtureCluster) namedManagerOracle(ctx context.Context, out string, fields []string) error {
	if len(fields) != 2 || (fields[1] != "before" && fields[1] != "after") || c.profile.mgrCount != "2" {
		return errors.New("invalid named MGR oracle request")
	}
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var previous *namedManagerOracleReceipt
	if fields[1] == "after" {
		data, err := os.ReadFile(filepath.Join(out, "named-mgr-oracle-before.json"))
		var baseline namedManagerOracleReceipt
		if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &baseline) != nil || baseline.Phase != "before" || !namedManagerOracleReady(baseline.After, nil) {
			return errors.New("named MGR after oracle requires a valid before receipt")
		}
		previous = &baseline
	}
	var before managerPreparationMap
	for {
		var err error
		before, err = c.observeManagerMap(call)
		if err != nil {
			return err
		}
		if namedManagerOracleReady(before, previous) {
			break
		}
		select {
		case <-call.Done():
			return fmt.Errorf("waiting for named MGR native observation: %w", call.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	version, err := c.cluster.Ceph(call, "tell", "mgr."+before.ActiveName, "version", "--format", "json")
	if err != nil {
		return fmt.Errorf("native named MGR tell: %w", err)
	}
	if !json.Valid(version) {
		return errors.New("native named MGR tell returned invalid JSON")
	}
	after, err := c.observeManagerMap(call)
	if err != nil {
		return err
	}
	if !after.Available || before.ActiveGID != after.ActiveGID || before.ActiveName != after.ActiveName {
		return errors.New("native named MGR changed during observation")
	}
	receipt := namedManagerOracleReceipt{fields[1], before, after, version}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(out, "named-mgr-oracle-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(append(data, '\n')); err != nil {
		file.Close()
		return err
	}
	if err = file.Chmod(0644); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), filepath.Join(out, "named-mgr-oracle-"+fields[1]+".json")); err != nil {
		return err
	}
	return publishAck(out, "named-mgr-observed."+fields[0])
}

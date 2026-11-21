package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// capMgrDaemonControl admits the MGR daemon-role control acceptance on the
// ordinary one-OSD, two-MGR container fixture. Its test stops both stock MGR
// containers and one MON through the daemon control, fails the stock MGRs out
// of the map so that MON selects the test's MGR daemon, and restarts every
// stopped daemon before it ends. Nothing in this file changes the cluster;
// the mgr-daemon-dump control only reads it.
var capMgrDaemonControl = defineCapability("mgr-daemon-control",
	"let a test MGR daemon become the MON-selected active MGR by stopping the stock MGRs and one MON, then restart them",
	"OSD=1 MGR_COUNT=2 RUNTIME=container", nil)

// mgrDaemonDumpReceipt is the public receipt of one mgr-daemon-dump request.
// Admitted reports whether this run declared capMgrDaemonControl, so the
// broad suites that also select the test can skip it without a native read.
// Dump is the native `ceph mgr dump --format json` output, unmodified; it
// holds daemon names, global IDs, epochs and addresses, no key material.
type mgrDaemonDumpReceipt struct {
	Label    string          `json:"label"`
	Admitted bool            `json:"admitted"`
	Dump     json.RawMessage `json:"dump,omitempty"`
}

// mgrDaemonDump answers "<id> <label>": it runs one bounded native mgr dump
// through the fixture's CLI and replaces mgr-daemon-dump.json atomically.
func (c *fixtureCluster) mgrDaemonDump(ctx context.Context, out string, fields []string) error {
	if len(fields) != 2 {
		return errors.New("invalid MGR daemon dump request")
	}
	receipt := mgrDaemonDumpReceipt{Label: fields[1], Admitted: c.profile.admits(capMgrDaemonControl)}
	if receipt.Admitted {
		call, cancel := context.WithTimeout(ctx, nativeManagerMapTimeout)
		data, err := c.cluster.Ceph(call, "mgr", "dump", "--format", "json")
		cancel()
		if err != nil {
			return fmt.Errorf("native mgr dump for %s: %w", fields[1], err)
		}
		if !json.Valid(data) {
			return errors.New("native mgr dump is not JSON")
		}
		receipt.Dump = data
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary := filepath.Join(out, "mgr-daemon-dump.tmp")
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, "mgr-daemon-dump.json"))
}

var _ = defineRequestControl("mgr-daemon-dump", controlAfterRanked, (*fixtureCluster).mgrDaemonDump)

var _ = collectDiagnosticFiles("mgr-daemon-dump.json")

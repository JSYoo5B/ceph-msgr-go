package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// mgrOSDInjectReceipt is the public receipt of one mgr-osd-inject request:
// the admin-socket command's exit code and its bounded output (the daemon's
// JSON answer to config set, no key material).
type mgrOSDInjectReceipt struct {
	Label    string `json:"label"`
	Admitted bool   `json:"admitted"`
	Value    uint64 `json:"value"`
	Code     int    `json:"code"`
	Output   string `json:"output"`
}

// mgrOSDInject answers "<id> <label> <period>": it sets osd.0's
// ms_inject_socket_failures to period (0 disables it) through the daemon's
// own admin socket inside its container. The admin socket is a Unix
// socket, so the command reaches the daemon while its Messenger sockets
// fail, which a MON config change cannot rely on: the OSD learns it over
// its MON session, itself cut by the injection. The value is a daemon-local
// override and replaces mgr-osd-inject.json atomically.
func (c *fixtureCluster) mgrOSDInject(ctx context.Context, out string, fields []string) error {
	if len(fields) != 3 {
		return errors.New("invalid MGR OSD injection request")
	}
	value, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid socket failure period %q", fields[2])
	}
	receipt := mgrOSDInjectReceipt{Label: fields[1], Admitted: c.profile.admits(capMgrDaemonControl), Value: value, Code: -1}
	if receipt.Admitted {
		daemon, err := c.daemon(ctx, "osd", "0")
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		code, stream, err := daemon.Exec(call, []string{"ceph", "daemon", "osd.0", "config", "set", "ms_inject_socket_failures", fields[2]}, tcexec.Multiplexed())
		var data []byte
		if err == nil && stream != nil {
			data, err = io.ReadAll(io.LimitReader(stream, 4096))
		}
		cancel()
		if err != nil {
			return fmt.Errorf("osd.0 admin socket config set: %w", err)
		}
		receipt.Code, receipt.Output = code, string(data)
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	temporary := filepath.Join(out, "mgr-osd-inject.tmp")
	if err := os.WriteFile(temporary, data, 0o644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, "mgr-osd-inject.json"))
}

var _ = defineRequestControl("mgr-osd-inject", controlAfterRanked, (*fixtureCluster).mgrOSDInject)

var _ = collectDiagnosticFiles("mgr-osd-inject.json")

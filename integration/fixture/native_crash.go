package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// nativeCrashTimeout bounds the crash metadata read on a control failure.
const nativeCrashTimeout = 20 * time.Second

// Pinned ProtocolV2.cc:2277 dereferences messenger->auth_server without a
// check, and DaemonServer::init starts its messenger before
// set_auth_server(monc) (DaemonServer.cc:219-224). Any authenticating peer in
// that activation window crashes the native ceph-mgr; its fatal-signal handler
// may then fail to terminate the process, leaving native CLI calls hanging.
const nativeMgrAuthServerFrame = "ProtocolV2::handle_auth_request"

type nativeCrash struct {
	Daemon    string `json:"daemon"`
	Process   string `json:"process,omitempty"`
	CrashID   string `json:"crash_id"`
	Timestamp string `json:"timestamp,omitempty"`
	Frame     string `json:"frame,omitempty"`
	Known     string `json:"known,omitempty"`
}

// nativeCrashes selects owned Ceph daemon crash records. The messenger-oracles
// client container is excluded: its native CLI processes are oracles, not
// fixture daemons.
func nativeCrashes(report *ceph.CrashMetadataReport) []nativeCrash {
	if report == nil {
		return nil
	}
	var crashes []nativeCrash
	for _, container := range report.Containers {
		if container.Role == "client" {
			continue
		}
		for _, record := range container.Records {
			crash := nativeCrash{Daemon: record.Entity, Process: record.Process, CrashID: record.CrashID, Timestamp: record.Timestamp}
			if crash.Daemon == "" {
				crash.Daemon = container.Role + "." + container.Name
			}
			for _, frame := range crashBacktrace(record.Fields["backtrace"]) {
				if strings.HasPrefix(frame, "(") && crash.Frame == "" {
					crash.Frame = frame
				}
				if strings.Contains(frame, nativeMgrAuthServerFrame) && strings.HasPrefix(crash.Daemon, "mgr.") {
					crash.Frame = frame
					crash.Known = "native ceph-mgr null auth_server during DaemonServer activation"
					break
				}
			}
			crashes = append(crashes, crash)
		}
	}
	return crashes
}

// crashBacktrace accepts the provider's kept string list and its JSON form.
func crashBacktrace(value any) []string {
	switch value := value.(type) {
	case []string:
		return value
	case []any:
		var frames []string
		for _, item := range value {
			if frame, ok := item.(string); ok {
				frames = append(frames, frame)
			}
		}
		return frames
	}
	return nil
}

// classifyNativeCrash labels a control failure that coincides with a native
// daemon crash as a native crash fixture error, keeping the original cause.
func classifyNativeCrash(cause error, crashes []nativeCrash) error {
	if cause == nil || len(crashes) == 0 {
		return cause
	}
	var parts []string
	for _, crash := range crashes {
		part := fmt.Sprintf("%s crash %s at %s", crash.Daemon, crash.CrashID, crash.Timestamp)
		if crash.Frame != "" {
			part += " in " + crash.Frame
		}
		if crash.Known != "" {
			part += " (" + crash.Known + ")"
		}
		parts = append(parts, part)
	}
	return fmt.Errorf("native daemon crash fixture error: %s; control failure: %w", strings.Join(parts, "; "), cause)
}

// classifyControlFailure reads bounded crash metadata from the owned daemons
// and publishes the selected records next to the classified error.
func (c *fixtureCluster) classifyControlFailure(out string, cause error) error {
	if c.cluster == nil {
		return cause
	}
	ctx, cancel := context.WithTimeout(context.Background(), nativeCrashTimeout)
	defer cancel()
	report, err := c.cluster.CollectCrashMetadata(ctx, ceph.CrashMetadataConfig{Timeout: nativeCrashTimeout})
	crashes := nativeCrashes(report)
	if err != nil && len(crashes) == 0 {
		return cause
	}
	if len(crashes) != 0 {
		if data, encodeErr := json.Marshal(crashes); encodeErr == nil {
			_ = os.WriteFile(filepath.Join(out, "native-crash.json"), data, 0644)
		}
	}
	return classifyNativeCrash(cause, crashes)
}

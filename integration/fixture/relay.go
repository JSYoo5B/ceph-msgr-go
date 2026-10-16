package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The host-runtime relay listens inside the owned messenger-oracles container.
// Port 40000 was inside Linux's default ephemeral range (32768-60999), so a
// native oracle CLI socket in the same network namespace could hold it and
// the relay failed with "bind: address already in use". 30400 lies below that
// range and outside Ceph's MON (3300, 6789) and daemon (6800-7568) ports.
const (
	relayListenPort     = "30400"
	relayContainerPort  = relayListenPort + "/tcp"
	relayStartupTimeout = 60 * time.Second
	relayLogLimit       = 4096
)

// relayLogFailure interprets relay.log before readiness. The relay writes only
// fatal errors (log.Fatal), so any output before the ready file is a failure.
func relayLogFailure(text string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if len(text) > relayLogLimit {
		text = text[:relayLogLimit]
	}
	if strings.Contains(text, "bind:") {
		return fmt.Errorf("fixture error: relay could not bind its container port %s: %s", relayContainerPort, text)
	}
	return fmt.Errorf("fixture error: relay failed before readiness: %s", text)
}

// waitRelay waits for the relay's ready file through the host side of the
// /out bind mount, and fails as soon as relay.log reports a fatal error.
func waitRelay(ctx context.Context, out string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(filepath.Join(out, "relay.ready")); err == nil {
			return nil
		}
		if data, err := os.ReadFile(filepath.Join(out, "relay.log")); err == nil {
			if failure := relayLogFailure(string(data)); failure != nil {
				return failure
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("fixture relay readiness: %w", ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("fixture error: relay did not become ready within %v", timeout)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

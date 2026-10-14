package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Deliberately select public native metadata rather than archiving /out, the
// Ceph configuration, keyrings, auth databases or process memory.
var metadataFiles = []string{
	"fixture-manifest.json", "fixture-ownership.json", "daemon-version", "relay.log", "native-oracles.log", "native-oracles.error", "osd-map-control.log", "peers.json", "pool-policy.json", "provider.error", "provider.cleanup.error",
	"mapped-oracle-status.json", "mapped-oracle-mon.json", "mapped-oracle-mgr.json", "mapped-oracle-pg.json", "mapped-oracle-summary.json",
	"tell-oracle-mon.json", "tell-oracle-mgr.json", "tell-oracle-mgr-map.json", "named-mon-b.json",
	"log-oracle.json", "log-oracle-sentinel.txt", "config-oracle.json", "config-oracle-history.jsonl", "hostname-config-oracle.json",
	"command-oracle-mon.json", "command-oracle-mgr.json", "command-oracle-summary.json",
	"tell-command-oracle-mon-a.json", "tell-command-oracle-mon-b.json", "tell-command-oracle-mon-c.json", "tell-command-oracle-mgr.json", "tell-command-oracle-summary.json",
	"manager-oracle.json", "digest-oracle.json", "monitor-oracle.json", "osd-oracle.json", "osd-map-native-oracle.json", "osd-map-manifest.json",
	"osd-mutation-result.json", "osd-compound-result.json", "service-keys-history.jsonl", "manager-preparation.jsonl", "manager-native-auth.json", "osd-crash-state.json", "osd-map-epoch.json", "osd-map-applied.json", "osd-native-stat.txt", "osd-object.bin", "osd-native-read.bin",
	"osd-xattr-native.json", "osd-xattr-native-command.json",
	"osd-xattr-result.json", "osd-xattr-command.json",
	"osd-xattr-limits.json",
}

func saveMetadata(out, diagnostics string) error {
	if diagnostics == "" {
		return nil
	}
	if err := os.MkdirAll(diagnostics, 0o700); err != nil {
		return err
	}
	for _, name := range metadataFiles {
		file, err := os.Open(filepath.Join(out, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		err = saveBounded(file, filepath.Join(diagnostics, name))
		file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func collectDiagnostics(c *fixtureCluster, out, diagnostics string) error {
	if diagnostics == "" {
		fmt.Fprintln(os.Stderr, "Ceph fixture failed; set CEPH_MSGR_TEST_DIAGNOSTICS to preserve native logs and oracle metadata.")
		return nil
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		return err
	}
	if c == nil || c.cluster == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var additional []ceph.DiagnosticsContainer
	if c.Container != nil {
		additional = append(additional, ceph.DiagnosticsContainer{Name: "messenger-oracles", Role: "client", Container: c.Container})
	}
	report, diagnosticErr := c.cluster.CollectDiagnostics(ctx, ceph.DiagnosticsConfig{AdditionalContainers: additional})
	crashCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	crashes, crashErr := c.cluster.CollectCrashMetadata(crashCtx, ceph.CrashMetadataConfig{AdditionalContainers: additional})
	var writeErrs []error
	for _, entry := range []struct {
		name  string
		value any
	}{{"provider-diagnostics.json", report}, {"crash-metadata.json", crashes}} {
		if entry.value == nil {
			continue
		}
		data, err := json.MarshalIndent(entry.value, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(diagnostics, entry.name), data, 0600)
		}
		writeErrs = append(writeErrs, err)
	}
	return errors.Join(diagnosticErr, crashErr, errors.Join(writeErrs...))
}

func saveBounded(source io.Reader, path string) error {
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return fmt.Errorf("diagnostic %s exceeds %d bytes", filepath.Base(path), limit)
	}
	return os.WriteFile(path, data, 0o600)
}

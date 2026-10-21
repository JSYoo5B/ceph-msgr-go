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
	"osd-readiness.jsonl", "osd-full-failover-results.json",
	"osd-checksum-compound-input.json", "osd-checksum-compound-result.json", "osd-checksum-compound-command.json", "osd-checksum-compound-results.json",
	"osd-enforce-snap-context-input.json", "osd-enforce-snap-context-result.json", "osd-enforce-snap-context-command.json", "osd-enforce-snap-context-results.json", "osd-enforce-snap-context-native-write.json",
	"osd-cache-pools-fixture.json", "osd-cache-pools-native.json", "osd-cache-pools-command.json", "osd-cache-pools-results.json",
	"osd-ignore-cache-probe.json", "osd-ignore-cache-plan.json", "osd-ignore-cache-attached.json", "osd-ignore-cache-detached.json", "osd-ignore-cache-native.json", "osd-ignore-cache-command.json",
	"fixture-manifest.json", "fixture-ownership.json", "daemon-version", "relay.log", "native-oracles.log", "native-oracles.error", "osd-map-control.log", "peers.json", "pool-policy.json", "provider.error", "provider.cleanup.error",
	"mapped-oracle-status.json", "mapped-oracle-mon.json", "mapped-oracle-mgr.json", "mapped-oracle-pg.json", "mapped-oracle-summary.json",
	"tell-oracle-mon.json", "tell-oracle-mgr.json", "tell-oracle-mgr-map.json", "named-mon-b.json",
	"mon-ping-native-a.json", "mon-ping-native-b.json", "mon-ping-native-c.json", "mon-ping-results.json",
	"named-mgr-oracle-before.json", "named-mgr-oracle-after.json", "named-mgr-results.json", "mgr-service-recovery.json", "mgr-service-close.json", "mgr-service-metadata.json",
	"log-oracle.json", "log-oracle-sentinel.txt", "config-oracle.json", "config-oracle-history.jsonl", "hostname-config-oracle.json",
	"command-oracle-mon.json", "command-oracle-mgr.json", "command-oracle-summary.json",
	"tell-command-oracle-mon-a.json", "tell-command-oracle-mon-b.json", "tell-command-oracle-mon-c.json", "tell-command-oracle-mgr.json", "tell-command-oracle-summary.json",
	"manager-oracle.json", "digest-oracle.json", "monitor-oracle.json", "osd-oracle.json", "osd-map-native-oracle.json", "osd-map-manifest.json",
	"monitor-election-history.jsonl", "monitor-election-settled.json",
	"monitor-election-settled-added.json", "monitor-election-settled-removed.json",
	"osd-mutation-result.json", "osd-compound-result.json", "service-keys-history.jsonl", "manager-preparation.jsonl", "manager-native-auth.json", "manager-keyring-gate.json", "auth-first-failure-bundle.json", "mgr-transition-receipt.json", "fixture-unusable.json", "native-crash.json", "osd-crash-state.json", "osd-map-epoch.json", "osd-map-applied.json", "osd-native-stat.txt", "osd-object.bin", "osd-native-read.bin",
	"osd-pool-input.json", "osd-pool-result.json", "osd-pool-command.json", "osd-writesame-input.json", "osd-writesame-result.json", "osd-writesame-command.json", "osd-writesame-results.json", "writesame-oracle-provenance.json",
	"osd-u64-xattr-input.json", "osd-u64-xattr-result.json", "osd-u64-xattr-command.json", "osd-u64-xattr-results.json", "u64-xattr-oracle-provenance.json",
	"osd-xattr-native.json", "osd-xattr-native-command.json",
	"osd-xattr-result.json", "osd-xattr-command.json",
	"osd-xattr-limits.json",
	"osd-mutation-command.json",
	"osd-xattr-permission-list.json", "osd-xattr-permission-set.json", "osd-xattr-permission-remove.json",
	"osd-xattr-permission-go-set.json", "osd-xattr-permission-go-remove.json",
	"osd-omap-command.json", "osd-omap-result.json", "osd-omap-owned.json",
	"osd-omap-read-results.json",
	"osd-omap-write-command.json", "osd-omap-write-result.json", "osd-omap-write-owned.json",
	"osd-omap-write-results.json", "osd-omap-compare-results.json",
	"osd-command-command.json", "osd-command-result.json", "osd-command-results.json",
	"osd-omap-range-results.json", "osd-call-results.json", "osd-call-compound-results.json", "osd-alloc-hint-results.json",
	"osd-watch-notify-status.json", "osd-watch-notify-worker.log", "osd-watch-notify-input.json", "osd-watch-notify-result.json", "osd-watch-notify-command.json", "osd-watch-notify-results.json",
	"osd-watch-reconnect-results.json",
	"osd-reply-v8-results.json", "osd-request-v9-results.json",
	"osd-upmap-input.json", "osd-upmap-result.json", "osd-upmap-results.json", "osd-upmap-command.json",
	"osd-upmap-owned.json", "osd-upmap-state.json", "osd-upmap-commands.json", "osd-upmap-native.json",
	"osd-upmap-failure.json", "osd-upmap-guard-result.json", "osd-upmap-guard.log", "osd-upmap-seed-items.bin", "osd-upmap-seed-full.bin",
	"mds-maps-input.json", "mds-maps-owned.json", "mds-maps-result.json", "mds-maps-commands.json", "mds-maps-command.json", "mds-maps-go.json",
	"mds-fresh-owned.json", "mds-fresh-pool-policy.json", "mds-fresh-oracle.json", "mds-fresh-session-result.json", "mds-fresh-results.json", "mds-fresh-cleanup.json",
	"mds-commands-oracle.json", "mds-commands-native-result.json", "mds-commands-results.json",
	"mds-sessions-oracle.json", "mds-sessions-owned.json", "mds-sessions-result.json", "mds-sessions-results.json", "mds-sessions-cleanup.json",
	"mds-cephfs-oracle.json", "mds-cephfs-owned.json", "mds-cephfs-result.json", "mds-cephfs-results.json", "mds-cephfs-cleanup.json",
	"mds-cephfs-target.json", "mds-cephfs-recovery-results.json", "mds-cephfs-push-results.json",
	"mds-cephfs-ranks-oracle.json", "mds-cephfs-ranks-owned.json", "mds-cephfs-ranks-result.json", "mds-cephfs-ranks-results.json", "mds-cephfs-ranks-replay-results.json", "mds-cephfs-ranks-cleanup.json",
	"mds-cephfs-completion-oracle.json", "mds-cephfs-completion-owned.json", "mds-cephfs-completion-result.json", "mds-cephfs-completion-target.json",
	"mds-cephfs-completion-results.json", "mds-cephfs-completion-cleanup.json",
	"mds-cephfs-data-target.json", "mds-cephfs-data-oracle.json", "mds-cephfs-data-owned.json", "mds-cephfs-data-result.json", "mds-cephfs-data-results.json", "mds-cephfs-data-cleanup.json",
	"osd-split-input.json", "osd-split-result.json", "osd-split-status.json", "osd-split-command.json", "osd-split-worker.log", "osd-split-results.json",
	"osd-split-stale-input.json", "osd-split-stale-receipt.json",
	"osd-history-query.json",
	"osd-pg-list-input.json", "osd-pg-list-result.json", "osd-pg-list-command.json", "osd-pg-list-results.json",
	"osd-sparse-read-input.json", "osd-sparse-read-result.json", "osd-sparse-read-command.json", "osd-sparse-read-results.json", "sparse-read-oracle-provenance.json",
	"osd-checksum-command.json", "osd-checksum-input.json", "osd-checksum-result.json", "osd-checksum-results.json",
	"osd-mapext-command.json", "osd-mapext-input.json", "osd-mapext-result.json", "osd-mapext-results.json", "osd-snap-mapext-results.json", "mapext-range-oracle-provenance.json", "osd-mapext-range-input.json", "osd-mapext-range-result.json", "osd-mapext-range-command.json",
	"osd-replicated-pg-results.json", "osd-backoff-input.json", "osd-backoff-result.json", "osd-backoff-status.json", "osd-backoff-command.json", "osd-backoff-worker.log", "osd-backoff-results.json",
	"osd-pg-list-v8-results.json", "osd-backoff-late-ack-results.json",
	"osd-backoff-ack-input.json", "osd-backoff-ack-receipt.json",
	"osd-object-backoff-input.json", "osd-object-backoff-result.json", "osd-object-backoff-state.json", "osd-object-backoff-command.json", "osd-object-backoff-results.json",
	"osd-watchers-result.json", "osd-watchers-status.json", "osd-watchers-worker.log", "osd-watchers-command.json", "osd-watchers-results.json",
	"osd-omap-write-snaps.json", "osd-snap-results.json", "osd-snap-sparse-read-results.json", "osd-snap-checksum-results.json",
	"rbd-base-owned.json", "rbd-base-setup.json", "rbd-base-oracle.json", "rbd-base-result.json", "rbd-base-watch-state.json", "rbd-base-cleanup.json", "rbd-base-results.json",
	"rbd-write-setup.json", "rbd-write-observe-written.json", "rbd-write-observe-resized.json", "rbd-write-observe-locked.json", "rbd-write-observe-unlocked.json",
	"rbd-write-watch.json", "rbd-write-watch-state.json", "rbd-write-results.json",
	"rbd-lock-setup.json", "rbd-lock-lock.json", "rbd-lock-observe-released.json", "rbd-lock-state.json", "rbd-lock-results.json",
	"rbd-clone-setup.json", "rbd-clone-observe-copyup.json", "rbd-clone-write.json", "rbd-clone-snapshot.json", "rbd-clone-observe-snapshot.json",
	"rbd-clone-flatten.json", "rbd-clone-results.json",
	"rbd-journal-setup.json", "rbd-journal-observe-registered.json", "rbd-journal-write.json", "rbd-journal-observe-committed.json",
	"rbd-journal-inspect.json", "rbd-journal-replay.json", "rbd-journal-results.json",
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

package main

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// Deliberately select public native metadata rather than archiving /out, the
// Ceph configuration, keyrings, auth databases or process memory.
var metadataFiles = []string{
	"fixture-manifest.json", "fixture-ownership.json", "daemon-version", "relay.log", "provider.error", "provider.cleanup.error",
	"mapped-oracle-status.json", "mapped-oracle-mon.json", "mapped-oracle-mgr.json", "mapped-oracle-pg.json", "mapped-oracle-summary.json",
	"tell-oracle-mon.json", "tell-oracle-mgr.json", "tell-oracle-mgr-map.json", "named-mon-b.json",
	"log-oracle.json", "log-oracle-sentinel.txt", "config-oracle.json", "config-oracle-history.jsonl", "hostname-config-oracle.json",
	"command-oracle-mon.json", "command-oracle-mgr.json", "command-oracle-summary.json",
	"tell-command-oracle-mon-a.json", "tell-command-oracle-mon-b.json", "tell-command-oracle-mon-c.json", "tell-command-oracle-mgr.json", "tell-command-oracle-summary.json",
	"manager-oracle.json", "digest-oracle.json", "monitor-oracle.json", "osd-oracle.json", "osd-map-native-oracle.json", "osd-map-manifest.json",
	"osd-map-epoch.json", "osd-map-applied.json", "osd-native-stat.txt", "osd-object.bin", "osd-native-read.bin",
}

var daemonFiles = []string{
	"mon.a.log", "mon.b.log", "mon.c.log", "mgr.a.log", "mgr.b.log", "osd.0.log", "osd.0.mkfs.log", "osd.probe.log",
	"osd-map-control.log", "osd-map-applied.json", "service-keys-probe.json", "service-keys-probe.log", "mode-probe.json", "mode-probe.log", "fixture-exit",
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

func collectDiagnostics(c *ceph.Container, out, diagnostics string) error {
	if diagnostics == "" {
		fmt.Fprintln(os.Stderr, "Ceph fixture failed; set CEPH_MSGR_TEST_DIAGNOSTICS to preserve native logs and oracle metadata.")
		return nil
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		return err
	}
	if c == nil || c.Container == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var missing []string
	for _, name := range daemonFiles {
		file, err := c.CopyFileFromContainer(ctx, "/tmp/ceph-msgr-test/"+name)
		if err != nil {
			// Early bootstrap and profiles without OSDs legitimately lack files.
			missing = append(missing, name)
			continue
		}
		err = saveBounded(file, filepath.Join(diagnostics, name))
		file.Close()
		if err != nil {
			return err
		}
	}
	// Stream and filter metadata even after the fixture exits. Never persist an
	// archive: a crash directory may also contain credentials or process dumps.
	provider, err := testcontainers.NewDockerProvider()
	crashCollected := false
	if err == nil {
		defer provider.Close()
		archive, err := provider.Client().CopyFromContainer(ctx, c.GetContainerID(), client.CopyFromContainerOptions{SourcePath: "/var/lib/ceph/crash"})
		if err == nil {
			defer archive.Content.Close()
			data, err := selectCrashMetadata(archive.Content)
			crashCollected = err == nil
			if err := os.WriteFile(filepath.Join(diagnostics, "crash-metadata.jsonl"), data, 0o600); err != nil {
				return err
			}
		}
	}
	if !crashCollected {
		missing = append(missing, "crash-metadata.jsonl")
	}
	data, _ := json.MarshalIndent(map[string]any{"unavailable_files": missing}, "", "  ")
	return os.WriteFile(filepath.Join(diagnostics, "diagnostic-collection.json"), append(data, '\n'), 0o600)
}

func selectCrashMetadata(source io.Reader) ([]byte, error) {
	archive := tar.NewReader(io.LimitReader(source, 64<<20))
	var output []byte
	for {
		member, err := archive.Next()
		if err == io.EOF {
			return output, nil
		}
		if err != nil {
			return output, err
		}
		if (member.Typeflag != tar.TypeReg && member.Typeflag != tar.TypeRegA) || path.Base(member.Name) != "meta" || member.Size <= 0 || member.Size > 1<<20 {
			continue
		}
		data, err := io.ReadAll(archive)
		if err != nil {
			return output, err
		}
		var value map[string]json.RawMessage
		if err := json.Unmarshal(data, &value); err != nil || value == nil {
			continue
		}
		data, err = json.Marshal(value)
		if err != nil {
			return output, err
		}
		output = append(append(output, data...), '\n')
	}
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

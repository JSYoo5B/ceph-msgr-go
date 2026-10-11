package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubprocessEnvironmentIsolatesExternalCephCredentials(t *testing.T) {
	t.Setenv("CEPH_MSGR_KEY_FILE", "external-key")
	t.Setenv("CEPH_MSGR_TEST_PROXY", "external-proxy")
	t.Setenv("CEPH_TEST_CONFIG", "external-config")
	t.Setenv("FIXTURE_TEST_OTHER", "kept")
	env := environment(map[string]string{"CEPH_MSGR_KEY_FILE": "private-fixture-key"}, true)
	joined := "\n" + strings.Join(env, "\n") + "\n"
	if strings.Contains(joined, "external-") || !strings.Contains(joined, "\nCEPH_MSGR_KEY_FILE=private-fixture-key\n") || !strings.Contains(joined, "\nFIXTURE_TEST_OTHER=kept\n") {
		t.Fatalf("subprocess did not isolate fixture credentials")
	}
}

func TestCancelledStartupDoesNotCreateDockerResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c, err := startCluster(ctx, profile{}, t.TempDir())
	if c != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled startup: container=%v error=%v", c, err)
	}
}

func TestDiagnosticsDoNotCopyCredentialsOrProcessMemory(t *testing.T) {
	out, diagnostics := t.TempDir(), t.TempDir()
	for _, name := range []string{"key", "admin.keyring", "clients.keyring", "ceph.conf", "core", "daemon-version"} {
		if err := os.WriteFile(filepath.Join(out, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(diagnostics)
	if err != nil || len(entries) != 1 || entries[0].Name() != "daemon-version" {
		t.Fatalf("diagnostic selection: entries=%v error=%v", entries, err)
	}
}

func TestCrashArchiveSelectsOnlyBoundedRegularMetadataObjects(t *testing.T) {
	var source bytes.Buffer
	archive := tar.NewWriter(&source)
	for _, entry := range []struct {
		name, data string
		kind       byte
	}{
		{"crash/a/meta", `{"crash_id":"a"}`, tar.TypeReg},
		{"crash/a/core", "process-memory", tar.TypeReg},
		{"crash/b/meta", "[]", tar.TypeReg},
		{"crash/c/meta", "{broken", tar.TypeReg},
		{"crash/d/meta", "", tar.TypeSymlink},
		{"crash/e/meta", strings.Repeat("x", (1<<20)+1), tar.TypeReg},
		{"crash/f/meta", `{"crash_id":"f"}`, tar.TypeReg},
	} {
		header := &tar.Header{Name: entry.name, Typeflag: entry.kind, Size: int64(len(entry.data)), Mode: 0o600}
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write([]byte(entry.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := selectCrashMetadata(&source)
	if err != nil || string(result) != "{\"crash_id\":\"a\"}\n{\"crash_id\":\"f\"}\n" {
		t.Fatalf("crash metadata selection: %q error=%v", result, err)
	}
}

func TestCrashArchiveReportsTruncatedMetadata(t *testing.T) {
	var source bytes.Buffer
	archive := tar.NewWriter(&source)
	if err := archive.WriteHeader(&tar.Header{Name: "crash/a/meta", Size: 1024, Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	// A crash archive can be incomplete when the daemon/engine exits.
	if _, err := archive.Write([]byte("{\"id\":1}")); err != nil {
		t.Fatal(err)
	}
	if _, err := selectCrashMetadata(&source); err == nil {
		t.Fatal("truncated archive treated as complete diagnostics")
	}
}

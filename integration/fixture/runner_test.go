package main

import (
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

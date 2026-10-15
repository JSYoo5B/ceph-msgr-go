package testcluster

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPublicEvidenceReplacesPrivateReceiptForHostDiagnostics(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "osd-checksum-input.json")
	if err := os.WriteFile(path, []byte("old container receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	// On Windows, os.Stat can defer loading a file's identity until SameFile
	// is called. After publication that path already names the replacement.
	// File.Stat captures the old handle's identity now; close it before Rename
	// so the test does not itself prevent replacement on Windows.
	previous, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := previous.Stat()
	if closeErr := previous.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := WritePublicEvidence(path, []byte("public receipt")); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Fatal("publication truncated the earlier receipt instead of replacing it")
	}
	if runtime.GOOS != "windows" && after.Mode().Perm() != 0o644 {
		t.Fatalf("host cannot read container-owned public receipt: mode=%#o", after.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "public receipt" {
		t.Fatalf("host diagnostic read: %q, %v", data, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary files left after publication: %v, %v", entries, err)
	}
}

func TestPublicEvidenceDoesNotFollowExistingReceiptSymlink(t *testing.T) {
	directory := t.TempDir()
	private := filepath.Join(directory, "private-key")
	if err := os.WriteFile(private, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "public.json")
	if err := os.Symlink(private, path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skip("Windows symlink creation is unavailable:", err)
		}
		t.Fatal(err)
	}
	if err := WritePublicEvidence(path, []byte("public")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(private)
	if err != nil || string(data) != "private" {
		t.Fatalf("publication modified symlink target: %q, %v", data, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatal("public receipt did not replace the link", info, err)
	}
}

func TestPublicEvidenceFailedRenameRemovesOnlyItsTemporaryFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "existing-directory")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WritePublicEvidence(path, []byte("public")); err == nil {
		t.Fatal("publication replaced a directory")
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "existing-directory" || !entries[0].IsDir() {
		t.Fatalf("publication removed an unrelated entry or leaked its temporary file: %v, %v", entries, err)
	}
}

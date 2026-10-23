package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSoakReceiptCollectionIsImmutable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "osd-soak-v1-steady-worker-0.json")
	for _, data := range []string{"first", "first"} {
		if err := saveSoakReceipt(strings.NewReader(data), path); err != nil {
			t.Fatal("identical collection should be idempotent", err)
		}
	}
	if err := saveSoakReceipt(strings.NewReader("changed"), path); err == nil {
		t.Fatal("a later observation replaced the first receipt")
	}
	if names, err := soakReceiptNames(dir); err != nil || len(names) != 1 {
		t.Fatal("closed receipt name missing", names, err)
	}
	if err := os.Symlink(path, filepath.Join(dir, "osd-soak-v1-steady-worker-1.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := soakReceiptNames(dir); err == nil {
		t.Fatal("symlink receipt was admitted")
	}
}

func TestSoakSourceSHAArchiveAssertion(t *testing.T) {
	root := t.TempDir()
	sha := strings.Repeat("a", 40)
	if got := (profile{osdSoak: "4m", sourceSHA: sha}).testEnvironment("/out", "")["CEPH_MSGR_TEST_SOURCE_SHA"]; got != sha {
		t.Fatal("soak test source assertion not passed to test runtime", got)
	}
	if _, exists := (profile{}).testEnvironment("/out", "")["CEPH_MSGR_TEST_SOURCE_SHA"]; exists {
		t.Fatal("non-soak environment gained a source assertion")
	}
	if got, err := soakSourceSHA(root, sha); err != nil || got != sha {
		t.Fatal("archive source assertion refused", got, err)
	}
	for _, value := range []string{"", "short", strings.Repeat("A", 40), sha + "x"} {
		if _, err := soakSourceSHA(root, value); err == nil {
			t.Fatal("invalid archive source accepted", value)
		}
	}
}

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteSameOracleProfileAndAssetProof(t *testing.T) {
	const host = "/private/native/writesame-oracle"
	if _, err := loadProfile(func(name string) string {
		if name == "CEPH_MSGR_TEST_WRITESAME_ORACLE" {
			return host
		}
		return ""
	}); err == nil || !strings.Contains(err.Error(), "OSD") {
		t.Fatal("WRITESAME oracle admitted without OSD", err)
	}
	env := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_WRITESAME_ORACLE": host}
	p, err := loadProfile(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	if p.writeSameOracle != host || p.testEnvironment("/fixture", "")["CEPH_MSGR_NATIVE_WRITESAME_ORACLE"] != "1" {
		t.Fatal("oracle availability lost")
	}
	for _, values := range []map[string]string{p.testEnvironment("/fixture", ""), p.containerEnvironment()} {
		for name, value := range values {
			if strings.Contains(value, host) || name == "CEPH_MSGR_TEST_WRITESAME_ORACLE" {
				t.Fatal("host path leaked", name)
			}
		}
	}
	project := t.TempDir()
	out := t.TempDir()
	if err := stageWriteSameOracle(project, out, profile{}); err != nil {
		t.Fatal("optional asset required", err)
	}
	if err := os.MkdirAll(filepath.Join(project, "tools"), 0700); err != nil {
		t.Fatal(err)
	}
	wrapper := []byte("inert fixture wrapper input\n")
	if err := os.WriteFile(filepath.Join(project, "tools/osd_writesame_oracle.cc"), wrapper, 0600); err != nil {
		t.Fatal(err)
	}
	binary := append([]byte{0x7f, 'E', 'L', 'F'}, make([]byte, 60)...)
	path := filepath.Join(t.TempDir(), "native-oracle")
	if err := os.WriteFile(path, binary, 0600); err != nil {
		t.Fatal(err)
	}
	proof := map[string]any{"source_commit": nativeEncoderCommit, "image": profileDefaultImage, "binary_sha256": fmt.Sprintf("%x", sha256.Sum256(binary)), "wrapper_sha256": fmt.Sprintf("%x", sha256.Sum256(wrapper))}
	save := func() {
		data, err := json.Marshal(proof)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".proof.json", data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	save()
	p.writeSameOracle = path
	if err := stageWriteSameOracle(project, out, p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "osd_writesame_oracle"))
	if err != nil || !bytes.Equal(got, binary) {
		t.Fatal("verified bytes changed", err)
	}
	for _, field := range []string{"source_commit", "image", "binary_sha256", "wrapper_sha256"} {
		original := proof[field]
		proof[field] = "wrong"
		save()
		if err := stageWriteSameOracle(project, t.TempDir(), p); err == nil {
			t.Fatal("bad proof accepted", field)
		}
		proof[field] = original
	}
	save()
	if err := os.WriteFile(path, []byte("not an ELF"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := stageWriteSameOracle(project, t.TempDir(), p); err == nil {
		t.Fatal("non-ELF accepted")
	}
}

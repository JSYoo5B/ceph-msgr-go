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

func TestSparseOracleProfileRequiresOSDAndKeepsHostPathPrivate(t *testing.T) {
	const hostPath = "/private/fixture-native-build/osd_sparse_read_oracle"
	if _, err := loadProfile(func(name string) string {
		if name == "CEPH_MSGR_TEST_SPARSE_READ_ORACLE" {
			return hostPath
		}
		return ""
	}); err == nil || !strings.Contains(err.Error(), "OSD") {
		t.Fatal("native oracle admitted without OSD fixture", err)
	}
	env := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_SPARSE_READ_ORACLE": hostPath}
	p, err := loadProfile(func(name string) string { return env[name] })
	if err != nil {
		t.Fatal(err)
	}
	if p.sparseReadOracle != hostPath {
		t.Fatal("host staging path lost")
	}
	testEnv := p.testEnvironment("/fixture/control", "")
	if testEnv["CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE"] != "1" {
		t.Fatal("test binary lacks native-oracle availability marker")
	}
	for _, target := range []map[string]string{testEnv, p.containerEnvironment()} {
		for name, value := range target {
			if strings.Contains(value, hostPath) || name == "CEPH_MSGR_TEST_SPARSE_READ_ORACLE" {
				t.Fatal("host native path leaked into fixture environment", name)
			}
		}
	}
	p.sparseReadOracle = ""
	if p.testEnvironment("/fixture/control", "")["CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE"] != "" {
		t.Fatal("default fixture advertised absent optional native oracle")
	}
}

func sparseOracleAssets(t *testing.T) (project, path string, binary []byte, proof map[string]any) {
	t.Helper()
	project = t.TempDir()
	for _, name := range []string{"integration/cluster.sh", "integration/osd-fixture.sh", "integration/publish-ack.sh", "integration/fixture/legacy.crush.txt", "tools/osd_xattr_native.py", "tools/osd_omap_native.py", "tools/osd_omap_write_native.py", "tools/osd_command_native.py", "tools/osd_pg_list_oracle.py", "tools/osd_sparse_read_oracle.py", "tools/osd_watch_notify_oracle.py", "tools/osd_sparse_read_oracle.cc"} {
		target := filepath.Join(project, name)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(name+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path = filepath.Join(t.TempDir(), "native-oracle")
	// Inert ELF-shaped bytes test staging only; no native build is claimed.
	binary = append([]byte{0x7f, 'E', 'L', 'F'}, make([]byte, 60)...)
	if err := os.WriteFile(path, binary, 0600); err != nil {
		t.Fatal(err)
	}
	wrapper, err := os.ReadFile(filepath.Join(project, "tools/osd_sparse_read_oracle.cc"))
	if err != nil {
		t.Fatal(err)
	}
	proof = map[string]any{"source_commit": nativeEncoderCommit, "image": profileDefaultImage, "binary_sha256": fmt.Sprintf("%x", sha256.Sum256(binary)), "wrapper_sha256": fmt.Sprintf("%x", sha256.Sum256(wrapper))}
	return
}
func writeSparseOracleProof(t *testing.T, path string, proof map[string]any) {
	t.Helper()
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".proof.json", data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSparseOracleAssetStagingIsOptionalAndPreservesVerifiedBytes(t *testing.T) {
	project, path, binary, proof := sparseOracleAssets(t)
	out := t.TempDir()
	if err := stageAssets(project, out, profile{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"osd_pg_list_oracle.py", "osd_sparse_read_oracle.py", "osd_watch_notify_oracle.py"} {
		if data, err := os.ReadFile(filepath.Join(out, name)); err != nil || !bytes.Equal(data, []byte("tools/"+name+"\n")) {
			t.Fatal("fixture oracle not staged", name, err)
		}
	}
	for _, name := range []string{"osd_sparse_read_oracle", "sparse-read-oracle-provenance.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); !os.IsNotExist(err) {
			t.Fatal("optional native binary staged without opt-in", name, err)
		}
	}
	writeSparseOracleProof(t, path, proof)
	out = t.TempDir()
	if err := stageAssets(project, out, profile{image: profileDefaultImage, sparseReadOracle: path}); err != nil {
		t.Fatal(err)
	}
	staged, err := os.ReadFile(filepath.Join(out, "osd_sparse_read_oracle"))
	if err != nil || !bytes.Equal(staged, binary) {
		t.Fatal("verified binary bytes changed", err)
	}
	original, err := os.ReadFile(path + ".proof.json")
	if err != nil {
		t.Fatal(err)
	}
	staged, err = os.ReadFile(filepath.Join(out, "sparse-read-oracle-provenance.json"))
	if err != nil || !bytes.Equal(staged, original) {
		t.Fatal("build proof changed during staging", err)
	}
	for name, want := range map[string]os.FileMode{"osd_sparse_read_oracle": 0700, "sparse-read-oracle-provenance.json": 0600} {
		stat, err := os.Stat(filepath.Join(out, name))
		if err != nil || stat.Mode().Perm() != want {
			t.Fatal("staged asset permissions", name, err)
		}
	}
}

func TestSparseOracleStagingRejectsMismatchedOrUnboundedEvidence(t *testing.T) {
	for _, kind := range []string{"missing proof", "different source", "different image", "different fixture image", "different wrapper", "different binary", "missing binary hash", "malformed proof", "oversized proof", "not ELF", "short ELF", "oversized ELF"} {
		t.Run(kind, func(t *testing.T) {
			project, path, bin, proof := sparseOracleAssets(t)
			p := profile{image: profileDefaultImage, sparseReadOracle: path}
			switch kind {
			case "different source":
				proof["source_commit"] = "other"
			case "different image":
				proof["image"] = "other"
			case "different fixture image":
				p.image = "custom-image"
			case "different wrapper":
				proof["wrapper_sha256"] = "other"
			case "different binary":
				proof["binary_sha256"] = "other"
			case "missing binary hash":
				delete(proof, "binary_sha256")
			}
			if kind != "missing proof" {
				writeSparseOracleProof(t, path, proof)
			}
			switch kind {
			case "malformed proof":
				if err := os.WriteFile(path+".proof.json", []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized proof":
				if err := os.WriteFile(path+".proof.json", bytes.Repeat([]byte{' '}, (128<<10)+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "not ELF":
				bin[0] = 0
				if err := os.WriteFile(path, bin, 0600); err != nil {
					t.Fatal(err)
				}
			case "short ELF":
				if err := os.WriteFile(path, bin[:63], 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized ELF":
				if err := os.Truncate(path, (32<<20)+1); err != nil {
					t.Fatal(err)
				}
			}
			out := t.TempDir()
			if err := stageAssets(project, out, p); err == nil {
				t.Fatal("native oracle admitted with invalid evidence")
			}
			for _, name := range []string{"osd_sparse_read_oracle", "sparse-read-oracle-provenance.json"} {
				if _, err := os.Stat(filepath.Join(out, name)); !os.IsNotExist(err) {
					t.Fatal("invalid oracle exported before validation", name, err)
				}
			}
		})
	}
}

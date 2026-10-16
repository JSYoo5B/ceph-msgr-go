package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMapExtOracleProfileAndPrivatePath(t *testing.T) {
	const hostPath = "/private/native-build/mapext"
	values := map[string]string{"CEPH_MSGR_TEST_MAPEXT_ORACLE": hostPath}
	if _, err := loadProfile(func(k string) string { return values[k] }); err == nil {
		t.Fatal("oracle accepted without an OSD fixture")
	}
	values["CEPH_MSGR_TEST_OSD"] = "1"
	p, err := loadProfile(func(k string) string { return values[k] })
	if err != nil || p.mapExtOracle != hostPath {
		t.Fatal(p.mapExtOracle, err)
	}
	if p.testEnvironment("/out", "")["CEPH_MSGR_NATIVE_MAPEXT_ORACLE"] != "1" {
		t.Fatal("missing native availability marker")
	}
	for _, env := range []map[string]string{p.testEnvironment("/out", ""), p.containerEnvironment()} {
		for k, value := range env {
			if k == "CEPH_MSGR_TEST_MAPEXT_ORACLE" || strings.Contains(value, hostPath) {
				t.Fatal("host path leaked", k)
			}
		}
	}
}

func TestMapExtOracleHasItsOwnSourceAndBinaryProof(t *testing.T) {
	project, path, binary, proof := sparseOracleAssets(t)
	const wrapper = "independent mapext wrapper\n"
	if err := os.WriteFile(filepath.Join(project, "tools/osd_mapext_range_oracle.cc"), []byte(wrapper), 0600); err != nil {
		t.Fatal(err)
	}
	// A sparse-read binary's proof cannot authorize a MAPEXT wrapper.
	writeSparseOracleProof(t, path, proof)
	p := profile{image: profileDefaultImage, mapExtOracle: path}
	if err := stageAssets(project, t.TempDir(), p); err == nil {
		t.Fatal("foreign wrapper proof admitted")
	}
	proof["wrapper_sha256"] = fmt.Sprintf("%x", sha256.Sum256([]byte(wrapper)))
	writeSparseOracleProof(t, path, proof)
	out := t.TempDir()
	if err := stageAssets(project, out, p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "osd_mapext_range_oracle"))
	if err != nil || !bytes.Equal(got, binary) {
		t.Fatal("verified native binary not retained", err)
	}
	if _, err := os.Stat(filepath.Join(out, "mapext-range-oracle-provenance.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "osd_sparse_read_oracle")); !os.IsNotExist(err) {
		t.Fatal("unselected oracle was staged", err)
	}
}

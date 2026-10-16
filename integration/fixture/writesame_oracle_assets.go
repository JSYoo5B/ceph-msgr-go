package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// This opt-in native client is a fixture asset, never a product dependency.
// Require the exact selected image, pinned source, wrapper and executable hash
// before staging, and pass only an availability marker to the test binary.
func stageWriteSameOracle(project, out string, p profile) error {
	if p.writeSameOracle == "" {
		return nil
	}
	boundedRead := func(path string, limit int64) ([]byte, error) {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		data, err := io.ReadAll(io.LimitReader(file, limit+1))
		if err == nil && int64(len(data)) > limit {
			err = fmt.Errorf("native WRITESAME asset exceeds bound")
		}
		return data, err
	}
	binary, err := boundedRead(p.writeSameOracle, 32<<20)
	if err != nil {
		return err
	}
	if len(binary) < 64 || !bytes.Equal(binary[:4], []byte{0x7f, 'E', 'L', 'F'}) {
		return fmt.Errorf("native WRITESAME oracle must be a bounded ELF binary")
	}
	proof, err := boundedRead(p.writeSameOracle+".proof.json", 128<<10)
	if err != nil {
		return err
	}
	var receipt struct {
		SourceCommit  string `json:"source_commit"`
		Image         string `json:"image"`
		BinarySHA256  string `json:"binary_sha256"`
		WrapperSHA256 string `json:"wrapper_sha256"`
	}
	if err := json.Unmarshal(proof, &receipt); err != nil {
		return err
	}
	wrapper, err := os.ReadFile(filepath.Join(project, "tools/osd_writesame_oracle.cc"))
	if err != nil {
		return err
	}
	if receipt.SourceCommit != nativeEncoderCommit || receipt.Image != profileDefaultImage || receipt.Image != p.image || receipt.BinarySHA256 != fmt.Sprintf("%x", sha256.Sum256(binary)) || receipt.WrapperSHA256 != fmt.Sprintf("%x", sha256.Sum256(wrapper)) {
		return fmt.Errorf("native WRITESAME oracle differs from pinned source/binary proof")
	}
	if err := os.WriteFile(filepath.Join(out, "osd_writesame_oracle"), binary, 0700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "writesame-oracle-provenance.json"), proof, 0600)
}

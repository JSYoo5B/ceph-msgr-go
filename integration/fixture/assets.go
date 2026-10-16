package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func stageAssets(project, out string, p profile) error {
	files := []string{"integration/cluster.sh", "integration/osd-fixture.sh", "integration/publish-ack.sh", "integration/fixture/legacy.crush.txt", "tools/osd_xattr_native.py", "tools/osd_omap_native.py", "tools/osd_omap_write_native.py", "tools/osd_command_native.py"}
	oracles, err := filepath.Glob(filepath.Join(project, "tools", "*_oracle.py"))
	if err != nil {
		return err
	}
	for _, path := range oracles {
		files = append(files, path)
	}
	for _, path := range files {
		if !filepath.IsAbs(path) {
			path = filepath.Join(project, path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, filepath.Base(path)), data, 0o700); err != nil {
			return err
		}
	}
	for _, oracle := range []struct{ path, name, wrapper, proof string }{
		{p.sparseReadOracle, "osd_sparse_read_oracle", "osd_sparse_read_oracle.cc", "sparse-read-oracle-provenance.json"},
		{p.mapExtOracle, "osd_mapext_range_oracle", "osd_mapext_range_oracle.cc", "mapext-range-oracle-provenance.json"},
	} {
		if oracle.path == "" {
			continue
		}
		file, err := os.Open(oracle.path)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(file, (32<<20)+1))
		file.Close()
		if err != nil {
			return err
		}
		if len(data) > 32<<20 || len(data) < 64 || !bytes.Equal(data[:4], []byte{0x7f, 'E', 'L', 'F'}) {
			return fmt.Errorf("native read oracle must be a bounded ELF binary")
		}
		proofFile, err := os.Open(oracle.path + ".proof.json")
		if err != nil {
			return fmt.Errorf("native read oracle lacks its build proof: %w", err)
		}
		proof, err := io.ReadAll(io.LimitReader(proofFile, (128<<10)+1))
		proofFile.Close()
		if err != nil {
			return fmt.Errorf("read native read oracle build proof: %w", err)
		}
		if len(proof) > 128<<10 {
			return fmt.Errorf("native read oracle build proof exceeds limit")
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
		wrapper, err := os.ReadFile(filepath.Join(project, "tools/"+oracle.wrapper))
		if err != nil {
			return err
		}
		if receipt.SourceCommit != nativeEncoderCommit || receipt.Image != profileDefaultImage || receipt.Image != p.image || receipt.BinarySHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) || receipt.WrapperSHA256 != fmt.Sprintf("%x", sha256.Sum256(wrapper)) {
			return fmt.Errorf("native read oracle differs from its pinned source/binary proof")
		}
		if err := os.WriteFile(filepath.Join(out, oracle.name), data, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, oracle.proof), proof, 0600); err != nil {
			return err
		}
	}
	return stageU64XattrOracle(project, out, p)
}

func buildTests(ctx context.Context, project, out, arch string, p profile) error {
	goos, goarch, cgo := "linux", arch, "0"
	if p.runtime == "host" {
		goos, goarch = runtime.GOOS, runtime.GOARCH
		if p.race == "1" {
			cgo = "1"
		}
	}
	for _, suite := range []struct{ name, pkg string }{{"api", "integration"}, {"client", "cephmsgr"}} {
		args := []string{"test", "-c", "-o", filepath.Join(out, suite.name+".test")}
		if p.race == "1" {
			args = append(args, "-race")
		}
		args = append(args, "./"+suite.pkg)
		if err := goBuild(ctx, project, goos, goarch, cgo, args...); err != nil {
			return fmt.Errorf("build %s suite: %w", suite.name, err)
		}
	}
	if p.runtime == "host" {
		return goBuild(ctx, project, "linux", arch, "0", "build", "-o", filepath.Join(out, "relay"), "./integration/relay")
	}
	return nil
}

func goBuild(ctx context.Context, project, goos, goarch, cgo string, args ...string) error {
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = project
	cmd.Env = environment(map[string]string{"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": cgo, "GOWORK": "off"}, false)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

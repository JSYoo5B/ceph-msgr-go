package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func stageAssets(project, out string, p profile) error {
	files := []string{"integration/cluster.sh", "integration/osd-fixture.sh", "integration/publish-ack.sh", "integration/fixture/legacy.crush.txt", "tools/osd_xattr_native.py", "tools/osd_omap_native.py"}
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
	return nil
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

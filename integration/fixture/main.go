// Development-only fixture runner. This nested module keeps Testcontainers and
// the native Ceph oracle tooling outside the product module's dependency graph.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	containerexec "github.com/testcontainers/testcontainers-go/exec"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := command(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type cleanupFailure struct{ cause error }

func (e *cleanupFailure) Error() string { return "fixture cleanup: " + e.cause.Error() }
func (e *cleanupFailure) Unwrap() error { return e.cause }

func command(ctx context.Context, args []string) (result error) {
	if len(args) != 0 && args[0] == "encode-mds-maps" {
		return runNativeEncoder(ctx, args[1:], mdsMapEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-mutations" {
		return nativeEncoderCommand(ctx, args[1:])
	}
	if len(args) != 0 && args[0] == "encode-osd-xattrs" {
		return runNativeEncoder(ctx, args[1:], xattrEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-omap" {
		return runNativeEncoder(ctx, args[1:], omapEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-omap-writes" {
		return runNativeEncoder(ctx, args[1:], omapWriteEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-omap-cmp" {
		return runNativeEncoder(ctx, args[1:], omapCmpEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-omap-range" {
		return runNativeEncoder(ctx, args[1:], omapRangeEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-call" {
		return runNativeEncoder(ctx, args[1:], callEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-alloc-hint" {
		return runNativeEncoder(ctx, args[1:], allocHintEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-watch-notify" {
		return runNativeEncoder(ctx, args[1:], watchNotifyEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-backoff" {
		return runNativeEncoder(ctx, args[1:], backoffEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-request-v8" {
		return runNativeEncoder(ctx, args[1:], requestV8EncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-pg-list-v8" {
		return runNativeEncoder(ctx, args[1:], pgListV8EncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-reply-v8" {
		return runNativeEncoder(ctx, args[1:], replyV8EncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-sparse-read" {
		return runNativeEncoder(ctx, args[1:], sparseReadEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-pg-list" {
		return runNativeEncoder(ctx, args[1:], pgListEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-checksum" {
		return runNativeEncoder(ctx, args[1:], checksumEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-mapext" {
		return runNativeEncoder(ctx, args[1:], mapextEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-watchers" {
		return runNativeEncoder(ctx, args[1:], watchersEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-snap" {
		return runNativeEncoder(ctx, args[1:], snapEncoderSpec)
	}
	if len(args) == 0 || (args[0] != "run" && args[0] != "serve") {
		return errors.New("usage: fixture-controller run|serve -project PATH [-output PATH] [-diagnostics PATH]")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	project := flags.String("project", "../..", "product checkout")
	output := flags.String("output", "", "private fixture IPC directory (required for serve)")
	diagnostics := flags.String("diagnostics", "", "whitelisted diagnostic output directory")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	p, err := loadProfile(os.Getenv)
	if err != nil {
		return err
	}
	if *diagnostics != "" {
		p.diagnostics = *diagnostics
	}
	root, err := filepath.Abs(*project)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "SPEC.md")); err != nil {
		return fmt.Errorf("product checkout: %w", err)
	}
	if args[0] == "serve" && *output == "" {
		return errors.New("serve requires an existing private -output directory")
	}
	out := *output
	if out == "" {
		out, err = os.MkdirTemp("", "ceph-msgr-integration-")
		if err != nil {
			return err
		}
		defer func() {
			var cleanup *cleanupFailure
			if errors.As(result, &cleanup) {
				fmt.Fprintln(os.Stderr, "Cleanup failed; private ownership files retained at "+out)
				return
			}
			_ = os.RemoveAll(out)
		}()
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}
	if args[0] == "serve" {
		if _, err := os.Stat(out); err != nil {
			return err
		}
		return serve(ctx, root, out, p)
	}
	arch, err := imageArchitecture(ctx, p.image)
	if err != nil {
		return err
	}
	if err := buildTests(ctx, root, out, arch, p); err != nil {
		return err
	}
	return run(ctx, root, out, p)
}

func run(ctx context.Context, project, out string, p profile) (result error) {
	if err := stageAssets(project, out, p); err != nil {
		return err
	}
	c, err := startCluster(ctx, p, out)
	defer func() {
		if c != nil {
			if result != nil {
				result = errors.Join(result, collectDiagnostics(c, out, p.diagnostics))
			}
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := c.Terminate(cleanup); err != nil {
				result = errors.Join(result, &cleanupFailure{cause: err})
			}
			if p.mdsFresh == "1" {
				result = errors.Join(result, saveMetadata(out, p.diagnostics))
			}
		}
	}()
	if ownerErr := recordOwnership(c, out); ownerErr != nil {
		return errors.Join(err, ownerErr)
	}
	if err != nil {
		return err
	}
	p = c.profile
	proxy, err := prepareEvidence(ctx, c, out, p)
	if err != nil {
		return err
	}
	// The client package's final recovery test leaves MON a down. Preserve the
	// public API -> client suite ordering within each disposable fixture.
	for _, name := range []string{"api", "client"} {
		if err := c.checkControl(); err != nil {
			return err
		}
		fmt.Printf("Running Ceph test suite: %s\n", name)
		args := []string{"-test.run", p.testRun, "-test.v", "-test.timeout", p.testTimeout}
		if p.runtime == "host" {
			cmd := exec.CommandContext(ctx, filepath.Join(out, name+".test"), args...)
			cmd.Env = environment(p.testEnvironment(out, proxy), true)
			cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("%s native suite: %w", name, err)
			}
		} else {
			args = append([]string{"/out/" + name + ".test"}, args...)
			code, output, err := c.Exec(ctx, args, containerexec.WithEnv(envList(p.testEnvironment("/out", ""))), containerexec.Multiplexed())
			if output != nil {
				_, copyErr := io.Copy(os.Stdout, output)
				err = errors.Join(err, copyErr)
			}
			if err != nil || code != 0 {
				return fmt.Errorf("%s container suite exit=%d: %w", name, code, errors.Join(err, errors.New("Ceph suite failed")))
			}
		}
	}
	if err := c.checkControl(); err != nil {
		return err
	}
	if err := c.finishMDSFresh(ctx); err != nil {
		return err
	}
	return saveMetadata(out, p.diagnostics)
}

func serve(ctx context.Context, project, out string, p profile) (result error) {
	if p.runtime != "host" {
		return errors.New("serve requires CEPH_MSGR_TEST_RUNTIME=host")
	}
	if err := stageAssets(project, out, p); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	c, err := startCluster(ctx, p, out)
	defer func() {
		if c != nil {
			if result != nil {
				result = errors.Join(result, collectDiagnostics(c, out, p.diagnostics))
			}
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			if err := c.Terminate(cleanup); err != nil {
				result = errors.Join(result, &cleanupFailure{cause: err})
				_ = os.WriteFile(filepath.Join(out, "provider.cleanup.error"), []byte(err.Error()+"\n"), 0o600)
			}
			stop()
			if p.mdsFresh == "1" {
				result = errors.Join(result, saveMetadata(out, p.diagnostics))
			}
		}
		if result != nil {
			_ = os.WriteFile(filepath.Join(out, "provider.error"), []byte(result.Error()+"\n"), 0o600)
			if err := saveMetadata(out, p.diagnostics); err != nil {
				result = errors.Join(result, err)
			}
		}
		_ = os.WriteFile(filepath.Join(out, "provider.stopped"), nil, 0o600)
	}()
	if ownerErr := recordOwnership(c, out); ownerErr != nil {
		return errors.Join(err, ownerErr)
	}
	if err != nil {
		return err
	}
	p = c.profile
	if _, err := prepareEvidence(ctx, c, out, p); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "provider.ready"), nil, 0o600); err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.checkControl(); err != nil {
				return err
			}
			if fileExists(filepath.Join(out, "provider.diagnostics")) {
				if err := collectDiagnostics(c, out, p.diagnostics); err != nil {
					return err
				}
				if err := os.Remove(filepath.Join(out, "provider.diagnostics")); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(out, "provider.diagnostics.done"), nil, 0o600); err != nil {
					return err
				}
			}
			if fileExists(filepath.Join(out, "provider.stop")) {
				return nil
			}
		}
	}
}

func recordOwnership(c *fixtureCluster, out string) error {
	if c == nil {
		return nil
	}
	containerID := ""
	if c.Container != nil {
		containerID = c.GetContainerID()
		if err := os.WriteFile(filepath.Join(out, "container-name"), []byte(containerID+"\n"), 0o600); err != nil {
			return err
		}
	}
	owned := map[string]string{}
	if c.Container != nil {
		owned["messenger-oracles"] = c.GetContainerID()
	}
	for _, mon := range c.cluster.Monitors() {
		owned["mon."+mon.DaemonName] = mon.GetContainerID()
	}
	for _, mgr := range c.cluster.Managers() {
		owned["mgr."+mgr.DaemonName] = mgr.GetContainerID()
	}
	for _, osd := range c.cluster.OSDs() {
		owned[fmt.Sprintf("osd.%d", osd.ID)] = osd.GetContainerID()
	}
	for name, id := range c.mdsFreshOwnership() {
		owned[name] = id
	}
	if control := c.cluster.ControlContainer(); control != nil {
		owned["control"] = control.GetContainerID()
	}
	data, err := json.MarshalIndent(map[string]any{"container_id": containerID, "network": c.NetworkName(), "containers": owned}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "fixture-ownership.json"), append(data, '\n'), 0o600)
}

func prepareEvidence(ctx context.Context, c *fixtureCluster, out string, p profile) (string, error) {
	imageID, architecture, err := inspectImageMetadata(ctx, c)
	if err != nil {
		return "", err
	}
	version, err := nativeVersion(ctx, c)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(out, "daemon-version"), []byte(version+"\n"), 0o600); err != nil {
		return "", err
	}
	manifest := map[string]any{"provider": "github.com/JSYoo5B/ceph-testcontainers-go", "provider_version": "v0.2.0", "image": p.image, "image_id": imageID, "architecture": architecture, "daemon_version": version,
		"bootstrap": "public-api", "topology": "3 MON and 0/2 MGR owned daemon containers plus a native oracle client", "runtime": p.runtime, "fsid": p.fsid,
		"key_type": p.keyType, "service_cipher": p.serviceCipher, "connection_mode": p.connectionMode, "ip_family": p.ipFamily, "osd": p.osd, "osd_store": p.osdStore, "osd_replicas": p.osdReplicas, "osd_profile": p.osdProfile, "native_sparse_read_oracle": p.sparseReadOracle != ""}
	manifest["mds_fresh"] = p.mdsFresh == "1"
	manifest["mds_commands"] = p.mdsCommands == "1"
	if p.mdsFresh == "1" {
		manifest["topology"] = "3 MON, 0/2 MGR, one OSD and one active rank 0 MDS with an owned filesystem plus a native oracle client"
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(out, "fixture-manifest.json"), append(data, '\n'), 0o600); err != nil {
		return "", err
	}
	if p.runtime != "host" {
		return "", nil
	}
	proxy, err := proxyEndpoint(ctx, c)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(out, "proxy"), []byte(proxy+"\n"), 0o600); err != nil {
		return "", err
	}
	return proxy, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Do not let a caller's external Ceph credentials/proxy leak into the native
// subprocess. Fixture configuration is passed explicitly for every suite.
func environment(values map[string]string, cleanCeph bool) []string {
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := values[key]; replaced || (cleanCeph && (strings.HasPrefix(key, "CEPH_MSGR_") || strings.HasPrefix(key, "CEPH_TEST_"))) {
			continue
		}
		env = append(env, entry)
	}
	return append(env, envList(values)...)
}

func envList(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env
}

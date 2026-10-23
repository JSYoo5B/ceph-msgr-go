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
	if len(args) != 0 && args[0] == "probe-osd-ignore-cache" {
		return ignoreCacheProbeCommand(ctx, args[1:])
	}
	if len(args) != 0 && args[0] == "encode-mds-maps" {
		return runNativeEncoder(ctx, args[1:], mdsMapEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-mds-sessions" {
		return runNativeEncoder(ctx, args[1:], mdsSessionEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-mds-requests" {
		return runNativeEncoder(ctx, args[1:], mdsRequestEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-mds-caps" {
		return runNativeEncoder(ctx, args[1:], mdsCapsEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-mds-reconnect" {
		return runNativeEncoder(ctx, args[1:], mdsReconnectEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-mutations" {
		return nativeEncoderCommand(ctx, args[1:])
	}
	if len(args) != 0 && args[0] == "encode-osd-writesame" {
		return runNativeEncoder(ctx, args[1:], writeSameEncoderSpec)
	}
	if len(args) != 0 && args[0] == "encode-osd-u64-xattrs" {
		return runNativeEncoder(ctx, args[1:], u64XattrEncoderSpec)
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
	if len(args) != 0 && args[0] == "encode-osd-truncate" {
		return runNativeEncoder(ctx, args[1:], truncateEncoderSpec)
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
	if p.osdSoak != "" {
		if p.sourceSHA, err = soakSourceSHA(root, p.sourceSHA); err != nil {
			return err
		}
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
			cleanup, cancel := context.WithTimeout(context.Background(), p.cleanupBudget())
			defer cancel()
			suiteErr := result
			terminationStarted := time.Now()
			terminationErr := c.Terminate(cleanup)
			if err := terminationErr; err != nil {
				result = errors.Join(result, &cleanupFailure{cause: err})
			}
			result = errors.Join(result, finishSoakReceipts(project, out, p, suiteErr, terminationErr, time.Since(terminationStarted)))
			if p.osd == "1" || p.mdsFresh == "1" || p.rbdBase == "1" || p.osdCachePools == "1" {
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
	if err := c.finishRBDBase(ctx); err != nil {
		return err
	}
	if err := c.finishOSDCachePools(ctx); err != nil {
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
			cleanup, stop := context.WithTimeout(context.Background(), p.cleanupBudget())
			if err := c.Terminate(cleanup); err != nil {
				result = errors.Join(result, &cleanupFailure{cause: err})
				_ = os.WriteFile(filepath.Join(out, "provider.cleanup.error"), []byte(err.Error()+"\n"), 0o600)
			}
			stop()
			if p.mdsFresh == "1" || p.rbdBase == "1" || p.osdCachePools == "1" {
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
	manifest := map[string]any{"image": p.image, "image_id": imageID, "architecture": architecture, "daemon_version": version,
		"bootstrap": "public-api", "topology": "3 MON and 0/2 MGR owned daemon containers plus a native oracle client", "runtime": p.runtime, "fsid": p.fsid,
		"key_type": p.keyType, "service_cipher": p.serviceCipher, "connection_mode": p.connectionMode, "ip_family": p.ipFamily, "osd": p.osd, "osd_store": p.osdStore, "osd_replicas": p.osdReplicas, "osd_profile": p.osdProfile, "native_sparse_read_oracle": p.sparseReadOracle != "", "native_mapext_range_oracle": p.mapExtOracle != ""}
	for field, value := range currentFixtureProviderIdentity() {
		manifest[field] = value
	}
	manifest["mon_ping"] = p.monPing == "1"
	manifest["osd_cache_pools"] = p.osdCachePools == "1"
	manifest["mgr_service"] = p.mgrService == "1"
	manifest["native_u64_xattr_oracle"] = p.u64XattrOracle != ""
	manifest["native_writesame_oracle"] = p.writeSameOracle != ""
	manifest["mds_fresh"] = p.mdsFresh == "1"
	manifest["mds_commands"] = p.mdsCommands == "1"
	manifest["mds_sessions"] = p.mdsSessions == "1"
	manifest["mds_cephfs"] = p.mdsCephFS == "1"
	manifest["mds_cephfs_ranks"] = p.mdsCephFSRanks == "1"
	manifest["mds_cephfs_completion"] = p.mdsCephFSCompletion == "1"
	manifest["mds_cephfs_data"] = p.mdsCephFSData == "1"
	manifest["rbd_base"] = p.rbdBase == "1"
	if p.rbdBase == "1" {
		manifest["topology"] = "3 MON, 0/2 MGR and one OSD with an owned RBD pool plus a native oracle client"
		manifest["rbd_base_mutations"] = "one owned pool (application rbd, eight PGs without HASHPSPOOL) created, initialized and removed with its CRUSH rule; two owned identities (Go and native librbd, profile rbd bound to the pool) created and removed; one native format-2 layering image written, snapshotted, rewritten and resized twice, discarded with the pool; on the write test's request a second native layering image written, snapshotted and rewritten, then written, resized, tagged with image metadata, locked and unlocked by the Go client, discarded with the pool; on the exclusive-lock test's request a third native image with exclusive-lock, object-map and fast-diff written by native librbd and the Go client, its managed lock handed between them, checked with rbd object-map check and discarded with the pool; on the clone test's request a native layering parent written around a snapshot and its format-2 clone, written by the Go client (copy-ups) and by native librbd, snapshotted and flattened natively, both discarded with the pool"
	}
	if p.mdsFresh == "1" {
		manifest["topology"] = "3 MON, 0/2 MGR, one OSD and one active rank 0 MDS with an owned filesystem plus a native oracle client"
	}
	if p.mdsCephFSRanks == "1" {
		manifest["topology"] = "3 MON, 0/2 MGR, one OSD and an owned filesystem with two active MDS ranks and one standby plus a native oracle client"
		manifest["cephfs_ranks_mutations"] = "owned Go and native LibCephFS identities, one directory tree created and removed through Go requests and verified natively; export pins on two owned directories set and restored with fresh-mount readbacks; one owned standby takeover of rank 0 (ceph mds fail of the proven GID) whose blocklist entries are removed and verified during cleanup"
	}
	if p.mdsCephFSCompletion == "1" {
		manifest["cephfs_completion_mutations"] = "owned Go and native LibCephFS identities and one directory tree created and removed through Go requests and verified natively; one native create+write and one native snapshot created and removed; runtime-only admin-socket changes of ms_blackhole_client, mds_early_reply, mds_log_pause and debug levels on the owned daemon, each restored or discarded by a restart; owned central mds_deny_all_reconnect and mds_reconnect_timeout entries of the owned daemon's section, each restored; one force_readonly; up to four owned rank-0 restarts and evictions of owned Go sessions whose blocklist entries are removed and verified during cleanup"
	}
	if p.mdsCephFSData == "1" {
		manifest["cephfs_data_mutations"] = "owned Go and native LibCephFS identities with OSD write grants on the owned data pool; one directory created natively with a small striped ceph.dir.layout that is removed again; six files created and written through Go requests and Go OSD writes or natively (one sparse, one truncated natively), all unlinked through Go requests; three native snapshots created and removed, including two dirty-data cap-snap generations; the directory removed through a Go request and verified natively; owned daemon-local mds_log_pause, ms_blackhole_client and debug_mds/debug_ms changes journaled, read back and restored or verified discarded after respawn; one journal barrier after a proven log resume with client ACKs blackholed; bounded owned-client capability source/handler, dropped-ACK and completed-flush logs; one explicitly armed rank-0 MDS GID failed once, its owned new incarnation adopted after native identity proof and runtime baseline readback, and only its new exact old-incarnation blocklist entries removed after the old GID leaves the map; for the writeback-fence selector only, up to two sequential changes of one owned OSD's debug_ms and ms_blackhole_client, each read back and restored, one background native read of shared-file, and one registered owned Go session evicted whose blocklist entry is removed and verified during cleanup"
	}
	if p.mdsSessions == "1" {
		manifest["session_mutations"] = "owned refuse_client_session, required_client_features, session timeouts, mds_defer_session_stale, eviction blocklist entries and one foreign identity, each reconciled during cleanup"
	}
	if p.mdsCephFS == "1" {
		manifest["cephfs_mutations"] = "owned Go, recovery, native and push-native LibCephFS identities, three directories with their files, one user xattr and two native writes removed through Go requests and verified natively; one native rename, one snapshot and one quota created and removed natively; the owned daemon's mds_min_caps_per_client and mds_max_caps_per_client lowered and restored, and one cache drop; one owned client eviction and one owned rank-0 MDS restart whose blocklist entries are removed and verified during cleanup"
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

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// rgwNativeEnv routes a catalog row to the dedicated RGW checkpoints of this
// module (RGW_NATIVE_VALIDATION.md) instead of the controller's own fixture.
// The native workflow forwards it from the row parameter rgw_native.
const rgwNativeEnv = "CEPH_MSGR_TEST_RGW_NATIVE"

// rgwNativeStream is the file, in the run's diagnostics directory, that keeps
// the selected checkpoint's raw test2json stream for the trusted result gate
// (tools/native_test_results.py, tools/rgw_native_gate.py).
const rgwNativeStream = "rgw-native.jsonl"

// rgwNativeSuite is the suite name the controller prints before the
// checkpoint's output, as it prints api and client for the product suites.
const rgwNativeSuite = "fixture"

// rgwNativeTargets is the closed set of tests a row may route to. Each starts
// and removes its own owned RGW cluster; the controller starts none. A new
// target is added here, to tools/rgw_native_gate.py and to a catalog row.
var rgwNativeTargets = []string{"TestRGWNativeCheckpointIntegration", "TestRGWNativeMultiStripeIntegration", "TestRGWNativeBackendFaultIntegration"}

// rgwNativeTimeout bounds one checkpoint's go test process, as the
// development workflow (.github/workflows/rgw-native.yml) does.
const rgwNativeTimeout = "20m"

// rgwNativeTarget returns the routed test of an exact selector ^Name$.
func rgwNativeTarget(selector string) (string, bool) {
	for _, name := range rgwNativeTargets {
		if selector == "^"+name+"$" {
			return name, true
		}
	}
	return "", false
}

// rgwNativeProfileProblem admits a routed row only as the checkpoints run:
// one exact known test, no capability or sequence, and every other input at
// the value the checkpoint itself uses (its cluster is fixed by
// rgwNativeStartFixture: Tentacle 20.2.4 pinned image, secure Messenger v2,
// aes256k keys and service tickets, IPv4, one MON/MGR/BlueStore OSD and one
// gateway). An input the checkpoint would ignore is refused rather than shown
// in the job's name without effect.
func rgwNativeProfileProblem(p profile) error {
	if _, ok := rgwNativeTarget(p.testRun); !ok {
		return fmt.Errorf("%s=1 requires CEPH_MSGR_TEST_RUN to be exactly one of ^%s$", rgwNativeEnv, strings.Join(rgwNativeTargets, "$ or ^"))
	}
	defaults := []struct{ name, value, want string }{
		{"CEPH_MSGR_TEST_IMAGE", p.image, profileDefaultImage},
		{"CEPH_MSGR_TEST_KEY_TYPE", p.keyType, "aes256k"},
		{"CEPH_MSGR_TEST_SERVICE_CIPHER", p.serviceCipher, "aes256k"},
		{"CEPH_MSGR_TEST_CONNECTION_MODE", p.connectionMode, "secure"},
		{"CEPH_MSGR_TEST_IP_FAMILY", p.ipFamily, "4"},
		{"CEPH_MSGR_TEST_RUNTIME", p.runtime, "container"},
		{"CEPH_MSGR_TEST_MGR_COUNT", p.mgrCount, "2"},
		{"CEPH_MSGR_TEST_OSD", p.osd, "0"},
		{"CEPH_MSGR_TEST_OSD_PROFILE", p.osdProfile, "legacy-v6"},
		{"CEPH_MSGR_TEST_OSD_STORE", p.osdStore, "memstore"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS", p.osdReplicas, "1"},
		{"CEPH_MSGR_TEST_OSD_CACHE_POOLS", p.osdCachePools, "0"},
		{"CEPH_MSGR_TEST_MGR_SERVICE", p.mgrService, "0"},
		{"CEPH_MSGR_TEST_MON_PING", p.monPing, "0"},
		{"CEPH_MSGR_TEST_MDS_FRESH", p.mdsFresh, "0"},
		{"CEPH_MSGR_TEST_RBD", p.rbdBase, "0"},
		{"CEPH_MSGR_TEST_RACE", p.race, "0"},
		{"CEPH_MSGR_TEST_EXPIRE_TICKETS", p.expireTickets, "0"},
		{"CEPH_MSGR_TEST_IDLE_SESSIONS", p.idleSessions, "0"},
		{"CEPH_MSGR_TEST_AUTH_EPOCH", p.authEpoch, "0"},
		{"CEPH_MSGR_TEST_SHORT_TICKETS", p.shortTickets, "0"},
		{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", p.nativeMgrPreparation, "0"},
		{"CEPH_MSGR_TEST_MODE_REJECTION", p.modeRejection, ""},
		{"CEPH_MSGR_TEST_MAPPED_IPV6", p.mappedIPv6, "0"},
		{"CEPH_MSGR_STRESS_DURATION", p.stressDuration, ""},
		{"CEPH_MSGR_TEST_OSD_SOAK", p.osdSoak, ""},
		{"CEPH_MSGR_TEST_MON_FREEZE", p.monFreeze, ""},
		{"CEPH_MSGR_TEST_CAPABILITIES", p.capabilities, ""},
		{"CEPH_MSGR_TEST_SPARSE_READ_ORACLE", p.sparseReadOracle, ""},
		{"CEPH_MSGR_TEST_MAPEXT_ORACLE", p.mapExtOracle, ""},
		{"CEPH_MSGR_TEST_U64_XATTR_ORACLE", p.u64XattrOracle, ""},
		{"CEPH_MSGR_TEST_WRITESAME_ORACLE", p.writeSameOracle, ""},
	}
	for _, input := range defaults {
		if input.value != input.want {
			return fmt.Errorf("%s=1 runs a checkpoint with its own fixed cluster; %s must be %q, not %q", rgwNativeEnv, input.name, input.want, input.value)
		}
	}
	// The MDS variants are only valid on top of MDS_FRESH=1, refused above.
	return nil
}

// runRGWNative runs the routed checkpoint of p with go test in this module
// and starts no fixture of its own. The raw test2json stream goes to
// rgwNativeStream in the diagnostics directory, which must not exist yet, so
// a stream of an earlier run is never read as this run's result. The output
// of each event is printed after "Running Ceph test suite: fixture", as the
// product suites print their verbose results.
func runRGWNative(ctx context.Context, project string, p profile, stdout io.Writer) error {
	name, ok := rgwNativeTarget(p.testRun)
	if !ok || p.rgwNative != "1" {
		return errors.New("RGW native routing requires an admitted RGW profile")
	}
	if p.diagnostics == "" {
		return errors.New("RGW native routing requires CEPH_MSGR_TEST_DIAGNOSTICS for its test2json stream")
	}
	if err := os.MkdirAll(p.diagnostics, 0o700); err != nil {
		return err
	}
	stream, err := os.OpenFile(filepath.Join(p.diagnostics, rgwNativeStream), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("RGW native stream: %w", err)
	}
	defer stream.Close()
	cmd := exec.CommandContext(ctx, "go", rgwNativeArguments(name)...)
	cmd.Dir = filepath.Join(project, "integration", "fixture")
	cmd.Env = environment(map[string]string{rgwNativeEnv: "1", "GOWORK": "off"}, true)
	cmd.Stderr = os.Stderr
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(stdout, "Running Ceph test suite: %s\n", rgwNativeSuite); err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("RGW native checkpoint %s: %w", name, err)
	}
	copyErr := copyTestStream(output, stream, stdout)
	waitErr := cmd.Wait()
	if waitErr != nil {
		waitErr = fmt.Errorf("RGW native checkpoint %s: %w", name, waitErr)
	}
	return errors.Join(waitErr, copyErr, stream.Sync())
}

// rgwNativeArguments is the go test command of one routed checkpoint.
func rgwNativeArguments(name string) []string {
	return []string{"test", "-mod=readonly", "-json", "-count=1", "-timeout=" + rgwNativeTimeout, "-run", "^" + name + "$", "."}
}

// copyTestStream keeps every line of a test2json stream and prints the output
// text of its events; a line that is not an event is printed as it is.
func copyTestStream(input io.Reader, stream, text io.Writer) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if _, err := stream.Write(append(slices.Clone(line), '\n')); err != nil {
			return err
		}
		var event struct{ Action, Output string }
		if err := json.Unmarshal(line, &event); err != nil {
			if _, err := fmt.Fprintf(text, "%s\n", line); err != nil {
				return err
			}
			continue
		}
		if event.Action == "output" {
			if _, err := io.WriteString(text, event.Output); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

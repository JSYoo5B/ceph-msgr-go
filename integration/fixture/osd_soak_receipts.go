package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var soakReceiptName = regexp.MustCompile(`^osd-soak-v1-(fixture-termination|(steady|failover)-(started|setup|workload|cleanup|recovery-clean|worker-[0-3]|fault-[0-9]{3}-(kill|restart|clean)))\.json$`)

// A revision supplied for an archive is a controller assertion, not a native
// observation. In Git checkouts it must equal the actual tested checkout.
func soakSourceSHA(project, supplied string) (string, error) {
	actual, gitErr := exec.Command("git", "-C", project, "rev-parse", "HEAD").Output()
	if gitErr == nil {
		value := strings.TrimSpace(string(actual))
		if supplied != "" && supplied != value {
			return "", errors.New("soak source SHA differs from tested checkout")
		}
		supplied = value
	}
	decoded, err := hex.DecodeString(supplied)
	if err != nil || len(decoded) != 20 || supplied != strings.ToLower(supplied) {
		return "", errors.New("soak requires a full checkout SHA or explicit CEPH_MSGR_TEST_SOURCE_SHA for an archive")
	}
	return supplied, nil
}

func soakReceiptNames(out string) ([]string, error) {
	entries, err := os.ReadDir(out)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "osd-soak-v1-") {
			continue
		}
		if !entry.Type().IsRegular() || !soakReceiptName.MatchString(entry.Name()) || len(names) >= 256 {
			return nil, errors.New("invalid or excessive soak receipt entries")
		}
		names = append(names, entry.Name())
	}
	return names, nil
}

// Repeated metadata collection is idempotent only for identical bytes; a
// second execution cannot silently replace an earlier observation.
func saveSoakReceipt(source io.Reader, path string) error {
	data, err := io.ReadAll(io.LimitReader(source, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		return errors.New("invalid soak receipt size")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		previous, readErr := os.ReadFile(path)
		if readErr != nil || !bytes.Equal(previous, data) {
			return errors.New("refusing to overwrite a different soak receipt")
		}
		return nil
	}
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return errors.Join(err, file.Close())
}

// This workload verdict runs after owned fixture termination. It is not the
// common successful-artifact manifest; shared bundle integration stays in #1.
func finishSoakReceipts(project, out string, p profile, suiteErr, terminateErr error, terminationElapsed time.Duration) error {
	if p.osdSoak == "" {
		return nil
	}
	duration, err := time.ParseDuration(p.osdSoak)
	if err != nil {
		return err
	}
	manifest, err := os.ReadFile(filepath.Join(out, "fixture-manifest.json"))
	if err != nil {
		return errors.New("soak fixture manifest missing at termination")
	}
	var metadata map[string]any
	if err := json.Unmarshal(manifest, &metadata); err != nil {
		return err
	}
	sum := sha256.Sum256(manifest)
	status := "passed"
	if suiteErr != nil || terminateErr != nil {
		status = "failed"
	}
	verdict := map[string]any{"schema_version": 1, "kind": "fixture_termination", "status": status,
		"configured_source_sha": p.sourceSHA, "configured_image": p.image, "observed_image_id": metadata["image_id"], "architecture": metadata["architecture"],
		"fixture_manifest_sha256": hex.EncodeToString(sum[:]), "configured_duration_ns": int64(duration),
		"suite_exit_success": suiteErr == nil, "termination_succeeded": terminateErr == nil, "termination_elapsed_ns": int64(terminationElapsed)}
	data, err := json.Marshal(verdict)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(out, "osd-soak-v1-fixture-termination.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	command := exec.Command("python3", filepath.Join(project, "tools", "osd_soak_receipts.py"), "verify", "--directory", out,
		"--revision", p.sourceSHA, "--image", p.image, "--duration-ns", fmt.Sprint(int64(duration)))
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("soak immutable receipts incomplete: %s", bytes.TrimSpace(output))
	}
	fmt.Print(string(output))
	return nil
}

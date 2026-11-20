package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func (c *fixtureCluster) runOracleCommand(ctx context.Context, out string, args []string, receipt, label string) error {
	return c.runOracleCommandWithTimeout(ctx, out, args, receipt, label, 80*time.Second)
}

func (c *fixtureCluster) runOracleCommandWithTimeout(ctx context.Context, out string, args []string, receipt, label string, timeout time.Duration) error {
	result, runErr := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{
		Argv:    args,
		Timeout: timeout, MaxStdout: 64 << 10, MaxStderr: 16 << 10,
		Label: label,
	})
	// This oracle contains only disposable object data and ordinary
	// native API errors. Preserve its bounded first-failure traceback rather
	// than discarding it with the generic control command's output.
	data, err := json.Marshal(result)
	if err == nil {
		err = os.WriteFile(filepath.Join(out, receipt), data, 0644)
	}
	if runErr != nil || err != nil {
		return errors.Join(runErr, err)
	}
	if !result.Complete() || result.ExitCode != 0 {
		return fmt.Errorf("%s exit=%d complete=%t; see %s", label, result.ExitCode, result.Complete(), receipt)
	}
	return nil
}

// An owned-pool oracle mode is declared only beside its handler in the
// oracle's staged sources (tools/osd_pool_oracle.py oracle_mode): the line
// "@oracle_mode" directly above "def mode_<name>(", in osd_pool_oracle.py or
// in the mode's own module osd_pool_mode_<name>_oracle.py. The fixture admits
// exactly the modes declared that way, so no list names them.
var (
	ownedPoolModeDeclaration = regexp.MustCompile(`(?m)^@oracle_mode\ndef mode_([a-z][a-z0-9]{0,31})\(`)
	ownedPoolModeDecorator   = regexp.MustCompile(`(?m)^[ \t]*@`)
	ownedPoolModeModule      = regexp.MustCompile(`^osd_pool_mode_([a-z][a-z0-9]{0,31})_oracle\.py$`)
)

// ownedPoolOracleModes reads the modes the owned-pool oracle staged in dir
// declares. Like the oracle's loader it refuses a source with any other
// decorator, a mode declared twice and a mode module that does not declare
// exactly the mode of its file name.
func ownedPoolOracleModes(dir string) (map[string]bool, error) {
	modules, err := filepath.Glob(filepath.Join(dir, "osd_pool_mode_*_oracle.py"))
	if err != nil {
		return nil, err
	}
	sort.Strings(modules)
	modes := map[string]bool{}
	for _, path := range append([]string{filepath.Join(dir, "osd_pool_oracle.py")}, modules...) {
		data, err := readBoundedFile(path, 1<<20)
		if err != nil {
			return nil, fmt.Errorf("owned-pool oracle source: %w", err)
		}
		declared := ownedPoolModeDeclaration.FindAllSubmatch(data, -1)
		if len(ownedPoolModeDecorator.FindAll(data, -1)) != len(declared) {
			return nil, fmt.Errorf("%s marks a mode outside the declaration rule", filepath.Base(path))
		}
		if name := filepath.Base(path); name != "osd_pool_oracle.py" {
			own := ownedPoolModeModule.FindStringSubmatch(name)
			if own == nil || len(declared) != 1 || string(declared[0][1]) != own[1] {
				return nil, fmt.Errorf("%s must declare exactly its own owned-pool oracle mode", name)
			}
		}
		for _, match := range declared {
			mode := string(match[1])
			if modes[mode] {
				return nil, fmt.Errorf("owned-pool oracle mode %s declared twice", mode)
			}
			modes[mode] = true
		}
	}
	if len(modes) == 0 {
		return nil, errors.New("owned-pool oracle declares no mode")
	}
	return modes, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = fmt.Errorf("%s exceeds %d bytes", filepath.Base(path), limit)
	}
	return data, err
}

func (c *fixtureCluster) controlOwnedPoolRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-osd-pool")
	if err != nil {
		return err
	}
	if fields != nil {
		// The modes the staged oracle that runs this request declares.
		modes, err := ownedPoolOracleModes(out)
		if err != nil {
			return err
		}
		if len(fields) != 2 || !nativePoolModeAllowed(c.profile, modes, fields[1]) {
			return errors.New("invalid native owned-pool oracle request")
		}
		if (fields[1] == "servicemetadata" || fields[1] == "servicecompare" || fields[1] == "mgrstate") && c.profile.mgrService != "1" {
			return errors.New("native metadata observation requires the exclusive MGR service profile")
		}
		args := []string{"python3", "/out/osd_pool_oracle.py", fields[1], "/tmp/ceph-msgr-test", "/out"}
		budget := 80 * time.Second
		if fields[1] == "servicewatch" {
			// A bounded 90 s watch may wait through the native 60 s beacon
			// grace and then require a stable absence interval.
			budget = 120 * time.Second
		}
		if fields[1] == "servicecompare" {
			budget = 240 * time.Second
		}
		if err = c.runOracleCommandWithTimeout(ctx, out, args, "osd-pool-command.json", "native owned-pool oracle", budget); err != nil {
			return err
		}
		if err = publishAck(out, "osd-pool-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

var _ = defineControl("osd-pool", frozenControlRank(46), (*fixtureCluster).controlOwnedPoolRequest)

// Cold bootstrap admits only MON MgrMap observation and the bounded service
// identity watch. It does not grant object/pool control to a MON-only fixture.
// The pginterval mode is admitted only by its capability.
// modes are the declared modes of the staged oracle (ownedPoolOracleModes).
func nativePoolModeAllowed(p profile, modes map[string]bool, mode string) bool {
	if mode == "pginterval" && !p.pgIntervalSelected() {
		// Observing one PG's interval, and ending it, is admitted by
		// capability osd-pg-interval alone.
		return false
	}
	return modes[mode] && (p.osd == "1" || p.mgrServiceColdBootstrap() && (mode == "mgrstate" || mode == "servicewatch"))
}

// saveMetadata (diagnostics.go) collects these public receipts of this
// file's controls when they exist.
var _ = collectDiagnosticFiles(
	"osd-pool-claims.json",
	"osd-pool-command.json",
	"osd-pool-input.json",
	"osd-pool-result.json",
)

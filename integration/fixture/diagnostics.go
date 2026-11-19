package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	ctdiagnostics "github.com/JSYoo5B/ceph-testcontainers-go/ceph/diagnostics"
)

// diagnosticFiles are the public native metadata files saveMetadata copies
// from the fixture output directory. Deliberately select public native
// metadata rather than archiving /out, the Ceph configuration, keyrings, auth
// databases or process memory. Each name is declared beside the control or
// fixture code that writes or requests it, with collectDiagnosticFiles; no
// list names them all.
var diagnosticFiles = map[string]bool{}

var diagnosticFileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// collectDiagnosticFiles declares public receipts that saveMetadata copies
// when they exist. Call it in a package-level declaration beside the code
// that writes or requests the files:
//
//	var _ = collectDiagnosticFiles("osd-example-results.json")
//
// A name is one plain file name of the output directory, never a pattern or
// a path, and is declared once; anything else stops the fixture at start.
func collectDiagnosticFiles(names ...string) bool {
	for _, name := range names {
		if !diagnosticFileName.MatchString(name) {
			panic(fmt.Sprintf("diagnostic file %q is not a plain file name", name))
		}
		if diagnosticFiles[name] {
			panic(fmt.Sprintf("diagnostic file %q is declared twice", name))
		}
		diagnosticFiles[name] = true
	}
	return true
}

func saveMetadata(out, diagnostics string) error {
	if diagnostics == "" {
		return nil
	}
	if err := os.MkdirAll(diagnostics, 0o700); err != nil {
		return err
	}
	soakNames, err := soakReceiptNames(out)
	if err != nil {
		return err
	}
	for _, name := range append(slices.Sorted(maps.Keys(diagnosticFiles)), soakNames...) {
		file, err := os.Open(filepath.Join(out, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if strings.HasPrefix(name, "osd-soak-v1-") {
			err = saveSoakReceipt(file, filepath.Join(diagnostics, name))
		} else {
			err = saveBounded(file, filepath.Join(diagnostics, name))
		}
		file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func collectDiagnostics(c *fixtureCluster, out, diagnostics string) error {
	if diagnostics == "" {
		fmt.Fprintln(os.Stderr, "Ceph fixture failed; set CEPH_MSGR_TEST_DIAGNOSTICS to preserve native logs and oracle metadata.")
		return nil
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		return err
	}
	if c == nil || c.cluster == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var additional []ctdiagnostics.Target
	if c.Container != nil {
		additional = append(additional, ctdiagnostics.Target{Name: "messenger-oracles", Role: "client", Container: c.Container})
	}
	report, diagnosticErr := ctdiagnostics.Collect(ctx, c.cluster, ctdiagnostics.Config{AdditionalContainers: additional})
	crashCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	crashes, crashErr := ctdiagnostics.CollectCrashMetadata(crashCtx, c.cluster, ctdiagnostics.CrashMetadataConfig{AdditionalContainers: additional})
	var writeErrs []error
	for _, entry := range []struct {
		name  string
		value any
	}{{"provider-diagnostics.json", report}, {"crash-metadata.json", crashes}} {
		if entry.value == nil {
			continue
		}
		data, err := json.MarshalIndent(entry.value, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(diagnostics, entry.name), data, 0600)
		}
		writeErrs = append(writeErrs, err)
	}
	return errors.Join(diagnosticErr, crashErr, errors.Join(writeErrs...))
}

func saveBounded(source io.Reader, path string) error {
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return fmt.Errorf("diagnostic %s exceeds %d bytes", filepath.Base(path), limit)
	}
	return os.WriteFile(path, data, 0o600)
}

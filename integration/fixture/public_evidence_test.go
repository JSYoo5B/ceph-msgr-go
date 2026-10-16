package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Container suites create their receipts as root, but saveMetadata runs on the
// host after both suites return. A passing suite must not become a harness
// failure just because a newly allowlisted public receipt was owner-readable.
func TestArchivedGoEvidenceIsReadableByHostCollector(t *testing.T) {
	archived := make(map[string]bool, len(metadataFiles))
	for _, name := range metadataFiles {
		archived[name] = true
	}
	files, err := filepath.Glob("../*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatal("integration source inventory", files, err)
	}
	for _, path := range files {
		set := token.NewFileSet()
		source, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(source, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 3 {
				return true
			}
			function, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || function.Sel.Name != "WriteFile" {
				return true
			}
			owner, ok := function.X.(*ast.Ident)
			if !ok || owner.Name != "os" {
				return true
			}
			name := ""
			ast.Inspect(call.Args[0], func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if ok && literal.Kind == token.STRING {
					value, err := strconv.Unquote(literal.Value)
					if err == nil && archived[value] {
						name = value
					}
				}
				return true
			})
			if name == "" {
				return true
			}
			mode, ok := call.Args[2].(*ast.BasicLit)
			if !ok || mode.Kind != token.INT {
				t.Errorf("%s: archived %s needs explicit public permissions or WritePublicEvidence", set.Position(call.Pos()), name)
				return true
			}
			value, err := strconv.ParseUint(mode.Value, 0, 32)
			if err != nil || value&0o044 != 0o044 {
				t.Errorf("%s: archived %s is unreadable by the host fixture owner; use WritePublicEvidence", set.Position(call.Pos()), name)
			}
			return true
		})
	}
}

func TestPublicEvidenceCollectorPreservesPrivateDiagnosticCopies(t *testing.T) {
	out, diagnostics := t.TempDir(), t.TempDir()
	// These public receipts model the files published by the container suite.
	// The host collector's archived copy remains private.
	for _, name := range []string{"osd-checksum-input.json", "osd-mapext-input.json", "osd-watchers-results.json", "osd-snap-checksum-results.json"} {
		if err := os.WriteFile(filepath.Join(out, name), []byte(`{"public":true}`), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(diagnostics)
	if err != nil || len(entries) != 4 {
		t.Fatal("public evidence collector", entries, err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !strings.HasSuffix(entry.Name(), ".json") || runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
			t.Fatal("diagnostic archive is not private", entry.Name(), info, err)
		}
	}
}

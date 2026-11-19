package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Every collected name is declared beside its control: diagnostics.go itself
// names no receipt, and each declaration is a plain file name.
func TestDiagnosticFilesAreDeclaredBesideTheirControls(t *testing.T) {
	if len(diagnosticFiles) == 0 {
		t.Fatal("no diagnostic file is declared")
	}
	for name := range diagnosticFiles {
		if !diagnosticFileName.MatchString(name) || strings.ContainsAny(name, "/*?[") {
			t.Fatal("declared diagnostic file is not a plain file name", name)
		}
	}
	set := token.NewFileSet()
	source, err := parser.ParseFile(set, "diagnostics.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(source, func(node ast.Node) bool {
		if literal, ok := node.(*ast.CompositeLit); ok {
			if array, ok := literal.Type.(*ast.ArrayType); ok && len(literal.Elts) > 0 {
				if element, ok := array.Elt.(*ast.Ident); ok && element.Name == "string" {
					t.Errorf("%s: diagnostics.go keeps a list of names again", set.Position(literal.Pos()))
				}
			}
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok && function.Name == "collectDiagnosticFiles" {
				t.Errorf("%s: a receipt is declared in diagnostics.go, not beside its control", set.Position(call.Pos()))
			}
		}
		return true
	})
	// The merge proof (tools/test_merge_proof.py) reads this line to see the
	// collected files of a merged tree.
	t.Logf("declared diagnostic files: %s", strings.Join(slices.Sorted(maps.Keys(diagnosticFiles)), " "))
}

func TestCollectDiagnosticFilesRefusesPatternsPathsAndRepeats(t *testing.T) {
	before := maps.Clone(diagnosticFiles)
	t.Cleanup(func() { diagnosticFiles = before })
	for _, names := range [][]string{
		{""}, {"."}, {".."}, {".hidden"}, {"../keyring"}, {"sub/osd.json"}, {"*.json"}, {"osd-?.json"}, {"keyring\n"},
		{"osd-pool-result.json"},                       // already declared beside the owned-pool control
		{"zz-new-receipt.json", "zz-new-receipt.json"}, // twice in one declaration
	} {
		diagnosticFiles = maps.Clone(before)
		func() {
			defer func() {
				if recover() == nil {
					t.Error("declaration accepted", names)
				}
			}()
			collectDiagnosticFiles(names...)
		}()
	}
}

// saveMetadata copies declared files that exist and nothing else from the
// output directory.
func TestSaveMetadataCopiesOnlyDeclaredFiles(t *testing.T) {
	out, diagnostics := t.TempDir(), t.TempDir()
	for _, name := range []string{"osd-pool-result.json", "keyring", "ceph.conf", "osd-pool-result.json.tmp", "undeclared-results.json"} {
		if err := os.WriteFile(filepath.Join(out, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveMetadata(out, diagnostics); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	var copied []string
	for _, entry := range entries {
		copied = append(copied, entry.Name())
	}
	if strings.Join(copied, " ") != "osd-pool-result.json" {
		t.Fatal("saveMetadata copied", copied)
	}
}

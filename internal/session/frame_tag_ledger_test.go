package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestFrameTagLedgerCoversEveryTag keeps FRAME_TAG_LEDGER.md aligned with the
// tag values the Messenger package defines.
func TestFrameTagLedgerCoversEveryTag(t *testing.T) {
	doc, err := os.ReadFile("FRAME_TAG_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	want := sourceFrameTags(t)
	seen := map[uint64]bool{}
	for _, line := range strings.Split(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 8 {
			continue
		}
		tag, err := strconv.ParseUint(strings.TrimSpace(cells[1]), 10, 8)
		if err != nil {
			continue
		}
		if seen[tag] || want[tag] == "" {
			t.Errorf("duplicate or undeclared frame tag %d", tag)
		}
		seen[tag] = true
		if name := strings.TrimSpace(cells[2]); name != want[tag] {
			t.Errorf("tag %d names %s, want %s", tag, name, want[tag])
		}
	}
	for tag, name := range want {
		if !seen[tag] {
			t.Errorf("missing frame tag %d %s", tag, name)
		}
	}
}

// Discover the source enum rather than mirroring today's tag names in a map.
// A changed declaration shape requires explicit inventory review, not silent
// omission of a newly added tag from the documentation and evidence join.
func sourceFrameTags(t *testing.T) map[uint64]string {
	t.Helper()
	paths, err := filepath.Glob("../msgr/*.go")
	if err != nil || len(paths) == 0 {
		t.Fatal("frame source inventory unavailable", err)
	}
	want := map[uint64]string{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.CONST {
				continue
			}
			isTag := false
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				if kind, ok := value.Type.(*ast.Ident); ok && kind.Name == "Tag" {
					isTag = true
				}
			}
			if !isTag {
				continue
			}
			for i, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				valid := len(value.Names) == 1 && value.Type == nil && len(value.Values) == 0
				if i == 0 && len(value.Values) == 1 && len(value.Names) == 1 {
					kind, typed := value.Type.(*ast.Ident)
					expr, binary := value.Values[0].(*ast.BinaryExpr)
					if typed && kind.Name == "Tag" && binary && expr.Op == token.ADD {
						left, ident := expr.X.(*ast.Ident)
						right, literal := expr.Y.(*ast.BasicLit)
						valid = ident && left.Name == "iota" && literal && right.Kind == token.INT && right.Value == "1"
					}
				}
				if !valid {
					t.Fatalf("%s: review inventory for changed Tag enum syntax", path)
				}
				tag := uint64(i + 1)
				if want[tag] != "" {
					t.Fatalf("multiple source declarations for tag %d", tag)
				}
				want[tag] = value.Names[0].Name
			}
		}
	}
	if len(want) == 0 {
		t.Fatal("no frame tags discovered")
	}
	return want
}

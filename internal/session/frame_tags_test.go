package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// frameTagDoc is the required start of a Tag constant's doc comment: its
// name, its handshake or session phase, and whether the client sends or
// receives it.
var frameTagDoc = regexp.MustCompile(`^(\w+) \((banner exchange|authentication|post-auth|identification|resume|established)\) is (sent|received)\b`)

// TestEveryFrameTagIsClassified requires every Tag constant the Messenger
// package declares to carry its own doc comment naming the tag, its phase and
// its direction, so a new tag cannot be added without stating what the
// client does with it.
func TestEveryFrameTagIsClassified(t *testing.T) {
	for tag, decl := range sourceFrameTags(t) {
		match := frameTagDoc.FindStringSubmatch(decl.doc)
		if match == nil || match[1] != decl.name {
			t.Errorf("tag %d %s: doc comment must start %q, got %q", tag, decl.name, decl.name+" (<phase>) is sent|received ...", decl.doc)
		}
	}
}

type frameTagDecl struct{ name, doc string }

// Discover the source enum rather than mirroring today's tag names in a map.
// A changed declaration shape requires explicit review, not silent omission
// of a newly added tag.
func sourceFrameTags(t *testing.T) map[uint64]frameTagDecl {
	t.Helper()
	paths, err := filepath.Glob("../msgr/*.go")
	if err != nil || len(paths) == 0 {
		t.Fatal("frame source inventory unavailable", err)
	}
	want := map[uint64]frameTagDecl{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution|parser.ParseComments)
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
				if want[tag].name != "" {
					t.Fatalf("multiple source declarations for tag %d", tag)
				}
				want[tag] = frameTagDecl{value.Names[0].Name, strings.Join(strings.Fields(value.Doc.Text()), " ")}
			}
		}
	}
	if len(want) == 0 {
		t.Fatal("no frame tags discovered")
	}
	return want
}

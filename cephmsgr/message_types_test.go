package cephmsgr

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// messageTypeDoc is the required start of a message type constant's doc
// comment: its name and whether the client sends it, receives it, does both,
// or only defines it.
var messageTypeDoc = regexp.MustCompile(`^(\w+) is (sent and received|sent|received|defined only)\b`)

// TestEveryMessageTypeIsClassified inventories the explicit uint16 message
// type constants of the msgr, mds and osd packages, including names that
// share a type ID, rather than maintaining a second list. Each must carry its
// own doc comment that classifies its direction and, unless it is defined
// only, states its header version handling, so a new message type cannot be
// added without that statement.
func TestEveryMessageTypeIsClassified(t *testing.T) {
	declared := 0
	for _, pkg := range []string{"msgr", "mds", "osd"} {
		paths, err := filepath.Glob(filepath.Join("..", "internal", pkg, "*.go"))
		if err != nil || len(paths) == 0 {
			t.Fatal("message source inventory unavailable", pkg, err)
		}
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
				for _, spec := range group.Specs {
					value := spec.(*ast.ValueSpec)
					for i, name := range value.Names {
						// Message alone is the msgr frame tag, not an envelope type.
						if name.Name == "Message" || !name.IsExported() ||
							!strings.HasSuffix(name.Name, "Message") && !strings.HasSuffix(name.Name, "MessageType") {
							continue
						}
						kind, typed := value.Type.(*ast.Ident)
						if !typed || kind.Name != "uint16" {
							continue // For example, uint32 SessionFlushMessage is a session operation.
						}
						if i >= len(value.Values) {
							t.Errorf("%s.%s needs an explicit message ID for the inventory", pkg, name.Name)
							continue
						}
						literal, ok := value.Values[i].(*ast.BasicLit)
						if !ok || literal.Kind != token.INT {
							t.Errorf("%s.%s needs a literal message ID for the inventory", pkg, name.Name)
							continue
						}
						if _, err := strconv.ParseUint(literal.Value, 0, 16); err != nil {
							t.Fatal(err)
						}
						declared++
						comment := value.Doc
						if group.Lparen == token.NoPos {
							comment = group.Doc
						}
						doc := strings.Join(strings.Fields(comment.Text()), " ")
						match := messageTypeDoc.FindStringSubmatch(doc)
						switch {
						case match == nil || match[1] != name.Name:
							t.Errorf("%s.%s: doc comment must start %q, got %q", pkg, name.Name, name.Name+" is sent|received|sent and received|defined only", doc)
						case match[2] != "defined only" && !strings.Contains(doc, "header"):
							t.Errorf("%s.%s: doc comment must state its header version handling", pkg, name.Name)
						}
					}
				}
			}
		}
	}
	if declared == 0 {
		t.Fatal("no message type constants discovered")
	}
}

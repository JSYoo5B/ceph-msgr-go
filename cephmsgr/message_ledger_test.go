package cephmsgr

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Inventory source declarations rather than maintaining a second enum list:
// a newly added message must not become invisible to this drift check.
func TestMessageLedgerCoversEveryType(t *testing.T) {
	doc, err := os.ReadFile("MESSAGE_LEDGER.md")
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64][]string{}
	for _, pkg := range []string{"msgr", "mds", "osd"} {
		paths, err := filepath.Glob(filepath.Join("..", "internal", pkg, "*.go"))
		if err != nil || len(paths) == 0 {
			t.Fatal("message source inventory unavailable", pkg, err)
		}
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
						id, err := strconv.ParseUint(literal.Value, 0, 16)
						if err != nil {
							t.Fatal(err)
						}
						want[id] = append(want[id], name.Name)
					}
				}
			}
		}
	}
	seen := map[uint64]bool{}
	for _, line := range strings.Split(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 7 {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSpace(cells[1]), 0, 16)
		if err != nil {
			continue // Header or prose, not a message row.
		}
		if seen[id] || len(want[id]) == 0 {
			t.Errorf("duplicate or undeclared message ID %#x", id)
		}
		seen[id] = true
		for _, name := range want[id] {
			if !regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).MatchString(cells[2]) {
				t.Errorf("message %#x row lacks %s", id, name)
			}
		}
	}
	for id, names := range want {
		if !seen[id] {
			t.Errorf("message %#x %v has no ledger row", id, names)
		}
	}
}

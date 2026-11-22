package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// receiptWrite is one write of a public receipt by an integration test: the
// file name it writes, or a path.Match pattern of the names when the test
// builds the name, and where.
type receiptWrite struct {
	Name     string
	Pattern  bool
	Position string
}

// plainControlInput is the shape of a control input the fixture reads under
// a plain name, which a test passes with its request instead
// (integration/control_input_test.go).
var plainControlInput = regexp.MustCompile(`(-input|-manifest)\.json$`)

// receiptWrites returns every write in files of a receipt into a directory:
// a call of os.WriteFile, os.Create, os.OpenFile or
// testcluster.WritePublicEvidence whose path is filepath.Join(dir, name) and
// whose name ends in ".json" and is no control input. The name is a string
// literal, an identifier the same file assigns string literals or built
// names to (each of them is a write), a field that a keyed literal or an
// assignment anywhere in files sets to string literals, or a concatenation
// or fmt.Sprintf of literals and other values, which becomes a pattern. Signals and blobs
// (names without ".json") are not receipts. Not resolved, and returned in
// unresolved: a name passed in as a parameter or built in another way.
func receiptWrites(fset *token.FileSet, files []*ast.File) (writes []receiptWrite, unresolved []string) {
	unquote := func(e ast.Expr) (string, bool) {
		literal, ok := e.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(literal.Value)
		return value, err == nil
	}
	fields := map[string][]string{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.KeyValueExpr:
				if key, ok := n.Key.(*ast.Ident); ok {
					if value, ok := unquote(n.Value); ok {
						fields[key.Name] = append(fields[key.Name], value)
					}
				}
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i, lhs := range n.Lhs {
						if selector, ok := lhs.(*ast.SelectorExpr); ok {
							if value, ok := unquote(n.Rhs[i]); ok {
								fields[selector.Sel.Name] = append(fields[selector.Sel.Name], value)
							}
						}
					}
				}
			}
			return true
		})
	}
	for _, f := range files {
		// pattern turns a built name into a path.Match pattern: literals stay
		// (with their metacharacters escaped), anything else is "*".
		var pattern func(ast.Expr) string
		pattern = func(e ast.Expr) string {
			if value, ok := unquote(e); ok {
				return strings.NewReplacer(`\`, `\\`, "*", `\*`, "?", `\?`, "[", `\[`).Replace(value)
			}
			switch e := e.(type) {
			case *ast.BinaryExpr:
				if e.Op == token.ADD {
					return pattern(e.X) + pattern(e.Y)
				}
			case *ast.CallExpr:
				if selector, ok := e.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Sprintf" && len(e.Args) > 0 {
					if format, ok := unquote(e.Args[0]); ok {
						return regexp.MustCompile(`%[-+# 0-9.]*[a-zA-Z]`).ReplaceAllString(strings.NewReplacer("*", `\*`, "?", `\?`, "[", `\[`).Replace(format), "*")
					}
				}
			}
			return "*"
		}
		// bound holds what the file assigns to each identifier: literals, and
		// built names as patterns.
		bound := map[string][]receiptWrite{}
		bind := func(names []ast.Expr, values []ast.Expr) {
			if len(names) != len(values) {
				return
			}
			for i, name := range names {
				id, ok := name.(*ast.Ident)
				if !ok {
					continue
				}
				if value, ok := unquote(values[i]); ok {
					bound[id.Name] = append(bound[id.Name], receiptWrite{Name: value})
				} else if p := pattern(values[i]); p != "*" {
					bound[id.Name] = append(bound[id.Name], receiptWrite{Name: p, Pattern: true})
				}
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				bind(n.Lhs, n.Rhs)
			case *ast.ValueSpec:
				names := make([]ast.Expr, len(n.Names))
				for i, name := range n.Names {
					names[i] = name
				}
				bind(names, n.Values)
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch owner.Name + "." + selector.Sel.Name {
			case "os.WriteFile", "os.Create", "os.OpenFile", "testcluster.WritePublicEvidence":
			default:
				return true
			}
			join, ok := call.Args[0].(*ast.CallExpr)
			if !ok || len(join.Args) < 2 {
				return true
			}
			if function, ok := join.Fun.(*ast.SelectorExpr); !ok || function.Sel.Name != "Join" {
				return true
			}
			at := fset.Position(call.Pos()).String()
			name := join.Args[len(join.Args)-1]
			var names []receiptWrite
			if value, ok := unquote(name); ok {
				names = []receiptWrite{{Name: value}}
			} else if id, ok := name.(*ast.Ident); ok {
				names = bound[id.Name]
			} else if selector, ok := name.(*ast.SelectorExpr); ok {
				for _, value := range fields[selector.Sel.Name] {
					names = append(names, receiptWrite{Name: value})
				}
			} else if p := pattern(name); p != "*" {
				names = []receiptWrite{{Name: p, Pattern: true}}
			}
			if len(names) == 0 {
				unresolved = append(unresolved, at)
				return true
			}
			for _, w := range names {
				if strings.HasSuffix(w.Name, ".json") && !plainControlInput.MatchString(w.Name) {
					w.Position = at
					writes = append(writes, w)
				}
			}
			return true
		})
	}
	return writes, unresolved
}

// undeclaredReceipts returns the writes whose name no collectDiagnosticFiles
// declaration covers: a literal name not declared, or a pattern that no
// declared name matches. A soak receipt (soakReceiptName) is collected by its
// shape instead.
func undeclaredReceipts(writes []receiptWrite, declared map[string]bool) []string {
	var missing []string
	for _, w := range writes {
		covered := declared[w.Name] || !w.Pattern && soakReceiptName.MatchString(w.Name)
		if w.Pattern {
			covered = false
			for name := range declared {
				if ok, err := path.Match(w.Name, name); err == nil && ok {
					covered = true
					break
				}
			}
		}
		if !covered {
			missing = append(missing, w.Position+": "+w.Name)
		}
	}
	return missing
}

// Every public receipt an integration test writes is declared for
// saveMetadata, so the hosted diagnostics keep it after the fixture is torn
// down.
func TestIntegrationReceiptsAreDeclaredForDiagnostics(t *testing.T) {
	paths, err := filepath.Glob("../*_test.go")
	if err != nil || len(paths) == 0 {
		t.Fatal("integration source inventory", paths, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	writes, unresolved := receiptWrites(fset, files)
	names := map[string]bool{}
	for _, w := range writes {
		names[w.Name] = true
	}
	// The inventory is not vacuous: it sees the copy receipts, a name bound
	// to a variable and one set through a recipe field.
	for _, want := range []string{"osd-copyfrom-three-chunk-loss-results.json", "osd-copyfrom-chunk-source-change-results.json",
		"osd-copyfrom-multichunk-loss-results.json", "rbd-lock-owner-loss-pending-results.json", "mds-cephfs-fcntl-recovery-results.json"} {
		if !names[want] {
			t.Error("the receipt inventory misses", want)
		}
	}
	if missing := undeclaredReceipts(writes, diagnosticFiles); len(missing) != 0 {
		t.Fatal("receipts written without a collectDiagnosticFiles declaration beside their control:\n" + strings.Join(missing, "\n"))
	}
	t.Logf("%d receipt writes declared; %d writes of a name passed in are not resolved: %s", len(writes), len(unresolved), strings.Join(unresolved, ", "))

	declared := map[string]bool{"osd-example-results.json": true, "osd-example-go-set.json": true}
	for name, c := range map[string]struct {
		src     string
		missing int
	}{
		"literal":          {`package p; func f(d string) { testcluster.WritePublicEvidence(filepath.Join(d, "osd-other-results.json"), nil) }`, 1},
		"variable":         {`package p; func f(d string) { n := "osd-other-results.json"; os.WriteFile(filepath.Join(d, n), nil, 0o644) }`, 1},
		"either variable":  {`package p; func f(d string) { n := "osd-example-results.json"; if d == "" { n = "osd-other-results.json" }; os.Create(filepath.Join(d, n)) }`, 1},
		"field":            {`package p; var r = recipe{Results: "osd-other-results.json"}; func f(d string) { os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"built":            {`package p; func f(d, op string) { os.WriteFile(filepath.Join(d, "osd-other-"+op+".json"), nil, 0o644) }`, 1},
		"formatted":        {`package p; func f(d string, i int) { os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-other-%d.json", i)), nil, 0o644) }`, 1},
		"declared":         {`package p; func f(d, op string) { os.WriteFile(filepath.Join(d, "osd-example-results.json"), nil, 0o644); os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }`, 0},
		"not a receipt":    {`package p; func f(d string, i int) { os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-proceed-%d", i)), nil, 0o644); os.WriteFile(filepath.Join(d, "osd-map-1.bin"), nil, 0o644) }`, 0},
		"control input":    {`package p; func f(d string) { os.WriteFile(filepath.Join(d, "osd-other-input.json"), nil, 0o644) }`, 0},
		"soak receipt":     {`package p; func f(d string) { os.WriteFile(filepath.Join(d, "osd-soak-v1-steady-setup.json"), nil, 0o644) }`, 0},
		"not a soak shape": {`package p; func f(d string) { os.WriteFile(filepath.Join(d, "osd-soak-v1-other.json"), nil, 0o644) }`, 1},
		"elsewhere":        {`package p; func f(d string) { os.WriteFile(d+"/osd-other-results.json", nil, 0o644) }`, 0},
	} {
		f, err := parser.ParseFile(fset, name+".go", c.src, 0)
		if err != nil {
			t.Fatal(name, err)
		}
		writes, _ := receiptWrites(fset, []*ast.File{f})
		if missing := undeclaredReceipts(writes, declared); len(missing) != c.missing {
			t.Errorf("%s: %d undeclared receipts, want %d: %q", name, len(missing), c.missing, missing)
		}
	}
	f, err := parser.ParseFile(fset, "param.go", `package p; func f(d, n string) { os.WriteFile(filepath.Join(d, n), nil, 0o644) }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if writes, unresolved := receiptWrites(fset, []*ast.File{f}); len(writes) != 0 || !slices.Equal(unresolved, []string{"param.go:1:34"}) {
		t.Error("a name passed in is not reported as unresolved:", writes, unresolved)
	}
}

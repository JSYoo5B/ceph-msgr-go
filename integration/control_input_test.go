package integration_test

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

// controlInputFile is the shape of the file names the fixture's handlers and
// oracles read as a request's input: "<control>-input.json" and the OSD map
// manifest.
var controlInputFile = regexp.MustCompile(`(-input|-manifest)\.json$`)

// controlInputWrites returns the writes in the parsed files that put a
// control input under the plain name the fixture reads: a call of
// os.WriteFile, os.Create, os.OpenFile or testcluster.WritePublicEvidence
// whose path names such a file, directly or through a variable the same file
// assigned from such a path. A test passes an input with its request instead
// (testcluster.ControlInput), so an abandoned request is never answered with
// the input of the request after it. Not checked: a path built in another
// file or through a function.
func controlInputWrites(fset *token.FileSet, files []*ast.File) []string {
	names := func(e ast.Node) bool {
		found := false
		ast.Inspect(e, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if v, err := strconv.Unquote(lit.Value); err == nil && controlInputFile.MatchString(v) {
					found = true
				}
			}
			return !found
		})
		return found
	}
	var found []string
	for _, f := range files {
		paths := map[string]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			if a, ok := n.(*ast.AssignStmt); ok && len(a.Lhs) == len(a.Rhs) {
				for i, rhs := range a.Rhs {
					if id, ok := a.Lhs[i].(*ast.Ident); ok && names(rhs) {
						paths[id.Name] = true
					}
				}
			}
			return true
		})
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			switch pkg.Name + "." + sel.Sel.Name {
			case "os.WriteFile", "os.Create", "os.OpenFile", "testcluster.WritePublicEvidence":
			default:
				return true
			}
			path := call.Args[0]
			if id, ok := path.(*ast.Ident); ok && paths[id.Name] || names(path) {
				found = append(found, fset.Position(call.Pos()).String())
			}
			return true
		})
	}
	return found
}

func TestControlInputsTravelWithTheirRequests(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatal("integration sources", paths, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if found := controlInputWrites(fset, files); len(found) != 0 {
		t.Fatal("control inputs written under the name the fixture reads; pass them with their request (testcluster.ControlInput):", strings.Join(found, ", "))
	}
	for name, src := range map[string]string{
		"direct":   `package p; func f(dir string) { os.WriteFile(filepath.Join(dir, "osd-example-input.json"), nil, 0600) }`,
		"evidence": `package p; func f(dir string) { testcluster.WritePublicEvidence(filepath.Join(dir, "osd-map-manifest.json"), nil) }`,
		"variable": `package p; func f(dir string) { p := filepath.Join(dir, "osd-example-input.json"); os.Create(p) }`,
	} {
		f, err := parser.ParseFile(fset, name+".go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(controlInputWrites(fset, []*ast.File{f})) != 1 {
			t.Error("not refused:", name)
		}
	}
	ok, err := parser.ParseFile(fset, "ok.go", `package p; func f(dir string) { os.WriteFile(filepath.Join(dir, "osd-example-results.json"), nil, 0600); os.Remove(filepath.Join(dir, "osd-example-input.json")) }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if found := controlInputWrites(fset, []*ast.File{ok}); len(found) != 0 {
		t.Error("refused a result write or an input removal:", found)
	}
}

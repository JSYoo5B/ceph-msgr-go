package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"unicode"
	"unicode/utf8"
)

// Decl is one top-level function of a test file. Test reports whether
// `go test` would treat it as a test function: the cmd/go name and signature
// rules for TestXxx(*testing.T), without type parameters.
type Decl struct {
	Dir  string `json:"dir"`
	File string `json:"file"`
	Name string `json:"name"`
	Line int    `json:"line"`
	Test bool   `json:"test"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "testdecls:", err)
		os.Exit(1)
	}
}

func run(dirs []string, out io.Writer) error {
	if len(dirs) == 0 {
		return errors.New("usage: testdecls DIR...")
	}
	encoder := json.NewEncoder(out)
	for _, dir := range dirs {
		decls, err := declarations(build.Default, dir)
		if err != nil {
			return err
		}
		for _, decl := range decls {
			if err := encoder.Encode(decl); err != nil {
				return err
			}
		}
	}
	return nil
}

// declarations parses the _test.go files that ctx selects in dir (build
// constraints, GOOS, GOARCH, CGO_ENABLED and release tags) and returns their
// top-level functions. Methods are not top-level functions and are omitted.
func declarations(ctx build.Context, dir string) ([]Decl, error) {
	pkg, err := ctx.ImportDir(dir, 0)
	var none *build.NoGoError
	if errors.As(err, &none) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var decls []Decl
	fset := token.NewFileSet()
	for _, name := range append(append([]string{}, pkg.TestGoFiles...), pkg.XTestGoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			decls = append(decls, Decl{Dir: dir, File: name, Name: fn.Name.Name,
				Line: fset.Position(fn.Pos()).Line, Test: isTest(fn)})
		}
	}
	return decls, nil
}

// isTest follows cmd/go's test discovery (isTest and isTestFunc in
// cmd/go/internal/load/test.go): the name is Test or Test followed by a
// non-lowercase rune, there are no results and exactly one parameter of type
// *T or *x.T. Like cmd/go it cannot see how "testing" was imported, so any
// selector ending in T counts. A generic function is refused by go test.
func isTest(fn *ast.FuncDecl) bool {
	name := fn.Name.Name
	if len(name) < 4 || name[:4] != "Test" {
		return false
	}
	if len(name) > 4 {
		r, _ := utf8.DecodeRuneInString(name[4:])
		if unicode.IsLower(r) {
			return false
		}
	}
	typ := fn.Type
	if typ.TypeParams != nil && len(typ.TypeParams.List) > 0 {
		return false
	}
	if typ.Results != nil && len(typ.Results.List) > 0 {
		return false
	}
	if typ.Params == nil || len(typ.Params.List) != 1 || len(typ.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := typ.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := ptr.X.(type) {
	case *ast.Ident:
		return x.Name == "T"
	case *ast.SelectorExpr:
		return x.Sel.Name == "T"
	}
	return false
}

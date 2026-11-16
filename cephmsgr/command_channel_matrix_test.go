package cephmsgr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

// TestCommandChannelMatrixSelectorsExist keeps the integration test names in
// COMMAND_CHANNEL_MATRIX.md resolvable. go test -list says which tests the two
// suite packages run; Go's parser, over the files the build context selects,
// says which file declares each one. A name resolves when one compiled test
// file of a suite package declares it with the test shape and go test lists
// it for that package.
func TestCommandChannelMatrixSelectorsExist(t *testing.T) {
	doc, err := os.ReadFile("COMMAND_CHANNEL_MATRIX.md")
	if err != nil {
		t.Fatal(err)
	}
	names := regexp.MustCompile("`(Test[A-Za-z0-9]+)`").FindAllStringSubmatch(string(doc), -1)
	if len(names) == 0 {
		t.Fatal("matrix names no tests")
	}
	listed := listedSuiteTests(t)
	declared := map[string]map[string][]string{}
	for _, suite := range []string{"integration", "cephmsgr"} {
		decls, err := suiteTestDecls(filepath.Join("..", suite))
		if err != nil {
			t.Fatal(err)
		}
		// The parser's view must equal Go's own listing, name by name and
		// in how often each name is listed, so the shape rule cannot drift
		// from go test's discovery unnoticed.
		if got, want := declarationCounts(decls), listed[suite]; !maps.Equal(got, want) {
			t.Fatalf("%s: parsed test declarations %v differ from go test -list %v", suite, got, want)
		}
		declared[suite] = decls
	}
	for _, m := range names {
		if _, ok := declared["integration"][m[1]]; ok {
			continue
		}
		if _, ok := declared["cephmsgr"][m[1]]; ok {
			continue
		}
		t.Errorf("selector %s is not a top-level test declared in integration/ or cephmsgr/", m[1])
	}
}

// suiteTestDecls returns test name -> declaring files for the _test.go files
// that the build context (CGO disabled, as the listing) selects in dir. Only
// test-shaped declarations are indexed, so repeated init functions play no
// part; the internal and the external test package may each declare a test
// of the same name, which go test lists and runs twice.
func suiteTestDecls(dir string) (map[string][]string, error) {
	ctx := build.Default
	ctx.CgoEnabled = false
	pkg, err := ctx.ImportDir(dir, 0)
	if err != nil {
		return nil, err
	}
	decls := map[string][]string{}
	fset := token.NewFileSet()
	for _, name := range append(slices.Clone(pkg.TestGoFiles), pkg.XTestGoFiles...) {
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && hasTestShape(fn) {
				decls[fn.Name.Name] = append(decls[fn.Name.Name], name)
			}
		}
	}
	return decls, nil
}

// declarationCounts maps each test name to how many files declare it, the
// count go test -list reports for the name.
func declarationCounts(decls map[string][]string) map[string]int {
	counts := make(map[string]int, len(decls))
	for name, files := range decls {
		counts[name] = len(files)
	}
	return counts
}

// hasTestShape follows cmd/go's discovery: a top-level function named Test or
// Test plus a non-lowercase rune, without type parameters or results, taking
// one *T or *x.T. TestMain(*testing.M) is the harness, not a test.
func hasTestShape(fn *ast.FuncDecl) bool {
	name, typ := fn.Name.Name, fn.Type
	if fn.Recv != nil || !strings.HasPrefix(name, "Test") {
		return false
	}
	if r, _ := utf8.DecodeRuneInString(name[4:]); len(name) > 4 && unicode.IsLower(r) {
		return false
	}
	if typ.TypeParams != nil || (typ.Results != nil && len(typ.Results.List) > 0) ||
		len(typ.Params.List) != 1 || len(typ.Params.List[0].Names) > 1 {
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

// listedSuiteTests returns, per suite package, each top-level test go test
// -list reports and how many times it is listed (twice when the internal and
// the external test package both declare it). Listing compiles the packages
// and runs no test.
func listedSuiteTests(t *testing.T) map[string]map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-json", "-list", "^Test", "./integration", "./cephmsgr")
	cmd.Dir = ".."
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("go test -list: %v\n%s\n%s", err, output, stderr.Bytes())
	}
	listed := map[string]map[string]int{"integration": {}, "cephmsgr": {}}
	done := map[string]string{}
	name := regexp.MustCompile(`^Test\w*$`)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		var event struct{ Action, Package, Output, Test string }
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		switch {
		case event.Package == "" || event.Test != "":
			t.Fatalf("unexpected go test -json event: %s", scanner.Text())
		case event.Action == "output":
			line := strings.TrimSuffix(event.Output, "\n")
			switch {
			case name.MatchString(line) && listed[path.Base(event.Package)] != nil:
				listed[path.Base(event.Package)][line]++
			case strings.HasPrefix(line, "ok  \t"+event.Package+"\t"):
			default:
				t.Fatalf("unexpected listing output for %s: %q", event.Package, line)
			}
		case event.Action == "pass" || event.Action == "fail" || event.Action == "skip":
			done[path.Base(event.Package)] = event.Action
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for _, suite := range []string{"integration", "cephmsgr"} {
		if done[suite] != "pass" {
			t.Fatalf("listing of %s ended with %q", suite, done[suite])
		}
	}
	return listed
}

// TestSuiteTestDeclsFollowGoDiscovery is the negative control of the parser
// side: a raw string that reads like a declaration, a build-ignored file, a
// method and the TestMain harness declare no test; an aliased testing import
// does. Repeated init functions beside a test, and the same test name in the
// internal and the external test package, are valid Go and are accepted.
func TestSuiteTestDeclsFollowGoDiscovery(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"right_test.go":    "package p\n\nimport \"testing\"\n\nfunc init() {}\n\nfunc init() {}\n\nfunc TestActual(t *testing.T) {}\n",
		"external_test.go": "package p_test\n\nimport \"testing\"\n\nfunc TestActual(t *testing.T) {}\n",
		"wrong_test.go":    "package p\n\nconst doc = `\nfunc TestActual(t *testing.T) {}\nfunc TestWrongOnly(t *testing.T) {}\n`\n",
		"ignored_test.go":  "//go:build ignore\n\npackage p\n\nimport \"testing\"\n\nfunc TestIgnored(t *testing.T) {}\n",
		"alias_test.go":    "package p\n\nimport testpkg \"testing\"\n\ntype suite struct{}\n\nfunc TestAlias(t *testpkg.T) {}\n\nfunc (suite) TestMethod(t *testpkg.T) {}\n\nfunc TestMain(m *testpkg.M) {}\n\nfunc Testable(t *testpkg.T) {}\n",
	}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	decls, err := suiteTestDecls(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"TestActual": {"external_test.go", "right_test.go"}, "TestAlias": {"alias_test.go"}}
	for _, files := range decls {
		slices.Sort(files)
	}
	if !maps.EqualFunc(decls, want, slices.Equal) {
		t.Fatalf("got %v, want %v", decls, want)
	}
	// go test lists TestActual twice for this package; the counts compared
	// with the listing must say so.
	if got, want := declarationCounts(decls), map[string]int{"TestActual": 2, "TestAlias": 1}; !maps.Equal(got, want) {
		t.Fatalf("declaration counts %v, want %v", got, want)
	}
}

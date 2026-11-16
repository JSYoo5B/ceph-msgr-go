package main

import (
	"bytes"
	"encoding/json"
	"go/build"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var shapes = map[string]string{
	"doc.go": "package p\n",
	"shapes_test.go": `package p

import (
	"os"
	"testing"
)

type suite struct{}

func Test(t *testing.T) {}

func TestX(_ *testing.T) {}

func (s suite) TestY(t *testing.T) {}

func Testable(n int) bool { return n > 0 }

func TestMain(m *testing.M) { os.Exit(m.Run()) }

func TestResult(t *testing.T) error { return nil }

func TestTwo(a, b *testing.T) {}

func TestGeneric[P any](t *testing.T) {}

func TestÉcole(t *testing.T) {}

func Test_under(t *testing.T) {}
`,
	"alias_test.go": `package p

import testpkg "testing"

func TestAlias(t *testpkg.T) {}
`,
	"wrong_test.go": "package p\n\nconst doc = `\nfunc TestActual(t *testing.T) {}\n`\n",
	"right_test.go": "package p\n\nimport \"testing\"\n\nfunc TestActual(t *testing.T) {}\n",
	"ignored_test.go": `//go:build ignore

package p

import "testing"

func TestIgnored(t *testing.T) {}
`,
	"external_test.go": `package p_test

import . "testing"

func TestDot(t *T) {}
`,
	"helper.go": "package p\n\nimport \"testing\"\n\nfunc TestInSource(t *testing.T) {}\n",
}

func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestDeclarationsFollowGoTestDiscovery(t *testing.T) {
	dir := writeTree(t, shapes)
	ctx := build.Default
	ctx.CgoEnabled = false
	decls, err := declarations(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, d := range decls {
		if d.Dir != dir || d.Line == 0 {
			t.Fatalf("bad position %+v", d)
		}
		if _, seen := got[d.Name]; seen {
			t.Fatalf("duplicate %s", d.Name)
		}
		got[d.Name] = d.File + map[bool]string{true: " test", false: " func"}[d.Test]
	}
	// Literal expectation: the build-ignored file, the non-test source file,
	// the method and the raw string are not declarations of a test file.
	want := map[string]string{
		"Test":        "shapes_test.go test",
		"TestX":       "shapes_test.go test",
		"Testable":    "shapes_test.go func",
		"TestMain":    "shapes_test.go func",
		"TestResult":  "shapes_test.go func",
		"TestTwo":     "shapes_test.go func",
		"TestGeneric": "shapes_test.go func",
		"TestÉcole":   "shapes_test.go test",
		"Test_under":  "shapes_test.go test",
		"TestAlias":   "alias_test.go test",
		"TestActual":  "right_test.go test",
		"TestDot":     "external_test.go test",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for name, where := range want {
		if got[name] != where {
			t.Errorf("%s: got %q, want %q", name, got[name], where)
		}
	}
}

func TestRunPrintsJSONLinesAndRefusesBadInput(t *testing.T) {
	dir := writeTree(t, map[string]string{"a_test.go": "package p\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {}\n"})
	var out bytes.Buffer
	if err := run([]string{dir}, &out); err != nil {
		t.Fatal(err)
	}
	var decl Decl
	if err := json.Unmarshal(out.Bytes(), &decl); err != nil || decl != (Decl{Dir: dir, File: "a_test.go", Name: "TestA", Line: 5, Test: true}) {
		t.Fatalf("got %q, %v", out.String(), err)
	}
	empty := t.TempDir()
	out.Reset()
	if err := run([]string{empty}, &out); err != nil || out.Len() != 0 {
		t.Fatalf("empty directory: %q, %v", out.String(), err)
	}
	broken := writeTree(t, map[string]string{"a_test.go": "package p\n\nfunc TestA(\n"})
	if err := run([]string{broken}, &out); err == nil {
		t.Fatal("accepted a file that does not parse")
	}
	mixed := writeTree(t, map[string]string{"a_test.go": "package p\n", "b_test.go": "package q\n"})
	if err := run([]string{mixed}, &out); err == nil {
		t.Fatal("accepted two packages in one directory")
	}
	if err := run([]string{filepath.Join(empty, "absent")}, &out); err == nil {
		t.Fatal("accepted a missing directory")
	}
	if err := run(nil, &out); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Fatalf("no arguments: %v", err)
	}
}

// TestShapeMatchesGoTestList compares the test shape with go test's own
// listing for every shape go test compiles.
func TestShapeMatchesGoTestList(t *testing.T) {
	files := map[string]string{"go.mod": "module example.com/p\n\ngo 1.24\n"}
	for name, text := range shapes {
		files[name] = text
	}
	files["shapes_test.go"] = strings.NewReplacer(
		"func TestResult(t *testing.T) error { return nil }", "",
		"func TestTwo(a, b *testing.T) {}", "",
		"func TestGeneric[P any](t *testing.T) {}", "").Replace(shapes["shapes_test.go"])
	dir := writeTree(t, files)
	cmd := exec.Command("go", "test", "-list", "^Test", ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go test -list: %v\n%s", err, output)
	}
	listed := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if strings.HasPrefix(line, "Test") {
			listed[line] = true
		}
	}
	decls, err := declarations(build.Default, dir)
	if err != nil {
		t.Fatal(err)
	}
	shaped := map[string]bool{}
	for _, d := range decls {
		if d.Test {
			shaped[d.Name] = true
		}
	}
	if len(listed) == 0 || !maps.Equal(listed, shaped) {
		t.Fatalf("go test lists %v, the parser finds %v", listed, shaped)
	}
}

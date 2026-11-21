package integration_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"testing"
)

// ownedPoolHashPolicyViolations counts the owned-pool create requests in the
// parsed files and returns those whose HASHPSPOOL policy is not taken from the selected fixture
// profile: a create whose request is not a map literal, has no "hashpspool"
// key, or sets it to the constant true or false. The owned-pool oracle
// (tools/osd_pool_oracle.py mode_create) never sets the flag; the pool takes
// the fixture's osd_pool_default_flag_hashpspool, which the legacy-v6 profile
// sets false and the replicated-PG profiles set true, and the oracle refuses
// a pool whose native flag differs from the request. A constant therefore
// fails in every suite that selects the test under the other profile, and the
// refusal during setup latches the provider's error for the later tests.
// Not checked: a variable that is constant under both profiles.
func ownedPoolHashPolicyViolations(fset *token.FileSet, files []*ast.File) (int, []string) {
	creates, found := 0, []string(nil)
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 4 {
				return true
			}
			if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "nativePool" && fn.Name != "nativePoolResult" {
				return true
			}
			if mode, ok := call.Args[2].(*ast.BasicLit); !ok || mode.Value != strconv.Quote("create") {
				return true
			}
			creates++
			at := fset.Position(call.Pos()).String()
			request, ok := call.Args[3].(*ast.CompositeLit)
			if !ok {
				found = append(found, at+": create request is not a map literal")
				return true
			}
			var value ast.Expr
			for _, e := range request.Elts {
				if kv, ok := e.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.BasicLit); ok && key.Value == strconv.Quote("hashpspool") {
						value = kv.Value
					}
				}
			}
			switch v := value.(type) {
			case nil:
				found = append(found, at+": create request has no hashpspool")
			case *ast.Ident:
				if v.Name == "true" || v.Name == "false" {
					found = append(found, at+": hashpspool is the constant "+v.Name)
				}
			}
			return true
		})
	}
	return creates, found
}

func parseOwnedPoolSources(t *testing.T, sources map[string]string) (*token.FileSet, []*ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	var files []*ast.File
	for name, src := range sources {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return fset, files
}

// TestOwnedPoolCreatesFollowFixtureHashPolicy checks every owned-pool create
// request of this package's tests without a cluster.
func TestOwnedPoolCreatesFollowFixtureHashPolicy(t *testing.T) {
	names, err := filepath.Glob("*_test.go")
	if err != nil || len(names) == 0 {
		t.Fatal("integration test sources", names, err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	creates, found := ownedPoolHashPolicyViolations(fset, files)
	if creates == 0 {
		t.Fatal("no owned-pool create request found")
	}
	for _, v := range found {
		t.Error(v)
	}
}

func TestOwnedPoolHashPolicyViolations(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"profile variable", `nativePool(t, ctx, "create", map[string]any{"slot": 1, "hashpspool": hash})`, 0},
		{"profile field", `nativePoolResult(t, ctx, "create", map[string]any{"hashpspool": r.hash}, nil)`, 0},
		{"other mode", `nativePool(t, ctx, "cleanup", nil)`, 0},
		{"constant true", `nativePool(t, ctx, "create", map[string]any{"slot": 1, "hashpspool": true})`, 1},
		{"constant false", `nativePoolResult(t, ctx, "create", map[string]any{"hashpspool": false}, nil)`, 1},
		{"missing key", `nativePool(t, ctx, "create", map[string]any{"slot": 1})`, 1},
		{"opaque request", `nativePool(t, ctx, "create", request)`, 1},
	} {
		fset, files := parseOwnedPoolSources(t, map[string]string{"x_test.go": "package x\nfunc f() {\n" + tc.body + "\n}\n"})
		if _, got := ownedPoolHashPolicyViolations(fset, files); len(got) != tc.want {
			t.Errorf("%s: %q, want %d violations", tc.name, got, tc.want)
		}
	}
}

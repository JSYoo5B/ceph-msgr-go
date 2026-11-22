package main

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// unenumeratedWrites are the path.Match patterns of built names a test
// writes whose holes the guard cannot enumerate, each with the reason it is
// not a lost receipt. A pattern no write uses fails the test.
var unenumeratedWrites = map[string]string{
	// The lease record the fixture reads back under the lease ID it issued
	// (leaseID in daemon_lease.go: a positive decimal integer).
	"daemon-lease-*.*": "lease record, a signal",
	// The kill acknowledgement of the copy-from thaw test, named after the
	// fixture's request ID.
	"osd-kill.*": "kill acknowledgement, a signal",
	// Soak receipts by worker and fault index. saveMetadata collects them by
	// the soakReceiptName shape, and the indices are decimal numbers the soak
	// workload bounds at run time.
	"osd-soak-v1-*-worker-*.json":  "soak worker receipt",
	"osd-soak-v1-*-fault-*-*.json": "soak fault receipt",
}

// undeclaredReceipts returns each receipt name a write can produce that is
// not covered: an exact name without a collectDiagnosticFiles declaration (a
// soak receipt must have the soakReceiptName shape instead), or a name with
// a hole that no pattern of unenumerated matches. It also returns the
// patterns a write used.
func undeclaredReceipts(writes []receiptWrite, declared map[string]bool, unenumerated map[string]string) (missing []string, used map[string]bool) {
	used = map[string]bool{}
	for _, w := range writes {
		for _, s := range w.Names {
			if !s.mayBeReceipt() {
				continue
			}
			name := s.String()
			listed := false
			if !s.exact() {
				for pattern := range unenumerated {
					if ok, _ := path.Match(pattern, name); ok {
						used[pattern], listed = true, true
					}
				}
			}
			switch {
			case s.exact() && strings.HasPrefix(name, "osd-soak-v1-"):
				if !soakReceiptName.MatchString(name) {
					missing = append(missing, w.Position+": soak receipt "+name+" does not have the collected shape")
				}
			case s.exact():
				if !declared[name] {
					missing = append(missing, w.Position+": "+name+" is not declared")
				}
			case !listed:
				missing = append(missing, w.Position+": "+name+" cannot be enumerated")
			}
		}
	}
	return missing, used
}

// Every public receipt an integration test writes is declared for
// saveMetadata by its exact name, so the hosted diagnostics keep it after
// the fixture is torn down.
func TestIntegrationReceiptsAreDeclaredForDiagnostics(t *testing.T) {
	paths, err := filepath.Glob("../*_test.go")
	if err != nil || len(paths) == 0 {
		t.Fatal("integration source inventory", paths, err)
	}
	fset := token.NewFileSet()
	imports := &sourceImporter{fset: fset, root: "../..", std: importer.ForCompiler(fset, "source", nil), packages: map[string]*types.Package{}}
	var files []*ast.File
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	info, errs := typeCheck(fset, files, imports)
	if len(errs) != 0 {
		t.Fatalf("the integration tests do not type-check here (%d errors), first: %v", len(errs), errs[0])
	}
	writes := receiptWrites(fset, files, info)
	names := map[string]bool{}
	count := 0
	for _, w := range writes {
		for _, s := range w.Names {
			if s.mayBeReceipt() {
				names[s.String()] = true
				count++
			}
		}
	}
	// The inventory is not vacuous: it sees the copy receipts, a name bound
	// to a variable, one set through a recipe field, one returned in a
	// recipe, the names of a closed op set and of a range over a table, and
	// soak receipts named through a method receiver and a parameter.
	for _, want := range []string{"osd-copyfrom-three-chunk-loss-results.json", "osd-copyfrom-chunk-source-change-results.json",
		"osd-copyfrom-multichunk-loss-results.json", "rbd-lock-owner-loss-pending-results.json", "mds-cephfs-fcntl-recovery-results.json",
		"mds-cephfs-heldflock-split-fragments-results.json", "osd-xattr-permission-list.json", "osd-xattr-permission-go-remove.json",
		"osd-soak-v1-steady-started.json", "osd-soak-v1-failover-recovery-clean.json", "osd-soak-v1-steady-worker-*.json"} {
		if !names[want] {
			t.Error("the receipt inventory misses", want)
		}
	}
	missing, used := undeclaredReceipts(writes, diagnosticFiles, unenumeratedWrites)
	if len(missing) != 0 {
		t.Error("receipts written without an exact collectDiagnosticFiles declaration beside their control:\n" + strings.Join(missing, "\n"))
	}
	for _, pattern := range slices.Sorted(maps.Keys(unenumeratedWrites)) {
		if !used[pattern] {
			t.Error("unenumeratedWrites lists a pattern no write builds:", pattern)
		}
	}
	t.Logf("%d receipt names of %d writes checked", count, len(writes))

	declared := map[string]bool{"osd-example-results.json": true, "osd-example-go-set.json": true}
	unenumerated := map[string]string{"osd-signal-*.*": "signal"}
	const header = `package p; import ("encoding/json"; "fmt"; "os"; "path/filepath"; "github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"); ` +
		`var _, _, _, _, _ = json.Marshal, fmt.Sprint, os.Getenv, filepath.Base, testcluster.WritePublicEvidence; ` +
		`type recipe struct{ Results string }; type action struct{ Op string }; type input struct{ Operations []action }; ` +
		`type record struct{ State, Lease string }; type client interface{ Name() string }; `
	for name, c := range map[string]struct {
		src     string
		missing int
	}{
		"literal":         {`func f(d string) { testcluster.WritePublicEvidence(filepath.Join(d, "osd-other-results.json"), nil) }`, 1},
		"variable":        {`func f(d string) { n := "osd-other-results.json"; os.WriteFile(filepath.Join(d, n), nil, 0o644) }`, 1},
		"either variable": {`func f(d string) { n := "osd-example-results.json"; if d == "" { n = "osd-other-results.json" }; os.Create(filepath.Join(d, n)) }`, 1},
		"shadowed":        {`func f(d string) { n := "osd-example-results.json"; os.WriteFile(filepath.Join(d, n), nil, 0o644); { n := "osd-other-results.json"; os.WriteFile(filepath.Join(d, n), nil, 0o644) } }`, 1},
		"constant":        {`const n = "osd-other-" + "results.json"; func f(d string) { os.WriteFile(filepath.Join(d, n), nil, 0o644) }`, 1},
		"field":           {`var r = recipe{Results: "osd-other-results.json"}; func f(d string) { os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"field assigned":  {`func f(d string) { r := recipe{Results: "osd-example-results.json"}; r.Results = "osd-other-results.json"; os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"positional":      {`func f(d string) { r := recipe{"osd-other-results.json"}; os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"returned":        {`func g() *recipe { return &recipe{Results: "osd-other-results.json"} }; func f(d string) { os.WriteFile(filepath.Join(d, g().Results), nil, 0o644) }`, 1},
		"path variable":   {`func f(d string) { p := filepath.Join(d, "osd-other-results.json"); os.WriteFile(p, nil, 0o644) }`, 1},
		"renamed":         {`func f(d, tmp string) { os.Rename(tmp, filepath.Join(d, "osd-other-results.json")) }`, 1},
		// The review's case: one op declared, another not.
		"one of two ops":  {`func f(d string) { for _, op := range []string{"set", "other"} { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) } }`, 1},
		"declared ops":    {`func f(d string) { for _, op := range []string{"set"} { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) } }`, 0},
		"parameter":       {`func w(d, op string) { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }; func f(d string) { w(d, "set"); w(d, "other") }`, 1},
		"closure":         {`func f(d string) { w := func(op string) { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }; w("set"); w("other") }`, 1},
		"method receiver": {`type s struct{ op string }; func (x *s) w(d string) { os.WriteFile(filepath.Join(d, "osd-example-go-"+x.op+".json"), nil, 0o644) }; func f(d string) { (&s{op: "set"}).w(d); y := &s{op: "other"}; y.w(d) }`, 1},
		"table field":     {`func f(d string) { for _, c := range []struct{ a action }{{a: action{Op: "set"}}, {a: action{Op: "other"}}} { os.WriteFile(filepath.Join(d, "osd-example-go-"+c.a.Op+".json"), nil, 0o644) } }`, 1},
		"appended":        {`func w(d string, in input) { os.WriteFile(filepath.Join(d, "osd-example-go-"+in.Operations[0].Op+".json"), nil, 0o644) }; func f(d string) { in := input{Operations: []action{{Op: "set"}}}; w(d, in); in.Operations = append(in.Operations, action{Op: "other"}); w(d, in) }`, 1},
		"map index":       {`func f(d, op string) { n, ok := map[string]string{"a": "osd-example-results.json", "b": "osd-other-results.json"}[op]; if ok { os.WriteFile(filepath.Join(d, n), nil, 0o644) } }`, 1},
		"formatted":       {`func f(d string) { s := "results"; os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-example-%s.json", s)), nil, 0o644) }`, 0},
		"formatted index": {`func f(d string) { i := 7; os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-other-%03d.json", i)), nil, 0o644) }`, 1},
		// A value the guard cannot enumerate fails instead of matching a
		// declared name.
		"unknown op":         {`func f(d string, in input) { op := in.Operations[0].Op; os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }`, 1},
		"unknown index":      {`func w(d string, i int) { os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-example-go-%d.json", i)), nil, 0o644) }; func f(d string) { for i := range 3 { w(d, i) } }`, 1},
		"decoded":            {`func f(d string, b []byte) { r := recipe{Results: "osd-example-results.json"}; json.Unmarshal(b, &r); os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"decoded by pointer": {`func f(d string, b []byte) { r := &recipe{Results: "osd-example-results.json"}; json.Unmarshal(b, r); os.WriteFile(filepath.Join(d, r.Results), nil, 0o644) }`, 1},
		"escaped function":   {`func w(d, op string) { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }; func f(d string) { w(d, "set"); g := w; g(d, "other") }`, 1},
		"never called":       {`func w(d, op string) { os.WriteFile(filepath.Join(d, "osd-example-go-"+op+".json"), nil, 0o644) }`, 1},
		"interface call":     {`func f(d string, x client) { os.WriteFile(filepath.Join(d, x.Name()), nil, 0o644) }`, 1},
		"element changed":    {`func f(d string) { q := []string{"osd-example-results.json"}; s := q; s[0] = "osd-other-results.json"; os.WriteFile(filepath.Join(d, q[0]), nil, 0o644) }`, 1},
		"pointee replaced":   {`func f(d string) { p := &recipe{Results: "osd-example-results.json"}; q := p; *q = recipe{Results: "osd-other-results.json"}; os.WriteFile(filepath.Join(d, p.Results), nil, 0o644) }`, 1},
		"decoded slice":      {`func f(d string, b []byte) { n := []string{"osd-example-results.json"}; json.Unmarshal(b, &n); os.WriteFile(filepath.Join(d, n[0]), nil, 0o644) }`, 1},
		"compound assigned":  {`func f(d string) { n := "osd-example-"; n += "other.json"; os.WriteFile(filepath.Join(d, n), nil, 0o644) }`, 1},
		"another directory":  {`func f(d string) { os.WriteFile(filepath.Join(d, "x", "osd-example-results.json"), nil, 0o644) }`, 1},
		"elsewhere":          {`func f(d string) { os.WriteFile(d+"/osd-other-results.json", nil, 0o644) }`, 1},
		"unlisted signal":    {`func f(d string, r record) { os.WriteFile(filepath.Join(d, "osd-other-"+r.Lease), nil, 0o600) }`, 1},
		"listed signal":      {`func f(d string, r record) { os.WriteFile(filepath.Join(d, "osd-signal-"+r.State+"."+r.Lease), nil, 0o600) }`, 0},
		"not a receipt":      {`func f(d string, i int) { os.WriteFile(filepath.Join(d, fmt.Sprintf("osd-proceed-%d", i)), nil, 0o644); os.WriteFile(filepath.Join(d, "osd-map-"+d+".bin"), nil, 0o644) }`, 0},
		"control input":      {`func f(d string) { os.WriteFile(filepath.Join(d, "osd-other-input.json"), nil, 0o644) }`, 0},
		"soak receipt":       {`func f(d string) { os.WriteFile(filepath.Join(d, "osd-soak-v1-steady-setup.json"), nil, 0o644) }`, 0},
		"not a soak shape":   {`func f(d string) { os.WriteFile(filepath.Join(d, "osd-soak-v1-other.json"), nil, 0o644) }`, 1},
	} {
		f, err := parser.ParseFile(fset, name+".go", header+c.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(name, err)
		}
		info, errs := typeCheck(fset, []*ast.File{f}, imports)
		if len(errs) != 0 {
			t.Fatal(name, errs)
		}
		missing, _ := undeclaredReceipts(receiptWrites(fset, []*ast.File{f}, info), declared, unenumerated)
		if len(missing) != c.missing {
			t.Errorf("%s: %d undeclared receipts, want %d: %q", name, len(missing), c.missing, missing)
		}
	}
}

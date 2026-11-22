package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// suitePartPackages are the directories of suiteNames, in that order.
var suitePartPackages = []string{"integration", "cephmsgr"}

// suitePartSuffix is the identifier suffix of part K of N of a row.
func suitePartSuffix(k, n int) string { return fmt.Sprintf(".suite-part-%d-of-%d", k, n) }

// suitePartOrigin is the row a suite part was partitioned from: its
// identifier without the part's suffix and its parameters without
// suite_part. ok is false for a row that is not a part or whose identifier
// does not end in its part's suffix.
func suitePartOrigin(row ciScenario) (ciScenario, bool) {
	value, ok := row.Params[suitePartInput.name]
	if !ok {
		return row, false
	}
	k, n, err := parseSuitePart(value)
	if err != nil || k == 0 || !strings.HasSuffix(row.ID, suitePartSuffix(k, n)) {
		return row, false
	}
	params := maps.Clone(row.Params)
	delete(params, suitePartInput.name)
	return ciScenario{ID: strings.TrimSuffix(row.ID, suitePartSuffix(k, n)), Params: params}, true
}

// ciSuiteListing is what Go lists for each suite (suiteNames order) with
// selector, in listing order, as the fixture's -test.list of each suite
// binary does; it compiles the suites and runs no test.
var ciSuiteListing = func() func(string) ([][]string, error) {
	var mu sync.Mutex
	cache := map[string][][]string{}
	return func(selector string) ([][]string, error) {
		mu.Lock()
		defer mu.Unlock()
		if listing, ok := cache[selector]; ok {
			return listing, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		args := []string{"test", "-mod=readonly", "-json", "-list", selector}
		for _, dir := range suitePartPackages {
			args = append(args, "./"+dir)
		}
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = filepath.Join("..", "..")
		cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
		output, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("go test -list %s: %w", selector, err)
		}
		listing := make([][]string, len(suitePartPackages))
		scanner := bufio.NewScanner(bytes.NewReader(output))
		for scanner.Scan() {
			var event struct{ Action, Package, Output string }
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				return nil, err
			}
			name := strings.TrimSpace(event.Output)
			suite := slices.Index(suitePartPackages, filepath.Base(event.Package))
			if event.Action == "output" && suite >= 0 && listedTest.MatchString(name) {
				listing[suite] = append(listing[suite], name)
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		cache[selector] = listing
		return listing, nil
	}
}()

// ciSuitePartNames are the tests part k of n of selector runs, keyed
// "<package>/<Test>" as ciNativeSources keys them.
func ciSuitePartNames(selector string, k, n int) (map[string]bool, error) {
	listing, err := ciSuiteListing(selector)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	for suite, tests := range suitePartTests(listing, k, n) {
		for _, test := range tests {
			names[suitePartPackages[suite]+"/"+test] = true
		}
	}
	return names, nil
}

// Every partitioned configuration of the catalog declares exactly its parts
// 1..N, under the identifiers of one origin row and with no whole row of the
// same configuration beside them; on this tree's listing the parts select
// every test of the configuration's default selection exactly once, each a
// nonempty part of at most suitePartMaxTests tests.
func TestSuitePartsCoverTheirSelectionOnce(t *testing.T) {
	rows := ciRepositoryCatalog(t)
	whole := map[string]string{}
	for _, row := range rows {
		if _, ok := row.Params[suitePartInput.name]; !ok {
			whole[ciConfiguration(row.Params)] = row.ID
		}
	}
	type part struct {
		k, n int
		id   string
		p    profile
	}
	configurations := map[string][]part{}
	origins := map[string]string{}
	for _, row := range rows {
		value, ok := row.Params[suitePartInput.name]
		if !ok {
			continue
		}
		k, n, err := parseSuitePart(value)
		if err != nil {
			t.Errorf("%s: %v", row.ID, err)
			continue
		}
		origin, ok := suitePartOrigin(row)
		if !ok {
			t.Errorf("%s: suite_part=%s, but the identifier does not end in %s", row.ID, value, suitePartSuffix(k, n))
			continue
		}
		key := ciConfiguration(origin.Params)
		if other, ok := origins[key]; ok && other != origin.ID {
			t.Errorf("%s: a part of %s, whose other parts are of %s", row.ID, origin.ID, other)
		}
		origins[key] = origin.ID
		if id, ok := whole[key]; ok {
			t.Errorf("%s: the whole row %s of this configuration runs every test of its parts again", row.ID, id)
		}
		p, err := ciProfile(row)
		if err != nil {
			t.Errorf("%s: %v", row.ID, err)
			continue
		}
		configurations[key] = append(configurations[key], part{k, n, row.ID, p})
	}
	if len(configurations) == 0 {
		t.Fatal("no catalog row is a suite part")
	}
	for key, parts := range configurations {
		origin := origins[key]
		n := parts[0].n
		declared := map[int]string{}
		for _, part := range parts {
			if part.n != n {
				t.Errorf("%s: part %d/%d of %s, whose parts are of %d", part.id, part.k, part.n, origin, n)
			}
			if other, ok := declared[part.k]; ok {
				t.Errorf("%s and %s are both part %d of %s", other, part.id, part.k, origin)
			}
			declared[part.k] = part.id
			if part.p.testRun != parts[0].p.testRun {
				t.Errorf("%s selects %s, another part of %s selects %s", part.id, part.p.testRun, origin, parts[0].p.testRun)
			}
		}
		for k := 1; k <= n; k++ {
			if _, ok := declared[k]; !ok {
				t.Errorf("%s: part %d/%d is not declared, so its tests run in no job", origin, k, n)
			}
		}
		listing, err := ciSuiteListing(parts[0].p.testRun)
		if err != nil {
			t.Fatal(err)
		}
		if problems := suitePartCoverageProblems(listing, n, suitePartMaxTests); len(problems) != 0 {
			t.Errorf("%s:\n%s", origin, strings.Join(problems, "\n"))
		}
		t.Logf("%s: %d parts of %d tests", origin, n, len(slices.Concat(listing...)))
	}
}

// ciConfiguration is a row's parameters as one comparable key.
func ciConfiguration(params map[string]string) string {
	data, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	return string(data)
}

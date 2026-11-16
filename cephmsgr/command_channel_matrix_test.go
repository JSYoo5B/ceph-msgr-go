package cephmsgr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestCommandChannelMatrixSelectorsExist keeps the integration test names in
// COMMAND_CHANNEL_MATRIX.md resolvable. A name resolves when go test -list
// reports it as a top-level test of the integration or cephmsgr package, so
// build constraints, import aliases and methods follow Go's own rules.
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
	for _, m := range names {
		if !listed[m[1]] {
			t.Errorf("selector %s is not a top-level test of integration/ or cephmsgr/", m[1])
		}
	}
}

// listedSuiteTests returns the top-level tests go test -list reports for the
// two native suite packages. Listing compiles the packages and runs no test.
func listedSuiteTests(t *testing.T) map[string]bool {
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
	listed := map[string]bool{}
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
			case name.MatchString(line):
				listed[line] = true
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

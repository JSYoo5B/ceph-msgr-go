package cephmsgr

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestCommandChannelMatrixSelectorsExist keeps the integration test names in
// COMMAND_CHANNEL_MATRIX.md resolvable.
func TestCommandChannelMatrixSelectorsExist(t *testing.T) {
	doc, err := os.ReadFile("COMMAND_CHANNEL_MATRIX.md")
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("../integration/*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatal("integration tests not found", err)
	}
	var source strings.Builder
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		source.Write(data)
	}
	names := regexp.MustCompile("`(Test[A-Za-z0-9]+)`").FindAllStringSubmatch(string(doc), -1)
	if len(names) == 0 {
		t.Fatal("matrix names no tests")
	}
	for _, m := range names {
		if !strings.Contains(source.String(), "func "+m[1]+"(") {
			t.Errorf("selector %s does not exist in integration/", m[1])
		}
	}
}

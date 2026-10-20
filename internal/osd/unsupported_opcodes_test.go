package osd

import (
	"encoding/csv"
	"os"
	"regexp"
	"slices"
	"testing"
)

// TestUnsupportedOpcodesAreClassified keeps UNSUPPORTED_OPCODES.md and the
// catalog's unsupported entries identical.
func TestUnsupportedOpcodesAreClassified(t *testing.T) {
	f, err := os.Open("opcodes-tentacle.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, r := range rows[1:] {
		if r[4] == "unsupported" {
			want = append(want, r[0])
		}
	}
	doc, err := os.ReadFile("UNSUPPORTED_OPCODES.md")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range regexp.MustCompile(`(?m)^\| ([A-Z0-9_]+) \| `).FindAllStringSubmatch(string(doc), -1) {
		got = append(got, m[1])
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		t.Fatalf("classification table differs from catalog\ncatalog: %v\ndoc:     %v", want, got)
	}
}

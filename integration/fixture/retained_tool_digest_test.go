package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// retainedToolDigests are the SHA-256 digests of tool files exactly as they
// ran when the retained receipts below were produced (main 387b9f2,
// tools/<file>). Issue #123 changed both files so the CHECKSUM driver merges
// the builder's returned proof instead of parsing captured stdout. Receipts
// produced before that change stay bound to the bytes that actually ran; they
// are not rehashed against the current files. Fresh receipts record and are
// checked against the current file digests.
var retainedToolDigests = map[string]string{
	allocHintEncoderSpec.driver: "79063616bd6254b2144dd2fb2a0199a57dc3d8c99e80700d3e720ab8c42d819f",
	checksumEncoderSpec.driver:  "deb6c20da9be732ce95542bd41c79aa8c7112d16b4f551c44176fa82b08cdd44",
}

// retainedToolReceipts lists, per receipt under internal/osd/testdata, which
// tool files it records at their retained digest. The set is closed: the
// audit below rejects any other receipt that names a retained digest.
var retainedToolReceipts = map[string][]string{
	"alloc-hint-native-provenance.json":  {allocHintEncoderSpec.driver},
	"checksum-native-provenance.json":    {allocHintEncoderSpec.driver, checksumEncoderSpec.driver},
	"copy-from-native.json":              {allocHintEncoderSpec.driver},
	"mapext-native-provenance.json":      {allocHintEncoderSpec.driver},
	"pg-list-native-provenance.json":     {allocHintEncoderSpec.driver},
	"sparse-read-native-provenance.json": {allocHintEncoderSpec.driver},
	"u64-xattr-native-provenance.json":   {allocHintEncoderSpec.driver},
	"watchers-native-provenance.json":    {allocHintEncoderSpec.driver},
	"writesame-native-provenance.json":   {allocHintEncoderSpec.driver},
}

// freshChecksumReceipt is the CHECKSUM receipt produced by the current driver
// and shared builder.
const freshChecksumReceipt = "checksum-native-provenance-20261110.json"

func currentToolDigest(t *testing.T, file string) string {
	t.Helper()
	data, err := os.ReadFile("../../tools/" + file)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// receiptToolDigest returns the digest a receipt must record for a tool file:
// the retained digest when that receipt predates the tool's current bytes,
// otherwise the digest of the current file.
func receiptToolDigest(t *testing.T, receipt, file string) string {
	t.Helper()
	if slices.Contains(retainedToolReceipts[receipt], file) {
		return retainedToolDigests[file]
	}
	return currentToolDigest(t, file)
}

func collectDigestStrings(value any, out map[string]bool) {
	switch v := value.(type) {
	case string:
		out[v] = true
	case map[string]any:
		for _, item := range v {
			collectDigestStrings(item, out)
		}
	case []any:
		for _, item := range v {
			collectDigestStrings(item, out)
		}
	}
}

func TestRetainedToolDigestsStayBoundToTheirReceipts(t *testing.T) {
	for file, retained := range retainedToolDigests {
		if currentToolDigest(t, file) == retained {
			t.Fatal("retained digest equals the current tool file; the pin no longer documents an earlier revision", file)
		}
	}
	paths, err := filepath.Glob("../../internal/osd/testdata/*.json")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	current := map[string][]string{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var receipt any
		if err := json.Unmarshal(raw, &receipt); err != nil {
			t.Fatal(path, err)
		}
		strs := map[string]bool{}
		collectDigestStrings(receipt, strs)
		name := filepath.Base(path)
		for file, retained := range retainedToolDigests {
			if strs[retained] {
				found[name] = append(found[name], file)
			}
			if strs[currentToolDigest(t, file)] {
				current[name] = append(current[name], file)
			}
		}
	}
	for name := range found {
		sort.Strings(found[name])
	}
	want := map[string][]string{}
	for name, files := range retainedToolReceipts {
		want[name] = slices.Sorted(slices.Values(files))
	}
	if fmt.Sprint(found) != fmt.Sprint(want) {
		t.Fatalf("receipts naming retained tool digests differ from the closed set:\n got %v\nwant %v", found, want)
	}
	for name, files := range current {
		if _, retained := retainedToolReceipts[name]; retained {
			t.Fatal("retained receipt also names a current tool digest", name, files)
		}
	}
	if got := strings.Join(slices.Sorted(slices.Values(current[freshChecksumReceipt])), ","); got != allocHintEncoderSpec.driver+","+checksumEncoderSpec.driver {
		t.Fatal("fresh CHECKSUM receipt does not record the current builder and launcher", got)
	}
}

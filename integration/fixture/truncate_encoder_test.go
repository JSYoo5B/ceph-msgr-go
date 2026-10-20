package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Reconstruct the public builder's JSON-line receipt from the actual native
// truncate-field provenance and its eighteen stored segment files.
func truncateEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	raw, err := os.ReadFile("../../internal/osd/testdata/truncate-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := decodeEncoderReceipt(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	fixtures := receipt["fixtures"].(map[string]any)
	if len(fixtures) != len(truncateEncoderSpec.names) {
		t.Fatal("actual native truncate-field receipt does not contain six vectors")
	}
	for _, name := range truncateEncoderSpec.names {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			t.Fatal("actual native truncate-field receipt lacks", name)
		}
		for _, segment := range []string{"front", "data", "header"} {
			data, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			text := strings.TrimSpace(string(data))
			b, err := hex.DecodeString(text)
			if err != nil {
				t.Fatal(err)
			}
			if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(b)) {
				t.Fatal("stored native truncate-field segment differs from its provenance digest", name, segment)
			}
			fixture[segment+"_hex"] = text
		}
	}
	for field, file := range map[string]string{"wrapper_sha256": truncateEncoderSpec.wrapper, "build_driver_sha256": truncateEncoderSpec.driver} {
		source, err := os.ReadFile("../../tools/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if receipt[field] != fmt.Sprintf("%x", sha256.Sum256(source)) {
			t.Fatal("stored native truncate-field receipt does not match its compiled source", file)
		}
	}
	return receipt, receipt["wrapper_sha256"].(string)
}

func TestTruncateEncoderProofKeepsItsVectorSet(t *testing.T) {
	path := func(t *testing.T, receipt map[string]any) string {
		data, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(t.TempDir(), "truncate-proof.log")
		if err := os.WriteFile(p, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	receipt, hash := truncateEncoderReceipt(t)
	if _, err := readNativeEncoderProofFor(path(t, receipt), hash, truncateEncoderSpec); err != nil {
		t.Fatal("actual native truncate-field receipt rejected", err)
	}
	if _, err := readNativeEncoderProofFor(path(t, receipt), hash, snapEncoderSpec); err == nil {
		t.Fatal("native truncate-field vector set accepted as snapshot proof")
	}
	single := truncateEncoderSpec
	single.requests = 0
	if _, err := readNativeEncoderProofFor(path(t, receipt), hash, single); err == nil {
		t.Fatal("native truncate-field requests accepted under the single-request header rule")
	}
	for name, edit := range map[string]func(map[string]any){
		"missing trimmed write reply": func(p map[string]any) {
			delete(p["fixtures"].(map[string]any), "truncate-native-reply-v6-write-trimmed")
		},
		"failed decoder": func(p map[string]any) { p["native_payload_decoder_verified"] = false },
		"wrong request data count": func(p map[string]any) {
			f := p["fixtures"].(map[string]any)["truncate-native-request-v6-write"].(map[string]any)
			f["native_header"].(map[string]any)["data_len"] = json.Number("0")
		},
	} {
		receipt, hash := truncateEncoderReceipt(t)
		edit(receipt)
		if _, err := readNativeEncoderProofFor(path(t, receipt), hash, truncateEncoderSpec); err == nil {
			t.Fatal("accepted invalid native truncate-field receipt:", name)
		}
	}
}

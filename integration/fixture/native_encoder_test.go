package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeEncoderProofRejectsStaleOrIncompleteEvidence(t *testing.T) {
	data, err := os.ReadFile("../../internal/osd/testdata/mutation-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	// The committed manifest stores segment hashes, while the live builder
	// receipt includes hex bytes. Reconstruct that receipt from its separately
	// committed, independently generated front/data artifacts.
	var receipt map[string]any
	if err := decodeEncoderReceipt(data, &receipt); err != nil {
		t.Fatal(err)
	}
	for name, value := range receipt["fixtures"].(map[string]any) {
		fixture := value.(map[string]any)
		for _, segment := range []string{"front", "data"} {
			h, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			fixture[segment+"_hex"] = strings.TrimSpace(string(h))
		}
	}
	data, err = json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := os.ReadFile("../../tools/osd_mutation_native_encoder.cc")
	if err != nil {
		t.Fatal(err)
	}
	wrapperHash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"source", func(p map[string]any) { p["ceph_commit"] = "other-source" }},
		{"image", func(p map[string]any) { p["image"] = "other-image" }},
		{"wrapper", func(p map[string]any) { p["wrapper_sha256"] = "other-wrapper" }},
		{"unverified", func(p map[string]any) { p["native_payload_encoder_verified"] = false }},
		{"missing-vector", func(p map[string]any) { delete(p["fixtures"].(map[string]any), "mutation-native-request-v6-all") }},
		{"invalid-hex", func(p map[string]any) {
			p["fixtures"].(map[string]any)["mutation-native-request-v6-all"].(map[string]any)["front_hex"] = "not-hex"
		}},
		{"wrong-length", func(p map[string]any) {
			p["fixtures"].(map[string]any)["mutation-native-request-v6-all"].(map[string]any)["data_bytes"] = 12
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "receipt.log")
			// A real, independently generated receipt succeeds even when
			// preceded by tool logs; corrupting provenance must fail closed.
			if err := os.WriteFile(path, append([]byte("build output\n"), compactEncoderProof(t, data)...), 0600); err != nil {
				t.Fatal(err)
			}
			verified, err := readNativeEncoderProof(path, wrapperHash)
			if err != nil {
				t.Fatal("actual native encoder receipt rejected", err)
			}
			if verified["native_envelope"].(map[string]any)["features"] != json.Number("3026423347916342070") {
				t.Fatal("native uint64 feature metadata lost precision")
			}
			var proof map[string]any
			if err := decodeEncoderReceipt(data, &proof); err != nil {
				t.Fatal(err)
			}
			tc.change(proof)
			bad, err := json.Marshal(proof)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, bad, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readNativeEncoderProof(path, wrapperHash); err == nil {
				t.Fatal("stale/incomplete receipt accepted as native encoder evidence")
			}
		})
	}
}

func compactEncoderProof(t *testing.T, data []byte) []byte {
	t.Helper()
	var proof map[string]any
	if err := decodeEncoderReceipt(data, &proof); err != nil {
		t.Fatal(err)
	}
	compact, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	return append(compact, '\n')
}

func decodeEncoderReceipt(data []byte, proof *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(proof)
}

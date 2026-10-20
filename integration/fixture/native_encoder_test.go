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
		for _, segment := range []string{"header", "front", "data"} {
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
		{"header-unverified", func(p map[string]any) { p["native_message_header_verified"] = false }},
		{"header-verification-missing", func(p map[string]any) { delete(p, "native_message_header_verified") }},
		{"missing-vector", func(p map[string]any) { delete(p["fixtures"].(map[string]any), "mutation-native-request-v6-all") }},
		{"invalid-hex", func(p map[string]any) {
			p["fixtures"].(map[string]any)["mutation-native-request-v6-all"].(map[string]any)["front_hex"] = "not-hex"
		}},
		{"wrong-length", func(p map[string]any) {
			p["fixtures"].(map[string]any)["mutation-native-request-v6-all"].(map[string]any)["data_bytes"] = 12
		}},
		{"header-missing", func(p map[string]any) { delete(encoderRequestFixture(p), "header_hex") }},
		{"header-invalid-hex", func(p map[string]any) { encoderRequestFixture(p)["header_hex"] = "not-hex" }},
		{"header-short", func(p map[string]any) {
			fixture := encoderRequestFixture(p)
			fixture["header_hex"] = fixture["header_hex"].(string)[:80]
			fixture["header_bytes"] = 40
		}},
		{"header-wrong-count", func(p map[string]any) { encoderRequestFixture(p)["header_bytes"] = 42 }},
		{"header-metadata-missing", func(p map[string]any) { delete(encoderRequestFixture(p), "native_header") }},
		{"header-wrong-tid", func(p map[string]any) { encoderRequestFixture(p)["native_header"].(map[string]any)["tid"] = 78 }},
		{"header-negative-tid", func(p map[string]any) { encoderRequestFixture(p)["native_header"].(map[string]any)["tid"] = -1 }},
		{"header-tid-overflow", func(p map[string]any) {
			encoderRequestFixture(p)["native_header"].(map[string]any)["tid"] = json.Number("18446744073709551616")
		}},
		{"header-field-missing", func(p map[string]any) { delete(encoderRequestFixture(p)["native_header"].(map[string]any), "flags") }},
		{"header-padding", func(p map[string]any) {
			fixture := encoderRequestFixture(p)
			h := fixture["header_hex"].(string)
			fixture["header_hex"] = h[:44] + "01000000" + h[52:]
		}},
		{"header-wrong-request-type", func(p map[string]any) {
			fixture := encoderRequestFixture(p)
			h := fixture["header_hex"].(string)
			fixture["header_hex"] = h[:32] + "2b00" + h[36:]
			fixture["native_header"].(map[string]any)["type"] = 43
		}},
		{"header-wrong-front-length", func(p map[string]any) { encoderRequestFixture(p)["native_header"].(map[string]any)["front_len"] = 520 }},
		{"header-wrong-data-length", func(p map[string]any) { encoderRequestFixture(p)["native_header"].(map[string]any)["data_len"] = 12 }},
		{"header-nonempty-middle", func(p map[string]any) { encoderRequestFixture(p)["native_header"].(map[string]any)["middle_len"] = 1 }},
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

func encoderRequestFixture(proof map[string]any) map[string]any {
	return proof["fixtures"].(map[string]any)["mutation-native-request-v6-all"].(map[string]any)
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

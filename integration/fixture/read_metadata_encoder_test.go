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

func TestReadMetadataNativeProofsMatchStoredSegments(t *testing.T) {
	for _, spec := range []nativeEncoderSpec{mapextEncoderSpec, checksumEncoderSpec} {
		t.Run(spec.command, func(t *testing.T) {
			prefix := "mapext"
			if spec.command == checksumEncoderSpec.command {
				prefix = "checksum"
			}
			raw, err := os.ReadFile("../../internal/osd/testdata/" + prefix + "-native-provenance.json")
			if err != nil {
				t.Fatal(err)
			}
			var proof map[string]any
			if err := decodeEncoderReceipt(raw, &proof); err != nil {
				t.Fatal(err)
			}
			for field, file := range map[string]string{"wrapper_sha256": spec.wrapper, "build_driver_sha256": allocHintEncoderSpec.driver, "launcher_sha256": spec.driver} {
				data, err := os.ReadFile("../../tools/" + file)
				if err != nil {
					t.Fatal(err)
				}
				if proof[field] != fmt.Sprintf("%x", sha256.Sum256(data)) {
					t.Fatal("native generator provenance mismatch", field)
				}
			}
			for _, name := range spec.names {
				fixture := proof["fixtures"].(map[string]any)[name].(map[string]any)
				for _, segment := range []string{"header", "front", "data"} {
					raw, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
					if err != nil {
						t.Fatal(err)
					}
					text := strings.TrimSpace(string(raw))
					data, err := hex.DecodeString(text)
					if err != nil {
						t.Fatal(err)
					}
					if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
						t.Fatal("native payload hash mismatch", name, segment)
					}
					fixture[segment+"_hex"] = text
				}
			}
			data, err := json.Marshal(proof)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "native.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readNativeEncoderProofFor(path, proof["wrapper_sha256"].(string), spec); err != nil {
				t.Fatal(err)
			}
		})
	}
}

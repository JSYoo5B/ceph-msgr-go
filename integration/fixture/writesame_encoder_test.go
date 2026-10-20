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

func TestWriteSameFreshNativeProofMatchesStoredSegments(t *testing.T) {
	raw, err := os.ReadFile("../../internal/osd/testdata/writesame-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof map[string]any
	if err := decodeEncoderReceipt(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof["builder_termination_completed"] != true {
		t.Fatal("native builder cleanup not recorded")
	}
	for field, file := range map[string]string{"wrapper_sha256": writeSameEncoderSpec.wrapper, "launcher_sha256": writeSameEncoderSpec.driver, "build_driver_sha256": allocHintEncoderSpec.driver} {
		source, err := os.ReadFile("../../tools/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if proof[field] != fmt.Sprintf("%x", sha256.Sum256(source)) {
			t.Fatal("native WRITESAME generator differs from retained proof", field)
		}
	}
	fixtures := proof["fixtures"].(map[string]any)
	if len(fixtures) != 5 {
		t.Fatal("native WRITESAME suite does not contain five vectors")
	}
	for _, name := range writeSameEncoderSpec.names {
		fixture := fixtures[name].(map[string]any)
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
				t.Fatal("native WRITESAME segment hash differs", name, segment)
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
	if _, err := readNativeEncoderProofFor(path, proof["wrapper_sha256"].(string), writeSameEncoderSpec); err != nil {
		t.Fatal(err)
	}
}

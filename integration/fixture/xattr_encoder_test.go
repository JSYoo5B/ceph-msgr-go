package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestXattrEncoderProofKeepsItsOwnSourceAndVectorSet(t *testing.T) {
	data, err := os.ReadFile("../../internal/osd/testdata/xattr-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := decodeEncoderReceipt(data, &receipt); err != nil {
		t.Fatal(err)
	}
	for name, value := range receipt["fixtures"].(map[string]any) {
		fixture := value.(map[string]any)
		for _, segment := range []string{"front", "data", "header"} {
			data, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			fixture[segment+"_hex"] = strings.TrimSpace(string(data))
		}
	}
	data, err = json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	wrapper, err := os.ReadFile("../../tools/osd_xattr_native_encoder.cc")
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	path := filepath.Join(t.TempDir(), "xattr-proof.log")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNativeEncoderProofFor(path, hash, xattrEncoderSpec); err != nil {
		t.Fatal("actual native xattr proof rejected", err)
	}
	if _, err := readNativeEncoderProof(path, hash); err == nil {
		t.Fatal("xattr proof accepted as the independently verified mutation vector set")
	}
	receipt["native_payload_decoder_verified"] = false
	invalid, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNativeEncoderProofFor(path, hash, xattrEncoderSpec); err == nil {
		t.Fatal("xattr proof accepted without its separate native decoder verification")
	}
}

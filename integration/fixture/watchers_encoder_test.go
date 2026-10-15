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

func TestWatchersEncoderPinnedProofAndCorruption(t *testing.T) {
	raw, err := os.ReadFile("../../internal/osd/testdata/watchers-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range []bool{false, true} {
		var receipt map[string]any
		if err := decodeEncoderReceipt(raw, &receipt); err != nil {
			t.Fatal(err)
		}
		for field, name := range map[string]string{"build_driver_sha256": allocHintEncoderSpec.driver, "launcher_sha256": watchersEncoderSpec.driver} {
			data, err := os.ReadFile("../../tools/" + name)
			if err != nil {
				t.Fatal(err)
			}
			if receipt[field] != fmt.Sprintf("%x", sha256.Sum256(data)) {
				t.Fatal("native driver differs from receipt", field)
			}
		}
		fixtures := receipt["fixtures"].(map[string]any)
		for _, name := range watchersEncoderSpec.names {
			f := fixtures[name].(map[string]any)
			for _, segment := range []string{"front", "data", "header"} {
				raw, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
				if err != nil {
					t.Fatal(err)
				}
				text := strings.TrimSpace(string(raw))
				data, err := hex.DecodeString(text)
				if err != nil {
					t.Fatal(err)
				}
				if f[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
					t.Fatal("native segment hash differs", name, segment)
				}
				f[segment+"_hex"] = text
			}
		}
		wrapper, err := os.ReadFile("../../tools/" + watchersEncoderSpec.wrapper)
		if err != nil {
			t.Fatal(err)
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
		if corrupt {
			delete(receipt, "native_payload_decoder_verified")
		}
		path := filepath.Join(t.TempDir(), "proof.json")
		encoded, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		_, err = readNativeEncoderProofFor(path, hash, watchersEncoderSpec)
		if (err != nil) != corrupt {
			t.Fatal("incorrect native proof admission", corrupt, err)
		}
	}
}

package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Reconstruct the public builder's bounded JSON-line receipt from the actual
// native provenance and all fifteen stored segment files. No Go encoder
// produces the reference bytes used to accept or corrupt the receipt.
func omapEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	raw, err := os.ReadFile("../../internal/osd/testdata/omap-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := decodeEncoderReceipt(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	fixtures := receipt["fixtures"].(map[string]any)
	if len(fixtures) != 5 {
		t.Fatal("actual native OMAP receipt does not contain five vectors")
	}
	for _, name := range []string{
		"omap-native-request-v6-read-all", "omap-native-reply-v6-read-mixed",
		"omap-native-reply-v6-keys-final", "omap-native-reply-v6-vals-empty",
		"omap-native-reply-v6-bykeys-empty-value",
	} {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			t.Fatal("actual native OMAP receipt lacks the independently produced vector", name)
		}
		for _, segment := range []string{"front", "data", "header"} {
			data, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			fixture[segment+"_hex"] = strings.TrimSpace(string(data))
		}
	}
	wrapper, err := os.ReadFile("../../tools/osd_omap_native_encoder.cc")
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	if receipt["wrapper_sha256"] != hash {
		t.Fatal("stored native OMAP receipt does not match its actual compiled wrapper source")
	}
	return receipt, hash
}

func omapEncoderReceiptPath(t *testing.T, receipt map[string]any) string {
	t.Helper()
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "omap-proof.log")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOmapEncoderProofKeepsItsOwnSourceDecoderAndVectorSet(t *testing.T) {
	receipt, hash := omapEncoderReceipt(t)
	path := omapEncoderReceiptPath(t, receipt)
	if _, err := readNativeEncoderProofFor(path, hash, omapEncoderSpec); err != nil {
		t.Fatal("actual native OMAP encoder/header/independent-decoder receipt rejected", err)
	}
	for _, other := range []nativeEncoderSpec{mutationEncoderSpec, xattrEncoderSpec} {
		if _, err := readNativeEncoderProofFor(path, hash, other); err == nil {
			t.Fatal("native OMAP vector set accepted by a different public producer", other.command)
		}
	}
}

func TestOmapEncoderProofRejectsUnverifiedOrMismatchedReceipts(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"missing independent decoder", func(p map[string]any) { delete(p, "native_payload_decoder_verified") }},
		{"failed independent decoder", func(p map[string]any) { p["native_payload_decoder_verified"] = false }},
		{"different compiled wrapper", func(p map[string]any) { p["wrapper_sha256"] = "other-wrapper" }},
		{"foreign vector with same count", func(p map[string]any) {
			fixtures := p["fixtures"].(map[string]any)
			fixtures["xattr-native-reply-v6-read-mixed"] = fixtures["omap-native-reply-v6-read-mixed"]
			delete(fixtures, "omap-native-reply-v6-read-mixed")
		}},
		{"wrong header data count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-native-reply-v6-read-mixed"].(map[string]any)
			fixture["native_header"].(map[string]any)["data_len"] = json.Number("0")
		}},
		{"wrong segment byte count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-native-request-v6-read-all"].(map[string]any)
			fixture["data_bytes"] = json.Number("0")
		}},
		{"request mislabeled as reply consistently", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-native-request-v6-read-all"].(map[string]any)
			header, err := hex.DecodeString(fixture["header_hex"].(string))
			if err != nil {
				panic(err)
			}
			binary.LittleEndian.PutUint16(header[16:18], 43)
			binary.LittleEndian.PutUint16(header[37:39], 2)
			fixture["header_hex"] = hex.EncodeToString(header)
			metadata := fixture["native_header"].(map[string]any)
			metadata["type"], metadata["compat"] = json.Number("43"), json.Number("2")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt, hash := omapEncoderReceipt(t)
			tc.edit(receipt)
			if _, err := readNativeEncoderProofFor(omapEncoderReceiptPath(t, receipt), hash, omapEncoderSpec); err == nil {
				t.Fatal("accepted invalid OMAP native receipt", tc.name)
			}
		})
	}
}

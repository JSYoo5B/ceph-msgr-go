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
// native OMAPRMKEYRANGE provenance and all fifteen stored segment files. No Go
// encoder produces the reference bytes used to accept or corrupt the receipt.
func omapRangeEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	raw, err := os.ReadFile("../../internal/osd/testdata/omap-range-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := decodeEncoderReceipt(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	fixtures := receipt["fixtures"].(map[string]any)
	if len(fixtures) != 5 {
		t.Fatal("actual native OMAPRMKEYRANGE receipt does not contain five vectors")
	}
	for _, name := range omapRangeEncoderSpec.names {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			t.Fatal("actual native OMAPRMKEYRANGE receipt lacks the independently produced vector", name)
		}
		for _, segment := range []string{"front", "data", "header"} {
			data, err := os.ReadFile("../../internal/osd/testdata/" + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			text := strings.TrimSpace(string(data))
			segmentBytes, err := hex.DecodeString(text)
			if err != nil {
				t.Fatal(err)
			}
			if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(segmentBytes)) {
				t.Fatal("stored native OMAPRMKEYRANGE segment differs from its provenance digest", name, segment)
			}
			fixture[segment+"_hex"] = text
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + omapRangeEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	if receipt["wrapper_sha256"] != hash {
		t.Fatal("stored native OMAPRMKEYRANGE receipt does not match its actual compiled wrapper source")
	}
	driver, err := os.ReadFile("../../tools/" + omapRangeEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	if receipt["build_driver_sha256"] != fmt.Sprintf("%x", sha256.Sum256(driver)) {
		t.Fatal("stored native OMAPRMKEYRANGE receipt does not match its actual build driver")
	}
	return receipt, hash
}

func omapRangeEncoderReceiptPath(t *testing.T, receipt map[string]any) string {
	t.Helper()
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "omap-range-proof.log")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOmapRangeEncoderProofKeepsItsOwnSourceDecoderAndVectorSet(t *testing.T) {
	receipt, hash := omapRangeEncoderReceipt(t)
	path := omapRangeEncoderReceiptPath(t, receipt)
	if _, err := readNativeEncoderProofFor(path, hash, omapRangeEncoderSpec); err != nil {
		t.Fatal("actual native OMAPRMKEYRANGE encoder/header/independent-decoder receipt rejected", err)
	}
	for _, other := range []nativeEncoderSpec{mutationEncoderSpec, xattrEncoderSpec, omapEncoderSpec, omapWriteEncoderSpec, omapCmpEncoderSpec} {
		if _, err := readNativeEncoderProofFor(path, hash, other); err == nil {
			t.Fatal("native OMAPRMKEYRANGE vector set accepted by a different public producer", other.command)
		}
	}
	// The OMAP_CMP and OMAP header/write receipts are valid native proofs with
	// the same count, but neither can stand in for the range-removal set.
	cmp, cmpHash := omapCmpEncoderReceipt(t)
	if _, err := readNativeEncoderProofFor(omapCmpEncoderReceiptPath(t, cmp), cmpHash, omapRangeEncoderSpec); err == nil {
		t.Fatal("native OMAP_CMP vector set accepted as OMAPRMKEYRANGE proof")
	}
	write, writeHash := omapWriteEncoderReceipt(t)
	if _, err := readNativeEncoderProofFor(omapWriteEncoderReceiptPath(t, write), writeHash, omapRangeEncoderSpec); err == nil {
		t.Fatal("native OMAP header/write vector set accepted as OMAPRMKEYRANGE proof")
	}
}

func TestOmapRangeEncoderProofRejectsUnverifiedOrMismatchedReceipts(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"missing independent decoder", func(p map[string]any) { delete(p, "native_payload_decoder_verified") }},
		{"failed independent decoder", func(p map[string]any) { p["native_payload_decoder_verified"] = false }},
		{"different compiled wrapper", func(p map[string]any) { p["wrapper_sha256"] = "other-wrapper" }},
		{"comparison wrapper", func(p map[string]any) {
			wrapper, err := os.ReadFile("../../tools/" + omapCmpEncoderSpec.wrapper)
			if err != nil {
				panic(err)
			}
			p["wrapper_sha256"] = fmt.Sprintf("%x", sha256.Sum256(wrapper))
		}},
		{"foreign vector with same count", func(p map[string]any) {
			fixtures := p["fixtures"].(map[string]any)
			fixtures["omap-cmp-native-reply-v6-enoent"] = fixtures["omap-range-native-reply-v6-enoent"]
			delete(fixtures, "omap-range-native-reply-v6-enoent")
		}},
		{"missing pool refusal vector", func(p map[string]any) {
			delete(p["fixtures"].(map[string]any), "omap-range-native-reply-v6-eopnotsupp")
		}},
		{"wrong header data count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-range-native-request-v6-all"].(map[string]any)
			fixture["native_header"].(map[string]any)["data_len"] = json.Number("0")
		}},
		{"wrong header front count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-range-native-reply-v6-eopnotsupp"].(map[string]any)
			fixture["native_header"].(map[string]any)["front_len"] = json.Number("249")
		}},
		{"wrong segment byte count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-range-native-request-v6-all"].(map[string]any)
			fixture["data_bytes"] = json.Number("0")
		}},
		{"empty front", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-range-native-reply-v6-failok"].(map[string]any)
			fixture["front_hex"], fixture["front_bytes"] = "", json.Number("0")
		}},
		{"request mislabeled as reply consistently", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["omap-range-native-request-v6-all"].(map[string]any)
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
			receipt, hash := omapRangeEncoderReceipt(t)
			tc.edit(receipt)
			if _, err := readNativeEncoderProofFor(omapRangeEncoderReceiptPath(t, receipt), hash, omapRangeEncoderSpec); err == nil {
				t.Fatal("accepted invalid OMAPRMKEYRANGE native receipt", tc.name)
			}
		})
	}
}

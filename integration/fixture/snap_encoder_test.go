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
// native snapshot context, ROLLBACK and LIST_SNAPS provenance and all
// twenty-four stored segment files. No Go encoder produces the reference
// bytes used to accept or corrupt the receipt.
func snapEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	raw, err := os.ReadFile("../../internal/osd/testdata/snap-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := decodeEncoderReceipt(raw, &receipt); err != nil {
		t.Fatal(err)
	}
	fixtures := receipt["fixtures"].(map[string]any)
	if len(fixtures) != 8 {
		t.Fatal("actual native snapshot receipt does not contain eight vectors")
	}
	for _, name := range snapEncoderSpec.names {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			t.Fatal("actual native snapshot receipt lacks the independently produced vector", name)
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
				t.Fatal("stored native snapshot segment differs from its provenance digest", name, segment)
			}
			fixture[segment+"_hex"] = text
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + snapEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	if receipt["wrapper_sha256"] != hash {
		t.Fatal("stored native snapshot receipt does not match its actual compiled wrapper source")
	}
	driver, err := os.ReadFile("../../tools/" + snapEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	if receipt["build_driver_sha256"] != fmt.Sprintf("%x", sha256.Sum256(driver)) {
		t.Fatal("stored native snapshot receipt does not match its actual build driver")
	}
	return receipt, hash
}

func snapEncoderReceiptPath(t *testing.T, receipt map[string]any) string {
	t.Helper()
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snap-proof.log")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSnapEncoderProofKeepsItsOwnSourceDecoderAndVectorSet(t *testing.T) {
	receipt, hash := snapEncoderReceipt(t)
	path := snapEncoderReceiptPath(t, receipt)
	if _, err := readNativeEncoderProofFor(path, hash, snapEncoderSpec); err != nil {
		t.Fatal("actual native snapshot encoder/header/independent-decoder receipt rejected", err)
	}
	for _, other := range []nativeEncoderSpec{mutationEncoderSpec, xattrEncoderSpec, omapEncoderSpec, omapWriteEncoderSpec, omapCmpEncoderSpec, omapRangeEncoderSpec, callEncoderSpec, allocHintEncoderSpec} {
		if _, err := readNativeEncoderProofFor(path, hash, other); err == nil {
			t.Fatal("native snapshot vector set accepted by a different public producer", other.command)
		}
	}
	// Four leading MOSDOp vectors: the original single-request header rule
	// must refuse the second request as a malformed reply.
	single := snapEncoderSpec
	single.requests = 0
	if _, err := readNativeEncoderProofFor(path, hash, single); err == nil {
		t.Fatal("native snapshot requests accepted under the single-request header rule")
	}
	// The SETALLOCHINT receipt is a valid native proof, but it cannot stand
	// in for the snapshot set.
	allocHint, allocHintHash := allocHintEncoderReceipt(t)
	if _, err := readNativeEncoderProofFor(allocHintEncoderReceiptPath(t, allocHint), allocHintHash, snapEncoderSpec); err == nil {
		t.Fatal("native SETALLOCHINT vector set accepted as snapshot proof")
	}
}

func snapRelabel(p map[string]any, name string, typ, compat uint16) {
	fixture := p["fixtures"].(map[string]any)[name].(map[string]any)
	header, err := hex.DecodeString(fixture["header_hex"].(string))
	if err != nil {
		panic(err)
	}
	binary.LittleEndian.PutUint16(header[16:18], typ)
	binary.LittleEndian.PutUint16(header[37:39], compat)
	fixture["header_hex"] = hex.EncodeToString(header)
	metadata := fixture["native_header"].(map[string]any)
	metadata["type"], metadata["compat"] = json.Number(fmt.Sprint(typ)), json.Number(fmt.Sprint(compat))
}

func TestSnapEncoderProofRejectsUnverifiedOrMismatchedReceipts(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"missing independent decoder", func(p map[string]any) { delete(p, "native_payload_decoder_verified") }},
		{"failed independent decoder", func(p map[string]any) { p["native_payload_decoder_verified"] = false }},
		{"different compiled wrapper", func(p map[string]any) { p["wrapper_sha256"] = "other-wrapper" }},
		{"allocation hint wrapper", func(p map[string]any) {
			wrapper, err := os.ReadFile("../../tools/" + allocHintEncoderSpec.wrapper)
			if err != nil {
				panic(err)
			}
			p["wrapper_sha256"] = fmt.Sprintf("%x", sha256.Sum256(wrapper))
		}},
		{"foreign vector with same count", func(p map[string]any) {
			fixtures := p["fixtures"].(map[string]any)
			fixtures["alloc-hint-native-reply-v6-success"] = fixtures["snap-native-reply-v6-rollback"]
			delete(fixtures, "snap-native-reply-v6-rollback")
		}},
		{"missing old snap context vector", func(p map[string]any) {
			delete(p["fixtures"].(map[string]any), "snap-native-reply-v6-eoldsnapc")
		}},
		{"missing listing request", func(p map[string]any) {
			delete(p["fixtures"].(map[string]any), "snap-native-request-v6-list")
		}},
		{"wrong request header data count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-request-v6-write"].(map[string]any)
			fixture["native_header"].(map[string]any)["data_len"] = json.Number("0")
		}},
		{"wrong listing reply data count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-reply-v6-list"].(map[string]any)
			fixture["native_header"].(map[string]any)["data_len"] = json.Number("0")
		}},
		{"wrong read reply data count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-reply-v6-read"].(map[string]any)
			fixture["native_header"].(map[string]any)["data_len"] = json.Number("4")
		}},
		{"wrong header front count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-reply-v6-eoldsnapc"].(map[string]any)
			fixture["native_header"].(map[string]any)["front_len"] = json.Number("1")
		}},
		{"wrong segment byte count", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-request-v6-rollback"].(map[string]any)
			fixture["front_bytes"] = json.Number("0")
		}},
		{"empty front", func(p map[string]any) {
			fixture := p["fixtures"].(map[string]any)["snap-native-reply-v6-rollback"].(map[string]any)
			fixture["front_hex"], fixture["front_bytes"] = "", json.Number("0")
		}},
		{"first request mislabeled as reply consistently", func(p map[string]any) {
			snapRelabel(p, "snap-native-request-v6-read", 43, 2)
		}},
		{"later request mislabeled as reply consistently", func(p map[string]any) {
			snapRelabel(p, "snap-native-request-v6-list", 43, 2)
		}},
		{"first reply mislabeled as request consistently", func(p map[string]any) {
			snapRelabel(p, "snap-native-reply-v6-list", 42, 3)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt, hash := snapEncoderReceipt(t)
			tc.edit(receipt)
			if _, err := readNativeEncoderProofFor(snapEncoderReceiptPath(t, receipt), hash, snapEncoderSpec); err == nil {
				t.Fatal("accepted invalid snapshot native receipt", tc.name)
			}
		})
	}
}

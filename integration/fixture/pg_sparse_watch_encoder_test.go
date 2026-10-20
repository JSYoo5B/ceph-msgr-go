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

// These tests reconstruct the live builder receipt from committed independent
// native segments. They do not synthesize a passing message or invoke Ceph.
func newOSDEncoderReceipt(t *testing.T, spec nativeEncoderSpec, prefix, buildDriver string) (map[string]any, string) {
	t.Helper()
	raw, err := os.ReadFile("../../internal/osd/testdata/" + prefix + "-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof map[string]any
	if err := decodeEncoderReceipt(raw, &proof); err != nil {
		t.Fatal(err)
	}
	for field, file := range map[string]string{"wrapper_sha256": spec.wrapper, "build_driver_sha256": buildDriver, "launcher_sha256": spec.driver} {
		data, err := os.ReadFile("../../tools/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if proof[field] != fmt.Sprintf("%x", sha256.Sum256(data)) {
			t.Fatalf("%s %s provenance differs from %s", prefix, field, file)
		}
	}
	fixtures := proof["fixtures"].(map[string]any)
	for _, name := range spec.names {
		fixture, ok := fixtures[name].(map[string]any)
		if !ok {
			t.Fatal("native manifest omits declared vector", name)
		}
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
				t.Fatal("native segment hash differs", name, segment)
			}
			fixture[segment+"_hex"] = text
		}
	}
	return proof, proof["wrapper_sha256"].(string)
}

func TestPGSparseWatchNativeProofsRejectWrongVectorClasses(t *testing.T) {
	for _, tc := range []struct {
		spec           nativeEncoderSpec
		prefix, driver string
	}{
		{pgListEncoderSpec, "pg-list", allocHintEncoderSpec.driver},
		{sparseReadEncoderSpec, "sparse-read", allocHintEncoderSpec.driver},
		// This builder includes its own event codec and has a distinct driver hash.
		{watchNotifyEncoderSpec, "watch-notify", watchNotifyEncoderSpec.driver},
	} {
		t.Run(tc.spec.command, func(t *testing.T) {
			original, hash := newOSDEncoderReceipt(t, tc.spec, tc.prefix, tc.driver)
			encoded, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "receipt.log")
			if err := os.WriteFile(path, append([]byte("native compile log\n"), encoded...), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := readNativeEncoderProofFor(path, hash, tc.spec); err != nil {
				t.Fatal("genuine native receipt rejected", err)
			}
			changes := map[string]func(map[string]any){
				"missing independent decoder": func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
				"missing declared vector":     func(p map[string]any) { delete(p["fixtures"].(map[string]any), tc.spec.names[0]) },
				"unrecognized extra vector":   func(p map[string]any) { p["fixtures"].(map[string]any)["extra"] = map[string]any{} },
				"request disguised as reply":  func(p map[string]any) { changeEncoderType(t, p, tc.spec.names[0], 43) },
				"reply disguised as request": func(p map[string]any) {
					index := 1
					if tc.spec.requests > 0 {
						index = tc.spec.requests
					}
					changeEncoderType(t, p, tc.spec.names[index], 42)
				},
				"unbounded native length": func(p map[string]any) {
					p["fixtures"].(map[string]any)[tc.spec.names[0]].(map[string]any)["front_bytes"] = json.Number("18446744073709551616")
				},
			}
			if tc.spec.events > 0 {
				changes["last request misclassified"] = func(p map[string]any) { changeEncoderType(t, p, tc.spec.names[tc.spec.requests-1], 43) }
				changes["first event disguised as reply"] = func(p map[string]any) { changeEncoderType(t, p, tc.spec.names[len(tc.spec.names)-tc.spec.events], 43) }
				changes["last event disguised as request"] = func(p map[string]any) { changeEncoderType(t, p, tc.spec.names[len(tc.spec.names)-1], 42) }
			}
			for name, change := range changes {
				t.Run(name, func(t *testing.T) {
					var bad map[string]any
					if err := decodeEncoderReceipt(encoded, &bad); err != nil {
						t.Fatal(err)
					}
					change(bad)
					raw, err := json.Marshal(bad)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, raw, 0600); err != nil {
						t.Fatal(err)
					}
					if _, err := readNativeEncoderProofFor(path, hash, tc.spec); err == nil {
						t.Fatal("corrupt native proof admitted")
					}
				})
			}
		})
	}
}

func changeEncoderType(t *testing.T, proof map[string]any, name string, typ uint16) {
	t.Helper()
	fixture := proof["fixtures"].(map[string]any)[name].(map[string]any)
	h, err := hex.DecodeString(fixture["header_hex"].(string))
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(h[16:18], typ)
	fixture["header_hex"] = hex.EncodeToString(h)
	fixture["native_header"].(map[string]any)["type"] = json.Number(fmt.Sprint(typ))
}

package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Use the retained independent native source outputs. In particular, a v6
// request cannot be relabeled as one of the explicit version-8 request suite.
// Existing v6/reply/watch/backoff proof branches retain their original checks.
func TestRequestV8NativeProofRejectsWrongVersionAndMessageClass(t *testing.T) {
	original, hash := newOSDEncoderReceipt(t, requestV8EncoderSpec, "request-v8", requestV8EncoderSpec.driver)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "receipt.log")
	verify := func(p map[string]any, spec nativeEncoderSpec) error {
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			return err
		}
		_, err = readNativeEncoderProofFor(path, hash, spec)
		return err
	}
	if err = verify(original, requestV8EncoderSpec); err != nil {
		t.Fatal("genuine v8 receipt rejected", err)
	}
	for _, name := range requestV8EncoderSpec.names {
		for _, typ := range []uint16{43, 44, 61} {
			t.Run(fmt.Sprintf("%s/type-%d", name, typ), func(t *testing.T) {
				var bad map[string]any
				if err := decodeEncoderReceipt(encoded, &bad); err != nil {
					t.Fatal(err)
				}
				changeEncoderType(t, bad, name, typ)
				if err := verify(bad, requestV8EncoderSpec); err == nil {
					t.Fatal("nonrequest vector admitted")
				}
			})
		}
		for _, version := range []uint16{6, 7, 9} {
			t.Run(fmt.Sprintf("%s/version-%d", name, version), func(t *testing.T) {
				var bad map[string]any
				if err := decodeEncoderReceipt(encoded, &bad); err != nil {
					t.Fatal(err)
				}
				fixture := bad["fixtures"].(map[string]any)[name].(map[string]any)
				h, _ := hex.DecodeString(fixture["header_hex"].(string))
				binary.LittleEndian.PutUint16(h[20:22], version)
				fixture["header_hex"] = hex.EncodeToString(h)
				fixture["native_header"].(map[string]any)["version"] = json.Number(fmt.Sprint(version))
				if err := verify(bad, requestV8EncoderSpec); err == nil {
					t.Fatal("wrong request version admitted")
				}
			})
		}
	}
	first := requestV8EncoderSpec.names[0]
	changes := map[string]func(map[string]any){
		"missing native routing decoder": func(p map[string]any) {
			delete(p["fixtures"].(map[string]any)[first].(map[string]any), "decoded_routing")
		},
		"missing native operation decoder": func(p map[string]any) {
			delete(p["fixtures"].(map[string]any)[first].(map[string]any), "decoded_operations")
		},
		"wrong explicit TID": func(p map[string]any) {
			f := p["fixtures"].(map[string]any)[first].(map[string]any)
			h, _ := hex.DecodeString(f["header_hex"].(string))
			binary.LittleEndian.PutUint64(h[8:16], 78)
			f["header_hex"] = hex.EncodeToString(h)
			f["native_header"].(map[string]any)["tid"] = json.Number("78")
		},
		"missing decoder": func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
		"missing vector":  func(p map[string]any) { delete(p["fixtures"].(map[string]any), first) },
		"extra vector":    func(p map[string]any) { p["fixtures"].(map[string]any)["extra"] = map[string]any{} },
		"wrong compat": func(p map[string]any) {
			f := p["fixtures"].(map[string]any)[first].(map[string]any)
			h, _ := hex.DecodeString(f["header_hex"].(string))
			binary.LittleEndian.PutUint16(h[37:39], 2)
			f["header_hex"] = hex.EncodeToString(h)
			f["native_header"].(map[string]any)["compat"] = json.Number("2")
		},
	}
	for _, field := range []string{"pool", "seed", "raw_hash", "client_inc", "reqid_inc", "shard"} {
		changes["wrong native routing "+field] = func(p map[string]any) {
			f := p["fixtures"].(map[string]any)[first].(map[string]any)
			f["decoded_routing"].(map[string]any)[field] = json.Number("0")
		}
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			var bad map[string]any
			if err := decodeEncoderReceipt(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			change(bad)
			if err := verify(bad, requestV8EncoderSpec); err == nil {
				t.Fatal("incomplete v8 proof admitted")
			}
		})
	}
	for _, change := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.requestVersion = 6 }, func(s *nativeEncoderSpec) { s.requests = 1 },
		func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
	} {
		bad := requestV8EncoderSpec
		change(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous all-request v8 classification admitted")
		}
	}
}

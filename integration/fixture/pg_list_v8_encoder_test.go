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

func TestPGListV8NativeProofKeepsRequestAndReplyEncodings(t *testing.T) {
	original, hash := newOSDEncoderReceipt(t, pgListV8EncoderSpec, "pg-list-v8", pgListV8EncoderSpec.driver)
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
	if err = verify(original, pgListV8EncoderSpec); err != nil {
		t.Fatal("genuine mixed native receipt rejected", err)
	}
	for i, name := range pgListV8EncoderSpec.names {
		badTypes := []uint16{42, 44, 61}
		badVersions := []uint16{7, 8, 9}
		if i == 0 {
			badTypes = []uint16{43, 44, 61}
			badVersions = []uint16{6, 7, 9}
		}
		for _, typ := range badTypes {
			t.Run(fmt.Sprintf("%s/type-%d", name, typ), func(t *testing.T) {
				var bad map[string]any
				if err := decodeEncoderReceipt(encoded, &bad); err != nil {
					t.Fatal(err)
				}
				changeEncoderType(t, bad, name, typ)
				if err := verify(bad, pgListV8EncoderSpec); err == nil {
					t.Fatal("wrong request/reply class admitted")
				}
			})
		}
		for _, version := range badVersions {
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
				if err := verify(bad, pgListV8EncoderSpec); err == nil {
					t.Fatal("wrong mixed request/reply version admitted")
				}
			})
		}
	}
	for _, change := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mixedV8 = false }, func(s *nativeEncoderSpec) { s.requestVersion = 0 },
		func(s *nativeEncoderSpec) { s.requests = 0 }, func(s *nativeEncoderSpec) { s.requests = 5 },
		func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
	} {
		bad := pgListV8EncoderSpec
		change(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous mixed-v8 suite admitted")
		}
	}
	first := pgListV8EncoderSpec.names[0]
	for _, field := range []string{"pool", "seed", "shard", "raw_hash", "client_inc", "reqid_inc"} {
		t.Run("decoder-routing-"+field, func(t *testing.T) {
			var bad map[string]any
			if err := decodeEncoderReceipt(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			fixture := bad["fixtures"].(map[string]any)[first].(map[string]any)
			fixture["decoded_routing"].(map[string]any)[field] = json.Number("0")
			if err := verify(bad, pgListV8EncoderSpec); err == nil {
				t.Fatal("native PGNLS routing identity mismatch admitted")
			}
		})
	}
}

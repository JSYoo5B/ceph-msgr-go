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

func TestReplyV8NativeProofHasNoImplicitFirstRequest(t *testing.T) {
	original, hash := newOSDEncoderReceipt(t, replyV8EncoderSpec, "reply-v8", replyV8EncoderSpec.driver)
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
	if err = verify(original, replyV8EncoderSpec); err != nil {
		t.Fatal("genuine all-reply receipt rejected", err)
	}
	for _, name := range replyV8EncoderSpec.names {
		for _, typ := range []uint16{42, 44, 61} {
			t.Run(fmt.Sprintf("%s/type-%d", name, typ), func(t *testing.T) {
				var bad map[string]any
				if err := decodeEncoderReceipt(encoded, &bad); err != nil {
					t.Fatal(err)
				}
				changeEncoderType(t, bad, name, typ)
				if err := verify(bad, replyV8EncoderSpec); err == nil {
					t.Fatal("nonreply type admitted")
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
				if err := verify(bad, replyV8EncoderSpec); err == nil {
					t.Fatal("wrong reply version admitted")
				}
			})
		}
	}
	first := replyV8EncoderSpec.names[0]
	fixture := func(p map[string]any) map[string]any { return p["fixtures"].(map[string]any)[first].(map[string]any) }
	op := func(p map[string]any) map[string]any {
		return fixture(p)["decoded_operations"].([]any)[0].(map[string]any)
	}
	changes := map[string]func(map[string]any){
		"missing independent decoder": func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
		"missing vector":              func(p map[string]any) { delete(p["fixtures"].(map[string]any), first) },
		"extra vector":                func(p map[string]any) { p["fixtures"].(map[string]any)["extra"] = map[string]any{} },
		"wrong compat": func(p map[string]any) {
			f := fixture(p)
			h, _ := hex.DecodeString(f["header_hex"].(string))
			binary.LittleEndian.PutUint16(h[37:39], 3)
			f["header_hex"] = hex.EncodeToString(h)
			f["native_header"].(map[string]any)["compat"] = json.Number("3")
		},
		"wrong TID": func(p map[string]any) {
			f := fixture(p)
			h, _ := hex.DecodeString(f["header_hex"].(string))
			binary.LittleEndian.PutUint64(h[8:16], 78)
			f["header_hex"] = hex.EncodeToString(h)
			f["native_header"].(map[string]any)["tid"] = json.Number("78")
		},
		"missing decoded operations": func(p map[string]any) { delete(fixture(p), "decoded_operations") },
		"empty decoded operations":   func(p map[string]any) { fixture(p)["decoded_operations"] = []any{} },
		"result outside int32":       func(p map[string]any) { op(p)["result"] = json.Number("-2147483649") },
		"missing signed result":      func(p map[string]any) { delete(op(p), "result") },
		"payload count inconsistent": func(p map[string]any) { op(p)["payload_len"] = json.Number("0") },
		"output inconsistent":        func(p map[string]any) { op(p)["outdata_hex"] = "ff" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			var bad map[string]any
			if err := decodeEncoderReceipt(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			change(bad)
			if err := verify(bad, replyV8EncoderSpec); err == nil {
				t.Fatal("corrupt reply proof admitted")
			}
		})
	}
	for _, change := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.replyVersion = 0 }, func(s *nativeEncoderSpec) { s.replyVersion = 6 },
		func(s *nativeEncoderSpec) { s.requests = 1 }, func(s *nativeEncoderSpec) { s.requestVersion = 8 },
		func(s *nativeEncoderSpec) { s.controls = true }, func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := replyV8EncoderSpec
		change(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous all-reply suite admitted")
		}
	}
}

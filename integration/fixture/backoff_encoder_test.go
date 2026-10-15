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

// Reconstruct genuine native builder receipts from committed native segments.
// Corrupting every declared control position must fail without weakening the
// existing MOSDOp/MOSDOpReply or trailing MWatchNotify classifications.
func TestBackoffNativeProofRejectsWrongControlClassesAndIdentity(t *testing.T) {
	original, hash := newOSDEncoderReceipt(t, backoffEncoderSpec, "backoff", backoffEncoderSpec.driver)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "receipt.log")
	verify := func(p map[string]any) error {
		raw, err := json.Marshal(p)
		if err != nil {
			return err
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			return err
		}
		_, err = readNativeEncoderProofFor(path, hash, backoffEncoderSpec)
		return err
	}
	if err = verify(original); err != nil {
		t.Fatal("genuine backoff receipt rejected", err)
	}
	for _, name := range backoffEncoderSpec.names {
		for _, typ := range []uint16{42, 43, 44} {
			t.Run(fmt.Sprintf("%s/type-%d", name, typ), func(t *testing.T) {
				var bad map[string]any
				if err = decodeEncoderReceipt(encoded, &bad); err != nil {
					t.Fatal(err)
				}
				changeEncoderType(t, bad, name, typ)
				if err = verify(bad); err == nil {
					t.Fatal("non-backoff class admitted")
				}
			})
		}
	}
	first := backoffEncoderSpec.names[0]
	fixture := func(p map[string]any) map[string]any { return p["fixtures"].(map[string]any)[first].(map[string]any) }
	decoded := func(p map[string]any) map[string]any {
		return fixture(p)["decoded_operations"].([]any)[0].(map[string]any)
	}
	headerChange := func(p map[string]any, offset int, value uint16, field string) {
		f := fixture(p)
		h, _ := hex.DecodeString(f["header_hex"].(string))
		binary.LittleEndian.PutUint16(h[offset:offset+2], value)
		f["header_hex"] = hex.EncodeToString(h)
		f["native_header"].(map[string]any)[field] = json.Number(fmt.Sprint(value))
	}
	cases := map[string]func(map[string]any){
		"missing independent decoder": func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
		"missing vector":              func(p map[string]any) { delete(p["fixtures"].(map[string]any), first) },
		"extra vector":                func(p map[string]any) { p["fixtures"].(map[string]any)["extra"] = map[string]any{} },
		"old message version":         func(p map[string]any) { headerChange(p, 20, 0, "version") },
		"future message version":      func(p map[string]any) { headerChange(p, 20, 2, "version") },
		"wrong compat":                func(p map[string]any) { headerChange(p, 37, 0, "compat") },
		"header TID nonzero": func(p map[string]any) {
			f := fixture(p)
			h, _ := hex.DecodeString(f["header_hex"].(string))
			binary.LittleEndian.PutUint64(h[8:16], 77)
			f["header_hex"] = hex.EncodeToString(h)
			f["native_header"].(map[string]any)["tid"] = json.Number("77")
		},
		"nonempty data": func(p map[string]any) {
			f := fixture(p)
			f["data_hex"] = "00"
			f["data_bytes"] = json.Number("1")
			f["native_header"].(map[string]any)["data_len"] = json.Number("1")
		},
		"missing decoded control": func(p map[string]any) { delete(fixture(p), "decoded_operations") },
		"empty decoded control":   func(p map[string]any) { fixture(p)["decoded_operations"] = []any{} },
		"wrong decoded opcode":    func(p map[string]any) { decoded(p)["code"] = json.Number("2") },
		"wrong decoded epoch":     func(p map[string]any) { decoded(p)["map_epoch"] = json.Number("12") },
		"wrong decoded ID":        func(p map[string]any) { decoded(p)["id"] = json.Number("18") },
		"overflowing decoded ID":  func(p map[string]any) { decoded(p)["id"] = json.Number("18446744073709551616") },
		"missing decoded range":   func(p map[string]any) { delete(decoded(p), "single") },
		"invalid front opcode": func(p map[string]any) {
			f := fixture(p)
			raw, _ := hex.DecodeString(f["front_hex"].(string))
			raw[28] = 4
			f["front_hex"] = hex.EncodeToString(raw)
			decoded(p)["code"] = json.Number("4")
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			var bad map[string]any
			if err := decodeEncoderReceipt(encoded, &bad); err != nil {
				t.Fatal(err)
			}
			change(bad)
			if err := verify(bad); err == nil {
				t.Fatal("corrupt control proof admitted")
			}
		})
	}
}

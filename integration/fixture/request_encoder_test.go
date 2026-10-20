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

// All success references are freshly generated native MClientRequest,
// MClientRequestForward and MClientReply segment files. Corrupting their
// proof receipt tests admission; it does not generate a wire oracle in Go.
func mdsRequestEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	base := "../../internal/mds/testdata/"
	raw, err := os.ReadFile(base + "mds-request-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := decodeEncoderReceipt(raw, &p); err != nil {
		t.Fatal(err)
	}
	for _, name := range mdsRequestEncoderSpec.names {
		fixture, ok := p["fixtures"].(map[string]any)[name].(map[string]any)
		if !ok {
			t.Fatal("native request vector missing", name)
		}
		for _, segment := range []string{"front", "data", "header"} {
			text, err := os.ReadFile(base + name + "." + segment + ".hex")
			if err != nil {
				t.Fatal(err)
			}
			encoded := strings.TrimSpace(string(text))
			data, err := hex.DecodeString(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(data)) {
				t.Fatal("retained native request segment digest differs", name, segment)
			}
			fixture[segment+"_hex"] = encoded
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + mdsRequestEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := os.ReadFile("../../tools/" + mdsRequestEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	driverHash := fmt.Sprintf("%x", sha256.Sum256(driver))
	if p["wrapper_sha256"] != hash || p["build_driver_sha256"] != driverHash || p["launcher_sha256"] != driverHash {
		t.Fatal("retained native request producer differs from source")
	}
	return p, hash
}

// replyBlobs splits a native MClientReply front into its head and the
// trace, extra and snap blob offsets.
func replyBlobs(t *testing.T, front []byte) (trace, extra, snap int) {
	t.Helper()
	at := 15 // op, result, mdsmap_epoch, safe, is_dentry, is_target
	offsets := [3]int{}
	for i := range offsets {
		n := int(binary.LittleEndian.Uint32(front[at:]))
		offsets[i] = at + 4
		at += 4 + n
	}
	if at != len(front) {
		t.Fatal("native reply front extent differs")
	}
	return offsets[0], offsets[1], offsets[2]
}

func TestMDSRequestNativeProofClosedClassesAndCorruptions(t *testing.T) {
	original, hash := mdsRequestEncoderReceipt(t)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "request-proof.log")
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
	if err := verify(original, mdsRequestEncoderSpec); err != nil {
		t.Fatal("fresh native request proof rejected", err)
	}
	clone := func() map[string]any {
		var p map[string]any
		if err := decodeEncoderReceipt(encoded, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	fixture := func(p map[string]any, name string) map[string]any {
		return p["fixtures"].(map[string]any)[name].(map[string]any)
	}
	receipt := func(f map[string]any) map[string]any {
		for _, key := range []string{"decoded_request", "decoded_forward", "decoded_reply"} {
			if r, ok := f[key].(map[string]any); ok {
				return r
			}
		}
		t.Fatal("fixture lacks a decoder receipt")
		return nil
	}
	for _, name := range mdsRequestEncoderSpec.names {
		want := nativeRequestVectors[name]
		for _, field := range []string{"front", "header", "data"} {
			t.Run(name+"/hash-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)[field+"_sha256"] = strings.Repeat("0", 64)
				if err := verify(p, mdsRequestEncoderSpec); err == nil {
					t.Fatal("segment hash corruption admitted")
				}
			})
		}
		otherType := uint16(26)
		if want.typ == 26 {
			otherType = 24
		}
		for _, mutation := range []struct {
			name string
			edit func([]byte)
		}{
			{"session class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 22) }},
			{"map class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 21) }},
			{"OSD class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 43) }},
			{"other request-family class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], otherType) }},
			{"zero TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 0) }},
			{"foreign TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 77) }},
			{"other family version", func(h []byte) { h[20] ^= 6 ^ 1 }},
			{"unpinned version 2", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 2) }},
			{"wrong compat", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 2) }},
			{"noncanonical compatibility zero", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 0) }},
			{"wrong sequence", func(h []byte) { binary.LittleEndian.PutUint64(h[:8], 2) }},
			{"wrong ack", func(h []byte) { binary.LittleEndian.PutUint64(h[28:36], 4) }},
			{"wrong priority", func(h []byte) { binary.LittleEndian.PutUint16(h[18:20], 196) }},
		} {
			t.Run(name+"/"+mutation.name, func(t *testing.T) {
				p := clone()
				f := fixture(p, name)
				h, _ := hex.DecodeString(f["header_hex"].(string))
				mutation.edit(h)
				syncMDSSessionHeaderMetadata(t, f, h)
				f["header_hex"] = hex.EncodeToString(h)
				rehashMDSMapFixture(t, f)
				if err := verify(p, mdsRequestEncoderSpec); err == nil {
					t.Fatal("foreign request-family header admitted")
				}
			})
		}
		for _, field := range nativeRequestReceiptKeys[want.typ] {
			t.Run(name+"/decoder-"+field, func(t *testing.T) {
				p := clone()
				delete(receipt(fixture(p, name)), field)
				if err := verify(p, mdsRequestEncoderSpec); err == nil {
					t.Fatal("incomplete native decoder receipt admitted")
				}
			})
		}
		t.Run(name+"/decoder-extra-field", func(t *testing.T) {
			p := clone()
			receipt(fixture(p, name))["unreviewed"] = json.Number("1")
			if err := verify(p, mdsRequestEncoderSpec); err == nil {
				t.Fatal("unreviewed native decoder receipt field admitted")
			}
		})
		t.Run(name+"/foreign-receipt-class", func(t *testing.T) {
			p := clone()
			f := fixture(p, name)
			f["decoded_session"] = receipt(f)
			if err := verify(p, mdsRequestEncoderSpec); err == nil {
				t.Fatal("second decoder receipt class admitted")
			}
		})
	}
	front := func(f map[string]any) []byte {
		b, _ := hex.DecodeString(f["front_hex"].(string))
		return b
	}
	// Each edit keeps segment digests, envelope lengths and (where noted)
	// the decoder receipt self-consistent, so only the closed vector contract
	// or the receipt-to-bytes binding can reject it.
	for kind, edit := range map[string]func(p map[string]any){
		"consistent-foreign-op": func(p map[string]any) {
			f := fixture(p, "mds-request-native-request-lookup")
			b := front(f)
			binary.LittleEndian.PutUint32(b[22:26], 0x101) // head.op after version..num_releases
			receipt(f)["op"] = json.Number("257")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-untruncated-legacy-retry": func(p map[string]any) {
			f := fixture(p, "mds-request-native-request-mkdir-release")
			b := front(f)
			b[18] = 0xff // legacy u8 num_retry; ext_num_retry stays 0x101
			receipt(f)["num_retry"] = json.Number("255")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-legacy-head-version": func(p map[string]any) {
			f := fixture(p, "mds-request-native-request-rename")
			b := front(f)
			binary.LittleEndian.PutUint16(b[:2], 2)
			r := receipt(f)
			r["head_version"], r["source_head_version"] = json.Number("2"), json.Number("2")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-foreign-result": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-unsafe-error")
			b := front(f)
			binary.LittleEndian.PutUint32(b[4:8], uint32(0xfffffffb)) // -EIO
			receipt(f)["result"] = json.Number("-5")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-safe-flag": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-unsafe-error")
			b := front(f)
			b[12] = 1
			receipt(f)["safe"] = json.Number("1")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-foreign-dentry": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-lookup")
			b := front(f)
			b[strings.Index(string(b), "\x04\x00\x00\x00file")+7] = 'd'
			receipt(f)["trace"].(map[string]any)["dentry_name"] = "fild"
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-foreign-forward": func(p map[string]any) {
			f := fixture(p, "mds-request-native-forward")
			b := front(f)
			binary.LittleEndian.PutUint32(b[:4], 2)
			receipt(f)["dest_mds"] = json.Number("2")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"receipt-only-path": func(p map[string]any) {
			receipt(fixture(p, "mds-request-native-request-lookup"))["path"] = "dir/filf"
		},
		"receipt-only-release": func(p map[string]any) {
			release := receipt(fixture(p, "mds-request-native-request-mkdir-release"))["releases"].([]any)[0].(map[string]any)
			release["cap_id"] = json.Number("4661")
		},
		"bytes-only-gid": func(p map[string]any) {
			f := fixture(p, "mds-request-native-request-lookup")
			b := front(f)
			// gid_list is followed by three empty byte strings (12 bytes).
			b[len(b)-12-8] ^= 1
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-fscrypt-file": func(p map[string]any) {
			f := fixture(p, "mds-request-native-request-mkdir-release")
			b := front(f)
			b[len(b)-1] ^= 0xff
			f["front_hex"] = hex.EncodeToString(b)
		},
		"receipt-only-target-ino": func(p map[string]any) {
			receipt(fixture(p, "mds-request-native-reply-lookup"))["trace"].(map[string]any)["target"].(map[string]any)["ino"] = json.Number("1099511627778")
		},
		"receipt-only-lease-alternate-name": func(p map[string]any) {
			receipt(fixture(p, "mds-request-native-reply-lookup"))["trace"].(map[string]any)["lease"].(map[string]any)["alternate_name_hex"] = "616c74"
		},
		"bytes-only-snap-change-attr": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-lookup")
			b := front(f)
			b[len(b)-8] ^= 1
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-readdir-flags": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-readdir")
			b := front(f)
			_, extra, _ := replyBlobs(t, b)
			dirstat := 6 + int(binary.LittleEndian.Uint32(b[extra+2:]))
			b[extra+dirstat+4] ^= 0x02 // flags low byte after the entry count
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-dirstat-rank": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-lookup")
			b := front(f)
			trace, _, _ := replyBlobs(t, b)
			inode := 6 + int(binary.LittleEndian.Uint32(b[trace+2:]))
			b[trace+inode+6+12+4] = 3 // second dist rank 2 -> 3, still ordered
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-readdir-entry-dropped": func(p map[string]any) {
			f := fixture(p, "mds-request-native-reply-readdir")
			r := receipt(f)["readdir"].(map[string]any)
			r["entries"] = r["entries"].([]any)[:1]
		},
	} {
		t.Run(kind, func(t *testing.T) {
			p := clone()
			edit(p)
			for _, name := range mdsRequestEncoderSpec.names {
				rehashMDSMapFixture(t, fixture(p, name))
			}
			if err := verify(p, mdsRequestEncoderSpec); err == nil {
				t.Fatal("self-consistent foreign request-family identity/bytes admitted")
			}
		})
	}

	for name, edit := range map[string]func(map[string]any){
		"source archive":           func(p map[string]any) { p["source_archive_sha256"] = strings.Repeat("0", 64) },
		"source URL":               func(p map[string]any) { p["source_archive_url"] = "https://example.invalid/source" },
		"native decoder":           func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
		"missing canonical encode": func(p map[string]any) { delete(p, "canonical_message_encode_verified") },
		"false canonical encode":   func(p map[string]any) { p["canonical_message_encode_verified"] = false },
		"empty library digest":     func(p map[string]any) { p["native_library_sha256"] = "" },
		"foreign native library": func(p map[string]any) {
			p["loader_audit"] = "libceph-common.so.2 => /usr/lib64/ceph/libceph-common.so.2"
		},
		"launcher differs": func(p map[string]any) { p["launcher_sha256"] = strings.Repeat("1", 64) },
		"missing vector":   func(p map[string]any) { delete(p["fixtures"].(map[string]any), mdsRequestEncoderSpec.names[0]) },
		"extra vector":     func(p map[string]any) { p["fixtures"].(map[string]any)["unreviewed-class"] = map[string]any{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := clone()
			edit(p)
			if err := verify(p, mdsRequestEncoderSpec); err == nil {
				t.Fatal("invalid native request provenance admitted")
			}
		})
	}
	for _, edit := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mdsRequests = false }, func(s *nativeEncoderSpec) { s.mdsSessions = true }, func(s *nativeEncoderSpec) { s.mdsMaps = true },
		func(s *nativeEncoderSpec) { s.command = "encode-arbitrary" }, func(s *nativeEncoderSpec) { s.names = s.names[1:] },
		func(s *nativeEncoderSpec) { s.requests = 1 }, func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
		func(s *nativeEncoderSpec) { s.requestVersion = 8 }, func(s *nativeEncoderSpec) { s.replyVersion = 8 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := mdsRequestEncoderSpec
		edit(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous native request suite admitted", bad)
		}
	}
	for _, other := range []nativeEncoderSpec{mdsMapEncoderSpec, mdsSessionEncoderSpec, snapEncoderSpec, mutationEncoderSpec, backoffEncoderSpec, requestV8EncoderSpec, replyV8EncoderSpec} {
		if err := verify(original, other); err == nil {
			t.Fatal("request suite admitted as another suite", other.command)
		}
	}
	// Retained map and session proofs cannot enter the request family, and
	// neither family accepts the request flag as an override.
	for _, retained := range []struct {
		spec    nativeEncoderSpec
		receipt func(*testing.T) (map[string]any, string)
	}{{mdsMapEncoderSpec, mdsMapEncoderReceipt}, {mdsSessionEncoderSpec, mdsSessionEncoderReceipt}} {
		proof, proofHash := retained.receipt(t)
		raw, err := json.Marshal(proof)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, mdsRequestEncoderSpec); err == nil {
			t.Fatal("retained proof admitted as request suite", retained.spec.command)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, retained.spec); err != nil {
			t.Fatal("retained proof no longer admitted by its own suite", retained.spec.command, err)
		}
		flagged := retained.spec
		flagged.mdsRequests = true
		if _, err = readNativeEncoderProofFor(path, proofHash, flagged); err == nil {
			t.Fatal("request flag admitted on another MDS suite", retained.spec.command)
		}
	}
}

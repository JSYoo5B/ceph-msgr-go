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

// All success references are freshly generated native MClientReconnect
// segment files. Corrupting their proof receipt tests admission; it does not
// generate a wire oracle in Go.
func mdsReconnectEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	base := "../../internal/mds/testdata/"
	raw, err := os.ReadFile(base + "mds-reconnect-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := decodeEncoderReceipt(raw, &p); err != nil {
		t.Fatal(err)
	}
	for _, name := range mdsReconnectEncoderSpec.names {
		fixture, ok := p["fixtures"].(map[string]any)[name].(map[string]any)
		if !ok {
			t.Fatal("native reconnect vector missing", name)
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
				t.Fatal("retained native reconnect segment digest differs", name, segment)
			}
			fixture[segment+"_hex"] = encoded
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + mdsReconnectEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := os.ReadFile("../../tools/" + mdsReconnectEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	driverHash := fmt.Sprintf("%x", sha256.Sum256(driver))
	if p["wrapper_sha256"] != hash || p["build_driver_sha256"] != driverHash || p["launcher_sha256"] != driverHash {
		t.Fatal("retained native reconnect producer differs from source")
	}
	return p, hash
}

// settleMDSReconnectFixture keeps segment digests, byte counts and envelope
// lengths self-consistent after an edit.
func settleMDSReconnectFixture(t *testing.T, f map[string]any) {
	t.Helper()
	metadata := f["native_header"].(map[string]any)
	for _, segment := range []string{"front", "data", "header"} {
		b, err := hex.DecodeString(f[segment+"_hex"].(string))
		if err != nil {
			t.Fatal(err)
		}
		f[segment+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(b))
		f[segment+"_bytes"] = json.Number(fmt.Sprint(len(b)))
		if segment != "header" {
			metadata[segment+"_len"] = json.Number(fmt.Sprint(len(b)))
		}
	}
}

func TestMDSReconnectNativeProofClosedClassesAndCorruptions(t *testing.T) {
	original, hash := mdsReconnectEncoderReceipt(t)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "reconnect-proof.log")
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
	if err := verify(original, mdsReconnectEncoderSpec); err != nil {
		t.Fatal("fresh native reconnect proof rejected", err)
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
	receipt := func(f map[string]any) map[string]any { return f["decoded_reconnect"].(map[string]any) }
	for _, name := range mdsReconnectEncoderSpec.names {
		for _, field := range []string{"front", "header", "data"} {
			t.Run(name+"/hash-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)[field+"_sha256"] = strings.Repeat("0", 64)
				if err := verify(p, mdsReconnectEncoderSpec); err == nil {
					t.Fatal("segment hash corruption admitted")
				}
			})
		}
		for _, mutation := range []struct {
			name string
			edit func([]byte)
		}{
			{"session class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 22) }},
			{"request class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 24) }},
			{"capability class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], nativeCapsTypeCaps) }},
			{"OSD class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 43) }},
			{"foreign TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 41) }},
			{"legacy version 4 without more", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 4) }},
			{"feature-selected version 3", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 3) }},
			{"wrong compat", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 1) }},
			{"compat equal to version", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 5) }},
			{"noncanonical compatibility zero", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 0) }},
			{"data offset", func(h []byte) { binary.LittleEndian.PutUint16(h[26:28], 1) }},
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
				settleMDSReconnectFixture(t, f)
				if err := verify(p, mdsReconnectEncoderSpec); err == nil {
					t.Fatal("foreign reconnect-family header admitted")
				}
			})
		}
		for _, field := range []string{"middle_len", "front_len"} {
			t.Run(name+"/inconsistent-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)["native_header"].(map[string]any)[field] = json.Number("1")
				if err := verify(p, mdsReconnectEncoderSpec); err == nil {
					t.Fatal("inconsistent envelope length admitted")
				}
			})
		}
		t.Run(name+"/recorded-middle-segment", func(t *testing.T) {
			p := clone()
			f := fixture(p, name)
			f["middle_hex"], f["middle_bytes"], f["middle_sha256"] = "", json.Number("0"), fmt.Sprintf("%x", sha256.Sum256(nil))
			if err := verify(p, mdsReconnectEncoderSpec); err == nil {
				t.Fatal("recorded middle segment admitted")
			}
		})
		for _, field := range nativeReconnectReceiptKeys {
			t.Run(name+"/decoder-"+field, func(t *testing.T) {
				p := clone()
				delete(receipt(fixture(p, name)), field)
				if err := verify(p, mdsReconnectEncoderSpec); err == nil {
					t.Fatal("incomplete native decoder receipt admitted")
				}
			})
		}
		t.Run(name+"/decoder-extra-field", func(t *testing.T) {
			p := clone()
			receipt(fixture(p, name))["unreviewed"] = json.Number("1")
			if err := verify(p, mdsReconnectEncoderSpec); err == nil {
				t.Fatal("unreviewed native decoder receipt field admitted")
			}
		})
		t.Run(name+"/foreign-receipt-class", func(t *testing.T) {
			p := clone()
			f := fixture(p, name)
			f["decoded_caps"] = receipt(f)
			if err := verify(p, mdsReconnectEncoderSpec); err == nil {
				t.Fatal("second decoder receipt class admitted")
			}
		})
	}
	segment := func(f map[string]any, name string) []byte {
		b, _ := hex.DecodeString(f[name+"_hex"].(string))
		return b
	}
	two, more, empty := mdsReconnectEncoderSpec.names[0], mdsReconnectEncoderSpec.names[1], mdsReconnectEncoderSpec.names[2]
	capReceipt := func(p map[string]any, i int) map[string]any {
		return receipt(fixture(p, two))["caps"].([]any)[i].(map[string]any)
	}
	editData := func(p map[string]any, name string, edit func([]byte) []byte) {
		f := fixture(p, name)
		f["data_hex"] = hex.EncodeToString(edit(segment(f, "data")))
	}
	// Two-caps data layout: cap count at 0; the "dir" cap (ino at 4,
	// envelope at 12, cap_id at 25) ends at 69; the "data.bin" cap (ino at
	// 69, envelope length at 79, pathbase at 119, flock_len at 127, flock
	// blob 131..221, snap_follows at 221) ends at 229; realm count at 229,
	// first realm seq at 247; the more flag is the last byte.
	// Each edit keeps segment digests and envelope lengths (and where noted
	// the decoder receipt) self-consistent, so only the closed vector
	// contract or the receipt-to-bytes binding can reject it.
	for kind, edit := range map[string]func(p map[string]any){
		"consistent-payload-in-front": func(p map[string]any) {
			f := fixture(p, two)
			f["front_hex"], f["data_hex"] = f["data_hex"], ""
		},
		"consistent-foreign-more": func(p map[string]any) {
			editData(p, empty, func(b []byte) []byte { b[len(b)-1] = 1; return b })
			receipt(fixture(p, empty))["more"] = true
		},
		"consistent-noncanonical-more": func(p map[string]any) {
			editData(p, more, func(b []byte) []byte { b[len(b)-1] = 2; return b })
		},
		"consistent-dropped-realm": func(p map[string]any) {
			editData(p, empty, func(b []byte) []byte {
				binary.LittleEndian.PutUint32(b[4:8], 0)
				return append(b[:8:8], b[38:]...)
			})
			receipt(fixture(p, empty))["realms"] = []any{}
		},
		"consistent-unordered-caps": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte {
				out := append([]byte{}, b[:4]...)
				out = append(out, b[69:229]...)
				out = append(out, b[4:69]...)
				return append(out, b[229:]...)
			})
			caps := receipt(fixture(p, two))["caps"].([]any)
			caps[0], caps[1] = caps[1], caps[0]
		},
		"consistent-legacy-cap-envelope": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[12] = 1; return b })
		},
		"consistent-dropped-flock": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte {
				binary.LittleEndian.PutUint32(b[79:83], 146-90)
				binary.LittleEndian.PutUint32(b[127:131], 0)
				return append(b[:131:131], b[221:]...)
			})
			c := capReceipt(p, 1)
			c["flock_len"], c["flockbl_hex"], c["fcntl_locks"], c["flock_locks"] = json.Number("0"), "", []any{}, []any{}
		},
		"receipt-only-path":          func(p map[string]any) { capReceipt(p, 0)["path"] = "dir2" },
		"receipt-only-wanted":        func(p map[string]any) { capReceipt(p, 1)["wanted"] = json.Number("15361") },
		"receipt-only-snap-follows":  func(p map[string]any) { capReceipt(p, 1)["snap_follows"] = json.Number("8") },
		"receipt-only-flock-len":     func(p map[string]any) { capReceipt(p, 1)["flock_len"] = json.Number("89") },
		"receipt-only-fcntl-type":    func(p map[string]any) { lockReceipt(capReceipt(p, 1), "fcntl_locks")["type"] = json.Number("2") },
		"receipt-only-flock-owner":   func(p map[string]any) { lockReceipt(capReceipt(p, 1), "flock_locks")["owner"] = json.Number("39320") },
		"receipt-only-missing-locks": func(p map[string]any) { delete(capReceipt(p, 0), "flock_locks") },
		"receipt-only-realm-parent": func(p map[string]any) {
			receipt(fixture(p, two))["realms"].([]any)[0].(map[string]any)["parent"] = json.Number("0")
		},
		"receipt-only-more": func(p map[string]any) { receipt(fixture(p, more))["more"] = false },
		"bytes-only-cap-id": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[25] ^= 1; return b })
		},
		"bytes-only-pathbase": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[119] ^= 1; return b })
		},
		"bytes-only-flock-pid": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[212] ^= 1; return b })
		},
		"bytes-only-snap-follows": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[221] ^= 1; return b })
		},
		"bytes-only-realm-seq": func(p map[string]any) {
			editData(p, two, func(b []byte) []byte { b[247] ^= 1; return b })
		},
		"bytes-only-trailing-byte": func(p map[string]any) {
			editData(p, empty, func(b []byte) []byte { return append(b, 0) })
		},
	} {
		t.Run(kind, func(t *testing.T) {
			p := clone()
			edit(p)
			for _, name := range mdsReconnectEncoderSpec.names {
				settleMDSReconnectFixture(t, fixture(p, name))
			}
			if err := verify(p, mdsReconnectEncoderSpec); err == nil {
				t.Fatal("self-consistent foreign reconnect-family identity/bytes admitted")
			}
		})
	}
	t.Run("settled-original", func(t *testing.T) {
		p := clone()
		for _, name := range mdsReconnectEncoderSpec.names {
			settleMDSReconnectFixture(t, fixture(p, name))
		}
		if err := verify(p, mdsReconnectEncoderSpec); err != nil {
			t.Fatal("settling an unedited proof changed its admission", err)
		}
	})
	t.Run("data-bytes-count", func(t *testing.T) {
		p := clone()
		fixture(p, two)["data_bytes"] = json.Number("1")
		if err := verify(p, mdsReconnectEncoderSpec); err == nil {
			t.Fatal("inconsistent data byte count admitted")
		}
	})

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
		"missing vector":   func(p map[string]any) { delete(p["fixtures"].(map[string]any), two) },
		"extra vector":     func(p map[string]any) { p["fixtures"].(map[string]any)["unreviewed-class"] = map[string]any{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := clone()
			edit(p)
			if err := verify(p, mdsReconnectEncoderSpec); err == nil {
				t.Fatal("invalid native reconnect provenance admitted")
			}
		})
	}
	for _, edit := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mdsReconnect = false }, func(s *nativeEncoderSpec) { s.mdsCaps = true },
		func(s *nativeEncoderSpec) { s.mdsRequests = true }, func(s *nativeEncoderSpec) { s.mdsSessions = true },
		func(s *nativeEncoderSpec) { s.mdsMaps = true }, func(s *nativeEncoderSpec) { s.command = "encode-arbitrary" },
		func(s *nativeEncoderSpec) { s.names = s.names[1:] }, func(s *nativeEncoderSpec) { s.requests = 1 },
		func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
		func(s *nativeEncoderSpec) { s.requestVersion = 8 }, func(s *nativeEncoderSpec) { s.replyVersion = 8 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := mdsReconnectEncoderSpec
		edit(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous native reconnect suite admitted", bad)
		}
	}
	for _, other := range []nativeEncoderSpec{mdsMapEncoderSpec, mdsSessionEncoderSpec, mdsRequestEncoderSpec, mdsCapsEncoderSpec, snapEncoderSpec, mutationEncoderSpec, backoffEncoderSpec, requestV8EncoderSpec, replyV8EncoderSpec} {
		if err := verify(original, other); err == nil {
			t.Fatal("reconnect suite admitted as another suite", other.command)
		}
	}
	// Retained map, session, request and capability proofs cannot enter the
	// reconnect family, and none of them accepts the reconnect flag as an
	// override.
	for _, retained := range []struct {
		spec    nativeEncoderSpec
		receipt func(*testing.T) (map[string]any, string)
	}{{mdsMapEncoderSpec, mdsMapEncoderReceipt}, {mdsSessionEncoderSpec, mdsSessionEncoderReceipt}, {mdsRequestEncoderSpec, mdsRequestEncoderReceipt}, {mdsCapsEncoderSpec, mdsCapsEncoderReceipt}} {
		proof, proofHash := retained.receipt(t)
		raw, err := json.Marshal(proof)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, mdsReconnectEncoderSpec); err == nil {
			t.Fatal("retained proof admitted as reconnect suite", retained.spec.command)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, retained.spec); err != nil {
			t.Fatal("retained proof no longer admitted by its own suite", retained.spec.command, err)
		}
		flagged := retained.spec
		flagged.mdsReconnect = true
		if _, err = readNativeEncoderProofFor(path, proofHash, flagged); err == nil {
			t.Fatal("reconnect flag admitted on another MDS suite", retained.spec.command)
		}
	}
	// Every reconnect vector must record its data segment; a front-only
	// layout is refused.
	stripped := clone()
	for _, name := range mdsReconnectEncoderSpec.names {
		f := fixture(stripped, name)
		delete(f, "data_hex")
		delete(f, "data_bytes")
	}
	if err := verify(stripped, mdsReconnectEncoderSpec); err == nil {
		t.Fatal("reconnect proof without data segments admitted")
	}
}

func lockReceipt(c map[string]any, key string) map[string]any {
	return c[key].([]any)[0].(map[string]any)
}

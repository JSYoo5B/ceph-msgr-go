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

// All success references are freshly generated native MClientCaps,
// MClientCapRelease, MClientLease, MClientSnap and MClientQuota segment
// files. Corrupting their proof receipt tests admission; it does not generate
// a wire oracle in Go.
func mdsCapsEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	base := "../../internal/mds/testdata/"
	raw, err := os.ReadFile(base + "mds-caps-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := decodeEncoderReceipt(raw, &p); err != nil {
		t.Fatal(err)
	}
	for _, name := range mdsCapsEncoderSpec.names {
		fixture, ok := p["fixtures"].(map[string]any)[name].(map[string]any)
		if !ok {
			t.Fatal("native capability vector missing", name)
		}
		for _, segment := range []string{"front", "middle", "data", "header"} {
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
				t.Fatal("retained native capability segment digest differs", name, segment)
			}
			fixture[segment+"_hex"] = encoded
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + mdsCapsEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := os.ReadFile("../../tools/" + mdsCapsEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	driverHash := fmt.Sprintf("%x", sha256.Sum256(driver))
	if p["wrapper_sha256"] != hash || p["build_driver_sha256"] != driverHash || p["launcher_sha256"] != driverHash {
		t.Fatal("retained native capability producer differs from source")
	}
	return p, hash
}

func rehashMDSCapsFixture(t *testing.T, f map[string]any) {
	t.Helper()
	for _, segment := range []string{"front", "middle", "data", "header"} {
		b, err := hex.DecodeString(f[segment+"_hex"].(string))
		if err != nil {
			t.Fatal(err)
		}
		f[segment+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
}

func TestMDSCapsNativeProofClosedClassesAndCorruptions(t *testing.T) {
	original, hash := mdsCapsEncoderReceipt(t)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "caps-proof.log")
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
	if err := verify(original, mdsCapsEncoderSpec); err != nil {
		t.Fatal("fresh native capability proof rejected", err)
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
		for _, key := range []string{"decoded_caps", "decoded_cap_release", "decoded_lease", "decoded_snap", "decoded_quota"} {
			if r, ok := f[key].(map[string]any); ok {
				return r
			}
		}
		t.Fatal("fixture lacks a decoder receipt")
		return nil
	}
	for _, name := range mdsCapsEncoderSpec.names {
		want := nativeCapsVectors[name]
		for _, field := range []string{"front", "middle", "header", "data"} {
			t.Run(name+"/hash-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)[field+"_sha256"] = strings.Repeat("0", 64)
				if err := verify(p, mdsCapsEncoderSpec); err == nil {
					t.Fatal("segment hash corruption admitted")
				}
			})
		}
		otherType := uint16(nativeCapsTypeLease)
		if want.typ == nativeCapsTypeLease {
			otherType = nativeCapsTypeCaps
		}
		for _, mutation := range []struct {
			name string
			edit func([]byte)
		}{
			{"session class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 22) }},
			{"request class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 24) }},
			{"OSD class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 43) }},
			{"other capability-family class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], otherType) }},
			{"foreign TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 41) }},
			{"swapped flush TID", func(h []byte) {
				binary.LittleEndian.PutUint64(h[8:16], binary.LittleEndian.Uint64(h[8:16])^nativeCapsFlushTID)
			}},
			{"legacy caps version 11", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 11) }},
			{"unpinned version 3", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 3) }},
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
				rehashMDSCapsFixture(t, f)
				if err := verify(p, mdsCapsEncoderSpec); err == nil {
					t.Fatal("foreign capability-family header admitted")
				}
			})
		}
		t.Run(name+"/inconsistent-middle-length", func(t *testing.T) {
			p := clone()
			f := fixture(p, name)
			metadata := f["native_header"].(map[string]any)
			metadata["middle_len"] = json.Number(fmt.Sprint(len(f["middle_hex"].(string))/2 + 1))
			if err := verify(p, mdsCapsEncoderSpec); err == nil {
				t.Fatal("inconsistent middle length admitted")
			}
		})
		for _, field := range nativeCapsReceiptKeys[want.typ] {
			t.Run(name+"/decoder-"+field, func(t *testing.T) {
				p := clone()
				delete(receipt(fixture(p, name)), field)
				if err := verify(p, mdsCapsEncoderSpec); err == nil {
					t.Fatal("incomplete native decoder receipt admitted")
				}
			})
		}
		t.Run(name+"/decoder-extra-field", func(t *testing.T) {
			p := clone()
			receipt(fixture(p, name))["unreviewed"] = json.Number("1")
			if err := verify(p, mdsCapsEncoderSpec); err == nil {
				t.Fatal("unreviewed native decoder receipt field admitted")
			}
		})
		t.Run(name+"/foreign-receipt-class", func(t *testing.T) {
			p := clone()
			f := fixture(p, name)
			f["decoded_request"] = receipt(f)
			if err := verify(p, mdsCapsEncoderSpec); err == nil {
				t.Fatal("second decoder receipt class admitted")
			}
		})
	}
	segment := func(f map[string]any, name string) []byte {
		b, _ := hex.DecodeString(f[name+"_hex"].(string))
		return b
	}
	caps := "mds-caps-native-caps-grant"
	// Each edit keeps segment digests and envelope lengths (and where noted
	// the decoder receipt) self-consistent, so only the closed vector
	// contract or the receipt-to-bytes binding can reject it.
	for kind, edit := range map[string]func(p map[string]any){
		"consistent-foreign-op": func(p map[string]any) {
			f := fixture(p, caps)
			b := segment(f, "front")
			binary.LittleEndian.PutUint32(b[:4], 1) // REVOKE instead of GRANT
			receipt(f)["op"] = json.Number("1")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-foreign-lease-action": func(p map[string]any) {
			f := fixture(p, "mds-caps-native-lease-revoke")
			b := segment(f, "front")
			b[0] = 3 // RENEW
			receipt(f)["action"] = json.Number("3")
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-dropped-middle": func(p map[string]any) {
			f := fixture(p, caps)
			f["middle_hex"] = ""
			f["native_header"].(map[string]any)["middle_len"] = json.Number("0")
		},
		"consistent-export-padding": func(p map[string]any) {
			f := fixture(p, "mds-caps-native-caps-export")
			b := segment(f, "front")
			b[92+21] = 1 // first padding byte after the export peer
			f["front_hex"] = hex.EncodeToString(b)
		},
		"consistent-snap-split-count": func(p map[string]any) {
			f := fixture(p, "mds-caps-native-snap-split")
			b := segment(f, "front")
			// Drop the one split realm (after the 24-byte head and two inos).
			binary.LittleEndian.PutUint32(b[16:20], 0)
			b = append(b[:40:40], b[48:]...)
			r := receipt(f)
			r["split_realms"] = []any{}
			r["num_split_realms"] = json.Number("0")
			f["front_hex"] = hex.EncodeToString(b)
			f["native_header"].(map[string]any)["front_len"] = json.Number(fmt.Sprint(len(b)))
		},
		"receipt-only-xattr-value": func(p map[string]any) {
			x := receipt(fixture(p, caps))["xattrs"].([]any)[1].(map[string]any)
			x["value_hex"] = "00ff11"
		},
		"receipt-only-peer-mds": func(p map[string]any) {
			receipt(fixture(p, "mds-caps-native-caps-import"))["peer"].(map[string]any)["mds"] = json.Number("2")
		},
		"receipt-only-grant-peer": func(p map[string]any) {
			receipt(fixture(p, caps))["peer"] = map[string]any{"cap_id": json.Number("0"), "issue_seq": json.Number("0"), "mseq": json.Number("0"), "mds": json.Number("0"), "flags": json.Number("0")}
		},
		"receipt-only-layout-pool": func(p map[string]any) {
			receipt(fixture(p, "mds-caps-native-caps-import"))["layout"].(map[string]any)["pool_id"] = json.Number("0")
		},
		"receipt-only-nfiles": func(p map[string]any) {
			receipt(fixture(p, "mds-caps-native-caps-flush-ack"))["nfiles"] = json.Number("0")
		},
		"receipt-only-realm-change-attr": func(p map[string]any) {
			receipt(fixture(p, "mds-caps-native-snap-split"))["snap_realms"].([]any)[1].(map[string]any)["change_attr"] = json.Number("8")
		},
		"receipt-only-quota": func(p map[string]any) {
			receipt(fixture(p, "mds-caps-native-quota"))["max_files"] = json.Number("1001")
		},
		"bytes-only-middle-xattr": func(p map[string]any) {
			f := fixture(p, caps)
			b := segment(f, "middle")
			b[len(b)-1] ^= 1
			f["middle_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-caller-uid": func(p map[string]any) {
			f := fixture(p, "mds-caps-native-caps-update")
			b := segment(f, "front")
			// The v7 caller uid follows the inline blob, barrier and oldest
			// flush TID at the head+body+empty snap/flock lengths.
			at := 92 + 84 + 4 + 8 + 4 + 4 + 8
			b[at] ^= 1
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-fscrypt-file": func(p map[string]any) {
			f := fixture(p, caps)
			b := segment(f, "front")
			b[len(b)-1] ^= 0xff
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-cap-item": func(p map[string]any) {
			f := fixture(p, "mds-caps-native-cap-release")
			b := segment(f, "front")
			b[4+8] ^= 1 // first item cap_id
			f["front_hex"] = hex.EncodeToString(b)
		},
		"bytes-only-legacy-cas-hash": func(p map[string]any) {
			f := fixture(p, caps)
			b := segment(f, "front")
			b[92+28+24+12] = 1 // cas_hash after su, sc and os
			f["front_hex"] = hex.EncodeToString(b)
		},
	} {
		t.Run(kind, func(t *testing.T) {
			p := clone()
			edit(p)
			for _, name := range mdsCapsEncoderSpec.names {
				f := fixture(p, name)
				rehashMDSCapsFixture(t, f)
				f["front_bytes"] = json.Number(fmt.Sprint(len(segment(f, "front"))))
				f["middle_bytes"] = json.Number(fmt.Sprint(len(segment(f, "middle"))))
			}
			if err := verify(p, mdsCapsEncoderSpec); err == nil {
				t.Fatal("self-consistent foreign capability-family identity/bytes admitted")
			}
		})
	}
	t.Run("middle-bytes-count", func(t *testing.T) {
		p := clone()
		fixture(p, caps)["middle_bytes"] = json.Number("1")
		if err := verify(p, mdsCapsEncoderSpec); err == nil {
			t.Fatal("inconsistent middle byte count admitted")
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
		"missing vector":   func(p map[string]any) { delete(p["fixtures"].(map[string]any), mdsCapsEncoderSpec.names[0]) },
		"extra vector":     func(p map[string]any) { p["fixtures"].(map[string]any)["unreviewed-class"] = map[string]any{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := clone()
			edit(p)
			if err := verify(p, mdsCapsEncoderSpec); err == nil {
				t.Fatal("invalid native capability provenance admitted")
			}
		})
	}
	for _, edit := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mdsCaps = false }, func(s *nativeEncoderSpec) { s.mdsRequests = true },
		func(s *nativeEncoderSpec) { s.mdsSessions = true }, func(s *nativeEncoderSpec) { s.mdsMaps = true },
		func(s *nativeEncoderSpec) { s.command = "encode-arbitrary" }, func(s *nativeEncoderSpec) { s.names = s.names[1:] },
		func(s *nativeEncoderSpec) { s.requests = 1 }, func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
		func(s *nativeEncoderSpec) { s.requestVersion = 8 }, func(s *nativeEncoderSpec) { s.replyVersion = 8 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := mdsCapsEncoderSpec
		edit(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous native capability suite admitted", bad)
		}
	}
	for _, other := range []nativeEncoderSpec{mdsMapEncoderSpec, mdsSessionEncoderSpec, mdsRequestEncoderSpec, snapEncoderSpec, mutationEncoderSpec, backoffEncoderSpec, requestV8EncoderSpec, replyV8EncoderSpec} {
		if err := verify(original, other); err == nil {
			t.Fatal("capability suite admitted as another suite", other.command)
		}
	}
	// Retained map, session and request proofs cannot enter the capability
	// family, and none of them accepts the capability flag as an override.
	for _, retained := range []struct {
		spec    nativeEncoderSpec
		receipt func(*testing.T) (map[string]any, string)
	}{{mdsMapEncoderSpec, mdsMapEncoderReceipt}, {mdsSessionEncoderSpec, mdsSessionEncoderReceipt}, {mdsRequestEncoderSpec, mdsRequestEncoderReceipt}} {
		proof, proofHash := retained.receipt(t)
		raw, err := json.Marshal(proof)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, mdsCapsEncoderSpec); err == nil {
			t.Fatal("retained proof admitted as capability suite", retained.spec.command)
		}
		if _, err = readNativeEncoderProofFor(path, proofHash, retained.spec); err != nil {
			t.Fatal("retained proof no longer admitted by its own suite", retained.spec.command, err)
		}
		flagged := retained.spec
		flagged.mdsCaps = true
		if _, err = readNativeEncoderProofFor(path, proofHash, flagged); err == nil {
			t.Fatal("capability flag admitted on another MDS suite", retained.spec.command)
		}
	}
	// Every capability vector must record its middle segment, even when it
	// is empty; a front/data-only layout is refused.
	stripped := clone()
	for _, name := range mdsCapsEncoderSpec.names {
		f := fixture(stripped, name)
		delete(f, "middle_hex")
		delete(f, "middle_bytes")
	}
	if err := verify(stripped, mdsCapsEncoderSpec); err == nil {
		t.Fatal("capability proof without middle segments admitted")
	}
}

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

// All success references are freshly generated native MClientSession segment
// files. Corrupting their proof receipt tests admission; it does not generate
// a wire oracle in Go.
func mdsSessionEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	base := "../../internal/mds/testdata/"
	raw, err := os.ReadFile(base + "mds-session-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := decodeEncoderReceipt(raw, &p); err != nil {
		t.Fatal(err)
	}
	for _, name := range mdsSessionEncoderSpec.names {
		fixture, ok := p["fixtures"].(map[string]any)[name].(map[string]any)
		if !ok {
			t.Fatal("native session vector missing", name)
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
				t.Fatal("retained native session segment digest differs", name, segment)
			}
			fixture[segment+"_hex"] = encoded
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + mdsSessionEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := os.ReadFile("../../tools/" + mdsSessionEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	driverHash := fmt.Sprintf("%x", sha256.Sum256(driver))
	if p["wrapper_sha256"] != hash || p["build_driver_sha256"] != driverHash || p["launcher_sha256"] != driverHash {
		t.Fatal("retained native session producer differs from source")
	}
	return p, hash
}

func syncMDSSessionHeaderMetadata(t *testing.T, f map[string]any, h []byte) {
	t.Helper()
	metadata := f["native_header"].(map[string]any)
	for field, n := range map[string]uint64{
		"seq": binary.LittleEndian.Uint64(h[:8]), "tid": binary.LittleEndian.Uint64(h[8:16]),
		"type": uint64(binary.LittleEndian.Uint16(h[16:18])), "priority": uint64(binary.LittleEndian.Uint16(h[18:20])),
		"version": uint64(binary.LittleEndian.Uint16(h[20:22])), "ack_seq": binary.LittleEndian.Uint64(h[28:36]),
		"compat": uint64(binary.LittleEndian.Uint16(h[37:39])),
	} {
		metadata[field] = json.Number(fmt.Sprint(n))
	}
}

func TestMDSSessionNativeProofClosedClassesAndCorruptions(t *testing.T) {
	original, hash := mdsSessionEncoderReceipt(t)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "session-proof.log")
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
	if err := verify(original, mdsSessionEncoderSpec); err != nil {
		t.Fatal("fresh native session proof rejected", err)
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
	for _, name := range mdsSessionEncoderSpec.names {
		for _, field := range []string{"front", "header", "data"} {
			t.Run(name+"/hash-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)[field+"_sha256"] = strings.Repeat("0", 64)
				if err := verify(p, mdsSessionEncoderSpec); err == nil {
					t.Fatal("segment hash corruption admitted")
				}
			})
		}
		for _, mutation := range []struct {
			name string
			edit func([]byte)
		}{
			{"map class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 21) }},
			{"OSD class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 43) }},
			{"nonzero TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 77) }},
			{"other session version", func(h []byte) { h[20] ^= 7 ^ 1 }},
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
				if err := verify(p, mdsSessionEncoderSpec); err == nil {
					t.Fatal("foreign session header admitted")
				}
			})
		}
		for _, field := range []string{"message_type", "version", "op", "seq", "stamp_sec", "stamp_nsec", "max_caps", "max_leases", "metadata", "supported_features_hex", "metric_spec_hex", "flags", "cap_auths", "oldest_client_tid", "source_oldest_client_tid"} {
			t.Run(name+"/decoder-"+field, func(t *testing.T) {
				p := clone()
				delete(fixture(p, name)["decoded_session"].(map[string]any), field)
				if err := verify(p, mdsSessionEncoderSpec); err == nil {
					t.Fatal("incomplete native decoder receipt admitted")
				}
			})
		}
	}
	// Each edit keeps segment digests, envelope lengths and (where noted)
	// the decoder receipt self-consistent, so only the closed vector contract
	// or the receipt-to-bytes binding can reject it.
	for kind, edit := range map[string]func(p map[string]any){
		"consistent-foreign-op": func(p map[string]any) {
			f := fixture(p, "mds-session-native-stale")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			binary.LittleEndian.PutUint32(front[:4], 3)
			f["decoded_session"].(map[string]any)["op"] = json.Number("3")
			f["front_hex"] = hex.EncodeToString(front)
		},
		"consistent-transmitted-tid": func(p map[string]any) {
			f := fixture(p, "mds-session-native-request-renewcaps")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			front = binary.LittleEndian.AppendUint64(front, 9)
			f["decoded_session"].(map[string]any)["oldest_client_tid"] = json.Number("9")
			f["native_header"].(map[string]any)["front_len"] = json.Number(fmt.Sprint(len(front)))
			f["front_hex"], f["front_bytes"] = hex.EncodeToString(front), json.Number(fmt.Sprint(len(front)))
		},
		"consistent-foreign-metadata": func(p map[string]any) {
			f := fixture(p, "mds-session-native-reject")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			i := strings.Index(string(front), "blocklisted")
			front[i] = 'B'
			f["decoded_session"].(map[string]any)["metadata"].(map[string]any)["error_string"] = "Blocklisted (blacklisted)"
			f["front_hex"] = hex.EncodeToString(front)
		},
		"consistent-foreign-features": func(p map[string]any) {
			f := fixture(p, "mds-session-native-open")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			front[36] ^= 0x80 // first supported-feature block byte after the 0-entry metadata map
			receipt := f["decoded_session"].(map[string]any)
			receipt["supported_features_hex"] = hex.EncodeToString(front[32:44])
			f["front_hex"] = hex.EncodeToString(front)
		},
		"receipt-only-metadata": func(p map[string]any) {
			fixture(p, "mds-session-native-request-open")["decoded_session"].(map[string]any)["metadata"].(map[string]any)["root"] = "/x"
		},
		"bytes-only-cap-auth-path": func(p map[string]any) {
			f := fixture(p, "mds-session-native-open")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			front[strings.Index(string(front), "volumes/a")] = 'V'
			f["front_hex"] = hex.EncodeToString(front)
		},
		"bytes-only-oldest-tid": func(p map[string]any) {
			f := fixture(p, "mds-session-native-open")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			front[len(front)-1] = 0
			f["front_hex"] = hex.EncodeToString(front)
		},
	} {
		t.Run(kind, func(t *testing.T) {
			p := clone()
			edit(p)
			for _, name := range mdsSessionEncoderSpec.names {
				rehashMDSMapFixture(t, fixture(p, name))
			}
			if err := verify(p, mdsSessionEncoderSpec); err == nil {
				t.Fatal("self-consistent foreign session identity/bytes admitted")
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
		"missing vector":   func(p map[string]any) { delete(p["fixtures"].(map[string]any), mdsSessionEncoderSpec.names[0]) },
		"extra vector":     func(p map[string]any) { p["fixtures"].(map[string]any)["unreviewed-class"] = map[string]any{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := clone()
			edit(p)
			if err := verify(p, mdsSessionEncoderSpec); err == nil {
				t.Fatal("invalid native session provenance admitted")
			}
		})
	}
	for _, edit := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mdsSessions = false }, func(s *nativeEncoderSpec) { s.mdsMaps = true },
		func(s *nativeEncoderSpec) { s.command = "encode-arbitrary" }, func(s *nativeEncoderSpec) { s.names = s.names[1:] },
		func(s *nativeEncoderSpec) { s.requests = 1 }, func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
		func(s *nativeEncoderSpec) { s.requestVersion = 8 }, func(s *nativeEncoderSpec) { s.replyVersion = 8 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := mdsSessionEncoderSpec
		edit(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous native session suite admitted", bad)
		}
	}
	// snapEncoderSpec also has eight vectors, so the count alone cannot reject it.
	for _, other := range []nativeEncoderSpec{mdsMapEncoderSpec, snapEncoderSpec, mutationEncoderSpec, backoffEncoderSpec, requestV8EncoderSpec, replyV8EncoderSpec} {
		if err := verify(original, other); err == nil {
			t.Fatal("session suite admitted as another suite", other.command)
		}
	}
	// A retained map proof cannot enter the session family either.
	mapProof, mapHash := mdsMapEncoderReceipt(t)
	raw, err := json.Marshal(mapProof)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = readNativeEncoderProofFor(path, mapHash, mdsSessionEncoderSpec); err == nil {
		t.Fatal("map proof admitted as session suite")
	}
}

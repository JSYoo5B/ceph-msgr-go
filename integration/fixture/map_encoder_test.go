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

// All success references are freshly generated native segment files. Corrupting
// their proof receipt tests admission; it does not generate a wire oracle in Go.
func mdsMapEncoderReceipt(t *testing.T) (map[string]any, string) {
	t.Helper()
	base := "../../internal/mds/testdata/"
	raw, err := os.ReadFile(base + "mds-map-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	if err := decodeEncoderReceipt(raw, &p); err != nil {
		t.Fatal(err)
	}
	for _, name := range mdsMapEncoderSpec.names {
		fixture, ok := p["fixtures"].(map[string]any)[name].(map[string]any)
		if !ok {
			t.Fatal("native map vector missing", name)
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
				t.Fatal("retained native map segment digest differs", name, segment)
			}
			fixture[segment+"_hex"] = encoded
		}
	}
	wrapper, err := os.ReadFile("../../tools/" + mdsMapEncoderSpec.wrapper)
	if err != nil {
		t.Fatal(err)
	}
	driver, err := os.ReadFile("../../tools/" + mdsMapEncoderSpec.driver)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(wrapper))
	driverHash := fmt.Sprintf("%x", sha256.Sum256(driver))
	if p["wrapper_sha256"] != hash || p["build_driver_sha256"] != driverHash || p["launcher_sha256"] != driverHash {
		t.Fatal("retained native map producer differs from source")
	}
	return p, hash
}

func rehashMDSMapFixture(t *testing.T, f map[string]any) {
	t.Helper()
	for _, segment := range []string{"front", "data", "header"} {
		b, err := hex.DecodeString(f[segment+"_hex"].(string))
		if err != nil {
			t.Fatal(err)
		}
		f[segment+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
}

func syncMDSMapHeaderMetadata(t *testing.T, f map[string]any, h []byte) {
	t.Helper()
	metadata := f["native_header"].(map[string]any)
	for field, n := range map[string]uint64{"seq": binary.LittleEndian.Uint64(h[:8]), "tid": binary.LittleEndian.Uint64(h[8:16]), "type": uint64(binary.LittleEndian.Uint16(h[16:18])), "version": uint64(binary.LittleEndian.Uint16(h[20:22])), "compat": uint64(binary.LittleEndian.Uint16(h[37:39]))} {
		metadata[field] = json.Number(fmt.Sprint(n))
	}
}

func TestMDSMapNativeProofClosedClassesAndCorruptions(t *testing.T) {
	original, hash := mdsMapEncoderReceipt(t)
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "map-proof.log")
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
	if err := verify(original, mdsMapEncoderSpec); err != nil {
		t.Fatal("fresh native map proof rejected", err)
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
	for _, name := range mdsMapEncoderSpec.names {
		for _, field := range []string{"front", "header", "data"} {
			t.Run(name+"/hash-"+field, func(t *testing.T) {
				p := clone()
				fixture(p, name)[field+"_sha256"] = strings.Repeat("0", 64)
				if err := verify(p, mdsMapEncoderSpec); err == nil {
					t.Fatal("segment hash corruption admitted")
				}
			})
		}
		for _, mutation := range []struct {
			name string
			edit func([]byte)
		}{
			{"OSD class", func(h []byte) { binary.LittleEndian.PutUint16(h[16:18], 43) }},
			{"nonzero TID", func(h []byte) { binary.LittleEndian.PutUint64(h[8:16], 77) }},
			{"wrong version", func(h []byte) { binary.LittleEndian.PutUint16(h[20:22], 8) }},
			{"wrong compat", func(h []byte) { binary.LittleEndian.PutUint16(h[37:39], 2) }},
			{"wrong sequence", func(h []byte) { binary.LittleEndian.PutUint64(h[:8], 2) }},
		} {
			t.Run(name+"/"+mutation.name, func(t *testing.T) {
				p := clone()
				f := fixture(p, name)
				h, _ := hex.DecodeString(f["header_hex"].(string))
				mutation.edit(h)
				syncMDSMapHeaderMetadata(t, f, h)
				f["header_hex"] = hex.EncodeToString(h)
				rehashMDSMapFixture(t, f)
				if err := verify(p, mdsMapEncoderSpec); err == nil {
					t.Fatal("foreign map header admitted")
				}
			})
		}
		for _, field := range []string{"message_type", "epoch", "entry_count", "fsid_hex", "encoded_map_sha256"} {
			t.Run(name+"/decoder-"+field, func(t *testing.T) {
				p := clone()
				delete(fixture(p, name)["decoded_map"].(map[string]any), field)
				if err := verify(p, mdsMapEncoderSpec); err == nil {
					t.Fatal("incomplete native decoder receipt admitted")
				}
			})
		}
	}
	for _, kind := range []string{"FSID", "epoch", "map-bytes"} {
		t.Run("consistent-foreign-"+kind, func(t *testing.T) {
			p := clone()
			f := fixture(p, "mds-map-native-mdsmap-populated")
			front, _ := hex.DecodeString(f["front_hex"].(string))
			decoded := f["decoded_map"].(map[string]any)
			switch kind {
			case "FSID":
				front[0] ^= 1
				decoded["fsid_hex"] = hex.EncodeToString(front[:16])
			case "epoch":
				binary.LittleEndian.PutUint32(front[16:20], 24)
				binary.LittleEndian.PutUint32(front[30:34], 24)
				decoded["epoch"] = json.Number("24")
				decoded["encoded_map_sha256"] = fmt.Sprintf("%x", sha256.Sum256(front[24:len(front)-4]))
			case "map-bytes":
				front[len(front)-5] ^= 1 // keep the native decoder digest untouched
			}
			f["front_hex"] = hex.EncodeToString(front)
			rehashMDSMapFixture(t, f)
			if err := verify(p, mdsMapEncoderSpec); err == nil {
				t.Fatal("self-consistent foreign map identity/bytes admitted")
			}
		})
	}

	for name, edit := range map[string]func(map[string]any){
		"source archive":       func(p map[string]any) { p["source_archive_sha256"] = strings.Repeat("0", 64) },
		"source URL":           func(p map[string]any) { p["source_archive_url"] = "https://example.invalid/source" },
		"native decoder":       func(p map[string]any) { delete(p, "native_payload_decoder_verified") },
		"empty library digest": func(p map[string]any) { p["native_library_sha256"] = "" },
		"foreign native library": func(p map[string]any) {
			p["loader_audit"] = "libceph-common.so.2 => /usr/lib64/ceph/libceph-common.so.2"
		},
		"missing vector": func(p map[string]any) { delete(p["fixtures"].(map[string]any), mdsMapEncoderSpec.names[0]) },
		"extra vector":   func(p map[string]any) { p["fixtures"].(map[string]any)["unreviewed-class"] = map[string]any{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := clone()
			edit(p)
			if err := verify(p, mdsMapEncoderSpec); err == nil {
				t.Fatal("invalid native map provenance admitted")
			}
		})
	}
	for _, edit := range []func(*nativeEncoderSpec){
		func(s *nativeEncoderSpec) { s.mdsMaps = false }, func(s *nativeEncoderSpec) { s.command = "encode-arbitrary" },
		func(s *nativeEncoderSpec) { s.requests = 1 }, func(s *nativeEncoderSpec) { s.events = 1 }, func(s *nativeEncoderSpec) { s.controls = true },
		func(s *nativeEncoderSpec) { s.requestVersion = 8 }, func(s *nativeEncoderSpec) { s.replyVersion = 8 }, func(s *nativeEncoderSpec) { s.mixedV8 = true },
	} {
		bad := mdsMapEncoderSpec
		edit(&bad)
		if err := verify(original, bad); err == nil {
			t.Fatal("ambiguous native map suite admitted", bad)
		}
	}
	for _, other := range []nativeEncoderSpec{mutationEncoderSpec, backoffEncoderSpec, requestV8EncoderSpec, replyV8EncoderSpec} {
		if err := verify(original, other); err == nil {
			t.Fatal("map suite admitted as OSD suite", other.command)
		}
	}
}

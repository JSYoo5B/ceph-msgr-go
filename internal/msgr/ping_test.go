package msgr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func pingReplyFrame(raw []byte) MessageData {
	e := wire.Encoder{}
	e.Bytes(raw)
	return MessageData{Type: PingMessage, Version: 1, CompatVersion: 0, Priority: 127, Front: e.Data}
}
func TestPingRequest(t *testing.T) {
	m := Ping()
	if m.Type != 2 || m.Transaction != 0 || m.Version != 1 || m.CompatVersion != 1 || m.Priority != 127 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
		t.Fatal(m)
	}
}
func TestPingReplyRawCopy(t *testing.T) {
	for _, raw := range [][]byte{nil, {}, []byte(`{"pong":{"health":"HEALTH_OK"}}`), {'r', 'a', 'w', 0, 0xff, 0x80, '\n'}} {
		m := pingReplyFrame(raw)
		got, err := PingReply(m, 0)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatal(got, err)
		}
		if len(got) != 0 {
			saved := m.Front[4]
			got[0] ^= 0xff
			if m.Front[4] != saved {
				t.Fatal("result aliases input frame")
			}
		}
	}
}
func TestPingReplyStrictEnvelopeAndLength(t *testing.T) {
	valid := pingReplyFrame([]byte("pong"))
	for _, mutate := range []func(*MessageData){
		func(m *MessageData) { m.Type = MonCommandReplyMessage }, func(m *MessageData) { m.Transaction = 1 },
		func(m *MessageData) { m.Version = 0 }, func(m *MessageData) { m.Version = 2 },
		func(m *MessageData) { m.CompatVersion = 1 }, func(m *MessageData) { m.CompatVersion = 2 },
		func(m *MessageData) { m.Middle = []byte{0} }, func(m *MessageData) { m.Data = []byte{0} },
	} {
		m := valid
		mutate(&m)
		if _, err := PingReply(m, 0); !errors.Is(err, ErrFrame) {
			t.Fatal("non-producer envelope accepted", m, err)
		}
	}
	for _, front := range [][]byte{nil, {0}, {0, 0, 0}, {1, 0, 0, 0}, {4, 0, 0, 0, 'a', 'b', 'c'}} {
		m := valid
		m.Front = front
		if _, err := PingReply(m, 0); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal("truncated Ceph string accepted", front, err)
		}
	}
	m := valid
	m.Front = append(bytes.Clone(m.Front), 0)
	if _, err := PingReply(m, 0); err == nil {
		t.Fatal("trailing byte accepted")
	}
	m = valid
	m.Front = []byte{255, 255, 255, 255}
	if _, err := PingReply(m, 0); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("oversized declared string accepted", err)
	}
}
func TestPingReplyBounds(t *testing.T) {
	m := pingReplyFrame([]byte("pong"))
	exact := uint32(41 + len(m.Front))
	if _, err := PingReply(m, exact); err != nil {
		t.Fatal("exact total-frame budget rejected", err)
	}
	for _, limit := range []uint32{1, 40, 41, exact - 1} {
		if _, err := PingReply(m, limit); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("header-inclusive frame bound ignored", limit, err)
		}
	}
}

func TestPingNativeSourceProducerVectors(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "mon-ping-native-provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	type fixture struct {
		FrontHex  string `json:"front_hex"`
		MiddleHex string `json:"middle_hex"`
		DataHex   string `json:"data_hex"`
		HeaderHex string `json:"header_hex"`
		FrontSHA  string `json:"front_sha256"`
		MiddleSHA string `json:"middle_sha256"`
		DataSHA   string `json:"data_sha256"`
		HeaderSHA string `json:"header_sha256"`
		Header    struct {
			Type    uint16 `json:"type"`
			Version uint16 `json:"version"`
			Compat  uint16 `json:"compat"`
			TID     uint64 `json:"tid"`
		} `json:"native_header"`
		Decoded struct {
			IsReply      bool   `json:"is_reply"`
			RawHex       string `json:"raw_hex"`
			Prepopulated bool   `json:"payload_prepopulated"`
		} `json:"decoded_ping"`
	}
	var proof struct {
		Commit       string             `json:"ceph_commit"`
		Image        string             `json:"image"`
		Architecture string             `json:"build_architecture"`
		ArchiveURL   string             `json:"source_archive_url"`
		ArchiveSHA   string             `json:"source_archive_sha256"`
		ConfigSHA    string             `json:"generated_config_sha256"`
		LibrarySHA   string             `json:"native_library_sha256"`
		BinarySHA    string             `json:"binary_sha256"`
		SourceSHA    map[string]string  `json:"source_files_sha256"`
		WrapperSHA   string             `json:"wrapper_sha256"`
		DriverSHA    string             `json:"build_driver_sha256"`
		Encoder      bool               `json:"native_payload_encoder_verified"`
		Canonical    bool               `json:"canonical_message_encode_verified"`
		NativeHeader bool               `json:"native_message_header_verified"`
		Decoder      bool               `json:"native_payload_decoder_verified"`
		Fixtures     map[string]fixture `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Encoder || !proof.Canonical || !proof.NativeHeader || !proof.Decoder || len(proof.Fixtures) != 4 {
		t.Fatal("native producer proof missing", proof.Commit)
	}
	if proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" || proof.Architecture != "x86_64" || proof.ArchiveURL != "https://codeload.github.com/ceph/ceph/tar.gz/"+proof.Commit || proof.ArchiveSHA != "f2144fc4375532342657dc531e256f04aab51ad35e46beec6d6b940eeccbbc1d" {
		t.Fatal("pinned fresh amd64 producer provenance differs")
	}
	for _, digest := range []string{proof.ConfigSHA, proof.LibrarySHA, proof.BinarySHA} {
		raw, err := hex.DecodeString(digest)
		if err != nil || len(raw) != sha256.Size {
			t.Fatal("missing native artifact digest", digest)
		}
	}
	if len(proof.SourceSHA) != 5 {
		t.Fatal("pinned producer/decoder source digest set differs")
	}
	for _, path := range []string{"src/messages/MPing.h", "src/msg/Message.h", "src/msg/Message.cc", "src/mon/Monitor.cc", "src/mon/MonClient.h"} {
		raw, err := hex.DecodeString(proof.SourceSHA[path])
		if err != nil || len(raw) != sha256.Size {
			t.Fatal("missing pinned producer/decoder source digest", path)
		}
	}
	for _, source := range []struct{ path, hash string }{{"mon_ping_native_encoder.cc", proof.WrapperSHA}, {"mon_ping_native_encoder.py", proof.DriverSHA}} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "tools", source.path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != source.hash {
			t.Fatal("native producer source differs from frozen proof", source.path)
		}
	}
	names := []string{"mon-ping-native-request", "mon-ping-native-reply-json", "mon-ping-native-reply-empty", "mon-ping-native-reply-binary"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			f, ok := proof.Fixtures[name]
			if !ok {
				t.Fatal("missing vector")
			}
			decode := func(value, hash string) []byte {
				t.Helper()
				raw, err := hex.DecodeString(value)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(raw)
				if hex.EncodeToString(sum[:]) != hash {
					t.Fatal("native segment digest differs")
				}
				return raw
			}
			front, middle, payload, header := decode(f.FrontHex, f.FrontSHA), decode(f.MiddleHex, f.MiddleSHA), decode(f.DataHex, f.DataSHA), decode(f.HeaderHex, f.HeaderSHA)
			frame := Frame{Tag: Message, Segments: [][]byte{header, front, middle, payload}}
			m, err := DecodeMessage(frame)
			if err != nil {
				t.Fatal(err)
			}
			if m.Type != 2 || m.Transaction != 0 || m.Version != 1 || m.Type != f.Header.Type || m.Transaction != f.Header.TID || m.Version != f.Header.Version || m.CompatVersion != f.Header.Compat {
				t.Fatal("native envelope mismatch", m)
			}
			if name == names[0] {
				if f.Decoded.IsReply || f.Decoded.Prepopulated || f.Decoded.RawHex != "" {
					t.Fatal("request producer receipt differs")
				}
				want := Ping()
				want.Sequence, want.AckSequence = 1, 3
				for i, segment := range want.Frame().Segments {
					if !bytes.Equal(segment, frame.Segments[i]) {
						t.Fatal("native request segment differs", i)
					}
				}
			} else {
				if !f.Decoded.IsReply || !f.Decoded.Prepopulated || m.CompatVersion != 0 {
					t.Fatal("reply producer incorrectly normalized compat")
				}
				got, err := PingReply(m, 0)
				if err != nil || hex.EncodeToString(got) != f.Decoded.RawHex {
					t.Fatal("fresh native string decode differs", got, err)
				}
			}
		})
	}
}
func FuzzPingReply(f *testing.F) {
	for _, raw := range [][]byte{nil, {0, 0, 0, 0}, {4, 0, 0, 0, 'p', 'o', 'n', 'g'}, {255, 255, 255, 255}} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, front []byte) {
		m := MessageData{Type: 2, Version: 1, CompatVersion: 0, Front: front}
		raw, err := PingReply(m, 65536)
		if err == nil && 41+4+len(raw) > 65536 {
			t.Fatal("frame bound bypass")
		}
	})
}

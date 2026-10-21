package msgr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// These retained bytes come from fresh pinned native message objects, never
// from the Go encoder or a decoded object subsequently used as a producer.
func TestMgrServiceNativeVectors(t *testing.T) {
	type vector struct {
		Type       uint16         `json:"type"`
		Version    uint16         `json:"version"`
		Compat     uint16         `json:"compat"`
		Front      string         `json:"front_hex"`
		Middle     string         `json:"middle_hex"`
		Data       string         `json:"data_hex"`
		Header     string         `json:"header_hex"`
		FrontHash  string         `json:"front_sha256"`
		MiddleHash string         `json:"middle_sha256"`
		DataHash   string         `json:"data_sha256"`
		HeaderHash string         `json:"header_sha256"`
		Decoded    map[string]any `json:"decoded"`
	}
	var proof struct {
		Commit       string            `json:"ceph_commit"`
		Image        string            `json:"image"`
		Architecture string            `json:"build_architecture"`
		Archive      string            `json:"source_archive_sha256"`
		Wrapper      string            `json:"wrapper_sha256"`
		Driver       string            `json:"build_driver_sha256"`
		Config       string            `json:"generated_config_sha256"`
		Library      string            `json:"native_library_sha256"`
		Binary       string            `json:"binary_sha256"`
		Bootstrap    map[string]string `json:"bootstrap_sources_sha256"`
		Sources      map[string]string `json:"source_files_sha256"`
		Canonical    bool              `json:"canonical_message_encode_verified"`
		Decoder      bool              `json:"native_payload_decoder_verified"`
		Loader       bool              `json:"matching_native_loader_verified"`
		Fixtures     map[string]vector `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/mgr-service-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" ||
		proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" ||
		proof.Architecture != "x86_64" || proof.Archive != "f2144fc4375532342657dc531e256f04aab51ad35e46beec6d6b940eeccbbc1d" ||
		!proof.Canonical || !proof.Decoder || !proof.Loader {
		t.Fatal("pinned native provenance missing")
	}
	validDigest := func(value string) {
		t.Helper()
		b, err := hex.DecodeString(value)
		if err != nil || len(b) != sha256.Size {
			t.Fatal("missing native artifact digest", value)
		}
	}
	for _, value := range []string{proof.Config, proof.Library, proof.Binary} {
		validDigest(value)
	}
	if len(proof.Sources) != 9 || len(proof.Bootstrap) != 2 {
		t.Fatal("native source inventory differs")
	}
	for _, path := range []string{"src/messages/MMgrOpen.h", "src/messages/MMgrUpdate.h",
		"src/messages/MMgrReport.h", "src/messages/MMgrConfigure.h", "src/messages/MMgrClose.h",
		"src/mgr/MgrClient.cc", "src/mgr/DaemonServer.cc", "src/msg/Message.cc", "src/msg/async/frames_v2.h"} {
		validDigest(proof.Sources[path])
	}
	producers := map[string]string{"mgr_service_native_encoder.cc": proof.Wrapper, "mgr_service_native_encoder.py": proof.Driver}
	for _, path := range []string{"mon_ping_native_encoder.py", "mon_ping_native_encoder.cc"} {
		producers[path] = proof.Bootstrap[path]
	}
	for path, want := range producers {
		b, err := os.ReadFile(filepath.Join("..", "..", "tools", path))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatal("producer differs from retained proof", path)
		}
	}
	binary := "\x00\xff"
	metadata := map[string]string{"b": "2", "a": "1", "binary": binary}
	status := map[string]string{"state": "up"}
	busy, task := map[string]string{"state": "busy", "binary": binary}, map[string]string{"task": "x"}
	updated := map[string]string{"zone": "b", "binary": binary}
	type shape struct {
		typ, version uint16
		encode       func() (MessageData, error)
	}
	shapes := map[string]shape{
		"open-empty":       {MgrOpenMessage, 3, func() (MessageData, error) { return MgrOpen("svc", "d1", nil, nil) }},
		"open-populated":   {MgrOpenMessage, 3, func() (MessageData, error) { return MgrOpen("svc", "d1", metadata, status) }},
		"update":           {MgrUpdateMessage, 2, func() (MessageData, error) { return MgrUpdate("svc", "d1", updated) }},
		"report-omitted":   {MgrReportMessage, 9, func() (MessageData, error) { return MgrReport("svc", "d1", nil, nil) }},
		"report-empty":     {MgrReportMessage, 9, func() (MessageData, error) { return MgrReport("svc", "d1", map[string]string{}, map[string]string{}) }},
		"report-populated": {MgrReportMessage, 9, func() (MessageData, error) { return MgrReport("svc", "d1", busy, task) }},
		"configure":        {MgrConfigureMessage, 4, nil},
		"close":            {MgrCloseMessage, 1, func() (MessageData, error) { return MgrClose("svc", "d1") }},
	}
	if len(proof.Fixtures) != len(shapes) {
		t.Fatal("native vector set differs")
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			f, ok := proof.Fixtures[name]
			if !ok || f.Type != shape.typ || f.Version != shape.version || f.Compat != 1 {
				t.Fatal("native message class differs")
			}
			decode := func(value, hash string) []byte {
				t.Helper()
				b, err := hex.DecodeString(value)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(b)
				if hex.EncodeToString(sum[:]) != hash {
					t.Fatal("retained native bytes differ from digest")
				}
				return b
			}
			frame := Frame{Tag: Message, Segments: [][]byte{
				decode(f.Header, f.HeaderHash), decode(f.Front, f.FrontHash),
				decode(f.Middle, f.MiddleHash), decode(f.Data, f.DataHash),
			}}
			m, err := DecodeMessage(frame)
			if err != nil || m.Type != shape.typ || m.Version != shape.version || m.CompatVersion != 1 ||
				m.Transaction != 0 || m.Sequence != 1 || m.AckSequence != 3 || m.Priority != 127 ||
				len(m.Middle)+len(m.Data) != 0 {
				t.Fatal("native header decode differs", m, err)
			}
			if shape.encode != nil {
				got, err := shape.encode()
				if err != nil {
					t.Fatal(err)
				}
				got.Sequence, got.AckSequence = 1, 3
				for i, segment := range got.Frame().Segments {
					if !bytes.Equal(segment, frame.Segments[i]) {
						t.Fatalf("Go segment %d differs from native: got %x want %x", i, segment, frame.Segments[i])
					}
				}
			}
			switch name {
			case "configure":
				c, err := DecodeMgrConfigure(m, wire.DefaultLimit)
				if err != nil || c.Version != 4 || c.StatsPeriod != 5 || c.StatsThreshold != 2 || len(c.Extension) != 0 ||
					f.Decoded["period"] != float64(c.StatsPeriod) || f.Decoded["threshold"] != float64(c.StatsThreshold) {
					t.Fatal("native configure fields differ", c, err)
				}
			case "close":
				service, daemon, err := DecodeMgrClose(m, wire.DefaultLimit)
				if err != nil || service != "svc" || daemon != "d1" {
					t.Fatal("native close decode differs", service, daemon, err)
				}
			case "report-omitted", "report-empty", "report-populated":
				for _, field := range []string{"status_hex", "task_hex"} {
					value, exists := f.Decoded[field]
					if !exists || (value == nil) != (name == "report-omitted") {
						t.Fatal("native optional-map presence differs", field)
					}
				}
			}
		})
	}
}

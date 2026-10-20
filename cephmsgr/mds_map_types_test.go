package cephmsgr

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/mds"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func publicNativeMDSFront(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("../internal/mds/testdata/mds-map-native-" + name + ".front.hex")
	if err != nil {
		t.Fatal(err)
	}
	front, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	return front
}

func publicMDSJSONFields(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestPublicFSMapUserPreservesNativeDirectory(t *testing.T) {
	for _, name := range []string{"empty", "multi"} {
		t.Run(name, func(t *testing.T) {
			m, err := mds.DecodeFSMapUser(msgr.MessageData{Type: mds.FSMapUserMessage, Version: 1, CompatVersion: 1, Front: publicNativeMDSFront(t, "fsmap-"+name)}, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := publicFSMapUser(m, 999)
			if got.SourceGeneration != 999 || got.Epoch != m.Epoch || got.LegacyClientFSCID != m.LegacyClientFSCID || len(got.Filesystems) != len(m.Filesystems) {
				t.Fatal("filesystem directory identity or local generation lost", got)
			}
			for i, fs := range m.Filesystems {
				if got.Filesystems[i].ID != fs.ID || got.Filesystems[i].Name != fs.Name {
					t.Fatal("filesystem order, signed ID or binary name lost", got.Filesystems)
				}
			}
			if !bytes.Equal(got.RawFront, m.RawFront) || !bytes.Equal(got.RawMap, m.RawMap) {
				t.Fatal("filesystem raw encodings lost")
			}
		})
	}
}

func TestPublicMDSMapPreservesAllNativeMetadata(t *testing.T) {
	for _, name := range []string{"empty", "populated", "unknown"} {
		t.Run(name, func(t *testing.T) {
			m, err := mds.DecodeMDSMapMessage(msgr.MessageData{Type: mds.MDSMapMessageType, Version: 2, CompatVersion: 1, Front: publicNativeMDSFront(t, "mdsmap-"+name)}, 0)
			if err != nil {
				t.Fatal(err)
			}
			got := publicMDSMap(m, -7, 999)
			if got.FSID != "00112233-4455-6677-8899-aabbccddeeff" || got.FilesystemID != -7 || got.SourceGeneration != 999 || got.Epoch != m.Epoch || got.MapFSName != m.MapFSName {
				t.Fatal("MDS envelope or locally supplied binding/generation lost", got)
			}
			// Compare every decoded field from independently generated native
			// vectors. Raw moves to the envelope, outside public Map metadata.
			want := publicMDSJSONFields(t, m.Map)
			delete(want, "Raw")
			if fields := publicMDSJSONFields(t, got.Map); !reflect.DeepEqual(fields, want) {
				t.Fatalf("public metadata differs from native decoding\ngot: %#v\nwant: %#v", fields, want)
			}
			if !bytes.Equal(got.RawFront, m.RawFront) || !bytes.Equal(got.RawMap, m.Map.Raw) {
				t.Fatal("MDS raw encodings lost")
			}
			if name == "unknown" && (got.Map.Daemons[0].State != 777 || got.Map.Flags != 0x80000032 || got.Map.Daemons[0].Flags != 1<<63|1 || got.Map.RequiredClientFeatures[63] != 128 || got.Map.Compat.Incompatible.Mask&(1<<62) == 0) {
				t.Fatal("unknown state, flags or feature requirements were normalized")
			}
		})
	}
}

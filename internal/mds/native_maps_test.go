package mds

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

func nativeMapBytes(t *testing.T, name, segment string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name + "." + segment + ".hex")
	if err != nil {
		t.Fatal(err)
	}
	data, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func nativeMapMessage(t *testing.T, name string) msgr.MessageData {
	t.Helper()
	header := nativeMapBytes(t, name, "header")
	m, err := msgr.DecodeMessage(msgr.Frame{Tag: msgr.Message, Segments: [][]byte{header, nativeMapBytes(t, name, "front"), nil, nativeMapBytes(t, name, "data")}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Sequence != 1 || m.Transaction != 0 || m.AckSequence != 3 || m.Priority != 127 || len(m.Data) != 0 || !bytes.Equal(header, m.Frame().Segments[0]) {
		t.Fatal("native offline map header lost", m)
	}
	if m.CompatVersion != 1 || (m.Type == FSMapUserMessage && m.Version != 1) || (m.Type == MDSMapMessageType && m.Version != 2) {
		t.Fatal("native canonical current map version/compat lost", m)
	}
	return m
}

func TestDecodeFreshNativeFSMapUserMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		epoch   uint32
		legacy  int32
		entries []Filesystem
	}{{"empty", 0, -1, []Filesystem{}}, {"multi", 17, 1, []Filesystem{{1, "cephfs-a"}, {7, "binary\x00name"}}}} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMapMessage(t, "mds-map-native-fsmap-"+tc.name)
			got, err := DecodeFSMapUser(m, wire.DefaultLimit)
			if err != nil || got.Epoch != tc.epoch || got.LegacyClientFSCID != tc.legacy || !reflect.DeepEqual(got.Filesystems, tc.entries) || !bytes.Equal(got.RawFront, m.Front) || !bytes.Equal(got.RawMap, m.Front[4:]) {
				t.Fatal("native filesystem map differs", got, err)
			}
		})
	}
}

func TestDecodeFreshNativeMDSMapMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		epoch   uint32
		daemons int
	}{{"empty", 0, 0}, {"populated", 23, 2}, {"unknown", 24, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			m := nativeMapMessage(t, "mds-map-native-mdsmap-"+tc.name)
			got, err := DecodeMDSMapMessage(m, wire.DefaultLimit)
			if err != nil {
				t.Fatal(err)
			}
			wantFSID := []byte{0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255}
			if !bytes.Equal(got.FSID[:], wantFSID) || got.Epoch != tc.epoch || got.Map.Epoch != tc.epoch || len(got.Map.Daemons) != tc.daemons || got.MapFSName != "" || !bytes.Equal(got.RawFront, m.Front) {
				t.Fatal("native MDSMap envelope identity lost", got)
			}
			rawOnly, err := DecodeMDSMap(got.Map.Raw, wire.DefaultLimit)
			if err != nil || !reflect.DeepEqual(rawOnly, got.Map) {
				t.Fatal("standalone native map differs", err)
			}
			if tc.name == "empty" {
				if got.Map.Enabled || got.Map.Flags != 18 || got.Map.Name != "cephfs" || got.Map.SessionTimeout != 60 || got.Map.SessionAutoclose != 300 || got.Map.MaxFileSize != 1<<40 || got.Map.MetadataPool != -1 || got.Map.CASPool != -1 || got.Map.MaxXattrSize != 65536 || got.Map.QuiesceLeader != 0 {
					t.Fatal("native empty defaults differ", got.Map)
				}
				// Native decoding inserts INLINE into its in-memory compatibility set.
				// This communication layer keeps the actual empty encoded mask instead.
				if got.Map.Compat.Incompatible.Mask != 0 {
					t.Fatal("native decoder fixup leaked into raw wire metadata")
				}
				return
			}
			md := got.Map
			if md.Flags != 0x80000032 || !md.Enabled || !md.InlineDataEnabled || md.LastFailure != 19 || md.LastFailureOSDEpoch != 31 || md.MaxMDS != 3 || md.TableServer != 1 || md.Root != 0 || md.SessionTimeout != 61 || md.SessionAutoclose != 301 || md.MaxFileSize != 1<<42 || md.MetadataPool != 10 || md.CASPool != -1 || !reflect.DeepEqual(md.DataPools, []int64{11, 12}) || md.MaxXattrSize != 131072 || md.BalanceRankMask != "0-2" || md.Balancer != "balance-policy" || md.StandbyCountWanted != -1 || md.OldMaxMDS != 2 {
				t.Fatal("native map body differs", md)
			}
			if md.Created != (Time{1700000000, 123456789}) || md.Modified != (Time{1700000001, 987654321}) || md.EverAllowedFeatures != 0xb2 || md.ExplicitlyAllowedFeatures != 0xa2 || !reflect.DeepEqual(md.In, []int32{0, 1, 2}) || !reflect.DeepEqual(md.Up, []RankGID{{0, 50}, {1, 51}}) || !reflect.DeepEqual(md.Failed, []int32{2}) || !reflect.DeepEqual(md.Stopped, []int32{3}) || !reflect.DeepEqual(md.Damaged, []int32{4}) {
				t.Fatal("native rank/time metadata differs", md)
			}
			if md.Compat.Incompatible.Mask&(1<<62) == 0 || md.Compat.Incompatible.Names[len(md.Compat.Incompatible.Names)-1] != (NamedFeature{62, "opaque-wire-feature"}) {
				t.Fatal("unknown compatibility bit/name lost")
			}
			wantBytes := 16
			if tc.name == "unknown" {
				wantBytes = 64
			}
			if len(md.RequiredClientFeatures) != wantBytes || md.RequiredClientFeatures[0] != 2 || md.RequiredClientFeatures[8] != 2 || tc.name == "unknown" && md.RequiredClientFeatures[63] != 128 {
				t.Fatal("native required-client feature vector lost", md.RequiredClientFeatures)
			}
			if md.QuiesceLeader != 50 || len(md.QuiesceMembers) != 2 || md.QuiesceMembers[0]+md.QuiesceMembers[1] != 101 {
				t.Fatal("native unordered quiesce membership lost")
			}
			for i, daemon := range md.Daemons {
				state := StateActive
				if i == 1 {
					state = StateReplay
				}
				if tc.name == "unknown" {
					state = 777
				}
				if daemon.GID != uint64(50+i) || daemon.Rank != int32(i) || daemon.Incarnation != int32(7+i) || daemon.State != state || daemon.StateSequence != uint64(150+i) || daemon.Flags != 1<<63|1 || daemon.MDSFeatures != 1<<63|5 || daemon.JoinFSCID != 7 || daemon.StandbyForRank != -1 || daemon.StandbyForName != "" || daemon.StandbyReplay || !reflect.DeepEqual(daemon.ExportTargets, []int32{2, 3}) || len(daemon.Addresses) != 2 || daemon.Addresses[0].Type != 2 || daemon.Addresses[0].Nonce != 42 || daemon.Addresses[0].Endpoint.String() != "192.0.2.10:6800" || daemon.Addresses[1].Nonce != 43 || daemon.Addresses[1].Endpoint.String() != "[2001:db8::10]:6801" {
					t.Fatal("native daemon fields differ", daemon)
				}
			}
		})
	}
}

func TestNativeMapProvenanceMatchesCurrentSourcesAndSegments(t *testing.T) {
	data, err := os.ReadFile("testdata/mds-map-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var proof struct {
		Commit     string                    `json:"ceph_commit"`
		Wrapper    string                    `json:"wrapper_sha256"`
		Driver     string                    `json:"build_driver_sha256"`
		Launcher   string                    `json:"launcher_sha256"`
		Terminated bool                      `json:"builder_termination_completed"`
		Canonical  bool                      `json:"canonical_message_encode_verified"`
		Decoder    bool                      `json:"native_payload_decoder_verified"`
		Fixtures   map[string]map[string]any `json:"fixtures"`
	}
	if err = json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || !proof.Terminated || !proof.Decoder || !proof.Canonical || len(proof.Fixtures) != 5 {
		t.Fatal("native proof lacks pinned source, cleanup or decoder")
	}
	for _, producer := range []struct{ hash, file string }{{proof.Wrapper, "mds_map_native_encoder.cc"}, {proof.Driver, "mds_map_native_encoder.py"}, {proof.Launcher, "mds_map_native_encoder.py"}} {
		raw, err := os.ReadFile("../../tools/" + producer.file)
		if err != nil {
			t.Fatal(err)
		}
		if producer.hash != fmt.Sprintf("%x", sha256.Sum256(raw)) {
			t.Fatal("native map producer hash differs", producer.file)
		}
	}
	for name, fixture := range proof.Fixtures {
		for _, segment := range []string{"header", "front", "data"} {
			raw := nativeMapBytes(t, name, segment)
			if fixture[segment+"_sha256"] != fmt.Sprintf("%x", sha256.Sum256(raw)) {
				t.Fatal("native segment differs", name, segment)
			}
		}
	}
}

package msgr

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type mgrReportVector struct {
	Type    uint16          `json:"type"`
	Version uint16          `json:"version"`
	Compat  uint16          `json:"compat"`
	Kind    string          `json:"kind"`
	Front   string          `json:"front_hex"`
	Middle  string          `json:"middle_hex"`
	Data    string          `json:"data_hex"`
	Header  string          `json:"header_hex"`
	Bytes   string          `json:"bytes_hex"`
	Decoded json.RawMessage `json:"decoded"`
}

// mgrReportVectors loads testdata/mgr-report-native-provenance.json: two
// MMonMgrReports, three PGMapDigests encoded with CEPH_FEATURES_ALL (each
// with its native statfs projection and dump), an encoded ServiceMap and
// two cut digests the native decoder refused, all produced by fresh native
// Ceph v20.2.4 (commit 7f793731f1b39eb4f465e960113d2363c311b964) objects
// through their canonical encoders and checked by separate native decoders.
// Regenerate on a Linux amd64 Docker host with copies of
// tools/mgr_report_native_encoder.{py,cc} and
// tools/mon_ping_native_encoder.{py,cc} in a writable directory mounted at
// /tools and an empty /work:
//
//	docker run --rm -v <tools>:/tools -v <work>:/work \
//	  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
//	  python3 /tools/mgr_report_native_encoder.py --work /work --wrapper /tools/mgr_report_native_encoder.cc
//
// The last JSON line of its output is the provenance file. Headers are an
// offline MessageFrame projection, not a ProtocolV2 capture.
func mgrReportVectors(t *testing.T) map[string]mgrReportVector {
	t.Helper()
	var proof struct {
		Commit       string                     `json:"ceph_commit"`
		Image        string                     `json:"image"`
		Architecture string                     `json:"build_architecture"`
		Wrapper      string                     `json:"wrapper_sha256"`
		Driver       string                     `json:"build_driver_sha256"`
		Bootstrap    map[string]string          `json:"bootstrap_sources_sha256"`
		Canonical    bool                       `json:"canonical_message_encode_verified"`
		Decoder      bool                       `json:"native_payload_decoder_verified"`
		Loader       bool                       `json:"matching_native_loader_verified"`
		Fixtures     map[string]mgrReportVector `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/mgr-report-native-provenance.json")
	if err != nil || json.Unmarshal(raw, &proof) != nil {
		t.Fatal("native provenance", err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" ||
		proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" ||
		proof.Architecture != "x86_64" || !proof.Canonical || !proof.Decoder || !proof.Loader || len(proof.Fixtures) != 8 {
		t.Fatal("pinned native provenance missing")
	}
	producers := map[string]string{"mgr_report_native_encoder.cc": proof.Wrapper, "mgr_report_native_encoder.py": proof.Driver}
	for _, path := range []string{"mon_ping_native_encoder.py", "mon_ping_native_encoder.cc"} {
		producers[path] = proof.Bootstrap[path]
	}
	for path, want := range producers {
		b, err := os.ReadFile(filepath.Join("..", "..", "tools", path))
		if err != nil {
			t.Fatal(err)
		}
		if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != want {
			t.Fatal("producer differs from retained proof", path)
		}
	}
	return proof.Fixtures
}

// nativePopulatedReport is the content the native producer put into
// report-populated, here in an order the encoder must sort.
func nativePopulatedReport(serviceMap, digest []byte) MonMgrReport {
	return MonMgrReport{
		HealthChecks: []HealthCheck{
			{Code: "GOTEST_REPORT_WARN", Severity: HealthWarn, Summary: "gotest warning summary", Detail: []string{"first detail", "second detail ✓"}, Count: 3},
			{Code: "GOTEST_REPORT_ERR", Severity: HealthError, Summary: "gotest error summary"},
		},
		ServiceMap: serviceMap,
		Progress: []ProgressEvent{
			{ID: "event-b", Message: "rebalancing", Progress: 0.25, AddToStatus: true},
			{ID: "event-a", Progress: 1},
		},
		GlobalID: 4242,
		Digest:   digest,
	}
}

// TestMonMgrReportMatchesNativeEncoding encodes the native producer's
// content and compares the front byte for byte, the data segment, the
// envelope and the frame header's type, priority and versions.
func TestMonMgrReportMatchesNativeEncoding(t *testing.T) {
	v := mgrReportVectors(t)
	digestA, digestEmpty := unhex(t, v["digest-a"].Bytes), unhex(t, v["digest-empty"].Bytes)
	for name, content := range map[string]MonMgrReport{
		"report-populated": nativePopulatedReport(unhex(t, v["service-map"].Bytes), digestA),
		"report-plain":     {Digest: digestEmpty},
	} {
		t.Run(name, func(t *testing.T) {
			native := v[name]
			m, err := EncodeMonMgrReport(content, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(m.Front, unhex(t, native.Front)) {
				t.Fatalf("front differs\n go     %x\n native %s", m.Front, native.Front)
			}
			if !bytes.Equal(m.Data, unhex(t, native.Data)) || native.Middle != "" || len(m.Middle) != 0 {
				t.Fatal("data or middle segment differs")
			}
			header := unhex(t, native.Header)
			if len(header) != 41 || m.Type != native.Type || m.Version != native.Version || m.CompatVersion != native.Compat ||
				binary.LittleEndian.Uint16(header[16:]) != m.Type || binary.LittleEndian.Uint16(header[18:]) != m.Priority ||
				binary.LittleEndian.Uint16(header[20:]) != m.Version || binary.LittleEndian.Uint16(header[37:]) != m.CompatVersion {
				t.Fatalf("envelope differs: %+v header %x", m, header)
			}
		})
	}
}

// TestPGMapDigestDecodesNativeEncodings reads the native digests and
// compares their fields with the native decoder's values, the statfs
// projection a MON answers MStatfs with included.
func TestPGMapDigestDecodesNativeEncodings(t *testing.T) {
	v := mgrReportVectors(t)
	for _, name := range []string{"digest-a", "digest-b", "digest-empty"} {
		t.Run(name, func(t *testing.T) {
			var want struct {
				NumPG, NumPGActive, NumPGUnknown, NumOSD int64
				Pools, OSDLastSeq                        int
				PGSumObjects, PGSumBytes                 int64
				OSDSumTotal, OSDSumAvailable             uint64
				StampDelta                               [2]uint32
				Statfs                                   struct{ KB, KBUsed, KBAvail, Objects uint64 }
			}
			var decoded struct {
				NumPG        int64     `json:"num_pg"`
				NumPGActive  int64     `json:"num_pg_active"`
				NumPGUnknown int64     `json:"num_pg_unknown"`
				NumOSD       int64     `json:"num_osd"`
				Pools        int       `json:"pools"`
				OSDLastSeq   int       `json:"osd_last_seq"`
				Objects      int64     `json:"pg_sum_objects"`
				Bytes        int64     `json:"pg_sum_bytes"`
				Total        uint64    `json:"osd_sum_total"`
				Available    uint64    `json:"osd_sum_available"`
				StampDelta   [2]uint32 `json:"stamp_delta"`
				Statfs       struct {
					KB      uint64 `json:"kb"`
					KBUsed  uint64 `json:"kb_used"`
					KBAvail uint64 `json:"kb_avail"`
					Objects uint64 `json:"num_objects"`
				} `json:"statfs"`
			}
			if err := json.Unmarshal(v[name].Decoded, &decoded); err != nil {
				t.Fatal(err)
			}
			want.NumPG, want.NumPGActive, want.NumPGUnknown, want.NumOSD = decoded.NumPG, decoded.NumPGActive, decoded.NumPGUnknown, decoded.NumOSD
			want.Pools, want.OSDLastSeq, want.PGSumObjects, want.PGSumBytes = decoded.Pools, decoded.OSDLastSeq, decoded.Objects, decoded.Bytes
			want.OSDSumTotal, want.OSDSumAvailable, want.StampDelta = decoded.Total, decoded.Available, decoded.StampDelta
			want.Statfs = struct{ KB, KBUsed, KBAvail, Objects uint64 }{decoded.Statfs.KB, decoded.Statfs.KBUsed, decoded.Statfs.KBAvail, decoded.Statfs.Objects}
			g, err := DecodePGMapDigest(unhex(t, v[name].Bytes), 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			statfs := g.OSDSum.Statfs
			got := want
			got.NumPG, got.NumPGActive, got.NumPGUnknown, got.NumOSD = g.NumPG, g.NumPGActive, g.NumPGUnknown, g.NumOSDs
			got.Pools, got.OSDLastSeq, got.PGSumObjects, got.PGSumBytes = len(g.PoolSums), len(g.OSDLastSeq), g.Sum.Sum.NumObjects, g.Sum.Sum.NumBytes
			got.OSDSumTotal, got.OSDSumAvailable, got.StampDelta = statfs.Total, statfs.Available, [2]uint32{g.StampDelta.Seconds, g.StampDelta.Nanoseconds}
			// PGMapDigest::get_statfs without a pool: store_statfs_t kb(),
			// kb_used_raw() (total - available) and kb_avail(), and the PG
			// sum's object count.
			got.Statfs = struct{ KB, KBUsed, KBAvail, Objects uint64 }{statfs.Total >> 10, (statfs.Total - statfs.Available) >> 10, statfs.Available >> 10, uint64(g.Sum.Sum.NumObjects)}
			if got != want || g.StructVersion != 5 {
				t.Fatalf("digest fields differ\n go     %+v\n native %+v", got, want)
			}
			if name == "digest-empty" {
				return
			}
			// Every populated collection was read with its native values.
			if len(g.PGsByState) != 1 || g.PGsByState[0].State != 1<<1|1<<2 || len(g.PGsByOSD) != 1 || g.PGsByOSD[0].Acting != 32 ||
				len(g.PGsByPool) != 1 || g.PGsByPool[0].Key != 9001 || len(g.PoolDeltas) != 1 || g.PoolDeltas[0].Pool != 9001 ||
				len(g.PoolDeltaStamps) != 1 || len(g.AvailableByRule) != 1 || g.AvailableByRule[0].Value < 1<<40 ||
				len(g.PurgedSnaps) != 1 || len(g.PurgedSnaps[0].Intervals) != 1 || g.PurgedSnaps[0].Intervals[0].Start != 3 ||
				len(g.OSDSumByClass) != 1 || g.OSDSumByClass[0].Class != "hdd" || len(g.UnavailablePGs) != 1 ||
				len(g.UnavailablePGs[0].PGs) != 1 || g.UnavailablePGs[0].PGs[0].Pool != 9001 || g.PoolSums[0].Pool != 9001 ||
				g.PoolSums[0].Stat.Up != 3 || g.PoolSums[0].Stat.Acting != 2 || g.OSDSum.UpFrom < 12 {
				t.Fatalf("populated digest collections differ: %+v", g)
			}
		})
	}
}

// TestMgrReportRefusesWhatTheNativeDecoderRefuses checks that the cut
// digests the native decoder refused are refused before a report is built,
// and that the native ServiceMap is accepted.
func TestMgrReportRefusesWhatTheNativeDecoderRefuses(t *testing.T) {
	v := mgrReportVectors(t)
	for _, name := range []string{"digest-truncated", "digest-short-struct"} {
		var receipt struct {
			Refused bool `json:"native_refused"`
		}
		if json.Unmarshal(v[name].Decoded, &receipt) != nil || !receipt.Refused {
			t.Fatal("native refusal receipt", name)
		}
		cut := unhex(t, v[name].Bytes)
		if _, err := DecodePGMapDigest(cut, 1<<20); err == nil {
			t.Fatal("Go read a digest the native decoder refused", name)
		}
		if _, err := EncodeMonMgrReport(MonMgrReport{Digest: cut}, 1<<20); !errors.Is(err, ErrMgrReportContent) {
			t.Fatal("report with a refused digest", name, err)
		}
	}
	m, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: unhex(t, v["service-map"].Bytes)}, 1<<20)
	if err != nil || m.Epoch != 7 || m.Services["gotest"].Daemons["reporter"].GID != 4242 {
		t.Fatal("native service map", m, err)
	}
}

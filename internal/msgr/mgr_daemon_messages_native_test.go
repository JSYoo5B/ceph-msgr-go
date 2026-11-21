package msgr

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type mgrMessageVector struct {
	Type    uint16          `json:"type"`
	Version uint16          `json:"version"`
	Compat  uint16          `json:"compat"`
	TID     uint64          `json:"tid"`
	Front   string          `json:"front_hex"`
	Middle  string          `json:"middle_hex"`
	Data    string          `json:"data_hex"`
	Header  string          `json:"header_hex"`
	Decoded json.RawMessage `json:"decoded"`
}

// mgrMessageVectors loads testdata/mgr-messages-native-provenance.json:
// the MGR daemon messages produced by fresh native Ceph v20.2.4 (commit
// 7f793731f1b39eb4f465e960113d2363c311b964) objects through the canonical
// Message::encode, each checked by a separate native decoder, MPGStats also
// dumped by its native formatter, and two truncated fronts the native
// decoder refused. Regenerate on a Linux amd64 Docker host with copies of
// tools/mgr_messages_native_encoder.{py,cc} and
// tools/mon_ping_native_encoder.{py,cc} in a writable directory mounted at
// /tools and an empty /work:
//
//	docker run --rm -v <tools>:/tools -v <work>:/work --entrypoint python3 \
//	  quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9 \
//	  /tools/mgr_messages_native_encoder.py --work /work --wrapper /tools/mgr_messages_native_encoder.cc
//
// The last JSON line of its output is the provenance file. Headers are an
// offline MessageFrame projection (sequence 1, ACK 3, default priority), not
// a ProtocolV2 capture.
func mgrMessageVectors(t *testing.T) map[string]mgrMessageVector {
	t.Helper()
	var proof struct {
		Commit       string                      `json:"ceph_commit"`
		Image        string                      `json:"image"`
		Architecture string                      `json:"build_architecture"`
		Wrapper      string                      `json:"wrapper_sha256"`
		Driver       string                      `json:"build_driver_sha256"`
		Bootstrap    map[string]string           `json:"bootstrap_sources_sha256"`
		Canonical    bool                        `json:"canonical_message_encode_verified"`
		Decoder      bool                        `json:"native_payload_decoder_verified"`
		Loader       bool                        `json:"matching_native_loader_verified"`
		Fixtures     map[string]mgrMessageVector `json:"fixtures"`
	}
	raw, err := os.ReadFile("testdata/mgr-messages-native-provenance.json")
	if err != nil || json.Unmarshal(raw, &proof) != nil {
		t.Fatal("native provenance", err)
	}
	if proof.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" ||
		proof.Image != "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9" ||
		proof.Architecture != "x86_64" || !proof.Canonical || !proof.Decoder || !proof.Loader || len(proof.Fixtures) != 21 {
		t.Fatal("pinned native provenance missing")
	}
	producers := map[string]string{"mgr_messages_native_encoder.cc": proof.Wrapper, "mgr_messages_native_encoder.py": proof.Driver}
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

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// nativeMgrMessage checks a vector's envelope apart from its front: the
// header's type, version, compat and TID, and an empty middle segment.
func nativeMgrMessage(t *testing.T, vectors map[string]mgrMessageVector, name string) MessageData {
	t.Helper()
	v, ok := vectors[name]
	if !ok {
		t.Fatal("missing native vector", name)
	}
	m, err := DecodeMessage(Frame{Tag: Message, Segments: [][]byte{unhex(t, v.Header)}})
	if err != nil || m.Type != v.Type || m.Version != v.Version || m.CompatVersion != v.Compat || m.Transaction != v.TID || m.Priority != 127 || v.Middle != "" {
		t.Fatal("native envelope", name, m, err)
	}
	m.Front, m.Data = unhex(t, v.Front), unhex(t, v.Data)
	return m
}

func decodedFields(t *testing.T, v mgrMessageVector, out any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(v.Decoded))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		t.Fatal(err)
	}
}

// sameMessage compares a Go-encoded message with a native vector: header
// fields, front and data byte for byte.
func sameMessage(t *testing.T, name string, got, native MessageData) {
	t.Helper()
	if got.Type != native.Type || got.Version != native.Version || got.CompatVersion != native.CompatVersion ||
		got.Transaction != native.Transaction || got.Priority != native.Priority ||
		!bytes.Equal(got.Front, native.Front) || !bytes.Equal(got.Data, native.Data) || len(got.Middle) != 0 {
		t.Fatalf("%s: Go %d %d/%d tid %d front %x data %x; native %d %d/%d tid %d front %x data %x", name,
			got.Type, got.Version, got.CompatVersion, got.Transaction, got.Front, got.Data,
			native.Type, native.Version, native.CompatVersion, native.Transaction, native.Front, native.Data)
	}
}

func allBytes() []byte {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// TestMgrDaemonMessagesDecodeNativeEncodings decodes every native vector of
// a message a MGR receives and checks each field against the values the
// native producer set (tools/mgr_messages_native_encoder.cc), and refuses
// the fronts the native decoder refused.
func TestMgrDaemonMessagesDecodeNativeEncodings(t *testing.T) {
	vectors := mgrMessageVectors(t)
	binary := "\x00\xff"
	open, err := DecodeMgrDaemonOpen(nativeMgrMessage(t, vectors, "open-osd"), 1<<20)
	if err != nil || open.DaemonName != "0" || open.ServiceName != "" || open.ServiceDaemon || open.Metadata != nil ||
		string(open.Config) != "config\x00bl" || string(open.ConfigDefaults) != binary {
		t.Fatal("native OSD open", open, err)
	}
	open, err = DecodeMgrDaemonOpen(nativeMgrMessage(t, vectors, "open-service"), 1<<20)
	if err != nil || open.DaemonName != "d1" || open.ServiceName != "ceph-msgr" || !open.ServiceDaemon ||
		!reflect.DeepEqual(open.Metadata, map[string]string{"a": "1", "b": binary}) || !reflect.DeepEqual(open.Status, map[string]string{"s": "ok"}) ||
		len(open.Config) != 0 || len(open.ConfigDefaults) != 0 {
		t.Fatal("native service open", open, err)
	}
	update, err := DecodeMgrDaemonUpdate(nativeMgrMessage(t, vectors, "update-metadata"), 1<<20)
	if err != nil || update.DaemonName != "a" || update.ServiceName != "mon" || !update.NeedMetadataUpdate ||
		!reflect.DeepEqual(update.Metadata, map[string]string{"ceph_version_short": "20.2.4", "k": binary}) ||
		!reflect.DeepEqual(update.Status, map[string]string{"x": "y"}) {
		t.Fatal("native update", update, err)
	}
	update, err = DecodeMgrDaemonUpdate(nativeMgrMessage(t, vectors, "update-plain"), 1<<20)
	if err != nil || update.NeedMetadataUpdate || update.Metadata != nil || update.Status != nil {
		t.Fatal("native plain update", update, err)
	}

	var schema MgrCounterSchema
	for _, name := range []string{"report-declare", "report-second", "report-populated"} {
		var fields struct {
			Values [][]any `json:"values"`
		}
		decodedFields(t, vectors[name], &fields)
		r, err := DecodeMgrDaemonReport(nativeMgrMessage(t, vectors, name), 1<<20)
		if err != nil {
			t.Fatal(name, err)
		}
		if name == "report-populated" {
			schema = MgrCounterSchema{}
		}
		values, err := schema.Apply(r)
		if err != nil || len(values) != len(fields.Values) {
			t.Fatal(name, "packed counters", values, err)
		}
		for i, want := range fields.Values {
			got := []any{values[i].Path, values[i].Value}
			if values[i].Type&MgrPerfCounterLongRunAvg != 0 {
				got = append(got, values[i].AvgCount, values[i].AvgCount2)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatal(name, "counter", i, got, want)
			}
		}
	}
	first, _ := DecodeMgrDaemonReport(nativeMgrMessage(t, vectors, "report-declare"), 1<<20)
	if !reflect.DeepEqual(first.Declare, []MgrPerfCounterType{
		{Version: 3, Path: "b", Description: "desc b", Nick: "nb", Type: 0x0a, Priority: 8, Unit: 1},
		{Version: 3, Path: "c", Description: "desc c", Nick: "nc", Type: 0x05, Priority: 5, Unit: 0},
	}) || first.Status != nil || first.TaskStatus != nil || first.MetricReport != nil {
		t.Fatalf("native declarations %+v", first)
	}
	var populatedFields struct {
		Query  string `json:"query_hex"`
		Report string `json:"report_hex"`
	}
	decodedFields(t, vectors["report-populated"], &populatedFields)
	full, err := DecodeMgrDaemonReport(nativeMgrMessage(t, vectors, "report-populated"), 1<<20)
	query, report := unhex(t, populatedFields.Query), unhex(t, populatedFields.Report)
	if err != nil || full.DaemonName != "d1" || full.ServiceName != "ceph-msgr" || !reflect.DeepEqual(full.Undeclare, []string{"gone"}) ||
		!reflect.DeepEqual(full.Status, map[string]string{"s": "ok", "t": binary}) || full.TaskStatus == nil || len(full.TaskStatus) != 0 ||
		!reflect.DeepEqual(full.HealthMetrics, []MgrHealthMetric{{0, 7}, {1, 2<<32 | 1}}) || string(full.Config) != "cfg" ||
		len(full.OSDPerfMetricReports) != 1 || !bytes.Equal(full.OSDPerfMetricReports[0].Key, query) || !bytes.Equal(full.OSDPerfMetricReports[0].Value, report) ||
		full.MetricReport == nil || full.MetricReport.Type != MgrMetricOSD || !bytes.Equal(full.MetricReport.Payload, envelope(1, 1, le32(1), query, report)) {
		t.Fatalf("native populated report %+v %v", full, err)
	}
	mds, err := DecodeMgrDaemonReport(nativeMgrMessage(t, vectors, "report-mds-metric"), 1<<20)
	if err != nil || mds.MetricReport == nil || mds.MetricReport.Type != MgrMetricMDS || mds.MetricReport.Payload[0] != 1 {
		t.Fatal("native MDS metric report", mds, err)
	}

	for name, typ := range map[string]uint16{"report-truncated": MgrReportMessage, "pgstats-truncated": PGStatsMessage} {
		front := unhex(t, vectors[name].Front)
		m := MessageData{Type: typ, Version: 9, CompatVersion: 1, Front: front}
		var err error
		if typ == PGStatsMessage {
			m.Version = 2
			_, err = DecodePGStats(m, 1<<20)
		} else {
			_, err = DecodeMgrDaemonReport(m, 1<<20)
		}
		if err == nil {
			t.Error(name, "accepted a front the native decoder refused")
		}
	}

	fsid := [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	command := []string{`{"prefix":"gotest echo"}`, "x"}
	for name, want := range map[string]struct {
		fsid [16]byte
		data []byte
	}{"mgr-command": {[16]byte{}, allBytes()}, "tell-command": {fsid, []byte(binary)}} {
		m := nativeMgrMessage(t, vectors, name)
		c, err := DecodeMgrCommandRequest(m, 1<<20)
		if err != nil || c.FSID != want.fsid || !reflect.DeepEqual(c.Command, command) || !bytes.Equal(m.Data, want.data) {
			t.Fatal("native", name, c, err)
		}
	}
}

// TestMgrDaemonMessagesEncodeNativeBytes compares what the MGR sends with
// native encodings of the same values: the command replies of both
// families, MMgrConfigure (plain, with an OSD perf-metric query, with an MDS
// metric configuration and without it for a peer lacking SERVER_PACIFIC)
// and the close echo.
func TestMgrDaemonMessagesEncodeNativeBytes(t *testing.T) {
	vectors := mgrMessageVectors(t)
	for name, c := range map[string]struct {
		request uint16
		tid     uint64
		result  int32
		status  string
		output  []byte
	}{
		"mgr-command-reply":  {MgrCommandMessage, 7, -22, "bad input", allBytes()},
		"tell-command-reply": {TellCommandMessage, 9, -61, "gone", []byte("\x00\xff")},
	} {
		got, err := CommandReplyFor(c.request, c.tid, c.result, c.status, c.output, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		sameMessage(t, name, got, nativeMgrMessage(t, vectors, name))
	}
	plain, err := EncodeMgrConfigure(MgrConfigureData{StatsPeriod: 5, StatsThreshold: 2}, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sameMessage(t, "configure-plain", plain, nativeMgrMessage(t, vectors, "configure-plain"))
	var queryFields struct {
		Query  string `json:"query_hex"`
		Limits string `json:"limits_hex"`
	}
	decodedFields(t, vectors["configure-osd-queries"], &queryFields)
	queries, err := EncodeMgrConfigure(MgrConfigureData{StatsPeriod: 1, OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: unhex(t, queryFields.Query), Value: unhex(t, queryFields.Limits)}}}, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sameMessage(t, "configure-osd-queries", queries, nativeMgrMessage(t, vectors, "configure-osd-queries"))
	for name, features := range map[string]uint64{"configure-mds": serverPacificMask, "configure-mds-nopacific": serverPacificMask &^ (1 << 20)} {
		var fields struct {
			Payload string `json:"payload_hex"`
		}
		decodedFields(t, vectors[name], &fields)
		m, err := EncodeMgrConfigure(MgrConfigureData{StatsPeriod: 3, MetricConfig: &MgrMetricPayload{Type: MgrMetricMDS, Payload: unhex(t, fields.Payload)}}, features, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		sameMessage(t, name, m, nativeMgrMessage(t, vectors, name))
	}
	native := nativeMgrMessage(t, vectors, "close")
	c, err := DecodeMgrDaemonClose(native, 1<<20)
	if err != nil || c.DaemonName != "d1" || c.ServiceName != "ceph-msgr" {
		t.Fatal("native close", c, err)
	}
	echo, err := MgrCloseEcho(c)
	if err != nil {
		t.Fatal(err)
	}
	sameMessage(t, "close echo", echo, native)
}

// nativePGStatsDump is the part of MPGStats::dump_stats this test compares.
type nativePGStatsDump struct {
	OSD struct {
		HBPeers           []int32 `json:"hb_peers"`
		KB                int64   `json:"kb"`
		KBUsed            int64   `json:"kb_used"`
		KBAvail           int64   `json:"kb_avail"`
		KBUsedData        int64   `json:"kb_used_data"`
		SnapTrimQueueLen  int32   `json:"snap_trim_queue_len"`
		NumSnapTrimming   int32   `json:"num_snap_trimming"`
		NumShardsRepaired uint64  `json:"num_shards_repaired"`
		UpFrom            uint32  `json:"up_from"`
		Seq               uint64  `json:"seq"`
		NumPGs            uint32  `json:"num_pgs"`
		NumOSDs           uint32  `json:"num_osds"`
		Histogram         struct {
			Values []int32 `json:"histogram"`
		} `json:"op_queue_age_hist"`
		Perf struct {
			Commit uint64 `json:"commit_latency_ns"`
			Apply  uint64 `json:"apply_latency_ns"`
		} `json:"perf_stat"`
		Statfs map[string]int64 `json:"statfs"`
		Alerts []string         `json:"alerts"`
		Ping   []struct {
			OSD        int32 `json:"osd"`
			Interfaces []struct {
				Interface string             `json:"interface"`
				Average   map[string]float64 `json:"average"`
				Last      float64            `json:"last"`
			} `json:"interfaces"`
		} `json:"network_ping_times"`
	} `json:"osd_stat"`
	PGs []struct {
		Pool          uint64           `json:"pool"`
		Seed          uint32           `json:"seed"`
		Version       string           `json:"version"`
		ReportedSeq   uint64           `json:"reported_seq"`
		ReportedEpoch uint32           `json:"reported_epoch"`
		State         string           `json:"state"`
		Up            []int32          `json:"up"`
		Acting        []int32          `json:"acting"`
		UpPrimary     int32            `json:"up_primary"`
		ActingPrimary int32            `json:"acting_primary"`
		Created       uint32           `json:"created"`
		MappingEpoch  uint32           `json:"mapping_epoch"`
		Parent        string           `json:"parent"`
		LogSize       int64            `json:"log_size"`
		LogDupsSize   int64            `json:"log_dups_size"`
		BlockedBy     []int32          `json:"blocked_by"`
		Snaptrimq     uint32           `json:"snaptrimq_len"`
		ScrubDuration float64          `json:"scrub_duration"`
		LastScrub     string           `json:"last_scrub"`
		LastChange    string           `json:"last_change"`
		Sum           map[string]int64 `json:"stat_sum"`
		Purged        []struct {
			Start  string `json:"start"`
			Length string `json:"length"`
		} `json:"purged_snaps"`
		Locations []struct {
			Shards  string `json:"shards"`
			Objects int32  `json:"objects"`
		} `json:"object_location_counts"`
		Manifest bool `json:"manifest_stats_invalid"`
		Stats    bool `json:"stats_invalid"`
	} `json:"pg_stat"`
	Pools []map[string]int64 `json:"pool_stat"`
}

// TestPGStatsDecodeNativeEncodings decodes the two native MPGStats vectors
// (encoded with CEPH_FEATURES_ALL and with the MGR acceptor's negotiated
// mask; under both a Tentacle OSD encodes objectstore_perf_stat_t version 1
// in milliseconds, as CEPH_FEATURES_ALL lacks OS_PERF_STAT_NS) and compares the decoded fields with the native formatter's own dump of
// the natively decoded message.
func TestPGStatsDecodeNativeEncodings(t *testing.T) {
	vectors := mgrMessageVectors(t)
	sumFields := map[string]func(ObjectStatSum) int64{
		"num_bytes": func(s ObjectStatSum) int64 { return s.NumBytes }, "num_objects": func(s ObjectStatSum) int64 { return s.NumObjects },
		"num_object_clones": func(s ObjectStatSum) int64 { return s.NumObjectClones }, "num_object_copies": func(s ObjectStatSum) int64 { return s.NumObjectCopies },
		"num_objects_missing_on_primary": func(s ObjectStatSum) int64 { return s.NumObjectsMissingOnPrimary },
		"num_objects_degraded":           func(s ObjectStatSum) int64 { return s.NumObjectsDegraded }, "num_objects_unfound": func(s ObjectStatSum) int64 { return s.NumObjectsUnfound },
		"num_read": func(s ObjectStatSum) int64 { return s.NumRead }, "num_read_kb": func(s ObjectStatSum) int64 { return s.NumReadKB },
		"num_write": func(s ObjectStatSum) int64 { return s.NumWrite }, "num_write_kb": func(s ObjectStatSum) int64 { return s.NumWriteKB },
		"num_scrub_errors": func(s ObjectStatSum) int64 { return s.NumScrubErrors }, "num_objects_recovered": func(s ObjectStatSum) int64 { return s.NumObjectsRecovered },
		"num_bytes_recovered": func(s ObjectStatSum) int64 { return s.NumBytesRecovered }, "num_keys_recovered": func(s ObjectStatSum) int64 { return s.NumKeysRecovered },
		"num_shallow_scrub_errors": func(s ObjectStatSum) int64 { return s.NumShallowScrubErrors }, "num_deep_scrub_errors": func(s ObjectStatSum) int64 { return s.NumDeepScrubErrors },
		"num_objects_dirty": func(s ObjectStatSum) int64 { return s.NumObjectsDirty }, "num_whiteouts": func(s ObjectStatSum) int64 { return s.NumWhiteouts },
		"num_objects_omap": func(s ObjectStatSum) int64 { return s.NumObjectsOmap }, "num_objects_hit_set_archive": func(s ObjectStatSum) int64 { return s.NumObjectsHitSetArchive },
		"num_objects_misplaced": func(s ObjectStatSum) int64 { return s.NumObjectsMisplaced }, "num_bytes_hit_set_archive": func(s ObjectStatSum) int64 { return s.NumBytesHitSetArchive },
		"num_flush": func(s ObjectStatSum) int64 { return s.NumFlush }, "num_flush_kb": func(s ObjectStatSum) int64 { return s.NumFlushKB },
		"num_evict": func(s ObjectStatSum) int64 { return s.NumEvict }, "num_evict_kb": func(s ObjectStatSum) int64 { return s.NumEvictKB },
		"num_promote": func(s ObjectStatSum) int64 { return s.NumPromote }, "num_flush_mode_high": func(s ObjectStatSum) int64 { return int64(s.NumFlushModeHigh) },
		"num_flush_mode_low": func(s ObjectStatSum) int64 { return int64(s.NumFlushModeLow) }, "num_evict_mode_some": func(s ObjectStatSum) int64 { return int64(s.NumEvictModeSome) },
		"num_evict_mode_full": func(s ObjectStatSum) int64 { return int64(s.NumEvictModeFull) }, "num_objects_pinned": func(s ObjectStatSum) int64 { return s.NumObjectsPinned },
		"num_objects_missing": func(s ObjectStatSum) int64 { return s.NumObjectsMissing }, "num_legacy_snapsets": func(s ObjectStatSum) int64 { return s.NumLegacySnapsets },
		"num_large_omap_objects": func(s ObjectStatSum) int64 { return s.NumLargeOmapObjects }, "num_objects_manifest": func(s ObjectStatSum) int64 { return s.NumObjectsManifest },
		"num_omap_bytes": func(s ObjectStatSum) int64 { return s.NumOmapBytes }, "num_omap_keys": func(s ObjectStatSum) int64 { return s.NumOmapKeys },
		"num_objects_repaired": func(s ObjectStatSum) int64 { return s.NumObjectsRepaired },
	}
	stamp := func(u UTime) string { return fmt.Sprintf("%d.%06d", u.Seconds, u.Nanoseconds/1000) }
	for name, perfVersion := range map[string]uint8{"pgstats": 1, "pgstats-acceptor": 1} {
		var fields struct {
			Epoch uint32            `json:"epoch"`
			Dump  nativePGStatsDump `json:"dump"`
		}
		if err := json.Unmarshal(vectors[name].Decoded, &fields); err != nil {
			t.Fatal(err)
		}
		s, err := DecodePGStats(nativeMgrMessage(t, vectors, name), 1<<20)
		if err != nil {
			t.Fatal(name, err)
		}
		n, o := fields.Dump, s.OSD
		if s.Epoch != fields.Epoch || s.FSID != [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff} ||
			o.StructVersion != 14 || o.PerfStatVersion != perfVersion || !reflect.DeepEqual(o.HBPeers, n.OSD.HBPeers) || len(o.NumHBOut) != 0 ||
			o.KB != n.OSD.KB || o.KBUsed != n.OSD.KBUsed || o.KBAvail != n.OSD.KBAvail || o.KBUsedData != n.OSD.KBUsedData ||
			o.SnapTrimQueueLen != n.OSD.SnapTrimQueueLen || o.NumSnapTrimming != n.OSD.NumSnapTrimming ||
			o.NumShardsRepaired != n.OSD.NumShardsRepaired || o.UpFrom != n.OSD.UpFrom || o.Seq != n.OSD.Seq || o.NumPGs != n.OSD.NumPGs ||
			o.NumOSDs != n.OSD.NumOSDs || !reflect.DeepEqual(o.OpQueueAgeHist, n.OSD.Histogram.Values) ||
			o.CommitLatencyNS != n.OSD.Perf.Commit || o.ApplyLatencyNS != n.OSD.Perf.Apply ||
			int64(o.Statfs.Total) != n.OSD.Statfs["total"] || int64(o.Statfs.Available) != n.OSD.Statfs["available"] ||
			o.Statfs.Allocated != n.OSD.Statfs["allocated"] || o.Statfs.InternalMetadata != n.OSD.Statfs["internal_metadata"] ||
			len(o.Alerts) != 1 || !strings.Contains(n.OSD.Alerts[0], "some alert:details") || o.Alerts[0].Alerts["some alert"] != "details" {
			t.Fatalf("%s: OSD statistics %+v, native %+v", name, o, n.OSD)
		}
		if len(o.PingTimes) != 1 || len(n.OSD.Ping) != 1 || o.PingTimes[0].OSD != n.OSD.Ping[0].OSD ||
			float64(o.PingTimes[0].BackPing[2])/1000 != n.OSD.Ping[0].Interfaces[0].Average["15min"] ||
			float64(o.PingTimes[0].FrontLast)/1000 != n.OSD.Ping[0].Interfaces[1].Last {
			t.Fatalf("%s: ping times %+v, native %+v", name, o.PingTimes, n.OSD.Ping)
		}
		if len(s.PGs) != len(n.PGs) || len(s.PGs) != 2 {
			t.Fatal(name, "PG count", len(s.PGs))
		}
		for i, e := range s.PGs {
			p, g := n.PGs[i], e.Stat
			if e.PG.Pool != p.Pool || e.PG.Seed != p.Seed || fmt.Sprintf("%d'%d", g.Version.Epoch, g.Version.Version) != p.Version ||
				g.ReportedSeq != p.ReportedSeq || g.ReportedEpoch != p.ReportedEpoch || g.State != 1<<1|1<<2|1<<35 || !strings.HasPrefix(p.State, "active+clean") ||
				!reflect.DeepEqual(g.Up, p.Up) || !reflect.DeepEqual(g.Acting, p.Acting) || g.UpPrimary != p.UpPrimary || g.ActingPrimary != p.ActingPrimary ||
				g.Created != p.Created || g.MappingEpoch != p.MappingEpoch || fmt.Sprintf("%d.%x", g.Parent.Pool, g.Parent.Seed) != p.Parent ||
				g.LogSize != p.LogSize || g.LogDupsSize != p.LogDupsSize || !reflect.DeepEqual(g.BlockedBy, p.BlockedBy) ||
				g.SnapTrimQueueLen != p.Snaptrimq || g.ScrubDuration != p.ScrubDuration || math.IsNaN(g.SnapTrimDuration) ||
				fmt.Sprintf("%d'%d", g.LastScrub.Epoch, g.LastScrub.Version) != p.LastScrub || stamp(g.LastChange) != p.LastChange ||
				len(g.PurgedSnaps) != 1 || fmt.Sprint(g.PurgedSnaps[0].Start) != p.Purged[0].Start || fmt.Sprint(g.PurgedSnaps[0].Length) != p.Purged[0].Length ||
				len(g.ObjectLocationCounts) != 1 || g.ObjectLocationCounts[0].Count != p.Locations[0].Objects ||
				fmt.Sprintf("%d(%d)", g.ObjectLocationCounts[0].Shards[0].OSD, g.ObjectLocationCounts[0].Shards[0].Shard) != p.Locations[0].Shards ||
				g.ManifestStatsInvalid != p.Manifest || g.StatsInvalid != p.Stats || g.StructVersion != 30 ||
				g.ScrubOSDToRespond != 33 || g.ScrubOrdinalOfRequestedReplica != 34 || g.ScrubNumToReserve != 35 || !g.ScrubDeep || g.ScrubPeriodic {
				t.Fatalf("%s: PG %d %+v, native %+v", name, i, g, p)
			}
			if len(p.Sum) != len(sumFields) {
				t.Fatal(name, "native stat_sum fields", len(p.Sum))
			}
			for field, get := range sumFields {
				if got, want := get(g.Sum), p.Sum[field]; got != want {
					t.Fatalf("%s: PG %d %s = %d, native %d", name, i, field, got, want)
				}
			}
		}
		if len(s.PoolStats) != 1 || s.PoolStats[0].Pool != n.Pools[0]["poolid"] || s.PoolStats[0].Statfs.DataStored != n.Pools[0]["data_stored"] ||
			int64(s.PoolStats[0].Statfs.Available) != n.Pools[0]["available"] {
			t.Fatal(name, "pool statistics", s.PoolStats, n.Pools)
		}
	}
}

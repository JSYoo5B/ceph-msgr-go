package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// The query and limits are native-produced bytes, not a second product codec.
// This test-only projection admits exactly that CLIENT_ID/OPS/BYTES query and
// at most ten groups. Product callers retain the complete opaque envelopes.
func mgrPerfQuery(t *testing.T) cephmsgr.MgrOpaqueEntry {
	t.Helper()
	decode := func(s string) []byte {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	return cephmsgr.MgrOpaqueEntry{
		Key:   decode("0101210000000101010b00000000060000005e282e2a2924020101010000000001010100000003"),
		Value: decode("0100000001010f000000010101000000030a00000000000000"),
	}
}

type mgrPerfCounts struct{ Ops, Bytes uint64 }

func mgrPerfProjection(payload, query []byte, descriptors []uint8) (map[string]mgrPerfCounts, error) {
	d := wire.NewDecoder(payload)
	v, body := d.Struct(1)
	if v != 1 {
		return nil, fmt.Errorf("OSD payload version %d", v)
	}
	groups := map[string]mgrPerfCounts{}
	n := body.Count(12, 1)
	for range n {
		if !bytes.Equal(body.Raw(uint32(len(query))), query) {
			return nil, fmt.Errorf("different query")
		}
		version, report := body.Struct(1)
		if version != 1 || report.U8() != uint8(len(descriptors)) {
			return nil, fmt.Errorf("report version/descriptors")
		}
		for _, typ := range descriptors {
			version, desc := report.Struct(1)
			if version != 1 || desc.U8() != typ || desc.Done() != nil {
				return nil, fmt.Errorf("counter descriptor")
			}
		}
		for range report.Count(16, 10) {
			if report.U32() != 1 || report.U32() != 1 {
				return nil, fmt.Errorf("client key dimensions")
			}
			key := report.String()
			packed := wire.NewDecoder(report.Bytes())
			value := mgrPerfCounts{Ops: packed.U64()}
			if len(descriptors) == 2 {
				value.Bytes = packed.U64()
			}
			if _, exists := groups[key]; exists || len(key) > 128 {
				return nil, fmt.Errorf("duplicate/oversized client key")
			}
			if err := packed.Done(); err != nil {
				return nil, err
			}
			groups[key] = value
		}
		if err := report.Done(); err != nil {
			return nil, err
		}
	}
	if err := body.Done(); err != nil {
		return nil, err
	}
	if err := d.Done(); err != nil {
		return nil, err
	}
	return groups, nil
}

// A separate native handle gives this phase a fresh global ID. Counts are
// interval deltas, so accumulate reports instead of treating the last as total.
func mgrPerfLive(t *testing.T, ctx context.Context, peer *cephmsgr.MgrPeer, variant bool) {
	t.Helper()
	query := mgrPerfQuery(t)
	c := cephmsgr.MgrConfigure{StatsPeriod: 1, OSDPerfMetricQueries: []cephmsgr.MgrOpaqueEntry{query}}
	if variant {
		var body, envelope wire.Encoder
		body.U32(1)
		body.Raw(query.Key)
		body.Raw(query.Value)
		envelope.Struct(1, 1, body.Data)
		c.OSDPerfMetricQueries = nil
		c.MetricConfig = &cephmsgr.MgrMetricPayload{Type: 0, Payload: envelope.Data}
	}
	if err := peer.Configure(ctx, c); err != nil {
		t.Fatal(err)
	}
	next := func() *cephmsgr.MgrPeerReport {
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for {
			m, err := peer.Next(call)
			if err != nil {
				t.Fatal(err)
			}
			if m.Report != nil {
				return m.Report
			}
		}
	}
	projection := func(r *cephmsgr.MgrPeerReport) map[string]mgrPerfCounts {
		if r.MetricReport == nil || r.MetricReport.Type != 0 {
			t.Fatal("missing stock OSD metric variant")
		}
		values, err := mgrPerfProjection(r.MetricReport.Payload, query.Key, []uint8{0, 3})
		if err != nil {
			t.Fatal(err)
		}
		return values
	}
	// Wait for the exact query envelope, not merely a Configure write receipt.
	installed := false
	for i := 0; i < 5; i++ {
		r := next()
		projection(r)
		if bytes.Contains(r.MetricReport.Payload, query.Key) {
			installed = true
			break
		}
	}
	if !installed {
		t.Fatal("stock OSD did not report the configured query")
	}
	var native struct {
		Workload struct {
			GlobalID  uint64 `json:"global_id"`
			Namespace string `json:"namespace"`
			Objects   []struct {
				Object      string `json:"object"`
				Size        int    `json:"size"`
				WriteReturn int    `json:"write_return"`
				ReadReturn  int    `json:"read_return"`
				SHA256      string `json:"sha256"`
			} `json:"objects"`
			Ops, Bytes uint64
		} `json:"workload"`
	}
	call, cancel := context.WithTimeout(ctx, 60*time.Second)
	nativePoolResult(t, call, "mgrperf", map[string]any{"slot": 1}, &native)
	cancel()
	if native.Workload.GlobalID == 0 || native.Workload.Ops != 6 || native.Workload.Bytes != 10758 || len(native.Workload.Objects) != 3 {
		t.Fatalf("unexpected native workload %+v", native)
	}
	for i, row := range native.Workload.Objects {
		size := []int{257, 1025, 4097}[i]
		data := make([]byte, size)
		for j := range data {
			data[j] = byte((j + i) % 251)
		}
		if row.Object != fmt.Sprintf("object-%d", i) || row.Size != size || row.WriteReturn != 0 || row.ReadReturn != size || row.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) {
			t.Fatalf("native readback %d: %+v", i, row)
		}
	}
	key := fmt.Sprintf("client.%d", native.Workload.GlobalID)
	total := mgrPerfCounts{}
	var rawReports [][]byte
	for i := 0; i < 10; i++ {
		r := next()
		values := projection(r)
		rawReports = append(rawReports, r.MetricReport.Payload)
		value := values[key]
		total.Ops += value.Ops
		total.Bytes += value.Bytes
		if total.Ops > native.Workload.Ops || total.Bytes > native.Workload.Bytes {
			t.Fatalf("native workload exceeded: %+v", total)
		}
		if total.Ops == native.Workload.Ops && total.Bytes == native.Workload.Bytes {
			break
		}
	}
	if total.Ops != native.Workload.Ops || total.Bytes != native.Workload.Bytes {
		t.Fatalf("metric totals %+v differ from native %+v", total, native)
	}
	// Empty legacy queries mean no update in stock MgrClient; an explicit empty
	// OSD variant is needed to withdraw. Observe it on two consecutive reports.
	var empty wire.Encoder
	empty.Struct(1, 1, []byte{0, 0, 0, 0})
	if err := peer.Configure(ctx, cephmsgr.MgrConfigure{StatsPeriod: 1, MetricConfig: &cephmsgr.MgrMetricPayload{Type: 0, Payload: empty.Data}}); err != nil {
		t.Fatal(err)
	}
	cleared := 0
	for i := 0; i < 5 && cleared < 2; i++ {
		r := next()
		projection(r)
		if bytes.Equal(r.MetricReport.Payload, empty.Data) {
			cleared++
		} else {
			cleared = 0
		}
	}
	if cleared != 2 {
		t.Fatal("query withdrawal was not observed")
	}
	receipt := map[string]any{"variant_config": variant, "generation": peer.Identity().Generation, "native": native, "query": query, "reports": rawReports, "totals": total, "withdrawal_reports": cleared}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("mgr-perf-%t.json", variant)
	if err := os.WriteFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), name), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("stock OSD metric report: variant configure=%t, generation=%d, native %s, OPS=%d BYTES=%d; explicit withdrawal observed twice", variant, peer.Identity().Generation, key, total.Ops, total.Bytes)
}

func TestMgrPerfMetricProjection(t *testing.T) {
	// Calibrate the projection against the independently native-produced report
	// (one OPS descriptor, client.4242=17), then refuse every truncated prefix.
	raw, err := os.ReadFile("../internal/msgr/testdata/mgr-messages-native-provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Fixtures map[string]struct {
			Decoded map[string]json.RawMessage `json:"decoded"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	decode := func(key string) []byte {
		var s string
		if err := json.Unmarshal(p.Fixtures["report-populated"].Decoded[key], &s); err != nil {
			t.Fatal(err)
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	queryEntry := mgrPerfQuery(t)
	for field, want := range map[string][]byte{"query_hex": queryEntry.Key, "limits_hex": queryEntry.Value} {
		var value string
		if err := json.Unmarshal(p.Fixtures["configure-osd-queries"].Decoded[field], &value); err != nil {
			t.Fatal(err)
		}
		if value != hex.EncodeToString(want) {
			t.Fatal("query differs from native provenance", field)
		}
	}
	query := queryEntry.Key
	var body, envelope wire.Encoder
	body.U32(1)
	body.Raw(query)
	body.Raw(decode("report_hex"))
	envelope.Struct(1, 1, body.Data)
	values, err := mgrPerfProjection(envelope.Data, query, []uint8{0})
	if err != nil || len(values) != 1 || values["client.4242"].Ops != 17 {
		t.Fatalf("native report projection: %+v %v", values, err)
	}
	for i := 0; i < len(envelope.Data); i++ {
		if _, err := mgrPerfProjection(envelope.Data[:i], query, []uint8{0}); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	if _, err := mgrPerfProjection(append(envelope.Data, 0), query, []uint8{0}); err == nil {
		t.Fatal("accepted trailing byte")
	}
	if _, err := mgrPerfProjection(envelope.Data, query, []uint8{0, 3}); err == nil {
		t.Fatal("accepted different descriptors")
	}
	wrong := bytes.Clone(query)
	wrong[len(wrong)-1] ^= 1
	if _, err := mgrPerfProjection(envelope.Data, wrong, []uint8{0}); err == nil {
		t.Fatal("accepted different query")
	}
}

func TestMgrPerfMetricProjectionBounds(t *testing.T) {
	query := mgrPerfQuery(t).Key
	makePayload := func(groups uint32, keys []string, packedBytes int) []byte {
		var report, body, envelope wire.Encoder
		report.U8(2)
		report.Struct(1, 1, []byte{0})
		report.Struct(1, 1, []byte{3})
		report.U32(groups)
		for _, key := range keys {
			report.U32(1)
			report.U32(1)
			report.String(key)
			report.Bytes(make([]byte, packedBytes))
		}
		body.U32(1)
		body.Raw(query)
		body.Struct(1, 1, report.Data)
		envelope.Struct(1, 1, body.Data)
		return envelope.Data
	}
	for name, payload := range map[string][]byte{
		"duplicate":     makePayload(2, []string{"client.1", "client.1"}, 16),
		"excess-groups": makePayload(11, nil, 16),
		"oversized-key": makePayload(1, []string{string(bytes.Repeat([]byte{'a'}, 129))}, 16),
		"short-counter": makePayload(1, []string{"client.1"}, 8),
		"long-counter":  makePayload(1, []string{"client.1"}, 24),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := mgrPerfProjection(payload, query, []uint8{0, 3}); err == nil {
				t.Fatal("accepted invalid report")
			}
		})
	}
}

package msgr

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// allocatedPerCall is the heap bytes f allocates per call, averaged over
// ten calls.
func allocatedPerCall(f func()) uint64 {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 10 {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / 10
}

// TestMonMgrReportFrameSizeIsExact: the early bound is the encoded frame's
// size, so it neither admits an oversized report nor refuses one that fits.
func TestMonMgrReportFrameSizeIsExact(t *testing.T) {
	v := mgrReportVectors(t)
	digest := unhex(t, v["digest-a"].Bytes)
	for name, r := range map[string]MonMgrReport{
		"populated": nativePopulatedReport(unhex(t, v["service-map"].Bytes), digest),
		"plain":     {Digest: unhex(t, v["digest-empty"].Bytes)},
		"details":   {Digest: digest, HealthChecks: []HealthCheck{{Code: "A", Detail: []string{"", "x", strings.Repeat("y", 300)}}}},
	} {
		m, err := EncodeMonMgrReport(r, 1<<20)
		if err != nil {
			t.Fatal(name, err)
		}
		if got, want := MonMgrReportFrameSize(r), uint64(len(m.Front)+len(m.Data)+41); got != want {
			t.Errorf("%s: bound %d, frame %d", name, got, want)
		}
		if _, err := EncodeMonMgrReport(r, uint32(MonMgrReportFrameSize(r))); err != nil {
			t.Errorf("%s: a report of exactly the limit was refused: %v", name, err)
		}
		if _, err := EncodeMonMgrReport(r, uint32(MonMgrReportFrameSize(r))-1); !errors.Is(err, wire.ErrLimit) {
			t.Errorf("%s: a report one byte over the limit: %v", name, err)
		}
	}
}

// TestOversizedReportIsRefusedBeforeCopying: an 8 MiB health summary,
// detail line or progress message under a 4 KiB limit is refused as
// wire.ErrLimit without copying it. Encoding first and checking after
// allocated about three times the payload per call.
func TestOversizedReportIsRefusedBeforeCopying(t *testing.T) {
	v := mgrReportVectors(t)
	digest := unhex(t, v["digest-empty"].Bytes)
	big := strings.Repeat("x", 8<<20)
	for name, r := range map[string]MonMgrReport{
		"summary":  {Digest: digest, HealthChecks: []HealthCheck{{Code: "A", Summary: big}}},
		"detail":   {Digest: digest, HealthChecks: []HealthCheck{{Code: "A", Detail: []string{big}}}},
		"progress": {Digest: digest, Progress: []ProgressEvent{{ID: "p", Message: big}}},
		"entries":  {Digest: digest, HealthChecks: make([]HealthCheck, 1<<16)},
	} {
		var err error
		bytes := allocatedPerCall(func() { _, err = EncodeMonMgrReport(r, 4096) })
		if !errors.Is(err, wire.ErrLimit) {
			t.Fatal(name, err)
		}
		if bytes > 16<<10 {
			t.Errorf("%s: refusing allocated %d bytes per call", name, bytes)
		}
	}
}

// TestOversizedConfigureIsRefusedBeforeCopying: OSD perf-metric queries and
// a metric configuration beyond the limit are refused before their bytes
// are appended to a front.
func TestOversizedConfigureIsRefusedBeforeCopying(t *testing.T) {
	big := make([]byte, 8<<20)
	for name, c := range map[string]MgrConfigureData{
		"query":  {OSDPerfMetricQueries: []MgrOpaqueEntry{{Key: big}}},
		"config": {MetricConfig: &MgrMetricPayload{Type: MgrMetricOSD, Payload: big}},
	} {
		var err error
		bytes := allocatedPerCall(func() { _, err = EncodeMgrConfigure(c, 0, 4096) })
		if !errors.Is(err, wire.ErrLimit) {
			t.Fatal(name, err)
		}
		if bytes > 16<<10 {
			t.Errorf("%s: refusing allocated %d bytes per call", name, bytes)
		}
	}
	// An MDS configuration a peer without SERVER_PACIFIC gets as absent
	// does not count against the front.
	if _, err := EncodeMgrConfigure(MgrConfigureData{MetricConfig: &MgrMetricPayload{Type: MgrMetricMDS, Payload: big}}, 0, 4096); err != nil {
		t.Fatal("absent MDS configuration", err)
	}
}

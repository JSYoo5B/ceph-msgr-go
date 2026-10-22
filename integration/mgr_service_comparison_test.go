package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type mgrServiceComparison struct {
	GID            uint64                       `json:"gid"`
	StartEpoch     uint32                       `json:"start_epoch"`
	DumpEpoch      uint32                       `json:"dump_epoch"`
	Metadata       map[string]string            `json:"metadata"`
	Statuses       map[string]map[string]string `json:"statuses"`
	Removed        bool                         `json:"removed"`
	AbsenceSeconds float64                      `json:"absence_seconds"`
}

func testMgrServiceNativeComparison(t *testing.T, ctx context.Context, c *cephmsgr.Client) {
	t.Helper()
	receipt := struct {
		Go           mgrServiceComparison `json:"go"`
		Native       mgrServiceComparison `json:"native"`
		NilPreserved bool                 `json:"nil_preserved"`
		FieldsEqual  bool                 `json:"fields_equal"`
	}{Go: mgrServiceComparison{Statuses: make(map[string]map[string]string)}}
	defer func() {
		data, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "mgr-service-comparison.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve service comparison evidence", err)
		}
	}()
	const service = "ceph-msgr-compare"
	metadata := map[string]string{"fixture": "same-fields", "zone": "rack-λ"}
	statuses := map[string]map[string]string{
		"initial":     {"state": "registered", "registration_only": "yes"},
		"replacement": {"state": "busy"},
		"empty":       {},
	}
	svc, err := c.OpenMgrService(ctx, cephmsgr.MgrServiceOptions{Service: service, Daemon: "go", Metadata: metadata, Status: statuses["initial"], DisableBeacon: true})
	if err != nil {
		t.Fatal("open comparison service", err)
	}
	closed := false
	defer func() {
		if !closed {
			stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			svc.Close(stop)
		}
	}()
	lastBeacon := ""
	observe := func(wanted map[string]string, after string) {
		t.Helper()
		observed := nativePool(t, ctx, "servicewatch", map[string]any{
			"service": service, "daemon": "go", "expect": "present", "wait_seconds": 20,
			"metadata": metadata, "status": wanted, "exact_status": true, "after_beacon": after,
		})
		var entry struct {
			GID        uint64            `json:"gid"`
			StartEpoch uint32            `json:"start_epoch"`
			Metadata   map[string]string `json:"metadata"`
		}
		var status struct {
			Status     map[string]string `json:"status"`
			LastBeacon string            `json:"last_beacon"`
		}
		if !observed.ServiceMatched || json.Unmarshal(observed.ServiceEntry, &entry) != nil ||
			json.Unmarshal(observed.ServiceStatusEntry, &status) != nil || !reflect.DeepEqual(status.Status, wanted) ||
			!reflect.DeepEqual(entry.Metadata, metadata) || entry.GID != c.Snapshot().GlobalID ||
			entry.StartEpoch == 0 || entry.StartEpoch > observed.ServiceDumpEpoch || status.LastBeacon == "" || after != "" && status.LastBeacon <= after {
			t.Fatal("native observation of Go comparison service did not match")
		}
		if receipt.Go.GID != 0 && (receipt.Go.GID != entry.GID || receipt.Go.StartEpoch != entry.StartEpoch) {
			t.Fatal("Go comparison registration identity changed")
		}
		receipt.Go.GID, receipt.Go.StartEpoch, receipt.Go.DumpEpoch = entry.GID, entry.StartEpoch, observed.ServiceDumpEpoch
		receipt.Go.Metadata = entry.Metadata
		lastBeacon = status.LastBeacon
	}
	for _, phase := range []string{"initial", "replacement", "empty"} {
		if phase != "initial" {
			if err := svc.Report(ctx, statuses[phase], nil); err != nil {
				t.Fatal("comparison report", phase, err)
			}
		}
		observe(statuses[phase], "")
		receipt.Go.Statuses[phase] = statuses[phase]
		if phase == "replacement" {
			if err := svc.Report(ctx, nil, nil); err != nil {
				t.Fatal("omitted comparison report", err)
			}
			// With automatic beacons disabled, a strictly newer native beacon
			// establishes receipt of this omitted-status report, rather than
			// merely sampling the old state before the report arrives.
			observe(statuses[phase], lastBeacon)
			receipt.NilPreserved = true
		}
	}
	closed = true
	if err := svc.Close(ctx); err != nil {
		t.Fatal("close Go comparison service", err)
	}
	absence := serviceWatchInput(service, "go", "absent", nil, nil, nil)
	absence.WaitSeconds, absence.StableSeconds = 30, 3
	gone := nativePool(t, ctx, "servicewatch", absence)
	receipt.Go.Removed, receipt.Go.AbsenceSeconds = gone.ServiceMatched, gone.ServiceStableSeconds
	if !receipt.Go.Removed {
		t.Fatal("Go comparison service did not disappear")
	}
	native := nativePool(t, ctx, "servicecompare", map[string]any{
		"service": service, "daemon": "native", "metadata": metadata, "statuses": statuses,
	}).ServiceComparison
	if native == nil || native.GID == 0 || native.GID == receipt.Go.GID || native.StartEpoch == 0 ||
		native.StartEpoch > native.DumpEpoch || !native.Removed || native.AbsenceSeconds < 3 {
		t.Fatal("native comparison identity or removal missing")
	}
	receipt.Native = *native
	receipt.FieldsEqual = reflect.DeepEqual(receipt.Go.Metadata, native.Metadata) && reflect.DeepEqual(receipt.Go.Statuses, native.Statuses)
	if !receipt.FieldsEqual {
		t.Fatal("Go and native registration fields differ")
	}
	t.Logf("Go/native service fields match; GIDs %d/%d, stable removal %.1f/%.1fs", receipt.Go.GID, native.GID, receipt.Go.AbsenceSeconds, native.AbsenceSeconds)
}

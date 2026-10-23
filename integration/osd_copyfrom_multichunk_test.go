package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

const multiChunkLimit = 32 << 10

type copyStagingObservation struct {
	Collection string   `json:"collection"`
	Objects    int      `json:"objects"`
	Max        int      `json:"max"`
	Complete   bool     `json:"complete"`
	Temp       []string `json:"temp"`
}

type copyHistoryOp struct {
	Description string `json:"description"`
	InitiatedAt string `json:"initiated_at"`
}

// The fixture retains every description; this receipt keeps only copy-gets.
type copyGetHistory struct {
	Size     int             `json:"history_size"`
	Ops      int             `json:"ops"`
	CopyGets []copyHistoryOp `json:"copy_gets"`
}

// The pinned process_copy_chunk creates a temporary object while the cursor
// is incomplete and submits its removal after the error callback. A lowered
// owned chunk limit forces that branch with data the bounded native observer
// can still compare. Receiving EIO is not a cleanup barrier, so cleanup is
// observed by a bounded poll of a complete native PG enumeration. This does
// not establish removal timing, destination loss or automatic replay.
func TestCephOSDCopyFromMultiChunkLateErrorIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_COPYFROM_MULTICHUNK_ERROR") != "1" {
		t.Skip("requires the fixture-selected exclusive multi-chunk copy fault marker")
	}
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("multi-chunk copy error requires its exclusive single-BlueStore fixture")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	inventory := [][]byte{[]byte("a"), []byte("z"), []byte("stale")}
	data := make([]byte, 2*multiChunkLimit)
	for i := range data {
		data[i] = byte(i*7 + i>>8 + 3)
	}
	attrs := []nativeXattrEntry{{Name: []byte("source-attr"), Value: []byte{0, 255}}}
	model := map[string][]byte{"a": []byte("source-map"), "z": {}}
	receipt := struct {
		SourceBefore, SourceAfter           omapWriteState
		DestinationBefore, DestinationAfter omapWriteState
		Option                              json.RawMessage `json:"option"`
		Pool                                int64           `json:"pool"`
		Primary                             int32           `json:"primary"`
		PGID                                string          `json:"pgid"`
		PG, Hash, RouteEpoch                uint32
		HistoryBefore, HistoryAfter         copyGetHistory
		SourceCopyGets                      []string                 `json:"source_copy_gets"`
		StagingBefore                       copyStagingObservation   `json:"staging_before"`
		StagingPolls                        []copyStagingObservation `json:"staging_polls"`
		TemporaryObserved                   []string                 `json:"temporary_observed"`
		CleanupObservedMillis               int64                    `json:"cleanup_observed_after_reply_ms"`
		Reply                               cephmsgr.OSDReply        `json:"go_reply"`
		Readback                            cephmsgr.OSDReply        `json:"read_only_reply"`
		ServerError                         bool                     `json:"signed_server_eio"`
		Restored                            bool                     `json:"option_restored"`
		Removed                             bool                     `json:"objects_removed"`
	}{Pool: base.Pool}
	prepared, optionAttempted := false, false
	control := func(call context.Context, args ...string) error {
		return testcluster.RequestControl(call, dir, "limit-osd-copyfrom-error", "osd-copyfrom-error", args...)
	}
	t.Cleanup(func() {
		defer func() {
			if raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json")); err == nil {
				receipt.Option = raw
			}
			raw, err := json.Marshal(receipt)
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-copyfrom-multichunk-results.json"), raw)
			}
			if err != nil {
				t.Error("multi-chunk copy error receipt", err)
			}
		}()
		// Restoration and object cleanup have fresh, separate bounded
		// contexts; neither replays the copy.
		if optionAttempted && !receipt.Restored {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			err := control(cleanup, "restore")
			stop()
			if err != nil {
				t.Error("multi-chunk copy option restoration", err)
			} else {
				receipt.Restored = true
			}
		}
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
			stop()
			receipt.Removed = result.Cleanup.Removed && len(result.Cleanup.Objects) == 2
			if !receipt.Removed {
				t.Error("multi-chunk copy owned-object cleanup differs", result.Cleanup)
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{
		Seed: &omapWriteSeed{Baseline: data, Attributes: attrs, Entries: omapWriteEntries(model)}, Reserve: 1,
	})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("native multi-chunk source preparation differs")
	}
	src, dst := owned.Objects[0], owned.Objects[1]
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: inventory}).State
	}
	place := func(name string) osdFixtureOracle {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if route.ID != base.ID || route.Name != name || route.Pool != base.Pool {
			t.Fatal("exclusive multi-chunk placement differs")
		}
		return route
	}
	sourceRoute, target := place(src), place(dst)
	o := mutationConnection(t, c, ctx, target)
	defer o.Close()
	setup := osdObjectRequest(sourceRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte{0, 'm', 255}}})
	setup.ModificationTime = cephmsgr.OSDTime{Seconds: 1700012000, Nanoseconds: 123456789}
	if _, err := o.Request(ctx, setup); err != nil {
		t.Fatal("source header setup", err)
	}
	prefill := osdObjectRequest(target,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("stale")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "stale"}, Data: []byte("old")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "stale", Value: []byte("old")}}}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("old")}})
	prefill.ModificationTime = cephmsgr.OSDTime{Seconds: 1700012001, Nanoseconds: 987654321}
	if _, err := o.Request(ctx, prefill); err != nil {
		t.Fatal("multi-chunk populated destination setup", err)
	}
	receipt.SourceBefore, receipt.DestinationBefore = observe(src), observe(dst)
	source, before := receipt.SourceBefore, receipt.DestinationBefore
	if source.Code != 0 || !bytes.Equal(source.Data, data) || source.Version == 0 || !equalOmapModel(omapWriteInventory(t, source), model) ||
		before.Code != 0 || !bytes.Equal(before.Data, []byte("stale")) || before.Version == 0 || xattrInventory(before.nativeXattrSnapshot)["stale"] != "old" ||
		!equalOmapModel(omapWriteInventory(t, before), map[string][]byte{"stale": []byte("old")}) {
		t.Fatal("independent multi-chunk preconditions differ")
	}
	optionAttempted = true
	if err := control(ctx, "apply"); err != nil {
		t.Fatal("apply and independently read back chunk limit and copy-error injector", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json"))
	if err != nil {
		t.Fatal(err)
	}
	receipt.Option = raw
	// The admin-socket receipt is independent of Messenger command output.
	var setting struct {
		OSD          int32  `json:"osd"`
		MaxChunk     uint64 `json:"max_chunk"`
		ChunkApplied uint64 `json:"chunk_applied"`
		Before       string `json:"effective_before"`
		Applied      string `json:"effective_applied"`
	}
	if err = json.Unmarshal(raw, &setting); err != nil {
		t.Fatal(err)
	}
	if setting.OSD != target.ID || setting.Before != "false" || setting.Applied != "true" || setting.ChunkApplied != multiChunkLimit ||
		setting.MaxChunk <= uint64(len(data)) || uint64(len(data)) <= setting.ChunkApplied {
		t.Fatal("effective injector/multi-chunk boundary differs", setting)
	}
	temporaryPool := "#" + strconv.FormatInt(-2-base.Pool, 10) + ":"
	staging := func() copyStagingObservation {
		if err := control(ctx, "staging", target.PGID); err != nil {
			t.Fatal("native staging enumeration", err)
		}
		var list copyStagingObservation
		raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-staging-list.json"))
		if err == nil {
			err = json.Unmarshal(raw, &list)
		}
		if err != nil || list.Collection != target.PGID+"_head" || !list.Complete || list.Objects < 2 {
			t.Fatal("complete native PG enumeration differs", err, list)
		}
		return list
	}
	history := func() copyGetHistory {
		if err := control(ctx, "history"); err != nil {
			t.Fatal("native op history", err)
		}
		var h copyGetHistory
		raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-staging-history.json"))
		if err == nil {
			err = json.Unmarshal(raw, &h)
		}
		// OpHistory trims its shortest ops first only when full. A history
		// below its enlarged capacity has not discarded fast copy-gets.
		if err != nil || h.Size != 1024 || h.Ops >= h.Size {
			t.Fatal("native op history receipt is not complete", err, h.Size, h.Ops)
		}
		return h
	}
	sourceGets := func(h copyGetHistory) []string {
		var gets []string
		for _, op := range h.CopyGets {
			if strings.Contains(op.Description, ":::"+src+":head ") {
				gets = append(gets, op.Description)
			}
		}
		return gets
	}
	receipt.StagingBefore = staging()
	receipt.HistoryBefore = history()
	if len(receipt.StagingBefore.Temp) != 0 || len(sourceGets(receipt.HistoryBefore)) != 0 {
		t.Fatal("native staging or copy history existed before the Go copy", receipt.StagingBefore, receipt.HistoryBefore)
	}
	r := osdObjectRequest(target,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src, SourceVersion: source.Version}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "multi-chunk-suffix"}, Data: []byte("must-not-appear")})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700012002}
	receipt.Primary, receipt.PGID, receipt.PG, receipt.Hash, receipt.RouteEpoch = target.ID, target.PGID, target.PG, target.Hash, target.Epoch
	reply, err := o.Request(ctx, r)
	replied := time.Now()
	receipt.Reply = reply
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	receipt.ServerError = errors.As(err, &server) && server.Code == -5 && !errors.As(err, &unknown)
	if !receipt.ServerError || reply.Code != -5 || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction ||
		reply.GlobalID == 0 || reply.Submission.GlobalID != reply.GlobalID || reply.Object != dst || reply.MapEpoch < target.Epoch || reply.RetryAttempt != 0 ||
		len(reply.RawFront) == 0 || len(reply.RawData) != 0 || len(reply.Operations) != 2 {
		t.Fatal("multi-chunk signed error receipt differs", err, reply)
	}
	copyResult, suffix := reply.Operations[0], reply.Operations[1]
	if copyResult.Code != cephmsgr.OSDCopyFrom2 || copyResult.Result != -115 || copyResult.CopyFromVersion != source.Version || copyResult.CopyFromSnapshot != cephmsgr.OSDHeadSnapshot ||
		suffix.Code != cephmsgr.OSDSetXattr || suffix.Result != 0 || copyResult.PayloadLength != 0 || suffix.PayloadLength != 0 || len(copyResult.Data)+len(suffix.Data) != 0 {
		t.Fatal("multi-chunk original union/per-op representation differs", copyResult, suffix)
	}
	// One copy-get per chunk names the source; at least two with the lowered
	// max is independent native evidence that the incomplete-cursor branch ran.
	// Completed ops reach the history asynchronously, so poll briefly.
	for attempt := 0; ; attempt++ {
		receipt.HistoryAfter = history()
		receipt.SourceCopyGets = sourceGets(receipt.HistoryAfter)
		if len(receipt.SourceCopyGets) >= 2 || attempt >= 20 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, description := range receipt.SourceCopyGets {
		if !strings.Contains(description, "[copy-get max "+strconv.Itoa(multiChunkLimit)+"]") {
			t.Fatal("source copy-get did not use the lowered chunk limit", description)
		}
	}
	if len(receipt.SourceCopyGets) < 2 {
		t.Fatal("native history lacks multi-chunk copy-get evidence", receipt.HistoryAfter)
	}
	// Temporary removal is submitted after the error callback. Poll a
	// complete enumeration; an observed partial object must carry the
	// pinned generate_temp_object identity for this PG and primary role.
	deadline := time.Now().Add(30 * time.Second)
	for {
		list := staging()
		receipt.StagingPolls = append(receipt.StagingPolls, list)
		for _, entry := range list.Temp {
			if !strings.HasPrefix(entry, temporaryPool) || !strings.Contains(entry, ":::temp_"+target.PGID+"_0_") {
				t.Fatal("unexpected native temporary object", entry)
			}
			if !strings.Contains(strings.Join(receipt.TemporaryObserved, "\n"), entry) {
				receipt.TemporaryObserved = append(receipt.TemporaryObserved, entry)
			}
		}
		if len(list.Temp) == 0 {
			receipt.CleanupObservedMillis = time.Since(replied).Milliseconds()
			break
		}
		if time.Now().After(deadline) || len(receipt.StagingPolls) >= 120 {
			t.Fatal("temporary copy object remained after bounded poll", receipt.TemporaryObserved)
		}
		time.Sleep(250 * time.Millisecond)
	}
	receipt.DestinationAfter, receipt.SourceAfter = observe(dst), observe(src)
	// A zero untouched suffix result does not establish suffix execution.
	if !reflect.DeepEqual(receipt.DestinationAfter, before) || !reflect.DeepEqual(receipt.SourceAfter, source) {
		t.Fatal("multi-chunk copy error changed native source or populated destination")
	}
	if _, present := xattrInventory(receipt.DestinationAfter.nativeXattrSnapshot)["multi-chunk-suffix"]; present {
		t.Fatal("suffix marker appeared after asynchronous error")
	}
	restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
	err = control(restore, "restore")
	stop()
	if err != nil {
		t.Fatal("restore and independently read back chunk limit and copy-error option", err)
	}
	receipt.Restored = true
	if raw, err = os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json")); err != nil {
		t.Fatal(err)
	}
	receipt.Option = raw
	var restored struct {
		MaxChunk      uint64 `json:"max_chunk"`
		ChunkRestored uint64 `json:"chunk_restored"`
		Restored      string `json:"effective_restored"`
	}
	if err = json.Unmarshal(raw, &restored); err != nil || restored.Restored != "false" || restored.ChunkRestored != restored.MaxChunk || restored.MaxChunk != setting.MaxChunk {
		t.Fatal("restored effective settings differ", err, restored)
	}
	read, err := o.Request(ctx, osdObjectRequest(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 32}))
	receipt.Readback = read
	if err != nil || read.Transaction != reply.Transaction+1 || read.UserVersion != before.Version || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, before.Data) {
		t.Fatal("same connection read-only request after restored settings differs", err, read)
	}
}

// A broad native suite must skip this fault before reading any fixture input.
// The empty private directory deliberately cannot satisfy osdFixture.
func TestCopyFromMultiChunkLateErrorRequiresExclusiveMarker(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CEPH_MSGR_NATIVE_COPYFROM_MULTICHUNK_ERROR", "")
	t.Setenv("CEPH_MSGR_NATIVE_COPYFROM_LATE_ERROR", "1")
	t.Setenv("CEPH_MSGR_TEST_OSD", "1")
	t.Setenv("CEPH_MSGR_TEST_OSD_STORE", "bluestore")
	t.Setenv("CEPH_MSGR_TEST_OSD_REPLICAS", "1")
	t.Setenv("CEPH_MSGR_CONTROL_DIR", dir)
	if !t.Run("unselected", TestCephOSDCopyFromMultiChunkLateErrorIntegration) {
		t.Fatal("unselected fault accessed the fixture")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("unselected fault published a fixture control", err)
	}
}

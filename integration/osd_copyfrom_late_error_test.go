package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// The pinned injector runs after copy cursor completion/digest checks and before
// fill_in_final_tx. This verifies a late server error, not daemon loss, temporary
// copy staging, rollback of arbitrary vectors, or automatic mutation replay.
func TestCephOSDCopyFromLateErrorIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("late copy error requires its exclusive single-BlueStore fixture")
	}
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	inventory := [][]byte{[]byte("a"), []byte("z"), []byte("stale")}
	data := []byte("bounded copy source\x00\xff")
	attrs := []nativeXattrEntry{{Name: []byte("source-attr"), Value: []byte{0, 255}}}
	model := map[string][]byte{"a": []byte("source-map"), "z": {}}
	receipt := struct {
		SourceBefore, SourceAfter           omapWriteState
		DestinationBefore, DestinationAfter omapWriteState
		NativeExit                          *int `json:"native_cli_exit"`
		NativeBefore, NativeAfter           omapWriteState
		Option                              json.RawMessage `json:"option"`
		Pool                                int64           `json:"pool"`
		Primary                             int32           `json:"primary"`
		PG, Hash, RouteEpoch                uint32
		Reply                               cephmsgr.OSDReply `json:"go_reply"`
		Readback                            cephmsgr.OSDReply `json:"read_only_reply"`
		ServerError                         bool              `json:"signed_server_eio"`
		Restored                            bool              `json:"option_restored"`
		Removed                             bool              `json:"objects_removed"`
	}{Pool: base.Pool}
	prepared, optionAttempted := false, false
	option := func(call context.Context, action string) error {
		return testcluster.RequestControl(call, dir, "limit-osd-copyfrom-error", "osd-copyfrom-error", action)
	}
	t.Cleanup(func() {
		defer func() {
			if raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json")); err == nil {
				receipt.Option = raw
			}
			raw, err := json.Marshal(receipt)
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(dir, "osd-copyfrom-late-error-results.json"), raw)
			}
			if err != nil {
				t.Error("late copy error receipt", err)
			}
		}()
		// Cancellation of the subject wait does not authorize mutation replay.
		// Restoration and object cleanup have fresh, separate bounded contexts.
		if optionAttempted && !receipt.Restored {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			err := option(cleanup, "restore")
			stop()
			if err != nil {
				t.Error("copy-error option restoration", err)
			} else {
				receipt.Restored = true
			}
		}
		if raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json")); err == nil {
			receipt.Option = raw
		}
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
			stop()
			receipt.Removed = result.Cleanup.Removed && len(result.Cleanup.Objects) == 3
			if !receipt.Removed {
				t.Error("late copy owned-object cleanup differs", result.Cleanup)
			}
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{
		Seed: &omapWriteSeed{Baseline: data, Attributes: attrs, Entries: omapWriteEntries(model)}, Reserve: 2,
	})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 3 {
		t.Fatal("native late-copy source preparation differs")
	}
	src, nativeDst, dst := owned.Objects[0], owned.Objects[1], owned.Objects[2]
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: inventory}).State
	}
	place := func(name string) osdFixtureOracle {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if route.ID != base.ID || route.Name != name || route.Pool != base.Pool {
			t.Fatal("exclusive copy-error placement differs")
		}
		return route
	}
	sourceRoute, target := place(src), place(dst)
	o := mutationConnection(t, c, ctx, target)
	defer o.Close()
	setup := osdObjectRequest(sourceRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte{0, 's', 255}}})
	setup.ModificationTime = cephmsgr.OSDTime{Seconds: 1700011000, Nanoseconds: 123456789}
	if _, err := o.Request(ctx, setup); err != nil {
		t.Fatal("source header setup", err)
	}
	prefill := osdObjectRequest(target,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("stale")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "stale"}, Data: []byte("old")},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "stale", Value: []byte("old")}}}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("old")}})
	prefill.ModificationTime = cephmsgr.OSDTime{Seconds: 1700011001, Nanoseconds: 987654321}
	if _, err := o.Request(ctx, prefill); err != nil {
		t.Fatal("late-copy populated destination setup", err)
	}
	receipt.SourceBefore, receipt.DestinationBefore = observe(src), observe(dst)
	source := receipt.SourceBefore
	before := receipt.DestinationBefore
	if source.Code != 0 || !bytes.Equal(source.Data, data) || source.Version == 0 || !equalOmapModel(omapWriteInventory(t, source), model) ||
		before.Code != 0 || !bytes.Equal(before.Data, []byte("stale")) || before.Version == 0 || xattrInventory(before.nativeXattrSnapshot)["stale"] != "old" ||
		!equalOmapModel(omapWriteInventory(t, before), map[string][]byte{"stale": []byte("old")}) {
		t.Fatal("independent copy-error preconditions differ")
	}
	optionAttempted = true
	if err := option(ctx, "apply"); err != nil {
		t.Fatal("apply and independently read back copy-error injector", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "osd-copyfrom-error-option.json"))
	if err != nil {
		t.Fatal(err)
	}
	receipt.Option = raw
	// Keep field names explicit: the option receipt is independently produced
	// by the owned daemon's admin socket, not by a Messenger command response.
	var setting struct {
		OSD      int32  `json:"osd"`
		MaxChunk uint64 `json:"max_chunk"`
		Before   string `json:"effective_before"`
		Applied  string `json:"effective_applied"`
	}
	if err = json.Unmarshal(raw, &setting); err != nil {
		t.Fatal(err)
	}
	if setting.OSD != target.ID || setting.Before != "false" || setting.Applied != "true" || setting.MaxChunk <= 64<<10 || uint64(len(data)) >= setting.MaxChunk {
		t.Fatal("effective injector/chunk boundary differs", setting)
	}
	// Calibrate ONCE against a separate absent owned target. Preserve the CLI
	// exit as a process exit; it is not a signed wire errno or a twin compound.
	native := nativeOmapWrite(t, ctx, base, "copy", omapWriteInput{Source: src, Object: nativeDst, InventoryKeys: inventory})
	receipt.NativeExit, receipt.NativeBefore, receipt.NativeAfter = native.CopyExit, native.Before, native.After
	if native.CopyExit == nil || *native.CopyExit == 0 || native.Before.Code != -2 || native.After.Code != -2 {
		t.Fatal("native copy injector calibration differs")
	}
	r := osdObjectRequest(target,
		cephmsgr.OSDOperation{Code: cephmsgr.OSDCopyFrom2, CopyFrom: cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: src, SourceVersion: source.Version}},
		cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "late-copy-suffix"}, Data: []byte("must-not-appear")})
	r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700011002}
	receipt.Primary, receipt.PG, receipt.Hash, receipt.RouteEpoch = target.ID, target.PG, target.Hash, target.Epoch
	reply, err := o.Request(ctx, r)
	receipt.Reply = reply
	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	receipt.ServerError = errors.As(err, &server) && server.Code == -5 && !errors.As(err, &unknown)
	receipt.DestinationAfter, receipt.SourceAfter = observe(dst), observe(src)
	if !receipt.ServerError || reply.Code != -5 || reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction ||
		reply.GlobalID == 0 || reply.Submission.GlobalID != reply.GlobalID || reply.Object != dst || reply.MapEpoch < target.Epoch || reply.RetryAttempt != 0 ||
		len(reply.RawFront) == 0 || len(reply.RawData) != 0 || len(reply.Operations) != 2 {
		t.Fatal("late copy signed error receipt differs", err, reply)
	}
	copyResult, suffix := reply.Operations[0], reply.Operations[1]
	if copyResult.Code != cephmsgr.OSDCopyFrom2 || copyResult.Result != -115 || copyResult.CopyFromVersion != source.Version || copyResult.CopyFromSnapshot != cephmsgr.OSDHeadSnapshot ||
		suffix.Code != cephmsgr.OSDSetXattr || suffix.Result != 0 || copyResult.PayloadLength != 0 || suffix.PayloadLength != 0 || len(copyResult.Data)+len(suffix.Data) != 0 {
		t.Fatal("late copy original union/per-op representation differs", copyResult, suffix)
	}
	// A zero untouched suffix result does not establish suffix execution.
	if !reflect.DeepEqual(receipt.DestinationAfter, before) || !reflect.DeepEqual(receipt.SourceAfter, source) {
		t.Fatal("late-error copy changed native source or populated destination")
	}
	if _, present := xattrInventory(receipt.DestinationAfter.nativeXattrSnapshot)["late-copy-suffix"]; present {
		t.Fatal("suffix marker appeared after asynchronous error")
	}
	restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
	err = option(restore, "restore")
	stop()
	if err != nil {
		t.Fatal("restore and independently read back original copy-error option", err)
	}
	receipt.Restored = true
	read, err := o.Request(ctx, osdObjectRequest(target, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 32}))
	receipt.Readback = read
	if err != nil || read.Transaction != reply.Transaction+1 || read.UserVersion != before.Version || len(read.Operations) != 1 || !bytes.Equal(read.Operations[0].Data, before.Data) {
		t.Fatal("same connection read-only request after restored injector differs", err, read)
	}
}

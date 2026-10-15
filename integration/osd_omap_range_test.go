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

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephOSDOmapRangeRemovalIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := []byte{0, 0xff, 'r', 'n', 'g'}
	model := map[string][]byte{"": {}, "\x00k": {0}, "a": []byte("a"), "b": []byte("b"), "b\x00x": {0xff}, "b0": []byte("b0"),
		"c": {}, "c0": []byte("c0"), "\xff": {0x80}}
	inventory := [][]byte{[]byte("created")}
	for key := range model {
		inventory = append(inventory, []byte(key))
	}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned native OMAP range cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Entries: omapWriteEntries(model)}, Reserve: 2})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 3 || !equalOmapModel(omapWriteInventory(t, owned.State), model) {
		t.Fatal("exclusive native OMAP range baseline differs")
	}
	models := map[string]map[string][]byte{owned.Objects[0]: model}
	headers := map[string][]byte{owned.Objects[0]: {}}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	route := func(name string, exists bool) (osdFixtureOracle, *cephmsgr.OSDConnection) {
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
		selected, want := routing.Placement, int32(-2)
		if exists {
			want = 0
		}
		if routing.Actual.Code != want || selected.Name != name || selected.Pool != base.Pool || selected.ID < 0 || selected.Address == "" {
			t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
		}
		if connections[selected.ID] == nil {
			connections[selected.ID] = mutationConnection(t, c, ctx, selected)
		}
		return selected, connections[selected.ID]
	}
	rmrange := func(flags cephmsgr.OSDOperationFlags, begin, end string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveOmapKeyRange, Flags: flags, Omap: cephmsgr.OSDOmapInput{RangeBegin: begin, RangeEnd: end}}
	}
	nativeActions := func(ops []cephmsgr.OSDOperation) []omapWriteAction {
		var actions []omapWriteAction
		for _, op := range ops {
			switch op.Code {
			case cephmsgr.OSDRemoveOmapKeyRange:
				actions = append(actions, omapWriteAction{Op: "rmrange", Flags: uint32(op.Flags), Begin: []byte(op.Omap.RangeBegin), End: []byte(op.Omap.RangeEnd)})
			case cephmsgr.OSDSetOmapValues:
				action := omapWriteAction{Op: "set", Flags: uint32(op.Flags)}
				for _, entry := range op.Omap.Entries {
					action.Entries = append(action.Entries, omapNativeEntry{Key: []byte(entry.Key), Value: append([]byte{}, entry.Value...)})
				}
				actions = append(actions, action)
			default:
				t.Fatal("native C mirror cannot express opcode", op.Code)
			}
		}
		return actions
	}
	type caseEvidence struct {
		Name   string            `json:"name"`
		Native *omapWriteReceipt `json:"native_mirror,omitempty"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
		Before omapWriteState    `json:"native_before"`
		After  omapWriteState    `json:"native_after"`
	}
	var evidence []caseEvidence
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 512<<10 {
			err = os.WriteFile(filepath.Join(dir, "osd-omap-range-results.json"), data, 0644)
		}
		if err != nil {
			t.Error("bounded OMAP range evidence", err)
		}
	}()
	tick := uint32(0)
	type step struct {
		name    string
		object  string
		ops     []cephmsgr.OSDOperation
		code    int32
		results []int32
		mirror  bool
		apply   func(map[string][]byte)
	}
	run := func(tc step) {
		t.Run(tc.name, func(t *testing.T) {
			exists := models[tc.object] != nil
			selected, connection := route(tc.object, exists)
			tick++
			request := osdObjectRequest(selected, tc.ops...)
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1730000000 + tick, Nanoseconds: tick}
			var native *omapWriteReceipt
			if tc.mirror {
				input := omapWriteInput{Operations: nativeActions(tc.ops), MTime: &omapWriteMTime{Seconds: request.ModificationTime.Seconds, Nanos: request.ModificationTime.Nanoseconds}, InventoryKeys: inventory}
				if exists {
					input.Seed = &omapWriteSeed{Baseline: baseline, Entries: omapWriteEntries(models[tc.object])}
				}
				result := nativeOmapWrite(t, ctx, base, "compare", input)
				if result.Aggregate == nil || *result.Aggregate != tc.code || !result.Cleanup.Mirror {
					t.Fatal("native C range mirror aggregate/cleanup differs", result.Aggregate, tc.code)
				}
				native = &result
			}
			before := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object, InventoryKeys: inventory}).State
			reply, err := connection.Request(ctx, request)
			var server *cephmsgr.OSDError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatal("known OMAP range refusal differs", err)
				}
			} else if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("Go OMAP range removal", err)
			}
			for i, op := range reply.Operations {
				want := int32(0)
				if tc.results != nil {
					want = tc.results[i]
				}
				t.Logf("op=%d code=%x result=%d length=%d", i, op.Code, op.Result, op.Length)
				if op.Result != want || op.PayloadLength != 0 {
					t.Fatalf("OMAP range per-op result differs: op=%d got=%d want=%d", i, op.Result, want)
				}
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object, InventoryKeys: inventory}).State
			if tc.code < 0 {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("refused OMAP range removal changed native state")
				}
			} else {
				if models[tc.object] == nil {
					models[tc.object], headers[tc.object] = map[string][]byte{}, []byte{}
				}
				tc.apply(models[tc.object])
				if after.Version <= before.Version || after.Seconds != int64(request.ModificationTime.Seconds) || !equalOmapModel(omapWriteInventory(t, after), models[tc.object]) || !bytes.Equal(*after.Header, headers[tc.object]) {
					t.Fatalf("committed OMAP range state differs: %+v header=%x", omapWriteInventory(t, after), *after.Header)
				}
				if native != nil && !equalOmapModel(omapWriteInventory(t, native.After), models[tc.object]) {
					t.Fatal("native C range mirror inventory differs from the Go-committed state")
				}
			}
			evidence = append(evidence, caseEvidence{Name: tc.name, Native: native, Go: reply, Before: before, After: after})
		})
	}
	main, absent := owned.Objects[0], owned.Objects[1:]
	header := []byte{0, 0xff, 'h'}
	steps := []step{
		{name: "set header before range removals", object: main, ops: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}}},
			apply: func(map[string][]byte) { headers[main] = header }},
		{name: "half-open middle range keeps header", object: main, mirror: true, ops: []cephmsgr.OSDOperation{rmrange(0, "b", "c")},
			apply: func(m map[string][]byte) { delete(m, "b"); delete(m, "b\x00x"); delete(m, "b0") }},
		{name: "equal endpoints is an executed empty range", object: main, mirror: true, ops: []cephmsgr.OSDOperation{rmrange(0, "c", "c")}, apply: func(map[string][]byte) {}},
		{name: "empty begin removes the empty and NUL keys", object: main, mirror: true, ops: []cephmsgr.OSDOperation{rmrange(0, "", "a")},
			apply: func(m map[string][]byte) { delete(m, ""); delete(m, "\x00k") }},
		{name: "high-byte end includes 0xff key", object: main, mirror: true, ops: []cephmsgr.OSDOperation{rmrange(0, "c", "\xff\xff")},
			apply: func(m map[string][]byte) { delete(m, "c"); delete(m, "c0"); delete(m, "\xff") }},
		{name: "range removal on absent object is ENOENT", object: absent[0], code: -2, results: []int32{-2}, mirror: true, ops: []cephmsgr.OSDOperation{rmrange(0, "a", "b")}},
		{name: "FAILOK range removal on absent object then set creates", object: absent[1], mirror: true, results: []int32{-2, 0},
			ops:   []cephmsgr.OSDOperation{rmrange(cephmsgr.OSDFailOK, "a", "b"), {Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "created", Value: []byte("1")}}}}},
			apply: func(m map[string][]byte) { m["created"] = []byte("1") }},
	}
	for _, tc := range steps {
		run(tc)
		if t.Failed() {
			return
		}
	}
	t.Run("reversed range native no-op and Go local refusal", func(t *testing.T) {
		selected, connection := route(main, true)
		request := osdObjectRequest(selected, rmrange(0, "z", "a"))
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1}
		reply, err := connection.Request(ctx, request)
		if err == nil || reply.Submission.Transaction != 0 {
			t.Fatal("Go transmitted a reversed OMAP range", err)
		}
		if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
			// MemStore::_omap_rmkeyrange passes lower_bound(first) and
			// lower_bound(last) to std::map::erase; a reversed range is
			// undefined behavior that stopped the single memstore OSD.
			t.Logf("Go refused before transmission: %v; native reversed range is not sent to MemStore", err)
			return
		}
		native := nativeOmapWrite(t, ctx, base, "compare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Entries: omapWriteEntries(model)}, Operations: []omapWriteAction{{Op: "rmrange", Begin: []byte("z"), End: []byte("a")}},
			MTime: &omapWriteMTime{Seconds: 1739999999}, InventoryKeys: inventory})
		if native.Aggregate == nil || *native.Aggregate != 0 || !equalOmapModel(omapWriteInventory(t, native.After), model) {
			t.Fatal("native reversed range was not an empty removal on a small object", native.Aggregate)
		}
		t.Logf("native reversed range aggregate=%d with unchanged inventory; Go refused before transmission: %v", *native.Aggregate, err)
		evidence = append(evidence, caseEvidence{Name: "reversed range native", Native: &native})
	})
}

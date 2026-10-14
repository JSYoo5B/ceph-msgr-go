package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Test-only model of the pinned server comparison (buffer.h:1269-1297 under
// the unconditional -fsigned-char build flag). The product never evaluates
// comparisons; this is a third reference beside the native and Go results.
func omapNativeLess(left, right []byte) bool {
	for i := 0; i < len(left) && i < len(right); i++ {
		if a, b := int8(left[i]), int8(right[i]); a != b {
			return a < b
		}
	}
	return len(left) < len(right)
}

func omapModelCompare(exists bool, values map[string][]byte, assertions []cephmsgr.OSDOmapAssertion) int32 {
	if !exists {
		return -2
	}
	ordered := append([]cephmsgr.OSDOmapAssertion(nil), assertions...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
	for _, assertion := range ordered {
		stored := values[assertion.Key] // missing key compares as empty
		var ok bool
		switch assertion.Comparison {
		case cephmsgr.OSDOmapEqual:
			ok = bytes.Equal(stored, assertion.Value)
		case cephmsgr.OSDOmapLess:
			ok = omapNativeLess(stored, assertion.Value)
		case cephmsgr.OSDOmapGreater:
			ok = omapNativeLess(assertion.Value, stored)
		default:
			return -22
		}
		if !ok {
			return -125
		}
	}
	return 0
}

func omapCmpAction(flags cephmsgr.OSDOperationFlags, key string, value []byte, comparison int) omapWriteAction {
	return omapWriteAction{Op: "cmp", Flags: uint32(flags), Key: []byte(key), Value: append([]byte{}, value...), Comparison: &comparison}
}

func TestCephOSDOmapCompareIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := []byte{0, 0xff, 'c', 'm', 'p'}
	model := map[string][]byte{"s80": {0x80}, "s7f": {0x7f}, "sff": {0xff}, "s0180": {1, 0x80}, "sa80": []byte("a\x80"),
		"sab": []byte("ab"), "e": {}, "snul": []byte("a\x00b"), "s00": {0}}
	inventory := [][]byte{[]byte("")}
	for key := range model {
		inventory = append(inventory, []byte(key))
	}
	for _, key := range []string{"missing", "cmp-set", "cmp-fail", "cmp-failok"} {
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
			t.Error("owned native OMAP comparison cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Entries: omapWriteEntries(model)}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 || !equalOmapModel(omapWriteInventory(t, owned.State), model) {
		t.Fatal("exclusive native OMAP comparison baseline differs")
	}
	main, absent := owned.Objects[0], owned.Objects[1]
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	placements := map[string]osdFixtureOracle{}
	route := func(name string, exists bool) (osdFixtureOracle, *cephmsgr.OSDConnection) {
		if selected, ok := placements[name]; ok {
			return selected, connections[selected.ID]
		}
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
		selected, want := routing.Placement, int32(-2)
		if exists {
			want = 0
		}
		if routing.Actual.Code != want || selected.Name != name || selected.Pool != base.Pool || selected.Epoch < base.Epoch || selected.ID < 0 || selected.Address == "" || selected.RawPGID != fmt.Sprintf("%d.%x", selected.Pool, selected.Hash) {
			t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
		}
		t.Logf("caller placement object=%s rawPG=%s mappedPG=%s epoch=%d primary=%d endpoint=%s", name, selected.RawPGID, selected.PGID, selected.Epoch, selected.ID, selected.Address)
		placements[name] = selected
		if connections[selected.ID] == nil {
			connections[selected.ID] = mutationConnection(t, c, ctx, selected)
		}
		return selected, connections[selected.ID]
	}
	type caseEvidence struct {
		Name   string            `json:"name"`
		Native omapWriteReceipt  `json:"native"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
		Model  int32             `json:"model"`
	}
	evidence := struct {
		Schema int            `json:"schema_version"`
		Source string         `json:"source_commit"`
		Owned  []string       `json:"owned_objects"`
		Cases  []caseEvidence `json:"cases"`
	}{Schema: 1, Source: owned.Commit, Owned: owned.Objects}
	defer func() {
		data, err := json.Marshal(evidence)
		if err != nil || len(data) > 512<<10 {
			t.Error("bounded OMAP comparison evidence", err, len(data))
			return
		}
		if err := os.WriteFile(filepath.Join(dir, "osd-omap-compare-results.json"), data, 0644); err != nil {
			t.Error(err)
		}
	}()
	assertion := func(key string, value []byte, comparison cephmsgr.OSDOmapComparison) cephmsgr.OSDOmapAssertion {
		return cephmsgr.OSDOmapAssertion{Key: key, Value: value, Comparison: comparison}
	}
	compareOp := func(flags cephmsgr.OSDOperationFlags, assertions ...cephmsgr.OSDOmapAssertion) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareOmap, Flags: flags, Omap: cephmsgr.OSDOmapInput{Assertions: assertions}}
	}
	// One C cmp2 call is one operation with one assertion. A multi-assertion
	// Go operation is mirrored by single-assertion C operations in key order,
	// which is indirect evidence for the aggregate only.
	nativeActions := func(ops []cephmsgr.OSDOperation) []omapWriteAction {
		var actions []omapWriteAction
		for _, op := range ops {
			switch op.Code {
			case cephmsgr.OSDCompareOmap:
				ordered := append([]cephmsgr.OSDOmapAssertion(nil), op.Omap.Assertions...)
				sort.Slice(ordered, func(i, j int) bool { return ordered[i].Key < ordered[j].Key })
				for _, a := range ordered {
					actions = append(actions, omapCmpAction(op.Flags, a.Key, a.Value, int(a.Comparison)))
				}
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
	tick := uint32(0)
	type step struct {
		name    string
		object  string
		ops     []cephmsgr.OSDOperation
		code    int32
		results []int32 // nil means all zero
		direct  bool    // one native C operation per Go operation
		apply   func()
	}
	run := func(tc step) {
		t.Run(tc.name, func(t *testing.T) {
			exists := tc.object == main
			selected, connection := route(tc.object, exists)
			write := false
			for _, op := range tc.ops {
				write = write || op.Code != cephmsgr.OSDCompareOmap
			}
			request := osdObjectRequest(selected, tc.ops...)
			var native omapWriteReceipt
			var nativeAggregate int32
			var prvals []omapNativePrval
			if write {
				tick++
				request.ModificationTime = cephmsgr.OSDTime{Seconds: 1720000000 + tick, Nanoseconds: tick}
				native = nativeOmapWrite(t, ctx, base, "compare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Entries: omapWriteEntries(model)}, Operations: nativeActions(tc.ops),
					MTime: &omapWriteMTime{Seconds: request.ModificationTime.Seconds, Nanos: request.ModificationTime.Nanoseconds}, InventoryKeys: inventory})
				if native.Aggregate == nil || !native.Cleanup.Mirror {
					t.Fatal("native C comparison mirror lacks aggregate or cleanup")
				}
				nativeAggregate, prvals = *native.Aggregate, native.Prvals
			} else {
				native = nativeOmapWrite(t, ctx, base, "assert", omapWriteInput{Object: tc.object, Operations: nativeActions(tc.ops), InventoryKeys: inventory})
				if native.Vector == nil {
					t.Fatal("native read-only comparison lacks a vector receipt")
				}
				nativeAggregate, prvals = native.Vector.Aggregate, native.Vector.Prvals
			}
			before := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object, InventoryKeys: inventory}).State
			reply, err := connection.Request(ctx, request)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code || errors.As(err, &unknown) {
					t.Fatal("known OMAP comparison refusal differs", err)
				}
			} else if err != nil {
				t.Fatal("Go OMAP comparison request", err)
			}
			if reply.Code != tc.code || nativeAggregate != tc.code || reply.Object != tc.object || reply.PG.Seed != selected.Hash || reply.RetryAttempt != 0 || len(reply.Operations) != len(tc.ops) {
				t.Fatalf("Go/native aggregate or identity differs: go=%d native=%d want=%d", reply.Code, nativeAggregate, tc.code)
			}
			var model0 int32
			if tc.ops[0].Code == cephmsgr.OSDCompareOmap {
				model0 = omapModelCompare(exists, model, tc.ops[0].Omap.Assertions)
			}
			for i, result := range reply.Operations {
				want := int32(0)
				if tc.results != nil {
					want = tc.results[i]
				}
				t.Logf("op=%d code=%x go=%d length=%d payload=%d", i, result.Code, result.Result, result.Length, result.PayloadLength)
				if result.PayloadLength != 0 || exists && result.Result != want {
					t.Fatalf("Go OMAP comparison per-op result/output differs: op=%d got=%d want=%d", i, result.Result, want)
				}
				if tc.direct && exists && tc.ops[i].Code == cephmsgr.OSDCompareOmap {
					if prvals[i].Index != i || prvals[i].Prval != result.Result {
						t.Fatalf("Go per-op result differs from native C prval: op=%d go=%d native=%+v", i, result.Result, prvals)
					}
				}
			}
			for _, p := range prvals {
				t.Logf("native prval index=%d value=%d", p.Index, p.Prval)
			}
			if tc.ops[0].Code == cephmsgr.OSDCompareOmap && (exists && model0 != reply.Operations[0].Result || !exists && model0 != reply.Code) {
				t.Fatalf("Go OMAP comparison differs from the signed-char model: go=%d model=%d", reply.Operations[0].Result, model0)
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object, InventoryKeys: inventory}).State
			if !write || tc.code < 0 {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("read-only or refused OMAP comparison changed native state")
				}
			} else {
				tc.apply()
				if after.Version <= before.Version || after.Seconds != int64(request.ModificationTime.Seconds) || !equalOmapModel(omapWriteInventory(t, after), model) || !bytes.Equal(after.Data, baseline) {
					t.Fatal("committed compare-before-write state differs from model")
				}
				if !equalOmapModel(omapWriteInventory(t, native.After), model) || native.After.Seconds != after.Seconds {
					t.Fatal("native C mirror state differs from the Go-committed state")
				}
			}
			evidence.Cases = append(evidence.Cases, caseEvidence{Name: tc.name, Native: native, Go: reply, Model: model0})
		})
	}
	var steps []step
	for _, tc := range []struct {
		name       string
		key        string
		operand    []byte
		comparison cephmsgr.OSDOmapComparison
		want       int32
	}{
		{"signed 0x80 less than 0x7f", "s80", []byte{0x7f}, cephmsgr.OSDOmapLess, 0},
		{"signed 0x80 not greater than 0x7f", "s80", []byte{0x7f}, cephmsgr.OSDOmapGreater, -125},
		{"signed 0x7f greater than 0x80", "s7f", []byte{0x80}, cephmsgr.OSDOmapGreater, 0},
		{"signed 0xff less than 0x00", "sff", []byte{0}, cephmsgr.OSDOmapLess, 0},
		{"signed tail 01 80 less than 01 7f", "s0180", []byte{1, 0x7f}, cephmsgr.OSDOmapLess, 0},
		{"control 0xff greater than 0x80", "sff", []byte{0x80}, cephmsgr.OSDOmapGreater, 0},
		{"length beats negative tail", "sa80", []byte("a"), cephmsgr.OSDOmapGreater, 0},
		{"prefix less than longer", "sab", []byte("abc"), cephmsgr.OSDOmapLess, 0},
		{"prefix not equal to longer", "sab", []byte("abc"), cephmsgr.OSDOmapEqual, -125},
		{"prefix not greater than longer", "sab", []byte("abc"), cephmsgr.OSDOmapGreater, -125},
		{"empty value equals empty", "e", []byte{}, cephmsgr.OSDOmapEqual, 0},
		{"missing key equals empty", "missing", []byte{}, cephmsgr.OSDOmapEqual, 0},
		{"missing key not less than empty", "missing", []byte{}, cephmsgr.OSDOmapLess, -125},
		{"empty less than 0x80", "e", []byte{0x80}, cephmsgr.OSDOmapLess, 0},
		{"NUL byte greater than empty", "s00", []byte{}, cephmsgr.OSDOmapGreater, 0},
		{"embedded NUL ordering", "snul", []byte("a\x00c"), cephmsgr.OSDOmapLess, 0},
		{"embedded NUL equality", "snul", []byte("a\x00b"), cephmsgr.OSDOmapEqual, 0},
	} {
		if got := omapModelCompare(true, model, []cephmsgr.OSDOmapAssertion{assertion(tc.key, tc.operand, tc.comparison)}); got != tc.want {
			t.Fatal("test expectation disagrees with the signed-char model", tc.name, got)
		}
		steps = append(steps, step{name: tc.name, object: main, code: tc.want, results: []int32{tc.want}, direct: true,
			ops: []cephmsgr.OSDOperation{compareOp(0, assertion(tc.key, tc.operand, tc.comparison))}})
	}
	steps = append(steps,
		step{name: "two assertions pass in key order", object: main, ops: []cephmsgr.OSDOperation{compareOp(0, assertion("sab", []byte("abc"), cephmsgr.OSDOmapLess), assertion("s80", []byte{0x7f}, cephmsgr.OSDOmapLess))}},
		step{name: "later key failure cancels one operation", object: main, code: -125, results: []int32{-125},
			ops: []cephmsgr.OSDOperation{compareOp(0, assertion("sab", []byte("abc"), cephmsgr.OSDOmapEqual), assertion("s80", []byte{0x7f}, cephmsgr.OSDOmapLess))}},
		step{name: "second operation failure stops the vector", object: main, code: -125, results: []int32{0, -125}, direct: true,
			ops: []cephmsgr.OSDOperation{compareOp(0, assertion("s80", []byte{0x7f}, cephmsgr.OSDOmapLess)), compareOp(0, assertion("sab", []byte("abc"), cephmsgr.OSDOmapEqual))}},
		step{name: "missing object is known ENOENT", object: absent, code: -2, direct: true, ops: []cephmsgr.OSDOperation{compareOp(0, assertion("k", nil, cephmsgr.OSDOmapEqual))}},
		step{name: "passing guard commits the write", object: main, direct: true,
			ops:   []cephmsgr.OSDOperation{compareOp(0, assertion("s80", []byte{0x7f}, cephmsgr.OSDOmapLess)), {Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "cmp-set", Value: []byte("1")}}}}},
			apply: func() { model["cmp-set"] = []byte("1") }},
		step{name: "failing guard cancels the write", object: main, code: -125, results: []int32{-125, 0}, direct: true,
			ops: []cephmsgr.OSDOperation{compareOp(0, assertion("sab", []byte("abc"), cephmsgr.OSDOmapEqual)), {Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "cmp-fail", Value: []byte("x")}}}}}},
		step{name: "FAILOK guard failure lets the write commit", object: main, results: []int32{-125, 0}, direct: true,
			ops:   []cephmsgr.OSDOperation{compareOp(cephmsgr.OSDFailOK, assertion("sab", []byte("abc"), cephmsgr.OSDOmapEqual)), {Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "cmp-failok", Value: []byte("y")}}}}},
			apply: func() { model["cmp-failok"] = []byte("y") }},
	)
	for _, tc := range steps {
		run(tc)
		if t.Failed() {
			return
		}
	}
	// Identifiers the OMAP handler does not evaluate are refused locally by
	// Go; the native C builder can still send them to characterize the server.
	t.Run("unsupported identifiers native EINVAL and Go local refusal", func(t *testing.T) {
		selected, connection := route(main, true)
		for _, id := range []int{2, 4, 6} {
			native := nativeOmapWrite(t, ctx, base, "assert", omapWriteInput{Object: main, Operations: []omapWriteAction{omapCmpAction(0, "s80", []byte{0x80}, id)}, InventoryKeys: inventory})
			if native.Vector == nil || native.Vector.Aggregate != -22 || len(native.Vector.Prvals) != 1 || native.Vector.Prvals[0].Prval != -22 {
				t.Fatal("native server did not refuse an unsupported OMAP comparison identifier", id, native.Vector)
			}
			reply, err := connection.Request(ctx, osdObjectRequest(selected, compareOp(0, assertion("s80", []byte{0x80}, cephmsgr.OSDOmapComparison(id)))))
			if !errors.Is(err, cephmsgr.ErrUnsupportedOSDOperation) || reply.Submission.Transaction != 0 {
				t.Fatal("Go transmitted an unsupported OMAP comparison identifier", id, err)
			}
			evidence.Cases = append(evidence.Cases, caseEvidence{Name: fmt.Sprintf("unsupported identifier %d", id), Native: native, Model: -22})
		}
	})
}

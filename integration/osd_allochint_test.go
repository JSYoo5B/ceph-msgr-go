package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

func TestCephOSDAllocationHintIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	seedData := []byte{0, 0xff, 'h', 'i', 'n', 't'}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned native allocation hint cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: seedData}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("exclusive native allocation hint baseline differs")
	}
	main, absent := owned.Objects[0], owned.Objects[1]
	data := map[string][]byte{main: seedData}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	route := func(name string) (osdFixtureOracle, *cephmsgr.OSDConnection) {
		want := int32(-2)
		if data[name] != nil {
			want = 0
		}
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
		selected := routing.Placement
		if routing.Actual.Code != want || selected.Name != name || selected.Pool != base.Pool || selected.ID < 0 || selected.Address == "" {
			t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
		}
		if connections[selected.ID] == nil {
			connections[selected.ID] = mutationConnection(t, c, ctx, selected)
		}
		return selected, connections[selected.ID]
	}
	hint := func(flags cephmsgr.OSDOperationFlags, objectSize, writeSize uint64, hintFlags cephmsgr.OSDAllocHintFlags) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetAllocHint, Flags: flags, AllocHint: cephmsgr.OSDAllocHint{ExpectedObjectSize: objectSize, ExpectedWriteSize: writeSize, Flags: hintFlags}}
	}
	type caseEvidence struct {
		Name   string            `json:"name"`
		Mirror omapWriteReceipt  `json:"native_mirror"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
		After  omapWriteReceipt  `json:"native_after"`
	}
	var evidence []caseEvidence
	defer func() {
		encoded, err := json.Marshal(evidence)
		if err == nil && len(encoded) <= 512<<10 {
			err = os.WriteFile(filepath.Join(dir, "osd-alloc-hint-results.json"), encoded, 0644)
		}
		if err != nil {
			t.Error("bounded allocation hint evidence", err)
		}
	}()
	tick := uint32(0)
	type step struct {
		name     string
		object   string
		ops      []cephmsgr.OSDOperation
		native   []omapWriteAction
		info     omapObjectInfo // expected committed hint fields
		wantData []byte
	}
	run := func(tc step) {
		t.Run(tc.name, func(t *testing.T) {
			selected, connection := route(tc.object)
			tick++
			request := osdObjectRequest(selected, tc.ops...)
			request.ModificationTime = cephmsgr.OSDTime{Seconds: 1750000000 + tick, Nanoseconds: tick}
			input := omapWriteInput{Operations: tc.native, ObjectInfo: true, MTime: &omapWriteMTime{Seconds: request.ModificationTime.Seconds, Nanos: request.ModificationTime.Nanoseconds}}
			if data[tc.object] != nil {
				input.Seed = &omapWriteSeed{Baseline: data[tc.object]}
			}
			mirror := nativeOmapWrite(t, ctx, base, "compare", input)
			if mirror.Aggregate == nil || *mirror.Aggregate != 0 || mirror.ObjectInfo == nil || !mirror.Cleanup.Mirror {
				t.Fatal("native allocation hint mirror failed or lacks object_info", mirror.Aggregate)
			}
			before := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object}).State
			reply, err := connection.Request(ctx, request)
			if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("Go allocation hint vector", err)
			}
			for i, op := range reply.Operations {
				if op.Result != 0 || op.Code != tc.ops[i].Code || op.AllocHint != tc.ops[i].AllocHint {
					t.Fatal("allocation hint per-op result or union echo differs", i, op.Result, op.AllocHint)
				}
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object, ObjectInfo: true})
			got := *after.ObjectInfo
			if got.ExpectedObjectSize != tc.info.ExpectedObjectSize || got.ExpectedWriteSize != tc.info.ExpectedWriteSize || got.AllocHintFlags != tc.info.AllocHintFlags {
				t.Fatalf("committed object_info hint differs: %+v want %+v", got, tc.info)
			}
			mirrorInfo := *mirror.ObjectInfo
			// The C mirror runs only the hint; a following Go data write
			// changes size, so size is compared only for hint-only vectors.
			if mirrorInfo.ExpectedObjectSize != got.ExpectedObjectSize || mirrorInfo.ExpectedWriteSize != got.ExpectedWriteSize || mirrorInfo.AllocHintFlags != got.AllocHintFlags || len(tc.ops) == 1 && mirrorInfo.Size != got.Size {
				t.Fatalf("native mirror object_info differs: %+v / %+v", mirrorInfo, got)
			}
			if after.State.Code != 0 || after.State.Version <= before.Version || after.State.Seconds != int64(request.ModificationTime.Seconds) || !bytes.Equal(after.State.Data, tc.wantData) || got.Size != uint64(len(tc.wantData)) || got.UserVersion != after.State.Version {
				t.Fatalf("allocation hint changed object bytes or lost version/mtime: %+v", got)
			}
			data[tc.object] = after.State.Data
			t.Logf("object_info expected_object_size=%d expected_write_size=%d alloc_hint_flags=%d size=%d user_version=%d match native mirror", got.ExpectedObjectSize, got.ExpectedWriteSize, got.AllocHintFlags, got.Size, got.UserVersion)
			evidence = append(evidence, caseEvidence{Name: tc.name, Mirror: mirror, Go: reply, After: after})
		})
	}
	seqCompress := cephmsgr.OSDAllocHintSequentialWrite | cephmsgr.OSDAllocHintCompressible
	all := cephmsgr.OSDAllocHintFlags(1<<11 - 1)
	nativeHint := func(objectSize, writeSize uint64, flags uint32) omapWriteAction {
		return omapWriteAction{Op: "allochint", ObjectSize: objectSize, WriteSize: writeSize, HintFlags: flags}
	}
	steps := []step{
		{name: "hint on existing object keeps bytes", object: main, ops: []cephmsgr.OSDOperation{hint(0, 4<<20, 64<<10, seqCompress)},
			native: []omapWriteAction{nativeHint(4<<20, 64<<10, uint32(seqCompress))}, info: omapObjectInfo{ExpectedObjectSize: 4 << 20, ExpectedWriteSize: 64 << 10, AllocHintFlags: uint32(seqCompress)}, wantData: seedData},
		{name: "maximum sizes and every known flag", object: main, ops: []cephmsgr.OSDOperation{hint(0, ^uint64(0), ^uint64(0), all)},
			native: []omapWriteAction{nativeHint(^uint64(0), ^uint64(0), uint32(all))}, info: omapObjectInfo{ExpectedObjectSize: ^uint64(0), ExpectedWriteSize: ^uint64(0), AllocHintFlags: uint32(all)}, wantData: seedData},
		{name: "zero hint resets recorded fields", object: main, ops: []cephmsgr.OSDOperation{hint(0, 0, 0, 0)},
			native: []omapWriteAction{nativeHint(0, 0, 0)}, wantData: seedData},
		{name: "hint creates an absent object", object: absent, ops: []cephmsgr.OSDOperation{hint(0, 1<<20, 4096, cephmsgr.OSDAllocHintAppendOnly)},
			native: []omapWriteAction{nativeHint(1<<20, 4096, uint32(cephmsgr.OSDAllocHintAppendOnly))}, info: omapObjectInfo{ExpectedObjectSize: 1 << 20, ExpectedWriteSize: 4096, AllocHintFlags: uint32(cephmsgr.OSDAllocHintAppendOnly)}, wantData: []byte{}},
		{name: "FAILOK hint before a full write", object: absent, ops: []cephmsgr.OSDOperation{hint(cephmsgr.OSDFailOK, 8192, 512, cephmsgr.OSDAllocHintImmutable), {Code: cephmsgr.OSDWriteFull, Length: 3, Data: []byte("abc")}},
			native: []omapWriteAction{nativeHint(8192, 512, uint32(cephmsgr.OSDAllocHintImmutable))}, info: omapObjectInfo{ExpectedObjectSize: 8192, ExpectedWriteSize: 512, AllocHintFlags: uint32(cephmsgr.OSDAllocHintImmutable)}, wantData: []byte("abc")},
	}
	for _, tc := range steps {
		run(tc)
		if t.Failed() {
			return
		}
	}
	t.Run("unknown flag bit native record and Go local refusal", func(t *testing.T) {
		selected, connection := route(main)
		mirror := nativeOmapWrite(t, ctx, base, "compare", omapWriteInput{Seed: &omapWriteSeed{Baseline: seedData}, ObjectInfo: true,
			Operations: []omapWriteAction{nativeHint(4096, 4096, 1<<11)}, MTime: &omapWriteMTime{Seconds: 1759999999}})
		if mirror.Aggregate == nil || *mirror.Aggregate != 0 || mirror.ObjectInfo == nil {
			t.Fatal("native unknown hint flag mirror failed", mirror.Aggregate)
		}
		request := osdObjectRequest(selected, hint(0, 4096, 4096, 1<<11))
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1}
		reply, err := connection.Request(ctx, request)
		if err == nil || reply.Submission.Transaction != 0 {
			t.Fatal("Go transmitted an unknown allocation hint flag", err)
		}
		t.Logf("native server recorded alloc_hint_flags=%d for an unknown bit; Go refused before transmission: %v", mirror.ObjectInfo.AllocHintFlags, err)
		evidence = append(evidence, caseEvidence{Name: "unknown flag bit native", Mirror: mirror})
	})
}

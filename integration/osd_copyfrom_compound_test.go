package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
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

// Both admitted copy opcodes may lead xattr edits. Native C API/CLI reads
// independently observe object state; they do not execute a twin copy vector.
func TestCephOSDCopyFromCompoundIntegration(t *testing.T) {
	fixture, request := osdFixture, osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture, request = replicatedPGFixture, replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	data := []byte("compound copy source\x00\xff")
	attrs := []nativeXattrEntry{{Name: []byte("keep"), Value: []byte("k")}, {Name: []byte("drop"), Value: []byte("d")}}
	model := map[string][]byte{"": {0, 255}, "a": []byte("source-map"), "z": {}}
	inventory := [][]byte{[]byte(""), []byte("a"), []byte("z"), []byte("stale")}
	header := []byte{0, 'h', 255}
	type caseReceipt struct {
		Name           string         `json:"name"`
		Opcode         uint16         `json:"opcode"`
		Populated      bool           `json:"populated"`
		Pool           int64          `json:"pool"`
		Primary        int32          `json:"primary"`
		PG             uint32         `json:"pg"`
		RouteEpoch     uint32         `json:"route_epoch"`
		ReplyEpoch     uint32         `json:"reply_epoch"`
		CopyTID        uint64         `json:"copy_tid"`
		NextTID        uint64         `json:"next_tid"`
		PerOpResults   []int32        `json:"per_op_results"`
		Before         omapWriteState `json:"before"`
		After          omapWriteState `json:"after"`
		AfterRefusals  omapWriteState `json:"after_refusals"`
		SourceAfter    omapWriteState `json:"source_after"`
		RefusedVectors int            `json:"refused_vectors"`
	}
	receipt := struct {
		Source  omapWriteState `json:"source"`
		Cases   []caseReceipt  `json:"cases"`
		Removed bool           `json:"objects_removed"`
	}{}
	prepared := false
	t.Cleanup(func() {
		if prepared {
			cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
			defer stop()
			r := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
			receipt.Removed = r.Cleanup.Removed && len(r.Cleanup.Objects) == 5
			if !receipt.Removed {
				t.Error("compound copy owned-object cleanup differs", r.Cleanup)
			}
		}
		raw, err := json.Marshal(receipt)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-compound-results.json"), raw)
		}
		if err != nil {
			t.Error("compound copy receipt", err)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{
		Seed: &omapWriteSeed{Baseline: data, Attributes: attrs, Entries: omapWriteEntries(model)}, Reserve: 4,
	})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 5 {
		t.Fatal("compound source preparation differs")
	}
	sourceName := owned.Objects[0]
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: inventory}).State
	}
	place := func(name string) osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	}
	sourceRoute := place(sourceName)
	sourceConn := mutationConnection(t, c, ctx, sourceRoute)
	defer sourceConn.Close()
	sourceSetup := request(sourceRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}})
	sourceSetup.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009000, Nanoseconds: 123456789}
	if _, err := sourceConn.Request(ctx, sourceSetup); err != nil {
		t.Fatal("source header setup", err)
	}
	receipt.Source = observe(sourceName)
	if !bytes.Equal(receipt.Source.Data, data) || !equalOmapModel(omapWriteInventory(t, receipt.Source), model) ||
		receipt.Source.Header == nil || !bytes.Equal(*receipt.Source.Header, header) || receipt.Source.Version == 0 {
		t.Fatal("independent native source differs")
	}
	for slot, tc := range []struct {
		name      string
		code      cephmsgr.OSDOpcode
		populated bool
	}{
		{"copy-from/absent", cephmsgr.OSDCopyFrom, false},
		{"copy-from/populated", cephmsgr.OSDCopyFrom, true},
		{"copy-from2/absent", cephmsgr.OSDCopyFrom2, false},
		{"copy-from2/populated", cephmsgr.OSDCopyFrom2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := owned.Objects[slot+1]
			route := place(dst)
			o := mutationConnection(t, c, ctx, route)
			defer o.Close()
			if tc.populated {
				prefill := request(route,
					cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("stale")},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "stale"}, Data: []byte("old")},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "stale", Value: []byte("old")}}}},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: []byte("old")}})
				prefill.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009001}
				if _, err := o.Request(ctx, prefill); err != nil {
					t.Fatal("destination prefill", err)
				}
			}
			before := observe(dst)
			if tc.populated && (before.Code != 0 || !bytes.Equal(before.Data, []byte("stale")) || xattrInventory(before.nativeXattrSnapshot)["stale"] != "old" || !equalOmapModel(omapWriteInventory(t, before), map[string][]byte{"stale": []byte("old")})) || !tc.populated && before.Code != -2 {
				t.Fatal("native destination precondition differs")
			}
			copyOp := cephmsgr.OSDOperation{Code: tc.code, CopyFrom: cephmsgr.OSDCopyFromInput{
				SourcePool: base.Pool, SourceName: sourceName, SourceVersion: receipt.Source.Version,
				SourceFadvise: cephmsgr.OSDFadviseSequential,
			}}
			setx := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "added"}, Data: []byte{0, 'a'}}
			rmx := cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: "drop"}}
			r := request(route, copyOp, setx, rmx)
			// finish_copyfrom preserves the source mtime and clears ctx->mtime;
			// the suffix xattr writes must not restore this distinct caller stamp.
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700009002, Nanoseconds: 987654321}
			reply, err := o.Request(ctx, r)
			if err != nil || reply.Code != 0 || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 || len(reply.Operations) != 3 ||
				reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != dst ||
				reply.MapEpoch < route.Epoch || reply.RetryAttempt != 0 || reply.GlobalID == 0 || reply.Submission.GlobalID != reply.GlobalID || len(reply.RawData) != 0 {
				t.Fatal("compound copy receipt", err, reply)
			}
			var results []int32
			for i, op := range reply.Operations {
				if op.Code != r.Operations[i].Code || op.Result != 0 || op.PayloadLength != 0 || len(op.Data) != 0 {
					t.Fatal("compound copy per-op result differs", op)
				}
				results = append(results, op.Result)
			}
			if reply.Operations[0].CopyFromVersion != receipt.Source.Version || reply.Operations[0].CopyFromSnapshot != cephmsgr.OSDHeadSnapshot {
				t.Fatal("copy source union differs")
			}
			after := observe(dst)
			nativeAttrs := xattrInventory(after.nativeXattrSnapshot)
			if after.Code != 0 || !bytes.Equal(after.Data, data) || len(nativeAttrs) != 2 || nativeAttrs["keep"] != "k" || nativeAttrs["added"] != string([]byte{0, 'a'}) ||
				!equalOmapModel(omapWriteInventory(t, after), model) || after.Header == nil || !bytes.Equal(*after.Header, header) ||
				after.Version != reply.UserVersion || after.Version <= before.Version || after.Seconds != receipt.Source.Seconds || after.Nanos != receipt.Source.Nanos {
				t.Fatal("native destination data/metadata/version/mtime differs", after)
			}
			refused := [][]cephmsgr.OSDOperation{
				{setx, copyOp}, {copyOp, {Code: cephmsgr.OSDRead, Length: 1}},
				{copyOp, {Code: cephmsgr.OSDWriteFull, Length: 1, Data: []byte("x")}},
				{copyOp, {Code: cephmsgr.OSDStat}}, {copyOp, copyOp},
			}
			for _, ops := range refused {
				bad := request(route, ops...)
				bad.ModificationTime = r.ModificationTime
				badReply, err := o.Request(ctx, bad)
				var unknown *cephmsgr.OutcomeUnknownError
				if !errors.Is(err, cephmsgr.ErrUnsupportedOSDOperation) || errors.As(err, &unknown) || badReply.Submission != (cephmsgr.OSDSubmission{}) || badReply.Transaction != 0 || len(badReply.RawFront)+len(badReply.RawData) != 0 {
					t.Fatal("unsupported compound was not known-unsent", err)
				}
			}
			afterRefusals := observe(dst)
			sourceAfter := observe(sourceName)
			if !reflect.DeepEqual(afterRefusals, after) || !reflect.DeepEqual(sourceAfter, receipt.Source) {
				t.Fatal("refused vectors changed destination or copy changed source")
			}
			stat, err := o.Request(ctx, request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
			if err != nil || stat.Transaction != reply.Transaction+1 || len(stat.Operations) != 1 || stat.Operations[0].Code != cephmsgr.OSDStat || stat.Operations[0].Result != 0 || len(stat.Operations[0].Data) != 16 ||
				binary.LittleEndian.Uint64(stat.Operations[0].Data) != uint64(len(data)) || stat.UserVersion != after.Version ||
				binary.LittleEndian.Uint32(stat.Operations[0].Data[8:]) != uint32(after.Seconds) || binary.LittleEndian.Uint32(stat.Operations[0].Data[12:]) != uint32(after.Nanos) {
				t.Fatal("refused vectors allocated a TID or damaged connection", err, stat)
			}
			receipt.Cases = append(receipt.Cases, caseReceipt{Name: tc.name, Opcode: uint16(tc.code), Populated: tc.populated,
				Pool: base.Pool, Primary: route.ID, PG: route.PG, RouteEpoch: route.Epoch, ReplyEpoch: reply.MapEpoch,
				CopyTID: reply.Transaction, NextTID: stat.Transaction, PerOpResults: results,
				Before: before, After: after, AfterRefusals: afterRefusals, SourceAfter: sourceAfter, RefusedVectors: len(refused)})
		})
	}
}

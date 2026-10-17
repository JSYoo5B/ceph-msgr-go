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
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

// TestCephOSDCopyFromIntegration runs Go COPY_FROM/COPY_FROM2 on a disposable
// OSD and observes both objects through the independent native C API/CLI. The
// native client never executes copy-from here: librados' copy_from2 cannot set
// TRUNCATE_SEQ, so the independent evidence is the native-observed source and
// destination state, not a native twin execution.
func TestCephOSDCopyFromIntegration(t *testing.T) {
	fixture := osdFixture
	request := osdObjectRequest
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
		request = replicatedPGRequest
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	baseline := []byte{0, 0xff, 'c', 'o', 'p', 'y', 0, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}
	attrs := []nativeXattrEntry{{Name: []byte("copy-attr-a"), Value: []byte{0, 0xff, 'A'}}, {Name: []byte("copy-attr-b"), Value: []byte("bee")}}
	model := map[string][]byte{"a": []byte("a-value"), "b": {}, "\x00nul": {0xff}, "keep": []byte("k")}
	universe := []string{"", "a", "b", "\x00nul", "keep", "extra"}
	inventory := make([][]byte, len(universe))
	for i, key := range universe {
		inventory[i] = []byte(key)
	}
	header := []byte{9, 0, 0xff, 'h'}
	const reserved = 8
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed || len(result.Cleanup.Objects) != reserved+1 {
			t.Error("owned copy-from objects were not all removed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline, Attributes: attrs, Entries: omapWriteEntries(model)}, Reserve: reserved})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != reserved+1 || !bytes.Equal(owned.State.Data, baseline) {
		t.Fatal("native copy-from source seed differs", owned.SeedCode, owned.Objects)
	}
	src, absent := owned.Objects[0], owned.Objects[1:]
	routes := map[string]osdFixtureOracle{}
	connections := map[uint64]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	route := func(name string) (osdFixtureOracle, *cephmsgr.OSDConnection) {
		if r, ok := routes[name]; ok {
			return r, connections[uint64(r.ID)]
		}
		r := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		if r.Name != name || r.Pool != base.Pool || r.ID < 0 || r.Address == "" {
			t.Fatal("native placement differs", name, r)
		}
		routes[name] = r
		if connections[uint64(r.ID)] == nil {
			connections[uint64(r.ID)] = mutationConnection(t, c, ctx, r)
		}
		return r, connections[uint64(r.ID)]
	}
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: inventory}).State
	}
	send := func(name string, ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		target, connection := route(name)
		r := request(target, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000100}
		return connection.Request(ctx, r)
	}
	must := func(name string, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		reply, err := send(name, ops...)
		if err != nil {
			t.Fatal("Go setup request", name, err)
		}
		return reply
	}
	// Give the source an omap header through Go; native observation is the
	// reference for everything that follows.
	must(src, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}})
	source := observe(src)
	if source.Header == nil || !bytes.Equal(*source.Header, header) || !equalOmapModel(omapWriteInventory(t, source), model) {
		t.Fatal("native source state differs from the seeded inventory and Go header")
	}
	copyOp := func(code cephmsgr.OSDOpcode, name string, edit func(*cephmsgr.OSDCopyFromInput)) cephmsgr.OSDOperation {
		input := cephmsgr.OSDCopyFromInput{SourcePool: base.Pool, SourceName: name}
		if edit != nil {
			edit(&input)
		}
		return cephmsgr.OSDOperation{Code: code, CopyFrom: input}
	}
	sameState := func(t *testing.T, got, want omapWriteState) {
		t.Helper()
		if got.Code != 0 || !bytes.Equal(got.Data, want.Data) || !reflect.DeepEqual(got.Attributes, want.Attributes) || got.Header == nil || want.Header == nil || !bytes.Equal(*got.Header, *want.Header) || !equalOmapModel(omapWriteInventory(t, got), omapWriteInventory(t, want)) {
			t.Fatalf("destination differs from source\n got data=%x attrs=%v\nwant data=%x attrs=%v", got.Data, got.Attributes, want.Data, want.Attributes)
		}
	}
	type step struct {
		Name   string               `json:"name"`
		Object string               `json:"object"`
		Reply  cephmsgr.OSDReply    `json:"go_reply"`
		Error  string               `json:"go_error,omitempty"`
		After  omapWriteState       `json:"native_destination"`
		Source omapWriteState       `json:"native_source"`
		Extra  map[string]any       `json:"extra,omitempty"`
		Ops    []cephmsgr.OSDOpcode `json:"opcodes"`
	}
	var steps []step
	defer func() {
		raw, err := json.Marshal(steps)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-copyfrom-results.json"), raw)
		}
		if err != nil {
			t.Error("copy-from evidence", err)
		}
	}()
	record := func(name, object string, reply cephmsgr.OSDReply, err error, ops ...cephmsgr.OSDOperation) *step {
		s := step{Name: name, Object: object, Reply: reply, After: observe(object), Source: observe(src)}
		if err != nil {
			s.Error = err.Error()
		}
		for _, op := range ops {
			s.Ops = append(s.Ops, op.Code)
		}
		steps = append(steps, s)
		return &steps[len(steps)-1]
	}
	// A copy that fails after the daemon started it asynchronously reports
	// its aggregate code with the internal in-progress marker (-EINPROGRESS)
	// as the per-op result; a refusal before it starts (self copy) reports the
	// same code in both places.
	const inProgress = int32(-115)
	checkReceipt := func(t *testing.T, reply cephmsgr.OSDReply, object string, op cephmsgr.OSDOperation, result, opResult int32) {
		t.Helper()
		target := routes[object]
		got := reply.Operations[0]
		if reply.Transaction == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != object || reply.RetryAttempt != 0 || len(reply.Operations) != 1 || len(reply.RawData) != 0 ||
			got.Code != op.Code || got.Result != opResult || reply.Code != result || got.PayloadLength != 0 || len(got.Data) != 0 || got.CopyFromVersion != op.CopyFrom.SourceVersion || got.Offset != 0 || got.Length != 0 {
			t.Fatal("raw copy-from receipt differs", reply)
		}
		if reply.MapEpoch < target.Epoch {
			t.Fatal("reply epoch older than caller epoch", reply.MapEpoch, target.Epoch)
		}
		if result >= 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("successful copy lacks ONDISK")
		}
	}
	for _, tc := range []struct {
		name string
		code cephmsgr.OSDOpcode
		slot int
		// prefill populates the destination with differing data before copying.
		prefill bool
	}{
		{name: "COPY_FROM2 to an absent object", code: cephmsgr.OSDCopyFrom2, slot: 0},
		{name: "COPY_FROM to an absent object", code: cephmsgr.OSDCopyFrom, slot: 1},
		{name: "COPY_FROM2 replaces a populated object", code: cephmsgr.OSDCopyFrom2, slot: 2, prefill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := absent[tc.slot]
			if tc.prefill {
				must(dst,
					cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 5, Data: []byte("stale")},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "stale-attr"}, Data: []byte("x")},
					cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "extra", Value: []byte("e")}}}},
				)
			}
			op := copyOp(tc.code, src, nil)
			reply, err := send(dst, op)
			s := record(tc.name, dst, reply, err, op)
			if err != nil {
				t.Fatal(err)
			}
			checkReceipt(t, reply, dst, op, 0, 0)
			sameState(t, s.After, s.Source)
			if !reflect.DeepEqual(s.Source.Attributes, source.Attributes) || !bytes.Equal(s.Source.Data, source.Data) || s.Source.Version != source.Version {
				t.Fatal("copy altered the source")
			}
			if reply.UserVersion != s.After.Version {
				t.Fatal("receipt user version differs from native observation", reply.UserVersion, s.After.Version)
			}
		})
	}
	t.Run("source version", func(t *testing.T) {
		dst := absent[3]
		var codes []int32
		for _, delta := range []int64{1, 1000, -1} {
			version := uint64(int64(source.Version) + delta)
			op := copyOp(cephmsgr.OSDCopyFrom2, src, func(in *cephmsgr.OSDCopyFromInput) { in.SourceVersion = version })
			reply, err := send(dst, op)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code >= 0 {
				t.Fatal("mismatched source version was not a known server refusal", delta, err)
			}
			record(fmt.Sprintf("version %+d", delta), dst, reply, err, op)
			checkReceipt(t, reply, dst, op, server.Code, inProgress)
			if state := observe(dst); state.Code != -2 {
				t.Fatal("refused copy created the destination", delta, state.Code)
			}
			codes = append(codes, server.Code)
		}
		op := copyOp(cephmsgr.OSDCopyFrom2, src, func(in *cephmsgr.OSDCopyFromInput) { in.SourceVersion = source.Version })
		reply, err := send(dst, op)
		s := record("exact version", dst, reply, err, op)
		if err != nil {
			t.Fatal(err)
		}
		checkReceipt(t, reply, dst, op, 0, 0)
		sameState(t, s.After, s.Source)
		t.Logf("source version mismatch codes (+1, +1000, -1): %v", codes)
		s.Extra = map[string]any{"mismatch_codes": codes}
	})
	t.Run("known refusals leave the destination untouched", func(t *testing.T) {
		missing := absent[4]
		for _, tc := range []struct {
			name, object   string
			op             cephmsgr.OSDOperation
			want, opResult int32
		}{
			{name: "missing source", object: missing, op: copyOp(cephmsgr.OSDCopyFrom2, absent[5], nil), want: -2, opResult: inProgress},
			{name: "copy onto itself", object: src, op: copyOp(cephmsgr.OSDCopyFrom2, src, nil), want: -22, opResult: -22},
		} {
			reply, err := send(tc.object, tc.op)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != tc.want {
				t.Fatal(tc.name, "expected signed refusal", tc.want, err)
			}
			s := record(tc.name, tc.object, reply, err, tc.op)
			checkReceipt(t, reply, tc.object, tc.op, tc.want, tc.opResult)
			if tc.object == missing && s.After.Code != -2 {
				t.Fatal("missing-source copy created its destination", s.After.Code)
			}
			if tc.object == src && (!bytes.Equal(s.After.Data, source.Data) || s.After.Version != source.Version) {
				t.Fatal("self-copy altered the source")
			}
		}
	})
	t.Run("TRUNCATE_SEQ changes the destination truncate state", func(t *testing.T) {
		// A write carrying an older truncate sequence is trimmed only by an
		// object whose recorded sequence is newer. The source has sequence 0,
		// so the plain copy must accept it whole and the sequenced copy must not.
		plain, sequenced := absent[7], absent[6]
		for _, tc := range []struct {
			name, dst string
			edit      func(*cephmsgr.OSDCopyFromInput)
		}{
			{"COPY_FROM2 plain for truncate comparison", plain, nil},
			{"COPY_FROM2 with TRUNCATE_SEQ", sequenced, func(in *cephmsgr.OSDCopyFromInput) { in.TruncateSequence, in.TruncateSize = 5, 4 }},
		} {
			op := copyOp(cephmsgr.OSDCopyFrom2, src, tc.edit)
			reply, err := send(tc.dst, op)
			record(tc.name, tc.dst, reply, err, op)
			if err != nil {
				t.Fatal(err)
			}
			checkReceipt(t, reply, tc.dst, op, 0, 0)
		}
		write := func(dst string) (cephmsgr.OSDOperationResult, omapWriteState) {
			op := cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 40, Length: 8, Data: []byte("TRUNCATE"), TruncateSize: 100, TruncateSequence: 4}
			reply, err := send(dst, op)
			if err != nil {
				t.Fatal("older-sequence write", err)
			}
			return reply.Operations[0], observe(dst)
		}
		plainResult, plainState := write(plain)
		seqResult, seqState := write(sequenced)
		t.Logf("older-sequence write length plain=%d sequenced=%d, sizes %d/%d", plainResult.Length, seqResult.Length, len(plainState.Data), len(seqState.Data))
		// The source's sequence is 0, so the sequence-4 write is newer: it truncates
		// the object to the write's size (100) and applies in full.
		if plainResult.Length != 8 || len(plainState.Data) != 100 {
			t.Fatal("source truncate sequence was not inherited by a plain copy", plainResult.Length, len(plainState.Data))
		}
		// The sequenced copy keeps sequence 5, so the same write is old and
		// trimmed at the 32-byte object size, past which it writes nothing.
		if seqResult.Length != 0 || len(seqState.Data) != 32 {
			t.Fatal("TRUNCATE_SEQ copy did not install the supplied truncate sequence", seqResult.Length, len(seqState.Data))
		}
	})
}

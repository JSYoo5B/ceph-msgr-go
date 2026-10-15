package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// Compound class-call vectors the client admits (CALL after read-mode CALLs,
// CREATE before CALL, writes after a CALL) compared with an independent
// native librados client sending the same operations in the same order: a C
// write op (CREATE, OMAP set and rados_write_op_exec with a prval per CALL,
// RETURNVEC, explicit mtime) and, for CALL-only vectors, a C read op
// (rados_read_op_exec with a per-op output and prval, RETURNVEC) that the OSD
// executes as a write because a method is registered WR. Every native vector
// runs on its own mirror seeded with the Go object's current bytes and
// xattrs. Vectors with a CALL after a write-mode CALL are refused before
// transmission and are never sent, by Go or natively: a write method followed
// by a full-reading method made live Tentacle repair the primary copy in a
// loop without replying (the historical run in the validation record). Only
// the stock `hello` class and `lock.assert_locked` are used; the expected
// values restate the pinned src/cls/hello/cls_hello.cc,
// src/cls/lock/cls_lock.cc, src/osd/objclass.cc and src/osd/PrimaryLogPG.cc
// (do_osd_ops CREATE/CALL/STAT/SYNC_READ/OMAPSETVALS, execute_ctx reply
// bound) at 7f793731f1b39eb4f465e960113d2363c311b964 (LGPL-2.1 references; no
// code copied). See integration/fixture/OSD_CALL_COMPOUND_VALIDATION.md.

type compoundCallLeaf struct {
	name    string
	object  string
	ops     []cephmsgr.OSDOperation
	code    int32
	results []int32
	outputs [][]byte
	// lengths are the expected OutputLength of each CALL; nil means the
	// returned output length.
	lengths []uint64
	commits bool
	// cachedAdv admits the primary's cached object_info advance of a
	// refused successful vector (EOVERFLOW): data and xattrs stay unchanged.
	cachedAdv bool
	check     func(t *testing.T, after, writeOp omapWriteState, readOp *omapWriteState)
}

type compoundCallEvidence struct {
	Name    string             `json:"name"`
	Refused bool               `json:"refused_locally,omitempty"`
	Probe   *cephmsgr.OSDReply `json:"session_probe,omitempty"`
	Object  string             `json:"object"`
	Go      cephmsgr.OSDReply  `json:"go_reply"`
	GoError string             `json:"go_error,omitempty"`
	Before  omapWriteState     `json:"native_before"`
	After   omapWriteState     `json:"native_after"`
	WriteOp omapWriteReceipt   `json:"native_write_op"`
	ReadOp  *omapWriteReceipt  `json:"native_read_op,omitempty"`
}

// compoundCallOp declares the method's mode: read mode is the caller's
// declaration that the method is registered read-only.
func compoundCallOp(mode cephmsgr.OSDCallMode, class, method string, input []byte) cephmsgr.OSDOperation {
	return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 4096}}
}

func compoundCallAction(t *testing.T, op cephmsgr.OSDOperation) omapWriteAction {
	t.Helper()
	switch op.Code {
	case cephmsgr.OSDCreate:
		return omapWriteAction{Op: "create", Exclusive: op.Flags&cephmsgr.OSDExclusiveCreate != 0}
	case cephmsgr.OSDCall:
		return omapWriteAction{Op: "call", Class: op.Class.Class, Method: op.Class.Method, Input: bytes.Clone(op.Data)}
	case cephmsgr.OSDSetOmapValues:
		var entries []omapNativeEntry
		for _, e := range op.Omap.Entries {
			entries = append(entries, omapNativeEntry{Key: []byte(e.Key), Value: bytes.Clone(e.Value)})
		}
		return omapWriteAction{Op: "set", Entries: entries}
	}
	t.Fatal("compound oracle covers CREATE, OMAP set and CALL only", op.Code)
	return omapWriteAction{}
}

// compoundCallIdentityXattrs carry the requesting client's entity instance,
// which differs between the Go client and the native mirror's client.
var compoundCallIdentityXattrs = []string{"said_by", "amplified_by"}

// sameCompoundState compares bytes and xattrs of two objects written by
// different clients: identity-bearing values only need to exist in both.
func sameCompoundState(a, b omapWriteState) bool {
	if a.Code != b.Code || !bytes.Equal(a.Data, b.Data) {
		return false
	}
	x, y := callXattrs(a), callXattrs(b)
	if len(x) != len(y) {
		return false
	}
	for name, value := range x {
		other, ok := y[name]
		if !ok || !slices.Contains(compoundCallIdentityXattrs, name) && !bytes.Equal(value, other) {
			return false
		}
	}
	return true
}

// unchangedCompoundState admits only the EOVERFLOW cached object_info
// advance (newer user version, the refused request's mtime) when allowed.
func unchangedCompoundState(before, after omapWriteState, cachedAdv bool) bool {
	if reflect.DeepEqual(before, after) {
		return true
	}
	advanced := before
	advanced.Version, advanced.Seconds, advanced.Nanos = after.Version, after.Seconds, after.Nanos
	return cachedAdv && after.Version > before.Version && reflect.DeepEqual(advanced, after)
}

type compoundCallRunner struct {
	c           *cephmsgr.Client
	ctx         context.Context
	base        osdFixtureOracle
	connections map[int32]*cephmsgr.OSDConnection
	exists      map[string]bool
	tick        uint32
	// lastTransaction is the TID of the latest sent request; the client
	// allocates TIDs only for transmitted requests.
	lastTransaction uint64
	evidence        []compoundCallEvidence
}

func (r *compoundCallRunner) route(t *testing.T, name string) (osdFixtureOracle, *cephmsgr.OSDConnection) {
	t.Helper()
	want := int32(-2)
	if r.exists[name] {
		want = 0
	}
	routing := nativeMutation(t, r.ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
	selected := routing.Placement
	if routing.Actual.Code != want || selected.Name != name || selected.Pool != r.base.Pool || selected.ID < 0 || selected.Address == "" {
		t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
	}
	if r.connections[selected.ID] == nil {
		r.connections[selected.ID] = mutationConnection(t, r.c, r.ctx, selected)
	}
	return selected, r.connections[selected.ID]
}

func (r *compoundCallRunner) observe(t *testing.T, name string) omapWriteState {
	t.Helper()
	return nativeOmapWrite(t, r.ctx, r.base, "observe", omapWriteInput{Object: name}).State
}

// seed is the native mirror source for the Go object's current state.
func compoundCallSeed(before omapWriteState) *omapWriteSeed {
	if before.Code != 0 {
		return nil
	}
	return &omapWriteSeed{Baseline: before.Data, Attributes: slices.Clone(before.Attributes)}
}

func (r *compoundCallRunner) run(t *testing.T, tc compoundCallLeaf) {
	t.Run(tc.name, func(t *testing.T) {
		if len(tc.results) != len(tc.ops) || len(tc.outputs) != len(tc.ops) || tc.lengths != nil && len(tc.lengths) != len(tc.ops) {
			t.Fatal("leaf expectation shape differs from its vector")
		}
		selected, connection := r.route(t, tc.object)
		before := r.observe(t, tc.object)
		seed := compoundCallSeed(before)
		callOnly := true
		var actions []omapWriteAction
		for _, op := range tc.ops {
			actions = append(actions, compoundCallAction(t, op))
			callOnly = callOnly && op.Code == cephmsgr.OSDCall
		}
		r.tick++
		mtime := cephmsgr.OSDTime{Seconds: 1750000000 + r.tick, Nanoseconds: r.tick}
		writeOp := nativeOmapWrite(t, r.ctx, r.base, "compare", omapWriteInput{Seed: seed, Operations: actions, MTime: &omapWriteMTime{Seconds: mtime.Seconds, Nanos: mtime.Nanoseconds}})
		if writeOp.Aggregate == nil || !writeOp.Cleanup.Mirror {
			t.Fatal("native write-op mirror lacks an aggregate or was not removed")
		}
		var readOp *omapWriteReceipt
		if callOnly {
			receipt := nativeOmapWrite(t, r.ctx, r.base, "exec", omapWriteInput{Seed: seed, Operations: actions})
			if receipt.Vector == nil || len(receipt.Vector.Prvals) != len(tc.ops) || len(receipt.Vector.Outputs) != len(tc.ops) || !receipt.Cleanup.Mirror {
				t.Fatal("native read-op CALL vector lacks per-op receipts or its mirror")
			}
			readOp = &receipt
		}
		request := osdObjectRequest(selected, tc.ops...)
		// The client admits a modification time only on a vector it
		// classifies as a write: a write-mode CALL or a write operation. A
		// vector of read-mode CALLs goes without one, whatever the OSD then
		// executes.
		for _, op := range tc.ops {
			if op.Code != cephmsgr.OSDCall || op.Class.Mode == cephmsgr.OSDCallWrite {
				request.ModificationTime = mtime
			}
		}
		reply, err := connection.Request(r.ctx, request)
		if reply.Transaction != 0 {
			r.lastTransaction = reply.Transaction
		}
		e := compoundCallEvidence{Name: tc.name, Object: tc.object, Go: reply, Before: before, WriteOp: writeOp, ReadOp: readOp}
		if err != nil {
			e.GoError = err.Error()
		}
		defer func() { r.evidence = append(r.evidence, e) }()
		var server *cephmsgr.OSDError
		if tc.code < 0 {
			if !errors.As(err, &server) || server.Code != tc.code {
				t.Fatal("compound CALL server result differs", err)
			}
		} else if err != nil {
			t.Fatal("compound CALL request", err)
		}
		goResults, goOutputs := []int32{}, [][]byte{}
		for _, op := range reply.Operations {
			goResults, goOutputs = append(goResults, op.Result), append(goOutputs, op.Data)
		}
		t.Logf("go aggregate=%d results=%v outputs=%q flags=%#x version=%d; native write-op aggregate=%d prvals=%v", reply.Code, goResults, goOutputs, reply.Flags, reply.UserVersion, *writeOp.Aggregate, writeOp.Prvals)
		if readOp != nil {
			t.Logf("native read-op aggregate=%d prvals=%v outputs=%q", readOp.Vector.Aggregate, readOp.Vector.Prvals, readOp.Vector.Outputs)
		}
		if reply.Code != tc.code || *writeOp.Aggregate != tc.code || readOp != nil && readOp.Vector.Aggregate != tc.code {
			t.Fatalf("aggregate differs: go %d, native write op %d, want %d", reply.Code, *writeOp.Aggregate, tc.code)
		}
		if len(reply.Operations) != len(tc.ops) {
			t.Fatal("Go reply operation count differs", len(reply.Operations))
		}
		var calls []int
		for i, op := range reply.Operations {
			requested := tc.ops[i]
			if op.Code != requested.Code || op.Result != tc.results[i] || !bytes.Equal(op.Data, tc.outputs[i]) {
				t.Fatalf("Go op %d differs: result %d output %q, want %d %q", i, op.Result, op.Data, tc.results[i], tc.outputs[i])
			}
			if requested.Code != cephmsgr.OSDCall {
				continue
			}
			calls = append(calls, i)
			length := uint64(len(tc.outputs[i]))
			if tc.lengths != nil {
				length = tc.lengths[i]
			}
			if op.Class.ClassLength != uint8(len(requested.Class.Class)) || op.Class.MethodLength != uint8(len(requested.Class.Method)) ||
				op.Class.InputLength != uint32(len(requested.Data)) || op.Class.Argc != 0 || op.Class.OutputLength != length {
				t.Fatalf("Go op %d CALL union echo differs: %+v", i, op.Class)
			}
		}
		// librados exposes a prval for each CALL (none for CREATE).
		if len(writeOp.Prvals) != len(calls) {
			t.Fatal("native write-op prvals differ from the CALL positions", writeOp.Prvals)
		}
		for j, p := range writeOp.Prvals {
			if p.Index != calls[j] || p.Prval != tc.results[p.Index] {
				t.Fatalf("native write-op prval %d = %d, want %d", p.Index, p.Prval, tc.results[p.Index])
			}
		}
		if readOp != nil {
			for i, p := range readOp.Vector.Prvals {
				if p.Index != i || p.Prval != tc.results[i] || !bytes.Equal(readOp.Vector.Outputs[i], tc.outputs[i]) {
					t.Fatalf("native read-op op %d = %d %q, want %d %q", i, p.Prval, readOp.Vector.Outputs[i], tc.results[i], tc.outputs[i])
				}
			}
		}
		after := r.observe(t, tc.object)
		e.After = after
		mirrors := []omapWriteReceipt{writeOp}
		if readOp != nil {
			mirrors = append(mirrors, *readOp)
		}
		if tc.commits {
			r.exists[tc.object] = true
			if after.Code != 0 || before.Code == 0 && after.Version <= before.Version {
				t.Fatal("committed compound did not advance the native object version", before.Version, after.Version)
			}
			for _, m := range mirrors {
				if !sameCompoundState(after, m.After) || m.Before.Code == 0 && m.After.Version <= m.Before.Version {
					a, _ := json.Marshal(after)
					n, _ := json.Marshal(m.After)
					t.Fatalf("committed Go object differs from the native mirror\ngo=%s\nnative=%s", a, n)
				}
			}
		} else {
			if !unchangedCompoundState(before, after, tc.cachedAdv) {
				b, _ := json.Marshal(before)
				a, _ := json.Marshal(after)
				t.Fatalf("refused compound changed the Go object\nbefore=%s\nafter=%s", b, a)
			}
			for _, m := range mirrors {
				if !unchangedCompoundState(m.Before, m.After, tc.cachedAdv) || !sameCompoundState(before, m.Before) {
					t.Fatal("refused compound changed a native mirror", m.Mode)
				}
			}
		}
		if tc.check != nil {
			var read *omapWriteState
			if readOp != nil {
				read = &readOp.After
			}
			tc.check(t, after, writeOp.After, read)
		}
	})
}

// refuse sends a vector with a CALL after a write-mode CALL: the client must
// refuse it before transmission (no TID, no submission), the object must stay
// unchanged and the same session must serve the next request with the next
// TID. No native client sends these vectors.
func (r *compoundCallRunner) refuse(t *testing.T, name, object string, ops ...cephmsgr.OSDOperation) {
	t.Run(name, func(t *testing.T) {
		selected, connection := r.route(t, object)
		before := r.observe(t, object)
		r.tick++
		request := osdObjectRequest(selected, ops...)
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1750000000 + r.tick, Nanoseconds: r.tick}
		reply, refusal := connection.Request(r.ctx, request)
		e := compoundCallEvidence{Name: name, Object: object, Refused: true, Go: reply, Before: before}
		if refusal != nil {
			e.GoError = refusal.Error()
		}
		defer func() { r.evidence = append(r.evidence, e) }()
		if !errors.Is(refusal, cephmsgr.ErrUnsupportedOSDOperation) || reply.Transaction != 0 || reply.Submission != (cephmsgr.OSDSubmission{}) || len(reply.Operations) != 0 {
			t.Fatalf("CALL after a write-mode CALL was not refused locally: %v (tid %d, submission %+v)", refusal, reply.Transaction, reply.Submission)
		}
		probe, err := connection.Request(r.ctx, osdObjectRequest(selected, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}))
		e.Probe = &probe
		var server *cephmsgr.OSDError
		want := int32(-2)
		if r.exists[object] {
			want = 0
		}
		if want == 0 && err != nil || want != 0 && (!errors.As(err, &server) || server.Code != want) || probe.Code != want {
			t.Fatal("session did not serve a STAT after the local refusal", err)
		}
		if r.lastTransaction != 0 && probe.Transaction != r.lastTransaction+1 {
			t.Fatalf("the refused vector consumed a TID: probe %d after %d", probe.Transaction, r.lastTransaction)
		}
		r.lastTransaction = probe.Transaction
		after := r.observe(t, object)
		e.After = after
		if !reflect.DeepEqual(before, after) {
			t.Fatal("locally refused vector changed the object")
		}
		t.Logf("refused before transmission: %v; next STAT tid %d code %d", refusal, probe.Transaction, probe.Code)
	})
}

func compoundOmapPage(state omapWriteState) []omapNativeEntry {
	if state.Omap == nil || len(state.Omap.Operations) == 0 {
		return nil
	}
	return state.Omap.Operations[0].Entries
}

func TestCephOSDClassCallCompoundIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	seedData := []byte("hello world data")
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned native compound CALL cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: seedData}, Reserve: 4})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 5 || !bytes.Equal(owned.State.Data, seedData) || len(owned.State.Attributes) != 0 {
		t.Fatal("exclusive native compound CALL baseline differs")
	}
	main, fresh := owned.Objects[0], owned.Objects[1:]
	r := &compoundCallRunner{c: c, ctx: ctx, base: base, connections: map[int32]*cephmsgr.OSDConnection{}, exists: map[string]bool{main: true}}
	defer func() {
		for _, connection := range r.connections {
			connection.Close()
		}
		data, err := json.Marshal(r.evidence)
		if err == nil && len(data) <= 1<<20 {
			err = os.WriteFile(filepath.Join(dir, "osd-call-compound-results.json"), data, 0644)
		}
		if err != nil {
			t.Error("bounded compound CALL evidence", err)
		}
	}()
	create := cephmsgr.OSDOperation{Code: cephmsgr.OSDCreate}
	exclusive := cephmsgr.OSDOperation{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}
	reader := func(method string, input []byte) cephmsgr.OSDOperation {
		return compoundCallOp(cephmsgr.OSDCallRead, "hello", method, input)
	}
	writer := func(method string, input []byte) cephmsgr.OSDOperation {
		return compoundCallOp(cephmsgr.OSDCallWrite, "hello", method, input)
	}
	// say_hello and replay are registered RD only; record_hello and
	// write_return_data WR, turn_it_to_11 RD|WR; lock.assert_locked RD.
	sayHello := func(n int) (cephmsgr.OSDOperation, []byte) {
		input := bytes.Repeat([]byte("x"), n)
		if n == 0 {
			return reader("say_hello", nil), []byte("Hello, world!")
		}
		return reader("say_hello", input), []byte("Hello, " + string(input) + "!")
	}
	writeReturn, writeOutput := writer("write_return_data", nil), []byte("you might see this")
	assertLocked := func(mode cephmsgr.OSDCallMode) cephmsgr.OSDOperation {
		return compoundCallOp(mode, "lock", "assert_locked", rbdLockAssertInput("ceph-msgr-compound", ""))
	}
	foo := func(t *testing.T, after, writeOp omapWriteState, readOp *omapWriteState) {
		t.Helper()
		if !bytes.Equal(callXattrs(after)["foo"], []byte("bar")) || !bytes.Equal(callXattrs(writeOp)["foo"], []byte("bar")) {
			t.Fatal("write_return_data xattr foo=bar was not committed like the native mirror")
		}
	}
	noFoo := func(t *testing.T, after, writeOp omapWriteState, readOp *omapWriteState) {
		t.Helper()
		if _, ok := callXattrs(after)["foo"]; ok {
			t.Fatal("unexecuted or refused write_return_data committed its xattr")
		}
	}
	short, shortOut := sayHello(0)
	fifty, fiftyOut := sayHello(50)
	sixty, sixtyOut := sayHello(60)
	invalid, _ := sayHello(101)
	omapSet := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "compound-key", Value: []byte("compound-value")}}}}

	// An exclusive CREATE of an existing object fails first; the method is
	// not executed.
	r.run(t, compoundCallLeaf{name: "exclusive create of an existing object aborts the method", object: main, ops: []cephmsgr.OSDOperation{exclusive, writer("record_hello", nil)},
		code: -17, results: []int32{-17, 0}, outputs: [][]byte{nil, nil}})
	// A read-only method declared in write mode counts as a write-mode CALL.
	r.refuse(t, "write-mode lock assertion before a method is refused", main, assertLocked(cephmsgr.OSDCallWrite), writer("turn_it_to_11", nil))
	if t.Failed() {
		return
	}
	for _, tc := range []compoundCallLeaf{
		// say_hello refuses input above 100 bytes before writing output.
		{name: "failing first read method aborts a writing method", object: main, ops: []cephmsgr.OSDOperation{invalid, writeReturn},
			code: -22, results: []int32{-22, 0}, outputs: [][]byte{nil, nil}, check: noFoo},
		// cls_lock: an object without the lock xattr has no lockers (-EBUSY).
		{name: "failed read-mode lock assertion aborts a data-writing method", object: main, ops: []cephmsgr.OSDOperation{assertLocked(cephmsgr.OSDCallRead), writer("turn_it_to_11", nil)},
			code: -16, results: []int32{-16, 0}, outputs: [][]byte{nil, nil}},
		// osd_max_write_op_reply_len bounds each op: only the 68-byte output
		// becomes -EOVERFLOW; the other op keeps its result and output, and
		// nothing is committed.
		{name: "per-op reply bound refuses only the long output", object: main, ops: []cephmsgr.OSDOperation{sixty, writeReturn},
			code: -75, results: []int32{-75, 42}, outputs: [][]byte{nil, writeOutput}, lengths: []uint64{uint64(len(sixtyOut)), uint64(len(writeOutput))}, cachedAdv: true, check: noFoo},
	} {
		r.run(t, tc)
		if t.Failed() {
			return
		}
	}
	// A write method followed by any CALL is refused before transmission,
	// including the vector that made the OSD repair the primary in a loop.
	for _, tc := range []struct {
		name string
		ops  []cephmsgr.OSDOperation
	}{
		{"full rewrite then full read is refused", []cephmsgr.OSDOperation{writer("turn_it_to_11", nil), reader("replay", nil)}},
		{"write method then read method is refused", []cephmsgr.OSDOperation{writeReturn, short}},
		{"read, write, read methods are refused", []cephmsgr.OSDOperation{short, writeReturn, short}},
	} {
		r.refuse(t, tc.name, main, tc.ops...)
		if t.Failed() {
			return
		}
	}
	for _, tc := range []compoundCallLeaf{
		// The aggregate is the last executed op's return.
		{name: "two outputs within the reply bound", object: main, ops: []cephmsgr.OSDOperation{short, writeReturn},
			code: 42, results: []int32{0, 42}, outputs: [][]byte{shortOut, writeOutput}, commits: true, check: foo},
		{name: "outputs above the bound in total but not per op", object: main, ops: []cephmsgr.OSDOperation{fifty, writeReturn},
			code: 42, results: []int32{0, 42}, outputs: [][]byte{fiftyOut, writeOutput}, commits: true, check: foo},
		// A write after a CALL is admitted; OMAPSETVALS returns 0, so the
		// aggregate is 0 while the method's own result stays 42.
		{name: "aggregate is the last executed operation's return", object: main, ops: []cephmsgr.OSDOperation{writeReturn, omapSet},
			code: 0, results: []int32{42, 0}, outputs: [][]byte{writeOutput, nil}, commits: true,
			check: func(t *testing.T, after, writeOp omapWriteState, readOp *omapWriteState) {
				foo(t, after, writeOp, readOp)
				want := []omapNativeEntry{{Key: []byte("compound-key"), Value: []byte("compound-value")}}
				if !equalOmapEntries(compoundOmapPage(after), want) || !equalOmapEntries(compoundOmapPage(writeOp), want) {
					t.Fatal("OMAP value after the CALL was not committed like the native mirror", compoundOmapPage(after), compoundOmapPage(writeOp))
				}
			}},
		// record_hello's cls_cxx_stat sees the staged CREATE (ctx->new_obs).
		{name: "exclusive create is visible to the method's stat", object: fresh[0], ops: []cephmsgr.OSDOperation{exclusive, writer("record_hello", nil)},
			code: -17, results: []int32{0, -17}, outputs: [][]byte{nil, nil}},
		{name: "plain create is visible to the method's stat", object: fresh[1], ops: []cephmsgr.OSDOperation{create, writer("record_hello", nil)},
			code: -17, results: []int32{0, -17}, outputs: [][]byte{nil, nil}},
	} {
		r.run(t, tc)
		if t.Failed() {
			return
		}
	}
	r.refuse(t, "staged initializer then read method is refused", fresh[2], writer("record_hello", nil), reader("replay", nil))
	if t.Failed() {
		return
	}
	leaves := []compoundCallLeaf{
		// Read mode is only the caller's declaration: the OSD classifies by
		// registration, so record_hello declared read-only is admitted and
		// still stages its write. replay's SYNC_READ is trimmed to the
		// staged size but reads the committed store, where the object does
		// not exist yet; nothing is committed. Safe only because the object
		// is absent (no committed bytes to check against a staged digest).
		{name: "write method declared read-only still runs as a write", object: fresh[2], ops: []cephmsgr.OSDOperation{reader("record_hello", nil), reader("replay", nil)},
			code: -2, results: []int32{0, -2}, outputs: [][]byte{nil, nil}},
		{name: "exclusive create then a method that does not stat", object: fresh[3], ops: []cephmsgr.OSDOperation{exclusive, writeReturn},
			code: 42, results: []int32{0, 42}, outputs: [][]byte{nil, writeOutput}, commits: true,
			check: func(t *testing.T, after, writeOp omapWriteState, readOp *omapWriteState) {
				foo(t, after, writeOp, readOp)
				if len(after.Data) != 0 || len(writeOp.Data) != 0 {
					t.Fatal("create and write_return_data wrote object bytes")
				}
			}},
	}
	for _, tc := range leaves {
		r.run(t, tc)
		if t.Failed() {
			return
		}
	}
}

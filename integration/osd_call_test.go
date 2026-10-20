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
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

func callXattrs(state omapWriteState) map[string][]byte {
	result := map[string][]byte{}
	for _, entry := range state.Attributes {
		result[string(entry.Name)] = entry.Value
	}
	return result
}

func TestCephOSDClassCallIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
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
			t.Error("owned native CALL cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: seedData}, Reserve: 3})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 4 || !bytes.Equal(owned.State.Data, seedData) {
		t.Fatal("exclusive native CALL baseline differs")
	}
	main, absent, created, readMode := owned.Objects[0], owned.Objects[1], owned.Objects[2], owned.Objects[3]
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	route := func(c *cephmsgr.Client, name string, want int32) (osdFixtureOracle, *cephmsgr.OSDConnection) {
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
	call := func(mode cephmsgr.OSDCallMode, class, method string, input []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDCall, Data: input, Class: cephmsgr.OSDClassInput{Class: class, Method: method, Mode: mode, MaxOutputBytes: 4096}}
	}
	action := func(op cephmsgr.OSDOperation) omapWriteAction {
		return omapWriteAction{Op: "call", Flags: uint32(op.Flags), Class: op.Class.Class, Method: op.Class.Method, Input: append([]byte{}, op.Data...)}
	}
	type caseEvidence struct {
		Name   string            `json:"name"`
		Native omapWriteReceipt  `json:"native"`
		Go     cephmsgr.OSDReply `json:"go_reply"`
		Before omapWriteState    `json:"native_before"`
		After  omapWriteState    `json:"native_after"`
	}
	var evidence []caseEvidence
	defer func() {
		data, err := json.Marshal(evidence)
		if err == nil && len(data) <= 512<<10 {
			err = os.WriteFile(filepath.Join(dir, "osd-call-results.json"), data, 0644)
		}
		if err != nil {
			t.Error("bounded CALL evidence", err)
		}
	}()
	exists := map[string]bool{main: true}
	tick := uint32(0)
	type step struct {
		name      string
		object    string
		op        cephmsgr.OSDOperation
		code      int32
		result    int32
		output    []byte
		onObject  bool // native read-op CALL on the owned object (read methods)
		commits   bool
		cachedAdv bool // admit the EOVERFLOW cached object_info advance
		check     func(t *testing.T, before, after, mirrorAfter omapWriteState)
	}
	run := func(tc step) {
		t.Run(tc.name, func(t *testing.T) {
			want := int32(-2)
			if exists[tc.object] {
				want = 0
			}
			selected, connection := route(c, tc.object, want)
			before := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object}).State
			var native omapWriteReceipt
			if tc.onObject {
				native = nativeOmapWrite(t, ctx, base, "exec", omapWriteInput{Object: tc.object, Operations: []omapWriteAction{action(tc.op)}})
			} else {
				input := omapWriteInput{Operations: []omapWriteAction{action(tc.op)}}
				if exists[tc.object] {
					var attrs []nativeXattrEntry
					for _, entry := range before.Attributes {
						attrs = append(attrs, entry)
					}
					input.Seed = &omapWriteSeed{Baseline: before.Data, Attributes: attrs}
				}
				native = nativeOmapWrite(t, ctx, base, "exec", input)
				if !native.Cleanup.Mirror {
					t.Fatal("native CALL mirror was not removed")
				}
			}
			if native.Vector == nil || len(native.Vector.Prvals) != 1 {
				t.Fatal("native CALL lacks a vector receipt")
			}
			request := osdObjectRequest(selected, tc.op)
			if tc.op.Class.Mode == cephmsgr.OSDCallWrite {
				tick++
				request.ModificationTime = cephmsgr.OSDTime{Seconds: 1740000000 + tick, Nanoseconds: tick}
			}
			reply, err := connection.Request(ctx, request)
			var server *cephmsgr.OSDError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code {
					t.Fatal("known CALL server result differs", err)
				}
			} else if err != nil {
				t.Fatal("Go CALL request", err)
			}
			op := reply.Operations[0]
			t.Logf("go aggregate=%d result=%d output=%q outputLength=%d flags=%#x; native aggregate=%d prval=%d output=%q", reply.Code, op.Result, op.Data, op.Class.OutputLength, reply.Flags, native.Vector.Aggregate, native.Vector.Prvals[0].Prval, native.Vector.Output)
			if reply.Code != tc.code || native.Vector.Aggregate != tc.code || op.Result != tc.result || !bytes.Equal(op.Data, tc.output) || !bytes.Equal(native.Vector.Output, tc.output) {
				t.Fatalf("Go/native CALL result or output differs from the expected native semantics")
			}
			if exists[tc.object] && native.Vector.Prvals[0].Prval != op.Result {
				t.Fatal("Go per-op CALL result differs from native C prval")
			}
			if op.Class.ClassLength != uint8(len(tc.op.Class.Class)) || op.Class.MethodLength != uint8(len(tc.op.Class.Method)) || op.Class.InputLength != uint32(len(tc.op.Data)) {
				t.Fatal("CALL reply union echo differs", op.Class)
			}
			after := nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: tc.object}).State
			if tc.commits {
				exists[tc.object] = true
				if after.Code != 0 || after.Version <= before.Version {
					t.Fatal("committed CALL did not advance the native object version", before.Version, after.Version)
				}
			} else if !reflect.DeepEqual(before, after) {
				// The cached user_version is the PG's last_user_version + 1
				// (pinned finish_ctx), which native mirror writes in the same PG
				// may already have advanced; only data and xattrs must match.
				advanced := before
				advanced.Version, advanced.Seconds, advanced.Nanos = after.Version, int64(request.ModificationTime.Seconds), int64(request.ModificationTime.Nanoseconds)
				if !tc.cachedAdv || after.Version <= before.Version || !reflect.DeepEqual(advanced, after) {
					b, _ := json.Marshal(before)
					a, _ := json.Marshal(after)
					t.Fatalf("refused or read-only CALL changed native state\nbefore=%s\nafter=%s", b, a)
				}
				t.Log("primary cached object_info advanced to the refused request's version/mtime; data and xattrs unchanged")
			}
			if tc.check != nil {
				tc.check(t, before, after, native.After)
			}
			evidence = append(evidence, caseEvidence{Name: tc.name, Native: native, Go: reply, Before: before, After: after})
		})
	}
	identity := func(t *testing.T, value []byte) {
		t.Helper()
		if !bytes.Contains(value, []byte(fmt.Sprintf("client.%d", c.Snapshot().GlobalID))) {
			t.Fatalf("class method origin %q does not name the Go client's global ID", value)
		}
	}
	steps := []step{
		{name: "read method default output", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "say_hello", nil), output: []byte("Hello, world!")},
		{name: "read method binary-free input", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "say_hello", []byte("Tester")), output: []byte("Hello, Tester!")},
		{name: "read method refuses large input", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "say_hello", bytes.Repeat([]byte("x"), 101)), code: -22, result: -22},
		{name: "read method returns object bytes", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "replay", nil), output: seedData},
		{name: "unknown method refused before execution", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "no_such_method", nil), code: -95},
		{name: "class outside the load list refused", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "nosuchcls", "method", nil), code: -1},
		{name: "write method that reads fails with EIO", object: main, op: call(cephmsgr.OSDCallWrite, "hello", "bad_reader", nil), code: -5, result: -5},
		{name: "read method that writes fails with EIO", object: main, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "bad_writer", []byte("x")), code: -5, result: -5},
		{name: "read method on absent object is ENOENT", object: absent, op: call(cephmsgr.OSDCallRead, "hello", "say_hello", nil), code: -2},
		{name: "write method positive result and output commit", object: main, op: call(cephmsgr.OSDCallWrite, "hello", "write_return_data", nil), code: 42, result: 42, output: []byte("you might see this"), commits: true,
			check: func(t *testing.T, before, after, mirror omapWriteState) {
				if !bytes.Equal(callXattrs(after)["foo"], []byte("bar")) || !bytes.Equal(callXattrs(mirror)["foo"], []byte("bar")) || !bytes.Equal(after.Data, seedData) {
					t.Fatal("committed write method xattr/data differs from native mirror")
				}
			}},
		{name: "write method error output is returned without commit", object: main, op: call(cephmsgr.OSDCallWrite, "hello", "write_return_data", []byte("x")), code: -22, result: -22, output: []byte("too much input data!")},
		{name: "write output above the reply bound is EOVERFLOW", object: main, op: call(cephmsgr.OSDCallWrite, "hello", "write_too_much_return_data", nil), code: -75, result: -75, cachedAdv: true},
		{name: "write method creates an absent object", object: created, op: call(cephmsgr.OSDCallWrite, "hello", "record_hello", nil), commits: true,
			check: func(t *testing.T, before, after, mirror omapWriteState) {
				if !bytes.Equal(after.Data, []byte("Hello, world!")) || !bytes.Equal(mirror.Data, after.Data) {
					t.Fatal("record_hello object bytes differ from native mirror", after.Data, mirror.Data)
				}
				identity(t, callXattrs(after)["said_by"])
			}},
		{name: "write method refuses an existing object", object: created, op: call(cephmsgr.OSDCallWrite, "hello", "record_hello", nil), code: -17, result: -17},
		{name: "write method mutates existing bytes", object: created, op: call(cephmsgr.OSDCallWrite, "hello", "turn_it_to_11", nil), commits: true,
			check: func(t *testing.T, before, after, mirror omapWriteState) {
				if !bytes.Equal(after.Data, []byte("HELLO, WORLD!")) || !bytes.Equal(mirror.Data, after.Data) {
					t.Fatal("turn_it_to_11 bytes differ from native mirror", after.Data, mirror.Data)
				}
				identity(t, callXattrs(after)["amplified_by"])
			}},
		{name: "read method replays the mutated bytes", object: created, onObject: true, op: call(cephmsgr.OSDCallRead, "hello", "replay", nil), output: []byte("HELLO, WORLD!")},
		{name: "read-mode request to a write method still writes", object: readMode, op: call(cephmsgr.OSDCallRead, "hello", "record_hello", []byte("reader")), commits: true,
			check: func(t *testing.T, before, after, mirror omapWriteState) {
				if !bytes.Equal(after.Data, []byte("Hello, reader!")) || !bytes.Equal(mirror.Data, after.Data) {
					t.Fatal("read-mode record_hello did not execute as a write", after.Data, mirror.Data)
				}
			}},
	}
	for _, tc := range steps {
		run(tc)
		if t.Failed() {
			return
		}
	}
	t.Run("readonly capability refusal", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD, options.Identity = true, "client.readonly"
		key, err := os.ReadFile(filepath.Join(dir, "readonly.key"))
		if err != nil {
			t.Fatal(err)
		}
		options.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
		clear(key)
		if err != nil {
			t.Fatal("parse fixture readonly key", err)
		}
		reader, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("authenticate explicit readonly identity", err)
		}
		defer reader.Close()
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": main, "namespace": ""})
		restricted := mutationConnection(t, reader, ctx, routing.Placement)
		defer restricted.Close()
		op := call(cephmsgr.OSDCallRead, "hello", "say_hello", nil)
		native := nativeOmapWrite(t, ctx, base, "exec", omapWriteInput{Object: main, Role: "readonly", Operations: []omapWriteAction{action(op)}})
		reply, err := restricted.Request(ctx, osdObjectRequest(routing.Placement, op))
		var server *cephmsgr.OSDError
		if native.Vector == nil || native.Vector.Aggregate != -1 || !errors.As(err, &server) || server.Code != -1 {
			t.Fatal("readonly CALL refusal differs from native", native.Vector, err)
		}
		if stat, err := restricted.Request(ctx, osdObjectRequest(routing.Placement, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})); err != nil || stat.Code != 0 {
			t.Fatal("readonly OSD session did not remain usable after the CALL refusal", err)
		}
		evidence = append(evidence, caseEvidence{Name: "readonly capability refusal", Native: native, Go: reply})
	})
}

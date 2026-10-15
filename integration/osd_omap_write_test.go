package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type omapWriteState struct {
	omapNativeSnapshot
	Header *[]byte `json:"header"`
}

type omapWriteAction struct {
	Op         string            `json:"op"`
	Flags      uint32            `json:"flags"`
	Entries    []omapNativeEntry `json:"entries,omitempty"`
	Keys       [][]byte          `json:"keys,omitempty"`
	Key        []byte            `json:"key,omitempty"`
	Value      []byte            `json:"value,omitempty"`
	Comparison *int              `json:"comparison,omitempty"`
	Begin      []byte            `json:"begin,omitempty"`
	End        []byte            `json:"end,omitempty"`
	Class      string            `json:"class,omitempty"`
	Method     string            `json:"method,omitempty"`
	Input      []byte            `json:"input,omitempty"`
}

type omapNativePrval struct {
	Index int   `json:"index"`
	Prval int32 `json:"prval"`
}

type omapWriteSeed struct {
	Baseline   []byte             `json:"baseline"`
	Attributes []nativeXattrEntry `json:"attributes,omitempty"`
	Entries    []omapNativeEntry  `json:"entries,omitempty"`
}

type omapWriteInput struct {
	Seed          *omapWriteSeed    `json:"seed,omitempty"`
	Reserve       int               `json:"reserve,omitempty"`
	Object        string            `json:"object,omitempty"`
	Role          string            `json:"role,omitempty"`
	Operations    []omapWriteAction `json:"operations,omitempty"`
	MTime         *omapWriteMTime   `json:"mtime,omitempty"`
	InventoryKeys [][]byte          `json:"inventory_keys,omitempty"`
}

type omapWriteMTime struct {
	Seconds uint32 `json:"seconds"`
	Nanos   uint32 `json:"nanos"`
}

type omapWriteReceipt struct {
	Schema        int    `json:"schema_version"`
	Commit        string `json:"source_commit"`
	Mode          string `json:"mode"`
	DaemonVersion string `json:"fixture_daemon_version"`
	Fixture       struct {
		Pool   string `json:"pool"`
		ID     int64  `json:"pool_id"`
		Object string `json:"object"`
	} `json:"fixture"`
	Scope struct {
		Independent bool `json:"independent_native_c_api"`
		HeaderCLI   bool `json:"header_via_pinned_rados_cli"`
		RawWire     bool `json:"raw_wire_payload_available"`
		PerOp       bool `json:"native_write_per_op_results_available"`
		CmpPrval    bool `json:"native_cmp_per_op_prval_available"`
	} `json:"scope"`
	Objects   []string          `json:"objects"`
	Object    string            `json:"object"`
	SeedCode  *int32            `json:"seed_aggregate"`
	State     omapWriteState    `json:"state"`
	Before    omapWriteState    `json:"before"`
	After     omapWriteState    `json:"after"`
	Aggregate *int32            `json:"aggregate"`
	Prvals    []omapNativePrval `json:"prvals"`
	Vector    *struct {
		Aggregate int32             `json:"aggregate"`
		Prvals    []omapNativePrval `json:"prvals"`
		Output    []byte            `json:"output"`
	} `json:"vector"`
	Cleanup struct {
		Objects []struct {
			Object string `json:"object"`
			Code   int32  `json:"code"`
		} `json:"objects"`
		Removed bool   `json:"objects_removed"`
		Mirror  bool   `json:"mirror_removed"`
		Code    *int32 `json:"code"`
	} `json:"cleanup"`
}

func nativeOmapWrite(t *testing.T, ctx context.Context, base osdFixtureOracle, mode string, input omapWriteInput) omapWriteReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(input)
	if err != nil || len(data) > 1<<20 {
		t.Fatal("bounded OMAP write native input", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "osd-omap-write-input.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "osd-omap-write-result.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-omap-write", mode); err != nil {
		t.Fatal("native OMAP write "+mode, err)
	}
	f, err := os.Open(filepath.Join(dir, "osd-omap-write-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err = io.ReadAll(io.LimitReader(f, (512<<10)+1))
	if err != nil || len(data) > 512<<10 {
		t.Fatal("OMAP write native receipt exceeds bound", err)
	}
	var result omapWriteReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	const source = "7f793731f1b39eb4f465e960113d2363c311b964"
	if result.Schema != 1 || result.Commit != source || result.Mode != mode || result.Fixture.ID != base.Pool || result.Fixture.Object != base.Name || !strings.Contains(result.DaemonVersion, source) || !strings.Contains(result.DaemonVersion, "20.2.4") {
		t.Fatal("native OMAP write receipt provenance differs")
	}
	if !result.Scope.Independent || !result.Scope.HeaderCLI || result.Scope.RawWire || result.Scope.PerOp || !result.Scope.CmpPrval {
		t.Fatal("native OMAP write oracle fabricated wire or per-op evidence")
	}
	return result
}

// Interpret native inventories only in this consumer test. BYKEYS2 covers the
// empty key and every known key; the bounded page proves no unknown nonempty key.
func omapWriteInventory(t *testing.T, state omapWriteState) map[string][]byte {
	t.Helper()
	if state.Code != 0 || state.Omap == nil || len(state.Omap.Operations) != 2 || state.Header == nil {
		t.Fatal("native OMAP write state lacks an existing object inventory/header")
	}
	page, keyed := state.Omap.Operations[0], state.Omap.Operations[1]
	if page.More == nil || *page.More {
		t.Fatal("native OMAP inventory page is incomplete")
	}
	result := map[string][]byte{}
	var nonempty []omapNativeEntry
	for _, entry := range keyed.Entries {
		result[string(entry.Key)] = entry.Value
		if len(entry.Key) != 0 {
			nonempty = append(nonempty, entry)
		}
	}
	if !equalOmapEntries(page.Entries, nonempty) {
		t.Fatalf("native OMAP page has keys outside the known inventory: %+v / %+v", page.Entries, keyed.Entries)
	}
	return result
}

func omapWriteEntries(values map[string][]byte) []omapNativeEntry {
	result := make([]omapNativeEntry, 0, len(values))
	for key, value := range values {
		result = append(result, omapNativeEntry{Key: []byte(key), Value: bytes.Clone(value)})
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Key, result[j].Key) < 0 })
	return result
}

// Compare bytes rather than nil/empty slice identity from separate decoders.
func equalOmapEntries(got, want []omapNativeEntry) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			return false
		}
	}
	return true
}

func equalOmapModel(got, want map[string][]byte) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		stored, ok := got[key]
		if !ok || !bytes.Equal(stored, value) {
			return false
		}
	}
	return true
}

func TestCephOSDOmapWritesIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	baseline := []byte{0, 0xff, 'o', 'm', 'a', 'p', 'w', 0, 10}
	attrs := []nativeXattrEntry{{Name: []byte("omapw-fixture-binary"), Value: []byte{0, 0xff, 'W'}}}
	model := map[string][]byte{"a": []byte("a-value"), "b": {}, "\x00nul": {0xff}, "keep": []byte("k")}
	universe := []string{"", "a", "b", "\x00nul", "keep", "\xffhigh", "new-empty", "c", "rollback", "created", "never"}
	inventory := make([][]byte, len(universe))
	for i, key := range universe {
		inventory[i] = []byte(key)
	}
	seed := &omapWriteSeed{Baseline: baseline, Attributes: attrs, Entries: omapWriteEntries(model)}
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{})
		if !result.Cleanup.Removed || len(result.Cleanup.Objects) != 4 {
			t.Error("owned native OMAP write cleanup failed", result.Cleanup)
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: seed, Reserve: 3})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 4 || owned.State.Version == 0 || !bytes.Equal(owned.State.Data, baseline) || !reflect.DeepEqual(owned.State.Attributes, attrs) || owned.State.Header == nil || len(*owned.State.Header) != 0 {
		t.Fatal("exclusive native OMAP write baseline differs", owned.SeedCode, owned.Objects)
	}
	if !equalOmapModel(omapWriteInventory(t, owned.State), model) {
		t.Fatal("exclusive native OMAP seed inventory differs")
	}
	placements := map[string]osdFixtureOracle{}
	connections := map[string]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	route := func(name string, exists bool) (osdFixtureOracle, *cephmsgr.OSDConnection) {
		if selected, ok := placements[name]; ok {
			return selected, connections[fmt.Sprint(selected.ID)]
		}
		routing := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
		selected := routing.Placement
		wantCode := int32(-2)
		if exists {
			wantCode = 0
		}
		if routing.Actual.Code != wantCode || selected.Name != name || selected.Namespace != "" || selected.Pool != base.Pool || selected.Epoch < base.Epoch || selected.ID < 0 || selected.Address == "" || selected.RawPGID != fmt.Sprintf("%d.%x", selected.Pool, selected.Hash) || selected.PGID != fmt.Sprintf("%d.%x", selected.Pool, selected.PG) {
			t.Fatal("caller-owned native placement/existence differs", name, routing.Actual.Code)
		}
		acting := false
		for _, id := range selected.Acting {
			acting = acting || id == selected.ID
		}
		if !acting {
			t.Fatal("explicit primary absent from acting set", name)
		}
		t.Logf("caller placement object=%s rawPG=%s mappedPG=%s epoch=%d primary=%d endpoint=%s", name, selected.RawPGID, selected.PGID, selected.Epoch, selected.ID, selected.Address)
		placements[name] = selected
		key := fmt.Sprint(selected.ID)
		if connections[key] == nil {
			connections[key] = mutationConnection(t, c, ctx, selected)
		}
		return selected, connections[key]
	}
	main, absent := owned.Objects[0], owned.Objects[1:]
	headers := map[string][]byte{main: {}}
	models := map[string]map[string][]byte{main: model}
	observe := func(name string) omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: name, InventoryKeys: inventory}).State
	}
	type stepEvidence struct {
		Name        string           `json:"name"`
		Placement   osdFixtureOracle `json:"caller_placement"`
		Before      omapWriteState   `json:"native_before"`
		Go          cephmsgr.OSDReply
		After       omapWriteState          `json:"native_after"`
		Mirror      *omapWriteReceipt       `json:"native_mirror,omitempty"`
		Observation string                  `json:"observation,omitempty"`
		MTime       cephmsgr.OSDTime        `json:"mtime"`
		Ops         []cephmsgr.OSDOperation `json:"operations"`
	}
	evidence := struct {
		Schema int            `json:"schema_version"`
		Source string         `json:"source_commit"`
		Owned  []string       `json:"owned_objects"`
		Steps  []stepEvidence `json:"steps"`
	}{Schema: 1, Source: owned.Commit, Owned: owned.Objects}
	defer func() {
		data, err := json.Marshal(evidence)
		if err != nil || len(data) > 512<<10 {
			t.Error("bounded OMAP write acceptance evidence", err, len(data))
			return
		}
		if err := os.WriteFile(filepath.Join(dir, "osd-omap-write-results.json"), data, 0644); err != nil {
			t.Error(err)
		}
	}()
	set := func(entries ...cephmsgr.OSDOmapEntry) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: entries}}
	}
	remove := func(keys ...string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveOmapKeys, Omap: cephmsgr.OSDOmapInput{Keys: keys}}
	}
	setHeader := func(header []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}}
	}
	getHeader := cephmsgr.OSDOperation{Code: cephmsgr.OSDGetOmapHeader, Omap: cephmsgr.OSDOmapInput{MaxOutputBytes: 64 << 10}}
	clear := cephmsgr.OSDOperation{Code: cephmsgr.OSDClearOmap}
	// mirror converts the C-expressible part of a Go vector for an
	// independently constructed native C write vector on a disposable mirror.
	mirror := func(ops []cephmsgr.OSDOperation) []omapWriteAction {
		var actions []omapWriteAction
		for _, op := range ops {
			action := omapWriteAction{Flags: uint32(op.Flags)}
			switch op.Code {
			case cephmsgr.OSDSetOmapValues:
				action.Op = "set"
				for _, entry := range op.Omap.Entries {
					action.Entries = append(action.Entries, omapNativeEntry{Key: []byte(entry.Key), Value: append([]byte{}, entry.Value...)})
				}
				if action.Entries == nil {
					action.Entries = []omapNativeEntry{}
				}
			case cephmsgr.OSDRemoveOmapKeys:
				action.Op = "remove"
				for _, key := range op.Omap.Keys {
					action.Keys = append(action.Keys, []byte(key))
				}
			case cephmsgr.OSDClearOmap:
				action.Op = "clear"
			default:
				t.Fatal("native C mirror cannot express opcode", op.Code)
			}
			actions = append(actions, action)
		}
		return actions
	}
	type step struct {
		name    string
		object  string
		ops     []cephmsgr.OSDOperation
		code    int32
		results []int32 // nil means all zero
		reads   func(t *testing.T, reply cephmsgr.OSDReply, before omapWriteState)
		apply   func(object string) // model update after success
		mirror  *omapWriteSeed      // nonnil: compare with a seeded native mirror
		absent  bool                // mirror starts absent
		// cachedAdvance admits only the Tentacle EOVERFLOW cache anomaly.
		cachedAdvance bool
	}
	tick := uint32(0)
	run := func(tc step) {
		t.Run(tc.name, func(t *testing.T) {
			exists := models[tc.object] != nil
			selected, connection := route(tc.object, exists)
			before := observe(tc.object)
			if exists != (before.Code == 0) {
				t.Fatal("native pre-state existence differs from model", before.Code)
			}
			write := false
			for _, op := range tc.ops {
				write = write || op.Code != cephmsgr.OSDGetOmapHeader && op.Code != cephmsgr.OSDGetOmapKeys && op.Code != cephmsgr.OSDGetOmapValues && op.Code != cephmsgr.OSDGetOmapValuesByKeys
			}
			request := osdObjectRequest(selected, tc.ops...)
			var mtime cephmsgr.OSDTime
			if write {
				tick++
				mtime = cephmsgr.OSDTime{Seconds: 1710000000 + tick, Nanoseconds: tick*1000 + 7}
				request.ModificationTime = mtime
			}
			var native *omapWriteReceipt
			if tc.mirror != nil || tc.absent {
				result := nativeOmapWrite(t, ctx, base, "compare", omapWriteInput{Seed: tc.mirror, Operations: mirror(tc.ops), MTime: &omapWriteMTime{Seconds: mtime.Seconds, Nanos: mtime.Nanoseconds}, InventoryKeys: inventory})
				if !result.Cleanup.Mirror || result.Aggregate == nil {
					t.Fatal("native mirror lacks cleanup or aggregate")
				}
				native = &result
			}
			reply, err := connection.Request(ctx, request)
			var server *cephmsgr.OSDError
			var unknown *cephmsgr.OutcomeUnknownError
			if tc.code < 0 {
				if !errors.As(err, &server) || server.Code != tc.code || errors.As(err, &unknown) {
					t.Fatal("known OMAP server refusal differs", err)
				}
			} else if err != nil {
				t.Fatal("Go OMAP header/write request", err)
			}
			if reply.Code != tc.code || reply.Transaction == 0 || reply.GlobalID == 0 || reply.Submission.Transaction != reply.Transaction || reply.Object != tc.object || reply.PG.Pool != uint64(selected.Pool) || reply.PG.Seed != selected.Hash || reply.MapEpoch < selected.Epoch || reply.RetryAttempt != 0 || len(reply.Operations) != len(tc.ops) {
				t.Fatal("OMAP write reply identity/result count differs", reply.Code, reply.Object)
			}
			if write && tc.code == 0 && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("successful OMAP mutation reply lacks ONDISK")
			}
			for i, result := range reply.Operations {
				want := int32(0)
				if tc.results != nil {
					want = tc.results[i]
				}
				if result.Code != tc.ops[i].Code || result.Result != want && tc.code == 0 {
					t.Fatalf("OMAP per-op result differs: op=%d code=%x result=%d want=%d", i, result.Code, result.Result, want)
				}
				t.Logf("op=%d code=%x result=%d length=%d payload=%d", i, result.Code, result.Result, result.Length, result.PayloadLength)
			}
			if tc.reads != nil {
				tc.reads(t, reply, before)
			}
			after := observe(tc.object)
			observation, observed := "", before
			if tc.cachedAdvance && !reflect.DeepEqual(before, after) {
				// Pinned finish_ctx (PrimaryLogPG.cc:9154) applies the new
				// object_info to the primary's cached context before the
				// RETURNVEC bound (4287-4321) refuses without a transaction.
				// Only that exact cached version/mtime advance is admitted.
				advanced := before
				advanced.Version, advanced.Seconds, advanced.Nanos = before.Version+1, int64(mtime.Seconds), int64(mtime.Nanoseconds)
				if !reflect.DeepEqual(advanced, after) {
					b, _ := json.Marshal(before)
					a, _ := json.Marshal(after)
					t.Fatalf("refused vector changed state beyond the cached object_info advance\nbefore=%s\nafter=%s", b, a)
				}
				observation = "primary cached object_info advanced to the refused request's version/mtime; OMAP, header, bytes and xattrs unchanged"
				t.Log(observation)
				before = after
			}
			if tc.code < 0 || !write {
				if !reflect.DeepEqual(before, after) {
					b, _ := json.Marshal(before)
					a, _ := json.Marshal(after)
					t.Fatalf("refused or read-only OMAP request changed native state\nbefore=%s\nafter=%s", b, a)
				}
			} else {
				if tc.apply != nil {
					tc.apply(tc.object)
				}
				if after.Code != 0 || after.Version <= before.Version || after.Seconds != int64(mtime.Seconds) || after.Nanos != int64(mtime.Nanoseconds) {
					t.Fatalf("committed OMAP mutation version/mtime differs: %d->%d %d.%d", before.Version, after.Version, after.Seconds, after.Nanos)
				}
				wantData, wantAttrs := baseline, attrs
				if tc.object != main {
					wantData, wantAttrs = []byte{}, nil
				}
				if !bytes.Equal(after.Data, wantData) || len(after.Attributes) != len(wantAttrs) || len(wantAttrs) != 0 && !reflect.DeepEqual(after.Attributes, wantAttrs) {
					t.Fatal("OMAP mutation changed object bytes or xattrs")
				}
				if !equalOmapModel(omapWriteInventory(t, after), models[tc.object]) || !bytes.Equal(*after.Header, headers[tc.object]) {
					t.Fatalf("native OMAP/header after Go mutation differs: %+v header=%x want=%x", omapWriteInventory(t, after), *after.Header, headers[tc.object])
				}
			}
			if native != nil {
				if *native.Aggregate != tc.code {
					t.Fatal("native C mirror aggregate differs from Go", *native.Aggregate, tc.code)
				}
				if tc.code == 0 {
					if native.After.Seconds != after.Seconds || native.After.Nanos != after.Nanos || !bytes.Equal(native.After.Data, after.Data) || !reflect.DeepEqual(native.After.Attributes, after.Attributes) ||
						!equalOmapModel(omapWriteInventory(t, native.After), omapWriteInventory(t, after)) || !bytes.Equal(*native.After.Header, *after.Header) {
						t.Fatal("native C mirror state differs from Go-mutated state")
					}
				}
			}
			evidence.Steps = append(evidence.Steps, stepEvidence{Name: tc.name, Placement: selected, Before: observed, Go: reply, After: after, Mirror: native, Observation: observation, MTime: mtime, Ops: tc.ops})
		})
	}
	header1 := []byte{0, 0xff, 'H', '\n', 0x80}
	header2 := []byte("h2")
	steps := []step{
		{name: "set binary empty NUL keys and replace", object: main, mirror: seed,
			ops: []cephmsgr.OSDOperation{set(cephmsgr.OSDOmapEntry{Key: "\xffhigh", Value: []byte{0x80, 0}}, cephmsgr.OSDOmapEntry{Key: ""}, cephmsgr.OSDOmapEntry{Key: "new-empty", Value: []byte{}}, cephmsgr.OSDOmapEntry{Key: "a", Value: []byte("replaced")})},
			apply: func(o string) {
				models[o][""], models[o]["\xffhigh"], models[o]["new-empty"], models[o]["a"] = []byte{}, []byte{0x80, 0}, []byte{}, []byte("replaced")
			}},
		{name: "set binary header", object: main, ops: []cephmsgr.OSDOperation{setHeader(header1)}, apply: func(o string) { headers[o] = header1 }},
		{name: "header read matches CLI raw header", object: main, ops: []cephmsgr.OSDOperation{getHeader}, reads: func(t *testing.T, reply cephmsgr.OSDReply, before omapWriteState) {
			if !bytes.Equal(reply.Operations[0].Data, *before.Header) || !bytes.Equal(reply.Operations[0].Data, header1) || reply.Operations[0].Length != 0 {
				t.Fatalf("Go raw header differs from native CLI header: %x / %x", reply.Operations[0].Data, *before.Header)
			}
		}},
		// Pinned PrimaryLogPG.cc:4287-4321 bounds every RETURNVEC output of a
		// successful mutation vector by osd_max_write_op_reply_len (64 bytes by
		// default), then replies EOVERFLOW without submitting the transaction.
		{name: "oversized read output refuses mutation vector without commit", object: main, code: -75, cachedAdvance: true,
			ops: []cephmsgr.OSDOperation{getHeader, {Code: cephmsgr.OSDGetOmapValues, Omap: cephmsgr.OSDOmapInput{MaxEntries: 128, MaxOutputBytes: 64 << 10}}, {Code: cephmsgr.OSDGetOmapValuesByKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{"", "a", "never"}, MaxOutputBytes: 64 << 10}}, set(cephmsgr.OSDOmapEntry{Key: "c", Value: []byte("c")}), remove("a", "never"), setHeader(header2)},
			reads: func(t *testing.T, reply cephmsgr.OSDReply, before omapWriteState) {
				ops := reply.Operations
				// complete_read_ctx (PrimaryLogPG.cc:9222) adds ACK|ONDISK to
				// every refused reply; the flag is not a commit receipt.
				if ops[1].Result != -75 || len(ops[1].Data) != 0 {
					t.Fatal("oversized mutation-vector page did not report cleared EOVERFLOW output", ops[1].Result, len(ops[1].Data))
				}
				t.Logf("refused mutation vector flags=%#x", reply.Flags)
				if ops[0].Result != 0 || !bytes.Equal(ops[0].Data, header1) || ops[2].Result != 0 || len(ops[2].Data) == 0 || len(ops[2].Data) > 64 {
					t.Fatal("bounded outputs in the refused vector were not retained", ops[0].Result, ops[2].Result)
				}
				for _, op := range ops[3:] {
					if op.Result != 0 || len(op.Data) != 0 {
						t.Fatal("refused vector rewrote staged mutation results", op.Result)
					}
				}
			}},
		{name: "small reads precede atomic compound writes", object: main,
			ops: []cephmsgr.OSDOperation{getHeader, {Code: cephmsgr.OSDGetOmapKeys, Omap: cephmsgr.OSDOmapInput{MaxEntries: 2, MaxOutputBytes: 64}}, {Code: cephmsgr.OSDGetOmapValuesByKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{"", "a", "never"}, MaxOutputBytes: 64}}, set(cephmsgr.OSDOmapEntry{Key: "c", Value: []byte("c")}), remove("a", "never"), setHeader(header2)},
			reads: func(t *testing.T, reply cephmsgr.OSDReply, before omapWriteState) {
				if !bytes.Equal(reply.Operations[0].Data, header1) || !bytes.Equal(header1, *before.Header) {
					t.Fatal("compound header read observed staged writes")
				}
				page, more := decodeOmapRead(t, cephmsgr.OSDGetOmapKeys, reply.Operations[1].Data)
				if more == nil || !*more || !equalOmapEntries(page, []omapNativeEntry{{Key: []byte("\x00nul"), Value: []byte{}}, {Key: []byte("a"), Value: []byte{}}}) {
					t.Fatal("compound page read differs from committed pre-state", page)
				}
				keyed, _ := decodeOmapRead(t, cephmsgr.OSDGetOmapValuesByKeys, reply.Operations[2].Data)
				if !equalOmapEntries(keyed, []omapNativeEntry{{Key: []byte{}, Value: []byte{}}, {Key: []byte("a"), Value: []byte("replaced")}}) {
					t.Fatal("compound BYKEYS read differs from committed pre-state", keyed)
				}
			},
			apply: func(o string) { models[o]["c"] = []byte("c"); delete(models[o], "a"); headers[o] = header2 }},
		{name: "remove keys retains header", object: main, ops: []cephmsgr.OSDOperation{remove("\x00nul", "never")}, apply: func(o string) { delete(models[o], "\x00nul") }},
		{name: "empty set map is an executed mutation", object: main, ops: []cephmsgr.OSDOperation{set()}},
		{name: "empty removal set is an executed mutation", object: main, ops: []cephmsgr.OSDOperation{remove()}},
		{name: "empty header is not clear", object: main, ops: []cephmsgr.OSDOperation{setHeader([]byte{})}, apply: func(o string) { headers[o] = []byte{} }},
		{name: "failed guard rolls back staged OMAP writes", object: main, code: -75,
			ops: []cephmsgr.OSDOperation{set(cephmsgr.OSDOmapEntry{Key: "rollback", Value: []byte("x")}), setHeader([]byte("rollback")), {Code: cephmsgr.OSDAssertVersion, Version: 1 << 40}}},
		{name: "set header creates missing object", object: absent[0], ops: []cephmsgr.OSDOperation{setHeader([]byte("created-header"))},
			apply: func(o string) { models[o], headers[o] = map[string][]byte{}, []byte("created-header") }},
		{name: "FAILOK clear on missing object then set creates", object: absent[1], absent: true, results: []int32{-2, 0},
			ops:   []cephmsgr.OSDOperation{{Code: cephmsgr.OSDClearOmap, Flags: cephmsgr.OSDFailOK}, set(cephmsgr.OSDOmapEntry{Key: "created", Value: []byte("1")})},
			apply: func(o string) { models[o], headers[o] = map[string][]byte{"created": []byte("1")}, []byte{} }},
		{name: "remove on missing object is known ENOENT", object: absent[2], code: -2, ops: []cephmsgr.OSDOperation{remove("a")}},
		{name: "clear on missing object is known ENOENT", object: absent[2], code: -2, ops: []cephmsgr.OSDOperation{clear}},
		{name: "header before clear", object: main, ops: []cephmsgr.OSDOperation{setHeader([]byte("before-clear"))}, apply: func(o string) { headers[o] = []byte("before-clear") }},
		{name: "clear removes values and header", object: main, ops: []cephmsgr.OSDOperation{clear}, apply: func(o string) { models[o], headers[o] = map[string][]byte{}, []byte{} }},
		{name: "header read after clear is empty", object: main, ops: []cephmsgr.OSDOperation{getHeader}, reads: func(t *testing.T, reply cephmsgr.OSDReply, before omapWriteState) {
			if len(reply.Operations[0].Data) != 0 || len(*before.Header) != 0 {
				t.Fatal("cleared header remained visible", reply.Operations[0].Data)
			}
		}},
	}
	for _, tc := range steps {
		run(tc)
		if t.Failed() {
			break
		}
	}
}

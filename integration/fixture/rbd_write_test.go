package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRBDWritePlanIsTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdWritePlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != rbdBasePlanFor(token).Pool || plan.BaseImage != rbdBasePlanFor(token).Image ||
		plan.Image != "ceph-msgr-write-"+token || plan.Snapshot != "ceph-msgr-wsnap-a1a1a1a1" || plan.MetadataKey != "ceph-msgr-write.a1a1a1a1" || plan.MetadataValue != "go-written-a1a1a1a1" {
		t.Fatal("owned RBD write plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdWritePlanFor(token)) || reflect.DeepEqual(plan, rbdWritePlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("write plan is not deterministic and token-bound")
	}
	objectSize := uint64(1) << plan.Order
	touched := func(extents []rbdBaseExtent, size uint64) map[uint64]bool {
		result := map[uint64]bool{}
		for _, e := range extents {
			if e.Length == 0 || e.Offset%4096 != 0 || e.Length%4096 != 0 || e.Offset+e.Length > size {
				t.Fatal("plan write is not a 4 KiB-aligned extent inside the image", e)
			}
			for o := e.Offset / objectSize; o <= (e.Offset+e.Length-1)/objectSize; o++ {
				result[o] = true
			}
		}
		return result
	}
	pre, post, golang := touched(plan.PreWrites, plan.Size), touched(plan.PostWrites, plan.Size), touched(plan.GoWrites, plan.Size)
	// Object 0 exists without a clone, object 1 already has one, objects 2
	// and 3 are absent; the Go writes reach all four, and one Go extent
	// crosses the boundary of objects 2 and 3.
	if plan.Size/objectSize != 4 || !reflect.DeepEqual(pre, map[uint64]bool{0: true, 1: true}) || !reflect.DeepEqual(post, map[uint64]bool{1: true}) ||
		!reflect.DeepEqual(golang, map[uint64]bool{0: true, 1: true, 2: true, 3: true}) || len(touched(plan.GoWrites[3:], plan.Size)) != 2 {
		t.Fatal("RBD write plan layout changed", pre, post, golang)
	}
	if plan.GoResize%objectSize != 0 || plan.GoResizeWatched%objectSize != 0 || !reflect.DeepEqual(plan.sizes(), []uint64{256 << 10, 320 << 10, 384 << 10}) {
		t.Fatal("RBD write plan sizes changed", plan.sizes())
	}
	foreign := plan
	foreign.Features = 61
	if foreign.owned(token) {
		t.Fatal("write plan with more image features admitted")
	}
}

func TestRBDWriteRequestIsClosedAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	// At each step only the next action of the closed sequence is admitted:
	// neither a repeat nor a skipped or earlier action.
	for next := range rbdWriteActions {
		for i, action := range rbdWriteActions {
			id, got, err := parseRBDWriteRequest([]string{"7", token, action}, token, 6, next)
			if (err == nil) != (i == next) || err == nil && (id != 7 || got != action) {
				t.Fatal("RBD write action order differs", next, action, err)
			}
		}
	}
	if _, _, err := parseRBDWriteRequest([]string{"99", token, "watch"}, token, 1, len(rbdWriteActions)); err == nil {
		t.Fatal("action after the closed sequence admitted")
	}
	for _, fields := range [][]string{
		{"7", token, "resize"}, {"7", token, "lock"}, {"7", token}, {"7", token, "setup", "extra"},
		{"6", token, "setup"}, {"0", token, "setup"}, {"07", token, "setup"}, {"7", strings.Repeat("b2", 16), "setup"}, {"7", token, "watch"},
	} {
		if _, _, err := parseRBDWriteRequest(fields, token, 6, 0); err == nil {
			t.Fatal("unscoped RBD write request admitted", fields)
		}
	}
}

// rbdWriteOutput is a well-formed output of one write action for the plan.
func rbdWriteOutput(token string, plan rbdWritePlan, action string, size uint64) map[string]any {
	prefix := "rbd_data.5678ef01"
	calls := []map[string]any{}
	for i, call := range rbdWriteClassCalls {
		object := "rbd_header.5678ef01"
		if i == 0 {
			object = "rbd_id." + plan.Image
		}
		calls = append(calls, map[string]any{"object": object, "class": call[0], "method": call[1], "input_hex": "", "code": 0, "output_hex": "00"})
	}
	count := int(size >> plan.Order)
	objects, offsets := []map[string]any{}, map[string]uint64{"head": 0, "snap": 0}
	for i := 0; i < count; i++ {
		entry := map[string]any{"index": i, "name": fmt.Sprintf("%s.%016x", prefix, i)}
		for _, label := range []string{"head", "snap"} {
			objectSize := uint64(0)
			if i < 2 {
				objectSize = 4096
			}
			view := map[string]any{"exists": objectSize != 0, "errno": 0, "size": objectSize, "offset": offsets[label], "sha256": strings.Repeat("0", 64)}
			if objectSize == 0 {
				view["errno"], view["sha256"] = 2, ""
			}
			entry[label] = view
			offsets[label] += objectSize
		}
		objects = append(objects, entry)
	}
	blobs := map[string]any{}
	for key := range rbdWriteBlobViews {
		length := map[string]uint64{"image_head": size, "image_snap": plan.Size, "objects_head": offsets["head"], "objects_snap": offsets["snap"]}[key]
		blobs[key] = map[string]any{"name": rbdWriteBlobName(action, key), "length": length, "sha256": strings.Repeat("3", 64)}
	}
	value := map[string]any{
		"action": action, "token": token, "request_id": 9, "pool": plan.Pool, "pool_id": 3, "image": plan.Image, "native_global_id": 4242,
		"image_id": "5678ef01", "header_object": "rbd_header.5678ef01", "id_object": "rbd_id." + plan.Image,
		"stat":     map[string]any{"size": size, "obj_size": 65536, "num_objs": count, "order": 16, "block_name_prefix": prefix, "parent_pool": uint64(1<<64 - 1), "parent_name": ""},
		"features": 1, "op_features": 0, "flags": 0, "stripe_unit": 65536, "stripe_count": 1, "data_pool_id": 3,
		"snapshots": []map[string]any{{"id": 4, "name": plan.Snapshot, "size": plan.Size, "namespace": 0}},
		"metadata":  []map[string]any{}, "lockers": map[string]any{"empty": true, "tag": "", "exclusive": false, "lockers": []map[string]any{}},
		"watchers": []map[string]any{}, "class_calls": calls, "objects": objects, "blobs": blobs,
	}
	switch action {
	case "setup":
		writes := []map[string]any{}
		for _, e := range plan.PreWrites {
			writes = append(writes, map[string]any{"phase": "pre", "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("1", 64)})
		}
		for _, e := range plan.PostWrites {
			writes = append(writes, map[string]any{"phase": "post", "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("2", 64)})
		}
		value["writes"] = writes
	case "observe-locked":
		value["lockers"] = map[string]any{"empty": false, "tag": "internal", "exclusive": true, "lockers": []map[string]any{{"client": "client.4743", "cookie": "auto 38", "addr": "172.19.0.9:0/1"}}}
		value["watchers"] = []map[string]any{{"addr": "172.19.0.9:0/1", "id": 4743, "cookie": 38}}
		value["metadata"] = []map[string]any{{"key": plan.MetadataKey, "value": plan.MetadataValue}}
	case "watch":
		watchers := []map[string]any{{"addr": "172.19.0.9:0/2", "id": 4242, "cookie": 1}}
		header := map[string]any{"object": "rbd_header.5678ef01", "class": "rbd", "method": "get_size", "input_hex": "feffffffffffffff", "code": 0, "output_hex": "100000060000000000"}
		value["phases"] = []map[string]any{
			{"phase": "opened", "size": plan.GoResize, "stat_size": plan.GoResize, "watchers": watchers},
			{"phase": "stale", "size": plan.GoResize, "stat_size": plan.GoResize, "watchers": watchers, "wait_ns": 7, "header_size": header},
			{"phase": "refreshed", "size": plan.GoResizeWatched, "stat_size": plan.GoResizeWatched, "watchers": watchers, "wait_ns": 9},
		}
	}
	return value
}

func TestRBDWriteNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdWritePlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, tc := range []struct {
		action string
		size   uint64
	}{{"setup", plan.Size}, {"observe-written", plan.Size}, {"observe-resized", plan.GoResize}, {"observe-locked", plan.GoResize}, {"observe-unlocked", plan.GoResize}, {"watch", plan.GoResizeWatched}} {
		got, err := decodeRBDWriteNative(tc.action, token, 9, plan, encode(rbdWriteOutput(token, plan, tc.action, tc.size)))
		if err != nil || got.Stat.Size != tc.size || got.Blobs["image_head"].Name != rbdWriteBlobName(tc.action, "image_head") {
			t.Fatal("valid RBD write output refused", tc.action, err)
		}
		if _, err := decodeRBDWriteNative(tc.action, token, 8, plan, encode(rbdWriteOutput(token, plan, tc.action, tc.size))); err == nil {
			t.Fatal("output of another request admitted", tc.action)
		}
	}
	if _, err := decodeRBDWriteNative("observe-written", token, 9, plan, encode(rbdWriteOutput(token, plan, "observe-resized", plan.GoResize))); err == nil {
		t.Fatal("output of another action admitted")
	}
	for name, change := range map[string]func(map[string]any){
		"unknown field":        func(v map[string]any) { v["extra"] = 1 },
		"foreign image":        func(v map[string]any) { v["image"] = plan.BaseImage },
		"zero global ID":       func(v map[string]any) { v["native_global_id"] = 0 },
		"header of another id": func(v map[string]any) { v["header_object"] = "rbd_header.other" },
		"more features":        func(v map[string]any) { v["features"] = 61 },
		"parent":               func(v map[string]any) { v["stat"].(map[string]any)["parent_pool"] = 1 },
		"unplanned size":       func(v map[string]any) { v["stat"].(map[string]any)["size"] = 300 << 10 },
		"object count":         func(v map[string]any) { v["stat"].(map[string]any)["num_objs"] = 9 },
		"second snapshot": func(v map[string]any) {
			v["snapshots"] = append(v["snapshots"].([]map[string]any), v["snapshots"].([]map[string]any)[0])
		},
		"resized snapshot":     func(v map[string]any) { v["snapshots"].([]map[string]any)[0]["size"] = plan.GoResize },
		"missing class call":   func(v map[string]any) { v["class_calls"] = v["class_calls"].([]map[string]any)[1:] },
		"class swapped":        func(v map[string]any) { v["class_calls"].([]map[string]any)[7]["class"] = "rbd" },
		"lockers missing":      func(v map[string]any) { delete(v, "lockers") },
		"lockers inconsistent": func(v map[string]any) { v["lockers"].(map[string]any)["empty"] = false },
		"watcher without addr": func(v map[string]any) { v["watchers"] = []map[string]any{{"addr": "", "id": 1, "cookie": 1}} },
		"too much metadata":    func(v map[string]any) { v["metadata"] = make([]map[string]any, 17) },
		"short write":          func(v map[string]any) { v["writes"].([]map[string]any)[0]["written"] = 1 },
		"misnamed object":      func(v map[string]any) { v["objects"].([]map[string]any)[1]["name"] = "rbd_data.other.0000000000000001" },
		"absent with bytes":    func(v map[string]any) { v["objects"].([]map[string]any)[3]["head"].(map[string]any)["size"] = 1 },
		"missing blob":         func(v map[string]any) { delete(v["blobs"].(map[string]any), "image_snap") },
		"blob of another phase": func(v map[string]any) {
			v["blobs"].(map[string]any)["image_head"].(map[string]any)["name"] = rbdWriteBlobName("watch", "image_head")
		},
		"phases on setup":   func(v map[string]any) { v["phases"] = []map[string]any{} },
		"metadata on setup": func(v map[string]any) { v["metadata"] = []map[string]any{{"key": "k", "value": "v"}} },
	} {
		value := rbdWriteOutput(token, plan, "setup", plan.Size)
		change(value)
		if _, err := decodeRBDWriteNative("setup", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD write setup output admitted:", name)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"phase order":         func(v map[string]any) { p := v["phases"].([]map[string]any); p[1], p[2] = p[2], p[1] },
		"missing phase":       func(v map[string]any) { v["phases"] = v["phases"].([]map[string]any)[:2] },
		"opened waited":       func(v map[string]any) { v["phases"].([]map[string]any)[0]["wait_ns"] = 1 },
		"header size missing": func(v map[string]any) { delete(v["phases"].([]map[string]any)[1], "header_size") },
		"header size on head": func(v map[string]any) {
			v["phases"].([]map[string]any)[2]["header_size"] = v["phases"].([]map[string]any)[1]["header_size"]
		},
		"header size of snapshot": func(v map[string]any) {
			v["phases"].([]map[string]any)[1]["header_size"].(map[string]any)["input_hex"] = "0400000000000000"
		},
		"unplanned phase size": func(v map[string]any) { v["phases"].([]map[string]any)[2]["size"] = 1 },
		"writes on watch":      func(v map[string]any) { v["writes"] = []map[string]any{} },
	} {
		value := rbdWriteOutput(token, plan, "watch", plan.GoResizeWatched)
		change(value)
		if _, err := decodeRBDWriteNative("watch", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD write watch output admitted:", name)
		}
	}
	if _, err := decodeRBDWriteNative("setup", token, 9, plan, bytes.Repeat([]byte(" "), rbdBaseJSONLimit+1)); err == nil {
		t.Fatal("oversized output admitted")
	}
}

func TestRBDWriteBinaryViewsMatchReceipts(t *testing.T) {
	dir := t.TempDir()
	blobs := map[string]rbdBaseBlob{}
	for key := range rbdWriteBlobViews {
		name := rbdWriteBlobName("observe-written", key)
		data := bytes.Repeat([]byte(key), 100)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		blobs[key] = rbdBaseBlob{Name: name, Length: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	if err := checkRBDWriteBlobs(dir, "observe-written", blobs); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDWriteBlobs(dir, "observe-resized", blobs); err == nil {
		t.Fatal("binary views of another action admitted")
	}
	if err := os.WriteFile(filepath.Join(dir, rbdWriteBlobName("observe-written", "image_snap")), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDWriteBlobs(dir, "observe-written", blobs); err == nil {
		t.Fatal("changed binary view admitted")
	}
}

func TestRBDWriteJournalMarksObservationsReadOnly(t *testing.T) {
	dir, token := t.TempDir(), strings.Repeat("a1", 16)
	c := &fixtureCluster{profile: profile{fsid: "94b09669-3ee8-4a10-af82-346cb674debb"}}
	c.rbdBase = &rbdBaseScope{out: dir, token: token, plan: rbdBasePlanFor(token), writePlan: rbdWritePlanFor(token), poolID: -1}
	setup, err := c.rbdBaseBeginAction("native-write-setup", c.rbdBase.writePlan.Image, 7, false)
	if err != nil || setup.ReadOnly {
		t.Fatal("write setup journal", err)
	}
	if err := c.rbdBaseFinish(setup, nil); err != nil {
		t.Fatal(err)
	}
	observe, err := c.rbdBaseBeginAction("native-write-observe-written", c.rbdBase.writePlan.Image, 8, true)
	if err != nil || !observe.ReadOnly {
		t.Fatal("read-only observation journal", err)
	}
	if _, err := c.rbdBaseBeginAction("native-write-observe-written", c.rbdBase.writePlan.Image, 9, true); err == nil {
		t.Fatal("one-shot observation issued twice")
	}
	if err := c.rbdBaseFinish(observe, fmt.Errorf("native process lost")); err == nil || observe.State != "uncertain" {
		t.Fatal("uncertain observation not retained")
	}
	var journal struct {
		Token     string             `json:"token"`
		WritePlan rbdWritePlan       `json:"write_plan"`
		Mutations []*rbdBaseMutation `json:"mutations"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "rbd-base-owned.json"))
	if err != nil || json.Unmarshal(data, &journal) != nil || journal.Token != token || !reflect.DeepEqual(journal.WritePlan, rbdWritePlanFor(token)) || len(journal.Mutations) != 2 ||
		journal.Mutations[0].State != "applied" || journal.Mutations[0].ReadOnly || journal.Mutations[1].State != "uncertain" || !journal.Mutations[1].ReadOnly {
		t.Fatal("RBD write journal differs", string(data), err)
	}
}

// The embedded write driver must at least be valid Python where an
// interpreter is available; the live acceptance runs it against native
// librbd.
func TestRBDWriteNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdWriteNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD write driver is not valid Python", err, string(output))
	}
}

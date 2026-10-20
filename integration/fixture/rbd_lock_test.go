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
	"time"
)

func TestRBDLockPlanIsTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdLockPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != rbdBasePlanFor(token).Pool || plan.BaseImage != rbdBasePlanFor(token).Image ||
		plan.WriteImage != rbdWritePlanFor(token).Image || plan.Image != "ceph-msgr-lock-"+token || plan.Features != 29 {
		t.Fatal("owned RBD lock plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdLockPlanFor(token)) || reflect.DeepEqual(plan, rbdLockPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("lock plan is not deterministic and token-bound")
	}
	objectSize := uint64(1) << plan.Order
	touched := func(extents []rbdBaseExtent) map[uint64]bool {
		result := map[uint64]bool{}
		for _, e := range extents {
			if e.Length == 0 || e.Offset%4096 != 0 || e.Length%4096 != 0 || e.Offset+e.Length > plan.Size {
				t.Fatal("plan write is not a 4 KiB-aligned extent inside the image", e)
			}
			for o := e.Offset / objectSize; o <= (e.Offset+e.Length-1)/objectSize; o++ {
				result[o] = true
			}
		}
		return result
	}
	// One object per writer, in protocol order; object 4 stays absent so the
	// object map keeps a nonexistent entry. The Go and reacquire writes start
	// inside absent objects (librbd adds the allocation hint there).
	if plan.Size/objectSize != 5 || !reflect.DeepEqual(touched(plan.SetupWrites), map[uint64]bool{0: true}) || !reflect.DeepEqual(touched(plan.OwnerWrites), map[uint64]bool{1: true}) ||
		!reflect.DeepEqual(touched(plan.GoWrites), map[uint64]bool{2: true}) || !reflect.DeepEqual(touched(plan.ReacquireWrites), map[uint64]bool{3: true}) ||
		plan.GoWrites[0].Offset%objectSize == 0 || plan.ReacquireWrites[0].Offset%objectSize == 0 || plan.SetupWrites[0] != (rbdBaseExtent{0, objectSize}) {
		t.Fatal("RBD lock plan layout changed", plan)
	}
	for name, change := range map[string]func(*rbdLockPlan){
		"journaling":    func(p *rbdLockPlan) { p.Features |= 64 },
		"no object map": func(p *rbdLockPlan) { p.Features = 5 },
		"foreign write": func(p *rbdLockPlan) { p.WriteImage = "ceph-msgr-write-other" },
		"no Go write":   func(p *rbdLockPlan) { p.GoWrites = nil },
		"other size":    func(p *rbdLockPlan) { p.Size = 256 << 10 },
	} {
		foreign := rbdLockPlanFor(token)
		change(&foreign)
		if foreign.owned(token) {
			t.Fatal("foreign lock plan admitted:", name)
		}
	}
	nativeID, nativeImage := rbdLockSetIDNames(token, "native")
	goID, goImage := rbdLockSetIDNames(token, "go")
	if nativeID == goID || nativeImage == goImage || !strings.HasPrefix(nativeID, "rbd_id.ceph-msgr-setid-") || !strings.HasSuffix(goID, token) ||
		nativeID == "rbd_id."+plan.Image || strings.Trim(nativeImage+goImage, "0123456789abcdef") != "" || len(goImage) != 12 {
		t.Fatal("set_id compound names are not token-bound, distinct alphanumeric ids", nativeID, goID, nativeImage, goImage)
	}
	if !strings.Contains(rbdLockNativePython, `'f00d' + token[:8]`) || !strings.Contains(rbdLockNativePython, `'beef' + token[:8]`) ||
		!strings.Contains(rbdLockNativePython, "WRONG_COOKIE = '"+rbdLockWrongCookie+"'") {
		t.Fatal("native driver set_id names or cookie differ from the Go side")
	}
	if rbdLockNativeTimeout("lock") < 4*30*time.Second+60*time.Second || rbdLockNativeTimeout("setup") != time.Minute {
		t.Fatal("native lock action budget cannot cover its four proceed waits and the check")
	}
}

func TestRBDLockRequestIsClosedAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for next := range rbdLockActions {
		for i, action := range rbdLockActions {
			id, got, err := parseRBDLockRequest([]string{"7", token, action}, token, 6, next)
			if (err == nil) != (i == next) || err == nil && (id != 7 || got != action) {
				t.Fatal("RBD lock action order differs", next, action, err)
			}
		}
	}
	if _, _, err := parseRBDLockRequest([]string{"99", token, "observe-released"}, token, 1, len(rbdLockActions)); err == nil {
		t.Fatal("action after the closed sequence admitted")
	}
	for _, fields := range [][]string{
		{"7", token, "unlock"}, {"7", token, "check"}, {"7", token}, {"7", token, "setup", "extra"},
		{"6", token, "setup"}, {"0", token, "setup"}, {"07", token, "setup"}, {"7", strings.Repeat("b2", 16), "setup"}, {"7", token, "lock"},
	} {
		if _, _, err := parseRBDLockRequest(fields, token, 6, 0); err == nil {
			t.Fatal("unscoped RBD lock request admitted", fields)
		}
	}
}

// rbdLockOutput is a well-formed output of one lock action for the plan.
func rbdLockOutput(token string, plan rbdLockPlan, action string) map[string]any {
	prefix, header, omap := "rbd_data.5678ef01", "rbd_header.5678ef01", "rbd_object_map.5678ef01"
	objects := map[string]string{"id": "rbd_id." + plan.Image, "header": header, "map": omap}
	calls := []map[string]any{}
	for _, call := range rbdLockClassCalls {
		calls = append(calls, map[string]any{"object": objects[call[0]], "class": call[1], "method": call[2], "input_hex": "", "code": 0, "output_hex": "00"})
	}
	count := int(plan.Size >> plan.Order)
	views, offset := []map[string]any{}, uint64(0)
	for i := 0; i < count; i++ {
		size := uint64(0)
		if i < 4 {
			size = 16384
		}
		view := map[string]any{"exists": size != 0, "errno": 0, "size": size, "offset": offset, "sha256": strings.Repeat("0", 64)}
		if size == 0 {
			view["errno"], view["sha256"] = 2, ""
		}
		offset += size
		views = append(views, map[string]any{"index": i, "name": fmt.Sprintf("%s.%016x", prefix, i), "head": view})
	}
	blobs := map[string]any{}
	for key := range rbdLockBlobViews {
		length := map[string]uint64{"image_head": plan.Size, "objects_head": offset}[key]
		blobs[key] = map[string]any{"name": rbdLockBlobName(action, key), "length": length, "sha256": strings.Repeat("3", 64)}
	}
	noLockers := func() map[string]any {
		return map[string]any{"empty": true, "tag": "", "exclusive": false, "lockers": []map[string]any{}}
	}
	call := func(object string) map[string]any {
		return map[string]any{"object": object, "class": "lock", "method": "get_info", "input_hex": "01010c00000008000000", "code": 0, "output_hex": "0101"}
	}
	raw := func() map[string]any { return map[string]any{"object": omap, "exists": true, "hex": "0e000000"} }
	diff := []map[string]any{{"offset": 0, "length": 65536, "exists": true}}
	value := map[string]any{
		"action": action, "token": token, "request_id": 9, "pool": plan.Pool, "pool_id": 3, "image": plan.Image, "native_global_id": 4242,
		"image_id": "5678ef01", "header_object": header, "id_object": "rbd_id." + plan.Image, "map_object": omap,
		"stat":     map[string]any{"size": plan.Size, "obj_size": 65536, "num_objs": count, "order": 16, "block_name_prefix": prefix, "parent_pool": uint64(1<<64 - 1), "parent_name": ""},
		"features": 29, "op_features": 0, "flags": 0, "stripe_unit": 65536, "stripe_count": 1, "data_pool_id": 3, "snapshots": []map[string]any{},
		"lockers": noLockers(), "watchers": []map[string]any{}, "diff": diff, "class_calls": calls, "map_raw": raw(), "objects": views, "blobs": blobs,
	}
	writes := func(phase string, extents []rbdBaseExtent) []map[string]any {
		result := []map[string]any{}
		for _, e := range extents {
			result = append(result, map[string]any{"phase": phase, "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("1", 64), "elapsed_ns": 5})
		}
		return result
	}
	sums := []map[string]any{}
	for _, e := range plan.GoWrites {
		sums = append(sums, map[string]any{"offset": e.Offset, "length": e.Length, "sha256": strings.Repeat("2", 64)})
	}
	versioned := func(object string) map[string]any {
		return map[string]any{"object": object, "exists": true, "hex": "0c000000" + hex.EncodeToString([]byte("f00da1a1a1a1")), "version": 3}
	}
	switch action {
	case "setup":
		value["writes"] = writes("setup", plan.SetupWrites)
		object, id := rbdLockSetIDNames(token, "native")
		value["set_id"] = map[string]any{"object": object, "image_id": id, "input_hex": "0c000000", "first": 0, "first_view": versioned(object),
			"second": -17, "second_view": versioned(object), "get_id": map[string]any{"object": object, "class": "rbd", "method": "get_id", "input_hex": "", "code": 0, "output_hex": "0c000000"}}
	case "observe-released":
		object, _ := rbdLockSetIDNames(token, "go")
		value["set_id_go"] = map[string]any{"object": object, "view": versioned(object),
			"get_id": map[string]any{"object": object, "class": "rbd", "method": "get_id", "input_hex": "", "code": 0, "output_hex": "0c000000"}}
	case "lock":
		watchers := []map[string]any{{"addr": "172.19.0.9:0/2", "id": 4242, "cookie": 1}}
		phases := []map[string]any{}
		for i, label := range rbdLockPhases {
			p := map[string]any{"phase": label, "lockers": noLockers(), "flags": 0, "size": plan.Size, "diff": diff, "header_lock": call(header), "map_lock": call(omap), "map_raw": raw()}
			if label != "closed" {
				p["is_owner"], p["owners"], p["watchers"] = label != "released", []map[string]any{}, watchers
			} else {
				p["close_ns"] = 11
			}
			if i > 0 {
				p["wait_ns"] = 7
			}
			switch label {
			case "owner":
				p["writes"] = writes("owner", plan.OwnerWrites)
				p["negative_compound"] = map[string]any{"object": omap, "inputs_hex": []string{"0101", "0400000000000000"}, "code": -16, "map_before": raw(), "map_after": raw()}
			case "go-owner":
				p["go_extents"] = sums
			case "reacquired":
				p["writes"], p["write_ns"], p["go_extents"] = writes("reacquire", plan.ReacquireWrites), 13, sums
			}
			phases = append(phases, p)
		}
		value["phases"] = phases
		value["check"] = map[string]any{"argv": []string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "object-map", "check", "--no-progress", plan.Pool + "/" + plan.Image},
			"returncode": 0, "stdout": "", "stderr": "", "elapsed_ns": 17}
	}
	return value
}

func TestRBDLockNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdLockPlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, action := range rbdLockActions {
		got, err := decodeRBDLockNative(action, token, 9, plan, encode(rbdLockOutput(token, plan, action)))
		if err != nil || got.Stat.Size != plan.Size || got.Blobs["image_head"].Name != rbdLockBlobName(action, "image_head") {
			t.Fatal("valid RBD lock output refused", action, err)
		}
		if _, err := decodeRBDLockNative(action, token, 8, plan, encode(rbdLockOutput(token, plan, action))); err == nil {
			t.Fatal("output of another request admitted", action)
		}
	}
	if _, err := decodeRBDLockNative("observe-released", token, 9, plan, encode(rbdLockOutput(token, plan, "setup"))); err == nil {
		t.Fatal("output of another action admitted")
	}
	for name, change := range map[string]func(map[string]any){
		"unknown field":     func(v map[string]any) { v["extra"] = 1 },
		"foreign image":     func(v map[string]any) { v["image"] = plan.WriteImage },
		"zero global ID":    func(v map[string]any) { v["native_global_id"] = 0 },
		"map of another id": func(v map[string]any) { v["map_object"] = "rbd_object_map.other" },
		"layering only":     func(v map[string]any) { v["features"] = 1 },
		"snapshot": func(v map[string]any) {
			v["snapshots"] = []map[string]any{{"id": 4, "name": "s", "size": plan.Size, "namespace": 0}}
		},
		"resized":              func(v map[string]any) { v["stat"].(map[string]any)["size"] = 384 << 10 },
		"missing class call":   func(v map[string]any) { v["class_calls"] = v["class_calls"].([]map[string]any)[1:] },
		"map call on header":   func(v map[string]any) { v["class_calls"].([]map[string]any)[7]["object"] = "rbd_header.5678ef01" },
		"class swapped":        func(v map[string]any) { v["class_calls"].([]map[string]any)[5]["class"] = "rbd" },
		"raw of the header":    func(v map[string]any) { v["map_raw"].(map[string]any)["object"] = "rbd_header.5678ef01" },
		"raw absent with hex":  func(v map[string]any) { v["map_raw"].(map[string]any)["exists"] = false },
		"raw not hex":          func(v map[string]any) { v["map_raw"].(map[string]any)["hex"] = "zz" },
		"lockers inconsistent": func(v map[string]any) { v["lockers"].(map[string]any)["empty"] = false },
		"diff overlap": func(v map[string]any) {
			v["diff"] = []map[string]any{{"offset": 0, "length": 65536, "exists": true}, {"offset": 4096, "length": 4096, "exists": true}}
		},
		"diff past the end": func(v map[string]any) {
			v["diff"] = []map[string]any{{"offset": plan.Size, "length": 1, "exists": true}}
		},
		"short write":       func(v map[string]any) { v["writes"].([]map[string]any)[0]["written"] = 1 },
		"misnamed object":   func(v map[string]any) { v["objects"].([]map[string]any)[1]["name"] = "rbd_data.other.0000000000000001" },
		"absent with bytes": func(v map[string]any) { v["objects"].([]map[string]any)[4]["head"].(map[string]any)["size"] = 1 },
		"missing blob":      func(v map[string]any) { delete(v["blobs"].(map[string]any), "objects_head") },
		"blob of lock": func(v map[string]any) {
			v["blobs"].(map[string]any)["image_head"].(map[string]any)["name"] = rbdLockBlobName("lock", "image_head")
		},
		"phases on setup": func(v map[string]any) { v["phases"] = []map[string]any{} },
		"check on setup": func(v map[string]any) {
			v["check"] = map[string]any{"argv": []string{}, "returncode": 0, "stdout": "", "stderr": "", "elapsed_ns": 1}
		},
		"watched after open": func(v map[string]any) {
			v["watchers"] = []map[string]any{{"addr": "172.19.0.9:0/2", "id": 4242, "cookie": 1}}
		},
		"set_id missing":          func(v map[string]any) { delete(v, "set_id") },
		"set_id of the Go name":   func(v map[string]any) { v["set_id"].(map[string]any)["object"], _ = rbdLockSetIDNames(token, "go") },
		"set_id positive":         func(v map[string]any) { v["set_id"].(map[string]any)["second"] = 1 },
		"set_id view unversioned": func(v map[string]any) { v["set_id"].(map[string]any)["first_view"].(map[string]any)["version"] = 0 },
		"set_id get_id swapped":   func(v map[string]any) { v["set_id"].(map[string]any)["get_id"].(map[string]any)["method"] = "set_id" },
		"Go set_id on setup": func(v map[string]any) {
			v["set_id_go"] = map[string]any{"object": "x", "view": map[string]any{}, "get_id": map[string]any{}}
		},
	} {
		value := rbdLockOutput(token, plan, "setup")
		change(value)
		if _, err := decodeRBDLockNative("setup", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD lock setup output admitted:", name)
		}
	}
	phase := func(v map[string]any, i int) map[string]any { return v["phases"].([]map[string]any)[i] }
	for name, change := range map[string]func(map[string]any){
		"phase order":          func(v map[string]any) { p := v["phases"].([]map[string]any); p[1], p[2] = p[2], p[1] },
		"missing phase":        func(v map[string]any) { v["phases"] = v["phases"].([]map[string]any)[:4] },
		"owner waited":         func(v map[string]any) { phase(v, 0)["wait_ns"] = 1 },
		"released not waited":  func(v map[string]any) { delete(phase(v, 1), "wait_ns") },
		"owner without writes": func(v map[string]any) { delete(phase(v, 0), "writes") },
		"owner write of Go": func(v map[string]any) {
			phase(v, 0)["writes"].([]map[string]any)[0]["offset"] = plan.GoWrites[0].Offset
		},
		"go extents missing":   func(v map[string]any) { delete(phase(v, 2), "go_extents") },
		"reacquire not timed":  func(v map[string]any) { delete(phase(v, 3), "write_ns") },
		"write on released":    func(v map[string]any) { phase(v, 1)["writes"] = phase(v, 0)["writes"] },
		"closed still owner":   func(v map[string]any) { phase(v, 4)["is_owner"] = true },
		"closed with watchers": func(v map[string]any) { phase(v, 4)["watchers"] = phase(v, 0)["watchers"] },
		"closed not timed":     func(v map[string]any) { delete(phase(v, 4), "close_ns") },
		"open phase closed":    func(v map[string]any) { phase(v, 2)["close_ns"] = 3 },
		"two owners": func(v map[string]any) {
			phase(v, 0)["owners"] = []map[string]any{{"mode": 1, "owner": "a"}, {"mode": 1, "owner": "b"}}
		},
		"map lock of header":    func(v map[string]any) { phase(v, 2)["map_lock"].(map[string]any)["object"] = "rbd_header.5678ef01" },
		"header lock missing":   func(v map[string]any) { delete(phase(v, 3), "header_lock") },
		"map raw missing":       func(v map[string]any) { delete(phase(v, 1), "map_raw") },
		"resized phase":         func(v map[string]any) { phase(v, 1)["size"] = 1 },
		"check missing":         func(v map[string]any) { delete(v, "check") },
		"check of another tool": func(v map[string]any) { v["check"].(map[string]any)["argv"] = []string{"object-map", "rebuild"} },
		"check not timed":       func(v map[string]any) { v["check"].(map[string]any)["elapsed_ns"] = 0 },
		"check stderr bound":    func(v map[string]any) { v["check"].(map[string]any)["stderr"] = strings.Repeat("e", 4097) },
		"writes on lock":        func(v map[string]any) { v["writes"] = []map[string]any{} },
		"compound missing":      func(v map[string]any) { delete(phase(v, 0), "negative_compound") },
		"compound on released":  func(v map[string]any) { phase(v, 1)["negative_compound"] = phase(v, 0)["negative_compound"] },
		"compound on header": func(v map[string]any) {
			phase(v, 0)["negative_compound"].(map[string]any)["object"] = "rbd_header.5678ef01"
		},
		"compound one input": func(v map[string]any) {
			phase(v, 0)["negative_compound"].(map[string]any)["inputs_hex"] = []string{"0101"}
		},
		"compound positive": func(v map[string]any) { phase(v, 0)["negative_compound"].(map[string]any)["code"] = 1 },
		"compound no after": func(v map[string]any) { delete(phase(v, 0)["negative_compound"].(map[string]any), "map_after") },
		"set_id on lock":    func(v map[string]any) { v["set_id"] = rbdLockOutput(token, plan, "setup")["set_id"] },
	} {
		value := rbdLockOutput(token, plan, "lock")
		change(value)
		if _, err := decodeRBDLockNative("lock", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD lock action output admitted:", name)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"Go set_id missing": func(v map[string]any) { delete(v, "set_id_go") },
		"Go set_id native name": func(v map[string]any) {
			v["set_id_go"].(map[string]any)["object"], _ = rbdLockSetIDNames(token, "native")
		},
		"Go set_id absent with bytes": func(v map[string]any) {
			v["set_id_go"].(map[string]any)["view"].(map[string]any)["exists"] = false
		},
		"Go get_id of another object": func(v map[string]any) {
			v["set_id_go"].(map[string]any)["get_id"].(map[string]any)["object"] = "rbd_id." + plan.Image
		},
	} {
		value := rbdLockOutput(token, plan, "observe-released")
		change(value)
		if _, err := decodeRBDLockNative("observe-released", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD lock observation admitted:", name)
		}
	}
	if _, err := decodeRBDLockNative("setup", token, 9, plan, bytes.Repeat([]byte(" "), rbdBaseJSONLimit+1)); err == nil {
		t.Fatal("oversized output admitted")
	}
}

func TestRBDLockBinaryViewsMatchReceipts(t *testing.T) {
	dir := t.TempDir()
	blobs := map[string]rbdBaseBlob{}
	for key := range rbdLockBlobViews {
		name := rbdLockBlobName("lock", key)
		data := bytes.Repeat([]byte(key), 100)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		blobs[key] = rbdBaseBlob{Name: name, Length: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	if err := checkRBDLockBlobs(dir, "lock", blobs); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDLockBlobs(dir, "setup", blobs); err == nil {
		t.Fatal("binary views of another action admitted")
	}
	if err := os.WriteFile(filepath.Join(dir, rbdLockBlobName("lock", "objects_head")), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDLockBlobs(dir, "lock", blobs); err == nil {
		t.Fatal("changed binary view admitted")
	}
}

func TestRBDLockJournalCarriesThePlan(t *testing.T) {
	dir, token := t.TempDir(), strings.Repeat("a1", 16)
	c := &fixtureCluster{profile: profile{fsid: "94b09669-3ee8-4a10-af82-346cb674debb"}}
	c.rbdBase = &rbdBaseScope{out: dir, token: token, plan: rbdBasePlanFor(token), writePlan: rbdWritePlanFor(token), lockPlan: rbdLockPlanFor(token), poolID: -1}
	lock, err := c.rbdBaseBeginAction("native-lock-lock", c.rbdBase.lockPlan.Image+":header-watch,exclusive-lock,object-map-check", 7, false)
	if err != nil || lock.ReadOnly {
		t.Fatal("lock action journal", err)
	}
	if err := c.rbdBaseFinish(lock, fmt.Errorf("native process killed")); err == nil || lock.State != "uncertain" {
		t.Fatal("uncertain lock action not retained")
	}
	if _, err := c.rbdBaseBeginAction("native-lock-lock", c.rbdBase.lockPlan.Image, 8, false); err == nil {
		t.Fatal("one-shot lock action issued twice")
	}
	observe, err := c.rbdBaseBeginAction("native-lock-observe-released", c.rbdBase.lockPlan.Image, 9, true)
	if err != nil || !observe.ReadOnly || c.rbdBaseFinish(observe, nil) != nil {
		t.Fatal("read-only lock observation journal", err)
	}
	var journal struct {
		LockPlan  rbdLockPlan        `json:"lock_plan"`
		Mutations []*rbdBaseMutation `json:"mutations"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "rbd-base-owned.json"))
	if err != nil || json.Unmarshal(data, &journal) != nil || !reflect.DeepEqual(journal.LockPlan, rbdLockPlanFor(token)) || len(journal.Mutations) != 2 ||
		journal.Mutations[0].State != "uncertain" || journal.Mutations[0].ReadOnly || journal.Mutations[1].State != "applied" || !journal.Mutations[1].ReadOnly {
		t.Fatal("RBD lock journal differs", string(data), err)
	}
	// The control refuses to run outside a prepared RBD scope.
	if err := (&fixtureCluster{profile: profile{rbdBase: "1"}}).rbdLockControl(t.Context(), dir, []string{"1", token, "setup"}); err == nil {
		t.Fatal("lock control admitted without a prepared scope")
	}
}

// The embedded lock driver must at least be valid Python where an interpreter
// is available; the live acceptance runs it against native librbd.
func TestRBDLockNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdLockNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD lock driver is not valid Python", err, string(output))
	}
}

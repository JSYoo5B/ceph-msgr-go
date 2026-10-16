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

func TestRBDClonePlanIsTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdClonePlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != rbdBasePlanFor(token).Pool || plan.BaseImage != rbdBasePlanFor(token).Image ||
		plan.Parent != "ceph-msgr-parent-"+token || plan.Clone != "ceph-msgr-clone-"+token || plan.ParentSnapshot != "ceph-msgr-psnap-a1a1a1a1" ||
		plan.CloneSnapshot != "ceph-msgr-csnap-a1a1a1a1" || plan.Features != 1 || plan.CloneFormat != 2 {
		t.Fatal("owned RBD clone plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdClonePlanFor(token)) || reflect.DeepEqual(plan, rbdClonePlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("clone plan is not deterministic and token-bound")
	}
	objectSize := uint64(1) << plan.Order
	// pieces maps each extent to (object, in-object range); no planned
	// extent crosses an object boundary.
	pieces := func(extents []rbdBaseExtent) map[uint64][]rbdBaseExtent {
		result := map[uint64][]rbdBaseExtent{}
		for _, e := range extents {
			if e.Length == 0 || e.Offset%4096 != 0 || e.Length%4096 != 0 || e.Offset+e.Length > plan.Size || e.Offset/objectSize != (e.Offset+e.Length-1)/objectSize {
				t.Fatal("plan write is not a 4 KiB-aligned extent inside one object", e)
			}
			result[e.Offset/objectSize] = append(result[e.Offset/objectSize], rbdBaseExtent{e.Offset % objectSize, e.Length})
		}
		return result
	}
	const k = 1 << 10
	pre, post := pieces(plan.ParentPreWrites), pieces(plan.ParentPostWrites)
	goCopyup, nativeWrite, goSnapshot := pieces(plan.GoCopyupWrites), pieces(plan.NativeWrites), pieces(plan.GoSnapshotWrites)
	// The parent at its snapshot: object 4 absent, object 1 and 3 with
	// holes, every object changed after the snapshot. The clone: Go copies
	// up object 1 across a parent extent and a hole, native librbd copies up
	// object 3 (holes on both sides of its write), Go writes object 1 again
	// and copies up object 2 after the clone snapshot; 0, 4 and 5 stay
	// unwritten.
	if plan.Size/objectSize != 6 || len(pre[4]) != 0 || len(pre) != 5 || len(post) != 6 ||
		!reflect.DeepEqual(pre[1], []rbdBaseExtent{{8 * k, 16 * k}, {40 * k, 8 * k}}) || !reflect.DeepEqual(pre[3], []rbdBaseExtent{{16 * k, 4 * k}, {40 * k, 8 * k}}) ||
		!reflect.DeepEqual(goCopyup, map[uint64][]rbdBaseExtent{1: {{20 * k, 8 * k}}}) || !reflect.DeepEqual(nativeWrite, map[uint64][]rbdBaseExtent{3: {{4 * k, 4 * k}}}) ||
		!reflect.DeepEqual(goSnapshot, map[uint64][]rbdBaseExtent{1: {{44 * k, 8 * k}}, 2: {{28 * k, 8 * k}}}) {
		t.Fatal("RBD clone plan layout changed", plan)
	}
	for name, change := range map[string]func(*rbdClonePlan){
		"clone v1":       func(p *rbdClonePlan) { p.CloneFormat = 1 },
		"exclusive lock": func(p *rbdClonePlan) { p.Features |= 4 },
		"foreign parent": func(p *rbdClonePlan) { p.Parent = "ceph-msgr-parent-other" },
		"foreign clone":  func(p *rbdClonePlan) { p.Clone = p.Parent },
		"no Go copy-up":  func(p *rbdClonePlan) { p.GoCopyupWrites = nil },
		"other size":     func(p *rbdClonePlan) { p.Size = 320 << 10 },
		"other snapshot": func(p *rbdClonePlan) { p.CloneSnapshot = p.ParentSnapshot },
	} {
		foreign := rbdClonePlanFor(token)
		change(&foreign)
		if foreign.owned(token) {
			t.Fatal("foreign clone plan admitted:", name)
		}
	}
	// The other image acceptances of the same pool admit the parent and the
	// clone, which precede them in file order.
	for _, driver := range []string{rbdLockNativePython, rbdWriteNativePython} {
		if !strings.Contains(driver, "'ceph-msgr-parent-' + token, 'ceph-msgr-clone-' + token") {
			t.Fatal("RBD image driver does not admit the clone acceptance's images")
		}
	}
	if !strings.Contains(rbdCloneNativePython, "TRACE_LIMIT, TRACE_WIDTH = 96, 600") || rbdCloneTraceLimit != 96 || rbdCloneTraceWidth != 600 {
		t.Fatal("native trace bounds differ between the driver and the fixture")
	}
}

func TestRBDCloneRequestIsClosedAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for next := range rbdCloneActions {
		for i, action := range rbdCloneActions {
			id, got, err := parseRBDCloneRequest([]string{"7", token, action}, token, 6, next)
			if (err == nil) != (i == next) || err == nil && (id != 7 || got != action) {
				t.Fatal("RBD clone action order differs", next, action, err)
			}
		}
	}
	if _, _, err := parseRBDCloneRequest([]string{"99", token, "flatten"}, token, 1, len(rbdCloneActions)); err == nil {
		t.Fatal("action after the closed sequence admitted")
	}
	for _, fields := range [][]string{
		{"7", token, "unprotect"}, {"7", token, "remove"}, {"7", token}, {"7", token, "setup", "extra"},
		{"6", token, "setup"}, {"0", token, "setup"}, {"07", token, "setup"}, {"7", strings.Repeat("b2", 16), "setup"}, {"7", token, "write"},
	} {
		if _, _, err := parseRBDCloneRequest(fields, token, 6, 0); err == nil {
			t.Fatal("unscoped RBD clone request admitted", fields)
		}
	}
	targets := map[string]bool{}
	for _, action := range rbdCloneActions {
		targets[rbdCloneTarget(rbdClonePlanFor(token), action)] = true
	}
	if len(targets) != 5 || !rbdCloneHasSnapshot("flatten") || rbdCloneHasSnapshot("write") || !rbdCloneTraced("flatten") || rbdCloneTraced("snapshot") {
		t.Fatal("clone action targets or snapshot/trace scopes differ", targets)
	}
}

// rbdCloneOutput is a well-formed output of one clone action for the plan.
func rbdCloneOutput(token string, plan rbdClonePlan, action string) map[string]any {
	cloneID, parentID := "c10e0001", "9a7e0001"
	snapshot := rbdCloneHasSnapshot(action)
	count := int(plan.Size >> plan.Order)
	view := func(id string, snapshots []map[string]any, parent bool) map[string]any {
		v := map[string]any{"image_id": id, "features": 257, "op_features": 1, "flags": 0, "snapshots": snapshots, "parent": nil, "overlap": 0,
			"stat": map[string]any{"size": plan.Size, "obj_size": 65536, "num_objs": count, "order": 16, "block_name_prefix": "rbd_data." + id, "parent_pool": uint64(1<<64 - 1), "parent_name": ""}}
		if parent {
			v["parent"] = map[string]any{"pool_id": 3, "pool_name": plan.Pool, "pool_namespace": "", "image_id": parentID, "image_name": plan.Parent, "trash": false,
				"snap_id": 4, "snap_name": plan.ParentSnapshot, "snap_namespace_type": 0}
			v["overlap"] = plan.Size
			v["op_features"] = 2
		}
		return v
	}
	psnaps := []map[string]any{{"id": 4, "name": plan.ParentSnapshot, "size": plan.Size, "namespace": 0}}
	csnaps := []map[string]any{}
	if snapshot {
		csnaps = []map[string]any{{"id": 6, "name": plan.CloneSnapshot, "size": plan.Size, "namespace": 0}}
	}
	calls := func(objects map[string]string, list [][3]string) []map[string]any {
		result := []map[string]any{}
		for _, c := range list {
			result = append(result, map[string]any{"object": objects[c[0]], "class": c[1], "method": c[2], "input_hex": "", "code": 0, "output_hex": "00"})
		}
		return result
	}
	objectViews := func(prefix string, head, snap bool) ([]map[string]any, map[string]uint64) {
		views, offsets := []map[string]any{}, map[string]uint64{}
		for i := 0; i < count; i++ {
			entry := map[string]any{"index": i, "name": fmt.Sprintf("%s.%016x", prefix, i)}
			for label, present := range map[string]bool{"head": head, "snap": snap} {
				if !present {
					continue
				}
				size := uint64(0)
				if i%2 == 0 {
					size = 16384
				}
				v := map[string]any{"exists": size != 0, "errno": 0, "size": size, "offset": offsets[label], "sha256": strings.Repeat("0", 64)}
				if size == 0 {
					v["errno"], v["sha256"] = 2, ""
				}
				offsets[label] += size
				entry[label] = v
			}
			views = append(views, entry)
		}
		return views, offsets
	}
	cloneObjects, cloneOffsets := objectViews("rbd_data."+cloneID, true, snapshot)
	parentObjects, parentOffsets := objectViews("rbd_data."+parentID, false, true)
	lengths := map[string]uint64{"clone_head": plan.Size, "clone_objects_head": cloneOffsets["head"], "clone_snap": plan.Size, "clone_objects_snap": cloneOffsets["snap"],
		"parent_head": plan.Size, "parent_snap": plan.Size, "parent_objects_snap": parentOffsets["snap"]}
	blobs := map[string]any{}
	for _, key := range rbdCloneBlobKeys(action) {
		blobs[key] = map[string]any{"name": rbdCloneBlobName(action, key), "length": lengths[key], "sha256": strings.Repeat("3", 64)}
	}
	value := map[string]any{
		"action": action, "token": token, "request_id": 9, "pool": plan.Pool, "pool_id": 3, "parent": plan.Parent, "clone": plan.Clone, "native_global_id": 4242,
		"clone_view": view(cloneID, csnaps, action != "flatten"), "parent_view": view(parentID, psnaps, false), "parent_snapshot_view": view(parentID, psnaps, false),
		"children":      []map[string]any{{"pool": plan.Pool, "pool_namespace": "", "image": plan.Clone, "id": cloneID, "trash": false}},
		"clone_calls":   calls(map[string]string{"id": "rbd_id." + plan.Clone, "header": "rbd_header." + cloneID}, rbdCloneCalls),
		"parent_calls":  calls(map[string]string{"id": "rbd_id." + plan.Parent, "header": "rbd_header." + parentID}, rbdCloneParentCalls),
		"clone_objects": cloneObjects, "parent_objects": parentObjects, "blobs": blobs,
	}
	if snapshot {
		value["clone_snapshot_view"] = view(cloneID, csnaps, true)
		var methods [][3]string
		for _, m := range rbdCloneSnapshotCalls {
			methods = append(methods, [3]string{"header", "rbd", m})
		}
		value["clone_snapshot_calls"] = calls(map[string]string{"header": "rbd_header." + cloneID}, methods)
	}
	writes := func(phase string, extents []rbdBaseExtent) []map[string]any {
		result := []map[string]any{}
		for _, e := range extents {
			result = append(result, map[string]any{"phase": phase, "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("1", 64), "elapsed_ns": 5})
		}
		return result
	}
	trace := []map[string]any{
		{"direction": "send", "message": "osd_op(unknown.0.0:5 3.6 3:6c2e5d4f:::rbd_data." + cloneID + ".0000000000000003:head [stat] snapc 0=[] ondisk+write e24)"},
		{"direction": "receive", "message": "osd_op_reply(5 rbd_data." + cloneID + ".0000000000000003 [stat] v0'0 uv0 ondisk = -2 ((2) No such file or directory))"},
	}
	switch action {
	case "setup":
		value["writes"] = append(writes("parent-pre", plan.ParentPreWrites), writes("parent-post", plan.ParentPostWrites)...)
	case "write":
		value["writes"], value["trace"] = writes("native", plan.NativeWrites), trace
	case "flatten":
		value["trace"], value["flatten_ns"] = trace, 17
	}
	return value
}

func TestRBDCloneNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdClonePlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, action := range rbdCloneActions {
		got, err := decodeRBDCloneNative(action, token, 9, plan, encode(rbdCloneOutput(token, plan, action)))
		if err != nil || got.CloneView.Stat.Size != plan.Size || got.Blobs["clone_head"].Name != rbdCloneBlobName(action, "clone_head") || (got.CloneSnapshotView != nil) != rbdCloneHasSnapshot(action) {
			t.Fatal("valid RBD clone output refused", action, err)
		}
		if _, err := decodeRBDCloneNative(action, token, 8, plan, encode(rbdCloneOutput(token, plan, action))); err == nil {
			t.Fatal("output of another request admitted", action)
		}
	}
	if _, err := decodeRBDCloneNative("observe-copyup", token, 9, plan, encode(rbdCloneOutput(token, plan, "setup"))); err == nil {
		t.Fatal("output of another action admitted")
	}
	if _, err := decodeRBDCloneNative("observe-snapshot", token, 9, plan, encode(rbdCloneOutput(token, plan, "observe-copyup"))); err == nil {
		t.Fatal("observation without the clone snapshot admitted after the snapshot")
	}
	views := func(v map[string]any, key string) map[string]any { return v[key].(map[string]any) }
	for name, change := range map[string]func(map[string]any){
		"unknown field":        func(v map[string]any) { v["extra"] = 1 },
		"foreign clone":        func(v map[string]any) { v["clone"] = plan.BaseImage },
		"zero global ID":       func(v map[string]any) { v["native_global_id"] = 0 },
		"clone without parent": func(v map[string]any) { views(v, "clone_view")["parent"] = nil },
		"parent with a parent": func(v map[string]any) { views(v, "parent_view")["overlap"] = 1 },
		"parent of another id": func(v map[string]any) { views(v, "clone_view")["parent"].(map[string]any)["image_id"] = "other" },
		"parent snapshot name": func(v map[string]any) {
			views(v, "clone_view")["parent"].(map[string]any)["snap_name"] = plan.CloneSnapshot
		},
		"clone is the parent":  func(v map[string]any) { views(v, "clone_view")["image_id"] = "9a7e0001" },
		"clone snapshot early": func(v map[string]any) { v["clone_snapshot_view"] = views(v, "clone_view") },
		"resized parent":       func(v map[string]any) { views(v, "parent_view")["stat"].(map[string]any)["size"] = 1 },
		"exclusive-lock clone": func(v map[string]any) { views(v, "clone_view")["features"] = 5 },
		"no operations bit":    func(v map[string]any) { views(v, "parent_view")["features"] = 1 },
		"prefix of another id": func(v map[string]any) {
			views(v, "clone_view")["stat"].(map[string]any)["block_name_prefix"] = "rbd_data.other"
		},
		"parent snapshot gone": func(v map[string]any) { views(v, "parent_view")["snapshots"] = []map[string]any{} },
		"missing clone call":   func(v map[string]any) { v["clone_calls"] = v["clone_calls"].([]map[string]any)[1:] },
		"parent call on clone": func(v map[string]any) { v["parent_calls"].([]map[string]any)[2]["object"] = "rbd_header.c10e0001" },
		"call class swapped":   func(v map[string]any) { v["clone_calls"].([]map[string]any)[5]["class"] = "rbd" },
		"snapshot calls early": func(v map[string]any) { v["clone_snapshot_calls"] = []map[string]any{} },
		"misnamed object": func(v map[string]any) {
			v["clone_objects"].([]map[string]any)[1]["name"] = "rbd_data.other.0000000000000001"
		},
		"parent head view": func(v map[string]any) {
			v["parent_objects"].([]map[string]any)[0]["head"] = v["parent_objects"].([]map[string]any)[0]["snap"]
		},
		"absent with bytes": func(v map[string]any) { v["clone_objects"].([]map[string]any)[1]["head"].(map[string]any)["size"] = 1 },
		"missing blob":      func(v map[string]any) { delete(v["blobs"].(map[string]any), "parent_objects_snap") },
		"snapshot blob early": func(v map[string]any) {
			v["blobs"].(map[string]any)["clone_snap"] = v["blobs"].(map[string]any)["clone_head"]
		},
		"blob of another": func(v map[string]any) {
			v["blobs"].(map[string]any)["clone_head"].(map[string]any)["name"] = rbdCloneBlobName("write", "clone_head")
		},
		"blob length":           func(v map[string]any) { v["blobs"].(map[string]any)["parent_snap"].(map[string]any)["length"] = 1 },
		"short write":           func(v map[string]any) { v["writes"].([]map[string]any)[0]["written"] = 1 },
		"post write as pre":     func(v map[string]any) { v["writes"].([]map[string]any)[7]["phase"] = "parent-pre" },
		"trace on setup":        func(v map[string]any) { v["trace"] = rbdCloneOutput(token, plan, "write")["trace"] },
		"flatten time on setup": func(v map[string]any) { v["flatten_ns"] = 3 },
	} {
		value := rbdCloneOutput(token, plan, "setup")
		change(value)
		if _, err := decodeRBDCloneNative("setup", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD clone setup output admitted:", name)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"trace missing": func(v map[string]any) { delete(v, "trace") },
		"trace of the header": func(v map[string]any) {
			v["trace"].([]map[string]any)[0]["message"] = "osd_op(unknown.0.0:5 rbd_header.c10e0001 [call])"
		},
		"trace other direction": func(v map[string]any) { v["trace"].([]map[string]any)[0]["direction"] = "other" },
		"trace too wide": func(v map[string]any) {
			v["trace"].([]map[string]any)[0]["message"] = "osd_op(rbd_data." + strings.Repeat("x", rbdCloneTraceWidth) + ")"
		},
		"writes missing": func(v map[string]any) { delete(v, "writes") },
		"write of Go":    func(v map[string]any) { v["writes"].([]map[string]any)[0]["offset"] = plan.GoCopyupWrites[0].Offset },
		"parent writes":  func(v map[string]any) { v["writes"] = rbdCloneOutput(token, plan, "setup")["writes"] },
		"snapshot present": func(v map[string]any) {
			views(v, "clone_view")["snapshots"] = []map[string]any{{"id": 6, "name": plan.CloneSnapshot, "size": plan.Size, "namespace": 0}}
		},
	} {
		value := rbdCloneOutput(token, plan, "write")
		change(value)
		if _, err := decodeRBDCloneNative("write", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD clone write output admitted:", name)
		}
	}
	tooLong := rbdCloneOutput(token, plan, "write")
	var lines []map[string]any
	for len(lines) <= rbdCloneTraceLimit {
		lines = append(lines, tooLong["trace"].([]map[string]any)[0])
	}
	tooLong["trace"] = lines
	if _, err := decodeRBDCloneNative("write", token, 9, plan, encode(tooLong)); err == nil {
		t.Fatal("trace beyond its bound admitted")
	}
	for name, change := range map[string]func(map[string]any){
		"no flatten time":         func(v map[string]any) { delete(v, "flatten_ns") },
		"snapshot view missing":   func(v map[string]any) { delete(v, "clone_snapshot_view") },
		"snapshot calls missing":  func(v map[string]any) { delete(v, "clone_snapshot_calls") },
		"snapshot without parent": func(v map[string]any) { views(v, "clone_snapshot_view")["parent"] = nil },
		"snapshot object views":   func(v map[string]any) { delete(v["clone_objects"].([]map[string]any)[2], "snap") },
		"snapshot blob missing":   func(v map[string]any) { delete(v["blobs"].(map[string]any), "clone_objects_snap") },
		"two clone snapshots": func(v map[string]any) {
			views(v, "clone_view")["snapshots"] = append(views(v, "clone_view")["snapshots"].([]map[string]any), map[string]any{"id": 7, "name": "x", "size": plan.Size, "namespace": 0})
		},
		"snapshot call method": func(v map[string]any) {
			v["clone_snapshot_calls"].([]map[string]any)[3]["method"] = "set_protection_status"
		},
	} {
		value := rbdCloneOutput(token, plan, "flatten")
		change(value)
		if _, err := decodeRBDCloneNative("flatten", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed RBD clone flatten output admitted:", name)
		}
	}
	// A flattened head may report no parent; other actions may not.
	value := rbdCloneOutput(token, plan, "flatten")
	views(value, "clone_view")["parent"], views(value, "clone_view")["overlap"] = nil, nil
	if _, err := decodeRBDCloneNative("flatten", token, 9, plan, encode(value)); err != nil {
		t.Fatal("flattened head without a parent refused", err)
	}
	if _, err := decodeRBDCloneNative("setup", token, 9, plan, bytes.Repeat([]byte(" "), rbdBaseJSONLimit+1)); err == nil {
		t.Fatal("oversized output admitted")
	}
}

func TestRBDCloneBinaryViewsMatchReceipts(t *testing.T) {
	dir := t.TempDir()
	blobs := map[string]rbdBaseBlob{}
	for _, key := range rbdCloneBlobKeys("flatten") {
		name := rbdCloneBlobName("flatten", key)
		data := bytes.Repeat([]byte(key), 100)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		blobs[key] = rbdBaseBlob{Name: name, Length: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	if len(blobs) != 7 || len(rbdCloneBlobKeys("write")) != 5 {
		t.Fatal("clone binary view sets differ")
	}
	if err := checkRBDCloneBlobs(dir, "flatten", blobs); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDCloneBlobs(dir, "snapshot", blobs); err == nil {
		t.Fatal("binary views of another action admitted")
	}
	if err := os.WriteFile(filepath.Join(dir, rbdCloneBlobName("flatten", "clone_objects_snap")), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDCloneBlobs(dir, "flatten", blobs); err == nil {
		t.Fatal("changed binary view admitted")
	}
}

func TestRBDCloneJournalCarriesThePlan(t *testing.T) {
	dir, token := t.TempDir(), strings.Repeat("a1", 16)
	c := &fixtureCluster{profile: profile{fsid: "94b09669-3ee8-4a10-af82-346cb674debb"}}
	c.rbdBase = &rbdBaseScope{out: dir, token: token, plan: rbdBasePlanFor(token), writePlan: rbdWritePlanFor(token), lockPlan: rbdLockPlanFor(token), clonePlan: rbdClonePlanFor(token), poolID: -1}
	flatten, err := c.rbdBaseBeginAction("native-clone-flatten", rbdCloneTarget(c.rbdBase.clonePlan, "flatten"), 7, false)
	if err != nil || flatten.ReadOnly || flatten.Target != c.rbdBase.clonePlan.Clone+":flatten" {
		t.Fatal("flatten action journal", err)
	}
	if err := c.rbdBaseFinish(flatten, fmt.Errorf("native process killed")); err == nil || flatten.State != "uncertain" {
		t.Fatal("uncertain flatten not retained")
	}
	if _, err := c.rbdBaseBeginAction("native-clone-flatten", c.rbdBase.clonePlan.Clone, 8, false); err == nil {
		t.Fatal("one-shot flatten issued twice")
	}
	observe, err := c.rbdBaseBeginAction("native-clone-observe-copyup", c.rbdBase.clonePlan.Clone, 9, true)
	if err != nil || !observe.ReadOnly || c.rbdBaseFinish(observe, nil) != nil {
		t.Fatal("read-only clone observation journal", err)
	}
	var journal struct {
		ClonePlan rbdClonePlan       `json:"clone_plan"`
		Mutations []*rbdBaseMutation `json:"mutations"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "rbd-base-owned.json"))
	if err != nil || json.Unmarshal(data, &journal) != nil || !reflect.DeepEqual(journal.ClonePlan, rbdClonePlanFor(token)) || len(journal.Mutations) != 2 ||
		journal.Mutations[0].State != "uncertain" || journal.Mutations[0].ReadOnly || journal.Mutations[1].State != "applied" || !journal.Mutations[1].ReadOnly {
		t.Fatal("RBD clone journal differs", string(data), err)
	}
	// The control refuses to run outside a prepared RBD scope.
	if err := (&fixtureCluster{profile: profile{rbdBase: "1"}}).rbdCloneControl(t.Context(), dir, []string{"1", token, "setup"}); err == nil {
		t.Fatal("clone control admitted without a prepared scope")
	}
}

// The embedded clone driver must at least be valid Python where an
// interpreter is available; the live acceptance runs it against native
// librbd.
func TestRBDCloneNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdCloneNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD clone driver is not valid Python", err, string(output))
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestRBDJournalPlanIsTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdJournalPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != rbdBasePlanFor(token).Pool || plan.BaseImage != rbdBasePlanFor(token).Image ||
		plan.Image != "ceph-msgr-journal-"+token || plan.OracleClient != "ceph-msgr-oracle-a1a1a1a1" || plan.GoClient != "ceph-msgr-mirror-a1a1a1a1" || plan.Features != 69 {
		t.Fatal("owned RBD journal plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdJournalPlanFor(token)) || reflect.DeepEqual(plan, rbdJournalPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("journal plan is not deterministic and token-bound")
	}
	// Writes stay inside one object and below librbd's 16 KiB journal
	// payload; discards cover whole objects (64 KiB discard granularity).
	objectSize := uint64(1) << plan.Order
	for _, e := range slices.Concat(plan.SetupEvents, plan.WriteEvents, plan.ConcurrentOwnerEvents, plan.ConcurrentEvents) {
		inside := e.Length > 0 && e.Offset%4096 == 0 && e.Offset+e.Length <= plan.Size && e.Offset/objectSize == (e.Offset+e.Length-1)/objectSize
		if !inside || e.Kind == "write" && e.Length > 8<<10 || e.Kind == "discard" && (e.Offset%objectSize != 0 || e.Length != objectSize) || e.Kind != "write" && e.Kind != "discard" {
			t.Fatal("plan event is not a bounded write or a whole-object discard", e)
		}
	}
	for _, e := range slices.Concat(plan.GoWrites, plan.GoConcurrentWrites) {
		if e.Length == 0 || e.Length > 8<<10 || e.Offset+e.Length > plan.Size || e.Offset/objectSize != (e.Offset+e.Length-1)/objectSize {
			t.Fatal("Go journal write is not bounded", e)
		}
	}
	for name, change := range map[string]func(*rbdJournalPlan){
		"no journaling":  func(p *rbdJournalPlan) { p.Features = 5 },
		"foreign image":  func(p *rbdJournalPlan) { p.Image = p.BaseImage },
		"foreign client": func(p *rbdJournalPlan) { p.GoClient = "" },
		"no Go writes":   func(p *rbdJournalPlan) { p.GoWrites = nil },
		"no concurrency": func(p *rbdJournalPlan) { p.ConcurrentEvents = nil },
		"no Go appender": func(p *rbdJournalPlan) { p.GoConcurrentWrites = nil },
		"discard in session": func(p *rbdJournalPlan) {
			p.ConcurrentEvents = []rbdJournalEvent{{"discard", 0, 64 << 10}}
		},
	} {
		changed := rbdJournalPlanFor(token)
		change(&changed)
		if changed.owned(token) {
			t.Fatal("changed journal plan admitted", name)
		}
	}
	for _, field := range []string{"'ceph-msgr-journal-' + token", "'ceph-msgr-oracle-' + token[:8]", "'ceph-msgr-mirror-' + token[:8]", "plan.get('features') != 69"} {
		if !strings.Contains(rbdJournalNativePython, field) {
			t.Fatal("native journal driver no longer checks", field)
		}
	}
	// The concurrent session gives Go one entry per splay offset 0 to 2, and
	// one native aio write overlaps a Go extent so that the image shows the
	// order of the two owners.
	if len(plan.GoConcurrentWrites) != 3 || !slices.ContainsFunc(plan.ConcurrentEvents, func(e rbdJournalEvent) bool {
		return slices.ContainsFunc(plan.GoConcurrentWrites, func(g rbdBaseExtent) bool { return e.Offset < g.Offset+g.Length && g.Offset < e.Offset+e.Length })
	}) {
		t.Fatal("concurrent plan lacks an overlap between the two owners", plan)
	}
	for _, field := range []string{"image.aio_write(", "wait_proceed(3)", "'rbd-journal-proceed-%d'", "'rbd-journal-state.json'"} {
		if !strings.Contains(rbdJournalNativePython, field) {
			t.Fatal("native journal driver concurrent session lacks", field)
		}
	}
	// A user flush would journal an AioFlush event the test does not plan.
	if strings.Contains(rbdJournalNativePython, ".flush()") {
		t.Fatal("native journal driver flushes")
	}
	for _, driver := range []string{rbdLockNativePython, rbdWriteNativePython} {
		if !strings.Contains(driver, "'ceph-msgr-journal-' + token") {
			t.Fatal("the lock and write setups must admit the journaling image in the owned pool")
		}
	}
}

func TestRBDJournalRequestIsClosedAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for next := range rbdJournalActions {
		for i, action := range rbdJournalActions {
			id, got, err := parseRBDJournalRequest([]string{"7", token, action}, token, 6, next)
			if (err == nil) != (i == next) || err == nil && (id != 7 || got != action) {
				t.Fatal("RBD journal action order differs", next, action, err)
			}
		}
	}
	if _, _, err := parseRBDJournalRequest([]string{"99", token, "replay"}, token, 1, len(rbdJournalActions)); err == nil {
		t.Fatal("action after the closed sequence admitted")
	}
	for _, fields := range [][]string{
		{"7", token, "reset"}, {"7", token, "client-disconnect"}, {"7", token}, {"7", token, "setup", "extra"},
		{"6", token, "setup"}, {"0", token, "setup"}, {"07", token, "setup"}, {"7", strings.Repeat("b2", 16), "setup"}, {"7", token, "write"},
	} {
		if _, _, err := parseRBDJournalRequest(fields, token, 6, 0); err == nil {
			t.Fatal("unscoped RBD journal request admitted", fields)
		}
	}
	if !rbdJournalReadOnly("observe-registered") || !rbdJournalReadOnly("observe-committed") || rbdJournalReadOnly("inspect") || rbdJournalReadOnly("replay") {
		t.Fatal("journal action classification differs")
	}
}

// rbdJournalOutput is a well-formed output of one journal action.
func rbdJournalOutput(token string, plan rbdJournalPlan, action string) map[string]any {
	id := "5678ef01"
	header, prefix := "journal."+id, "journal_data.3."+id+"."
	calls := []map[string]any{}
	for _, method := range rbdJournalCalls {
		calls = append(calls, map[string]any{"object": header, "class": "journal", "method": method, "input_hex": "", "code": 0, "output_hex": "00"})
	}
	objects, offset := []map[string]any{}, uint64(0)
	for i := 0; i < rbdJournalObjectCount; i++ {
		size := uint64(0)
		if i < 4 {
			size = 8300
		}
		o := map[string]any{"number": i, "object": fmt.Sprintf("%s%d", prefix, i), "exists": size != 0, "size": size, "offset": offset, "sha256": strings.Repeat("0", 64)}
		if size == 0 {
			o["sha256"] = ""
		}
		offset += size
		objects = append(objects, o)
	}
	cli := func(args ...string) map[string]any {
		return map[string]any{"argv": append([]string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "journal"}, args...),
			"returncode": 0, "stdout": "{}", "stderr": "", "elapsed_ns": 3, "parsed": map[string]any{}}
	}
	spec := plan.Pool + "/" + id
	value := map[string]any{
		"action": action, "token": token, "request_id": 9, "pool": plan.Pool, "pool_id": 3, "image": plan.Image, "native_global_id": 4242, "started_ns": 10, "finished_ns": 20,
		"image_id": id, "header_object": "rbd_header." + id, "journal_header": header, "journal_prefix": prefix,
		"stat":     map[string]any{"size": plan.Size, "obj_size": 65536, "num_objs": 4, "order": 16, "block_name_prefix": "rbd_data." + id},
		"features": 69, "flags": 0, "lockers": map[string]any{"empty": true, "tag": "", "exclusive": false, "lockers": []map[string]any{}}, "watchers": []map[string]any{},
		"class_calls": calls, "journal_objects": objects, "info": cli("info", "--format", "json", spec), "status": cli("status", "--format", "json", spec),
		"blobs": map[string]any{
			"image_head":      map[string]any{"name": rbdJournalBlobName(action, "image_head"), "length": plan.Size, "sha256": strings.Repeat("3", 64)},
			"journal_objects": map[string]any{"name": rbdJournalBlobName(action, "journal_objects"), "length": offset, "sha256": strings.Repeat("4", 64)},
		},
	}
	if rbdJournalInspected(action) {
		value["inspect"] = cli("inspect", "--verbose", spec)
	}
	events := func(phase string, planned []rbdJournalEvent) []map[string]any {
		result := []map[string]any{}
		for _, e := range planned {
			sum := strings.Repeat("1", 64)
			if e.Kind == "discard" {
				sum = ""
			}
			result = append(result, map[string]any{"phase": phase, "kind": e.Kind, "offset": e.Offset, "length": e.Length, "sha256": sum, "started_ns": 11, "finished_ns": 12})
		}
		return result
	}
	switch action {
	case "setup":
		value["oracle_register"] = map[string]any{"object": header, "class": "journal", "method": "client_register", "input_hex": "00", "code": 0, "output_hex": ""}
		value["events"], value["close_ns"] = events("setup", plan.SetupEvents), 5
	case "write":
		value["events"], value["close_ns"] = events("write", plan.WriteEvents), 5
	case "replay":
		value["acquire_ns"], value["owner"], value["close_ns"] = 7, true, 5
	case "concurrent":
		owner, issued, completed := events("concurrent-owner", plan.ConcurrentOwnerEvents), events("concurrent", plan.ConcurrentEvents), events("concurrent", plan.ConcurrentEvents)
		for _, e := range issued {
			delete(e, "finished_ns")
		}
		watchers := []map[string]any{{"addr": "10.0.0.1:0/1", "id": 4242, "cookie": 7}}
		returns := make([]int, len(plan.ConcurrentEvents))
		value["events"], value["close_ns"], value["aio_return_values"], value["final_wait_ns"] = slices.Concat(owner, completed), 5, returns, 6
		value["phases"] = []map[string]any{
			{"phase": "owner", "time_ns": 1, "is_owner": true, "owners": []map[string]any{{"mode": 0, "owner": "10.0.0.1:0/1"}}, "watchers": watchers, "events": owner},
			{"phase": "issued", "time_ns": 2, "is_owner": false, "owners": []map[string]any{}, "watchers": watchers, "wait_ns": 3, "events": issued},
			{"phase": "completed", "time_ns": 3, "is_owner": true, "owners": []map[string]any{}, "watchers": watchers, "events": completed, "aio_return_values": returns},
			{"phase": "closed", "time_ns": 4, "is_owner": false, "owners": []map[string]any{}, "watchers": []map[string]any{}, "wait_ns": 4},
		}
	}
	return value
}

func TestRBDJournalNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdJournalPlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	for _, action := range rbdJournalActions {
		if _, err := decodeRBDJournalNative(action, token, 9, plan, encode(rbdJournalOutput(token, plan, action))); err != nil {
			t.Fatal("well-formed journal output refused", action, err)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"unknown field":      func(v map[string]any) { v["extra"] = 1 },
		"other image":        func(v map[string]any) { v["image"] = plan.BaseImage },
		"other prefix":       func(v map[string]any) { v["journal_prefix"] = "journal_data.4.5678ef01." },
		"no journaling":      func(v map[string]any) { v["features"] = 5 },
		"missing object":     func(v map[string]any) { v["journal_objects"] = v["journal_objects"].([]map[string]any)[:7] },
		"missing inspect":    func(v map[string]any) { delete(v, "inspect") },
		"failed register":    func(v map[string]any) { v["oracle_register"].(map[string]any)["code"] = -17 },
		"short events":       func(v map[string]any) { v["events"] = v["events"].([]map[string]any)[:2] },
		"foreign class call": func(v map[string]any) { v["class_calls"].([]map[string]any)[0]["class"] = "rbd" },
		"other cli argv":     func(v map[string]any) { v["status"].(map[string]any)["argv"] = []string{"journal", "status"} },
		"cli without output": func(v map[string]any) { delete(v["info"].(map[string]any), "parsed") },
	} {
		value := rbdJournalOutput(token, plan, "setup")
		change(value)
		if _, err := decodeRBDJournalNative("setup", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed journal output admitted", name)
		}
	}
	observe := rbdJournalOutput(token, plan, "observe-registered")
	observe["events"] = rbdJournalOutput(token, plan, "write")["events"]
	if _, err := decodeRBDJournalNative("observe-registered", token, 9, plan, encode(observe)); err == nil {
		t.Fatal("observation with native events admitted")
	}
	if _, err := decodeRBDJournalNative("replay", token, 9, plan, encode(rbdJournalOutput(token, plan, "write"))); err == nil {
		t.Fatal("write output admitted as replay")
	}
	phases := func(v map[string]any) []map[string]any { return v["phases"].([]map[string]any) }
	for name, change := range map[string]func(map[string]any){
		"missing phase":          func(v map[string]any) { v["phases"] = phases(v)[:3] },
		"reordered phases":       func(v map[string]any) { p := phases(v); p[1], p[2] = p[2], p[1] },
		"owner without lock":     func(v map[string]any) { phases(v)[0]["is_owner"] = false },
		"completed without lock": func(v map[string]any) { phases(v)[2]["is_owner"] = false },
		"issued already done":    func(v map[string]any) { phases(v)[1]["events"].([]map[string]any)[0]["finished_ns"] = 9 },
		"watcher after close":    func(v map[string]any) { phases(v)[3]["watchers"] = phases(v)[0]["watchers"] },
		"missing aio results":    func(v map[string]any) { delete(v, "aio_return_values") },
		"no final signal":        func(v map[string]any) { delete(v, "final_wait_ns") },
		"no inspect":             func(v map[string]any) { delete(v, "inspect") },
		"native events only":     func(v map[string]any) { v["events"] = v["events"].([]map[string]any)[2:] },
	} {
		value := rbdJournalOutput(token, plan, "concurrent")
		change(value)
		if _, err := decodeRBDJournalNative("concurrent", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed concurrent journal output admitted", name)
		}
	}
	replay := rbdJournalOutput(token, plan, "replay")
	replay["phases"] = rbdJournalOutput(token, plan, "concurrent")["phases"]
	if _, err := decodeRBDJournalNative("replay", token, 9, plan, encode(replay)); err == nil {
		t.Fatal("replay output with concurrent phases admitted")
	}
	if err := (&fixtureCluster{profile: profile{rbdBase: "1"}}).rbdJournalControl(t.Context(), t.TempDir(), []string{"1", token, "setup"}); err == nil {
		t.Fatal("journal control admitted without a prepared scope")
	}
}

// The embedded journal driver must at least be valid Python where an
// interpreter is available; the live acceptance runs it against native
// librbd.
func TestRBDJournalNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdJournalNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD journal driver is not valid Python", err, string(output))
	}
}

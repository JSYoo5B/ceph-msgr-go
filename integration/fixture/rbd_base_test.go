package main

import (
	"bytes"
	"context"
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

func rbdBaseProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"}
}

func TestRBDBaseClosedProfileAndCredentials(t *testing.T) {
	for _, runtime := range []string{"host", "container"} {
		env := rbdBaseProfileEnv()
		env["CEPH_MSGR_TEST_RUNTIME"] = runtime
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil || p.testRun != "^TestCephRBD(Base|Write)Integration$" || p.rbdBase != "1" || p.mdsFresh != "0" || p.osdPGV8() || p.cleanupBudget() != 2*time.Minute {
			t.Fatal("RBD base did not select its isolated legacy-v6 fixture", p, err)
		}
		if bootstrap := fixtureBootstrapConfig(p); !strings.Contains(bootstrap, "osd_pool_default_flag_hashpspool = false") || strings.Contains(bootstrap, "mon_allow_pool_delete") {
			t.Fatal("RBD base bootstrap must keep legacy pools and the default pool-deletion policy", bootstrap)
		}
		p.rbdBaseIdentity = "client.rbd-base-owned"
		for _, dir := range []string{"/out", t.TempDir()} {
			x := p.testEnvironment(dir, "")
			if x["CEPH_MSGR_RBD_BASE_IDENTITY"] != p.rbdBaseIdentity || x["CEPH_MSGR_RBD_BASE_KEY_FILE"] != filepath.Join(dir, "rbd-base.key") || x["CEPH_MSGR_TEST_RBD"] != "1" ||
				p.containerEnvironment()["CEPH_MSGR_TEST_RBD"] != "1" || x["CEPH_MSGR_MDS_FRESH_IDENTITY"] != "" || x["CEPH_MSGR_KEY_FILE"] != filepath.Join(dir, "key") {
				t.Fatal("RBD base credentials were mixed or lost", x)
			}
		}
	}
	// Without the opt-in, no RBD credential reaches the suite.
	plain, err := loadProfile(func(k string) string { return map[string]string{"CEPH_MSGR_TEST_OSD": "1"}[k] })
	if err != nil || plain.testEnvironment("/out", "")["CEPH_MSGR_RBD_BASE_KEY_FILE"] != "" || plain.testEnvironment("/out", "")["CEPH_MSGR_TEST_RBD"] != "0" {
		t.Fatal("ordinary OSD profile exposes RBD credentials", err)
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_RBD": "true"}, {"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"}, {"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		{"CEPH_MSGR_TEST_MDS_FRESH": "1"}, {"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_MDS_CEPHFS": "1"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephOSDIOIntegration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCeph(RBDBase|OSDIO)Integration$"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephRBDBaseIntegration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCephRBDWriteIntegration$"}, {"CEPH_MSGR_TEST_RUN": "^TestCephRBD.*Integration$"},
		{"CEPH_MSGR_TEST_SPARSE_READ_ORACLE": "/tmp/oracle"}, {"CEPH_MSGR_TEST_MGR_COUNT": "0"},
	} {
		env := rbdBaseProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe RBD base profile accepted", change, p, err)
		}
	}
	env := rbdBaseProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = "^TestCephRBD(Base|Write)Integration$"
	env["CEPH_MSGR_TEST_OSD_STORE"] = "bluestore"
	env["CEPH_MSGR_TEST_CONNECTION_MODE"] = "crc"
	if p, err := loadProfile(func(k string) string { return env[k] }); err != nil || p.testRun != env["CEPH_MSGR_TEST_RUN"] {
		t.Fatal("explicit exclusive RBD base selector refused", p, err)
	}
}

func TestRBDBasePlanAndCapsAreTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdBasePlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != "ceph-msgr-rbd-"+token || plan.Image != "ceph-msgr-image-"+token || plan.Snapshot != "ceph-msgr-snap-a1a1a1a1" {
		t.Fatal("owned RBD plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdBasePlanFor(token)) || reflect.DeepEqual(plan, rbdBasePlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("plan is not deterministic and token-bound")
	}
	objectSize := uint64(1) << plan.Order
	touched := map[uint64]bool{}
	for _, e := range append(append([]rbdBaseExtent{}, plan.PreWrites...), plan.PostWrites...) {
		if e.Length == 0 || e.Offset%4096 != 0 || e.Length%4096 != 0 || e.Offset+e.Length > plan.Size {
			t.Fatal("plan write is not a 4 KiB-aligned extent inside the image", e)
		}
		for o := e.Offset / objectSize; o <= (e.Offset+e.Length-1)/objectSize; o++ {
			touched[o] = true
		}
	}
	// Objects 0 to 4 are written, 5 to 7 stay absent: several objects, an
	// absent object range and in-object holes.
	if plan.Size/objectSize != 8 || len(touched) != 5 || touched[5] || touched[6] || touched[7] || plan.Features != 1 || plan.ResizeAck <= plan.Size || plan.ResizeNoAck <= plan.ResizeAck {
		t.Fatal("RBD plan layout changed", touched, plan)
	}
	caps := rbdBaseCaps(plan.Pool)
	if caps.Mon != "profile rbd" || caps.OSD != "profile rbd pool="+plan.Pool || caps.MGR != "" || caps.MDS != "" {
		t.Fatal("RBD identities must hold only the RBD profile bound to the owned pool", caps)
	}
	foreign := plan
	foreign.Features = 61
	if foreign.owned(token) {
		t.Fatal("plan with more image features admitted")
	}
}

func TestRBDBaseRequestIsClosedAndScoped(t *testing.T) {
	token := strings.Repeat("a1", 16)
	id, action, err := parseRBDBaseRequest([]string{"7", token, "watch"}, token, 6)
	if err != nil || id != 7 || action != "watch" {
		t.Fatal("valid RBD base request refused", id, action, err)
	}
	for _, fields := range [][]string{
		{"7", token, "setup"}, {"7", token, "resize"}, {"7", token}, {"7", token, "watch", "extra"},
		{"6", token, "watch"}, {"0", token, "watch"}, {"07", token, "watch"}, {"7", strings.Repeat("b2", 16), "watch"},
	} {
		if _, _, err := parseRBDBaseRequest(fields, token, 6); err == nil {
			t.Fatal("unscoped RBD base request admitted", fields)
		}
	}
	if _, _, err := parseRBDBaseRequest([]string{"7", "zz", "watch"}, "zz", 0); err == nil {
		t.Fatal("non-token scope admitted")
	}
}

// rbdBaseSetupOutput is a well-formed setup output for the plan.
func rbdBaseSetupOutput(token string, plan rbdBasePlan) map[string]any {
	prefix := "rbd_data.1234abcd"
	calls := []map[string]any{}
	for i, method := range rbdBaseClassMethods {
		object := "rbd_header.1234abcd"
		if i == 0 {
			object = "rbd_id." + plan.Image
		}
		calls = append(calls, map[string]any{"object": object, "method": method, "input_hex": "", "code": 0, "output_hex": "00"})
	}
	objects, offsets := []map[string]any{}, map[string]uint64{"head": 0, "snap": 0}
	for i := 0; i < 8; i++ {
		entry := map[string]any{"index": i, "name": fmt.Sprintf("%s.%016x", prefix, i)}
		for _, label := range []string{"head", "snap"} {
			size := uint64(0)
			if i < 3 {
				size = 4096
			}
			view := map[string]any{"exists": size != 0, "errno": 0, "size": size, "offset": offsets[label], "sha256": strings.Repeat("0", 64)}
			if size == 0 {
				view["errno"], view["sha256"] = 2, ""
			}
			entry[label] = view
			offsets[label] += size
		}
		objects = append(objects, entry)
	}
	writes := []map[string]any{}
	for _, e := range plan.PreWrites {
		writes = append(writes, map[string]any{"phase": "pre", "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("1", 64)})
	}
	for _, e := range plan.PostWrites {
		writes = append(writes, map[string]any{"phase": "post", "offset": e.Offset, "length": e.Length, "written": e.Length, "sha256": strings.Repeat("2", 64)})
	}
	blobs := map[string]any{}
	for key, name := range rbdBaseBlobNames {
		length := plan.Size
		if strings.HasPrefix(key, "objects_") {
			length = 3 * 4096
		}
		blobs[key] = map[string]any{"name": name, "length": length, "sha256": strings.Repeat("3", 64)}
	}
	return map[string]any{
		"action": "setup", "token": token, "request_id": 0, "pool": plan.Pool, "pool_id": 3, "image": plan.Image, "native_global_id": 4242,
		"image_id": "1234abcd", "header_object": "rbd_header.1234abcd", "id_object": "rbd_id." + plan.Image,
		"stat":     map[string]any{"size": plan.Size, "obj_size": 65536, "num_objs": 8, "order": 16, "block_name_prefix": prefix, "parent_pool": uint64(1<<64 - 1), "parent_name": ""},
		"features": 1, "op_features": 0, "flags": 0, "stripe_unit": 65536, "stripe_count": 1, "data_pool_id": 3,
		"snapshots":   []map[string]any{{"id": 4, "name": plan.Snapshot, "size": plan.Size, "namespace": 0}},
		"class_calls": calls, "writes": writes, "objects": objects, "blobs": blobs, "pool_objects": []string{"rbd_directory"},
	}
}

func rbdBaseWatchOutput(token string, plan rbdBasePlan) map[string]any {
	setup := rbdBaseSetupOutput(token, plan)
	for _, key := range []string{"writes", "objects", "blobs", "pool_objects"} {
		delete(setup, key)
	}
	setup["action"], setup["request_id"] = "watch", 9
	setup["stat"].(map[string]any)["size"] = plan.ResizeNoAck
	watchers := []map[string]any{{"addr": "172.19.0.9:0/1", "id": 4242, "cookie": 1}}
	setup["phases"] = []map[string]any{
		{"phase": "opened", "size": plan.Size, "watchers": watchers},
		{"phase": "resized-acknowledged", "size": plan.ResizeAck, "watchers": watchers, "resize_ns": 1000, "wait_ns": 5},
		{"phase": "resized-unacknowledged", "size": plan.ResizeNoAck, "watchers": watchers, "resize_ns": 5000000000, "wait_ns": 5},
	}
	return setup
}

func TestRBDBaseNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdBasePlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if _, err := decodeRBDBaseNative("setup", token, 0, plan, encode(rbdBaseSetupOutput(token, plan))); err != nil {
		t.Fatal("valid setup output refused", err)
	}
	if got, err := decodeRBDBaseNative("watch", token, 9, plan, encode(rbdBaseWatchOutput(token, plan))); err != nil || len(got.Phases) != 3 || got.Stat.Size != plan.ResizeNoAck {
		t.Fatal("valid watch output refused", err)
	}
	if _, err := decodeRBDBaseNative("watch", token, 8, plan, encode(rbdBaseWatchOutput(token, plan))); err == nil {
		t.Fatal("watch output of another request admitted")
	}
	for name, change := range map[string]func(map[string]any){
		"unknown field":        func(v map[string]any) { v["extra"] = 1 },
		"foreign token":        func(v map[string]any) { v["token"] = strings.Repeat("b2", 16) },
		"foreign pool":         func(v map[string]any) { v["pool"] = "rbd" },
		"zero global ID":       func(v map[string]any) { v["native_global_id"] = 0 },
		"header of another id": func(v map[string]any) { v["header_object"] = "rbd_header.other" },
		"more features":        func(v map[string]any) { v["features"] = 61 },
		"fancy striping":       func(v map[string]any) { v["stripe_count"] = 2 },
		"parent":               func(v map[string]any) { v["stat"].(map[string]any)["parent_pool"] = 1 },
		"second snapshot": func(v map[string]any) {
			v["snapshots"] = append(v["snapshots"].([]map[string]any), map[string]any{"id": 5, "name": "x", "size": 1, "namespace": 0})
		},
		"missing class call": func(v map[string]any) { v["class_calls"] = v["class_calls"].([]map[string]any)[1:] },
		"short write":        func(v map[string]any) { v["writes"].([]map[string]any)[0]["written"] = 1 },
		"misnamed object":    func(v map[string]any) { v["objects"].([]map[string]any)[1]["name"] = "rbd_data.other.0000000000000001" },
		"absent with bytes": func(v map[string]any) {
			v["objects"].([]map[string]any)[7]["head"].(map[string]any)["size"] = 1
		},
		"missing blob":     func(v map[string]any) { delete(v["blobs"].(map[string]any), "image_snap") },
		"oversized pool":   func(v map[string]any) { v["pool_objects"] = make([]string, 65) },
		"phases on setup":  func(v map[string]any) { v["phases"] = []map[string]any{} },
		"setup in phases?": func(v map[string]any) { v["action"] = "watch" },
	} {
		value := rbdBaseSetupOutput(token, plan)
		change(value)
		if _, err := decodeRBDBaseNative("setup", token, 0, plan, encode(value)); err == nil {
			t.Fatal("malformed setup output admitted:", name)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"phase order":      func(v map[string]any) { p := v["phases"].([]map[string]any); p[1], p[2] = p[2], p[1] },
		"missing phase":    func(v map[string]any) { v["phases"] = v["phases"].([]map[string]any)[:2] },
		"unresized size":   func(v map[string]any) { v["stat"].(map[string]any)["size"] = plan.Size },
		"opened resize":    func(v map[string]any) { v["phases"].([]map[string]any)[0]["resize_ns"] = 1 },
		"objects on watch": func(v map[string]any) { v["pool_objects"] = []string{"x"} },
	} {
		value := rbdBaseWatchOutput(token, plan)
		change(value)
		if _, err := decodeRBDBaseNative("watch", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed watch output admitted:", name)
		}
	}
	if _, err := decodeRBDBaseNative("setup", token, 0, plan, bytes.Repeat([]byte(" "), rbdBaseJSONLimit+1)); err == nil {
		t.Fatal("oversized output admitted")
	}
}

func TestRBDBaseBinaryViewsMatchReceipts(t *testing.T) {
	dir := t.TempDir()
	blobs := map[string]rbdBaseBlob{}
	for key, name := range rbdBaseBlobNames {
		data := bytes.Repeat([]byte(key), 100)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		blobs[key] = rbdBaseBlob{Name: name, Length: uint64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	}
	if err := checkRBDBaseBlobs(dir, blobs); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, rbdBaseBlobNames["image_snap"]), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkRBDBaseBlobs(dir, blobs); err == nil {
		t.Fatal("changed binary view admitted")
	}
}

func TestRBDBaseJournalIsOneShotAndCleanupIsCached(t *testing.T) {
	dir, token := t.TempDir(), strings.Repeat("a1", 16)
	c := &fixtureCluster{profile: profile{fsid: "94b09669-3ee8-4a10-af82-346cb674debb"}}
	c.rbdBase = &rbdBaseScope{out: dir, token: token, plan: rbdBasePlanFor(token), poolID: -1}
	m, err := c.rbdBaseBegin("native-resize", "image", 7)
	if err != nil || m.State != "intended" {
		t.Fatal("journal before issue", err)
	}
	if _, err := c.rbdBaseBegin("native-resize", "image", 8); err == nil {
		t.Fatal("one-shot native mutation issued twice")
	}
	if err := c.rbdBaseFinish(m, fmt.Errorf("native process lost")); err == nil || m.State != "uncertain" {
		t.Fatal("uncertain native mutation not retained")
	}
	var journal struct {
		Token     string             `json:"token"`
		Mutations []*rbdBaseMutation `json:"mutations"`
	}
	data, err := os.ReadFile(filepath.Join(dir, "rbd-base-owned.json"))
	if err != nil || json.Unmarshal(data, &journal) != nil || journal.Token != token || len(journal.Mutations) != 1 || journal.Mutations[0].State != "uncertain" {
		t.Fatal("RBD base journal differs", string(data), err)
	}
	// Nothing owned in the cluster: cleanup completes without provider calls,
	// and a repeated call returns the cached result.
	if err := c.finishRBDBase(context.Background()); err != nil {
		t.Fatal(err)
	}
	var receipt rbdBaseCleanup
	data, err = os.ReadFile(filepath.Join(dir, "rbd-base-cleanup.json"))
	if err != nil || json.Unmarshal(data, &receipt) != nil || !receipt.Completed || !receipt.PoolRemoved || !receipt.ClientIdentityRemoved {
		t.Fatal("empty-scope cleanup receipt differs", string(data), err)
	}
	if err := c.finishRBDBase(context.Background()); err != nil {
		t.Fatal("cached cleanup changed", err)
	}
	// An intended identity whose creation outcome is unknown is reported.
	c.rbdBase = &rbdBaseScope{out: dir, token: token, plan: rbdBasePlanFor(token), poolID: -1, goIntent: true}
	if err := c.finishRBDBase(context.Background()); err == nil || !strings.Contains(err.Error(), "outcome is unknown") {
		t.Fatal("unknown identity outcome hidden", err)
	}
}

// The embedded driver must at least be valid Python where an interpreter is
// available; the live acceptance runs it against native librbd.
func TestRBDBaseNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdBaseNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD driver is not valid Python", err, string(output))
	}
}

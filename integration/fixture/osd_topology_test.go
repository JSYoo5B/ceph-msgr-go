package main

import (
	"cmp"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestOSDTopologyVocabularySeparatesDaemonsFromReplication(t *testing.T) {
	want := map[string]osdTopology{
		"1": {daemons: 1, poolSize: 1, poolMinSize: 1},
		"3": {daemons: 3, poolSize: 3, poolMinSize: 2},
		"4": {daemons: 4, poolSize: 3, poolMinSize: 2, erasureCapacity: true},
	}
	if len(osdTopologies) != len(want) {
		t.Fatal("OSD topology vocabulary changed", osdTopologies)
	}
	for name, topology := range want {
		if osdTopologies[name] != topology || (profile{osdReplicas: name}).osdTopology() != topology {
			t.Fatalf("topology %s = %+v, want %+v", name, osdTopologies[name], topology)
		}
	}
	if got := strings.Join(osdTopologyNames(), ","); got != "1,3,4" {
		t.Fatal("topology names", got)
	}
	if (profile{osdReplicas: "2"}).osdTopology().daemons != 0 {
		t.Fatal("an unknown topology must have no daemons")
	}
}

func TestOSDTopologyErasureCapacityProfile(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "4"}
	for _, osdProfile := range []string{"legacy-v6", "replicated-pg-v8"} {
		for _, selector := range append([]string{""}, osdErasureWidthSelectors...) {
			env := copyProfileEnv(base)
			env["CEPH_MSGR_TEST_OSD_PROFILE"], env["CEPH_MSGR_TEST_RUN"] = osdProfile, selector
			p, err := loadProfile(func(name string) string { return env[name] })
			if err != nil {
				t.Fatal(osdProfile, selector, err)
			}
			want := cmp.Or(selector, "^TestCephOSDErasureK2M2Integration$")
			if p.testRun != want || p.testTimeout != "30m" || p.containerEnvironment()["CEPH_MSGR_TEST_OSD_REPLICAS"] != "4" ||
				p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_REPLICAS"] != "4" {
				t.Fatalf("four-OSD profile %+v", p)
			}
		}
	}
	for name, change := range map[string]map[string]string{
		"no OSD":            {"CEPH_MSGR_TEST_OSD": "0"},
		"MemStore":          {"CEPH_MSGR_TEST_OSD_STORE": "memstore"},
		"reply-v8":          {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"},
		"request-v9":        {"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-request-v9-reply-v8"},
		"broad selector":    {"CEPH_MSGR_TEST_RUN": "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		"legacy EC test":    {"CEPH_MSGR_TEST_RUN": "^TestCephOSDErasureIntegration$"},
		"k2m1 failover":     {"CEPH_MSGR_TEST_RUN": "^TestCephOSDErasureOptimizedFailoverIntegration$"},
		"joined selectors":  {"CEPH_MSGR_TEST_RUN": "^TestCephOSDErasure(K2M2|OptimizedK2M2(Failover)?)Integration$"},
		"joined in-flight":  {"CEPH_MSGR_TEST_RUN": "^TestCephOSDErasureOptimizedK2M2(Failover|InFlightMutation)Integration$"},
		"k2m1 snapshot":     {"CEPH_MSGR_TEST_RUN": "^TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration$"},
		"soak":              {"CEPH_MSGR_TEST_OSD_SOAK": "4m"},
		"MON freeze":        {"CEPH_MSGR_TEST_MON_FREEZE": "1"},
		"cache pools":       {"CEPH_MSGR_TEST_OSD_CACHE_POOLS": "1"},
		"MGR service":       {"CEPH_MSGR_TEST_MGR_SERVICE": "1"},
		"RBD":               {"CEPH_MSGR_TEST_RBD": "1"},
		"MDS":               {"CEPH_MSGR_TEST_MDS_FRESH": "1"},
		"WRITESAME oracle":  {"CEPH_MSGR_TEST_WRITESAME_ORACLE": "/oracle"},
		"sparse oracle":     {"CEPH_MSGR_TEST_SPARSE_READ_ORACLE": "/oracle"},
		"unknown topology":  {"CEPH_MSGR_TEST_OSD_REPLICAS": "2"},
		"padded topology":   {"CEPH_MSGR_TEST_OSD_REPLICAS": "04"},
		"arbitrary integer": {"CEPH_MSGR_TEST_OSD_REPLICAS": "6"},
	} {
		env := copyProfileEnv(base)
		for key, value := range change {
			env[key] = value
		}
		if p, err := loadProfile(func(name string) string { return env[name] }); err == nil {
			t.Fatalf("%s: four-OSD profile accepted: %+v", name, p)
		}
	}
}

func TestOwnedOSDInventoryMatchesTopology(t *testing.T) {
	ids, err := ownedOSDInventory([]int{3, 1, 0, 2}, osdTopologies["4"])
	if err != nil || len(ids) != 4 || ids[0] != 0 || ids[3] != 3 {
		t.Fatal(ids, err)
	}
	for name, started := range map[string][]int{
		"missing daemon":   {0, 1, 2},
		"extra daemon":     {0, 1, 2, 3, 4},
		"gap":              {0, 1, 2, 4},
		"duplicate":        {0, 1, 1, 2},
		"not from osd.0":   {1, 2, 3, 4},
		"empty":            nil,
		"negative":         {-1, 0, 1, 2},
		"three of a four":  {0, 1, 3},
		"inventory of one": {0},
	} {
		if _, err := ownedOSDInventory(started, osdTopologies["4"]); err == nil {
			t.Fatal("inventory accepted:", name)
		}
	}
	if _, err := ownedOSDInventory([]int{0}, osdTopology{}); err == nil {
		t.Fatal("zero topology accepted")
	}
}

func TestLegacyCRUSHForInventoryExtendsOnlyDevicesAndRoot(t *testing.T) {
	template, err := os.ReadFile("legacy.crush.txt")
	if err != nil {
		t.Fatal(err)
	}
	single, err := legacyCRUSHForInventory(string(template), []int{0})
	if err != nil || single != string(template) {
		t.Fatal("one-OSD inventory must keep the template unchanged", err)
	}
	three, err := legacyCRUSHForInventory(string(template), []int{0, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	// The original three-OSD expansion, byte for byte.
	want := strings.Replace(string(template), "device 0 osd.0", "device 0 osd.0\ndevice 1 osd.1\ndevice 2 osd.2", 1)
	want = strings.Replace(want, "item osd.0 weight 1.00000", "item osd.0 weight 1.00000\n    item osd.1 weight 1.00000\n    item osd.2 weight 1.00000", 1)
	if three != want {
		t.Fatalf("three-OSD map changed:\n%s", three)
	}
	four, err := legacyCRUSHForInventory(string(template), []int{0, 1, 2, 3})
	if err != nil || !strings.Contains(four, "device 3 osd.3\ntype 0 osd") || !strings.Contains(four, "    item osd.3 weight 1.00000\n}") ||
		strings.Count(four, "device ") != 4 || strings.Count(four, "item osd.") != 4 {
		t.Fatalf("four-OSD map (template bytes=%d, CRLF=%d):\n%q %v", len(template), strings.Count(string(template), "\r\n"), four, err)
	}
	for name, bad := range map[string]string{
		"already extended": three,
		"missing device":   strings.Replace(string(template), "device 0 osd.0", "", 1),
		"missing item":     strings.Replace(string(template), "item osd.0 weight 1.00000", "", 1),
	} {
		if _, err := legacyCRUSHForInventory(bad, []int{0, 1, 2, 3}); err == nil {
			t.Fatal("template accepted:", name)
		}
	}
	if _, err := legacyCRUSHForInventory(string(template), []int{1, 2}); err == nil {
		t.Fatal("inventory without osd.0 accepted")
	}
}

func TestVerifyCRUSHInventoryRequiresTheOwnedRoot(t *testing.T) {
	type device struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	type item struct {
		ID     int   `json:"id"`
		Weight int64 `json:"weight"`
		Pos    int   `json:"pos"`
	}
	type bucket struct {
		ID       int    `json:"id"`
		Name     string `json:"name"`
		TypeName string `json:"type_name"`
		Items    []item `json:"items"`
	}
	build := func(edit func(*[]device, *[]bucket)) []byte {
		devices := []device{{0, "osd.0"}, {1, "osd.1"}, {2, "osd.2"}, {3, "osd.3"}}
		buckets := []bucket{{-1, "default", "root", []item{{0, 0x10000, 0}, {1, 0x10000, 1}, {2, 0x10000, 2}, {3, 0x10000, 3}}}}
		if edit != nil {
			edit(&devices, &buckets)
		}
		data, _ := json.Marshal(map[string]any{"devices": devices, "buckets": buckets, "rules": []any{}})
		return data
	}
	ids := []int{0, 1, 2, 3}
	if err := verifyCRUSHInventory(build(nil), ids); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*[]device, *[]bucket){
		"missing device":   func(d *[]device, _ *[]bucket) { *d = (*d)[:3] },
		"renamed device":   func(d *[]device, _ *[]bucket) { (*d)[3].Name = "osd.9" },
		"foreign device":   func(d *[]device, _ *[]bucket) { (*d)[3] = device{7, "osd.7"} },
		"missing item":     func(_ *[]device, b *[]bucket) { (*b)[0].Items = (*b)[0].Items[:3] },
		"weighted item":    func(_ *[]device, b *[]bucket) { (*b)[0].Items[2].Weight = 0 },
		"duplicate item":   func(_ *[]device, b *[]bucket) { (*b)[0].Items[3].ID = 2 },
		"host bucket":      func(_ *[]device, b *[]bucket) { (*b)[0].TypeName = "host" },
		"renamed root":     func(_ *[]device, b *[]bucket) { (*b)[0].Name = "other" },
		"second bucket":    func(_ *[]device, b *[]bucket) { *b = append(*b, bucket{-2, "default~hdd", "root", nil}) },
		"no bucket at all": func(_ *[]device, b *[]bucket) { *b = nil },
	} {
		if err := verifyCRUSHInventory(build(edit), ids); err == nil {
			t.Fatal("installed CRUSH inventory accepted:", name)
		}
	}
	if err := verifyCRUSHInventory([]byte(`{"devices":[{"name":"osd.0"}],"buckets":[]}`), []int{0}); err == nil {
		t.Fatal("device without ID accepted")
	}
	if err := verifyCRUSHInventory([]byte("not json"), ids); err == nil {
		t.Fatal("malformed dump accepted")
	}
}

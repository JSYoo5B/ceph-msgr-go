package main

import (
	"fmt"
	"strings"
	"testing"
)

// The owned CHARMAP requirement is a charmap-run action only, and its guards
// read the native images exactly: a session-dump feature image declares bit
// 22 only in its least significant block, and the map must require charmap
// alone (or nothing) to count as settled.
func TestMDSCephFSCharmapRequirementIsOwnedAndExact(t *testing.T) {
	charmap := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSCharmapIntegration$", capabilities: capMDSCharmap.name}
	other := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSCompletionIntegration$"}
	for _, action := range []string{"charmap-require-on", "charmap-require-off", "restart-fire-require"} {
		if err := admitMDSCephFSCharmap(action, charmap); err != nil {
			t.Fatal("charmap requirement refused under its capability", action, err)
		}
		if err := admitMDSCephFSCharmap(action, other); err == nil {
			t.Fatal("charmap requirement admitted without its capability", action)
		}
	}
	for printed, want := range map[string]bool{
		"0x00000000005cb7ff":                 true,  // the Go opt-in
		"0x0000000000ffffff":                 true,  // the stock client
		"0x00000000001cb7ff":                 false, // the default CephFS profile
		"0x00000000000005ff":                 false, // the session-only profile
		"0x00000000004000000000000000000000": false, // bit 86, not 22
		"0x000000000000000100000000005cb7ff": true,
		"0x5cb7ff":                           false,
		"00000000005cb7ff":                   false,
		"0x":                                 false,
		"0x00000000005cb7fg":                 false,
	} {
		if got := mdsCephFSCharmapDeclares(printed); got != want {
			t.Fatal("charmap declaration of a native feature image differs", printed, got)
		}
	}
	for status, want := range map[*mdsCephFSRankStatus]bool{
		{Failed: []int{0}, Daemons: []mdsCephFSRankDaemon{}}:                                         true,
		{Failed: []int{}, Daemons: []mdsCephFSRankDaemon{}}:                                          false,
		{Failed: []int{0}, Daemons: []mdsCephFSRankDaemon{{Name: "a", Rank: 0, State: "up:replay"}}}: false,
		{Failed: []int{0}, Rank0: &mdsCephFSRankDaemon{Name: "a"}, Daemons: []mdsCephFSRankDaemon{}}: false,
	} {
		if mdsCephFSCharmapRankUnheld(*status) != want {
			t.Fatal("ordered requirement rank check differs", *status)
		}
	}
	for _, tc := range []struct {
		required map[string]string
		on, off  bool
	}{
		{nil, false, true},
		{map[string]string{}, false, true},
		{map[string]string{"feature_22": "charmap"}, true, false},
		{map[string]string{"feature_22": "charmap", "feature_9": "reply_encoding"}, false, false},
		{map[string]string{"feature_22": "blockdiff"}, false, false},
		{map[string]string{"feature_23": "charmap"}, false, false},
	} {
		m := mdsSessionsMap{Required: tc.required}
		if mdsCephFSCharmapRequiresOnly(m, true) != tc.on || mdsCephFSCharmapRequiresOnly(m, false) != tc.off {
			t.Fatal("charmap requirement settle check differs", tc.required)
		}
	}
}

// The consumer-request actions are a consumer-request run's only, and the
// fixed plan names only the owned child, its file and bounded values.
func TestMDSCephFSConsumerRequestsAreOwnedAndBounded(t *testing.T) {
	consumer := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSConsumerRequestsIntegration$", capabilities: capMDSConsumerRequests.name}
	other := profile{mdsFresh: "1", mdsCephFSCompletion: "1", testRun: "^TestCephMDSCephFSCompletionIntegration$"}
	for _, action := range mdsCephFSConsumerActions {
		if err := admitMDSCephFSConsumer(action, consumer); err != nil {
			t.Fatal("consumer-request action refused under its capability", action, err)
		}
		if err := admitMDSCephFSConsumer(action, other); err == nil {
			t.Fatal("consumer-request action admitted without its capability", action)
		}
	}
	if caps := mdsCephFSCompletionNativeCapsFor(consumer, "owned"); caps.MDS != "allow rwps fsname=owned" {
		t.Fatal("consumer-request native identity lacks the vxattr flag", caps)
	}
	if caps := mdsCephFSCompletionNativeCapsFor(other, "owned"); caps.MDS != "allow rws fsname=owned" {
		t.Fatal("a completion run gained the vxattr flag", caps)
	}
	plan := mdsCephFSConsumerPlanFor(strings.Repeat("c3", 16))
	if !plan.valid() {
		t.Fatal("the fixed consumer-request plan is not valid", plan)
	}
	for _, mutate := range []func(*mdsCephFSConsumerPlan){
		func(p *mdsCephFSConsumerPlan) { p.Dir = "../bdiff" },
		func(p *mdsCephFSConsumerPlan) { p.Snapshots[1] = p.Snapshots[0] },
		func(p *mdsCephFSConsumerPlan) { p.Snapshots[0] = "a/b" },
		func(p *mdsCephFSConsumerPlan) { p.Overwrite.Offset = p.Objects * p.ObjectSize },
		func(p *mdsCephFSConsumerPlan) { p.Append.Offset++ },
		func(p *mdsCephFSConsumerPlan) { p.MaxObjects = 0 },
		func(p *mdsCephFSConsumerPlan) { p.Vxattrs[0].Name = "user.x" },
		func(p *mdsCephFSConsumerPlan) { p.Vxattrs[0].Target = "parent" },
	} {
		p := mdsCephFSConsumerPlanFor(strings.Repeat("c3", 16))
		mutate(&p)
		if p.valid() {
			t.Fatal("an unsafe consumer-request plan is valid", p)
		}
	}
}

// The consumer-request and ranks charmap drivers' outputs are accepted only
// in their exact shapes: a read needs every plan vxattr in order and pages
// that end with one final page, and a listing only bounded valid entries.
func TestMDSCephFSConsumerAndRanksCharmapOutputShapes(t *testing.T) {
	plan := mdsCephFSConsumerPlanFor(strings.Repeat("c3", 16))
	stat := func(mode uint32) string {
		return fmt.Sprintf(`{"exists":true,"errno":0,"ino":%d,"mode":%d,"uid":0,"gid":0,"nlink":1,"size":0}`, 4096+mode%7, mode)
	}
	vx := []string{}
	for _, v := range plan.Vxattrs {
		vx = append(vx, fmt.Sprintf(`{"target":%q,"name":%q,"errno":0,"value_hex":"30"}`, v.Target, v.Name))
	}
	read := func(pages, vxattrs string) []byte {
		return []byte(`{"action":"read","path":"/d/bdiff","native_global_id":7,"diff_global_id":8,"stat":` + stat(0o40755) + `,"file_stat":` + stat(0o100644) +
			`,"snapshot_file_stats":[` + stat(0o100644) + `,` + stat(0o100644) + `],"vxattrs":[` + vxattrs + `],"pages":` + pages + `}`)
	}
	good := `[{"return":1,"blocks":[[73728,4096]]},{"return":0,"blocks":[]}]`
	if _, err := decodeMDSCephFSConsumerNative("read", "/d/bdiff", plan, read(good, strings.Join(vx, ","))); err != nil {
		t.Fatal("a valid consumer read was refused", err)
	}
	for _, bad := range [][]byte{
		read(`[{"return":0,"blocks":[]},{"return":0,"blocks":[]}]`, strings.Join(vx, ",")),
		read(`[{"return":1,"blocks":[]}]`, strings.Join(vx, ",")),
		read(good, strings.Join(vx[1:], ",")),
		read(good, strings.Join(append([]string{vx[1], vx[0]}, vx[2:]...), ",")),
		read(good, strings.Join(append([]string{`{"target":"dir","name":"ceph.dir.layout.json","errno":61,"value_hex":"30"}`}, vx[1:]...), ",")),
		[]byte(strings.Replace(string(read(good, strings.Join(vx, ","))), `"diff_global_id":8`, `"diff_global_id":7`, 1)),
	} {
		if _, err := decodeMDSCephFSConsumerNative("read", "/d/bdiff", plan, bad); err == nil {
			t.Fatalf("an invalid consumer read was accepted: %s", bad)
		}
	}
	names := func(entries string) []byte {
		return []byte(`{"action":"names","path":"/d/cmap","native_global_id":7,"stat":` + stat(0o40755) +
			`,"charmap":{"casesensitive":"0","normalization":"nfkc","encoding":"utf8"},"entries":[` + entries + `]}`)
	}
	entry := `{"name":"x","d_ino":9,"stat":` + stat(0o40755) + `,"charmap":{"casesensitive":"0","normalization":"nfkc","encoding":"utf8"}}`
	if _, err := decodeMDSCephFSRanksCharmapNative("names", "/d/cmap", names(entry)); err != nil {
		t.Fatal("a valid ranks charmap listing was refused", err)
	}
	for _, bad := range [][]byte{names(entry + "," + entry), names(strings.Replace(entry, `"d_ino":9`, `"d_ino":0`, 1)), []byte(strings.Replace(string(names("")), `"charmap":{"casesensitive":"0","normalization":"nfkc","encoding":"utf8"},`, ``, 1))} {
		if _, err := decodeMDSCephFSRanksCharmapNative("names", "/d/cmap", bad); err == nil {
			t.Fatalf("an invalid ranks charmap listing was accepted: %s", bad)
		}
	}
}

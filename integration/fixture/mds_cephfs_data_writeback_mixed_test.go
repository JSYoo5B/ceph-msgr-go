package main

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// mdsCephFSMixedWritebackSelector is the native test that declares
// mds-writeback-mixed (its //scenario:row declarations).
const mdsCephFSMixedWritebackSelector = "^TestCephMDSCephFSMixedWritebackRecoveryIntegration$"

func mixedWritebackEnv() map[string]string {
	env := mdsCephFSDataProfileEnv()
	env["CEPH_MSGR_TEST_RUN"] = mdsCephFSMixedWritebackSelector
	env["CEPH_MSGR_TEST_CAPABILITIES"] = "mds-writeback-fence,mds-writeback-mixed"
	return env
}

// The catalog rows of the mixed-extent recipe declare both capabilities, and
// the object-bytes view is admitted only with both on the data fixture.
func TestMDSCephFSMixedWritebackCapabilityAdmission(t *testing.T) {
	if got := catalogCapabilities(t, mdsCephFSMixedWritebackSelector); got != "mds-writeback-fence,mds-writeback-mixed" {
		t.Fatal("the mixed-extent rows declare", got)
	}
	env := mixedWritebackEnv()
	p, err := loadProfile(func(k string) string { return env[k] })
	if err != nil || p.testRun != mdsCephFSMixedWritebackSelector || !p.admits(capMDSWritebackMixed) || !p.admits(capMDSWritebackFence) {
		t.Fatal("the mixed-extent selector was not admitted with both capabilities", p, err)
	}
	for _, change := range []map[string]string{
		// The object-bytes view extends the OSD drop controls.
		{"CEPH_MSGR_TEST_CAPABILITIES": "mds-writeback-mixed"},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_DATA": "0"},
		{"CEPH_MSGR_TEST_RUN": "^TestCephMDSCephFS(MixedWritebackRecovery|Data)Integration$"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
	} {
		env := mixedWritebackEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe mixed-extent profile accepted", change, p, err)
		}
	}
	// The writeback-fence rows alone do not admit the object-bytes view.
	env = mdsCephFSDataProfileEnv()
	env["CEPH_MSGR_TEST_RUN"], env["CEPH_MSGR_TEST_CAPABILITIES"] = mdsCephFSWritebackSelector, "mds-writeback-fence"
	if p, err := loadProfile(func(k string) string { return env[k] }); err != nil || p.admits(capMDSWritebackMixed) {
		t.Fatal("the writeback-fence profile admits the object-bytes view", err)
	}
}

func TestMDSCephFSDataObjectBytesRequestIsClosed(t *testing.T) {
	token := strings.Repeat("c3", 16)
	_, a, err := parseMDSCephFSDataRequest([]string{"1", token, "object-bytes", "capsnap-file"}, token, 0)
	if err != nil || a != (mdsCephFSDataAction{name: "object-bytes", target: "capsnap-file"}) {
		t.Fatal("object-bytes request differs", a, err)
	}
	for _, fields := range [][]string{
		{"1", token, "object-bytes"}, {"1", token, "object-bytes", "go-file"}, {"1", token, "object-bytes", "shared-file"},
		{"1", token, "object-bytes", "dir"}, {"1", token, "object-bytes", "capsnap-a-file"}, {"1", token, "object-bytes", "capsnap-file", "2"},
	} {
		if _, _, err := parseMDSCephFSDataRequest(fields, token, 0); err == nil {
			t.Fatal("unsafe object-bytes request accepted", fields)
		}
	}
	x := &mdsCephFSDataScope{plan: mdsCephFSDataPlanFor(token)}
	if !slices.Equal(x.extra("object-bytes", "capsnap-file"), []string{"2"}) {
		t.Fatal("object-bytes input differs from the plan", x.extra("object-bytes", "capsnap-file"))
	}
}

// The view is refused before any native work without its capability and,
// with it, wherever native work is refused (the cluster is absent, so native
// work would panic).
func TestMDSCephFSDataObjectBytesAdmissionPrecedesNativeWork(t *testing.T) {
	token := strings.Repeat("c3", 16)
	out := t.TempDir()
	action := mdsCephFSDataAction{name: "object-bytes", target: "capsnap-file"}
	fence := writebackProfile(profile{})
	mixed := fence
	mixed.testRun, mixed.capabilities = mdsCephFSMixedWritebackSelector, "mds-writeback-fence,mds-writeback-mixed"
	if !mixed.admits(capMDSWritebackMixed) {
		t.Fatal("the test profile does not admit the object-bytes view")
	}
	for _, tc := range []struct {
		name    string
		p       profile
		drop    *mdsCephFSDataOSDDrop
		restart *mdsCephFSRestart
		reader  bool
		want    string
	}{
		{"no capability", profile{}, nil, nil, false, "capability mds-writeback-mixed"},
		{"writeback-fence only", fence, nil, nil, false, "capability mds-writeback-mixed"},
		{"applied OSD drop", mixed, &mdsCephFSDataOSDDrop{State: "applied"}, nil, false, "native action refused"},
		{"uncertain OSD drop", mixed, &mdsCephFSDataOSDDrop{State: "uncertain"}, nil, false, "native action refused"},
		{"pending restart", mixed, nil, &mdsCephFSRestart{State: "applied"}, false, "restart is not adopted"},
		{"running reader", mixed, nil, nil, true, "native action refused"},
	} {
		x := &mdsCephFSDataScope{out: out, plan: mdsCephFSDataPlanFor(token), owned: map[string]uint64{}, current: dataRestartDaemon(), baselineConfig: dataRuntimeBaseline(),
			osdDrop: tc.drop, restart: tc.restart}
		if tc.reader {
			x.reader = newMDSCephFSDataReaderRun(1)
		}
		c := &fixtureCluster{profile: tc.p, mdsFresh: &mdsFreshScope{out: out, token: token, name: "owned"}, mdsCephFSData: x}
		var result mdsCephFSDataResult
		err := c.mdsCephFSDataAct(context.Background(), 1, action, &result)
		if err == nil || !strings.Contains(err.Error(), tc.want) || result.Native != nil || result.Process != nil || len(x.mutations) != 0 {
			t.Fatal("object-bytes reached native work", tc.name, err)
		}
	}
}

func TestMDSCephFSDataObjectBytesOutputShape(t *testing.T) {
	token := strings.Repeat("c3", 16)
	plan := mdsCephFSDataPlanFor(token)
	pool := "owned-data"
	path, _ := plan.path("capsnap-file")
	hash := strings.Repeat("cd", 32)
	digest := func(length, chunks int) string {
		parts := make([]string, chunks)
		for i := range parts {
			parts[i] = `"` + hash + `"`
		}
		return `{"length":` + itoa(length) + `,"sha256":"` + hash + `","chunks":[` + strings.Join(parts, ",") + `]}`
	}
	file := `{"exists":true,"errno":0,"ino":1099511627777,"mode":33188,"uid":0,"gid":0,"nlink":1,"size":65536,"mtime":1,"version":2}`
	absent := `{"exists":false,"errno":2,"ino":0,"mode":0,"uid":0,"gid":0,"nlink":0,"size":0,"mtime":0,"version":0}`
	head := func(action, p string) string {
		return `{"action":"` + action + `","path":"` + p + `","native_global_id":7`
	}
	object := func(i int, rest string) string {
		return `{"objno":` + itoa(i) + `,"name":"10000000001.0000000` + itoa(i) + `"` + rest + `}`
	}
	present := object(0, `,"exists":true,"size":131072,"digest":`+digest(131072, 8))
	missing := object(1, `,"exists":false,"size":0`)
	view := func(action string, objects ...string) string {
		return head(action, path) + `,"stat":` + file + `,"objects":[` + strings.Join(objects, ",") + `]}`
	}
	for _, tc := range []struct {
		name, action, data string
		ok                 bool
	}{
		{"object 0 with content, object 1 absent", "object-bytes", view("object-bytes", present, missing), true},
		{"both objects with content", "object-bytes", view("object-bytes", present, object(1, `,"exists":true,"size":65536,"digest":`+digest(65536, 4))), true},
		{"absent file", "object-bytes", head("object-bytes", path) + `,"stat":` + absent + `,"objects":[]}`, true},
		{"existing object without content", "object-bytes", view("object-bytes", object(0, `,"exists":true,"size":131072`), missing), false},
		{"content length differs from size", "object-bytes", view("object-bytes", object(0, `,"exists":true,"size":131072,"digest":`+digest(65536, 4)), missing), false},
		{"content of an absent object", "object-bytes", view("object-bytes", present, object(1, `,"exists":false,"size":0,"digest":`+digest(0, 0))), false},
		{"chunk count differs", "object-bytes", view("object-bytes", object(0, `,"exists":true,"size":131072,"digest":`+digest(131072, 7)), missing), false},
		{"one object of two", "object-bytes", view("object-bytes", present), false},
		{"clones in the content view", "object-bytes", view("object-bytes", object(0, `,"exists":true,"size":131072,"digest":`+digest(131072, 8)+
			`,"snaps":{"seq":1,"clones":[]}`), missing), false},
		{"content in the plain object view", "objects", view("objects", present, missing), false},
	} {
		_, err := decodeMDSCephFSDataNative(tc.action, "capsnap-file", path, pool, plan, []byte(tc.data))
		if (err == nil) != tc.ok {
			t.Fatal("object-bytes output decision differs", tc.name, err)
		}
	}
	// Only capsnap-file has a content view.
	goPath, _ := plan.path("go-file")
	data := head("object-bytes", goPath) + `,"stat":` + file + `,"objects":[]}`
	if _, err := decodeMDSCephFSDataNative("object-bytes", "go-file", goPath, pool, plan, []byte(data)); err == nil {
		t.Fatal("an object-bytes view of another file was accepted")
	}
}

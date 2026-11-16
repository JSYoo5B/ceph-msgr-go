package main

import "testing"

// The daemon controls keep every action for MON, MGR and OSD. An MDS may only
// be stopped, and only by the public MDS command acceptance: not by the
// applied-command cell on the same fixture, not by any other MDS profile and
// not with any other action.
func TestDaemonControlAdmitsOnlyStoppingTheCommandFixtureMDS(t *testing.T) {
	load := func(change map[string]string) profile {
		t.Helper()
		env := mdsCommandsProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		p, err := loadProfile(func(k string) string { return env[k] })
		if err != nil {
			t.Fatal(change, err)
		}
		return p
	}
	commands := load(nil)
	if commands.mdsCommands != "1" || !commands.daemonControlAdmitted("mds", "stop") {
		t.Fatal("the public MDS command acceptance cannot stop its MDS")
	}
	for _, action := range []string{"pause", "resume", "start", "kill", ""} {
		if commands.daemonControlAdmitted("mds", action) {
			t.Fatal("an MDS action other than stop was admitted", action)
		}
	}
	for name, p := range map[string]profile{
		"applied-command cell": load(map[string]string{"CEPH_MSGR_TEST_RUN": daemonCommandMDSSelector, "CEPH_MSGR_TEST_CAPABILITIES": "mds-applied-command"}),
		"fresh MDS fixture":    load(map[string]string{"CEPH_MSGR_TEST_MDS_COMMANDS": "0", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"}),
		"no MDS fixture":       {},
	} {
		if p.daemonControlAdmitted("mds", "stop") {
			t.Fatal("stopping an MDS was admitted outside the public command acceptance", name)
		}
	}
	for _, p := range []profile{commands, {}} {
		for _, kind := range []string{"mon", "mgr", "osd"} {
			for _, action := range []string{"pause", "resume", "stop", "start", "kill"} {
				if !p.daemonControlAdmitted(kind, action) {
					t.Fatal("an existing daemon control lost its admission", kind, action)
				}
			}
		}
		if p.daemonControlAdmitted("rgw", "stop") {
			t.Fatal("an unknown daemon kind was admitted")
		}
	}
	if len(daemonControlKinds) != 4 || daemonControlKinds[3] != "mds" {
		t.Fatal("daemon control kinds changed", daemonControlKinds)
	}
}

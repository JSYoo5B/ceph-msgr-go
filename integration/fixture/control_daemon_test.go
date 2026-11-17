package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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

// An already released pause lease needs no second daemon resume. An earlier
// invalid daemon request must still fail before the recovery acknowledgement.
func TestRecoverPausedOSDOrdersPendingDaemonControl(t *testing.T) {
	for _, pending := range []bool{false, true} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "recover-paused-osd"), []byte("42 2"), 0600); err != nil {
			t.Fatal(err)
		}
		if pending {
			if err := os.WriteFile(filepath.Join(dir, "resume-osd"), []byte("41"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		var c fixtureCluster
		err := c.controlDaemonRequests(context.Background(), dir)
		_, ack := os.Stat(filepath.Join(dir, "osd-pause-recovered.42"))
		if pending {
			if err == nil || !errors.Is(ack, os.ErrNotExist) {
				t.Fatal("recovery acknowledged before pending resume", err, ack)
			}
		} else if err != nil || ack != nil {
			t.Fatal("recovery of an already resumed OSD", err, ack)
		}
	}
}

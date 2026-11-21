package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
		if err := writeRequestFile(dir, "recover-paused-osd", "42 2"); err != nil {
			t.Fatal(err)
		}
		if pending {
			if err := writeRequestFile(dir, "resume-osd", "41"); err != nil {
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

// Two pause leases that ended together both published a recovery before the
// fixture polled: every pending recovery is served, per OSD, and a second
// recovery of one OSD is only acknowledged. While an owned-pool oracle
// request runs (pauseEndingActions), the pause of an OSD that no recovery
// names stays pending.
func TestRecoverPausedOSDServesEveryPendingRecovery(t *testing.T) {
	for _, scope := range []daemonActionScope{allDaemonActions, pauseEndingActions} {
		dir := t.TempDir()
		for _, request := range []string{"43 2", "44 3", "45 2"} {
			if err := writeRequestFile(dir, "recover-paused-osd", request); err != nil {
				t.Fatal(err)
			}
		}
		if scope == pauseEndingActions {
			if err := writeRequestFile(dir, "pause-osd", "46 5"); err != nil {
				t.Fatal(err)
			}
		}
		var c fixtureCluster
		if err := c.daemonRequests(context.Background(), dir, scope); err != nil {
			t.Fatal(scope, err)
		}
		for _, id := range []string{"43", "44", "45"} {
			if !fileExists(filepath.Join(dir, "osd-pause-recovered."+id)) {
				t.Fatal("recovery", id, "was not acknowledged in scope", scope)
			}
		}
		if requestPending(t, dir, "recover-paused-osd") || scope == pauseEndingActions && !requestPending(t, dir, "pause-osd") {
			t.Fatal("a recovery stayed pending, or an unrecovered OSD's pause was claimed, in scope", scope)
		}
	}
}

// The pending requests of one control are claimed one at a time in the
// order of their request IDs, whatever their envelopes' directory order.
func TestRequestsAreClaimedInPublicationOrder(t *testing.T) {
	out := t.TempDir()
	for _, request := range []string{"30 c", "4 a", "12 b"} {
		writeControlRequest(t, out, "verify-x", request)
	}
	var c fixtureCluster
	var got []string
	for range 3 {
		fields, err := c.request(out, "verify-x")
		if err != nil || len(fields) != 2 {
			t.Fatal(fields, err)
		}
		got = append(got, fields[1])
	}
	if strings.Join(got, "") != "abc" || requestPending(t, out, "verify-x") {
		t.Fatal("claimed out of order or left pending:", got)
	}
	if err := os.WriteFile(filepath.Join(out, requestEnvelopeName("verify-x", "5")), []byte("6 a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.request(out, "verify-x"); err == nil {
		t.Fatal("an envelope holding another request ID was claimed")
	}
}

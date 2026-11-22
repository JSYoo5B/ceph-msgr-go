package main

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// The daemon controls and the pause leases (daemon_lease.go, action "pause")
// keep every action for MON, MGR and OSD. An MDS may only be stopped, and
// only by the public MDS command acceptance: not by the applied-command cell
// on the same fixture, not by any other MDS profile and not with any other
// action.
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
	for _, action := range []string{"pause", "start", "kill", ""} {
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
			for _, action := range []string{"pause", "stop", "start", "kill"} {
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

// orderedDaemon records the daemon actions in the order they ran.
type orderedDaemon struct {
	testcontainers.Container
	actions []string
	// bounds are the time left before each action's context deadline,
	// zero for an action without one.
	bounds []time.Duration
}

func (d *orderedDaemon) record(ctx context.Context, action string) {
	d.actions = append(d.actions, action)
	var bound time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		bound = time.Until(deadline)
	}
	d.bounds = append(d.bounds, bound)
}

func (d *orderedDaemon) Stop(ctx context.Context, _ *time.Duration) error {
	d.record(ctx, "stop")
	return nil
}

func (d *orderedDaemon) Start(ctx context.Context) error {
	d.record(ctx, "start")
	return nil
}

// The pending actions of one daemon kind run in publication order, whatever
// their action names, so a start that a cleanup published after a kill or
// stop the test abandoned runs after it and restarts the daemon. (Before,
// each poll ran the oldest stop, then the oldest start, then the oldest
// kill, so such a start ran first and the late kill left the daemon down.)
func TestDaemonControlRunsTheActionsOfAKindInPublicationOrder(t *testing.T) {
	out := t.TempDir()
	d := &orderedDaemon{}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "start-mon", "11 a")
	writeControlRequest(t, out, "stop-mon", "10 a")
	writeControlRequest(t, out, "start-mon", "9 a")
	if err := c.controls(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(d.actions, []string{"start", "stop", "start"}) {
		t.Fatal("the daemon actions ran out of publication order", d.actions)
	}
	for _, ack := range []string{"mon-start.9", "mon-stop.10", "mon-start.11"} {
		if !fileExists(filepath.Join(out, ack)) {
			t.Fatal("not acknowledged:", ack)
		}
	}
}

// Each daemon action runs under its own bound, so a stuck Docker call fails
// the fixture instead of holding every other control's requests behind it.
func TestDaemonControlBoundsEachAction(t *testing.T) {
	out := t.TempDir()
	d := &orderedDaemon{}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "stop-mon", "21 a")
	writeControlRequest(t, out, "start-mon", "22 a")
	if err := c.controls(context.Background(), out); err != nil {
		t.Fatal(err)
	}
	if len(d.bounds) != 2 {
		t.Fatal("the daemon actions did not run", d.actions)
	}
	for i, bound := range d.bounds {
		if bound <= 0 || bound > fixtureStartupTimeout {
			t.Fatal("daemon action", d.actions[i], "ran without its bound", bound)
		}
	}
}

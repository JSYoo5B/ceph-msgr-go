package integration_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// A raw OSD command on a connection whose OSD was SIGKILLed never reports a
// result it did not receive; after the daemon restarts, a new connection to its
// new address answers, and the old connection is not silently replaced.
func TestCephOSDCommandCrashIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon restart needs the BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	version := cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}
	o := mutationConnection(t, c, ctx, base)
	first, err := o.Command(ctx, version)
	if err != nil || first.Code != 0 || len(first.Data) == 0 {
		t.Fatal("command before the crash", err)
	}
	name := strconv.FormatInt(int64(base.ID), 10)
	if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		if !restarted {
			recoverMutationFixture(t, c, base, original, name, true)
		}
	}()
	callCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	_, err = o.Command(callCtx, version)
	stop()
	if err == nil {
		t.Fatal("a command to a killed OSD reported success")
	}
	// A command admitted before the session died is an unknown outcome; one
	// refused because the session had already ended was never sent and keeps
	// the session's own cause. Either way no result is invented.
	var unknown *cephmsgr.OutcomeUnknownError
	t.Logf("command after SIGKILL: unknown=%v err=%v", errors.As(err, &unknown), err)
	if _, err := o.Command(ctx, version); err == nil {
		t.Fatal("the old connection silently recovered")
	}
	o.Close()
	recoverMutationFixture(t, c, base, original, name, true)
	restarted = true
	fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
	if fresh.Address == base.Address {
		t.Fatal("the restarted OSD kept its address nonce")
	}
	n := mutationConnection(t, c, ctx, fresh)
	defer n.Close()
	again, err := n.Command(ctx, version)
	if err != nil || again.Code != 0 || string(again.Data) != string(first.Data) || again.Submission.Transaction <= first.Submission.Transaction {
		t.Fatal("command on a new connection after restart", err)
	}
}

// A long OSD command (bench overwriting 1 GiB in 4 KiB blocks) is in flight
// when its OSD is SIGKILLed, and in a second round when it is stopped (SIGTERM,
// docker stop). A SIGKILL ends the call as an unknown outcome carrying its
// transaction, never with a result. A stop must not report success either; a
// known error the daemon sends while shutting down is accepted and logged.
// Nothing is resent to the restarted daemon, and a new connection answers.
func TestCephOSDCommandCrashInFlightIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon restart needs the BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	current := base
	for _, action := range []string{"kill", "stop"} {
		o := mutationConnection(t, c, ctx, current)
		// 262144 overwrites of 4 KiB inside one 4 MiB object (no device
		// growth). 4 KiB blocks are bounded by osd_bench_small_size_max_iops
		// (100 by default, 12 MiB per bench). The limit is raised in the
		// target daemon's runtime configuration only; the restart drops it.
		if r, err := o.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"config set","var":"osd_bench_small_size_max_iops","val":["100000"]}`)}); err != nil || r.Code != 0 {
			t.Fatal("raise the bench limit", err)
		}
		bench := cephmsgr.Command{JSON: []byte(`{"prefix":"bench","count":1073741824,"size":4096,"object_size":4194304,"object_num":1}`)}
		type outcome struct {
			result cephmsgr.OSDCommandResult
			err    error
		}
		done := make(chan outcome, 1)
		started := time.Now()
		go func() {
			call, stop := context.WithTimeout(ctx, 120*time.Second)
			defer stop()
			result, err := o.Command(call, bench)
			done <- outcome{result, err}
		}()
		select {
		case r := <-done:
			t.Fatalf("%s: bench finished in %s before the daemon action: code %d %v", action, time.Since(started), r.result.Code, r.err)
		case <-time.After(500 * time.Millisecond):
		}
		name := strconv.FormatInt(int64(current.ID), 10)
		if err := testcluster.ControlDaemon(ctx, dir, action, "osd", name); err != nil {
			t.Fatal(action, err)
		}
		restarted := false
		restore := func() {
			o.Close()
			recoverMutationFixture(t, c, current, original, name, true)
			restarted = true
		}
		defer func() {
			if !restarted {
				restore()
			}
		}()
		r := <-done
		var unknown *cephmsgr.OutcomeUnknownError
		var refused *cephmsgr.CommandError
		switch {
		case errors.As(r.err, &unknown) && r.result.Code == 0 && len(r.result.Data) == 0 && r.result.Submission.Transaction != 0:
			t.Logf("%s: bench in flight for %s ended unknown (transaction %d): %v", action, time.Since(started).Round(time.Millisecond), r.result.Submission.Transaction, r.err)
		case action == "stop" && errors.As(r.err, &refused) && refused.Code < 0:
			t.Logf("%s: bench in flight for %s was refused by the stopping daemon: code %d %q", action, time.Since(started).Round(time.Millisecond), refused.Code, r.result.Message)
		default:
			t.Fatalf("%s: a command in flight must not report a result: code %d, transaction %d: %v", action, r.result.Code, r.result.Submission.Transaction, r.err)
		}
		restore()
		fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true}).Placement
		current = adoptMutationPlacement(t, current, fresh)
		n := mutationConnection(t, c, ctx, current)
		if again, err := n.Command(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"version"}`)}); err != nil || again.Code != 0 {
			t.Fatal(action, "command on a new connection after restart", err)
		}
		n.Close()
	}
}

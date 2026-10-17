package integration_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
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

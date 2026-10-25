package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func writeControlRequest(t *testing.T, out, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(out, name), []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestControllerPreservesEarlierRoleAndFailureGateOrder(t *testing.T) {
	for _, workerFailed := range []bool{false, true} {
		t.Run(map[bool]string{false: "earlier-role-refusal", true: "worker-failure-before-role"}[workerFailed], func(t *testing.T) {
			out := t.TempDir()
			d := &controlledDaemon{}
			c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
			// An MDS request precedes the generic daemon actions, and the
			// worker failure gate precedes both. Neither can be reordered.
			writeControlRequest(t, out, "mds-sessions", "1 invalid")
			writeControlRequest(t, out, "stop-mon", "2 a")
			wanted := "invalid owned MDS sessions control scope"
			if workerFailed {
				writeControlRequest(t, out, "native-oracles.error", "synthetic failure")
				wanted = "native oracle worker failed"
			}
			err := c.controls(context.Background(), out)
			if err == nil || !strings.Contains(err.Error(), wanted) || d.stops != 0 {
				t.Fatal("later action ran before an earlier failure", err, d.stops)
			}
			if !fileExists(filepath.Join(out, "stop-mon")) || fileExists(filepath.Join(out, "mon-stop.2")) {
				t.Fatal("a request after the first failure was consumed or acknowledged")
			}
			if fileExists(filepath.Join(out, "mds-sessions")) != workerFailed {
				t.Fatal("the worker failure checkpoint moved relative to the MDS request")
			}
		})
	}
}

func TestControllerRetainsEarlierMutationAndAckAfterLaterRoleRefusal(t *testing.T) {
	out := t.TempDir()
	d := &controlledDaemon{}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "stop-mon", "1 a")
	writeControlRequest(t, out, "verify-mgr-keyring", "2 invalid")
	err := c.controls(context.Background(), out)
	if err == nil || !strings.Contains(err.Error(), "native MGR keyring gate") || d.stops != 1 {
		t.Fatal("later MGR refusal moved before the earlier daemon mutation", err, d.stops)
	}
	if !fileExists(filepath.Join(out, "mon-stop.1")) || fileExists(filepath.Join(out, "mgr-keyring-verify.2")) {
		t.Fatal("earlier acknowledgement was lost or a refused action was acknowledged")
	}
	if fileExists(filepath.Join(out, "stop-mon")) || fileExists(filepath.Join(out, "verify-mgr-keyring")) {
		t.Fatal("consumed requests were retained after partial polling progress")
	}
	if err := c.controls(context.Background(), out); err != nil || d.stops != 1 {
		t.Fatal("a later poll replayed the completed earlier mutation", err, d.stops)
	}
}

type uncertainControlledDaemon struct {
	testcontainers.Container
	stops   int
	failure error
}

func (d *uncertainControlledDaemon) Stop(context.Context, *time.Duration) error {
	d.stops++
	return d.failure
}

func TestControllerDoesNotReplayConsumedUncertainMutation(t *testing.T) {
	out := t.TempDir()
	uncertain := errors.New("synthetic stop outcome unknown")
	d := &uncertainControlledDaemon{failure: uncertain}
	c := &fixtureCluster{cluster: &ceph.Container{Container: d}}
	writeControlRequest(t, out, "stop-mon", "1 a")
	writeControlRequest(t, out, "verify-mgr-keyring", "2 invalid")
	if err := c.controls(context.Background(), out); err != uncertain || d.stops != 1 {
		t.Fatal("failed mutation lost its first error or was retried", err, d.stops)
	}
	if fileExists(filepath.Join(out, "stop-mon")) || fileExists(filepath.Join(out, "mon-stop.1")) || !fileExists(filepath.Join(out, "verify-mgr-keyring")) {
		t.Fatal("uncertain action was acknowledged or its later request was consumed")
	}
	if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "native MGR keyring gate") || d.stops != 1 {
		t.Fatal("subsequent polling replayed uncertainty instead of reaching the pending request", err, d.stops)
	}
	if fileExists(filepath.Join(out, "mon-stop.1")) {
		t.Fatal("uncertain mutation acquired a later success acknowledgement")
	}
}

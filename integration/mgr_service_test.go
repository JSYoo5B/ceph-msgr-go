package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// serviceWatch asks the native oracle (public librados MON and MGR commands,
// not the Go client) to wait until the named daemon matches, or is absent.
func serviceWatch(t *testing.T, ctx context.Context, service, daemon, expect string, metadata, status, task map[string]string) poolReceipt {
	t.Helper()
	return nativePool(t, ctx, "servicewatch", map[string]any{
		"service": service, "daemon": daemon, "expect": expect, "wait_seconds": 60,
		"metadata": metadata, "status": status, "task_status": task,
	})
}

// A Go-originated service registration is observed natively: the MON service
// map (service dump) and the MGR (service status) show the daemon with the
// metadata, status and task status the Go client sent, follow an update and a
// report, and drop the daemon after the acknowledged close. The Go service map
// watch is cross-checked against the native dump.
//
// The test registers a daemon in the cluster's service map, so it runs only in
// the opt-in CEPH_MSGR_TEST_MGR_SERVICE=1 profile; see
// cephmsgr/MGR_SERVICE_CONTRACT.md.
func TestCephMgrServiceRegistrationIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MGR_SERVICE") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MGR_SERVICE=1: Go service registration changes the cluster service map")
	}
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("native observation requires the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	options := integrationOptions(t)
	options.EnableMgrService = true
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	watch, err := c.WatchServiceMap(ctx, cephmsgr.ServiceMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()

	const service = "ceph-msgr-go"
	daemon := "go-" + time.Now().Format("150405")
	metadata := map[string]string{"ceph_msgr_role": "go", "ceph_msgr_daemon": daemon}
	status := map[string]string{"state": "registered"}
	svc, err := c.OpenMgrService(ctx, cephmsgr.MgrServiceOptions{Service: service, Daemon: daemon, Metadata: metadata, Status: status})
	if err != nil {
		t.Fatal("open registration:", err)
	}
	closed := false
	defer func() {
		if !closed {
			// Best effort only: a failed test leaves the daemon to the MGR's
			// beacon grace, which the fixture teardown outlasts.
			stop, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			svc.Close(stop)
		}
	}()
	t.Logf("registered %s/%s, MGR configure %+v", service, daemon, svc.Configure())

	// Native observation of the open: metadata and status as sent.
	native := serviceWatch(t, ctx, service, daemon, "present", metadata, status, nil)
	if !native.ServiceMatched {
		t.Fatalf("native service dump/status lack the Go registration: entry=%s status=%s", native.ServiceEntry, native.ServiceStatusEntry)
	}
	var entry struct {
		GID uint64 `json:"gid"`
	}
	if err := json.Unmarshal(native.ServiceEntry, &entry); err != nil || entry.GID == 0 || entry.GID != c.Snapshot().GlobalID {
		t.Fatalf("native daemon GID %d differs from the Go client's authenticated ID %d (%v)", entry.GID, c.Snapshot().GlobalID, err)
	}
	// The Go stream must show the same daemon.
	for seen := false; !seen; {
		waitCtx, stop := context.WithTimeout(ctx, 60*time.Second)
		m, err := watch.Next(waitCtx)
		stop()
		if err != nil {
			t.Fatal("Go service map:", err)
		}
		if d, ok := m.Services[service].Daemons[daemon]; ok {
			if d.GID != entry.GID || d.Metadata["ceph_msgr_daemon"] != daemon {
				t.Fatalf("Go service map daemon %+v differs from the native dump", d)
			}
			seen = true
		}
	}

	// Update and report: new metadata, status and task status.
	updated := map[string]string{"ceph_msgr_role": "go", "ceph_msgr_daemon": daemon, "zone": "b"}
	if err := svc.UpdateMetadata(ctx, updated); err != nil {
		t.Fatal(err)
	}
	busy, task := map[string]string{"state": "busy"}, map[string]string{"task": "x"}
	if err := svc.Report(ctx, busy, task); err != nil {
		t.Fatal(err)
	}
	native = serviceWatch(t, ctx, service, daemon, "present", updated, busy, task)
	if !native.ServiceMatched {
		t.Fatalf("native observation lacks the update and report: entry=%s status=%s", native.ServiceEntry, native.ServiceStatusEntry)
	}

	// Acknowledged close removes the daemon natively.
	if err := svc.Close(ctx); err != nil {
		t.Fatal("close:", err)
	}
	closed = true
	if gone := serviceWatch(t, ctx, service, daemon, "absent", nil, nil, nil); !gone.ServiceMatched {
		t.Fatalf("native service dump still lists the closed daemon: %s", gone.ServiceEntry)
	}
	if err := svc.Report(ctx, nil, nil); err == nil {
		t.Fatal("a closed registration accepted a report")
	}
	t.Logf("native observation confirmed open, update, report and close of %s/%s", service, daemon)
}

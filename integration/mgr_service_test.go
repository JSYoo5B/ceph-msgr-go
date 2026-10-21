package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// serviceWatch asks the native oracle (public librados MON and MGR commands,
// not the Go client) to wait until the named daemon matches, or is absent.
type serviceWatchRequest struct {
	Service     string            `json:"service"`
	Daemon      string            `json:"daemon"`
	Expect      string            `json:"expect"`
	WaitSeconds int               `json:"wait_seconds"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Status      map[string]string `json:"status,omitempty"`
	TaskStatus  map[string]string `json:"task_status,omitempty"`
}

func serviceWatchInput(service, daemon, expect string, metadata, status, task map[string]string) serviceWatchRequest {
	return serviceWatchRequest{service, daemon, expect, 60, metadata, status, task}
}

func serviceWatch(t *testing.T, ctx context.Context, service, daemon, expect string, metadata, status, task map[string]string) poolReceipt {
	t.Helper()
	return nativePool(t, ctx, "servicewatch", serviceWatchInput(service, daemon, expect, metadata, status, task))
}

// The Python oracle tests consume the same fixtures. Compare the actual Go
// serialization, including omitted optional maps, against that shared input.
func TestServiceWatchRequestMatchesNativeOracleInputs(t *testing.T) {
	data, err := os.ReadFile("testdata/mgr-service-watch.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures map[string]any
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]serviceWatchRequest{
		"open":   serviceWatchInput("svc", "d1", "present", map[string]string{"zone": "a"}, map[string]string{"state": "up"}, nil),
		"report": serviceWatchInput("svc", "d1", "present", map[string]string{"zone": "a"}, map[string]string{"state": "busy"}, map[string]string{"t": "2"}),
		"absent": serviceWatchInput("svc", "d1", "absent", nil, nil, nil),
	} {
		raw, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var actual any
		if err := json.Unmarshal(raw, &actual); err != nil || !reflect.DeepEqual(actual, fixtures[name]) {
			t.Fatalf("%s Go serialization differs from the native oracle input: %s (%v)", name, raw, err)
		}
	}
}

// A Go-originated service registration is observed natively: the MON service
// map (service dump) and the MGR (service status) show the daemon with the
// initial metadata, status and task status the Go client sent, follow a report,
// and drop the daemon after the acknowledged close. MMgrUpdate changes only
// MGR daemon-state metadata, which these commands do not expose. The Go service map
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

	// Queue a metadata update, then independently observe the report. The
	// pinned handle_update changes daemon_state only: the MON's ServiceMap
	// retains the initial registration metadata. This does not verify that
	// the replacement metadata was applied inside the MGR.
	updated := map[string]string{"ceph_msgr_role": "go", "ceph_msgr_daemon": daemon, "zone": "b"}
	if err := svc.UpdateMetadata(ctx, updated); err != nil {
		t.Fatal(err)
	}
	busy, task := map[string]string{"state": "busy"}, map[string]string{"task": "x"}
	if err := svc.Report(ctx, busy, task); err != nil {
		t.Fatal(err)
	}
	native = serviceWatch(t, ctx, service, daemon, "present", metadata, busy, task)
	if !native.ServiceMatched {
		t.Fatalf("native observation lacks the report: entry=%s status=%s", native.ServiceEntry, native.ServiceStatusEntry)
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
	t.Logf("native observation confirmed open, report and close of %s/%s; metadata update was queued only", service, daemon)
}

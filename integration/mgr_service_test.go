package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// serviceWatch asks the native oracle (public librados MON and MGR commands,
// not the Go client) to wait until the named daemon matches, or is absent.
type serviceWatchRequest struct {
	Service       string            `json:"service"`
	Daemon        string            `json:"daemon"`
	Expect        string            `json:"expect"`
	WaitSeconds   int               `json:"wait_seconds"`
	StableSeconds int               `json:"stable_seconds,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Status        map[string]string `json:"status,omitempty"`
	TaskStatus    map[string]string `json:"task_status,omitempty"`
}

func serviceWatchInput(service, daemon, expect string, metadata, status, task map[string]string) serviceWatchRequest {
	return serviceWatchRequest{Service: service, Daemon: daemon, Expect: expect, WaitSeconds: 60, Metadata: metadata, Status: status, TaskStatus: task}
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
	t.Run("renewal_and_active_manager_change", func(t *testing.T) {
		testMgrServiceRecovery(t, ctx, c, service, daemon+"-recovery")
	})
}

// Preserve only public identities and oracle outcomes, never session error text
// (which can contain runner endpoints). The full native maps are recorded by
// the existing fixture oracle separately.
type mgrServiceRecoveryReceipt struct {
	ClientGID           uint64            `json:"client_gid"`
	Before              namedMgrNativeMap `json:"before"`
	After               namedMgrNativeMap `json:"after"`
	InitialTicketExpiry time.Time         `json:"initial_ticket_expiry"`
	RenewedTicketExpiry time.Time         `json:"renewed_ticket_expiry"`
	RenewedReport       bool              `json:"renewed_report"`
	Terminal            bool              `json:"terminal"`
	ManagerChangedCause bool              `json:"manager_changed_cause"`
	ReportRefused       bool              `json:"report_refused"`
	UpdateRefused       bool              `json:"update_refused"`
	ClosePreservedCause bool              `json:"close_preserved_cause"`
	AbsentSeconds       float64           `json:"absent_seconds"`
	AbsenceObservations int               `json:"absence_observations"`
	ReopenedGID         uint64            `json:"reopened_gid"`
	ReopenedStartEpoch  uint32            `json:"reopened_start_epoch"`
	Removed             bool              `json:"removed"`
}

func testMgrServiceRecovery(t *testing.T, ctx context.Context, c *cephmsgr.Client, service, daemon string) {
	t.Helper()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	var receipt mgrServiceRecoveryReceipt
	defer func() {
		data, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "mgr-service-recovery.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve service recovery receipt", err)
		}
	}()
	prepareNativeManagerFailover(t, ctx, control)
	before := namedMgrNativeObserve(t, ctx, control, "before")
	receipt.Before = before.After
	initial := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.GlobalID == before.After.GID && s.Manager.MapEpoch >= before.After.Epoch
	})
	receipt.ClientGID, receipt.InitialTicketExpiry = initial.GlobalID, initial.MgrTicket.Expires
	options := cephmsgr.MgrServiceOptions{Service: service, Daemon: daemon,
		Metadata: map[string]string{"phase": "before"}, Status: map[string]string{"state": "before"}}
	svc, err := c.OpenMgrService(ctx, options)
	if err != nil {
		t.Fatal("open recovery service", err)
	}
	defer func() {
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		svc.Close(stop)
	}()
	if !serviceWatch(t, ctx, service, daemon, "present", options.Metadata, options.Status, nil).ServiceMatched {
		t.Fatal("initial recovery service was not observed natively")
	}
	// A report on an established session does not itself authenticate a renewed
	// ticket. The later explicit Open uses the current ticket after this expiry.
	renewed := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return time.Now().After(initial.MgrTicket.Expires) && s.MgrTicket.Expires.After(initial.MgrTicket.Expires)
	})
	if renewed.GlobalID != initial.GlobalID {
		t.Fatal("renewal changed client identity")
	}
	receipt.RenewedTicketExpiry = renewed.MgrTicket.Expires
	status := map[string]string{"state": "renewed"}
	if err := svc.Report(ctx, status, nil); err != nil {
		t.Fatal("report across ticket expiry", err)
	}
	receipt.RenewedReport = serviceWatch(t, ctx, service, daemon, "present", options.Metadata, status, nil).ServiceMatched
	if !receipt.RenewedReport {
		t.Fatal("post-renewal report was not observed natively")
	}

	// One explicit mutation, never retried on an uncertain result.
	command, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": before.After.Name})
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: command}); err != nil {
		t.Fatal("fail active MGR once", err)
	}
	after := namedMgrNativeObserve(t, ctx, control, "after")
	receipt.After = after.After
	waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		return s.Manager.Available && s.Manager.GlobalID == after.After.GID && s.Manager.MapEpoch >= after.After.Epoch
	})
	select {
	case <-svc.Done():
	case <-ctx.Done():
		t.Fatal("old service did not terminate", ctx.Err())
	}
	terminal := svc.Err()
	if terminal == nil {
		t.Fatal("failover invented an acknowledged service close")
	}
	receipt.Terminal, receipt.ManagerChangedCause = true, errors.Is(terminal, cephmsgr.ErrManagerChanged)
	checkRefusal := func(err error) bool {
		return errors.Is(err, cephmsgr.ErrMgrServiceClosed) && errors.Is(err, terminal)
	}
	receipt.ReportRefused = checkRefusal(svc.Report(ctx, nil, nil))
	receipt.UpdateRefused = checkRefusal(svc.UpdateMetadata(ctx, map[string]string{"phase": "forbidden"}))
	receipt.ClosePreservedCause = errors.Is(svc.Close(ctx), terminal)
	if !receipt.ReportRefused || !receipt.UpdateRefused || !receipt.ClosePreservedCause {
		t.Fatal("failed handle lost its terminal cause or admitted another message")
	}
	// Recover ordinary commands without invoking OpenMgrService again. Native
	// absence must persist across polls after beacon grace; this is bounded
	// observation, not a count of wire transmissions or server executions.
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare replacement command session", err)
	}
	absent := serviceWatchInput(service, daemon, "absent", nil, nil, nil)
	absent.WaitSeconds, absent.StableSeconds = 90, 3
	missing := nativePool(t, ctx, "servicewatch", absent)
	if !missing.ServiceMatched || missing.ServiceStableSeconds < 3 || missing.ServiceObservations < 4 {
		t.Fatal("native service did not remain absent before explicit re-registration")
	}
	receipt.AbsentSeconds, receipt.AbsenceObservations = missing.ServiceStableSeconds, missing.ServiceObservations
	options.Metadata, options.Status = map[string]string{"phase": "explicit"}, map[string]string{"state": "reopened"}
	reopened, err := c.OpenMgrService(ctx, options)
	if err != nil {
		t.Fatal("explicit re-registration", err)
	}
	defer func() {
		stop, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		reopened.Close(stop)
	}()
	visible := serviceWatch(t, ctx, service, daemon, "present", options.Metadata, options.Status, nil)
	var entry struct {
		GID        uint64 `json:"gid"`
		StartEpoch uint32 `json:"start_epoch"`
	}
	if !visible.ServiceMatched || json.Unmarshal(visible.ServiceEntry, &entry) != nil || entry.GID != initial.GlobalID || entry.StartEpoch <= missing.ServiceDumpEpoch {
		t.Fatal("explicit service lacks the expected native identity/start epoch", entry)
	}
	receipt.ReopenedGID, receipt.ReopenedStartEpoch = entry.GID, entry.StartEpoch
	if err := reopened.Close(ctx); err != nil {
		t.Fatal("close replacement service", err)
	}
	receipt.Removed = serviceWatch(t, ctx, service, daemon, "absent", nil, nil, nil).ServiceMatched
	if !receipt.Removed {
		t.Fatal("replacement service removal not observed natively")
	}
	t.Logf("service recovery: client GID %d, MGR %s/%d -> %s/%d, native absence %.1fs across %d observations, explicit start epoch %d",
		receipt.ClientGID, receipt.Before.Name, receipt.Before.GID, receipt.After.Name, receipt.After.GID,
		receipt.AbsentSeconds, receipt.AbsenceObservations, receipt.ReopenedStartEpoch)
}

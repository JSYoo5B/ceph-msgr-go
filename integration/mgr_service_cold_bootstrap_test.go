package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type coldBootstrapDials struct {
	mu        sync.Mutex
	endpoints []string
	overflow  bool
}

type coldBootstrapDialCounts struct {
	Total        int  `json:"total"`
	MON          int  `json:"mon"`
	MGR          int  `json:"mgr"`
	Unclassified int  `json:"unclassified"`
	Overflow     bool `json:"overflow"`
}

func (d *coldBootstrapDials) record(endpoint string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.endpoints) == 256 {
		d.overflow = true
		return
	}
	d.endpoints = append(d.endpoints, endpoint)
}

// Classify every attempt against authenticated map endpoints, including MGRs
// created after the fixture exported its initial MON-only peer inventory.
func (d *coldBootstrapDials) counts(monitors, managers []string) coldBootstrapDialCounts {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := coldBootstrapDialCounts{Total: len(d.endpoints), Overflow: d.overflow}
	for _, endpoint := range d.endpoints {
		switch {
		case slices.Contains(monitors, endpoint):
			c.MON++
		case slices.Contains(managers, endpoint):
			c.MGR++
		default:
			c.Unclassified++
		}
	}
	return c
}

func TestColdBootstrapDialAttributionIncludesLaterManager(t *testing.T) {
	var d coldBootstrapDials
	mon, mgr := []string{"127.0.0.1:3300"}, []string{"127.0.0.2:6800"}
	d.record(mon[0])
	if got := d.counts(mon, nil); got.MON != 1 || got.MGR != 0 || got.Unclassified != 0 {
		t.Fatal(got)
	}
	d.record(mgr[0])
	if got := d.counts(mon, nil); got.Unclassified != 1 || got.Total != 2 {
		t.Fatal("later endpoint was discarded", got)
	}
	if got := d.counts(mon, mgr); got.MGR != 1 || got.MON != 1 || got.Unclassified != 0 {
		t.Fatal(got)
	}
	for i := 0; i < 255; i++ {
		d.record(mon[0])
	}
	if got := d.counts(mon, mgr); !got.Overflow || got.Total != 256 {
		t.Fatal("overflow hidden", got)
	}
}

// A distinct closed fixture starts with no MGR processes. The native MON map,
// every dial attempt and renewed MON ticket precede one explicit activation.
func TestCephMgrServiceColdBootstrapIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MGR_SERVICE") != "1" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "0" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires the closed zero-initial-MGR service fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	receipt := struct {
		Initial, Active                                                          mgrServiceNativeMap
		Before, After                                                            coldBootstrapDialCounts
		Renewed, KnownDeadline, KnownCancel, KnownClose, CanceledAbsent, Removed bool
		TicketBefore, TicketAfter                                                time.Time
		GlobalID                                                                 uint64
	}{}
	t.Cleanup(func() {
		data, err := json.MarshalIndent(receipt, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(control, "mgr-service-cold-bootstrap.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("preserve cold-bootstrap receipt", err)
		}
	})
	initialNative := nativePool(t, ctx, "mgrstate", map[string]any{}).ManagerMap
	if initialNative == nil || initialNative.Epoch == 0 || initialNative.Available || initialNative.GID != 0 || initialNative.Name != "" {
		t.Fatal("fixture did not start without a native MGR", initialNative)
	}
	receipt.Initial = *initialNative
	var dials coldBootstrapDials
	options := integrationOptions(t)
	options.EnableMgrService = true
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, address string) (net.Conn, error) {
		dials.record(address)
		return dial(call, network, address)
	}
	client, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	initial := waitClientState(t, client, ctx, func(s cephmsgr.State) bool {
		return s.Monitor.Ready && s.Manager.MapEpoch >= initialNative.Epoch && !s.Manager.Available
	})
	receipt.GlobalID = initial.GlobalID
	const service = "ceph-msgr-cold"
	identity := func(daemon string) cephmsgr.MgrServiceOptions {
		return cephmsgr.MgrServiceOptions{Service: service, Daemon: daemon, Metadata: map[string]string{"phase": daemon}}
	}
	type opened struct {
		svc *cephmsgr.MgrService
		err error
	}
	start := func(c *cephmsgr.Client, call context.Context, daemon string) <-chan opened {
		t.Helper()
		entered := &mgrServiceWaitContext{Context: call, entered: make(chan struct{})}
		result := make(chan opened, 1)
		go func() { s, e := c.OpenMgrService(entered, identity(daemon)); result <- opened{s, e} }()
		select {
		case <-entered.entered:
		case <-ctx.Done():
			t.Fatal("setup did not enter", daemon, ctx.Err())
		}
		return result
	}
	known := func(result <-chan opened, want error) bool {
		t.Helper()
		select {
		case got := <-result:
			if got.svc != nil || !errors.Is(got.err, want) || errors.Is(got.err, cephmsgr.ErrMgrServiceOutcomeUnknown) {
				t.Fatal("unsent setup lost its known result", got.err)
			}
			return true
		case <-ctx.Done():
			t.Fatal("setup did not finish", ctx.Err())
			return false
		}
	}
	deadline, stopDeadline := context.WithTimeout(ctx, 200*time.Millisecond)
	receipt.KnownDeadline = known(start(client, deadline, "deadline"), context.DeadlineExceeded)
	stopDeadline()
	cancelCtx, stopCancel := context.WithCancel(ctx)
	canceled := start(client, cancelCtx, "cancel")
	stopCancel()
	receipt.KnownCancel = known(canceled, context.Canceled)
	closing, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer closing.Close()
	closed := start(closing, ctx, "client-close")
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	receipt.KnownClose = known(closed, cephmsgr.ErrClosed)
	waiting := start(client, ctx, "waiting")
	receipt.TicketBefore = client.Snapshot().AuthTicket.Expires
	if receipt.TicketBefore.IsZero() {
		t.Fatal("pending setup has no initial MON auth ticket")
	}
	// A genuine MON renewal must complete while the service setup waits and
	// without using the shared MGR setup gate for a preparation probe.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !client.Snapshot().AuthTicket.Expires.After(receipt.TicketBefore) {
		if err := client.WaitMonReady(ctx); err != nil {
			t.Fatal(err)
		}
		result, err := client.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		if err != nil || !json.Valid(result.Data) {
			t.Fatal("MON unavailable before activation", err)
		}
		select {
		case got := <-waiting:
			t.Fatal("service returned before activation", got.err)
		case <-ctx.Done():
			t.Fatal("MON renewal absent", ctx.Err())
		case <-ticker.C:
		}
	}
	receipt.TicketAfter = client.Snapshot().AuthTicket.Expires
	receipt.Renewed = receipt.TicketAfter.After(receipt.TicketBefore)
	before := nativePool(t, ctx, "mgrstate", map[string]any{}).ManagerMap
	if before == nil || before.Available || before.GID != 0 || before.Name != "" {
		t.Fatal("manager appeared before activation", before)
	}
	receipt.Before = dials.counts(initial.Monitor.Endpoints, nil)
	if receipt.Before.MON == 0 || receipt.Before.MGR != 0 || receipt.Before.Unclassified != 0 || receipt.Before.Overflow {
		t.Fatal("pre-activation dial evidence is incomplete", receipt.Before)
	}
	// This marker is submitted once. The fixture adds its owned MGRs once;
	// unknown activation is never retried by this test.
	if err := testcluster.WritePublicEvidence(filepath.Join(control, "start-mgrs"), nil); err != nil {
		t.Fatal(err)
	}
	var svc *cephmsgr.MgrService
	select {
	case got := <-waiting:
		if got.err != nil || got.svc == nil {
			t.Fatal("pending service failed after activation", got.err)
		}
		svc = got.svc
	case <-ctx.Done():
		t.Fatal("activation did not release pending service", ctx.Err())
	}
	closeAttempted := false
	defer func() {
		if !closeAttempted {
			closeAttempted = true
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if err := svc.Close(cleanup); err != nil {
				t.Error("service close cleanup", err)
			}
		}
	}()
	active := waitClientState(t, client, ctx, func(s cephmsgr.State) bool { return s.Manager.Available && len(s.Manager.Endpoints) > 0 })
	nativeActive := nativePool(t, ctx, "mgrstate", map[string]any{}).ManagerMap
	if nativeActive == nil || !nativeActive.Available || nativeActive.Name != active.Manager.Name || nativeActive.GID != active.Manager.GlobalID {
		t.Fatal("active MGR differs from native identity", nativeActive)
	}
	receipt.Active = *nativeActive
	receipt.After = dials.counts(initial.Monitor.Endpoints, active.Manager.Endpoints)
	if receipt.After.MGR != 1 || receipt.After.Unclassified != 0 || receipt.After.Overflow {
		t.Fatal("late MGR endpoint was not attributed", receipt.After)
	}
	if svc.Configure().StatsPeriod < 0 {
		t.Fatal("invalid configure period")
	}
	visible := serviceWatch(t, ctx, service, "waiting", "present", map[string]string{"phase": "waiting"}, nil, nil)
	var entry struct {
		GID uint64 `json:"gid"`
	}
	if !visible.ServiceMatched || json.Unmarshal(visible.ServiceEntry, &entry) != nil || entry.GID != initial.GlobalID {
		t.Fatal("native registration identity mismatch")
	}
	receipt.CanceledAbsent = true
	for _, daemon := range []string{"deadline", "cancel", "client-close"} {
		request := serviceWatchInput(service, daemon, "absent", nil, nil, nil)
		request.WaitSeconds = 5
		request.StableSeconds = 1
		if !nativePool(t, ctx, "servicewatch", request).ServiceMatched {
			receipt.CanceledAbsent = false
			t.Fatal("canceled identity appeared", daemon)
		}
	}
	closeAttempted = true
	if err := svc.Close(ctx); err != nil {
		t.Fatal("close not acknowledged", err)
	}
	absent := serviceWatchInput(service, "waiting", "absent", nil, nil, nil)
	absent.WaitSeconds = 90
	absent.StableSeconds = 2
	receipt.Removed = nativePool(t, ctx, "servicewatch", absent).ServiceMatched
	if !receipt.Removed {
		t.Fatal("registration remained after close")
	}
	t.Logf("cold bootstrap native map epochs=%d->%d; MON renewal before activation; known deadline/cancel/Close; attributed dials before=%+v after=%+v; client GID=%d, MGR=%s/%d; native registration absent after close", receipt.Initial.Epoch, receipt.Active.Epoch, receipt.Before, receipt.After, initial.GlobalID, active.Manager.Name, active.Manager.GlobalID)
}

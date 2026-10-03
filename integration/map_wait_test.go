package integration_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephMapWaitIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.MaxInFlight = 1
	dial := options.DialContext
	var managerDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(address)
		if port == "36800" || port == "36801" {
			managerDials.Add(1)
		}
		return dial(ctx, network, address)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	mon := nativeMonitorMapWait(t, ctx, c, control, "initial")
	mgr := nativeManagerSnapshot(t, ctx, c, control, "initial").Manager
	initial, err := c.WaitMgrMap(ctx, 0)
	if err != nil || initial.MapEpoch < mgr.MapEpoch || initial.Ready {
		t.Fatal("initial manager wait", initial, err)
	}
	command := func(prefix string, args map[string]any) {
		t.Helper()
		cmd, err := cephmsgr.NewCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := admin.MonCommand(ctx, cmd); err != nil {
			t.Fatal(prefix, err)
		}
	}
	held, stopHeld := context.WithCancel(ctx)
	gate := &configSlotContext{Context: held, entered: make(chan struct{}), release: make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(gate, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
		finished <- err
	}()
	t.Cleanup(func() { stopHeld(); gate.unblock() })
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("sole command slot not occupied", ctx.Err())
	}
	type monResult struct {
		state cephmsgr.MonitorState
		err   error
	}
	awaitedMon := make(chan monResult, 1)
	go func() { state, err := c.WaitMonMap(ctx, mon.MapEpoch); awaitedMon <- monResult{state, err} }()
	host, _, err := net.SplitHostPort(mon.Endpoints[0])
	if err != nil {
		t.Fatal(err)
	}
	// A nonexistent fourth MON is added only to this disposable fixture.
	// Existing three MONs retain quorum; mutation commands are sent once.
	command("mon add", map[string]any{"name": "map-wait-spare", "addr": "v2:" + net.JoinHostPort(host, "33320") + "/0"})
	var added cephmsgr.MonitorState
	select {
	case result := <-awaitedMon:
		added = result.state
		if result.err != nil || added.MapEpoch <= mon.MapEpoch || !slices.ContainsFunc(added.Members, func(m cephmsgr.MonitorMember) bool { return m.Name == "map-wait-spare" }) {
			t.Fatal("added member did not wake map wait", added, result.err)
		}
	case <-ctx.Done():
		t.Fatal("wait added MON map", ctx.Err())
	}
	nativeMonitorMapWait(t, ctx, c, control, "added")
	added.Members[0].Name = "caller-owned"
	if c.Snapshot().Monitor.Members[0].Name == "caller-owned" {
		t.Fatal("waited members alias client map")
	}
	command("mon rm", map[string]any{"name": "map-wait-spare"})
	removed, err := c.WaitMonMap(ctx, added.MapEpoch)
	if err != nil || slices.ContainsFunc(removed.Members, func(m cephmsgr.MonitorMember) bool { return m.Name == "map-wait-spare" }) {
		t.Fatal("removed member did not wake map wait", removed, err)
	}
	nativeMonitorMapWait(t, ctx, c, control, "removed")
	type mgrResult struct {
		state cephmsgr.ManagerState
		err   error
	}
	awaitedMgr := make(chan mgrResult, 1)
	go func() { state, err := c.WaitMgrMap(ctx, mgr.MapEpoch); awaitedMgr <- mgrResult{state, err} }()
	command("mgr fail", map[string]any{"who": mgr.Name})
	var replacement cephmsgr.ManagerState
	select {
	case result := <-awaitedMgr:
		replacement = result.state
		if result.err != nil || replacement.MapEpoch <= mgr.MapEpoch {
			t.Fatal("MGR update did not wake map wait", replacement, result.err)
		}
	case <-ctx.Done():
		t.Fatal("wait replacement MGR map", ctx.Err())
	}
	for !replacement.Available || replacement.Name == mgr.Name {
		replacement, err = c.WaitMgrMap(ctx, replacement.MapEpoch)
		if err != nil {
			t.Fatal("wait available replacement", err)
		}
	}
	verified := nativeManagerSnapshot(t, ctx, c, control, "replacement").Manager
	if verified.Name != replacement.Name || verified.Ready || managerDials.Load() != 0 {
		t.Fatal("map waiting opened MGR or lost promotion", verified, managerDials.Load())
	}
	local, stopLocal := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = c.WaitMgrMap(local, ^uint32(0))
	stopLocal()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("local wait cancellation", err)
	}
	if _, err = c.WaitMgrMap(ctx, 0); err != nil {
		t.Fatal("cancelled wait terminated client", err)
	}
	stopHeld()
	gate.unblock()
	select {
	case err := <-finished:
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
			t.Fatal("held unsent command", err)
		}
	case <-ctx.Done():
		t.Fatal("release held slot", ctx.Err())
	}
	closedWait := make(chan error, 1)
	go func() { _, err := c.WaitMonMap(ctx, ^uint32(0)); closedWait <- err }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closedWait:
		if !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal("Close did not release wait", err)
		}
	case <-ctx.Done():
		t.Fatal("closed map waiter", ctx.Err())
	}
	if !reflect.DeepEqual(c.Snapshot().Monitor.Members, removed.Members) {
		t.Fatal("Close lost last monitor map")
	}
	t.Log("native MON add/remove and MGR promotion woke map waits while the sole command slot was held; local cancellation and Close passed")
}

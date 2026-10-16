package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func TestCephMapWaitIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prepareNativeManagerFailover(t, ctx, control)
	options := integrationOptions(t)
	options.MaxInFlight = 1
	dial := options.DialContext
	var managerDials atomic.Uint32
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(address)
		if role == "mgr.a" || role == "mgr.b" {
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
	t.Cleanup(func() { admin.Close() })
	mon := nativeMonitorMapWait(t, ctx, c, control, "initial")
	mgr := nativeManagerSnapshot(t, ctx, c, control, "initial").Manager
	initial, err := c.WaitMgrMap(ctx, 0)
	if err != nil || initial.MapEpoch < mgr.MapEpoch || initial.Ready {
		t.Fatal("initial manager wait", initial, err)
	}
	command := func(prefix string, args map[string]any) {
		t.Helper()
		cmd, err := newCommand(prefix, args)
		if err != nil {
			t.Fatal(err)
		}
		if result, err := admin.MonCommand(ctx, cmd); err != nil || result.Code != 0 {
			t.Fatal(prefix, result.Code, err)
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
	host, _, err := testcluster.SplitPeer(mon.Endpoints[0])
	if err != nil {
		t.Fatal(err)
	}
	// A nonexistent fourth MON is added only to this disposable fixture.
	// Existing three MONs retain quorum; mutation commands are sent once.
	command("mon add", map[string]any{"name": "map-wait-spare", "addr": "v2:" + net.JoinHostPort(host, "33320") + "/0"})
	removeSubmitted := false
	t.Cleanup(func() {
		if removeSubmitted {
			return
		}
		// The remove starts an election (on Tentacle 20.2.4 up to three
		// rounds, about 7 s), so later tests must not start fresh clients
		// before the same settled-election barrier as the main path.
		cleanup, stop := context.WithTimeout(context.Background(), 75*time.Second)
		defer stop()
		before := admin.Snapshot().Monitor
		payload, _ := json.Marshal(map[string]string{"prefix": "mon rm", "name": "map-wait-spare"})
		result, err := admin.MonCommand(cleanup, cephmsgr.Command{JSON: payload})
		if err != nil || result.Code != 0 {
			// Submitted once; an uncertain remove is never resubmitted.
			t.Errorf("remove disposable spare MON: code=%d error=%v", result.Code, err)
			markFixtureUnusable(t, control, fmt.Sprintf("cleanup mon rm map-wait-spare: code=%d error=%v", result.Code, err))
			return
		}
		removed := before
		for err == nil && slices.ContainsFunc(removed.Members, func(m cephmsgr.MonitorMember) bool { return m.Name == "map-wait-spare" }) {
			removed, err = admin.WaitMonMap(cleanup, removed.MapEpoch)
		}
		summary := ""
		if err == nil {
			summary, err = settleNativeMonitorElection(cleanup, admin, control, "removed", removed, mon.Members)
		}
		if err != nil {
			t.Errorf("fixture error: cleanup MON remove did not reach a settled election: %v", err)
			markFixtureUnusable(t, control, fmt.Sprintf("cleanup MON remove did not reach a settled election: %v", err))
			return
		}
		t.Log("cleanup", summary)
	})
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
	waitNativeMonitorElection(t, ctx, c, control, "added", added, mon.Members)
	nativeMonitorMapWait(t, ctx, c, control, "added")
	added.Members[0].Name = "caller-owned"
	if c.Snapshot().Monitor.Members[0].Name == "caller-owned" {
		t.Fatal("waited members alias client map")
	}
	// Mark submission before sending: an uncertain remove must not be replayed.
	removeSubmitted = true
	command("mon rm", map[string]any{"name": "map-wait-spare"})
	removed, err := c.WaitMonMap(ctx, added.MapEpoch)
	if err != nil || slices.ContainsFunc(removed.Members, func(m cephmsgr.MonitorMember) bool { return m.Name == "map-wait-spare" }) {
		t.Fatal("removed member did not wake map wait", removed, err)
	}
	waitNativeMonitorElection(t, ctx, c, control, "removed", removed, mon.Members)
	nativeMonitorMapWait(t, ctx, c, control, "removed")
	// MON membership changes can trigger elections while an earlier failed
	// native MGR respawns. Establish actual standby availability before this
	// separate MGR failover, preserving any native AUTH refusal and restoration.
	prepareNativeManagerFailover(t, ctx, control)
	mgr = nativeManagerSnapshot(t, ctx, c, control, "restored").Manager
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

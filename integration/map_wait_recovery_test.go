package integration_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephMapWaitRecoveryIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	dial := options.DialContext
	var managerDials atomic.Uint32
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	options.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(address)
		if port == "36800" || port == "36801" {
			managerDials.Add(1)
		}
		if armed.Load() && (port == "33301" || port == "33302") {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return dial(ctx, network, address)
	}
	fault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	fault.cleanupAfterClientClose(t, c)
	admin, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	original := nativeManagerSnapshot(t, ctx, c, control, "initial")
	armed.Store(true)
	if err := fault.interrupt(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("learned MON recovery did not start", ctx.Err())
	}
	retained := c.Snapshot()
	if retained.Monitor.Ready || retained.Monitor.MapEpoch != original.Monitor.MapEpoch || len(retained.Monitor.Endpoints) < 2 {
		t.Fatal("fixture did not hold MON recovery", retained.Monitor)
	}
	observed := &mapWaitObservedContext{Context: ctx, waiting: make(chan struct{})}
	type result struct {
		state cephmsgr.ManagerState
		err   error
	}
	waited := make(chan result, 1)
	go func() { state, err := c.WaitMgrMap(observed, retained.Manager.MapEpoch); waited <- result{state, err} }()
	select {
	case <-observed.waiting:
	case early := <-waited:
		t.Fatal("map wait did not span held recovery", early.err)
	case <-ctx.Done():
		t.Fatal("map wait did not block", ctx.Err())
	}
	cmd, err := cephmsgr.NewCommand("mgr fail", map[string]any{"who": original.Manager.Name})
	if err != nil {
		t.Fatal(err)
	}
	// The admin connection sends this fixture mutation once while the subject
	// has no MON path. The pending wait must survive and observe resubscription.
	if _, err := admin.MonCommand(ctx, cmd); err != nil {
		t.Fatal("once-only fixture promotion", err)
	}
	select {
	case early := <-waited:
		t.Fatal("map wait finished before learned MON was released", early.state, early.err)
	default:
	}
	unblock()
	var updated cephmsgr.ManagerState
	select {
	case got := <-waited:
		updated = got.state
		if got.err != nil || updated.MapEpoch <= retained.Manager.MapEpoch {
			t.Fatal("recovered map wait", updated, got.err)
		}
	case <-ctx.Done():
		t.Fatal("new map after MON recovery", ctx.Err())
	}
	for !updated.Available || updated.Name == original.Manager.Name {
		updated, err = c.WaitMgrMap(ctx, updated.MapEpoch)
		if err != nil {
			t.Fatal("wait recovered active manager", err)
		}
	}
	recovered := waitClientState(t, c, ctx, func(state cephmsgr.State) bool {
		return state.Monitor.Ready && state.Monitor.Endpoint != original.Monitor.Endpoint
	})
	verified := nativeManagerSnapshot(t, ctx, c, control, "replacement")
	if verified.Manager.Name != updated.Name || recovered.Manager.Ready || managerDials.Load() != 0 {
		t.Fatal("recovery lost map or opened MGR", verified.Manager, managerDials.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	fault.assertHeld(t)
	t.Logf("pending map wait survived held sole-seed failure, recovered through %s, and observed native MGR promotion without a MGR connection", recovered.Monitor.Endpoint)
}

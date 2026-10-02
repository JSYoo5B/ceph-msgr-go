package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephLogRecoveryIntegration(t *testing.T) {
	ordinaryLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1]
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	dial := options.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	var seedMu sync.Mutex
	var seedConn net.Conn
	var seedBlocked bool
	var seedDials, rejectedSeedDials int
	blockedDial := func(ctx context.Context, network string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, &net.OpError{Op: "dial", Net: network, Err: io.EOF}
	}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(endpoint)
		isSeed := err == nil && port == "33300"
		seedMu.Lock()
		blocked := isSeed && seedBlocked
		if blocked {
			rejectedSeedDials++
		}
		seedMu.Unlock()
		if blocked {
			return blockedDial(ctx, network)
		}
		conn, err := dial(ctx, network, endpoint)
		if err != nil || !isSeed {
			return conn, err
		}
		seedMu.Lock()
		if seedBlocked {
			// A dial begun before the fault must not escape the blacklist.
			rejectedSeedDials++
			seedMu.Unlock()
			conn.Close()
			return blockedDial(ctx, network)
		}
		seedConn = conn
		seedDials++
		seedMu.Unlock()
		return conn, nil
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate log recovery client", err)
	}
	t.Cleanup(func() { c.Close() })
	// Use retained fixture history rather than assuming asynchronous local
	// registration has already reached MON before the first once-only append.
	stream, err := c.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal("prepare MGR alongside MON logs", err)
	}
	initial := c.Snapshot()
	if initial.FSID == "" || initial.GlobalID == 0 || !initial.Monitor.Ready || !initial.Manager.Ready || !initial.Manager.Available {
		t.Fatal("log recovery did not establish healthy MON/MGR sessions", initial)
	}
	identity := func(state cephmsgr.State) {
		t.Helper()
		if state.GlobalID != initial.GlobalID || state.FSID != initial.FSID || state.AuthRejection != nil || state.Closed || state.Monitor.MapEpoch == 0 || state.Monitor.MinimumRelease < 20 || state.Manager.MapEpoch == 0 {
			t.Fatal("log recovery lost authenticated identity", state)
		}
	}
	var last uint64
	observe := func(label string, producer *cephmsgr.Client) {
		t.Helper()
		text := fmt.Sprintf("ceph-msgr-log-recovery-%s-%d", label, time.Now().UnixNano())
		publishLogOnce(t, ctx, producer, text)
		batch, entry := waitLogEntry(t, ctx, stream, text)
		if batch.Version <= last || batch.FSID != initial.FSID || entry.Sequence != 0 || entry.Priority != 1 || entry.Channel != "cluster" || entry.NameType != 8 || entry.NameID != "test" {
			t.Fatal("recovered log lost raw entry or service cursor", batch, entry)
		}
		last = batch.Version
		identity(c.Snapshot())
		for _, mgr := range []bool{false, true} {
			if err := fixtureRecoveryRead(ctx, c, mgr); err != nil {
				t.Fatal("ordinary MON/MGR command alongside recovered logs", mgr, err)
			}
		}
	}
	observe("initial", c)
	previous := initial
	for cycle := 1; cycle <= 2; cycle++ {
		deadline := previous.AuthTicket.Expires
		if previous.MgrTicket.Expires.Before(deadline) {
			deadline = previous.MgrTicket.Expires
		}
		renewal, stop := context.WithDeadline(ctx, deadline)
		state := waitClientState(t, c, renewal, func(s cephmsgr.State) bool {
			identity(s)
			return s.Monitor.Ready && s.Manager.Ready && s.Manager.Available && s.AuthTicket.Expires.After(previous.AuthTicket.Expires) && s.MgrTicket.Expires.After(previous.MgrTicket.Expires)
		})
		stop()
		if !time.Now().Before(deadline) {
			t.Fatal("log renewal crossed prior ticket expiry", state)
		}
		observe(fmt.Sprintf("renewal-%d", cycle), c)
		t.Logf("log stream survived renewal cycle=%d client-id=%d service-version=%d", cycle, state.GlobalID, last)
		previous = state
	}
	_, port, err := net.SplitHostPort(previous.Monitor.Endpoint)
	if err != nil || port != "33300" {
		t.Fatal("log test did not begin on sole MON a seed", previous.Monitor.Endpoint, err)
	}
	// This fault affects only this client's TCP path. Keep Ceph daemons and
	// quorum healthy for the once-only log mutations and the fixture's 12s TTL.
	// Restoring a local flag needs no live test context or daemon-control IPC.
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error("close log client before restoring its seed path", err)
		}
		seedMu.Lock()
		seedBlocked = false
		seedMu.Unlock()
	})
	seedMu.Lock()
	seedBlocked = true // Block new dials before releasing the recorded socket.
	lost, dialsBeforeFault := seedConn, seedDials
	seedMu.Unlock()
	if lost == nil {
		t.Fatal("no successful sole-seed TCP connection was recorded")
	}
	if err := lost.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal("inject client MON TCP connection loss", err)
	}
	recovered := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		identity(s)
		_, peerPort, err := net.SplitHostPort(s.Monitor.Endpoint)
		return s.Monitor.Ready && err == nil && (peerPort == "33301" || peerPort == "33302")
	})
	// The only configured seed remains locally unavailable until cleanup.
	// This independent producer connects directly to the admitted learned MON.
	observerOptions := integrationOptions(t)
	observerOptions.Monitors = []string{recovered.Monitor.Endpoint}
	observer, err := cephmsgr.Dial(ctx, observerOptions)
	if err != nil {
		t.Fatal("authenticate independent log producer at the learned MON", err)
	}
	t.Cleanup(func() { observer.Close() })
	observerState := observer.Snapshot()
	if observerState.GlobalID == 0 || observerState.GlobalID == initial.GlobalID || observerState.FSID != initial.FSID || !observerState.Monitor.Ready || observerState.AuthRejection != nil {
		t.Fatal("log observer did not establish an independent admitted identity", observerState)
	}
	if err := fixtureRecoveryRead(ctx, observer, false); err != nil {
		t.Fatal("independent learned-MON status before log mutation", err)
	}
	observe("learned-mon", observer)
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal("MGR command after MON log recovery", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("MON command after local log Close", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	closed := c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || closed.GlobalID != initial.GlobalID || closed.FSID != initial.FSID || closed.AuthRejection != nil {
		t.Fatal("closed log client retained readiness or lost identity", closed)
	}
	seedMu.Lock()
	blocked, successfulDials, rejectedDials := seedBlocked, seedDials, rejectedSeedDials
	seedMu.Unlock()
	if !blocked || successfulDials != dialsBeforeFault || rejectedDials == 0 {
		t.Fatal("sole-seed blacklist was not sustained through recovery and Close", blocked, successfulDials, dialsBeforeFault, rejectedDials)
	}
	t.Logf("client TCP fault only; healthy Ceph daemons/quorum; log watch survived two renewals and unavailable sole seed %q -> learned %q; client-id=%d service-version=%d blocked seed dials=%d", initial.Monitor.Endpoint, recovered.Monitor.Endpoint, initial.GlobalID, last, rejectedDials)
}

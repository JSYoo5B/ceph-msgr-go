package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func testMappedAddressLifecycle(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" || os.Getenv("CEPH_MSGR_TEST_MAPPED_IPV6") != "1" || os.Getenv("CEPH_MSGR_TEST_PROXY") != "" {
		t.Skip("requires the container-local mapped Ceph fixture")
	}
	if os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" {
		t.Fatal("mapped lifecycle requires the two-MGR fixture")
	}
	const fsid = "80bbab73-69c1-4a0c-a746-4271357750b8"
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = []string{"v2:[::ffff:7f00:1]:33300/0"}
	options.DialContext = nil
	options.ExpectedFSID = fsid
	options.ConnectTimeout = 5 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	seedFault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("mapped lifecycle authentication", err)
	}
	t.Cleanup(func() { c.Close() })
	for _, target := range []struct {
		call   func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)
		prefix string
	}{{c.MonCommand, "status"}, {c.MgrCommand, "pg stat"}} {
		command, _ := json.Marshal(map[string]string{"prefix": target.prefix, "format": "json"})
		result, err := target.call(ctx, cephmsgr.Command{JSON: command})
		if err != nil || result.Code != 0 || !json.Valid(result.Data) {
			t.Fatalf("initial mapped %s: code=%d dataBytes=%d err=%v", target.prefix, result.Code, len(result.Data), err)
		}
	}
	initial := c.Snapshot()
	if initial.GlobalID == 0 || !initial.Monitor.Ready || !initial.Manager.Ready || !initial.Manager.Available {
		t.Fatal("mapped lifecycle did not establish MON/MGR sessions", initial)
	}
	checkMapped := func(stage string, state cephmsgr.State) {
		t.Helper()
		if state.Closed || state.FSID != fsid || state.GlobalID != initial.GlobalID || state.AuthRejection != nil || state.Monitor.MapEpoch == 0 || state.Monitor.MinimumRelease < 20 || state.Manager.MapEpoch == 0 {
			t.Fatal(stage, "lost authenticated cluster/client identity", state)
		}
		mappedOracleEndpoints(t, stage+" MON", state.Monitor.Endpoints, 33300, 33301, 33302)
		if state.Manager.Available {
			port := map[string]uint16{"a": 36800, "b": 36801}[state.Manager.Name]
			if port == 0 || state.Manager.GlobalID == 0 {
				t.Fatal(stage, "has an unexpected active MGR identity", state)
			}
			mappedOracleEndpoints(t, stage+" MGR", state.Manager.Endpoints, port)
		}
	}
	checkMapped("initial", initial)
	previous := initial
	for cycle := 1; cycle <= 2; cycle++ {
		deadline := previous.AuthTicket.Expires
		if previous.MgrTicket.Expires.Before(deadline) {
			deadline = previous.MgrTicket.Expires
		}
		renew, stop := context.WithDeadline(ctx, deadline)
		state := waitClientState(t, c, renew, func(s cephmsgr.State) bool {
			checkMapped("renewal", s)
			return s.Monitor.Ready && s.Manager.Ready && s.Manager.Available && s.AuthTicket.Expires.After(previous.AuthTicket.Expires) && s.MgrTicket.Expires.After(previous.MgrTicket.Expires)
		})
		stop()
		if !time.Now().Before(deadline) {
			t.Fatal("mapped ticket renewal completed after prior expiry", state)
		}
		t.Logf("mapped renewal cycle=%d client-id=%d AUTH expiry=%s MGR expiry=%s", cycle, state.GlobalID, state.AuthTicket.Expires.Format(time.RFC3339Nano), state.MgrTicket.Expires.Format(time.RFC3339Nano))
		previous = state
	}
	// The only configured seed is MON a. Its local TCP path stays unavailable
	// through Close, so b/c must be an authenticated learned AF_INET6 target.
	_, port, err := net.SplitHostPort(previous.Monitor.Endpoint)
	if err != nil || port != "33300" {
		t.Fatal("mapped learned-path oracle did not begin on the sole seed", previous.Monitor.Endpoint, err)
	}
	// Keep Ceph daemons and quorum running: this isolates client learned-peer
	// recovery from native daemon proof/ticket expiry under the fixture's 12s TTL.
	seedFault.cleanupAfterClientClose(t, c)
	t.Logf("inject mapped client TCP path fault only; sole MON-a seed blocked until client Close; Ceph daemons/quorum remain running; client-id=%d", previous.GlobalID)
	if err := seedFault.interrupt(); err != nil {
		t.Fatal("inject mapped client MON TCP connection loss", err)
	}
	recovered := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkMapped("learned MON recovery", s)
		_, peerPort, err := net.SplitHostPort(s.Monitor.Endpoint)
		return s.Monitor.Ready && err == nil && (peerPort == "33301" || peerPort == "33302")
	})
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("status through learned mapped MON while sole seed path blocked", err)
	}
	t.Logf("mapped unavailable client seed path %q -> learned peer %q; client-id=%d", previous.Monitor.Endpoint, recovered.Monitor.Endpoint, recovered.GlobalID)
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare mapped MGR before takeover", err)
	}
	beforeTakeover := c.Snapshot()
	checkMapped("before MGR takeover", beforeTakeover)
	observerOptions := options
	observerOptions.Monitors = []string{"v2:[::ffff:7f00:1]:33300/0", "v2:[::ffff:7f00:1]:33301/0", "v2:[::ffff:7f00:1]:33302/0"}
	observerOptions.DialContext = nil // Independent default dialer has no client fault.
	observer, err := cephmsgr.Dial(ctx, observerOptions)
	if err != nil {
		t.Fatal("independent mapped observer authentication", err)
	}
	t.Cleanup(func() { observer.Close() })
	if id := observer.Snapshot().GlobalID; id == 0 || id == initial.GlobalID {
		t.Fatal("mapped observer did not acquire an independent global ID", id)
	}
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": beforeTakeover.Manager.Name})
	// Submit this mutation exactly once; never retry an uncertain outcome.
	result, err := observer.MonCommand(ctx, cephmsgr.Command{JSON: encoded})
	if err != nil || result.Code != 0 {
		t.Fatal("once-only mapped MGR fail", result.Code, err)
	}
	replacement := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkMapped("MGR takeover", s)
		return s.Monitor.Ready && s.Manager.Available && s.Manager.MapEpoch > beforeTakeover.Manager.MapEpoch && s.Manager.GlobalID != beforeTakeover.Manager.GlobalID && s.Manager.Name != beforeTakeover.Manager.Name
	})
	if replacement.Manager.Ready {
		t.Fatal("advertised mapped replacement MGR was connected before lazy setup", replacement)
	}
	result, err = observer.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
	var server struct {
		Epoch     uint32 `json:"epoch"`
		GlobalID  uint64 `json:"active_gid"`
		Name      string `json:"active_name"`
		Available bool   `json:"available"`
	}
	if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &server) != nil || !server.Available || server.Epoch <= beforeTakeover.Manager.MapEpoch || server.GlobalID == 0 || server.GlobalID == beforeTakeover.Manager.GlobalID || server.Name == beforeTakeover.Manager.Name {
		t.Fatal("independent mapped MgrMap query did not confirm takeover", result.Code, err, server)
	}
	replacement = waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkMapped("confirmed MGR takeover", s)
		return s.Monitor.Ready && s.Manager.Available && s.Manager.MapEpoch >= server.Epoch && s.Manager.GlobalID == server.GlobalID && s.Manager.Name == server.Name
	})
	if replacement.Manager.Ready {
		t.Fatal("local map observation opened the lazy mapped MGR session", replacement)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare replacement mapped MGR", err)
	}
	for _, mgr := range []bool{false, true} {
		if err := fixtureRecoveryRead(ctx, c, mgr); err != nil {
			t.Fatal("read-only mapped command after takeover", mgr, err)
		}
	}
	final := c.Snapshot()
	checkMapped("recovered MON/MGR", final)
	if !final.Monitor.Ready || !final.Manager.Ready || final.Manager.GlobalID != server.GlobalID || final.Manager.Name != server.Name {
		t.Fatal("mapped recovery did not retain prepared replacement", final)
	}
	if err := c.Close(); err != nil {
		t.Fatal("close mapped lifecycle client", err)
	}
	closed := c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || closed.FSID != fsid || closed.GlobalID != initial.GlobalID || closed.AuthRejection != nil || closed.Manager.GlobalID != final.Manager.GlobalID {
		t.Fatal("closed mapped lifecycle lost metadata or retained readiness", closed)
	}
	mappedOracleEndpoints(t, "closed MON", closed.Monitor.Endpoints, 33300, 33301, 33302)
	mappedOracleEndpoints(t, "closed MGR", closed.Manager.Endpoints, map[string]uint16{"a": 36800, "b": 36801}[closed.Manager.Name])
	closedWait, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *cephmsgr.OutcomeUnknownError
		if err := wait(closedWait); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("closed mapped readiness lost known non-execution", err)
		}
	}
	stats := seedFault.assertHeld(t)
	t.Logf("client TCP fault only; healthy Ceph daemons/quorum; mapped two ticket renewals, learned MON admission and lazy MGR takeover passed; retained client-id=%d MGR %s/%d -> %s/%d; blocked seed dials=%d", final.GlobalID, beforeTakeover.Manager.Name, beforeTakeover.Manager.GlobalID, final.Manager.Name, final.Manager.GlobalID, stats.Rejected)
}

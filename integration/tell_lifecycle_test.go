package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephTellRecoveryIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_MAPPED_IPV6") == "1" {
		t.Skip("requires the ordinary two-MGR disposable fixture")
	}
	for _, name := range []string{"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS"} {
		if os.Getenv(name) == "1" {
			t.Skip("requires ordinary ticket lifetimes for tell recovery")
		}
	}
	if os.Getenv("CEPH_MSGR_TEST_MODE_REJECTION") != "" {
		t.Skip("requires secure MON and MGR listeners")
	}
	oracles := make(map[string]tellVersion)
	for _, role := range []string{"mon", "mgr"} {
		data, err := os.ReadFile(filepath.Join(control, "tell-oracle-"+role+".json"))
		var version tellVersion
		if err != nil || json.Unmarshal(data, &version) != nil || !strings.HasPrefix(version.Version, "20.2.") || version.Release != "tentacle" || version.ReleaseType == "" {
			t.Fatal("independent native tell version oracle failed", role, err)
		}
		oracles[role] = version
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.Monitors = options.Monitors[:1] // Fixture MON a; b/c must be learned.
	options.ConnectTimeout = 2 * time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = 200*time.Millisecond, 2*time.Second
	fault := installSeedPathFault(&options)
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal("authenticate tell recovery client", err)
	}
	fault.cleanupAfterClientClose(t, c)
	versionCommand := cephmsgr.Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	checkTell := func(role string, call func(context.Context, cephmsgr.Command) (cephmsgr.Result, error)) {
		t.Helper()
		result, err := call(ctx, versionCommand)
		var version tellVersion
		if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &version) != nil || version != oracles[role] {
			t.Fatalf("recovered %s tell disagrees with native daemon version: code=%d bytes=%d err=%v version=%+v", role, result.Code, len(result.Data), err, version)
		}
	}
	checkTell("mon", c.MonTell)
	checkTell("mgr", c.MgrTell)
	initial := c.Snapshot()
	if initial.GlobalID == 0 || initial.FSID == "" || !initial.Monitor.Ready || !initial.Manager.Ready || !initial.Manager.Available {
		t.Fatal("tell recovery did not establish MON/MGR sessions", initial)
	}
	checkIdentity := func(state cephmsgr.State) {
		t.Helper()
		if state.Closed || state.FSID != initial.FSID || state.GlobalID != initial.GlobalID || state.AuthRejection != nil || state.Monitor.MapEpoch == 0 || state.Monitor.MinimumRelease < 20 || state.Manager.MapEpoch == 0 {
			t.Fatal("tell recovery lost admitted cluster/client identity", state)
		}
	}
	previous := initial
	for cycle := 1; cycle <= 2; cycle++ {
		deadline := previous.AuthTicket.Expires
		if previous.MgrTicket.Expires.Before(deadline) {
			deadline = previous.MgrTicket.Expires
		}
		renew, stop := context.WithDeadline(ctx, deadline)
		state := waitClientState(t, c, renew, func(s cephmsgr.State) bool {
			checkIdentity(s)
			return s.Monitor.Ready && s.Manager.Ready && s.Manager.Available && s.AuthTicket.Expires.After(previous.AuthTicket.Expires) && s.MgrTicket.Expires.After(previous.MgrTicket.Expires)
		})
		stop()
		if !time.Now().Before(deadline) {
			t.Fatal("tell ticket renewal completed after prior expiry", state)
		}
		checkTell("mon", c.MonTell)
		checkTell("mgr", c.MgrTell)
		t.Logf("tell after renewal cycle=%d retained client-id=%d AUTH/MGR expiry=%s/%s", cycle, state.GlobalID, state.AuthTicket.Expires.Format(time.RFC3339Nano), state.MgrTicket.Expires.Format(time.RFC3339Nano))
		previous = state
	}
	_, port, err := net.SplitHostPort(previous.Monitor.Endpoint)
	if err != nil || port != "33300" {
		t.Fatal("tell recovery did not begin on the sole MON a seed", previous.Monitor.Endpoint, err)
	}
	// Interrupt only this client's seed connection. Native MON/MGR daemon
	// authentication stays independent of this tell recovery fault.
	if err := fault.interrupt(); err != nil {
		t.Fatal("interrupt tell recovery seed TCP path", err)
	}
	recovered := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkIdentity(s)
		_, peerPort, err := net.SplitHostPort(s.Monitor.Endpoint)
		return s.Monitor.Ready && err == nil && (peerPort == "33301" || peerPort == "33302")
	})
	checkTell("mon", c.MonTell)
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("ordinary status through learned MON while seed TCP is blocked", err)
	}
	t.Logf("current-peer MON tell passed after sole-seed %q -> learned %q; client-id=%d", previous.Monitor.Endpoint, recovered.Monitor.Endpoint, recovered.GlobalID)
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare current MGR before tell takeover", err)
	}
	before := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkIdentity(s)
		return s.Monitor.Ready && s.Manager.Ready && s.Manager.Available
	})
	observer, err := cephmsgr.Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal("authenticate independent tell recovery observer", err)
	}
	t.Cleanup(func() { observer.Close() })
	if id := observer.Snapshot().GlobalID; id == 0 || id == initial.GlobalID {
		t.Fatal("tell observer did not acquire an independent client identity", id)
	}
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": before.Manager.Name})
	// The observer submits this mutation exactly once; never replay it.
	result, err := observer.MonCommand(ctx, cephmsgr.Command{JSON: encoded})
	if err != nil || result.Code != 0 {
		t.Fatal("once-only tell recovery MGR fail", result.Code, err)
	}
	replacement := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkIdentity(s)
		return s.Monitor.Ready && s.Manager.Available && s.Manager.MapEpoch > before.Manager.MapEpoch && s.Manager.GlobalID != before.Manager.GlobalID && s.Manager.Name != before.Manager.Name
	})
	if replacement.Manager.Ready {
		t.Fatal("advertised replacement MGR was connected before lazy preparation", replacement)
	}
	result, err = observer.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
	var server struct {
		Epoch     uint32 `json:"epoch"`
		GlobalID  uint64 `json:"active_gid"`
		Name      string `json:"active_name"`
		Available bool   `json:"available"`
	}
	if err != nil || result.Code != 0 || json.Unmarshal(result.Data, &server) != nil || !server.Available || server.Epoch <= before.Manager.MapEpoch || server.GlobalID == 0 || server.GlobalID == before.Manager.GlobalID || server.Name == before.Manager.Name {
		t.Fatal("independent MgrMap query did not confirm tell takeover", result.Code, err, server)
	}
	confirmed := waitClientState(t, c, ctx, func(s cephmsgr.State) bool {
		checkIdentity(s)
		return s.Monitor.Ready && s.Manager.Available && s.Manager.MapEpoch >= server.Epoch && s.Manager.GlobalID == server.GlobalID && s.Manager.Name == server.Name
	})
	if confirmed.Manager.Ready {
		t.Fatal("observing replacement map opened the lazy MGR session", confirmed)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("prepare replacement MGR for tell", err)
	}
	checkTell("mgr", c.MgrTell)
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal("ordinary pg stat after replacement MGR tell", err)
	}
	final := c.Snapshot()
	checkIdentity(final)
	_, mgrPort, err := net.SplitHostPort(final.Manager.Endpoint)
	if !final.Monitor.Ready || !final.Manager.Ready || final.Manager.GlobalID != server.GlobalID || final.Manager.Name != server.Name || err != nil || mgrPort != map[string]string{"a": "36800", "b": "36801"}[server.Name] {
		t.Fatal("tell did not run on the confirmed replacement MGR", final, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("close tell recovery client", err)
	}
	stats := fault.assertHeld(t)
	closed := c.Snapshot()
	if !closed.Closed || closed.Monitor.Ready || closed.Manager.Ready || closed.GlobalID != initial.GlobalID || closed.FSID != initial.FSID || closed.AuthRejection != nil || closed.Manager.GlobalID != final.Manager.GlobalID {
		t.Fatal("closed tell recovery lost metadata or retained readiness", closed)
	}
	closedWait, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for _, call := range []func(context.Context, cephmsgr.Command) (cephmsgr.Result, error){c.MonTell, c.MgrTell} {
		var unknown *cephmsgr.OutcomeUnknownError
		if _, err := call(closedWait, versionCommand); !errors.Is(err, cephmsgr.ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("tell after recovered Close lost known non-execution", err)
		}
	}
	t.Logf("current-peer tell survived two ticket renewals, blocked seed TCP recovery and lazy MGR takeover; client-id=%d MGR %s/%d -> %s/%d seed dials=%d rejected=%d", final.GlobalID, before.Manager.Name, before.Manager.GlobalID, final.Manager.Name, final.Manager.GlobalID, stats.Successful, stats.Rejected)
}

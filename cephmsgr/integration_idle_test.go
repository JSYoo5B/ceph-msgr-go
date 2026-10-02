package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type idleSessionReadProbe struct {
	net.Conn
	reads *atomic.Int32
}

func (c *idleSessionReadProbe) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.reads.Add(1)
	}
	return n, err
}

func TestCephIdleSessionsIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_IDLE_SESSIONS") != "1" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("requires the isolated long-ticket disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	options := integrationOptions(t)
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Second, 4*time.Second
	observer, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("independent idle fixture observer", err)
	}
	defer observer.Close()
	// Pinned DaemonServer::config show returns a single running value plus
	// newline for a key, even with JSON formatting. Unlike config get, this
	// queries the daemon's reported effective configuration, not the MON DB.
	configCtx, stopConfig := context.WithTimeout(ctx, 10*time.Second)
	defer stopConfig()
	for _, setting := range []struct {
		name  string
		value float64
	}{
		{"mon_session_timeout", 3},
		{"mon_tick_interval", 1},
		{"mon_client_ping_interval", 1},
		{"mon_client_hunt_interval", 1},
		{"mon_subscribe_interval", 2},
		{"auth_mon_ticket_ttl", 120},
		{"auth_service_ticket_ttl", 120},
	} {
		encoded, _ := json.Marshal(map[string]string{"prefix": "config show", "who": "mon.a", "key": setting.name})
		for {
			result, err := observer.MgrCommand(configCtx, Command{JSON: encoded})
			var server *CommandError
			if errors.As(err, &server) && server.Code == -2 && result.Code == -2 {
				// The active MGR can be ready before mon.a's configuration
				// report arrives. Retry only this known read-only absence.
				select {
				case <-time.After(100 * time.Millisecond):
					continue
				case <-configCtx.Done():
					t.Fatal("mon.a did not report its running configuration", setting.name, configCtx.Err())
				}
			}
			if err != nil || result.Code != 0 || len(result.Data) > 64 {
				t.Fatal("read actual mon.a idle fixture setting", setting.name, result.Code, err)
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(string(result.Data)), 64)
			if err != nil || value != setting.value {
				t.Fatal("mon.a idle fixture setting differs from the required server boundary", setting.name, value)
			}
			break
		}
	}
	stopConfig()
	// Bind both subjects to the MON whose effective configuration was checked.
	var seed string
	for _, candidate := range options.Monitors {
		endpoint, _, err := seedAddress(candidate)
		_, port, _ := net.SplitHostPort(endpoint)
		if err == nil && port == "33300" {
			seed = candidate
			break
		}
	}
	if seed == "" {
		t.Fatal("idle fixture did not provide the verified mon.a seed")
	}
	options.Monitors = []string{seed}
	dial := options.DialContext
	var monDials, mgrDials, monReads, mgrReads atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(endpoint)
		dials, reads := &monDials, &monReads
		if port == "36800" || port == "36801" {
			dials, reads = &mgrDials, &mgrReads
		}
		dials.Add(1)
		conn, err := dial(ctx, network, endpoint)
		if err == nil {
			conn = &idleSessionReadProbe{Conn: conn, reads: reads}
		}
		return conn, err
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	query := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if result, err := c.MgrCommand(ctx, query); err != nil || !json.Valid(result.Data) {
		t.Fatal("establish the initial MGR session", err)
	}
	c.mu.Lock()
	mon, mgr, auth, managerID := c.mon, c.mgr, c.auth, c.mgrMap.GlobalID
	c.mu.Unlock()
	for _, ticket := range auth.Tickets {
		if time.Until(ticket.RenewAfter) < time.Minute {
			t.Fatal("fixture ticket would renew during the idle test")
		}
	}
	// The control has the same valid credentials and stateful subscriptions,
	// but cannot send its first keepalive within the eight-second observation.
	// Its 30-second local timeout cannot cause the expected server-side close.
	controlOptions := integrationOptions(t)
	controlOptions.Monitors = []string{seed}
	controlOptions.KeepaliveInterval, controlOptions.KeepaliveTimeout = 10*time.Second, 30*time.Second
	control, err := Dial(ctx, controlOptions)
	if err != nil {
		t.Fatal("authenticate the slow-keepalive trim control", err)
	}
	defer control.Close()
	control.mu.Lock()
	controlMon, controlAuth := control.mon, control.auth
	control.mu.Unlock()
	for _, ticket := range controlAuth.Tickets {
		if time.Until(ticket.RenewAfter) < time.Minute {
			t.Fatal("trim control would renew instead of remaining idle")
		}
	}
	initialMonReads, initialMgrReads := monReads.Load(), mgrReads.Load()
	// Exceed the server's three-second session timeout, local silence timeout
	// and two-second legacy subscribe interval without commands or renewal.
	started := time.Now()
	controlDone := controlMon.Done()
	var trimmedAfter time.Duration
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	idle := true
	for idle {
		select {
		case <-timer.C:
			idle = false
		case <-controlDone:
			trimmedAfter = time.Since(started)
			if trimmedAfter < 2*time.Second || trimmedAfter >= 8*time.Second || !testcluster.IsRecoveryError(controlMon.Err()) {
				t.Fatal("slow-keepalive control did not observe the known server trim boundary", trimmedAfter, controlMon.Err())
			}
			// Keep the original session's close observable even if the
			// coordinator already began fresh-session recovery. No command
			// is issued by this control or repeated on a replacement session.
			if err := control.Close(); err != nil {
				t.Fatal("close the trimmed control", err)
			}
			controlDone = nil
		case <-mon.Done():
			t.Fatal("idle MON session failed", mon.Err())
		case <-mgr.Done():
			t.Fatal("idle MGR session failed", mgr.Err())
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if controlDone != nil {
		t.Fatal("MON did not trim the idle slow-keepalive control before its first ping")
	}
	c.mu.Lock()
	unchanged := c.mon == mon && c.mgr == mgr && c.auth == auth && c.monReady
	c.mu.Unlock()
	monActivity, mgrActivity := monReads.Load()-initialMonReads, mgrReads.Load()-initialMgrReads
	if !unchanged || mon.Err() != nil || mgr.Err() != nil || monDials.Load() != 1 || mgrDials.Load() != 1 || monActivity < 3 || mgrActivity < 3 {
		t.Fatal("idle session reconnected, renewed or stopped receiving", unchanged, mon.Err(), mgr.Err(), monDials.Load(), mgrDials.Load(), monActivity, mgrActivity)
	}
	// Change the active MGR from an independent native client. The idle
	// client's original MON connection must still deliver its MgrMap.
	c.mu.Lock()
	name := c.mgrMap.Name
	c.mu.Unlock()
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := observer.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("independent MGR switch", err)
	}
	for {
		c.mu.Lock()
		changed, signal := c.mgrMap.Available && c.mgrMap.GlobalID != managerID, c.changed
		c.mu.Unlock()
		if changed {
			break
		}
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("idle stateful subscription stopped delivering maps", ctx.Err())
		}
	}
	if result, err := c.MgrCommand(ctx, query); err != nil || !json.Valid(result.Data) {
		t.Fatal("new MGR command after idle map update", err)
	}
	c.mu.Lock()
	unchanged = c.mon == mon && c.auth == auth && c.monReady
	c.mu.Unlock()
	if !unchanged || monDials.Load() != 1 || mgrDials.Load() != 2 {
		t.Fatal("map delivery required MON reconnect or renewal", unchanged, monDials.Load(), mgrDials.Load())
	}
	t.Logf("idle=8s server-timeout=3s tick=1s slow-keepalive-control-trim=%s ticket-TTL=120s MON/MGR encrypted incoming reads=%d/%d; unchanged MON/auth, MGR connections=%d after one independent failover", trimmedAfter, monActivity, mgrActivity, mgrDials.Load())
}

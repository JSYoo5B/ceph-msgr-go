package cephmsgr

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
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
	initialMonReads, initialMgrReads := monReads.Load(), mgrReads.Load()
	// Exceed both the local silence timeout and the fixture's two-second
	// legacy subscribe interval, without application traffic or renewal.
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-mon.Done():
		t.Fatal("idle MON session failed", mon.Err())
	case <-mgr.Done():
		t.Fatal("idle MGR session failed", mgr.Err())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
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
	observer, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
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
	t.Logf("idle=8s ticket-TTL=120s MON/MGR encrypted incoming reads=%d/%d; unchanged MON/auth, MGR connections=%d after independent failover", monActivity, mgrActivity, mgrDials.Load())
}

package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestCephPausedManagerIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("requires a disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Second, 3*time.Second
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	command := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	if _, err := c.MgrCommand(ctx, command); err != nil {
		t.Fatal("establish MGR", err)
	}
	c.mu.Lock()
	old, name, id := c.mgr, c.mgrMap.Name, c.mgrMap.GlobalID
	c.mu.Unlock()
	if err := testcluster.ControlDaemon(ctx, control, "pause", "mgr", name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		resume, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(resume, control, "resume", "mgr", name); err != nil {
			t.Error("resume paused fixture MGR", err)
		}
	}()
	// SIGSTOP leaves the daemon's kernel TCP socket open, but no Messenger
	// reply or keepalive acknowledgment can be produced by the process.
	_, err = c.MgrCommand(ctx, command)
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrKeepaliveTimeout) || !errors.As(err, &unknown) {
		t.Fatal("paused daemon lost its uncertain liveness result", err)
	}
	if !errors.Is(old.Err(), ErrKeepaliveTimeout) {
		t.Fatal("old MGR was not terminated for silence", old.Err())
	}
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := c.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("fail paused MGR through native MON command", err)
	}
	for {
		c.mu.Lock()
		changed, signal := c.mgrMap.Available && c.mgrMap.GlobalID != id, c.changed
		c.mu.Unlock()
		if changed {
			break
		}
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("standby did not replace the paused MGR", ctx.Err())
		}
	}
	result, err := c.MgrCommand(ctx, command)
	if err != nil || !json.Valid(result.Data) {
		t.Fatal("command on replacement MGR", err)
	}
	t.Logf("paused MGR=%s: open socket silence detected and standby command passed", name)
}

func TestCephPausedMonitorIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("requires a disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.ConnectTimeout = time.Second
	options.KeepaliveInterval, options.KeepaliveTimeout = time.Second, 3*time.Second
	dial := options.DialContext
	var blockReconnect atomic.Bool
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(endpoint)
		if blockReconnect.Load() && (port == "33300" || port == "33301" || port == "33302") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return dial(ctx, network, endpoint)
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}); err != nil {
		t.Fatal("establish MGR before MON pause", err)
	}
	c.mu.Lock()
	old, oldManager := c.mon, c.mgr
	c.mu.Unlock()
	_, port, err := net.SplitHostPort(old.RemoteAddr().String())
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	if err != nil || name == "" {
		t.Fatal("unexpected fixture MON endpoint", old.RemoteAddr())
	}
	blockReconnect.Store(true)
	if err := testcluster.ControlDaemon(ctx, control, "pause", "mon", name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		resume, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(resume, control, "resume", "mon", name); err != nil {
			t.Error("resume paused fixture MON", err)
		}
	}()
	_, err = c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrKeepaliveTimeout) || !errors.As(err, &unknown) {
		t.Fatal("paused MON lost its uncertain liveness result", err)
	}
	state := c.Snapshot()
	if state.Monitor.Ready || state.Manager.Ready || !state.Manager.Available || state.Manager.Endpoint == "" || oldManager.Err() != nil {
		t.Fatal("live MGR concealed the MON admission dependency during actual silence", state, oldManager.Err())
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
		err := wait(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
			t.Fatal("preparation bypassed unavailable MON admission", err)
		}
	}
	blockReconnect.Store(false)
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("MON command after open socket silence", err)
	}
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	if current == old || current.RemoteAddr().String() == old.RemoteAddr().String() {
		t.Fatal("paused MON connection was reused")
	}
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal("MGR command after paused MON recovery", err)
	}
	state = waitClientState(t, c, ctx, func(s State) bool { return s.Monitor.Ready && s.Manager.Ready })
	if !state.Monitor.Ready || !state.Manager.Ready {
		t.Fatal("public admission state did not recover after actual MON silence", state)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); err != nil {
			t.Fatal("preparation after actual MON silence", err)
		}
	}
	t.Logf("paused MON=%s: silence detected, another quorum member served commands", name)
}

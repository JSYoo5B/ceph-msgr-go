package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func controlFixtureMonitors(ctx context.Context, control string, start bool) error {
	request, response := "stop-mons", "mons-stopped"
	if start {
		request, response = "start-mons", "mons-started"
	}
	return testcluster.RequestControl(ctx, control, request, response)
}

func fixtureRecoveryRead(ctx context.Context, c *Client, mgr bool) error {
	call, prefix := c.MonCommand, "status"
	if mgr {
		call, prefix = c.MgrCommand, "pg stat"
	}
	command, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
	for {
		result, err := call(ctx, Command{JSON: command})
		if err == nil {
			if result.Code != 0 || !json.Valid(result.Data) {
				return fmt.Errorf("invalid %s output after quorum recovery", prefix)
			}
			return nil
		}
		if !testcluster.IsRecoveryError(err, ErrManagerChanged, ErrKeepaliveTimeout, session.ErrRetired) {
			return err
		}
		// This is a read-only oracle. Mutations are never retried by this
		// helper or by the product's session recovery.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// All three MONs are stopped and started again, three times. While no MON is
// reachable, MonCommand, MgrCommand, MonTell, MgrTell, MonTellTo and MgrTellTo
// each end by their deadline without an unknown outcome: every channel first
// waits for an admitted MON, and exact-name targets are resolved from maps
// that need one, so the running manager is never reached.
//
//scenario:row mon-mgr.aes.ip4.crc.host.t-suite-afb7b2 key=aes service=aes mode=crc family=4 stress= runtime=host test_run=^TestCeph(CompleteMonitorOutage|NamedMonitorTell|NamedManagerTell)Integration$
//scenario:row mon-mgr.aes256k.ip4.host.t-suite-afb7b2 key=aes256k service=aes256k family=4 stress= runtime=host test_run=^TestCeph(CompleteMonitorOutage|NamedMonitorTell|NamedManagerTell)Integration$
//scenario:covers CLIENT to MON/MGR
//scenario:covers Daemon-to-daemon
//fixture:shareable stops and starts all MONs 3 times; start waits for 3-MON quorum and an MGR read succeeds after each
func TestCephCompleteMonitorOutageIntegration(t *testing.T) {
	baseline := runtime.NumGoroutine()
	c, ctx := fixtureClient(t, 2*time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 3; cycle++ {
		c.mu.Lock()
		old, manager := c.mon, c.mgrMap.Name
		c.mu.Unlock()
		if manager == "" {
			t.Fatal("no active manager name before the outage")
		}
		if err := controlFixtureMonitors(ctx, control, false); err != nil {
			t.Fatal("stop all MONs", err)
		}
		select {
		case <-old.Done():
		case <-ctx.Done():
			t.Fatal("old MON session remained open", ctx.Err())
		}
		// With no MON reachable, every command channel of the client waits
		// for MON admission and ends with the caller's deadline: known not to
		// have been sent. That includes both manager channels while the
		// manager itself is still running, and both exact-name Tells, whose
		// target is resolved from maps that need an admitted MON.
		status, version := Command{JSON: []byte(`{"prefix":"status"}`)}, Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
		for _, api := range []struct {
			name string
			call func(context.Context) (Result, error)
		}{
			{"MonCommand", func(ctx context.Context) (Result, error) { return c.MonCommand(ctx, status) }},
			{"MgrCommand", func(ctx context.Context) (Result, error) { return c.MgrCommand(ctx, status) }},
			{"MonTell", func(ctx context.Context) (Result, error) { return c.MonTell(ctx, version) }},
			{"MgrTell", func(ctx context.Context) (Result, error) { return c.MgrTell(ctx, version) }},
			{"MonTellTo", func(ctx context.Context) (Result, error) { return c.MonTellTo(ctx, "b", version) }},
			{"MgrTellTo", func(ctx context.Context) (Result, error) { return c.MgrTellTo(ctx, manager, version) }},
		} {
			short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			result, err := api.call(short)
			cancel()
			var unknown *OutcomeUnknownError
			if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || result.Code != 0 || result.Message != "" || len(result.Data) != 0 {
				t.Fatal("untransmitted outage wait has incorrect result", api.name, err)
			}
		}
		waiting := make(chan error, 2)
		for _, mgr := range []bool{false, true} {
			go func(mgr bool) { waiting <- fixtureRecoveryRead(ctx, c, mgr) }(mgr)
		}
		// Keep the operation contexts alive while the cluster has no MONs.
		select {
		case err := <-waiting:
			t.Fatal("request returned before MONs restarted", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := controlFixtureMonitors(ctx, control, true); err != nil {
			t.Fatal("restart all MONs", err)
		}
		for i := 0; i < cap(waiting); i++ {
			select {
			case err := <-waiting:
				if err != nil {
					t.Fatal("command after complete MON outage", err)
				}
			case <-ctx.Done():
				t.Fatal("restarted MONs did not release callers", ctx.Err())
			}
		}
		t.Logf("complete MON outage/recovery cycle=%d passed", cycle+1)
	}
	c.Close()
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Errorf("workers remain after quorum recovery: before=%d after=%d", baseline, after)
	}
}

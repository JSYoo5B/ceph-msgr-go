package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func fixtureClient(t *testing.T, timeout time.Duration) (*Client, context.Context) {
	t.Helper()
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("configuration and module changes require the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, ctx
}

func TestCephConfigCommandsIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, 20*time.Second)
	set := `{"prefix":"config set","who":"client.native-config-test","name":"debug_ms","value":"2/3"}`
	get := `{"prefix":"config get","who":"client.native-config-test","key":"debug_ms"}`
	remove := `{"prefix":"config rm","who":"client.native-config-test","name":"debug_ms"}`
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(set)}); err != nil {
		t.Fatal("config set", err)
	}
	result, err := c.MonCommand(ctx, Command{JSON: []byte(get)})
	if err != nil || strings.TrimSpace(string(result.Data)) != "2/3" {
		t.Fatal("config get did not preserve value", err, string(result.Data))
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(remove)}); err != nil {
		t.Fatal("config rm", err)
	}
	result, err = c.MonCommand(ctx, Command{JSON: []byte(get)})
	if err != nil || strings.TrimSpace(string(result.Data)) == "2/3" {
		t.Fatal("config override survived removal", err, string(result.Data))
	}
}

func checkModuleCommands(t *testing.T, c *Client, ctx context.Context) {
	t.Helper()
	result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"balancer status"}`)})
	var balancer map[string]json.RawMessage
	if err != nil || json.Unmarshal(result.Data, &balancer) != nil || balancer["active"] == nil || balancer["mode"] == nil {
		t.Fatal("balancer module JSON", err, string(result.Data))
	}
	result, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"crash stat"}`)})
	if err != nil || !strings.Contains(string(result.Data), "crashes recorded") {
		t.Fatal("crash module text", err, string(result.Data))
	}
	result, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"iostat","print_header":true,"width":80}`)})
	if err != nil || !strings.Contains(string(result.Data), "Read IOPS") || !strings.Contains(string(result.Data), "B/s") || json.Valid(result.Data) {
		t.Fatal("iostat module text", err, string(result.Data))
	}
}

func waitIostatState(t *testing.T, c *Client, ctx context.Context, enabled bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"iostat"}`)})
		var commandError *CommandError
		missing := errors.As(err, &commandError) && commandError.Code == -95 && strings.Contains(result.Message, "Module 'iostat' is not enabled")
		if enabled && err == nil && strings.Contains(string(result.Data), "B/s") || !enabled && missing {
			if missing {
				t.Logf("disabled module preserves server code=%d and status", result.Code)
			}
			return
		}
		if err != nil && !missing {
			// Module reloading can restart the MGR listener. Only this read
			// probe is repeated; enable/disable commands are sent once.
			var connection *net.OpError
			if !errors.As(err, &connection) && !errors.Is(err, ErrManagerChanged) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
				t.Fatal("unexpected module state error", err)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("module state did not settle", enabled, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestCephMgrModulesIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, time.Minute)
	checkModuleCommands(t, c, ctx)
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mgr module disable","module":"iostat"}`)}); err != nil {
		t.Fatal("disable optional module", err)
	}
	waitIostatState(t, c, ctx, false)
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mgr module enable","module":"iostat"}`)}); err != nil {
		t.Fatal("enable optional module", err)
	}
	waitIostatState(t, c, ctx, true)
	checkModuleCommands(t, c, ctx)
	c.mu.Lock()
	oldID, name := c.mgrMap.GlobalID, c.mgrMap.Name
	c.mu.Unlock()
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := c.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("MGR fail", err)
	}
	for {
		c.mu.Lock()
		changed, signal := c.mgrMap.Available && c.mgrMap.GlobalID != oldID, c.changed
		c.mu.Unlock()
		if changed {
			break
		}
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal("MGR module takeover", ctx.Err())
		}
	}
	waitIostatState(t, c, ctx, true)
	checkModuleCommands(t, c, ctx)
}

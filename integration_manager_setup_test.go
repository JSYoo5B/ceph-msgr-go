package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type managerSetupWriteProbe struct {
	net.Conn
	writes *atomic.Int32
}

func (c *managerSetupWriteProbe) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n >= 4096 {
		c.writes.Add(1)
	}
	return n, err
}

func TestCephManagerSetupFailoverIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, time.Minute)
	status := Command{JSON: []byte(`{"prefix":"balancer status"}`)}
	result, err := observer.MgrCommand(ctx, status)
	if err != nil {
		t.Fatal("read initial balancer mode", err)
	}
	var original struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(result.Data, &original); err != nil || original.Mode == "" {
		t.Fatal("decode initial balancer mode", err)
	}
	wanted := "upmap"
	if original.Mode == wanted {
		wanted = "none"
	}
	defer func() {
		restore, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		encoded, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": original.Mode})
		if _, err := observer.MgrCommand(restore, Command{JSON: encoded}); err != nil {
			t.Error("restore balancer mode", err)
		}
	}()
	options := integrationOptions(t)
	options.ConnectTimeout = 30 * time.Second
	dial := options.DialContext
	started, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	var stalled atomic.Bool
	var writes atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, port, err := net.SplitHostPort(endpoint)
		manager := err == nil && (port == "36800" || port == "36801")
		if manager && stalled.CompareAndSwap(false, true) {
			close(started)
			select {
			case <-release:
				return nil, io.EOF
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		conn, err := dial(ctx, network, endpoint)
		if err == nil && manager {
			return &managerSetupWriteProbe{Conn: conn, writes: &writes}, nil
		}
		return conn, err
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Valid trailing JSON whitespace identifies the command frame: handshake
	// and probe frames are smaller than 4 KiB. The command has no bulk input.
	mutation, _ := json.Marshal(map[string]string{"prefix": "balancer mode", "mode": wanted})
	mutation = append(mutation, bytes.Repeat([]byte{' '}, 40<<10)...)
	finished := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(ctx, Command{JSON: mutation})
		finished <- err
	}()
	select {
	case <-started:
	case err := <-finished:
		t.Fatal("command ended before the old MGR setup fault", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c.mu.Lock()
	name, id := c.mgrMap.Name, c.mgrMap.GlobalID
	c.mu.Unlock()
	encoded, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": name})
	if _, err := observer.MonCommand(ctx, Command{JSON: encoded}); err != nil {
		t.Fatal("switch MGR while another client's setup is pending", err)
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
		case err := <-finished:
			t.Fatal("setup ended before the new MGR was available", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if writes.Load() != 0 {
		t.Fatal("mutation was submitted before old MGR setup completed")
	}
	release <- struct{}{}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal("obsolete setup failure prevented current MGR mutation", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err = observer.MgrCommand(ctx, status)
	var current struct {
		Mode string `json:"mode"`
	}
	if err != nil || json.Unmarshal(result.Data, &current) != nil || current.Mode != wanted || writes.Load() != 1 {
		t.Fatal("independent observer did not confirm one submitted mode change", err, current.Mode, writes.Load())
	}
	t.Logf("MGR setup overlapped active replacement: obsolete EOF discarded, command writes=%d, independent mode=%s", writes.Load(), current.Mode)
}

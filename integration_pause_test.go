package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func controlFixtureDaemon(ctx context.Context, control, action, daemon, name string) error {
	id := time.Now().UnixNano()
	file, err := os.CreateTemp(control, action+"-"+daemon+"-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := fmt.Fprintf(file, "%d %s\n", id, name); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, action+"-"+daemon)); err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("%s-%s.%d", daemon, action, id))
	defer os.Remove(ack)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(ack); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

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
	if err := controlFixtureDaemon(ctx, control, "pause", "mgr", name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		resume, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := controlFixtureDaemon(resume, control, "resume", "mgr", name); err != nil {
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
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.mu.Lock()
	old := c.mon
	c.mu.Unlock()
	_, port, err := net.SplitHostPort(old.RemoteAddr().String())
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	if err != nil || name == "" {
		t.Fatal("unexpected fixture MON endpoint", old.RemoteAddr())
	}
	if err := controlFixtureDaemon(ctx, control, "pause", "mon", name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		resume, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := controlFixtureDaemon(resume, control, "resume", "mon", name); err != nil {
			t.Error("resume paused fixture MON", err)
		}
	}()
	_, err = c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, ErrKeepaliveTimeout) || !errors.As(err, &unknown) {
		t.Fatal("paused MON lost its uncertain liveness result", err)
	}
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
	t.Logf("paused MON=%s: silence detected, another quorum member served commands", name)
}

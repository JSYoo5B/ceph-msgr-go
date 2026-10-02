package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func controlFixtureMonitors(ctx context.Context, control string, start bool) error {
	request, response := "stop-mons", "mons-stopped"
	if start {
		request, response = "start-mons", "mons-started"
	}
	id := time.Now().UnixNano()
	file, err := os.CreateTemp(control, request+"-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := fmt.Fprintf(file, "%d\n", id); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(control, request)); err != nil {
		return err
	}
	ack := filepath.Join(control, fmt.Sprintf("%s.%d", response, id))
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

func fixtureRecoveryRead(ctx context.Context, c *Client, mgr bool) error {
	call, prefix := c.MonCommand, "status"
	if mgr {
		call, prefix = c.MgrCommand, "pg stat"
	}
	command, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
	for {
		result, err := call(ctx, Command{JSON: command})
		if err == nil {
			if !json.Valid(result.Data) {
				return fmt.Errorf("invalid %s output after quorum recovery", prefix)
			}
			return nil
		}
		var unknown *OutcomeUnknownError
		var network *net.OpError
		if !errors.As(err, &unknown) && !errors.As(err, &network) && !errors.Is(err, ErrManagerChanged) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
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

func TestCephCompleteMonitorOutageIntegration(t *testing.T) {
	baseline := runtime.NumGoroutine()
	c, ctx := fixtureClient(t, 2*time.Minute)
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := fixtureRecoveryRead(ctx, c, true); err != nil {
		t.Fatal(err)
	}
	for cycle := 0; cycle < 3; cycle++ {
		c.mu.Lock()
		old := c.mon
		c.mu.Unlock()
		if err := controlFixtureMonitors(ctx, control, false); err != nil {
			t.Fatal("stop all MONs", err)
		}
		select {
		case <-old.Done():
		case <-ctx.Done():
			t.Fatal("old MON session remained open", ctx.Err())
		}
		for _, call := range []func(context.Context, Command) (Result, error){c.MonCommand, c.MgrCommand} {
			short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			_, err := call(short, Command{JSON: []byte(`{"prefix":"status"}`)})
			cancel()
			var unknown *OutcomeUnknownError
			if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
				t.Fatal("untransmitted outage wait has incorrect result", err)
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

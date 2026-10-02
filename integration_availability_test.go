package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCephManagerAvailabilityIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "0" {
		t.Skip("requires a disposable fixture with delayed MGR startup")
	}
	c, ctx := fixtureClient(t, time.Minute)
	for {
		c.mu.Lock()
		epoch, available, changed := c.mgrMap.Epoch, c.mgrMap.Available, c.changed
		c.mu.Unlock()
		if epoch != 0 {
			if available {
				t.Fatal("fixture unexpectedly started with an available MGR")
			}
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("missing initial MgrMap", ctx.Err())
		}
	}
	command := Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}
	state := c.Snapshot()
	if !state.Monitor.Ready || state.Manager.Available || state.Manager.Ready {
		t.Fatal("unavailable MGR state obscured the ready MON", state)
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err := c.MgrCommand(short, command)
	cancel()
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("MGR discovery wait did not preserve known non-execution", err)
	}

	// Close must release callers blocked on discovery and on the MGR gate.
	closing, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer closing.Close()
	closedCalls := make(chan error, 8)
	for i := 0; i < cap(closedCalls); i++ {
		go func() { _, err := closing.MgrCommand(ctx, command); closedCalls <- err }()
	}
	for len(closing.calls) != cap(closedCalls) {
		select {
		case err := <-closedCalls:
			t.Fatal("discovery caller returned before Close", err)
		case <-ctx.Done():
			t.Fatal("discovery callers did not enter their waits", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(closedCalls); i++ {
		select {
		case err := <-closedCalls:
			if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
				t.Fatal("closed discovery wait has incorrect outcome", err)
			}
		case <-ctx.Done():
			t.Fatal("Close left a discovery caller waiting", ctx.Err())
		}
	}

	finished := make(chan error, 1)
	go func() {
		result, err := c.MgrCommand(ctx, command)
		if err == nil && !json.Valid(result.Data) {
			err = errors.New("invalid MGR output after delayed startup")
		}
		finished <- err
	}()
	// Observe genuine renewal while MGR is absent, with a pending MGR call.
	initial := c.Snapshot().AuthTicket
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for !c.Snapshot().AuthTicket.Expires.After(initial.Expires) {
		if result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil || !json.Valid(result.Data) {
			t.Fatal("MON unavailable while waiting for MGR", err)
		}
		select {
		case err := <-finished:
			t.Fatal("MGR call returned while no MGR existed", err)
		case <-ctx.Done():
			t.Fatal("MON tickets did not renew without MGR", ctx.Err())
		case <-ticker.C:
		}
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "start-mgrs"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal("waiting command failed after MGR startup", err)
		}
	case <-ctx.Done():
		t.Fatal("MGR startup did not release the waiting command", ctx.Err())
	}
	state = c.Snapshot()
	if !state.Manager.Available || !state.Manager.Ready || !state.Monitor.Ready {
		t.Fatal("delayed MGR startup did not update public state", state)
	}
	t.Log("MON stayed available and renewed tickets; MGR wait canceled locally or completed after discovery")
}

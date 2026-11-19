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

//scenario:suite mon-mgr.aes256k.ip4.managers-0 key=aes256k service=aes256k family=4 stress= managers=0
//fixture:isolated creates and starts both MGR daemons on a fixture that began with no MGR; they are left running
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
	if err := c.WaitMonReady(ctx); err != nil {
		t.Fatal("missing MGR blocked MON preparation", err)
	}
	prepare, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	err := c.WaitMgrReady(prepare)
	stop()
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("MGR preparation did not preserve known non-execution", err)
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	_, err = c.MgrCommand(short, command)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("MGR discovery wait did not preserve known non-execution", err)
	}
	// The current-manager Tell waits for an active manager in the same way.
	// The exact-name Tell does not wait: a name that the MgrMap does not
	// list is refused at once. Neither was sent.
	tell := Command{JSON: []byte(`{"prefix":"version","format":"json"}`)}
	short, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
	result, err := c.MgrTell(short, tell)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || result.Code != 0 || len(result.Data) != 0 {
		t.Fatal("MGR Tell discovery wait did not preserve known non-execution", err)
	}
	if result, err = c.MgrTellTo(ctx, "a", tell); !errors.Is(err, ErrManagerNotFound) || errors.As(err, &unknown) || result.Code != 0 || len(result.Data) != 0 {
		t.Fatal("an exact-name MGR Tell without any manager in the map was not a known-unsent refusal", err)
	}

	// Close must release callers blocked on discovery and on the MGR gate.
	closing, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer closing.Close()
	closedCalls := make(chan error, 8)
	for i := 0; i < cap(closedCalls); i++ {
		// Half of the waiters use the current-manager Tell.
		call, waiting := closing.MgrCommand, command
		if i%2 == 1 {
			call, waiting = closing.MgrTell, tell
		}
		go func() { _, err := call(ctx, waiting); closedCalls <- err }()
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
	closedWaits := make(chan error, 4)
	for i := 0; i < cap(closedWaits); i++ {
		go func() { closedWaits <- closing.WaitMgrReady(ctx) }()
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
	for i := 0; i < cap(closedWaits); i++ {
		select {
		case err := <-closedWaits:
			if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
				t.Fatal("closed preparation wait has incorrect outcome", err)
			}
		case <-ctx.Done():
			t.Fatal("Close left a preparation caller waiting", ctx.Err())
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
	prepared := make(chan error, 1)
	go func() { prepared <- c.WaitMgrReady(ctx) }()
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
		case err := <-prepared:
			t.Fatal("MGR preparation returned while no MGR existed", err)
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
	select {
	case err := <-prepared:
		if err != nil {
			t.Fatal("waiting preparation failed after MGR startup", err)
		}
	case <-ctx.Done():
		t.Fatal("MGR startup did not release waiting preparation", ctx.Err())
	}
	state = waitClientState(t, c, ctx, func(s State) bool { return s.Monitor.Ready && s.Manager.Ready })
	if !state.Manager.Available || !state.Manager.Ready || !state.Monitor.Ready {
		t.Fatal("delayed MGR startup did not update public state", state)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("MGR preparation after delayed startup", err)
	}
	t.Log("MON stayed available and renewed tickets; command and preparation waits canceled locally, ended at Close or completed after discovery")
}

package cephmsgr

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitClientState(t *testing.T, c *Client, ctx context.Context, matches func(State) bool) State {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state := c.Snapshot()
		if matches(state) {
			return state
		}
		select {
		case <-ctx.Done():
			t.Fatal("client state did not settle", ctx.Err(), state)
		case <-ticker.C:
		}
	}
}

func TestSnapshotReadinessAndIsolation(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1, 2, 3})
	dial := options.DialContext
	var attempts atomic.Int32
	var blocked atomic.Bool
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		attempts.Add(1)
		if blocked.Load() {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	state := waitClientState(t, c, ctx, func(s State) bool { return s.Manager.Available })
	if state.Closed || state.FSID != "01020300-0000-0000-0000-000000000000" || state.GlobalID != 42 || !state.Monitor.Ready || state.Manager.Ready || state.AuthRejection != nil {
		t.Fatal("unexpected bootstrap state", state)
	}
	if state.Monitor.MapEpoch != 1 || state.Monitor.MinimumRelease != 20 || state.Monitor.Endpoints[0] != "192.0.2.1:3300" || state.Manager.GlobalID != 99 || state.Manager.Name != "a" {
		t.Fatal("authenticated maps were not exposed", state)
	}
	if !state.AuthTicket.Expires.After(state.AuthTicket.RenewAfter) || !state.MgrTicket.Expires.After(state.MgrTicket.RenewAfter) {
		t.Fatal("ticket scheduling was lost", state)
	}
	before := attempts.Load()
	state.Monitor.Endpoints[0], state.Manager.Endpoints[0] = "changed", "changed"
	for i := 0; i < 10; i++ {
		if c.Snapshot().Manager.Ready || attempts.Load() != before {
			t.Fatal("local snapshot opened a MGR connection")
		}
	}
	if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal(err)
	}
	state = c.Snapshot()
	if !state.Manager.Ready || state.Manager.Endpoint == "" || state.Monitor.Endpoints[0] != "192.0.2.1:3300" || state.Manager.Endpoints[0] != "192.0.2.1:6800" {
		t.Fatal("snapshot mutation affected the client or MGR readiness", state)
	}
	blocked.Store(true)
	c.rejectAuthentication(errors.Join(errors.New("renewal"), &AuthenticationError{Method: 2, Code: -13}))
	state = c.Snapshot()
	if state.AuthRejection == nil || state.AuthRejection.Code != -13 || state.Monitor.Ready || state.Manager.Ready || state.Closed {
		t.Fatal("rejected authentication was reported as ready", state)
	}
	state.AuthRejection.Code = 0
	if c.Snapshot().AuthRejection.Code != -13 {
		t.Fatal("snapshot altered the published authentication rejection")
	}
	c.Close()
	state = c.Snapshot()
	if !state.Closed || state.Monitor.Ready || state.Manager.Ready || state.GlobalID != 42 || state.FSID == "" || !state.Manager.Available {
		t.Fatal("Close lost known state or retained readiness", state)
	}
}

func TestSnapshotConcurrentCommandsAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, mockOptions(t, 20, [16]byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				state := c.Snapshot()
				if state.Closed {
					if state.Monitor.Ready || state.Manager.Ready {
						t.Error("closed client reported a ready session")
					}
					return
				}
				if len(state.Manager.Endpoints) > 0 {
					state.Manager.Endpoints[0] = "caller owns this slice"
				}
			}
		}()
	}
	for i := 0; i < 10; i++ {
		if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
			t.Error(err)
			break
		}
	}
	c.Close()
	wg.Wait()
}

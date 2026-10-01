package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCephAppliedMonitorMutationIsNotReplayedIntegration(t *testing.T) {
	observer, ctx := fixtureClient(t, time.Minute)
	key := fmt.Sprintf("native-mon-no-replay-%d", time.Now().UnixNano())
	command := func(prefix string) Command {
		encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "key": key})
		return Command{JSON: encoded}
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := observer.MonCommand(cleanup, command("config-key rm")); err != nil {
			t.Error("remove no-replay fixture key", err)
		}
	}()
	options := integrationOptions(t)
	dial := options.DialContext
	var rejectedPort atomic.Value
	rejectedPort.Store("")
	var writes atomic.Int32
	var mu sync.Mutex
	connections := make(map[string]*lostReplyConn)
	blocked := make(chan struct{})
	var blockOnce sync.Once
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		_, port, _ := net.SplitHostPort(endpoint)
		if port == rejectedPort.Load().(string) {
			return nil, net.ErrClosed
		}
		conn, err := dial(ctx, network, endpoint)
		if err != nil {
			return nil, err
		}
		stalled := &lostReplyConn{Conn: conn, armed: &atomic.Bool{}, closed: make(chan struct{}), signalBlocked: func() { blockOnce.Do(func() { close(blocked) }) }}
		mu.Lock()
		connections[endpoint] = stalled
		mu.Unlock()
		return &commandWriteProbe{Conn: stalled, writes: &writes}, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.mu.Lock()
	old := c.mon
	endpoint := old.RemoteAddr().String()
	c.mu.Unlock()
	_, port, _ := net.SplitHostPort(endpoint)
	name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
	mu.Lock()
	stalled := connections[endpoint]
	mu.Unlock()
	if name == "" || stalled == nil {
		t.Fatal("could not identify the authenticated fixture MON", endpoint)
	}
	stalled.armed.Store(true)
	input := bytes.Repeat([]byte("applied MON mutation; "), 2048)
	mutation := command("config-key set")
	mutation.Input = input
	finished := make(chan error, 1)
	go func() { _, err := c.MonCommand(ctx, mutation); finished <- err }()
	for {
		result, err := observer.MonCommand(ctx, command("config-key get"))
		if err == nil && bytes.Equal(result.Data, input) {
			break
		}
		var server *CommandError
		if err == nil || !errors.As(err, &server) || server.Code != -2 {
			t.Fatal("independent client could not confirm the first mutation", err)
		}
		select {
		case err := <-finished:
			t.Fatal("mutation ended before its independently observed execution", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	select {
	case <-blocked:
	case err := <-finished:
		t.Fatal("applied MON mutation did not lose its reply", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	overwrite := command("config-key set")
	overwrite.Input = []byte("independent overwrite survives MON replacement")
	if _, err := observer.MonCommand(ctx, overwrite); err != nil {
		t.Fatal("commit the independent overwrite", err)
	}
	result, err := observer.MonCommand(ctx, command("config-key get"))
	if err != nil || !bytes.Equal(result.Data, overwrite.Input) || writes.Load() != 1 {
		t.Fatal("independent overwrite was not confirmed before MON replacement", err, writes.Load())
	}
	// Restart the real daemon, then release the intentionally withheld receive.
	// Exclude its endpoint so recovery must authenticate a different MON.
	rejectedPort.Store(port)
	if err := restartFixtureMonitor(ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), name); err != nil {
		t.Fatal("restart the MON that applied the unanswered mutation", err)
	}
	stalled.Close()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("applied MON mutation lost its unknown outcome on connection replacement", err)
		}
	case <-ctx.Done():
		t.Fatal("MON replacement did not release the pending mutation", ctx.Err())
	}
	result, err = c.MonCommand(ctx, command("config-key get"))
	if err != nil || !bytes.Equal(result.Data, overwrite.Input) || writes.Load() != 1 {
		t.Fatal("fresh MON replayed the applied mutation", err, writes.Load())
	}
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	if current == old || current.RemoteAddr().String() == endpoint {
		t.Fatal("recovery did not authenticate a different MON")
	}
	result, err = observer.MonCommand(ctx, command("config-key get"))
	if err != nil || !bytes.Equal(result.Data, overwrite.Input) {
		t.Fatal("independent observer found a replayed MON mutation", err)
	}
	t.Logf("MON %s restarted after applying an unanswered mutation; writes=%d, new authenticated MON=%s, independent overwrite preserved", name, writes.Load(), current.RemoteAddr())
}

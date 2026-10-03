package cephmsgr

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func TestConfigWatchRecoveryRequestsFullMapAndIgnoresUnadmittedSources(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	first := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	first.registration(t, ctx, c.options.Identity)
	first.send(t, ctx, configTestMessage("removed in recovery", "old", "retained", "old value"))
	logTestBarrier(t, ctx, c)
	c.mu.Lock()
	previous := c.mon
	c.mu.Unlock()
	first.conn.Close()
	second := configTestNextPeer(t, ctx, peers)
	second.registration(t, ctx, c.options.Identity) // Fresh Subscribe + GetConfig, no cursor.
	bad := msgr.MessageData{Type: 62, Version: 2, CompatVersion: 2, Front: []byte{0}}
	if err := c.handleConfig(previous, bad); err != nil {
		t.Fatal("stale source was decoded", err)
	}
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	unadmitted := &Client{mon: current, options: c.options}
	unadmitted.configWatch = &ConfigStream{client: unadmitted, source: current, changed: make(chan struct{}), cancel: func() {}}
	if err := unadmitted.handleConfig(current, bad); err != nil {
		t.Fatal("unadmitted candidate was decoded", err)
	}
	unadmitted.monReady = true
	unadmitted.configWatch.source = new(session.Session)
	if err := unadmitted.handleConfig(current, bad); err != nil {
		t.Fatal("watch belonging to another source decoded this message", err)
	}
	second.send(t, ctx, configTestMessage("retained", "new full value"))
	logTestBarrier(t, ctx, c)
	if config, err := stream.Next(ctx); err != nil || !reflect.DeepEqual(config, map[string]string{"retained": "new full value"}) {
		t.Fatal("recovery queued stale map or merged removed keys", config, err)
	}
	if c.Snapshot().Manager.Ready {
		t.Fatal("config recovery opened a MGR connection")
	}
}

func TestConfigWatchMalformedCurrentMapFailsUncertainCallWithoutReplay(t *testing.T) {
	for _, mode := range []string{"truncated", "unsupported compatibility", "duplicate key", "unexpected data"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			p := configTestNextPeer(t, ctx, peers)
			stream, err := c.WatchConfig(ctx, ConfigOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.registration(t, ctx, c.options.Identity)
			p.send(t, ctx, configTestMessage("accepted", "before malformed"))
			logTestBarrier(t, ctx, c)
			finished := make(chan error, 1)
			go func() {
				_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
				finished <- err
			}()
			for {
				select {
				case command := <-p.commands:
					if strings.Contains(string(command.Front), "hold mutation") {
						goto received
					}
				case <-ctx.Done():
					t.Fatal("mutation did not reach peer", ctx.Err())
				}
			}
		received:
			message := configTestMessage("malformed", "map")
			switch mode {
			case "truncated":
				message.Front = message.Front[:len(message.Front)-1]
			case "unsupported compatibility":
				message.Version, message.CompatVersion = 2, 2
			case "duplicate key":
				message = configTestMessage("a", "one", "a", "two")
			case "unexpected data":
				message.Data = []byte{0}
			}
			p.send(t, ctx, message)
			select {
			case err := <-finished:
				var unknown *OutcomeUnknownError
				if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) {
					t.Fatal("malformed config hid started call uncertainty/provenance", err)
				}
			case <-ctx.Done():
				t.Fatal("malformed config left started call pending", ctx.Err())
			}
			if config, err := stream.Next(ctx); err != nil || config["accepted"] != "before malformed" {
				t.Fatal("malformed current map discarded accepted data", config, err)
			}
			configTestTerminal(t, ctx, stream, ErrMalformedMessage)
			if err := c.WaitMonReady(ctx); err != nil {
				t.Fatal("ordinary MON recovery after malformed config", err)
			}
			replacement := configTestNextPeer(t, ctx, peers)
			logTestBarrier(t, ctx, c)
			for {
				select {
				case command := <-replacement.commands:
					if strings.Contains(string(command.Front), "hold mutation") {
						t.Fatal("malformed config replayed uncertain mutation")
					}
				default:
					return
				}
			}
		})
	}
}

func TestConfigWatchRestoredMonitorRequestsFullMapWhenWorkerMissedCandidate(t *testing.T) {
	owner := make(chan struct{})
	c, ctx, peers := configTestFixture(t, func(index int, p *configTestPeer) {
		if index == 1 {
			p.release = 19
		}
		if index != 0 {
			p.owner = owner
		}
	})
	p := configTestNextPeer(t, ctx, peers)
	base, cancel := context.WithCancel(ctx)
	gate := &logTestLoopGate{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
	stream := &ConfigStream{client: c, ctx: gate, cancel: cancel, done: make(chan struct{}), limit: uint64(c.options.MaxFrameSize), changed: make(chan struct{})}
	c.mu.Lock()
	c.configWatch = stream
	c.wg.Add(1)
	old := c.mon
	c.mu.Unlock()
	go stream.run()
	t.Cleanup(func() { gate.unblock(); stream.Close() })
	p.registration(t, ctx, c.options.Identity)
	p.send(t, ctx, configTestMessage("before rejected candidate", "old full value"))
	logTestBarrier(t, ctx, c)
	gate.armed.Store(true)
	c.mu.Lock()
	c.signal()
	c.mu.Unlock()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("worker did not reach controlled boundary", ctx.Err())
	}
	if err := c.connectMonitor(context.WithValue(ctx, logTestOwnerKey{}, owner)); !errors.Is(err, maps.ErrRelease) {
		t.Fatal("candidate did not fail release admission", err)
	}
	c.mu.Lock()
	restored := c.mon == old && c.monReady
	c.mu.Unlock()
	if !restored || old.Err() != nil {
		t.Fatal("candidate did not restore old authenticated MON", restored, old.Err())
	}
	gate.unblock()
	p.registration(t, ctx, c.options.Identity) // Includes a new GetConfig on this same session.
	p.send(t, ctx, configTestMessage("after restored source", "fresh full map"))
	logTestBarrier(t, ctx, c)
	if config, err := stream.Next(ctx); err != nil || !reflect.DeepEqual(config, map[string]string{"after restored source": "fresh full map"}) {
		t.Fatal("restored session did not request/accept fresh full map", config, err)
	}
}

func TestConfigWatchConcurrentNextTransfersMapOnceAndCloseReleasesWaiters(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchConfig(context.Background(), ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	p.send(t, ctx, configTestMessage("single", "ownership"))
	logTestBarrier(t, ctx, c)
	type result struct {
		config map[string]string
		err    error
	}
	results := make(chan result, 8)
	for range 8 {
		go func() { config, err := stream.Next(context.Background()); results <- result{config, err} }()
	}
	select {
	case got := <-results:
		if got.err != nil || !reflect.DeepEqual(got.config, map[string]string{"single": "ownership"}) {
			t.Fatal("first wait did not get accepted map", got)
		}
	case <-ctx.Done():
		t.Fatal("concurrent Next did not consume accepted map", ctx.Err())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("Client.Close returned before worker joined")
	}
	for range 7 {
		select {
		case got := <-results:
			if got.config != nil || !errors.Is(got.err, ErrClosed) {
				t.Fatal("map returned more than once or close failed to release waiter", got)
			}
		case <-ctx.Done():
			t.Fatal("Client.Close left concurrent Next waiting", ctx.Err())
		}
	}
	configTestTerminal(t, ctx, stream, ErrClosed)
}

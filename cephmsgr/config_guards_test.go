package cephmsgr

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

type configTestReentrantContext struct {
	context.Context
	client *Client
	checks atomic.Int32
}

func (ctx *configTestReentrantContext) Err() error {
	ctx.checks.Add(1)
	ctx.client.Snapshot()
	return ctx.Context.Err()
}

func (ctx *configTestReentrantContext) Done() <-chan struct{} {
	ctx.checks.Add(1)
	ctx.client.Snapshot()
	return ctx.Context.Done()
}

func (ctx *configTestReentrantContext) Value(key any) any {
	ctx.checks.Add(1)
	ctx.client.Snapshot()
	return ctx.Context.Value(key)
}

func TestConfigWatchCallerContextCallbacksRunOutsideClientLock(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	lifetime := &configTestReentrantContext{Context: ctx, client: c}
	type registered struct {
		stream *ConfigStream
		err    error
	}
	registration := make(chan registered, 1)
	go func() {
		stream, err := c.WatchConfig(lifetime, ConfigOptions{})
		registration <- registered{stream, err}
	}()
	var stream *ConfigStream
	select {
	case result := <-registration:
		if result.err != nil {
			t.Fatal(result.err)
		}
		stream = result.stream
	case <-ctx.Done():
		t.Fatal("custom watch context reentered a held client lock", ctx.Err())
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	p.send(t, ctx, configTestMessage("caller", "owned"))
	logTestBarrier(t, ctx, c)
	wait := &configTestReentrantContext{Context: ctx, client: c}
	next := make(chan error, 1)
	go func() {
		config, err := stream.Next(wait)
		if err == nil && config["caller"] != "owned" {
			err = errors.New("wrong config")
		}
		next <- err
	}()
	select {
	case err := <-next:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("custom Next context reentered a held client lock", ctx.Err())
	}
	if lifetime.checks.Load() == 0 || wait.checks.Load() == 0 {
		t.Fatal("reentrant contexts were not exercised")
	}
}

type configTestReentrantError struct {
	client *Client
	checks atomic.Int32
}

func (err *configTestReentrantError) Error() string { return "causal config protocol failure" }

func (err *configTestReentrantError) Is(target error) bool {
	err.checks.Add(1)
	err.client.Snapshot()
	return target == ErrMalformedMessage
}

func TestConfigWatchFatalSourceGuardInspectsErrorsOutsideLock(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.registration(t, ctx, c.options.Identity)
	p.send(t, ctx, configTestMessage("before fatal source", "accepted"))
	logTestBarrier(t, ctx, c)
	c.mu.Lock()
	source := c.mon
	c.mu.Unlock()
	cause := &configTestReentrantError{client: c}
	finished := make(chan struct{})
	go func() { c.stopConfigForFailedSession(source, cause); close(finished) }()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("custom error inspection reentered a held client lock", ctx.Err())
	}
	if cause.checks.Load() == 0 {
		t.Fatal("custom fatal-error inspection was not exercised")
	}
	stream.Close()
	if config, err := stream.Next(ctx); err != nil || config["before fatal source"] != "accepted" {
		t.Fatal("fatal source discarded accepted map", config, err)
	}
	for range 2 {
		if config, err := stream.Next(ctx); config != nil || err != cause {
			t.Fatal("later Close replaced the first registered fatal source cause", config, err)
		}
	}
	logTestBarrier(t, ctx, c)

	// A retired/private source cannot terminate a current watch merely because
	// its error happens to classify as a fatal Messenger error.
	replacement, err := c.WatchConfig(ctx, ConfigOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	p.registration(t, ctx, c.options.Identity)
	c.stopConfigForFailedSession(new(session.Session), cause)
	p.send(t, ctx, configTestMessage("current source", "still active"))
	if config, err := replacement.Next(ctx); err != nil || config["current source"] != "still active" {
		t.Fatal("stale source stopped current watch", config, err)
	}
}

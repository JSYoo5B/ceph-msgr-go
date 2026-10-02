package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func TestMonitorFailoverDoesNotReplayUncertainCommand(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	options.Monitors = append(options.Monitors, "192.0.2.3:3300")
	var primary, secondary, mutations atomic.Int32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if endpoint == "192.0.2.1:3300" && primary.Add(1) > 1 {
			return nil, errors.New("primary unavailable")
		}
		if endpoint == "192.0.2.3:3300" {
			secondary.Add(1)
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, command: func(m msgr.MessageData) {
			if strings.Contains(string(m.Front), "drop") {
				mutations.Add(1)
			}
		}})
		return client, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"drop"}`)})
	var unknown *OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatal("lost command outcome should be unknown", err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("new request did not use recovered monitor", err)
	}
	if mutations.Load() != 1 || secondary.Load() != 1 {
		t.Fatal("mutation replayed or secondary unused", mutations.Load(), secondary.Load())
	}
}

func TestRetirementTimeoutDoesNotReplayTransmittedMutation(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	options.ConnectTimeout = 100 * time.Millisecond
	started, release := make(chan struct{}), make(chan struct{})
	var mutations atomic.Int32
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, command: func(m msgr.MessageData) {
			if strings.Contains(string(m.Front), "mutation") && mutations.Add(1) == 1 {
				close(started)
				<-release
			}
		}})
		return client, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer close(release)
	done := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mutation"}`)})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c.mu.Lock()
	old := c.mon
	c.mu.Unlock()
	c.retireMonitor(old)
	select {
	case err := <-done:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, session.ErrRetired) {
			t.Fatal("transmitted retirement was treated as safe admission rejection", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if mutations.Load() != 1 {
		t.Fatal("mutation replayed after retirement", mutations.Load())
	}
}

func TestContextCancellationWhileWaitingForMonitorRecovery(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	var attempts atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return dial(ctx, network, endpoint)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	c, err := Dial(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.MonCommand(context.Background(), Command{JSON: []byte(`{"prefix":"drop"}`)})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("request did not stop waiting for recovery", err)
	}
}

func TestTicketRenewalDrainsPreviousMonitorRequests(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var attempts atomic.Int32
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, peer := net.Pipe()
		first := attempts.Add(1) == 1
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, command: func(m msgr.MessageData) {
			if first && strings.Contains(string(m.Front), "delay") {
				close(started)
				<-release
			}
		}})
		return client, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"delay"}`)})
		result <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	snapshot := c.snapshotAuth()
	for id, ticket := range snapshot.Tickets {
		ticket.RenewAfter = time.Now().Add(-time.Second)
		snapshot.Tickets[id] = ticket
	}
	c.mu.Lock()
	old := c.mon
	c.auth = snapshot
	c.mu.Unlock()
	c.wakeMonitor()
	for {
		c.mu.Lock()
		ready := c.mon != old && c.monReady
		changed := c.changed
		c.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if old.Err() != nil {
		t.Fatal("renewal closed a monitor with an outstanding command", old.Err())
	}
	// Unblock before Close so the test peer can respond and shut down.
	release <- struct{}{}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("renewal interrupted command", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("renewed connection unusable", err)
	}
}

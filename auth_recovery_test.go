package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestRejectedRenewalPreservesServerErrorAndRecovers(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	var rejecting atomic.Bool
	rejection := &AuthenticationError{Method: 2, Code: -13}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if rejecting.Load() && strings.HasSuffix(endpoint, ":3300") {
			return nil, rejection
		}
		return dial(ctx, network, endpoint)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// Force a missing MGR ticket, then let the actual coordinator attempt
	// renewal. The waiting call has not submitted a command to any daemon.
	snapshot := c.snapshotAuth()
	delete(snapshot.Tickets, cephx.ServiceMgr)
	c.mu.Lock()
	c.auth = snapshot
	c.mu.Unlock()
	rejecting.Store(true)
	_, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, rejection) || errors.As(err, &unknown) {
		t.Fatal("waiting call lost explicit renewal rejection", err)
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); !errors.Is(err, rejection) {
		t.Fatal("new MON call used a rejected authentication state", err)
	}
	rejecting.Store(false)
	c.wakeMonitor()
	for {
		_, err = c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)})
		if err == nil {
			break
		}
		if !errors.Is(err, rejection) {
			t.Fatal("restored authentication did not recover", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRejectedRenewalDoesNotReplayStartedMutation(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	var attempts, mutations atomic.Int32
	started := make(chan struct{})
	rejection := &AuthenticationError{Method: 2, Code: -13}
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if attempts.Add(1) > 1 {
			return nil, rejection
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: 1, command: func(m msgr.MessageData) {
			if strings.Contains(string(m.Front), "block mutation") {
				mutations.Add(1)
				close(started)
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
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"block mutation"}`)})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	snapshot := c.snapshotAuth()
	delete(snapshot.Tickets, cephx.ServiceMgr)
	c.mu.Lock()
	c.auth = snapshot
	c.mu.Unlock()
	c.wakeMonitor()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, rejection) {
			t.Fatal("started mutation lost uncertainty or authentication cause", err)
		}
	case <-ctx.Done():
		t.Fatal("renewal rejection did not release the outstanding call", ctx.Err())
	}
	if mutations.Load() != 1 {
		t.Fatal("mutation replayed after authentication rejection", mutations.Load())
	}
}

func TestManagerHandshakeCannotPublishAfterAuthenticationRejection(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var rejecting atomic.Bool
	rejection := &AuthenticationError{Method: 2, Code: -13}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else if rejecting.Load() {
			return nil, rejection
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
	finished := make(chan error, 1)
	go func() { _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); finished <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	rejecting.Store(true)
	snapshot := c.snapshotAuth()
	delete(snapshot.Tickets, cephx.ServiceMgr)
	c.mu.Lock()
	c.auth = snapshot
	c.mu.Unlock()
	c.wakeMonitor()
	for {
		c.mu.Lock()
		rejected, changed := c.authErr != nil, c.changed
		c.mu.Unlock()
		if rejected {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	release <- struct{}{}
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, rejection) || errors.As(err, &unknown) {
			t.Fatal("late MGR handshake admitted a command after rejection", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestManagerHandshakeCannotPublishAfterGlobalIDChange(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var monitors, managers, oldCommands, newCommands atomic.Int32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		cfg := peerConfig{fsid: [16]byte{1}, release: 20, role: 1, clientID: 42}
		if strings.HasSuffix(endpoint, ":6800") {
			cfg.role, cfg.id = 16, 99
			if managers.Add(1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				cfg.command = func(msgr.MessageData) { oldCommands.Add(1) }
			} else {
				cfg.clientID = 43
				cfg.command = func(msgr.MessageData) { newCommands.Add(1) }
			}
		} else if monitors.Add(1) > 1 {
			cfg.clientID = 43
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, cfg)
		return client, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	finished := make(chan error, 1)
	go func() { _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat"}`)}); finished <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A real reauthentication handshake assigns a new client identity while
	// the MGR handshake retains its earlier immutable authorizer snapshot.
	snapshot := c.snapshotAuth()
	for id, ticket := range snapshot.Tickets {
		ticket.RenewAfter = time.Now().Add(-time.Second)
		snapshot.Tickets[id] = ticket
	}
	c.mu.Lock()
	c.auth = snapshot
	c.mu.Unlock()
	c.wakeMonitor()
	for {
		c.mu.Lock()
		changedIdentity, changed := c.monReady && c.auth.GlobalID == 43, c.changed
		c.mu.Unlock()
		if changedIdentity {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	release <- struct{}{}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal("new identity did not handle the unsubmitted command", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if oldCommands.Load() != 0 || newCommands.Load() != 1 || managers.Load() != 2 {
		t.Fatal("late handshake admitted a command with the previous identity", oldCommands.Load(), newCommands.Load(), managers.Load())
	}
}

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

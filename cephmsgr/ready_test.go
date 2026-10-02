package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestReadinessOpensManagerWithoutCommandsOrCommandSlots(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxInFlight = 1
	var commands atomic.Int32
	var managers atomic.Int32
	blocked := make(chan struct{})
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		client, peer := net.Pipe()
		role, id := uint8(1), uint64(0)
		if strings.HasSuffix(endpoint, ":6800") {
			role, id = 16, 99
			managers.Add(1)
		}
		go mockDaemon(peer, peerConfig{fsid: [16]byte{1}, release: 20, role: role, id: id, command: func(m msgr.MessageData) {
			commands.Add(1)
			if strings.Contains(string(m.Front), "block") {
				close(blocked)
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
	if err := c.WaitMonReady(ctx); err != nil || managers.Load() != 0 {
		t.Fatal("MON readiness opened MGR", err)
	}
	if err := c.WaitMgrReady(ctx); err != nil || managers.Load() != 1 {
		t.Fatal("MGR readiness did not authenticate the lazy connection", err)
	}
	if commands.Load() != 0 {
		t.Fatal("readiness submitted an application command", commands.Load())
	}
	call, stop := context.WithCancel(ctx)
	defer stop()
	finished := make(chan error, 1)
	go func() { _, err := c.MonCommand(call, Command{JSON: []byte(`{"prefix":"block"}`)}); finished <- err }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); err != nil {
			t.Fatal("readiness was blocked by the occupied command slot", err)
		}
	}
	if commands.Load() != 1 || managers.Load() != 1 {
		t.Fatal("readiness sent a probe command or replaced a ready session", commands.Load(), managers.Load())
	}
	stop()
	var unknown *OutcomeUnknownError
	if err := <-finished; !errors.As(err, &unknown) {
		t.Fatal("the actual canceled command lost uncertainty", err)
	}
}

func TestReadinessCancellationDoesNotCancelClient(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	started := make(chan struct{})
	var once sync.Once
	var stall atomic.Bool
	stall.Store(true)
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") && stall.Load() {
			once.Do(func() { close(started) })
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
	call, stop := context.WithCancel(ctx)
	finished := make(chan error, 1)
	go func() { finished <- c.WaitMgrReady(call) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	var unknown *OutcomeUnknownError
	if err := <-finished; !errors.Is(err, context.Canceled) || errors.As(err, &unknown) {
		t.Fatal("canceled connection readiness claimed uncertain execution", err)
	}
	if err := c.WaitMonReady(ctx); err != nil || c.Snapshot().Closed {
		t.Fatal("canceling MGR readiness canceled the client", err)
	}
	stall.Store(false)
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal("canceled setup did not release the MGR gate", err)
	}
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(call); !errors.Is(err, context.Canceled) {
			t.Fatal("ready connection ignored an already canceled wait", err)
		}
	}
	c.Close()
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		if err := wait(ctx); !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
			t.Fatal("closed readiness wait lost its known outcome", err)
		}
		if err := wait(call); !errors.Is(err, context.Canceled) {
			t.Fatal("already canceled readiness lost context priority", err)
		}
	}
}

func TestCloseReleasesManagerReadinessWaiters(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	started := make(chan struct{})
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") {
			close(started)
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
	finished := make(chan error, 8)
	for i := 0; i < cap(finished); i++ {
		go func() { finished <- c.WaitMgrReady(ctx) }()
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c.Close()
	for i := 0; i < cap(finished); i++ {
		select {
		case err := <-finished:
			var unknown *OutcomeUnknownError
			if !errors.Is(err, ErrClosed) || errors.As(err, &unknown) {
				t.Fatal("Close lost the known readiness outcome", err)
			}
		case <-ctx.Done():
			t.Fatal("Close left a readiness caller waiting", ctx.Err())
		}
	}
}

func TestReadinessPreservesAuthenticationRejection(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	var rejecting atomic.Bool
	rejection := &AuthenticationError{Method: 2, Code: -13}
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if rejecting.Load() {
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
	rejecting.Store(true)
	c.rejectAuthentication(rejection)
	for _, wait := range []func(context.Context) error{c.WaitMonReady, c.WaitMgrReady} {
		var unknown *OutcomeUnknownError
		if err := wait(ctx); !errors.Is(err, rejection) || errors.As(err, &unknown) {
			t.Fatal("readiness lost the explicit server rejection", err)
		}
	}
}

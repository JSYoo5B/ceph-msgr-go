package cephmsgr

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestManagerSetupFailureUsesRenewedTicketWithoutChangingManager(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	started, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	var attempts, commands atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":6800") {
			return dial(ctx, network, endpoint)
		}
		if attempts.Add(1) == 1 {
			close(started)
			select {
			case <-release:
				return nil, &AuthenticationError{Method: 2, Code: -13}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		client, peer := net.Pipe()
		go mockDaemon(peer, peerConfig{role: 16, id: 99, command: func(msgr.MessageData) { commands.Add(1) }})
		return client, nil
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
		_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c.mu.Lock()
	oldAuth, mon, managerID := c.auth, c.mon, c.mgrMap.GlobalID
	c.mu.Unlock()
	// Reauthentication publishes new tickets while the old authorizer's
	// connection attempt remains pending. The active MGR identity is unchanged.
	mon.Fail(io.EOF)
	c.wakeMonitor()
	for {
		c.mu.Lock()
		renewed, changed := c.monReady && c.auth != oldAuth, c.changed
		unchanged := c.mgrMap.GlobalID == managerID && c.auth.GlobalID == oldAuth.GlobalID
		c.mu.Unlock()
		if !unchanged {
			t.Fatal("renewal changed the client or MGR identity")
		}
		if renewed {
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
		if err != nil || attempts.Load() != 2 || commands.Load() != 1 {
			t.Fatal("renewed authorizer did not admit exactly one mutation", err, attempts.Load(), commands.Load())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestManagerSuccessfulObsoleteSetupDiscardsEarlierEndpointFailures(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	started, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	var commands, oldCommands, failedEndpoints atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		switch {
		case strings.HasSuffix(endpoint, ":6799"):
			failedEndpoints.Add(1)
			return nil, io.EOF
		case strings.HasSuffix(endpoint, ":6800"):
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			client, peer := net.Pipe()
			go mockDaemon(peer, peerConfig{role: 16, id: 99, command: func(msgr.MessageData) { oldCommands.Add(1) }})
			return client, nil
		case strings.HasSuffix(endpoint, ":6801"):
			client, peer := net.Pipe()
			go mockDaemon(peer, peerConfig{role: 16, id: 100, command: func(msgr.MessageData) { commands.Add(1) }})
			return client, nil
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
	for {
		c.mu.Lock()
		if c.mgrMap.Available {
			c.mgrMap.Addresses = append([]msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("127.0.0.1:6799")}}, c.mgrMap.Addresses...)
			c.mu.Unlock()
			break
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	finished := make(chan error, 1)
	go func() {
		_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"switch"}`)}); err != nil {
		t.Fatal(err)
	}
	release <- struct{}{}
	select {
	case err := <-finished:
		if err != nil || commands.Load() != 1 || oldCommands.Load() != 0 || failedEndpoints.Load() != 1 {
			t.Fatal("obsolete successful setup returned earlier failure or submitted to the old MGR", err, commands.Load(), oldCommands.Load(), failedEndpoints.Load())
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestManagerSetupFailureUsesTheCurrentMapBeforeSubmittingMutation(t *testing.T) {
	for _, failure := range []string{"dial", "handshake", "old authorizer"} {
		t.Run(failure, func(t *testing.T) {
			options := mockOptions(t, 20, [16]byte{1})
			dial := options.DialContext
			started, release := make(chan struct{}), make(chan struct{}, 1)
			defer close(release)
			var oldAttempts, newAttempts, commands atomic.Int32
			options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
				if strings.HasSuffix(endpoint, ":6800") {
					if oldAttempts.Add(1) == 1 {
						close(started)
					}
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					switch failure {
					case "handshake":
						client, peer := net.Pipe()
						peer.Close()
						return client, nil
					case "old authorizer":
						return nil, &AuthenticationError{Method: 2, Code: -13}
					default:
						return nil, io.EOF
					}
				}
				if strings.HasSuffix(endpoint, ":6801") {
					newAttempts.Add(1)
					client, peer := net.Pipe()
					go mockDaemon(peer, peerConfig{role: 16, id: 100, command: func(msgr.MessageData) { commands.Add(1) }})
					return client, nil
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
			go func() {
				_, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
				finished <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// The synthetic MON sends a MgrMap message before its
			// command reply, while the earlier MGR setup remains blocked.
			if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"switch"}`)}); err != nil {
				t.Fatal(err)
			}
			release <- struct{}{}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal("obsolete MGR setup failure defeated the current map", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if oldAttempts.Load() != 1 || newAttempts.Load() != 1 || commands.Load() != 1 {
				t.Fatal("mutation was not submitted exactly once to the current MGR", oldAttempts.Load(), newAttempts.Load(), commands.Load())
			}
		})
	}
}

func TestManagerCurrentSetupFailureIsNotRetried(t *testing.T) {
	options := mockOptions(t, 20, [16]byte{1})
	dial := options.DialContext
	rejection := &AuthenticationError{Method: 2, Code: -13}
	var attempts atomic.Int32
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if strings.HasSuffix(endpoint, ":6800") {
			attempts.Add(1)
			return nil, rejection
		}
		return dial(ctx, network, endpoint)
	}
	c, err := Dial(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.MgrCommand(context.Background(), Command{JSON: []byte(`{"prefix":"test mutation"}`)})
	var unknown *OutcomeUnknownError
	if !errors.Is(err, rejection) || errors.As(err, &unknown) || attempts.Load() != 1 {
		t.Fatal("current authentication rejection was retried or lost its known outcome", err, attempts.Load())
	}
}

func TestManagerSetupCancellationAndCloseRemainKnown(t *testing.T) {
	for _, closing := range []bool{false, true} {
		options := mockOptions(t, 20, [16]byte{1})
		dial := options.DialContext
		started := make(chan struct{})
		var attempts atomic.Int32
		options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			if strings.HasSuffix(endpoint, ":6800") {
				if attempts.Add(1) == 1 {
					close(started)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			if strings.HasSuffix(endpoint, ":6801") {
				attempts.Add(1)
			}
			return dial(ctx, network, endpoint)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)
		c, err := Dial(ctx, options)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		call, stop := context.WithCancel(ctx)
		t.Cleanup(stop)
		finished := make(chan error, 1)
		go func() {
			_, err := c.MgrCommand(call, Command{JSON: []byte(`{"prefix":"test mutation"}`)})
			finished <- err
		}()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if _, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"switch"}`)}); err != nil {
			t.Fatal(err)
		}
		want := error(context.Canceled)
		if closing {
			want = ErrClosed
			c.Close()
		} else {
			stop()
		}
		select {
		case err := <-finished:
			var unknown *OutcomeUnknownError
			if !errors.Is(err, want) || errors.As(err, &unknown) || attempts.Load() != 1 {
				t.Errorf("canceled setup started another attempt or lost its known outcome: closing=%v attempts=%d err=%v", closing, attempts.Load(), err)
			}
		case <-ctx.Done():
			t.Error(ctx.Err())
		}
		stop()
		c.Close()
		cancel()
	}
}

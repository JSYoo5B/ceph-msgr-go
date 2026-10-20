package cephmsgr

import (
	"context"
	"testing"
)

func TestPublishedAuthenticationRejectionPreservesRegisteredStreams(t *testing.T) {
	f := newStreamCloseFixture(t)
	cause := &AuthenticationError{Method: 2, Code: -13}
	rejected := make(chan struct{})
	go func() { f.client.rejectAuthentication(cause); close(rejected) }()
	awaitCleanup(t, f.ctx, f.held.closing)
	// Source cleanup and both workers are still held. The same publication
	// boundary must expose the rejection and terminate both registered watches.
	f.client.mu.Lock()
	published := f.client.authErr == cause
	configCause, logCause := f.config.terminal, f.logs.terminal
	configActive, logActive := f.client.configWatch != nil, f.client.logWatch != nil
	f.client.mu.Unlock()
	if !published || configCause != cause || logCause != cause || configActive || logActive {
		t.Error("auth rejection was published without its registered stream causes", published, configCause, logCause, configActive, logActive)
	}
	state := f.client.Snapshot()
	if state.AuthRejection == nil || state.AuthRejection.Method != 2 || state.AuthRejection.Code != -13 || state.Monitor.Ready || state.Manager.Ready {
		t.Fatal("published auth rejection lost server code or readiness", state)
	}
	closed := make(chan error, 2)
	go func() { closed <- f.config.Close() }()
	go func() { closed <- f.logs.Close() }()
	awaitCleanup(t, f.ctx, f.configGate.Context.Done())
	awaitCleanup(t, f.ctx, f.logGate.Context.Done())
	clientClosed := make(chan error, 1)
	go func() { clientClosed <- f.client.Close() }()
	awaitCleanup(t, f.ctx, f.client.ctx.Done())
	f.configGate.unblock()
	f.logGate.unblock()
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-f.ctx.Done():
			t.Fatal("Stream.Close did not join after auth rejection", f.ctx.Err())
		}
	}
	f.finish()
	awaitCleanup(t, f.ctx, rejected)
	select {
	case err := <-clientClosed:
		if err != nil {
			t.Fatal(err)
		}
	case <-f.ctx.Done():
		t.Fatal("Client.Close did not join rejected source cleanup", f.ctx.Err())
	}
	f.drain(t, cause)
}

func TestPublishedAuthenticationRejectionPreservesEarlierStreamTerminals(t *testing.T) {
	for _, mode := range []string{"overflow", "lifetime cancellation", "stream close", "client close"} {
		t.Run(mode, func(t *testing.T) {
			f := newStreamCloseFixture(t)
			configCause, logCause := error(ErrConfigStreamClosed), error(ErrLogStreamClosed)
			var clientClosed chan error
			switch mode {
			case "overflow":
				configCause, logCause = ErrConfigOverflow, ErrLogOverflow
				f.config.stop(configCause)
				f.logs.stop(logCause)
			case "lifetime cancellation":
				configCause, logCause = context.Canceled, context.Canceled
				f.config.cancel()
				f.logs.cancel()
			case "stream close":
				go f.config.Close()
				go f.logs.Close()
			case "client close":
				configCause, logCause = ErrClosed, ErrClosed
				clientClosed = make(chan error, 1)
				go func() { clientClosed <- f.client.Close() }()
				awaitCleanup(t, f.ctx, f.held.closing)
			}
			awaitCleanup(t, f.ctx, f.configGate.Context.Done())
			awaitCleanup(t, f.ctx, f.logGate.Context.Done())
			f.configGate.unblock()
			f.logGate.unblock()
			awaitCleanup(t, f.ctx, f.config.done)
			awaitCleanup(t, f.ctx, f.logs.done)
			// The terminal watch slots have already been released. A later
			// auth rejection must not replace either watch's earlier cause.
			cause := &AuthenticationError{Method: 2, Code: -13}
			rejected := make(chan struct{})
			go func() { f.client.rejectAuthentication(cause); close(rejected) }()
			if mode != "client close" {
				awaitCleanup(t, f.ctx, f.held.closing)
			}
			f.finish()
			awaitCleanup(t, f.ctx, rejected)
			if clientClosed != nil {
				select {
				case err := <-clientClosed:
					if err != nil {
						t.Fatal(err)
					}
				case <-f.ctx.Done():
					t.Fatal("Client.Close did not join earlier shutdown", f.ctx.Err())
				}
			}
			if err := f.client.Close(); err != nil {
				t.Fatal(err)
			}
			f.drainCauses(t, configCause, logCause)
		})
	}
}

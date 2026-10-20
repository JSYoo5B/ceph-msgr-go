package cephmsgr

import (
	"context"
	"testing"
)

func TestDigestWatchClosePreservesSourceAndAuthenticationCauses(t *testing.T) {
	for _, mode := range []string{"stream", "client", "auth rejection"} {
		t.Run(mode, func(t *testing.T) {
			f := newStreamCloseFixture(t)
			base, cancel := context.WithCancel(context.Background())
			gate := &logTestLoopGate{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
			gate.armed.Store(true)
			stream := &DigestStream{client: f.client, ctx: gate, cancel: func() { f.client.Snapshot(); cancel() }, done: make(chan struct{}), source: f.source, observed: f.source, pending: &ClusterDigest{MonStatus: []byte("accepted"), Health: []byte("before failure")}, changed: make(chan struct{}), limit: 1024}
			f.client.mu.Lock()
			f.client.digestWatch = stream
			f.client.wg.Add(1)
			f.client.mu.Unlock()
			go stream.run()
			t.Cleanup(func() { gate.unblock(); stream.Close() })
			awaitCleanup(t, f.ctx, gate.entered)
			var cause error = &streamCloseCause{client: f.client, cause: ErrMalformedMessage}
			failed := make(chan struct{})
			if mode == "auth rejection" {
				cause = &AuthenticationError{Method: 2, Code: -13}
				go func() { f.client.rejectAuthentication(cause); close(failed) }()
			} else {
				go func() { f.source.Fail(cause); close(failed) }()
			}
			awaitCleanup(t, f.ctx, f.held.closing)
			// Session.Err is terminal but its Conn.Close/onClose and this watch
			// worker are held. Public Close must preserve the registered cause.
			closed := make(chan error, 1)
			if mode == "client" {
				go func() { closed <- f.client.Close() }()
			} else {
				go func() { closed <- stream.Close() }()
			}
			awaitCleanup(t, f.ctx, base.Done())
			f.client.mu.Lock()
			terminal, active := stream.terminal, f.client.digestWatch != nil
			f.client.mu.Unlock()
			if terminal != cause || active {
				t.Fatal("Close replaced the source/auth cause or retained the watch slot", terminal, active)
			}
			select {
			case <-closed:
				t.Fatal("Close returned before the digest worker joined")
			default:
			}
			gate.unblock()
			f.finish()
			awaitCleanup(t, f.ctx, failed)
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-f.ctx.Done():
				t.Fatal("Close did not join cleanup", f.ctx.Err())
			}
			awaitCleanup(t, f.ctx, stream.done)
			digestTestNext(t, f.ctx, stream, "accepted", "before failure")
			digestTestTerminal(t, f.ctx, stream, cause)
			if wrapped, ok := cause.(*streamCloseCause); ok && wrapped.inspected.Load() == 0 {
				t.Fatal("source cause was not inspected outside the client lock")
			}
		})
	}
}

func TestDigestWatchConcurrentNextTransfersPairOnceAndCloseReleasesWaiters(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchDigest(context.Background(), DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	digestTestRegistration(t, ctx, p)
	p.send(t, ctx, digestTestMessage("single", "ownership"))
	logTestBarrier(t, ctx, c)
	type result struct {
		digest ClusterDigest
		err    error
	}
	results := make(chan result, 4)
	for range 4 {
		go func() { got, err := stream.Next(context.Background()); results <- result{got, err} }()
	}
	select {
	case got := <-results:
		if got.err != nil || string(got.digest.MonStatus) != "single" || string(got.digest.Health) != "ownership" {
			t.Fatal("first waiter did not get the accepted pair", got)
		}
	case <-ctx.Done():
		t.Fatal("concurrent Next did not get the accepted pair", ctx.Err())
	}
	c.Close()
	awaitCleanup(t, ctx, stream.done)
	for range 3 {
		select {
		case got := <-results:
			if len(got.digest.MonStatus) != 0 || len(got.digest.Health) != 0 || got.err != ErrClosed {
				t.Fatal("pair was delivered twice or Close did not release a waiter", got)
			}
		case <-ctx.Done():
			t.Fatal("Client.Close left a Next waiting", ctx.Err())
		}
	}
}

package cephmsgr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestDigestWatchRecoversAndReregistersOnRenewal(t *testing.T) {
	for _, mode := range []string{"transport", "renewal"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := configTestFixture(t)
			first := configTestNextPeer(t, ctx, peers)
			stream, err := c.WatchDigest(ctx, DigestOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			digestTestRegistration(t, ctx, first)
			first.send(t, ctx, digestTestMessage("before", "old health"))
			logTestBarrier(t, ctx, c)
			c.mu.Lock()
			previous := c.mon
			c.mu.Unlock()
			if mode == "transport" {
				first.conn.Close()
			} else if err := c.connectMonitor(ctx); err != nil {
				t.Fatal("renewal did not replace the MON source", err)
			}
			second := configTestNextPeer(t, ctx, peers)
			digestTestRegistration(t, ctx, second)
			if err := c.handleDigest(previous, msgr.MessageData{Type: 0x705, Version: 1, CompatVersion: 1, Front: []byte{0}}); err != nil {
				t.Fatal("previous source was decoded after recovery", err)
			}
			second.send(t, ctx, digestTestMessage("after", "fresh health"))
			logTestBarrier(t, ctx, c)
			digestTestNext(t, ctx, stream, "after", "fresh health")
			if state := c.Snapshot(); !state.Monitor.Ready || state.Manager.Ready || state.Manager.Endpoint != "" {
				t.Fatal("digest recovery opened MGR or lost admission", state)
			}
		})
	}
}

func TestDigestWatchRestoredMONReregistersAfterMissedCandidate(t *testing.T) {
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
	stream := &DigestStream{client: c, ctx: gate, cancel: cancel, done: make(chan struct{}), limit: uint64(c.options.MaxFrameSize), changed: make(chan struct{})}
	c.mu.Lock()
	c.digestWatch = stream
	c.wg.Add(1)
	old := c.mon
	c.mu.Unlock()
	go stream.run()
	t.Cleanup(func() { gate.unblock(); stream.Close() })
	digestTestRegistration(t, ctx, p)
	gate.armed.Store(true)
	c.mu.Lock()
	c.signal()
	c.mu.Unlock()
	awaitCleanup(t, ctx, gate.entered)
	if err := c.connectMonitor(context.WithValue(ctx, logTestOwnerKey{}, owner)); !errors.Is(err, maps.ErrRelease) {
		t.Fatal("candidate did not fail release admission", err)
	}
	c.mu.Lock()
	restored := c.mon == old && c.monReady
	c.mu.Unlock()
	if !restored || old.Err() != nil {
		t.Fatal("old admitted MON was not restored", restored, old.Err())
	}
	gate.unblock()
	digestTestRegistration(t, ctx, p)
	p.send(t, ctx, digestTestMessage("restored", "fresh"))
	digestTestNext(t, ctx, stream, "restored", "fresh")
}

func TestDigestWatchMalformedPairPreservesAcceptedDataAndCommandOutcome(t *testing.T) {
	c, ctx, peers := configTestFixture(t)
	p := configTestNextPeer(t, ctx, peers)
	stream, err := c.WatchDigest(ctx, DigestOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	digestTestRegistration(t, ctx, p)
	p.send(t, ctx, digestTestMessage("accepted", "before malformed"))
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
			t.Fatal("command did not reach the peer", ctx.Err())
		}
	}
received:
	message := digestTestMessage("truncated", "pair")
	message.Front = message.Front[:len(message.Front)-1]
	p.send(t, ctx, message)
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) {
			t.Fatal("malformed digest lost an already started command's cause", err)
		}
	case <-ctx.Done():
		t.Fatal("malformed digest left a started command pending", ctx.Err())
	}
	digestTestNext(t, ctx, stream, "accepted", "before malformed")
	digestTestTerminal(t, ctx, stream, ErrMalformedMessage)
	if err := c.WaitMonReady(ctx); err != nil {
		t.Fatal("ordinary MON recovery failed", err)
	}
	second := configTestNextPeer(t, ctx, peers)
	logTestBarrier(t, ctx, c)
	for len(second.commands) != 0 {
		if command := <-second.commands; strings.Contains(string(command.Front), "hold mutation") {
			t.Fatal("uncertain command was replayed during digest failure recovery")
		}
	}
}

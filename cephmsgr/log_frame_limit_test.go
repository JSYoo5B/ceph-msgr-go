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
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

func TestLogWatchFrameLimitPreservesCurrentAndStaleSourceCauses(t *testing.T) {
	for _, mode := range []string{"current source", "replacement before worker", "replacement before cleanup", "old source after replacement"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers, held, releaseClose := logFrameLimitFixture(t, mode)
			p := logTestNextPeer(t, ctx, peers)
			stream, gate := logFrameLimitWatch(t, c, ctx, mode != "current source")
			p.subscription(t, ctx, "log-info", 0)
			entry := logTestRawEntry("accepted before the frame limit")
			p.send(t, ctx, logTestMessage([16]byte{1}, 7, entry))
			logTestBarrier(t, ctx, c)
			c.mu.Lock()
			source := c.mon
			c.mu.Unlock()
			if gate != nil {
				gate.armed.Store(true)
				c.mu.Lock()
				c.signal()
				c.mu.Unlock()
				awaitCleanup(t, ctx, gate.entered)
			}
			finished := make(chan error, 1)
			go func() {
				_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
				finished <- err
			}()
			// Drain the earlier status barrier and require receipt of this exact
			// mutation before the peer injects its authenticated oversized frame.
			for received := false; !received; {
				select {
				case command := <-p.commands:
					received = strings.Contains(string(command.Front), "hold mutation")
				case <-ctx.Done():
					t.Fatal("mutation was not received", ctx.Err())
				}
			}
			var replacement *logTestPeer
			if mode == "old source after replacement" {
				if err := c.connectMonitor(ctx); err != nil {
					t.Fatal("healthy MON replacement failed", err)
				}
				replacement = logTestNextPeer(t, ctx, peers)
			}
			request := logTestSend{message: logTestMessage([16]byte{1}, 8, logTestRawEntry(strings.Repeat("x", 64<<10))), result: make(chan error, 1)}
			select {
			case p.sends <- request:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			awaitCleanup(t, ctx, p.done)
			if !errors.Is(source.Err(), wire.ErrLimit) {
				t.Fatal("secure Reader lost its frame-limit refusal", source.Err())
			}
			var unknown *OutcomeUnknownError
			if err := admissionResult(t, ctx, finished); !errors.As(err, &unknown) || !errors.Is(unknown.Cause, wire.ErrLimit) {
				t.Fatal("received mutation lost its raw refusal or unknown outcome", err)
			}
			if gate != nil && replacement == nil {
				if held != nil {
					awaitCleanup(t, ctx, held.closing)
				}
				if err := c.WaitMonReady(ctx); err != nil {
					t.Fatal("ordinary MON recovery failed", err)
				}
				replacement = logTestNextPeer(t, ctx, peers)
			}
			if replacement != nil {
				c.mu.Lock()
				replaced := c.mon != source && c.monReady
				c.mu.Unlock()
				if !replaced {
					t.Fatal("the replacement did not precede the worker")
				}
				gate.unblock()
				logTestBarrier(t, ctx, c)
				for len(replacement.commands) != 0 {
					if command := <-replacement.commands; strings.Contains(string(command.Front), "hold mutation") {
						t.Fatal("recovery replayed the uncertain mutation")
					}
				}
			}
			if held != nil {
				closed := make(chan error, 1)
				go func() { closed <- c.Close() }()
				waitClientState(t, c, ctx, func(state State) bool { return state.Closed })
				select {
				case err := <-closed:
					t.Fatal("Close returned before the failed source finished cleanup", err)
				case <-time.After(20 * time.Millisecond):
				}
				releaseClose()
				if err := admissionResult(t, ctx, closed); err != nil {
					t.Fatal(err)
				}
				awaitCleanup(t, ctx, held.cleaned)
				awaitCleanup(t, ctx, stream.done)
			}
			logTestBatch(t, ctx, stream, 7, entry)
			if mode != "old source after replacement" {
				bounded, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
				logTestTerminal(t, bounded, stream, wire.ErrLimit)
				return
			}
			// A source that became stale while still draining a command must not
			// terminate the new watch. Its accepted cursor still resumes at 8.
			replacement.subscription(t, ctx, "log-info", 8)
			continued := logTestRawEntry("new current source still streams")
			replacement.send(t, ctx, logTestMessage([16]byte{1}, 8, continued))
			logTestBarrier(t, ctx, c)
			logTestBatch(t, ctx, stream, 8, continued)
			c.mu.Lock()
			active := c.logWatch == stream && stream.terminal == nil
			c.mu.Unlock()
			if !active {
				t.Fatal("stale refusal released or terminated the replacement watch")
			}
			bounded, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			if _, err := stream.Next(bounded); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("stale source terminated the replacement watch", err)
			}
			if !c.Snapshot().Monitor.Ready {
				t.Fatal("stale refusal damaged main readiness")
			}
		})
	}
}

func logFrameLimitWatch(t *testing.T, c *Client, ctx context.Context, gated bool) (*LogStream, *logTestLoopGate) {
	t.Helper()
	if !gated {
		stream, err := c.WatchLogs(ctx, LogOptions{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { stream.Close() })
		return stream, nil
	}
	base, cancel := context.WithCancel(ctx)
	gate := &logTestLoopGate{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
	// Match WatchLogs ownership while controlling only the worker's scheduling.
	stream := &LogStream{client: c, ctx: gate, cancel: cancel, done: make(chan struct{}), level: "info", limit: uint64(c.options.MaxFrameSize), changed: make(chan struct{})}
	c.mu.Lock()
	c.logWatch = stream
	c.wg.Add(1)
	c.mu.Unlock()
	go stream.run()
	t.Cleanup(func() { gate.unblock(); stream.Close() })
	return stream, gate
}

// Arm the cleanup gate only after bootstrap succeeds, so fixture admission
// failure cannot strand Dial behind the gate the test has not received yet.
type logFrameLimitCloseConn struct {
	*managerCleanupConn
	armed atomic.Bool
}

func (c *logFrameLimitCloseConn) Close() error {
	if c.armed.Load() {
		return c.managerCleanupConn.Close()
	}
	return c.Conn.Close()
}

func logFrameLimitFixture(t *testing.T, mode string) (*Client, context.Context, <-chan *logTestPeer, *managerCleanupConn, func()) {
	t.Helper()
	if mode != "replacement before cleanup" {
		c, ctx, peers := logTestFixture(t, func(index int, p *logTestPeer) {
			if mode == "current source" && index != 0 {
				p.owner = make(chan struct{}) // Defer replacement until this cause is observed.
			}
		})
		return c, ctx, peers, nil, func() {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxFrameSize, options.MaxInFlight = 64<<10, 1
	dial := options.DialContext
	peers := make(chan *logTestPeer, 16)
	var mu sync.Mutex
	var all []*logTestPeer
	var held *managerCleanupConn
	var protected *logFrameLimitCloseConn
	release := make(chan struct{})
	var once sync.Once
	releaseClose := func() { once.Do(func() { close(release) }) }
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":3300") {
			return dial(ctx, network, endpoint)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		p := &logTestPeer{conn: server, sends: make(chan logTestSend), subscriptions: make(chan logTestSubscription, 8), commands: make(chan msgr.MessageData, 128), done: make(chan struct{}), release: 20}
		mu.Lock()
		initial := len(all) == 0
		all = append(all, p)
		if initial {
			held = &managerCleanupConn{Conn: client, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
			protected = &logFrameLimitCloseConn{managerCleanupConn: held}
		}
		mu.Unlock()
		peers <- p
		go p.serve(!initial)
		if initial {
			return protected, nil
		}
		return client, nil
	}
	var c *Client
	t.Cleanup(func() {
		releaseClose()
		cancel()
		if c != nil {
			c.Close()
		}
		mu.Lock()
		owned := append([]*logTestPeer(nil), all...)
		mu.Unlock()
		cleanup, finish := context.WithTimeout(context.Background(), 5*time.Second)
		defer finish()
		for _, p := range owned {
			p.conn.Close()
			awaitCleanup(t, cleanup, p.done)
		}
	})
	var err error
	c, err = Dial(ctx, options)
	if err != nil {
		t.Fatal("frame-limit fixture admission", err)
	}
	protected.armed.Store(true)
	return c, ctx, peers, held, releaseClose
}

package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/maps"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func TestLogWatchLifetimeCancellationDrainsAcceptedData(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	lifetime, cancel := context.WithCancel(ctx)
	stream, err := c.WatchLogs(lifetime, LogOptions{Level: LogWarn})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-warn", 0)
	entry := logTestRawEntry("accepted before cancellation")
	p.send(t, ctx, logTestMessage([16]byte{1}, 4, entry))
	logTestBarrier(t, ctx, c)
	cancel()
	select {
	case <-stream.done:
	case <-ctx.Done():
		t.Fatal("watch lifetime did not terminate its worker", ctx.Err())
	}
	logTestBatch(t, ctx, stream, 4, entry)
	logTestTerminal(t, ctx, stream, context.Canceled)
	logTestBarrier(t, ctx, c)
}

func TestLogWatchDedupAndRecoveryCursor(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	first := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{Level: LogError})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	first.subscription(t, ctx, "log-error", 0)
	accepted := logTestRawEntry("accepted but not consumed before recovery")
	for _, version := range []uint64{7, 7, 6, 9} {
		first.send(t, ctx, logTestMessage([16]byte{1}, version, accepted))
	}
	logTestBarrier(t, ctx, c)
	first.conn.Close()
	second := logTestNextPeer(t, ctx, peers)
	second.subscription(t, ctx, "log-error", 10) // Last queued, not last Next.
	logTestBatch(t, ctx, stream, 7, accepted)
	logTestBatch(t, ctx, stream, 9, accepted)
	second.send(t, ctx, logTestMessage([16]byte{1}, 9, logTestRawEntry("duplicate from replacement")))
	second.send(t, ctx, logTestMessage([16]byte{1}, 10, logTestRawEntry("new replacement batch")))
	logTestBarrier(t, ctx, c)
	logTestBatch(t, ctx, stream, 10, logTestRawEntry("new replacement batch"))
	wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := stream.Next(wait); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("duplicate/older service versions escaped dedup", err)
	}
	logTestBarrier(t, ctx, c)
}

func TestLogWatchOverflowIsTerminalAndDoesNotBlockCommands(t *testing.T) {
	for _, mode := range []string{"retained bytes", "hard batch count", "single oversized batch"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := logTestFixture(t)
			p := logTestNextPeer(t, ctx, peers)
			budget, accepted, text := uint32(4<<10), 1, strings.Repeat("x", 2<<10)
			if mode == "hard batch count" {
				budget, accepted, text = 64<<10, 64, ""
			} else if mode == "single oversized batch" {
				budget, accepted, text = 1024, 0, strings.Repeat("x", 4<<10)
			}
			stream, err := c.WatchLogs(ctx, LogOptions{MaxBufferedBytes: budget})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, "log-info", 0)
			for i := 1; i <= accepted+1; i++ {
				var entries []LogEntry
				if text != "" {
					entries = []LogEntry{logTestRawEntry(text)}
				}
				p.send(t, ctx, logTestMessage([16]byte{1}, uint64(i), entries...))
			}
			// A command reply is a receive-order barrier: overflow must not park
			// the sole session reader behind an application that has not called Next.
			logTestBarrier(t, ctx, c)
			for i := 1; i <= accepted; i++ {
				var entries []LogEntry
				if text != "" {
					entries = []LogEntry{logTestRawEntry(text)}
				}
				logTestBatch(t, ctx, stream, uint64(i), entries...)
			}
			logTestTerminal(t, ctx, stream, ErrLogOverflow)
			logTestBarrier(t, ctx, c)
		})
	}
}

func TestLogWatchSourceGuardsAndCursorOverflow(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-info", 0)
	bad := msgr.MessageData{Type: 52, Version: 2, CompatVersion: 2, Front: []byte{0}}
	if err := c.handleLog(new(session.Session), bad); err != nil {
		t.Fatal("retired source was decoded before identity guard", err)
	}
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	// Isolate the admission guard without racing the real recovery worker.
	unadmitted := &Client{mon: current, options: c.options}
	unadmitted.logWatch = &LogStream{client: unadmitted, source: current, changed: make(chan struct{}), cancel: func() {}}
	if err := unadmitted.handleLog(current, bad); err != nil {
		t.Fatal("unadmitted MON log was decoded", err)
	}
	unadmitted.monReady = true
	unadmitted.logWatch.source = new(session.Session)
	if err := unadmitted.handleLog(current, bad); err != nil {
		t.Fatal("watch belonging to another MON decoded this source", err)
	}
	p.send(t, ctx, logTestMessage([16]byte{1}, math.MaxUint64, logTestRawEntry("must not wrap cursor")))
	logTestBarrier(t, ctx, c)
	logTestTerminal(t, ctx, stream, ErrLogCursorOverflow)
	logTestBarrier(t, ctx, c)
}

func TestLogWatchMalformedPayloadFailsStartedCommand(t *testing.T) {
	for _, mode := range []string{"truncated", "unsupported compatibility", "wrong FSID", "unsupported entry compatibility"} {
		t.Run(mode, func(t *testing.T) {
			c, ctx, peers := logTestFixture(t)
			p := logTestNextPeer(t, ctx, peers)
			stream, err := c.WatchLogs(ctx, LogOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			p.subscription(t, ctx, "log-info", 0)
			result := make(chan error, 1)
			go func() {
				_, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
				result <- err
			}()
			select {
			case <-p.commands:
			case <-ctx.Done():
				t.Fatal("mutation did not reach peer", ctx.Err())
			}
			entry := logTestRawEntry("invalid payload")
			fsid := [16]byte{1}
			if mode == "wrong FSID" {
				fsid = [16]byte{2}
			}
			message := logTestMessage(fsid, 7, entry)
			if mode == "truncated" {
				message.Front = message.Front[:len(message.Front)-1]
			} else if mode == "unsupported compatibility" {
				message.Version, message.CompatVersion = 2, 2
			} else if mode == "unsupported entry compatibility" {
				// Paxos(18), FSID(16), count(4), then the entry's envelope.
				message.Front[38], message.Front[39] = 6, 6
			}
			p.send(t, ctx, message)
			select {
			case err := <-result:
				var unknown *OutcomeUnknownError
				if !errors.As(err, &unknown) || !errors.Is(err, ErrMalformedMessage) || !errors.Is(err, msgr.ErrFrame) {
					t.Fatal("malformed current MON log hid started command uncertainty/provenance", err)
				}
			case <-ctx.Done():
				t.Fatal("malformed log did not fail the pending command", ctx.Err())
			}
			logTestTerminal(t, ctx, stream, ErrMalformedMessage)
			if err := c.WaitMonReady(ctx); err != nil {
				t.Fatal("ordinary MON recovery after malformed log", err)
			}
			replacement := logTestNextPeer(t, ctx, peers)
			logTestBarrier(t, ctx, c)
			for {
				select {
				case command := <-replacement.commands:
					if strings.Contains(string(command.Front), "hold mutation") {
						t.Fatal("malformed log caused uncertain mutation replay")
					}
				default:
					return
				}
			}
		})
	}
}

func TestLogWatchClientCloseJoinsWorkerAndReleasesNext(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(context.Background(), LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p.subscription(t, ctx, "log-info", 0)
	waiting := make(chan error, 1)
	go func() { _, err := stream.Next(context.Background()); waiting <- err }()
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Client.Close did not join the log worker", ctx.Err())
	}
	select {
	case <-stream.done:
	default:
		t.Fatal("Client.Close returned with its log worker still running")
	}
	select {
	case err := <-waiting:
		if !errors.Is(err, ErrClosed) {
			t.Fatal("Client.Close lost the log terminal cause", err)
		}
	case <-ctx.Done():
		t.Fatal("Client.Close left Next waiting", ctx.Err())
	}
	logTestTerminal(t, ctx, stream, ErrClosed)
	if _, err := c.WatchLogs(ctx, LogOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatal("closed client admitted another log watch", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogWatchRejectsInvalidOptionsBeforeSubscription(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	for _, options := range []LogOptions{
		{Level: LogLevel(255)}, {MaxBufferedBytes: 1}, {MaxBufferedBytes: (1 << 30) + 1}, {StartVersion: math.MaxUint64},
	} {
		if stream, err := c.WatchLogs(ctx, options); err == nil {
			stream.Close()
			t.Fatal("invalid log options were accepted", options)
		} else if options.StartVersion == math.MaxUint64 && !errors.Is(err, ErrLogCursorOverflow) {
			t.Fatal("maximal caller cursor lost its explicit overflow error", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.WatchLogs(canceled, LogOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled watch was started", err)
	}
	select {
	case sub := <-p.subscriptions:
		t.Fatal("invalid/canceled watch changed the server subscription", fmt.Sprintf("%+v", sub))
	default:
	}
	logTestBarrier(t, ctx, c)
	valid, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal("invalid options retained the watch slot", err)
	}
	p.subscription(t, ctx, "log-info", 0)
	valid.Close()
}

func TestLogWatchClosedClientPrecedesOptionValidation(t *testing.T) {
	c, ctx, _ := logTestFixture(t)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, options := range []LogOptions{
		{}, {Level: LogLevel(255)}, {MaxBufferedBytes: 1},
		{MaxBufferedBytes: (1 << 30) + 1}, {StartVersion: math.MaxUint64},
	} {
		if stream, err := c.WatchLogs(ctx, options); stream != nil || !errors.Is(err, ErrClosed) {
			t.Fatal("closed client validated options before its lifetime", options, stream, err)
		}
		if stream, err := c.WatchLogs(canceled, options); stream != nil || !errors.Is(err, context.Canceled) {
			t.Fatal("closed client hid caller cancellation", options, stream, err)
		}
	}
}

func TestLogWatchInclusiveStartAndAcceptedRecoveryCursor(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	first := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{Level: LogDebug, StartVersion: 15})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	first.subscription(t, ctx, "log-debug", 15)
	for _, version := range []uint64{0, 14, 15, 15} {
		first.send(t, ctx, logTestMessage([16]byte{1}, version, logTestRawEntry("inclusive cursor")))
	}
	logTestBarrier(t, ctx, c)
	first.conn.Close()
	second := logTestNextPeer(t, ctx, peers)
	second.subscription(t, ctx, "log-debug", 16)
	logTestBatch(t, ctx, stream, 15, logTestRawEntry("inclusive cursor"))
	second.send(t, ctx, logTestMessage([16]byte{1}, 16, logTestRawEntry("continued cursor")))
	logTestBarrier(t, ctx, c)
	logTestBatch(t, ctx, stream, 16, logTestRawEntry("continued cursor"))
}

func TestLogWatchSubscribeAckDoesNotEstablishPermission(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-info", 0)
	// Pinned MMonSubscribeAck.h encodes interval U32 and FSID16. Tentacle
	// normally suppresses this ACK for our stateful MON feature; this tests
	// only that receiving one cannot manufacture a permission/delivery claim.
	front := logTestU32(nil, 30)
	fsid := [16]byte{1}
	front = append(front, fsid[:]...)
	p.send(t, ctx, msgr.MessageData{Type: msgr.SubscribeAckMessage, Version: 1, Front: front})
	logTestBarrier(t, ctx, c)
	wait, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if batch, err := stream.Next(wait); !errors.Is(err, context.DeadlineExceeded) || len(batch.Entries) != 0 || batch.Version != 0 {
		t.Fatal("subscription ACK manufactured log delivery/permission", batch, err)
	}
	p.send(t, ctx, logTestMessage(fsid, 6, logTestRawEntry("actual delivered log")))
	logTestBatch(t, ctx, stream, 6, logTestRawEntry("actual delivered log"))
	logTestBarrier(t, ctx, c)
}

func TestLogWatchPreservesServerGapAndUnknownPriorities(t *testing.T) {
	c, ctx, peers := logTestFixture(t)
	p := logTestNextPeer(t, ctx, peers)
	stream, err := c.WatchLogs(ctx, LogOptions{Level: LogError})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-error", 0)
	// Tentacle LogMonitor can insert a WARN trim notice even for log-error.
	// It has default entity/address/time fields. Text does not authenticate a
	// gap classification, and Level cannot justify dropping any delivered entry.
	gap := LogEntry{Priority: 3, Message: "skipped log messages from 2 to 8"}
	otherWarning := logTestRawEntry("an unrelated warning with the same priority")
	otherWarning.Priority = 3
	unknown := logTestRawEntry("future priority preserved\x00\xff")
	p.send(t, ctx, logTestMessage([16]byte{1}, 8, gap, otherWarning, unknown))
	logTestBarrier(t, ctx, c)
	logTestBatch(t, ctx, stream, 8, gap, otherWarning, unknown)
}

type logTestLoopGate struct {
	context.Context
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *logTestLoopGate) Err() error {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
	return g.Context.Err()
}

func (g *logTestLoopGate) unblock() { g.once.Do(func() { close(g.release) }) }

func TestLogWatchRestoredMonitorReregistersWhenWorkerMissedCandidate(t *testing.T) {
	owner := make(chan struct{})
	c, ctx, peers := logTestFixture(t, func(index int, p *logTestPeer) {
		if index == 1 {
			p.release = 19 // Valid encoding, but refused by minimum-release policy.
		}
		if index != 0 {
			p.owner = owner // Keep private admission distinct from the supervisor.
		}
	})
	p := logTestNextPeer(t, ctx, peers)
	base, cancel := context.WithCancel(ctx)
	gate := &logTestLoopGate{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
	// Construct the same worker ownership as WatchLogs, with a caller-context
	// gate instead of a production hook. The worker cannot observe the temporary
	// candidate and restoration while paused at its next Err check.
	stream := &LogStream{client: c, ctx: gate, cancel: cancel, done: make(chan struct{}), level: "info", limit: uint64(c.options.MaxFrameSize), changed: make(chan struct{})}
	c.mu.Lock()
	c.logWatch = stream
	c.wg.Add(1)
	old := c.mon
	c.mu.Unlock()
	go stream.run()
	t.Cleanup(func() { gate.unblock(); stream.Close() })
	p.subscription(t, ctx, "log-info", 0)
	entry := logTestRawEntry("accepted before failed candidate")
	p.send(t, ctx, logTestMessage([16]byte{1}, 7, entry))
	logTestBarrier(t, ctx, c) // Initial registration has left the writer too.
	gate.armed.Store(true)
	c.mu.Lock()
	c.signal()
	c.mu.Unlock()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("log worker did not reach the controlled admission boundary", ctx.Err())
	}
	operation := context.WithValue(ctx, logTestOwnerKey{}, owner)
	if err := c.connectMonitor(operation); !errors.Is(err, maps.ErrRelease) {
		t.Fatal("candidate did not fail its explicit release admission", err)
	}
	c.mu.Lock()
	restored := c.mon == old && c.monReady
	c.mu.Unlock()
	if !restored || old.Err() != nil {
		t.Fatal("candidate failure did not preserve the healthy old MON", restored, old.Err())
	}
	gate.unblock()
	p.subscription(t, ctx, "log-info", 8)
	logTestBatch(t, ctx, stream, 7, entry)
	p.send(t, ctx, logTestMessage([16]byte{1}, 8, logTestRawEntry("restored monitor batch")))
	logTestBarrier(t, ctx, c)
	logTestBatch(t, ctx, stream, 8, logTestRawEntry("restored monitor batch"))
}

func TestLogWatchRegistrationDoesNotUseCommandSlot(t *testing.T) {
	c, ctx, peers := logTestFixture(t) // Exactly one command slot.
	p := logTestNextPeer(t, ctx, peers)
	mutation, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := c.MonCommand(mutation, Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
		finished <- err
	}()
	select {
	case <-p.commands:
	case <-ctx.Done():
		t.Fatal("mutation did not occupy the only command slot", ctx.Err())
	}
	stream, err := c.WatchLogs(ctx, LogOptions{})
	if err != nil {
		t.Fatal("full command capacity prevented log registration", err)
	}
	defer stream.Close()
	p.subscription(t, ctx, "log-info", 0)
	entry := logTestRawEntry("log beside pending mutation")
	p.send(t, ctx, logTestMessage([16]byte{1}, 3, entry))
	logTestBatch(t, ctx, stream, 3, entry)
	cancel()
	select {
	case err := <-finished:
		var unknown *OutcomeUnknownError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &unknown) {
			t.Fatal("log registration changed the held mutation's outcome", err)
		}
	case <-ctx.Done():
		t.Fatal("held mutation cancellation did not release its command slot", ctx.Err())
	}
	logTestBarrier(t, ctx, c)
}

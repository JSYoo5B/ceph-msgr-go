package cephmsgr

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type streamCloseCause struct {
	client    *Client
	cause     error
	inspected atomic.Uint32
}

func (e *streamCloseCause) Error() string { return "registered source failed before shutdown" }
func (e *streamCloseCause) Unwrap() error {
	e.inspected.Add(1)
	e.client.Snapshot()
	return e.cause
}

type streamCloseFixture struct {
	client              *Client
	ctx                 context.Context
	source              *session.Session
	held                *managerCleanupConn
	config              *ConfigStream
	logs                *LogStream
	configGate, logGate *logTestLoopGate
	finish              func()
}

func newStreamCloseFixture(t *testing.T) *streamCloseFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	base, clientCancel := context.WithCancel(context.Background())
	clientConn, peer := net.Pipe()
	release := make(chan struct{})
	held := &managerCleanupConn{Conn: clientConn, closing: make(chan struct{}), cleaned: make(chan struct{}), release: release}
	c := &Client{ctx: base, cancel: clientCancel, changed: make(chan struct{}), auth: &cephx.Client{Name: "client.unit", Tickets: make(map[uint32]cephx.Ticket)}, monReady: true, sessions: make(map[*session.Session]struct{})}
	var source *session.Session
	source = session.New(&session.Transport{Conn: held}, session.Config{}, nil, func(err error) {
		c.stopLogForFailedSession(source, err)
		c.stopConfigForFailedSession(source, err)
	})
	c.mon = source
	// Reproduce attach's cleanup ownership without a reader/writer: any
	// protocol worker may Fail a source before custom Conn.Close finishes.
	c.sessions[source] = struct{}{}
	c.wg.Add(1)
	go func() {
		<-source.Done()
		source.Wait()
		c.mu.Lock()
		delete(c.sessions, source)
		c.mu.Unlock()
		c.wg.Done()
	}()
	configBase, configCancel := context.WithCancel(context.Background())
	logBase, logCancel := context.WithCancel(context.Background())
	configGate := &logTestLoopGate{Context: configBase, entered: make(chan struct{}), release: make(chan struct{})}
	logGate := &logTestLoopGate{Context: logBase, entered: make(chan struct{}), release: make(chan struct{})}
	configGate.armed.Store(true)
	logGate.armed.Store(true)
	config := &ConfigStream{client: c, ctx: configGate, cancel: func() { c.Snapshot(); configCancel() }, done: make(chan struct{}), source: source, observed: source, pending: map[string]string{"accepted": "before source failure"}, changed: make(chan struct{}), limit: 1024}
	logs := &LogStream{client: c, ctx: logGate, cancel: func() { c.Snapshot(); logCancel() }, done: make(chan struct{}), source: source, observed: source, queue: []queuedLogBatch{{batch: LogBatch{Version: 7}, bytes: 512}}, bufferedBytes: 512, changed: make(chan struct{}), limit: 1024}
	c.configWatch, c.logWatch = config, logs
	c.wg.Add(2)
	go config.run()
	go logs.run()
	awaitCleanup(t, ctx, configGate.entered)
	awaitCleanup(t, ctx, logGate.entered)
	var once sync.Once
	finish := func() { once.Do(func() { configGate.unblock(); logGate.unblock(); close(release) }) }
	t.Cleanup(func() { finish(); c.Close(); peer.Close(); cancel() })
	return &streamCloseFixture{client: c, ctx: ctx, source: source, held: held, config: config, logs: logs, configGate: configGate, logGate: logGate, finish: finish}
}

func (f *streamCloseFixture) drain(t *testing.T, cause error) {
	f.drainCauses(t, cause, cause)
}

func (f *streamCloseFixture) drainCauses(t *testing.T, configCause, logCause error) {
	t.Helper()
	if config, err := f.config.Next(f.ctx); err != nil || config["accepted"] != "before source failure" {
		t.Error("shutdown discarded accepted config", config, err)
	}
	if batch, err := f.logs.Next(f.ctx); err != nil || batch.Version != 7 {
		t.Error("shutdown discarded accepted log batch", batch, err)
	}
	for range 2 {
		if config, err := f.config.Next(f.ctx); config != nil || err != configCause {
			t.Error("config lost stable first source cause", config, err, configCause)
		}
		if batch, err := f.logs.Next(f.ctx); batch.Version != 0 || err != logCause {
			t.Error("logs lost stable first source cause", batch, err, logCause)
		}
	}
	select {
	case <-f.config.done:
	default:
		t.Error("config worker was not joined")
	}
	select {
	case <-f.logs.done:
	default:
		t.Error("log worker was not joined")
	}
}

func (f *streamCloseFixture) replaceSource(t *testing.T, updateObserved bool) {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() { peer.Close() })
	replacement := session.New(&session.Transport{Conn: conn}, session.Config{}, nil, nil)
	f.client.mu.Lock()
	f.client.mon = replacement
	if updateObserved {
		f.config.source, f.config.observed = replacement, replacement
		f.logs.source, f.logs.observed = replacement, replacement
	}
	f.client.sessions[replacement] = struct{}{}
	f.client.wg.Add(1)
	f.client.mu.Unlock()
	go func() {
		<-replacement.Done()
		replacement.Wait()
		f.client.mu.Lock()
		delete(f.client.sessions, replacement)
		f.client.mu.Unlock()
		f.client.wg.Done()
	}()
}

func TestClientClosePreservesRegisteredStreamSourceCause(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
	}{
		{"malformed frame", ErrMalformedMessage},
		{"logical frame limit", wire.ErrLimit},
		{"authentication", &AuthenticationError{Method: 2, Code: -13}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newStreamCloseFixture(t)
			cause := &streamCloseCause{client: f.client, cause: test.cause}
			go f.source.Fail(cause)
			awaitCleanup(t, f.ctx, f.held.closing)
			if f.source.Err() != cause {
				t.Fatal("source failure was not recorded before shutdown")
			}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() { first <- f.client.Close() }()
			awaitCleanup(t, f.ctx, f.client.ctx.Done())
			go func() { second <- f.client.Close() }()
			for _, result := range []<-chan error{first, second} {
				select {
				case err := <-result:
					t.Fatal("Close returned before connection and stream cleanup", err)
				case <-time.After(time.Millisecond):
				}
			}
			// Let both workers observe shutdown before releasing onClose;
			// otherwise its fatal-source guard can hide the Close race.
			f.configGate.unblock()
			f.logGate.unblock()
			awaitCleanup(t, f.ctx, f.config.done)
			awaitCleanup(t, f.ctx, f.logs.done)
			f.finish()
			for _, result := range []<-chan error{first, second} {
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-f.ctx.Done():
					t.Fatal("Close failed to join cleanup", f.ctx.Err())
				}
			}
			if cause.inspected.Load() == 0 {
				t.Error("fatal custom error inspection was not exercised")
			}
			f.drain(t, cause)
		})
	}
}

func TestClientCloseIgnoresAlreadyReplacedStreamSourceCause(t *testing.T) {
	f := newStreamCloseFixture(t)
	cause := &streamCloseCause{client: f.client, cause: ErrMalformedMessage}
	f.replaceSource(t, true)
	go f.source.Fail(cause)
	awaitCleanup(t, f.ctx, f.held.closing)
	closed := make(chan error, 1)
	go func() { closed <- f.client.Close() }()
	awaitCleanup(t, f.ctx, f.client.ctx.Done())
	f.configGate.unblock()
	f.logGate.unblock()
	awaitCleanup(t, f.ctx, f.config.done)
	awaitCleanup(t, f.ctx, f.logs.done)
	f.finish()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-f.ctx.Done():
		t.Fatal("Close failed to join replaced source", f.ctx.Err())
	}
	f.drain(t, ErrClosed)
	if !errors.Is(f.source.Err(), ErrMalformedMessage) {
		t.Fatal("stale source's own error was lost")
	}
}

func TestStreamClosePreservesRecordedSourceCauseAndJoinsWorker(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		fatal bool
	}{
		{"malformed frame", ErrMalformedMessage, true},
		{"logical frame limit", wire.ErrLimit, true},
		{"authentication", &AuthenticationError{Method: 2, Code: -13}, true},
		{"ordinary transport", io.EOF, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newStreamCloseFixture(t)
			cause := &streamCloseCause{client: f.client, cause: test.cause}
			go f.source.Fail(cause)
			awaitCleanup(t, f.ctx, f.held.closing)
			if f.source.Err() != cause {
				t.Fatal("source failure was not recorded before stream shutdown")
			}
			closed := make(chan error, 4)
			for range 2 {
				go func() { closed <- f.config.Close() }()
				go func() { closed <- f.logs.Close() }()
			}
			awaitCleanup(t, f.ctx, f.configGate.Context.Done())
			awaitCleanup(t, f.ctx, f.logGate.Context.Done())
			select {
			case err := <-closed:
				t.Fatal("Stream.Close returned before its worker joined", err)
			case <-time.After(time.Millisecond):
			}
			clientClosed := make(chan error, 1)
			go func() { clientClosed <- f.client.Close() }()
			awaitCleanup(t, f.ctx, f.client.ctx.Done())
			f.configGate.unblock()
			f.logGate.unblock()
			for range 4 {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-f.ctx.Done():
					t.Fatal("Stream.Close failed to join worker", f.ctx.Err())
				}
			}
			// A stream owns its worker, while the client still owns shared
			// transport cleanup. Neither watch may join or release that socket.
			select {
			case err := <-clientClosed:
				t.Fatal("Client.Close returned before shared connection cleanup", err)
			case <-time.After(time.Millisecond):
			}
			f.finish()
			select {
			case err := <-clientClosed:
				if err != nil {
					t.Fatal(err)
				}
			case <-f.ctx.Done():
				t.Fatal("Client.Close failed to join transport cleanup", f.ctx.Err())
			}
			if cause.inspected.Load() == 0 {
				t.Error("reentrant fatal-error inspection was not exercised")
			}
			if test.fatal {
				f.drain(t, cause)
			} else {
				f.drainCauses(t, ErrConfigStreamClosed, ErrLogStreamClosed)
			}
		})
	}
}

func TestStreamCloseIgnoresAlreadyReplacedSource(t *testing.T) {
	f := newStreamCloseFixture(t)
	cause := &streamCloseCause{client: f.client, cause: ErrMalformedMessage}
	f.replaceSource(t, false) // The worker has not observed this new current MON.
	go f.source.Fail(cause)
	awaitCleanup(t, f.ctx, f.held.closing)
	closed := make(chan error, 2)
	go func() { closed <- f.config.Close() }()
	go func() { closed <- f.logs.Close() }()
	awaitCleanup(t, f.ctx, f.configGate.Context.Done())
	awaitCleanup(t, f.ctx, f.logGate.Context.Done())
	f.configGate.unblock()
	f.logGate.unblock()
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-f.ctx.Done():
			t.Fatal("Stream.Close failed to join stale-source worker", f.ctx.Err())
		}
	}
	f.finish()
	if err := f.client.Close(); err != nil {
		t.Fatal(err)
	}
	f.drainCauses(t, ErrConfigStreamClosed, ErrLogStreamClosed)
}

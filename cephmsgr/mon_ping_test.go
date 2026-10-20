package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// The private peer uses the native MPing envelope directly. It deliberately
// does not use the product ping encoder/decoder to create or inspect replies.
type monPingPeer struct {
	conn     net.Conn
	requests chan msgr.MessageData
	sends    chan namedTestSend
	done     chan struct{}
	errors   chan error
	initial  chan [2]uint64
	mapReply msgr.MessageData
	reply    func(msgr.MessageData) *msgr.MessageData
	stall    bool
}

func (p *monPingPeer) serve() {
	defer close(p.done)
	defer p.conn.Close()
	if p.stall {
		io.Copy(io.Discard, p.conn)
		return
	}
	p.conn.SetDeadline(time.Now().Add(4 * time.Second))
	trace := &namedTestTraceConn{Conn: p.conn, active: true}
	cfg := peerConfig{role: 1, id: 1, clientID: 84}
	cfg.payload = func(tag msgr.Tag, raw []byte) []byte {
		if tag != msgr.ServerIdent {
			return raw
		}
		out := namedTestAddresses([]msgr.Address{namedTestAddressB()})
		for _, value := range []uint64{1, 1, session.Features, session.RequiredFeatures, 1, 0} {
			out = binary.LittleEndian.AppendUint64(out, value)
		}
		return out
	}
	r, w, err := mockAuthenticate(trace, cfg)
	trace.active = false
	if err != nil {
		p.errors <- err
		return
	}
	initial, err := namedTestInitialID(trace.trace.Bytes())
	if err != nil {
		p.errors <- err
		return
	}
	clientID, err := namedTestClientID(trace.trace.Bytes())
	if err != nil {
		p.errors <- err
		return
	}
	p.initial <- [2]uint64{initial, clientID}
	type incoming struct {
		frame msgr.Frame
		err   error
	}
	frames, abort, drained := make(chan incoming), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(drained)
		for {
			frame, err := r.Read()
			select {
			case frames <- incoming{frame, err}:
			case <-abort:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { close(abort); p.conn.Close(); <-drained }()
	var sequence uint64
	send := func(m msgr.MessageData) error {
		sequence++
		m.Sequence, m.Priority = sequence, 127
		return w.Write(m.Frame())
	}
	for {
		select {
		case request := <-p.sends:
			err := send(request.message)
			request.result <- err
			if err != nil {
				return
			}
		case got := <-frames:
			if got.err != nil {
				return
			}
			if got.frame.Tag != msgr.Message {
				continue
			}
			m, err := msgr.DecodeMessage(got.frame)
			if err != nil {
				p.errors <- err
				return
			}
			p.requests <- m
			if m.Type == 5 {
				if send(p.mapReply) != nil {
					return
				}
				continue
			}
			if m.Type != 2 {
				p.errors <- fmt.Errorf("private ping connection sent type %d", m.Type)
				return
			}
			if p.reply != nil {
				if reply := p.reply(m); reply != nil && send(*reply) != nil {
					return
				}
			}
		}
	}
}

func (p *monPingPeer) next(t *testing.T, ctx context.Context, typ uint16) msgr.MessageData {
	t.Helper()
	for {
		select {
		case m := <-p.requests:
			if m.Type == typ {
				return m
			}
		case err := <-p.errors:
			t.Fatal("ping peer failed", err)
		case <-ctx.Done():
			t.Fatal("ping peer did not receive request", typ, ctx.Err())
		}
	}
}

func (p *monPingPeer) send(t *testing.T, ctx context.Context, m msgr.MessageData) {
	t.Helper()
	request := namedTestSend{message: m, result: make(chan error, 1)}
	select {
	case p.sends <- request:
	case <-p.done:
		t.Fatal("ping peer closed before delivery")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-request.result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type monPingFixture struct {
	*namedTestFixture
	pingTargets chan *monPingPeer
	pingMu      sync.Mutex
	pingPeers   []*monPingPeer
}

func monPingFixtureFor(t *testing.T, maxInFlight int, configure func(*monPingPeer)) *monPingFixture {
	t.Helper()
	f := &monPingFixture{namedTestFixture: &namedTestFixture{}, pingTargets: make(chan *monPingPeer, 16)}
	members := []namedTestMember{
		{"a", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}},
		{"b", []msgr.Address{namedTestAddressB()}},
	}
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxInFlight = maxInFlight
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, peer := net.Pipe()
		switch endpoint {
		case "192.0.2.1:6800":
			f.mgrDials.Add(1)
			go mockDaemon(peer, peerConfig{role: 16, id: 99, fsid: [16]byte{1}, clientID: 42})
		case "192.0.2.1:3300":
			f.monDials.Add(1)
			p := &namedTestPeer{conn: peer, client: client, initial: true, globalID: 42,
				mapMessage: msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: namedTestMap([16]byte{1}, 20, members)},
				requests:   make(chan msgr.MessageData, 32), sends: make(chan namedTestSend), done: make(chan struct{}), initialID: make(chan uint64, 1), errors: make(chan error, 2)}
			f.primary = p
			go p.serve()
		case "192.0.2.2:3301":
			f.namedDials.Add(1)
			p := &monPingPeer{conn: peer, requests: make(chan msgr.MessageData, 16), sends: make(chan namedTestSend),
				done: make(chan struct{}), errors: make(chan error, 2), initial: make(chan [2]uint64, 1),
				mapReply: msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: namedTestMap([16]byte{1}, 20, members)}}
			if configure != nil {
				configure(p)
			}
			f.pingMu.Lock()
			f.pingPeers = append(f.pingPeers, p)
			f.pingMu.Unlock()
			f.pingTargets <- p
			go p.serve()
		default:
			client.Close()
			peer.Close()
			return nil, fmt.Errorf("unexpected ping fixture endpoint %q", endpoint)
		}
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	f.ctx = ctx
	t.Cleanup(cancel)
	c, err := Dial(ctx, options)
	f.c = c
	t.Cleanup(func() {
		if c != nil {
			c.Close()
		}
		if f.primary != nil {
			f.primary.conn.Close()
			monPingWaitDone(t, f.ctx, f.primary.done)
		}
		f.pingMu.Lock()
		peers := append([]*monPingPeer(nil), f.pingPeers...)
		f.pingMu.Unlock()
		for _, p := range peers {
			p.conn.Close()
			monPingWaitDone(t, f.ctx, p.done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WaitMgrReady(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *monPingFixture) target(t *testing.T) *monPingPeer {
	t.Helper()
	select {
	case p := <-f.pingTargets:
		return p
	case <-f.ctx.Done():
		t.Fatal("no ping target was dialed", f.ctx.Err())
		return nil
	}
}

func monPingWaitDone(t *testing.T, ctx context.Context, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Error("ping operation or peer outlived context", ctx.Err())
	}
}

func monPingReplyBytes(raw []byte) msgr.MessageData {
	// Monitor::handle_ping pre-fills the payload: native compat remains zero.
	return msgr.MessageData{Type: 2, Version: 1, Front: append(binary.LittleEndian.AppendUint32(nil, uint32(len(raw))), raw...)}
}

func TestMonPingRawReplyAndPrimaryIsolation(t *testing.T) {
	raw := []byte{0, 255, '\r', '\n', '{', '}'}
	f := monPingFixtureFor(t, 4, func(p *monPingPeer) {
		p.reply = func(msgr.MessageData) *msgr.MessageData { m := monPingReplyBytes(raw); return &m }
	})
	f.c.mu.Lock()
	primary, mgr, auth := f.c.mon, f.c.mgr, f.c.auth
	f.c.mu.Unlock()
	before := f.c.Snapshot()
	namedTestPrimaryHealthy(t, f.namedTestFixture)
	first := f.primary.next(t, f.ctx, msgr.MonCommandMessage)
	for range 2 {
		got, err := f.c.PingMonitor(f.ctx, "b")
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatal("ping interpreted or lost raw output", got, err)
		}
		p := f.target(t)
		m := p.next(t, f.ctx, 2)
		if m.Transaction != 0 || m.Version != 1 || m.CompatVersion != 1 || len(m.Front)+len(m.Middle)+len(m.Data) != 0 {
			t.Fatal("ping request differs from native empty TID-zero request", m)
		}
		select {
		case ids := <-p.initial:
			if ids != [2]uint64{0, 84} {
				t.Fatal("ping reused primary credentials", ids)
			}
		case <-f.ctx.Done():
			t.Fatal(f.ctx.Err())
		}
		monPingWaitDone(t, f.ctx, p.done)
	}
	namedTestPrimaryHealthy(t, f.namedTestFixture)
	last := f.primary.next(t, f.ctx, msgr.MonCommandMessage)
	f.c.mu.Lock()
	preserved := f.c.mon == primary && f.c.mgr == mgr && f.c.auth == auth
	f.c.mu.Unlock()
	if !preserved || !reflect.DeepEqual(before, f.c.Snapshot()) || last.Transaction != first.Transaction+1 || f.monDials.Load() != 1 || f.namedDials.Load() != 2 {
		t.Fatal("ping changed primary identity, connection or command TID", before, f.c.Snapshot(), first.Transaction, last.Transaction)
	}
}

func TestMonPingRejectsMalformedReply(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*msgr.MessageData)
	}{
		{"wrong-type", func(m *msgr.MessageData) { m.Type = 3 }},
		{"wrong-version", func(m *msgr.MessageData) { m.Version = 2 }},
		{"request-compat", func(m *msgr.MessageData) { m.CompatVersion = 1 }},
		{"wrong-compat", func(m *msgr.MessageData) { m.CompatVersion = 2 }},
		{"nonzero-tid", func(m *msgr.MessageData) { m.Transaction = 7 }},
		{"middle", func(m *msgr.MessageData) { m.Middle = []byte{0} }},
		{"data", func(m *msgr.MessageData) { m.Data = []byte{0} }},
		{"truncated", func(m *msgr.MessageData) { m.Front = m.Front[:4] }},
		{"trailing", func(m *msgr.MessageData) { m.Front = append(m.Front, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := monPingFixtureFor(t, 1, func(p *monPingPeer) {
				p.reply = func(msgr.MessageData) *msgr.MessageData {
					m := monPingReplyBytes([]byte("raw"))
					tc.edit(&m)
					return &m
				}
			})
			_, err := f.c.PingMonitor(f.ctx, "b")
			var unknown *OutcomeUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, msgr.ErrFrame) || f.namedDials.Load() != 1 {
				t.Fatal("invalid ping reply was accepted or retried", err, f.namedDials.Load())
			}
			namedTestPrimaryHealthy(t, f.namedTestFixture)
		})
	}
}

type monPingResult struct {
	raw []byte
	err error
}

func monPingStart(c *Client, ctx context.Context) <-chan monPingResult {
	done := make(chan monPingResult, 1)
	go func() { raw, err := c.PingMonitor(ctx, "b"); done <- monPingResult{raw, err} }()
	return done
}

func monPingResultFrom(t *testing.T, ctx context.Context, done <-chan monPingResult) monPingResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal("ping did not complete", ctx.Err())
		return monPingResult{}
	}
}

func TestMonPingSharedAdmissionAndCanceledWait(t *testing.T) {
	f := monPingFixtureFor(t, 1, nil)
	ctx, cancel := context.WithCancel(f.ctx)
	first := monPingStart(f.c, ctx)
	p := f.target(t)
	p.next(t, f.ctx, 2)
	blocked, stop := context.WithTimeout(f.ctx, 30*time.Millisecond)
	defer stop()
	_, err := f.c.PingMonitor(blocked, "b")
	var unknown *OutcomeUnknownError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) || f.namedDials.Load() != 1 {
		t.Fatal("waiting ping bypassed MaxInFlight or became uncertain", err, f.namedDials.Load())
	}
	// Ordinary commands use the same admission slot as the outstanding ping.
	blockedCommand, stopCommand := context.WithTimeout(f.ctx, 30*time.Millisecond)
	defer stopCommand()
	_, err = f.c.MonCommand(blockedCommand, Command{JSON: []byte(`{"prefix":"status"}`)})
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &unknown) {
		t.Fatal("command did not share ping admission", err)
	}
	cancel()
	result := monPingResultFrom(t, f.ctx, first)
	if !errors.Is(result.err, context.Canceled) || !errors.As(result.err, &unknown) {
		t.Fatal("sent ping cancellation lost uncertainty", result.err)
	}
	monPingWaitDone(t, f.ctx, p.done)
	second := monPingStart(f.c, f.ctx)
	q := f.target(t)
	q.next(t, f.ctx, 2)
	q.send(t, f.ctx, monPingReplyBytes([]byte("second connection")))
	result = monPingResultFrom(t, f.ctx, second)
	if result.err != nil || string(result.raw) != "second connection" || f.namedDials.Load() != 2 {
		t.Fatal("new ping reused an earlier response or leaked admission", result, f.namedDials.Load())
	}
	namedTestPrimaryHealthy(t, f.namedTestFixture)
}

func TestMonPingConcurrentConnectionsAndSourceLoss(t *testing.T) {
	f := monPingFixtureFor(t, 2, nil)
	first := monPingStart(f.c, f.ctx)
	p := f.target(t)
	p.next(t, f.ctx, 2)
	second := monPingStart(f.c, f.ctx)
	q := f.target(t)
	q.next(t, f.ctx, 2)
	// Reply in the opposite order. Both responses have TID zero, so source
	// isolation, rather than the shared session transaction table, matters.
	q.send(t, f.ctx, monPingReplyBytes(nil))
	result := monPingResultFrom(t, f.ctx, second)
	if result.err != nil || len(result.raw) != 0 {
		t.Fatal("empty raw ping reply was not accepted", result)
	}
	select {
	case result := <-first:
		t.Fatal("another connection's TID-zero reply completed this ping", result)
	default:
	}
	p.conn.Close()
	result = monPingResultFrom(t, f.ctx, first)
	var unknown *OutcomeUnknownError
	if !errors.As(result.err, &unknown) || f.namedDials.Load() != 2 {
		t.Fatal("source loss was successful or automatically retried", result.err, f.namedDials.Load())
	}
	monPingWaitDone(t, f.ctx, p.done)
	monPingWaitDone(t, f.ctx, q.done)
	namedTestPrimaryHealthy(t, f.namedTestFixture)
}

func TestMonPingCloseJoinsSetupAndPendingReply(t *testing.T) {
	for _, setup := range []bool{true, false} {
		t.Run(fmt.Sprintf("setup=%v", setup), func(t *testing.T) {
			f := monPingFixtureFor(t, 1, func(p *monPingPeer) { p.stall = setup })
			done := monPingStart(f.c, f.ctx)
			p := f.target(t)
			if !setup {
				p.next(t, f.ctx, 2)
			}
			closed := make(chan struct{})
			go func() { f.c.Close(); close(closed) }()
			monPingWaitDone(t, f.ctx, closed)
			result := monPingResultFrom(t, f.ctx, done)
			var unknown *OutcomeUnknownError
			if !errors.Is(result.err, ErrClosed) || errors.As(result.err, &unknown) == setup {
				t.Fatal("Close lost setup/send classification", result.err)
			}
			monPingWaitDone(t, f.ctx, p.done)
			if len(f.c.calls) != 0 || f.namedDials.Load() != 1 {
				t.Fatal("Close left a ping slot or retried", len(f.c.calls), f.namedDials.Load())
			}
		})
	}
}

func TestMonPingRefusesMissingOrChangedTargetBeforeSend(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%v", changed), func(t *testing.T) {
			f := monPingFixtureFor(t, 1, func(p *monPingPeer) {
				p.mapReply.Front = namedTestMap([16]byte{2}, 20, []namedTestMember{{"b", []msgr.Address{namedTestAddressB()}}})
			})
			name := "mon.b"
			if changed {
				name = "b"
			}
			_, err := f.c.PingMonitor(f.ctx, name)
			var unknown *OutcomeUnknownError
			if err == nil || errors.As(err, &unknown) || changed && !errors.Is(err, msgr.ErrAuthentication) || !changed && !errors.Is(err, ErrMonitorNotFound) {
				t.Fatal("ping accepted an unadmitted target", err)
			}
			if changed {
				p := f.target(t)
				monPingWaitDone(t, f.ctx, p.done)
				for len(p.requests) != 0 {
					if m := <-p.requests; m.Type == 2 {
						t.Fatal("ping sent before independent target admission")
					}
				}
			} else if f.namedDials.Load() != 0 {
				t.Fatal("missing exact name opened a connection")
			}
			namedTestPrimaryHealthy(t, f.namedTestFixture)
		})
	}
}

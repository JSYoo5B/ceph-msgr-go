package cephmsgr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

type namedTestSend struct {
	message msgr.MessageData
	result  chan error
}

type namedTestPeer struct {
	conn       net.Conn
	client     net.Conn
	stallAuth  bool
	dropAuth   bool
	initial    bool
	mapMessage msgr.MessageData
	mapMissing bool
	globalID   uint64
	clientID   uint64
	address    msgr.Address
	requests   chan msgr.MessageData
	sends      chan namedTestSend
	done       chan struct{}
	initialID  chan uint64
	errors     chan error
	reply      func(*msgr.MessageData)
	tellCount  atomic.Uint32
}

func (p *namedTestPeer) serve() {
	defer close(p.done)
	defer p.conn.Close()
	if p.dropAuth {
		return
	}
	if p.stallAuth {
		io.Copy(io.Discard, p.conn)
		return
	}
	p.conn.SetDeadline(time.Now().Add(4 * time.Second))
	trace := &namedTestTraceConn{Conn: p.conn, active: true}
	cfg := peerConfig{role: 1, clientID: p.globalID}
	if !p.initial {
		cfg.id = 1
		cfg.payload = func(tag msgr.Tag, payload []byte) []byte {
			if tag != msgr.ServerIdent {
				return payload
			}
			// Independent exact SERVER_IDENT address vector. The feature words
			// are semantic constants; no product address encoder is used here.
			out := namedTestAddresses([]msgr.Address{p.address})
			for _, value := range []uint64{1, 1, session.Features, session.RequiredFeatures, 1, 0} {
				out = binary.LittleEndian.AppendUint64(out, value)
			}
			return out
		}
	}
	r, w, err := mockAuthenticate(trace, cfg)
	trace.active = false
	if err != nil {
		p.errors <- err
		return
	}
	id, err := namedTestInitialID(trace.trace.Bytes())
	if err != nil {
		p.errors <- err
		return
	}
	p.clientID, err = namedTestClientID(trace.trace.Bytes())
	if err != nil {
		p.errors <- err
		return
	}
	p.initialID <- id
	type incoming struct {
		frame msgr.Frame
		err   error
	}
	frames, abort, drained := make(chan incoming), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(drained)
		for {
			f, err := r.Read()
			select {
			case frames <- incoming{f, err}:
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
	if p.initial {
		if send(p.mapMessage) != nil {
			return
		}
		if send(msgr.MessageData{Type: msgr.MgrMapMessage, Version: 1, Front: mockMgrMap(1, 99, 6800)}) != nil {
			return
		}
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
				if !p.mapMissing && send(p.mapMessage) != nil {
					return
				}
				continue
			}
			if m.Type == msgr.SubscribeMessage {
				if !p.initial {
					p.errors <- errors.New("named operation subscribed shared maps/logs")
					return
				}
				continue
			}
			if m.Type != 97 && m.Type != msgr.MonCommandMessage {
				p.errors <- fmt.Errorf("unexpected message type %d", m.Type)
				return
			}
			if m.Type == 97 {
				p.tellCount.Add(1)
			}
			if strings.Contains(string(m.Front), "hold") {
				continue
			}
			if strings.Contains(string(m.Front), "drop") {
				return
			}
			code := int32(0)
			if strings.Contains(string(m.Front), "deny") {
				code = -13
			}
			front := binary.LittleEndian.AppendUint32(nil, uint32(code))
			front = namedTestString(front, "named status")
			typ := uint16(98)
			if m.Type == msgr.MonCommandMessage {
				front = append(make([]byte, 18), front...)
				front = binary.LittleEndian.AppendUint32(front, 0)
				typ = msgr.MonCommandReplyMessage
			}
			data := m.Data
			if len(data) == 0 {
				data = []byte(`{"named":true}`)
			}
			reply := msgr.MessageData{Type: typ, Version: 1, Transaction: m.Transaction, Front: front, Data: data}
			if p.reply != nil {
				p.reply(&reply)
			}
			if send(reply) != nil {
				return
			}
		}
	}
}

func (p *namedTestPeer) next(t *testing.T, ctx context.Context, typ uint16) msgr.MessageData {
	t.Helper()
	for {
		select {
		case m := <-p.requests:
			if m.Type == typ {
				return m
			}
		case err := <-p.errors:
			t.Fatal("named peer failed", err)
		case <-ctx.Done():
			t.Fatal("named peer did not receive expected type", typ, ctx.Err())
		}
	}
}

func (p *namedTestPeer) send(t *testing.T, ctx context.Context, m msgr.MessageData) {
	t.Helper()
	request := namedTestSend{message: m, result: make(chan error, 1)}
	select {
	case p.sends <- request:
	case <-p.done:
		t.Fatal("named peer closed before delivery")
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

type namedTestFixture struct {
	c          *Client
	ctx        context.Context
	primary    *namedTestPeer
	targets    chan *namedTestPeer
	monDials   atomic.Uint32
	namedDials atomic.Uint32
	mgrDials   atomic.Uint32
	mu         sync.Mutex
	peers      []*namedTestPeer
}

func namedTestFixtureFor(t *testing.T, address msgr.Address, change func(*Options, []namedTestMember), configure func(*namedTestPeer)) *namedTestFixture {
	t.Helper()
	f := &namedTestFixture{targets: make(chan *namedTestPeer, 16)}
	members := []namedTestMember{
		{"a", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}},
		{"b", []msgr.Address{address}},
		{"c", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.3:3302")}}},
	}
	options := mockOptions(t, 20, [16]byte{1})
	if change != nil {
		change(&options, members)
	}
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if endpoint == "192.0.2.1:6800" {
			f.mgrDials.Add(1)
			client, peer := net.Pipe()
			go mockDaemon(peer, peerConfig{role: 16, id: 99, fsid: [16]byte{1}, clientID: 42})
			return client, nil
		}
		initial := endpoint == "192.0.2.1:3300"
		if initial {
			f.monDials.Add(1)
		} else {
			f.namedDials.Add(1)
			if endpoint != dialAddress(address) {
				return nil, errors.New("named MON attempted an unrelated daemon")
			}
		}
		client, peer := net.Pipe()
		p := &namedTestPeer{conn: peer, client: client, initial: initial, globalID: 84, address: address,
			mapMessage: msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: namedTestMap([16]byte{1}, 20, members)},
			requests:   make(chan msgr.MessageData, 32), sends: make(chan namedTestSend), done: make(chan struct{}), initialID: make(chan uint64, 1), errors: make(chan error, 2)}
		if initial {
			p.globalID, f.primary = 42, p
		} else if configure != nil {
			configure(p)
		}
		f.mu.Lock()
		f.peers = append(f.peers, p)
		f.mu.Unlock()
		if !initial {
			f.targets <- p
		}
		go p.serve()
		return p.client, nil
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
		f.mu.Lock()
		peers := append([]*namedTestPeer(nil), f.peers...)
		f.mu.Unlock()
		for _, p := range peers {
			p.conn.Close()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Error("named peer outlived cleanup")
			}
		}
	})
	if err != nil {
		t.Fatal("named fixture bootstrap failed", err)
	}
	return f
}

func namedTestTarget(t *testing.T, f *namedTestFixture) *namedTestPeer {
	t.Helper()
	select {
	case p := <-f.targets:
		return p
	case <-f.ctx.Done():
		t.Fatal("no named peer was dialed", f.ctx.Err())
		return nil
	}
}

func namedTestAddressB() msgr.Address {
	return msgr.Address{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("192.0.2.2:3301")}
}

func namedTestKnown(t *testing.T, err error) {
	t.Helper()
	var unknown *OutcomeUnknownError
	if err == nil || errors.As(err, &unknown) {
		t.Fatal("unsubmitted tell did not retain a known local outcome", err)
	}
}

func namedTestPrimaryHealthy(t *testing.T, f *namedTestFixture) {
	t.Helper()
	if _, err := f.c.MonCommand(f.ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
		t.Fatal("named operation damaged the primary MON", err)
	}
	if state := f.c.Snapshot(); !state.Monitor.Ready || state.GlobalID != 42 || state.AuthRejection != nil {
		t.Fatal("named operation changed primary authentication", state)
	}
}

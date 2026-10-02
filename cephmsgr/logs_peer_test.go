package cephmsgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Log payloads below are laid out independently from the product decoder.
// Pinned Tentacle MLog.h, PaxosServiceMessage.h and LogEntry.cc define the
// service version/FSID/deque followed by version-5 LogEntry envelopes.
func logTestU16(p []byte, v uint16) []byte { return binary.LittleEndian.AppendUint16(p, v) }

func logTestU32(p []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(p, v) }

func logTestU64(p []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(p, v) }

func logTestString(p []byte, v string) []byte {
	return append(logTestU32(p, uint32(len(v))), v...)
}

func logTestEnvelope(version, compat byte, p []byte) []byte {
	return append(logTestU32([]byte{version, compat}, uint32(len(p))), p...)
}

func logTestAddress(a LogAddress) []byte {
	var sock []byte
	if a.Endpoint.Addr().Is4() {
		sock = make([]byte, 16)
		binary.LittleEndian.PutUint16(sock, 2) // Linux AF_INET, irrespective of host.
		ip := a.Endpoint.Addr().As4()
		copy(sock[4:8], ip[:])
	} else {
		sock = make([]byte, 28)
		binary.LittleEndian.PutUint16(sock, 10) // Linux AF_INET6; mapped stays IPv6.
		binary.BigEndian.PutUint32(sock[4:8], a.FlowInfo)
		ip := a.Endpoint.Addr().As16()
		copy(sock[8:24], ip[:])
		binary.LittleEndian.PutUint32(sock[24:28], a.ScopeID)
	}
	binary.BigEndian.PutUint16(sock[2:4], a.Endpoint.Port())
	p := logTestU32(nil, a.Type)
	p = logTestU32(p, a.Nonce)
	p = append(logTestU32(p, uint32(len(sock))), sock...)
	return append([]byte{1}, logTestEnvelope(1, 1, p)...)
}

func logTestEntry(e LogEntry) []byte {
	p := logTestString(logTestU32(nil, e.NameType), e.NameID)
	p = logTestU64(append(p, e.RankType), uint64(e.RankNumber))
	p = logTestU32(append(p, 2), uint32(len(e.Addresses)))
	for _, a := range e.Addresses {
		p = append(p, logTestAddress(a)...)
	}
	p = logTestU32(logTestU32(p, e.Seconds), e.Nanoseconds)
	p = logTestU16(logTestU64(p, e.Sequence), e.Priority)
	p = logTestString(logTestString(p, e.Message), e.Channel)
	return logTestEnvelope(5, 5, p)
}

func logTestMessage(fsid [16]byte, version uint64, entries ...LogEntry) msgr.MessageData {
	p := logTestU64(logTestU16(logTestU64(nil, version), 0xffff), 0)
	p = append(p, fsid[:]...)
	p = logTestU32(p, uint32(len(entries)))
	for _, entry := range entries {
		p = append(p, logTestEntry(entry)...)
	}
	return msgr.MessageData{Type: 52, Version: 1, Front: p}
}

func logTestRawEntry(message string) LogEntry {
	return LogEntry{
		NameType: 0x12345678, NameID: "opaque\x00name\xff", RankType: 0xfe, RankNumber: -3,
		Addresses: []LogAddress{
			{Type: 2, Nonce: 0x11223344, Endpoint: netip.MustParseAddrPort("192.0.2.9:1234")},
			{Type: 2, Nonce: 0xaabbccdd, Endpoint: netip.MustParseAddrPort("[::ffff:192.0.2.10]:5678"), FlowInfo: 0x10203040, ScopeID: 0x50607080},
		},
		Seconds: 0xfffffff0, Nanoseconds: 1000000001, Sequence: 0xfedcba9876543210,
		Priority: 0xffff, Message: message, Channel: "unknown\x00channel\xff",
	}
}

type logTestSubscription struct {
	name  string
	start uint64
	flags byte
	all   map[string]uint64
}

type logTestSend struct {
	message msgr.MessageData
	result  chan error
}

type logTestPeer struct {
	conn          net.Conn
	sends         chan logTestSend
	subscriptions chan logTestSubscription
	commands      chan msgr.MessageData
	done          chan struct{}
	release       byte
	owner         any
}

type logTestOwnerKey struct{}

// Authentication/framing reuse the existing synthetic peer. MLog bytes and
// subscription observations are independent of the product log codec. One
// writer owns server sequence numbers and secure nonces, including replies.
func (p *logTestPeer) serve(early bool) {
	defer close(p.done)
	defer p.conn.Close()
	r, w, err := mockAuthenticate(p.conn, peerConfig{role: 1, release: 20, fsid: [16]byte{1}})
	if err != nil {
		return
	}
	type incoming struct {
		frame msgr.Frame
		err   error
	}
	frames := make(chan incoming)
	drained := make(chan struct{})
	abort := make(chan struct{})
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
	if early {
		// A recovering candidate must not decode logs before MonMap admission.
		if send(msgr.MessageData{Type: 52, Version: 1, CompatVersion: 1, Front: []byte{0}}) != nil {
			return
		}
	}
	for _, m := range []msgr.MessageData{
		{Type: msgr.MonMapMessage, Version: 1, Front: mockMonMap(p.release, [16]byte{1})},
		{Type: msgr.MgrMapMessage, Version: 1, Front: mockMgrMap(1, 99, 6800)},
	} {
		if send(m) != nil {
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
				return
			}
			if m.Type == msgr.SubscribeMessage {
				d := wire.NewDecoder(m.Front)
				count := d.U32()
				if count > 8 || m.Version != 3 || m.CompatVersion != 1 {
					return
				}
				all := make(map[string]uint64)
				var sub logTestSubscription
				for range count {
					name, start, flags := d.String(), d.U64(), d.U8()
					all[name] = start
					if strings.HasPrefix(name, "log-") {
						sub = logTestSubscription{name: name, start: start, flags: flags}
					}
				}
				_ = d.String()
				if d.Done() != nil {
					return
				}
				if sub.name != "" {
					sub.all = all
					p.subscriptions <- sub
				}
				continue
			}
			if m.Type != msgr.MonCommandMessage {
				return
			}
			p.commands <- m
			if strings.Contains(string(m.Front), "hold mutation") {
				continue
			}
			e := wire.Encoder{}
			msgr.Paxos(&e)
			e.U32(0)
			e.String("log-side status")
			e.U32(0)
			if send(msgr.MessageData{Type: msgr.MonCommandReplyMessage, Version: 1, Transaction: m.Transaction, Front: e.Data, Data: []byte(`{"logs":true}`)}) != nil {
				return
			}
		}
	}
}

func (p *logTestPeer) send(t *testing.T, ctx context.Context, m msgr.MessageData) {
	t.Helper()
	request := logTestSend{message: m, result: make(chan error, 1)}
	select {
	case p.sends <- request:
	case <-p.done:
		t.Fatal("log peer ended before send")
	case <-ctx.Done():
		t.Fatal("log peer send did not start", ctx.Err())
	}
	select {
	case err := <-request.result:
		if err != nil {
			t.Fatal("log peer send failed", err)
		}
	case <-ctx.Done():
		t.Fatal("log peer send did not finish", ctx.Err())
	}
}

func (p *logTestPeer) subscription(t *testing.T, ctx context.Context, name string, start uint64) {
	t.Helper()
	select {
	case sub := <-p.subscriptions:
		if sub.name != name || sub.start != start || sub.flags != 0 || len(sub.all) != 3 || sub.all["monmap"] != 0 || sub.all["mgrmap"] != 0 {
			t.Fatal("log subscription lost level/cursor/continuous map subscriptions", sub, name, start)
		}
	case <-p.done:
		t.Fatal("log peer ended before subscription")
	case <-ctx.Done():
		t.Fatal("log subscription not observed", ctx.Err())
	}
}

func logTestFixture(t *testing.T, configure ...func(int, *logTestPeer)) (*Client, context.Context, <-chan *logTestPeer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxFrameSize = 64 << 10
	options.MaxInFlight = 1
	dial := options.DialContext
	peers := make(chan *logTestPeer, 16)
	var mu sync.Mutex
	var all []*logTestPeer
	options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
		if !strings.HasSuffix(endpoint, ":3300") {
			return dial(ctx, network, endpoint)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mu.Lock()
		index := len(all)
		mu.Unlock()
		p := &logTestPeer{sends: make(chan logTestSend), subscriptions: make(chan logTestSubscription, 8), commands: make(chan msgr.MessageData, 128), done: make(chan struct{}), release: 20}
		for _, configurePeer := range configure {
			configurePeer(index, p)
		}
		if p.owner != nil && ctx.Value(logTestOwnerKey{}) != p.owner {
			return nil, errors.New("log fixture candidate requires its explicit operation context")
		}
		client, server := net.Pipe()
		p.conn = server
		mu.Lock()
		all = append(all, p)
		mu.Unlock()
		peers <- p
		go p.serve(index != 0)
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		cancel()
		t.Fatal("admit log fixture", err)
	}
	t.Cleanup(func() {
		cancel()
		c.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, p := range all {
			p.conn.Close()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Error("log fixture peer did not terminate")
			}
		}
	})
	return c, ctx, peers
}

func logTestNextPeer(t *testing.T, ctx context.Context, peers <-chan *logTestPeer) *logTestPeer {
	t.Helper()
	select {
	case p := <-peers:
		return p
	case <-ctx.Done():
		t.Fatal("log recovery did not dial another MON", ctx.Err())
		return nil
	}
}

func logTestBarrier(t *testing.T, ctx context.Context, c *Client) {
	t.Helper()
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"status"}`)})
	if err != nil || result.Code != 0 || result.Message != "log-side status" || !bytes.Equal(result.Data, []byte(`{"logs":true}`)) {
		t.Fatal("ordinary command blocked or corrupted by logs", result, err)
	}
}

func logTestBatch(t *testing.T, ctx context.Context, stream *LogStream, version uint64, entries ...LogEntry) {
	t.Helper()
	batch, err := stream.Next(ctx)
	if err != nil || batch.FSID != "01000000-0000-0000-0000-000000000000" || batch.Version != version || len(batch.Entries) != len(entries) || len(entries) != 0 && !reflect.DeepEqual(batch.Entries, entries) {
		t.Fatal("log batch lost its raw fields/order", batch, err, version, entries)
	}
}

func logTestTerminal(t *testing.T, ctx context.Context, stream *LogStream, want error) {
	t.Helper()
	for range 2 {
		batch, err := stream.Next(ctx)
		if !errors.Is(err, want) || len(batch.Entries) != 0 || batch.Version != 0 {
			t.Fatal("log terminal was unstable or returned rejected data", batch, err, want)
		}
	}
}

package cephmsgr

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// Authentication and maps reuse the established synthetic fixture. Requests
// remain raw MessageData so hostname observation does not use its builders.
type hostnameTestPeer struct {
	conn     net.Conn
	requests chan msgr.MessageData
	done     chan struct{}
	mu       sync.Mutex
	writer   *msgr.Writer
	sequence uint64
}

func (p *hostnameTestPeer) send(message msgr.MessageData) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sequence++
	message.Sequence, message.Priority = p.sequence, 127
	return p.writer.Write(message.Frame())
}

func (p *hostnameTestPeer) serve() {
	defer close(p.done)
	defer p.conn.Close()
	reader, writer, err := mockAuthenticate(p.conn, peerConfig{role: 1, release: 20, fsid: [16]byte{1}})
	if err != nil {
		return
	}
	p.writer = writer
	for _, message := range []msgr.MessageData{
		{Type: msgr.MonMapMessage, Version: 1, Front: mockMonMap(20, [16]byte{1})},
		{Type: msgr.MgrMapMessage, Version: 1, Front: mockMgrMap(1, 99, 6800)},
	} {
		if p.send(message) != nil {
			return
		}
	}
	for {
		frame, err := reader.Read()
		if err != nil {
			return
		}
		if frame.Tag != msgr.Message {
			continue
		}
		message, err := msgr.DecodeMessage(frame)
		if err != nil {
			return
		}
		p.requests <- message
		if message.Type != msgr.MonCommandMessage {
			continue
		}
		front := wire.Encoder{}
		msgr.Paxos(&front)
		front.U32(0)
		front.String("log-side status")
		front.U32(0)
		if p.send(msgr.MessageData{Type: msgr.MonCommandReplyMessage, Version: 1, Transaction: message.Transaction, Front: front.Data, Data: []byte(`{"logs":true}`)}) != nil {
			return
		}
	}
}

func hostnameTestFixture(t *testing.T, hostname string) (*Client, context.Context, *Options, <-chan *hostnameTestPeer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.Hostname = hostname
	options.MaxFrameSize, options.MaxInFlight = 64<<10, 1
	deadline, _ := ctx.Deadline()
	peers := make(chan *hostnameTestPeer, 8)
	var mu sync.Mutex
	var all []*hostnameTestPeer
	options.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client, server := net.Pipe()
		server.SetDeadline(deadline)
		peer := &hostnameTestPeer{conn: server, requests: make(chan msgr.MessageData, 32), done: make(chan struct{})}
		mu.Lock()
		all = append(all, peer)
		mu.Unlock()
		peers <- peer
		go peer.serve()
		return client, nil
	}
	client, err := Dial(ctx, options)
	if err != nil {
		cancel()
		t.Fatal("hostname fixture admission", err)
	}
	t.Cleanup(func() {
		cancel()
		client.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, peer := range all {
			peer.conn.Close()
			select {
			case <-peer.done:
			case <-time.After(time.Second):
				t.Error("hostname peer did not stop")
			}
		}
	})
	return client, ctx, &options, peers
}

func hostnameTestNextPeer(t *testing.T, ctx context.Context, peers <-chan *hostnameTestPeer) *hostnameTestPeer {
	t.Helper()
	select {
	case peer := <-peers:
		return peer
	case <-ctx.Done():
		t.Fatal("hostname source did not appear", ctx.Err())
		return nil
	}
}

func (p *hostnameTestPeer) registrations(t *testing.T, ctx context.Context, hostname, identity string, kinds ...string) {
	t.Helper()
	pending := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		pending[kind] = true
	}
	for len(pending) != 0 {
		select {
		case message := <-p.requests:
			decoder := wire.NewDecoder(message.Front)
			kind := "maps"
			if message.Type == msgr.GetConfigMessage {
				kind = "getconfig"
				if decoder.U32() != 8 || decoder.String() != strings.TrimPrefix(identity, "client.") || decoder.String() != hostname || decoder.String() != "" || decoder.Done() != nil {
					t.Fatal("MGetConfig changed the authenticated name, caller hostname or empty class")
				}
			} else if message.Type == msgr.SubscribeMessage {
				count := decoder.U32()
				maps := make(map[string]uint64)
				for range count {
					name, start, flags := decoder.String(), decoder.U64(), decoder.U8()
					maps[name] = start
					if name != "monmap" && name != "mgrmap" {
						kind = name
					}
					if flags != 0 {
						t.Fatal("hostname changed continuous subscription flags")
					}
				}
				_, mon := maps["monmap"]
				_, mgr := maps["mgrmap"]
				if !mon || !mgr || maps["monmap"] != 0 || maps["mgrmap"] != 0 || decoder.String() != hostname || decoder.Done() != nil {
					t.Fatal("subscription dropped maps or replaced the caller hostname")
				}
			} else {
				t.Fatal("watch registration sent an application command", message.Type)
			}
			if !pending[kind] {
				t.Fatal("unexpected or duplicate registration", kind)
			}
			delete(pending, kind)
		case <-ctx.Done():
			t.Fatal("hostname registration was not observed", pending, ctx.Err())
		}
	}
}

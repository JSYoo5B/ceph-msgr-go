package cephmsgr

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type configTestPeer struct {
	*logTestPeer
	configs             chan msgr.MessageData
	configSubscriptions chan logTestSubscription
}

func (p *configTestPeer) serveConfig(early bool) {
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
	frames, drained, abort := make(chan incoming), make(chan struct{}), make(chan struct{})
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
	if early && send(msgr.MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: []byte{0}}) != nil {
		return
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
			switch m.Type {
			case msgr.SubscribeMessage:
				d := wire.NewDecoder(m.Front)
				count := d.U32()
				if count > 8 || m.Version != 3 || m.CompatVersion != 1 {
					return
				}
				all := make(map[string]uint64)
				var logSub, configSub logTestSubscription
				for range count {
					name, start, flags := d.String(), d.U64(), d.U8()
					all[name] = start
					if strings.HasPrefix(name, "log-") {
						logSub = logTestSubscription{name: name, start: start, flags: flags}
					}
					if name == "config" {
						configSub = logTestSubscription{name: name, start: start, flags: flags}
					}
				}
				if d.String() != "" || d.Done() != nil {
					return
				}
				if logSub.name != "" {
					logSub.all = all
					p.subscriptions <- logSub
				}
				if configSub.name != "" {
					configSub.all = all
					p.configSubscriptions <- configSub
				}
			case msgr.GetConfigMessage:
				p.configs <- m
			case msgr.MonCommandMessage:
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
			default:
				return
			}
		}
	}
}

func configTestFixture(t *testing.T, configure ...func(int, *configTestPeer)) (*Client, context.Context, <-chan *configTestPeer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	options := mockOptions(t, 20, [16]byte{1})
	options.MaxFrameSize, options.MaxInFlight = 64<<10, 1
	dial := options.DialContext
	peers := make(chan *configTestPeer, 16)
	var mu sync.Mutex
	var all []*configTestPeer
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
		p := &configTestPeer{logTestPeer: &logTestPeer{sends: make(chan logTestSend), subscriptions: make(chan logTestSubscription, 8), commands: make(chan msgr.MessageData, 128), done: make(chan struct{}), release: 20}, configs: make(chan msgr.MessageData, 8), configSubscriptions: make(chan logTestSubscription, 8)}
		for _, configurePeer := range configure {
			configurePeer(index, p)
		}
		if p.owner != nil && ctx.Value(logTestOwnerKey{}) != p.owner {
			return nil, errors.New("config fixture candidate requires its explicit operation context")
		}
		client, server := net.Pipe()
		p.conn = server
		mu.Lock()
		all = append(all, p)
		mu.Unlock()
		peers <- p
		go p.serveConfig(index != 0)
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		cancel()
		t.Fatal("admit config fixture", err)
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
				t.Error("config fixture peer did not terminate")
			}
		}
	})
	return c, ctx, peers
}

func configTestNextPeer(t *testing.T, ctx context.Context, peers <-chan *configTestPeer) *configTestPeer {
	t.Helper()
	select {
	case p := <-peers:
		return p
	case <-ctx.Done():
		t.Fatal("config recovery did not dial another MON", ctx.Err())
		return nil
	}
}

func (p *configTestPeer) registration(t *testing.T, ctx context.Context, identity string) {
	t.Helper()
	select {
	case sub := <-p.configSubscriptions:
		if sub.name != "config" || sub.start != 0 || sub.flags != 0 || len(sub.all) != 3 || sub.all["monmap"] != 0 || sub.all["mgrmap"] != 0 {
			t.Fatal("config registration lost map subscriptions or continuous semantics", sub)
		}
	case <-ctx.Done():
		t.Fatal("config subscription was not observed", ctx.Err())
	}
	select {
	case m := <-p.configs:
		d := wire.NewDecoder(m.Front)
		if m.Type != 63 || m.Version != 1 || m.CompatVersion != 1 || m.Transaction != 0 || len(m.Middle) != 0 || len(m.Data) != 0 || d.U32() != 8 || d.String() != strings.TrimPrefix(identity, "client.") || d.String() != "" || d.String() != "" || d.Done() != nil {
			t.Fatal("full-map request selected another identity or host/class", m)
		}
	case <-ctx.Done():
		t.Fatal("full config request was not observed", ctx.Err())
	}
}

// Independently assembled MConfig count and raw length-prefixed key/value pairs.
func configTestMessage(pairs ...string) msgr.MessageData {
	p := logTestU32(nil, uint32(len(pairs)/2))
	for _, value := range pairs {
		p = logTestString(p, value)
	}
	return msgr.MessageData{Type: 62, Version: 1, CompatVersion: 1, Front: p}
}

func configTestTerminal(t *testing.T, ctx context.Context, stream *ConfigStream, want error) {
	t.Helper()
	for range 2 {
		config, err := stream.Next(ctx)
		if config != nil || !errors.Is(err, want) {
			t.Fatal("config terminal was unstable or returned rejected data", config, err, want)
		}
	}
}

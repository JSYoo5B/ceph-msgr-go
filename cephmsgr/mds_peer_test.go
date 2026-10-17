package cephmsgr

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

// This independent synthetic peer exercises public service selection and a
// fresh stateful MDS handshake. It is not native interoperability evidence.
func mdsMockAuthenticate(conn net.Conn, cfg peerConfig) (*msgr.Reader, *msgr.Writer, error) {
	if cfg.clientID == 0 {
		cfg.clientID = 42
	}
	rx := &traceReader{Reader: conn, active: true}
	tx := &traceWriter{Writer: conn, active: true}
	if _, err := msgr.ReadBanner(rx, msgr.Banner{Supported: 3, Required: 1}); err != nil {
		return nil, nil, err
	}
	if err := msgr.WriteFull(tx, (msgr.Banner{Supported: 3, Required: 1}).Encode()); err != nil {
		return nil, nil, err
	}
	r, w := msgr.NewReader(rx, 0), msgr.NewWriter(tx, 0)
	write := func(tag msgr.Tag, p []byte) error {
		if cfg.payload != nil {
			p = cfg.payload(tag, p)
		}
		return w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}})
	}
	read := func(tag msgr.Tag) ([]byte, error) {
		f, err := r.Read()
		if err != nil {
			return nil, err
		}
		if f.Tag != tag || len(f.Segments) != 1 {
			return nil, fmt.Errorf("expected tag %d, got %d", tag, f.Tag)
		}
		return f.Segments[0], nil
	}
	if _, err := read(msgr.Hello); err != nil {
		return nil, nil, err
	}
	hello := wire.Encoder{}
	hello.U8(cfg.role)
	msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
	if err := write(msgr.Hello, hello.Data); err != nil {
		return nil, nil, err
	}
	p, err := read(msgr.AuthRequest)
	if err != nil {
		return nil, nil, err
	}
	d := wire.NewDecoder(p)
	if d.U32() != 2 || d.U32() != 1 {
		return nil, nil, errors.New("bad method/mode count")
	}
	mode := d.U32()
	if mode != 1 && mode != 2 {
		return nil, nil, errors.New("bad connection mode")
	}
	authPayload := d.Bytes()
	if err := d.Done(); err != nil {
		return nil, nil, err
	}
	key, encoded := mockCredential()
	secret := make([]byte, 40)
	for i := range secret {
		secret[i] = byte(i)
	}
	var final []byte
	if cfg.role == 1 {
		initial := wire.NewDecoder(authPayload)
		if initial.U8() != 10 || initial.U32() != 8 || initial.String() != "test" {
			return nil, nil, errors.New("identity")
		}
		initial.U64()
		if err := initial.Done(); err != nil {
			return nil, nil, err
		}
		challenge := wire.Encoder{}
		challenge.U8(1)
		challenge.U64(0x1234)
		more := wire.Encoder{}
		more.Bytes(challenge.Data)
		if err := write(msgr.AuthReplyMore, more.Data); err != nil {
			return nil, nil, err
		}
		p, err := read(msgr.AuthRequestMore)
		if err != nil {
			return nil, nil, err
		}
		container := wire.NewDecoder(p)
		proof := wire.NewDecoder(container.Bytes())
		if proof.U16() != 0x100 || proof.U8() != 3 {
			return nil, nil, errors.New("auth request version")
		}
		nonce, got := proof.U64(), proof.U64()
		proof.U8()
		proof.U64()
		proof.Bytes()
		services := cfg.services
		if services == 0 {
			services = cephx.ServiceAuth | cephx.ServiceMgr
		}
		if proof.U32() != services || proof.Done() != nil {
			return nil, nil, errors.New("ticket request")
		}
		challengeBlob := wire.Encoder{}
		challengeBlob.U64(0x1234)
		challengeBlob.U64(nonce)
		hash := key.value.Signature(challengeBlob.Data)
		var want uint64
		for i := 0; i < len(hash); i += 8 {
			want ^= binary.LittleEndian.Uint64(hash[i:])
		}
		if got != want {
			return nil, nil, errors.New("bad challenge proof")
		}
		tickets, err := mockTicket(key.value, encoded, cephx.ServiceAuth, cfg.authTTL)
		if err != nil {
			return nil, nil, err
		}
		connSecret := wire.Encoder{}
		connSecret.Bytes(secret)
		encrypted, err := key.value.Seal(3, connSecret.Data)
		if err != nil {
			return nil, nil, err
		}
		wrapped := wire.Encoder{}
		wrapped.Bytes(encrypted)
		// Construct each selected optional service ticket independently and
		// combine its entry, rather than assuming a fixed MGR/OSD count.
		extra := []byte{1, 0, 0, 0, 0}
		var count uint32
		for _, service := range []uint32{cephx.ServiceMgr, cephx.ServiceOSD, cephx.ServiceMDS} {
			if services&service == 0 {
				continue
			}
			ticket, err := mockTicket(key.value, encoded, service, cfg.mgrTTL)
			if err != nil {
				return nil, nil, err
			}
			extra = append(extra, ticket[5:]...)
			count++
		}
		binary.LittleEndian.PutUint32(extra[1:5], count)
		response := wire.Encoder{}
		response.U16(0x100)
		response.U32(0)
		response.Raw(tickets)
		response.Bytes(wrapped.Data)
		response.Bytes(extra)
		final = response.Data
	} else {
		service := cephx.ServiceMgr
		if cfg.role == 4 {
			service = cephx.ServiceOSD
		} else if cfg.role == 2 {
			service = cephx.ServiceMDS
		}
		parse := func(p []byte) (uint64, bool, uint64, error) {
			d := wire.NewDecoder(p)
			if d.U8() != 1 || d.U64() != cfg.clientID || d.U32() != service {
				return 0, false, 0, errors.New("mgr authorizer")
			}
			d.U8()
			d.U64()
			d.Bytes()
			plain, err := key.value.Open(0x10, d.Bytes())
			if err != nil {
				return 0, false, 0, err
			}
			body := wire.NewDecoder(plain)
			if body.U8() != 2 {
				return 0, false, 0, wire.ErrVersion
			}
			nonce, challenged, challenge := body.U64(), body.Bool(), body.U64()
			return nonce, challenged, challenge, body.Done()
		}
		nonce, challenged, _, err := parse(authPayload)
		if err != nil || challenged {
			return nil, nil, errors.New("mgr initial proof")
		}
		challenge := wire.Encoder{}
		challenge.U8(1)
		challenge.U64(57)
		encrypted, err := key.value.Seal(0x11, challenge.Data)
		if err != nil {
			return nil, nil, err
		}
		more := wire.Encoder{}
		more.Bytes(encrypted)
		if err := write(msgr.AuthReplyMore, more.Data); err != nil {
			return nil, nil, err
		}
		p, err := read(msgr.AuthRequestMore)
		if err != nil {
			return nil, nil, err
		}
		moreReply := wire.NewDecoder(p)
		actual, challenged, challengePlusOne, err := parse(moreReply.Bytes())
		if err != nil || actual != nonce || !challenged || challengePlusOne != 58 {
			return nil, nil, errors.New("mgr challenge proof")
		}
		reply := wire.Encoder{}
		reply.U8(2)
		reply.U64(nonce + 1)
		reply.Bytes(secret)
		encrypted, err = key.value.Seal(0x12, reply.Data)
		if err != nil {
			return nil, nil, err
		}
		out := wire.Encoder{}
		out.Bytes(encrypted)
		final = out.Data
	}
	done := wire.Encoder{}
	done.U64(cfg.clientID)
	done.U32(mode)
	done.Bytes(final)
	if err := write(msgr.AuthDone, done.Data); err != nil {
		return nil, nil, err
	}
	sig := key.value.Signature(rx.data.Bytes())
	expected := key.value.Signature(tx.data.Bytes())
	rx.active, tx.active = false, false
	if mode == 2 {
		r.EnableSecure(secret[:16], secret[28:40])
		w.EnableSecure(secret[:16], secret[16:28])
	}
	p, err = read(msgr.AuthSignature)
	if err != nil {
		return nil, nil, err
	}
	if !hmac.Equal(p, expected) {
		return nil, nil, errors.New("transcript")
	}
	if err := write(msgr.AuthSignature, sig); err != nil {
		return nil, nil, err
	}
	if _, err := read(msgr.CompressionRequest); err != nil {
		return nil, nil, err
	}
	noCompression := wire.Encoder{}
	noCompression.U8(0)
	noCompression.U32(0)
	if err := write(msgr.CompressionDone, noCompression.Data); err != nil {
		return nil, nil, err
	}
	f, err := r.Read()
	if err != nil {
		return nil, nil, err
	}
	if f.Tag == msgr.SessionReconnect && cfg.reconnect != nil {
		tag, payload := cfg.reconnect(f)
		if err := write(tag, payload); err != nil {
			return nil, nil, err
		}
		if tag != msgr.SessionReconnectOK {
			return nil, nil, errMockSessionNotResumed
		}
		conn.SetDeadline(time.Time{})
		return r, w, nil
	}
	if f.Tag != msgr.ClientIdent || len(f.Segments) != 1 {
		return nil, nil, fmt.Errorf("expected tag %d, got %d", msgr.ClientIdent, f.Tag)
	}
	p = f.Segments[0]
	clientIdent := wire.NewDecoder(p)
	clientAddresses := msgr.DecodeAddresses(clientIdent)
	target := msgr.DecodeAddress(clientIdent)
	if cfg.clientAddresses != nil {
		cfg.clientAddresses(clientAddresses)
	}
	clientID, _ := clientIdent.U64(), clientIdent.U64()
	supported, required := clientIdent.U64(), clientIdent.U64()
	flags, cookie := clientIdent.U64(), clientIdent.U64()
	if err := clientIdent.Done(); err != nil {
		return nil, nil, err
	}
	wantSupported, wantRequired := session.Features, session.RequiredFeatures
	if cfg.cephfs {
		wantSupported, wantRequired = session.CephFSFeatures, session.CephFSFeatures&^session.Features|session.RequiredFeatures
	}
	if cfg.role == 2 && (clientID != cfg.clientID || supported != wantSupported || required != wantRequired || flags != 0 || cookie == 0 || len(clientAddresses) != 1 || target.Type != 2 || target.Nonce != 0 && target.Nonce != 17 || target.Endpoint != netip.MustParseAddrPort("192.0.2.1:6805")) {
		return nil, nil, errors.New("MDS fresh stateful client ident changed identity, address, flags, cookie or fixed features")
	}
	if err := clientIdent.Err(); err != nil {
		return nil, nil, err
	}
	if cfg.ident != nil {
		cfg.ident(target)
	}
	addresses := cfg.addresses
	if addresses == nil {
		addresses = []msgr.Address{target}
	}
	ident := wire.Encoder{}
	msgr.EncodeAddresses(&ident, addresses)
	ident.U64(cfg.id)
	ident.U64(1)
	features := session.Features
	if cfg.cephfs {
		features = session.CephFSFeatures
	}
	if cfg.serverFeatures != 0 {
		features = cfg.serverFeatures
	}
	if cfg.role == 4 {
		features |= 1<<8 | 1<<9
	}
	ident.U64(features)
	ident.U64(session.RequiredFeatures)
	if cfg.role == 2 {
		ident.U64(0) // fresh stateful; no CEPH_MSG_CONNECT_LOSSY
		ident.U64(0x1020304050607080)
	} else {
		ident.U64(1)
		ident.U64(0)
	}
	if err := write(msgr.ServerIdent, ident.Data); err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Time{})
	return r, w, nil
}

var errMockSessionNotResumed = errors.New("mock MDS answered SESSION_RECONNECT without resuming")

type mdsTestPeer struct {
	conn     net.Conn
	writer   *msgr.Writer
	requests chan msgr.MessageData
	done     chan struct{}
	errors   chan error
	mu       sync.Mutex
	seq      uint64
	client   *mdsTestClientConn
	// sessions admits MClientSession messages for CephFS session tests.
	sessions atomic.Bool
	// cephfs selects the CephFS profile handshake and admits request and
	// client capability/lease messages.
	cephfs bool
	// serverFeatures overrides the features advertised in ServerIdent.
	serverFeatures uint64
	// rank is the active rank reported in ServerIdent.
	rank uint64
	// reconnect answers SESSION_RECONNECT (see peerConfig.reconnect).
	reconnect func(msgr.Frame) (msgr.Tag, []byte)
}

type mdsTestWritePause struct{ started, release chan struct{} }

type mdsTestClientConn struct {
	net.Conn
	mu    sync.Mutex
	pause *mdsTestWritePause
}

func (c *mdsTestClientConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	pause := c.pause
	c.pause = nil
	c.mu.Unlock()
	if pause != nil {
		close(pause.started)
		<-pause.release
	}
	return c.Conn.Write(p)
}

func (p *mdsTestPeer) pauseNextClientWrite() *mdsTestWritePause {
	pause := &mdsTestWritePause{started: make(chan struct{}), release: make(chan struct{})}
	p.client.mu.Lock()
	p.client.pause = pause
	p.client.mu.Unlock()
	return pause
}

func (p *mdsTestPeer) serve() {
	defer close(p.done)
	defer p.conn.Close()
	p.conn.SetDeadline(time.Now().Add(3 * time.Second))
	r, w, err := mdsMockAuthenticate(p.conn, peerConfig{role: 2, id: p.rank, cephfs: p.cephfs, serverFeatures: p.serverFeatures, reconnect: p.reconnect})
	if err != nil {
		p.errors <- err
		return
	}
	p.writer = w
	for {
		frame, err := r.Read()
		if err != nil {
			return
		}
		if frame.Tag == msgr.Keepalive {
			p.mu.Lock()
			err = w.Write(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: frame.Segments})
			p.mu.Unlock()
			if err != nil {
				return
			}
			continue
		}
		if frame.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(frame)
		if err != nil {
			p.errors <- err
			return
		}
		// D2 opens no filesystem client session. Session OPEN, map selection
		// or any other MDS message would violate the command-only contract.
		cephfsType := m.Type == 23 || m.Type == 24 || m.Type == 0x310 || m.Type == 0x311 || m.Type == 0x313
		if m.Type != msgr.TellCommandMessage && !((m.Type == 22 || m.Type == 27) && p.sessions.Load()) && !(cephfsType && p.cephfs) {
			p.errors <- fmt.Errorf("MDS command transport unexpectedly sent message %d", m.Type)
			return
		}
		select {
		case p.requests <- m:
		case <-p.done:
			return
		}
	}
}

func (p *mdsTestPeer) next(t *testing.T) msgr.MessageData {
	t.Helper()
	select {
	case m := <-p.requests:
		return m
	case err := <-p.errors:
		t.Fatal("MDS peer rejected the public transport", err)
	case <-p.done:
		select {
		case err := <-p.errors:
			t.Fatal("MDS peer rejected the public transport", err)
		default:
			t.Fatal("MDS peer closed before a command arrived")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("MDS command did not arrive")
	}
	return msgr.MessageData{}
}

func (p *mdsTestPeer) send(t *testing.T, m msgr.MessageData) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	m.Sequence = p.seq
	m.Priority = 127
	if err := p.writer.Write(m.Frame()); err != nil {
		t.Fatal("MDS peer could not send reply", err)
	}
}

func (p *mdsTestPeer) reply(t *testing.T, request msgr.MessageData, code int32, text string, data []byte) {
	t.Helper()
	e := wire.Encoder{}
	e.U32(uint32(code))
	e.String(text)
	p.send(t, msgr.MessageData{Type: msgr.TellCommandReplyMessage, Version: 1, CompatVersion: 1, Transaction: request.Transaction, Front: e.Data, Data: data})
}

func mdsMockMonitor(conn net.Conn) error {
	return mdsMockMonitorID(conn, 42)
}

func mdsMockMonitorID(conn net.Conn, id uint64) error {
	return mdsMockMonitorServices(conn, id, cephx.ServiceAuth|cephx.ServiceMgr|cephx.ServiceMDS)
}

func mdsMockMonitorServices(conn net.Conn, id uint64, services uint32) error {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	r, w, err := mdsMockAuthenticate(conn, peerConfig{role: 1, clientID: id, services: services})
	if err != nil {
		return err
	}
	var seq uint64
	send := func(m msgr.MessageData) error {
		seq++
		m.Sequence, m.Priority, m.Version, m.CompatVersion = seq, 127, 1, 1
		return w.Write(m.Frame())
	}
	if err := send(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(20, [16]byte{1})}); err != nil {
		return err
	}
	if err := send(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(1, 99, 6800)}); err != nil {
		return err
	}
	for {
		frame, err := r.Read()
		if err != nil {
			return err
		}
		if frame.Tag == msgr.Keepalive {
			if err := w.Write(msgr.Frame{Tag: msgr.KeepaliveAck, Segments: frame.Segments}); err != nil {
				return err
			}
			continue
		}
		if frame.Tag != msgr.Message {
			continue
		}
		m, err := msgr.DecodeMessage(frame)
		if err != nil {
			return err
		}
		if m.Type == msgr.SubscribeMessage {
			continue
		}
		if m.Type != msgr.MonCommandMessage {
			return fmt.Errorf("unexpected MON message %d", m.Type)
		}
		e := wire.Encoder{}
		msgr.Paxos(&e)
		e.U32(0)
		e.String("MON barrier")
		e.U32(0)
		if err := send(msgr.MessageData{Type: msgr.MonCommandReplyMessage, Transaction: m.Transaction, Front: e.Data}); err != nil {
			return err
		}
	}
}

func mdsTestOpen(t *testing.T) (*Client, *MDSConnection, *mdsTestPeer, context.Context, *atomic.Uint32) {
	t.Helper()
	return mdsTestOpenMode(t, SecureMode)
}

func mdsTestOpenMode(t *testing.T, mode ConnectionMode) (*Client, *MDSConnection, *mdsTestPeer, context.Context, *atomic.Uint32) {
	t.Helper()
	c, peers, ctx, dials := mdsTestClient(t, mode)
	m, err := c.OpenMDS(ctx, MDSTarget{FilesystemID: 7, Rank: 0, Address: "v2:192.0.2.1:6805/17"})
	if err != nil {
		select {
		case peer := <-peers:
			select {
			case cause := <-peer.errors:
				t.Fatal("public MDS fresh transport failed", err, cause)
			default:
			}
		default:
		}
		t.Fatal("public OpenMDS failed", err)
	}
	select {
	case peer := <-peers:
		return c, m, peer, ctx, dials
	case <-ctx.Done():
		t.Fatal("MDS dial did not create a peer", ctx.Err())
	}
	return nil, nil, nil, nil, nil
}

func mdsTestClient(t *testing.T, mode ConnectionMode) (*Client, <-chan *mdsTestPeer, context.Context, *atomic.Uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	options := mockOptions(t, 20, [16]byte{1})
	options.EnableMDS, options.MaxMDSConnections, options.ConnectionMode = true, 1, mode
	peers := make(chan *mdsTestPeer, 16)
	var dials atomic.Uint32
	options.DialContext = func(ctx context.Context, _, endpoint string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		clientPipe, server := net.Pipe()
		client := &mdsTestClientConn{Conn: clientPipe}
		if endpoint == "192.0.2.1:6805" {
			dials.Add(1)
			peer := &mdsTestPeer{conn: server, client: client, requests: make(chan msgr.MessageData, 64), done: make(chan struct{}), errors: make(chan error, 1)}
			peers <- peer
			go peer.serve()
		} else {
			go mdsMockMonitor(server)
		}
		return client, nil
	}
	c, err := Dial(ctx, options)
	if err != nil {
		t.Fatal("public Dial with MDS service selection failed", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, peers, ctx, &dials
}

package cephmsgr

import (
	"bytes"
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic checks of the public MGR listener: admission, publication,
// delivery, bounds, cancellation and shutdown. The peer below is a test
// OSD built from this repository's frame and CephX code, so these tests
// are not interoperability evidence; the native MGR acceptor test is.

type recordingConn struct {
	net.Conn
	rx, tx  bytes.Buffer
	enabled bool
}

func (c *recordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.enabled {
		c.rx.Write(p[:n])
	}
	return n, err
}

func (c *recordingConn) Write(p []byte) (int, error) {
	if c.enabled {
		c.tx.Write(p)
	}
	return c.Conn.Write(p)
}

type testOSD struct {
	conn   net.Conn
	r      *msgr.Reader
	w      *msgr.Writer
	server msgr.Frame
	gid    uint64
}

type testOSDConfig struct {
	mode     ConnectionMode
	osd      uint64
	globalID uint64
	secretID uint64
	ringKey  cephx.Key
	target   msgr.Address
	// stopAfter ends the handshake early: "hello" stops before HELLO.
	stopAfter string
	// client makes the peer client.test (entity type CLIENT) whose
	// CLIENT_IDENT numeric name is its global ID; identWrongID sends another.
	client, identWrongID bool
}

func osdTicketInfo(t *testing.T, ringKey, session cephx.Key, sessionBytes []byte, osd, globalID uint64) []byte {
	return peerTicketInfo(t, ringKey, sessionBytes, cephx.ServiceOSD, fmt.Sprint(osd), globalID)
}

func peerTicketInfo(t *testing.T, ringKey cephx.Key, sessionBytes []byte, entityType uint32, entityID string, globalID uint64) []byte {
	t.Helper()
	e := wire.Encoder{}
	e.U8(1)
	e.U8(2)
	e.U32(entityType)
	e.String(entityID)
	e.U64(globalID)
	e.U64(^uint64(0))
	e.U64(0)
	e.U32(uint32(time.Now().Add(time.Hour).Unix()))
	e.U32(0)
	e.U8(1)
	e.U8(0)
	caps := wire.Encoder{}
	caps.String("allow profile osd")
	e.Bytes(caps.Data)
	e.U32(0)
	e.Raw(sessionBytes)
	blob, err := ringKey.Seal(0x30, e.Data)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

// dialTestOSD runs an OSD-shaped initiating handshake against address.
func dialTestOSD(t *testing.T, address string, c testOSDConfig) (*testOSD, error) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		return nil, err
	}
	raw.SetDeadline(time.Now().Add(5 * time.Second))
	if c.stopAfter == "connect" {
		return &testOSD{conn: raw}, nil
	}
	conn := &recordingConn{Conn: raw, enabled: true}
	fail := func(err error) (*testOSD, error) { raw.Close(); return nil, err }
	local := msgr.Banner{Supported: msgr.Revision1, Required: msgr.Revision1}
	if err := msgr.WriteFull(conn, local.Encode()); err != nil {
		return fail(err)
	}
	if _, err := msgr.ReadBanner(conn, local); err != nil {
		return fail(err)
	}
	r, w := msgr.NewReader(conn, 1<<20), msgr.NewWriter(conn, 1<<20)
	write := func(tag msgr.Tag, p []byte) error { return w.Write(msgr.Frame{Tag: tag, Segments: [][]byte{p}}) }
	hello := wire.Encoder{}
	if c.client {
		hello.U8(entityTypeClient)
	} else {
		hello.U8(4)
	}
	c.target.Encode(&hello)
	if err := write(msgr.Hello, hello.Data); err != nil {
		return fail(err)
	}
	if f, err := r.Read(); err != nil || f.Tag != msgr.Hello {
		return fail(fmt.Errorf("hello: %v", err))
	}
	_, sessionBytes := mockCredential()
	session, _ := mockCredential()
	client, err := cephx.NewClient("client.osd", session.value, 0)
	if err != nil {
		return fail(err)
	}
	client.GlobalID = c.globalID
	blob := osdTicketInfo(t, c.ringKey, session.value, sessionBytes, c.osd, c.globalID)
	if c.client {
		blob = peerTicketInfo(t, c.ringKey, sessionBytes, entityTypeClient, "test", c.globalID)
	}
	client.Tickets[cephx.ServiceMgr] = cephx.Ticket{Key: session.value, SecretID: c.secretID, Blob: blob, Expires: time.Now().Add(time.Hour)}
	authorizer, err := client.Authorizer(cephx.ServiceMgr)
	if err != nil {
		return fail(err)
	}
	payload, _ := authorizer.Payload(nil)
	request := wire.Encoder{}
	request.U32(2)
	request.U32(1)
	request.U32(uint32(c.mode.sessionMode()))
	request.Bytes(payload)
	if err := write(msgr.AuthRequest, request.Data); err != nil {
		return fail(err)
	}
	var secret []byte
	gid := uint64(0)
	for done := false; !done; {
		f, err := r.Read()
		if err != nil {
			return fail(err)
		}
		d := wire.NewDecoder(f.Segments[0])
		switch f.Tag {
		case msgr.AuthReplyMore:
			proof, err := authorizer.Payload(d.Bytes())
			if err != nil {
				return fail(err)
			}
			more := wire.Encoder{}
			more.Bytes(proof)
			if err := write(msgr.AuthRequestMore, more.Data); err != nil {
				return fail(err)
			}
		case msgr.AuthDone:
			gid = d.U64()
			d.U32()
			if secret, err = authorizer.Finish(d.Bytes(), c.mode == SecureMode); err != nil {
				return fail(err)
			}
			done = true
		case msgr.AuthBadMethod:
			return fail(&cephx.AuthenticationError{Method: d.U32(), Code: int32(d.U32())})
		default:
			return fail(fmt.Errorf("tag %d", f.Tag))
		}
	}
	if c.mode == SecureMode {
		r.EnableSecure(secret[:16], secret[16:28])
		w.EnableSecure(secret[:16], secret[28:40])
	}
	sig, expected := session.value.Signature(conn.rx.Bytes()), session.value.Signature(conn.tx.Bytes())
	conn.enabled = false
	if err := write(msgr.AuthSignature, sig); err != nil {
		return fail(err)
	}
	if f, err := r.Read(); err != nil || f.Tag != msgr.AuthSignature || !hmac.Equal(f.Segments[0], expected) {
		return fail(fmt.Errorf("server signature: %v", err))
	}
	ident := wire.Encoder{}
	msgr.EncodeAddresses(&ident, []msgr.Address{{Type: 2, Nonce: 9, Endpoint: netip.MustParseAddrPort("192.0.2.30:6802")}})
	c.target.Encode(&ident)
	switch {
	case c.client && c.identWrongID:
		ident.U64(c.globalID + 1)
	case c.client:
		ident.U64(c.globalID)
	default:
		ident.U64(c.osd)
	}
	ident.U64(1)
	ident.U64(^uint64(0) >> 2)
	ident.U64(1<<0 | 1<<9 | 1<<13 | 1<<59)
	ident.U64(1)
	ident.U64(0)
	if err := write(msgr.ClientIdent, ident.Data); err != nil {
		return fail(err)
	}
	server, err := r.Read()
	if err != nil {
		return fail(err)
	}
	if server.Tag != msgr.ServerIdent {
		return fail(fmt.Errorf("server ident tag %d", server.Tag))
	}
	raw.SetDeadline(time.Time{})
	return &testOSD{conn: raw, r: r, w: w, server: server, gid: gid}, nil
}

func (o *testOSD) send(t *testing.T, seq uint64, typ uint16, front []byte) {
	t.Helper()
	if err := o.w.Write(msgr.MessageData{Sequence: seq, Type: typ, Version: 1, CompatVersion: 1, Front: front}.Frame()); err != nil {
		t.Fatal(err)
	}
}

type listenerFixture struct {
	mon      *fakeMgrMon
	daemon   *MgrDaemon
	listener *MgrListener
	address  msgr.Address
	ring     cephx.Key
}

func newListenerFixture(t *testing.T, options MgrListenerOptions) *listenerFixture {
	t.Helper()
	f := &listenerFixture{mon: newFakeMgrMon(t)}
	d, err := DialMgrDaemon(context.Background(), f.mon.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := d.WaitRotatingKeys(ctx, 0); err != nil {
		t.Fatal(err)
	}
	f.daemon = d
	ring, _ := mockCredential()
	f.ring = ring.value
	bound, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := bound.Addr().(*net.TCPAddr).AddrPort()
	f.address = msgr.Address{Type: 2, Nonce: 31, Endpoint: endpoint}
	options.Listener = bound
	if options.Addresses == nil {
		options.Addresses = []ManagerAddress{{Type: 2, Nonce: 31, Endpoint: endpoint}}
	}
	l, err := d.Listen(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	f.listener = l
	return f
}

func (f *listenerFixture) osd(t *testing.T, c testOSDConfig) (*testOSD, error) {
	t.Helper()
	if c.secretID == 0 {
		c.secretID = 6
	}
	if c.globalID == 0 {
		c.globalID = 777
	}
	if c.ringKey.Type() == 0 {
		c.ringKey = f.ring
	}
	if c.target == (msgr.Address{}) {
		c.target = f.address
	}
	return dialTestOSD(t, f.listener.State().Bound.String(), c)
}

func allowAll(context.Context, MgrPeerIdentity) error { return nil }

func TestMgrListenerRefusesEveryPeerWithoutAnAuthorizeHook(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{})
	if _, err := f.osd(t, testOSDConfig{osd: 2}); err == nil {
		t.Fatal("peer admitted without an Authorize hook")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refusal, err := f.listener.WaitRefusal(ctx, 0)
	if err != nil || !refusal.Refused || refusal.Stage != "authorization" || !errors.Is(refusal.Err, ErrMgrPeerDenied) || refusal.Entity != "osd.2" {
		t.Fatal("refusal", refusal, err)
	}
	if s := f.listener.State(); s.Accepted != 0 || s.Peers != 0 || s.Refused != 1 {
		t.Fatal("state", s)
	}
}

func TestMgrListenerPublishesIdentifiedPeersAndDeliversOwnedMessages(t *testing.T) {
	for _, mode := range []ConnectionMode{SecureMode, CRCMode} {
		var seen []MgrPeerIdentity
		var mu sync.Mutex
		f := newListenerFixture(t, MgrListenerOptions{ConnectionMode: mode, Authorize: func(_ context.Context, id MgrPeerIdentity) error {
			mu.Lock()
			seen = append(seen, id)
			mu.Unlock()
			return nil
		}})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		osd, err := f.osd(t, testOSDConfig{mode: mode, osd: 2})
		if err != nil {
			t.Fatal(err)
		}
		if osd.gid != 777 {
			t.Fatal("AUTH_DONE global ID", osd.gid)
		}
		d := wire.NewDecoder(osd.server.Segments[0])
		addresses := msgr.DecodeAddresses(d)
		if len(addresses) != 1 || addresses[0] != f.address || d.U64() != 4242 || d.U64() != 1 {
			t.Fatal("SERVER_IDENT identity", addresses)
		}
		peer, err := f.listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id := peer.Identity()
		if id.Generation != 1 || id.Entity != "osd.2" || id.GlobalID != 777 || id.SecretID != 6 || id.MgrGlobalID != 4242 ||
			id.PeerID != 2 || id.Caps != (MgrPeerCaps{Text: "allow profile osd"}) || id.ConnectionMode != mode ||
			id.Features != session.MgrOSDPeerFeatures || len(id.Addresses) != 1 || id.Remote.Addr() != netip.MustParseAddr("127.0.0.1") {
			t.Fatalf("identity %+v", id)
		}
		mu.Lock()
		if len(seen) != 1 || seen[0].Generation != 0 || seen[0].Entity != "osd.2" {
			t.Fatal("authorize input", seen)
		}
		mu.Unlock()
		front := []byte("raw only")
		osd.send(t, 1, rawOnlyType, front)
		m, err := peer.Next(ctx)
		if err != nil || m.Type != rawOnlyType || m.Sequence != 1 || string(m.Front) != "raw only" || m.Open != nil {
			t.Fatal("inbound message", m, err)
		}
		m.Front[0] = 'x'
		// A canceled wait consumes nothing; the next message still arrives.
		short, stop := context.WithCancel(ctx)
		stop()
		if _, err := peer.Next(short); !errors.Is(err, context.Canceled) {
			t.Fatal("canceled Next", err)
		}
		osd.send(t, 2, 0x701, nil)
		if m, err := peer.Next(ctx); err != nil || m.Type != 0x701 {
			t.Fatal("second message", m, err)
		}
		// A type the initiating side matches to its requests (here a MGR
		// command reply) is delivered like any other message.
		osd.send(t, 3, msgr.MgrCommandReplyMessage, []byte("reply"))
		if m, err := peer.Next(ctx); err != nil || m.Type != msgr.MgrCommandReplyMessage || string(m.Front) != "reply" {
			t.Fatal("reply-typed message", m, err)
		}
		// The peer's loss ends the session, after which a reconnection is a
		// new session with a new generation.
		osd.conn.Close()
		select {
		case <-peer.Done():
		case <-ctx.Done():
			t.Fatal("peer loss not observed")
		}
		if _, err := peer.Next(ctx); err == nil {
			t.Fatal("Next after loss")
		}
		again, err := f.osd(t, testOSDConfig{mode: mode, osd: 2})
		if err != nil {
			t.Fatal(err)
		}
		second, err := f.listener.Accept(ctx)
		if err != nil || second.Identity().Generation != 2 || second == peer {
			t.Fatal("reconnection", err)
		}
		again.conn.Close()
		cancel()
	}
}

func TestMgrListenerRefusalsAreRecordedWithoutPublication(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	other := f.address
	other.Nonce++
	wrong := acceptKey(0x44)
	for name, c := range map[string]testOSDConfig{
		"unknown secret ID": {osd: 1, secretID: 99},
		"wrong ring key":    {osd: 1, ringKey: wrong},
		"other target":      {osd: 1, target: other},
		"CRC to secure":     {osd: 1, mode: CRCMode},
	} {
		before := f.listener.State()
		if _, err := f.osd(t, c); err == nil {
			t.Fatal(name, "admitted")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		refusal, err := f.listener.WaitRefusal(ctx, before.Refused+before.Failed)
		cancel()
		if err != nil || !refusal.Refused {
			t.Fatal(name, refusal, err)
		}
		t.Logf("%s: %s %s", name, refusal.Stage, refusal.Reason)
	}
	if s := f.listener.State(); s.Accepted != 0 || s.Peers != 0 || s.Refused != 4 {
		t.Fatal("state", s)
	}
}

func acceptKey(fill byte) cephx.Key {
	e := wire.Encoder{}
	e.U16(2)
	e.U32(0)
	e.U32(0)
	e.U16(32)
	e.Raw(bytes.Repeat([]byte{fill}, 32))
	return cephx.DecodeKey(wire.NewDecoder(e.Data))
}

func TestMgrListenerBoundsHandshakesAndIsolatesSlowPeers(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll, MaxHandshakes: 2, HandshakeTimeout: 500 * time.Millisecond})
	// Two stalled connections hold both handshake slots; a third connection
	// is closed at once, and both stalled ones end at the deadline.
	var stalled []*testOSD
	for range 2 {
		o, err := f.osd(t, testOSDConfig{stopAfter: "connect"})
		if err != nil {
			t.Fatal(err)
		}
		stalled = append(stalled, o)
	}
	waitListener(t, f.listener, func(s MgrListenerState) bool { return s.Handshakes == 2 })
	if _, err := f.osd(t, testOSDConfig{osd: 1}); err == nil {
		t.Fatal("handshake beyond the limit admitted")
	}
	s := waitListener(t, f.listener, func(s MgrListenerState) bool { return s.Handshakes == 0 && s.Failed == 2 })
	if s.Refused != 1 || s.Recent[0].Stage != "admission" {
		t.Fatal("handshake limit refusal", s.Recent)
	}
	for _, o := range stalled {
		o.conn.Close()
	}
	// One stalled peer does not delay another peer's admission.
	slow, _ := f.osd(t, testOSDConfig{stopAfter: "connect"})
	defer slow.conn.Close()
	start := time.Now()
	osd, err := f.osd(t, testOSDConfig{osd: 1})
	if err != nil || time.Since(start) > 400*time.Millisecond {
		t.Fatal("admission waited for a slow peer", err, time.Since(start))
	}
	osd.conn.Close()
}

func waitListener(t *testing.T, l *MgrListener, ok func(MgrListenerState) bool) MgrListenerState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := l.State()
		if ok(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener state did not settle: %+v", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMgrPeerOverflowFailsAfterDraining(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll, MaxBufferedMessages: 2})
	osd, err := f.osd(t, testOSDConfig{osd: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer osd.conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := f.listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		osd.send(t, seq, rawOnlyType, []byte{byte(seq)})
	}
	<-peer.Done()
	for want := byte(1); want <= 2; want++ {
		if m, err := peer.Next(ctx); err != nil || m.Front[0] != want {
			t.Fatal("drained message", m, err)
		}
	}
	if _, err := peer.Next(ctx); !errors.Is(err, ErrMgrPeerOverflow) {
		t.Fatal("overflow cause", err)
	}
}

func TestMgrListenerCloseReleasesEveryWorkerAndWaiter(t *testing.T) {
	before := runtime.NumGoroutine()
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll, HandshakeTimeout: 10 * time.Second})
	osd, err := f.osd(t, testOSDConfig{osd: 1})
	if err != nil {
		t.Fatal(err)
	}
	stalled, _ := f.osd(t, testOSDConfig{stopAfter: "connect"})
	ctx := context.Background()
	peer, err := f.listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending, _ := f.osd(t, testOSDConfig{osd: 2}) // published, never accepted
	waitListener(t, f.listener, func(s MgrListenerState) bool { return s.Pending == 1 && s.Handshakes == 1 })
	waits := make(chan error, 2)
	go func() { _, err := peer.Next(ctx); waits <- err }()
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.listener.Accept(ctx)
		_, err := f.listener.Accept(ctx)
		waits <- err
	}()
	time.Sleep(100 * time.Millisecond)
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() { defer wg.Done(); f.listener.Close() }()
	}
	wg.Wait()
	for range 2 {
		select {
		case err := <-waits:
			if !errors.Is(err, ErrClosed) {
				t.Fatal("waiter cause", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waiter not released by Close")
		}
	}
	for _, c := range []*testOSD{osd, stalled, pending} {
		c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadAll(c.conn); err != nil {
			t.Fatal("peer socket not closed by Close", err)
		}
		c.conn.Close()
	}
	if _, err := net.Dial("tcp", f.listener.State().Bound.String()); err == nil {
		t.Fatal("listener socket still accepts")
	}
	f.daemon.Close()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("goroutines %d after Close, %d before", n, before)
	}
}

func TestMgrDaemonCloseClosesItsListeners(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	f.daemon.Close()
	if s := f.listener.State(); !s.Closed {
		t.Fatal("listener survived its daemon", s)
	}
	if _, err := f.listener.Accept(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal("Accept after daemon Close", err)
	}
	bound, _ := net.Listen("tcp", "127.0.0.1:0")
	if _, err := f.daemon.Listen(MgrListenerOptions{Listener: bound, Addresses: []ManagerAddress{{Type: 2, Endpoint: bound.Addr().(*net.TCPAddr).AddrPort()}}}); !errors.Is(err, ErrClosed) {
		t.Fatal("Listen on a closed daemon", err)
	}
}

// TestMgrDaemonFatalMonCauseClosesItsListeners ends the daemon with a fatal
// cause of its established MON session (a MonMap of another cluster): the
// daemon's failure must close its listeners as Close does.
func TestMgrDaemonFatalMonCauseClosesItsListeners(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	mon := f.mon.next(t)
	if err := mon.push(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMapAuthEpoch(20, [16]byte{2}, []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}, 2, 0)}); err != nil {
		t.Fatal(err)
	}
	state := waitDaemon(t, f.daemon, func(s MgrDaemonState) bool { return s.Err != nil })
	if !isFSIDMismatch(state.Err) {
		t.Fatal("daemon cause", state.Err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !f.listener.State().Closed && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s := f.listener.State(); !s.Closed {
		t.Fatal("listener survived its daemon's failure", s)
	}
	if _, err := f.listener.Accept(context.Background()); err == nil {
		t.Fatal("Accept after the daemon failed")
	}
}

func TestMgrListenerValidatesItsOptions(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll})
	endpoint := netip.MustParseAddrPort("127.0.0.1:1")
	for name, change := range map[string]func(*MgrListenerOptions){
		"no addresses":  func(o *MgrListenerOptions) { o.Addresses = nil },
		"msgr1 address": func(o *MgrListenerOptions) { o.Addresses = []ManagerAddress{{Type: 1, Endpoint: endpoint}} },
		"no endpoint":   func(o *MgrListenerOptions) { o.Addresses = []ManagerAddress{{Type: 2}} },
		"mode":          func(o *MgrListenerOptions) { o.ConnectionMode = 9 },
		"handshakes":    func(o *MgrListenerOptions) { o.MaxHandshakes = 2000 },
		"pending":       func(o *MgrListenerOptions) { o.MaxPeers, o.MaxPendingPeers = 2, 3 },
		"keepalive":     func(o *MgrListenerOptions) { o.KeepaliveInterval, o.KeepaliveTimeout = time.Second, time.Second },
		"frame":         func(o *MgrListenerOptions) { o.MaxFrameSize = 100 },
	} {
		bound, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		options := MgrListenerOptions{Listener: bound, Addresses: []ManagerAddress{{Type: 2, Endpoint: endpoint}}}
		change(&options)
		if l, err := f.daemon.Listen(options); err == nil {
			l.Close()
			t.Fatal(name, "accepted")
		}
		if _, err := net.Dial("tcp", bound.Addr().String()); err == nil {
			t.Fatal(name, "left the refused listener open")
		}
	}
	if _, err := f.daemon.Listen(MgrListenerOptions{}); err == nil {
		t.Fatal("accepted no listener")
	}
}

func TestMgrListenerRefusesPeersBeyondItsLimitsBeforeServerIdent(t *testing.T) {
	f := newListenerFixture(t, MgrListenerOptions{Authorize: allowAll, MaxPeers: 2, MaxPendingPeers: 1})
	first, err := f.osd(t, testOSDConfig{osd: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer first.conn.Close()
	if _, err := f.osd(t, testOSDConfig{osd: 2}); err == nil {
		t.Fatal("a peer beyond MaxPendingPeers got SERVER_IDENT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refusal, err := f.listener.WaitRefusal(ctx, 0)
	if err != nil || refusal.Stage != "authorization" || !errors.Is(refusal.Err, errMgrPeerLimit) {
		t.Fatal("pending limit", refusal, err)
	}
	if _, err := f.listener.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := f.osd(t, testOSDConfig{osd: 2})
	if err != nil {
		t.Fatal("a slot freed by Accept", err)
	}
	defer second.conn.Close()
	if _, err := f.listener.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	// Two published sessions fill MaxPeers.
	if _, err := f.osd(t, testOSDConfig{osd: 3}); err == nil {
		t.Fatal("a peer beyond MaxPeers got SERVER_IDENT")
	}
	if s := f.listener.State(); s.Peers != 2 || s.Accepted != 2 || s.Refused != 2 {
		t.Fatal("state", s)
	}
}

// acceptRecorder reports the first Accept call of the listener it wraps.
type acceptRecorder struct {
	net.Listener
	once  sync.Once
	first func()
}

func (r *acceptRecorder) Accept() (net.Conn, error) {
	r.once.Do(r.first)
	return r.Listener.Accept()
}

// TestMgrListenerWorkerIsAccountedWhenPublished closes the daemon right
// after Listen published the listener and before Listen returned. The
// daemon's Close finds the listener, so it must also wait for that
// listener's accept worker: the worker runs before Close returns, never
// after.
func TestMgrListenerWorkerIsAccountedWhenPublished(t *testing.T) {
	f := newFakeMgrMon(t)
	hooks := &mgrDaemonHooks{}
	d, err := DialMgrDaemon(context.WithValue(context.Background(), mgrDaemonHooksKey{}, hooks), f.options())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var closeReturned atomic.Bool
	hooks.afterListenerPublished = func(*MgrListener) {
		d.Close()
		closeReturned.Store(true)
	}
	bound, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan bool, 1)
	recorder := &acceptRecorder{Listener: bound, first: func() { accepted <- closeReturned.Load() }}
	l, err := d.Listen(MgrListenerOptions{Listener: recorder, Addresses: []ManagerAddress{{Type: 2, Endpoint: bound.Addr().(*net.TCPAddr).AddrPort()}}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case afterClose := <-accepted:
		if afterClose {
			t.Fatal("the accept worker started after the daemon's Close returned")
		}
	default:
		t.Fatal("the daemon's Close returned before the listener's accept worker ran")
	}
	if s := l.State(); !s.Closed {
		t.Fatal("listener survived its daemon", s)
	}
}

// rawOnlyType is a message type the peer delivers raw only (no MGR daemon
// message has it).
const rawOnlyType = 0x7f0

// TestMgrListenerCanceledWaitsConsumeNothing calls Accept with a peer
// queued and Next with a message queued, each with a canceled context.
func TestMgrListenerCanceledWaitsConsumeNothing(t *testing.T) {
	l := &MgrListener{changed: make(chan struct{}), options: MgrListenerOptions{MaxBufferedMessages: 4, MaxBufferedBytes: 1 << 20}}
	p := &MgrPeer{listener: l, changed: make(chan struct{})}
	l.pending = []*MgrPeer{p}
	if err := p.enqueue(msgr.MessageData{Type: rawOnlyType, Front: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Accept(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("Accept", err)
	}
	if _, err := p.Next(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("Next", err)
	}
	if got, err := l.Accept(context.Background()); err != nil || got != p {
		t.Fatal("the queued peer was consumed", got, err)
	}
	if m, err := p.Next(context.Background()); err != nil || m.Type != rawOnlyType {
		t.Fatal("the queued message was consumed", m, err)
	}
}

// TestMgrPeerQueuesNothingAfterItsTerminalCause delivers a message after
// the session's close callback recorded its cause, as a reader still
// holding a message when the session fails does: Next must keep returning
// the cause, with nothing delivered after it.
func TestMgrPeerQueuesNothingAfterItsTerminalCause(t *testing.T) {
	l := &MgrListener{options: MgrListenerOptions{MaxBufferedMessages: 4, MaxBufferedBytes: 1 << 20}}
	p := &MgrPeer{listener: l, changed: make(chan struct{})}
	cause := errors.New("test transport loss")
	p.closed(cause)
	if _, err := p.Next(context.Background()); !errors.Is(err, cause) {
		t.Fatal("Next before the late message", err)
	}
	if err := p.enqueue(msgr.MessageData{Type: rawOnlyType, Front: []byte{1}}); !errors.Is(err, cause) {
		t.Fatal("a message was queued after the terminal cause", err)
	}
	if _, err := p.Next(context.Background()); !errors.Is(err, cause) {
		t.Fatal("Next after the late message", err)
	}
}

// TestMgrListenerCanceledContendedWaitsConsumeNothing cancels Accept and
// Next after they sampled a live context but before they acquired their
// queue lock, and publishes a peer and a message under that lock. Neither
// existed before cancellation, so each call must return the context error
// and leave it queued.
func TestMgrListenerCanceledContendedWaitsConsumeNothing(t *testing.T) {
	l := &MgrListener{changed: make(chan struct{}), options: MgrListenerOptions{MaxBufferedMessages: 4, MaxBufferedBytes: 1 << 20}}
	p := &MgrPeer{listener: l, changed: make(chan struct{})}
	err := contendedCancel(t, &l.mu, func(ctx context.Context) error {
		_, err := l.Accept(ctx)
		return err
	}, func() {
		l.pending = append(l.pending, p)
		l.signal()
	})
	if !errors.Is(err, context.Canceled) || len(l.pending) != 1 {
		t.Fatal("Accept consumed a peer published after cancellation", err, l.pending)
	}
	err = contendedCancel(t, &p.mu, func(ctx context.Context) error {
		_, err := p.Next(ctx)
		return err
	}, func() {
		p.queue = append(p.queue, MgrInboundMessage{Type: rawOnlyType, Front: []byte{1}})
		p.sizes = append(p.sizes, 1+64)
		p.bytes += 1 + 64
		p.signal()
	})
	if !errors.Is(err, context.Canceled) || len(p.queue) != 1 {
		t.Fatal("Next consumed a message published after cancellation", err, p.queue)
	}
}

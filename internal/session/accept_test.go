package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic checks of the accepting handshake against this repository's
// own initiating code, used as an OSD-typed peer. They establish the frame
// order, refusals and identity binding of accept.go, not interoperability
// with a stock daemon, which the native MGR acceptor test covers.

func encodedTestKey(kind uint16, fill byte) []byte {
	n := 16
	if kind == cephx.AES256K {
		n = 32
	}
	e := wire.Encoder{}
	e.U16(kind)
	e.U32(0)
	e.U32(0)
	e.U16(uint16(n))
	e.Raw(bytes.Repeat([]byte{fill}, n))
	return e.Data
}

func acceptTestKey(kind uint16, fill byte) cephx.Key {
	key, err := cephx.ParseKey(base64.StdEncoding.EncodeToString(encodedTestKey(kind, fill)))
	if err != nil {
		panic(err)
	}
	return key
}

// osdTicketBlob seals a source-shaped CephXServiceTicketInfo for the
// entity, with the session key of sessionFill.
func osdTicketBlob(t testing.TB, service cephx.Key, kind uint16, sessionFill byte, entity uint32, id string, globalID uint64) []byte {
	t.Helper()
	e := wire.Encoder{}
	e.U8(1)
	e.U8(2)
	e.U32(entity)
	e.String(id)
	e.U64(globalID)
	e.U64(^uint64(0))
	e.U32(1700000000)
	e.U32(0)
	e.U32(1700003600)
	e.U32(0)
	e.U8(1)
	e.U8(0)
	caps := wire.Encoder{}
	caps.String("allow profile osd")
	e.Bytes(caps.Data)
	e.U32(0)
	e.Raw(encodedTestKey(kind, sessionFill))
	blob, err := service.Seal(0x30, e.Data)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

type acceptFixture struct {
	t        *testing.T
	kind     uint16
	service  cephx.Key
	session  cephx.Key
	secretID uint64
	address  msgr.Address
	policy   AcceptPolicy
	osdID    string
	globalID uint64
	seq      uint64
	mu       sync.Mutex
}

func newAcceptFixture(t *testing.T, kind uint16, mode ConnectionMode) *acceptFixture {
	f := &acceptFixture{t: t, kind: kind, service: acceptTestKey(kind, 0x31), session: acceptTestKey(kind, 0x52), secretID: 7, osdID: "3", globalID: 4242}
	f.address = msgr.Address{Type: 2, Nonce: 77, Endpoint: netip.MustParseAddrPort("192.0.2.10:6800")}
	f.policy = AcceptPolicy{
		Self: 0x10, ServerID: 9001, Addresses: []msgr.Address{f.address}, Peers: map[uint8]AcceptPeerFeatures{4: {MgrOSDPeerFeatures, MgrOSDPeerRequiredFeatures}}, Mode: mode,
		Service: cephx.ServiceMgr, Keys: f.lookup, HandshakeLimit: 64 << 10, MessageLimit: 1 << 20,
		GlobalSeq: func() uint64 { f.mu.Lock(); defer f.mu.Unlock(); f.seq++; return f.seq },
		Authorize: func(context.Context, AcceptedPeer) error { return nil },
	}
	return f
}

func (f *acceptFixture) lookup(id uint64) (cephx.Key, bool) {
	if id == f.secretID {
		return f.service, true
	}
	return cephx.Key{}, false
}

func (f *acceptFixture) authorizer() *cephx.Authorizer {
	c, err := cephx.NewClient("client.peer", acceptTestKey(f.kind, 0x11), 0)
	if err != nil {
		f.t.Fatal(err)
	}
	c.GlobalID = f.globalID
	c.Tickets[cephx.ServiceMgr] = cephx.Ticket{Key: f.session, SecretID: f.secretID, Blob: osdTicketBlob(f.t, f.service, f.kind, 0x52, 4, f.osdID, f.globalID), Expires: time.Now().Add(time.Hour)}
	a, err := c.Authorizer(cephx.ServiceMgr)
	if err != nil {
		f.t.Fatal(err)
	}
	return a
}

// osdPeerAuth presents an OSD entity type in HELLO.
type osdPeerAuth struct{ ServiceAuth }

func (osdPeerAuth) entityType() uint8 { return 4 }

type osdIdent struct {
	addresses             []msgr.Address
	target                *msgr.Address
	id                    uint64
	supported, required   uint64
	reconnectFirst, empty bool
}

// osdPeer authenticates as an OSD with this repository's initiating code,
// then sends a CLIENT_IDENT shaped like a stock OSD's MgrClient one.
func (f *acceptFixture) osdPeer(conn net.Conn, mode ConnectionMode, ident osdIdent) (*authenticatedChannel, msgr.Frame, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	stage := ""
	channel, err := authenticateChannel(conn, &stage, f.address, 0x10, osdPeerAuth{ServiceAuth{Authorizer: f.authorizer()}}, mode, 1<<20)
	if err != nil {
		return nil, msgr.Frame{}, err
	}
	if ident.reconnectFirst {
		e := wire.Encoder{}
		msgr.EncodeAddresses(&e, []msgr.Address{{Type: 2, Nonce: 5, Endpoint: netip.MustParseAddrPort("192.0.2.20:6801")}})
		for range 5 {
			e.U64(1)
		}
		if err := channel.write(msgr.SessionReconnect, e.Data); err != nil {
			return nil, msgr.Frame{}, err
		}
		reset, err := channel.r.Read()
		if err != nil || reset.Tag != msgr.SessionReset || !bytes.Equal(reset.Segments[0], []byte{1}) {
			return nil, msgr.Frame{}, fmt.Errorf("session reset: %v %v", reset.Tag, err)
		}
	}
	if ident.addresses == nil && !ident.empty {
		ident.addresses = []msgr.Address{{Type: 2, Nonce: 5, Endpoint: netip.MustParseAddrPort("192.0.2.20:6801")}}
	}
	target := f.address
	if ident.target != nil {
		target = *ident.target
	}
	if ident.supported == 0 {
		ident.supported = ^uint64(0)
	}
	if ident.required == 0 {
		ident.required = 1<<0 | 1<<9 | 1<<13 | RequiredFeatures
	}
	e := wire.Encoder{}
	msgr.EncodeAddresses(&e, ident.addresses)
	target.Encode(&e)
	e.U64(ident.id)
	e.U64(11)
	e.U64(ident.supported)
	e.U64(ident.required)
	e.U64(sessionFlagLossy)
	e.U64(0)
	if err := channel.write(msgr.ClientIdent, e.Data); err != nil {
		return nil, msgr.Frame{}, err
	}
	reply, err := channel.r.Read()
	return channel, reply, err
}

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		accepted <- c
	}()
	client, err = net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		t.Fatal("no accepted connection")
	}
	return client, server
}

type acceptResult struct {
	transport *Transport
	peer      AcceptedPeer
	err       error
}

func (f *acceptFixture) accept(server net.Conn) chan acceptResult {
	done := make(chan acceptResult, 1)
	go func() {
		transport, peer, err := Accept(context.Background(), server, f.policy, 3*time.Second)
		done <- acceptResult{transport, peer, err}
	}()
	return done
}

func TestAcceptAdmitsAnOSDPeerInEachModeAndKeyType(t *testing.T) {
	for _, kind := range []uint16{cephx.AES, cephx.AES256K} {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			f := newAcceptFixture(t, kind, mode)
			client, server := tcpPair(t)
			done := f.accept(server)
			channel, reply, err := f.osdPeer(client, mode, osdIdent{id: 3, supported: MgrOSDPeerFeatures | 1<<40})
			if err != nil || reply.Tag != msgr.ServerIdent {
				t.Fatalf("%d/%d: server ident %v %v", kind, mode, reply.Tag, err)
			}
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}
			d := wire.NewDecoder(reply.Segments[0])
			addresses := msgr.DecodeAddresses(d)
			gid, gseq, supported, required, flags, cookie := d.U64(), d.U64(), d.U64(), d.U64(), d.U64(), d.U64()
			if d.Done() != nil || len(addresses) != 1 || addresses[0] != f.address || gid != 9001 || gseq != 1 ||
				supported != MgrOSDPeerFeatures || required != 0x0800000000200000 || flags != 1 || cookie != 0 {
				t.Fatalf("server ident %v %d %d %#x %#x %d %d", addresses, gid, gseq, supported, required, flags, cookie)
			}
			p := result.peer
			if p.PeerType != 4 || p.Ticket.EntityID != "3" || p.Ticket.GlobalID != 4242 || p.Ticket.SecretID != 7 ||
				p.PeerID != 3 || p.PeerGlobalSeq != 11 || p.ServerGlobalSeq != 1 || p.Features != MgrOSDPeerFeatures ||
				p.Mode != mode || p.Target != f.address || result.transport.GlobalID != 4242 || result.transport.Features != MgrOSDPeerFeatures {
				t.Fatalf("accepted peer %+v", p)
			}
			if channel.globalID != 4242 {
				t.Fatal("AUTH_DONE global ID", channel.globalID)
			}
			// Both directions carry messages on the published transport.
			m := msgr.MessageData{Sequence: 1, Type: 0x700, Version: 3, CompatVersion: 1, Front: []byte("open")}
			if err := channel.w.Write(m.Frame()); err != nil {
				t.Fatal(err)
			}
			got, err := result.transport.Reader.Read()
			if err != nil {
				t.Fatal(err)
			}
			if decoded, err := msgr.DecodeMessage(got); err != nil || string(decoded.Front) != "open" {
				t.Fatal("inbound message", err)
			}
			if err := result.transport.Writer.Write(msgr.MessageData{Sequence: 1, Type: 0x701, Version: 1, CompatVersion: 1}.Frame()); err != nil {
				t.Fatal(err)
			}
			if back, err := channel.r.Read(); err != nil || back.Tag != msgr.Message {
				t.Fatal("outbound message", err)
			}
			client.Close()
			result.transport.Conn.Close()
		}
	}
}

func acceptRefusal(t *testing.T, result acceptResult, stage AcceptStage, refused bool, cause error) {
	t.Helper()
	if result.transport != nil {
		result.transport.Conn.Close()
		t.Fatal("refused peer was published")
	}
	var accept *AcceptError
	if !errors.As(result.err, &accept) || accept.Stage != stage || accept.Refused != refused || (cause != nil && !errors.Is(result.err, cause)) {
		t.Fatalf("accept error %v, want stage %s refused %v cause %v", result.err, stage, refused, cause)
	}
}

func TestAcceptRefusesModesAndAuthorizersWithBadMethod(t *testing.T) {
	// The peer offers only CRC; the acceptor allows only secure.
	f := newAcceptFixture(t, cephx.AES256K, SecureMode)
	client, server := tcpPair(t)
	done := f.accept(server)
	_, _, err := f.osdPeer(client, CRCMode, osdIdent{id: 3})
	var rejection *cephx.AuthenticationError
	if !errors.As(err, &rejection) || rejection.Code != -95 || rejection.Method != 2 {
		t.Fatal("mode refusal seen by the peer", err)
	}
	client.Close()
	acceptRefusal(t, <-done, AcceptAuthentication, true, ErrModeRefused)

	// Unknown secret ID, foreign service key and a wrong ticket key all end
	// with AUTH_BAD_METHOD -EACCES and the verifier's reason.
	for name, setup := range map[string]func(*acceptFixture){
		"unknown secret": func(f *acceptFixture) {
			f.secretID = 8
			f.policy.Keys = func(id uint64) (cephx.Key, bool) { return f.service, id == 7 }
		},
		"wrong key": func(f *acceptFixture) {
			f.policy.Keys = func(uint64) (cephx.Key, bool) { return acceptTestKey(f.kind, 0x99), true }
		},
	} {
		f := newAcceptFixture(t, cephx.AES256K, SecureMode)
		setup(f)
		client, server := tcpPair(t)
		done := f.accept(server)
		_, _, err := f.osdPeer(client, SecureMode, osdIdent{id: 3})
		if !errors.As(err, &rejection) || rejection.Code != -13 {
			t.Fatal(name, "peer saw", err)
		}
		client.Close()
		result := <-done
		var refusal *cephx.AuthorizerError
		if !errors.As(result.err, &refusal) {
			t.Fatal(name, result.err)
		}
		acceptRefusal(t, result, AcceptAuthentication, true, nil)
	}
}

func TestAcceptBindsIdentityTargetAndFeatures(t *testing.T) {
	other := msgr.Address{Type: 2, Nonce: 78, Endpoint: netip.MustParseAddrPort("192.0.2.10:6800")}
	for _, c := range []struct {
		name    string
		ident   osdIdent
		osdID   string
		stage   AcceptStage
		refused bool
		cause   error
		frame   msgr.Tag
	}{
		{"other OSD ID", osdIdent{id: 4}, "3", AcceptIdentification, true, ErrPeerIdentity, 0},
		{"non-decimal ticket ID", osdIdent{id: 3}, "03", AcceptIdentification, true, ErrPeerIdentity, 0},
		{"target nonce", osdIdent{id: 3, target: &other}, "3", AcceptIdentification, true, ErrPeerIdentity, 0},
		{"no address", osdIdent{id: 3, empty: true}, "3", AcceptIdentification, false, msgr.ErrFrame, 0},
		{"blank address", osdIdent{id: 3, addresses: []msgr.Address{{}}}, "3", AcceptIdentification, false, msgr.ErrFrame, 0},
		{"lacks SERVER_LUMINOUS", osdIdent{id: 3, supported: ^uint64(1 << 21)}, "3", AcceptIdentification, true, msgr.ErrFeatures, msgr.IdentMissingFeatures},
		{"lacks MSG_ADDR2", osdIdent{id: 3, supported: ^uint64(1 << 59)}, "3", AcceptIdentification, true, msgr.ErrFeatures, msgr.IdentMissingFeatures},
		{"requires CRUSH_MSR", osdIdent{id: 3, required: RequiredFeatures | 1<<38}, "3", AcceptIdentification, true, msgr.ErrFeatures, 0},
	} {
		f := newAcceptFixture(t, cephx.AES, CRCMode)
		f.osdID = c.osdID
		client, server := tcpPair(t)
		done := f.accept(server)
		_, reply, err := f.osdPeer(client, CRCMode, c.ident)
		if c.frame != 0 {
			if err != nil || reply.Tag != c.frame {
				t.Fatalf("%s: reply %v %v", c.name, reply.Tag, err)
			}
			want := uint64(1 << 21)
			if strings.Contains(c.name, "ADDR2") {
				want = 1 << 59
			}
			if got := wire.NewDecoder(reply.Segments[0]).U64(); got != want {
				t.Fatalf("%s: missing mask %#x", c.name, got)
			}
		} else if err == nil {
			t.Fatalf("%s: got frame %d instead of a close", c.name, reply.Tag)
		}
		client.Close()
		acceptRefusal(t, <-done, c.stage, c.refused, c.cause)
		if f.seq != 0 {
			t.Fatalf("%s consumed a global sequence", c.name)
		}
	}
}

func TestAcceptAnswersReconnectWithResetAndRunsAuthorizationLast(t *testing.T) {
	f := newAcceptFixture(t, cephx.AES256K, SecureMode)
	var seen AcceptedPeer
	f.policy.Authorize = func(_ context.Context, p AcceptedPeer) error { seen = p; return nil }
	client, server := tcpPair(t)
	done := f.accept(server)
	if _, reply, err := f.osdPeer(client, SecureMode, osdIdent{id: 3, reconnectFirst: true}); err != nil || reply.Tag != msgr.ServerIdent {
		t.Fatal("after reset", reply.Tag, err)
	}
	result := <-done
	if result.err != nil || seen.PeerID != 3 || seen.Ticket.EntityID != "3" || seen.ServerGlobalSeq != 0 {
		t.Fatal("authorization input", seen, result.err)
	}
	result.transport.Conn.Close()
	client.Close()

	denied := errors.New("not this OSD")
	f.policy.Authorize = func(context.Context, AcceptedPeer) error { return denied }
	client, server = tcpPair(t)
	done = f.accept(server)
	if _, _, err := f.osdPeer(client, SecureMode, osdIdent{id: 3}); err == nil {
		t.Fatal("denied peer got SERVER_IDENT")
	}
	client.Close()
	acceptRefusal(t, <-done, AcceptAuthorization, true, denied)
	if f.seq != 1 {
		t.Fatal("a denied peer consumed a global sequence", f.seq)
	}
}

// tamperingWriter flips the last byte of its at-th write, without
// recomputing anything: the receiver's CRC or GCM check must catch it.
type tamperingWriter struct {
	net.Conn
	writes int
	at     int
}

func (w *tamperingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.at {
		p = bytes.Clone(p)
		p[len(p)-1] ^= 1
	}
	return w.Conn.Write(p)
}

func TestAcceptRefusesTamperedSignatureAndStalledPeers(t *testing.T) {
	// A peer signature over another transcript: the peer's HELLO claims a
	// different address for the acceptor than the one it signs.
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		f := newAcceptFixture(t, cephx.AES256K, mode)
		f.policy.newVerifier = func(service uint32, keys cephx.ServiceKeyLookup, secure bool) serviceVerifier {
			return &signatureSkew{cephx.NewServiceVerifier(service, keys, secure)}
		}
		client, server := tcpPair(t)
		done := f.accept(server)
		f.osdPeer(client, mode, osdIdent{id: 3})
		client.Close()
		acceptRefusal(t, <-done, AcceptSignature, false, msgr.ErrAuthentication)
	}
	// A peer signature corrupted on the wire fails the frame check: GCM in
	// secure mode, CRC in CRC mode. Neither publishes a session.
	for mode, cause := range map[ConnectionMode]error{SecureMode: msgr.ErrAuthentication, CRCMode: msgr.ErrCRC} {
		f := newAcceptFixture(t, cephx.AES256K, mode)
		client, server := tcpPair(t)
		done := f.accept(server)
		f.osdPeer(&tamperingWriter{Conn: client, at: 5}, mode, osdIdent{id: 3})
		client.Close()
		acceptRefusal(t, <-done, AcceptSignature, false, cause)
	}
	// A stalled peer ends at the handshake deadline, and a context end
	// interrupts a handshake waiting on the peer.
	f := newAcceptFixture(t, cephx.AES, CRCMode)
	client, server := tcpPair(t)
	start := time.Now()
	_, _, err := Accept(context.Background(), server, f.policy, 200*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) || time.Since(start) > 2*time.Second {
		t.Fatal("stalled handshake", err)
	}
	client.Close()
	client, server = tcpPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	if _, _, err := Accept(ctx, server, f.policy, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled handshake", err)
	}
	client.Close()
	// A partial write of the acceptor's frame ends the handshake; nothing is
	// published from a connection whose framing broke.
	client, server = tcpPair(t)
	done := f.accept(&tamperingWriter{Conn: server, at: 2})
	f.osdPeer(client, CRCMode, osdIdent{id: 3})
	client.Close()
	if result := <-done; result.transport != nil || result.err == nil {
		t.Fatal("published after a corrupted hello", result.err)
	}
}

func isTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// signatureSkew hands Accept a session key other than the one the peer
// holds, as if the transcript the peer signed were another one.
type signatureSkew struct{ *cephx.ServiceVerifier }

func (s *signatureSkew) SessionKey() cephx.Key {
	return acceptTestKey(cephx.AES256K, 0x77)
}

func TestAcceptPolicyIsComplete(t *testing.T) {
	f := newAcceptFixture(t, cephx.AES, CRCMode)
	for name, change := range map[string]func(*AcceptPolicy){
		"mode":      func(p *AcceptPolicy) { p.Mode = 3 },
		"addresses": func(p *AcceptPolicy) { p.Addresses = nil },
		"keys":      func(p *AcceptPolicy) { p.Keys = nil },
		"authorize": func(p *AcceptPolicy) { p.Authorize = nil },
		"required":  func(p *AcceptPolicy) { p.Peers = map[uint8]AcceptPeerFeatures{4: {MgrOSDPeerFeatures, 1 << 21}} },
		"peers":     func(p *AcceptPolicy) { p.Peers = nil },
		"supported": func(p *AcceptPolicy) {
			p.Peers = map[uint8]AcceptPeerFeatures{4: {Features, MgrOSDPeerRequiredFeatures}}
		},
	} {
		policy := f.policy
		change(&policy)
		client, server := tcpPair(t)
		if _, _, err := Accept(context.Background(), server, policy, time.Second); err == nil {
			t.Fatal(name, "accepted an incomplete policy")
		}
		if _, err := server.Write([]byte{0}); err == nil {
			t.Fatal(name, "left the connection open")
		}
		client.Close()
	}
}

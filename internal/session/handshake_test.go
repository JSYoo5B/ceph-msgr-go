package session

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/wire"
)

type fixtureAuth struct {
	key    cephx.Key
	secret []byte
}

func (a fixtureAuth) Initial() ([]byte, error)                       { return []byte{10}, nil }
func (a fixtureAuth) More([]byte) ([]byte, error)                    { return nil, errors.New("unexpected challenge") }
func (a fixtureAuth) Done(uint64, []byte) (cephx.Key, []byte, error) { return a.key, a.secret, nil }

func fixtureAuthData() fixtureAuth {
	e := wire.Encoder{}
	e.U16(2)
	e.U32(0)
	e.U32(0)
	e.U16(32)
	e.Raw(make([]byte, 32))
	key, _ := cephx.ParseKey(base64.StdEncoding.EncodeToString(e.Data))
	secret := make([]byte, 40)
	for i := range secret {
		secret[i] = byte(i)
	}
	return fixtureAuth{key: key, secret: secret}
}

// This peer simulates handshake faults, not independent Ceph interoperability.
func handshakePeer(conn net.Conn, a fixtureAuth, badSignature bool, mode uint32, serverFlags uint64) error {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	rx := &recordingReader{Reader: conn, active: true}
	tx := &recordingWriter{Writer: conn, active: true}
	banner := msgr.Banner{Supported: 3, Required: 1}
	if _, err := msgr.ReadBanner(rx, banner); err != nil {
		return err
	}
	if err := msgr.WriteFull(tx, banner.Encode()); err != nil {
		return err
	}
	r, w := msgr.NewReader(rx, 0), msgr.NewWriter(tx, 0)
	if f, err := r.Read(); err != nil || f.Tag != msgr.Hello {
		return errors.New("client hello")
	}
	hello := wire.Encoder{}
	hello.U8(1)
	msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.2:0")}.Encode(&hello)
	if err := w.Write(msgr.Frame{Tag: msgr.Hello, Segments: [][]byte{hello.Data}}); err != nil {
		return err
	}
	if f, err := r.Read(); err != nil || f.Tag != msgr.AuthRequest {
		return errors.New("auth request")
	}
	if mode == 0 {
		e := wire.Encoder{}
		e.U32(2)
		rejection := int32(-13)
		e.U32(uint32(rejection))
		e.U32(1)
		e.U32(2)
		e.U32(1)
		e.U32(2)
		if badSignature {
			e.Data = e.Data[:len(e.Data)-1]
		}
		return w.Write(msgr.Frame{Tag: msgr.AuthBadMethod, Segments: [][]byte{e.Data}})
	}
	done := wire.Encoder{}
	done.U64(42)
	done.U32(mode)
	done.Bytes(nil)
	if err := w.Write(msgr.Frame{Tag: msgr.AuthDone, Segments: [][]byte{done.Data}}); err != nil {
		return err
	}
	if mode != 2 {
		return nil
	}
	signature := a.key.Signature(rx.data.Bytes())
	expected := a.key.Signature(tx.data.Bytes())
	rx.active, tx.active = false, false
	r.EnableSecure(a.secret[:16], a.secret[28:40])
	w.EnableSecure(a.secret[:16], a.secret[16:28])
	f, err := r.Read()
	if err != nil {
		return err
	}
	if f.Tag != msgr.AuthSignature || !hmac.Equal(expected, f.Segments[0]) {
		return errors.New("client transcript signature")
	}
	if badSignature {
		signature[0] ^= 1
	}
	if err := w.Write(msgr.Frame{Tag: msgr.AuthSignature, Segments: [][]byte{signature}}); err != nil {
		return err
	}
	if badSignature {
		return nil
	}
	f, err = r.Read()
	if err != nil || f.Tag != msgr.CompressionRequest {
		return errors.New("compression negotiation")
	}
	e := wire.Encoder{}
	e.U8(0)
	e.U32(0)
	if err := w.Write(msgr.Frame{Tag: msgr.CompressionDone, Segments: [][]byte{e.Data}}); err != nil {
		return err
	}
	f, err = r.Read()
	if err != nil || f.Tag != msgr.ClientIdent {
		return errors.New("client ident")
	}
	ident := wire.Encoder{}
	msgr.EncodeAddresses(&ident, nil)
	ident.U64(0)
	ident.U64(1)
	ident.U64(Features)
	ident.U64(RequiredFeatures)
	ident.U64(serverFlags)
	ident.U64(0)
	if err := w.Write(msgr.Frame{Tag: msgr.ServerIdent, Segments: [][]byte{ident.Data}}); err != nil {
		return err
	}
	_, err = r.Read()
	return err
}

func TestAuthenticatedHandshakeAndContextLifetime(t *testing.T) {
	client, server := net.Pipe()
	a := fixtureAuthData()
	peerDone := make(chan error, 1)
	go func() { peerDone <- handshakePeer(server, a, false, 2, 1) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr, err := Handshake(ctx, client, msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}, 1, 0, a, 4096, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Conn.Close()
	cancel()
	if err := tr.Writer.Write(msgr.Frame{Tag: msgr.Ack, Segments: [][]byte{make([]byte, 8)}}); err != nil {
		t.Fatal("Dial context closed established connection", err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}

func TestHandshakeRejectsTamperingAndDowngrade(t *testing.T) {
	for _, tc := range []struct {
		bad  bool
		mode uint32
	}{{true, 2}, {false, 1}} {
		client, server := net.Pipe()
		a := fixtureAuthData()
		done := make(chan error, 1)
		go func() { done <- handshakePeer(server, a, tc.bad, tc.mode, 1) }()
		_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, a, 4096, time.Second)
		if err == nil {
			t.Fatal("unsafe handshake accepted")
		}
		if tc.bad && !errors.Is(err, msgr.ErrAuthentication) {
			t.Fatal(err)
		}
		<-done
	}
}

func TestHandshakeCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Handshake(ctx, client, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), 4096, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAuthenticationRejectionPreservesCodeAndValidatesFrame(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		client, peer := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- handshakePeer(peer, fixtureAuthData(), malformed, 0, 1) }()
		_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, fixtureAuthData(), 4096, time.Second)
		var rejected *cephx.AuthenticationError
		if err == nil || errors.As(err, &rejected) == malformed {
			t.Fatal("rejection or malformed frame misclassified", err)
		}
		if !malformed && (rejected.Method != 2 || rejected.Code != -13) {
			t.Fatal(rejected)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestHandshakeRejectsUnsupportedSessionFlags(t *testing.T) {
	for _, flags := range []uint64{0, 3} {
		client, server := net.Pipe()
		done := make(chan error, 1)
		auth := fixtureAuthData()
		go func() { done <- handshakePeer(server, auth, false, 2, flags) }()
		transport, err := Handshake(context.Background(), client, msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}, 1, 0, auth, 4096, time.Second)
		if transport != nil {
			transport.Conn.Close()
		}
		<-done
		if !errors.Is(err, msgr.ErrFeatures) {
			t.Fatalf("unimplemented session flags %#x were not rejected: %v", flags, err)
		}
	}
}

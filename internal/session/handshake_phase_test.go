package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Synthetic phase-boundary checks through the existing handshake entry points.
// They show which setup phase reports a refusal and that no failed phase
// publishes a transport or leaves the caller's socket open. They are not
// native interoperability evidence.

// closeRecordingConn records whether the handshake closed the caller's end.
// The synthetic peers close their own end when they return, which makes a
// write on the caller's end fail even if the handshake leaked it, so socket
// ownership is observed on the caller's Close itself.
type closeRecordingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeRecordingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// requireUnpublishedFailure checks a failed handshake's published result. It
// must run as soon as the handshake returns, before test cleanup closes conn.
func requireUnpublishedFailure(t *testing.T, conn *closeRecordingConn, transport *Transport, err error, stage string, cause error, text string) {
	t.Helper()
	if !conn.closed.Load() {
		t.Error("failed setup left the caller's socket open")
	}
	if transport != nil {
		transport.Conn.Close()
		t.Fatal("failed setup phase published a transport")
	}
	if err == nil {
		t.Fatal("failed setup phase was accepted")
	}
	if prefix := "ceph messenger " + stage + ":"; !strings.HasPrefix(err.Error(), prefix) {
		t.Fatalf("failure reported outside phase %q: %v", stage, err)
	}
	if cause != nil && !errors.Is(err, cause) {
		t.Fatalf("failure lost its cause %v: %v", cause, err)
	}
	if text != "" && !strings.Contains(err.Error(), text) {
		t.Fatalf("failure lost %q: %v", text, err)
	}
}

func TestHandshakeAuthenticatedChannelPhaseFailuresPublishNothing(t *testing.T) {
	missing := uint64(1 << 62)
	for _, test := range []struct {
		name      string
		requested ConnectionMode
		cfg       handshakePeerConfig
		// shortSecret makes the client authenticator return 39 secret bytes.
		shortSecret bool
		stage       string
		cause       error
		text        string
	}{
		{"CRC bad signature", CRCMode, handshakePeerConfig{mode: 1, badSignature: true}, false, "signature", msgr.ErrAuthentication, ""},
		{"secure bad signature", SecureMode, handshakePeerConfig{mode: 2, badSignature: true}, false, "signature", msgr.ErrAuthentication, ""},
		{"CRC short signature", CRCMode, handshakePeerConfig{mode: 1, shortSignature: true}, false, "signature", msgr.ErrAuthentication, ""},
		{"secure short signature", SecureMode, handshakePeerConfig{mode: 2, shortSignature: true}, false, "signature", msgr.ErrAuthentication, ""},
		{"CRC refuses secure selection", CRCMode, handshakePeerConfig{mode: 2}, false, "authentication", nil, "server selected connection mode 2, requested 1"},
		{"secure refuses CRC selection", SecureMode, handshakePeerConfig{mode: 1}, false, "authentication", nil, "server selected connection mode 1, requested 2"},
		{"CRC rejected method", CRCMode, handshakePeerConfig{}, false, "authentication", nil, ""},
		{"secure short connection secret", SecureMode, handshakePeerConfig{mode: 2}, true, "authentication", nil, "incomplete authentication"},
		{"CRC truncated AUTH_DONE", CRCMode, handshakePeerConfig{mode: 1, malformedTag: msgr.AuthDone}, false, "authentication", msgr.ErrFrame, ""},
		{"secure truncated AUTH_DONE", SecureMode, handshakePeerConfig{mode: 2, malformedTag: msgr.AuthDone}, false, "authentication", msgr.ErrFrame, ""},
		{"CRC truncated COMPRESSION_DONE", CRCMode, handshakePeerConfig{mode: 1, malformedTag: msgr.CompressionDone}, false, "compression negotiation", msgr.ErrFrame, ""},
		{"secure unoffered compression", SecureMode, handshakePeerConfig{mode: 2, selectedCompression: msgr.CompAlgZstd}, false, "compression negotiation", nil, "unsupported compression method"},
		{"CRC missing features", CRCMode, handshakePeerConfig{mode: 1, missingFeatures: &missing}, false, "session identification", msgr.ErrFeatures, "peer requires 0x4000000000000000"},
		{"secure truncated SERVER_IDENT", SecureMode, handshakePeerConfig{mode: 2, malformedTag: msgr.ServerIdent}, false, "session identification", msgr.ErrFrame, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			t.Cleanup(func() { client.Close(); peer.Close() })
			fixture := fixtureAuthData()
			auth := fixture
			if test.shortSecret {
				auth.secret = auth.secret[:39]
			}
			cfg := test.cfg
			cfg.offeredMode, cfg.serverFlags = test.requested, 1
			done := make(chan error, 1)
			go func() { done <- handshakePeer(peer, fixture, cfg) }()
			conn := &closeRecordingConn{Conn: client}
			transport, err := Handshake(context.Background(), conn,
				msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")},
				1, 0, auth, test.requested, 4096, time.Second)
			requireUnpublishedFailure(t, conn, transport, err, test.stage, test.cause, test.text)
			var rejected *cephx.AuthenticationError
			if test.cfg.mode == 0 && !errors.As(err, &rejected) {
				t.Fatal("AUTH_BAD_METHOD lost its rejection", err)
			}
			<-done
		})
	}
}

func TestHandshakeIdentificationAndResumePhasesFollowAuthenticatedChannel(t *testing.T) {
	for _, kind := range []uint16{cephx.AES, cephx.AES256K} {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			name := fmt.Sprintf("key%d/mode%d", kind, mode)
			t.Run(name+"/fresh identification", func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(kind, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, freshMDSPeerDefaults()) }()
				tr, err := HandshakeFreshMDS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				if tr.Conn != client || tr.GlobalID != 42 || tr.Features != Features || tr.Resume.GlobalID != 42 || tr.Resume.ServerCookie != 17 || tr.Resume.ClientCookie == 0 {
					t.Fatal("fresh identification published inconsistent state", tr)
				}
				tr.Conn.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
			t.Run(name+"/bad signature before identification", func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(kind, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				cfg := freshMDSPeerDefaults()
				cfg.badSignature = true
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
				conn := &closeRecordingConn{Conn: client}
				tr, err := HandshakeFreshMDS(context.Background(), conn, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second)
				requireUnpublishedFailure(t, conn, tr, err, "signature", msgr.ErrAuthentication, "")
				if err := <-done; err != nil {
					t.Fatal("bad signature led to further client traffic", err)
				}
			})
			t.Run(name+"/resume", func(t *testing.T) {
				auth, fixture, err := freshMDSAuthorizer(kind, mode)
				if err != nil {
					t.Fatal(err)
				}
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				cfg := freshMDSPeerDefaults()
				cfg.reconnect = func(f msgr.Frame, r *msgr.Reader, w *msgr.Writer) error {
					got, err := decodeReconnect(f)
					if err != nil {
						return err
					}
					if got.global != 101 || got.connect != 1 || got.message != 9 {
						return fmt.Errorf("reconnect sequences %+v", got)
					}
					e := wire.Encoder{}
					e.U64(4)
					if err := w.Write(msgr.Frame{Tag: msgr.SessionReconnectOK, Segments: [][]byte{e.Data}}); err != nil {
						return err
					}
					r.Read()
					return nil
				}
				state := ResumeState{Addresses: []msgr.Address{{Type: 2, Nonce: 77}}, ClientCookie: 5, ServerCookie: 17, GlobalSeq: 100, InSeq: 9, Features: Features, GlobalID: 42}
				done := make(chan error, 1)
				go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
				tr, err := HandshakeResumeCephFS(context.Background(), client, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second, state)
				if err != nil {
					t.Fatal(err)
				}
				if tr.Conn != client || tr.GlobalID != 42 || tr.Features != Features || tr.PeerSequence != 4 || tr.Resume.GlobalSeq != 101 || tr.Resume.ConnectSeq != 1 || tr.Resume.ServerCookie != 17 {
					t.Fatal("resume published inconsistent state", tr)
				}
				tr.Conn.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
			for _, test := range []struct {
				name     string
				globalID uint64
				answer   msgr.Tag
				cause    error
				text     string
			}{
				{"resume with another identity", 43, 0, msgr.ErrAuthentication, "different client identity"},
				{"resume reset", 42, msgr.SessionReset, ErrSessionReset, ""},
				{"resume wait", 42, msgr.Wait, msgr.ErrFrame, "wait"},
			} {
				t.Run(name+"/"+test.name, func(t *testing.T) {
					auth, fixture, err := freshMDSAuthorizer(kind, mode)
					if err != nil {
						t.Fatal(err)
					}
					client, peer := net.Pipe()
					t.Cleanup(func() { client.Close(); peer.Close() })
					cfg := freshMDSPeerDefaults()
					reconnected := false
					cfg.reconnect = func(f msgr.Frame, r *msgr.Reader, w *msgr.Writer) error {
						if _, err := decodeReconnect(f); err != nil {
							return err
						}
						reconnected = true
						payload := []byte(nil)
						if test.answer == msgr.SessionReset {
							payload = []byte{1}
						}
						if err := w.Write(msgr.Frame{Tag: test.answer, Segments: [][]byte{payload}}); err != nil {
							return err
						}
						return freshMDSNoTraffic(r)
					}
					state := ResumeState{Addresses: []msgr.Address{{Type: 2, Nonce: 77}}, ClientCookie: 5, ServerCookie: 17, GlobalSeq: 100, Features: Features, GlobalID: test.globalID}
					done := make(chan error, 1)
					go func() { done <- freshMDSPeer(peer, fixture, mode, cfg) }()
					conn := &closeRecordingConn{Conn: client}
					tr, err := HandshakeResumeCephFS(context.Background(), conn, msgr.Address{Type: 2}, 0, auth, mode, 4096, time.Second, state)
					requireUnpublishedFailure(t, conn, tr, err, "session reconnect", test.cause, test.text)
					peerErr := <-done
					if test.answer == 0 {
						// The identity check precedes SESSION_RECONNECT, so the
						// peer sees the socket close after compression.
						if reconnected {
							t.Fatal("resume with another CephX identity sent SESSION_RECONNECT")
						}
						return
					}
					if peerErr != nil || !reconnected {
						t.Fatal("peer did not complete its resume answer", peerErr)
					}
				})
			}
		}
	}
}

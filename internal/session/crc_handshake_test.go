package session

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

type modeCheckingAuth struct {
	fixtureAuth
	requireSecret bool
	done          bool
}

func (a *modeCheckingAuth) Done(_ uint64, _ []byte, requireSecret bool) (cephx.Key, []byte, error) {
	a.done = true
	if requireSecret != a.requireSecret {
		return cephx.Key{}, nil, errors.New("wrong connection secret requirement")
	}
	return a.key, a.secret, nil
}

func TestCRCHandshakeAuthenticatesAndRetainsCRCFraming(t *testing.T) {
	for _, role := range []uint8{1, 16} {
		t.Run(map[uint8]string{1: "MON", 16: "MGR"}[role], func(t *testing.T) {
			client, peer := net.Pipe()
			fixture := fixtureAuthData()
			fixture.secret = nil // CRC must not depend on a GCM connection secret.
			auth := &modeCheckingAuth{fixtureAuth: fixture}
			data := msgr.Frame{Tag: msgr.Message, Segments: [][]byte{{1, 2}, []byte("front"), nil, {0, 255, 1}}}
			peerDone := make(chan error, 1)
			go func() {
				peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{
					mode: 1, offeredMode: CRCMode, peerRole: role, serverFlags: 1,
					afterIdent: func(conn net.Conn, r *msgr.Reader, w *msgr.Writer) error {
						f, err := r.Read()
						if err != nil {
							return err
						}
						if f.Tag != data.Tag || len(f.Segments) != 4 {
							return errors.New("wrong post-authentication frame")
						}
						for i, segment := range data.Segments {
							if !bytes.Equal(segment, f.Segments[i]) {
								return errors.New("post-authentication segment changed")
							}
						}
						if err := w.Write(data); err != nil {
							return err
						}
						var damaged bytes.Buffer
						if err := msgr.NewWriter(&damaged, 4096).Write(data); err != nil {
							return err
						}
						// Corrupt the data segment's checksum after framing it.
						damaged.Bytes()[damaged.Len()-1] ^= 1
						return msgr.WriteFull(conn, damaged.Bytes())
					},
				})
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tr, err := Handshake(ctx, client, msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}, role, 0, auth, CRCMode, 4096, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Conn.Close()
			if !auth.done || tr.GlobalID != 42 {
				t.Fatal("CRC skipped authentication completion")
			}
			cancel() // The setup context must not close an established CRC transport.
			if err := tr.Writer.Write(data); err != nil {
				t.Fatal(err)
			}
			if f, err := tr.Reader.Read(); err != nil || f.Tag != data.Tag || !bytes.Equal(f.Segments[3], data.Segments[3]) {
				t.Fatal("post-authentication CRC frame failed", err)
			}
			if _, err := tr.Reader.Read(); !errors.Is(err, msgr.ErrCRC) {
				t.Fatal("damaged authenticated CRC frame was accepted", err)
			}
			if err := <-peerDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHandshakePinsModeAndRequiresSignature(t *testing.T) {
	for _, test := range []struct {
		name         string
		requested    ConnectionMode
		selected     uint32
		badSignature bool
	}{
		{"CRC transcript signature", CRCMode, 1, true},
		{"secure refuses CRC", SecureMode, 1, false},
		{"CRC refuses secure", CRCMode, 2, false},
		{"CRC refuses unknown", CRCMode, 99, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			fixture := fixtureAuthData()
			peerDone := make(chan error, 1)
			go func() {
				peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{mode: test.selected, offeredMode: test.requested, badSignature: test.badSignature, serverFlags: 1})
			}()
			_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, fixture, test.requested, 4096, time.Second)
			if err == nil || (test.badSignature && !errors.Is(err, msgr.ErrAuthentication)) || (!test.badSignature && !strings.Contains(err.Error(), "server selected connection mode")) {
				t.Fatal("mode or signature violation was accepted or misclassified", err)
			}
			if err := <-peerDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}

type continuingAuth struct{ fixtureAuth }

func (a continuingAuth) More([]byte) ([]byte, error) { return nil, nil }

func TestCRCHandshakeCannotCompleteWithoutAuthDone(t *testing.T) {
	client, peer := net.Pipe()
	fixture := fixtureAuthData()
	peerDone := make(chan error, 1)
	go func() {
		peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 1, offeredMode: CRCMode, authMoreRounds: 8})
	}()
	_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, continuingAuth{fixture}, CRCMode, 4096, time.Second)
	if err == nil || !strings.Contains(err.Error(), "incomplete authentication") {
		t.Fatal("CRC proceeded after the challenge limit without AUTH_DONE", err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
}

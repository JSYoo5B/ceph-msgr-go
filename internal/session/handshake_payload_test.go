package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestCompleteMalformedHandshakePayloadIsProtocolFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		tag  msgr.Tag
	}{
		{"HELLO", msgr.Hello},
		{"AUTH_REPLY_MORE", msgr.AuthReplyMore},
		{"AUTH_BAD_METHOD", msgr.AuthBadMethod},
		{"AUTH_DONE", msgr.AuthDone},
		{"COMPRESSION_DONE", msgr.CompressionDone},
		{"IDENT_MISSING_FEATURES", msgr.IdentMissingFeatures},
		{"SERVER_IDENT", msgr.ServerIdent},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, peer := net.Pipe()
			auth := fixtureAuthData()
			mode := uint32(2)
			if test.tag == msgr.AuthBadMethod {
				mode = 0
			}
			peerDone := make(chan error, 1)
			go func() {
				peerDone <- handshakePeer(peer, auth, handshakePeerConfig{mode: mode, serverFlags: 1, malformedTag: test.tag})
			}()
			_, err := Handshake(context.Background(), client, msgr.Address{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}, 1, 0, auth, 4096, time.Second)
			if !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("complete malformed payload was classified as transport loss: %v", err)
			}
			if err := <-peerDone; err != nil {
				t.Fatal("peer did not transmit the complete frame", err)
			}
		})
	}
}

func TestCompleteMalformedCephXPayloadIsProtocolFailure(t *testing.T) {
	for _, more := range []bool{true, false} {
		t.Run(fmt.Sprint("challenge=", more), func(t *testing.T) {
			client, peer := net.Pipe()
			fixture := fixtureAuthData()
			auth, err := cephx.NewClient("client.test", fixture.key)
			if err != nil {
				t.Fatal(err)
			}
			peerDone := make(chan error, 1)
			go func() {
				peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 2, serverFlags: 1, authMore: more})
			}()
			_, err = Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, MonAuth{Client: auth}, 4096, time.Second)
			if !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("complete nested CephX payload was classified as transport loss: %v", err)
			}
			// A peer sending empty AUTH_DONE cannot complete this handshake.
			// The client closes the socket after parsing its complete payload.
			<-peerDone
		})
	}
}

func TestCompleteEncryptedMgrChallengeRetainsDecodeFailure(t *testing.T) {
	fixture := fixtureAuthData()
	challenge, err := fixture.key.Seal(0x11, []byte{1}) // Valid encryption; the challenge nonce is missing.
	if err != nil {
		t.Fatal(err)
	}
	auth, err := cephx.NewClient("client.test", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	auth.GlobalID = 42
	auth.Tickets[cephx.ServiceMgr] = cephx.Ticket{Key: fixture.key, Expires: time.Now().Add(time.Hour)}
	authorizer, err := auth.Authorizer(cephx.ServiceMgr)
	if err != nil {
		t.Fatal(err)
	}
	client, peer := net.Pipe()
	peerDone := make(chan error, 1)
	go func() {
		peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 2, serverFlags: 1, authMore: true, authMorePayload: challenge, peerRole: 16})
	}()
	_, err = Handshake(context.Background(), client, msgr.Address{Type: 2}, 16, 0, MgrAuth{Authorizer: authorizer}, 4096, time.Second)
	if !errors.Is(err, msgr.ErrFrame) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("complete authenticated MGR challenge was classified as network truncation", err)
	}
	if err := <-peerDone; err != nil {
		t.Fatal("peer did not transmit the complete challenge frame", err)
	}
}

type failedProofAuth struct {
	fixtureAuth
	cause error
}

func (a failedProofAuth) More([]byte) ([]byte, error) { return nil, a.cause }
func (a failedProofAuth) Done(uint64, []byte) (cephx.Key, []byte, error) {
	return cephx.Key{}, nil, a.cause
}

func TestAuthenticatorNonEncodingFailuresRetainTheirClasses(t *testing.T) {
	for _, cause := range []error{
		&cephx.AuthenticationError{Method: 2, Code: -13},
		cephx.ErrIntegrity,
		errors.New("local proof generation failed"),
	} {
		for _, more := range []bool{true, false} {
			t.Run(fmt.Sprint(cause, "/challenge=", more), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				peerDone := make(chan error, 1)
				go func() {
					peerDone <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 2, serverFlags: 1, authMore: more})
				}()
				_, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 1, 0, failedProofAuth{fixtureAuth: fixture, cause: cause}, 4096, time.Second)
				if !errors.Is(err, cause) || errors.Is(err, msgr.ErrFrame) {
					t.Errorf("non-encoding authenticator failure was relabeled as malformed data: %v", err)
				}
				var rejection *cephx.AuthenticationError
				if _, explicit := cause.(*cephx.AuthenticationError); explicit && (!errors.As(err, &rejection) || rejection.Method != 2 || rejection.Code != -13) {
					t.Errorf("explicit rejection class or server code lost: %v", err)
				}
				<-peerDone
			})
		}
	}
}

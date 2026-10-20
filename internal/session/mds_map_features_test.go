package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// Embedding the real policy must forward its private fixed boolean seam.
// The transcript is synthetic and does not establish native MON admission.
type mdsMapHandshakeAuth struct {
	MonAuth
	fixture fixtureAuth
}

func (a mdsMapHandshakeAuth) Initial() ([]byte, error)      { return a.fixture.Initial() }
func (a mdsMapHandshakeAuth) More(p []byte) ([]byte, error) { return a.fixture.More(p) }
func (a mdsMapHandshakeAuth) Done(id uint64, p []byte, secret bool) (cephx.Key, []byte, error) {
	return a.fixture.Done(id, p, secret)
}

func TestMDSMapFeaturePolicyAddsOnlyMDSENCOnOptInMON(t *testing.T) {
	const mdsENC = uint64(1 << 29) // Pinned ceph_features.h MDSENC.
	for _, role := range []uint8{0, 1, 2, 4, 8, 16, 32, 255} {
		for _, enabled := range []bool{false, true} {
			for _, pgV8 := range []bool{false, true} {
				mon := MonAuth{MDSMaps: enabled, ReplicatedPGV8: pgV8}
				embedded := mdsMapHandshakeAuth{MonAuth: mon}
				for _, auth := range []Authenticator{mon, &mon, embedded, &embedded} {
					gotSupported, gotRequired := handshakeFeatures(role, auth)
					wantSupported, wantRequired := featuresForRole(role), RequiredFeatures
					if role == 1 {
						if enabled {
							wantSupported |= mdsENC
							wantRequired |= mdsENC
						}
						if pgV8 {
							wantSupported |= 1 << 30
							wantRequired |= 1 << 30
						}
					}
					if gotSupported != wantSupported || gotRequired != wantRequired {
						t.Fatalf("MDS opt-in escaped its MON-only fixed mask: role=%d enabled=%t pgV8=%t auth=%T got=%x/%x want=%x/%x", role, enabled, pgV8, auth, gotSupported, gotRequired, wantSupported, wantRequired)
					}
				}
			}
		}
	}
}

func TestMDSMapHandshakeRequiresMDSENCBeforeApplicationTraffic(t *testing.T) {
	const mdsENC = uint64(1 << 29)
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tc := range []struct {
			name                                        string
			enabled, peerSupports, peerRequires, reject bool
		}{
			{name: "default unchanged"},
			{name: "opt-in rejects missing encoding", enabled: true, reject: true},
			{name: "opt-in negotiates current encoding", enabled: true, peerSupports: true, peerRequires: true},
			{name: "default rejects server-required encoding", peerSupports: true, peerRequires: true, reject: true},
		} {
			t.Run(fmt.Sprintf("mode%d/%s", mode, tc.name), func(t *testing.T) {
				client, peer := net.Pipe()
				t.Cleanup(func() { client.Close(); peer.Close() })
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := mdsMapHandshakeAuth{MonAuth: MonAuth{MDSMaps: tc.enabled}, fixture: fixture}
				peerSupported, peerRequired := featuresForRole(1), RequiredFeatures
				if tc.peerSupports {
					peerSupported |= mdsENC
				}
				if tc.peerRequires {
					peerRequired |= mdsENC
				}
				advertised := make(chan uint64, 1)
				done := make(chan error, 1)
				go func() {
					done <- handshakePeer(peer, fixture, handshakePeerConfig{
						mode: uint32(mode), offeredMode: mode, peerRole: 1, serverFlags: 1,
						serverSupported: peerSupported, serverRequired: peerRequired,
						identFeatures: func(bits uint64) error { advertised <- bits; return nil },
						afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
							frame, err := r.Read()
							if err == nil {
								return fmt.Errorf("unexpected application/control frame %d", frame.Tag)
							}
							if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
								return err
							}
							return nil
						},
					})
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				transport, err := Handshake(ctx, client, msgr.Address{Type: 2}, 1, 0, auth, mode, 4096, time.Second)
				want := featuresForRole(1)
				if tc.enabled {
					want |= mdsENC
				}
				if tc.reject {
					if transport != nil {
						transport.Conn.Close()
						t.Fatal("unsupported MDS encoding produced a transport")
					}
					if !errors.Is(err, msgr.ErrFeatures) {
						t.Fatal("required MDS encoding was silently downgraded", err)
					}
				} else {
					if err != nil || transport == nil {
						t.Fatal("supported MDS map feature policy rejected", err)
					}
					if transport.Features != want {
						transport.Conn.Close()
						t.Fatal("negotiated MDS map features differ", transport.Features, want)
					}
					transport.Conn.Close()
				}
				select {
				case bits := <-advertised:
					if bits != want {
						t.Fatal("MDS map opt-in advertised an uncontrolled feature mask", bits, want)
					}
				case <-ctx.Done():
					t.Fatal("peer did not observe feature identification", ctx.Err())
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal("MDS encoding refusal allowed application traffic", err)
					}
				case <-ctx.Done():
					t.Fatal("peer did not terminate", ctx.Err())
				}
			})
		}
	}
}

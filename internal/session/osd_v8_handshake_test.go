package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// These synthetic peers exercise negotiated wire policy and authenticated
// Messenger framing. They do not establish native Ceph or object I/O support.
// Embedding the real ServiceAuth policy retains its private boolean seam while
// the existing fixture supplies deterministic authentication transcript proofs.
type replicatedPGV8HandshakeAuth struct {
	ServiceAuth
	fixture fixtureAuth
}

func (a replicatedPGV8HandshakeAuth) Initial() ([]byte, error)      { return a.fixture.Initial() }
func (a replicatedPGV8HandshakeAuth) More(p []byte) ([]byte, error) { return a.fixture.More(p) }
func (a replicatedPGV8HandshakeAuth) Done(id uint64, p []byte, secret bool) (cephx.Key, []byte, error) {
	return a.fixture.Done(id, p, secret)
}

func TestReplicatedPGV8HandshakeFeaturePolicyIsFixedAndOSDOnly(t *testing.T) {
	// Pinned include/ceph_features.h:8/9 locator+PG,21/57 combined
	// incarnation-2 feature,30 HASHPSPOOL and56 request encoding. Neither
	// new reply encoding58 nor SERVER_SQUID49 is an OSD profile capability.
	const profile = uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57)
	if replicatedPGV8Features != profile {
		t.Fatal("fixed profile changed", replicatedPGV8Features)
	}
	for _, role := range []uint8{1, 16, 4} {
		for _, enabled := range []bool{false, true} {
			for _, auth := range []Authenticator{ServiceAuth{ReplicatedPGV8: enabled}, &ServiceAuth{ReplicatedPGV8: enabled}} {
				supported, required := handshakeFeatures(role, auth)
				wantSupported, wantRequired := featuresForRole(role), RequiredFeatures
				if role == 4 && enabled {
					wantSupported |= profile
					wantRequired |= profile
				}
				if supported != wantSupported || required != wantRequired {
					t.Fatal("profile escaped role or changed a default", role, enabled, supported, required)
				}
				if role == 4 && supported&(1<<58|1<<49|monAdmissionFeatures&^crushV2AdmissionFeature) != 0 {
					t.Fatal("OSD profile added unsupported reply, server or MON bits")
				}
			}
		}
	}
}

func TestReplicatedPGV8HandshakeRejectsEveryMissingProfileCapabilityBeforeApplication(t *testing.T) {
	const profile = uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57)
	full := Features | profile
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, missing := range []uint{8, 9, 21, 30, 56, 57} {
			t.Run(fmt.Sprintf("mode%d/missing%d", mode, missing), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{ReplicatedPGV8: true}, fixture: fixture}
				done := make(chan error, 1)
				advertised := make(chan uint64, 1)
				go func() {
					done <- handshakePeer(peer, fixture, handshakePeerConfig{
						mode: uint32(mode), offeredMode: mode, peerRole: 4, serverFlags: 1,
						serverSupported: full &^ (uint64(1) << missing), serverRequired: RequiredFeatures,
						identFeatures: func(bits uint64) error { advertised <- bits; return nil },
						afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
							frame, err := r.Read()
							if err == nil {
								return fmt.Errorf("missing profile bit%d permitted application/control frame%d", missing, frame.Tag)
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
				transport, err := Handshake(ctx, client, msgr.Address{Type: 2}, 4, 0, auth, mode, 4096, time.Second)
				if transport != nil {
					transport.Conn.Close()
					t.Fatal("missing required profile capability produced a transport")
				}
				if !errors.Is(err, msgr.ErrFeatures) {
					t.Fatal("profile silently downgraded", err)
				}
				if got := <-advertised; got != full|crushV2AdmissionFeature {
					t.Fatal("peer did not receive fixed profile", got)
				}
				if err := <-done; err != nil {
					t.Fatal("rejected handshake transmitted application traffic", err)
				}
			})
		}
	}
}

func TestReplicatedPGV8HandshakeIntersectsFeaturesWithoutNewReplyOrSquid(t *testing.T) {
	const profile = uint64(1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57)
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, peerRequired := range []uint64{RequiredFeatures | profile, RequiredFeatures | 1<<58, RequiredFeatures | 1<<49, RequiredFeatures | 1<<18} {
			t.Run(fmt.Sprintf("mode%d/required%x", mode, peerRequired), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{ReplicatedPGV8: true}, fixture: fixture}
				done := make(chan error, 1)
				go func() {
					done <- handshakePeer(peer, fixture, handshakePeerConfig{
						mode: uint32(mode), offeredMode: mode, peerRole: 4, serverFlags: 1,
						serverSupported: Features | profile | 1<<58 | 1<<49 | 1<<18, serverRequired: peerRequired,
						identFeatures: func(bits uint64) error {
							if bits != Features|profile|crushV2AdmissionFeature {
								return errors.New("OSD profile advertised uncontrolled features")
							}
							return nil
						},
						afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
							_, err := r.Read()
							if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
								return nil
							}
							if err == nil {
								return errors.New("unexpected application frame")
							}
							return err
						},
					})
				}()
				transport, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 4, 0, auth, mode, 4096, time.Second)
				if peerRequired == RequiredFeatures|profile {
					if err != nil || transport == nil {
						t.Fatal("fixed profile failed negotiation", err)
					}
					if transport.Features != Features|profile {
						t.Fatal("unsupported server features entered negotiated set", transport.Features)
					}
					transport.Conn.Close()
				} else if transport != nil || !errors.Is(err, msgr.ErrFeatures) {
					if transport != nil {
						transport.Conn.Close()
					}
					t.Fatal("unsupported peer requirement accepted", err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestReplicatedPGV8HandshakeDoesNotDowngradeSecureMode(t *testing.T) {
	client, peer := net.Pipe()
	fixture := fixtureAuthData()
	auth := replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{ReplicatedPGV8: true}, fixture: fixture}
	done := make(chan error, 1)
	go func() {
		done <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 1, offeredMode: SecureMode, peerRole: 4, serverFlags: 1,
			identFeatures: func(uint64) error { return errors.New("security mismatch reached feature identification") },
		})
	}()
	transport, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 4, 0, auth, SecureMode, 4096, time.Second)
	if transport != nil {
		transport.Conn.Close()
		t.Fatal("secure profile silently downgraded")
	}
	if err == nil || !strings.Contains(err.Error(), "server selected connection mode") {
		t.Fatal("security refusal class changed", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

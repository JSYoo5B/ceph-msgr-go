package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/cephx"
	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

// Synthetic authentication transcript proofs reuse the real MonAuth profile
// policy without claiming native Ceph admission or object interoperability.
type replicatedPGV8MONHandshakeAuth struct {
	MonAuth
	fixture fixtureAuth
}

func (a replicatedPGV8MONHandshakeAuth) Initial() ([]byte, error)      { return a.fixture.Initial() }
func (a replicatedPGV8MONHandshakeAuth) More(p []byte) ([]byte, error) { return a.fixture.More(p) }
func (a replicatedPGV8MONHandshakeAuth) Done(id uint64, p []byte, secret bool) (cephx.Key, []byte, error) {
	return a.fixture.Done(id, p, secret)
}

func TestReplicatedPGV8MONProfileAddsOnlyOptInHashPSPool(t *testing.T) {
	const hashPSPool = uint64(1 << 30) // Pinned ceph_features.h OSDHASHPSPOOL, incarnation 1.
	for _, role := range []uint8{1, 4, 16} {
		for _, enabled := range []bool{false, true} {
			for _, auth := range []Authenticator{MonAuth{ReplicatedPGV8: enabled}, &MonAuth{ReplicatedPGV8: enabled}} {
				supported, required := handshakeFeatures(role, auth)
				wantSupported, wantRequired := featuresForRole(role), RequiredFeatures
				if role == 1 && enabled {
					wantSupported |= hashPSPool
					wantRequired |= hashPSPool
				}
				if supported != wantSupported || required != wantRequired {
					t.Fatal("MON opt-in escaped its fixed role/mask", role, enabled, supported, required)
				}
				if role == 1 && enabled && supported^featuresForRole(1) != hashPSPool {
					t.Fatal("MON opt-in widened admission beyond HASHPSPOOL")
				}
			}
		}
	}
	// The pre-existing unconditional admission exception stays exactly fixed.
	if monAdmissionFeatures != 1<<18|1<<25|1<<41|1<<48|1<<58 {
		t.Fatal("MON admission exception changed")
	}
}

func TestReplicatedPGV8MONHandshakeRequiresHashPSPoolWithoutDefaultDowngrade(t *testing.T) {
	const hashPSPool = uint64(1 << 30)
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tc := range []struct {
			name                                    string
			optIn, peerHash, requireHash, wantError bool
		}{
			{name: "default unchanged"},
			{name: "opt-in missing required peer capability", optIn: true, wantError: true},
			{name: "opt-in capability negotiated", optIn: true, peerHash: true, requireHash: true},
			{name: "default cannot claim server-required capability", peerHash: true, requireHash: true, wantError: true},
		} {
			t.Run(fmt.Sprintf("mode%d/%s", mode, tc.name), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := replicatedPGV8MONHandshakeAuth{MonAuth: MonAuth{ReplicatedPGV8: tc.optIn}, fixture: fixture}
				peerSupported, peerRequired := featuresForRole(1), RequiredFeatures
				if tc.peerHash {
					peerSupported |= hashPSPool
				}
				if tc.requireHash {
					peerRequired |= hashPSPool
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
				if tc.wantError {
					if transport != nil {
						transport.Conn.Close()
						t.Fatal("unsupported MON pool capability produced a transport")
					}
					if !errors.Is(err, msgr.ErrFeatures) {
						t.Fatal("MON profile requirement was silently downgraded", err)
					}
				} else {
					if transport == nil || err != nil {
						t.Fatal("supported MON profile failed", err)
					}
					want := featuresForRole(1)
					if tc.optIn {
						want |= hashPSPool
					}
					if transport.Features != want {
						t.Fatal("MON negotiated feature set differs", transport.Features, want)
					}
					transport.Conn.Close()
				}
				want := featuresForRole(1)
				if tc.optIn {
					want |= hashPSPool
				}
				if bits := <-advertised; bits != want {
					t.Fatal("MON advertised incorrect scoped capability", bits, want)
				}
				if err := <-done; err != nil {
					t.Fatal("MON feature failure allowed application traffic", err)
				}
			})
		}
	}
}

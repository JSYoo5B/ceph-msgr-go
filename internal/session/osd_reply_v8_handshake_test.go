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

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestReplicatedPGReplyV8FeaturePolicyIsFixedAndOSDOnly(t *testing.T) {
	const profile uint64 = 1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57 | 1<<58
	if replicatedPGReplyV8Features != profile {
		t.Fatal("fixed reply profile changed", replicatedPGReplyV8Features)
	}
	for _, role := range []uint8{1, 2, 4, 16} {
		for _, old := range []bool{false, true} {
			for _, reply := range []bool{false, true} {
				a := ServiceAuth{ReplicatedPGV8: old, ReplicatedPGReplyV8: reply}
				if a.replicatedPGV8() != (old || reply) || a.replicatedPGReplyV8() != reply {
					t.Fatal("profile selector did not include its base", old, reply)
				}
				for _, auth := range []Authenticator{a, &a, replicatedPGV8HandshakeAuth{ServiceAuth: a}} {
					supported, required := handshakeFeatures(role, auth)
					wantSupported, wantRequired := featuresForRole(role), RequiredFeatures
					if role == 4 && (old || reply) {
						wantSupported |= replicatedPGV8Features
						wantRequired |= replicatedPGV8Features
					}
					if role == 4 && reply {
						wantSupported |= profile
						wantRequired |= profile
					}
					if supported != wantSupported || required != wantRequired {
						t.Fatal("reply profile escaped role or changed defaults", role, old, reply, supported, required)
					}
					if role == 4 && supported&(1<<18|1<<25|1<<41|1<<48|1<<49) != 0 {
						t.Fatal("OSD reply profile copied MON or Squid feature policy")
					}
				}
			}
		}
	}
}

// These are authenticated synthetic Messenger transcripts, using the existing
// AES256K fixture. They establish admission and framing policy, not native OSD
// reply decoding or Ceph interoperability, which have separate oracle tests.
func TestReplicatedPGReplyV8HandshakeAdmission(t *testing.T) {
	const profile uint64 = 1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<56 | 1<<57 | 1<<58
	full := Features | profile
	type scenario struct {
		name                string
		auth                ServiceAuth
		supported, required uint64
		accept              bool
	}
	newAuth := ServiceAuth{ReplicatedPGReplyV8: true}
	cases := []scenario{
		{"new_reply_required", newAuth, full, RequiredFeatures | profile, true},
		{"reply_missing", newAuth, full &^ (1 << 58), RequiredFeatures, false},
		{"default_rejects_reply_requirement", ServiceAuth{}, full, RequiredFeatures | 1<<58, false},
		{"old_rejects_reply_requirement", ServiceAuth{ReplicatedPGV8: true}, full, RequiredFeatures | 1<<58, false},
		{"old_stays_old_when_reply_optional", ServiceAuth{ReplicatedPGV8: true}, full, RequiredFeatures, true},
	}
	for _, missing := range []uint{8, 9, 21, 30, 56, 57} {
		cases = append(cases, scenario{fmt.Sprintf("base_missing%d", missing), newAuth, full &^ (uint64(1) << missing), RequiredFeatures, false})
	}
	for _, unsupported := range []uint{18, 25, 41, 48, 49} {
		cases = append(cases, scenario{fmt.Sprintf("unsupported_required%d", unsupported), newAuth,
			full | (uint64(1) << unsupported), RequiredFeatures | (uint64(1) << unsupported), false})
	}
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("mode%d/%s", mode, tc.name), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := replicatedPGV8HandshakeAuth{ServiceAuth: tc.auth, fixture: fixture}
				advertised, _ := handshakeFeatures(4, auth)
				done := make(chan error, 1)
				go func() {
					done <- handshakePeer(peer, fixture, handshakePeerConfig{
						mode: uint32(mode), offeredMode: mode, peerRole: 4, serverFlags: sessionFlagLossy,
						serverSupported: tc.supported, serverRequired: tc.required,
						identFeatures: func(bits uint64) error {
							if bits != advertised {
								return fmt.Errorf("unexpected ClientIdent features: %x, want %x", bits, advertised)
							}
							return nil
						},
						afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
							frame, err := r.Read()
							if err == nil {
								return fmt.Errorf("unexpected application/control traffic: %d", frame.Tag)
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
				}
				if tc.accept {
					if err != nil || transport == nil {
						t.Fatal("implemented profile refused", err)
					}
					if transport.Features != advertised&tc.supported {
						t.Fatal("negotiated features differ from intersection", transport.Features)
					}
				} else if transport != nil || !errors.Is(err, msgr.ErrFeatures) {
					t.Fatal("unsupported or missing capability permitted downgrade", err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestReplicatedPGReplyV8HandshakeDoesNotDowngradeSecurity(t *testing.T) {
	client, peer := net.Pipe()
	fixture := fixtureAuthData()
	auth := replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{ReplicatedPGReplyV8: true}, fixture: fixture}
	done := make(chan error, 1)
	go func() {
		done <- handshakePeer(peer, fixture, handshakePeerConfig{mode: 1, offeredMode: SecureMode, peerRole: 4, serverFlags: sessionFlagLossy,
			identFeatures: func(uint64) error { return errors.New("security mismatch reached identification") },
		})
	}()
	transport, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, 4, 0, auth, SecureMode, 4096, time.Second)
	if transport != nil {
		transport.Conn.Close()
		t.Fatal("secure reply profile silently downgraded")
	}
	if err == nil || !strings.Contains(err.Error(), "server selected connection mode") {
		t.Fatal("security refusal class changed", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedPGReplyV8HandshakeCannotEnableOSDFeaturesOnOtherRoles(t *testing.T) {
	for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
		for _, role := range []uint8{1, 2, 16} {
			t.Run(fmt.Sprintf("mode%d/role%d", mode, role), func(t *testing.T) {
				client, peer := net.Pipe()
				fixture := fixtureAuthData()
				if mode == CRCMode {
					fixture.secret = nil
				}
				auth := replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{ReplicatedPGReplyV8: true}, fixture: fixture}
				// MON already admits bit 58 for its own CRUSH policy. Requiring
				// OSD's bit 21 there must still fail; MDS/MGR admit neither bit.
				extra := uint64(1 << 58)
				if role == 1 {
					extra = 1 << 21
				}
				done := make(chan error, 1)
				go func() {
					done <- handshakePeer(peer, fixture, handshakePeerConfig{
						mode: uint32(mode), offeredMode: mode, peerRole: role, serverFlags: sessionFlagLossy,
						serverSupported: featuresForRole(role) | replicatedPGReplyV8Features,
						serverRequired:  RequiredFeatures | extra,
						identFeatures: func(bits uint64) error {
							if bits != featuresForRole(role) {
								return errors.New("OSD feature selection escaped its daemon role")
							}
							return nil
						},
						afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
							if _, err := r.Read(); errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
								return nil
							} else if err != nil {
								return err
							}
							return errors.New("unsupported role transmitted application traffic")
						},
					})
				}()
				transport, err := Handshake(context.Background(), client, msgr.Address{Type: 2}, role, 0, auth, mode, 4096, time.Second)
				if transport != nil {
					transport.Conn.Close()
				}
				if transport != nil || !errors.Is(err, msgr.ErrFeatures) {
					t.Fatal("OSD reply selector enabled a different daemon role", err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

package session

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

func TestOSDCachePoolFeaturesAreOptInAndRoleIsolated(t *testing.T) {
	const cache = uint64(1 << 35)
	for _, role := range []uint8{1, 2, 4, 16} {
		for _, enabled := range []bool{false, true} {
			for _, modern := range []int{0, 1, 2, 3} {
				mon := MonAuth{OSDCachePools: enabled, ReplicatedPGV8: modern != 0, MDSMaps: true}
				monBase := mon
				monBase.OSDCachePools = false
				service := ServiceAuth{OSDCachePools: enabled, ReplicatedPGV8: modern == 1, ReplicatedPGReplyV8: modern == 2, ReplicatedPGRequestV9: modern == 3}
				serviceBase := service
				serviceBase.OSDCachePools = false
				for _, tc := range []struct {
					auth, base Authenticator
					target     uint8
				}{{mon, monBase, 1}, {&mon, monBase, 1}, {service, serviceBase, 4}, {&service, serviceBase, 4}} {
					got, required := handshakeFeatures(role, tc.auth)
					want, wantRequired := handshakeFeatures(role, tc.base)
					if enabled && role == tc.target {
						want |= cache
						wantRequired |= cache
					}
					if got != want || required != wantRequired {
						t.Fatal("cache capability changed another role/profile", role, enabled, modern, got, required, want, wantRequired)
					}
				}
			}
		}
	}
	if monAdmissionFeatures != 1<<18|1<<25|1<<36|1<<41|1<<48|1<<58 {
		t.Fatal("MON CRUSH admission exception changed")
	}
}

func TestOSDCachePoolHandshakeRequiresCapabilityWithoutFallback(t *testing.T) {
	const cache = uint64(1 << 35)
	for _, role := range []uint8{1, 4} {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			for _, tc := range []struct {
				name                             string
				optIn, peer, required, wantError bool
			}{
				{name: "default"}, {name: "opt-in", optIn: true, peer: true, required: true},
				{name: "missing-capability", optIn: true, wantError: true},
				{name: "default-refuses-required", peer: true, required: true, wantError: true},
			} {
				t.Run(fmt.Sprintf("role%d/mode%d/%s", role, mode, tc.name), func(t *testing.T) {
					client, peer := net.Pipe()
					defer peer.Close()
					fixture := fixtureAuthData()
					if mode == CRCMode {
						fixture.secret = nil
					}
					var auth Authenticator
					if role == 1 {
						auth = replicatedPGV8MONHandshakeAuth{MonAuth: MonAuth{OSDCachePools: tc.optIn}, fixture: fixture}
					} else {
						auth = replicatedPGV8HandshakeAuth{ServiceAuth: ServiceAuth{OSDCachePools: tc.optIn}, fixture: fixture}
					}
					supported, required := featuresForRole(role), RequiredFeatures
					if tc.peer {
						supported |= cache
					}
					if tc.required {
						required |= cache
					}
					done := make(chan error, 1)
					go func() {
						done <- handshakePeer(peer, fixture, handshakePeerConfig{mode: uint32(mode), offeredMode: mode, peerRole: role, serverFlags: 1, serverSupported: supported, serverRequired: required, identFeatures: func(bits uint64) error {
							if (bits&cache != 0) != tc.optIn {
								return fmt.Errorf("wrong cache bit advertisement")
							}
							return nil
						}})
					}()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					tr, err := Handshake(ctx, client, msgr.Address{Type: 2}, role, 0, auth, mode, 4096, time.Second)
					if tc.wantError {
						if tr != nil {
							tr.Conn.Close()
							t.Fatal("unsupported cache capability admitted")
						}
						if !errors.Is(err, msgr.ErrFeatures) {
							t.Fatal("feature refusal changed", err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if (tr.Features&cache != 0) != tc.optIn {
							t.Fatal("negotiated cache capability differs")
						}
						tr.Conn.Close()
					}
					client.Close()
					select {
					case <-done:
					case <-ctx.Done():
						t.Fatal("peer handshake did not finish")
					}
				})
			}
		}
	}
}

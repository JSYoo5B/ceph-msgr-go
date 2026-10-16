package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestReplicatedPGRequestV9FeaturePolicyIsFixedAndOSDOnly(t *testing.T) {
	const profile uint64 = 1<<8 | 1<<9 | 1<<21 | 1<<30 | 1<<49 | 1<<56 | 1<<57 | 1<<58
	if replicatedPGRequestV9Features != profile {
		t.Fatal("untraced v9 mask changed")
	}
	a := ServiceAuth{ReplicatedPGRequestV9: true}
	if !a.replicatedPGV8() || !a.replicatedPGReplyV8() || !a.replicatedPGRequestV9() {
		t.Fatal("new fixed profile omitted its base")
	}
	for _, role := range []uint8{1, 2, 4, 16} {
		for _, auth := range []Authenticator{a, &a, replicatedPGV8HandshakeAuth{ServiceAuth: a}} {
			got, required := handshakeFeatures(role, auth)
			want, wantRequired := featuresForRole(role), RequiredFeatures
			if role == 4 {
				want |= profile
				wantRequired |= profile
			}
			if got != want || required != wantRequired {
				t.Fatal("v9 escaped role or changed defaults", role, got, required)
			}
			if role != 4 && got&(1<<49) != 0 || role == 4 && got&(1<<18|1<<25|1<<38|1<<41|1<<48) != 0 {
				t.Fatal("v9 copied unsupported service/CRUSH features", role, got)
			}
		}
	}
	for _, a := range []ServiceAuth{{}, {ReplicatedPGV8: true}, {ReplicatedPGReplyV8: true}} {
		got, required := handshakeFeatures(4, a)
		if got&(1<<49) != 0 || required&(1<<49) != 0 {
			t.Fatal("older fixed profile gained v9")
		}
	}
}

func TestReplicatedPGRequestV9HandshakeRequiresEveryFixedCapability(t *testing.T) {
	full := Features | replicatedPGRequestV9Features
	type scenario struct {
		name                string
		auth                ServiceAuth
		supported, required uint64
		accept              bool
	}
	cases := []scenario{{"new-fixed-profile", ServiceAuth{ReplicatedPGRequestV9: true}, full, RequiredFeatures | replicatedPGRequestV9Features, true}, {"v8-refuses-v9-required", ServiceAuth{ReplicatedPGReplyV8: true}, full, RequiredFeatures | 1<<49, false}, {"v6-refuses-v9-required", ServiceAuth{}, full, RequiredFeatures | 1<<49, false}, {"v8-with-peer-v9-optional", ServiceAuth{ReplicatedPGReplyV8: true}, full, RequiredFeatures, true}, {"v9-refuses-msr", ServiceAuth{ReplicatedPGRequestV9: true}, full | 1<<38, RequiredFeatures | 1<<38, false}}
	for _, missing := range []uint{8, 9, 21, 30, 49, 56, 57, 58} {
		cases = append(cases, scenario{fmt.Sprintf("missing%d", missing), ServiceAuth{ReplicatedPGRequestV9: true}, full &^ (uint64(1) << missing), RequiredFeatures, false})
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
					done <- handshakePeer(peer, fixture, handshakePeerConfig{mode: uint32(mode), offeredMode: mode, peerRole: 4, serverFlags: sessionFlagLossy, serverSupported: tc.supported, serverRequired: tc.required, identFeatures: func(bits uint64) error {
						if bits != advertised {
							return fmt.Errorf("unexpected v9 ClientIdent %x", bits)
						}
						return nil
					}, afterIdent: func(_ net.Conn, r *msgr.Reader, _ *msgr.Writer) error {
						frame, err := r.Read()
						if err == nil {
							return fmt.Errorf("unexpected application/control after admission: %d", frame.Tag)
						}
						if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
							return err
						}
						return nil
					}})
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				tr, err := Handshake(ctx, client, msgr.Address{Type: 2}, 4, 0, auth, mode, 4096, time.Second)
				if tr != nil {
					tr.Conn.Close()
				}
				if tc.accept {
					if err != nil || tr == nil || tr.Features != advertised&tc.supported {
						t.Fatal("fixed v9 admission failed", err)
					}
				} else if tr != nil || !errors.Is(err, msgr.ErrFeatures) {
					t.Fatal("v9 was silently downgraded or widened", err)
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

type featureAdmissionProfile struct {
	name                string
	role                uint8
	mon                 MonAuth
	service             ServiceAuth
	policy              handshakePolicy
	supported, required uint64
}

// Include combinations, not only the individual opt-ins: their role-scoped
// additions must compose without replacing an earlier required capability.
func featureAdmissionProfiles() []featureAdmissionProfile {
	profiles := []featureAdmissionProfile{{name: "mgr", role: 16}}
	for bits := 0; bits < 8; bits++ {
		profiles = append(profiles, featureAdmissionProfile{
			name: fmt.Sprintf("mon-%d", bits), role: 1,
			mon: MonAuth{MDSMaps: bits&1 != 0, ReplicatedPGV8: bits&2 != 0, OSDCachePools: bits&4 != 0},
		})
	}
	for version := 0; version < 4; version++ {
		for cache := 0; cache < 2; cache++ {
			profiles = append(profiles, featureAdmissionProfile{
				name: fmt.Sprintf("osd-%d-cache%d", version, cache), role: 4,
				service: ServiceAuth{ReplicatedPGV8: version == 1, ReplicatedPGReplyV8: version == 2,
					ReplicatedPGRequestV9: version == 3, OSDCachePools: cache != 0},
			})
		}
	}
	for i := range profiles {
		profile := &profiles[i]
		var auth Authenticator
		if profile.role == 1 {
			auth = profile.mon
		} else if profile.role == 4 {
			auth = profile.service
		}
		profile.supported, profile.required = handshakeFeatures(profile.role, auth)
	}
	return append(profiles,
		featureAdmissionProfile{name: "fresh-mds", role: 2, policy: freshMDSHandshake,
			supported: Features, required: RequiredFeatures},
		featureAdmissionProfile{name: "cephfs", role: 2, policy: freshCephFSHandshake,
			supported: CephFSFeatures, required: CephFSFeatures&^Features | RequiredFeatures},
	)
}

// Exercise complete authenticated peer transcripts and the actual admission
// decision. This is synthetic feature policy, not a native Ceph observation or
// proof of every operation associated with an aliased feature bit.
func checkFeatureAdmission(t *testing.T, profile featureAdmissionProfile, mode ConnectionMode, supported, required uint64, accept bool) {
	t.Helper()
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	fixture := fixtureAuthData()
	if mode == CRCMode {
		fixture.secret = nil
	}
	var auth Authenticator = fixture
	if profile.role == 1 {
		auth = replicatedPGV8MONHandshakeAuth{MonAuth: profile.mon, fixture: fixture}
	} else if profile.role == 4 {
		auth = replicatedPGV8HandshakeAuth{ServiceAuth: profile.service, fixture: fixture}
	}
	var flags uint64
	if profile.policy == lossyHandshake {
		flags = sessionFlagLossy
	}
	done := make(chan error, 1)
	go func() {
		done <- handshakePeer(peer, fixture, handshakePeerConfig{
			mode: uint32(mode), offeredMode: mode, peerRole: profile.role,
			serverFlags: flags, serverCookie: 17, serverSupported: supported, serverRequired: required,
			identFeatures: func(got uint64) error {
				if got != profile.supported {
					return fmt.Errorf("client advertised %#x, want %#x", got, profile.supported)
				}
				return nil
			},
			afterIdent: func(_ net.Conn, reader *msgr.Reader, _ *msgr.Writer) error {
				frame, err := reader.Read()
				if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
					return nil
				}
				if err != nil {
					return err
				}
				return fmt.Errorf("admission emitted unexpected application/control tag %d", frame.Tag)
			},
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	transport, err := handshake(ctx, client, msgr.Address{Type: 2}, profile.role, 0,
		auth, mode, 4096, time.Second, profile.policy, nil, 1)
	if transport != nil {
		transport.Conn.Close()
	}
	if accept {
		if err != nil || transport == nil {
			t.Fatalf("supported feature refused: transport=%v err=%v", transport != nil, err)
		}
		if got := transport.Features; got != supported&profile.supported {
			t.Fatalf("negotiation inherited unimplemented peer bits: %#x", got)
		}
	} else if transport != nil || !errors.Is(err, msgr.ErrFeatures) {
		t.Fatalf("missing/unsupported requirement: transport=%v err=%v", transport != nil, err)
	}
	if err := <-done; err != nil {
		t.Fatal("peer did not complete the feature-admission transcript:", err)
	}
}

func TestEveryPeerRequiredFeatureIsRoleAndProfileScoped(t *testing.T) {
	for _, profile := range featureAdmissionProfiles() {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			for bit := uint(0); bit < 64; bit++ {
				t.Run(fmt.Sprintf("%s/mode%d/required%d", profile.name, mode, bit), func(t *testing.T) {
					mask := uint64(1) << bit
					checkFeatureAdmission(t, profile, mode, ^uint64(0), RequiredFeatures|mask, profile.supported&mask != 0)
				})
			}
		}
	}
}

func TestEveryLocalRequiredFeatureIsCheckedBeforeTransportPublication(t *testing.T) {
	for _, profile := range featureAdmissionProfiles() {
		for _, mode := range []ConnectionMode{CRCMode, SecureMode} {
			for bit := uint(0); bit < 64; bit++ {
				if profile.required&(uint64(1)<<bit) == 0 {
					continue
				}
				t.Run(fmt.Sprintf("%s/mode%d/missing%d", profile.name, mode, bit), func(t *testing.T) {
					checkFeatureAdmission(t, profile, mode, ^(uint64(1) << bit), RequiredFeatures, false)
				})
			}
		}
	}
}

func TestAdvertisedFeatureIncarnationsIncludeTheirMarkers(t *testing.T) {
	// Active incarnations in the pinned header; the old retired meanings of
	// these bits must never be inferred from an incomplete reused mask.
	markers := map[uint]uint64{
		2:  1<<28 | 1<<57, // SERVER_NAUTILUS, incarnation 3.
		21: 1 << 57,       // SERVER_LUMINOUS and its aliases, incarnation 2.
		28: 1 << 57,       // SERVER_MIMIC, incarnation 2.
		49: 1 << 57,       // SERVER_SQUID, incarnation 2.
	}
	for _, profile := range featureAdmissionProfiles() {
		for _, mask := range []uint64{profile.supported, profile.required} {
			for bit, marker := range markers {
				if mask&(uint64(1)<<bit) != 0 && mask&marker != marker {
					t.Errorf("%s mask %#x has incomplete incarnation for bit %d", profile.name, mask, bit)
				}
			}
		}
	}
}

func TestFeatureSemanticsCoversEveryAdvertisedBit(t *testing.T) {
	doc, err := os.ReadFile("FEATURE_SEMANTICS.md")
	if err != nil {
		t.Fatal(err)
	}
	var advertised uint64
	for _, profile := range featureAdmissionProfiles() {
		advertised |= profile.supported
	}
	var documented uint64
	for _, line := range strings.Split(string(doc), "\n") {
		cells := strings.Split(line, "|")
		if len(cells) < 6 {
			continue
		}
		bit, err := strconv.ParseUint(strings.TrimSpace(cells[1]), 10, 6)
		if err != nil {
			continue
		}
		mask := uint64(1) << bit
		if documented&mask != 0 {
			t.Errorf("duplicate feature bit %d", bit)
		}
		for _, cell := range cells[2 : len(cells)-1] {
			if strings.TrimSpace(cell) == "" {
				t.Errorf("bit %d has an empty semantic boundary", bit)
			}
		}
		documented |= mask
	}
	if documented != advertised {
		t.Fatalf("semantic rows missing %#x; undeclared %#x", advertised&^documented, documented&^advertised)
	}
	for _, link := range regexp.MustCompile(`\[[^\]]+\]\(([^)]+)\)`).FindAllStringSubmatch(string(doc), -1) {
		info, err := os.Stat(link[1])
		if err != nil || info.IsDir() {
			t.Errorf("missing code/contract reference %s", link[1])
		}
	}
}

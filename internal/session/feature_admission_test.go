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

// featureObligations states, for every bit any session profile advertises,
// its active name and incarnation in Ceph v20.2.4 include/ceph_features.h and
// the obligation and limit of the roles that advertise it. Incarnation 1 has
// no marker, 2 also needs bit 57 and 3 also needs bits 57 and 28, so
// SERVER_NAUTILUS means 2+28+57, SERVER_MIMIC 28+57, SERVER_LUMINOUS and its
// aliases 21+57 and SERVER_SQUID 49+57. These names select encodings; they do
// not establish a peer release. SERVER_TENTACLE (50+57) is deliberately not
// advertised. Names that share a mask cannot be told apart by negotiation, so
// a profile advertising bit 21 bounds the whole promise of its aliases. The
// role/profile masks in TestFeatureMasksByRole, not a union of these names,
// are what ClientIdent advertises. Admission, codec parity and operation
// results are separate evidence; the coverage check below cannot prove the
// statements correct.
var featureObligations = map[uint]string{
	1:  "NOSRCADDR/1: base header2 has no legacy source-entity field; the authenticated connection identifies the peer",
	2:  "SERVER_NAUTILUS/3: base current-encoding marker with 28+57; no support for older families or daemon roles",
	3:  "FLOCK/1: CephFS flock and reconnect wire fields; the consumer owns lock state, waits and interrupts",
	4:  "SUBSCRIBE2/1: base current MON subscription encoding; a watch ACK is not authorization or delivered data",
	5:  "MONNAMES/1: base keeps named MON-map members and exact named targets, with no silent fallback",
	7:  "DIRLAYOUTHASH/1: CephFS directory layout metadata; hash and rank selection stay with the caller",
	8:  "OBJECTLOCATOR/1: OSD locator pool, namespace, key and hash encoding; the caller supplies placement",
	9:  "PGID64/1: MON and OSD 64-bit pool and PG; map acquisition applies no inner map and computes no placement",
	15: "MONENC/1: base current MON map and address forms; envelope parsing does not prove older-release support",
	18: "CRUSH_TUNABLES/1: MON-only admission; never placement capability on another role",
	21: "SERVER_LUMINOUS, RESEND_ON_SPLIT, RADOS_BACKOFF, OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS/2: modern OSD profiles with actual-PG framing and bounded backoff; upmap changes and broader CRUSH semantics are unverified",
	23: "MSG_AUTH/1: base msgr2 transcript verification and CRC/secure framing; no msgr1 transport or per-message signature",
	25: "CRUSH_TUNABLES2/1: MON-only admission; no CRUSH computation",
	26: "CREATEPOOLID/1: CephFS current create/reply layout with the inode layout pool; not pool management",
	27: "REPLY_CREATE_INODE/1: CephFS keeps the create reply's inode identity; the consumer keeps inode state",
	28: "SERVER_MIMIC/2: base, with its bit-57 marker; supplies the incarnation-3 marker of bit 2, not a release claim",
	29: "MDSENC/1: EnableMDSMaps adds and requires it on MON only, for current MDS maps; fresh MDS transport does not inherit it",
	30: "OSDHASHPSPOOL/1: modern OSD profile on MON and OSD; the caller supplies raw hash and actual PG, nothing is folded or mapped",
	35: "OSD_CACHEPOOL/1: EnableOSDCachePools on MON and OSD only; bounded IGNORE_CACHE, no flush/evict/pin/overlay policy",
	36: "CRUSH_V2/1: MON and OSD admission once any EC rule exists; computes no placement and establishes no EC I/O",
	37: "EXPORT_PEER/1: CephFS keeps cap export/import peer metadata; the consumer owns handoff and per-rank state",
	40: "MDS_INLINE_DATA/1: CephFS keeps inline inode data and version; OSD I/O and file/cache policy are separate",
	41: "CRUSH_TUNABLES3, OSD_PRIMARY_AFFINITY/1: MON-only admission; no primary selection or placement",
	42: "MSGR_KEEPALIVE2/1: base keepalive and ACK timestamps for liveness; no execution or durability acknowledgement",
	47: "MDS_QUOTA/1: CephFS decodes quota and rstat pushes; the consumer enforces limits and keeps inode state",
	48: "CRUSH_V4/1: MON-only admission; no CRUSH computation",
	49: "SERVER_SQUID/2: only the untraced request-v9/reply-v8 OSD profile requires 49+57; no OTEL, trace acceptance or CRUSH_MSR",
	56: "NEW_OSDOP_ENCODING/1: modern OSD actual-PG request v8/v9; the base v6 profile refuses the requirement and never picks v8 from an optional peer bit",
	57: "MON_STATEFUL_SUB, SERVER_JEWEL/1: base subscription cursor and incarnation marker; a cursor is not full history and renewal replays nothing",
	58: "CRUSH_TUNABLES5, NEW_OSDOPREPLY_ENCODING, FS_FILE_LAYOUT_V2/1: MON admission; OSD reply-v8/v9 profiles require strict reply v8; CephFS requires current layouts; the old OSD profile never gains reply v8",
	59: "FS_BTIME, FS_CHANGE_ATTR, MSG_ADDR2/1: base requires address vectors; CephFS also keeps birth/change metadata; other sessions are not filesystem clients",
	61: "CEPHX_V2/1: base CephX v2 authorizer and proof; requested tickets are not permissions or completion",
}

// TestEveryAdvertisedFeatureHasAnObligation requires featureObligations to
// cover exactly the union of bits the admission profiles advertise, so a newly
// advertised bit needs its own stated obligation. Unsupported required bits,
// including CRUSH_MSR, SERVER_TENTACLE and reserved 62/63, are refused by the
// admission matrix above.
func TestEveryAdvertisedFeatureHasAnObligation(t *testing.T) {
	var advertised, stated uint64
	for _, profile := range featureAdmissionProfiles() {
		advertised |= profile.supported
	}
	for bit, obligation := range featureObligations {
		if bit > 63 || strings.TrimSpace(obligation) == "" {
			t.Errorf("bit %d: empty or out-of-range obligation", bit)
			continue
		}
		stated |= uint64(1) << bit
	}
	if stated != advertised {
		t.Fatalf("obligations missing %#x; undeclared %#x", advertised&^stated, stated&^advertised)
	}
}

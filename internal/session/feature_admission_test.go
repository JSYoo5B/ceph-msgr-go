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

// featureBit is one Messenger feature bit of the pinned source: its active
// names, their incarnation, and what this repository promises when a session
// profile advertises the bit. An empty obligation means no profile advertises
// it.
type featureBit struct {
	names       string // Active DEFINE_CEPH_FEATURE names; "-" when only retired names remain.
	incarnation int    // 0 when no name is active.
	obligation  string
}

// tentacleFeatureBits is the closed 64-bit feature set of Ceph v20.2.4
// (commit 7f793731f1b39eb4f465e960113d2363c311b964) include/ceph_features.h,
// one line per bit with its active names and incarnation; retired meanings
// are excluded. Advertising a bit changes its own line. Incarnation 1 has no
// marker, 2 also needs bit 57 and 3 also needs bits 57 and 28, so
// SERVER_NAUTILUS means 2+28+57, SERVER_MIMIC 28+57, SERVER_LUMINOUS and its
// aliases 21+57 and SERVER_SQUID 49+57. These names select encodings; they do
// not establish a peer release. Names that share a mask cannot be told apart
// by negotiation, so a profile advertising bit 21 bounds the whole promise of
// its aliases. The role/profile masks in TestFeatureMasksByRole, not a union
// of these names, are what ClientIdent advertises. Admission, codec parity and
// operation results are separate evidence; the checks below cannot prove an
// obligation's statement correct.
var tentacleFeatureBits = [64]featureBit{
	0:  {"UID", 1, "MGR acceptor only, to OSD peers: the stock OSD requires it of its MGR; it carries no field of its own"},
	1:  {"NOSRCADDR", 1, "base header2 has no legacy source-entity field; the authenticated connection identifies the peer"},
	2:  {"SERVER_NAUTILUS", 3, "base current-encoding marker with 28+57; no support for older families or daemon roles"},
	3:  {"FLOCK", 1, "CephFS flock and reconnect wire fields; the consumer owns lock state, waits and interrupts"},
	4:  {"SUBSCRIBE2", 1, "base current MON subscription encoding; a watch ACK is not authorization or delivered data"},
	5:  {"MONNAMES", 1, "base keeps named MON-map members and exact named targets, with no silent fallback"},
	6:  {"RECONNECT_SEQ", 1, ""},
	7:  {"DIRLAYOUTHASH", 1, "CephFS directory layout metadata; hash and rank selection stay with the caller"},
	8:  {"OBJECTLOCATOR", 1, "OSD locator pool, namespace, key and hash encoding; the caller supplies placement"},
	9:  {"PGID64", 1, "MON and OSD 64-bit pool and PG; map acquisition applies no inner map and computes no placement; an OSD peer of the MGR acceptor encodes PG IDs it sends in this form, delivered raw"},
	10: {"INCSUBOSDMAP", 1, ""},
	11: {"PGPOOL3", 1, ""},
	12: {"OSDREPLYMUX", 1, ""},
	13: {"OSDENC", 1, "MGR acceptor only, to OSD peers: current OSD type encodings in the messages it delivers raw; the acceptor decodes none of them"},
	14: {"SERVER_KRAKEN", 2, ""},
	15: {"MONENC", 1, "base current MON map and address forms; envelope parsing does not prove older-release support"},
	16: {"SERVER_OCTOPUS, OSD_REPOP_MLCOD", 3, ""},
	17: {"OS_PERF_STAT_NS", 3, ""},
	18: {"CRUSH_TUNABLES", 1, "MON-only admission; never placement capability on another role"},
	19: {"OSD_PGLOG_HARDLIMIT", 2, ""},
	20: {"SERVER_PACIFIC", 3, ""},
	21: {"SERVER_LUMINOUS, RESEND_ON_SPLIT, RADOS_BACKOFF, OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS", 2, "modern OSD profiles with actual-PG framing and bounded backoff; upmap changes and broader CRUSH semantics are unverified; the MGR acceptor requires it of OSD peers as native DaemonServer does, claiming nothing about its aliases"},
	22: {"OSD_FIXED_COLLECTION_LIST", 2, ""},
	23: {"MSG_AUTH", 1, "base msgr2 transcript verification and CRC/secure framing; no msgr1 transport or per-message signature"},
	24: {"RECOVERY_RESERVATION_2", 2, ""},
	25: {"CRUSH_TUNABLES2", 1, "MON-only admission; no CRUSH computation"},
	26: {"CREATEPOOLID", 1, "CephFS current create/reply layout with the inode layout pool; not pool management"},
	27: {"REPLY_CREATE_INODE", 1, "CephFS keeps the create reply's inode identity; the consumer keeps inode state"},
	28: {"SERVER_MIMIC", 2, "base, with its bit-57 marker; supplies the incarnation-3 marker of bit 2, not a release claim"},
	29: {"MDSENC", 1, "EnableMDSMaps adds and requires it on MON only, for current MDS maps; fresh MDS transport does not inherit it"},
	30: {"OSDHASHPSPOOL", 1, "modern OSD profile on MON and OSD; the caller supplies raw hash and actual PG, nothing is folded or mapped"},
	31: {"SERVER_REEF", 3, ""},
	32: {"STRETCH_MODE", 3, ""},
	33: {"SERVER_QUINCY", 3, ""},
	34: {"RANGE_BLOCKLIST", 3, ""},
	35: {"OSD_CACHEPOOL", 1, "EnableOSDCachePools on MON and OSD only; bounded IGNORE_CACHE, no flush/evict/pin/overlay policy"},
	36: {"CRUSH_V2", 1, "MON and OSD admission once any EC rule exists; computes no placement and establishes no EC I/O"},
	37: {"EXPORT_PEER", 1, "CephFS keeps cap export/import peer metadata; the consumer owns handoff and per-rank state"},
	38: {"CRUSH_MSR", 2, ""}, // Not enabled by the request-v9 profile.
	39: {"OSDMAP_ENC", 1, ""},
	40: {"MDS_INLINE_DATA", 1, "CephFS keeps inline inode data and version; OSD I/O and file/cache policy are separate"},
	41: {"CRUSH_TUNABLES3, OSD_PRIMARY_AFFINITY", 1, "MON-only admission; no primary selection or placement"},
	42: {"MSGR_KEEPALIVE2", 1, "base keepalive and ACK timestamps for liveness; no execution or durability acknowledgement"},
	43: {"OSD_POOLRESEND", 1, ""},
	44: {"NVMEOFHA", 2, ""},
	45: {"NVMEOFHAMAP", 2, ""},
	46: {"OSD_FADVISE_FLAGS", 1, ""},
	47: {"MDS_QUOTA", 1, "CephFS decodes quota and rstat pushes; the consumer enforces limits and keeps inode state"},
	48: {"CRUSH_V4", 1, "MON-only admission; no CRUSH computation"},
	49: {"SERVER_SQUID", 2, "only the untraced request-v9/reply-v8 OSD profile requires 49+57; no OTEL, trace acceptance or CRUSH_MSR"},
	50: {"SERVER_TENTACLE", 2, ""}, // Deliberately absent: a Tentacle-only client need not promise every Tentacle daemon feature.
	51: {"-", 0, ""},
	52: {"-", 0, ""},
	53: {"-", 0, ""},
	54: {"-", 0, ""},
	55: {"-", 0, ""},
	56: {"NEW_OSDOP_ENCODING", 1, "modern OSD actual-PG request v8/v9; the base v6 profile refuses the requirement and never picks v8 from an optional peer bit"},
	57: {"MON_STATEFUL_SUB, SERVER_JEWEL", 1, "base subscription cursor and incarnation marker; a cursor is not full history and renewal replays nothing"},
	58: {"CRUSH_TUNABLES5, NEW_OSDOPREPLY_ENCODING, FS_FILE_LAYOUT_V2", 1, "MON admission; OSD reply-v8/v9 profiles require strict reply v8; CephFS requires current layouts; the old OSD profile never gains reply v8"},
	59: {"FS_BTIME, FS_CHANGE_ATTR, MSG_ADDR2", 1, "base requires address vectors; CephFS also keeps birth/change metadata; other sessions are not filesystem clients"},
	60: {"OSD_RECOVERY_DELETES", 1, ""},
	61: {"CEPHX_V2", 1, "base CephX v2 authorizer and proof; requested tickets are not permissions or completion"},
	62: {"RESERVED", 1, ""}, // A sentinel; never used.
	63: {"-", 0, ""},
}

// TestFeatureBitTableMatchesTheAdvertisedProfiles binds tentacleFeatureBits
// to handshakeFeatures through the admission profiles: the bits with an
// obligation are exactly the union the profiles advertise, a bit with no
// active name is never advertised, and every advertised or required mask
// carries the incarnation markers of each of its bits. Unsupported required
// bits, including CRUSH_MSR, SERVER_TENTACLE and reserved 62/63, are refused
// by the admission matrix above.
func TestFeatureBitTableMatchesTheAdvertisedProfiles(t *testing.T) {
	markers := map[int]uint64{1: 0, 2: 1 << 57, 3: 1<<57 | 1<<28}
	var advertised, stated uint64
	for bit, feature := range tentacleFeatureBits {
		if feature.names == "" || (feature.names == "-") != (feature.incarnation == 0) {
			t.Errorf("bit %d: names %q and incarnation %d disagree", bit, feature.names, feature.incarnation)
		}
		if _, ok := markers[feature.incarnation]; !ok && feature.incarnation != 0 {
			t.Errorf("bit %d: unknown incarnation %d", bit, feature.incarnation)
		}
		if strings.TrimSpace(feature.obligation) != "" {
			if feature.incarnation == 0 {
				t.Errorf("bit %d has no active name but states an obligation", bit)
			}
			stated |= uint64(1) << bit
		}
	}
	// The MGR acceptor's SERVER_IDENT mask is advertised too. Its required
	// mask is the native one, bit 21 without the bit-57 marker (accept.go),
	// so only its supported mask is held to the marker rule.
	acceptor := featureAdmissionProfile{name: "mgr-acceptor-osd-peer", supported: MgrOSDPeerFeatures}
	for _, profile := range append(featureAdmissionProfiles(), acceptor) {
		advertised |= profile.supported
		for _, mask := range []uint64{profile.supported, profile.required} {
			for bit, feature := range tentacleFeatureBits {
				marker := markers[feature.incarnation]
				if mask&(uint64(1)<<bit) != 0 && mask&marker != marker {
					t.Errorf("%s mask %#x lacks the incarnation %d markers of bit %d", profile.name, mask, feature.incarnation, bit)
				}
			}
		}
	}
	if stated != advertised {
		t.Fatalf("obligations missing for %#x; stated for unadvertised %#x", advertised&^stated, stated&^advertised)
	}
}

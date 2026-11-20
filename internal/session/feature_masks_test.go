package session

import "testing"

// TestFeatureMasksByRole pins what each session role and opt-in profile
// advertises (supported) and requires of the peer (required) in ClientIdent,
// against the bit names of Ceph v20.2.4 include/ceph_features.h. A bit is
// advertised only when the codec or admission rule it promises is
// implemented; a Messenger feature bit does not establish a release family,
// and the full CEPH_FEATURES_ALL mask is never used. Rows marked "adds" are
// additions to the selected role or profile, made to both masks only when
// the caller selects that profile. The meaning of each advertised bit is in
// featureObligations.
func TestFeatureMasksByRole(t *testing.T) {
	delta := func(role uint8, auth Authenticator) [2]uint64 {
		baseS, baseR := handshakeFeatures(role, nil)
		gotS, gotR := handshakeFeatures(role, auth)
		return [2]uint64{gotS &^ baseS, gotR &^ baseR}
	}
	full := func(role uint8) [2]uint64 {
		s, r := handshakeFeatures(role, nil)
		return [2]uint64{s, r}
	}
	for _, row := range []struct {
		profile string
		got     [2]uint64
		want    [2]uint64 // supported, required
	}{
		// MGR and any role without a row below: NOSRCADDR (1),
		// SERVER_NAUTILUS (2), SUBSCRIBE2 (4), MONNAMES (5), MONENC (15),
		// MSG_AUTH (23), SERVER_MIMIC (28), MSGR_KEEPALIVE2 (42),
		// SERVER_JEWEL/MON_STATEFUL_SUB (57), MSG_ADDR2 (59), CEPHX_V2 (61).
		{"base", full(16), [2]uint64{0x2a00040010808036, 0x0800000000000000}},
		// MON adds PGID64 (9) and the MON-only CRUSH admission bits 18, 25,
		// 36, 41, 48 and 58: admission only, never OSDMap subscription
		// permission or CRUSH/object I/O.
		{"MON", full(1), [2]uint64{0x2e01061012848236, 0x0800000000000000}},
		// OSD adds OBJECTLOCATOR (8), PGID64 (9) and CRUSH_V2 (36, admission
		// only: required once any erasure-code rule exists).
		{"OSD", full(4), [2]uint64{0x2a00041010808336, 0x0800000000000000}},
		// Opt-in OSD profiles add bits 8, 9, 21 (SERVER_LUMINOUS and its
		// aliases), 30, 56 and 57; reply-v8 adds 58; request-v9 adds 49
		// (SERVER_SQUID), never CRUSH_MSR (38).
		{"adds OSD replicated-PG v8", [2]uint64{replicatedPGV8Features, replicatedPGV8Features}, [2]uint64{0x0300000040200300, 0x0300000040200300}},
		{"adds OSD reply-v8", [2]uint64{replicatedPGReplyV8Features, replicatedPGReplyV8Features}, [2]uint64{0x0700000040200300, 0x0700000040200300}},
		{"adds OSD request-v9", [2]uint64{replicatedPGRequestV9Features, replicatedPGRequestV9Features}, [2]uint64{0x0702000040200300, 0x0702000040200300}},
		// A fresh MDS session offers the base only.
		{"fresh MDS", [2]uint64{Features, RequiredFeatures}, [2]uint64{0x2a00040010808036, 0x0800000000000000}},
		// A fresh CephFS session adds FLOCK (3), DIRLAYOUTHASH (7),
		// CREATEPOOLID (26), REPLY_CREATE_INODE (27), EXPORT_PEER (37),
		// MDS_INLINE_DATA (40), MDS_QUOTA (47) and FS_FILE_LAYOUT_V2 (58),
		// each also required, so a missing one is refused before any CephFS
		// message.
		{"fresh CephFS", [2]uint64{CephFSFeatures, CephFSFeatures&^Features | RequiredFeatures}, [2]uint64{0x2e0085201c8080be, 0x0c0081200c000088}},
		// MON MDSENC (29) for EnableMDSMaps, MON OSDHASHPSPOOL (30) for the
		// replicated-PG profile, OSD_CACHEPOOL (35) for EnableOSDCachePools.
		{"adds MON MDSENC", delta(1, MonAuth{MDSMaps: true}), [2]uint64{0x0000000020000000, 0x0000000020000000}},
		{"adds MON HASHPSPOOL", delta(1, MonAuth{ReplicatedPGV8: true}), [2]uint64{0x0000000040000000, 0x0000000040000000}},
		{"adds MON CACHEPOOL", delta(1, MonAuth{OSDCachePools: true}), [2]uint64{0x0000000800000000, 0x0000000800000000}},
		{"adds OSD CACHEPOOL", delta(4, ServiceAuth{OSDCachePools: true}), [2]uint64{0x0000000800000000, 0x0000000800000000}},
	} {
		if row.got != row.want {
			t.Errorf("%s: supported/required %#016x/%#016x, want %#016x/%#016x", row.profile, row.got[0], row.got[1], row.want[0], row.want[1])
		}
	}
}

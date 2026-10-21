# Messenger feature masks by role

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`,
`include/ceph_features.h`. Source: `featuresForRole`, `handshakeFeatures`,
`CephFSFeatures` and the profile constants in [handshake.go](handshake.go).
`TestFeatureLedgerMasks` fails if the masks below differ from the code.

This ledger states what each session role advertises (`supported`) and
requires of the peer (`required`) in `ClientIdent`. It is a statement of
declared bits, not of verified interoperability: a bit is advertised only when
the codec or admission rule it promises is implemented, and a Messenger feature
bit does not establish the Ceph release family. Evidence for each profile is in
the contracts and validation records named in the last column.

The [CephX exchange ledger](../cephx/EXCHANGE_LEDGER.md) joins role-specific
ticket/authorizer behavior to unit selectors and retained native profiles.
Those authentication observations do not validate every feature bit or widen
the profile-specific evidence in this table.

Bit names (pinned header): 1 NOSRCADDR, 2 SERVER_NAUTILUS (incarnation 3),
4 SUBSCRIBE2, 5 MONNAMES, 15 MONENC, 23 MSG_AUTH, 28 SERVER_MIMIC,
42 MSGR_KEEPALIVE2, 57 SERVER_JEWEL/MON_STATEFUL_SUB, 59 MSG_ADDR2
(FS_BTIME/FS_CHANGE_ATTR overlap), 61 CEPHX_V2. Bit 2 names SERVER_NAUTILUS; its incarnation-3 marker pair is bits 28 and 57.

| Role / profile | Supported mask | Required mask | Added bits and meaning | Evidence |
| --- | --- | --- | --- | --- |
| Base (MGR and any role without a row below) | `0x2a00040010808036` | `0x0800000000000000` | The bits above | [SPEC.md](../../SPEC.md), handshake tests |
| MON (entity type 1) | `0x2e01061012848236` | `0x0800000000000000` | Base + PGID64 (9) + MON admission bits CRUSH_TUNABLES (18), CRUSH_TUNABLES2 (25), CRUSH_V2 (36), CRUSH_TUNABLES3/PRIMARY_AFFINITY (41), CRUSH_V4 (48), CRUSH_TUNABLES5 (58). Admission only: never permission to subscribe to OSDMap or claim CRUSH/object I/O. | `mon_map_features_test.go`, [SPEC.md](../../SPEC.md) |
| OSD (entity type 4) | `0x2a00041010808336` | `0x0800000000000000` | Base + OBJECTLOCATOR (8), PGID64 (9), CRUSH_V2 (36, admission only: required by MON and OSDs once any erasure-code rule exists) | [OSD CONTRACT](../osd/CONTRACT.md) |
| OSD replicated-PG v8 profile (opt-in) | adds `0x0300000040200300` | adds `0x0300000040200300` | OBJECTLOCATOR, PGID64, bit 21 (SERVER_LUMINOUS; also promises RESEND_ON_SPLIT, RADOS_BACKOFF, OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS), OSDHASHPSPOOL (30), NEW_OSDOP_ENCODING (56), 57 | [OSD CONTRACT](../osd/CONTRACT.md) |
| OSD reply-v8 profile (opt-in) | adds `0x0700000040200300` | adds `0x0700000040200300` | v8 profile + bit 58 (NEW_OSDOPREPLY_ENCODING; also CRUSH_TUNABLES5, FS_FILE_LAYOUT_V2) | [OSD CONTRACT](../osd/CONTRACT.md) |
| OSD request-v9 profile (opt-in) | adds `0x0702000040200300` | adds `0x0702000040200300` | reply-v8 profile + SERVER_SQUID (49, incarnation 2 with 57); CRUSH_MSR (38) is not enabled | [OSD CONTRACT](../osd/CONTRACT.md) |
| Fresh MDS session | `0x2a00040010808036` | `0x0800000000000000` | Base only; MON/OSD policy extensions are never offered | [MDS_FRESH_CONTRACT.md](MDS_FRESH_CONTRACT.md) |
| Fresh CephFS session | `0x2e0085201c8080be` | `0x0c0081200c000088` | Base + FLOCK (3), DIRLAYOUTHASH (7), CREATEPOOLID (26), REPLY_CREATE_INODE (27), EXPORT_PEER (37), MDS_INLINE_DATA (40), MDS_QUOTA (47), FS_FILE_LAYOUT_V2 (58). Every added CephFS bit is required together with the base required mask; a missing required bit is refused before any CephFS message. | [MDS_CEPHFS_CONTRACT.md](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |

Opt-in additions on a single role (not shown in the masks above):
OSD_CACHEPOOL (35) on MON and OSD sessions for the cache-pool profile, MDSENC
(29) on MON sessions for MDS map subscriptions, and OSDHASHPSPOOL (30) on MON
sessions for the replicated-PG profile. Each is added to both `supported` and
`required` only when the caller selects that profile.

Bits not listed are not advertised. In particular the full
`CEPH_FEATURES_ALL` mask is never used, because it would promise OSD message
codecs this repository does not implement.

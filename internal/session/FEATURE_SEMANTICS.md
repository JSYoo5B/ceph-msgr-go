# Advertised feature bits and their role-specific obligations

Reference: Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964`,
`src/include/ceph_features.h` (Git blob
`5d8b38fc0a1d7436993585a0e50bfa21989ff789`). The local source blob was checked
against that pinned tree. This is an independent statement of wire facts;
upstream implementation is not incorporated. Ceph sources retain their own
license notices. Active names are listed below; retired meanings are excluded.

[FEATURE_LEDGER.md](FEATURE_LEDGER.md) defines complete role/profile masks and
the three opt-in additions. Its masks, not a union of this table's names, are
what ClientIdent advertises. [PROTOCOL_EVIDENCE.md](../../cephmsgr/PROTOCOL_EVIDENCE.md)
joins those profiles to caller boundaries, unit selectors and attributed native
receipts. Admission, codec parity and actual operation results remain separate.
No row asserts a new daemon run or enables another bit.

## Meaning of overlap and incarnation

Incarnation 1 has no marker; incarnation 2 additionally needs bit 57;
incarnation 3 additionally needs bits 57 and 28. Thus SERVER_NAUTILUS means
2+28+57, SERVER_MIMIC means 28+57, SERVER_LUMINOUS and its aliases mean 21+57,
and SERVER_SQUID means 49+57. The base mask already supplies 28 and 57;
the modern OSD profiles also require their full profile masks from the peer.
These source names select encodings; they do not establish a peer release.
SERVER_TENTACLE (50+57) is deliberately absent: a Tentacle-only product need
not advertise every feature that the Tentacle daemon itself implements.

Some names share the same complete mask, so negotiation cannot distinguish
them. Bit 21 cannot advertise only backoff while concealing upmap/choose-args;
the role/profile contract must bound the complete promise. The caller supplies
placement and applied map state; the library never computes CRUSH or replays
an uncertain operation. Admission-only exceptions are restricted to the roles
below and never turn on an unsupported message codec.

## Bit obligations

The unit [admission matrix](feature_admission_test.go) enumerates 19 concrete
role/option combinations, both CRC and secure, and all 64 peer-required bits.
It requires an ErrFeatures refusal and no transport/application publication
for unsupported requirements. Separate cases remove every locally required
bit from peer support. Successful controls require the exact intersection,
so optional unknown peer bits are never inherited. These tests use complete
authenticated synthetic transcripts, not independent native authentication
or per-operation semantic oracles. The row check requires every advertised
bit to appear once; it cannot prove the prose or linked implementation correct.

| Bit | Active pinned names / incarnation | Role obligation and explicit limit | Code / contract gate |
| --- | --- | --- | --- |
| 1 | NOSRCADDR / 1 | Base transport uses header2 without a legacy source-entity field; authenticated connection identifies the peer | [message.go](../msgr/message.go), [frame ledger](FRAME_TAG_LEDGER.md) |
| 2 | SERVER_NAUTILUS / 3 | Base's current encoding marker with 28+57; no support for older Ceph families or general daemon role | [handshake.go](handshake.go), [specification](../../SPEC.md) |
| 3 | FLOCK / 1 | CephFS-only flock/reconnect wire fields; consumer owns lock state, waits and interrupt policy | [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md), [reconnect codec](../mds/reconnect.go) |
| 4 | SUBSCRIBE2 / 1 | Base current MON subscription request encoding; watch registration/ACK is not authorization or delivered data | [message.go](../msgr/message.go), [watch boundaries](../../cephmsgr/PROTOCOL_EVIDENCE.md) |
| 5 | MONNAMES / 1 | Base preserves named MON-map members and explicit named targets; unavailable exact targets do not silently fall back | [MON Tell](../../cephmsgr/named_mon.go), [map/version contract](../../cephmsgr/MAP_VERSION_CONTRACT.md) |
| 7 | DIRLAYOUTHASH / 1 | CephFS directory layout metadata; hash/rank selection remains caller-owned, not an autonomous namespace router | [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md), [reply codec](../mds/reply.go) |
| 8 | OBJECTLOCATOR / 1 | OSD locator pool/namespace/key/hash encoding; caller supplies placement | [OSD contract](../osd/CONTRACT.md), [request codec](../osd/request.go) |
| 9 | PGID64 / 1 | MON/OSD 64-bit pool/PG representation; MON map acquisition does not apply inner maps or calculate placement | [MON mask check](mon_map_features_test.go), [OSD contract](../osd/CONTRACT.md) |
| 15 | MONENC / 1 | Base current MON map/address forms; compatible envelope parsing does not prove older-release support | [map/version contract](../../cephmsgr/MAP_VERSION_CONTRACT.md), [message ledger](../../cephmsgr/MESSAGE_LEDGER.md) |
| 18 | CRUSH_TUNABLES / 1 | MON-only admission exception; never OSD/MGR/MDS placement capability | [MON mask check](mon_map_features_test.go), [handshake.go](handshake.go) |
| 21 | SERVER_LUMINOUS, RESEND_ON_SPLIT, RADOS_BACKOFF, OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS / 2 | Modern OSD profiles require 21+57 with actual-PG framing and bounded backoff. Verified placement is constrained to the selected legacy-CRUSH/no-upmap profiles; caller map/routing duties are explicit. Actual upmap changes and broader CRUSH semantics remain unverified, not automatic placement support | [v8 request contract](../osd/REQUEST_V8_CONTRACT.md), [O4 contract](../osd/O4_CONTRACT.md), [backoff contract](../osd/BACKOFF_CONTRACT.md) |
| 23 | MSG_AUTH / 1 | Base authentication capability; msgr2 transcript verification and selected CRC/secure framing apply. No legacy msgr1 transport or per-message msgr1 signature API | [handshake.go](handshake.go), [frame ledger](FRAME_TAG_LEDGER.md) |
| 25 | CRUSH_TUNABLES2 / 1 | MON-only admission exception; no CRUSH computation | [MON mask check](mon_map_features_test.go) |
| 26 | CREATEPOOLID / 1 | CephFS current create/reply layout, including inode layout's pool metadata; not a pool-creation management workflow | [reply codec](../mds/reply.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 27 | REPLY_CREATE_INODE / 1 | CephFS preserves the create reply's inode identity; consumer maintains inode state | [reply codec](../mds/reply.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 28 | SERVER_MIMIC / 2 | Base includes its bit-57 marker and supplies the incarnation-3 marker for bit 2; not a standalone release claim | [handshake.go](handshake.go) |
| 29 | MDSENC / 1 | EnableMDS adds and requires this on MON only for current MDS map reception; fresh MDS transport does not inherit it | [MDS map policy](mds_map_features_test.go), [MDS maps](../../cephmsgr/MDS_MAPS_CONTRACT.md) |
| 30 | OSDHASHPSPOOL / 1 | Modern OSD profile adds/requires it on MON and OSD. Caller supplies independent raw hash and actual PG; the library does not fold/hash/map objects | [MON profile check](mon_v8_profile_handshake_test.go), [v8 request contract](../osd/REQUEST_V8_CONTRACT.md) |
| 35 | OSD_CACHEPOOL / 1 | Explicit EnableOSDCachePools adds/requires it on MON/OSD only. Bounded IGNORE_CACHE is implemented; flush/evict/pin/overlay policy is outside that capability | [cache-pool contract](../osd/CACHE_POOL_CONTRACT.md), [role isolation](osd_cache_pool_features_test.go) |
| 36 | CRUSH_V2 / 1 | Narrow MON and OSD admission exception once any EC rule exists. Does not compute placement or establish EC I/O; other CRUSH admission bits stay MON-only | [handshake.go](handshake.go), [MON mask check](mon_map_features_test.go) |
| 37 | EXPORT_PEER / 1 | CephFS preserves cap export/import peer metadata; consumer owns handoff and per-rank state | [cap codec](../mds/caps.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 40 | MDS_INLINE_DATA / 1 | CephFS preserves inline inode data/version; coupled OSD I/O and consumer file/cache policy remain separate | [reply codec](../mds/reply.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 41 | CRUSH_TUNABLES3, OSD_PRIMARY_AFFINITY / 1 | MON-only admission exception for this shared mask; no automatic primary selection or placement | [MON mask check](mon_map_features_test.go) |
| 42 | MSGR_KEEPALIVE2 / 1 | Base msgr2 keepalive/ACK timestamps and liveness handling; no execution or durability acknowledgement | [session.go](session.go), [frame ledger](FRAME_TAG_LEDGER.md) |
| 47 | MDS_QUOTA / 1 | CephFS decodes quota/rstat pushes; consumer enforces limits and inode state | [cap codec](../mds/caps.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 48 | CRUSH_V4 / 1 | MON-only admission exception; no CRUSH computation | [MON mask check](mon_map_features_test.go) |
| 49 | SERVER_SQUID / 2 | Only explicit untraced request-v9/reply-v8 OSD profile requires 49+57. No OTEL SDK, nonempty trace acceptance or CRUSH_MSR | [request-v9 contract](../osd/REQUEST_V9_CONTRACT.md), [v9 handshake](osd_request_v9_handshake_test.go) |
| 56 | NEW_OSDOP_ENCODING / 1 | Modern OSD actual-PG request v8/v9; base v6 profile rejects the requirement and never chooses v8 merely from a peer's optional bit | [v8 request contract](../osd/REQUEST_V8_CONTRACT.md), [v8 handshake](osd_v8_handshake_test.go) |
| 57 | MON_STATEFUL_SUB, SERVER_JEWEL / 1 | Base current subscription cursor handling and incarnation marker. A cursor is not complete event history; renewal never automatically replays uncertain commands | [watch boundaries](../../cephmsgr/PROTOCOL_EVIDENCE.md), [CephX exchanges](../cephx/EXCHANGE_LEDGER.md) |
| 58 | CRUSH_TUNABLES5, NEW_OSDOPREPLY_ENCODING, FS_FILE_LAYOUT_V2 / 1 | MON admission only; OSD reply-v8/v9 profiles require strict reply v8; CephFS requires current layout fields. These roles remain isolated; the old OSD profile does not gain reply v8 | [reply-v8 contract](../osd/OSD_REPLY_V8_CONTRACT.md), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 59 | FS_BTIME, FS_CHANGE_ATTR, MSG_ADDR2 / 1 | Base requires current address vectors; CephFS additionally preserves birth/change inode metadata in its current codecs. Shared mask does not make non-CephFS sessions filesystem clients | [address codec](../msgr/address.go), [reply codec](../mds/reply.go), [CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md) |
| 61 | CEPHX_V2 / 1 | Base CephX v2 authorizer/proof behavior; requested tickets are not service permissions or operation completion | [CephX exchange ledger](../cephx/EXCHANGE_LEDGER.md) |

## Remaining verification boundaries

The complete mask and per-bit admission checks do not establish the full
semantic promise of aliased bit 21 across arbitrary maps, upmap changes,
CRUSH choose-args or every split/failure profile. Their caller prerequisites,
implemented backoff/identity gates and unverified native configurations remain
in the linked contracts. Unsupported required bits, including CRUSH_MSR,
SERVER_TENTACLE and reserved 62/63, must not be silently accepted by any
currently advertised profile. Missing native oracles and historical receipts
are not evidence of a missing encoder; consumers still own map/routing policy.

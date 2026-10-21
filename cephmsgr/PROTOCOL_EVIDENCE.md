# Public boundaries and protocol evidence

Reference: Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964`.
This joins every row in [MESSAGE_LEDGER.md](MESSAGE_LEDGER.md) and
[FEATURE_LEDGER.md](../internal/session/FEATURE_LEDGER.md) to a caller boundary,
a checked-in unit selector and a retained native observation or explicit gap.
Header versions/contracts remain in those ledgers; nested authentication is in
[EXCHANGE_LEDGER.md](../internal/cephx/EXCHANGE_LEDGER.md), frame tags in
[FRAME_TAG_LEDGER.md](../internal/session/FRAME_TAG_LEDGER.md), and all unsupported
OSD opcodes in [UNSUPPORTED_OPCODES.md](../internal/osd/UNSUPPORTED_OPCODES.md).

`python3 tools/protocol_evidence.py` checks exact row coverage, test definitions,
receipt IDs and repository-local links. Basic CI runs this through the Python
tool tests. It does not execute the linked selectors or verify native outcomes;
semantic assertions and original records still need review. Native-vector tests
in the unit column exercise retained independent codec outputs, not live daemons.
A unit selector is one concrete check of the row, not exhaustive failure coverage.

Public names below are communication primitives; option dependencies and the
contract's bounds/refusals still apply. Command schemas, placement, inode/cache
state, service policy and retry decisions belong to the consumer. Cancellation
of a local wait never establishes remote rollback. Defined-only constants expose
no operation and cannot acquire a support claim from an unrelated test.

## Message evidence

IDs are decimal, including the 0x700 MGR and 0x310 CephFS families. Receipt U
means no native receipt is joined here, not proof that no historical test exists.
A receipt proves only its stated message use/profile. Shared IDs used by several
channels do not imply all targets are verified: type 97/98's R3 receipt covers
selected MON/active-MGR Tell only; MDS and OSD command acceptance remains in #5.

| Type | Caller boundary or explicit refusal | Unit / retained-vector selector | Native receipt |
| --- | --- | --- | --- |
| 2 | PingMonitor; exact named MON, malformed replies refused | [TestMonPingRejectsMalformedReply](mon_ping_test.go) | R1 |
| 4 | Snapshot MON identity/map metadata; malformed maps fail the source | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 5 | Dial and MON recovery request the MON map; no consumer routing policy | [TestGetMonMapIndependentHeader](../internal/msgr/getmap_test.go) | R3 |
| 6 | RequestOSDMaps; bounded explicit epochs, opaque inner maps | [TestGetOSDMapsIndependentVectors](../internal/msgr/osd_map_control_test.go) | R3 |
| 13 | Statfs; optional data-pool ID, no typed capacity policy | [TestStatfsClusterAndPool](statfs_test.go) | R4 |
| 14 | StatfsResult; raw counters and server result retained | [TestStatfsEncodingAndReplyDecoding](statfs_test.go) | R4 |
| 15 | WatchLogs/WatchConfig/WatchDigest/WatchServiceMap and opt-in map streams; ACK is not permission | [TestLogWatchSubscribeAckDoesNotEstablishPermission](logs_recovery_test.go) | R3 |
| 16 | Not exposed: no subscription-ACK delivery or authorization API | none | none |
| 17 | Not exposed: CephX uses Messenger auth frames | none | none |
| 18 | Not exposed: CephX uses Messenger auth frames | none | none |
| 19 | MapVersion; bounded name/query and per-operation context | [TestMapVersionRoundTripAndRefusals](map_version_test.go) | R5 |
| 20 | MapVersion result; preserve server handle/version, wrong envelope refused | [TestMapVersionCodec](map_version_test.go) | R5 |
| 21 | WatchMDSMaps; EnableMDS opt-in, filesystem/rank binding remains explicit | [TestMonMDSMapsStreamsBindExplicitIDAndPreserveRaw](mon_mds_maps_test.go) | R6 |
| 22 | OpenSession/Renew/Close and Next; session rejection is explicit | [TestMDSSessionOpenRenewCloseWire](mds_session_test.go) | R7 |
| 23 | Reconnect with consumer-supplied caps/realms; no automatic state reconstruction | [TestCephFSReconnectDeniedAndSplit](mds_reconnect_test.go) | R9 |
| 24 | Submit CephFS metadata request; consumer inode/path/cache state | [TestCephFSRequestReplyForwardAndSafety](mds_cephfs_test.go) | R8 |
| 25 | Forward metadata and explicit ResubmitForwarded; preserve identity | [TestCephFSResubmitForwardedKeepsTIDOnDestinationRank](mds_cephfs_test.go) | R15 |
| 26 | Wait/WaitSafe; unsafe receipt is not safe completion | [TestCephFSRequestReplyForwardAndSafety](mds_cephfs_test.go) | R8 |
| 27 | Reclaim/FinishReclaim on an open admitted session | [TestMDSReclaimRefusalsAndUnknownOutcomes](mds_reclaim_test.go) | R16 |
| 28 | Reclaim result; malformed reply is terminal, sent failure stays unknown | [TestMDSReclaimMalformedReplyEndsTheSession](mds_reclaim_test.go) | R16 |
| 41 | WatchOSDMaps/RequestOSDMaps/NextMap; raw batches, no CRUSH or auto-application | [TestMonOSDMapWatchRawFIFOOwnershipAndLocalCancellation](mon_osd_maps_test.go) | R3 |
| 42 | OSDConnection requests; caller placement, selected profile, no automatic replay | [TestRequestRejectsUnsupportedEncodingAndOperations](../internal/osd/codec_test.go) | R3, R10, R11, R12 |
| 43 | OSDReply; raw per-op codes/bytes/identity/completion, profile-bound decoder | [TestOSDReplyV8ProfileDoesNotFallbackToNativeV6](osd_reply_v8_native_test.go) | R3, R10, R11, R12 |
| 44 | WatchObject/NotifyObject event Next and explicit Acknowledge | [TestOSDWatchDispatchExplicitAckAndLifecycle](osd_watch_notify_test.go) | R12 |
| 48 | PoolOpResult; signed server code, snapshot ID and uncertainty | [TestPoolOpWireAndRefusalsAndUnknownOutcome](poolop_test.go) | R5 |
| 49 | PoolOp; raw fixed request, consumer snapshot/pool policy | [TestPoolOpSelfManagedAndPoolSnapshots](poolop_test.go) | R5 |
| 50 | MonCommand; raw JSON/binary input, no schema interpretation | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 51 | MonCommand Result; raw output and signed code, no uncertain replay | [TestMalformedCommandReplyPreservesOutputAndUnknownOutcome](reply_test.go) | R3 |
| 52 | WatchLogs Next; raw entry metadata, bounded delivery and gaps | [TestLogWatchRawFieldsMixedCommandsAndLocalWait](logs_test.go) | U |
| 61 | NextBackoff/AckBackoff; original identity/epoch, explicit ACK gate | [TestOSDBackoffTransportExplicitAckGateAndUnblock](osd_backoff_integration_test.go) | R10, R12 |
| 62 | WatchConfig Next; coalesced raw map, no configuration policy | [TestConfigWatchCoalescesFullMapsAndTransfersOwnership](config_test.go) | U |
| 63 | WatchConfig requests; consumer-selected identity, no text config loading | [TestConfigRequestsIndependentVectors](../internal/msgr/config_test.go) | U |
| 97 | MonTell/MonTellTo/MgrTell/MDS Command/OSD Command; exact role/target | [TestTellCommandPinnedVector](../internal/msgr/tell_test.go) | R3 |
| 98 | Tell Result; raw bytes/code, malformed or lost reply preserves uncertainty | [TestTellReplyRejectsMalformedBoundedFront](../internal/msgr/tell_test.go) | R3 |
| 103 | WatchFSMapUser; native filesystem directory, EnableMDS required | [TestPublicFSMapUserPreservesNativeDirectory](mds_map_types_test.go) | R6 |
| 784 | Next Cap event/SendCaps; consumer owns capabilities, flush and inode state | [TestEncodeClientCapsUpdateMatchesFreshNative](../internal/mds/native_caps_test.go) | R8 |
| 785 | Next Lease event/SendLease; explicit consumer lease ACK | [TestLeaseMatchesFreshNative](../internal/mds/native_caps_test.go) | R8 |
| 786 | Next Snap event; consumer applies snapshot realm state | [TestDecodeFreshNativeSnapSplit](../internal/mds/native_caps_test.go) | R8 |
| 787 | ReleaseCaps; bounded explicit consumer releases | [TestEncodeCapReleaseMatchesFreshNative](../internal/mds/native_caps_test.go) | R8 |
| 788 | Next Quota event; raw limits, consumer enforcement | [TestDecodeFreshNativeQuota](../internal/mds/native_caps_test.go) | R8 |
| 1792 | EnableMgrService/OpenMgrService only; ordinary MGR commands unchanged | [TestMgrServiceRequiresOptInAndValidIdentity](mgr_service_test.go) | R2 |
| 1793 | MgrService.Configure; unknown extensions preserved raw | [TestMgrConfigureDecoding](../internal/msgr/mgr_service_test.go) | R2 |
| 1794 | MgrService.Report; bounded metadata/status, no perf schema API | [TestMgrReportEncoding](../internal/msgr/mgr_service_test.go) | R2 |
| 1796 | Snapshot active-MGR discovery; current daemon identity, no standby reroute | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 1797 | WatchDigest Next; raw status/health pair, no typed module policy | [TestDigestWatchCoalescesRawPairsAndTransfersOwnership](digest_test.go) | U |
| 1799 | WatchServiceMap Next; raw daemon metadata/epochs, not service registration | [TestDecodeServiceMap](../internal/msgr/servicemap_test.go) | R2 |
| 1800 | MgrService.Close; require echo or retain unknown outcome | [TestMgrServiceUnconfirmedOpenAndCloseKeepUncertainty](mgr_service_test.go) | R2 |
| 1801 | MgrCommand; raw JSON/binary input to active manager | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 1802 | MgrCommand Result; raw bytes/signed code, no uncertain replay | [TestMalformedCommandReplyPreservesOutputAndUnknownOutcome](reply_test.go) | R3 |
| 1803 | MgrService.UpdateMetadata; explicit update, no replay after loss | [TestMgrUpdateEncoding](../internal/msgr/mgr_service_test.go) | R2 |

## Feature evidence

A completed handshake exercises the selected mask's admission. It does not
independently establish every aliased bit's semantics or every operation that a
server associates with that bit. In particular MON CRUSH admission and OSD's
CRUSH_V2 exception never claim placement computation. Do not form a Cartesian
product of key/mode/address/role dimensions from separate observations.

| Role / profile | Caller boundary or explicit refusal | Unit selector | Native receipt |
| --- | --- | --- | --- |
| Base (MGR and any role without a row below) | Dial; fixed transport/CephX mask, no arbitrary feature override | [TestMONMapFeaturesKeepImplementedCapabilitiesScoped](../internal/session/mon_map_features_test.go) | R2, R3 |
| MON (entity type 1) | Dial MON; CRUSH-generation bits are admission only, never placement | [TestMONMapFeaturesKeepImplementedCapabilitiesScoped](../internal/session/mon_map_features_test.go) | R1, R2, R3 |
| OSD (entity type 4) | EnableOSD/OpenOSD; CRUSH_V2 is admission only, caller supplies placement | [TestOSDHandshakeBindsZeroIDAndNegotiatesImplementedFeatures](../internal/session/osd_handshake_test.go) | R3 |
| OSD replicated-PG v8 profile (opt-in) | OSDProfileReplicatedPG; missing profile bits refused before application | [TestReplicatedPGV8HandshakeRejectsEveryMissingProfileCapabilityBeforeApplication](../internal/session/osd_v8_handshake_test.go) | R10 |
| OSD reply-v8 profile (opt-in) | OSDProfileReplicatedPGReplyV8; strict reply-v8, no v6 fallback | [TestReplicatedPGReplyV8FeaturePolicyIsFixedAndOSDOnly](../internal/session/osd_reply_v8_handshake_test.go) | R11 |
| OSD request-v9 profile (opt-in) | OSDProfileReplicatedPGRequestV9ReplyV8; untraced v9, no CRUSH_MSR | [TestReplicatedPGRequestV9HandshakeRequiresEveryFixedCapability](../internal/session/osd_request_v9_handshake_test.go) | R12 |
| Fresh MDS session | EnableMDS/OpenMDS; base only, refuse lossy/recovery admission | [TestFreshMDSHandshakeRejectsUnimplementedAdmission](../internal/session/fresh_mds_handshake_test.go) | R13 |
| Fresh CephFS session | EnableCephFS; fixed required features and consumer-owned state | [TestCephFSProtocolViolationsAndFeatureGates](mds_cephfs_test.go) | R8 |
| MON MDSENC opt-in | EnableMDS; MON-only current MDS map encoding | [TestMDSMapHandshakeRequiresMDSENCBeforeApplicationTraffic](../internal/session/mds_map_features_test.go) | R6 |
| MON HASHPSPOOL opt-in | Modern OSD profile; MON-only addition, independent of CRUSH exception | [TestReplicatedPGV8MONProfileAddsOnlyOptInHashPSPool](../internal/session/mon_v8_profile_handshake_test.go) | R10, R11, R12 |
| MON/OSD CACHEPOOL opt-in | EnableOSDCachePools; missing capability refused without fallback | [TestOSDCachePoolHandshakeRequiresCapabilityWithoutFallback](../internal/session/osd_cache_pool_features_test.go) | R14 |

## Native receipts

All referenced records use the pinned Tentacle v20.2.4 source and image digest
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
R3 also records a separate 20.2.3 run; that does not widen the v20.2.4 rows here.
Attribution categories describe the record, not current-tree verification:
`exact-commit`, `file-attributed`, `historical` (no single committed tested tree),
and `unjoined`. A historical `source-N` label is not a Git SHA. Original records
retain selected selectors, fixture parameters, first failures and cleanup limits.
Nothing in this document relabels those runs as passes of the present commit.

| ID | Attribution | Profile and actual boundary | Record |
| --- | --- | --- | --- |
| U | unjoined | No retained native receipt joined to this row; unit/reference evidence only | none |
| R1 | exact-commit | `da661e737bca69f3dd5695eed9ba26a13188ea55`, Linux/amd64 host, IPv4, CRC, AES/AES. Manual run 39574326673 actually passed TestCephMonPingIntegration and five leaves; final status/artifact match that SHA/image, seven containers terminated. Only MON ping, not full command acceptance | [Manual request receipt](../.github/scenarios/README.md) |
| R2 | exact-commit | `dec9084f0a858e51224f16a9178c1218f8dd8e12` CRC/AES/AES IPv4 host and `66084e170a7f5805bd9126ac28e9f08f8c92b25a` secure/aes256k/aes256k IPv6 container, Linux/amd64. Go service registration, metadata/status update, native ServiceMap observation, acknowledged removal and cleanup passed; no renewal/failover claim | [MGR service](../integration/fixture/MGR_SERVICE_VALIDATION.md) |
| R3 | historical | Migration working tree `source-1`, Linux/arm64 daemons with Darwin/arm64 host relay or Linux/arm64 container clients. Selected MON/MGR commands/Tell, maps and legacy OSD I/O passed across listed key/mode/address profiles. No common committed tree or full matrix acceptance; scopes remain those of each original row | [Migration](../integration/fixture/VALIDATION.md) |
| R4 | historical | No committed tested tree/file hashes identified by this record; container runtime, selected legacy-v6/replicated-PG BlueStore and secure/aes256k IPv6 request-v9 MemStore profiles. Native cluster counters matched; pool counts checked for consistency, with the later CephFS pool-statfs comparison recorded separately | [Statfs](../integration/fixture/MON_STATFS_VALIDATION.md) |
| R5 | historical | No committed tested tree/file hashes identified by this record; container runtime, selected legacy-v6/replicated-PG BlueStore and secure/aes256k IPv6 request-v9 MemStore pool/snapshot operations. Native pre/postconditions and lost-reply boundaries are those in the record; not every command mutation | [Pool operations](../integration/fixture/MON_POOL_OP_VALIDATION.md) |
| R6 | file-attributed | Frozen file hashes, CRC/AES IPv4 host and secure/aes256k IPv6 container. Native FSMapUser/MDSMap metadata, removals and same-source binding refusal passed. Optional renewed-source rebind was not observed | [MDS maps](../integration/fixture/MDS_MAPS_VALIDATION.md) |
| R7 | historical | `source-1` plus selected file hashes; CRC/AES IPv4 host and secure/aes256k IPv6 container. Selected session OPEN/renewal/events/CLOSE passed; isolated export excludes unrelated edits | [MDS sessions](../integration/fixture/MDS_SESSIONS_VALIDATION.md) |
| R8 | historical | Selected `source-1` working trees and retained hashes; CRC/AES IPv4 host and secure/aes256k IPv6 container. Metadata, caps, lease ACK/release, snap and quota paths passed in the respective records; no full CephFS client claim | [CephFS](../integration/fixture/MDS_CEPHFS_VALIDATION.md), [Push](../integration/fixture/MDS_CEPHFS_PUSH_VALIDATION.md) |
| R9 | file-attributed | Separate frozen recovery sources/hashes, CRC/AES IPv4 host and secure/aes256k IPv6 container. Explicit reconnect/replay and pending cap-flush paths have independent observations; later extensions do not retroactively change the first run | [MDS recovery](../integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md) |
| R10 | historical | `source-1`; CRC/AES IPv4 host BlueStore and secure/aes256k IPv6 container MemStore, arm64. Explicit PG v8 I/O/PGNLS and real BLOCK/ACK/UNBLOCK passed; caller supplies routing and legacy CRUSH constraints remain | [Replicated PG](../integration/fixture/OSD_REPLICATED_PG_VALIDATION.md) |
| R11 | file-attributed | Corrected acceptance hash `e5136a1ad3461045e6f5f239d6ad90d3e2f17b80f97b6453eeecb58c405a8c4e`; CRC/AES IPv4 host and secure/aes256k IPv6 container. Three corrected v8-reply leaves passed separately from earlier 13 passing leaves in failed runs. No single successful combined 16-case run claimed | [Reply v8](../integration/fixture/OSD_REPLY_V8_VALIDATION.md) |
| R12 | historical | `source-1` plus frozen native/product source identities; Linux/arm64 daemons, CRC/AES IPv4 host BlueStore and secure/aes256k IPv6 container MemStore. Eight top-level tests/16 leaves passed, no selected skips; CRC capture observed v9 requests/v8 replies, secure did not expose headers | [Request v9](../integration/fixture/OSD_REQUEST_V9_VALIDATION.md) |
| R13 | file-attributed | Corrected acceptance hash `4b1c7ddf57e15193cb71bf54fef368a22ab57ce7d95e21c19e0236267fe4e9c3`; CRC Darwin/arm64 host IPv4 AES/AES and secure Linux/arm64 container IPv6 aes256k/aes256k. Fresh MDS authentication/keepalive passed; CRC receipt reused, secure rerun after label correction; no session/recovery claim | [Fresh MDS](../integration/fixture/MDS_FRESH_VALIDATION.md) |
| R14 | file-attributed | Source aggregate SHA-256 `194e502d331867954baf55c6d8a3dddb7c045404d49d50f21d4a3750fba413c2`, Linux/amd64 CRC/AES IPv4 host BlueStore and secure/aes256k IPv6 container MemStore, legacy-v6 native cache-tier profile. Opt-in capability/flag isolation and selected bypass operations only; tier promotion/eviction and all cache opcodes remain separate requirements | [Ignore cache](../integration/fixture/OSD_IGNORE_CACHE_VALIDATION.md) |
| R15 | file-attributed | Frozen source hashes with historical `source-1` base, CRC/AES IPv4 host and secure/aes256k IPv6 container, arm64. Native pinned-rank LOOKUP produced destination/forward count, explicit resubmission succeeded on rank 1, and native counters/statx independently matched. Initial CephFS R8 did not observe forwarding | [Multiple ranks](../integration/fixture/MDS_CEPHFS_RANKS_VALIDATION.md) |
| R16 | historical | No committed tested tree/file hashes identified by this record; container runtime, secure/aes256k IPv4 BlueStore and IPv6 MemStore. Reclaim and FinishReclaim passed with native session removal/blocklist observation. Different-identity EPERM, native twin sender and MDS failover remain unverified | [MDS reclaim](../integration/fixture/MDS_RECLAIM_VALIDATION.md) |

## Remaining acceptance work

This join is an inventory and evidence-attribution check, not full acceptance
of #4 or #5. Unjoined rows, historical source identities and profile-specific
failure/oracle gaps remain visible. The stale-contract sweep and per-bit semantic
promise audit still need completion; do not enable new bits to improve counts.
A missing native oracle/receipt is different from missing wire code. Daemon-only
operations, server handler constraints and consumer-owned state remain distinct.

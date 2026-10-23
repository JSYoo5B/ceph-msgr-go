# Public boundaries and protocol evidence

Reference: Tentacle v20.2.4, `7f793731f1b39eb4f465e960113d2363c311b964`.
This joins every frame-tag row and every row in
[MESSAGE_LEDGER.md](MESSAGE_LEDGER.md) and
[FEATURE_LEDGER.md](../internal/session/FEATURE_LEDGER.md) to a caller boundary,
a checked-in unit selector and a retained native observation or explicit gap.
Header versions/contracts remain in those ledgers; nested authentication is in
[EXCHANGE_LEDGER.md](../internal/cephx/EXCHANGE_LEDGER.md), frame tags in
[FRAME_TAG_LEDGER.md](../internal/session/FRAME_TAG_LEDGER.md), and unsupported
OSD opcodes in [UNSUPPORTED_OPCODES.md](../internal/osd/UNSUPPORTED_OPCODES.md).

`python3 tools/protocol_evidence.py` checks exact row coverage, test definitions,
receipt IDs, opcode values/contracts, frame observation kinds and repository-local
links. Basic CI runs this through the Python tool tests. It does not execute
the linked selectors or verify native outcomes;
semantic assertions and original records still need review. Native-vector tests
in the unit column exercise retained independent codec outputs, not live daemons.
A unit selector is one concrete check of the row, not exhaustive failure coverage.

Public names below are communication primitives; option dependencies and the
contract's bounds/refusals still apply. Command schemas, placement, inode/cache
state, service policy and retry decisions belong to the consumer. Cancellation
of a local wait never establishes remote rollback. Defined-only constants expose
no operation and cannot acquire a support claim from an unrelated test.

## Frame evidence

The [frame ledger](../internal/session/FRAME_TAG_LEDGER.md) retains numeric
names, phases, wire/refusal semantics and the broader unit selectors. Each
frame below joins a caller boundary and one concrete unit check to a receipt
with an explicit observation kind. Receipt attribution (exact commit, file
hashes or historical source) remains independent of observation kind:

- `captured`: the retained record explicitly describes observed transferred
  frame/message headers; R12 covers CRC Message frames only, not secure headers.
- `reported`: the native acceptance record explicitly reports the exchange or
  decoded result, without asserting a retained raw-frame capture.
- `inferred`: successful native operation/resume plus the current source path
  entails the tag; the receipt is not a direct per-tag observation or capture.
- `unobserved`: no native tag receipt is joined. Synthetic rejection/control
  tests still establish the stated unit boundary; this is not missing wire code.

The checker rejects missing/duplicate/undeclared tags, dead unit references,
unknown observation kinds and assigning an observed kind to an unjoined
receipt. It does not inspect raw traces or prove that an inference/observation
is justified; the original records and source path still require review.
A header observation is not every payload/version/failure case for that tag.
MDS stream resume is distinct from application MClientReconnect (type 23).
Standalone ACK, peer-initiated keepalive, retry/WAIT and negative auth/feature
replies are not claimed native merely because another operation passed.

| Tag | Caller boundary or explicit refusal | Unit selector | Native evidence |
| --- | --- | --- | --- |
| 1 | Dial/OpenOSD/OpenMDS setup; wrong peer role/address refused | [TestHandshakeValidatesServerAddressIdentity](../internal/session/handshake_test.go) | R1:inferred |
| 2 | Programmatic CephX identity and one immutable connection mode | [TestHandshakePinsModeAndRequiresSignature](../internal/session/crc_handshake_test.go) | R1:inferred |
| 3 | Typed authentication rejection; no method/security downgrade | [TestAuthenticationRejectionPreservesCodeAndValidatesFrame](../internal/session/handshake_test.go) | U:unobserved |
| 4 | MON challenge processing; malformed or excessive rounds fail setup | [TestCompleteMalformedCephXPayloadIsProtocolFailure](../internal/session/handshake_payload_test.go) | R1:inferred |
| 5 | CephX challenge response, not an application command | [TestCompleteEncryptedMgrChallengeRetainsDecodeFailure](../internal/session/handshake_payload_test.go) | R1:inferred |
| 6 | Authentication publication; mode/secret requirements precede transport | [TestCRCHandshakeCannotCompleteWithoutAuthDone](../internal/session/crc_handshake_test.go) | R1:inferred, R2:inferred |
| 7 | Transcript signature checked in CRC and secure; tampering refused | [TestHandshakeRejectsTamperingAndDowngrade](../internal/session/handshake_test.go) | R1:inferred, R2:inferred |
| 8 | Fixed role/profile masks; no arbitrary caller feature advertisement | [TestEveryPeerRequiredFeatureIsRoleAndProfileScoped](../internal/session/feature_admission_test.go) | R1:inferred, R2:inferred |
| 9 | Authenticated address/ID, feature and session-flag admission | [TestHandshakeRejectsUnsupportedSessionFlags](../internal/session/handshake_test.go) | R1:inferred, R13:inferred |
| 10 | ErrFeatures with complete peer mask text; no transport or downgrade | [TestHandshakeMissingFeaturesPreservesPeerMask](../internal/session/handshake_refusal_test.go) | U:unobserved |
| 11 | Stateful MDS/CephFS stream resume; not a new application session | [TestResumeHandshakeFollowsRetriesAndReset](../internal/session/resume_test.go) | R8:inferred, R9:inferred |
| 12 | ErrSessionReset ends the stateful session; sent calls remain unknown | [TestStatefulSessionTerminalResumeOutcomes](../internal/session/resume_test.go) | R9:reported |
| 13 | Bounded connect-sequence retry within Messenger resume, not mutation replay | [TestResumeHandshakeFollowsRetriesAndReset](../internal/session/resume_test.go) | U:unobserved |
| 14 | Bounded global-sequence retry; overflow/malformed reply refused | [TestResumeHandshakeFollowsRetriesAndReset](../internal/session/resume_test.go) | U:unobserved |
| 15 | Resume established stream and preserve unacknowledged identity | [TestStatefulSessionResumesAndRetransmitsUnacknowledged](../internal/session/resume_test.go) | R8:inferred |
| 16 | Resume WAIT is a terminal refusal; no wait/replace policy | [TestResumeHandshakeFollowsRetriesAndReset](../internal/session/resume_test.go) | U:unobserved |
| 17 | Raw commands/wire messages; type/TID/sequence matching and uncertainty | [TestTruncatedResponseCannotEstablishCommandOutcome](../internal/session/fault_test.go) | R12:captured, R31:captured, R1:inferred |
| 18 | Client liveness probe or peer echo; receipt covers client-originated probe only | [TestPeerKeepaliveEchoPreservesSecureCommandSequences](../internal/session/keepalive_peer_test.go) | R13:reported |
| 19 | Liveness ACK timestamp; no execution/durability guarantee | [TestKeepaliveAcknowledgmentsKeepAnIdleSessionAlive](../internal/session/liveness_test.go) | R13:reported |
| 20 | Standalone transport ACK; does not establish operation execution | [TestStatefulSessionResumesAndRetransmitsUnacknowledged](../internal/session/resume_test.go) | U:unobserved |
| 21 | CRC offers snappy/zstd, secure declines; offer does not enable compression | [TestCRCHandshakeAuthenticatesAndRetainsCRCFraming](../internal/session/crc_handshake_test.go) | R17:reported |
| 22 | Accept only offered implemented method; retained native result is decline only | [TestHandshakeRejectsUnofferedCompressionSelection](../internal/session/handshake_refusal_test.go) | R17:reported |

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
| 50 | MonCommand; raw JSON/binary input, no schema interpretation | [TestNativeMONAndMGRCommandPath](client_test.go), [TestMonOutcomeProbeBindsCRCRequestAndPartialWrite](mon_outcome_probe_test.go) | R3, R31 |
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
| 1792 | EnableMgrService/OpenMgrService only; ordinary MGR commands unchanged | [TestMgrServiceRequiresOptInAndValidIdentity](mgr_service_test.go), [TestMgrServiceUnavailableWaitIsKnownBeforeSendAndReleasesReservation](mgr_service_test.go), [TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) | R2, R18, R19, R22, R23 |
| 1793 | MgrService.Configure; unknown extensions preserved raw | [TestMgrConfigureDecoding](../internal/msgr/mgr_service_test.go), [TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) | R2, R18, R19 |
| 1794 | MgrService.Report; bounded metadata/status, no perf schema API | [TestMgrReportEncoding](../internal/msgr/mgr_service_test.go), [TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) | R2, R18, R19, R22 |
| 1796 | Snapshot active-MGR discovery; current daemon identity, no standby reroute | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 1797 | WatchDigest Next; raw status/health pair, no typed module policy | [TestDigestWatchCoalescesRawPairsAndTransfersOwnership](digest_test.go) | U |
| 1799 | WatchServiceMap Next; raw daemon metadata/epochs, not service registration | [TestDecodeServiceMap](../internal/msgr/servicemap_test.go) | R2 |
| 1800 | MgrService.Close; require echo or retain unknown outcome | [TestMgrServiceUnconfirmedOpenAndCloseKeepUncertainty](mgr_service_test.go), [TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) | R2, R18, R19, R20, R22 |
| 1801 | MgrCommand; raw JSON/binary input to active manager | [TestNativeMONAndMGRCommandPath](client_test.go) | R3 |
| 1802 | MgrCommand Result; raw bytes/signed code, no uncertain replay | [TestMalformedCommandReplyPreservesOutputAndUnknownOutcome](reply_test.go) | R3 |
| 1803 | MgrService.UpdateMetadata; explicit update, no replay after loss | [TestMgrUpdateEncoding](../internal/msgr/mgr_service_test.go), [TestMgrServiceNativeVectors](../internal/msgr/mgr_service_native_test.go) | R2, R18, R21 |

## Implemented opcode evidence

Every `implemented` entry in [opcodes-tentacle.csv](../internal/osd/opcodes-tentacle.csv)
has its exact wire value, bounded contract and unit/vector selector below.
The remaining 28 entries are exhaustively classified in
[UNSUPPORTED_OPCODES.md](../internal/osd/UNSUPPORTED_OPCODES.md), checked against
the same catalog by `TestUnsupportedOpcodesAreClassified`. These 52/28 counts
are an enum inventory, not a completion percentage or a complete RADOS API.
Public `OSDOperation`/`OSDRequest` and result fields retain raw bytes, identities,
per-op codes and unknown outcomes. Contracts define valid combinations, target/
profile requirements and caller-owned placement; an opcode being implemented
never admits every compound or configuration.

Native receipt U means the corresponding live record is not joined here;
contract-specific validation links remain in each contract. Native-vector tests
in the unit column compare offline bytes, not live execution. R12's selected
read/stat/append observation does not cover every operation admitted by its
request profile. No other opcode inherits that native result.

| Opcode | Wire value and bounded contract | Unit / retained-vector selector | Native receipt |
| --- | --- | --- | --- |
| READ | `0x1201` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded raw outputs and request identity | [TestEncodeRequestMatchesNativeCeph](../internal/osd/codec_test.go) | R12 |
| STAT | `0x1202` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded raw outputs and request identity | [TestEncodeRequestMatchesNativeCeph](../internal/osd/codec_test.go) | R12 |
| MAPEXT | `0x1203` [MAPEXT_CONTRACT.md](../internal/osd/MAPEXT_CONTRACT.md); bounded extent map; no placement policy | [TestMapExtRequestMatchesFreshNativeEncoder](../internal/osd/mapext_test.go) | U |
| CHECKSUM | `0x121f` [CHECKSUM_CONTRACT.md](../internal/osd/CHECKSUM_CONTRACT.md); algorithm/count/output bounds; standalone/compound scope remains explicit | [TestChecksumMatchesFreshNativeEncoder](../internal/osd/checksum_test.go) | U |
| SPARSE_READ | `0x1205` [SPARSE_READ_CONTRACT.md](../internal/osd/SPARSE_READ_CONTRACT.md); owned extent/data output; no filesystem hole policy | [TestSparseReadRequestMatchesFreshNativeEncoder](../internal/osd/sparse_read_test.go) | U |
| NOTIFY | `0x1206` [WATCH_NOTIFY_CONTRACT.md](../internal/osd/WATCH_NOTIFY_CONTRACT.md); bounded cookie/event/ACK lifecycle; explicit consumer handling | [TestWatchOperationsMatchFreshNative](../internal/osd/watch_notify_test.go) | U |
| NOTIFY_ACK | `0x1207` [WATCH_NOTIFY_CONTRACT.md](../internal/osd/WATCH_NOTIFY_CONTRACT.md); bounded cookie/event/ACK lifecycle; explicit consumer handling | [TestWatchOperationsMatchFreshNative](../internal/osd/watch_notify_test.go) | U |
| ASSERT_VER | `0x1208` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| LIST_WATCHERS | `0x1209` [WATCHERS_CONTRACT.md](../internal/osd/WATCHERS_CONTRACT.md); bounded standalone head watcher inventory | [TestWatchersRequestMatchesFreshNativeEncoder](../internal/osd/watchers_native_test.go) | U |
| LIST_SNAPS | `0x120a` [SNAPSHOT_CONTRACT.md](../internal/osd/SNAPSHOT_CONTRACT.md); caller snapshots; head/snapshot admission and vector restrictions | [TestSnapshotRequestsMatchFreshNativeEncoder](../internal/osd/snapshot_native_test.go) | U |
| WRITE | `0x2201` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| WRITEFULL | `0x2202` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| TRUNCATE | `0x2203` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| ZERO | `0x2204` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| DELETE | `0x2205` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| APPEND | `0x2206` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded mutation; sent loss remains unknown | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | R12 |
| CREATE | `0x220d` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| ROLLBACK | `0x220e` [SNAPSHOT_CONTRACT.md](../internal/osd/SNAPSHOT_CONTRACT.md); caller snapshots; head/snapshot admission and vector restrictions | [TestSnapshotRequestsMatchFreshNativeEncoder](../internal/osd/snapshot_native_test.go) | U |
| WATCH | `0x220f` [WATCH_NOTIFY_CONTRACT.md](../internal/osd/WATCH_NOTIFY_CONTRACT.md); bounded cookie/event/ACK lifecycle; explicit consumer handling | [TestWatchOperationsMatchFreshNative](../internal/osd/watch_notify_test.go) | U |
| OMAPGETKEYS | `0x1211` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); bounded raw page/header results; caller pagination | [TestOmapRequestMatchesFreshNativeEncoder](../internal/osd/omap_native_test.go) | U |
| OMAPGETVALS | `0x1212` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); bounded raw page/header results; caller pagination | [TestOmapRequestMatchesFreshNativeEncoder](../internal/osd/omap_native_test.go) | U |
| OMAPGETHEADER | `0x1213` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); bounded raw page/header results; caller pagination | [TestOmapRequestMatchesFreshNativeEncoder](../internal/osd/omap_native_test.go) | U |
| OMAPGETVALSBYKEYS | `0x1214` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); bounded raw page/header results; caller pagination | [TestOmapRequestMatchesFreshNativeEncoder](../internal/osd/omap_native_test.go) | U |
| OMAPSETVALS | `0x2215` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); ordered writes with request/output budgets | [TestOmapWriteRequestMatchesFreshNativeEncoder](../internal/osd/omap_write_native_test.go) | U |
| OMAPSETHEADER | `0x2216` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); ordered writes with request/output budgets | [TestOmapWriteRequestMatchesFreshNativeEncoder](../internal/osd/omap_write_native_test.go) | U |
| OMAPCLEAR | `0x2217` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); ordered writes with request/output budgets | [TestOmapWriteRequestMatchesFreshNativeEncoder](../internal/osd/omap_write_native_test.go) | U |
| OMAPRMKEYS | `0x2218` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); ordered writes with request/output budgets | [TestOmapWriteRequestMatchesFreshNativeEncoder](../internal/osd/omap_write_native_test.go) | U |
| OMAPRMKEYRANGE | `0x222c` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); bounded half-open key range; binary key ownership | [TestOmapRemoveKeyRangeRequestMatchesFreshNativeEncoder](../internal/osd/omap_range_native_test.go) | U |
| OMAP_CMP | `0x1219` [OMAP_CONTRACT.md](../internal/osd/OMAP_CONTRACT.md); raw comparisons, signed refusal and FAILOK ordering | [TestOmapCompareRequestMatchesFreshNativeEncoder](../internal/osd/omap_compare_native_test.go) | U |
| COPY_FROM | `0x221a` [COPY_FROM_CONTRACT.md](../internal/osd/COPY_FROM_CONTRACT.md); bounded head source/version; no autonomous routing or uncertain replay | [TestCopyFromFreshNativeRequests](../internal/osd/copyfrom_native_test.go) and [TestCopyFromCompoundAdmission](../internal/osd/copyfrom_compound_test.go) | R24, R25 |
| COPY_FROM2 | `0x222d` [COPY_FROM_CONTRACT.md](../internal/osd/COPY_FROM_CONTRACT.md); bounded head source/version; no autonomous routing or uncertain replay | [TestCopyFromFreshNativeRequests](../internal/osd/copyfrom_native_test.go) and [TestCopyFromCompoundAdmission](../internal/osd/copyfrom_compound_test.go) | R24, R25 |
| UNDIRTY | `0x221c` [DIRTY_CONTRACT.md](../internal/osd/DIRTY_CONTRACT.md); standalone flagless head operations; raw bool or ONDISK result | [TestDirtyOperationsValidationModes](../internal/osd/dirty_test.go) | U |
| ISDIRTY | `0x121d` [DIRTY_CONTRACT.md](../internal/osd/DIRTY_CONTRACT.md); standalone flagless head operations; raw bool or ONDISK result | [TestDirtyOperationsValidationModes](../internal/osd/dirty_test.go) | U |
| SETALLOCHINT | `0x2223` [ALLOC_HINT_CONTRACT.md](../internal/osd/ALLOC_HINT_CONTRACT.md); advisory size/flags, no allocation guarantee | [TestAllocHintRequestMatchesFreshNativeEncoder](../internal/osd/allochint_native_test.go) | U |
| WRITESAME | `0x2226` [WRITESAME_CONTRACT.md](../internal/osd/WRITESAME_CONTRACT.md); bounded pattern/extent; explicit snapshot and compound restrictions | [TestWriteSameMatchesFreshNativeVectors](../internal/osd/writesame_native_test.go) | U |
| CMPEXT | `0x1220` [CONTRACT.md](../internal/osd/CONTRACT.md); bounded vectors, signed per-op results and completion/uncertainty | [TestMutationVectorWireAndOwnership](../internal/osd/mutations_test.go) | U |
| CACHE_FLUSH | `0x821f` [CACHE_POOL_CONTRACT.md](../internal/osd/CACHE_POOL_CONTRACT.md); standalone inputless cache-tier operation with the native request flags; caller targets the tier; no promotion or flush policy | [TestCacheOperationsRetain21FreshNativeRequests](../internal/osd/cache_op_native_test.go) and [TestCacheOperationRefusalsBeforeEncoding](../internal/osd/cache_op_test.go) | R26 |
| CACHE_EVICT | `0x8220` [CACHE_POOL_CONTRACT.md](../internal/osd/CACHE_POOL_CONTRACT.md); standalone inputless cache-tier operation with the native request flags; caller targets the tier; no promotion or flush policy | [TestCacheOperationsRetain21FreshNativeRequests](../internal/osd/cache_op_native_test.go) and [TestCacheOperationRefusalsBeforeEncoding](../internal/osd/cache_op_test.go) | R26 |
| CACHE_TRY_FLUSH | `0x8221` [CACHE_POOL_CONTRACT.md](../internal/osd/CACHE_POOL_CONTRACT.md); standalone inputless cache-tier operation with the native request flags; caller targets the tier; no promotion or flush policy | [TestCacheOperationsRetain21FreshNativeRequests](../internal/osd/cache_op_native_test.go) and [TestCacheOperationRefusalsBeforeEncoding](../internal/osd/cache_op_test.go) | R26 |
| CACHE_PIN | `0x2224` [CACHE_POOL_CONTRACT.md](../internal/osd/CACHE_POOL_CONTRACT.md); standalone inputless cache-tier operation with the native request flags; caller targets the tier; no promotion or flush policy | [TestCacheOperationsRetain21FreshNativeRequests](../internal/osd/cache_op_native_test.go) and [TestCacheOperationRefusalsBeforeEncoding](../internal/osd/cache_op_test.go) | R26 |
| CACHE_UNPIN | `0x2225` [CACHE_POOL_CONTRACT.md](../internal/osd/CACHE_POOL_CONTRACT.md); standalone inputless cache-tier operation with the native request flags; caller targets the tier; no promotion or flush policy | [TestCacheOperationsRetain21FreshNativeRequests](../internal/osd/cache_op_native_test.go) and [TestCacheOperationRefusalsBeforeEncoding](../internal/osd/cache_op_test.go) | R26 |
| SET_REDIRECT | `0x2227` [MANIFEST_CONTRACT.md](../internal/osd/MANIFEST_CONTRACT.md); standalone flagless manifest operations; no reference-counted chunk/tier workflow | [TestInactiveCopyFromRejectedBeforeStandaloneBranches](../internal/osd/inactive_copyfrom_test.go) | U |
| SET_CHUNK | `0x8228` [MANIFEST_CONTRACT.md](../internal/osd/MANIFEST_CONTRACT.md); standalone offset-zero chunk with WITH_REFERENCE and the native READ request; caller supplies the target; no chunking, dedup or snapshot reference policy | [TestSetChunkRetainsFreshNativeRequests](../internal/osd/set_chunk_native_test.go) and [TestSetChunkRefusalsBeforeEncoding](../internal/osd/set_chunk_test.go) | R27 |
| UNSET_MANIFEST | `0x222a` [MANIFEST_CONTRACT.md](../internal/osd/MANIFEST_CONTRACT.md); standalone flagless manifest operations; no reference-counted chunk/tier workflow | [TestInactiveCopyFromRejectedBeforeStandaloneBranches](../internal/osd/inactive_copyfrom_test.go) | U |
| GETXATTR | `0x1301` [METADATA_CONTRACT.md](../internal/osd/METADATA_CONTRACT.md); bounded binary key/value/comparison contracts; no bulk xattr variants | [TestXattrRequestPackedUnionAndBinaryOwnership](../internal/osd/xattr_test.go) | U |
| GETXATTRS | `0x1302` [METADATA_CONTRACT.md](../internal/osd/METADATA_CONTRACT.md); bounded binary key/value/comparison contracts; no bulk xattr variants | [TestXattrRequestPackedUnionAndBinaryOwnership](../internal/osd/xattr_test.go) | U |
| CMPXATTR | `0x1303` [METADATA_CONTRACT.md](../internal/osd/METADATA_CONTRACT.md); bounded binary key/value/comparison contracts; no bulk xattr variants | [TestXattrRequestPackedUnionAndBinaryOwnership](../internal/osd/xattr_test.go) | U |
| SETXATTR | `0x2301` [METADATA_CONTRACT.md](../internal/osd/METADATA_CONTRACT.md); bounded binary key/value/comparison contracts; no bulk xattr variants | [TestXattrRequestPackedUnionAndBinaryOwnership](../internal/osd/xattr_test.go) | U |
| RMXATTR | `0x2304` [METADATA_CONTRACT.md](../internal/osd/METADATA_CONTRACT.md); bounded binary key/value/comparison contracts; no bulk xattr variants | [TestXattrRequestPackedUnionAndBinaryOwnership](../internal/osd/xattr_test.go) | U |
| CALL | `0x1401` [CLASS_CONTRACT.md](../internal/osd/CLASS_CONTRACT.md); bounded raw class/method/input/output; caller interprets class semantics | [TestClassCallRequestMatchesFreshNativeEncoder](../internal/osd/class_native_test.go) | U |
| PGNLS | `0x1505` [PG_LIST_CONTRACT.md](../internal/osd/PG_LIST_CONTRACT.md); one bounded caller-selected PG page; no pool-wide iterator | [TestPGListRequestMatchesFreshNativeEncoder](../internal/osd/pg_list_test.go) | U |
| PGNLS_FILTER | `0x1506` [PG_LIST_CONTRACT.md](../internal/osd/PG_LIST_CONTRACT.md); bounded filter form; inherited PG page/cursor identity | [TestPGListFilterEncodesObjecterLayout](../internal/osd/pg_list_filter_test.go) | U |
| SCRUBLS | `0x1507` [SCRUB_LIST_CONTRACT.md](../internal/osd/SCRUB_LIST_CONTRACT.md); bounded scrub listing and raw object metadata; no scrub policy | [TestScrubListArgumentAndEnvelope](../internal/osd/scrub_list_test.go) | U |

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
| R2 | exact-commit | `dec9084f0a858e51224f16a9178c1218f8dd8e12` CRC/AES/AES IPv4 host and `66084e170a7f5805bd9126ac28e9f08f8c92b25a` secure/aes256k/aes256k IPv6 container, Linux/amd64. Go service registration, status/task report, native ServiceMap observation, acknowledged removal and cleanup passed; metadata update was queued only, application unverified; no renewal/failover claim | [MGR service](../integration/fixture/MGR_SERVICE_VALIDATION.md) |
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
| R17 | historical | No committed tested tree/file hashes identified by this record; container runtime, CRC, AES client/service, IPv4, legacy-v6 single BlueStore. MON and OSD declined the snappy/zstd offer while commands succeeded. No live compressed frame, secure compression, zlib or daemon-peer proof | [Compression decline](../integration/fixture/MSGR_COMPRESSION_VALIDATION.md) |
| R18 | file-attributed | Pinned source/archive and producer/build/segment hashes in the receipt, Linux/amd64. Eight fresh native MGR service vectors cover five message types, binary map values, omitted versus empty reports, current configure and close codec. Offline header projection; no live lifecycle/renewal/failover or metadata-application claim | [MGR codec vectors](../internal/msgr/testdata/mgr-service-native-README.md) |
| R19 | exact-commit | `97ad9d7a245f952d038e1574a5903bd93729ea1a`, Linux/amd64, CRC/AES IPv4 host and secure/aes256k IPv6 container. Ticket-expiry report, new active MGR identity, terminal old-handle refusals, sampled stable native absence, explicit registration and acknowledged removal. No applied metadata, lost-close echo or wire replay-count claim | [MGR service recovery](../integration/fixture/MGR_SERVICE_RECOVERY_VALIDATION.md) |
| R20 | exact-commit | `bbb84c8590222dd70bf8fe13967995ae2e84de9a`, Linux/amd64 CRC/AES IPv4 host and secure/aes256k IPv6 container. Withheld receive: native removal precedes local cancellation; discarded send: native registration outlasts local unconfirmed Close, then expires. Both preserve uncertainty/terminal/idempotent result and show one MGR dial. No captured echo or native execution-count claim | [MGR unconfirmed close](../integration/fixture/MGR_SERVICE_CLOSE_VALIDATION.md) |
| R21 | exact-commit | `e8c18f7c9f9913f6abacf12184943fd5b8d0c46e`, Linux/amd64 CRC/AES IPv4 host and secure/aes256k IPv6 container. Independent native MGR daemon-state reads match initial metadata, exact replacements and removal of omitted keys; a locally refused empty update leaves previous state intact. Initial MON ServiceMap metadata remains distinct. Owned selftest module restored; no same-field native registration parity claim | [MGR metadata application](../integration/fixture/MGR_SERVICE_METADATA_VALIDATION.md) |
| R22 | exact-commit | `b1323d481f4a8a769259e531f898cbeffb2ed9a4`, Linux/amd64 CRC/AES IPv4 host and secure/aes256k IPv6 container. Distinct Go/native registrations match caller metadata and exact initial/replacement/empty status maps, preserve per-registration GID/start epoch and reach sampled stable removal. A newer beacon gates Go nil-report preservation. Native system metadata is excluded; native shutdown return is not an echo claim. First failed removal boundary is retained separately | [MGR native field comparison](../integration/fixture/MGR_SERVICE_COMPARISON_VALIDATION.md) |
| R23 | exact-commit | `4a1d2e44d81990a6f88302712f1070c3dd109bea`, Linux/amd64 CRC/AES IPv4 host and secure/aes256k IPv6 container. Native MON down flag gives unavailable MgrMap and zero pre-activation MGR dials. Deadline/cancel/client-close remain known; canceled identities stay absent after restoration. Pending initial setup completes, an explicit call reuses a released identity, both are removed, and flag/module state is restored. Controlled map absence, not zero-daemon cold bootstrap | [MGR initially unavailable](../integration/fixture/MGR_SERVICE_UNAVAILABLE_VALIDATION.md) |
| R24 | file-attributed | Pinned Ceph v20.2.4 native SDK, Linux/amd64; five fresh v6 COPY_FROM/COPY_FROM2 request encodings and separate native decoders. COPY_FROM and truncate-enabled COPY_FROM2 match Go completely; flagless COPY_FROM2 retains exactly twelve extra native zero bytes and the matching operation payload length. No new daemon execution or native v8/v9 claim | [COPY_FROM native requests](../internal/osd/testdata/copy-from-native-README.md) |
| R25 | exact-commit | `16c3bfaf07ef54408ddfe655ae7eb77216c14f51`, Linux/amd64 CRC/AES IPv4 host legacy-v6 single BlueStore and secure/aes256k IPv6 container request-v8/reply-v6 three BlueStore. Both copy opcodes plus xattr suffixes cover absent/populated destinations, native bytes/xattrs/OMAP/header/source mtime/user versions, five known-unsent refusals and next-TID continuity. Independent rados cp reproduces the empty-OMAP-key omission; first failed expectation is retained. Six objects and all owned containers/networks removed; no native compound twin or post-source-read destination-loss claim | [COPY_FROM compound acceptance](../integration/fixture/OSD_COPY_FROM_COMPOUND_VALIDATION.md) |
| R26 | exact-commit | `9077d5e0beb70a3409149342cc3fa7e204747e30` (run 39878019051), Linux/amd64 secure/aes256k IPv6 container legacy-v6 single BlueStore, owned writeback tier without overlay. Go-originated CACHE_EVICT/PIN/FLUSH/TRY_FLUSH/UNPIN once each against a closed native observer (IGNORE_CACHE STAT+READ, C++ is_dirty): dirty EBUSY, pinned EPERM, flush write-back and clean dirty state, try-flush 0 with B in both pools and clean dirty state, clean eviction with unchanged base, absent ENOENT; owned heads removed. No live EAGAIN, lost reply, other profile or SET_CHUNK claim | [Go cache-op acceptance](../integration/fixture/OSD_CACHE_OP_GO_VALIDATION.md) |
| R27 | exact-commit | `19e162af43c980ccc613cdaf76365dbac7bb7e18` (run 39902724497), Linux/amd64 secure/aes256k IPv6 container legacy-v6 single BlueStore, two owned plain replicated pools. Go-originated SET_CHUNK and source DELETE once each against native owned-pool observation: absent source -2, out of range -34, absent target -2 with per-op -115, chunk 0, overlaps -95, second-source chunk 0, exact by_object `chunk_refs` with one then two source heads, source bytes/version/mtime kept, deletions released one reference then removed the target; owned pools removed. No non-zero source offset, live lost reply, scoped identity, snapshot clone or other profile claim | [Go SET_CHUNK acceptance](../integration/fixture/OSD_SET_CHUNK_GO_VALIDATION.md) |
| R31 | exact-commit | `fa71fb23a132297a07209d1f8e6564bdec24c492` (run 39926008773, attempt 1), Linux/amd64 CRC/AES IPv4 host, three MONs/two MGRs. Native owned-key absence, applied binary A, distinct overwrite B before/after another authenticated MON, original OutcomeUnknownError and one native removal plus fresh absence passed. CRC outbound Message/MonCommand header TID/sequence and connection bindings retained; local writes are not server execution counts. Seven fixture-container terminations reported; no independent Docker inventory, secure-frame identity or MGR applied-mutation claim | [MON command outcome](../integration/fixture/MON_COMMAND_OUTCOME_VALIDATION.md) |

## Acceptance boundary

This join accounts for the declared frame/message/role rows, implemented opcode
contracts, unsupported opcode classifications and their evidence
boundaries; it does not establish full native acceptance or #5's command-channel
coverage. [FEATURE_SEMANTICS.md](../internal/session/FEATURE_SEMANTICS.md) adds
all advertised bit meanings, role obligations and alias/incarnation limits.
Unjoined rows, historical source identities and profile-specific failure/oracle
gaps remain visible; do not enable new bits to improve counts.
A missing native oracle/receipt is different from missing wire code. Daemon-only
operations, server handler constraints and consumer-owned state remain distinct.

The #4 reconciliation covers frame tags here, nested CephX exchanges in their
ledger, message versions/contracts in MESSAGE_LEDGER, role masks and bit aliases
in the feature ledgers, and every OSD catalog entry through the two opcode
tables. The compression plan separates codec tests, native declined offers and
unreachable client compressed frames; zlib/secure compression remain deferred.
The MDS map contract's event list is explicitly an earlier map-codec stage,
with current session/request/capability/recovery paths linked separately.
The OSD core/dirty contracts now distinguish base-v6 scope, implemented
watch/backoff/FADVISE and available IGNORE_CACHE fixtures from unimplemented
tier/manifest operations. This is implementation/evidence accounting, not a
request to erase historical failures or require every future native profile
before classifying the current wire support.

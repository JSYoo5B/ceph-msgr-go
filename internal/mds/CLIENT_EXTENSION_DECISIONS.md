# CephFS client feature extension decisions

Pinned reference: Ceph `v20.2.4`, commit
`7f793731f1b39eb4f465e960113d2363c311b964`. Source audit: 2026-10-23.
Source paths below are relative to the pinned `src/` directory. They are an
independent restatement of wire and server behavior from LGPL-2.1 sources; no
code is copied.

This document decides, for every CephFS client feature bit in the pinned
`mds/cephfs_features.h` (`CEPHFS_FEATURE_MAX` 23, names from
`mds/cephfs_features.cc:11-38`), whether this repository declares it, and for
each withheld bit what the wire, lifecycle, server and consumer obligations are.
It is a source audit plus a drift check. The audit itself enabled no feature
bit and is not native interoperability evidence; each later declaration lists
its own evidence under "Declared extensions". The declared sets themselves
are specified in
[MDS_SESSION_CONTRACT.md](../../cephmsgr/MDS_SESSION_CONTRACT.md) and
[MDS_CEPHFS_CONTRACT.md](../../cephmsgr/MDS_CEPHFS_CONTRACT.md); the codes are
`ClientFeatures` and `CephFSClientFeatures` in [session.go](session.go).

## Vocabulary

A decision applies to the current product:

- `baseline`: declared by at least one current profile; covered by its
  contract and evidence, not revisited here.
- `implement`: the next communication stage for this bit. It still requires
  fresh native vectors for any new codec, plus operation, push and failure
  acceptance for the selected profile with independent namespace or inode
  observation, before the bit can be declared.
- `defer`: optional; no usable caller contract or native oracle yet. Stays
  withheld.
- `consumer-owned`: the obligation the bit expresses is a policy of the layer
  that uses this library (permission, capability or inode policy, or result
  interpretation). The library keeps the bit withheld and keeps the raw wire
  data available.

The gap column separates what is missing:

- `codec`: a message or field layout this repository does not encode/decode.
- `lifecycle`: request, push, acknowledgement or recovery semantics the
  session state machine does not implement.
- `policy`: a promise about client behavior that the transport cannot keep on
  the caller's behalf.
- `optional`: the selected baseline can omit the bit without losing a
  communication path (performance, telemetry or a gate the MDS never reads).
- `evidence`: the wire path already exists; only the declaration and its
  native admission evidence are missing.

## Feature table

The drift test [client_extension_decisions_test.go](client_extension_decisions_test.go)
binds this table to the code: one row per pinned bit 0 to 23, names equal to
the pinned feature names, Session/CephFS columns equal to the bits of
`ClientFeatures` and `CephFSClientFeatures`, `baseline` exactly for declared
bits, a decision for every withheld bit, a detail section per withheld bit, and
no declared bit beyond the pinned list.

| Bit | Name | Session | CephFS | Gap | Decision |
| --- | --- | --- | --- | --- | --- |
| 0 | `reserved` | declared | declared | - | baseline |
| 1 | `reserved` | declared | declared | - | baseline |
| 2 | `reserved` | declared | declared | - | baseline |
| 3 | `reserved` | declared | declared | - | baseline |
| 4 | `reserved` | declared | declared | - | baseline |
| 5 | `jewel` | declared | declared | - | baseline |
| 6 | `kraken` | declared | declared | - | baseline |
| 7 | `luminous` | declared | declared | - | baseline |
| 8 | `mimic` | declared | declared | - | baseline |
| 9 | `reply_encoding` | withheld | declared | - | baseline |
| 10 | `reclaim_client` | declared | declared | - | baseline |
| 11 | `lazy_caps_wanted` | withheld | withheld | policy, optional | consumer-owned |
| 12 | `multi_reconnect` | withheld | declared | - | baseline |
| 13 | `deleg_ino` | withheld | withheld | codec, lifecycle, policy | implement |
| 14 | `metric_collect` | withheld | withheld | codec, optional | defer |
| 15 | `alternate_name` | withheld | declared | - | baseline |
| 16 | `notify_session_state` | withheld | withheld | lifecycle, optional | defer |
| 17 | `op_getvxattr` | withheld | withheld | optional | consumer-owned |
| 18 | `32bits_retry_fwd` | withheld | declared | - | baseline |
| 19 | `new_snaprealm_info` | withheld | declared | - | baseline |
| 20 | `has_owner_uidgid` | withheld | declared | - | baseline |
| 21 | `client_mds_auth_caps` | withheld | withheld | policy | consumer-owned |
| 22 | `charmap` | withheld | withheld | codec, policy | implement |
| 23 | `blockdiff` | withheld | withheld | optional | consumer-owned |

Decision counts for the eight bits withheld by both profiles: implement 2,
defer 2, consumer-owned 4.

## How the MDS uses a client bit

The pinned MDS reads the client's declared bitset for the bits withheld at
audit time (10 to 14, 16, 17 and 21 to 23) only in the places below. Baseline
bits have further behavior branches that their profile contracts already
cover: MIMIC (8) selects v7 session messages and REPLY_ENCODING (9) the
versioned reply encodings (for example `mds/Server.cc:660`, `mds/CInode.cc:4217`),
and NEW_SNAPREALM_INFO (19) selects the snap trace format
(`mds/Server.cc:12034`).

- Admission. `Server::handle_client_session` REJECTs a REQUEST_OPEN whose
  bitset lacks any bit of the filesystem's `required_client_features`, with
  `error_string` `missing required features '0x…'` (`mds/Server.cc:743-753`).
  `Server::handle_client_reconnect` answers CLOSE for the same reason
  (`mds/Server.cc:1636-1662`), and `Server::update_required_client_features`
  evicts open sessions when a requirement is added (`mds/Server.cc:1778-1802`).
  Operators set the requirement with `fs required_client_features`
  (`mon/FSCommands.cc:959-1034`) or with the deprecated
  `fs set min_compat_client` (`mon/FSCommands.cc:766-777`), which
  `MDSMap::set_min_compat_client` maps to a single release-alias bit: Nautilus
  requires bit 12, Octopus and every later release require bit 13
  (`mds/MDSMap.cc:1314-1328`). There is no CephFS release bit after Octopus.
- Behavior of withheld bits. Bit 11 (`mds/Locker.cc:2821-2830`), bit 13
  (`mds/Server.cc:2558-2582`), bit 16 (`mds/Server.cc:650-670`, read from the
  REQUEST_OPEN message itself), bit 21 (`mds/Server.cc:757-760` and
  `1646-1649`) and bit 22 (`mds/Server.cc:4785-4801`).

Bits 10, 12, 14, 17 and 23 are never read from the client's bitset for
behavior. The native client gates those features on the MDS's advertised
bitset instead (`client/Client.cc:17459`, `3317`, `7350`, `8099`, `10052`,
through `Client::make_request`'s `feature_needed` at `client/Client.cc:2201`).
For them, the client declaration only satisfies an operator requirement.

Explicit refusal is preserved for every withheld bit. The unit test
[TestMDSWithheldFeatureRequirementIsRejectedExplicitly](../../cephmsgr/mds_withheld_feature_test.go)
checks, for both profiles and each undeclared bit of the first block, that the
REQUEST_OPEN declaration stays fixed, the native-format REJECT returns
`*MDSSessionRejectedError` with the raw reason and no session, and no
client-initiated retry follows. The reconnect CLOSE is covered by
[TestCephFSReconnectDeniedAndSplit](../../cephmsgr/mds_reconnect_test.go). The
live session-profile REJECT for a `reply_encoding` requirement is recorded in
[MDS_SESSIONS_VALIDATION.md](../../integration/fixture/MDS_SESSIONS_VALIDATION.md);
no live CephFS-profile refusal has been recorded.

## Declared extensions

These bits were staged as `implement` by the audit and are now declared. Their
rows are `baseline`; the evidence that changed the decision stays here.

### Bit 10: reclaim_client (declared)

- Wire: MClientReclaim (type 27, v1/compat1: uuid, flags with
  `CEPH_RECLAIM_RESET` or `FLAG_FINISH` alone) and MClientReclaimReply
  (type 28: result, OSD map epoch, address vector). No request/reply field or
  push changes with the client bit.
- Server use: none beyond admission. The pinned source reads
  `CEPHFS_FEATURE_RECLAIM_CLIENT` only in the native client, and there against
  the MDS's advertisement (`client/Client.cc:17459`). Keeping a session for
  reclaim during reconnect is driven by the `timeout` session metadata, not by
  this bit (`mds/Server.cc:1878-1888`). Declaring it therefore changes no MDS
  behavior; it satisfies `fs required_client_features add reclaim_client`.
  By source, an open session declaring it is also not evicted when that
  requirement is added (`mds/Server.cc:1778-1802`); that path is not exercised
  live.
- Ack/recovery: the RESET reply is the only answer; FINISH has none. A lost
  reply is an unknown outcome because the reply carries no request identity.
- Server prerequisites: the MDS advertises RECLAIM_CLIENT and is at least in
  clientreplay; the target session has the same CephX identity and a `uuid`.
- Consumer-owned: the uuid and timeout metadata, and the decision to reclaim.
- Code: `ClientFeatures` and `CephFSClientFeatures` in [session.go](session.go)
  set bit 10; [reclaim.go](reclaim.go), `MDSSession.Reclaim` and
  `FinishReclaim` in [mds_session.go](../../cephmsgr/mds_session.go); tests
  [TestEncodeReclaimMatchesTheNativeLayout](reclaim_test.go),
  [TestMDSReclaimRefusalsAndUnknownOutcomes](../../cephmsgr/mds_reclaim_test.go).
- Evidence: the live leaf `admit_required_reclaim_client` of
  `TestCephMDSSessionsIntegration` opens a session in each profile while the
  owned filesystem requires `reclaim_client`, and the reclaim leaf
  `reclaim_evicts_the_target_session` runs in the same suite on the same
  source, GitHub Actions run 39885092766 on both `mds-sessions` rows
  ([MDS_SESSIONS_VALIDATION.md](../../integration/fixture/MDS_SESSIONS_VALIDATION.md),
  [MDS_RECLAIM_VALIDATION.md](../../integration/fixture/MDS_RECLAIM_VALIDATION.md)).
  The REQUEST_OPEN layout is covered by the fresh native vector; the encoder
  copies the bitset image unchanged, and the native `session ls` of both live
  profiles prints the declared images.
- Not claimed: reclaim by a native libcephfs twin, multi-rank or failover
  reclaim, and a requirement that combines `reclaim_client` with another bit.


### Bit 12: multi_reconnect (CephFS profile declared)

- Wire: MClientReconnect v5/compat4 in the data segment with the `more` flag,
  split when the state exceeds a frame.
- Server use: none beyond admission (and `min_compat_client` Nautilus). The
  native client splits only when the MDS advertises the bit
  (`client/Client.cc:3317`).
- Ack/recovery: OPEN after the final message; CLOSE when denied.
- Server prerequisites: the MDS advertises MULTI_RECONNECT (already required
  by `ReconnectSession`) and is in its reconnect phase.
- Consumer-owned: the reconnect capability and realm state.
- Current code: [reconnect.go](reconnect.go), `ReconnectSession` in
  [mds_reconnect.go](../../cephmsgr/mds_reconnect.go); tests
  [TestEncodeReconnectMatchesFreshNative](native_reconnect_test.go),
  [TestCephFSReconnectDeniedAndSplit](../../cephmsgr/mds_reconnect_test.go).
- Declaration: CephFS only; the session-only profile cannot reconnect and
  keeps bit 12 withheld. The split acceptance uses actual retained caps on
  256 owned files, a 64 KiB frame budget and native nonfinal/final handler
  logs, then compares namespace/inodes independently. Admission requires
  `multi_reconnect` and checks CephFS OPEN plus session-only REJECT.
  See [MDS_MULTI_RECONNECT_VALIDATION.md](../../integration/fixture/MDS_MULTI_RECONNECT_VALIDATION.md)
  for the exact tested source, profiles and remaining failure boundaries.

## Withheld features

### Bit 11: lazy_caps_wanted

- Wire: no new message. The bit changes server behavior only.
- Server use: when a stale session resumes, `Locker::resume_stale_caps` stops
  reissuing capabilities at the first one that is not "notable" (not
  revoking, no writeable range, not wanting exclusive or file read/write caps)
  (`mds/Locker.cc:2821-2830`; `revoke_stale_caps` at `2754-2770`).
- Client obligation: after STALE returns to OPEN, the client re-announces the
  wanted mask of each capability it still needs with an MClientCaps UPDATE.
- Consumer-owned: capability table, wanted masks and the re-announce policy.
  The library sends no capability message on its own
  ([MDS_CEPHFS_CONTRACT.md](../../cephmsgr/MDS_CEPHFS_CONTRACT.md), "MDS
  pushes and client answers"); `SendCaps` already carries `Wanted`.
- Gap: policy; omission only keeps the MDS's eager reissue (optional).
  Decision: consumer-owned. A future explicit opt-in would require the caller
  to state that it implements the re-announce policy, plus a live stale/resume
  acceptance. No higher-layer operation is blocked.


### Bit 13: deleg_ino

- Wire: when the MDS option `mds_allow_async_dirops` is true (default true,
  `common/options/mds.yaml.in:1008-1011`) and the client declares the bit,
  the create reply's extra blob becomes `openc_response_t` (struct v1/compat1:
  u64 `created_ino`, `interval_set<inodeno_t>` delegated ranges) instead of
  the bare u64 inode (`mds/Server.cc:2558-2582`, `messages/MClientReply.h:304-334`).
  The same function fills the extra of completed-request replies for any
  inode-creating request (`mds/Server.cc:2625-2648`, `2864-2872`).
- Async directory operations: a request with `CEPH_MDS_FLAG_ASYNC` (4) and a
  delegated inode in `head.ino` is queued like a REPLAY during reconnect and
  its inode reserved (`mds/Server.cc:336-349`); new inodes are taken from the
  session's delegated set (`mds/Server.cc:3562-3610`). Async creates and
  unlinks rely on the directory lock-cache capabilities `CEPH_CAP_DIR_CREATE`
  and `CEPH_CAP_DIR_UNLINK` (`include/ceph_fs.h:989-991`). Delegated inodes
  are journaled with the session, cleared when it closes
  (`mds/Server.cc:925-938`) and handed to purge when a session misses the
  reconnect window (`mds/Server.cc:1897-1898`).
- Native reference behavior: the userspace client declares the bit, decodes
  `openc_response_t` and discards the delegated ranges; it never issues an
  async request (`client/Client.cc:2018-2034`). The kernel client is the only
  async producer, so no userspace native oracle exists for the async path.
- Consumer-owned: inode allocation from delegated ranges, async completion
  tracking, replay ordering and directory capability policy. A native inode
  allocator is not copied into this repository.
- Current code: `Submit` refuses the ASYNC flag and a preset inode
  ([mds_request.go](../../cephmsgr/mds_request.go)); `MDSReply.Extra` stays
  raw; test-only consumers read the 8-byte form, for example
  `integration/mds_cephfs_completion_test.go`.
- Gap: codec (`openc_response_t`), lifecycle (async requests, their
  completion, recovery and the loss of delegated ranges on session end) and
  policy (allocation).
- Decision: implement a decode-only stage that matches the native userspace
  client: an `openc_response_t` decoder for create and completed-request
  extras, raw delegated ranges preserved, ASYNC and preset inodes still
  refused. Async directory operations stay deferred until a caller contract
  and an oracle for completion and recovery exist. Dependencies: a fresh
  native vector of `openc_response_t`, updated test-only consumers, a live
  CREATE and completed-CREATE acceptance with the bit declared, and an
  admission acceptance with `deleg_ino` required. This also unblocks
  filesystems that use the deprecated `min_compat_client` at Octopus or later.

### Bit 14: metric_collect

- Wire: MClientMetrics (type 29, v1/compat1, `messages/MClientMetrics.h:14-51`):
  a vector of typed metric payloads (cap info, read/write/metadata latency,
  dentry lease, opened files, pinned caps, opened inodes, I/O sizes and the
  latency average/deviation forms listed in `mds/cephfs_features.h`).
  REQUEST_OPEN carries a client metric spec; OPEN carries the MDS's spec
  (already decoded raw as `MetricFlags`).
- Server use: none beyond admission. `MetricsHandler` accepts metrics from any
  client session, drops them during recovery and for sessionless connections,
  and sends no acknowledgement (`mds/MetricsHandler.cc:27-31`, `329-346`).
  The native client sends them only to an active rank 0 that advertises the bit
  (`client/Client.cc:7335-7353`).
- Consumer-owned: every metric value (latencies, cap and lease hit ratios,
  open counts), which only the I/O layer can measure.
- Current code: not represented
  ([MESSAGE_LEDGER.md](../../cephmsgr/MESSAGE_LEDGER.md)); REQUEST_OPEN sends
  an empty metric spec.
- Gap: codec; optional telemetry. Decision: defer until a caller contract for
  raw caller-supplied payloads and a native oracle exist. No operation is
  blocked.

### Bit 16: notify_session_state

- Wire: no new message. The MDS reads the bit from a REQUEST_OPEN that arrives
  while the session is already opening, open, stale or being killed: for an
  open session on a non-stopping MDS it resends OPEN with the current push
  sequence and supported features, plus FORCE_RO when read-only; otherwise it
  drops the request (`mds/Server.cc:650-670`).
- Client obligation: accept a repeated OPEN and FORCE_RO on an existing
  session as a state notification.
- Current code: one REQUEST_OPEN per local session; a canceled open keeps the
  still-opening handle on the lossless transport instead of reopening
  ([MDS_SESSION_CONTRACT.md](../../cephmsgr/MDS_SESSION_CONTRACT.md)). A later
  OPEN is already delivered as an event
  ([TestMDSSessionEventsStaleAndFlushAck](../../cephmsgr/mds_session_test.go)).
- Gap: lifecycle (a duplicate-open path that does not exist); optional.
  Decision: defer. No operation is blocked.

### Bit 17: op_getvxattr

- Wire: request `CEPH_MDS_OP_GETVXATTR` (0x00106, `include/ceph_fs.h:403`)
  with the inode path and the xattr name as the second path; the reply extra is
  a struct v1/compat1 string (`mds/Server.cc:7077-7266`). Non-`ceph.`
  names answer -ENODATA; the native client also maps -EOPNOTSUPP to -ENODATA
  (`client/Client.cc:8078-8110`).
- Server use: none beyond admission; the native client requires the MDS's
  advertisement (`client/Client.cc:8099`).
- Current code: `OpGetVxattr` exists in [request.go](request.go) and the raw
  `Submit` path does not restrict the operation number; there is no exported
  public constant, test or native evidence for this operation.
- Consumer-owned: choosing the vxattr, checking the MDS advertisement with
  `MDSFeatureBitsetHas` and interpreting the returned text.
- Gap: optional (the bit is never read by the MDS). Decision: consumer-owned;
  an exported constant and native acceptance are deferred until a consumer
  needs them. No operation is blocked by absent wire data.

### Bit 21: client_mds_auth_caps

- Wire: no new message. OPEN's cap-auth grants are already decoded raw
  (`CapAuth` in [session.go](session.go)).
- Server use: a session with a `root_squash` grant that does not declare the
  bit is recorded as a broken root-squash client (`mds/Server.cc:757-760`,
  `1646-1649`), which the MDS reports as `MDS_CLIENTS_BROKEN_ROOTSQUASH`
  with HEALTH_ERR (`mds/Beacon.cc:485-502`).
- Client obligation: enforce MDS auth caps locally before buffering setattr
  or data changes; the native client then sends capability UPDATEs with caller
  UID/GID -1 (`client/Client.cc:3986-3997`).
- Consumer-owned: the permission policy and the caller UID/GID in
  `SendCaps`. The library enforces no grant.
- Gap: policy. Decision: consumer-owned. The source path implies a health
  error for root-squash identities; that has not been observed live.

### Bit 22: charmap

- Wire: a directory's charmap (`charmap_md_t`, struct v1/compat1: bool case
  sensitive, normalization string, encoding string;
  `include/cephfs/types.h:247-283`) travels in the InodeStat optional metadata
  multiton (`include/cephfs/types.h:509-520`), which this repository keeps raw
  as `OptionalMetadata` ([reply.go](reply.go)). Names use the existing
  ALTERNATE_NAME (bit 15) fields.
- Server use: CREATE, MKDIR, SYMLINK, LINK and RENAME into a directory with a
  charmap answer -EPERM when the client lacks the bit
  (`mds/Server.cc:4785-4801`, called at `4833`, `7458`, `7560`, `7665`,
  `9098`).
- Client obligation: validate the encoding, normalize and, when case
  insensitive, fold each new name, sending the original as the alternate name
  (`client/Client.cc:1274-1351`, `1353-1371`).
- Consumer-owned: Unicode validation, normalization and case folding. The
  library must not implement them.
- Gap: codec (an optional `charmap_md_t` decoder over `OptionalMetadata`) and
  policy (name wrapping).
- Decision: implement as an explicit caller opt-in on the CephFS profile in
  which the caller states that it wraps names, never as a default. This is the
  one concrete operation blocked by an absent declaration: entry creation in
  charmap directories. Dependencies: fresh native vectors of InodeStat with a
  charmap, then a live acceptance comparing names and alternate names with a
  native client in case-sensitive and case-insensitive directories, and the
  -EPERM refusal without the opt-in.

### Bit 23: blockdiff

- Wire: request `CEPH_MDS_OP_FILE_BLOCKDIFF` (0x01405,
  `include/ceph_fs.h:432`) with two snapshot paths and the `blockdiff`
  argument member (u64 scan index, u64 maximum objects that the MDS narrows to
  u32, `include/ceph_fs.h:656-661`); the reply extra is `BlockDiff` (result, next
  scan index, `interval_set<uint64_t>` blocks; `mds/mdstypes.h:974-985`)
  (`mds/Server.cc:11800-11858`).
- Server use: none beyond admission; the native client requires the MDS's
  advertisement (`client/Client.cc:10052`).
- Current code: `OpFileBlockdiff` exists in [request.go](request.go); raw
  `Submit` can carry it with caller-built arguments; no exported constant,
  test or native evidence.
- Consumer-owned: snapshot selection, paging by scan index and mapping block
  ranges to file data.
- Gap: optional. Decision: consumer-owned, like bit 17. No operation is
  blocked by absent wire data.

## Blocked higher-layer operations

- Opening, keeping or reconnecting on a filesystem with a required bit
  outside the selected profile. A `multi_reconnect` requirement (including
  deprecated Nautilus `min_compat_client`) is admitted only by CephFS; the
  session-only profile refuses it. Both profiles refuse `deleg_ino`
  (deprecated Octopus-or-later `min_compat_client`) until its decode-only
  stage. Both profiles admit `reclaim_client`.
- Creating, linking or renaming entries into a directory with a charmap
  (bit 22): -EPERM.
- Root-squash identities are reported as broken clients (bit 21). That is an
  operational warning, not a refused operation.

Async directory operations (bit 13), metrics (bit 14) and duplicate-open state
notification (bit 16) only change latency or telemetry. GETVXATTR and
BLOCKDIFF (bits 17, 23) are not blocked by absent wire data.

## Order and dependencies

Recovering already supported paths and the general coverage ledger take
precedence (issue #11). Within this table the order was bits 10 and 12
(evidence only), then bit 13 decode-only, then the bit 22 opt-in. Bit 10 is
declared, as is bit 12 for CephFS; bit 13 decode-only is next. Each
declaration change must update `ClientFeatures` or `CephFSClientFeatures`,
this table, the profile contract and the native `session ls` feature
assertions of the live tests together. The drift test fails on any one of
them alone.

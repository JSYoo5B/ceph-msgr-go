# Tentacle OSD control and event contract

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
Source and product audit: 2026-10-15. This document completes a bounded source
inventory for O0, updated for bounded O3 watch/notify and the fixed replicated-PG
backoff profile. Feature additions require the explicit profile before Dial. [WATCH_NOTIFY_CONTRACT.md](WATCH_NOTIFY_CONTRACT.md)
records its implementation and verification boundary. [CONTRACT.md](CONTRACT.md) defines
current object vectors and their completion/uncertainty contract.

## Current O1 boundary

The pinned native client dispatcher distinguishes OSD operation replies,
OSDMap publications, backoff instructions, watch notifications and OSD command
replies. These are separate message classes, not interchangeable operation
results. The first two are the required application messages for the current
explicit-target, head-only replicated O1 profile. Backoff generation is gated
by a feature omitted by the default profile and negotiated by the explicit replicated-PG profile. Watch events use the cookie reserved by
WatchObject or NotifyObject; raw object Request cannot submit these operations.
OSD commands are implemented in the separate D1 milestone. This inventory is for the
client public connection; replication, recovery, peering and heartbeat messages
on daemon connections remain final fixture requirements, not client codecs.

| Wire type | Pinned message | Current handling | Completion identity |
| --- | --- | --- | --- |
| 41 / 0x29 | MOSDMap | Implemented bounded FIFO; caller applies maps | Authenticated connection and cluster FSID; map epochs, not pending operation TID |
| 42 / 0x2a | MOSDOp | Only supported request vector sent | Authenticated client, incarnation, header TID, attempt zero |
| 43 / 0x2b | MOSDOpReply | Matched to a started pending request; validated against vector | Header TID plus returned wire identity/metadata |
| 44 / 0x2c | MWatchNotify | Bounded per-stream FIFO with explicit acknowledgement | Registration/notify cookie and notify ID; not a pending call TID |
| 61 / 0x3d | MOSDBackoff | Replicated-PG profile: bounded events, explicit ACK and writer gate; default: refused | Current connection, actual PG/shard, connection-local backoff ID |

Message numbers come from [ceph_fs.h][ids]. Application message headers are
separate from msgr2 frame tags. Messenger sequence ACK and keepalive/keepalive
ACK remain session transport controls; none supplies object ONDISK completion,
notify acknowledgement, map application or permission to replay a mutation.

The current [OSD callback](../../cephmsgr/osd.go) accepts MOSDMap and MWatchNotify
outside normal reply dispatch, and accepts MOSDBackoff only on the replicated-PG profile. Other controls return `ErrUnsupportedFeatures`.
[Session dispatch](../session/session.go) matches reply type and header TID;
late replies with no pending TID are discarded. A non-reply event must never be
fed to this pending-call path, even if its header TID numerically equals an
outstanding request. A control failure terminates that OSD session, preserves
uncertainty for already started requests, and does not reconnect/resubmit them.
It is a local unsupported-protocol failure, not a signed OSD refusal result.

## O1 map delivery and failure contract

[MOSDMap][map] has current header version 4/compatibility 3. Front fields are
FSID, an epoch-to-blob map of incrementals, an epoch-to-blob map of full maps,
trim lower bound, newest available epoch, then the retired removed-snapshot map
(current encoder writes an empty map). The v1 branch omits the final fields;
v2 adds trim/newest. Current product acceptance is v1 and v4 envelopes, not a
promise to accept every intermediate header version. Inner maps stay opaque.

The authenticated OSD can send maps in response to stale request epochs.
[OSDService::build_incremental_map_msg][map-send] may start with a full map when
the requested history is trimmed and limits each publication. A delivered batch
is therefore not proof of a complete incremental chain, current placement or
request execution. The caller must acquire missing history, apply maps and
select targets; no pending mutation is automatically moved to a newer epoch.

[NextMap](../../cephmsgr/osd_maps.go) preserves FSID, envelope version,
trim/latest epoch, ordered blobs and raw front bytes. The queue holds at most
64 batches and a configured byte budget. Accepted batches drain before the
stable terminal error. A canceled local NextMap wait consumes nothing; Close
is not a remote unsubscribe. Invalid envelopes or foreign FSID fail the OSD
session. Overflow returns `ErrOSDMapOverflow` and fails the session rather than
silently dropping a routing update; it can consequently leave a sent mutation
with an unknown outcome. Ownership is per returned batch, with blob slices
viewing the caller-owned raw front buffer.

## Feature gates

Default role-4 negotiation in [handshake.go](../session/handshake.go) adds
OBJECTLOCATOR (bit 8) and PGID64 (bit 9) to the implemented common transport/map
features. It omits new object request/reply encodings and the MON-only admission
CRUSH bits. Requiring an unsupported feature fails before object transmission;
a caller cannot override the advertised feature set.

In [ceph_features.h][features], RADOS_BACKOFF, RESEND_ON_SPLIT,
OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS and SERVER_LUMINOUS share bit 21,
incarnation 2. The incarnation marker is bit 57; the complete feature mask is
`(1 << 21) | (1 << 57)`. This is not an independently selectable backoff promise.
The default OSD set includes bit 57 but omits bit 21. Adding bit 21
requires the combined O3/O4 contracts and acceptance, including caller-owned
placement/map input and split handling. A backoff decoder alone is insufficient.
The replicated-PG profile now combines v8 actual-PG requests, caller placement,
HASHPSPOOL and bounded backoff admission. It adds bit 30 to opt-in MON admission
before Dial and a fixed bundle to OSD sessions. MON CRUSH admission remains a
narrow exception, with no transfer to OSD sessions. Actual split recovery and
broader CRUSH configurations remain unverified; see O4_CONTRACT.md.

[PrimaryLogPG::do_request/do_op][backoff-gate] tests RADOS_BACKOFF before
issuing PG-wide or object-specific blocks. Peering/down/incomplete PGs and
unreadable/degraded objects can trigger blocks under the respective daemon
configuration. Without this feature the server can instead delay requests in
its own queues; absence of a backoff event is not a guarantee of prompt service.
Context cancellation and Close must remain usable while the server delays.

## O3 backoff wire and responsibility

[MOSDBackoff][backoff] is version 1/compatibility 1. Its front contains, in
order, `spg_t`, map epoch u32, opcode u8, ID u64, begin hobject, end hobject.
Block = 1, ACK_BLOCK = 2, UNBLOCK = 3, defined in [rados.h][rados].
[spg_t][spg] is a versioned PG/shard structure, not the raw placement hash echoed by
the current v6 object request. The caller must provide actual mapped PG/shard
metadata. Current admission conservatively blocks the whole connection while
any active or released-but-unacknowledged block remains.

The [PG sender][pg-backoff] allocates IDs in the current client session.
Begin/end denote a half-open range `[begin, end)`; equality denotes one object.
[hobject_t encoding][hobject] version 4/compatibility 3 preserves locator key,
object name, snapshot, raw hash, max marker, namespace and signed pool. Range
[comparison][hobject-order] uses the canonical object order: max, pool, bit-reversed hash,
namespace, effective locator key/object, then snapshot. Comparing ordinary
object strings or unreversed numeric hashes cannot implement this gate.

[Objecter::handle_osd_backoff][backoff-client] records a BLOCK on the current
connection and sends ACK_BLOCK on that same connection with the original
PG/shard, epoch, ID and bounds. The epoch must not be replaced with the client's
latest epoch: the server can discard the acknowledgement after a PG split.
ACK_BLOCK is protocol receipt after installing a send restriction, not object
execution or a user-level acknowledgement. UNBLOCK resolves connection-local
state by ID. Connection replacement must discard old gates; a stale connection
or reused ID cannot affect a new target session.

The native Objecter can resend outstanding operations after UNBLOCK. That is
native client routing/retry policy, not permission to copy that behavior here.
A BLOCK-triggering request may already have been transmitted and be discarded
or delayed by the server. Its fate cannot be inferred from BLOCK/UNBLOCK alone.
Future Go acceptance must distinguish first transmission of locally queued,
unsent work from resubmission of already sent work; the latter remains an
explicit caller decision with preserved uncertainty and identity. O3 is blocked
until its public contract resolves how sent calls/events end without silently
replaying mutations. An automatic resend-on-unblock implementation would violate
current project policy.

Future gates must bound retained ranges/IDs/bytes and locally waiting requests.
The protocol receiver and ACK path must progress independently of blocked
application sends. Per-call cancellation removes only local unsent work or ends
the local wait with the existing sent-outcome contract. Close wakes all gates
and waits for receiver/writer cleanup. Handling duplicate blocks, unknown
unblocks, mismatched bounds, malformed identities and resource exhaustion needs
an explicit tested policy; native logging/ignore behavior does not supply that
Go policy. No such gate or event API is implemented today.

## O3 watch and notify wire

[MWatchNotify][watch] has current header version 3/compatibility 1. Its front
is: payload version u8 (current 1), event opcode u8, cookie u64, obsolete raw
version u64, notify ID u64, length-prefixed blob, signed return code i32,
notifier global ID u64. Headers below v2 omit return code; below v3 omit
notifier ID. Those source decoder branches describe field history, not a request
to support Ceph releases older than Tentacle. Current native producers use v3.

[Watch.cc][watch-server] demonstrates three distinct deliveries:

| Event | Value | Required preservation and acknowledgement meaning |
| --- | --- | --- |
| NOTIFY | 1 | Watch cookie, OSD notify ID, notifier GID and front blob payload. User handler completion is separate from receipt. |
| NOTIFY_COMPLETE | 2 | Notifier cookie/notify ID and signed return code; separate message data contains map `(watcher GID, cookie) -> reply blob` followed by missed-watcher pairs. Front blob is empty in this producer. Timeout uses `-ETIMEDOUT`; received replies/missed identities remain available. |
| DISCONNECT | 3 | Watch cookie and raw fields; marks loss of the registration connection. It does not establish notify success or remote unwatch. |

Do not collapse front blob and message data into one payload. The normal
MOSDOpReply to NOTIFY and asynchronous NOTIFY_COMPLETE are different results.
[Objecter::handle_watch_notify][watch-client] dispatches by cookie and checks
notify ID for notifier completion; user notification delivery is asynchronous.
The product must preserve authenticated source OSD/session generation in local
metadata: cookie alone does not establish that a delayed event belongs to the
current registration. The event wire has no registration generation field or
object locator to recover that association on its own.

The standalone head LIST_WATCHERS `0x1209` observation is implemented in
[WATCHERS_CONTRACT.md](WATCHERS_CONTRACT.md), with native acceptance recorded
separately. It does not register or renew a watch. Event companion operations
WATCH `0x220f`, NOTIFY `0x1206`, NOTIFY_ACK `0x1207` now have separate codecs and
owner-bound APIs. Their packed unions follow the [ObjectOperation builders][watch-ops]. WATCH stores
cookie, unused version, action, registration generation and timeout; actions
are UNWATCH 0, WATCH 3, RECONNECT 5, PING 7. Historical action 1 is not an excuse
to add older-release behavior. NOTIFY stores a cookie and input protocol
version u32, timeout u32, length-prefixed user blob. NOTIFY_ACK input is notify
ID u64, cookie u64 and length-prefixed reply blob. Receiving a notification must
not generate NOTIFY_ACK before the caller elects to acknowledge its handling.

The implemented registration lifetime distinguishes local cancellation, explicit
UNWATCH submission, server DISCONNECT, failed connection and renewal/PING.
RECONNECT has a native-tested wire codec; its public recovery lifetime remains
unimplemented. No implicit re-registration or replay of a notify
is authorized by ticket renewal. Bounded per-registration event queues, byte
ownership, overflow terminal cause, concurrent Next cancellation, late delivery
and Client Close are defined in WATCH_NOTIFY_CONTRACT.md. Raw
server codes and completion data remain available; lock schema, RBD policy and
user callback decisions remain outside this repository.

## Acceptance work

The original O0-E2/E3 inventory tests are synthetic dispatch/admission evidence.
Watch/notify now also has 16 fresh native request/reply/event vectors and an
independent native watcher/notifier fixture. Backoff has five fresh native
wire vectors and normal/late-ACK daemon acceptance on two narrow profiles.
Broader split/CRUSH feature semantics remain unverified. Existing records establish only their
described paths; a source inventory alone establishes no interoperability.

| Work ID | Concrete check | Current status / next prerequisite |
| --- | --- | --- |
| O0-E1 | Interleave maps before reply, preserve FIFO/ownership; cancel Next, overflow, malformed/foreign FSID, Close with pending mutation | Existing synthetic map tests; current native stale-map delivery recorded separately. Reuse exact-head results; add new cases only for uncovered behavior. |
| O0-E2 | Inject valid type 44 and 61 controls with TID zero and with a pending mutation's TID; prove they cannot complete that call, preserve unsupported cause and uncertain submission, no retry | Synthetic `TestUnsupportedOSDControlsCannotCompleteOrReplaySentMutation` passes all four type/TID combinations under the race detector; source-derived payloads, no native event proof. |
| O0-E3 | Assert role-4 feature mask omits bit 21 and MON admission bits; require combined bit-21 feature and observe pre-object rejection | Existing mask test covers omission/admission boundaries. Synthetic `TestOSDHandshakeRejectsCombinedBackoffRequirementBeforeApplication` passes under the race detector, with peer-observed EOF before any application frame. |
| O3-E1 | Fresh native encoder/decoder vectors for spg_t/hobject bounds, BLOCK/ACK_BLOCK/UNBLOCK and all current watch events; reject malformed count/length/version/opcodes | Watch/notify has 16 fresh native vectors; malformed and ownership cases are synthetic. Backoff has five fresh native vectors, including original-epoch ACK and newer-epoch UNBLOCK. |
| O3-E2 | Native PG-wide and single-object block; observe original-epoch ACK and server unblock, split/connection replacement, zero automatic replay, cancellation/Close while blocked | CRC/AES single BlueStore and secure/AES256K IPv6 single MemStore each pass normal and delayed ACK: real PG-wide BLOCK/UNBLOCK, exact native ACK reception, local unsent refusal and one explicit fresh append. Single-object block, split and failover remain unverified. |
| O3-E3 | Native watcher and notifier on disposable objects; binary NOTIFY, explicit ACK reply, completion data, partial timeout, DISCONNECT, reconnect/renewal | CRC/AES host 3 BlueStore and secure/AES256K IPv6 container single MemStore each cover binary notify/ACK, two native watchers, partial timeout, explicit ping/unwatch. Live DISCONNECT/reconnect and failover remain unverified. |
| O3-E4 | Queue overflow, duplicate/late/unknown cookie and notify ID, reused backoff ID across sessions, concurrent waits and Close; retained bytes and stable first cause | Watch queue/lifetime policy is implemented with synthetic race regressions, including authentication replacement and early completion correlation. Backoff has bounded identity/session and FIFO ACK-order race tests, including released unACKed tombstones; broader daemon failure paths remain pending. |

## Source provenance

All links below resolve to the same pinned commit. The message headers,
Objecter, PG, PrimaryLogPG and Watch source inspected here carry LGPL-2.1
notices. `ceph_fs.h` and `rados.h` allow LGPL-2.1 or LGPL-3.0; the repository
[COPYING][license] provides the default and file-specific notices, including
headers without an inline license block. This document independently describes
wire facts and required behavior. It incorporates no upstream C++ code or
native dependency into the product.

[ids]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_fs.h#L176
[map]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDMap.h#L26
[map-send]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSD.cc#L1410
[features]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_features.h#L114
[backoff-gate]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc#L1871
[backoff]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDBackoff.h#L22
[rados]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h#L574
[pg-backoff]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PG.cc#L570
[spg]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/osd_types.h#L601
[hobject-order]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/hobject.h#L349
[hobject]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/hobject.cc#L110
[backoff-client]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc#L3835
[watch]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MWatchNotify.h#L23
[watch-server]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/Watch.cc#L188
[watch-client]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.cc#L948
[watch-ops]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osdc/Objecter.h#L1438
[license]: https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/COPYING

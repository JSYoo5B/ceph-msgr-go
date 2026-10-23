# Explicit cache-pool capability and IGNORE_CACHE

Reference: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`.
The native headers and implementation carry LGPL-2.1 notices; the Go code is
an independent wire translation. The relevant definitions are
[`OSD_CACHEPOOL`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/ceph_features.h),
[`CEPH_OSD_FLAG_IGNORE_CACHE`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/include/rados.h),
the request encoding in
[`MOSDOp.h`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDOp.h),
and cache handling in
[`PrimaryLogPG.cc`](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/PrimaryLogPG.cc).

## Admission and wire input

`Options.EnableOSDCachePools` is selected before `Dial` and requires
`EnableOSD`. It adds implemented feature bit 35 to both supported and required
features of MON and OSD sessions, including independent named MON and ping
connections. MGR and MDS feature sets remain independent. A missing required
bit fails the handshake; the client does not retry with a weaker mask. The
default client does not declare this capability. This is separate from the
narrow MON CRUSH-generation admission exception.

`OSDRequest.IgnoreCache` sets request-level wire flag `0x8000`. Native
librados operation flag `8` is a different API constant translated by the
native client; it must not be placed directly on the wire or on an individual
packed operation. Encoding version remains selected by `OSDProfile`.
The existing v6, v8 and untraced v9 request encoders share this policy.

The initial accepted shape is one standalone STAT, READ, WRITEFULL or DELETE
of a head object on replicated placement. Ordinary operation validation still
applies, including READ output limits, WRITEFULL length equal to input bytes,
explicit mutation mtime and unchanged signed reply handling. The caller
supplies the pool, namespace, object, raw hash, map epoch, target address and
nonce, plus actual PG and primary for a modern profile.

Known unsupported combinations fail before acquiring a shared request slot,
waiting for MON readiness or allocating a TID: absent opt-in/capability,
compound vectors, other opcodes, snapshots or snap context, ORDERSNAP,
replica-read flags, lock bypass, redirect bypass, read/write ordering,
per-operation flags, extent truncate fields, nonzero retry attempt, explicit
resubmission and erasure-coded placement. `IgnoreCache=false` retains the
existing request policy. FULL_TRY/FULL_FORCE retain their existing writing
validation; their combination with IgnoreCache has no live validation here.

## Server semantics and caller responsibility

The flag bypasses the selected tier's promotion/proxy path. It neither chooses
the base pool nor changes the caller's target. Dirty tracking remains active.
DELETE must not be advertised as always physically removing the object:
snapshot clones can require a whiteout. Overlay discovery, autonomous routing,
promotion policy and cache management workflows are outside this capability;
the explicit cache-tier operations below are a separate contract.

The existing request identity, cancellation and unknown-outcome contract
continues to apply. A sent mutation with a lost response remains uncertain;
there is no automatic replay. Explicit resubmission with this flag is refused
in this initial scope.

## Cache-tier operations

`CACHE_FLUSH` (`0x821f`), `CACHE_TRY_FLUSH` (`0x8221`), `CACHE_EVICT`
(`0x8220`), `CACHE_PIN` (`0x2224`) and `CACHE_UNPIN` (`0x2225`) are admitted
as one standalone, inputless, flagless operation on a head object, with
`EnableOSDCachePools` and the negotiated bit 35, on replicated placement. Each
takes exactly the request flags its native caller sends
([rados.cc](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/tools/rados/rados.cc)
`do_cache_flush`/`do_cache_try_flush`/`do_cache_evict`, `librados_util.cc`
`translate_flags`, `Objecter` `get_read_flags`/`prepare_mutate_op`):

| Opcode | Request mode | Required request inputs | Refused |
| --- | --- | --- | --- |
| CACHE_FLUSH | READ | IgnoreCache, IgnoreOverlay | SkipRWLocks |
| CACHE_TRY_FLUSH | READ | IgnoreCache, IgnoreOverlay, SkipRWLocks | |
| CACHE_EVICT | READ | IgnoreCache, IgnoreOverlay, SkipRWLocks | |
| CACHE_PIN, CACHE_UNPIN | WRITE | optional IgnoreCache | IgnoreOverlay, SkipRWLocks |

`IgnoreOverlay` (`CEPH_OSD_FLAG_IGNORE_OVERLAY`, `0x20000`) is refused on every
other request. The native Objecter reads it when it chooses a target; the
pinned OSD request path does not consult it. Because the caller already
supplies the tier pool, PG and primary, the flag is sent only so the request
matches the native one; it does not route anything. SKIPRWLOCKS selects the
lock mode that the flush handlers check (`PrimaryLogPG.cc`: try-flush without
it and flush with it return EINVAL), so a wrong combination is refused
locally rather than sent. Snapshot reads or context, ORDERSNAP, replica-read,
redirect, ordering, FULL_TRY/FULL_FORCE, nonzero retry, explicit
resubmission, erasure-coded placement, compound vectors, per-op flags and any
extent, version or input field are refused before a request slot, monitor
readiness or TID is used.

Dirty flush writes the head to the base pool and clears dirty; clean flush
or try-flush can be a no-op. Evict deletes a clean unpinned head, refuses dirty
or watched heads with EBUSY and pinned heads with EPERM. An absent object may
return ENOENT before a handler. Pin/unpin change the pin flag and return
ENOENT on an absent head. Internal EAGAIN from the pinned flush path is consumed
by execute_ctx without a terminal reply; the OSD requeues its retained request.
This is distinct from caller resubmission. The product never automatically
resubmits a cache request or reinterprets an actual signed server errno.

All successful cache operations require ONDISK while preserving native READ
or WRITE encoding. ACK-only and ONNVRAM-only success leave the sent outcome
unknown, with original submission and raw reply retained. A signed negative
aggregate remains an OSDError even without ONDISK; untouched zero per-op fields
do not establish operation execution. The pinned no-op/read and transaction
commit paths can both return ACK+ONDISK, so those bits do not identify a state
transition or establish crash durability. Operation-specific independent
observations are required; see the [completion table and native evidence](../../integration/fixture/OSD_CACHE_COMPLETION_VALIDATION.md).
Choosing which objects to flush or evict, promotion, the tier agent, hit sets,
overlays and SET_CHUNK manifests remain caller or later work.

The [fresh native vectors](testdata/cache-op-native-README.md) cover all seven
request shapes on v6, v8 and untraced v9, and the Go encoder matches each
header, front, middle and data byte. The
[native-only checkpoint](../../integration/fixture/OSD_CACHE_TIER_OPS_NATIVE_VALIDATION.md)
records the server results of the same operations from librados on one
fixture profile. The [Go acceptance](../../integration/fixture/OSD_CACHE_OP_GO_VALIDATION.md)
sent all five operations from the Go client on one secure/aes256k IPv6
container legacy-v6 single-BlueStore profile and matched a closed native
observer at every step; other profiles and a live EAGAIN try-flush remain
unverified. Its lost-reply CACHE_FLUSH cell separates two claims: bounded
native observation of the applied flush (base and tier B, dirty=false) while
the local wait is blocked, and Go-side caller and connection accounting (one
caller invocation, no observed redial, the next transaction on a fresh
connection). Stable post-state cannot replace the latter, since a replayed
clean flush is a no-op. The accounting does not count outbound messages: a
same-TID rewrite on the same transport is excluded only by source inference.
Neither claim establishes ONDISK receipt, crash durability or daemon-side
request accounting.

## Evidence boundaries

The [fresh native codec proof](testdata/ignore-cache-native-README.md)
compares all 24 combinations of three wire versions, four operations and two
flag values. Synthetic authenticated transport tests separately cover feature
isolation, the public/private MON paths, an OSD READ and preservation of an
uncertain WRITEFULL identity without redial or replay.

The earlier [native-only cache probe](../../integration/fixture/OSD_IGNORE_CACHE_NATIVE_PROBE_VALIDATION.md)
records cache behavior, permission boundaries and cleanup on two owned
BlueStore fixtures. It uses librados rather than the Go product and does not
establish Go live interoperability.

The subsequent [Go/native acceptance](../../integration/fixture/OSD_IGNORE_CACHE_VALIDATION.md)
passed 12 leaves on each of two Linux amd64 single-BlueStore configurations:
legacy-v6 CRC/AES IPv4 host and secure/AES256K IPv6 container. Eight standalone
Go/native operation pairs cover ordinary promotion, bypassed misses, local
replacement and the different subsequent-read behavior after DELETE. Each
object's own before/after bytes, user version and mtime are compared; absolute
versions of distinct paired objects are not compared. Scoped authorization,
MON feature refusal, independent MON/Tell and MGR transport, log subscription
and complete owned cleanup are also checked. Scoped MON `allow r` retains its
native Tell EACCES; the positive Tell uses a separate admin identity.

Only that live v6 topology is established. Modern profiles retain offline
codec and synthetic evidence. EC, snapshot combinations, FULL_TRY/FULL_FORCE
combinations, replica reads, cache faults and power-loss durability were not
validated by this acceptance. Whiteout evidence is subsequent read behavior;
the raw object_info whiteout/dirty flags were not inspected.

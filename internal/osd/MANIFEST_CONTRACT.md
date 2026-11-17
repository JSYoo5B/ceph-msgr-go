# Manifest redirect (SET_REDIRECT, UNSET_MANIFEST)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h` SET_REDIRECT 0x2227 and UNSET_MANIFEST 0x222a,
`osdc/Objecter.h` set_redirect/unset_manifest, `osd/PrimaryLogPG.cc`
do_osd_ops and the manifest read proxy; LGPL-2.1, independently translated).

`OSDSetRedirect` carries the COPY_FROM union (target snap, always the head,
and the target version) and indata (target object name and locator), exactly
as Objecter's set_redirect without CEPH_OSD_OP_FLAG_WITH_REFERENCE. The OSD
makes the targeted object a manifest of type redirect, truncates its data,
sets its user version to the given target version and from then on serves
its reads from the target (in any pool); a WRITEFULL to it was observed to be
proxied to the target as well. `OSDUnsetManifest` has no input and
clears the manifest; a redirected object stays, now plain and empty. A
chunked object that still holds its own bytes keeps them, and the OSD drops
that object's chunk references after the source commit, without a completion
back to the caller (observed live on one profile; see the SET_CHUNK record
below). Both are
standalone and flagless here; reference-counted redirects are not
implemented. SET_CHUNK and the TIER_PROMOTE, TIER_FLUSH and TIER_EVICT
operations have their own bounded contracts below.

`OSDRequest.IgnoreRedirect` sets CEPH_OSD_FLAG_IGNORE_REDIRECT so the OSD
serves the manifest object itself (maybe_handle_manifest returns early);
`OrderReadsWrites` sets RWORDERED and is accepted on any vector;
`SkipRWLocks` sets SKIPRWLOCKS and is refused on a writing vector, whose
writes would otherwise go unserialized.

A read the OSD proxies to the redirect target answers with the requested
extent length echoed rather than the number of bytes returned (observed
live); the read validator therefore accepts an extent length equal to either
the payload or the request, the payload never exceeding the request.

Server refusals observed live: the redirected object must exist (-ENOENT),
must not target itself (-EINVAL), and UNSET_MANIFEST on a plain object is
-EOPNOTSUPP. A redirect inside a cache-tier pool would be -EINVAL.

Verification: [OSD_REDIRECT_VALIDATION.md](../../integration/fixture/OSD_REDIRECT_VALIDATION.md).

## SET_CHUNK and manifest tier operations

Source audit of the pinned commit for issue #8. This repository emits
SET_CHUNK under the bounded contract in
[Go SET_CHUNK contract](#go-set_chunk-contract) and the TIER_* opcodes under
the [Go tier operation contract](#go-tier-operation-contract). Line numbers
refer to `src/osd/PrimaryLogPG.cc` unless another file is named.

### SET_CHUNK (0x8228)

Native construction: `ObjectReadOperation::set_chunk` (`librados_cxx.cc`) calls
Objecter `set_chunk` (`osdc/Objecter.h:1603-1613`). The indata is u64
src_offset, u64 src_length, `object_locator_t` of the target I/O context,
`object_t` target name and u64 tgt_offset; the caller's flag becomes the
operation's packed per-op flags. `rados set-chunk` always passes
CEPH_OSD_OP_FLAG_WITH_REFERENCE (0x80) and no request flag, and submits through
the read path, so the request carries no mtime.

Classification: the opcode's mode is CACHE, not modify. `OpInfo` therefore sets
only the cache flag (`osd/osd_op_util.cc`), so the client's capability check
asks for neither read nor write access; the operation is write-ordered, and
`maybe_handle_manifest` never proxies it (2588-2598). SET_CHUNK is not a
user-visible modification (6066-6081): the source's user version stays, and
the mtime changes only when the request carries one (9084).

Handler (7280-7400), in order: a tier pool -EINVAL; an absent source -ENOENT; a
redirect manifest -EINVAL; undecodable indata -EINVAL; zero length -EINVAL;
src_offset + src_length beyond the object size -ERANGE; no WITH_REFERENCE flag
-EOPNOTSUPP; an erasure-coded pool -EOPNOTSUPP; an interval overlapping an
existing chunk -EOPNOTSUPP; an unresolvable target locator returns the
placement error. A later existing-reference check at the same offset (-EINVAL)
cannot be reached by a non-empty existing chunk, because the overlap check
refuses it first. Then `inc_refcount_by_set` (3653) blocks the source object
and has the OSD's own Objecter call `cas.chunk_get_ref` on the target with
IGNORE_CACHE, IGNORE_OVERLAY and RWORDERED (`refcount_manifest`, 3790). The
target must exist: `chunk_get_ref` reads the target's `chunk_refs` attribute
and fails with -ENOENT for an absent object (`cls/cas/cls_cas.cc`). A failed
increment is returned to the client.

`finish_set_manifest_refcount` (10866) records a failure under both the chunk
offset and key 0. With a non-zero src_offset, a failed increment therefore
leaves two results for one chunk, and the request never completes while the
source object stays blocked. This is a source-derived hazard, not an observed
result; the native checkpoint uses offset 0 for its absent-target control.

On success the source gains a chunked manifest entry at src_offset (target
hobject, tgt_offset, src_length, FLAG_HAS_REFERENCE) and FLAG_MANIFEST. Its data
stays local and no chunk is marked missing, so ordinary reads still return the
source's own bytes. The target's `chunk_refs` attribute holds a `chunk_refs_t`;
`chunk_set_refcount` uses the precise by_object form, a multiset of the
source head's hobject. Deleting the source or UNSET_MANIFEST drops its
references after commit (`dec_all_refcount_manifest`, 3755);
`cas.chunk_put_ref` removes the target when its last reference goes.
Snapshot clones change the reference arithmetic (`calc_refs_to_inc_on_set`
with adjacent clones); that path is not exercised here.

Required pool configuration: none. The source pool must be a replicated pool
that is not a tier; the target may be any pool its locator resolves. The
`dedup_tier`, `fingerprint_algorithm` and chunk options are consulted only by
the TIER_FLUSH dedup path, and cache-mode configuration is unrelated.

The reply has no per-operation output; the result is the single signed code.
The native client may resend an uncertain request.

### TIER_PROMOTE, TIER_FLUSH and TIER_EVICT

All three are inputless client primitives
([v20.2.4 handlers](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L7402-L7526)).
Each handler checks, in order, a tier pool (-EINVAL), an absent object
(-ENOENT) and a `require_osd_release` older than the operation (-EOPNOTSUPP,
unreachable on a Tentacle cluster). None is a user-visible modification
(6066-6081): the user version moves only where noted below, and
`maybe_handle_manifest` never proxies them (2588-2598). A CACHE-mode request
for an absent object is answered with -ENOENT before the handler, from the
object-context lookup (2443-2455); TIER_PROMOTE is a write and reaches its
handler, whose -ENOENT is logged as an error entry.

Native construction (`osdc/Objecter.h:1615-1629`, `tools/rados/rados.cc`
3991-4071): `rados tier-promote` submits an `ObjectWriteOperation` through
`IoCtx::operate` (WRITE and the client's mtime); `rados tier-flush` and
`rados tier-evict` submit an `ObjectReadOperation` through `aio_operate` with
`OPERATION_IGNORE_CACHE|OPERATION_IGNORE_OVERLAY` (READ, IGNORE_CACHE `0x8000`
and IGNORE_OVERLAY `0x20000`, no SKIPRWLOCKS and no mtime). IGNORE_CACHE only
skips the cache-pool path, which returns early on a pool without a cache mode
(2724-2735); IGNORE_OVERLAY is read by the native Objecter when it chooses a
target. The OSD checks OSD_CACHEPOOL only for pools in a cache-tier relation
(1942-1945), and a base pool with a dedup tier is not one.

- **TIER_PROMOTE** (0x2229, mode WR). A plain object has no manifest: result 0
  with no transaction, so the write is recorded as a log-only entry and the
  object's bytes, user version and mtime stay (`prepare_transaction`,
  9001-9009). For a manifest the handler starts a copy (`start_copy`, 9488)
  through the OSD's own client with IGNORE_CACHE, IGNORE_OVERLAY,
  MAP_SNAP_CLONE and RWORDERED, returns -EINPROGRESS and is executed again
  when the copy completes (`PromoteManifestCallback`, 4053). A chunked
  manifest reads **every** chunk from its chunk target
  (`_copy_some_manifest`, 9602) and writes those bytes into the object at the
  chunk offsets in a PROMOTE transaction that clears each chunk's MISSING flag
  (`process_copy_chunk_manifest`, 9893-10007); the chunk map and its
  references stay. Because SET_CHUNK does not compare bytes, the object's
  local range then holds the target's bytes even if it held other bytes
  before. That transaction is created with the OSD's clock as mtime
  (`simple_opc_create`, 11615) and keeps the user version. A redirect reads
  the target with COPY_GET and `finish_promote` (10171) writes its bytes,
  size, user version and mtime into the object and clears the manifest
  (10310-10320); a later UNSET_MANIFEST is then refused. A failed read of a
  chunk or redirect target is answered with that error by
  `reply_op_error`. The client's reply follows the re-executed log-only entry.
- **TIER_FLUSH** (0x822b, mode CACHE). A clean manifest returns 0 without a
  transaction. A dirty object, or any object without a manifest, starts a
  blocking flush with forced dedup (`start_flush`, 10896): an empty object
  returns 0, and otherwise `start_dedup` (10593) refuses a pool without a
  `fingerprint_algorithm` or a positive `dedup_tier` with -EINVAL. Configured,
  it chunks the whole object with the pool's `dedup_chunk_algorithm` and
  `dedup_cdc_chunk_size` (`do_cdc`, 10670; `fixed` cuts chunks of the size,
  `fastcdc` by content), names each chunk object by its fingerprint in the
  dedup pool, keeping the source's locator key and namespace
  (`get_fpoid_from_chunk`, 10726), and has its own client call
  `cas.chunk_create_or_get_ref` for each chunk (`cls/cas/cls_cas.cc`): an
  absent chunk object is created with the chunk bytes and a by_object
  reference to the source. The handler maps -EINPROGRESS to -EAGAIN, which
  `execute_ctx` consumes without a reply (4281-4285). When every reference
  is answered, `finish_set_dedup` (10762) submits a CLEAN transaction that
  installs the new chunk map, sets the manifest, clears dirty and releases
  the references of the previous chunk map after commit, then answers the
  retained request with `reply_op_error(op, r)` (10860) without waiting for
  that commit. `reply_op_error` copies the request's operations
  (`OSD.cc:1635-1649`, `messages/MOSDOpReply.h:143-166`), and the handler had
  stored -EAGAIN in the request's own operation (8206), so the reply carries
  the aggregate code (0 on success) beside a per-op -EAGAIN (-11), no user
  version, and the request's own ACK/ONDISK bits as its completion flags. The
  object's bytes and user version stay; the CLEAN transaction moves its mtime
  to the OSD's clock. A failed reference is answered with its code the same
  way.
- **TIER_EVICT** (0x822c, mode CACHE). A plain object is refused with -EINVAL.
  A chunked manifest marks every chunk MISSING, zeroes its local range, clears
  the data digest and clears dirty, in a cache transaction that keeps the
  size, user version and mtime (the request carries none) and drops no
  reference (`finish_ctx`, 9054-9064, skips cache operations). A redirect
  manifest has no chunks: result 0 without a transaction, so nothing changes.
  After eviction a READ whose extent lies wholly inside missing chunks is
  proxied to the chunk targets (`can_proxy_chunked_read`, 3905); any other
  data read or write of the object promotes it first (2650-2656), which is a
  separate path from TIER_PROMOTE.

A dedup configuration is a property of the source pool (`osd pool set`
`dedup_tier`, `fingerprint_algorithm`, `dedup_chunk_algorithm`,
`dedup_cdc_chunk_size`; `mon/OSDMonitor.cc`). It is unrelated to cache-mode
configuration and is not a prerequisite for SET_CHUNK, TIER_PROMOTE or
TIER_EVICT.

COPY_GET stays daemon-originated: its only caller is the OSD's own copy and
promote path (see [UNSUPPORTED_OPCODES.md](UNSUPPORTED_OPCODES.md)). The cache
and dedup tier agents are daemon-internal work, not client primitives.

### Go SET_CHUNK contract

`OSDSetChunk` (`internal/osd/set_chunk.go`) is admitted as one standalone head
operation on replicated placement:

- the packed union is zero and the indata is exactly the Objecter layout above,
  with the target named by `OSDChunkInput` (pool, name, key and namespace; no
  locator hash) and its TargetOffset;
- the per-operation flags are exactly `OSDWithReference` (`0x80`); every other
  opcode refuses that flag, and every other operation refuses a chunk input;
- the request flags are READ alone, as the native read submission sends them
  (`Objecter::get_read_flags`; `rados set-chunk` passes no request flag), with
  no mtime;
- no feature bit or opt-in is required: the OSD checks OSD_CACHEPOOL only for
  pools in a tier relation (`PrimaryLogPG.cc:1942`), which SET_CHUNK refuses
  anyway.

Refused before a request slot, monitor readiness or TID is used:

| Shape | Reason |
| --- | --- |
| SourceOffset other than zero | The non-zero-offset failed-increment hazard above: one chunk yields two results, the request never completes and the source stays blocked. A client cannot rule out a failed increment (an absent or concurrently removed target), so this shape is never sent. |
| Target equal to the targeted object (pool, namespace, name and effective key) | `inc_refcount_by_set` blocks the source and the OSD's own `chunk_get_ref` then targets that same blocked object (source-derived, not observed). |
| SourceLength zero, empty or NUL-containing target name, negative target pool | Malformed input (the OSD refuses a zero length with EINVAL). |
| Missing WITH_REFERENCE or any other per-operation flag | Native callers always send exactly WITH_REFERENCE; the OSD refuses its absence with EOPNOTSUPP. |
| Extent, data, version or other union inputs; compound vectors | Not part of the native operation. |
| mtime, snapshot read or context, ORDERSNAP, ENFORCE_SNAPC, replica-read, IGNORE_REDIRECT, RWORDERED, SKIPRWLOCKS, IGNORE_CACHE, IGNORE_OVERLAY, FULL_TRY/FULL_FORCE, nonzero retry, explicit resubmission, erasure-coded placement | Not sent by the native caller; an erasure-coded source is refused by the OSD. |

Signed aggregate and per-op results pass through unchanged (for example ENOENT
for an absent source or target, ERANGE beyond the source size, EOPNOTSUPP for
an overlapping chunk). A failed reference increment is answered by
`RefCountCallback` through `reply_op_error`: the aggregate is the increment's
code (ENOENT for an absent target), while the per-op result keeps the
`-EINPROGRESS` (-115) the handler stored before it blocked the source. That
pair was observed live and is returned as received. The reply carries no
output. Although the request mode is READ, a successful reply must carry OSD
ONDISK, as for any mutation ([CONTRACT.md](CONTRACT.md)): an ACK-only or
ONNVRAM-only success fails the session and is returned as
`OutcomeUnknownError`. The live successes carried ONDISK (`0x4000015`). A lost reply after
transmission is an unknown outcome and is never replayed: on success the OSD
has already taken the target reference, and the native client's own resend is
not reproduced. Choosing chunk boundaries, fingerprints, dedup pools, the
source's snapshot clones and the reference arithmetic they change remain
caller or later work.

Evidence:

- The [fresh native vectors](testdata/set-chunk-native-README.md) cover two
  admitted shapes and the two refused native shapes (non-zero source offset,
  missing WITH_REFERENCE) on v6, v8 and untraced v9. The Go encoder matches
  each admitted request's header, front, middle and data byte for byte and
  refuses the other two.
- Synthetic public transport tests send SET_CHUNK on three request/reply
  profiles, keep a signed refusal, and keep a sent request whose connection is
  lost unknown with its transaction, without redial or resend.
- [OSD_SET_CHUNK_GO_VALIDATION.md](../../integration/fixture/OSD_SET_CHUNK_GO_VALIDATION.md)
  records `TestCephOSDSetChunkGoIntegration`: Go-originated SET_CHUNK and
  source deletions against two owned plain pools on one secure/aes256k IPv6
  container legacy-v6 single-BlueStore profile, judged by native observations
  of bytes, versions, mtimes and the target's `chunk_refs`. The same record
  covers one standalone UNSET_MANIFEST on a chunked, locally populated
  source whose target another source still references: the source keeps its
  bytes, the target loses exactly that reference, and a second
  UNSET_MANIFEST is refused. Other profiles and a live lost reply remain
  unverified (the lost-reply boundary is synthetic).

### Go tier operation contract

`OSDTierPromote`, `OSDTierFlush` and `OSDTierEvict` (`internal/osd/tier_op.go`)
are admitted as one standalone, inputless head operation on replicated
placement, with exactly the native request flags:

| Opcode | Request mode | Required request inputs | Refused |
| --- | --- | --- | --- |
| TIER_PROMOTE | WRITE (optional mtime, as any write) | none | IgnoreCache, IgnoreOverlay |
| TIER_FLUSH, TIER_EVICT | READ | IgnoreCache, IgnoreOverlay | mtime |

No feature bit, `EnableOSDCachePools` or other opt-in is required; the
default client sends these requests with its ordinary features. IgnoreCache and
IgnoreOverlay keep their existing refusals for every other request.

Refused before a request slot, monitor readiness or TID is used: per-op flags
(including WITH_REFERENCE), extent, data, version, chunk, copy or other union
inputs, compound vectors, snapshot reads or context, ORDERSNAP,
ENFORCE_SNAPC, replica-read, IGNORE_REDIRECT, RWORDERED, SKIPRWLOCKS,
FULL_TRY/FULL_FORCE, nonzero retry, explicit resubmission and erasure-coded
placement (the dedup flush cannot read an erasure-coded object, and no native
caller targets a shard).

Signed aggregate and per-op results pass through unchanged: ENOENT for an
absent object, EINVAL for TIER_EVICT on a plain object and for TIER_FLUSH of a
non-empty object without a dedup configuration, and a target read's error for
a failed promote. On a pool in a cache-tier relation (a tier, or a base pool
with tiers) the OSD answers a session without OSD_CACHEPOOL with EOPNOTSUPP
before any handler (1942-1945); with that capability negotiated a tier pool's
handler returns EINVAL, and a base pool's handler runs. Neither path was
exercised live. A successful reply carries no output and must carry ONDISK,
as the state-changing SET_CHUNK and cache operations do
([CONTRACT.md](CONTRACT.md)); ACK-only and ONNVRAM-only successes fail the
session and are returned as `OutcomeUnknownError`. The dedup TIER_FLUSH success
is the one successful reply with a negative per-op result: aggregate 0 with
the retained per-op -EAGAIN (`TierFlushDeferredSuccess`) is accepted and
returned as received; any other negative per-op result in a successful reply
is malformed. Its ONDISK is the request's own bit echoed by `reply_op_error`
before the CLEAN transaction commits, so it establishes neither that the new
manifest is on disk nor crash persistence; equal flags on the other paths do
not identify a state change either. A lost reply after transmission is an
unknown outcome and is never replayed. Choosing objects to promote, flush or
evict, chunking and fingerprint policy, dedup pool configuration, snapshot
clones and the reference arithmetic they change remain caller or later work.

Evidence:

- The [fresh native vectors](testdata/tier-op-native-README.md) cover the
  three request shapes on v6, v8 and untraced v9; the Go encoder matches each
  header, front, middle and data byte for byte.
- Synthetic public transport tests send each operation on three request/reply
  profiles without the cache-pool capability, keep signed refusals, accept the
  deferred dedup flush shape only for TIER_FLUSH, refuse ACK-only and
  ONNVRAM-only success, and keep a sent request whose connection is lost
  unknown with its transaction, without redial or resend.
- [OSD_TIER_OP_GO_VALIDATION.md](../../integration/fixture/OSD_TIER_OP_GO_VALIDATION.md)
  records `TestCephOSDTierOpGoIntegration`: Go-originated TIER_PROMOTE,
  TIER_FLUSH and TIER_EVICT, each paired with the same operation issued once
  by a native librados twin on its own object, against two owned plain pools
  on one secure/aes256k IPv6 container legacy-v6 single-BlueStore profile,
  judged by native observations that never promote. A separate single-OSD
  legacy-v6 IPv4 CRC host run withheld one TIER_EVICT reply on the bound
  connection and transaction. Native reads saw the evicted extent and intact
  chunk reference while the caller retained an unknown outcome. The client
  sent one correlated request during observation. Live lost replies for
  TIER_PROMOTE and TIER_FLUSH, snapshot clones, scoped identities, fastcdc chunking and a
  failed dedup reference remain unverified.

### Native-only evidence and remaining stages

[OSD_SET_CHUNK_NATIVE_VALIDATION.md](../../integration/fixture/OSD_SET_CHUNK_NATIVE_VALIDATION.md)
records `TestCephOSDSetChunkNativeIntegration`: a closed native librados oracle
issues the refusals above and two disjoint chunks, and observes bytes,
versions, the target's `chunk_refs` and the source's own reference count before
and after, including the reference release on source deletion. Scoped-identity
refusals, snapshot reference accounting and a non-zero source offset from the
Go client remain separate follow-up stages; the TIER_* operations and one
fixed-chunk dedup pool configuration are covered by the Go tier operation
evidence above.

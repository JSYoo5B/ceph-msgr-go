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
clears the manifest; the object stays, now plain and empty. Both are
standalone and flagless here; reference-counted redirects and the tier
operations are not implemented. SET_CHUNK has its own bounded contract below.

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
[Go SET_CHUNK contract](#go-set_chunk-contract) and none of the TIER_*
opcodes. Line numbers refer to `src/osd/PrimaryLogPG.cc` unless another file
is named.

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

All three are inputless client primitives on a non-tier pool, and each refuses
an absent object with -ENOENT.

- TIER_PROMOTE (0x2229, `ObjectWriteOperation::tier_promote`, `rados
  tier-promote`, 7402): a plain object is a no-op success. A chunked manifest
  copies its missing chunks back from itself; a redirect copies its target into
  the object.
- TIER_FLUSH (0x822b, `ObjectReadOperation::tier_flush`, `rados tier-flush`
  with IGNORE_CACHE and IGNORE_OVERLAY, 7460): a dirty or non-manifest object
  starts a blocking dedup flush, and its in-progress result is returned as
  -EAGAIN; a clean manifest returns 0. The dedup path refuses a pool without
  `fingerprint_algorithm` or `dedup_tier` with -EINVAL (`start_dedup`), so this
  operation needs a dedup pool configuration that SET_CHUNK does not.
- TIER_EVICT (0x822c, `ObjectReadOperation::tier_evict`, `rados tier-evict`
  with the same flags, 7488): a non-manifest object is refused with -EINVAL;
  otherwise every chunk is marked missing and its local range is punched.

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
output. A lost reply after
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
- `TestCephOSDSetChunkGoIntegration` sends Go-originated SET_CHUNK and source
  deletions to two owned plain pools and judges each step by native
  observations of bytes, versions, mtimes and the target's `chunk_refs`; no
  live run is recorded yet.

### Native-only evidence and remaining stages

[OSD_SET_CHUNK_NATIVE_VALIDATION.md](../../integration/fixture/OSD_SET_CHUNK_NATIVE_VALIDATION.md)
records `TestCephOSDSetChunkNativeIntegration`: a closed native librados oracle
issues the refusals above and two disjoint chunks, and observes bytes,
versions, the target's `chunk_refs` and the source's own reference count before
and after, including the reference release on source deletion. Scoped-identity
refusals, snapshot reference accounting, a non-zero source offset from the Go
client, the TIER_* operations and dedup pools remain separate follow-up stages.

# Tentacle O2h snapshot context, LIST_SNAPS and ROLLBACK contract

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
Implementation and source audit: 2026-10-15. Native evidence is recorded in
[OSD_SNAPSHOT_VALIDATION.md](../../integration/fixture/OSD_SNAPSHOT_VALIDATION.md).
[CONTRACT.md](CONTRACT.md) supplies the v6 identity, explicit placement,
context, per-op result and uncertain-outcome rules this subset builds on.
The later ENFORCE_SNAPC extension has a separate
[12-vector native wire proof](testdata/enforce-snap-context-native-README.md).
[Live validation](../../integration/fixture/OSD_ENFORCE_SNAP_CONTEXT_VALIDATION.md)
covers pool-context substitution, enforced stale-context refusal and current
caller-context success in five Linux configurations: legacy-v6 with CRC/AES
IPv4 host three BlueStore OSDs or secure/AES256K IPv6 container single MemStore,
request-v8/reply-v6 with the former configuration, and request-v8/reply-v8 or
request-v9/reply-v8 with the latter. The C API does
not expose ENFORCE_SNAPC; its ordinary write control and independent selected
snapshot observations are distinct from the actual Go forced-write result.

## Scope

The client transmits caller-supplied snapshot metadata. It never allocates
or removes snap ids (that is a MON pool operation, MPoolOp), never compares
ids with the pool's snapshot state, and never selects a snapshot itself. The
subset is a read snap id, a write SnapContext with optional ORDERSNAP and
ENFORCE_SNAPC,
CEPH_OSD_OP_ROLLBACK (0x220e) and CEPH_OSD_OP_LIST_SNAPS (0x120a). Pool
snapshot names, MAP_SNAP_CLONE (cache-tier proxy flags) and
EC pools are outside it.

## Wire

The v6 front carries hobj.snap (u64), snap_seq (u64) and snaps (u32 count
and u64 values) after the operations (messages/MOSDOp.h:330-332). The server
decode (542-544) validates none of them. A zero `ReadSnap` is sent as
CEPH_NOSNAP (2^64-2); LIST_SNAPS is sent at CEPH_SNAPDIR (2^64-1), which the
client sets itself. ORDERSNAP is request flag 0x40 (include/rados.h:460).

ROLLBACK is WR|DATA (rados.h:290). Its `snap` union is `snapid` le64 at union
offset 0 with the remaining 20 bytes zero (rados.h:610-612) and no payload,
like the pinned builder (osdc/Objecter.h:1515-1518), which adds no per-op
flag. The reply echoes the union and returns no output.

LIST_SNAPS is RD|DATA (rados.h:267) with a zero union and no input. Its
output is `obj_list_snap_response_t`, version 2 compat 1: a vector of
`clone_info` (version 1 compat 1: cloneid u64, ascending snaps vector<u64>,
overlap vector of (offset, length) u64 pairs, size u64) followed by the
snapset seq (osd/osd_types.h:6786-6869). Clones come in snapset order; a head
that is not a whiteout is appended last with cloneid CEPH_NOSNAP, no snaps
and no overlap (osd/PrimaryLogPG.cc:6571-6656). No server limit bounds the
clone or overlap counts, so the client requires a positive local
`MaxOutputBytes` and decodes counts against the remaining bytes. Version 1
lacks seq, which native decoding reports as CEPH_NOSNAP; a newer compatible
version may append fields.

## Reads at a snapshot

For a vector that may write, any snap id other than CEPH_NOSNAP is refused
with -EINVAL before lookup (PrimaryLogPG.cc:2176-2183); the client refuses
such a request before transmission. A read resolves the snap through
`find_object_context` (12067-12263): CEPH_NOSNAP is the head; without a
snapset the result is -ENOENT; a snap above the snapset seq returns the head,
so an object never written with a SnapContext returns head data for any id;
otherwise the first clone whose id is at least the snap id must list that
snap, else -ENOENT. A missing or recovering clone makes the request wait
without a reply. The reply's user version is the clone's, whose size and
mtime were copied from the head when the clone was made; the reply does not
echo the snap id. The product supports explicit snapshot reads including
bounded standalone MAPEXT and CHECKSUM (CRC32C/xxHash32/xxHash64);
LIST_WATCHERS remains head-only in the current public contract. A read-mode
CALL to a write method is refused by the server with
-EINVAL. Snap id 0 never names a clone; the client sends it as the head
because zero is its head value.

## Write SnapContext

Writes take the client's SnapContext unless the pool is in pool-snapshot mode
without ENFORCE_SNAPC, in which case the server substitutes the pool's
context (execute_ctx, 4211-4221). Snaps in the snap trim queue or purged are
filtered out. An invalid context is refused with -EINVAL before execution
(8984-8987; `SnapContext::is_valid`, common/snap_types.cc:99-117: seq at most
CEPH_MAXSNAP, snaps strictly descending and not above seq). The client applies
the same rules before transmission and additionally refuses a zero snap id,
which `is_valid` accepts as a trailing element but the MON never allocates.
A read-only vector ignores any context on the server, so the client refuses a
context or ORDERSNAP there. No OSD path compares ids with the pool's
allocated snapshots.

`OSDRequest.EnforceSnapContext` explicitly sets request flag `0x100000`.
It leaves the caller's context in effect even in pool-snapshot mode. Both
an empty context and a valid nonempty context are accepted for an otherwise
valid writing vector, including a write-mode CALL or standalone ROLLBACK.
The default is false and leaves existing encoding unchanged. It does not
allocate, refresh or validate snapshot ids against the pool. With ORDERSNAP,
an empty or older context can therefore receive the server's known
EOLDSNAPC refusal. Without ORDERSNAP the server may accept it without making
a clone; callers own that choice.

The explicit flag is refused before public request admission for a read-only
vector, snapshot-selected write, watch/notify operation or malformed context.
The initial IgnoreCache subset also refuses this combination. Watch helpers
reject it before reserving a stream or transferring reconnect ownership.
All complete opcode and payload checks still apply before transmission.
No additional feature bit or automatic context/routing policy accompanies
the flag. An explicit resubmission must retain the original operations,
context, flag and request identity; uncertain writes are never replayed
automatically. Server PG-log deduplication and its retention limits remain
those of the existing explicit resubmission contract.

After all operations, `make_writeable` (8641-8811) clones the pre-write head
when it exists, is not a whiteout, and snaps[0] is newer than the snapset
seq. The clone id is the context seq, its snaps are the context snaps newer
than the old seq, and its size and overlap start from the old head. The clone
log entry takes one version and the head the next; the reply reports the
head's. The snapset seq becomes the maximum of the old seq and the context
seq even when no clone is made. DELETE under such a context, or of an object
with clones, leaves a whiteout head instead of removing it.

With ORDERSNAP, a context seq older than the snapset seq is refused before
execution with EOLDSNAPC, 85 in rados.h (4222-4229); the client names it
`OldSnapContext` rather than using a host errno, since 85 is ERESTART on
Linux. Without ORDERSNAP an older context writes normally without a clone.

## ROLLBACK

`_rollback_to` (8373-8627) resolves the target through the committed snapset:

- a clone covering the snap replaces the head's data, xattrs and OMAP and
  clears a whiteout; mtime changes only when the request carries one;
- a snap above the snapset seq targets the head itself, so only the version
  advances (and a newer context still clones);
- no covering clone (including snap 0) deletes the head, or leaves a
  whiteout when clones remain, and still returns 0; a head with watchers
  returns -EBUSY instead;
- a missing object returns 0 through the log-only path.

The request's SnapContext applies first, so a context newer than the snapset
clones the pre-rollback head. The client requires ROLLBACK to be the only
operation (native librados sends at most an ASSERT_VER before it), a snap id
from 1 to CEPH_MAXSNAP because 0 would delete the head, no FAILOK or EXCL and
no other union field. Success requires ONDISK.

## LIST_SNAPS

LIST_SNAPS must be the only operation at CEPH_SNAPDIR, otherwise -EINVAL
(2321-2337). A missing head is -ENOENT; a whiteout head is listed without a
head entry; an inconsistent snapset is -EINVAL with no output. The client
requires exactly one LIST_SNAPS with no flags, no SnapContext, no mtime and a
zero read snap. Reply validation requires the zero union echo, output within
the local budget, no output on failure, and on success exactly one listing
whose head entry, if any, is last and empty. The raw bytes stay in the
per-op data next to the decoded listing.

## Outcomes

EOLDSNAPC, write-at-snap -EINVAL and invalid-context -EINVAL are known
refusals that never executed. A lost reply to ROLLBACK or to any write with a
SnapContext is an unknown outcome: a clone may or may not exist and the head
may have been restored or deleted. It is never replayed; LIST_SNAPS and the
object version let the caller inspect the result.

## Provenance

This contract paraphrases pinned LGPL-2.1 sources (rados.h permits LGPL-2.1
or LGPL-3.0) and copies no implementation.

# Bounded Tentacle COPY_FROM and COPY_FROM2

Reference source: Ceph 20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, LGPL-2.1 headers and server
source (`include/rados.h:307-308,518-533,631-644`,
`osdc/Objecter.h:1520-1544`, `osd/PrimaryLogPG.cc:8106-8197`). This
implementation translates wire fields only; it copies no native
implementation and adds no native product dependency.

`COPY_FROM` (`0x221a`) and `COPY_FROM2` (`0x222d`) are write opcodes that make
the request's object a copy of a source object. The OSD fetches the source
itself (possibly from another PG) and replies once the copy finished, so the
caller supplies no source placement or routing; it supplies an explicit source
pool, name, key and namespace. The packed 21-byte union plus 7 zero bytes is
`snapid u64, src_version u64, flags u8, src_fadvise_flags u32`; the payload is
the source `object_t` string, its `object_locator_t` struct, and, for
COPY_FROM2 with `TRUNCATE_SEQ` (flag 32), `truncate_seq u32, truncate_size u64`.
The payload length in the op header covers all of it. Go deliberately sends
the minimal flagless COPY_FROM2 input. The pinned native Objecter builder
always appends the two truncate fields, even when the flag is clear; its
flagless input is therefore 12 bytes longer. PrimaryLogPG decodes these
fields only with the flag set. The retained
[native request vectors](testdata/copy-from-native-README.md) preserve that
difference and do not claim raw-byte equality for flagless COPY_FROM2.

`CopyFrom` input must be zero on every opcode other than COPY_FROM,
COPY_FROM2 and SET_REDIRECT. This is checked before standalone opcode
branches, so MAPEXT, CHECKSUM, SPARSE_READ, watch/notify and snapshot requests
cannot silently discard a supplied source. Invalid input is rejected before
allocating a request TID or sending bytes. Regression coverage includes all
eleven source fields across fourteen operation forms and v6/v8/v9 encoders,
plus continued use of the same public connection after rejection.

## Admitted subset

- A copy may stand alone or lead a vector followed only by SETXATTR/RMXATTR.
  A prefix, a second copy, data writes, reads, STAT and other suffixes are
  refused before submission. The narrow copy/xattr contract is below; it
  does not admit arbitrary operations through the restartable finisher.
- Source snapid is `CEPH_NOSNAP` (the head) unless a snapshot id 1 through
  `MaxSnap` is supplied, which the OSD resolves to the covering clone.
  Source FADVISE hints are admitted within `OpFadviseMask`.
  `MAP_SNAP_CLONE` and the cache-tier flags (`FLUSH`,
  `IGNORE_OVERLAY`, `IGNORE_CACHE`, `RWORDERED`) are unsupported.
- COPY_FROM carries flags 0 (librados `copy_from`). COPY_FROM2 carries flags 0
  or `TRUNCATE_SEQ` with a nonzero sequence. A size without the flag, a zero
  sequence, and the `(1, all-ones)` pair the OSD rewrites to `(0, 0)` are
  refused.
- No operation flags: FAILOK would hide the only result and EXCL is CREATE-only.
- The source locator uses no explicit hash and a non-negative pool. A source
  equal to the destination is refused by the server with EINVAL, not locally.
- A write SnapContext/ORDERSNAP applies to the destination; a read snapshot is
  refused like any other writing vector.

## Replies and outcomes

The reply must echo the original union with zero padding and carry no output
and no extent; signed server results (ENOENT for a missing source, ERANGE for a
source version mismatch, EINVAL) are preserved raw. A successful vector
requires ONDISK, like every mutation; ACK alone is an unknown outcome. Local
cancellation after send keeps an unknown outcome and nothing is replayed or
routed again.

## Status

Synthetic wire, validation and public-transport tests pass for v6 and v8
requests/replies (v9 shares the v8 encoder). Live acceptance passes on four
single/3-replica profiles ([record](../../integration/fixture/OSD_COPY_FROM_VALIDATION.md)).
Those historical live profiles use native observation, not twin execution.
Fresh [native request vectors](testdata/copy-from-native-README.md) separately
verify COPY_FROM and truncate-enabled COPY_FROM2 byte parity and the explicit
flagless COPY_FROM2 difference. They do not execute an object copy.
Observed: a copy that fails after the daemon started it reports its aggregate
code (ENOENT, ERANGE, EOVERFLOW) with per-op `-115` (EINPROGRESS); the
self-copy refusal reports `-22` in both places. Both are preserved raw. Source
snapshot ids were verified against a real clone; cross-pool and cross-namespace sources were verified
([record](../../integration/fixture/OSD_POOL_VALIDATION.md)). Later sections of
the copy validation record cover COPY_FROM2 with xattr edits, source-primary
change while its source is frozen, and destination loss while that source
read is blocked. A later [compound checkpoint](../../integration/fixture/OSD_COPY_FROM_COMPOUND_VALIDATION.md)
verifies both copy opcodes with xattr suffixes on absent/populated destinations,
using an independent native copy baseline, object bytes/xattrs/OMAP/header,
source mtime, user versions and known-unsent refusal/connection checks.
A separate [late-error checkpoint](../../integration/fixture/OSD_COPY_FROM_LATE_ERROR_VALIDATION.md)
uses the pinned copy-completion injector: Go COPY_FROM2 plus one xattr suffix
returns aggregate EIO with per-op [-EINPROGRESS, 0], leaves the populated
destination and source unchanged, and restores/read-checks the owned daemon
option. An untouched zero suffix result does not prove execution. This selected
single-BlueStore CRC/v6 error is distinct from a destination-loss or staging
barrier. Destination loss after source-read completion/staging remains unverified. A committed copy
survived OSD SIGKILL and restart on BlueStore; a lost reply was verified
(unknown outcome, no replay) on single and three-replica BlueStore.

`OSDCopyFromInput.SourceFadvise` sets the union's src_fadvise_flags (FADVISE
hints only) for the OSD's read of the source; the reply must echo it. Live:
[OSD_FADVISE_VALIDATION.md](../../integration/fixture/OSD_FADVISE_VALIDATION.md).

Compound form: a COPY_FROM or COPY_FROM2 may lead a vector whose later
operations are only SETXATTR and RMXATTR. The OSD stages the copy through its
finisher and then runs the whole vector in one transaction, so the
destination gets the source's bytes and attributes as edited by the later
operations. Any other operation, an operation before the copy, a second copy
or SET_REDIRECT in a vector is refused before submission. Live:
[OSD_COPY_FROM_VALIDATION.md](../../integration/fixture/OSD_COPY_FROM_VALIDATION.md).

## Native OMAP copy boundary

On the pinned selected profiles, a source's empty OMAP key is omitted by
COPY_FROM/COPY_FROM2 even when the native OSD reports success. Independent
`rados cp` reproduces the same result. `PrimaryLogPG::do_copy_get` starts
its OMAP walk with UPPER_BOUND at the empty cursor; nonempty keys and the
header are copied in these tests. This is server-copy behavior, not a Go
encoder correction or a promise of complete OMAP preservation. The product
returns the raw result without inspecting source OMAP or emulating lost keys.
See the [first failures, independent baseline and bounded acceptance](../../integration/fixture/OSD_COPY_FROM_COMPOUND_VALIDATION.md).

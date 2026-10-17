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
The payload length in the op header covers all of it.

## Admitted subset

- Standalone vectors only: every combination with another operation is refused
  before submission (the daemon completes the copy through a restartable
  finisher and the combined semantics are not characterized).
- Source snapid is `CEPH_NOSNAP` (the head) unless a snapshot id 1 through
  `MaxSnap` is supplied, which the OSD resolves to the covering clone.
  `MAP_SNAP_CLONE`, fadvise hints and the cache-tier flags (`FLUSH`,
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
It uses native observation, not a native-encoded vector or twin execution.
Observed: a copy that fails after the daemon started it reports its aggregate
code (ENOENT, ERANGE, EOVERFLOW) with per-op `-115` (EINPROGRESS); the
self-copy refusal reports `-22` in both places. Both are preserved raw. Source
snapshot ids were verified against a real clone; compound use and failover during a copy are unverified; a committed copy
survived OSD SIGKILL and restart on BlueStore; a lost reply was verified
(unknown outcome, no replay) on single and three-replica BlueStore.

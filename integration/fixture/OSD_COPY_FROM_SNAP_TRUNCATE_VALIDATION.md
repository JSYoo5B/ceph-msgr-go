# COPY_FROM truncated-source, TRUNCATE_SEQ and destination SnapContext cells

Reference: Tentacle v20.2.4 source `7f793731f1b39eb4f465e960113d2363c311b964`.
This checkpoint extends the
[snapshot/version classification](OSD_COPY_FROM_CLASSIFICATION_VALIDATION.md)
with a sequenced-truncated source head and its untruncated clone, the
COPY_FROM2 `TRUNCATE_SEQ` override and the SnapContext of the destination
write. It adds no product admission and injects no fault.

## Source-derived expectations

- Truncate pair. The source OSD's copy-get reply carries the selected
  object's `truncate_seq` and `truncate_size`
  ([do_copy_get](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L9341-L9342)).
  `CopyFromCallback::finish` keeps them only when the client sent no
  `TRUNCATE_SEQ` pair
  ([callback](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L344-L356),
  [decode](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8129-L8135),
  [set_truncate](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8176-L8177)),
  and `finish_copyfrom` writes the pair into the destination object_info
  unconditionally. It does not compare it with the source sequence and does
  not truncate data: the destination size is the copied data size
  ([finish_copyfrom](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L10097-L10176)).
  A clone inherits the head's pair through `copy_user_bits`
  ([osd_types.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L6458-L6471)),
  so a clone taken before a sequenced TRUNCATE keeps the earlier pair
  ([TRUNCATE](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L6986-L6995)).
- Destination snapshots. The copy is an ordinary write on the destination
  head. `make_writeable` clones the pre-copy head only when the head exists
  and `snaps[0]` is newer than the head's snapset seq; the clone id is the
  context seq and its snaps are the context snaps newer than the old seq
  ([make_writeable](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8690-L8778)).
  `finish_copyfrom` adds `[0, old size)` to the modified ranges, so the new
  clone keeps no overlap with the new head
  ([modified ranges](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L10155-L10157),
  [overlap](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8787-L8803)).
  A newer seq is recorded even without a clone
  ([seq](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L8806-L8809)).
  The source's own snapset is never copied into the destination.
- ORDERSNAP. `execute_ctx` refuses a context seq older than the head's
  snapset seq with EOLDSNAPC (-85) before any operation runs, so the copy is
  never started and its per-op result is the unexecuted zero rather than the
  -115 of a started copy
  ([execute_ctx](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L4223-L4230)).
  Without ORDERSNAP an older context writes without a clone.
- Local refusals. The public client admits a copy alone or followed only by
  SETXATTR/RMXATTR, so a TRUNCATE before or after it is refused before a TID
  is allocated. SnapContexts that `SnapContext::is_valid` rejects (a snap
  above seq, non-descending snaps) and this client's stricter zero-id and
  MAXSNAP bounds are refused the same way.

## Recipe and oracle

`TestCephOSDCopyFromSnapTruncateIntegration` natively seeds one 50-byte
source and reserves eight absent destinations, then allocates two
self-managed snapshots s1 < s2. A Go sequenced TRUNCATE (seq 3, size 20) plus
an xattr edit under `{s1, [s1]}` clones the untruncated source as clone s1 and
truncates the head. Native librados snapshot reads, the decoded object_info
(now including `truncate_seq`/`truncate_size`) and pinned `rados listsnaps`
establish the clone (50 bytes, pair 0/0), the head (20 bytes, pair 3/20)
and the clone overlap `[0, 20)`.

For each opcode:

- 6 local refusals on a seeded destination: copy+TRUNCATE, TRUNCATE+copy,
  snap above seq, ascending snaps, zero snap, seq above MAXSNAP. The
  surrounding reads have consecutive TIDs and the native destination state
  and clone list are unchanged.
- Truncate cells on one destination, re-seeded before every case with
  distinct bytes, xattrs and a sequenced truncate pair (seq 200+i) that is
  natively verified: head and clone sources for both opcodes, and for
  COPY_FROM2 head/clone with `TRUNCATE_SEQ` (7, 4) and head with (1, 5), a
  sequence lower than the source's. Each copy is checked natively for bytes,
  size, user xattrs, user version, mtime, the truncate pair and a head-only
  clone list with seq 0.
- SnapContext cells with clone s1 as source: `current` (seeded and copied
  under `{s1, [s1]}`), `newer` (re-seeded, copied under `{s2, [s2, s1]}`),
  `stale-ordersnap` and `stale` (seeded under `{s2, [s2, s1]}`, copied under
  `{s1, [s1]}` with and without ORDERSNAP) and `absent-newer` (an absent
  destination copied under the newer context). Native clone lists, the
  destination clone read and the head readback are compared with the
  expectations above.

Both source states and the source clone list must be unchanged at the end.
Cleanup removes the nine owned heads and both snapshots, waits for native
clone/whiteout trimming of every owned object and requires active+clean. The
receipt `osd-copyfrom-snap-truncate-results.json` is written after cleanup
and retains full public replies, every native before/after state, listings
and the refusal names.

Catalog row:
`osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-replicated-pg-reply-v8.t-osdcopyfromsnaptruncate`.

## Execution record

On 2026-11-20 the row passed on a macOS arm64 host with Docker Desktop
29.9.3 (arm64) and Go 1.27.4, against the pinned image digest
`6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`. This is
not the Linux amd64 CI configuration, and hosted native CI has not run this
row. The result gate passed: the parent passed in 25.17 s with all six
children (local refusals, truncate and SnapContext for each opcode) and no
skip or missing test. The archived 140,536-byte JSON has SHA-256
`0275ff2f5ea1223db68c7e671fe0d48066ed977a90fc82d9fd38fce0a9d5a865`, with
`cleaned: true`, 12 local refusals and 17 copy cells.

Snapshots were s1 = 2 and s2 = 3. Every observation matched its
source-derived expectation:

| Cell | COPY_FROM | COPY_FROM2 |
| --- | --- | --- |
| Head source | 20 bytes, pair 3/20, uv 3 | same |
| Clone source | 50 bytes, pair 0/0, uv 1 | same |
| Head, `TRUNCATE_SEQ` (7, 4) | not applicable | 20 bytes (not truncated), pair 7/4 |
| Clone, `TRUNCATE_SEQ` (7, 4) | not applicable | 50 bytes, pair 7/4 |
| Head, `TRUNCATE_SEQ` (1, 5) | not applicable | 20 bytes, pair 1/5 (below the source's 3) |
| current `{2, [2]}` | no clone, seq 2 | same |
| newer `{3, [3, 2]}` | clone 3, snaps [3], size 19, no overlap; clone read equals the pre-copy head | same |
| stale-ordersnap | aggregate -85, per-op 0, uv 0; destination and list unchanged | same |
| stale `{2, [2]}` on seq 3 | no clone, seq stays 3 | same |
| absent-newer | head only, seq 3 | same |

All successful copies returned 0/0 with ONDISK, gave the destination the
source's user version (lower than the destination's previous version in
several cells, as already recorded for primary change), replaced all user
xattrs and kept the source mtime. All truncate-cell destinations listed a
head only with seq 0. Both source states and the source clone list stayed
unchanged. Cleanup removed every owned head and both snapshots; native
clone/whiteout trimming took 1.82 s, active+clean was confirmed, eight
fixture containers terminated and the final testcontainers inventory was
empty.

## Mutation check

Two temporary product encoder mutations were applied together and then
reverted: COPY_FROM2 sent `truncate_size + 1`, and copy vectors were encoded
with an empty snaps list (seq kept). The same row failed (16.11 s) at the
intended assertions: COPY_FROM `newer` listed only the head with seq 3 (no
destination clone), and COPY_FROM2 `head-truncate-seq` observed pair 7/5
instead of 7/4. The snaps mutation also shows that a seq without snaps still
advances the destination snapset seq. Cleanup and active+clean still
completed, eight containers terminated and none remained. The mutation was
removed and the tracked diff confirmed clean before the checks below.

## Development notes

The first two executions stopped at the source precondition because the
native oracle projected object_info without `truncate_seq`/`truncate_size`,
so Go saw zero for both even though the TRUNCATE reply echoed sequence 3.
The oracle field list now includes the pair. A third execution exposed test
helpers that called `Fatal` on the parent test from subtests; the helpers now
take the calling test. These were test-tooling failures, not daemon results.

## Boundaries

Not covered here: snapshots removed before use (`filter_snapc`), a source
copied onto its own head from its own clone, multi-chunk copies of truncated
objects, sequenced reads or writes against a destination after a
`TRUNCATE_SEQ` copy, extreme snapshot/version values, cache-tier and direct
clone flags, other profiles and fault timing.

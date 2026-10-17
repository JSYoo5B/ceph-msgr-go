# Erasure-coded pool verification

Run 2026-10-17, Ceph 20.2.4 commit `7f793731f1b39eb4f465e960113d2363c311b964`,
image `quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`,
testcontainers fixture, container runtime, three BlueStore OSDs. Contract:
`internal/osd/ERASURE_CONTRACT.md`. Test: `TestCephOSDErasureIntegration`. The
pool oracle's `create` mode gained `erasure` (profile `ceph-msgr-owned-ec`,
k=2, m=1, `crush-failure-domain=osd`, `allow_ec_overwrites true`, removed on
cleanup), and the placement oracle now reports `pool_type` and `pg_shard` (the
target's position in the acting set for EC pools, -1 otherwise).

## Before the change (probe)

With the EC pool created natively, its rule was `set_chooseleaf_tries 5,
set_choose_tries 100, take default, choose_indep 0 type osd, emit`. A fresh
client's MON admission and a new OSD connection of an existing client were
both refused: "peer requires 0x0000001000000000" (CRUSH_V2).

## After the change

- Admission: a fresh client dials MON and opens an OSD connection while the EC
  rule exists, and replicated-pool I/O works.
- EC object I/O to the PG primary: WRITEFULL of 10000 binary bytes (ONDISK,
  native bytes and version equal), partial READ, partial overwrite, APPEND,
  SETXATTR and GETXATTR, STAT size, TRUNCATE and DELETE all match native
  observation. OMAP SETVALS is a known refusal `-95`.
- Watch/notify on the EC object: a Go watch receives a Go notification, the
  explicit acknowledgement payload reaches the notifier, and the completion has
  code 0, one reply and no missed watcher (legacy-v6 and replicated-pg-v8).
- Shard addressing (replicated-PG profile): NO_SHARD and a different shard get
  no reply; the five-second wait ends as an unknown outcome.

## Listing, degraded reads and failover (`TestCephOSDErasureFailoverIntegration`)

Six objects are written by Go into the EC pool. Then:

- PG listing of the subject's PG at its primary (with the primary's shard in
  the replicated-PG profile) lists exactly the written objects whose native
  placement is that PG.
- The OSD of a non-primary shard is SIGKILLed; once native placement shows the
  acting set with that position empty (`[2 2147483647 1]`), a Go read through
  the primary returns the exact bytes, as does a native read: the primary
  reconstructed the missing shard.
- The primary's OSD is SIGKILLed; the new primary holds another shard
  (observed shard 0 on osd.2 to shard 1 on osd.0, and osd.0 to osd.1 in the
  other run). A Go read with the new shard returns the bytes; in the
  replicated-PG profile the same read with the old shard gets no answer.
- Each killed OSD is restarted and PGs return to clean before the next step.

## Snapshots and degraded writes (`TestCephOSDErasureSnapshotAndDegradedWriteIntegration`)

- A self-managed snapshot id allocated through `Client.PoolOp` on the EC pool
  serves as a write SnapContext: after a Go overwrite, Go and native reads at
  the id return the original bytes and the head returns the new ones.
- With one non-primary shard's OSD SIGKILLed and dropped from the acting set,
  a Go WRITEFULL completes with ONDISK. After the OSD returns and PGs are clean,
  the native bytes and version match the receipt; a native read with a
  different shard's OSD then down still returns those bytes, so the shard
  written while its OSD was absent was recovered.
- Passed in legacy-v6 CRC/AES and replicated-pg-v8.

## Optimized EC (`TestCephOSDErasureOptimizedIntegration`, `TestCephOSDErasureOptimizedFailoverIntegration`)

The pool oracle's `optimized` input adds `osd pool set allow_ec_optimizations
true` after the overwrites step and confirms it with `osd pool get`; the pool
dump then shows `nonprimary_shards` `{1}`. The same two scenarios run on it.

- Object I/O, OMAP refusal (`-95`), the shard requirement, watch/notify and
  delete behave as on the plain k=2,m=1 pool.
- Failover: after the shard-0 primary is SIGKILLed the first native map names
  the OSD on nonprimary shard 1 (CRUSH order, no pg_temp); two epochs later a
  primary-first pg_temp names the parity shard's OSD (osd.2 on shard 2 and
  osd.1 on shard 2 in two runs). The test waits for that placement.
- The first attempt sent the actual shard 2 and got no reply for 60 seconds
  while a native read succeeded. Probing every shard showed the OSD answering
  only wire shard 1. Root cause: `osd/OSD.cc` treats a client without
  SERVER_TENTACLE as pre-Tentacle and converts its shard back from the
  primary-first numbering. The fix added `OSDPlacement.PrimaryFirst`
  (pool size and nonprimary shard count, set when the caller's map has a
  pg_temp for an optimized pool); with it the read through the new primary
  returns the bytes, and as a control the same request without it is
  unanswered. The pre-fix runs also used an epoch-34 placement that stayed
  current, so the silence was not an old-epoch discard.
- `TestCephOSDErasureOptimizedSnapshotAndDegradedWriteIntegration` repeats
  the snapshot and degraded-write scenario on the optimized pool: the
  self-managed snapshot keeps the original bytes, and a WRITEFULL accepted with
  one shard's OSD down is recovered onto it (a later read with another
  shard's OSD down matched). Passed in replicated-pg-v8 CRC/AES IPv4 and
  legacy-v6 CRC/AES.
- The placement oracle now finds an OSD's shard as Objecter does
  (primary-eligible shards first) and reports `pg_temp` presence and the
  nonprimary shard count.
- Passed in replicated-pg-v8 CRC/AES IPv4 (failover pair), replicated-pg-v8
  secure AES256K IPv6 (both optimized tests) and legacy-v6 CRC/AES (all four
  EC tests; the v6 OSD maps the shard itself), 2026-10-17.

## Profiles

Passed: legacy-v6 (object-hash v6 requests, the OSD derives the shard) with
the default secure CephX over IPv4 and with CRC/AES; replicated-pg-v8 with
explicit shards over IPv4 and with secure AES256K over IPv6. The
request-v9 profile's fixture allows only one OSD, so EC is not run there; its
encoder shares the v8 shard path.

Both EC tests passed in legacy-v6 CRC/AES and replicated-pg-v8 (IPv4); the
first also in the profiles listed above.

Not claimed: other EC profiles or plugins (beyond k=2,m=1 with and without
`allow_ec_optimizations`), EC partial-read or direct shard reads of optimized pools, MSR rules, writes while degraded,
backfill progress, pool snapshots on EC, notify across clients, compound vectors
beyond the cases above, replica (shard) reads, and multiple PG listing pages.

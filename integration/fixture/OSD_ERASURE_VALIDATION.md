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

## Notify across clients (2026-10-20)

The watch subtest of `TestCephOSDErasureIntegration` and its optimized
variant now sends the NOTIFY from a second Go client with its own OSD session
to the primary: the watcher receives the payload with that client's GlobalID
as notifier, and its ACK completes the notify with one reply. Passed in
legacy-v6 three BlueStore (secure AES256K IPv4) and replicated-pg-v8 three
BlueStore (secure AES256K IPv6).

## Listing pages (2026-10-20)

The failover tests' listing subtest now also pages through the primary
shard's PG with Count 1, passing each reply's raw cursor to the next request:
the pages return the same objects in the same order as the single 64-entry
page, then End. In replicated-pg-v8 the test seeds three more objects in the
subject's PG first, so both profiles page through several entries (6 in
legacy-v6's single PG, 4 in replicated-pg-v8's PG of 8), for plain and
optimized EC. Passed in legacy-v6 three BlueStore (secure AES256K IPv4) and
replicated-pg-v8 three BlueStore (secure AES256K IPv6, explicit shards).

## Pool snapshots (2026-10-20)

`TestCephOSDErasurePoolSnapshotIntegration`: on a k=2,m=1 EC pool (isa) the
native client seeds an object with an xattr and creates a pool snapshot. A Go
WRITEFULL with no SnapContext (the OSD applies the pool's) completes with
ONDISK; native reads return the new head at the reply's version and the
original bytes and xattr at the snapshot name; a Go read at the pool snapshot
id returns the original bytes, a head read the new ones, and LIST_SNAPS names
the snapshot id in a clone. Passed in legacy-v6 three BlueStore (secure
AES256K IPv4) and replicated-pg-v8 three BlueStore (secure AES256K IPv6,
explicit shards).

## Plugins (2026-10-18)

The owned profile names no plugin, so the pools above used the cluster
default. A native `osd erasure-code-profile get` (now logged by the test)
shows that Tentacle's default profile, and so `ceph-msgr-owned-ec`, uses the
`isa` plugin. The oracle's `create` mode gained an optional `plugin`
(`jerasure` or `isa`; empty keeps the default), and
`TestCephOSDErasureJerasureIntegration` runs the same cases on a k=2,m=1 pool
whose profile names `jerasure`. Every subtest passed in legacy-v6 three
BlueStore (secure AES256K IPv4) and replicated-pg-v8 three BlueStore (secure
AES256K IPv6, explicit shards); the default-plugin tests reran and passed in
both, with the plugin logged as `isa`.

## k=2,m=2 on four OSDs (2026-10-22)

Fixture: `CEPH_MSGR_TEST_OSD_REPLICAS` now selects a closed topology rather
than a daemon count. `4` starts four BlueStore OSDs (IDs 0-3) while the
replicated bootstrap and fixture data pools keep size 3/min_size 2. It is
admitted only with the selector `^TestCephOSDErasureK2M2Integration$` on
legacy-v6 or replicated-pg-v8. Before installing the legacy CRUSH map, the
preparation checks that the started daemons are exactly IDs 0-3, and after
installing it verifies the devices and the single root's items. The pool
oracle's `k2m2` width uses its own profile `ceph-msgr-owned-ec-k2m2` (isa,
`crush-failure-domain=osd`). It never force-sets that profile and refuses to
create a pool if the profile already exists. After setting the profile it
reads it back, and after creating the pool it verifies the pool, overwrite
flag and rule. Cleanup removes both owned profiles and reports any that are
still readable.

`TestCephOSDErasureK2M2Integration` takes size, min_size, owned OSDs,
nonprimary shards and the control shard from the oracle receipt
cross-checked with the Go client's authenticated `osd dump` and profile.
Hardcoded k=2,m=1 values are gone. The placement checks complete before
any Go I/O.

Observed in replicated-pg-v8, four BlueStore OSDs, secure AES256K, IPv4,
container runtime:

- Pool: type 3, plugin `isa` (cluster default also `isa`), k=2, m=2, size 4,
  min_size 3 (the pinned `OSDMonitor::prepare_pool_size` value
  `k + min(1, m-1)`, not lowered), `ec_overwrites` set, no
  `allow_ec_optimizations`, `nonprimary_shards` `{}`. The rule is
  `set_chooseleaf_tries 5, set_choose_tries 100, take default, choose_indep 0
  type osd, emit`, as for k=2,m=1.
- Placement at epoch 26: PG `3.0`, acting and up `[1 2 0 3]`, primary osd.1
  on shard 0, derived control shard 1.
- MON admission and replicated-pool I/O of a fresh client with the rule
  present; WRITEFULL of 10000 bytes (ONDISK), partial READ, partial WRITE,
  APPEND, SETXATTR/GETXATTR, STAT size, TRUNCATE and DELETE matched the
  independent native readback; OMAP SETVALS was refused with `-95`;
  NO_SHARD and the control shard got no reply within five seconds (unknown
  outcome); a cross-client watch/notify completed with one ACK reply.
- Owned pool and profile removal was confirmed by the test cleanup.

Receipts:
[run 39800399137](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39800399137)
at `f5b252adb14c1edc01f959afc5ef273d38ecbc90` (every subtest passed; that
source did not yet log the placement), and
[run 39804387429](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39804387429)
at `6dd4a94c608390e2ea26415a4b38d35518184060` (every subtest passed, with
the placement above). In the same run 39800399137 at `f5b252a`, the legacy-v6
three-BlueStore row (secure AES256K, IPv4, broad OSD selection) passed
`TestCephOSDErasureIntegration`, its optimized and jerasure variants, both
failover tests, both snapshot/degraded-write tests and the pool-snapshot
test. The three k=2,m=1 pools of the shared scenario (default isa,
optimized, jerasure) logged size 3 and min_size 2 under the new fact
checks; the failover and snapshot tests do not use those checks. The k=2,m=2 test was skipped there as intended.

Not verified for k=2,m=2: legacy-v6, other security, address or runtime
profiles, jerasure, `allow_ec_optimizations` and primary-first numbering,
degraded reads or writes, failover, listing, snapshots and any in-flight
mutation or epoch-change case. The unit-level numeric-domain test of
`OSDPrimaryFirst` (PoolSize 2..128) is synthetic and does not accept those
combinations as EC configurations.

Not claimed: other EC profiles or plugins (beyond k=2,m=1 with isa and
jerasure, with and without `allow_ec_optimizations` for isa, and the
non-optimized isa k=2,m=2 baseline above), EC partial-read or direct shard reads of optimized pools, MSR rules, writes while
degraded beyond the WRITEFULL case above,
backfill progress, compound vectors
beyond the cases above, and replica (shard) reads: a balanced read routed to
a non-primary shard crashes the OSD in the EC read pipeline (observed
2026-10-20, Ceph assert, not a client behavior; see
EC_NONPRIMARY_READ_CRASH.md), so that path is not exercised.

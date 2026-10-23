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

## Optimized k=2,m=2 on four OSDs (2026-10-23)

The four-OSD topology admits a second closed selector,
`^TestCephOSDErasureOptimizedK2M2(Failover)?Integration$`, on legacy-v6 or
replicated-pg-v8; its first selector stays the default. The pool oracle's
`k2m2` width now accepts `optimized` (isa only). After enabling
`allow_ec_optimizations` it reads the pool's `nonprimary_shards` back and
requires the set the pinned `OSDMonitor::enable_pool_ec_optimizations`
derives for a plugin without a chunk mapping: the data shards `1..k-1`, so
`{1}` for k=2,m=2. A different set fails the creation; nothing is repaired.
The Go fact check compares the authenticated map with the same derivation,
for k=2,m=1 as well. Neither replaces native placement: every shard, epoch
and primary still comes from the native `osd map`/`osd dump` oracle.

`TestCephOSDErasureOptimizedK2M2Integration` runs the baseline cases of the
k=2,m=2 test on the optimized pool. `TestCephOSDErasureOptimizedK2M2FailoverIntegration`
runs the existing failover phases, now parametrized by width: it checks the
pool facts and the subject's native placement first, and it takes the
nonprimary set from the map instead of assuming shard 1.

Observed in replicated-pg-v8, four BlueStore OSDs, secure AES256K, IPv4,
container runtime, at `3fbdfd3435444b62dce81db4a531724079e78b52`
([run 39884774385](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39884774385)):

- Both pools: type 3, plugin `isa` (cluster default `isa`), k=2, m=2, size 4,
  min_size 3, `ec_overwrites` set, `nonprimary_shards` `{1}`, the same
  `choose_indep 0 type osd` rule as the nonoptimized pool.
- Baseline: placement at epoch 50, PG `4.0`, acting and up `[3 1 0 2]`,
  primary osd.3 on shard 0 (control shard 1). MON admission of a fresh
  client, WRITEFULL (ONDISK), partial READ, partial WRITE, APPEND,
  SETXATTR/GETXATTR, STAT, TRUNCATE and DELETE matched the native readback.
  OMAP SETVALS was refused with `-95`. NO_SHARD and the control shard got no
  reply within five seconds (unknown outcome), and a cross-client
  watch/notify completed with one ACK reply.
- Failover pool (eight PGs): the subject's PG `3.5` listed 4 objects, and
  4 single-entry pages agreed with the single page. With nonprimary osd.3
  (shard 1) killed, the acting set was `[2 NONE 0 1]` and the shard-0 primary
  osd.2 reconstructed the read. After osd.3's restart and clean PGs, osd.2
  (shard 0) was killed. Epoch 36 transiently named osd.3 on nonprimary
  shard 1 (acting `[NONE 3 0 1]`, pure CRUSH order). At epoch 38 the PG had a
  pg_temp and osd.0 led on parity shard 2. The first read with
  `PrimaryFirst{PoolSize: 4, NonprimaryShards: 1}` (wire shard 1) returned
  the seeded bytes. The same request without primary-first numbering, and a
  request naming the old shard 0, got no reply within five seconds.
- Every killed OSD was restarted and the fixture waited for clean PGs;
  owned pool and profile removal was confirmed by each test's cleanup.

With four OSDs and size 4, every OSD holds a shard of every PG, so a lost
OSD leaves an empty position instead of a remap. One loss leaves three
shards, which is min_size. The new primary therefore always held parity
shard 2 (wire shard 1, the same wire value as k=2,m=1). Parity shard 3
(wire shard 2) can lead only after shards 0 and 2 are both lost, which
leaves the PG below min_size, so that permutation entry is not observed.

The failover phases are reads only and resend only the idempotent read
after a newer map. The in-flight mutation case is recorded separately below.

## In-flight mutation epoch change and degraded write, optimized k=2,m=2 (2026-10-23)

The four-OSD topology admits a third closed selector,
`^TestCephOSDErasureOptimizedK2M2(InFlightMutation|SnapshotAndDegradedWrite)Integration$`,
and one catalog row selects it on replicated-pg-v8. Both tests create the
optimized isa k=2,m=2 pool through the same oracle path and fact checks as
the preceding section.

`TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration` has two
isolated phases, each on its own object. A Go client with a development
connection wrapper (`internal/testcluster.LostReplyConn`) sends one
APPEND+SETXATTR with a fixed incarnation and mtime to the native primary.
Then the primary is SIGKILLed:

1. Reply withheld. Receive bytes from the primary are held, and the
   native observer polls until the appended bytes are visible. The primary
   is killed and the caller ends its local wait.
2. Request lost. The wrapper discards the client's outgoing bytes after
   authentication, so the primary never receives the request. The primary
   is killed and the session sees the closed connection.

Each phase requires `OutcomeUnknownError` without an `OSDError`, and a
`Submission` with the original transaction, global ID, incarnation, map
epoch, primary and actual shard. The wrapper counts OSD dials: exactly one
to the killed primary and none to the replacement. The native map must then
show a later epoch without the killed OSD and a primary on a
primary-eligible shard. The native observer reads bytes, the xattr, user
version and mtime, and these must match the injected fault. In phase 1 the
caller does not resubmit. In phase 2 the native state equals the
pre-state, so the caller resubmits the same operations to the new primary.
The request carries `Resubmit` with the original submission,
`RetryAttempt` 1 and the new placement with primary-first numbering. After
each phase the killed OSD is restarted, the fixture waits for clean PGs and
the native state is compared again.

`TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration` runs the
k=2,m=1 snapshot and degraded-write cases, now parametrized by width, on
the optimized k=2,m=2 pool. It checks the subject's placement against the
pool facts before the kill and reads the degraded write back natively
before the restart.

Observed in replicated-pg-v8, four BlueStore OSDs, secure AES256K, IPv4,
container runtime, at `0dfedcaceb813e95e9582f82b499b2d08966fca5`
([run 39901553620](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39901553620)):

- Pools: size 4, min_size 3, `nonprimary_shards` `{1}`, owned OSDs 0-3.
- Phase 1: PG `3.4` at epoch 27, acting `[1 2 3 0]`, primary osd.1 on
  shard 0, native version 1. The reply was withheld after the native
  observer saw the append. After osd.1 was killed, the local wait was ended:
  `OutcomeUnknownError` (context canceled) with transaction 1, incarnation
  7, epoch 27, shard 0. Epoch 28 transiently named osd.2 on nonprimary
  shard 1. At epoch 30 osd.3 led on parity shard 2 under a pg_temp
  (`PrimaryFirst{PoolSize: 4, NonprimaryShards: 1}`), acting
  `[NONE 2 3 0]`. The native observer read the appended bytes, version 2,
  mtime 1700005001 and the new xattr. That is one application, so there
  was no resubmission. After restart and clean PGs (epoch 34, osd.1 again
  primary on shard 0) the native state was unchanged.
- Phase 2: PG `3.7` at epoch 34, acting `[3 2 1 0]`, primary osd.3 on
  shard 0, native version 1. The request bytes were discarded. After osd.3
  was killed the session reported `OutcomeUnknownError` (EOF) with
  transaction 1, incarnation 7, epoch 34, shard 0. At epoch 37 osd.1 led on
  parity shard 2 under a pg_temp, acting `[NONE 2 1 0]`. The native state
  was the pre-state (version 1, mtime 1700005000, no xattr). The explicit
  resubmission (transaction 1, `RetryAttempt` 1, wire shard 1) returned
  code 0 with ONDISK, `RetryAttempt` 1 echoed, reply epoch 37 and user
  version 2. The native observer then read one append, version 2, the new
  mtime and xattr. After restart and clean PGs (epoch 41) the state was
  unchanged.
- Degraded write: PG `4.5` at epoch 50, acting `[3 1 2 0]`, primary osd.3
  on shard 0. osd.1 (nonprimary shard 1) was killed; at epoch 53, acting
  `[3 NONE 2 0]`, the WRITEFULL returned ONDISK with user version 2 and the
  native readback matched. After osd.1's restart and clean PGs the native
  bytes and version matched. With osd.0 then killed, the native read,
  which needs the recovered shard, matched. The self-managed snapshot case
  passed on the same pool (clone and head read back natively).
- Every killed OSD was restarted to clean PGs, and owned pool and profile
  removal was confirmed by each test's cleanup.

Run [39901146526](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/39901146526)
at `c25990a` showed the same placements and outcomes. Its phase 2 failed
only on a test assertion that expected `OSDSubmission.Attempt` 1 after the
resubmission. That field is local metadata which the client leaves at 0;
the server's echoed `RetryAttempt` was 1. The next commit asserts the echo
instead.

What these phases establish: the client keeps the uncertain outcome and
its identity across an EC primary change, and opens no new connection. The
native observer, not the client, tells applied from not applied. A caller
resubmission of a mutation that had not applied ran exactly once on the
replacement primary. They do not establish exactly-once delivery or
deduplication of an applied EC mutation on resubmission. They also do not
cover a request held at the primary (for example while the PG is not
active) when it dies, or a signed refusal across the epoch change.

Not verified for k=2,m=2: legacy-v6, other security, address or runtime
profiles, jerasure, wire shard 2 of the size-4 primary-first permutation,
snapshots or degraded writes on the nonoptimized pool, an in-flight
mutation held inside the OSD when the primary dies, and EC resubmission
after an applied mutation. The unit-level numeric-domain test of
`OSDPrimaryFirst` (PoolSize 2..128) is synthetic and does not accept those
combinations as EC configurations.

Not claimed: other EC profiles or plugins (beyond k=2,m=1 with isa and
jerasure, with and without `allow_ec_optimizations` for isa, and the
isa k=2,m=2 baseline with and without `allow_ec_optimizations` and the
optimized k=2,m=2 failover reads, degraded write, snapshot and in-flight
mutation cases above), EC partial-read or direct shard reads of optimized pools, MSR rules, writes while
degraded beyond the WRITEFULL cases above,
backfill progress, compound vectors
beyond the cases above, and replica (shard) reads: a balanced read routed to
a non-primary shard crashes the OSD in the EC read pipeline (observed
2026-10-20, Ceph assert, not a client behavior; see
EC_NONPRIMARY_READ_CRASH.md), so that path is not exercised.

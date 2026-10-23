# Erasure-coded pools: admission and shard addressing

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/ceph_features.h` CRUSH_V2 bit 36, `osd/OSDMap.cc` get_features,
`crush/CrushWrapper.cc` is_v2_rule, `messages/MOSDOp.h` spg_t; LGPL-2.1,
independently translated).

## Policy

By explicit user decision on 2026-10-17, CRUSH_V2 is advertised on MON and
OSD sessions. Every erasure-code rule uses `choose_indep`/`chooseleaf_indep`
and `set_*_tries` steps, so once the OSDMap holds one, `OSDMap::get_features`
makes MONs and OSDs require CRUSH_V2 from every client. Without it this client
could not connect to a cluster that merely contains an EC pool, even for
MON commands or replicated-pool I/O (observed live: "peer requires
0x0000001000000000"). The bit is admission only: the client computes no CRUSH
placement, applies no OSDMap and claims no other CRUSH generation. CRUSH_MSR
(multi-step retry rules) and the other OSD-side CRUSH bits remain
unadvertised; clusters requiring them are still refused explicitly. Creating
an MSR erasure rule (for example `crush-osds-per-failure-domain=3`) was
observed to need `require_min_compat_client squid` first ("new crush map
requires client version squid"), a cluster-wide client baseline beyond this
admission exception; such clusters are not supported.

## Requests to an EC pool

An EC PG is addressed by the shard the target OSD holds. With the
replicated-PG request profiles (`OSDProfileReplicatedPG*`), set
`OSDPlacement.Erasure` and `Shard` to the primary's position in the PG's acting
set; the caller derives it from its own map like `Primary`. NO_SHARD or another
shard makes the OSD treat the request as misdirected: it never answers, and
the wait ends with the caller's context (observed live). The object-hash
profile sends MOSDOp v6 without a shard and the OSD maps the raw hash to the
primary shard itself (observed live). There is no automatic shard selection.

Object operations travel unchanged; the pool decides what it supports. Live on
k=2, m=1 with `allow_ec_overwrites`: WRITEFULL, partial READ, partial WRITE,
APPEND, SETXATTR/GETXATTR, STAT, TRUNCATE and DELETE behaved as natively; OMAP
writes are refused with `-95` (EOPNOTSUPP). `OSDPGListTarget.Erasure/Shard`
addresses PG listing the same way (replicated-PG profiles; the v6 object-hash
listing has no shard and the OSD derives it). With one shard's OSD down the
primary reconstructs reads; after the primary dies the new primary holds a
different shard and only requests naming it are answered, so the caller must
re-derive the shard from the new map along with the primary. The same
object cases passed on a non-optimized isa k=2,m=2 pool (size 4, min_size 3)
over four OSDs in one replicated-PG profile, without faults, and on an
optimized isa k=2,m=2 pool in the same profile, together with PG listing, a
degraded read and primary failover. On the optimized k=2,m=2 pool a
self-managed snapshot and a write while one shard's OSD was down were also
verified. After that OSD's restart, clean PGs and another single-OSD
outage the native object bytes still matched; the chunk recovered onto the
returned shard is not checked. Other opcodes, recovery traffic and
other EC profiles are unverified.

## In-flight EC mutations across an epoch change

An EC mutation's outcome follows the same rule as any OSD request: a reply
is a known result, and a request whose frame was handed to the connection
but that got no reply ends with `OutcomeUnknownError`. The returned
`OSDReply.Submission` keeps the transaction ID, global ID, incarnation,
map epoch and placement (actual shard, primary-first facts) of that one
send. The client neither resends nor reconnects; it does not learn the new
primary, shard or pg_temp. The caller decides from its own observation
whether the first attempt applied. If it resends, it routes by its new map
and may pass `Resubmit` with the original submission so the replacement
primary can answer from the PG log. Duplicate detection covers only
requests still in that log and is not an exactly-once guarantee.

Observed live on the optimized isa k=2,m=2 pool (one replicated-PG
profile). An APPEND+SETXATTR was in flight when the shard-0 primary was
SIGKILLed and a parity shard's OSD took over under a primary-first pg_temp.
Two cases were checked. In one, the reply was withheld after the mutation
was applied. In the other, the request never left the client. Both ended
with `OutcomeUnknownError` and the original identity, and no further OSD
connection was opened. The native observer saw exactly one application in
the first case and none in the second. Only in the second case did the
caller resubmit the same operations under the original transaction to the
new primary (wire shard 1 for actual parity shard 2). The mutation was
then applied once. `OSDSubmission.Attempt` stays 0 on that resubmission;
the reply's `RetryAttempt` echoes the attempt. Not verified: a request held
at the primary when it dies, a resubmission after an applied mutation on an
EC pool, and a signed refusal across the epoch change.

## Optimized EC pools (`allow_ec_optimizations`)

Tentacle's optimized EC (`OSDMonitor::enable_pool_ec_optimizations`, Ceph
20.2.4) marks the data shards other than the first as `nonprimary_shards`
(`{1}` for k=2, m=1 and for k=2, m=2) and stores such a PG's pg_temp primary-first. The OSD
reads an EC request's spg_t shard from a client lacking SERVER_TENTACLE
(bit 50, incarnation 2) in that pre-Tentacle primary-first numbering whenever
the pool is optimized and the PG has a pg_temp, and converts it back
(`osd/OSD.cc` ms_fast_dispatch and dispatch_session_waiting,
`OSDMap::pgtemp_undo_primaryfirst`). This client's OSD sessions do not
declare SERVER_TENTACLE: it would also select Tentacle-only encodings such as
pg_pool_t v32, which this client does not implement. Instead
`OSDPlacement.PrimaryFirst` (and `OSDPGListTarget.PrimaryFirst`) carries the
pool size and nonprimary shard count, and the client encodes the actual
`Shard` with `OSDMap::pgtemp_primaryfirst`; `Submission.Placement` keeps the
actual shard. Set it exactly when the caller's map shows an optimized pool
and a pg_temp for the PG; shard 0 is never renumbered. Observed live: after
the shard-0 primary died, the new primary held parity shard 2 under a
primary-first pg_temp and answered only the renumbered request (wire shard
1); the actual shard 2 on the wire went unanswered. The same was observed on
an optimized k=2,m=2 pool (`PrimaryFirst{PoolSize: 4, NonprimaryShards: 1}`,
parity shard 2 sent as wire shard 1). The legacy-v6 object-hash
profile sends no shard and the OSD maps the PG itself.

Primary selection also changes. A nonprimary shard never becomes primary for
long: the first map after the loss of the primary named the OSD on shard 1
(pure CRUSH order); that OSD had the MON set a primary-first pg_temp and two
epochs later the map named the parity shard's OSD. A request built from the
transient map reaches a PG that is about to change interval and may be
discarded without a reply, like any request from an epoch older than the
PG's current primary interval; the caller re-derives placement from the newer
map and resends (nothing resends automatically). For finding the shard of an
OSD that appears twice in an optimized acting set, Objecter searches the
primary-eligible shards first; the fixture oracle does the same.

The writer-side backoff gate cannot undo the renumbering from the encoded
request, so a BLOCK on an EC PG gates requests for its object range on every
shard of that PG on the connection (over-refusal, never a bypass). Self-managed snapshots, watch/notify and a write during one
shard's absence were verified on optimized k=2,m=1 and k=2,m=2 pools. On
k=2,m=1 the read after recovery with another OSD down decodes from the
returned shard and the primary only. On k=2,m=2 three shards remain, so
that read shows only that the object bytes match.

Verification: [OSD_ERASURE_VALIDATION.md](../../integration/fixture/OSD_ERASURE_VALIDATION.md).

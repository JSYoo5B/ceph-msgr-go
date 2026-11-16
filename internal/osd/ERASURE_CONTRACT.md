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
the reply's `RetryAttempt` echoes the attempt.

A separate [applied-resubmission experiment](../../integration/fixture/OSD_EC_APPLIED_RESUBMIT_VALIDATION.md)
used one deliberate caller resubmission of an independently observed applied
APPEND+SETXATTR after primary replacement on that same EC profile. Its retained
TID/incarnation returned ONDISK with the original user version, and native bytes,
xattr and mtime did not change. A fresh-TID control appended again and advanced
the native version. The original caller's unknown outcome was preserved. This
is bounded PG-log duplicate handling, not an automatic replay policy or a
universal exactly-once guarantee.

A [held-request control](../../integration/fixture/OSD_EC_HELD_REQUEST_VALIDATION.md)
on that profile covers a request that reached the primary and was held
there. A Go APPEND+SETXATTR named a map epoch the cluster had not
published, so the primary queued it in `waiting_for_map` before decoding its
operations; the primary's op tracker showed it undecoded and delayed for the
map. The primary's OSD was then SIGKILLed. The call ended through the session
with `OutcomeUnknownError` and the TID the primary had tracked, and no further
OSD connection was opened. After the restart and clean PGs the native object
was unchanged and the returned primary's PG log did not hold the identity.
One caller resubmission under the original transaction then applied once.
For this client a PG below `min_size` does not hold a request: the
replicated-PG profiles advertise RADOS_BACKOFF, so the primary installs a
PG-wide backoff and drops the op. One exploratory run saw that BLOCK, and the
later kill of the primary ended that call as unknown too; it is not part of
the control.
Not verified: a request held after it reached `do_op`, retention/trim
boundaries, other profiles and a signed refusal across the epoch change.

A test-only [primary PG-history snapshot](../../integration/fixture/OSD_PG_HISTORY_CALIBRATION_VALIDATION.md)
on the same profile finds known Go request identities in the primary's native
PG log by entity, incarnation and TID. It calibrates presence on the primary
only. It does not establish that an identity is absent from replica or
shard-local history, so it is not a precondition for resubmitting after
history loss.

A test-only [shard-local capture](../../integration/fixture/OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md)
on the same profile reads every acting shard's own PG log from a private
copy of its store. After one version-pinned COPY_FROM2 from a replicated pool
into a fresh head of the optimized pool, the primary and all four shard
copies held the copy's request identity on the destination entry and the
source write's identity with its user version as that entry's first extra
request ID. This is a positive control for a copy that creates the head on
every shard. It establishes no absence, eviction, trimming or replay
behaviour.

A [second capture](../../integration/fixture/OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md)
on that profile follows one full write and one overwrite of a few bytes
inside shard 0's chunk. The overwrite's log entry names shards 0, 2 and 3.
Those three shard copies held it; the copy of nonprimary data shard 1 did
not, and its own log and `last_update` ended at the full write, while the
other shards recorded shard 1 as left out of that version range. A request
identity can therefore be missing from one acting shard's log by design,
without any trimming.

Two [continuations](../../integration/fixture/OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md)
capture that shard's later state. After one more overwrite inside shard 1's
chunk in the same interval, shard 1's own log held the full write's entry
and the new entry, and not the overwrite's entry between them. After the
PG's interval was ended once by an owned `pg repeer`, with the primary and
the acting set unchanged and no daemon stopped, shard 1's `last_update`,
`last_complete` and log head were at the overwrite's version while its last
entry was still the full write, and its own info recorded that version
range; the first write of the new interval, although confined to shard 0's
chunk, named every shard and was held by all four. A shard's log head is
therefore not evidence that the shard holds every entry up to it.

Two further [controls](../../integration/fixture/OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md)
take shard 1's OSD out of the PG after the overwrite, through the fixture's
forced kill of an owned OSD, and start it again. With nothing written in
between, the returned shard's copy was as after the interval change: only
the full write's entry under a head at the overwrite's version. With one
acknowledged Go overwrite inside shard 1's chunk while the PG was active
without it, the returned shard's copy held all three entries once the PG was
clean again, with an empty missing set: the primary's log at the return gave
it the entry of the overwrite it had been left out of, which names the other
three shards only. Which entries a shard's own log holds therefore also
depends on whether it was recovered after the write. Neither control reads
the returned shard's chunk bytes.

One more [control](../../integration/fixture/OSD_EC_PARTIAL_WRITE_PRIMARY_REPLACEMENT_VALIDATION.md)
takes the primary's own OSD out after the overwrite. The map named the
left-out shard's OSD first for one epoch, which did not activate the PG,
and then the first parity shard's OSD under a primary-first pg_temp, which
led the PG active with shard 0's position empty. Its info held the same
range for shard 1, so its peering sent shard 1 nothing. One acknowledged Go
overwrite inside shard 0's chunk went through it with primary-first shard
numbering; as the first write of its interval it named every shard,
although by its bytes it left shard 1 out. After the original primary
returned, under a new address, and recovered the object to itself, the PG
was clean on its original placement: shards 0, 2 and 3 held all three
entries, and shard 1 held the full write and that overwrite without the
earlier overwrite between them. A shard that can lead therefore takes over
the record of the range a nonprimary shard was left out of. The outage of a
parity shard's OSD, more than one write while a member is away, and a write
during the recovery itself were not observed with this capture, and nothing
here is a replay or deduplication claim.

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

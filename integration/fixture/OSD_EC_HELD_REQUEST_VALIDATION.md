# An EC mutation held at the primary when the primary's OSD dies

This is a test-only control for issue #9, work item 2 (an epoch change
around an in-flight mutation). The
[in-flight control](OSD_ERASURE_VALIDATION.md) covers a mutation whose reply
was withheld after it applied and one whose request never left the client.
This control covers the case between them: the request reached the acting
primary and the primary held it, unapplied, when its OSD was killed. It
answers three narrow questions:

- how can the primary hold a Go request of this profile, unapplied, long
  enough to kill its OSD, and how is that hold observed natively?
- what does the Go call report when the primary dies, and does the client
  send or connect again?
- after the OSD's return and clean PGs, was the mutation applied, and does
  the primary's PG log hold its identity?

It adds no product API, opcode or flag. The OSD leaves through the fixture's
forced kill of an owned OSD and returns through its start, the daemon
control that the existing failover tests use; no other fault is injected, no
cluster-wide flag is set and no daemon configuration changes. The only new
fixture capability is a read-only oracle mode for the primary's op tracker.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

The hold:

- A fast-dispatched client op is registered in the OSD's op tracker before
  it is queued for its PG
  ([registration](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L7692)),
  and its minimum epoch is the map epoch the client sent
  ([epochs](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L7711-L7712),
  [default](https://github.com/ceph/ceph/blob/v20.2.4/src/messages/MOSDFastDispatchOp.h#L15-L18)).
  An op for shard 0 from a client with RESEND_ON_SPLIT goes straight to its
  PG ([queue](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L7730-L7737)).
- `PrimaryLogPG::do_request` first queues an op whose minimum epoch is newer
  than the PG's map in `waiting_for_map`, marks it delayed with
  `op must wait for map` and asks for that map
  ([wait for map](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L1845-L1862)).
  This happens before the backoff, peering and activity checks and before
  `do_op`.
- Only a map of that epoch requeues the op
  ([requeue](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc#L1225-L1250)).
  The control sends an epoch 1000 beyond the placement's, far more epochs
  than the test publishes, so the hold can end only with the process.
- `do_op` finishes the decode of the operations and resets the op's
  description
  ([do_op](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L2001-L2011)).
  Until then the op prints its raw PG and `(undecoded)` where the object and
  the operations would be
  ([print](https://github.com/ceph/ceph/blob/v20.2.4/src/messages/MOSDOp.h#L622-L641)).
  A held op whose description shows `(undecoded)` has therefore not reached
  `do_op`, and nothing of it was executed or logged.
- A delayed op reports its delay as its flag point
  ([flag point](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OpRequest.h#L116-L126),
  [delay](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OpRequest.h#L161-L163)).
- From the same source, not observed: the request for the map raises the
  held OSD's own map subscription to that epoch
  ([subscription](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L8057-L8069)).
  The control kills that OSD within seconds and does not depend on it.

Why not a PG below `min_size`: the candidate was to kill two nonprimary
OSDs so that the PG is peered but not active and the primary queues the op
in `waiting_for_active`
([active wait](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L1929-L1938)).
That queue is not reached by this client. The replicated-PG profiles
advertise RADOS_BACKOFF ([backoff contract](../../internal/osd/BACKOFF_CONTRACT.md)),
and for such a client a PG that is down, incomplete, or peered and not
active installs a PG-wide backoff and drops the op
([PG backoff](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L1868-L1893),
[BLOCK](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PG.cc#L570-L594)).
A PG that is still peering holds the op in `waiting_for_peered` only while
`osd_backoff_on_peering` is false (its default,
[option](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/global.yaml.in#L3243-L3247)),
and only for the duration of peering, which the fixture's controls do not
bound. An exploratory run confirmed the drop: see
[Exploratory runs](#exploratory-runs).

The observation: `OpTracker::dump_ops_in_flight` dumps every tracked op
([dump](https://github.com/ceph/ceph/blob/v20.2.4/src/common/TrackedOp.cc#L236-L282),
[op](https://github.com/ceph/ceph/blob/v20.2.4/src/common/TrackedOp.cc#L505-L520),
[type data](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OpRequest.cc#L53-L89)),
and the OSD answers it as a tell command
([command](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L2854-L2878)).

No Ceph source is incorporated into the product.

## The in-flight snapshot

`tools/osd_pool_oracle.py` gains mode `opsinflight`. It reads the native
placement of an owned object, sends `dump_ops_in_flight` to the primary that
placement names, and reads the placement again. `tools/osd_ops_in_flight_oracle.py`
accepts the reply only if the command returned 0, the reply is at most
256 KiB of one strict JSON document `{"ops": [...], "num_ops": N}` with
`num_ops` equal to the array's length (at most 512), every op has exactly the
dumped fields and every event its three fields, and the two placement
records are identical. It reports every op whose request identity is
`client.<num>.<inc>:<tid>` for the caller's entity number and incarnation,
with the TID, the PG with its shard, the map epoch, the flag point and the
event names. Such an op whose description does not parse, or whose
`client_info` names another entity or TID, fails the receipt instead of
being skipped. The receipt never establishes absence.

The Go test rechecks the receipt in `checkHeldRequest`, independently of the
oracle's parse, and recounts the client's ops in the raw reply. It accepts
the snapshot only if it is complete; its placement records are identical and
equal the route the request used, at an epoch not older than the route's and
older than the held one; it was answered by the route's primary for the
route's PG; it holds exactly one op of the client entity and incarnation;
that op's PG is the route's PG and shard, its epoch is the held one, its
description shows `(undecoded)`, its flag point is `op must wait for map`,
and its events after `reached_pg` are exactly that one event.

## Scenario

`TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration` runs on
the four-BlueStore fixture (`osd-replicas-4`, `replicated-pg-v8`, secure,
aes256k, IPv4, container) with one owned optimized isa k=2,m=2 pool of eight
PGs. It

1. requires the object's placement to be led by the OSD of its own primary
   shard without a pg_temp, and writes a 6,000-byte seed with WRITEFULL
   (incarnation 20) through the ordinary Go client; native bytes, user
   version and mtime must match and the test xattr must be absent;
2. opens a second Go client whose OSD dials are counted per endpoint, one
   connection to the primary, and sends APPEND of 21 bytes plus SETXATTR
   `ec-held` (incarnation 21) whose map epoch is the placement's plus 1000;
3. takes in-flight snapshots of the primary, at most twenty half a second
   apart, until `checkHeldRequest` accepts one; the call must still be
   pending; a second snapshot must accept the same TID, and the native
   object must still equal the seed;
4. kills the primary's OSD with the fixture's forced kill and requires its
   SIGKILL receipt (exit code 137);
5. requires the session, not the caller's context, to end the call within
   30 seconds, with an `OutcomeUnknownError` and no server error, whose
   submission keeps the held TID, the client's global ID, incarnation 21,
   the held epoch, attempt 0 and the placement's primary and shard;
6. waits for a map naming another primary on a shard that can lead, older
   than the held epoch, reads the object natively and requires the client
   to have dialed nothing new and the old primary only once;
7. starts the OSD again, asks the fixture's readiness control for every OSD
   up and every PG `active+clean`, and requires the placement to equal the
   recorded one under the restarted primary's new address at an epoch older
   than the held one;
8. reads the object natively and requires the seed's bytes, user version,
   mtime and xattrs; requires no new dial again; and takes one primary
   PG-history snapshot that must hold the seed write's identity once in the
   log at its replay version, and neither the held identity nor a near miss
   (the held TID under the seed's incarnation);
9. as the caller's explicit decision, resubmits the same operations once to
   the returned primary under the original transaction (`Resubmit` with the
   original submission, retry attempt 1). The reply must be ONDISK with the
   original TID; the native object must hold the seed and one copy of the
   appended bytes at the reply's user version, with the new mtime and xattr;
   a second PG-history snapshot must hold the held identity once, at the
   reply's replay version, and the seed's once. The resubmission must be
   exactly one new dial attempt, at the returned primary's endpoint (the old
   one or another), with no attempt at any other OSD endpoint; the original
   outcome stays unknown.

If the call does not end within the 30 seconds, the test cancels it and
waits at most five more seconds for a late result, which it reports only as
a diagnostic, then fails. A worker that stays blocked after the cancellation
therefore cannot hold the test with the primary still down; the buffered
result channel lets that worker finish during cleanup.

| Statement | Status |
| --- | --- |
| The primary's op tracker holds exactly one op of the client and incarnation, for the route's PG and shard at the held epoch, undecoded, at flag point `op must wait for map`, with that event right after `reached_pg` and nothing after it | required, in two snapshots with the same TID |
| The Go call is still pending while the op is held; the native object equals the seed | required |
| The forced kill of the primary's OSD has exit code 137 | required |
| The session ends the call with `OutcomeUnknownError`, no server error, and the held TID, global ID, incarnation, held epoch, attempt 0 and placement in its submission | required |
| The tracked TID equals the TID of the unknown outcome | required: the op the primary held is the Go request |
| No dial after the unknown outcome, up to the caller's resubmission | required; a MON redial would be allowed, none happened |
| The resubmission is exactly one dial attempt, at the returned primary's endpoint, and no other OSD is dialed | required, by the change of each endpoint's attempt count |
| A map naming another primary, on a shard that can lead, with the killed OSD out of the acting set, at an epoch newer than the route's and older than the held one; the native object unchanged | required; the pg_temp and the shard are recorded |
| After the restart, every PG `active+clean` and the placement back on the recorded primary under a new address, without a pg_temp | required |
| Native bytes, user version, mtime and xattrs equal the pre-state | required: not applied |
| The returned primary's log holds the seed write once and neither the held identity nor the near miss | required, as an observation of that primary's log at that epoch |
| One resubmission under the original identity applies once: ONDISK, original TID, new user version, one appended copy, the xattr, and the identity once in the log at the reply's replay version | required |
| The replacement primary and its shard, the epochs, the PG, the addresses, the events before `reached_pg`, the description | recorded |

The statement that the mutation was not applied comes from the native object
and from the returned primary's log. The log snapshot says only that this
primary at that epoch holds no entry for the identity; the second snapshot,
after the resubmission, shows that the same snapshot finds that identity
when an entry exists. The control does not look at the other shards' logs.

## Cluster-free checks

- `tools/test_osd_ops_in_flight_oracle.py` accepts the client's op among a
  sub-op and two near misses (another incarnation, a longer entity number),
  an undecoded op, a replicated PG and an empty dump; refuses twelve
  untrusted shapes and an oversized reply, six inconsistent ops of the target
  client, five incomplete receipts (refused, oversized, moved placement,
  unparsable, inconsistent target op) and six invalid entities.
- `TestCheckHeldRequest` accepts the synthetic receipt and the same
  placement at a newer epoch, and refuses twenty-six changed receipts and
  five requests. `TestNewDials` accepts unchanged dials and a MON redial and
  refuses a second dial of the OSD and a dial of another endpoint, and
  reports two attempts at the OSD with both counts. For the resubmission,
  `checkOneDial` accepts one attempt at the old endpoint, at a new endpoint,
  and with a MON redial, and refuses no attempt, two attempts at the old or
  at a new endpoint, the attempt at an endpoint other than the returned
  primary's, an attempt at both endpoints, and an attempt at another OSD.
  With its attempt-count check removed it accepted the two cases with two
  attempts at one endpoint.
- `TestEndWithin` takes a result within the wait, reports a call that ends
  only on the cancellation, and returns within the wait and the grace for a
  call that stays blocked. With the grace removed from the diagnostic
  receive the test did not return and hit its five-second timeout.

## Reproduction

On an authorized Linux runner, hold its run lock and run the dedicated row
with the environment the catalog gives it, which includes the pinned image
and the row's fixture capabilities:

```sh
(
  env_lines=$(python3 tools/scenario_plan.py env osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2requestheldatdyingprimary) &&
  eval "$env_lines" &&
  sh integration/run.sh
)
```

The dedicated mandatory row is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2requestheldatdyingprimary`.
Request it with the CI comment command. Basic PR CI builds and unit-checks
the fixture but does not run this daemon test. The native result of a
pull-request head is recorded in that pull request with the tested SHA. A
pass at one SHA is not evidence for a later revision.

## Local native calibration

These runs preceded the pull request and are not exact-SHA CI receipts.
They used the catalog row's environment and the pinned image on Linux amd64
hosts with Docker 29.9.1 and Go 1.26.9, through `integration/run.sh` and the
native result gate, with one test process on each engine. Every row ran in
its own fixture.

The recorded runs used three archives of commit `b058487`, which added the
row; later commits change documents only. Each archive ran the row, the
first as the first run of that commit, and the first archive also ran the
five neighbouring rows below. The gate recorded the test as PASS with no
problem in all three runs:

| Archive of `b058487` | Test, row |
| --- | --- |
| first | 23.37 s, 88 s |
| second | 22.66 s, 87 s |
| third | 23.26 s, 88 s |

Every run observed the same PG, acting set and roles. The fixture's
topology is deterministic, so they are repetitions of one placement, not
different ones. In the second and third archive every epoch was one lower;
nothing else differed but the OSD addresses. The values below are those
of the first archive.

| Observation | Value |
| --- | --- |
| Placement | PG `3.1`, up = acting = `[0,2,3,1]`, primary osd.0 on shard 0, epoch 25 |
| Seed | `client.4722.20:1`, replay version `25'1`, user version 1 |
| Held request | `client.4743.21:1`, map epoch 1025 |
| In-flight snapshot | one op in the tracker, the held one, at the first snapshot: `osd_op(client.4743.21:1 3.1s0 3.5a44d5b1 (undecoded) ondisk+write+returnvec e1025)`, flag point `op must wait for map`, events `initiated`, `header_read`, `throttled`, `all_read`, `dispatched`, `queued_for_pg`, `reached_pg`, `op must wait for map`; the second snapshot the same |
| Kill | exit code 137 for `osd.0` |
| Go result | `ceph: request outcome unknown: EOF`, ended by the session; submission TID 1, global ID 4743, incarnation 21, epoch 1025, attempt 0, PG `3.1`, primary osd.0, shard 0 |
| Under the replacement | epoch 27, acting `[NONE,2,3,1]`, primary osd.3 on parity shard 2, pg_temp present; native user version 1, the seed's bytes, no xattr |
| Dials | the held request's OSD endpoint once, nothing else after the outcome |
| After the return | epoch 32, the recorded placement under osd.0's new address (same IP and port, another nonce), no pg_temp; native user version 1, the seed's bytes, mtime and xattrs |
| Returned primary's log | 566 bytes, head `25'1`, tail `0'0`, one entry: the seed at `25'1`; neither `client.4743.21:1` nor `client.4743.20:1` |
| Resubmission | TID 1, retry attempt 1, ONDISK, code 0, user version 2, replay version `32'2`; native bytes the seed plus one copy of the 21 appended bytes, user version 2, the new mtime and the xattr; the log then held `client.4743.21:1` once at `32'2` and the seed once |

After every run the Docker host had no container with the Testcontainers
label left.

### Neighbouring rows

The owned-pool oracle and the fixture's four-OSD selector list these rows
share grew. Each of the five rows passed in the first archive of `b058487`:

| Row | Tests, row |
| --- | --- |
| `...t-osderasureoptimizedk2m2inflight` | 35.99 s and 32.34 s, 124 s |
| `...t-osderasureoptimizedk2m2appliedresubmit` | 21.77 s, 78 s |
| `...t-osderasureoptimizedk2m2` | 43.66 s and 19.60 s, 119 s |
| `...t-osderasureoptimizedk2m2primaryhistorycalibration` | 9.74 s, 66 s |
| `...t-osderasureoptimizedk2m2partialwriteprimaryreplacementshardhistory` | 122.69 s, 179 s |

### Negative controls

Two native negative controls ran on remote copies of archives of `b058487`,
which were then discarded:

- Product mutation: an OSD request whose session failed returned the bare
  cause instead of `OutcomeUnknownError`. The test failed with `expected an
  unknown outcome without a server result EOF`, after the same hold.
- Scenario mutation: the request on the wire named the placement's own
  epoch while the expectations still named the held one. The primary
  executed it at once, and all twenty snapshots failed with `0 in-flight ops
  of client.4743.21 (want 1 of client.4743.21)`.

Both cleanups left no container with the Testcontainers label.

### Exploratory runs

Four exploratory runs on remote copies of the working tree, which were then
discarded, preceded the test.

- The first two added a step before the scenario: two nonprimary OSDs of
  another object's PG (`3.5`, acting `[2,3,0,1]`) were killed, which leaves
  two of four shards, below `min_size` 3. The first run stopped at once
  because the native placement reader refuses an acting set with two empty
  positions (`invalid native placement up set`). The second waited 25
  seconds instead. The Go client's connection to the primary osd.2 then
  received one PG-wide BLOCK for PG `3.5` shard 0 at epoch 28, ID 1, the
  APPEND stayed pending for eight seconds, and the kill of osd.2 ended it
  with `ceph: request outcome unknown: EOF`. That run then failed in its own
  restoration, which waits for every PG clean after the first of three OSDs
  starts. The drop on a PG below `min_size` is why the control holds the
  request in `waiting_for_map` instead.
- The third run of the control failed on all twenty snapshots with `op is
  "op must wait for map" with events [...]; want delayed`: the tracker
  reports a delayed op's detail as its flag point, which the check had not
  expected. The
  check was changed to that flag point, and the fourth run passed.

## Limits

- One placement of one PG on one profile: secure, aes256k client and service
  keys, IPv4, container runtime, replicated-pg-v8, four BlueStore OSDs, isa
  k=2,m=2 with optimizations. The held op is on shard 0, so no
  primary-first renumbering is involved in the held request.
- The hold is `waiting_for_map` for an epoch the cluster has not published.
  A client whose map is genuinely newer than a lagging primary's reaches the
  same queue; that lag itself was not produced. The other PG queues
  (`waiting_for_peered`, `waiting_for_active`, `waiting_for_readable`, the
  degraded and blocked object waits) and an op held inside the EC backend
  after `do_op` were not exercised. For this client a PG below `min_size`
  backs off and drops the op instead of holding it.
- The primary's OSD leaves by a forced kill. An orderly stop, an OSD marked
  down while it runs and a network partition were not run.
- Only the returned primary's log is read, at one epoch. No shard-local
  capture is taken; the held op never reached `do_op`, so no shard was sent
  anything for it.
- The resubmission goes to the returned primary after the PG is clean. A
  resubmission to the replacement primary, and one that the PG log answers
  as a duplicate, are covered by the in-flight and applied-resubmission
  controls, not here. No automatic replay is involved.

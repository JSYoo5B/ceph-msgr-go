# COPY_FROM and COPY_FROM2 across a destination primary change

Issue #7, checkpoint 3: an in-flight primary change and a lost reply, kept
separate from the post-ONDISK SIGKILL test and from the destination SIGKILL
while the source read was blocked ([earlier record](OSD_COPY_FROM_VALIDATION.md)).
Tests: `integration/osd_copyfrom_primary_change_test.go`. Contracts:
[COPY_FROM_CONTRACT.md](../../internal/osd/COPY_FROM_CONTRACT.md) and
[RESUBMIT_CONTRACT.md](../../internal/osd/RESUBMIT_CONTRACT.md).

Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
Each test covers COPY_FROM (flags 0) and COPY_FROM2 (flags 0) in one subtest
each, on three BlueStore replicas with two owned pools. The source is a fresh
head in pool 1 and the destination a fresh head in pool 2.

## Source basis

- [`PrimaryLogPG::on_change`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L13147)
  cancels every copy of a PG whose interval changed, with `requeue` set to
  `is_primary()`.
  [`CopyFromCallback::finish`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L344-L368)
  drops a copy cancelled with `-ECANCELED` without a reply, because a native
  Objecter would resend it. This client resends nothing. An OSD that is no
  longer primary therefore leaves the caller with an unanswered request.
- [`Objecter::target_should_be_paused`](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc#L2820-L2830)
  holds a READ op while the OSDMap carries `pauserd`.
  [`OSD::handle_osd_map`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L8215-L8217)
  shares each map with the OSD's own Objecter on receipt, which is the client
  the destination primary reads the source with (`_copy_some`). The MON sets
  `pauserd` only together with `pausewr`
  ([`osd set pause`](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/OSDMonitor.cc#L11995-L11996)).
- `objecter_requests` reaches that Objecter through the admin socket
  ([OSD.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L7379-L7393),
  [Objecter.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc#L4808-L4844)).
  A COPY_GET prints as `copy-get max <n> in=<n>b`; the input is the encoded
  cursor ([osd_types.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L7388-L7393)).
- A copy gives the destination the source's user version
  ([PrimaryLogPG.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L355-L356),
  [finish_copyfrom](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L10116)).
  That version can be lower than the destination's previous one.
- The primary is moved with `osd out`, as in the
  [RBD journal primary-change control](RBD_JOURNAL_PRIMARY_CHANGE_VALIDATION.md).
  The OSD keeps running and stays in the acting set as its last member under
  a pg_temp. Primary affinity is not used, because a non-default affinity
  requires a client feature the legacy-v6 client does not advertise.

## Lost reply, then a primary change

`TestCephOSDCopyFromLostReplyPrimaryChangeIntegration`, for each opcode:

1. One Go WRITEFULL and SETXATTR creates the source (write incarnation 31).
2. The copy (incarnation 33, unpinned source version) is sent to the
   destination primary through a relay that withholds every reply. The test
   waits until native readback shows the source's bytes on the destination,
   then cancels. The result is an `OutcomeUnknownError` with the transaction,
   global ID, incarnation and map epoch and no reply.
3. A second Go write replaces the source, so a second execution of the copy
   would be visible on the destination.
4. `osd out` of the destination primary. The test waits for two equal
   placements a second apart under another primary whose up set omits the
   former one.
5. One resubmission goes to the new primary on a new connection: the same
   operations and modification time, `Resubmit` set to the unknown outcome's
   submission, `RetryAttempt` 1 and the new placement.

Required: the original transaction, attempt 1, code 0, per-op result 0,
ONDISK, and the user version native readback gave the executed copy. Native
bytes, xattrs, user version and mtime stay those of the executed copy, so the
first source remains on the destination. The new primary's complete PG-log
snapshot holds the copy's identity once, as an ordinary modify entry of the
destination head at the reply's replay version. The first source write's
identity is the entry's first extra request ID with its user version, and
the second source write is not bound there. Near-miss identities do not
match. After recovery (below) the test waits until two placements a second
apart are the original placement again at a newer epoch: the original
primary, the same up and acting sets and no pg_temp. A clean PG still led by
the replacement fails the test instead of being read as the returned
primary. A new connection to the original primary then reads the same bytes
under a later transaction, native readback 2 s later is unchanged, and that
primary's PG log still holds the copy once with the same binding.

## A copy held before its source read

`TestCephOSDCopyFromInFlightPrimaryChangeIntegration`, for each opcode:

1. The source is created by one Go write, and the destination is seeded
   natively with other bytes and an xattr.
2. `osd set pause`. The native OSDMap must carry `pauserd` and `pausewr`, and
   the destination placement must be the original one at that epoch or later.
3. The copy is sent to the destination primary under that epoch, so the PG
   executes it only once the OSD, and so its Objecter, has the paused map.
4. **Phase barrier.** The test polls two bracketed native snapshots of the
   destination primary. Its op tracker (`dump_ops_in_flight`) must hold
   exactly one op of the copying client entity and incarnation, for the
   route's PG at the request's epoch. Its Objecter (`objecter_requests`) must
   hold exactly one op of the owned source, paused, with 0 attempts, made of
   a single `copy-get`. The Objecter counts every prepared send in `attempts`
   ([Objecter.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc#L3385-L3400))
   and also pauses an op it already sent when a later map calls for it
   ([Objecter.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osdc/Objecter.cc#L3081)),
   so a paused op with attempts 1 is a copy-get sent once and waiting to be
   sent again, and the barrier refuses it. The Go call must still be
   unanswered.
5. `osd out` of the destination primary. The test waits for the moved
   placement as above.
6. While the pause is still set, the test polls the same two snapshots of the
   former primary until neither holds anything of the copy. The barrier's
   copy-get had never been sent, and `pauserd` stayed set, so its
   disappearance is the cancellation and not the completion. The Go call stays unanswered for 2 s
   more and is then cancelled. The unknown outcome must carry the barrier
   op's transaction and the request's map epoch.
7. `osd unset pause`, confirmed natively. After 2 s the native destination
   must still be the seed (bytes, xattrs, user version, mtime) and the source
   unchanged. The new primary's PG log must not name the copy. This is an
   observation of that log at that epoch, not an absence proof.
8. One resubmission (`RetryAttempt` 1) goes to the new primary. It must
   execute: the original transaction, code 0, ONDISK, the first source's user
   version, and native bytes and xattrs of the first source. The new
   primary's PG log must hold the copy once with the source binding above.
9. A second Go write replaces the source, then a second resubmission
   (`RetryAttempt` 2) is sent. It must be answered from the log: the same
   transaction, user version and replay version, an unchanged native
   destination, and the copy still once in the log.
10. After recovery, the same wait for the original placement, fresh-session
    read, delayed native readback and returned-primary PG-log check as above.

## Recovery

One run-level recovery records each `osd out` and the pause before sending
them. It is registered on every subtest and on the test, so it runs on every
path. It first sends `osd unset pause` and `osd in` for every recorded OSD
and keeps going after a failed command. Then it checks the native OSDMap
flags once and waits once for `osd-ready clean`. If a command failed, it
reports every failure and skips the clean check. Both tests are
`//fixture:isolated`: the clean wait does not prove pg_temp absence, and the
owned-pool cleanup leaves `mon_allow_pool_delete`.

## Results

The runs used a Linux amd64 host with Docker 29.9.1. The source was this
change's test, tool and record tree on main `987e6e6`, run one row after
another on one Docker engine. Results were gated by
`tools/native_test_results.py`, which reported no problems for any row, and
each fixture's ten containers were terminated with none left over. Each
subtest is listed with its duration.

| Row (selector) | Lost reply COPY_FROM / COPY_FROM2 | Held copy COPY_FROM / COPY_FROM2 |
| --- | --- | --- |
| `osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprimarychange` | PASS 16.0 s / 15.4 s | PASS 21.4 s / 22.9 s |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprimarychange` | PASS 15.1 s / 14.9 s | PASS 23.9 s / 24.8 s |
| `osd.aes256k.ip4.osd-store-bluestore.osd-replicas-3` (legacy-v6) narrowed to the copy-from tests, see below | PASS 14.5 s / 14.1 s | PASS 22.5 s / 24.3 s |

The legacy-v6 run narrowed that broad row's selector to
`^TestCephOSD(CopyFrom|CopyFromCompound|CopyFromCrash|CopyFromDestinationCrash|CopyFromDestinationHistory|CopyFromLostReply|CopyFromSourceFailover|CrossPoolAndNamespaceCopyFrom|CopyFromLostReplyPrimaryChange|CopyFromInFlightPrimaryChange|ResubmitFailover)Integration$`.
All eleven selected tests passed on one fixture, so the existing copy-from
tests and the earlier primary-change resubmission test still pass with
these tests running beside them. The broad row itself was not run.

Observed values from the secure IPv6 row. The CRC row had the same OSDs and
versions at epochs two lower.

| Cell | Former → new primary | Resubmission reply | PG log |
| --- | --- | --- | --- |
| Lost reply, COPY_FROM | osd.1 → osd.2, epoch 29 | code 0, tid 2, attempt 1, user 1, replay 26'1, flags `0x400002c` | copy once on osd.2, with the first source write as extra ID; the same on the returned osd.1 at epoch 32 |
| Lost reply, COPY_FROM2 | osd.0 → osd.2, epoch 35 | code 0, tid 2, attempt 1, user 1, replay 32'1, flags `0x400002c` | the same on osd.2, then on the returned osd.0 at epoch 38 |
| Held copy, COPY_FROM | osd.1 → osd.2; barrier at epoch 47, dropped by epoch 50 | first: tid 2, user 1, replay 51'3, flags `0x400002d`; second: attempt 2, same user and replay version, flags `0x400002c` | no copy before the first resubmission, once after both; the same on the returned osd.1 at epoch 54 |
| Held copy, COPY_FROM2 | osd.1 → osd.0; barrier at epoch 55, dropped by epoch 58 | first: user 3, replay 59'3, flags `0x400002d`; second: same versions, flags `0x400002c` | the same, then on the returned osd.1 at epoch 62 |

In every barrier the op tracker showed the op at flag point `started`, with
the events `initiated`, `header_read`, `throttled`, `all_read`,
`dispatched`, `queued_for_pg`, `reached_pg` and `started`. The Objecter
showed one op, `copy-get max 8388608 in=29b`, paused, with 0 attempts. A
resubmission that executed was answered with ACK (`0x1`) among its flags.
Every answer from the PG log had ONDISK, RETRY and WRITE without ACK. These
flags are recorded as observed; the tests do not require them.

The first runs failed on test defects, all fixed in this change:

1. The first run did not select the replicated-PG client profile, so the MON
   refused admission.
2. The second used `osd set pauserd`, which the MON refuses with -22 because
   only `pause` exists.
3. The third parsed `copy-get` without its ` in=<n>b` suffix.
4. The fourth expected the executed copy's user version to be newer than
   that of the seeded destination. COPY_FROM gave the source's version, 1,
   against the seed's 2.
5. On legacy-v6 the lost-reply cell first armed its reply fault only on
   connections dialed while it was armed. The client then sent the copy on
   a connection to the same OSD that it already held, the fault never
   triggered, and the cell failed at its bound. Every OSD connection is now
   wrapped at dial and armed per OSD.

No product change was needed.

## Reproduction

```sh
(
  env_lines=$(python3 tools/scenario_plan.py env osd.aes256k.ip6.container.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprimarychange) &&
  eval "$env_lines" &&
  sh integration/run.sh
)
```

## Not claimed

- A daemon loss in either cell, or a copy held after its source read
  completed. Destination loss after source-read completion and staging
  remains open in #7.
- A barrier tied to `pausewr`. The copy-get is a read, and the pause affects
  only native Objecters; neither this client nor the oracle's observations
  consult it, and the oracle reads no object while it is set.
- Absence. The pre-resubmission PG-log observation in the held cell and the
  empty former-primary snapshots show what those daemons held at that
  moment.
- Erasure-coded destinations, pinned source versions, snapshot sources,
  compound copy/xattr vectors, the complete broad legacy-v6 three-replica
  rows, MemStore, and automatic routing or replay. The client never
  resubmits on its own.
- Exact-SHA hosted CI. These are worker-tree native runs, separate from CI
  on the pull request head.

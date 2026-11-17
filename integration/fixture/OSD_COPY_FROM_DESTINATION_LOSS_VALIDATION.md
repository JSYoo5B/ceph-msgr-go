# COPY_FROM and COPY_FROM2 with the destination primary lost after staging

Issue #7, checkpoint 3: the destination primary dies after the copy finished
reading its source and the primary committed locally, while at least one
replica commit was still outstanding. This is the interval left open by the
post-ONDISK SIGKILL test, by the
destination SIGKILL while the source read was blocked
([earlier record](OSD_COPY_FROM_VALIDATION.md)) and by the primary-change
cells, which move the primary without losing a daemon
([primary change](OSD_COPY_FROM_PRIMARY_CHANGE_VALIDATION.md)).
Test: `integration/osd_copyfrom_destination_loss_test.go`. Contracts:
[COPY_FROM_CONTRACT.md](../../internal/osd/COPY_FROM_CONTRACT.md) and
[RESUBMIT_CONTRACT.md](../../internal/osd/RESUBMIT_CONTRACT.md).

Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`, image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.
`TestCephOSDCopyFromStagedDestinationLossIntegration` runs four cells:
COPY_FROM (flags 0) and COPY_FROM2 (flags 0), each with one replica commit
before the loss (`replica-committed`) and with none (`primary-only`). It uses
three BlueStore replicas and two owned pools; the source is a fresh head in
pool 1 and the destination a natively seeded head in pool 2.

## Source basis

- [`CopyFromCallback::finish`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L344-L368)
  runs the client's op context again with `execute_ctx` once the source read
  is complete; `process_copy_chunk` fetches more while the cursor is
  incomplete. The final copy transaction is therefore issued under the
  client's own op. A temporary-object write of a multi-chunk copy is a
  separate op context without the client op.
- [`ReplicatedBackend::issue_op`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ReplicatedBackend.cc#L1134-L1187)
  marks that op `waiting for subops from <replicas>` (flag point
  `waiting for sub ops`) before it dispatches the individual repops.
  [`op_commit`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ReplicatedBackend.cc#L641-L664)
  marks `op_commit` at the primary's own commit and
  [`do_repop_reply`](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ReplicatedBackend.cc#L666-L721)
  marks `sub_op_commit_rec` at each replica's commit. The client is answered
  only once every acting replica committed, and a sent reply adds
  `commit_sent`.
- The replica set prints as a `set<pg_shard_t>`: ascending OSD ids separated
  by commas, a replicated shard as its OSD id
  ([osd_types.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L208-L215)).
- A copy gives the destination the source's user version and mtime
  ([PrimaryLogPG.cc](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L355-L356),
  `finish_copyfrom`).

## Procedure

For each cell:

1. One Go WRITEFULL and SETXATTR creates the source (write incarnation 31).
   The destination is seeded natively with other bytes and an xattr.
2. The replicas to pause are chosen from the destination's acting set. With
   one replica commit, one replica that is not the source's primary is
   paused, so the source read is still served. With none, both replicas are
   paused, and the source must have the destination primary as its primary
   too: on the `replicated-pg-v8` pools the test picks such a source name, and
   on the one-PG `legacy-v6` pools it skips that subtest when the two PGs have
   different primaries.
3. The replicas are paused with the fixture's daemon control
   (`docker pause`). The copy (incarnation 33, unpinned source version, mtime
   1700009100) is then sent to the destination primary.
4. **Phase barrier.** The test polls two bracketed native snapshots of the
   destination primary, as in the held-copy cell of the primary-change
   record. Its op tracker (`dump_ops_in_flight`) must hold exactly one op of
   the copying client entity and incarnation, for the route's PG at the
   request's epoch, at flag point `waiting for sub ops`, with exactly one
   `waiting for subops from <both replicas>`, then exactly one `op_commit`
   and exactly as many `sub_op_commit_rec` as replicas left running, and
   without `commit_sent`. Its Objecter (`objecter_requests`) must hold no op
   of the owned source. The Go call must still be unanswered. A paused
   replica cannot have committed: it was paused before the copy was sent.
5. The destination primary is SIGKILLed (exit 137 recorded by the fixture).
   The Go call must end as an `OutcomeUnknownError` carrying the barrier
   op's transaction, the client's global ID and incarnation and the
   request's map epoch, without a reply. The first freeze starts a 16 s pause budget.
   Freeze, snapshots, kill and the connection wait share the first 12 s;
   the remaining time is reserved for thaw. Exceeding either deadline fails
   the cell and leaves outstanding replicas to restart cleanup. The paused replicas are resumed, the
   time they were paused is recorded, and the dead OSD is marked down with
   `osd down`. The test waits for two equal placements a second apart under
   a new primary whose up set omits the dead one.
6. **Native result before any resubmission.** Native readback of the
   destination and the new primary's complete PG log decide the result:
   - the full copy: the first source's bytes and xattrs, its user version and
     its mtime, and the copy's identity once in the log as an ordinary modify
     entry of the destination head, with the first source write as its first
     extra request ID and that write's user version;
   - or the seed, unchanged in bytes, xattrs, user version and mtime, with
     neither identity in the log.

   Anything else fails the cell. With one replica commit only the full copy
   is accepted. The source must be unchanged.
7. A second Go write replaces the source, so a second execution would be
   visible. One resubmission goes to the new primary on a new connection:
   the same operations and mtime, `Resubmit` set to the unknown outcome's
   submission, `RetryAttempt` 1 and the new placement. It must return the
   original transaction, attempt 1, code 0 and ONDISK. If the copy survived
   it must be answered from the log (the first source's user version and the
   log entry's replay version) and leave the destination unchanged. If the
   seed was kept it must execute once and copy the second source. The log
   must then hold the copy once, bound to the source write that was copied,
   and must not bind the other write.
8. After the run-level recovery (below), the test waits until two placements
   a second apart are the original placement at a newer epoch, apart from the
   restarted OSD's address. A new connection to the original primary reads
   the same bytes under a later transaction, native readback 2 s later is
   unchanged, and that primary's PG log holds the copy once with the same
   binding.

## Recovery

The run-level recovery of the primary-change tests also covers these cells.
It records each pause and kill before sending it. It resumes, kills and
restarts any OSD still paused (one frozen past its heartbeat grace is not
thawed in place), starts every killed OSD, and keeps going after a failed
command. Then it checks the native OSDMap flags once, waits once for
`osd-ready clean`, and republishes the fixture's routing for the restarted
OSD. If a command failed it reports every failure and skips the clean check.
The test is `//fixture:isolated`: the clean wait does not prove pg_temp
absence, and the owned-pool cleanup leaves `mon_allow_pool_delete`.

The cell receipts (barrier op and events, raw snapshot digests, the unknown
outcome's identity and the class of its cause, never the error text,
native digests, log and replay versions, the resubmission reply including the signed per-operation result when observed)
are written with `testcluster.WritePublicEvidence` to
`osd-copyfrom-destination-loss-results.json`, which the fixture archives
with its diagnostics. A cell skipped at placement selection has no receipt.

## Results

The runs used a Linux amd64 host with Docker 29.9.1, one row after another
on one Docker engine. Results were gated by `tools/native_test_results.py`,
which reported no problems for any row, and no Testcontainers container was
left after cleanup. Each cell is listed with its duration.

| Row (selector) | COPY_FROM replica-committed / primary-only | COPY_FROM2 replica-committed / primary-only |
| --- | --- | --- |
| `osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprimarychange` | PASS 19.2 s / 28.1 s | PASS 18.9 s / 29.2 s |

Both dedicated replicated-PG-v8 rows subsequently passed all three selected
tests on `e3c1711`. The columns below give staged-loss subtest times in the
same opcode and replica-commit order as the first table.

| Configuration | COPY_FROM | COPY_FROM2 |
| --- | --- | --- |
| IPv4 CRC host | 19.84 s / 30.92 s | 19.24 s / 29.09 s |
| IPv6 secure container | 16.71 s / 23.00 s | 16.70 s / 31.49 s |

All eight cells observed the full copy under the new primary. Each explicit
resubmission returned the original PG log version. The earlier native
measurements below come from the first CRC IPv4 run.

On `e07cd85` the staged-loss test alone ran on the IPv4 secure legacy-v6
three-replica configuration. The replica-committed cases passed in 18.99 s
and 18.94 s. Both primary-only cases skipped at placement selection because
the source and destination pools had different primaries. The public result
contained exactly the two executed cells.


| Cell | Source primary; destination primary (killed); paused | Barrier epoch; events after `started` | Paused for | New primary, epoch | Native result before resubmission | Resubmission reply | Returned primary |
| --- | --- | --- | --- | --- | --- | --- | --- |
| COPY_FROM, replica-committed | osd.1; osd.1; osd.2 | 23; `waiting for subops from 0,2`, `op_commit`, `sub_op_commit_rec` | 0.91 s | osd.2, 25 | copy, user 3, log 23'3 | tid 2, attempt 1, user 3, replay 23'3, flags `0x400002c` | osd.1, epoch 28 |
| COPY_FROM, primary-only | osd.0; osd.0; osd.1, osd.2 | 28; `waiting for subops from 1,2`, `op_commit` | 1.51 s | osd.1, 30 | copy, user 1, log 28'3 | tid 2, attempt 1, user 1, replay 28'3, flags `0x400002c` | osd.0, epoch 33 |
| COPY_FROM2, replica-committed | osd.1; osd.0; osd.2 | 33; `waiting for subops from 1,2`, `sub_op_commit_rec`, `op_commit` | 0.90 s | osd.1, 35 | copy, user 1, log 33'3 | tid 2, attempt 1, user 1, replay 33'3, flags `0x400002c` | osd.0, epoch 38 |
| COPY_FROM2, primary-only | osd.0; osd.0; osd.1, osd.2 | 38; `waiting for subops from 1,2`, `op_commit` | 1.10 s | osd.1, 40 | copy, user 1, log 38'6 | tid 2, attempt 1, user 1, replay 38'6, flags `0x400002c` | osd.0, epoch 43 |

Every barrier showed the op tracker events `initiated`, `header_read`,
`throttled`, `all_read`, `dispatched`, `queued_for_pg`, `reached_pg` and
`started` before those listed, and an Objecter with no op of the source.
Every unknown outcome ended at the kill with `EOF` and carried the barrier's
transaction. A replica commit can be recorded before the primary's own
commit, as in the COPY_FROM2 replica-committed cell; the test accepts either
order. In both primary-only cells the copy survived: the new primary, chosen
while the killed OSD was down, held the full copy and its log entry, establishing survival after recovery. The primary records its
sub-op event before dispatching individual sends. These observations do
not timestamp delivery to either replica or intervening map changes. Every
resubmission was answered from the log with ONDISK, RETRY and WRITE without
ACK, and the returned original primary held the copy once. These flags are
recorded as observed; the test does not require them.

No product change was needed.

## Reproduction

```sh
(
  env_lines=$(python3 tools/scenario_plan.py env osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-3.osd-profile-replicated-pg-v8.t-osdcopyfromprimarychange) &&
  eval "$env_lines" &&
  sh integration/run.sh
)
```

## Not claimed

- The loss of a transaction staged on the primary alone. The kept-seed outcome
  has unit coverage and was never observed live.
- Which paused replica applied the copy, or when. The test observes the new
  primary's object and log after the loss, not the replicas' processing.
- Multi-chunk copies and their temporary objects. Every cell copies one
  chunk, so its staging is the final transaction only.
- OMAP and the OMAP header, which these cells do not seed or observe.
- Absence. An empty Objecter snapshot shows what the primary held at that
  moment; the source read's completion follows from the op having reached
  its repops.
- Host power loss or device durability. This is daemon termination.
- Erasure-coded destinations, pinned source versions, snapshot sources,
  compound copy/xattr vectors, MemStore, the complete broad legacy-v6
  three-replica rows, and automatic routing or replay. The client never
  resubmits on its own.
- Legacy-v6 primary-only execution and rows not listed under Results.
- Exact-SHA hosted CI. These are worker-tree native runs, separate from CI
  on the pull request head.

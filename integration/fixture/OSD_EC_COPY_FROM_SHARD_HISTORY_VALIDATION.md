# COPY_FROM2 into optimized EC: PG-log control on every acting shard

This is a test-only positive control for issue #9. It extends two earlier
controls: the [primary PG-history snapshot](OSD_PG_HISTORY_CALIBRATION_VALIDATION.md),
which reads only the primary, and the
[COPY_FROM2 destination control](OSD_COPY_FROM_HISTORY_VALIDATION.md), which
supplies a native `extra_reqids` position on a replicated pool. It answers
one narrow question: after one acknowledged copy into an optimized
erasure-coded pool, what does each acting shard's own PG log hold for the
copy and for the source write it carries? It adds no product API, opcode or
flag, injects no fault and changes no daemon configuration. It never states
that an identity is absent, evicted or trimmed, and it resubmits nothing.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

- The live `pg <pgid> log` command resolves the PG's primary shard and
  answers only there ([OSD dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSD.cc#L2775-L2825)).
  Sending it to four OSDs cannot show four histories.
- The primary of an EC write walks the acting, recovery and backfill shards
  and builds one sub-write per shard, which carries the operation's log
  entries. It may leave a shard out before building that sub-write, when the
  shard's transaction is empty
  ([dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L812-L889),
  [predicate](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECBackend.cc#L919-L928)).
  A receiver passes the entries to its own log
  ([local log application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECBackend.cc#L424-L443)).
- Creating an object adds a create to every shard's transaction and marks
  all shards written; a size change marks all shards written as well
  ([object creation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L390-L407),
  [size change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L597-L600)).
  A copy into a fresh head therefore has no shard with an empty transaction.
  An entry that names every shard is dumped with an empty `written_shards`
  set ([encoding](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L873-L886)).
- The offline reader selects the one collection of the exact `spg_t` it is
  given and decodes that collection's own log and missing set
  ([collection selection](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/ceph_objectstore_tool.cc#L4410-L4448),
  [shard-local reader](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/ceph_objectstore_tool.cc#L439-L480)).
  Its `info` and `log` operations mount the store read-write
  ([mount selection](https://github.com/ceph/ceph/blob/v20.2.4/src/tools/ceph_objectstore_tool.cc#L3992-L4011)),
  so they run only on private copies.
- How the source write's identity reaches the destination entry is in the
  [replicated control's source basis](OSD_COPY_FROM_HISTORY_VALIDATION.md#source-basis).

No Ceph source is incorporated into the product.

## Capture

The capture has three parts. None of them is retried.

1. **First bracket.** `tools/osd_pool_oracle.py` mode `shardhistory`, phase
   `before`, reads the native placement of the destination object and stores
   the complete record in the control directory.
2. **Copies.** The fixture control `capture-osd-shard-history`
   (`integration/fixture/shard_history.go`) takes the PG and the ordered
   acting vector from that stored record. It takes no argument. Every acting
   member must be a distinct owned OSD. For each index `i`, in order:
   - start a fresh reader container from the pinned image with network mode
     `none`, and require Docker to report that mode;
   - pause the container of `acting[i]`, stream its data directory
     (`/var/lib/ceph/osd/ceph-<id>`) through the Docker archive API into the
     reader, and resume the container whether or not the copy succeeded. The
     copy has a 12 s budget, below the 20 s default heartbeat grace, and the
     archive stream is bounded at 4 GiB;
   - read the copy's `whoami` and refuse any value other than `<id>`;
   - refuse a `block` that is not a regular file and any `block.db` or
     `block.wal`;
   - run `ceph-objectstore-tool --no-mon-config --data-path /shard/ceph-<id>
     --type bluestore --pgid <pgid>s<i> --format json --op info`, then
     `--op log`, each bounded at 64 KiB of output, with standard error kept
     apart. The reader holds an empty local configuration file, so the tool
     runs on its defaults and prints no configuration diagnostics;
   - remove the reader, which holds the only copy, and require Docker to
     report it gone. This also happens after any failure.

   The capture stops at the first shard that fails and reports the failure
   to the test. The pause uses the provider's pause lease, which the fixture
   also resumes during termination.
3. **Second bracket and judgement.** Phase `after` reads the placement again
   and passes both records, the capture record and the dumps to
   `tools/osd_shard_history_oracle.py`. One stored bracket admits one
   judgement.

The fixture admits the capture only for the selector
`^TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration$` and for the
selector of the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md),
each on the four-BlueStore topology, and tells the test through
`CEPH_MSGR_NATIVE_SHARD_HISTORY=1`. Any other selection refuses the control.

### Completeness

One shard is complete only if all of the following hold:

- the capture record is for that acting index, OSD and `<pgid>s<i>`, with
  the reader isolated, the OSD paused and resumed, the reader removed, the
  copied `whoami` equal to the OSD ID and no unsupported block layout;
- both offline operations exited 0 and wrote nothing to standard error;
- each dump is at most 64 KiB, has the length the capture recorded and is
  one strict UTF-8 JSON document without duplicate keys;
- the `info` dump names exactly `<pgid>s<i>` and shard `i`, and its
  `partial_writes_last_complete` records parse;
- the `log` dump is exactly `pg_log_t` plus `pg_missing_t`, and its log has
  the shape and ordering rules of the primary snapshot parser (versions
  increase inside (tail, head], the last entry is head, every request
  identity parses, no duplicate is newer than tail), and every entry's
  `written_shards` is a parsable shard set;
- the log runs from the info's `log_tail` to its `last_update`,
  `last_complete` equals `last_update`, and the missing set is empty.

A refused or unattributed copy is not read. The receipt is complete only if
every index of the acting vector has a complete shard, the capture was taken
under this first bracket (its SHA-256 is recorded), and the two placement
records are identical: epoch, PG, up and acting order, primary, target,
shard, pg_temp and address. It lists, per shard and identity, every position
in the ordinary log, in `extra_reqids` and in `dups`, with the raw log dump,
its length and SHA-256. It always states `absence_established: false`.

## Scenario

`TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration` runs on the
four-BlueStore fixture (`osd-replicas-4`, `replicated-pg-v8`, secure,
aes256k, IPv4, container), the profile of the primary calibration. It owns a
replicated pool (slot 1, the source) and an optimized isa k=2,m=2 pool with
overwrites (slot 2, the destination), each with eight PGs. Both heads must
be absent first.

1. The Go client sends one source write (WRITEFULL plus SETXATTR, client
   incarnation 7) to the source primary and one COPY_FROM2 pinned to that
   write's user version (incarnation 9) to the destination primary. Each
   reply must carry ONDISK and keep its transaction, GID, incarnation,
   object, pool and attempt 0. A failed or unknown request stops the test;
   nothing is resent.
2. Native readback: the destination holds the source's bytes and xattr at
   the copy reply's user version, and the source did not change.
3. The destination's live primary, through the bracketed `pghistory`
   snapshot and `copyFromDestinationBinding`: the copy's full identity is
   exactly one ordinary `modify` entry with return code 0 for the
   destination head at the copy's replay version, and the source write's
   full identity is `extra_reqids[0]` of that same entry with the source user
   version. The snapshot holds one extra request ID in total.
4. The capture above, with no further subject request. The map epoch must
   not have moved since the primary snapshot. The Go test rechecks each
   dump's length and digest and counts every exact quoted identity in the
   raw dump independently of the Python parser (`checkShardHistory`).
5. For each shard, `copyFromShardObservation` reports one of three
   observations: parent entry missing; parent entry present with the extra
   request ID missing; both present. Any other shape (the parent twice, in
   the duplicate tier, failed, on another object or version; the source
   identity without the parent, on another entry, not first, or with another
   user version) is refused as a contradiction of the one acknowledged copy.

Six near-miss identities must have no position on the primary and on every
shard: each request's real TID under the other request's incarnation, the
source write's key with the next incarnation, the next entity number, entity
type `osd`, and a TID that was never used.

### What is required and what is only recorded

| Statement | Status |
| --- | --- |
| Copy acknowledged ONDISK, native bytes and xattr equal the source | required |
| Live primary binds both identities | required |
| Receipt complete, identical brackets, every acting index attributed | required |
| No near miss on the primary or on any shard | required |
| Every shard shows the parent entry with the nested source identity and its user version, one extra request ID, no duplicate | required for this copy, which creates the head on every shard |
| The primary's store copy repeats its live answer (head, tail, counts, every position) | required |
| Pause duration, archive size, dump digest per shard | recorded in the test log |

The fourth row from the bottom is a statement about a fresh-head copy only.
A partial overwrite of an existing object may give a shard an empty
transaction; this control does not exercise it. The
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md)
does.

## Cluster-free checks

- `tools/test_osd_shard_history_oracle.py` uses synthetic dumps laid out like
  the offline reader's output; no daemon produced them. It covers both
  linked identities on four shards, a shard without the entry and a shard
  without the nested identity (both complete, neither an absence claim),
  identities in the duplicate tier, ten changed placement records, a capture
  under another bracket or for another PG or acting order, twenty
  unattributed or refused shard records (another OSD's `whoami`, wrong
  shard collection, index or OSD, a reader with a network, an OSD not
  resumed, a copy not removed, a separate DB device, a refused or
  diagnostic-writing tool run), twenty-nine truncated, reshaped or
  inconsistent dumps, ten near-miss identities and the mode's request
  validation. A refused shard's dumps are never opened.
- `integration/fixture/shard_history_test.go` drives the capture with a fake
  Docker engine: the step order, the resume and the removal on twelve
  failure paths, the stored-bracket plan (fifteen refused records), the stop
  at the first failed shard and the selector admission.
- `TestCheckShardHistory` and `TestCopyFromShardObservation` in
  `integration/osd_erasure_copyfrom_history_test.go` pin the Go checks: one
  accepted receipt and forty-eight refused ones (moved or mismatched
  brackets, swapped or missing shards, wrong `whoami`, raw bytes that differ
  from their digest, a parser that missed or invented a position), the three
  observations and twenty contradictory histories.

Mutation checks ran in scratch copies that were then restored: 55 mutants of
the Python judgement and mode, 46 of the two Go checks and 24 of the fixture
capture. The tests kill all of them. In a first round two Python mutants
and nine Go mutants survived, and cases were added for each. A twenty-fifth
fixture mutant removed a bound that the ownership check already implied;
that redundant bound was deleted instead.

## Reproduction

On an authorized Linux runner, hold its run lock, use the pinned image and:

```sh
CEPH_MSGR_TEST_OSD=1 \
CEPH_MSGR_TEST_OSD_STORE=bluestore \
CEPH_MSGR_TEST_OSD_REPLICAS=4 \
CEPH_MSGR_TEST_OSD_PROFILE=replicated-pg-v8 \
CEPH_MSGR_TEST_RUNTIME=container \
CEPH_MSGR_TEST_KEY_TYPE=aes256k \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
CEPH_MSGR_TEST_CONNECTION_MODE=secure \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration$' \
go -C integration/fixture run . run -project ../..
```

The dedicated mandatory row is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2copyfromhistory`.
Request it with the CI comment command. Each reader holds a 1 GiB copy of one
OSD's block file in its own filesystem while it exists; the copies are taken
one after another, so the Docker host needs about 1 GiB of free space beyond
the fixture. Basic PR CI builds and unit-checks the fixture but does not run
this daemon test. The native result of a pull-request head is recorded in
that pull request with the tested SHA. A pass at one SHA is not evidence for
a later revision.

## Local native calibration

These runs preceded the pull request and are not exact-SHA CI receipts. They
used the catalog row's environment and the pinned image on a Linux amd64
host with Docker 29.9.1 and Go 1.26.9, through `integration/run.sh` and the
native result gate, with one test process on the engine.

The recorded run used an archive of test commit `1114713` (later commits of
the same pull request change documents only) and was the first run of that
commit. The gate recorded
`TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration` as PASS with no
problem; the test took 64.74 s and the row 129 s.

| Observation | Value |
| --- | --- |
| Source write | `client.4722.7:1`, user version 1 |
| Copy | `client.4722.9:2`, replay version `28'3` |
| Destination placement, both brackets | epoch 28, PG `4.2`, up = acting = `[1,3,2,0]`, primary osd.1 on shard 0 |
| Live primary (osd.1, epoch 28) | 1,568 bytes, head `28'3`, tail `0'0`, 3 log entries, 1 extra request ID, 0 duplicates; `client.4722.9:2` is the `modify` entry of `4:53c99ff5:::ec-copy-history-destination:head` at `28'3`, and that entry's `extra_reqids` is `[{"reqid":"client.4722.7:1","user_version":"1"}]` |
| Near misses | no position on the primary or in any shard copy |

| Store copy | Selected and returned SPG | Paused | Parent entry | `extra_reqids[0]` of that entry | Extra / dup records | Missing set |
| --- | --- | --- | --- | --- | --- | --- |
| osd.1 (primary) | `4.2s0` | 836 ms | `client.4722.9:2` at `28'3` | `client.4722.7:1`, user version 1 | 1 / 0 | empty |
| osd.3 | `4.2s1` | 805 ms | `client.4722.9:2` at `28'3` | `client.4722.7:1`, user version 1 | 1 / 0 | empty |
| osd.2 | `4.2s2` | 778 ms | `client.4722.9:2` at `28'3` | `client.4722.7:1`, user version 1 | 1 / 0 | empty |
| osd.0 | `4.2s3` | 784 ms | `client.4722.9:2` at `28'3` | `client.4722.7:1`, user version 1 | 1 / 0 | empty |

Each copy reported `whoami` equal to its OSD ID, head = last_update =
last_complete = `28'3` and tail `0'0`. Each offline log dump was 1,625 bytes
with SHA-256
`f8f7408a02ff36aac9bec7e00943acdc7cd26d09292c20d16188f470a24a7c51`; the four
dumps were byte-identical. Each archive stream was 1,073,762,304 bytes: one
1 GiB block file plus the small files of the data directory. The two other
entries of the PG are the owned-pool oracle's own probe write and delete
from pool creation; they carry another client identity and no extra request
ID. The copy's entry was dumped with `"written_shards":"{}"`, a `CREATE`
rollback operation and `mtime` `0.000000`; these are recorded as observed
and are not asserted.

The same archive ran the two neighbouring rows afterwards, each in its own
fixture: the primary calibration
(`...t-osderasureoptimizedk2m2primaryhistorycalibration`, PASS in 9.84 s) and
the replicated COPY_FROM2 control
(`osd.aes256k.ip6...t-osdcopyfromdestinationhistory`, PASS in 10.84 s).

Two earlier runs of the working tree, before the test commit existed, are
part of the record:

- The first run failed in the judgement, not in the copy. All four copies
  were taken and read, and both tool runs exited 0, but each wrote
  `did not load config file, using default settings` and
  `can't open ceph.conf` to standard error, which the judgement refuses as
  unexplained diagnostics. The cause was the reader, which had no local
  configuration file. The reader now creates an empty one. The live primary
  of that run bound both identities in the same way, at epoch 27.
- The second run, with that change, passed with the same per-shard result
  at epoch 28 (pauses of 796, 805, 778 and 790 ms).

One native negative control ran on a remote copy of the working tree that
was then discarded: the fixture asked the offline reader for collection
`4.2s0` on the store of acting index 1. The reader exited 1 with
`PG '4.2s0' not found`, the capture stopped at that shard and the test
failed with that text. The OSD had been resumed and the reader removed.

After every run the Docker host had no container with the Testcontainers
label, no reader container, and no labelled volume or network left; the
store copies lived only inside the removed readers.

## Limits

- This is a positive control. A shard whose copy lacks a position would be
  an observation of that copy at that bracketed placement only. It would not
  establish that the identity is absent from the PG, was evicted or trimmed,
  or would be replayed. No absence was observed and none is claimed.
- The copies are sequential captures of paused containers, each recovered
  and mounted privately. They are not one atomic snapshot across the four
  stores, and pausing a container does not drain device I/O already in
  flight. Identical brackets fix placement and epoch, not store contents. No
  subject request was sent between the copy's acknowledgement and the last
  dump.
- Head and tail checks cannot detect an omitted interior entry or duplicate.
  The independent count of quoted identities covers only the requested keys.
- Only the ordinary log and one `extra_reqids` position have a native
  positive control on every shard. No native request was moved into `dups`:
  nothing was trimmed and no retention setting was changed. The
  duplicate-tier lookup is pinned only by synthetic tests.
- One fresh head with one source write was copied, so every shard was
  written. More than one transferred identity, a source with existing extra
  request IDs and the ten-identity cap were not exercised. A shard left out
  of a partial write is the subject of a
  [separate control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md).
- The reader supports one layout: a regular `block` file in the OSD data
  directory without a separate DB or WAL. Anything else is refused.
- No replay or deduplication behaviour follows from a match. The proposed
  history-eviction resubmission cell still needs a complete absence
  judgement for every acting member at the intended PG state, which this
  control does not provide.
- One profile ran: secure, aes256k client and service keys, IPv4, container
  runtime, replicated-pg-v8, four BlueStore OSDs, isa k=2,m=2 with
  optimizations. Other widths, plugins, a degraded or remapped PG, a PG with
  a pg_temp, COPY_FROM (the older opcode) and an erasure-coded source are not
  part of this control.

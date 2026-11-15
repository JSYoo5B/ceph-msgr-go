# Partial overwrite of an optimized EC object: PG-log control on every acting shard

This is a test-only control for issue #9. It reuses the
[shard-local capture](OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md) of the
COPY_FROM2 control, which showed an entry on every shard because that copy
created its head, and exercises the case that control left open: a shard
left out of a partial write. It answers one narrow question: after one
acknowledged full write and one acknowledged overwrite confined to a single
data chunk, what does each acting shard's own PG log hold for the two
requests? It adds no product API, opcode or flag, injects no fault and
changes no daemon configuration. It never states that an identity was
evicted, trimmed or lost, and it resubmits nothing.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

- The primary of an EC write walks the acting, recovery and backfill shards
  and builds one sub-write per shard, which carries the operation's log
  entries. Before it builds the sub-write it asks the operation whether the
  shard can be skipped, and continues with the next shard if so
  ([dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L812-L889)).
  With optimizations a write can be skipped exactly when the shard's
  transaction is empty
  ([predicate](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECBackend.cc#L919-L928)).
  A shard that is being recovered or backfilled, and the first write after
  an ordered callback, are never skipped. Every sub-write that is built
  counts toward the commits the operation waits for.
- Which shards have a transaction: a create, a size change, a cleared
  whiteout and the first write of an interval name every shard. Any other
  write names the parity shards and shard 0
  ([choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L597-L603)),
  plus every shard of its write plan
  ([plan shards](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L812-L817)).
  Attribute updates, which every write has for the object info, go only to
  shards that are not nonprimary
  ([attributes](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L948-L993)).
  A logical offset belongs to raw shard `(offset / chunk_size) % k`, with
  `chunk_size = stripe_width / k`
  ([stripe geometry](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECUtil.h#L632-L636)).
  An overwrite inside shard 0's chunk therefore names shard 0 and the parity
  shards, and a nonprimary data shard gets no transaction and no sub-write.
  An overwrite that touches a nonprimary data shard's chunk names that shard
  as well.
- The entry records the shards it names in `written_shards`; a set of all
  shards is stored as the empty set
  ([encoding](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L873-L886)).
  The log dump prints the set as `{}` or `{0,2,3}`
  ([entry dump](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L5142),
  [set print form](https://github.com/ceph/ceph/blob/v20.2.4/src/common/bitset_set.h#L424-L436)).
- When a shard that can be primary rolls an entry forward, it records each
  nonprimary shard the entry does not name in its own info as
  `partial_writes_last_complete`, a range from the previous version to the
  entry's version. A nonprimary shard records nothing
  ([record](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGBackend.cc#L414-L496)).
  The roll-forward is driven by a later sub-write: when the write cache is
  idle the primary submits an operation without a transaction for it, to
  the shards the previous write named
  ([roll-forward](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L955-L993),
  [its shards](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L918-L940)).
  The client's reply does not wait for it.
- The info dump prints the collection's `pgid`, its `shard`, the log bounds
  and those records
  ([info dump](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L3693-L3712)).
- The capture and the offline reader are described in the
  [COPY_FROM2 control's source basis](OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md#source-basis).

No Ceph source is incorporated into the product.

## Capture and receipt

The capture is the one of the COPY_FROM2 control, unchanged: two identical
native placement records around a sequential copy of every acting OSD's
store, each read offline in a reader without a network. The fixture now
admits it for two selectors, that control's and
`^TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration$`, each
on the four-BlueStore topology only.

`tools/osd_shard_history_oracle.py` reports three more things per shard, and
a shard is incomplete if any of them does not parse:

- the shard ID the info dump names, which must be the acting index the
  capture selected (`<pgid>s<i>`);
- the version and `written_shards` of every ordinary log entry. The text
  must be the pinned ascending set print form. The empty set is reported as
  an empty list and means every shard;
- the info's `partial_writes_last_complete` records (shard ID, from, to, in
  ascending shard order with `from` not after `to`) and their epoch.

The receipt also carries the raw info dump with its length and SHA-256. The
other completeness rules are unchanged: among them, each shard's log must
run from its info's `log_tail` to its `last_update`, `last_complete` must
equal `last_update`, and the missing set must be empty. A shard without the
overwrite's entry satisfies them with an older `last_update`.

## Scenario

`TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration` runs on
the four-BlueStore fixture (`osd-replicas-4`, `replicated-pg-v8`, secure,
aes256k, IPv4, container). It owns one optimized isa k=2,m=2 pool with
overwrites (slot 2) with eight PGs. The head must be absent first.

1. Geometry, from native sources only. The stripe width is read from the Go
   client's authenticated MON map dump. The oracle's profile readback must
   be isa with k=2 and m=2 and carry no `mapping`, so shard `i` holds raw
   chunk `i`. The pool's `nonprimary_shards`, compared between the oracle
   and the Go client's map, give the shards that cannot be primary.
   `partialWritePlan` restates the source rule above for the planned
   overwrite and refuses a pool whose nonprimary shards are not the data
   shards 1..k-1. The control requires the plan to leave out exactly the
   nonprimary shards.
2. The Go client sends one WRITEFULL of two stripes (client incarnation 7)
   and one WRITE of 14 bytes at offset 5 (incarnation 9) to the primary.
   Each reply must carry ONDISK and keep its transaction, GID, incarnation,
   object, pool and attempt 0. A failed or unknown request stops the test;
   nothing is resent.
3. Native readback after each write: the object holds exactly the expected
   bytes at the reply's user version.
4. The live primary, through the bracketed `pghistory` snapshot: each
   request's full identity is exactly one ordinary successful `modify` entry
   of the head at its reply's replay version, and the log ends at the
   overwrite.
5. The capture, with no further subject request. The map epoch must not have
   moved since the primary snapshot. `checkShardHistory` rechecks each log
   dump as before. `checkShardDumps` rechecks the info dump's length and
   digest and reads both raw dumps in Go, independently of the Python
   parser: the info's `pgid`, `shard` and partial-write records and every
   entry's version and `written_shards` text must equal the receipt.
6. For each shard, `partialWriteShardObservation` reports one of three
   observations: the overwrite's entry is not held and the log ends at the
   full write; the entry is held without a partial-write record; the entry
   is held with the record. Any other shape is refused as a contradiction of
   the two acknowledged writes: the full write's entry missing, repeated,
   failed, elsewhere or not naming every shard; the overwrite's entry
   repeated, failed, elsewhere, not directly after the full write, not last,
   or naming another shard set; a log that ends anywhere else; a record on a
   shard without the entry; a record for another shard or another range.

A shard's role is taken from its shard ID, the native width and the native
nonprimary shards. The acting index equals the shard ID because the capture
selected, and the store returned, collection `<pgid>s<i>` whose info names
shard `i`. The OSD's position among the wire targets is not used.

Four near-miss identities must have no position on the primary and on every
shard: each request's real TID under the other request's incarnation, the
overwrite's key with the next entity number, and entity type `osd`.

### What is required and what is only recorded

| Statement | Status |
| --- | --- |
| Both writes acknowledged ONDISK, native bytes and user version equal after each | required |
| Live primary holds both identities as ordinary entries, log ends at the overwrite | required |
| Receipt complete, identical brackets, every acting index attributed, info names the index as its shard | required |
| No near miss on the primary or on any shard | required |
| Every shard holds the full write's entry, and it names every shard | required: the write creates the head on every shard |
| Shard 0 and both parity shards hold the overwrite's entry directly after the full write as their last entry, naming exactly shards 0, 2 and 3 | required: the primary builds a sub-write for each named shard and the reply waits for all of them |
| The nonprimary data shard (shard 1) does not hold the overwrite's entry in this capture, and its own log and `last_update` end at the full write | required: the shard has no transaction, is not being recovered and gets no sub-write |
| A shard without the entry holds no partial-write record | required: only a shard that can be primary records one |
| A shard with the entry holds the record "shard 1, from the full write to the overwrite" | recorded; if a record is there it must be exactly that one |
| The primary's store copy repeats its live answer (head, tail, counts, every position) | required |
| Pause duration, archive size, dump digests per shard | recorded in the test log |

The record is not required because it is written by the roll-forward, which
no reply waits for. It was present on all three shards in every run below.

The statement about shard 1 is an observation of that shard's own log at
capture time, between two identical placement records and with no peering
in between. It is not a statement that the
entry was evicted or lost, or that the PG lacks it: the other three shards
hold it, and the primary's record states which versions shard 1 was left
out of.

## Cluster-free checks

- `tools/test_osd_shard_history_oracle.py` gains two tests with synthetic
  dumps. One lays out this scenario: three shards with both entries and the
  record, one shard whose log ends at the full write. The other refuses
  seventeen unparsable `written_shards` values (among them an unterminated,
  descending, repeated or out-of-range set, an array, an object and a
  missing key), five unusable shard IDs, eighteen malformed partial-write
  arrays, epochs and records, and an info that names another shard than the
  selected index.
- `TestPartialWritePlan` pins the restated plan on eleven overwrites
  (inside, at the edge of and across chunks and stripes, k=4, m=1) and
  refuses thirteen inputs it does not cover, among them an append, a write
  that grows the object and unexpected nonprimary shards. It also pins the
  role names and the stripe-width lookup.
- `TestPartialWriteShardObservation` pins the three observations, an
  overwrite that names every shard, and thirty-six contradictory histories.
- `TestCheckShardDumps` accepts three consistent receipts and refuses
  thirty-two that differ from their dumps.
- `TestShardHistoryCaptureAdmission` in the fixture pins the two selectors,
  requires the marker for each and refuses a selection of both.

A mutation check ran in a scratch copy that was then discarded: 86 mutants
of the plan, the observation, the dump check and the new oracle parsing.
Six survived a first round (the stripe wrap of the shard index, two entry
count comparisons, the adjacency of the two entries, and two forms of the
`written_shards` match); cases were added for each and all 86 are killed.

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
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration$' \
go -C integration/fixture run . run -project ../..
```

The dedicated mandatory row is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwriteshardhistory`.
Request it with the CI comment command. The disk requirement is that of the
COPY_FROM2 control: about 1 GiB of free space beyond the fixture. Basic PR CI
builds and unit-checks the fixture but does not run this daemon test. The
native result of a pull-request head is recorded in that pull request with
the tested SHA. A pass at one SHA is not evidence for a later revision.

## Local native calibration

These runs preceded the pull-request update and are not exact-SHA CI
receipts. They used the catalog row's environment and the pinned image on a
Linux amd64 host with Docker 29.9.1 and Go 1.26.9, through
`integration/run.sh` and the native result gate, with one test process on
the engine.

The recorded runs used archives of two commits. The first is test commit
`ef52774`. Its first run was the first run of that commit. The same archive
then ran the three neighbouring rows and this row twice more, each in its
own fixture. The gate recorded
`TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration` as PASS
with no problem all three times: 59.95 s, 59.85 s and 59.95 s for the test
and 125 s, 116 s and 116 s for the row.

Two later commits changed the capture and its judgement: a shard's dumps are
read only under the file name of its acting index and operation, and a
capture fails when the collection of a tool's standard error exits nonzero.
An archive of commit `553bc79`, which contains both (later commits of the
same pull request change documents only), ran this row and the COPY_FROM2
shard-history row once each, as the first run of that commit. Both passed:
this test in 59.95 s (row 125 s) and the COPY_FROM2 control in 64.75 s (row
121 s, all four shard copies of PG `4.2` holding both linked identities at
`28'3`).

All four runs of this test observed the same placement and the same
per-shard result. The fixture's topology is deterministic, so they are
repetitions of one placement, not different ones.

| Observation | Value |
| --- | --- |
| Geometry | stripe_width 8192, chunk 4096, profile k=2, m=2, plugin isa, no mapping, `nonprimary_shards` `{1}` |
| Object | 16,384 bytes (two stripes); overwrite of 14 bytes at offset 5 |
| Restated plan | names shards 0, 2 and 3, leaves out shard 1 |
| Full write | `client.4716.7:1` (first run; `client.4722.7:1` in the other three), replay version `25'1`, user version 1 |
| Overwrite | `client.4716.9:2` (`client.4722.9:2`), replay version `25'2`, user version 2 |
| Placement, both brackets | epoch 25, PG `3.7`, up = acting = `[3,2,1,0]`, primary osd.3 on shard 0 |
| Live primary (osd.3, epoch 25) | 1,242 bytes, head `25'2`, tail `0'0`, 2 log entries, 0 duplicates; the full write is the `modify` entry of `3:e65c9b8f:::ec-partial-write-history:head` at `25'1` with `"written_shards":"{}"`, the overwrite the `modify` entry at `25'2` with `"written_shards":"{0,2,3}"` and prior version `25'1` |
| Near misses | no position on the primary or in any shard copy |

| Store copy | Info `pgid` / `shard` | Role | Full write `25'1` | Overwrite `25'2` | Log head = `last_update` = `last_complete` | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | `3.7s0` / `0` | data | held, names every shard | held, names `{0,2,3}` | `25'2` | shard 1 from `25'1` to `25'2` (25) | 1,299 bytes |
| osd.2 | `3.7s1` / `1` | nonprimary data | held, names every shard | not held | `25'1` | none (0) | 632 bytes |
| osd.1 | `3.7s2` / `2` | parity | held, names every shard | held, names `{0,2,3}` | `25'2` | shard 1 from `25'1` to `25'2` (25) | 1,299 bytes |
| osd.0 | `3.7s3` / `3` | parity | held, names every shard | held, names `{0,2,3}` | `25'2` | shard 1 from `25'1` to `25'2` (25) | 1,299 bytes |

Each copy reported `whoami` equal to its OSD ID, tail `0'0` and an empty
missing set. The three log dumps that hold the overwrite were byte-identical
within a run. The pauses were between 763 and 842 ms and each archive stream
was 1,073,762,304 bytes. The PG held no other entry: the owned-pool oracle's
probe object of pool creation maps to another PG. The overwrite's entry was
dumped with a `ROLLBACK_EXTENTS` operation for extent `[0,4096]` on shards
`{0,2,3}` and a `SETATTRS` operation; shard 1's `last_user_version` stayed 1
while the others reported 2. These are recorded as observed and are not
asserted.

The first archive ran three neighbouring rows, because the receipt and the
log parser they share gained fields: the COPY_FROM2 shard-history control
(`...t-osderasureoptimizedk2m2copyfromhistory`, PASS in 64.85 s, with all
four shard copies of PG `4.2` holding both linked identities at `27'3`), the
primary calibration
(`...t-osderasureoptimizedk2m2primaryhistorycalibration`, PASS in 9.85 s)
and the replicated COPY_FROM2 control
(`osd.aes256k.ip6...t-osdcopyfromdestinationhistory`, PASS in 10.23 s).

One earlier run of the working tree, before the test commit existed, is
part of the record. It was the first run of the test and it passed with the
same placement and per-shard result (`client.4719.7:1` at `25'1`,
`client.4719.9:2` at `25'2`). It preceded the check that the info dump names
the acting index as its shard ID; the dumps it logged name shards 0 to 3 in
acting order.

Two native negative controls ran on remote copies of the working tree that
were then discarded. In both, the Go client sent the overwrite elsewhere
while the test still expected the plan for offset 5:

- at offset 4092, across the boundary between the two data chunks;
- at offset 4101, inside shard 1's chunk.

In both runs the overwrite was acknowledged and read back natively, all four
shard copies held its entry with head `25'2`, the entry was dumped with
`"written_shards":"{}"` and the primary's info held no partial-write record.
The test failed
on the first shard with `the overwrite's entry names shards []; want [0 2 3]`.
The second case is the source rule that shard 0 is always named: an
overwrite confined to the nonprimary data shard's chunk leaves no shard out.

After every run the Docker host had no container with the Testcontainers
label and no reader container left; the store copies lived only inside the
removed readers.

## Limits

- This is a control of one acknowledged partial write in a clean, active PG
  whose acting set did not change. A shard without the entry is an
  observation of that shard's copy at that bracketed placement. It does not
  establish that the identity is absent from the PG, was evicted or trimmed,
  or would be replayed.
- What happens to the left-out shard afterwards is not exercised. During
  peering the pinned source can advance that shard's `last_update` and log
  head to the end of a partial-write range without giving it the entries
  ([range application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L324-L364)).
  A log whose head is past its last entry is refused by this oracle's
  parser as incomplete. A capture after an interval change, a primary
  change onto another shard, recovery of the left-out shard or a later write
  that names it needs its own source reading and its own judgement.
- The copies are sequential captures of paused containers and are not one
  atomic snapshot across the four stores. Identical brackets fix placement
  and epoch, not store contents. No subject request was sent between the
  overwrite's acknowledgement and the last dump.
- Head and tail checks cannot detect an omitted interior entry. The Go
  recheck covers entry versions, `written_shards` and the requested
  identities, not every field of an entry.
- The partial-write record is not required, and its epoch is only recorded.
- One geometry ran: stripe width 8192, chunk 4096, an overwrite of 14 bytes
  at offset 5 of a two-stripe object. Other offsets, a write in a later
  stripe, an append or truncate, a parity-delta against a full-stripe
  rewrite, more than one skipped shard (k above 2) and plugins with a chunk
  mapping were not run natively. The restated plan is pinned for some of
  them by the cluster-free test only.
- No trimming, duplicate tier, replay or deduplication behaviour is
  exercised. The proposed history-eviction resubmission cell still needs a
  complete absence judgement for every acting member at the intended PG
  state, and this control shows that such a judgement has to account for a
  shard that never received an entry.
- One profile ran: secure, aes256k client and service keys, IPv4, container
  runtime, replicated-pg-v8, four BlueStore OSDs, isa k=2,m=2 with
  optimizations. Other widths, a degraded or remapped PG and a PG with a
  pg_temp are not part of this control.

# A shard left out of a partial write: its log after a later write and after a new interval

These are two test-only controls for issue #9. They continue the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md),
which ends with a nonprimary data shard whose own PG log does not hold an
acknowledged overwrite's entry, and reuse its
[shard-local capture](OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md). Each
answers one narrow question about that shard's own log:

- after one more acknowledged write of the same interval that names the
  shard, does it then hold the entry it was left out of?
- after the PG's interval ends and the PG peers again, with no daemon
  stopped and no data moved, where are its `last_update` and log head, and
  what does the first write of the new interval leave on it?

They add no product API, opcode or flag and inject no fault. The second
control sends one native `pg repeer` command for its own PG and changes no
daemon configuration. Neither states that an identity was evicted, trimmed
or lost, and neither resubmits anything.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

The partial-overwrite control's
[source basis](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#source-basis)
states which shards a write names and how a shard that can be primary
records the shards a write left out. In addition:

- A sub-write carries the log entries of its own operation only
  ([dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L812-L889)),
  and a receiver appends them to its own log
  ([local log application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECBackend.cc#L424-L443)).
  A log accepts an entry whose version is newer than its head; the versions
  need not be contiguous
  ([append](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.h#L653-L675)).
  A later write that names a shard therefore gives that shard its own entry
  and none of the entries the shard was left out of before.
- When a shard that can be primary rolls forward an entry that names a
  nonprimary shard, it resets that shard's partial-write record to the
  empty range at the entry's version; a nonprimary shard records nothing
  ([record](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGBackend.cc#L414-L496)).
- `pg repeer` stores a pg_temp of the acting primary alone, padded to the
  pool size, for a PG with more than one acting member
  ([command](https://github.com/ceph/ceph/blob/v20.2.4/src/mon/OSDMonitor.cc#L12551-L12588)).
  That ends the PG's interval. The primary of the shortened acting set wants
  its up set back and has the pg_temp removed, which starts one more
  interval with the original sets.
- During peering the primary applies the partial-write range it holds for a
  shard to its view of that shard's info: a range that starts at or before
  the shard's `last_update` and ends after it advances the shard's
  `last_update`, and its `last_complete` if the two were equal, to the end
  of the range
  ([range application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L324-L364),
  [on peer infos](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L422-L428)).
  A peer whose `last_update` then equals the primary's is activated with the
  primary's info alone, without log entries
  ([activation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3026-L3051)).
- The shard that receives that info merges the partial-write ranges of a
  sender that can be primary into its own info and applies the range of its
  own shard ID to its own info and log head
  ([merge and own range](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L369-L434),
  [receiver](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L7296-L7347)).
  It gets no entry. The primary sets the epoch of its ranges to the map
  epoch when it activates
  ([epoch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L2897-L2901)).
- The offline reader builds a collection's log with the info's
  `last_update` as its head
  ([log load](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.h#L1683-L1689)).
  A shard whose `last_update` was advanced that way is therefore dumped with
  a head that is newer than its last entry.
- An interval change marks the primary's next write as the first of its
  interval
  ([interval change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L998-L1010)),
  and the first write of an interval names every shard
  ([choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L597-L603)).
- `pg <pgid> query` reports the primary's state, map epoch, up and acting
  sets, its own info and its view of every peer's info
  ([query](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PrimaryLogPG.cc#L1055-L1075),
  [peering state](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L4559-L4606),
  [history](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc#L3579-L3600)).

No Ceph source is incorporated into the product.

## What changed in the capture's judgement

`tools/osd_pg_history_oracle.py` refused every log whose last entry is not
its head. The shard-local reader now parses such a log for one bounded
shape and reports it; every other caller and every other shard is judged as
before.

- `parse_log_body` still refuses the shape unless its caller asks for it,
  and it always refuses an entry that is newer than the head. The live
  primary snapshot does not ask.
- `tools/osd_shard_history_oracle.py` asks only for a shard that the owned
  pool's native `nonprimary_shards` names. The owned-pool oracle reads that
  set from its own map dump, in the pinned set print form; any other value
  admits no shard.
- For such a shard the receipt reports `head_past_entries` and the
  `last_entry`. The shard is complete only if its log holds at least one
  entry and its own info holds exactly one partial-write record for its own
  shard ID that starts at or before the last entry and ends at the head.
  The unchanged rules still apply: the info's `last_update` is the log's
  head, `last_complete` equals it, and the missing set is empty.
- The Go test rechecks it independently. `checkShardHistory` accepts
  `head_past_entries` only on the shards its caller lists, and only if the
  oracle admitted exactly those shards. `checkShardDumps` reads the head,
  the tail, the last entry, `last_update` and `last_complete` from the raw
  dumps and requires the same record for a head past the entries. The two
  earlier controls list no shard.

## The owned interval change

`tools/osd_pool_oracle.py` mode `pginterval` with action `repeer`:

1. reads the native placement of the control's object and one `pg query` of
   its primary, between two placement reads. It sends nothing unless the PG
   is an erasure-coded PG without a pg_temp, with equal up and acting sets,
   whose primary reports exactly `active+clean` for its own shard at a map
   epoch that is not behind the placement's;
2. sends `pg repeer <pgid>` to the MON once. It is not resent, and a nonzero
   result is reported;
3. repeats that observation every 0.5 s for at most 30 s until
   `tools/osd_pg_interval_oracle.py` judges the PG settled: two identical
   placement records around a complete query of the same primary; the
   placement equal to the starting one in every field but a newer epoch; no
   pg_temp; the primary reporting exactly `active+clean` with those sets;
   an interval that started after the one reported before the command,
   not after the bracketing epoch, in which the PG has started; and a
   primary whose own map epoch is not behind the bracketing epoch.

A PG that does not settle is reported with the reasons of its last
observation. Action `show` sends no command and reports one observation.
The Go test rechecks the receipt in `checkPGInterval` without relying on
the oracle's verdict.

The fixture admits the mode only for the selectors of these two controls
and of the two [outage controls](OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md),
which only observe with it, on the four-BlueStore topology of the capture;
every other profile is refused, the two older capture selectors included.

The command touches the temporary mapping of one PG. The PG is not active
while its acting set is the primary alone (one shard is below `min_size`),
so no write can be acknowledged in between. The test's cleanup requires, in
at most twenty observations, that the PG is `active+clean` on its original
up and acting sets and primary without a pg_temp, before the pool is
removed.

## Scenarios

Both tests run on the four-BlueStore fixture (`osd-replicas-4`,
`replicated-pg-v8`, secure, aes256k, IPv4, container) and start with the
partial-overwrite control's sequence, unchanged and with all of its
requirements: geometry from native sources, one WRITEFULL of two stripes
(client incarnation 7), one WRITE of 14 bytes at offset 5 (incarnation 9),
native readback after each, the live primary's log, and one capture of
every acting shard. Every Go request is sent once; a failed or unknown
result stops the test and nothing is resent.

A shard's role comes from its shard ID, the native width and the native
nonprimary shards. The acting index equals the shard ID because the capture
selected, and the store returned, collection `<pgid>s<i>` whose info names
shard `i`. The OSD's position among the wire targets is not used.

`shardLogObservation` reads each shard's receipt as a selection of the
acknowledged writes and refuses every shape that contradicts them: an
entry that is repeated, failed, elsewhere, out of order or names another
shard set; a foreign entry; a first write that is not held; a head that is
neither the last entry nor, when reported past the entries, a later write
without an entry. `shardLogExpected` restates the source rules above for
one shard. The test requires the two to be equal.

### Later write in the same interval

`TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration`

1. An observation of the PG records the epoch its interval started at. It
   is repeated, at most fifteen times a second apart, until the PG is clean
   on the placement the requests use and its primary is not behind that
   placement's map epoch.
2. The partial-overwrite sequence and its capture.
3. The Go client sends one WRITE of 12 bytes at `chunk + 5` (incarnation
   11), inside shard 1's chunk of the first stripe. The restated plan must
   name every shard.
4. Native readback: the object holds the expected bytes at the reply's user
   version.
5. The live primary holds the three requests as its only three entries,
   ending at the later write. Then a second capture, which admits no head
   past the entries on any shard.
6. One more observation: the interval still started at the recorded epoch.

| Statement | Status |
| --- | --- |
| The later write acknowledged ONDISK, native bytes and user version equal | required |
| Every shard holds the later write's entry as its last entry, naming every shard, with its head, `last_update` and `last_complete` there | required: the primary builds a sub-write for each named shard and the reply waits for all of them |
| Shard 0 and both parity shards hold all three entries | required |
| The nonprimary data shard (shard 1) holds the full write's entry and the later write's entry, and not the overwrite's entry between them | required: a sub-write carries only its own operation's entries |
| No shard's head is past its last entry | required: nothing peered |
| Shard 1 holds no partial-write record | required: a nonprimary shard records none |
| Shard 0 and both parity shards hold either the record "shard 1, from the full write to the overwrite" or the record "shard 1, from the later write to the later write" | required as one of the two; which one is recorded. Each is written by a roll-forward, which no reply waits for. No record at all is accepted only on a shard that showed none in the first capture |
| No near miss on the primary or on any shard; the primary's store copy repeats its live answer | required |
| The PG's interval started at the same epoch before the first write and after the last capture | required |

### New interval

`TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration`

1. The same first observation as above.
2. The partial-overwrite sequence and its capture.
3. The test observes the live primary, at most fifteen times a second
   apart, until its own info holds exactly the record "shard 1, from the
   full write to the overwrite". The interval change advances shard 1 only
   through that record, and no reply waits for it.
4. The owned interval change above. Its starting observation must show the
   interval of step 1 and that record.
5. A second capture on the placement record of the new epoch. It admits a
   head past the entries on the pool's nonprimary shards only.
6. Native readback: bytes and user version are those of the overwrite.
7. The Go client reads the whole object once through a new session on the
   new placement record (incarnation 12) and must get the expected bytes.
8. The Go client sends one WRITE of 12 bytes at offset 64 (incarnation 13).
   By its bytes the restated plan would leave shard 1 out again. Native
   readback follows.
9. A third capture, which admits no head past the entries.
10. One more observation: the interval still started at the epoch the
    change settled in.

| Statement | Status |
| --- | --- |
| One accepted `pg repeer`; the PG clean before it and clean after it on the same PG, up and acting sets, primary, shard and address, without a pg_temp, at a newer epoch, in an interval that started after the command | required |
| After the change, shard 0 and both parity shards hold the full write's and the overwrite's entry and their head is the overwrite | required |
| After the change, shard 1 holds only the full write's entry; its head, `last_update` and `last_complete` are the overwrite's version, past its last entry | required: the primary found it up to date through the range and sent no entry |
| After the change, every shard's info, shard 1's included, holds exactly the record "shard 1, from the full write to the overwrite" | required: peering merges the primary's records, and step 3 established the primary's |
| Native bytes and user version unchanged by the interval change; the Go read returns them | required |
| The first write of the new interval acknowledged ONDISK with a replay version whose epoch is not older than the interval's start; native bytes and user version equal | required |
| Every shard, shard 1 included, holds that write's entry as its last entry, naming every shard | required: the first write of an interval names every shard |
| Shard 1 then holds the full write's entry and that write's entry, not the overwrite's, and its head is no longer past its entries | required |
| Shard 1's info still holds the record it merged | required: a nonprimary shard never updates it |
| The other shards hold either that record or "shard 1, from the new write to the new write" | required as one of the two; which one is recorded |
| The epochs, the number of observations until the PG settled, the primary's view of each peer, the epoch of the partial-write records | recorded |
| Cleanup: the PG is clean on its original placement without a pg_temp | required |

The statements about shard 1 are observations of that shard's own log at
capture time, between two identical placement records. A head past the
last entry says that the shard's log position was advanced without the
entry; it does not say that the entry was evicted or lost, or that the PG
lacks it: the other three shards hold it, and the range that shard 1's own
info records names the versions it was left out of.

## Cluster-free checks

- `tools/test_osd_pg_history_oracle.py` gains one test: the parser returns
  a head past the last entry only on request, reports the last entry, and
  refuses an entry newer than the head either way.
- `tools/test_osd_shard_history_oracle.py` gains three tests with synthetic
  dumps. One lays out the capture after an interval change. One refuses the
  same dumps when the shard is not named as nonprimary (three sets), when
  the shape is on the primary's shard or on a parity shard, and for
  fourteen near misses of the shard's own record, log and info. One drives
  the mode with nine values of the pool's `nonprimary_shards`.
- `tools/test_osd_pg_interval_oracle.py` is new: the query parser on one
  reply of the pinned shape, on the bounds of its lists and names and on
  thirty-six truncated or reshaped replies; the settled judgement on three
  accepted and thirty-one unsettled observations; the starting-point
  judgement on two accepted and thirteen refused observations; and the mode
  with a fake native client: one command and three observations until the
  PG settles, no command for a starting point that is not clean, a refused
  command that is not resent, a PG that never settles within the time
  bound, action `show`, and invalid requests.
- `TestShardLogObservation` pins seven shapes, thirty-one contradictory
  logs and seven refused inputs. `TestShardLogExpected` pins thirteen
  expectations and seven refused inputs, the agreement of the two functions
  on the captures of both controls, and ten partial-write record lists.
- `TestCheckPGInterval` accepts three receipts and refuses sixty-three
  changed ones, eight of them on a placement that is unusable in itself
  although every record agrees on it. It accepts the observation without a
  command for two placements and refuses twenty-one.
- `TestCheckShardHistory` gains twelve admissions of a head past the last
  entry and four receipts that report it inconsistently.
  `TestCheckShardDumps` gains thirteen receipts whose bounds differ from
  their dumps, an advanced shard with two accepted ranges and fourteen near
  misses of it.
- `TestShardHistoryCaptureAdmission` in the fixture pins the four capture
  selectors, refuses a selection of several of them, and pins the two
  selectors that admit the interval mode.

A mutation check ran in scratch copies that were then discarded: single
operator flips in the new parsing and judgement code. Every mutant of the
interval oracle's mode, of the log bounds in `checkShardDumps` and of the
admission in `checkShardHistory` was killed. The survivors in the interval
judgements and in
`shardLogObservation` were list and name bounds, one redundant condition
that was then removed, and checks that a later condition repeats; cases
were added for the bounds, for a query answered with another shard ID, for
a single observation and for the unusable placements above.

## Reproduction

On an authorized Linux runner, hold its run lock, use the pinned image and
the environment of the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#reproduction)
with one of

```sh
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration$'
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration$'
```

The dedicated mandatory rows are
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritelaterwriteshardhistory`
and
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritenewintervalshardhistory`.
Request them with the CI comment command. Basic PR CI builds and unit-checks
the fixture but does not run these daemon tests. The native result of a
pull-request head is recorded in that pull request with the tested SHA. A
pass at one SHA is not evidence for a later revision.

## Local native calibration

These runs preceded the pull-request update and are not exact-SHA CI
receipts. They used the catalog rows' environment and the pinned image on a
Linux amd64 host with Docker 29.9.1 and Go 1.26.9, through
`integration/run.sh` and the native result gate, with one test process on
the engine. Every row ran in its own fixture.

The recorded runs used archives of two commits. The first is commit
`2b20f98`, which added the two rows. One archive of it ran both rows and
the four neighbouring rows below, each as the first run of that commit.
Commit `2c7193e` then widened one accepted record list of the later-write
control: the record a holder may show after the later write. Later
commits change documents only. Three archives of it ran both rows again, the
first as the first run of that commit, and the third also ran the four
neighbouring rows.

The gate recorded both tests as PASS with no problem in all eight runs:

| Commit | Later write: test, row | New interval: test, row |
| --- | --- | --- |
| `2b20f98` | 110.27 s, 175 s | 164.68 s, 221 s |
| `2c7193e` | 110.27 s, 177 s | 164.68 s, 221 s |
| `2c7193e` | 110.26 s, 176 s | 164.08 s, 220 s |
| `2c7193e` | 110.27 s, 175 s | 163.58 s, 219 s |

One earlier run of each row on the working tree, before the test commit
existed, is part of the record. It was the first run of each test and both
passed (110.17 s and 164.28 s) with the placement and per-shard result
below.

Every run observed the same PG, acting set, roles and per-shard result. The
fixture's topology is deterministic, so they are repetitions of one
placement, not different ones. In one run of the later-write control
(the first archive of `2c7193e`) the fixture's map was one epoch ahead when
the pool was created: the client's global ID was 4743 instead of 4722 and
every epoch below was one higher (`26'1`, `26'2`, `26'3`, an interval that
started at epoch 25). The values below are those of the other runs.

| Observation | Value |
| --- | --- |
| Geometry | stripe_width 8192, chunk 4096, profile k=2, m=2, plugin isa, no mapping, `nonprimary_shards` `{1}` |
| Placement | PG `3.7`, up = acting = `[3,2,1,0]`, primary osd.3 on shard 0; osd.2 holds nonprimary data shard 1, osd.1 and osd.0 the parity shards 2 and 3 |
| Full write | `client.4722.7:1`, replay version `25'1`, user version 1, names every shard |
| Partial overwrite | `client.4722.9:2`, 14 bytes at offset 5, replay version `25'2`, user version 2, names `{0,2,3}` |
| First capture, both controls | as in the partial-overwrite control: shards 0, 2 and 3 at `25'2` with the record "shard 1 from `25'1` to `25'2`" (epoch 25), shard 1 at `25'1` without a record |

### Later write in the same interval

The later write was `client.4722.11:3`, 12 bytes at offset 4101, replay
version `25'3`, user version 3. The restated plan named shards 0 to 3. The
live primary's log held three entries and ended at `25'3`. Both brackets of
both captures were at epoch 25, and the interval had started at epoch 24
before the first write and after the last capture.

| Store copy | Info `pgid` / `shard` | Role | Full write `25'1` | Overwrite `25'2` | Later write `25'3` | Log head = last entry = `last_update` = `last_complete` | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | `3.7s0` / `0` | data | held | held, names `{0,2,3}` | held, names every shard | `25'3` | shard 1 from `25'3` to `25'3` (25) | 1,967 bytes |
| osd.2 | `3.7s1` / `1` | nonprimary data | held | not held | held, names every shard | `25'3` | none (0) | 1,300 bytes |
| osd.1 | `3.7s2` / `2` | parity | held | held, names `{0,2,3}` | held, names every shard | `25'3` | shard 1 from `25'3` to `25'3` (25) | 1,967 bytes |
| osd.0 | `3.7s3` / `3` | parity | held | held, names `{0,2,3}` | held, names every shard | `25'3` | shard 1 from `25'3` to `25'3` (25) | 1,967 bytes |

Shard 1's two entries were dumped with versions `25'1` and `25'3`; the
later write's entry carried prior version `25'2`, the version of the entry
that copy does not hold. All three holders showed the reset record in every
run; the other accepted record was not observed.

### New interval

The live primary's info held the record at the first observation in every
run. The `pg repeer` command returned 0 and the PG settled at the fifth or
sixth observation, between 2.5 and 3.1 s after the starting observation began:

| | Before the command | After the PG settled |
| --- | --- | --- |
| Map epoch of both placement records | 25 | 28 |
| PG, up, acting, primary, shard, address, pg_temp | `3.7`, `[3,2,1,0]`, `[3,2,1,0]`, osd.3, 0, unchanged, none | the same |
| Primary's state | `active+clean` | `active+clean` |
| Interval started at epoch (`same_interval_since`) | 24 | 27 |
| Primary's own info | `last_update` `25'2`, record "shard 1 from `25'1` to `25'2`" (epoch 25) | the same versions and record (epoch 28) |

The primary's view of shard 1 (`peer_info` of `2(1)`) showed `last_update`
`25'2` after the change, and it had shown the same before the command, when
shard 1's own store was still at `25'1`. It is the primary's memory and is
recorded only.

Second capture, at epoch 28:

| Store copy | Role | Entries held | Last entry | Log head = `last_update` = `last_complete` | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | data | `25'1`, `25'2` | `25'2` | `25'2` | shard 1 from `25'1` to `25'2` (28) | 1,299 bytes |
| osd.2 | nonprimary data | `25'1` | `25'1` | `25'2`, past the last entry | shard 1 from `25'1` to `25'2` (28) | 632 bytes |
| osd.1 | parity | `25'1`, `25'2` | `25'2` | `25'2` | shard 1 from `25'1` to `25'2` (28) | 1,299 bytes |
| osd.0 | parity | `25'1`, `25'2` | `25'2` | `25'2` | shard 1 from `25'1` to `25'2` (28) | 1,299 bytes |

Shard 1's log dump had the same length as in the first capture: the same
single entry under a head of `25'2` instead of `25'1`. Its info still
reported `last_user_version` 1 while the others reported 2. The native
readback returned the overwrite's bytes at user version 2, and the Go read
returned the same 16,384 bytes.

The first write of the new interval was `client.4722.13:4` (the read took
transaction 3), 12 bytes at offset 64, replay version `28'3`, user version
3. By its bytes the restated plan named shards 0, 2 and 3. Third capture,
at epoch 28:

| Store copy | Role | Entries held | Log head = last entry = `last_update` = `last_complete` | New entry names | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | data | `25'1`, `25'2`, `28'3` | `28'3` | every shard | shard 1 from `28'3` to `28'3` (28) | 1,963 bytes |
| osd.2 | nonprimary data | `25'1`, `28'3` | `28'3` | every shard | shard 1 from `25'1` to `25'2` (28) | 1,296 bytes |
| osd.1 | parity | `25'1`, `25'2`, `28'3` | `28'3` | every shard | shard 1 from `28'3` to `28'3` (28) | 1,963 bytes |
| osd.0 | parity | `25'1`, `25'2`, `28'3` | `28'3` | every shard | shard 1 from `28'3` to `28'3` (28) | 1,963 bytes |

The three holders showed the reset record in every run; the other accepted
record was not observed. The interval still started at epoch 27 after the
last capture, and the cleanup found the PG `active+clean` at epoch 28 on its original up and
acting sets and primary without a pg_temp.

In every capture each copy reported `whoami` equal to its OSD ID, tail
`0'0` and an empty missing set, each OSD was paused between 730 and 827 ms and
each archive stream was 1,073,762,304 bytes. No near miss had a position on
the primary or in any copy. After every run the Docker host had no
container with the Testcontainers label and no reader container left.

### Neighbouring rows

The receipt and the log parser these rows share gained fields, and the
partial-overwrite control's body moved into helpers. Each of the four rows
passed in the first archive of `2b20f98` and again in the third archive of
`2c7193e`:

| Row | `2b20f98`: test, row | `2c7193e`: test, row | Observation |
| --- | --- | --- | --- |
| `...t-osderasureoptimizedk2m2partialwriteshardhistory` | 59.85 s, 116 s | 59.84 s, 116 s | the per-shard result of its [record](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md), at epoch 25 |
| `...t-osderasureoptimizedk2m2copyfromhistory` | 64.65 s, 121 s | 64.55 s, 120 s | all four shard copies of PG `4.2` held both linked identities, at `28'3` and at `27'3` |
| `...t-osderasureoptimizedk2m2primaryhistorycalibration` | 9.86 s, 68 s | 9.74 s, 66 s | PASS |
| `osd.aes256k.ip6...t-osdcopyfromdestinationhistory` | 10.93 s, 64 s | 11.13 s, 61 s | PASS |

### Negative controls

Three native negative controls ran on remote copies of the working tree
that were then discarded.

- Later write. The Go client sent the later write at offset 40, inside
  shard 0's chunk, while the test still expected the plan of a write inside
  shard 1's chunk. The write was acknowledged and read back natively. Shard
  1's copy still ended at `25'1`, the three holders' record had grown to
  "shard 1 from `25'1` to `25'3`", and the test failed on the first shard
  with `later write: its entry names shards [0 2 3]; want [0 1 2 3]`.
- New interval. The owned interval change ran before the two writes instead
  of after the overwrite, so the PG did not peer after the overwrite. The
  writes were acknowledged at `28'1` and `28'2`, shard 1's copy ended at
  `28'1` at its last entry without a record, and the test failed with
  `holds #1, head 28'1 at its last entry; the writes ... require that it
  holds #1, head 28'2 past its last entry`.
- Parser bound. Before the shard-local reader accepted the bounded shape,
  two exploratory runs captured shard 1 after an interval change. The
  receipt was incomplete with `shard 1 (osd.2): log: the last log entry is
  not head; the log array is incomplete`, which is what the reader still
  reports for a shard that the pool does not name as nonprimary.

The second of those exploratory runs used a longer sequence, which is not
part of the committed tests: a later write at `25'3` and a second partial
overwrite at `25'4` before the first `pg repeer`, after which shard 1's
copy held `25'1` and `25'3` under a head of `25'4`; then a write at `28'5`
that named every shard, a partial overwrite at `28'6` and a second
`pg repeer`, after which shard 1's copy held `25'1`, `25'3` and `28'5`
under a head of `28'6` with the record "shard 1 from `28'5` to `28'6`" at
epoch 31.

## Limits

- Both controls ran one placement of one PG in a clean, active state on one
  profile: secure, aes256k client and service keys, IPv4, container runtime,
  replicated-pg-v8, four BlueStore OSDs, isa k=2,m=2 with optimizations,
  stripe width 8192. Other widths, more than one nonprimary shard and
  plugins with a chunk mapping were not run.
- The interval change keeps the primary and the acting set. An outage of
  the left-out shard's OSD, without and with a degraded write, has its own
  [controls](OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md). A primary change
  onto another shard is not exercised; see the next section.
- A log whose head is past its last entry is accepted for exactly the shape
  described above. A nonprimary shard whose log holds no entry, a range that
  the shard's own info does not record, and the same shape on a shard that
  can be primary are refused as incomplete. They were not observed and are
  not known to be impossible.
- The copies are sequential captures of paused containers and are not one
  atomic snapshot across the four stores. Head and tail checks cannot detect
  an omitted interior entry.
- The interval started twice (the pg_temp and its removal); what the shards
  held in the intermediate interval, in which the PG was not active, was
  not captured.
- The primary's view of a peer's `last_update` in `pg query` is recorded
  only. It is the primary's memory, not the peer's store.
- No trimming, duplicate tier, replay or deduplication behaviour is
  exercised. The proposed history-eviction resubmission cell still needs a
  complete absence judgement for every acting member at the intended PG
  state. These controls add two facts that judgement has to account for: a
  shard can hold later entries without an earlier one, and a shard's head
  can be past its last entry.

## Not built: primary replacement

Issue #9 also asks for a primary replacement and a degraded write followed
by recovery, with a capture of the shards afterwards. The degraded write
and the recovery of the left-out shard are the subject of two later
[controls](OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md), which take that
shard's OSD out of the PG with the fixture's forced kill of an owned OSD,
as the existing degraded-write tests do. These two controls stop before a
primary replacement, for these reasons:

- Marking the acting primary's OSD out does not give the PG another active
  primary while that OSD runs. The member that leads the next interval
  fills the empty position from the out OSD, which still holds that shard
  ([EC acting choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L1760-L1817)),
  and asks for a pg_temp of the full set. Under a pg_temp the first member
  in primary-first order leads
  ([temporary mapping](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L3060-L3093)),
  which is the old primary on shard 0.
  One exploratory run on a remote copy of the working tree, which was then
  discarded, marked osd.3 out after the partial overwrite and in again. At
  the next epoch the map showed up and acting `[NONE,2,1,0]` led by osd.2,
  which answered `pg query` for `3.7s1` with state `unknown`. One epoch
  later every PG of the pool had a pg_temp, PG `3.7` was acting `[3,2,1,0]`
  led by osd.3 again, and it reported `active+clean+remapped` one epoch
  after that. After `osd in` it was `active+clean` on its original sets
  two epochs later, and the native bytes and user version were unchanged.
  No other member was an active primary, and the change touched all eight
  PGs of the pool.
- Another active primary therefore needs the old one down in the map, which
  means killing the primary's OSD. The left-out shard cannot lead, so a
  parity shard's OSD leads under a primary-first pg_temp, and the old
  primary leads again after its return, through further intervals and a
  recovery of its own. Marking a running OSD down and holding it down with
  the cluster-wide `noup` flag touches every PG of that OSD and depends on
  how and when the daemon reacts to its own map state. Neither was run with
  this capture, so no deterministic assertion can be stated for it yet.
- The capture copies every member of the acting vector and refuses a vector
  with an empty position, so a PG without a member can be captured only
  before the outage and after the return.

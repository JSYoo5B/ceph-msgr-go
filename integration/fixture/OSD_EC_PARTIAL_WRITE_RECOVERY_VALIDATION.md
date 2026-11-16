# A shard left out of a partial write: its log after its OSD's outage and after a recovery

These are two test-only controls for issue #9. They continue the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md),
which ends with a nonprimary data shard whose own PG log does not hold an
acknowledged overwrite's entry, and reuse its
[shard-local capture](OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md) and the
PG observation of its
[first two continuations](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md).
Both take that shard's OSD out of the PG and bring it back. Each answers one
narrow question about what that shard's own store copy holds once the PG is
clean again:

- with nothing written while the OSD was away, is the shard handed the
  entry it was left out of?
- with one acknowledged write while the OSD was away, which entries does the
  recovered shard hold, and where are its `last_update`, `last_complete`,
  log head, missing set and partial-write records?

They add no product API, opcode or flag. The OSD leaves through the
fixture's forced kill of an owned OSD and returns through its start, the
daemon control that the existing degraded-write tests use; no other fault
is injected and no daemon configuration changes. Neither control states
that an identity was evicted, trimmed or lost, and neither resubmits
anything.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

The earlier records state which shards a write names
([partial overwrite](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#source-basis)),
and how a partial-write range is recorded, applied during peering and
merged
([later state](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md#source-basis)).
In addition:

- A PG whose acting set has fewer members than the pool size is undersized
  ([state](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3224-L3228))
  and degraded once its statistics count a degraded object
  ([state](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L4354-L4358)).
  The k=2, m=2 pool has `min_size` 3, so the PG stays active without one
  member. The empty acting position is `CRUSH_ITEM_NONE`
  ([value](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.h#L1267)).
- The member's departure ends the interval, so the next write is the first
  of its interval and names every shard, the absent one included
  ([interval change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L998-L1010),
  [choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L597-L603)).
  The primary builds sub-writes for the members of the acting set only
  ([dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L812-L889)),
  so the absent shard gets none.
- When a shard that can be primary rolls forward an entry that names a
  nonprimary shard for which it holds a range, it resets that range to the
  empty range at the entry's version. The source comments this branch as
  "Log updated or shard absent"
  ([record](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGBackend.cc#L475-L490)).
  No client reply waits for a roll-forward; a primary without further
  writes in its pipeline submits an empty operation after the commit to
  start it
  ([kick](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L971-L993)).
- At the member's return the primary applies the range it holds for the
  shard to its view of the shard's info only if the range starts at or
  before the shard's `last_update`
  ([range application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L324-L364)).
  - With nothing written, the range still runs from the full write to the
    overwrite and covers the shard's `last_update`. The shard is found up
    to date and activated with the primary's info alone, as after any
    interval change
    ([activation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3026-L3051)).
    Nothing is missing and nothing is recovered.
  - With the range reset to the degraded write, it starts after the shard's
    `last_update` and is not applied. The primary sends the shard every
    entry of its own log after the shard's `last_update`
    ([catch-up](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3103-L3112)),
    which includes the entry of the overwrite the shard was left out of.
    The shard appends every received entry to its own log
    ([merge](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.cc#L455-L514),
    [append](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.h#L1422-L1436)).
    Only an entry that names a nonprimary shard makes the object missing on
    it
    ([missing set](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.h#L5161-L5203)):
    the overwrite's entry is skipped and the degraded write's entry marks
    the object missing at its version. The object is then recovered to the
    shard, and the PG is clean only after that.
- The returning shard merges the partial-write ranges of the primary into
  its own info
  ([merge](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L369-L434),
  [log receiver](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L7261-L7293)).
- A primary of a new interval has the MON record its `up_thru` before it
  activates
  ([need](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L1482-L1493),
  [wait](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L8094-L8099)).
  The returned OSD leads other PGs, so the map gains one more epoch after
  its return that does not change this PG's interval.

Not exercised by these controls, from the same source:

- A write that reaches a shard while it is a backfill or asynchronous
  recovery target invalidates that shard's partial-write ranges until the
  next interval
  ([invalidation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L4798-L4849)).
  These controls send nothing while the shard recovers, and the shard is
  recovered from the log, not by backfill: its `last_update` is not behind
  the primary's log tail
  ([choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3059-L3068)).
- If the return's peering ran before the degraded write's roll-forward, the
  primary would still hold the old range, advance the shard to the
  overwrite's version and send only the degraded write's entry. The second
  control excludes that by observing the reset range on the live primary
  before the OSD starts.
- A parity shard is named by every write, so its OSD's outage leaves it
  without later entries, never without an entry between two it holds; it is
  caught up from the log in the same way and takes no part in the question
  of a left-out shard. The primary's OSD's outage needs another shard to
  lead; see
  [not built](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md#not-built-primary-replacement).

No Ceph source is incorporated into the product.

## The observation of a PG without one member

`tools/osd_pool_oracle.py` mode `pginterval` gains action `absent`. Like
action `show` it sends no command: it reads the native placement of the
control's object, one `pg query` of its primary and the placement again.
`absent_failures` in `tools/osd_pg_interval_oracle.py` accepts the
observation only if

- the caller's recorded placement is a complete erasure-coded acting set
  without a pg_temp, the named position is another one than the primary's,
  and the recorded interval start is not after the recorded epoch;
- the two placement records are identical and equal the recorded one in
  every field but a newer epoch and up and acting sets in which exactly the
  named position is empty; there is no pg_temp;
- the primary reports exactly `active` and `undersized`, with or without
  `degraded`, with those sets, for its own shard, at a map epoch that is not
  behind the placement's;
- the interval the primary reports started after the recorded one and not
  after the placement's epoch, and the PG has started in it.

The Go test rechecks the receipt in `checkPGAbsent` without relying on the
oracle's verdict. The fixture admits the `pginterval` mode for the selectors
of these two controls as well; they use actions `absent` and `show` only.

### Later observations without the member

After the first accepted observation the tests observe the PG without the
member again: while waiting for the primary's partial-write records and
right before the OSD starts. The MON may publish an epoch that leaves the
PG's sets unchanged, and the primary may answer before it has that map.
Such a sample cannot show the PG's interval at the placement's epoch. On
hosted run 43789717139 attempt 1 the first control's pre-start sample was
one: placement epoch 28, primary at epoch 27, interval still the one since
epoch 26. The oracle refused it, and the single pre-start observation
stopped the test.

`absentAgain` and `reobserveAbsent` in the test now classify every later
sample against the accepted observation, at most thirty a second apart:

- a primary that reports another interval start has left the accepted
  interval, even from an older map. A primary's map epoch does not go back
  and the accepted observation's primary was not behind its placement, so a
  different start is a confirmed change; the test stops;
- a sample accepted by the oracle and by `checkPGAbsent`, in the accepted
  interval, is held;
- a sample without a placement record or a query reply, with placement
  records that differ around the query, or from a primary behind the
  placement's epoch is inconclusive and is observed again. It never counts
  as held;
- every other refusal (another state, another set, a pg_temp) is a
  confirmed departure; the test stops.

The OSD starts only after one held sample. The records wait requires a held
sample that shows the records. If the bound passes first, the test fails
and names the last sample's reason. The checks of each sample are
unchanged.

The capture is unchanged. It still refuses an acting vector with an empty
position, so both controls capture before the outage and after the return
only. A capture of the three remaining shards while the fourth is away was
not added: it would need its own admission for an incomplete vector, and the
live primary's `pg query` already reports what these controls need from
that window, the primary's own partial-write records.

## Scenarios

Both tests run on the four-BlueStore fixture (`osd-replicas-4`,
`replicated-pg-v8`, secure, aes256k, IPv4, container) and start with the
partial-overwrite control's sequence, unchanged and with all of its
requirements, and with the first observation of the earlier continuations.
Then both

1. observe the live primary until its own info holds exactly the record
   "shard 1, from the full write to the overwrite", at most fifteen times a
   second apart;
2. kill the OSD of the left-out shard with the fixture's forced kill and
   require its SIGKILL receipt (exit code 137 of that daemon);
3. observe, at most 150 times a second apart, until the PG is accepted as
   active without that member, and adopt the placement record of that state
   with a new session to the unchanged primary. The primary's info must
   still be at the overwrite with the same record;
4. do what is described below while the member is away;
5. observe, at most thirty times a second apart, until one sample is held
   in the same interval (see
   [later observations](#later-observations-without-the-member)), start
   the OSD, and observe, at most 150 times a second
   apart, until the PG is `active+clean` on its original placement without a
   pg_temp in an interval that started after that one;
6. ask the fixture's readiness control for every OSD up and in and every PG
   `active+clean`, observe the PG once more in the same interval and adopt
   its placement record with a new session;
7. capture every acting shard, read the object back natively and through
   the Go client, and observe that the interval did not change.

Every Go request is sent once; a failed or unknown result stops the test
and nothing is resent. A shard's role comes from its shard ID, the native
width and the native nonprimary shards; the left-out shard is the one the
restated plan of the overwrite leaves out, which must be exactly one
nonprimary shard. The acting index equals the shard ID because the capture
selected, and the store returned, collection `<pgid>s<i>` whose info names
shard `i`.

The cleanup starts the OSD again if the test did not and requires, in at
most 150 observations, the PG `active+clean` on its original up and acting
sets and primary without a pg_temp, before the pool is removed. An up set
that names the OSD again means that the map has it up and in.

### Return without a write

`TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration`

While the member is away the object is read back natively and once through
the Go client (incarnation 12); nothing is written. After the return the
capture admits a head past the entries on the pool's nonprimary shards
only, and the Go client reads the object once more (incarnation 13).

| Statement | Status |
| --- | --- |
| The PG active on its original primary with exactly the left-out shard's position empty, in a newer interval; the primary's info at the overwrite with the record "shard 1, from the full write to the overwrite" | required |
| Native bytes and user version unchanged while the member is away; the Go read returns the bytes and that user version | required |
| The PG `active+clean` on the same PG, up and acting sets, primary, shard and address, without a pg_temp, in an interval that started after the one without the member | required |
| After the return, shard 0 and both parity shards hold the full write's and the overwrite's entry and their head is the overwrite | required |
| After the return, shard 1 holds only the full write's entry; its head, `last_update` and `last_complete` are the overwrite's version, past its last entry; its missing set is empty | required: the primary found it up to date through the range and sent no entry |
| After the return, every shard's info, shard 1's included, holds exactly the record "shard 1, from the full write to the overwrite" | required: the primary's record was established in step 1 and nothing was written since |
| Native bytes and user version unchanged after the return; the Go read returns the bytes and that user version | required |
| The epochs, the number of observations, the states seen before each accepted observation, the primary's view of each peer, the epoch of the partial-write records | recorded |
| Cleanup: the PG is clean on its original placement without a pg_temp | required |

### Degraded write, then recovery

`TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration`

While the member is away the Go client sends one WRITE of 15 bytes at
`chunk + 5` (incarnation 13), inside the absent shard's chunk of the first
stripe: the bytes that change are the ones that shard stores. Native
readback follows, then one Go read (incarnation 14). The test then observes
the live primary, at most thirty times a second apart, until a held sample
in the interval without the member shows exactly the record "shard 1, from
the degraded write to the degraded write" in the primary's own info. After the return the capture admits no head past the entries, and
the Go client reads the object once more (incarnation 15).

| Statement | Status |
| --- | --- |
| The PG active without the member and the primary's info as in the first control, before the write | required |
| The degraded write acknowledged ONDISK with a replay version whose epoch is not older than the start of the interval without the member; native bytes and user version equal while the member is away; the Go read returns the bytes and that user version | required |
| Before the OSD starts, the live primary's info holds exactly the record "shard 1, from the degraded write to the degraded write" | required: the return's peering reads it |
| The PG `active+clean` on its original placement in a newer interval | required; it includes the recovery of the object to the returned shard |
| After the recovery, every shard holds exactly the three entries, in order, with its head, `last_update` and `last_complete` at the degraded write and an empty missing set | required |
| The degraded write's entry names every shard on every copy | required: the first write of an interval names every shard, the absent one included |
| After the recovery, shard 1's copy holds the overwrite's entry, which names shards 0, 2 and 3 only | required: the primary sent every entry after the shard's `last_update` |
| After the recovery, every shard's info, shard 1's included, holds exactly the record "shard 1, from the degraded write to the degraded write" | required |
| Native bytes and user version of the degraded write after the recovery; the Go read returns the bytes and that user version | required |
| The replay version's epoch, the epochs, the number of observations, the states seen, the primary's view of each peer, the epoch of the partial-write records | recorded |
| Cleanup: the PG is clean on its original placement without a pg_temp | required |

The statements about shard 1 are observations of that shard's own store
copy at capture time, between two identical placement records. In the first
control a head past the last entry says that the shard's log position was
advanced without the entry; it does not say that the entry was evicted or
lost. In the second control the copy holds an entry for a write that did
not change that shard's bytes; that says how the shard's log was brought up
to date, not that the shard took part in that write.

Neither control reads the returned shard's chunk. That the object was
recovered to it is stated by the shard's own empty missing set with
`last_complete` at its head and by the PG being clean; the object bytes are
checked through the native client and the Go client, which decode from any
two of the four shards.

## Cluster-free checks

- `tools/test_osd_pg_interval_oracle.py` gains three tests and extends one. The judgement
  accepts eight observations of a PG without one member (two state forms,
  an interval that starts at the placement's epoch, a primary ahead of the
  placement, each other position, a recorded interval that started at the
  recorded epoch) and refuses thirty-seven changed observations and fifteen
  requests. The mode with a fake native client: one accepted observation
  without a command, seven unaccepted ones, and twelve more invalid
  requests, among them `show` or `repeer` with a field of `absent`.
- `TestCheckPGAbsent` accepts six receipts and refuses forty-one changed
  ones and eleven requests. For the return it accepts a clean observation
  after two earlier intervals and refuses three interval relations and five
  changed receipts.
- `TestReobserveAbsent` drives the later observations with synthetic
  samples. It holds a sample at once and at a newer epoch, after lagging
  samples, after placement records that moved around the query, after a
  missing query reply or placement record, and once the wanted state
  appears. It fails on persistent lag and on a wanted state that never
  appears, both after the bound. It fails at once on a later interval
  (lagging or not), on a later interval or a peering state after a lagging
  sample, on another OSD in the empty position and on an earlier interval.
  Three mutations failed it as intended: lag treated as a departure (the
  hosted failure), an interval change read from a refused sample ignored,
  and an inconclusive sample accepted.
- `TestShardLogRecovered` pins the recovered shape against five synthetic
  shard receipts, of which only the one that holds every entry is equal to
  it, and refuses seven write sequences whose first or last write does not
  name every shard.
- `TestShardHistoryCaptureAdmission` in the fixture pins the six capture
  selectors and the four selectors that admit the interval mode.

## Reproduction

On an authorized Linux runner, hold its run lock, use the pinned image and
the environment of the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#reproduction)
with one of

```sh
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration$'
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration$'
```

The dedicated mandatory rows are
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwriteshardreturnshardhistory`
and
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwritedegradedwriteshardhistory`.
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

The recorded runs used three archives of commit `2fdf93c`, which added the
two rows; later commits change documents only. Each archive ran both rows,
the first as the first run of that commit, and the first archive also ran
the four neighbouring rows below. The gate recorded both tests as PASS with
no problem in all six runs:

| Archive of `2fdf93c` | Return without a write: test, row | Degraded write, then recovery: test, row |
| --- | --- | --- |
| first | 121.28 s, 187 s | 122.09 s, 178 s |
| second | 121.98 s, 187 s | 121.49 s, 178 s |
| third | 120.78 s, 186 s | 122.59 s, 179 s |

Five earlier runs on the working tree, before the test commit existed, are
part of the record. The first run of each test failed after the return, and
the failure was the test's own: it captured as soon as the PG was clean at
epoch 31, and the map moved to epoch 32 between the primary snapshot and the
shard capture (`map epoch moved from 31 to 32 between the primary snapshot
and the shard capture`; in the other test the capture's two brackets
differed). An instrumented run of the degraded-write control then read the
map dump once a second for twenty seconds after the PG was clean: it was at
epoch 32 throughout, with no pg_temp, and the returned osd.2 had `up_from`
30 and `up_thru` 31 while the other OSDs had `up_thru` 30. The move was
therefore the `up_thru` of the returned OSD, which leads other PGs of the
pool, and the map did not move again. That run passed (139.74 s), since its
twenty seconds outlasted the move. The tests were changed to ask the
fixture's readiness control for every PG clean before the capture, which is
step 6 above, and to compare a later placement record with an accepted
observation in everything but its epoch. After that change one run of each
test passed on the working tree (122.28 s and 123.39 s), and every run above
passed. In the two failed runs the cleanup found the PG `active+clean` on
its original placement with osd.2 back.

Every run observed the same PG, acting set, roles and per-shard result. The
fixture's topology is deterministic, so they are repetitions of one
placement, not different ones. In one run (the degraded-write row of the
third archive) the client's global ID was 4716 instead of 4722; nothing
else differed. In the first working-tree run of the degraded-write control
the PG was accepted without osd.2 at epoch 28 and the degraded write was
acknowledged at `28'3`; the values below are those of the six archive
runs.

| Observation | Value |
| --- | --- |
| Geometry | stripe_width 8192, chunk 4096, profile k=2, m=2, plugin isa, no mapping, `nonprimary_shards` `{1}` |
| Placement | PG `3.7`, up = acting = `[3,2,1,0]`, primary osd.3 on shard 0; osd.2 holds nonprimary data shard 1, osd.1 and osd.0 the parity shards 2 and 3 |
| Full write | `client.4722.7:1`, replay version `25'1`, user version 1, names every shard |
| Partial overwrite | `client.4722.9:2`, 14 bytes at offset 5, replay version `25'2`, user version 2, names `{0,2,3}` |
| First capture, both controls | at epoch 25, as in the partial-overwrite control: shards 0, 2 and 3 at `25'2` with the record "shard 1 from `25'1` to `25'2`" (epoch 25), shard 1 at `25'1` without a record |
| Live primary before the kill | its info held that record at the first observation; the interval had started at epoch 24 |

The outage, identical in both controls:

| | Before the kill | Without osd.2 | After the return |
| --- | --- | --- | --- |
| Map epoch of both placement records | 25 | 27 | 31 at the first clean observation, 32 at the capture |
| Up and acting sets | `[3,2,1,0]` | `[3,NONE,1,0]` | `[3,2,1,0]` |
| PG, primary, shard, address, pg_temp | `3.7`, osd.3, 0, unchanged, none | the same | the same |
| Primary's state | `active+clean` | `active+undersized+degraded` | `active+clean` |
| Interval started at epoch (`same_interval_since`) | 24 | 26 | 30 |
| Accepted at observation | | second or third; `peering`, once after `active+clean`, was seen before it | fourth; `active+undersized+degraded` and `peering` were seen before it |

The forced kill was recorded with exit code 137 for `osd.2` in every run.
The primary's `peer_info` listed shards 3 and 2 while osd.2 was away and
all three peers after the return. The interval still started at epoch 30
after the last capture, and the cleanup found the PG `active+clean` at
epoch 32 on its original up and acting sets and primary without a pg_temp.

### Return without a write

While osd.2 was away the primary's own info was at `25'2` with the record
"shard 1 from `25'1` to `25'2`" (epoch 27); the native readback returned
the overwrite's bytes at user version 2 and the Go read the same 16,384
bytes. After the return the primary's view of shard 1 (`peer_info` of
`2(1)`) showed `last_update` and `last_complete` `25'2`. Capture at epoch
32:

| Store copy | Info `pgid` / `shard` | Role | Entries held | Last entry | Log head = `last_update` = `last_complete` | Missing set | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | `3.7s0` / `0` | data | `25'1`, `25'2` | `25'2` | `25'2` | empty | shard 1 from `25'1` to `25'2` (31) | 1,299 bytes |
| osd.2 | `3.7s1` / `1` | nonprimary data | `25'1` | `25'1` | `25'2`, past the last entry | empty | shard 1 from `25'1` to `25'2` (31) | 632 bytes |
| osd.1 | `3.7s2` / `2` | parity | `25'1`, `25'2` | `25'2` | `25'2` | empty | shard 1 from `25'1` to `25'2` (31) | 1,299 bytes |
| osd.0 | `3.7s3` / `3` | parity | `25'1`, `25'2` | `25'2` | `25'2` | empty | shard 1 from `25'1` to `25'2` (31) | 1,299 bytes |

Shard 1's log dump had the same length as in the first capture: the same
single entry under a head of `25'2` instead of `25'1`. Its info reported
`last_user_version` 1 and `last_backfill` `MAX`. The native readback and the
Go read after the return returned the overwrite's bytes at user version 2.

### Degraded write, then recovery

The degraded write was `client.4722.13:3`, 15 bytes at offset 4101, replay
version `27'3`, user version 3. The native readback returned the expected
bytes at user version 3 while osd.2 was away, and the Go read the same
bytes. The live primary's info held the record "shard 1 from `27'3` to
`27'3`" (epoch 27) at the first observation after the write. After the
return the primary's info was at `27'3` with that record (epoch 31); its
view of shard 1 showed `last_update` `27'3` and `last_complete` `25'1`
while it reported the PG `active+clean`. That view is the primary's memory
and is recorded only. Capture at epoch 32:

| Store copy | Info `pgid` / `shard` | Role | Full write `25'1` | Overwrite `25'2` | Degraded write `27'3` | Log head = last entry = `last_update` = `last_complete` | Missing set | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | `3.7s0` / `0` | data | held | held, names `{0,2,3}` | held, names every shard | `27'3` | empty | shard 1 from `27'3` to `27'3` (31) | 1,965 bytes |
| osd.2 | `3.7s1` / `1` | nonprimary data | held | held, names `{0,2,3}` | held, names every shard | `27'3` | empty | shard 1 from `27'3` to `27'3` (31) | 1,965 bytes |
| osd.1 | `3.7s2` / `2` | parity | held | held, names `{0,2,3}` | held, names every shard | `27'3` | empty | shard 1 from `27'3` to `27'3` (31) | 1,965 bytes |
| osd.0 | `3.7s3` / `3` | parity | held | held, names `{0,2,3}` | held, names every shard | `27'3` | empty | shard 1 from `27'3` to `27'3` (31) | 1,965 bytes |

The four log dumps had the same SHA-256 within each run: shard 1's copy
held byte for byte the dump of the other three, including the overwrite's
entry with prior version `25'1` and `written_shards` `{0,2,3}`. The
degraded write's entry carried prior version `25'2`. Shard 1's info
reported `last_user_version` 3 and `last_backfill` `MAX`. The native
readback and the Go read after the recovery returned the degraded write's
bytes at user version 3.

In every capture each copy reported `whoami` equal to its OSD ID and tail
`0'0`, each OSD was paused between 733 and 835 ms and each archive stream
was 1,073,762,304 bytes. No near miss had a position on the primary or in
any copy. After every run the Docker host had no container with the
Testcontainers label and no reader container left.

### Neighbouring rows

The oracle mode these rows share gained an action and its request check
changed, and the fixture's selector lists grew. Each of the four rows
passed in the first archive of `2fdf93c`:

| Row | Test, row | Observation |
| --- | --- | --- |
| `...t-osderasureoptimizedk2m2partialwritelaterwriteshardhistory` | 110.16 s, 166 s | the PG stayed in the interval that started at epoch 24 |
| `...t-osderasureoptimizedk2m2partialwritenewintervalshardhistory` | 164.38 s, 220 s | the interval changed from 24 to 27, the cleanup found the PG clean at epoch 28 |
| `...t-osderasureoptimizedk2m2partialwriteshardhistory` | 59.94 s, 116 s | PASS |
| `...t-osderasureoptimizedk2m2copyfromhistory` | 64.95 s, 121 s | PASS |

### Negative controls

Two native negative controls ran on remote copies of the working tree of
commit `2fdf93c`, which were then discarded. Each changed one test's
sequence and kept its expectation of the shard that returns; both failed on
that shard. Two earlier runs of the same mutations, before the final
wording of the failure message, failed at the same places.

- Return without a write. The Go client sent one more write of 15 bytes at
  offset 64 (incarnation 21) while osd.2 was away. It was acknowledged at
  `27'3`, read back natively, and added to the expected writes as one that
  names every shard, so the restated expectation still says that the return
  gives shard 1 nothing beyond its own entries. The holders' record list was
  widened to admit the reset record that this write leaves, so that the
  judgement reached shard 1's log; without that widening the first run
  failed earlier, on shard 0, with `holds the partial-write records
  [{Shard:1 From:27'3 To:27'3}]; want one of [[{Shard:1 From:25'1
  To:25'2}]]`. Shard 1's copy then held all three entries, `25'2` included,
  and the test failed with `shard 3.7s1 (osd.2, nonprimary data) holds #1 #2
  #3, head 27'3 at its last entry; the writes [...] require that it holds #1
  #3, head 27'3 at its last entry`.
- Degraded write, then recovery. The degraded write was sent before osd.2
  was killed, in the interval of the overwrite, and nothing was written
  during the outage. It was acknowledged at `25'3`, named every shard by
  the source's rule for a write inside shard 1's chunk, and reset the
  holders' record to `25'3` before the kill. After the return shard 1's copy
  held `25'1` and `25'3` without the overwrite's entry, and the test failed
  with `shard 3.7s1 (osd.2, nonprimary data) holds #1 #3, head 25'3 at its
  last entry; the recovery after the writes [...] requires that it holds #1
  #2 #3, head 25'3 at its last entry`.

In both runs the cleanup found the PG `active+clean` on its original
placement with osd.2 back, and the Docker host had no container with the
Testcontainers label left.

## Limits

- Both controls ran one placement of one PG on one profile: secure, aes256k
  client and service keys, IPv4, container runtime, replicated-pg-v8, four
  BlueStore OSDs, isa k=2,m=2 with optimizations, stripe width 8192. Other
  widths, more than one nonprimary shard and plugins with a chunk mapping
  were not run.
- The OSD leaves by a forced kill. An orderly stop, an OSD marked out while
  it runs and a network partition were not run.
- Only the left-out shard's OSD leaves. The outage of a parity shard's OSD
  or of the primary's OSD, two members away at once (which leaves the PG
  below `min_size`) and a primary change were not run; see the source basis
  for why the first is not a left-out-shard question and
  [not built](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md#not-built-primary-replacement)
  for the last.
- One write is sent while the shard is away, and it is the first of its
  interval. A second degraded write that would leave the absent shard out
  by its bytes, a write during the recovery itself, backfill instead of log
  recovery, and the invalidation of the ranges on a recovery target were
  not run.
- The second control makes the return's peering deterministic by waiting
  for the reset range on the live primary. What the returned shard holds
  when the return precedes that roll-forward is stated from the source only.
- The returned shard's chunk bytes are not read. After the recovery three
  other shards remain and two suffice to decode, so a client read does not
  show which bytes that shard stores; the shard's own empty missing set and
  `last_complete` are its only statement about the object.
- The PG is not captured while a member is away or while it recovers; the
  capture refuses an incomplete acting vector, and the missing set of the
  returning shard between its activation and the end of the recovery was
  not observed.
- The copies are sequential captures of paused containers and are not one
  atomic snapshot across the four stores. Head and tail checks cannot detect
  an omitted interior entry.
- The primary's view of a peer in `pg query` is recorded only. It is the
  primary's memory, not the peer's store.
- No trimming, duplicate tier, replay or deduplication behaviour is
  exercised. The proposed history-eviction resubmission cell still needs a
  complete absence judgement for every acting member at the intended PG
  state. These controls add one more fact that judgement has to account
  for: whether a shard's own log holds an entry it was left out of depends
  on whether the shard was caught up from the primary's log after a later
  write, so the same acknowledged history leaves different logs on that
  shard after a return without a write and after a recovery.

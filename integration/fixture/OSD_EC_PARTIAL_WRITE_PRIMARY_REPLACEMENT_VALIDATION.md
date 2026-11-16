# A shard left out of a partial write: its log after an outage of the primary's OSD

This is a test-only control for issue #9. It continues the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md),
which ends with a nonprimary data shard whose own PG log does not hold an
acknowledged overwrite's entry, and reuses its
[shard-local capture](OSD_EC_COPY_FROM_SHARD_HISTORY_VALIDATION.md) and the
PG observation of the
[earlier continuations](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md) and
of the [outage controls](OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md). Those
controls kept the primary. This one takes the primary's own OSD out of the
PG, writes once through the member that leads instead, and brings the OSD
back. It answers three narrow questions:

- which member leads the PG while the primary's OSD is away, and can the
  left-out shard lead?
- does the replacement hand the left-out shard the entry it was left out of?
- once the original primary is back and the PG is clean, which entries do
  the returned primary and the left-out shard hold?

It adds no product API, opcode or flag. The OSD leaves through the fixture's
forced kill of an owned OSD and returns through its start, the daemon
control that the existing failover tests use; no other fault is injected, no
cluster-wide flag is set and no daemon configuration changes. The control
does not state that an identity was evicted, trimmed or lost, and it
resubmits nothing.

Reference: Ceph v20.2.4, source
`7f793731f1b39eb4f465e960113d2363c311b964`, pinned image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Source basis

The earlier records state which shards a write names
([partial overwrite](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#source-basis)),
how a partial-write range is recorded, applied during peering and merged
([later state](OSD_EC_PARTIAL_WRITE_LATER_STATE_VALIDATION.md#source-basis)),
and how a member that missed a write is caught up from the primary's log
([outage](OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md#source-basis)). In
addition:

- A shard listed in the pool's `nonprimary_shards` cannot lead. When the
  map names its OSD first, that OSD asks for a pg_temp of the acting set it
  wants instead of activating
  ([request](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L2639-L2664)).
  The MON stores a pg_temp of an optimized pool with the shards that can
  lead first, in shard order, and the nonprimary shards after them
  ([order](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L2968-L2989)).
  Without a primary_temp, the first member of that vector that exists and
  is up becomes primary
  ([temporary mapping](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/OSDMap.cc#L3060-L3093)).
  With `nonprimary_shards` `{1}` the order is shards 0, 2, 3, 1, so with
  shard 0's position empty the first parity shard, shard 2, leads.
- Every shard that can be primary records the left-out shard's range when it
  rolls the overwrite forward
  ([record](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGBackend.cc#L475-L490)),
  so the parity shard that now leads holds the same range as the old
  primary. Its peering applies that range to the left-out shard's info,
  finds the shard up to date and activates it with its info alone
  ([range application](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L324-L364),
  [activation](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3026-L3051)).
  The left-out shard does not receive the overwrite's entry from the new
  primary.
- The primary's departure ends the interval, so the next write is the first
  of its interval and names every shard
  ([interval change](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L998-L1010),
  [choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECTransaction.cc#L597-L603)),
  although by its bytes it leaves shard 1 out. The left-out shard is a
  member and gets the entry; its log accepts an entry newer than its head
  without the version in between
  ([append](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.h#L653-L675)).
  The absent shard 0 gets no sub-write
  ([dispatch](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/ECCommon.cc#L812-L889)).
  The roll-forward on the new primary resets the left-out shard's range to
  the empty range at that write.
- At the return the up set names the original primary again while the
  pg_temp still keeps it out of the acting set. The replacement compares the
  members' logs. The returning OSD is one entry behind, far below the cost
  at which a member is recovered asynchronously outside the acting set
  ([cost](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L2380-L2392),
  `osd_async_recovery_min_cost` default 100 in
  [options](https://github.com/ceph/ceph/blob/v20.2.4/src/common/options/global.yaml.in#L3172-L3179)),
  so the acting set it wants is the up set, and it asks for the pg_temp to
  be removed
  ([removal](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L2652-L2659)).
  The original primary then leads again.
- That primary takes the authoritative log from a shard that can be primary
  ([best info](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L1696-L1703),
  [choice](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L2514-L2517)),
  merges the entry it lacks into its own log
  ([master log](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PeeringState.cc#L3360-L3489),
  [merge](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/PGLog.cc#L455-L514)),
  and the object is missing on it until it is recovered. Shard 0 is not a
  nonprimary shard, so every entry, whatever it names, applies to it
  ([missing set](https://github.com/ceph/ceph/blob/v20.2.4/src/osd/osd_types.h#L5161-L5203)).
  The left-out shard is already at the authoritative head and is sent
  nothing.
- The restarted primary has another address in the map: in every run the
  IP and port were the same and the nonce was new. This is an observation,
  not a source statement; the control accepts the address as the only
  changed field of the recorded placement after the return, and requires it
  to differ.

Not exercised by this control, from the same source:

- A return before the write's roll-forward on the replacement. The control
  waits for the reset range on the replacement before it starts the OSD, as
  the degraded-write outage control does.
- The asynchronous recovery of the returning primary: with more than 100
  entries to recover the replacement would keep it out of the acting set
  under a pg_temp. The control writes once.
- A primary outage without a write. The replacement's own peering then
  gives the left-out shard nothing, as the outage of the left-out shard's
  own OSD without a write does; the control writes once so that the return
  has something to recover.

No Ceph source is incorporated into the product.

## The observation of a PG under another primary

`tools/osd_pool_oracle.py` mode `pginterval` gains action `replaced`. Like
actions `show` and `absent` it sends no command: it reads the native
placement of the control's object, one `pg query` of the primary that
placement names and the placement again. `replaced_failures` in
`tools/osd_pg_interval_oracle.py` accepts the observation only if

- the caller's recorded placement is a complete erasure-coded acting set
  without a pg_temp, led by the OSD of its primary shard; the pool's own map
  entry lists nonprimary shards, as many as the placement counts, without
  the primary's; the recorded interval start is not after the recorded
  epoch;
- the two placement records are identical and equal the recorded one in
  every field but a newer epoch, up and acting sets in which exactly the
  recorded primary's position is empty, a pg_temp, the OSD ID, primary ID and
  shard of the first member of the primary-first order (which may not be a
  nonprimary shard), and an address that is not the recorded one;
- that primary reports exactly `active` and `undersized`, with or without
  `degraded`, with those sets, for its own shard, at a map epoch that is not
  behind the placement's;
- the interval it reports started after the recorded one and not after the
  placement's epoch, and the PG has started in it.

The Go test rechecks the receipt in `checkPGReplaced` without relying on the
oracle's verdict, with its own restatement of the primary-first order. The
fixture admits the `pginterval` mode for the selector of this control; it
uses actions `replaced` and `show` only. After the return the control asks
action `show` for the recorded placement with the address of the restarted
primary, which it takes from a placement record that names the same OSD,
primary and shard under another address; every other field is still judged.

The capture is unchanged and still refuses an acting vector with an empty
position, so the control captures before the outage and after the return
only.

## Scenario

`TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration`
runs on the four-BlueStore fixture (`osd-replicas-4`, `replicated-pg-v8`,
secure, aes256k, IPv4, container) and starts with the partial-overwrite
control's sequence, unchanged and with all of its requirements, and with the
first observation of the earlier continuations. Then it

1. observes the live primary until its own info holds exactly the record
   "shard 1, from the full write to the overwrite", at most fifteen times a
   second apart;
2. kills the primary's OSD with the fixture's forced kill and requires its
   SIGKILL receipt (exit code 137 of that daemon);
3. observes, at most 150 times a second apart, until the PG is accepted as
   active under another primary. Every observation in between that has a
   placement and a query must not show an `active` state reported by the OSD
   of a nonprimary shard. The test then adopts the placement record with a
   new session to the new primary; the placement carries a pg_temp, so the
   Go request uses primary-first shard numbering. The new primary's info
   must be at the overwrite with the same record;
4. reads the object back natively and once through the Go client
   (incarnation 12);
5. sends one WRITE of 13 bytes at offset 45 (incarnation 13) through the new
   primary: inside shard 0's chunk of the first stripe, where by its bytes it
   names shards 0, 2 and 3 and leaves shard 1 out. Native readback follows,
   then one Go read (incarnation 14). The test then observes the new
   primary, at most thirty times a second apart, until a held observation
   (below) shows exactly the record "shard 1, from that write to that write"
   in its own info;
6. observes, at most thirty times a second apart, until one observation is
   held: the PG active under the same new primary in the same interval. It
   then starts the OSD and observes, at most 150 times a second
   apart, until the PG is `active+clean` on its original placement, under the
   original primary's new address, without a pg_temp, in an interval that
   started after the one under the replacement;
7. asks the fixture's readiness control for every OSD up and in and every PG
   `active+clean`, observes the PG, at most thirty times a second apart,
   until one observation is held clean in the same interval on the recorded
   placement under the same address, and adopts its placement record with a
   new session;
8. captures every acting shard, reads the object back natively and through
   the Go client (incarnation 15), and observes that the interval did not
   change.

Steps 5 to 7 judge each later observation as the outage controls of
`OSD_EC_PARTIAL_WRITE_RECOVERY_VALIDATION.md` do, with the same
`intervalHeld` and `reobserveInterval`. The MON may publish an epoch that
leaves the PG's sets unchanged before the primary has it; a sample from a
primary behind the placement's epoch, or without a placement record or a
query reply, or with placement records that differ around the query, is
inconclusive and is observed again. It never counts as held. A primary that
reports another interval start, even from an older map, has left the
accepted interval, and every other refusal is a confirmed departure; both
stop the test at once. Under the replacement, `checkPGStillReplaced` keeps
`checkPGReplaced`'s judgement of every sample and requires the accepted
replacement's OSD, shard and address. After the return, `checkPGReturned`
judges every sample against the recorded placement under the restarted
primary's address, so a record under another address is a departure. Each
sample's checks are unchanged. Before these, the pre-start check and the
records wait failed at the first sample from a primary behind the
placement's epoch, and the clean observation after the readiness control was
a single sample.

Every Go request is sent once; a failed or unknown result stops the test and
nothing is resent. Each Go read must return the bytes and the user version
of the acknowledged write that produced them. A shard's role comes from its
shard ID, the native width and the native nonprimary shards. The acting
index equals the shard ID because the capture selected, and the store
returned, collection `<pgid>s<i>` whose info names shard `i`.

The cleanup starts the OSD again if the test did not and requires, in at
most 150 observations, the PG `active+clean` on its original up and acting
sets, primary and shard, under the restarted primary's new address, without
a pg_temp, before the pool is removed.

| Statement | Status |
| --- | --- |
| The PG active and undersized under the first member of the primary-first order, the first parity shard's OSD on shard 2, with a pg_temp and exactly shard 0's position empty, in a newer interval | required |
| No observation taken while the primary's OSD is away shows the PG `active` under the OSD of a nonprimary shard | required of every observation taken; a state between two observations is not seen |
| The new primary's info at the overwrite with the record "shard 1, from the full write to the overwrite" | required: every shard that can be primary records the range |
| Native bytes and user version unchanged while the primary is away; the Go read through the new primary returns the bytes and that user version | required |
| The write acknowledged ONDISK with a replay version whose epoch is not older than the start of the interval under the replacement; native bytes and user version equal; the Go read returns the bytes and that user version | required |
| By its bytes the write names shards 0, 2 and 3 only | required, from the restated plan |
| Before the OSD starts, the new primary's info holds exactly the record "shard 1, from the write to the write" | required: the return's peering reads it |
| The PG `active+clean` on the same PG, up and acting sets, primary and shard, without a pg_temp, under another address of the same OSD, in an interval that started after the one under the replacement | required; it includes the recovery of the object to the returned primary |
| After the return, the returned primary's copy (shard 0) and both parity shards hold all three entries; their head, `last_update` and `last_complete` are the write's version; their missing sets are empty | required |
| After the return, shard 1 holds the full write's and the write's entries and not the overwrite's; its head, `last_update` and `last_complete` are the write's version, at its last entry; its missing set is empty | required: the replacement found it up to date through the range, and the write named it as the first of its interval |
| The write's entry names every shard on every copy | required |
| After the return, every shard's info holds exactly the record "shard 1, from the write to the write" | required |
| Native bytes and user version of the write after the return; the Go read returns the bytes and that user version | required |
| The transient epoch at which the map names the left-out shard's OSD first, the intermediate interval in which the up set has the original primary again under the pg_temp, the epochs, the number of observations, the primary's view of each peer, the epoch of the partial-write records, the addresses | recorded |
| Cleanup: the PG is clean on its original placement without a pg_temp | required |

The statements about shard 1 are observations of that shard's own store copy
at capture time, between two identical placement records. It holds an entry
for a write that did not change its bytes and lacks one for a write it did
not take part in; that says how the shard's log came to be, not that either
entry was evicted, lost or replayed.

The control does not read the returned primary's chunk. That the object was
recovered to it is stated by its own empty missing set with `last_complete`
at its head and by the PG being clean; the object bytes are checked through
the native client and the Go client, which decode from any two of the four
shards.

## Cluster-free checks

- `tools/test_osd_pg_interval_oracle.py` gains three tests and extends one.
  The judgement accepts seven observations of a PG under another primary
  (two state forms, an interval that starts at the placement's epoch, a
  primary ahead of the placement, a recorded interval that started at the
  recorded epoch, and a pool whose nonprimary shard is 2, which makes
  shard 1 lead), pins the primary-first order for k=2,m=2 and k=4,m=2, and
  refuses thirty-seven changed observations, sixteen requests and a pool
  whose remaining members are all nonprimary. The mode with a fake native
  client: one accepted observation that queries the primary the placement
  names, seven unaccepted ones (among them the transient lead of the
  left-out shard and an unreadable or empty nonprimary list in the map), and
  nine more invalid requests, among them `replaced` with a field of
  `absent` and `show` or `repeer` with a field of `replaced`.
- `TestCheckPGReplaced` accepts six receipts, pins the same two orders and
  refuses thirty-three changed receipts and ten requests. It accepts the
  restarted primary's placement with only its address changed and refuses
  five other changes.
- `TestReobserveReplaced` drives the later observations under the
  replacement with synthetic samples. It holds a sample at once and at a
  newer epoch, after lagging samples, once the records appear after a
  lagging sample, and after a lagging sample under another address. It
  fails on persistent lag and on records that never appear, both after the
  bound. It fails at once on the replacement under another address, on
  another replacement (lagging before it or not), on a later interval from a
  lagging primary, on the original primary back, and on a peering state
  after a lagging sample. With lag treated as a departure it failed on six
  cases, and without the address check it held the replacement under
  another address.
- `TestReobserveReturned` (in the recovery controls' file) drives the clean
  observation after the readiness control. It holds a sample at once, at a
  newer epoch and after lagging samples, and fails on persistent lag, on a
  later interval (lagging or not, or after a lagging sample), on the primary
  under another address and on a recovering state. With lag treated as a
  departure it failed on four cases.
- `TestShardHistoryCaptureAdmission` in the fixture pins the seven capture
  selectors and the five selectors that admit the interval mode.

## Reproduction

On an authorized Linux runner, hold its run lock, use the pinned image and
the environment of the
[partial-overwrite control](OSD_EC_PARTIAL_WRITE_SHARD_HISTORY_VALIDATION.md#reproduction)
with

```sh
CEPH_MSGR_TEST_RUN='^TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration$'
```

The dedicated mandatory row is
`osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureoptimizedk2m2partialwriteprimaryreplacementshardhistory`.
Request it with the CI comment command. Basic PR CI builds and unit-checks
the fixture but does not run this daemon test. The native result of a
pull-request head is recorded in that pull request with the tested SHA. A
pass at one SHA is not evidence for a later revision.

## Local native calibration

These runs preceded the pull request and are not exact-SHA CI receipts.
They used the catalog row's environment and the pinned image on a Linux
amd64 host with Docker 29.9.1 and Go 1.26.9, through `integration/run.sh`
and the native result gate, with one test process on the engine. Every row
ran in its own fixture.

The recorded runs used three archives of commit `63b12f1`, which added the
row; later commits change documents only. Each archive ran the row, the
first as the first run of that commit, and the first archive also ran the
six neighbouring rows below. The gate recorded the test as PASS with no
problem in all three runs:

| Archive of `63b12f1` | Test, row |
| --- | --- |
| first | 128.10 s, 193 s |
| second | 127.40 s, 192 s |
| third | 122.30 s, 188 s |

Two exploratory runs on remote copies of the working tree, which were then
discarded, preceded the test. The first logged every observation of an
earlier draft with and without the write; both drafts failed after the
return because they compared the clean PG with the recorded placement in
every field, and the restarted primary had another address
(`placement address is 'v2:...:6800/2230037947'; it was
'v2:...:6800/2432550315' before the command`, the port unchanged and the
nonce new). The judgement of the return was changed to take that one field
from a placement record that names the same OSD, primary and shard; the
second run, of the final test on the working tree, passed (127.10 s).

Every run observed the same PG, acting set, roles and per-shard result. The
fixture's topology is deterministic, so they are repetitions of one
placement, not different ones. In the second archive every epoch was one
lower (the writes at `24'1`, `24'2` and `27'3`), and in the third the
client's global ID was 4716 instead of 4722; nothing else differed. The
values below are those of the first archive.

| Observation | Value |
| --- | --- |
| Geometry | stripe_width 8192, chunk 4096, profile k=2, m=2, plugin isa, no mapping, `nonprimary_shards` `{1}` |
| Placement | PG `3.7`, up = acting = `[3,2,1,0]`, primary osd.3 on shard 0; osd.2 holds nonprimary data shard 1, osd.1 and osd.0 the parity shards 2 and 3 |
| Full write | `client.4722.7:1`, replay version `25'1`, user version 1, names every shard |
| Partial overwrite | `client.4722.9:2`, 14 bytes at offset 5, replay version `25'2`, user version 2, names `{0,2,3}` |
| First capture | at epoch 25, as in the partial-overwrite control: shards 0, 2 and 3 at `25'2` with the record "shard 1 from `25'1` to `25'2`" (epoch 25), shard 1 at `25'1` without a record |
| Live primary before the kill | its info held that record at the first observation; the interval had started at epoch 24 |

The outage:

| | Before the kill | Under the replacement | After the return |
| --- | --- | --- | --- |
| Map epoch of both placement records | 25 | 28 | 32 |
| Up and acting sets | `[3,2,1,0]` | `[NONE,2,1,0]` | `[3,2,1,0]` |
| Primary, shard, pg_temp | osd.3, 0, none | osd.1, 2, present | osd.3, 0, none |
| Primary's address | the recorded one | osd.1's | osd.3's, same IP and port, another nonce |
| Primary's state | `active+clean` | `active+undersized+degraded` | `active+clean` |
| Interval started at epoch (`same_interval_since`) | 24 | 27 | 31 |
| Accepted at observation | | second; the third archive's third, after `unknown` led by osd.2 on shard 1 without a pg_temp at epoch 26 and `peering` led by osd.1 under the pg_temp at epoch 27 | fifth |

The forced kill was recorded with exit code 137 for `osd.3` in every run. In
the first and second archive the one observation before the accepted one
carried no complete pair of placement and query, so no state was recorded
for it; the left-out shard's lead at the first epoch after the kill was
seen in the third archive and in both exploratory runs, always as
`unknown`, never active. The exploratory runs also showed the intermediate
interval of the return: at epoch 30 the up set was `[3,2,1,0]` again while
the pg_temp kept the acting set `[NONE,2,1,0]` under osd.1, which reported
`remapped`; at epoch 31 the pg_temp was gone and osd.3 led in `peering`,
reporting its own `last_update` `28'3` and `last_complete` `25'2` in the run
with the write; at epoch 32 it reported `active+clean`.

While osd.3 was away the new primary's info was at `25'2` with the record
"shard 1 from `25'1` to `25'2`" (epoch 28), and its view of shard 1
(`peer_info` of `2(1)`) showed `last_update` and `last_complete` `25'2`. The
native readback returned the overwrite's bytes at user version 2 and the Go
read the same 16,384 bytes at user version 2. The write was
`client.4722.13:4`, 13 bytes at offset 45, replay version `28'3`, user
version 3; the native readback and the Go read returned its bytes at user
version 3. The new primary's info held the record "shard 1 from `28'3` to
`28'3`" (epoch 28) at the first observation after the write. After the
return osd.3's info was at `28'3` with that record (epoch 32); its view of
shard 1 still listed the older record, which is the primary's memory and is
recorded only. Capture at epoch 32:

| Store copy | Info `pgid` / `shard` | Role | Full write `25'1` | Overwrite `25'2` | Write `28'3` | Log head = last entry = `last_update` = `last_complete` | Missing set | Partial-write records (epoch) | Log dump |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| osd.3 (primary) | `3.7s0` / `0` | data | held | held, names `{0,2,3}` | held, names every shard | `28'3` | empty | shard 1 from `28'3` to `28'3` (32) | 1,961 bytes |
| osd.2 | `3.7s1` / `1` | nonprimary data | held | not held | held, names every shard | `28'3` | empty | shard 1 from `28'3` to `28'3` (32) | 1,294 bytes |
| osd.1 | `3.7s2` / `2` | parity | held | held, names `{0,2,3}` | held, names every shard | `28'3` | empty | shard 1 from `28'3` to `28'3` (32) | 1,961 bytes |
| osd.0 | `3.7s3` / `3` | parity | held | held, names `{0,2,3}` | held, names every shard | `28'3` | empty | shard 1 from `28'3` to `28'3` (32) | 1,961 bytes |

The three log dumps of shards 0, 2 and 3 had the same SHA-256. The native readback and the
Go read after the return returned the write's bytes at user version 3, and
the interval still started at epoch 31 after the last capture. In both
captures of the first archive each copy reported `whoami` equal to its OSD
ID and tail `0'0`, each OSD was paused between 731 and 809 ms and each
archive stream was 1,073,762,304 bytes. No near miss had a position on the primary or in any
copy. The cleanup found the PG `active+clean` at epoch 32 on its original
up and acting sets, primary and shard, without a pg_temp, under osd.3's new
address. After every run the Docker host had no container with the
Testcontainers label left.

### Neighbouring rows

The oracle mode these rows share gained an action, and the fixture's
selector lists grew. Each of the six rows passed in the first archive of
`63b12f1`:

| Row | Test, row |
| --- | --- |
| `...t-osderasureoptimizedk2m2partialwriteshardreturnshardhistory` | 123.58 s, 179 s |
| `...t-osderasureoptimizedk2m2partialwritedegradedwriteshardhistory` | 121.19 s, 177 s |
| `...t-osderasureoptimizedk2m2partialwriteshardhistory` | 59.85 s, 116 s |
| `...t-osderasureoptimizedk2m2partialwritelaterwriteshardhistory` | 109.86 s, 166 s |
| `...t-osderasureoptimizedk2m2partialwritenewintervalshardhistory` | 163.78 s, 220 s |
| `...t-osderasureoptimizedk2m2copyfromhistory` | 64.64 s, 121 s |

### Negative control

One native negative control ran on a remote copy of an archive of
`63b12f1`, which was then discarded. Through the replacement primary the Go
client first sent one more write of 12 bytes at offset 85 (incarnation 21),
inside shard 0's chunk, and then the write of the scenario. The expectation
still said that the scenario's write names every shard. The records the
test waits for and requires were widened to "shard 1 from the first write
to the scenario's write", and the capture admitted a head past the entries
on the nonprimary shard, so that the judgement reached the shard logs. The
first write was acknowledged at `28'3` and the scenario's write at `28'4`;
the new primary's info held the record "shard 1 from `28'3` to `28'4`". The
test failed on the first shard it judged, with `shard 3.7s0 (osd.3, data)
contradicts the acknowledged writes: write under the replacement: its entry
names shards [0 2 3]; want [0 1 2 3]`: a write that is not the first of its
interval names the shards its bytes touch. The cleanup found the PG
`active+clean` on its original placement with osd.3 back, and the Docker
host had no container with the Testcontainers label left.

## Limits

- The control ran one placement of one PG on one profile: secure, aes256k
  client and service keys, IPv4, container runtime, replicated-pg-v8, four
  BlueStore OSDs, isa k=2,m=2 with optimizations, stripe width 8192. Other
  widths, more than one nonprimary shard and plugins with a chunk mapping
  were not run. With four OSDs and size 4 the PG cannot remap, so the
  replacement is always a member that already held its shard.
- The primary's OSD leaves by a forced kill. An orderly stop, an OSD marked
  out or down while it runs, the cluster-wide `noup` flag and a network
  partition were not run.
- One write is sent while the primary is away, and it is the first of its
  interval. No write is sent while the primary recovers, the primary's
  recovery is synchronous (one entry), and a primary outage without a write
  was not run; the replacement's own peering is stated from the source and
  from the new primary's view of shard 1 only.
- The left-out shard's lead at the first epoch after the kill was not seen
  in two of the three archive runs, because the observation that fell in
  that epoch had no complete query. The statement that it does not activate
  holds for the observations taken.
- The PG is not captured while the primary is away or while it recovers;
  the capture refuses an incomplete acting vector. The returned primary's
  chunk bytes are not read.
- The copies are sequential captures of paused containers and are not one
  atomic snapshot across the four stores. Head and tail checks cannot detect
  an omitted interior entry.
- The primary's view of a peer in `pg query` is recorded only. It is the
  primary's memory, not the peer's store.
- No trimming, duplicate tier, replay or deduplication behaviour is
  exercised. The proposed history-eviction resubmission cell still needs a
  complete absence judgement for every acting member at the intended PG
  state. This control adds one more fact that judgement has to account
  for: a primary replacement does not hand a nonprimary shard the entry it
  was left out of, because a shard that can lead carries the same range.
